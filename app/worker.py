import asyncio
import json
import logging
import time

from . import config, db
from .mock_client import MockAPIClient
from .rate_limiter import SlidingWindowRateLimiter

log = logging.getLogger("linkplease.worker")


class Worker:
    def __init__(self):
        self.client = MockAPIClient()
        self.limiter = SlidingWindowRateLimiter(config.RATE_LIMIT_MAX_CALLS, config.RATE_LIMIT_WINDOW_SECONDS)
        self._stop = asyncio.Event()
        self._send_sem = asyncio.Semaphore(config.SEND_CONCURRENCY)

    async def stop(self):
        self._stop.set()
        await self.client.close()

    async def run(self):
        while not self._stop.is_set():
            try:
                await self.process_new_events()
                await self.dispatch_pending_sends()
                await self.reconcile_queued()
            except Exception:
                log.exception("worker tick failed")
            await asyncio.sleep(config.WORKER_TICK_SECONDS)

    # ---------------------------------------------------------- events --

    async def process_new_events(self):
        events = await db.fetch_unprocessed_events(limit=200)
        for row in events:
            try:
                payload = json.loads(row["payload_json"])
                await self._handle_event(row["event_id"], row["event_type"], payload)
            except Exception:
                log.exception("failed to process event %s", row["event_id"])
                # Don't mark processed - we'll retry it next tick rather
                # than silently drop it.
                continue
            await db.mark_event_processed(row["event_id"])

    async def _handle_event(self, event_id: str, event_type: str, payload: dict):
        data = payload.get("data", {})

        if event_type == "comment.deleted":
            comment_id = data.get("comment_id")
            if not comment_id:
                return
            await db.mark_comment_deleted(comment_id)
            await db.cancel_pending_tasks_for_comment(comment_id)
            return

        if event_type == "comment.created":
            comment_id = data.get("comment_id")
            post_id = data.get("post_id")
            text = data.get("text", "") or ""
            created_at = data.get("created_at")
            frm = data.get("from", {}) or {}
            user_id = frm.get("user_id")
            username = frm.get("username")

            if not comment_id or not user_id:
                return

            await db.upsert_comment_created(comment_id, post_id, user_id, username, text, created_at)

            # A comment.deleted for this comment may have arrived first
            # (order isn't guaranteed). If so, don't create any DM tasks.
            comment = await db.get_comment(comment_id)
            if comment and comment["deleted"]:
                return

            rules = await db.get_all_rules()
            text_lower = text.lower()
            for rule in rules:
                if rule["keyword"].lower() in text_lower:
                    created = await db.try_create_dm_task(
                        rule["rule_id"], user_id, comment_id, rule["dm_message"]
                    )
                    if not created:
                        await db.log_blocked_duplicate(
                            rule["rule_id"], user_id, event_id, comment_id,
                            reason="user_already_matched_this_rule",
                        )
            return

        # Unknown event types are logged and ignored, not dropped silently
        # from a stats perspective - see FAILURES.md.
        log.warning("unrecognized event_type: %s", event_type)

    # ------------------------------------------------------------ sends --

    async def dispatch_pending_sends(self):
        tasks = await db.fetch_pending_tasks(limit=50)
        coros = [self._send_one(t) for t in tasks]
        if coros:
            await asyncio.gather(*coros)

    async def _send_one(self, task_row):
        task_id = task_row["id"]
        async with self._send_sem:
            claimed = await db.claim_task_for_sending(task_id)
            if not claimed:
                return  # another coroutine already grabbed it, or it moved on

            # Re-check: a comment.deleted may have arrived between us
            # reading this task and claiming it.
            if task_row["comment_id"]:
                comment = await db.get_comment(task_row["comment_id"])
                if comment and comment["deleted"]:
                    # Never attempted a send - this must not count as
                    # "failed" (that bucket means "gave up after retries").
                    # It falls out of every /stats bucket entirely.
                    await db.mark_task_cancelled(task_id, "comment deleted before send")
                    return

            attempts_used = task_row["attempts"] + 1
            idem_key = f"{task_row['rule_id']}:{task_row['user_id']}:{attempts_used}"

            await self.limiter.acquire()
            status, body, retry_after = await self.client.send_dm(
                recipient_user_id=task_row["user_id"],
                message=task_row["message"],
                comment_id=task_row["comment_id"],
                idempotency_key=idem_key,
            )

            if status in (200, 202) and body and body.get("dm_id"):
                await db.mark_task_queued(task_id, body["dm_id"], attempts_used)
                return

            if status == 429:
                # A 429 means the request was never actually processed by
                # their server - it's our fault for asking too soon, not a
                # real delivery attempt. Don't burn the retry budget on it:
                # retry with the same attempts count (and same idempotency
                # key) rather than incrementing.
                await self.limiter.penalize(retry_after or 5.0)
                backoff = retry_after or self._backoff_seconds(task_row["attempts"] + 1)
                await db.mark_task_retry(task_id, task_row["attempts"], time.time() + backoff, "rate_limited")
                return

            attempts = attempts_used

            if status == 400:
                detail = (body or {}).get("detail", "invalid_request")
                log.warning(
                    "DM SEND 400 | recipient=%s | comment_id=%s | idem_key=%s | body=%s",
                    task_row["user_id"], task_row["comment_id"], idem_key, body,
                )
                await db.mark_task_failed(task_id, attempts, f"invalid_request: {detail}")
                return

            # 500, network error, unexpected status - all retryable up to
            # MAX_SEND_ATTEMPTS.
            if attempts >= config.MAX_SEND_ATTEMPTS:
                log.warning(
                    "DM SEND gave up after max attempts | recipient=%s | comment_id=%s | "
                    "idem_key=%s | last_status=%s | last_body=%s",
                    task_row["user_id"], task_row["comment_id"], idem_key, status, body,
                )
                await db.mark_task_failed(task_id, attempts, f"max attempts reached, last status={status}")
                return

            backoff = self._backoff_seconds(attempts)
            await db.mark_task_retry(task_id, attempts, time.time() + backoff, f"status={status}")

    @staticmethod
    def _backoff_seconds(attempts: int) -> float:
        base = config.BASE_BACKOFF_SECONDS * (2 ** (attempts - 1))
        return min(base, config.MAX_BACKOFF_SECONDS)

    # ------------------------------------------------------- reconcile --

    async def reconcile_queued(self):
        """Part C: a 202 from POST /v1/dm/send only means accepted, not
        delivered. Poll GET /v1/dm/{dm_id} until it's terminal, and if it
        comes back failed, resend with a new idempotency key."""
        tasks = await db.fetch_queued_tasks_to_poll(limit=50)
        for t in tasks:
            await self._poll_one(t)

    async def _poll_one(self, task_row):
        task_id = task_row["id"]
        dm_id = task_row["dm_id"]
        if not dm_id:
            return

        status_code, body = await self.client.get_dm_status(dm_id)
        await db.bump_poll(task_id)

        if status_code != 200 or not body:
            if task_row["poll_count"] + 1 >= config.RECONCILE_MAX_POLLS:
                await db.mark_task_failed(task_id, task_row["attempts"], "gave up polling dm status")
            return

        dm_status = body.get("status")
        if dm_status == "delivered":
            await db.mark_task_delivered(task_id)
        elif dm_status == "failed":
            attempts = task_row["attempts"]
            if attempts >= config.MAX_SEND_ATTEMPTS:
                await db.mark_task_failed(task_id, attempts, "mock api reported failed, max attempts reached")
            else:
                await db.requeue_for_resend(task_id, attempts)
        else:
            # still queued - if we've polled way too many times, stop
            # burning cycles on it and mark it failed rather than polling
            # forever.
            if task_row["poll_count"] + 1 >= config.RECONCILE_MAX_POLLS:
                await db.mark_task_failed(task_id, task_row["attempts"], "dm stuck in queued, gave up polling")
