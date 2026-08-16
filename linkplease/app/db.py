"""
All persistence lives here. Every table exists because something in the
mock API's behavior forced it to:

- raw_events:    every webhook delivery we accept, keyed by event_id, so a
                  redelivered event (the API resends ~8% of events) is a
                  no-op the second time it arrives, and so we never rely on
                  an in-memory queue that a crash could wipe out.
- comments:       tracks each comment we've seen, including a `deleted`
                  tombstone flag. Deletes can arrive before creates (order
                  is not guaranteed), so this has to be upsert-safe in
                  either direction.
- rules:          keyword -> dm_message rules.
- dm_tasks:       the actual unit of work: "send rule R's message to user U
                  because of comment C". UNIQUE(rule_id, user_id) is the
                  mechanism that makes "never DM the same user twice for the
                  same rule" true even under concurrent/duplicate delivery,
                  because it's enforced by SQLite at insert time, not by a
                  check-then-act race in Python.
- blocked_duplicates: every time we decline to create a dm_task because one
                  already exists for that (rule_id, user_id), we log it here
                  so GET /stats can report an honest duplicates_blocked count.
"""

import aiosqlite
import asyncio
import json
import time
import uuid
from contextlib import asynccontextmanager

from . import config

_SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    comment_id TEXT,
    payload_json TEXT NOT NULL,
    received_at REAL NOT NULL,
    processed INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS comments (
    comment_id TEXT PRIMARY KEY,
    post_id TEXT,
    user_id TEXT,
    username TEXT,
    text TEXT,
    created_at TEXT,
    deleted INTEGER NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS rules (
    rule_id TEXT PRIMARY KEY,
    keyword TEXT NOT NULL,
    dm_message TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS dm_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    comment_id TEXT,
    message TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',  -- pending, sending, queued, delivered, failed, cancelled
    dm_id TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL,
    last_checked_at REAL,
    poll_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE(rule_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_dm_tasks_status ON dm_tasks(status);
CREATE INDEX IF NOT EXISTS idx_dm_tasks_comment ON dm_tasks(comment_id);

CREATE TABLE IF NOT EXISTS blocked_duplicates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id TEXT,
    user_id TEXT,
    event_id TEXT,
    comment_id TEXT,
    reason TEXT,
    created_at REAL NOT NULL
);
"""

_db_lock = asyncio.Lock()


async def init_db():
    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.executescript(_SCHEMA)
        await db.commit()


@asynccontextmanager
async def get_conn():
    # Single shared lock around writes keeps SQLite happy under concurrent
    # access from the webhook handler + worker loop without needing a
    # separate DB server for an assignment of this size.
    db = await aiosqlite.connect(config.DB_PATH)
    db.row_factory = aiosqlite.Row
    try:
        yield db
    finally:
        await db.close()


def now() -> float:
    return time.time()


# ---------------------------------------------------------------- events --

async def insert_raw_event(event_id: str, event_type: str, comment_id: str, payload: dict) -> bool:
    """Returns True if this was a new event, False if it was a redelivery
    (same event_id we've already stored)."""
    async with _db_lock:
        async with get_conn() as db:
            cur = await db.execute(
                "INSERT OR IGNORE INTO raw_events (event_id, event_type, comment_id, payload_json, received_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (event_id, event_type, comment_id, json.dumps(payload), now()),
            )
            await db.commit()
            return cur.rowcount > 0


async def fetch_unprocessed_events(limit: int = 100):
    async with get_conn() as db:
        cur = await db.execute(
            "SELECT * FROM raw_events WHERE processed = 0 ORDER BY received_at ASC LIMIT ?",
            (limit,),
        )
        return await cur.fetchall()


async def mark_event_processed(event_id: str):
    async with _db_lock:
        async with get_conn() as db:
            await db.execute("UPDATE raw_events SET processed = 1 WHERE event_id = ?", (event_id,))
            await db.commit()


# -------------------------------------------------------------- comments --

async def upsert_comment_created(comment_id: str, post_id: str, user_id: str, username: str, text: str, created_at: str):
    """Creates or updates a comment's content, but never clears an existing
    `deleted` tombstone (a delete can arrive before the create, and it must
    win)."""
    async with _db_lock:
        async with get_conn() as db:
            await db.execute(
                """
                INSERT INTO comments (comment_id, post_id, user_id, username, text, created_at, deleted, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 0, ?)
                ON CONFLICT(comment_id) DO UPDATE SET
                    post_id=excluded.post_id,
                    user_id=excluded.user_id,
                    username=excluded.username,
                    text=excluded.text,
                    created_at=excluded.created_at,
                    updated_at=excluded.updated_at
                """,
                (comment_id, post_id, user_id, username, text, created_at, now()),
            )
            await db.commit()


async def mark_comment_deleted(comment_id: str):
    async with _db_lock:
        async with get_conn() as db:
            await db.execute(
                """
                INSERT INTO comments (comment_id, deleted, updated_at)
                VALUES (?, 1, ?)
                ON CONFLICT(comment_id) DO UPDATE SET deleted=1, updated_at=excluded.updated_at
                """,
                (comment_id, now()),
            )
            await db.commit()


async def get_comment(comment_id: str):
    async with get_conn() as db:
        cur = await db.execute("SELECT * FROM comments WHERE comment_id = ?", (comment_id,))
        return await cur.fetchone()


# ----------------------------------------------------------------- rules --

async def create_rule(keyword: str, dm_message: str) -> str:
    rule_id = f"rule_{uuid.uuid4().hex[:12]}"
    async with _db_lock:
        async with get_conn() as db:
            await db.execute(
                "INSERT INTO rules (rule_id, keyword, dm_message, created_at) VALUES (?, ?, ?, ?)",
                (rule_id, keyword, dm_message, now()),
            )
            await db.commit()
    return rule_id


async def get_all_rules():
    async with get_conn() as db:
        cur = await db.execute("SELECT * FROM rules")
        return await cur.fetchall()


# -------------------------------------------------------------- dm_tasks --

async def try_create_dm_task(rule_id: str, user_id: str, comment_id: str, message: str) -> bool:
    """Attempts to create the (rule_id, user_id) task. Returns True if
    created (i.e. this user has never matched this rule before), False if
    one already existed (duplicate, correctly blocked)."""
    async with _db_lock:
        async with get_conn() as db:
            try:
                await db.execute(
                    """
                    INSERT INTO dm_tasks (rule_id, user_id, comment_id, message, status, attempts, created_at, updated_at)
                    VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)
                    """,
                    (rule_id, user_id, comment_id, message, now(), now()),
                )
                await db.commit()
                return True
            except aiosqlite.IntegrityError:
                await db.rollback()
                return False


async def log_blocked_duplicate(rule_id: str, user_id: str, event_id: str, comment_id: str, reason: str):
    async with _db_lock:
        async with get_conn() as db:
            await db.execute(
                "INSERT INTO blocked_duplicates (rule_id, user_id, event_id, comment_id, reason, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (rule_id, user_id, event_id, comment_id, reason, now()),
            )
            await db.commit()


async def cancel_pending_tasks_for_comment(comment_id: str):
    """Called when a comment.deleted arrives and the DM for it hasn't been
    sent yet. Covers tasks still sitting in 'pending' AND ones a worker has
    just claimed into 'sending' but not yet fired off - both are still
    cancellable."""
    async with _db_lock:
        async with get_conn() as db:
            await db.execute(
                "UPDATE dm_tasks SET status='cancelled', updated_at=? WHERE comment_id=? AND status IN ('pending','sending')",
                (now(), comment_id),
            )
            await db.commit()


async def mark_task_cancelled(task_id: int, reason: str):
    """Distinct from mark_task_failed: this task never attempted a send at
    all (e.g. its comment was deleted first), so it must not be counted in
    'failed' - 'failed' means we gave up after retries, per the spec. A
    cancelled task simply falls out of every /stats bucket."""
    async with _db_lock:
        async with get_conn() as db:
            await db.execute(
                "UPDATE dm_tasks SET status='cancelled', last_error=?, updated_at=? WHERE id=?",
                (reason, now(), task_id),
            )
            await db.commit()


async def fetch_pending_tasks(limit: int = 20):
    async with get_conn() as db:
        cur = await db.execute(
            """
            SELECT * FROM dm_tasks
            WHERE status = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
            ORDER BY created_at ASC LIMIT ?
            """,
            (now(), limit),
        )
        return await cur.fetchall()


async def fetch_queued_tasks_to_poll(limit: int = 20):
    async with get_conn() as db:
        cur = await db.execute(
            """
            SELECT * FROM dm_tasks
            WHERE status = 'queued' AND (last_checked_at IS NULL OR last_checked_at <= ?)
            ORDER BY last_checked_at ASC LIMIT ?
            """,
            (now() - config.RECONCILE_POLL_INTERVAL_SECONDS, limit),
        )
        return await cur.fetchall()


async def claim_task_for_sending(task_id: int) -> bool:
    """Atomically flips pending -> sending so two worker coroutines can
    never both send for the same task."""
    async with _db_lock:
        async with get_conn() as db:
            cur = await db.execute(
                "UPDATE dm_tasks SET status='sending', updated_at=? WHERE id=? AND status='pending'",
                (now(), task_id),
            )
            await db.commit()
            return cur.rowcount > 0


async def mark_task_queued(task_id: int, dm_id: str, attempts: int):
    async with _db_lock:
        async with get_conn() as db:
            await db.execute(
                "UPDATE dm_tasks SET status='queued', dm_id=?, attempts=?, last_checked_at=?, "
                "poll_count=0, updated_at=? WHERE id=?",
                (dm_id, attempts, now(), now(), task_id),
            )
            await db.commit()


async def mark_task_retry(task_id: int, attempts: int, next_attempt_at: float, error: str):
    async with _db_lock:
        async with get_conn() as db:
            await db.execute(
                "UPDATE dm_tasks SET status='pending', attempts=?, next_attempt_at=?, last_error=?, updated_at=? "
                "WHERE id=?",
                (attempts, next_attempt_at, error, now(), task_id),
            )
            await db.commit()


async def mark_task_failed(task_id: int, attempts: int, error: str):
    async with _db_lock:
        async with get_conn() as db:
            await db.execute(
                "UPDATE dm_tasks SET status='failed', attempts=?, last_error=?, updated_at=? WHERE id=?",
                (attempts, error, now(), task_id),
            )
            await db.commit()


async def mark_task_delivered(task_id: int):
    async with _db_lock:
        async with get_conn() as db:
            await db.execute(
                "UPDATE dm_tasks SET status='delivered', updated_at=? WHERE id=?",
                (now(), task_id),
            )
            await db.commit()


async def bump_poll(task_id: int):
    async with _db_lock:
        async with get_conn() as db:
            await db.execute(
                "UPDATE dm_tasks SET last_checked_at=?, poll_count=poll_count+1, updated_at=? WHERE id=?",
                (now(), now(), task_id),
            )
            await db.commit()


async def requeue_for_resend(task_id: int, attempts: int):
    """A dm_id came back with status='failed' from the mock API itself
    (accepted, then failed later). Send again with a fresh attempt number
    (and therefore a fresh idempotency key)."""
    async with _db_lock:
        async with get_conn() as db:
            await db.execute(
                "UPDATE dm_tasks SET status='pending', attempts=?, next_attempt_at=NULL, dm_id=NULL, updated_at=? "
                "WHERE id=?",
                (attempts, now(), task_id),
            )
            await db.commit()


# ----------------------------------------------------------------- stats --

async def get_stats():
    async with get_conn() as db:
        cur = await db.execute(
            "SELECT status, COUNT(*) as c FROM dm_tasks GROUP BY status"
        )
        rows = await cur.fetchall()
        counts = {r["status"]: r["c"] for r in rows}

        cur = await db.execute("SELECT COUNT(*) as c FROM blocked_duplicates")
        dup_row = await cur.fetchone()

    sent = counts.get("delivered", 0)
    failed = counts.get("failed", 0)
    queued = counts.get("pending", 0) + counts.get("sending", 0) + counts.get("queued", 0)
    duplicates_blocked = dup_row["c"] if dup_row else 0

    return {
        "sent": sent,
        "failed": failed,
        "queued": queued,
        "duplicates_blocked": duplicates_blocked,
    }
