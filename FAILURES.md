# FAILURES.md

Honest list of where this system can still lose a DM, send a duplicate, or
report a wrong number, and the specific conditions under which it happens.

1. **I could not run this against the real mock API before writing this
   file.** My dev sandbox's network egress is restricted to a fixed
   allowlist that doesn't include `pseudogram-api.onrender.com`, so
   everything above was validated with hand-crafted webhook payloads
   against a locally running instance (redelivered `event_id`, a
   comment.deleted arriving before its comment.created, a repeat comment
   from the same user, a forged signature) - not against the actual
   500-events-in-10-seconds simulator. The logic is sound on paper and the
   synthetic tests confirmed the dedup/ordering/signature behavior, but I
   have not seen it survive the real rate limiter, the real 15% post-accept
   failure rate, or real network latency at volume. Treat everything below
   as reasoned-through, not load-tested.

2. **Reprocessing an event after a crash can double-count
   `duplicates_blocked` without double-sending any DM.** Event handling and
   marking that event `processed=1` are two separate commits, not one
   transaction. If the process dies after `dm_tasks` gets its row (or after
   a duplicate gets logged) but before `raw_events.processed` is set, the
   event is reprocessed on restart. `try_create_dm_task`'s `UNIQUE(rule_id,
   user_id)` constraint means this never sends a second DM - but if the
   first pass already logged a blocked duplicate, the replay logs it again,
   inflating `duplicates_blocked` by one per crash-at-that-exact-moment.
   `sent`/`failed`/`queued` are unaffected.

3. **The rate limiter's memory of "how many calls we've made recently" is
   in-process and resets on restart.** If the app restarts mid-run, the
   sliding window forgets every call made in the preceding 60 seconds. The
   *mock API's* rate limiter doesn't forget, so the first burst of sends
   after a restart can draw a run of `429`s until our window and its window
   resync. Nothing is lost (429s get retried with backoff), but `queued`
   will show a temporary, avoidable spike right after any restart.

4. **A DM already accepted or delivered can't be recalled if
   `comment.deleted` arrives afterward, and there's a narrow race even
   before that.** `comment.deleted` cancels a task at either `pending` or
   `sending` (claimed-but-not-yet-fired) status, and cancelled tasks are
   correctly excluded from `sent`/`failed`/`queued`/`duplicates_blocked` -
   an earlier version of this mistakenly marked that race case as `failed`,
   which I caught in review and fixed, since "failed" is specifically
   defined as "gave up after retries" and a cancelled task never attempted
   one. What's still true: there's a gap between the last deleted-check and
   the request actually landing on the mock API where a delete could arrive
   in that gap and we'd send anyway. Once a task is `queued` or `delivered`,
   a later delete is a no-op by design (there's no unsend endpoint), so a
   DM can still go out for a comment that's since been deleted.

5. **This assumes a single process / single SQLite file.** The
   `UNIQUE(rule_id, user_id)` constraint that guarantees no double-DM only
   holds within one SQLite database. If this were ever scaled to multiple
   app instances without pointing them at the same DB (or migrating to a
   DB that supports real concurrent writers), two instances could both
   pass the "task doesn't exist yet" insert at effectively the same time
   against two different SQLite files and both send. Single-instance
   deployment (which is what I built and what the Procfile assumes) avoids
   this entirely, but it's a real ceiling on how this scales.

6. **`MAX_SEND_ATTEMPTS` is shared between "retry because the network/API
   was flaky" and "resend because the mock API told us the first delivery
   actually failed."** Both paths increment the same `attempts` counter. A
   task that burned 4 of its 6 attempts retrying through transient `500`s
   before finally getting accepted has only 2 attempts left if
   reconciliation later discovers that accepted DM failed - meaning a
   message that had a genuinely rocky but still-recoverable delivery path
   can end up permanently marked `failed` sooner than a message that sailed
   through on the first try. I'd split these into separate counters with
   one more week.
