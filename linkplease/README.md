# LinkPlease

A small clone of the "comment PRICE, get a DM" automation, built against the
PseudoGram mock API. Python + FastAPI, SQLite for persistence, no external
services required.

## How it works

Three moving pieces:

1. **`POST /webhook`** verifies the HMAC signature, writes the raw event to
   SQLite, and returns `200` immediately. It does no other work inline -
   this is what keeps it comfortably under the 5s deadline no matter how
   backed up the system is.
2. **A background worker loop** (started at app startup, runs forever)
   picks up unprocessed events, matches them against rules, and creates
   `dm_tasks`. The uniqueness constraint `UNIQUE(rule_id, user_id)` on the
   `dm_tasks` table - enforced by SQLite itself, not by an "if not exists"
   check in Python - is what guarantees a user is never DMed twice for the
   same rule, even if the same event is redelivered or two workers try to
   process it at the same instant.
3. **The same worker loop** sends pending tasks through `POST
   /v1/dm/send` (rate-limited to 10/60s via a shared sliding-window
   limiter, retried with exponential backoff on `500`s, respecting
   `Retry-After` on `429`s, giving up immediately on `400`s), and separately
   polls `GET /v1/dm/{dm_id}` for anything sitting in `queued` until it
   reaches a terminal state. If polling turns up `failed`, it resends with
   a new `Idempotency-Key` rather than assuming the first attempt is final.

Everything the worker needs to resume after a crash or restart is on disk in
SQLite (WAL mode) - there is no in-memory queue or scheduled-retry timer
that a restart would wipe out. See `FAILURES.md` for where this still
falls short.

### Idempotency key strategy

Each send attempt uses `Idempotency-Key = f"{rule_id}:{user_id}:{attempts}"`.
This does two things at once:
- If our process crashes right after sending but before recording the
  response, the next attempt reuses the *same* key (attempts didn't
  increment) and the mock API returns the original `dm_id` instead of
  sending a second DM.
- When we deliberately want to resend (the mock API reported the first
  attempt as `failed`), we increment `attempts` first, which produces a
  new key - so that resend isn't swallowed as a duplicate of the failed one.

### Out-of-order comment.deleted

Deletes are handled as tombstones, independent of arrival order:
- `comment.deleted` arriving first: the comment row is created with
  `deleted=1`. When `comment.created` arrives later, it upserts content but
  never clears that flag, and rule-matching checks the flag before creating
  any `dm_task`.
- `comment.deleted` arriving after a task exists but hasn't sent yet: the
  task is cancelled.
- `comment.deleted` arriving after the DM has already gone out: it's a
  no-op (there's no unsend). This is intentional - see FAILURES.md.

## Running locally

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

export PSEUDOGRAM_API_KEY=your_key_here
uvicorn app.main:app --reload --port 8000
```

`app.db` (SQLite) is created next to the app on first run. Delete
`linkplease.db*` to reset all state.

### Getting a key

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/apply \
  -H "Content-Type: application/json" \
  -d '{"name":"...", "email":"...", "phone":"...", "linkedin_url":"..."}'

curl -X POST https://pseudogram-api.onrender.com/v1/keygen \
  -H "Content-Type: application/json" \
  -d '{"email":"..."}'
```

### Testing against the mock API's simulator

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/simulate/start \
  -H "X-API-Key: $PSEUDOGRAM_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"webhook_url": "https://your-deployed-url/webhook", "count": 500, "duration_seconds": 10}'
```

Then compare `GET /stats` on this app against `GET
/v1/simulate/{run_id}/truth` on the mock API.

There's also `scripts/smoke_test.py`, a local-only script (not part of the
graded surface) that fires hand-crafted webhook events - a forged
signature, a redelivered `event_id`, an out-of-order delete, a duplicate
comment from the same user - at a locally running instance, to sanity-check
the dedup logic without spending simulator quota.

## Deploying

Any host that runs a long-lived Python process works (this needs a
persistent background loop, so it isn't a good fit for serverless/
request-scoped platforms unless you adapt the worker to run separately).
Render, Railway, Fly.io, or a small VPS are all straightforward.

Example `Procfile` for Render/Railway-style platforms is included. Set
`PSEUDOGRAM_API_KEY` as an environment variable in the host's dashboard -
don't commit it.

**Note on SQLite persistence:** on platforms with an ephemeral filesystem
(e.g. Render's free tier without a persistent disk), the SQLite file - and
therefore all rules and in-flight DM state - is lost on redeploy or
restart. Attach a persistent disk (or point `LINKPLEASE_DB_PATH` at a
mounted volume) before running the graded simulation.

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `PSEUDOGRAM_API_KEY` | (required) | Sent as `X-API-Key`, also the HMAC secret for verifying webhooks |
| `PSEUDOGRAM_BASE_URL` | `https://pseudogram-api.onrender.com` | Mock API base |
| `LINKPLEASE_DB_PATH` | `linkplease.db` | SQLite file path |
| `VERIFY_SIGNATURES` | `1` | Set to `0` to skip signature checks (local testing only) |
| `RATE_LIMIT_MAX_CALLS` / `RATE_LIMIT_WINDOW_SECONDS` | `10` / `60` | Send rate limit |
| `MAX_SEND_ATTEMPTS` | `6` | Retries before a task is marked `failed` |
| `SEND_CONCURRENCY` | `4` | Concurrent senders sharing the rate limiter |

## What's not built

Part C's "500 comments in 10 seconds, rate limit never breached" is
handled by design (webhook writes are cheap and unbounded; only the actual
send is bottlenecked by the shared limiter), but note that at 10 sends/60s,
clearing a backlog of a few hundred unique matches will take much longer
than 10 seconds - that's expected and correct given the rate limit, not a
bug. `queued` in `/stats` will reflect the backlog honestly.
