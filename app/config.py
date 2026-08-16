import os

# The API key you get from POST /v1/keygen on the mock API.
# Used both as X-API-Key on outgoing calls to the mock API, and as the
# HMAC secret for verifying inbound webhook signatures.
PSEUDOGRAM_API_KEY = os.environ.get("PSEUDOGRAM_API_KEY", "")

BASE_URL = os.environ.get("PSEUDOGRAM_BASE_URL", "https://pseudogram-api.onrender.com")

DB_PATH = os.environ.get("LINKPLEASE_DB_PATH", "linkplease.db")

# Mock API rate limit: 10 requests / rolling 60s, per the assignment spec.
RATE_LIMIT_MAX_CALLS = int(os.environ.get("RATE_LIMIT_MAX_CALLS", "10"))
RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("RATE_LIMIT_WINDOW_SECONDS", "60"))

# Retry / backoff tuning for POST /v1/dm/send
MAX_SEND_ATTEMPTS = int(os.environ.get("MAX_SEND_ATTEMPTS", "6"))
BASE_BACKOFF_SECONDS = float(os.environ.get("BASE_BACKOFF_SECONDS", "2"))
MAX_BACKOFF_SECONDS = float(os.environ.get("MAX_BACKOFF_SECONDS", "120"))

# How often to poll GET /v1/dm/{dm_id} for DMs sitting in "queued" (Part C
# reconciliation). Reads don't count against the rate limit so this can be
# more aggressive than the send retry backoff.
RECONCILE_POLL_INTERVAL_SECONDS = float(os.environ.get("RECONCILE_POLL_INTERVAL_SECONDS", "5"))
RECONCILE_MAX_POLLS = int(os.environ.get("RECONCILE_MAX_POLLS", "30"))  # ~2.5 min before we give up

# Main worker loop tick
WORKER_TICK_SECONDS = float(os.environ.get("WORKER_TICK_SECONDS", "0.5"))
SEND_CONCURRENCY = int(os.environ.get("SEND_CONCURRENCY", "4"))

# If set to "0", skips signature verification (only useful for local testing
# before you have a key). In any real deployment this should be verifying.
VERIFY_SIGNATURES = os.environ.get("VERIFY_SIGNATURES", "1") != "0"
