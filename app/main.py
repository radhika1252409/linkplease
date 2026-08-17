import hashlib
import hmac
import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel

from . import config, db
from .worker import Worker

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("linkplease")

worker = Worker()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    import asyncio
    task = asyncio.create_task(worker.run())
    log.info("LinkPlease worker started")
    yield
    await worker.stop()
    task.cancel()


app = FastAPI(title="LinkPlease", lifespan=lifespan)


class RuleIn(BaseModel):
    keyword: str
    dm_message: str


def verify_signature(raw_body: bytes, signature_header: str) -> bool:
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    provided = signature_header.split("=", 1)[1]
    expected = hmac.new(config.PSEUDOGRAM_API_KEY.encode(), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(provided, expected)


@app.post("/webhook")
async def webhook(request: Request):
    raw_body = await request.body()

    if config.VERIFY_SIGNATURES and config.PSEUDOGRAM_API_KEY:
        sig = request.headers.get("X-PseudoGram-Signature", "")
        if not verify_signature(raw_body, sig):
            key = config.PSEUDOGRAM_API_KEY
            candidates = {"full_key": key}
            if "." in key:
                prefix, _, suffix = key.partition(".")
                candidates["prefix_before_dot"] = prefix
                candidates["suffix_after_dot"] = suffix
            try:
                import base64 as _b64
                candidates["base64_decoded_full"] = _b64.b64decode(key + "==").decode(errors="replace")
            except Exception:
                pass

            provided = sig.split("=", 1)[1] if sig.startswith("sha256=") else sig
            match_found = None
            computed_map = {}
            for name, candidate_secret in candidates.items():
                computed = hmac.new(candidate_secret.encode(), raw_body, hashlib.sha256).hexdigest()
                computed_map[name] = computed
                if hmac.compare_digest(computed, provided):
                    match_found = name

            log.warning(
                "SIGNATURE MISMATCH | received=%s | match_found=%s | computed=%s | headers=%s",
                provided, match_found, computed_map, dict(request.headers),
            )
            raise HTTPException(status_code=401, detail="invalid signature")

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="invalid json")

    event_id = payload.get("event_id")
    event_type = payload.get("event_type")
    data = payload.get("data", {}) or {}
    comment_id = data.get("comment_id")

    if not event_id or not event_type:
        raise HTTPException(status_code=400, detail="missing event_id or event_type")

    # Just persist and return. All real work happens in the background
    # worker loop so we never risk missing the 5s deadline, and so a crash
    # right after this point doesn't lose the event - it's already on disk.
    await db.insert_raw_event(event_id, event_type, comment_id, payload)

    return {"status": "ok"}


@app.post("/rules", status_code=201)
async def create_rule(rule: RuleIn):
    if not rule.keyword.strip() or not rule.dm_message.strip():
        raise HTTPException(status_code=400, detail="keyword and dm_message are required")
    rule_id = await db.create_rule(rule.keyword, rule.dm_message)
    return {"rule_id": rule_id, "keyword": rule.keyword, "dm_message": rule.dm_message}


@app.get("/stats")
async def stats():
    return await db.get_stats()


@app.get("/health")
async def health():
    return {"status": "ok"}
