"""
Local smoke test - not part of the deliverable, just used to sanity check
dedup/ordering logic against a running local instance before deploying.
Run: python scripts/smoke_test.py
"""
import hashlib
import hmac
import json
import time
import urllib.request

BASE = "http://127.0.0.1:8812"
KEY = "testkey123"


def sign(body: bytes) -> str:
    return "sha256=" + hmac.new(KEY.encode(), body, hashlib.sha256).hexdigest()


def post_webhook(payload: dict, bad_sig=False):
    body = json.dumps(payload).encode()
    headers = {
        "Content-Type": "application/json",
        "X-PseudoGram-Signature": "sha256=deadbeef" if bad_sig else sign(body),
    }
    req = urllib.request.Request(f"{BASE}/webhook", data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def event(event_id, event_type, comment_id, user_id="usr_1", username="arjun", text="PRICE please"):
    return {
        "event_id": event_id,
        "event_type": event_type,
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": comment_id,
            "post_id": "post_1",
            "text": text,
            "created_at": "2026-08-10T09:14:21.900Z",
            "from": {"user_id": user_id, "username": username},
        } if event_type == "comment.created" else {"comment_id": comment_id},
    }


print("1) forged signature should be rejected:")
print(post_webhook(event("evt_bad", "comment.created", "cmt_bad"), bad_sig=True))

print("\n2) normal comment matching PRICE:")
print(post_webhook(event("evt_1", "comment.created", "cmt_1", user_id="usr_A")))

print("\n3) redelivery of the SAME event_id (should be a no-op, not double-counted):")
print(post_webhook(event("evt_1", "comment.created", "cmt_1", user_id="usr_A")))

print("\n4) same user comments PRICE again on a different comment -> should be blocked as duplicate:")
print(post_webhook(event("evt_2", "comment.created", "cmt_2", user_id="usr_A")))

print("\n5) different user, same keyword -> should get their own DM task:")
print(post_webhook(event("evt_3", "comment.created", "cmt_3", user_id="usr_B")))

print("\n6) case-insensitive + substring match ('price' lowercase, embedded in text):")
print(post_webhook(event("evt_4", "comment.created", "cmt_4", user_id="usr_C", text="omg price??")))

print("\n7) comment.deleted arriving BEFORE its comment.created (out of order):")
print(post_webhook(event("evt_5del", "comment.deleted", "cmt_5")))
print(post_webhook(event("evt_5", "comment.created", "cmt_5", user_id="usr_D")))

time.sleep(2)

import urllib.request
with urllib.request.urlopen(f"{BASE}/stats") as r:
    print("\nSTATS:", r.read().decode())
