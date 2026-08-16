import hashlib, hmac, json, time, urllib.request, urllib.error

BASE = "http://127.0.0.1:8812"
KEY = "testkey123"

def sign(body): return "sha256=" + hmac.new(KEY.encode(), body, hashlib.sha256).hexdigest()

def post_webhook(payload):
    body = json.dumps(payload).encode()
    req = urllib.request.Request(f"{BASE}/webhook", data=body,
        headers={"Content-Type": "application/json", "X-PseudoGram-Signature": sign(body)}, method="POST")
    with urllib.request.urlopen(req) as r:
        return r.status, r.read()

def ev(event_id, event_type, comment_id, user_id="u", username="u", text="PRICE"):
    d = {"comment_id": comment_id, "post_id": "p", "text": text, "created_at": "x",
         "from": {"user_id": user_id, "username": username}} if event_type == "comment.created" else {"comment_id": comment_id}
    return {"event_id": event_id, "event_type": event_type, "sent_at": "x", "data": d}

# comment created, then immediately deleted before the worker gets a chance to send
# (worker tick is 0.5s, so posting create+delete back-to-back should land the
# delete before the pending task is even dispatched)
print(post_webhook(ev("e100", "comment.created", "c100", user_id="usrX", text="PRICE")))
print(post_webhook(ev("e101", "comment.deleted", "c100")))

time.sleep(3)
with urllib.request.urlopen(f"{BASE}/stats") as r:
    print("STATS after created+deleted race:", r.read().decode())
