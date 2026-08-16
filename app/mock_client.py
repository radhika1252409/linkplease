import httpx

from . import config


class MockAPIClient:
    def __init__(self):
        self._client = httpx.AsyncClient(base_url=config.BASE_URL, timeout=10.0)

    async def close(self):
        await self._client.aclose()

    def _headers(self, idempotency_key: str = None):
        headers = {"X-API-Key": config.PSEUDOGRAM_API_KEY}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        return headers

    async def send_dm(self, recipient_user_id: str, message: str, comment_id: str, idempotency_key: str):
        """Returns (status_code, json_body_or_None, retry_after_seconds_or_None)."""
        try:
            resp = await self._client.post(
                "/v1/dm/send",
                json={
                    "recipient_user_id": recipient_user_id,
                    "message": message,
                    "comment_id": comment_id,
                },
                headers=self._headers(idempotency_key),
            )
        except httpx.RequestError as e:
            return None, {"error": "network_error", "detail": str(e)}, None

        retry_after = None
        if resp.status_code == 429:
            try:
                retry_after = float(resp.headers.get("Retry-After", "5"))
            except ValueError:
                retry_after = 5.0

        try:
            body = resp.json()
        except Exception:
            body = None

        return resp.status_code, body, retry_after

    async def get_dm_status(self, dm_id: str):
        """Returns (status_code, json_body_or_None). Reads don't count
        against the rate limit per the spec, so no limiter here."""
        try:
            resp = await self._client.get(f"/v1/dm/{dm_id}")
        except httpx.RequestError as e:
            return None, {"error": "network_error", "detail": str(e)}
        try:
            body = resp.json()
        except Exception:
            body = None
        return resp.status_code, body
