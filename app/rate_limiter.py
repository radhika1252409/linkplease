import asyncio
import time
from collections import deque


class SlidingWindowRateLimiter:
    """Enforces max_calls per rolling window_seconds. Shared by every
    coroutine that calls POST /v1/dm/send so we never breach the mock
    API's 10-requests-per-60s limit, even with several senders running
    concurrently.

    Deliberately conservative: acquire() blocks the caller until a slot is
    free rather than firing and handling 429s reactively. We still handle
    429s defensively (a 429 can happen if our clock and the server's drift,
    or if something else is also hitting the same key), but this keeps us
    from spending the whole budget on responses we already expect to be
    rate-limited.
    """

    def __init__(self, max_calls: int, window_seconds: float):
        self.max_calls = max_calls
        self.window_seconds = window_seconds
        self._calls = deque()
        self._lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self._lock:
                now = time.monotonic()
                while self._calls and now - self._calls[0] >= self.window_seconds:
                    self._calls.popleft()
                if len(self._calls) < self.max_calls:
                    self._calls.append(now)
                    return
                sleep_for = self.window_seconds - (now - self._calls[0]) + 0.05
            await asyncio.sleep(max(sleep_for, 0.05))

    async def penalize(self, retry_after: float):
        """Called after a 429 to push the window out further, in case our
        local clock is behind the server's."""
        async with self._lock:
            now = time.monotonic()
            # Pretend we made `max_calls` worth of calls right up until
            # retry_after from now, so acquire() won't let anything through
            # until then.
            for _ in range(self.max_calls):
                self._calls.append(now + retry_after)
