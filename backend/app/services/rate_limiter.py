import asyncio
import time


class RateLimiter:
    def __init__(self, max_concurrent: int = 5, min_interval: float = 0.5):
        self.sem = asyncio.Semaphore(max_concurrent)
        self.min_interval = float(min_interval)
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def __aenter__(self):
        await self.sem.acquire()
        await self._throttle()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.sem.release()

    async def _throttle(self):
        async with self._lock:
            now = time.monotonic()
            wait = self.min_interval - (now - self._last)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last = time.monotonic()
