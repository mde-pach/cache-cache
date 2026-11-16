"""Against a real Redis server, when CACHE_CACHE_REDIS_URL is set (CI runs one)."""

import asyncio
import os
import uuid

import pytest

import cache_cache
from cache_cache import Cache, cached
from cache_cache.backends.redis import RedisBackend

URL = os.environ.get("CACHE_CACHE_REDIS_URL")
pytestmark = pytest.mark.skipif(not URL, reason="CACHE_CACHE_REDIS_URL not set")


async def test_round_trip_and_invalidation_on_real_redis() -> None:
    calls = 0

    @cached(ttl=30, tags=["live:{x}"])
    async def f(x: int) -> int:
        nonlocal calls
        calls += 1
        return x * 2

    prefix = f"cc-test-{uuid.uuid4().hex[:8]}:"
    assert URL is not None
    async with Cache(backend=RedisBackend(url=URL, prefix=prefix)):
        assert await asyncio.gather(*(f(i) for i in range(10))) == [i * 2 for i in range(10)]
        assert await f(3) == 6
        await cache_cache.invalidate("live:3")
        await f(3)
    assert calls == 11
