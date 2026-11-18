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


def test_sync_round_trip_on_real_redis() -> None:
    from cache_cache import SyncCache, invalidate_sync
    from cache_cache.backends.redis import SyncRedisBackend

    calls = 0

    @cached(ttl=30, tags=["live-sync:{x}"])
    def f(x: int) -> int:
        nonlocal calls
        calls += 1
        return x * 2

    prefix = f"cc-test-{uuid.uuid4().hex[:8]}:"
    assert URL is not None
    with SyncCache(backend=SyncRedisBackend(url=URL, prefix=prefix)):
        assert [f(i) for i in range(5)] == [0, 2, 4, 6, 8]
        assert f(3) == 6
        invalidate_sync("live-sync")
        f(3)
    assert calls == 6


async def test_retired_deployment_is_deleted_on_real_redis() -> None:
    from redis.asyncio import Redis

    @cached(ttl=30)
    async def g(x: int) -> int:
        return x

    prefix = f"cc-test-{uuid.uuid4().hex[:8]}:"
    fast = {"heartbeat_interval": 0.05, "retire_after": 0.1}
    assert URL is not None
    async with Cache(backend=RedisBackend(url=URL, prefix=prefix), deployment="old", **fast):
        await asyncio.gather(*(g(i) for i in range(1200)))  # more than one SCAN/UNLINK batch
    async with Cache(backend=RedisBackend(url=URL, prefix=prefix), deployment="new", **fast):
        client = Redis.from_url(URL)
        try:
            for _ in range(100):
                if not await client.keys(f"{prefix}e:*"):
                    break
                await asyncio.sleep(0.05)
            assert await client.keys(f"{prefix}e:*") == []
        finally:
            await client.delete(*(await client.keys(f"{prefix}*")) or [f"{prefix}none"])
            await client.aclose()
