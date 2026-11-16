import asyncio
import logging
from typing import Any

import pytest

from cache_cache import (
    Cache,
    CachedCall,
    DontJoinOldCalls,
    LoggingObserver,
    Next,
    StatsObserver,
    cached,
)

from .conftest import FakeClock, tick


async def test_stats_observer() -> None:
    stats = StatsObserver()

    @cached
    async def f(x: int) -> int:
        return x

    async with Cache(observers=[stats]) as cache:
        await f(1)
        await f(1)
        async with cache.session():
            await f(2)
            await f(2)
    s = stats.stats[f.name]
    assert (s.calls, s.misses, s.hits, s.session_hits, s.stores) == (2, 2, 2, 1, 2)
    assert s.hit_ratio == 0.5


async def test_logging_observer(caplog: pytest.LogCaptureFixture) -> None:
    @cached
    async def f() -> int:
        return 1

    caplog.set_level(logging.DEBUG, "cache_cache")
    async with Cache(observers=[LoggingObserver()]):
        await f()
    assert any("cache miss" in r.message for r in caplog.records)


async def test_middleware_sees_the_lookup() -> None:
    seen: list[str] = []

    class Spy:
        async def __call__(self, call: CachedCall, call_next: Next) -> Any:
            seen.append(call.lookup.state)
            return await call_next(call)

    @cached
    async def f() -> int:
        return 1

    async with Cache(middleware=[Spy()]):
        await f()
        await f()
    assert seen == ["miss", "hit"]


async def test_middleware_can_short_circuit() -> None:
    class Override:
        async def __call__(self, call: CachedCall, call_next: Next) -> Any:
            return 42

    @cached
    async def f() -> int:
        return 1

    async with Cache(middleware=[Override()]):
        assert await f() == 42


async def test_dont_join_old_calls(clock: FakeClock) -> None:
    gate = asyncio.Event()
    calls = 0

    @cached
    async def slow() -> int:
        nonlocal calls
        calls += 1
        await gate.wait()
        return calls

    async with Cache(middleware=[DontJoinOldCalls(max_age=2)]):
        first = asyncio.create_task(slow())
        await tick()
        clock.advance(1)
        joined = asyncio.create_task(slow())
        await tick()
        clock.advance(5)
        fresh = asyncio.create_task(slow())
        await tick()
        gate.set()
        results = await asyncio.gather(first, joined, fresh)
    assert calls == 2
    assert results[0] == results[1]


async def test_failing_observer_does_not_break_calls(caplog: pytest.LogCaptureFixture) -> None:
    class Broken:
        def on_event(self, event: object) -> None:
            raise RuntimeError

    @cached
    async def f() -> int:
        return 1

    async with Cache(observers=[Broken()]):
        assert await f() == 1
