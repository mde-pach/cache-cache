import asyncio

import cache_cache
from cache_cache import Cache, cached, record

from .conftest import FakeClock, tick


class Source:
    def __init__(self) -> None:
        self.value = "v1"
        self.calls = 0
        self.fail = False

    @cached(ttl=10, tags=["src"], stale_while_revalidate=30)
    async def read(self) -> str:
        self.calls += 1
        return self.value

    @cached(ttl=10, tags=["src"], stale_if_error=30)
    async def read_or_fail(self) -> str:
        self.calls += 1
        if self.fail:
            raise ConnectionError("upstream down")
        return self.value


async def test_stale_while_revalidate_serves_old_value_and_refreshes(
    cache: Cache, clock: FakeClock
) -> None:
    src = Source()
    assert await src.read() == "v1"
    src.value = "v2"
    clock.advance(15)
    with record() as rec:
        assert await src.read() == "v1"
        await tick(10)
    assert rec.stale(Source.read) == 1
    assert src.calls == 2
    assert await src.read() == "v2"
    assert src.calls == 2


async def test_stale_window_ends(cache: Cache, clock: FakeClock) -> None:
    src = Source()
    await src.read()
    src.value = "v2"
    clock.advance(10 + 31)
    assert await src.read() == "v2"


async def test_stale_after_invalidation(cache: Cache, clock: FakeClock) -> None:
    src = Source()
    await src.read()
    src.value = "v2"
    # Invalidated by someone else (no session of ours recorded it):
    await cache.backend.bump_tags([cache.tag_name("src")])
    assert await src.read() == "v1"
    await tick(10)
    assert await src.read() == "v2"


async def test_no_stale_value_after_our_own_invalidation(cache: Cache, clock: FakeClock) -> None:
    src = Source()
    await src.read()
    src.value = "v2"
    async with cache.session():
        await cache_cache.invalidate("src")
        assert await src.read() == "v2"


async def test_background_refresh_runs_once(cache: Cache, clock: FakeClock) -> None:
    src = Source()
    await src.read()
    clock.advance(15)
    await asyncio.gather(*(src.read() for _ in range(5)))
    await tick(10)
    assert src.calls == 2


async def test_stale_if_error(cache: Cache, clock: FakeClock) -> None:
    src = Source()
    assert await src.read_or_fail() == "v1"
    src.fail = True
    clock.advance(15)
    with record() as rec:
        assert await src.read_or_fail() == "v1"
    assert [e.kind for e in rec.events].count("stale_if_error") == 1
    clock.advance(30)
    try:
        await src.read_or_fail()
    except ConnectionError:
        pass
    else:  # pragma: no cover
        raise AssertionError("stale window should be over")


async def test_shutdown_waits_for_background_refresh(clock: FakeClock) -> None:
    started = asyncio.Event()
    finished = []

    @cached(ttl=1, stale_while_revalidate=60)
    async def slow() -> int:
        if finished or started.is_set():
            finished.append(1)
            await asyncio.sleep(0.01)
            return 2
        started.set()
        return 1

    async with Cache():
        await slow()
        clock.advance(2)
        assert await slow() == 1
    assert finished == [1]
