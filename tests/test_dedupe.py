import asyncio

import pytest

from cache_cache import Cache, cached, dedupe, record

from .conftest import tick


class Fx:
    def __init__(self) -> None:
        self.calls = 0
        self.release = asyncio.Event()

    @dedupe
    async def rates(self) -> dict[str, float]:
        self.calls += 1
        await self.release.wait()
        return {"eur": 1.0}


async def test_concurrent_identical_calls_share_one_execution() -> None:
    fx = Fx()
    with record() as rec:
        tasks = [asyncio.create_task(fx.rates()) for _ in range(5)]
        await tick()
        fx.release.set()
        results = await asyncio.gather(*tasks)
    assert fx.calls == 1
    assert all(r == {"eur": 1.0} for r in results)
    assert rec.shared(Fx.rates) == 4
    assert rec.calls(Fx.rates) == 1


async def test_dedupe_stores_nothing(cache: Cache) -> None:
    fx = Fx()
    fx.release.set()
    await fx.rates()
    await fx.rates()
    assert fx.calls == 2
    assert len(cache.backend) == 0  # type: ignore[arg-type]


async def test_cancelling_the_first_caller_does_not_cancel_the_others() -> None:
    fx = Fx()
    first = asyncio.create_task(fx.rates())
    await tick()
    second = asyncio.create_task(fx.rates())
    await tick()
    first.cancel()
    await tick()
    fx.release.set()
    assert await second == {"eur": 1.0}
    assert first.cancelled()
    assert fx.calls == 1


async def test_exceptions_reach_every_caller_and_are_not_kept() -> None:
    calls = 0
    gate = asyncio.Event()

    @dedupe
    async def flaky() -> int:
        nonlocal calls
        calls += 1
        await gate.wait()
        raise ValueError("boom")

    tasks = [asyncio.create_task(flaky()) for _ in range(3)]
    await tick()
    gate.set()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert all(isinstance(r, ValueError) for r in results)
    assert calls == 1
    with pytest.raises(ValueError):
        await flaky()
    assert calls == 2


async def test_forget_starts_a_new_call() -> None:
    fx = Fx()
    first = asyncio.create_task(fx.rates())
    await tick()
    fx.rates.forget()
    second = asyncio.create_task(fx.rates())
    await tick()
    fx.release.set()
    await asyncio.gather(first, second)
    assert fx.calls == 2


async def test_cached_calls_are_deduped_too(cache: Cache) -> None:
    gate = asyncio.Event()
    calls = 0

    @cached
    async def slow(x: int) -> int:
        nonlocal calls
        calls += 1
        await gate.wait()
        return x

    tasks = [asyncio.create_task(slow(1)) for _ in range(10)]
    await tick(10)
    gate.set()
    assert await asyncio.gather(*tasks) == [1] * 10
    assert calls == 1


async def test_joined_callers_from_other_sessions_fill_their_own_session(cache: Cache) -> None:
    gate = asyncio.Event()

    @cached(session_only=True)
    async def slow() -> int:
        await gate.wait()
        return 1

    async def in_session() -> int:
        async with cache.session() as session:
            value = await slow()
            assert len(session) == 1
            return value

    tasks = [asyncio.create_task(in_session()) for _ in range(3)]
    await tick(10)
    gate.set()
    assert await asyncio.gather(*tasks) == [1, 1, 1]
