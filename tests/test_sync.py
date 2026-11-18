import threading
import time
from collections.abc import Iterator
from typing import Any

import fakeredis
import pytest

import cache_cache
from cache_cache import (
    Cache,
    ConfigurationError,
    NoActiveSession,
    StatsObserver,
    SyncCache,
    SyncCachedCall,
    SyncDontJoinOldCalls,
    SyncMemoryBackend,
    SyncNext,
    cached,
    dedupe,
    invalidate_sync,
    invalidates,
    record,
)
from cache_cache.backends.redis import RedisBackend, SyncRedisBackend

from .conftest import FakeClock, Product


@pytest.fixture
def sync_backend() -> SyncMemoryBackend:
    return SyncMemoryBackend()


@pytest.fixture
def sync_cache(sync_backend: SyncMemoryBackend) -> Iterator[SyncCache]:
    with SyncCache(backend=sync_backend, namespace="test") as cache:
        yield cache


class Repo:
    def __init__(self) -> None:
        self.calls: list[int] = []
        self.db = {i: Product(i, f"p{i}") for i in range(1, 5)}

    @cached(tags=["product:{pid}"])
    def get(self, pid: int) -> Product:
        self.calls.append(pid)
        return self.db[pid]

    @cached(ttl=600, tags=["product:list:{category}"])
    def by_category(self, category: str) -> list[Product]:
        self.calls.append(0)
        return [p for p in self.db.values() if p.category == category]

    @invalidates("product:{pid}", "product:list")
    def rename(self, pid: int, name: str) -> Product:
        self.db[pid] = Product(pid, name)
        return self.db[pid]


def test_hit_after_miss(sync_cache: SyncCache) -> None:
    repo = Repo()
    with record() as rec:
        assert repo.get(1) == Product(1, "p1")
        assert repo.get(1) == Product(1, "p1")
    assert repo.calls == [1]
    assert rec.calls(Repo.get) == 1
    assert rec.hits(repo.get) == 1


def test_module_function_and_ttl(sync_cache: SyncCache, clock: FakeClock) -> None:
    calls = []

    @cached(ttl=10)
    def square(x: int) -> int:
        calls.append(x)
        return x * x

    assert square(3) == 9
    clock.advance(5)
    assert square(3) == 9
    clock.advance(6)
    assert square(3) == 9
    assert calls == [3, 3]


def test_without_cache_calls_run(sync_backend: SyncMemoryBackend) -> None:
    repo = Repo()
    repo.get(1)
    repo.get(1)
    assert repo.calls == [1, 1]


def test_async_cache_does_not_serve_sync_functions() -> None:
    import asyncio

    repo = Repo()

    async def main() -> None:
        async with Cache():
            repo.get(1)
            repo.get(1)

    asyncio.run(main())
    assert repo.calls == [1, 1]


def test_session_layer(sync_cache: SyncCache, sync_backend: SyncMemoryBackend) -> None:
    repo = Repo()
    with sync_cache.session() as session:
        repo.get(1)
        sync_backend.delete(list(sync_backend._entries))
        repo.get(1)
        assert len(session) == 1
    assert repo.calls == [1]


def test_strict_mode(sync_cache: SyncCache) -> None:
    cache_cache.configure(strict=True)
    with pytest.raises(NoActiveSession):
        Repo().get(1)
    with sync_cache.session():
        Repo().get(1)


def test_hierarchical_invalidation(sync_cache: SyncCache) -> None:
    repo = Repo()
    repo.get(1)
    repo.get(2)
    repo.by_category("shoes")
    repo.calls.clear()
    repo.rename(1, "new")
    assert repo.get(1).name == "new"
    repo.get(2)
    repo.by_category("shoes")
    assert repo.calls == [1, 0]


def test_invalidate_sync_function(sync_cache: SyncCache) -> None:
    repo = Repo()
    repo.get(1)
    invalidate_sync("product")
    repo.get(1)
    assert repo.calls == [1, 1]
    with pytest.raises(ValueError):
        sync_cache.invalidate("a::b")


def test_transaction(sync_cache: SyncCache) -> None:
    repo = Repo()
    repo.get(1)
    with record() as rec, sync_cache.transaction() as transaction:
        repo.rename(1, "new")
        assert transaction.pending == ["product:1", "product:list"]
        assert rec.invalidated() == []
        assert repo.get(1).name == "new"
    assert rec.invalidated() == ["product:1", "product:list"]
    assert repo.get(1).name == "new"


def test_race_between_store_and_invalidation(sync_cache: SyncCache) -> None:
    started, release = threading.Event(), threading.Event()
    db = {"value": "old"}
    calls = 0

    @cached(ttl=None, tags=["thing"])
    def read() -> str:
        nonlocal calls
        calls += 1
        value = db["value"]
        started.set()
        release.wait(5)
        return value

    results: list[str] = []
    reader = threading.Thread(target=lambda: results.append(read()))
    reader.start()
    started.wait(5)
    db["value"] = "new"
    sync_cache.invalidate("thing")
    release.set()
    reader.join(5)
    assert results == ["old"]
    assert read() == "new"
    assert calls == 2


def test_dedupe_across_threads() -> None:
    gate = threading.Event()
    calls = 0
    lock = threading.Lock()

    @dedupe
    def rates() -> dict[str, float]:
        nonlocal calls
        with lock:
            calls += 1
        gate.wait(5)
        return {"eur": 1.0}

    results: list[dict[str, float]] = []
    threads = [threading.Thread(target=lambda: results.append(rates())) for _ in range(8)]
    with record():
        for t in threads:
            t.start()
        time.sleep(0.1)
        gate.set()
        for t in threads:
            t.join(5)
    assert calls == 1
    assert results == [{"eur": 1.0}] * 8


def test_dedupe_shares_exceptions_and_forgets_them() -> None:
    gate = threading.Event()
    calls = 0

    @dedupe
    def flaky() -> int:
        nonlocal calls
        calls += 1
        gate.wait(5)
        raise ValueError("boom")

    errors: list[BaseException] = []

    def run() -> None:
        try:
            flaky()
        except ValueError as exc:
            errors.append(exc)

    threads = [threading.Thread(target=run) for _ in range(4)]
    for t in threads:
        t.start()
    time.sleep(0.1)
    gate.set()
    for t in threads:
        t.join(5)
    assert len(errors) == 4
    assert calls == 1
    with pytest.raises(ValueError):
        flaky()
    assert calls == 2


def test_stale_while_revalidate_refreshes_in_a_thread(
    sync_cache: SyncCache, clock: FakeClock
) -> None:
    refreshed = threading.Event()
    state = {"value": "v1", "calls": 0}

    @cached(ttl=10, stale_while_revalidate=60)
    def read() -> str:
        state["calls"] += 1  # type: ignore[operator]
        if state["calls"] == 2:
            refreshed.set()
        return str(state["value"])

    assert read() == "v1"
    state["value"] = "v2"
    clock.advance(15)
    assert read() == "v1"
    assert refreshed.wait(5)
    deadline = time.monotonic() + 5
    while read() != "v2" and time.monotonic() < deadline:
        time.sleep(0.01)
    assert read() == "v2"
    assert state["calls"] == 2


def test_stale_if_error(sync_cache: SyncCache, clock: FakeClock) -> None:
    fail = False

    @cached(ttl=10, stale_if_error=30)
    def read() -> str:
        if fail:
            raise ConnectionError
        return "v1"

    read()
    fail = True
    clock.advance(15)
    assert read() == "v1"
    clock.advance(30)
    with pytest.raises(ConnectionError):
        read()


def test_controls(sync_cache: SyncCache) -> None:
    repo = Repo()
    assert repo.get.peek(1) is None
    repo.get(1)
    assert repo.get.peek(1) == Product(1, "p1")
    repo.get.refresh(1)
    assert repo.calls == [1, 1]
    repo.get.invalidate(1)
    assert repo.get.peek(1) is None
    repo.get.prime(Product(9, "primed"), 9)
    assert repo.get(9).name == "primed"
    repo.get.forget(9)


def test_conditions_and_key_function(sync_cache: SyncCache) -> None:
    calls = []

    @cached
    def search(text: str, trace: str) -> list[str]:
        calls.append(text)
        return [] if text == "none" else [text]

    @search.key
    def _key(text: str, trace: str) -> str:
        return text.lower()

    @search.condition
    def _cacheable(text: str, trace: str) -> bool:
        return text != "live"

    @search.result_condition
    def _keep(result: list[str]) -> bool:
        return bool(result)

    for text in ["A", "a", "live", "live", "none", "none"]:
        search(text, trace="x")
    assert calls == ["A", "live", "live", "none", "none"]


def test_batching_is_rejected_with_a_clear_message() -> None:
    @cached
    def get(pid: int) -> int:
        return pid

    with pytest.raises(ConfigurationError, match="batching needs async functions"):
        get.batched(on="pid")


def test_middleware_and_observers(clock: FakeClock) -> None:
    seen: list[str] = []

    class Spy:
        def __call__(self, call: SyncCachedCall, call_next: SyncNext) -> Any:
            seen.append(call.lookup.state)
            return call_next(call)

    stats = StatsObserver()

    @cached
    def f() -> int:
        return 1

    with SyncCache(middleware=[Spy(), SyncDontJoinOldCalls()], observers=[stats]):
        f()
        f()
    assert seen == ["miss", "hit"]
    assert stats.stats[f.name].hits == 1


def test_dont_join_old_calls_sync(clock: FakeClock) -> None:
    gate = threading.Event()
    calls = 0

    @cached
    def slow() -> int:
        nonlocal calls
        calls += 1
        gate.wait(5)
        return calls

    with SyncCache(middleware=[SyncDontJoinOldCalls(max_age=2)]):
        first = threading.Thread(target=slow)
        first.start()
        time.sleep(0.05)
        clock.advance(5)
        second = threading.Thread(target=slow)
        second.start()
        time.sleep(0.05)
        gate.set()
        first.join(5)
        second.join(5)
    assert calls == 2


def test_clear_and_deployments(sync_backend: SyncMemoryBackend) -> None:
    repo = Repo()
    with SyncCache(backend=sync_backend) as cache:
        repo.get(1)
        cache.clear()
        repo.get(1)
    with SyncCache(backend=sync_backend):
        repo.get(1)  # same deployment: shared
    with SyncCache(backend=sync_backend, deployment="next"):
        repo.get(1)  # new deployment: not shared
    assert repo.calls == [1, 1, 1]


def test_sync_retired_deployment_is_collected(clock: FakeClock) -> None:
    backend = SyncMemoryBackend()
    repo = Repo()
    with SyncCache(backend=backend, deployment="old"):
        repo.get(1)
    clock.advance(601)
    with SyncCache(backend=backend, deployment="new") as cache:
        assert len(backend) == 0  # collected when "new" opened
        assert cache.collect_retired() == 0


def test_sync_redis_backend() -> None:
    server = fakeredis.FakeServer()
    repo = Repo()
    with SyncCache(backend=SyncRedisBackend(fakeredis.FakeRedis(server=server))):
        repo.get(1)
    with SyncCache(backend=SyncRedisBackend(fakeredis.FakeRedis(server=server))):
        repo.get(1)
        invalidate_sync("product:1")
        repo.get(1)
    assert repo.calls == [1, 1]


async def test_sync_invalidation_reaches_async_entries() -> None:
    """A Django admin (sync) and an API (async) share Redis and tags."""
    server = fakeredis.FakeServer()
    calls = 0

    @cached(tags=["product:{pid}"])
    async def get_async(pid: int) -> int:
        nonlocal calls
        calls += 1
        return pid

    async with Cache(backend=RedisBackend(fakeredis.FakeAsyncRedis(server=server))):
        await get_async(1)
        with SyncCache(backend=SyncRedisBackend(fakeredis.FakeRedis(server=server))):
            invalidate_sync("product")
        await get_async(1)
    assert calls == 2


def test_open_validates_only_sync_functions() -> None:
    class Opaque:
        pass

    @cached
    async def broken_async(x: Opaque) -> int:
        return 1

    with SyncCache():
        pass
    with pytest.raises(ConfigurationError, match="no key encoding"):
        import asyncio

        asyncio.run(Cache().open())
