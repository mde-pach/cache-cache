import asyncio
from decimal import Decimal

import pytest

import cache_cache
from cache_cache import Cache, ConfigurationError, MemoryBackend, NoActiveSession, cached, record

from .conftest import FakeClock, Product


class DbSession:
    """Stands in for a database session: not encodable, must be ignored."""


class Repo:
    def __init__(self) -> None:
        self.calls = 0

    @cached(tags=["product:{pid}"])
    async def get(self, pid: int) -> Product:
        self.calls += 1
        return Product(pid, f"p{pid}")


async def test_hit_after_miss(cache: Cache) -> None:
    repo = Repo()
    with record() as rec:
        assert await repo.get(1) == Product(1, "p1")
        assert await repo.get(1) == Product(1, "p1")
        await repo.get(2)
    assert repo.calls == 2
    assert rec.calls(Repo.get) == 2
    assert rec.hits(repo.get) == 1


async def test_self_is_not_part_of_the_key(cache: Cache) -> None:
    a, b = Repo(), Repo()
    await a.get(1)
    await b.get(1)
    assert (a.calls, b.calls) == (1, 0)


async def test_cache_key_method_partitions_instances(cache: Cache) -> None:
    class Client:
        def __init__(self, tenant: str) -> None:
            self.tenant = tenant
            self.calls = 0

        def __cache_key__(self) -> str:
            return self.tenant

        @cached
        async def rates(self) -> dict[str, float]:
            self.calls += 1
            return {"eur": 1.0}

    a, b, a2 = Client("a"), Client("b"), Client("a")
    await a.rates()
    await b.rates()
    await a2.rates()
    assert (a.calls, b.calls, a2.calls) == (1, 1, 0)


async def test_unbound_call_through_the_class(cache: Cache) -> None:
    repo = Repo()
    await Repo.get(repo, 1)
    await repo.get(1)
    assert repo.calls == 1


async def test_module_level_function(cache: Cache) -> None:
    calls = []

    @cached
    async def square(x: int) -> int:
        calls.append(x)
        return x * x

    assert await square(3) == 9
    assert await square(3) == 9
    assert calls == [3]


async def test_ttl_expiry(cache: Cache, clock: FakeClock) -> None:
    calls = []

    @cached(ttl=10)
    async def f(x: int) -> int:
        calls.append(x)
        return x

    await f(1)
    clock.advance(9)
    await f(1)
    clock.advance(2)
    await f(1)
    assert calls == [1, 1]


async def test_default_ttl_from_cache(backend: MemoryBackend, clock: FakeClock) -> None:
    calls = []

    @cached
    async def f() -> int:
        calls.append(1)
        return 1

    async with Cache(backend=backend, ttl=5):
        await f()
        clock.advance(6)
        await f()
    assert len(calls) == 2


async def test_ttl_none_never_expires(cache: Cache, clock: FakeClock) -> None:
    calls = []

    @cached(ttl=None)
    async def f() -> int:
        calls.append(1)
        return 1

    await f()
    clock.advance(10**9)
    await f()
    assert len(calls) == 1


async def test_without_an_open_cache_calls_run_normally() -> None:
    calls = []

    @cached
    async def f() -> int:
        calls.append(1)
        return 1

    await f()
    await f()
    assert len(calls) == 2


async def test_backend_is_used_outside_a_session(cache: Cache) -> None:
    repo = Repo()
    await repo.get(1)
    async with cache.session():
        await repo.get(1)
    await repo.get(1)
    assert repo.calls == 1


async def test_strict_mode_requires_a_session(cache: Cache) -> None:
    cache_cache.configure(strict=True)
    with pytest.raises(NoActiveSession):
        await Repo().get(1)
    async with cache.session():
        await Repo().get(1)


async def test_session_layer_is_checked_first(cache: Cache, backend: MemoryBackend) -> None:
    repo = Repo()
    async with cache.session() as session:
        with record() as rec:
            await repo.get(1)
            await backend.delete(list(backend._entries))
            await repo.get(1)
        assert len(session) == 1
    assert repo.calls == 1
    assert [e.layer for e in rec.events if e.kind == "hit"] == ["session"]


async def test_session_max_entries(cache: Cache) -> None:
    repo = Repo()
    async with cache.session(max_entries=2) as session:
        for pid in range(5):
            await repo.get(pid)
        assert len(session) == 2


async def test_session_only_never_touches_the_backend(cache: Cache, backend: MemoryBackend) -> None:
    calls = []

    @cached(session_only=True)
    async def f(x: int) -> int:
        calls.append(x)
        return x

    async with cache.session():
        await f(1)
        await f(1)
    async with cache.session():
        await f(1)
    assert calls == [1, 1]
    assert len(backend) == 0


async def test_session_requires_open_cache() -> None:
    with pytest.raises(cache_cache.CacheClosedError):
        async with Cache().session():
            pass


async def test_key_ignore_by_type(backend: MemoryBackend) -> None:
    calls = []

    @cached
    async def f(db: DbSession, x: int) -> int:
        calls.append(x)
        return x

    async with Cache(backend=backend, key_ignore=[DbSession]):
        await f(DbSession(), 1)
        await f(DbSession(), 1)
    assert calls == [1]


async def test_function_level_key_ignore_and_formats(cache: Cache) -> None:
    calls = []

    @cached(
        key_ignore=[DbSession], key_formats={Decimal: lambda d: str(d.quantize(Decimal("0.00")))}
    )
    async def price(db: DbSession, amount: Decimal) -> str:
        calls.append(amount)
        return str(amount)

    await price(DbSession(), Decimal("1.1"))
    await price(DbSession(), Decimal("1.10"))
    assert len(calls) == 1


async def test_unencodable_parameter_fails_when_the_cache_opens() -> None:
    @cached
    async def f(db: DbSession) -> int:
        return 1

    with pytest.raises(ConfigurationError, match="'db'.*no key encoding"):
        await Cache().open()


async def test_key_function(cache: Cache) -> None:
    calls = []

    class Search:
        @cached
        async def search(self, text: str, page: int, trace_id: str) -> list[str]:
            calls.append(text)
            return [text]

        @search.key
        def _search_key(self, text: str, page: int, trace_id: str) -> tuple[str, int]:
            return (text.lower(), page)

    s = Search()
    await s.search("Shoes", 1, "t1")
    await s.search("shoes", 1, "t2")
    await s.search("shoes", 2, "t3")
    assert calls == ["Shoes", "shoes"]


async def test_conditions(cache: Cache) -> None:
    calls = []

    class Lists:
        @cached
        async def by_category(self, category: str) -> list[str]:
            calls.append(category)
            return [] if category == "empty" else [category]

        @by_category.condition
        def _cacheable(self, category: str) -> bool:
            return category != "flash-sale"

        @by_category.result_condition
        def _worth_keeping(self, result: list[str]) -> bool:
            return len(result) > 0

    lists = Lists()
    for category in ["flash-sale", "flash-sale", "empty", "empty", "shoes", "shoes"]:
        await lists.by_category(category)
    assert calls == ["flash-sale", "flash-sale", "empty", "empty", "shoes"]


def test_condition_signature_is_checked() -> None:
    @cached
    async def f(x: int) -> int:
        return x

    with pytest.raises(ConfigurationError, match="same parameters"):

        @f.condition
        def _bad(y: int) -> bool:
            return True

    with pytest.raises(ConfigurationError, match="must take"):

        @f.result_condition
        def _bad_result(a: int, b: int) -> bool:
            return True


def test_tag_placeholders_are_checked_at_import() -> None:
    with pytest.raises(
        ConfigurationError,
        match="'product:\\{id\\}' references 'id' but get\\(\\) has no such parameter",
    ):

        @cached(tags=["product:{id}"])
        async def get(pid: int) -> int:
            return pid


def test_sync_functions_are_rejected() -> None:
    with pytest.raises(ConfigurationError, match="async"):
        cached(lambda: 1)  # type: ignore[arg-type]


def test_invalid_options() -> None:
    with pytest.raises(ConfigurationError, match="ttl must be positive"):

        @cached(ttl=0)
        async def a() -> int:
            return 1

    with pytest.raises(ConfigurationError, match="needs a ttl"):

        @cached(ttl=None, stale_while_revalidate=5)
        async def b() -> int:
            return 1


async def test_code_change_changes_the_key(cache: Cache) -> None:
    def make(body: int):
        @cached
        async def f() -> int:
            return body

        return f

    one = make(1)
    exec_namespace: dict[str, object] = {"cached": cached}
    exec(
        "@cached\nasync def f() -> int:\n    return 2\n",
        exec_namespace,
    )
    assert await one() == 1
    # Same qualname, different source -> different key.
    assert one._plan(cache).key_prefix != exec_namespace["f"]._plan(cache).key_prefix  # type: ignore[attr-defined]


async def test_version_changes_the_key(cache: Cache) -> None:
    @cached(version="1")
    async def a() -> int:
        return 1

    @cached(version="2")
    async def b() -> int:
        return 1

    assert a._plan(cache).key_prefix.split(":")[-1] != b._plan(cache).key_prefix.split(":")[-1]


async def test_namespace_separates_caches(backend: MemoryBackend) -> None:
    calls = []

    @cached
    async def f() -> int:
        calls.append(1)
        return 1

    async with Cache(backend=backend, namespace="v1"):
        await f()
    async with Cache(backend=backend, namespace="v2"):
        await f()
    assert len(calls) == 2


async def test_disabled(cache: Cache) -> None:
    repo = Repo()
    with cache_cache.disabled():
        await repo.get(1)
        await repo.get(1)
    assert repo.calls == 2


async def test_exceptions_are_not_stored(cache: Cache) -> None:
    calls = []

    @cached
    async def f() -> int:
        calls.append(1)
        if len(calls) == 1:
            raise ValueError("boom")
        return 1

    with pytest.raises(ValueError):
        await f()
    assert await f() == 1
    assert len(calls) == 2


async def test_refresh_peek_invalidate_prime(cache: Cache) -> None:
    repo = Repo()
    assert await repo.get.peek(1) is None
    await repo.get(1)
    assert await repo.get.peek(1) == Product(1, "p1")
    assert repo.calls == 1

    await repo.get.refresh(1)
    assert repo.calls == 2

    await repo.get.invalidate(1)
    assert await repo.get.peek(1) is None
    await repo.get(1)
    assert repo.calls == 3

    await repo.get.prime(Product(9, "primed"), 9)
    assert await repo.get(9) == Product(9, "primed")
    assert repo.calls == 3


async def test_prime_into_session(cache: Cache) -> None:
    repo = Repo()
    async with cache.session() as session:
        await repo.get.prime(Product(5, "x"), 5)
        assert len(session) == 1
        assert await repo.get(5) == Product(5, "x")
    assert repo.calls == 0


async def test_controls_on_module_function(cache: Cache) -> None:
    calls = []

    @cached
    async def f(x: int) -> int:
        calls.append(x)
        return x

    await f(1)
    assert await f.peek(1) == 1
    await f.refresh(1)
    await f.invalidate(1)
    assert await f.peek(1) is None
    await f.prime(7, 2)
    assert await f(2) == 7
    assert calls == [1, 1]


async def test_backend_failure_degrades_to_calling(
    cache: Cache, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def broken(*args: object) -> list[object]:
        raise ConnectionError("down")

    monkeypatch.setattr(cache.backend, "get_entries", broken)
    repo = Repo()
    with record() as rec:
        assert await repo.get(1) == Product(1, "p1")
        assert await repo.get(1) == Product(1, "p1")
    assert repo.calls == 2
    assert any(e.kind == "error" for e in rec.events)
    assert not any(e.kind == "store" for e in rec.events)


async def test_concurrent_lookups_share_one_backend_round_trip(
    cache: Cache, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = Repo()
    original = cache.backend.get_entries
    round_trips = []

    async def counting(keys, types):  # type: ignore[no-untyped-def]
        round_trips.append(len(keys))
        return await original(keys, types)

    monkeypatch.setattr(cache.backend, "get_entries", counting)
    await asyncio.gather(*(repo.get(i) for i in range(20)))
    assert round_trips == [20]
