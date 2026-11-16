import asyncio

import pytest

import cache_cache
from cache_cache import Cache, cached, invalidates, record

from .conftest import FakeClock, Product, tick


class Repo:
    def __init__(self) -> None:
        self.gets: list[int] = []
        self.lists: list[str] = []
        self.db: dict[int, Product] = {
            i: Product(i, f"p{i}", "shoes" if i % 2 else "hats") for i in range(1, 5)
        }

    @cached(tags=["product:{pid}"])
    async def get(self, pid: int) -> Product:
        self.gets.append(pid)
        return self.db[pid]

    @cached(ttl=600, tags=["product:list:{category}"])
    async def by_category(self, category: str) -> list[Product]:
        self.lists.append(category)
        return [p for p in self.db.values() if p.category == category]

    @invalidates("product:{pid}", "product:list")
    async def rename(self, pid: int, name: str) -> Product:
        self.db[pid] = Product(pid, name, self.db[pid].category)
        return self.db[pid]


async def warm(repo: Repo) -> None:
    for pid in range(1, 5):
        await repo.get(pid)
    await repo.by_category("shoes")
    await repo.by_category("hats")
    repo.gets.clear()
    repo.lists.clear()


async def refetch(repo: Repo) -> tuple[list[int], list[str]]:
    await warm_again(repo)
    return sorted(repo.gets), sorted(repo.lists)


async def warm_again(repo: Repo) -> None:
    for pid in range(1, 5):
        await repo.get(pid)
    await repo.by_category("shoes")
    await repo.by_category("hats")


@pytest.mark.parametrize(
    ("tag", "gets", "lists"),
    [
        ("product", [1, 2, 3, 4], ["hats", "shoes"]),
        ("product:1", [1], []),
        ("product:list", [], ["hats", "shoes"]),
        ("product:list:shoes", [], ["shoes"]),
    ],
)
async def test_hierarchical_invalidation(
    cache: Cache, tag: str, gets: list[int], lists: list[str]
) -> None:
    repo = Repo()
    await warm(repo)
    await cache_cache.invalidate(tag)
    assert await refetch(repo) == (gets, lists)


async def test_invalidates_decorator(cache: Cache) -> None:
    repo = Repo()
    await warm(repo)
    with record() as rec:
        await repo.rename(1, "new")
    assert rec.invalidated() == ["product:1", "product:list"]
    assert (await repo.get(1)).name == "new"
    assert await refetch(repo) == ([1], ["hats", "shoes"])


async def test_invalidates_does_nothing_when_the_function_raises(cache: Cache) -> None:
    @invalidates("product:{pid}")
    async def fail(pid: int) -> None:
        raise RuntimeError

    with record() as rec, pytest.raises(RuntimeError):
        await fail(1)
    assert rec.invalidated() == []


def test_invalidates_checks_placeholders() -> None:
    with pytest.raises(cache_cache.ConfigurationError, match="references 'id'"):

        @invalidates("product:{id}")
        async def update(pid: int) -> None: ...


async def test_invalidation_drops_session_entries(cache: Cache) -> None:
    repo = Repo()
    async with cache.session() as session:
        await repo.get(1)
        await repo.get(2)
        await cache_cache.invalidate("product:1")
        assert len(session) == 1
        await repo.get(1)
    assert repo.gets == [1, 2, 1]


async def test_a_write_racing_an_invalidation_is_never_served(cache: Cache) -> None:
    """Process B reads the old row, A commits and invalidates, B stores the old row.

    With tag versions, B's entry records the version it saw before running, so it
    is already stale when stored.
    """
    started, release = asyncio.Event(), asyncio.Event()
    db = {"value": "old"}
    calls = 0

    @cached(ttl=None, tags=["thing"])
    async def read() -> str:
        nonlocal calls
        calls += 1
        value = db["value"]
        started.set()
        await release.wait()
        return value

    slow_reader = asyncio.create_task(read())
    await started.wait()
    db["value"] = "new"
    await cache_cache.invalidate("thing")
    release.set()
    assert await slow_reader == "old"
    assert await read() == "new"
    assert calls == 2


async def test_transaction_defers_invalidation_and_reads_its_writes(cache: Cache) -> None:
    repo = Repo()
    await warm(repo)
    async with cache.transaction() as transaction:
        await repo.rename(1, "new")
        assert transaction.pending == ["product:1", "product:list"]
        # Not invalidated yet, but this code doesn't see the old value:
        assert (await repo.get(1)).name == "new"
        assert (await repo.get(1)).name == "new"
        assert repo.gets == [1, 1]
        # Other entries are served as usual:
        await repo.get(2)
        assert repo.gets == [1, 1]
    assert await refetch(repo) == ([1, 1, 1], ["hats", "shoes"])


async def test_transaction_applies_even_when_the_block_raises(cache: Cache) -> None:
    repo = Repo()
    await warm(repo)
    with record() as rec, pytest.raises(RuntimeError):
        async with cache.transaction():
            await repo.rename(1, "x")
            raise RuntimeError
    assert rec.invalidated() == ["product:1", "product:list"]


async def test_nested_transactions_join_the_outer_one(cache: Cache) -> None:
    with record() as rec:
        async with cache.transaction():
            async with cache.transaction():
                await cache_cache.invalidate("a")
            assert rec.invalidated() == []
            await cache_cache.invalidate("b")
    assert rec.invalidated() == ["a", "b"]


async def test_invalidate_without_cache_is_a_no_op() -> None:
    await cache_cache.invalidate("anything")


async def test_invalid_tag_rejected(cache: Cache) -> None:
    with pytest.raises(ValueError):
        await cache.invalidate("a::b")


async def test_entries_survive_unrelated_invalidations(cache: Cache, clock: FakeClock) -> None:
    repo = Repo()
    await warm(repo)
    await cache_cache.invalidate("order")
    await cache_cache.invalidate("productx")
    assert await refetch(repo) == ([], [])
    await tick()
