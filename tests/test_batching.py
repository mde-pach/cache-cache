import asyncio
from collections.abc import Mapping, Sequence

import pytest

from cache_cache import Cache, ConfigurationError, cached, record

from .conftest import Product, tick


class Repo:
    def __init__(self, missing: set[int] = frozenset()) -> None:  # type: ignore[assignment]
        self.bulk_calls: list[tuple[list[int], bool]] = []
        self.missing = missing

    @cached(tags=["product:{pid}"])
    async def get(self, pid: int, include_deleted: bool = False) -> Product | None:
        raise AssertionError("served by get_many")

    @get.batched(on="pid", max_size=100)
    async def get_many(
        self, pids: Sequence[int], include_deleted: bool = False
    ) -> Mapping[int, Product]:
        self.bulk_calls.append((list(pids), include_deleted))
        return {pid: Product(pid, f"p{pid}") for pid in pids if pid not in self.missing}


class StrictRepo:
    def __init__(self) -> None:
        self.bulk_calls = 0

    @cached
    async def get(self, pid: int) -> Product:
        raise AssertionError

    @get.batched(on="pid")
    async def get_many(self, pids: Sequence[int]) -> Mapping[int, Product]:
        self.bulk_calls += 1
        return {pid: Product(pid, "x") for pid in pids if pid != 13}


async def test_concurrent_calls_become_one_bulk_call(cache: Cache) -> None:
    repo = Repo()
    with record() as rec:
        products = await asyncio.gather(*(repo.get(i) for i in range(50)))
    assert [p.id for p in products if p] == list(range(50))
    assert len(repo.bulk_calls) == 1
    assert rec.calls(Repo.get) == 1
    assert rec.batch_sizes(Repo.get) == [50]


async def test_results_are_cached_per_id(cache: Cache) -> None:
    repo = Repo()
    await asyncio.gather(*(repo.get(i) for i in range(5)))
    await asyncio.gather(*(repo.get(i) for i in range(8)))
    assert [ids for ids, _ in repo.bulk_calls] == [[0, 1, 2, 3, 4], [5, 6, 7]]


async def test_duplicate_ids_are_sent_once(cache: Cache) -> None:
    repo = Repo()
    await asyncio.gather(*(repo.get(i % 3) for i in range(9)))
    assert repo.bulk_calls == [([0, 1, 2], False)]


async def test_max_size_splits_batches(cache: Cache) -> None:
    class Small(Repo):
        pass

    repo = Repo()
    repo_spec = Repo.get._batch
    assert repo_spec is not None
    original = repo_spec.max_size
    repo_spec.max_size = 4
    try:
        with record() as rec:
            await asyncio.gather(*(repo.get(i) for i in range(10)))
    finally:
        repo_spec.max_size = original
    assert sorted(rec.batch_sizes(Repo.get)) == [2, 4, 4]


async def test_other_arguments_must_match_to_share_a_batch(cache: Cache) -> None:
    repo = Repo()
    await asyncio.gather(repo.get(1), repo.get(2), repo.get(3, include_deleted=True))
    assert sorted(repo.bulk_calls) == [([1, 2], False), ([3], True)]


async def test_missing_ids_resolve_to_none_for_optional_returns(cache: Cache) -> None:
    repo = Repo(missing={2})
    assert await asyncio.gather(repo.get(1), repo.get(2)) == [Product(1, "p1"), None]


async def test_missing_ids_raise_key_error_for_that_caller_only(cache: Cache) -> None:
    repo = StrictRepo()
    results = await asyncio.gather(repo.get(1), repo.get(13), return_exceptions=True)
    assert results[0] == Product(1, "x")
    assert isinstance(results[1], KeyError)


async def test_bulk_errors_reach_every_caller(cache: Cache) -> None:
    class Broken:
        @cached
        async def get(self, pid: int) -> int:
            raise AssertionError

        @get.batched(on="pid")
        async def get_many(self, pids: Sequence[int]) -> Mapping[int, int]:
            raise ConnectionError("db down")

    broken = Broken()
    results = await asyncio.gather(broken.get(1), broken.get(2), return_exceptions=True)
    assert all(isinstance(r, ConnectionError) for r in results)


async def test_instances_are_batched_separately(cache: Cache) -> None:
    a, b = Repo(), Repo()
    a.__dict__["__cache_key__"] = None  # instances are still interchangeable for keys
    await asyncio.gather(a.get(1), b.get(2))
    assert a.bulk_calls == [([1], False)]
    assert b.bulk_calls == [([2], False)]


async def test_calling_the_bulk_function_directly_uses_the_cache(cache: Cache) -> None:
    repo = Repo(missing={3})
    await repo.get(1)
    found = await repo.get_many([1, 2, 3])
    assert found == {1: Product(1, "p1"), 2: Product(2, "p2")}
    assert [ids for ids, _ in repo.bulk_calls] == [[1], [2, 3]]


async def test_window_waits_for_more_calls(cache: Cache) -> None:
    class Windowed:
        def __init__(self) -> None:
            self.sizes: list[int] = []

        @cached
        async def get(self, pid: int) -> int:
            raise AssertionError

        @get.batched(on="pid", window=0.05)
        async def get_many(self, pids: Sequence[int]) -> Mapping[int, int]:
            self.sizes.append(len(pids))
            return {p: p for p in pids}

    w = Windowed()

    async def later(pid: int, delay: float) -> int:
        await asyncio.sleep(delay)
        return await w.get(pid)

    await asyncio.gather(later(1, 0), later(2, 0.01), later(3, 0.02))
    assert w.sizes == [3]


async def test_batching_works_without_an_open_cache() -> None:
    repo = Repo()
    await asyncio.gather(*(repo.get(i) for i in range(3)))
    assert len(repo.bulk_calls) == 1


async def test_module_level_batching(cache: Cache) -> None:
    sizes = []

    @cached
    async def square(x: int) -> int:
        raise AssertionError

    @square.batched(on="x")
    async def squares(xs: list[int]) -> dict[int, int]:
        sizes.append(len(xs))
        return {x: x * x for x in xs}

    assert await asyncio.gather(square(2), square(3)) == [4, 9]
    assert sizes == [2]
    await tick()


def test_bulk_signature_is_checked() -> None:
    @cached
    async def get(pid: int, flag: bool = False) -> int:
        return pid

    with pytest.raises(ConfigurationError, match="with 'pid' replaced by a sequence"):

        @get.batched(on="pid")
        async def get_many(pids: list[int]) -> dict[int, int]:
            return {}

    with pytest.raises(ConfigurationError, match="no such parameter"):

        @get.batched(on="nope")
        async def get_many2(pids: list[int], flag: bool = False) -> dict[int, int]:
            return {}


async def test_bulk_parameter_must_be_a_sequence() -> None:
    @cached
    async def get(pid: int) -> int:
        return pid

    @get.batched(on="pid")
    async def get_many(pids: int) -> dict[int, int]:
        return {}

    with pytest.raises(ConfigurationError, match="must be a sequence"):
        await Cache().open()
