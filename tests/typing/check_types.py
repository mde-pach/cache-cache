# mypy: disable-error-code="empty-body"
"""Checked by mypy in CI (not run): signatures survive the decorators."""

from collections.abc import Mapping, Sequence
from typing import assert_type

from cache_cache import cached, dedupe, invalidates


class Product:
    pass


class Repo:
    @cached(tags=["product:{pid}"])
    async def get(self, pid: int, include_deleted: bool = False) -> Product | None: ...

    @get.batched(on="pid")
    async def get_many(
        self, pids: Sequence[int], include_deleted: bool = False
    ) -> Mapping[int, Product]: ...

    @dedupe
    async def rates(self) -> dict[str, float]: ...

    @invalidates("product:{pid}")
    async def update(self, pid: int, name: str) -> Product: ...

    @cached
    async def search(self, text: str) -> list[Product]: ...

    @search.condition
    def _cacheable(self, text: str) -> bool:
        return True

    @search.key
    def _key(self, text: str) -> str:
        return text.lower()


@cached(ttl=60)
async def square(x: int) -> int:
    return x * x


class SyncRepo:
    @cached(tags=["product:{pid}"])
    def get(self, pid: int) -> Product | None: ...

    @dedupe
    def rates(self) -> dict[str, float]: ...

    @invalidates("product:{pid}")
    def update(self, pid: int) -> Product: ...


@cached
def sync_square(x: int) -> int:
    return x * x


async def main() -> None:
    repo = Repo()
    assert_type(await repo.get(1), Product | None)
    assert_type(await repo.get(1, include_deleted=True), Product | None)
    assert_type(await repo.get.peek(1), Product | None)
    assert_type(await repo.get.refresh(1), Product | None)
    await repo.get.prime(Product(), 1)
    repo.get.forget(1)
    await repo.get.invalidate(1)
    assert_type(await repo.get_many([1, 2]), dict[int, Product | None])
    assert_type(await repo.rates(), dict[str, float])
    assert_type(await repo.update(1, "x"), Product)
    assert_type(await square(2), int)
    assert_type(await square.peek(2), int | None)

    sync_repo = SyncRepo()
    assert_type(sync_repo.get(1), Product | None)
    assert_type(sync_repo.get.peek(1), Product | None)
    assert_type(sync_repo.rates(), dict[str, float])
    assert_type(sync_repo.update(1), Product)
    assert_type(sync_square(2), int)
    sync_repo.get.prime(Product(), 1)

    await repo.get("1")  # type: ignore[arg-type]
    sync_repo.get("1")  # type: ignore[arg-type]
    await square(1, 2)  # type: ignore[call-arg]
    await repo.get.prime("not a product", 1)  # type: ignore[arg-type]
