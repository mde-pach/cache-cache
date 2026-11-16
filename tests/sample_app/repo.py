from cache_cache import cached, invalidate, invalidates


class ProductRepo:
    @cached(tags=["product:{pid}"])
    async def get(self, pid: int) -> int:
        return pid

    @cached(tags=["product:list:{category}"])
    async def by_category(self, category: str) -> list[int]:
        return []

    @cached(tags=["order:{oid}"])
    async def order(self, oid: int) -> int:
        return oid

    @invalidates("product:{pid}", "product:list")
    async def update(self, pid: int) -> None: ...

    @invalidates("prodcut:{pid}")
    async def typo(self, pid: int) -> None: ...


async def cleanup() -> None:
    await invalidate("product")
