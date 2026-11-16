# cache-cache

Caching for Python functions, `async def` or plain `def`. Three decorators cover four behaviours: deduping concurrent calls, caching per request, caching in a shared backend, and batching. The backend can be in memory or Redis.

```python
from cache_cache import Cache, cached, invalidates

cache = Cache(namespace="shop")


class ProductRepo:
    @cached(tags=["product:{pid}"])
    async def get(self, pid: int) -> Product: ...

    @cached(ttl=600, tags=["product:list:{category}"])
    async def by_category(self, category: str) -> list[Product]: ...

    @invalidates("product:{pid}", "product:list")
    async def update(self, pid: int, data: ProductUpdate) -> Product: ...


async with cache:                 # once, at startup
    async with cache.session():   # once per request, job or test
        await repo.get(42)        # runs the query
        await repo.get(42)        # served from the session
```

## What you get

- **Dedupe.** Concurrent identical calls share one execution (`@dedupe`, and every `@cached` function).
- **Two layers.** A session per unit of work, checked first, and a backend shared by every process.
- **Batching your callers don't see.** `asyncio.gather(*(repo.get(i) for i in ids))` becomes one bulk call. Callers keep calling `get`.
- **Hierarchical tags.** `product` invalidates `product:42` and `product:list:shoes`. Code that writes doesn't import code that reads.
- **No invalidation race.** An entry computed from data that was invalidated while it was running is never served. See [How it works](design.md).
- **Invalidation after commit.** Invalidations in `cache.transaction()` apply when the block exits, and the block reads its own writes.
- **Stale values.** Stale-while-revalidate and stale-if-error, per function.
- **Deploy-safe.** `Cache(version=GIT_SHA)` keeps each deploy's entries apart while invalidations still reach all of them, and `clear()` empties a namespace in one write.
- **Sync too.** The same decorators work on plain functions with a `SyncCache` (Django, Flask, workers). Dedupe works across threads.
- **Early errors.** Tag placeholders, key, condition and bulk signatures are checked at import. Key encodings and return types are checked when the cache opens, and the `cache-cache check` command catches mistyped tags in CI.
- **Hooks.** Observers for metrics and logs, and middleware that can change behaviour.

## Limits

- Batching needs `async` functions.
- Dedupe and batching happen within one process.
- Version 0.2: the API may still change.

Start with [Getting started](getting-started.md).
