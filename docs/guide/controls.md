# Per-call controls

Every `@cached` function (and `@dedupe`, for `forget`) has these methods, both on module functions and on bound methods:

```python
await repo.get.refresh(42)            # run the function, ignore the cache, store the new result
await repo.get.peek(42)               # Product | None: the fresh cached value, never calls
await repo.get.invalidate(42)         # evict this entry by its arguments
repo.get.forget(42)                   # the next call doesn't join the one in flight
await repo.get.prime(product, 42)     # store a value for these arguments
```

| Method | Notes |
|---|---|
| `refresh(*args)` | Later identical calls join this execution. Needs an open cache. |
| `peek(*args)` | Checks the session, then the backend. Returns `None` for stale or missing entries. It's `async` because the backend is. |
| `invalidate(*args)` | Removes the entry from the backend and from open sessions. Use tags to invalidate data; use this for one call. |
| `forget(*args)` | Only affects calls in flight in this process. |
| `prime(value, *args)` | The value comes first. Typical use: fill `get(id)` entries after a list query. |

```python
class ProductRepo:
    @cached(ttl=600, tags=["product:list:{category}"])
    async def by_category(self, category: str) -> list[Product]:
        products = await self.db.fetch_by_category(category)
        for product in products:
            await self.get.prime(product, product.id)
        return products
```

## Turning the cache off

```python
with cache_cache.disabled():
    await repo.get(42)    # runs the function: no cache, no dedupe
```

`disabled()` is a context manager scoped to the current context: tasks started inside the block inherit it. Batched functions still batch.
