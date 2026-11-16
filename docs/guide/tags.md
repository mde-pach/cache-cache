# Tags and invalidation

Tags name data, not functions. A function declares the data its result depends on, and code that writes invalidates the data it changed, without importing the code that reads it.

```python
class ProductRepo:
    @cached(tags=["product:{pid}"])
    async def get(self, pid: int) -> Product: ...

    @cached(ttl=600, tags=["product:list:{category}"])
    async def by_category(self, category: str) -> list[Product]: ...

    @invalidates("product:{pid}", "product:list")
    async def update(self, pid: int, data: ProductUpdate) -> Product: ...
```

A placeholder names a parameter, and attribute access works (`"order:{order.id}"`). A placeholder that names no parameter fails at import:

```text
ConfigurationError: 'product:{id}' references 'id' but get() has no such parameter
```

## Hierarchy

Tags are `:`-separated paths. Invalidating a tag invalidates everything below it:

| Call | Evicts |
|---|---|
| `invalidate("product")` | every product entry, lists included |
| `invalidate("product:42")` | `get(42)` only |
| `invalidate("product:list")` | every cached list |
| `invalidate("product:list:shoes")` | `by_category("shoes")` only |

Values substituted into a template are escaped, so they always form one segment. With `name="a:b"`, `"user:{name}"` renders as `user:a\:b`, and invalidating `user:a` doesn't touch it.

!!! note
    A placeholder can still produce the same text as a literal segment. `user:{name}` with `name="list"` and a literal `user:list` tag are the same tag. Avoid templates where a placeholder and a literal share a position under the same prefix.

## Invalidating

```python
@invalidates("product:{pid}", "product:list")    # after the function returns
async def update(self, pid: int, data: ProductUpdate) -> Product: ...

await cache_cache.invalidate("product:42")       # on the current cache
await cache.invalidate("product:42")             # on a given cache
```

- `@invalidates` only runs when the function returns. If it raises, nothing is invalidated.
- `cache_cache.invalidate` does nothing when no cache is open.

Invalidation applies to the backend, to every session open in this process, and to every lifetime. It costs one counter increment per tag, however many entries sit below it (see [How it works](../design.md#tag-versions)).

## After the commit: transactions

```python
async with cache.transaction():
    async with db.begin():
        await repo.update(42, data)
    # committed; product:42 and product:list are invalidated when the outer block exits
```

Inside the block:

- Invalidations are collected, not applied.
- Entries carrying a pending tag are neither served nor stored, so the block reads its own writes. Other entries are served as usual.

When the block exits:

- The collected tags are invalidated, even if the block raised. An extra invalidation is harmless, while a missing one serves stale data.
- A nested `cache.transaction()` joins the outermost one.

## Checking tags in CI

`cache-cache check myapp` reports tags that are invalidated but never declared:

```text
error: myapp.repo.ProductRepo.update: invalidates("prodcut:{pid}") matches no declared tag, did you mean "product:{pid}"?
warning: tag "category:{cid}" is never invalidated
```

See [Testing](testing.md#cache-cache-check).
