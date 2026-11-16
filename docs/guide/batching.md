# Batching

Concurrent calls with different arguments become one bulk call. Callers keep calling `get`.

```python
from collections.abc import Mapping, Sequence


class ProductRepo:
    @cached(tags=["product:{pid}"])
    async def get(self, pid: int, include_deleted: bool = False) -> Product | None: ...

    @get.batched(on="pid", max_size=100)
    async def get_many(
        self, pids: Sequence[int], include_deleted: bool = False
    ) -> Mapping[int, Product]:
        rows = await self.db.fetch_products(pids, include_deleted)
        return {row.id: Product(row.id, row.name) for row in rows}


await asyncio.gather(*(repo.get(o.product_id) for o in orders))   # one query, not 50
```

## How a batch forms

1. The cache lookups of every call started in the same event-loop iteration share one backend round trip.
2. The misses are queued together. After the current iteration, they are sent as one call to `get_many`, one call per `max_size` ids.
3. Each result is stored under its own `get(pid)` key, with its own tags.

Ids already cached never reach `get_many`, and duplicate ids are sent once.

## Rules

- `get_many` takes `get`'s parameters, with `pid` replaced by a sequence of pids at the same position. Different parameter names fail at import. A non-sequence annotation fails when the cache opens.
- Calls share a batch only when they are made on the same instance and every other argument has the same key encoding. Here, `get(3, include_deleted=True)` goes into a separate batch.
- The `on` values must be hashable, and the returned mapping must use the same values as keys.
- An id missing from the mapping resolves to `None` if `get`'s return type is optional. Otherwise it raises `KeyError` for that caller only.
- If `get_many` raises, every caller in that batch gets the exception.

## Waiting longer: `window`

By default, a batch is sent after the current event-loop iteration. If calls arrive spread over time, for example after other awaits, add a window:

```python
@get.batched(on="pid", window=0.005)   # wait 5 ms for more ids
```

`max_size` still applies: a batch that reaches `max_size` is sent immediately.

## Calling the bulk function directly

`get_many` stays callable, and it goes through the cache:

```python
found = await repo.get_many([1, 2, 3])   # {1: ..., 2: ...}
```

Each id is looked up as `get(id)`, and only the misses reach the bulk function. Missing ids are left out of the mapping.

## Without a cache

Batching doesn't need storage. It still applies with no open cache, inside `cache_cache.disabled()`, and for calls whose condition is false.
