# Backends

A backend stores entries and tag versions. The cache doesn't depend on a particular one.

## Memory

```python
from cache_cache import Cache, MemoryBackend

cache = Cache(backend=MemoryBackend(max_entries=100_000))
```

- Entries live in this process, with least-recently-used eviction above `max_entries`.
- Values are stored as-is, not copied, so every caller gets the same object. Treat them as read-only.
- `MemoryBackend` is the default.

## Redis

```bash
pip install "cache-cache[redis]"
```

```python
from redis.asyncio import Redis

from cache_cache import Cache, PydanticCodec
from cache_cache.backends.redis import RedisBackend

cache = Cache(backend=RedisBackend(url="redis://localhost:6379/0"))

# or with your own client (you manage its lifetime), a codec and a key prefix:
cache = Cache(
    backend=RedisBackend(client=Redis(), codec=PydanticCodec(), prefix="cc:"),
)
```

- Lookups are batched: one `MGET` for the entries and one for the tag versions per event-loop iteration.
- Entries are written with their expiry (`PX`).
- Tag versions are counters (`INCR`) without expiry. They are tiny, one per distinct tag.
- If Redis is unreachable during a lookup, the call runs uncached, an `error` event is emitted, and nothing is stored.
- Entries without `ttl` don't expire, so configure a `maxmemory` eviction policy.

## Sync backends

`SyncCache` takes sync backends with the same methods, not `async`:

- `SyncMemoryBackend(max_entries=...)`: thread-safe.
- `SyncRedisBackend(client=redis.Redis(...) | url=..., codec=..., prefix=...)`: the same layout as `RedisBackend`, so sync and async processes can share a Redis and its tags.

A custom sync backend implements `cache_cache.backends.SyncBackend`. Its methods may be called from several threads at once.

## Codecs

Backends that store bytes use a codec. The codec receives the decorated function's return annotation.

| Codec | Notes |
|---|---|
| `PickleCodec()` (default) | Handles almost anything. Only share the backend between trusted processes. |
| `PydanticCodec()` | JSON through a pydantic `TypeAdapter` of the return type. Values are validated when read; an entry that no longer validates is a miss. A return type pydantic can't handle fails when the cache opens. |

A codec implements `encode(value, tp) -> bytes`, `decode(data, tp)` and `check(tp)`. `check(tp)` raises `ConfigurationError` for types it can't handle.

## Writing a backend

Implement the `Backend` protocol from `cache_cache.backends`. Every method works on batches:

```python
class Backend(Protocol):
    async def open(self) -> None: ...
    async def close(self) -> None: ...
    async def get_entries(self, keys: Sequence[str], types: Sequence[Any]) -> list[Entry | None]: ...
    async def set_entries(self, items: Sequence[StoreItem]) -> None: ...
    async def delete(self, keys: Sequence[str]) -> None: ...
    async def get_tag_versions(self, tags: Sequence[str]) -> list[int]: ...   # 0 if never bumped
    async def bump_tags(self, tags: Sequence[str]) -> None: ...               # atomic increment
    def check_type(self, tp: Any) -> None: ...                                # raise ConfigurationError
```

- `Entry` holds `value`, `stored_at`, `fresh_until` (`None` for no expiry) and `tag_versions`.
- `StoreItem.ttl` is how long the backend should keep the entry: freshness plus any stale window. `None` means keep it.
- `get_entries` returns `None` for an entry it can't decode. Never raise for that.
- `bump_tags` must be atomic per tag across processes. That is what makes [tag versioning](../design.md#tag-versions) safe.
