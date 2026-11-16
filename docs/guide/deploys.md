# Deploys and clearing

A cached entry outlives the code that wrote it. When a deploy changes what a function returns, the new code must not read the old entries. cache-cache doesn't try to guess this from your code. You say which version is running.

## `version` on the cache

```python
cache = Cache(namespace="shop", version=os.environ["GIT_SHA"])
```

The version is part of every key:

- **Entries never mix.** During a rolling deploy, old and new processes read and write separate entries, so neither sees the other's format.
- **Tags stay shared.** Tags are namespaced, not versioned, so an invalidation from a new process also reaches the entries of old processes, and the other way round.
- **Old entries expire on their own,** with their ttl. In Redis, use a ttl, or an eviction policy such as `allkeys-lru`.

Use any string that changes when your code changes: a git SHA, a package version, a build number. The cost is a cold cache after each deploy. If that's too expensive, use a version that only changes when the data format changes (for example, a schema version you bump by hand).

## `version` on a function

```python
@cached(ttl=600, version="2")
async def by_category(self, category: str) -> list[Product]: ...
```

Bump it when one function's results change shape or meaning. Only that function's entries are left behind.

## `clear()`

```python
await cache.clear()      # SyncCache: cache.clear()
```

Every entry in the namespace becomes unreadable, across every version and every process sharing the backend. It is a single write: every entry records a namespace "generation" when it is stored, and `clear()` increments it. Nothing is deleted. The entries stop matching and expire with their ttl. Open sessions in this process are emptied too.

A cleared entry is never served as a stale value.

## `clear_on_open`

```python
cache = Cache(clear_on_open=True)
```

Clears when the cache opens, so every process start begins with an empty cache. This is rarely what you want with a shared backend:

- With several workers, every worker start clears the cache of all the others.
- Autoscaling and restarts after a crash clear it too, at the moment load is highest.
- During a rolling deploy, the old processes still running keep filling it with the old format.

Prefer `version`. `clear_on_open` makes sense for a single process on a shared backend, such as a CLI or a development server. With `MemoryBackend`, entries already disappear when the process ends.
