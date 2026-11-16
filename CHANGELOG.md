# Changelog

## 0.2.0 — unreleased

- **Sync support.** `@cached`, `@dedupe` and `@invalidates` work on plain `def` functions with a
  new `SyncCache`, `SyncMemoryBackend` and `SyncRedisBackend`. Dedupe shares executions between
  threads, and stale-while-revalidate refreshes run in a thread pool. Batching stays async-only.
  `invalidate_sync()`, `SyncTransaction`, `SyncMiddleware` and `SyncDontJoinOldCalls` are the sync counterparts.
- **Code fingerprints removed.** Keys no longer include a hash of the function's code and return type.
  It missed changes in called functions and invalidated on harmless refactors and Python upgrades.
- **`Cache(version=...)`.** A deploy version in every key. Entries of different versions never mix,
  while tags stay shared across versions.
- **`cache.clear()`.** Empties a namespace in one write, through a generation counter every entry
  depends on. `Cache(clear_on_open=True)` clears when the cache opens.
- New event kind: `clear`.

## 0.1.0

First version.

- `@cached`, `@dedupe`, `@invalidates`, and batching with `@fn.batched`.
- Per-unit-of-work sessions on top of a pluggable backend (memory, Redis).
- Hierarchical tags with versioned invalidation, and invalidation deferred until after a commit.
- Stale-while-revalidate and stale-if-error.
- Deterministic keys.
- Observers, middleware, stats and logging.
- `cache-cache check` for CI, and `cache_cache.record()` for tests.
