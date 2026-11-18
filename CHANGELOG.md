# Changelog

## 0.3.0 — unreleased

- **The cache empties on every deploy, automatically.** Entries are scoped to the deployment, so a
  new deploy never reads entries from a previous one, even when the same code is redeployed. A
  restart within a deploy keeps them. Tags stay shared, so invalidations cross deployments during a
  rolling deploy. Old processes write into their own scope and can't refill the new one, which is
  what breaks flush-on-deploy.
- `Cache(deployment="auto")` detects the ID from `CACHE_CACHE_DEPLOYMENT`, `VERCEL_DEPLOYMENT_ID`,
  `RAILWAY_DEPLOYMENT_ID`, `HEROKU_RELEASE_VERSION`, `FLY_IMAGE_REF` or `K_REVISION`. When none is
  set, entries are scoped to the process, with a warning. `deployment="process"`, an explicit ID,
  or `None` (entries survive deploys) are also accepted.
- **Retired deployments are deleted.** Processes send a heartbeat for their deployment, and a
  deployment with no heartbeat for `retire_after` (10 min) has its entries deleted (Redis: `SCAN` +
  `UNLINK` behind a lock, timed with the server clock). New `DeploymentRegistry` backend capability,
  `collect_retired()`, and a `retire` event.
- **Removed:** `Cache(version=...)` and `clear_on_open`. `clear()` and the function-level
  `version=` remain.
- Namespaces are now restricted to letters, digits, `_`, `.` and `-`.

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
