# Changelog

## 0.1.0 — unreleased

First version.

- `@cached`, `@dedupe`, `@invalidates`, and batching with `@fn.batched`.
- Per-unit-of-work sessions on top of a pluggable backend (memory, Redis).
- Hierarchical tags with versioned invalidation, and invalidation deferred until after a commit.
- Stale-while-revalidate and stale-if-error.
- Deterministic keys, and keys that change automatically when a function's code or return type changes.
- Observers, middleware, stats and logging.
- `cache-cache check` for CI, and `cache_cache.record()` for tests.
