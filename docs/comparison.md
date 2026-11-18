# Comparison

This comparison is based on each project's public documentation as of 2026. "Not documented" means we didn't find the feature in the docs, not that it can't be done. Corrections are welcome: [open an issue](https://github.com/mde-pach/cache-cache/issues).

| Feature | cache-cache | functools | cachetools | async-lru | aiocache | cashews | dogpile.cache | aiodataloader |
|---|---|---|---|---|---|---|---|---|
| Async functions | Yes | No | No | Yes | Yes | Yes | No | Yes |
| Sync functions | Yes (no batching) | Yes | Yes | No | No | No | Yes | No |
| Pluggable backends | Yes | No | In-memory | No | Yes | Yes | Yes | No |
| Concurrent identical calls share one execution | Yes | No | Not documented | Yes | Lock option | Lock option | Lock-based | Yes |
| Per-request lifetime | Session | No | No | No | Not documented | Not documented | No | One loader per request |
| Batching, callers unchanged | Yes | No | No | No | Explicit multi-key decorator | Not documented | Explicit multi-key decorator | Yes |
| Tag invalidation | Hierarchical | No | No | No | Not documented | Yes | Not documented | No |
| Invalidation after a DB commit | `cache.transaction()` | No | No | No | Not documented | Transaction support | Not documented | No |
| Stale-while-revalidate | Yes | No | No | No | Not documented | Yes | Yes | No |
| Stale-if-error | Yes | No | No | No | Not documented | Yes | Not documented | No |
| Empty on deploy | Automatic, per deployment | No | No | No | Not documented | Not documented | Not documented | No |
| Conditions on arguments and result | Yes | No | No | No | Partial | Yes | Partial | No |
| Prime values | Yes | No | Manual | No | Yes | Yes | Yes | Yes |
| Observers and middleware | Yes | No | No | No | Plugins | Middleware | Proxy backends | No |
| Checks at startup and in CI | Yes | No | No | No | No | No | No | No |
| Maturity | New | Stdlib | Mature | Mature | Mature | Mature | Mature | Mature |

## When to choose something else

- **Simple sync memoization:** `functools.cache` or `cachetools`. Use `SyncCache` when you need tags, sessions or a shared backend.
- **A mature async cache with many features:** cashews is the closest overall. Its feature set overlaps a lot with this library's.
- **Only per-request batching:** aiodataloader, or Strawberry's DataLoader if you use GraphQL.
- **HTTP caching (ETag, `Cache-Control`, 304):** a dedicated HTTP caching layer. cache-cache doesn't handle HTTP.
