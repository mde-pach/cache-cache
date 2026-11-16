# API reference

## Decorators

::: cache_cache.cached

::: cache_cache.dedupe

::: cache_cache.invalidates

## Cache and context

::: cache_cache.Cache
    options:
      members: [open, close, session, transaction, invalidate, clear, is_open]

::: cache_cache.Session
    options:
      members: false

::: cache_cache.Transaction
    options:
      members: false

::: cache_cache.invalidate

::: cache_cache.disabled

::: cache_cache.configure

## Sync engine

::: cache_cache.SyncCache
    options:
      members: [open, close, session, transaction, invalidate, clear, is_open]

::: cache_cache.SyncTransaction
    options:
      members: false

::: cache_cache.invalidate_sync

::: cache_cache.SyncCachedFunction
    options:
      members: [refresh, peek, invalidate, forget, prime]

::: cache_cache.SyncMemoryBackend

::: cache_cache.backends.redis.SyncRedisBackend

::: cache_cache.backends.SyncBackend

::: cache_cache.SyncDontJoinOldCalls

## Decorated functions

::: cache_cache.CachedFunction
    options:
      members: [key, condition, result_condition, batched, refresh, peek, invalidate, forget, prime, validate]

::: cache_cache.BatchedFunction
    options:
      members: false

## Hooks

::: cache_cache.Lookup

::: cache_cache.CacheEvent

::: cache_cache.Observer

::: cache_cache.Middleware

::: cache_cache.CachedCall

::: cache_cache.StatsObserver

::: cache_cache.FunctionStats

::: cache_cache.LoggingObserver

::: cache_cache.DontJoinOldCalls

## Backends and codecs

::: cache_cache.backends.Backend

::: cache_cache.backends.Entry

::: cache_cache.backends.StoreItem

::: cache_cache.MemoryBackend

::: cache_cache.backends.redis.RedisBackend

::: cache_cache.PickleCodec

::: cache_cache.PydanticCodec

## Testing

::: cache_cache.record

::: cache_cache.Recorder

::: cache_cache.check.run_check

## Errors

::: cache_cache.CacheCacheError

::: cache_cache.ConfigurationError

::: cache_cache.KeyEncodingError

::: cache_cache.NoActiveSession

::: cache_cache.CacheClosedError
