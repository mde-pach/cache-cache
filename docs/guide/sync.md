# Sync code

`@cached`, `@dedupe` and `@invalidates` also work on plain `def` functions: Django views, Flask handlers, Celery tasks, scripts. They use a `SyncCache` instead of a `Cache`, and everything works the same way except batching.

```python
from cache_cache import SyncCache, cached, invalidates

cache = SyncCache(namespace="shop", version=GIT_SHA)


class ProductRepo:
    @cached(tags=["product:{pid}"])
    def get(self, pid: int) -> Product: ...

    @invalidates("product:{pid}", "product:list")
    def update(self, pid: int, data: ProductUpdate) -> Product: ...


with cache:                       # once, at startup
    with cache.session():         # once per request, job or test
        repo.get(42)
        with cache.transaction(): # invalidate after the commit
            repo.update(42, data)
```

The decorator picks the engine from the function: `async def` uses the current `Cache`, `def` uses the current `SyncCache`. An app can open both.

## What's the same

- The session and backend layers, `ttl`, `session_only`, `version`, `clear()`.
- Tags, `@invalidates`, transactions and tag versioning.
- Key rules, conditions, key functions and checks.
- Stale-while-revalidate and stale-if-error.
- `refresh`, `peek`, `invalidate`, `forget` and `prime`, all plain methods.
- Observers, `record()` and `cache-cache check`.

## What's different

| | `Cache` (async) | `SyncCache` |
|---|---|---|
| Open | `async with cache:` | `with cache:` |
| Sessions, transactions | `async with` | `with` |
| Invalidate imperatively | `await cache_cache.invalidate(...)` | `cache_cache.invalidate_sync(...)` or `cache.invalidate(...)` |
| Dedupe | concurrent tasks share one execution | concurrent **threads** share one execution |
| Background refreshes | tasks | a thread pool (`refresh_workers=4`) |
| Batching | `@get.batched(...)` | not available |
| Backends | `MemoryBackend`, `RedisBackend` | `SyncMemoryBackend`, `SyncRedisBackend` |
| Middleware | `async def __call__(call, call_next)` | `def __call__(call, call_next)`, e.g. `SyncDontJoinOldCalls` |

**Why no batching:** batching merges calls that are waiting at the same time. Sync code runs one call after another, so nothing waits alongside. Call the bulk function directly, and use `prime()` to fill the single-item entries if you want them:

```python
products = repo.get_many(ids)          # a plain function
for pid, product in products.items():
    repo.get.prime(product, pid)
```

## Sessions per request

Context variables are per thread, so one session per request works in threaded servers.

=== "Django"

    ```python
    # middleware.py
    from myapp.cache import cache

    def cache_session(get_response):
        def middleware(request):
            with cache.session(max_entries=10_000):
                return get_response(request)
        return middleware

    # apps.py
    class MyAppConfig(AppConfig):
        def ready(self):
            cache.open()
    ```

=== "Flask"

    ```python
    from flask import Flask
    from myapp.cache import cache

    app = Flask(__name__)
    cache.open()

    @app.before_request
    def open_session():
        g.cache_session = cache.session().__enter__()

    @app.teardown_request
    def close_session(exc):
        g.cache_session.__exit__(None, None, None)
    ```

## Sharing a backend with async code

`SyncRedisBackend` and `RedisBackend` use the same layout. A sync admin and an async API on the same Redis and namespace share tags, so an invalidation from either side reaches both. The entries of a sync function and an async function are separate, since they are different functions.
