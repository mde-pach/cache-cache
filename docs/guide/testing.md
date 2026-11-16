# Testing

## Recording what happened

```python
import asyncio

import cache_cache


async def test_orders_page_loads_products_in_one_query(cache, repo):
    async with cache.session():
        with cache_cache.record() as rec:
            await asyncio.gather(*(repo.get(pid) for pid in range(50)))
            await repo.get(1)

    assert rec.calls(ProductRepo.get) == 1          # one bulk call
    assert rec.batch_sizes(ProductRepo.get) == [50]
    assert rec.hits(ProductRepo.get) == 1           # the last get(1), from the session
```

`record()` collects events from the current context and from tasks started inside it. A `Recorder` provides:

| Method | Counts |
|---|---|
| `calls(fn)` | real executions of the function (a bulk call counts once) |
| `hits(fn)`, `misses(fn)` | fresh hits, misses |
| `stale(fn)` | stale values served (both kinds) |
| `shared(fn)` | callers that joined a call in flight |
| `batch_sizes(fn)` | size of each bulk call |
| `invalidated()` | every tag invalidated, in order |
| `events` | the raw `CacheEvent` list |

`fn` can be the function on the class (`ProductRepo.get`), a bound method (`repo.get`) or the bulk function.

## A cache per test

```python
import pytest

from cache_cache import Cache, MemoryBackend


@pytest.fixture
async def cache():
    async with Cache(backend=MemoryBackend(), namespace="test") as cache:
        yield cache
```

A fresh `MemoryBackend` per test keeps tests independent. To test code with caching turned off, wrap it in `cache_cache.disabled()`.

## `cache-cache check` { #cache-cache-check }

Run the static checks in CI:

```bash
cache-cache check myapp            # exit 1 on errors
cache-cache check myapp --strict   # warnings fail too
```

The command imports the package and its submodules, then checks:

- every decorated function: tag placeholders, key, condition and bulk signatures, and key encodings of annotated parameters;
- every tag passed to `@invalidates` or to an `invalidate("…")` call with a string literal must match a declared tag, with a "did you mean" suggestion;
- declared tags that nothing invalidates are reported as warnings.

Or call it from a test:

```python
from cache_cache.check import run_check


def test_cache_declarations():
    report = run_check("myapp")
    assert report.ok, report.format()
```
