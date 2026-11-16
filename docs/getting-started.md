# Getting started

## Install

```bash
pip install "cache-cache @ git+https://github.com/mde-pach/cache-cache"            # in-memory backend only
pip install "cache-cache[redis] @ git+https://github.com/mde-pach/cache-cache"     # + RedisBackend
pip install "cache-cache[pydantic] @ git+https://github.com/mde-pach/cache-cache"  # + PydanticCodec
```

The package isn't published on PyPI yet.

You need Python 3.11 or later.

## Create the cache

Create one cache per application, at module level:

```python
# myapp/cache.py
from cache_cache import Cache, MemoryBackend

cache = Cache(
    namespace="myapp",          # prefix for every key and tag
    backend=MemoryBackend(),    # the default; see Backends for Redis
    ttl=300,                    # default lifetime for @cached functions, in seconds
)
```

## Decorate functions

```python
from dataclasses import dataclass

from cache_cache import cached, dedupe, invalidates


@dataclass(frozen=True)
class Product:
    id: int
    name: str


class ProductRepo:
    def __init__(self, db) -> None:
        self.db = db                      # dependencies live on the instance

    @cached(tags=["product:{pid}"])
    async def get(self, pid: int) -> Product:
        row = await self.db.fetch_product(pid)
        return Product(row.id, row.name)

    @invalidates("product:{pid}")
    async def rename(self, pid: int, name: str) -> None:
        await self.db.rename(pid, name)


class FxClient:
    @dedupe                               # share concurrent calls, store nothing
    async def rates(self) -> dict[str, float]: ...
```

`self` is never part of a key. Pass data as arguments and keep dependencies on the instance. If instances aren't interchangeable, for example one client per tenant, see [Keys](guide/keys.md#instances).

## Open the cache, then open sessions

```python
async def main() -> None:
    async with cache:                     # opens the backend and checks every decorated function
        async with cache.session():       # one per request, job or test
            repo = ProductRepo(db)
            await repo.get(1)             # runs the query and stores the result
            await repo.get(1)             # session hit
```

- **No open cache:** cached functions run normally. Dedupe and batching still work, but nothing is stored.
- **Cache open, no session:** the backend is used, without the session layer.
- **To require a session**, call `cache_cache.configure(strict=True)`. Calls outside a session then raise `NoActiveSession`.

## With FastAPI

```python
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI

from myapp.cache import cache


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with cache:
        yield


async def cache_session():
    async with cache.session(max_entries=10_000):
        yield


app = FastAPI(lifespan=lifespan, dependencies=[Depends(cache_session)])
```

More on this in [FastAPI](guide/fastapi.md).

## Next

- [Lifetimes](guide/lifetimes.md): how long a result lives, and where.
- [Tags and invalidation](guide/tags.md).
- [Batching](guide/batching.md).
