# FastAPI

Plain FastAPI, no plugin. Open the cache in `lifespan`, and open one session per request in a dependency.

```python
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI

from myapp.cache import cache
from myapp.repo import ProductRepo


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with cache:                     # opens the backend, checks every decorated function
        yield


async def cache_session():
    async with cache.session(max_entries=10_000):   # one bounded session per request
        yield


app = FastAPI(lifespan=lifespan, dependencies=[Depends(cache_session)])


def get_repo() -> ProductRepo:
    return ProductRepo()


@app.get("/products/{pid}")
async def read_product(pid: int, repo: ProductRepo = Depends(get_repo)):
    return await repo.get(pid)
```

## Writes

```python
@app.put("/products/{pid}")
async def update_product(pid: int, data: ProductUpdate, repo: ProductRepo = Depends(get_repo)):
    async with cache.transaction():          # invalidate after the commit
        async with db.begin():
            product = await repo.update(pid, data)
    return product
```

## Notes

- **Background tasks.** `BackgroundTasks` run after the response, when the request's session has closed. Calls made there use the backend without a session layer. Under `configure(strict=True)`, open a session inside the task.
- **Database sessions.** If a dependency passes an `AsyncSession` or the `Request` into cached functions, put those types in `Cache(key_ignore=[...])`. It's simpler to keep them on the repository instance.
