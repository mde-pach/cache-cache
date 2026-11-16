# cache-cache

[![CI](https://github.com/mde-pach/cache-cache/actions/workflows/ci.yml/badge.svg)](https://github.com/mde-pach/cache-cache/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/docs-mkdocs-blue)](https://mde-pach.github.io/cache-cache/)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

Caching for `async` Python. It covers:

- deduping concurrent calls,
- a per-request session layer,
- a shared backend (in memory or Redis),
- batching your callers don't see,
- hierarchical tag invalidation.

Mistakes in tags, keys and signatures are caught at startup or in CI, not in production.

```bash
# Not on PyPI yet: install from GitHub
pip install "cache-cache @ git+https://github.com/mde-pach/cache-cache"
pip install "cache-cache[redis,pydantic] @ git+https://github.com/mde-pach/cache-cache"
```

```python
import asyncio
from collections.abc import Mapping, Sequence

from cache_cache import Cache, cached, invalidates

cache = Cache(namespace="shop")


class ProductRepo:
    @cached(tags=["product:{pid}"])
    async def get(self, pid: int) -> Product | None: ...

    @get.batched(on="pid", max_size=100)          # concurrent get() calls -> one query
    async def get_many(self, pids: Sequence[int]) -> Mapping[int, Product]: ...

    @cached(ttl=600, tags=["product:list:{category}"])
    async def by_category(self, category: str) -> list[Product]: ...

    @invalidates("product:{pid}", "product:list")  # evicts get(pid) and every list
    async def update(self, pid: int, data: ProductUpdate) -> Product: ...


async def main() -> None:
    repo = ProductRepo()
    async with cache:                              # once, at startup
        async with cache.session():                # once per request, job or test
            await asyncio.gather(*(repo.get(i) for i in range(50)))   # 1 bulk call
            async with cache.transaction():        # invalidate after the commit
                await repo.update(1, data)
```

## Highlights

- **Tag versioning.** An entry computed from data that was invalidated while it ran is never served. Invalidating `product` is a single counter increment.
- **Deterministic keys** across processes, plus a fingerprint of each function's code and return type, so changing the code invalidates its entries.
- **Stale-while-revalidate** and **stale-if-error**, per function.
- **Observers and middleware**, with built-in stats and logging observers.
- **Testing:** `cache_cache.record()` counts calls, hits and batch sizes. `cache-cache check myapp` finds mistyped tags.

Documentation: **https://mde-pach.github.io/cache-cache/**

## Status

Alpha (0.1). Async functions only. Dedupe and batching work per process.

## Development

```bash
uv sync --all-groups
uv run pytest
uv run --group docs mkdocs serve
```

MIT licensed.
