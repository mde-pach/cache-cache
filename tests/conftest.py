from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass

import pytest

import cache_cache
from cache_cache import Cache, MemoryBackend, _clock
from cache_cache import _base as functions_module
from cache_cache import cache as cache_module


class FakeClock:
    def __init__(self) -> None:
        self.wall = 1_000_000.0
        self.mono = 1_000.0

    def now(self) -> float:
        return self.wall

    def monotonic(self) -> float:
        return self.mono

    def advance(self, seconds: float) -> None:
        self.wall += seconds
        self.mono += seconds


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    fake = FakeClock()
    monkeypatch.setattr(_clock, "now", fake.now)
    monkeypatch.setattr(_clock, "monotonic", fake.monotonic)
    return fake


@pytest.fixture(autouse=True)
def _deployment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests run as one deployment, like processes started by one deploy."""
    monkeypatch.setenv("CACHE_CACHE_DEPLOYMENT", "test-deploy")


@pytest.fixture(autouse=True)
def _isolate_registry() -> Iterator[None]:
    """Functions declared inside a test don't leak into later tests."""
    saved = list(functions_module._registry)
    saved_invalidations = list(functions_module._invalidations)
    yield
    functions_module._registry[:] = saved
    functions_module._invalidations[:] = saved_invalidations
    cache_module._open_caches.clear()
    cache_cache.configure(strict=False)


@pytest.fixture
def backend() -> MemoryBackend:
    return MemoryBackend()


@pytest.fixture
async def cache(backend: MemoryBackend) -> AsyncIterator[Cache]:
    async with Cache(backend=backend, namespace="test") as opened:
        yield opened


@pytest.fixture
async def session(cache: Cache) -> AsyncIterator[cache_cache.Session]:
    async with cache.session() as opened:
        yield opened


@dataclass(frozen=True)
class Product:
    id: int
    name: str
    category: str = "shoes"


async def tick(n: int = 5) -> None:
    for _ in range(n):
        await asyncio.sleep(0)
