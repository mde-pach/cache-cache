import asyncio
from collections.abc import AsyncIterator, Mapping, Sequence

import fakeredis
import pytest
from pydantic import BaseModel

import cache_cache
from cache_cache import Cache, ConfigurationError, PydanticCodec, cached
from cache_cache.backends.redis import RedisBackend

from .conftest import Product


class Item(BaseModel):
    id: int
    name: str


@pytest.fixture
def server() -> fakeredis.FakeServer:
    return fakeredis.FakeServer()


def make_backend(server: fakeredis.FakeServer, **kwargs: object) -> RedisBackend:
    client = fakeredis.FakeAsyncRedis(server=server)
    return RedisBackend(client, **kwargs)  # type: ignore[arg-type]


@pytest.fixture
async def redis_cache(server: fakeredis.FakeServer) -> AsyncIterator[Cache]:
    async with Cache(backend=make_backend(server)) as cache:
        yield cache


class Repo:
    def __init__(self) -> None:
        self.calls = 0

    @cached(ttl=60, tags=["product:{pid}"])
    async def get(self, pid: int) -> Product:
        self.calls += 1
        return Product(pid, "p")


async def test_entries_are_shared_between_processes(server: fakeredis.FakeServer) -> None:
    """Two caches on the same Redis behave like two processes."""
    repo = Repo()
    async with Cache(backend=make_backend(server)):
        await repo.get(1)
    async with Cache(backend=make_backend(server)):
        assert await repo.get(1) == Product(1, "p")
    assert repo.calls == 1


async def test_invalidation_crosses_processes(server: fakeredis.FakeServer) -> None:
    repo = Repo()
    async with Cache(backend=make_backend(server)):
        await repo.get(1)
    async with Cache(backend=make_backend(server)):
        await cache_cache.invalidate("product")
    async with Cache(backend=make_backend(server)):
        await repo.get(1)
    assert repo.calls == 2


async def test_redis_expiry_is_set(redis_cache: Cache) -> None:
    await Repo().get(1)
    backend = redis_cache.backend
    assert isinstance(backend, RedisBackend)
    keys = await backend.client.keys("cc:e:*")
    assert len(keys) == 1
    ttl = await backend.client.pttl(keys[0])
    assert 0 < ttl <= 60_000


async def test_batching_with_redis(redis_cache: Cache) -> None:
    class Batched:
        def __init__(self) -> None:
            self.sizes: list[int] = []

        @cached
        async def get(self, pid: int) -> int:
            raise AssertionError

        @get.batched(on="pid")
        async def get_many(self, pids: Sequence[int]) -> Mapping[int, int]:
            self.sizes.append(len(pids))
            return {p: p * 10 for p in pids}

    b = Batched()
    assert await asyncio.gather(*(b.get(i) for i in range(30))) == [i * 10 for i in range(30)]
    assert await asyncio.gather(*(b.get(i) for i in range(35))) == [i * 10 for i in range(35)]
    assert b.sizes == [30, 5]


async def test_pydantic_codec_round_trip(server: fakeredis.FakeServer) -> None:
    calls = 0

    @cached
    async def items(n: int) -> list[Item]:
        nonlocal calls
        calls += 1
        return [Item(id=i, name=str(i)) for i in range(n)]

    async with Cache(backend=make_backend(server, codec=PydanticCodec())):
        first = await items(3)
    async with Cache(backend=make_backend(server, codec=PydanticCodec())):
        second = await items(3)
    assert first == second
    assert isinstance(second[0], Item)
    assert calls == 1


async def test_pydantic_codec_rejects_unencodable_return_types(
    server: fakeredis.FakeServer,
) -> None:
    class Opaque:
        pass

    @cached
    async def f() -> Opaque:
        return Opaque()

    with pytest.raises(ConfigurationError, match="PydanticCodec can't encode"):
        await Cache(backend=make_backend(server, codec=PydanticCodec())).open()


async def test_corrupt_entries_are_misses(redis_cache: Cache) -> None:
    repo = Repo()
    await repo.get(1)
    backend = redis_cache.backend
    assert isinstance(backend, RedisBackend)
    for key in await backend.client.keys("cc:e:*"):
        await backend.client.set(key, b"garbage")
    await repo.get(1)
    assert repo.calls == 2


async def test_url_or_client_required() -> None:
    with pytest.raises(ValueError):
        RedisBackend()
    backend = RedisBackend(url="redis://localhost:1")
    with pytest.raises(RuntimeError):
        _ = backend.client
