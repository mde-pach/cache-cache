import asyncio
import logging
import os
import time

import fakeredis
import pytest

import cache_cache
from cache_cache import Cache, MemoryBackend, cached, record
from cache_cache.backends.redis import RedisBackend, SyncRedisBackend
from cache_cache.deployment import DEPLOYMENT_ENV_VARS, resolve_deployment

from .conftest import FakeClock


class Repo:
    def __init__(self) -> None:
        self.calls = 0

    @cached(tags=["product:{pid}"])
    async def get(self, pid: int) -> int:
        self.calls += 1
        return pid


# -- detection --------------------------------------------------------------------------


@pytest.mark.parametrize(("name", "_platform"), DEPLOYMENT_ENV_VARS)
def test_each_platform_variable_is_detected(name: str, _platform: str) -> None:
    first = resolve_deployment("auto", {name: "release-1"})
    second = resolve_deployment("auto", {name: "release-2"})
    assert first.kind == "detected"
    assert first.token and second.token and first.token != second.token
    assert resolve_deployment("auto", {name: "release-1"}) == first


def test_explicit_variable_wins() -> None:
    env = {"CACHE_CACHE_DEPLOYMENT": "mine", "VERCEL_DEPLOYMENT_ID": "dpl_1"}
    assert resolve_deployment("auto", env).label == "CACHE_CACHE_DEPLOYMENT: mine"


def test_fallback_is_this_process_with_a_warning(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, "cache_cache"):
        first = resolve_deployment("auto", {})
    assert first.kind == "process"
    assert "no deployment ID found" in caplog.text
    assert resolve_deployment("auto", {}).token == first.token  # stable in a process
    assert resolve_deployment("process").token == first.token


def test_explicit_and_unscoped() -> None:
    assert resolve_deployment("abc").kind == "explicit"
    assert resolve_deployment(None).token is None
    with pytest.raises(ValueError):
        resolve_deployment("  ")


# -- scoping ----------------------------------------------------------------------------


async def test_a_new_deployment_reads_nothing_from_the_previous_one(backend: MemoryBackend) -> None:
    repo = Repo()
    async with Cache(backend=backend, deployment="A"):
        await repo.get(1)
    async with Cache(backend=backend, deployment="A"):
        await repo.get(1)  # restart within the deployment: kept
    async with Cache(backend=backend, deployment="B"):
        await repo.get(1)  # new deployment, same code: empty
    assert repo.calls == 2


async def test_redeploying_the_same_code_detects_a_new_deployment(
    backend: MemoryBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = Repo()
    monkeypatch.delenv("CACHE_CACHE_DEPLOYMENT")
    monkeypatch.setenv("RAILWAY_DEPLOYMENT_ID", "deploy-1")
    async with Cache(backend=backend):
        await repo.get(1)
    monkeypatch.setenv("RAILWAY_DEPLOYMENT_ID", "deploy-2")
    async with Cache(backend=backend):
        await repo.get(1)
    assert repo.calls == 2


async def test_tags_reach_every_deployment_during_a_rollout(backend: MemoryBackend) -> None:
    repo = Repo()
    async with Cache(backend=backend, deployment="old"):
        await repo.get(1)
    async with Cache(backend=backend, deployment="new"):
        await cache_cache.invalidate("product:1")  # the new code writes
    async with Cache(backend=backend, deployment="old"):
        await repo.get(1)  # an old pod still running doesn't serve the stale value
    assert repo.calls == 2


async def test_unscoped_entries_survive_deployments(backend: MemoryBackend) -> None:
    repo = Repo()
    async with Cache(backend=backend, deployment=None):
        await repo.get(1)
    async with Cache(backend=backend, deployment=None):
        await repo.get(1)
    assert repo.calls == 1


async def test_repr_shows_the_deployment() -> None:
    async with Cache(deployment="A") as cache:
        assert "deployment='A'" in repr(cache)


# -- retiring old deployments -----------------------------------------------------------


async def test_retired_deployment_is_deleted(clock: FakeClock) -> None:
    backend = MemoryBackend()
    repo = Repo()
    async with Cache(backend=backend, deployment="old"):
        for pid in range(5):
            await repo.get(pid)
    assert len(backend) == 5
    async with Cache(backend=backend, deployment="new") as cache:
        await repo.get(1)
        assert len(backend) == 6  # "old" stopped 0s ago: still within retire_after
        clock.advance(601)
        with record() as rec:
            assert await cache.collect_retired() == 5
        assert len(backend) == 1
        assert [(e.kind, e.size) for e in rec.events] == [("retire", 5)]


async def test_a_live_deployment_is_never_deleted(clock: FakeClock) -> None:
    backend = MemoryBackend()
    repo = Repo()
    async with Cache(backend=backend, deployment="old") as old:
        await repo.get(1)
        async with Cache(backend=backend, deployment="new") as new:
            clock.advance(601)
            await old._heartbeat()  # an old pod is still running
            assert await new.collect_retired() == 0
    assert len(backend) == 1


async def test_other_namespaces_are_left_alone(clock: FakeClock) -> None:
    backend = MemoryBackend()
    repo = Repo()
    async with Cache(backend=backend, namespace="other-app", deployment="old"):
        await repo.get(1)
    clock.advance(601)
    async with Cache(backend=backend, deployment="new") as cache:
        assert await cache.collect_retired() == 0
    assert len(backend) == 1


async def test_no_collection_when_unscoped(clock: FakeClock) -> None:
    async with Cache(deployment=None) as cache:
        assert await cache.collect_retired() == 0


async def test_maintenance_loop_collects_in_the_background() -> None:
    backend = MemoryBackend()
    repo = Repo()
    async with Cache(backend=backend, deployment="old", heartbeat_interval=0.02, retire_after=0.05):
        await repo.get(1)
    async with Cache(backend=backend, deployment="new", heartbeat_interval=0.02, retire_after=0.05):
        deadline = time.monotonic() + 2
        while len(backend) and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        assert len(backend) == 0


class Retired:
    """Observer collecting ``retire`` events (they can come from a background thread)."""

    def __init__(self) -> None:
        self.sizes: list[int] = []

    def on_event(self, event: cache_cache.CacheEvent) -> None:
        if event.kind == "retire":
            self.sizes.append(event.size or 0)


def _wait_for(condition, timeout: float = 3.0) -> bool:  # type: ignore[no-untyped-def]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return False


FAST = {"heartbeat_interval": 0.05, "retire_after": 0.1}


async def test_redis_collection_deletes_only_the_retired_scope() -> None:
    server = fakeredis.FakeServer()
    client = fakeredis.FakeAsyncRedis(server=server)
    repo = Repo()
    retired = Retired()

    def backend() -> RedisBackend:
        return RedisBackend(fakeredis.FakeAsyncRedis(server=server))

    async with Cache(backend=backend(), deployment="old", **FAST):
        for pid in range(3):
            await repo.get(pid)
    async with Cache(backend=backend(), deployment="new", observers=[retired], **FAST):
        await repo.get(1)
        await cache_cache.invalidate("product:1")
        deadline = time.monotonic() + 3
        while not retired.sizes and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        assert retired.sizes == [3]
        assert len(await client.keys("cc:e:*")) == 1  # only the new deployment's entry
        assert await client.keys("cc:t:*")  # tag versions are kept
        assert await client.keys("cc:gc:*") == []  # the lock was released
        assert await client.zrange("cc:deployments", 0, -1) != []


def test_sync_redis_collection() -> None:
    from cache_cache import SyncCache

    server = fakeredis.FakeServer()
    client = fakeredis.FakeRedis(server=server)
    retired = Retired()

    @cached
    def f(x: int) -> int:
        return x

    def backend() -> SyncRedisBackend:
        return SyncRedisBackend(fakeredis.FakeRedis(server=server))

    with SyncCache(backend=backend(), deployment="old", **FAST):
        f(1)
        f(2)
    with SyncCache(backend=backend(), deployment="new", observers=[retired], **FAST):
        assert _wait_for(lambda: retired.sizes == [2])
        assert client.keys("cc:e:*") == []


def test_drop_scope_refuses_a_live_scope() -> None:
    from cache_cache import SyncMemoryBackend

    backend = SyncMemoryBackend()
    backend.heartbeat("cc:abc")
    assert backend.drop_scope("cc:abc", idle_for=60) == 0


def test_process_identity_changes_after_fork() -> None:
    if not hasattr(os, "fork"):
        pytest.skip("no fork")
    parent = resolve_deployment("process").token
    read, write = os.pipe()
    pid = os.fork()
    if pid == 0:  # pragma: no cover - child
        os.write(write, (resolve_deployment("process").token or "").encode())
        os._exit(0)
    os.waitpid(pid, 0)
    child = os.read(read, 64).decode()
    assert child and child != parent
