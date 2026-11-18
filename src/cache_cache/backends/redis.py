"""Redis backends: :class:`RedisBackend` (``redis.asyncio``) and :class:`SyncRedisBackend`."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from ..codecs import Codec, PickleCodec
from .base import Entry, StoreItem

if TYPE_CHECKING:  # pragma: no cover
    import redis
    import redis.asyncio

__all__ = ["RedisBackend", "SyncRedisBackend"]

_SEPARATOR = b"\n"
_GC_LOCK_SECONDS = 300
_UNLINK_BATCH = 500


def _glob_escape(text: str) -> str:
    return "".join(f"\\{char}" if char in "*?[]\\" else char for char in text)


class _RedisFormat:
    """Key layout and entry encoding, shared by both Redis backends."""

    def __init__(self, codec: Codec | None, prefix: str) -> None:
        self.codec: Codec = codec or PickleCodec()
        self.prefix = prefix

    def _entry_key(self, key: str) -> str:
        return f"{self.prefix}e:{key}"

    def _tag_key(self, tag: str) -> str:
        return f"{self.prefix}t:{tag}"

    @property
    def _registry_key(self) -> str:
        return f"{self.prefix}deployments"

    def _gc_lock_key(self, scope: str) -> str:
        return f"{self.prefix}gc:{scope}"

    def _scope_pattern(self, scope: str) -> str:
        return f"{_glob_escape(self.prefix)}e:{_glob_escape(scope)}:*"

    @staticmethod
    def _seconds(time_reply: tuple[int, int]) -> float:
        seconds, micros = time_reply
        return int(seconds) + int(micros) / 1_000_000

    @staticmethod
    def _text(value: Any) -> str:
        return value.decode() if isinstance(value, bytes) else str(value)

    def _decode(self, data: bytes | str | None, tp: Any) -> Entry | None:
        if data is None:
            return None
        if isinstance(data, str):
            data = data.encode()
        try:
            header, _, payload = data.partition(_SEPARATOR)
            meta = json.loads(header)
            value = self.codec.decode(payload, tp)
        except Exception:
            return None
        return Entry(
            value=value, stored_at=meta["s"], fresh_until=meta["f"], tag_versions=meta["t"]
        )

    def _encode(self, item: StoreItem) -> bytes:
        entry = item.entry
        header = json.dumps(
            {"s": entry.stored_at, "f": entry.fresh_until, "t": entry.tag_versions},
            separators=(",", ":"),
        ).encode()
        return header + _SEPARATOR + self.codec.encode(entry.value, item.type)

    @staticmethod
    def _px(ttl: float | None) -> int | None:
        return None if ttl is None else max(1, int(ttl * 1000))

    def check_type(self, tp: Any) -> None:
        self.codec.check(tp)


def _check_client_or_url(client: object, url: str | None) -> None:
    if (client is None) == (url is None):
        raise ValueError("pass exactly one of client= or url=")


class RedisBackend(_RedisFormat):
    """Stores entries in Redis with their expiry, and tag versions as counters.

    Args:
        client: a ``redis.asyncio.Redis`` client. Its lifetime is yours unless
            ``url`` is used instead.
        url: create (and close) a client from this URL.
        codec: how values become bytes. Defaults to :class:`~cache_cache.PickleCodec`.
        prefix: prepended to every Redis key.
    """

    def __init__(
        self,
        client: redis.asyncio.Redis | None = None,
        *,
        url: str | None = None,
        codec: Codec | None = None,
        prefix: str = "cc:",
    ) -> None:
        _check_client_or_url(client, url)
        super().__init__(codec, prefix)
        self._client = client
        self._url = url
        self._owns_client = client is None

    @property
    def client(self) -> redis.asyncio.Redis:
        if self._client is None:
            raise RuntimeError("RedisBackend is not open")
        return self._client

    async def open(self) -> None:
        if self._client is None:
            from redis.asyncio import Redis

            assert self._url is not None
            self._client = Redis.from_url(self._url)

    async def close(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def get_entries(self, keys: Sequence[str], types: Sequence[Any]) -> list[Entry | None]:
        if not keys:
            return []
        raw = await self.client.mget([self._entry_key(k) for k in keys])
        return [self._decode(data, tp) for data, tp in zip(raw, types, strict=True)]

    async def set_entries(self, items: Sequence[StoreItem]) -> None:
        if not items:
            return
        async with self.client.pipeline(transaction=False) as pipe:
            for item in items:
                pipe.set(self._entry_key(item.key), self._encode(item), px=self._px(item.ttl))
            await pipe.execute()

    async def delete(self, keys: Sequence[str]) -> None:
        if keys:
            await self.client.delete(*[self._entry_key(k) for k in keys])

    async def get_tag_versions(self, tags: Sequence[str]) -> list[int]:
        if not tags:
            return []
        raw = await self.client.mget([self._tag_key(t) for t in tags])
        return [int(v) if v is not None else 0 for v in raw]

    async def bump_tags(self, tags: Sequence[str]) -> None:
        if not tags:
            return
        async with self.client.pipeline(transaction=False) as pipe:
            for tag in tags:
                pipe.incr(self._tag_key(tag))
            await pipe.execute()

    # -- deployment registry (times come from the Redis server, not this host) ---------

    async def _now(self) -> float:
        return self._seconds(await self.client.time())

    async def heartbeat(self, scope: str) -> None:
        await self.client.zadd(self._registry_key, {scope: await self._now()})

    async def retired_scopes(self, idle_for: float) -> list[str]:
        limit = await self._now() - idle_for
        found = await self.client.zrangebyscore(self._registry_key, "-inf", limit)
        return [self._text(scope) for scope in found]

    async def drop_scope(self, scope: str, idle_for: float) -> int:
        lock = self._gc_lock_key(scope)
        if not await self.client.set(lock, b"1", nx=True, ex=_GC_LOCK_SECONDS):
            return 0  # another process is dropping it
        try:
            seen = await self.client.zscore(self._registry_key, scope)
            if seen is not None and seen > await self._now() - idle_for:
                return 0  # a process of this deployment came back
            deleted = 0
            batch: list[bytes | str] = []
            async for key in self.client.scan_iter(match=self._scope_pattern(scope), count=1000):
                batch.append(key)
                if len(batch) >= _UNLINK_BATCH:
                    deleted += await self.client.unlink(*batch)
                    batch = []
            if batch:
                deleted += await self.client.unlink(*batch)
            await self.client.zrem(self._registry_key, scope)
            return deleted
        finally:
            await self.client.delete(lock)


class SyncRedisBackend(_RedisFormat):
    """:class:`RedisBackend` for :class:`~cache_cache.SyncCache`, on a ``redis.Redis`` client.

    It uses the same layout, so sync and async processes can share entries.
    """

    def __init__(
        self,
        client: redis.Redis | None = None,
        *,
        url: str | None = None,
        codec: Codec | None = None,
        prefix: str = "cc:",
    ) -> None:
        _check_client_or_url(client, url)
        super().__init__(codec, prefix)
        self._client = client
        self._url = url
        self._owns_client = client is None

    @property
    def client(self) -> redis.Redis:
        if self._client is None:
            raise RuntimeError("SyncRedisBackend is not open")
        return self._client

    def open(self) -> None:
        if self._client is None:
            from redis import Redis

            assert self._url is not None
            self._client = Redis.from_url(self._url)

    def close(self) -> None:
        if self._owns_client and self._client is not None:
            self._client.close()
            self._client = None

    def get_entries(self, keys: Sequence[str], types: Sequence[Any]) -> list[Entry | None]:
        if not keys:
            return []
        raw = self.client.mget([self._entry_key(k) for k in keys])
        return [self._decode(data, tp) for data, tp in zip(raw, types, strict=True)]

    def set_entries(self, items: Sequence[StoreItem]) -> None:
        if not items:
            return
        with self.client.pipeline(transaction=False) as pipe:
            for item in items:
                pipe.set(self._entry_key(item.key), self._encode(item), px=self._px(item.ttl))
            pipe.execute()

    def delete(self, keys: Sequence[str]) -> None:
        if keys:
            self.client.delete(*[self._entry_key(k) for k in keys])

    def get_tag_versions(self, tags: Sequence[str]) -> list[int]:
        if not tags:
            return []
        raw = self.client.mget([self._tag_key(t) for t in tags])
        return [int(v) if v is not None else 0 for v in raw]

    def bump_tags(self, tags: Sequence[str]) -> None:
        if not tags:
            return
        with self.client.pipeline(transaction=False) as pipe:
            for tag in tags:
                pipe.incr(self._tag_key(tag))
            pipe.execute()

    # -- deployment registry (times come from the Redis server, not this host) ---------

    def _now(self) -> float:
        return self._seconds(self.client.time())

    def heartbeat(self, scope: str) -> None:
        self.client.zadd(self._registry_key, {scope: self._now()})

    def retired_scopes(self, idle_for: float) -> list[str]:
        found = self.client.zrangebyscore(self._registry_key, "-inf", self._now() - idle_for)
        return [self._text(scope) for scope in found]

    def drop_scope(self, scope: str, idle_for: float) -> int:
        lock = self._gc_lock_key(scope)
        if not self.client.set(lock, b"1", nx=True, ex=_GC_LOCK_SECONDS):
            return 0
        try:
            seen = self.client.zscore(self._registry_key, scope)
            if seen is not None and seen > self._now() - idle_for:
                return 0
            deleted = 0
            batch: list[bytes | str] = []
            for key in self.client.scan_iter(match=self._scope_pattern(scope), count=1000):
                batch.append(key)
                if len(batch) >= _UNLINK_BATCH:
                    deleted += self.client.unlink(*batch)
                    batch = []
            if batch:
                deleted += self.client.unlink(*batch)
            self.client.zrem(self._registry_key, scope)
            return deleted
        finally:
            self.client.delete(lock)
