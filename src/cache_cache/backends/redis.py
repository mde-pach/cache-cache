"""Redis backend, built on ``redis.asyncio``."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from ..codecs import Codec, PickleCodec
from .base import Entry, StoreItem

if TYPE_CHECKING:  # pragma: no cover
    from redis.asyncio import Redis

__all__ = ["RedisBackend"]

_SEPARATOR = b"\n"


class RedisBackend:
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
        client: Redis | None = None,
        *,
        url: str | None = None,
        codec: Codec | None = None,
        prefix: str = "cc:",
    ) -> None:
        if (client is None) == (url is None):
            raise ValueError("pass exactly one of client= or url=")
        self._client = client
        self._url = url
        self._owns_client = client is None
        self.codec: Codec = codec or PickleCodec()
        self.prefix = prefix

    @property
    def client(self) -> Redis:
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

    def _entry_key(self, key: str) -> str:
        return f"{self.prefix}e:{key}"

    def _tag_key(self, tag: str) -> str:
        return f"{self.prefix}t:{tag}"

    async def get_entries(self, keys: Sequence[str], types: Sequence[Any]) -> list[Entry | None]:
        if not keys:
            return []
        raw = await self.client.mget([self._entry_key(k) for k in keys])
        return [self._decode(data, tp) for data, tp in zip(raw, types, strict=True)]

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

    async def set_entries(self, items: Sequence[StoreItem]) -> None:
        if not items:
            return
        async with self.client.pipeline(transaction=False) as pipe:
            for item in items:
                px = None if item.ttl is None else max(1, int(item.ttl * 1000))
                pipe.set(self._entry_key(item.key), self._encode(item), px=px)
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

    def check_type(self, tp: Any) -> None:
        self.codec.check(tp)
