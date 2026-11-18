"""In-process backends."""

from __future__ import annotations

import threading
from collections import OrderedDict
from collections.abc import Sequence
from typing import Any

from .. import _clock
from .base import Entry, StoreItem

__all__ = ["MemoryBackend", "SyncMemoryBackend"]


class SyncMemoryBackend:
    """Keeps entries in a dict in this process. Thread-safe. For :class:`~cache_cache.SyncCache`.

    Values are stored as-is, not copied: callers share the same object. Treat
    cached values as read-only, or use frozen dataclasses / models.

    Args:
        max_entries: evict the least recently used entries above this size.
    """

    def __init__(self, *, max_entries: int | None = None) -> None:
        self.max_entries = max_entries
        self._entries: OrderedDict[str, tuple[Entry, float | None]] = OrderedDict()
        self._tags: dict[str, int] = {}
        self._scopes: dict[str, float] = {}
        self._lock = threading.Lock()

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def get_entries(self, keys: Sequence[str], types: Sequence[Any]) -> list[Entry | None]:
        now = _clock.now()
        found: list[Entry | None] = []
        with self._lock:
            for key in keys:
                item = self._entries.get(key)
                if item is None:
                    found.append(None)
                    continue
                entry, expires_at = item
                if expires_at is not None and now >= expires_at:
                    del self._entries[key]
                    found.append(None)
                    continue
                self._entries.move_to_end(key)
                found.append(entry)
        return found

    def set_entries(self, items: Sequence[StoreItem]) -> None:
        now = _clock.now()
        with self._lock:
            for item in items:
                expires_at = None if item.ttl is None else now + item.ttl
                self._entries[item.key] = (item.entry, expires_at)
                self._entries.move_to_end(item.key)
            if self.max_entries is not None:
                while len(self._entries) > self.max_entries:
                    self._entries.popitem(last=False)

    def delete(self, keys: Sequence[str]) -> None:
        with self._lock:
            for key in keys:
                self._entries.pop(key, None)

    def get_tag_versions(self, tags: Sequence[str]) -> list[int]:
        with self._lock:
            return [self._tags.get(tag, 0) for tag in tags]

    def bump_tags(self, tags: Sequence[str]) -> None:
        with self._lock:
            for tag in tags:
                self._tags[tag] = self._tags.get(tag, 0) + 1

    def check_type(self, tp: Any) -> None:
        return None

    def heartbeat(self, scope: str) -> None:
        with self._lock:
            self._scopes[scope] = _clock.now()

    def retired_scopes(self, idle_for: float) -> list[str]:
        limit = _clock.now() - idle_for
        with self._lock:
            return [scope for scope, seen in self._scopes.items() if seen <= limit]

    def drop_scope(self, scope: str, idle_for: float) -> int:
        prefix = f"{scope}:"
        with self._lock:
            seen = self._scopes.get(scope)
            if seen is not None and seen > _clock.now() - idle_for:
                return 0
            doomed = [key for key in self._entries if key.startswith(prefix)]
            for key in doomed:
                del self._entries[key]
            self._scopes.pop(scope, None)
            return len(doomed)

    def __len__(self) -> int:
        return len(self._entries)


class MemoryBackend:
    """Keeps entries in a dict in this process. For the async :class:`~cache_cache.Cache`.

    Values are stored as-is, not copied: callers share the same object. Treat
    cached values as read-only, or use frozen dataclasses / models.

    Args:
        max_entries: evict the least recently used entries above this size.
    """

    def __init__(self, *, max_entries: int | None = None) -> None:
        self._store = SyncMemoryBackend(max_entries=max_entries)

    @property
    def max_entries(self) -> int | None:
        return self._store.max_entries

    @property
    def _entries(self) -> OrderedDict[str, tuple[Entry, float | None]]:
        return self._store._entries

    async def open(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def get_entries(self, keys: Sequence[str], types: Sequence[Any]) -> list[Entry | None]:
        return self._store.get_entries(keys, types)

    async def set_entries(self, items: Sequence[StoreItem]) -> None:
        self._store.set_entries(items)

    async def delete(self, keys: Sequence[str]) -> None:
        self._store.delete(keys)

    async def get_tag_versions(self, tags: Sequence[str]) -> list[int]:
        return self._store.get_tag_versions(tags)

    async def bump_tags(self, tags: Sequence[str]) -> None:
        self._store.bump_tags(tags)

    def check_type(self, tp: Any) -> None:
        return None

    async def heartbeat(self, scope: str) -> None:
        self._store.heartbeat(scope)

    async def retired_scopes(self, idle_for: float) -> list[str]:
        return self._store.retired_scopes(idle_for)

    async def drop_scope(self, scope: str, idle_for: float) -> int:
        return self._store.drop_scope(scope, idle_for)

    def __len__(self) -> int:
        return len(self._store)
