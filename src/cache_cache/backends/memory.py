"""In-process backend."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Sequence
from typing import Any

from .. import _clock
from .base import Entry, StoreItem

__all__ = ["MemoryBackend"]


class MemoryBackend:
    """Keeps entries in a dict in this process.

    Values are stored as-is, not copied: callers share the same object. Treat
    cached values as read-only, or use frozen dataclasses / models.

    Args:
        max_entries: evict the least recently used entries above this size.
    """

    def __init__(self, *, max_entries: int | None = None) -> None:
        self.max_entries = max_entries
        self._entries: OrderedDict[str, tuple[Entry, float | None]] = OrderedDict()
        self._tags: dict[str, int] = {}

    async def open(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def get_entries(self, keys: Sequence[str], types: Sequence[Any]) -> list[Entry | None]:
        now = _clock.now()
        found: list[Entry | None] = []
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

    async def set_entries(self, items: Sequence[StoreItem]) -> None:
        now = _clock.now()
        for item in items:
            expires_at = None if item.ttl is None else now + item.ttl
            self._entries[item.key] = (item.entry, expires_at)
            self._entries.move_to_end(item.key)
        if self.max_entries is not None:
            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)

    async def delete(self, keys: Sequence[str]) -> None:
        for key in keys:
            self._entries.pop(key, None)

    async def get_tag_versions(self, tags: Sequence[str]) -> list[int]:
        return [self._tags.get(tag, 0) for tag in tags]

    async def bump_tags(self, tags: Sequence[str]) -> None:
        for tag in tags:
            self._tags[tag] = self._tags.get(tag, 0) + 1

    def check_type(self, tp: Any) -> None:
        return None

    def __len__(self) -> int:
        return len(self._entries)
