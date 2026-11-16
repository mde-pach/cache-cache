"""The backend protocol."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

__all__ = ["Backend", "Entry", "StoreItem", "SyncBackend"]


@dataclass(slots=True)
class Entry:
    """A stored result.

    ``tag_versions`` holds the version of every tag (and tag ancestor) observed
    *before* the function ran. If any of them has changed since, the entry was
    computed from data that has since been invalidated, so it is stale.
    """

    value: Any
    stored_at: float
    fresh_until: float | None
    tag_versions: dict[str, int] = field(default_factory=dict)


@dataclass(slots=True)
class StoreItem:
    key: str
    entry: Entry
    ttl: float | None
    """Seconds the backend keeps the entry (freshness plus any stale window). ``None`` keeps it."""
    type: Any
    """The return annotation, for backends that encode values."""


@runtime_checkable
class Backend(Protocol):
    """Storage for entries and tag versions.

    Every method takes batches: the engine merges the lookups of one event-loop
    iteration into one ``get_entries`` and one ``get_tag_versions`` call.
    """

    async def open(self) -> None: ...

    async def close(self) -> None: ...

    async def get_entries(self, keys: Sequence[str], types: Sequence[Any]) -> list[Entry | None]:
        """Entries for ``keys``, in order; ``None`` where missing or undecodable."""
        ...

    async def set_entries(self, items: Sequence[StoreItem]) -> None: ...

    async def delete(self, keys: Sequence[str]) -> None: ...

    async def get_tag_versions(self, tags: Sequence[str]) -> list[int]:
        """Current versions, in order. A tag that was never bumped has version 0."""
        ...

    async def bump_tags(self, tags: Sequence[str]) -> None:
        """Increment each tag's version. Must be atomic per tag."""
        ...

    def check_type(self, tp: Any) -> None:
        """Raise :class:`~cache_cache.ConfigurationError` if ``tp`` can't be stored."""
        ...


@runtime_checkable
class SyncBackend(Protocol):
    """:class:`Backend` for :class:`~cache_cache.SyncCache`: the same methods, not async.

    Methods may be called from several threads at once.
    """

    def open(self) -> None: ...

    def close(self) -> None: ...

    def get_entries(self, keys: Sequence[str], types: Sequence[Any]) -> list[Entry | None]: ...

    def set_entries(self, items: Sequence[StoreItem]) -> None: ...

    def delete(self, keys: Sequence[str]) -> None: ...

    def get_tag_versions(self, tags: Sequence[str]) -> list[int]: ...

    def bump_tags(self, tags: Sequence[str]) -> None: ...

    def check_type(self, tp: Any) -> None: ...
