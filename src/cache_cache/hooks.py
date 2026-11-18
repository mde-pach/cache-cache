"""Observers (read-only) and middleware (can change behaviour)."""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

__all__ = [
    "CacheEvent",
    "CachedCall",
    "DontJoinOldCalls",
    "EventKind",
    "FunctionInfo",
    "FunctionStats",
    "LoggingObserver",
    "Lookup",
    "Middleware",
    "Next",
    "Observer",
    "StatsObserver",
    "SyncCachedCall",
    "SyncDontJoinOldCalls",
    "SyncMiddleware",
    "SyncNext",
]


class FunctionInfo(Protocol):
    """The decorated function, as seen by hooks: a :class:`~cache_cache.CachedFunction`."""

    name: str


LookupState = Literal["miss", "hit", "stale", "in_flight"]
Layer = Literal["session", "backend"]

EventKind = Literal[
    "hit",  # served a fresh value
    "miss",  # nothing usable: the function will run
    "stale",  # served a stale value (stale-while-revalidate)
    "stale_if_error",  # the function failed, a stale value was served instead
    "joined",  # waited for an identical call already in flight
    "call",  # the underlying function (or bulk function) actually ran
    "batch",  # a bulk call was sent; ``size`` is the number of ids
    "store",  # a result was written to the backend
    "bypass",  # caching skipped: condition, transaction or disabled()
    "invalidate",  # tags were invalidated; see ``tags``
    "clear",  # the namespace was cleared
    "retire",  # a retired deployment's entries were deleted; ``tags`` = (scope,), ``size`` = count
    "error",  # the function or the backend raised; see ``error``
]


@dataclass(frozen=True, slots=True)
class Lookup:
    """What the cache knows about a call before the function runs."""

    state: LookupState
    layer: Layer | None = None
    """Where the entry was found."""
    age: float | None = None
    """Seconds since the entry was stored, or since the in-flight call started."""


@dataclass(frozen=True, slots=True)
class CacheEvent:
    kind: EventKind
    function: FunctionInfo | None = None
    """The decorated function. For bulk calls, the single-item function they serve."""
    key: str | None = None
    layer: Layer | None = None
    age: float | None = None
    size: int | None = None
    tags: tuple[str, ...] = ()
    duration: float | None = None
    error: BaseException | None = None

    @property
    def name(self) -> str | None:
        return self.function.name if self.function is not None else None


@runtime_checkable
class Observer(Protocol):
    """Receives every event. Must not block and must not raise (errors are logged)."""

    def on_event(self, event: CacheEvent) -> None: ...


class CachedCall:
    """One call going through the middleware chain."""

    __slots__ = ("_run", "args", "function", "instance", "key", "kwargs", "lookup")

    def __init__(
        self,
        *,
        function: FunctionInfo,
        instance: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        key: str,
        lookup: Lookup,
        run: Callable[[], Awaitable[Any]],
    ) -> None:
        self.function = function
        self.instance = instance
        self.args = args
        self.kwargs = kwargs
        self.key = key
        self.lookup = lookup
        self._run = run

    async def run(self) -> Any:
        """Start a fresh call, ignoring any cached value and any call in flight.

        The result is stored as usual, and later identical calls join this one.
        """
        return await self._run()

    def __repr__(self) -> str:
        return f"CachedCall({self.function.name}, lookup={self.lookup})"


class SyncCachedCall:
    """One call of a sync function going through the sync middleware chain."""

    __slots__ = ("_run", "args", "function", "instance", "key", "kwargs", "lookup")

    def __init__(
        self,
        *,
        function: FunctionInfo,
        instance: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        key: str,
        lookup: Lookup,
        run: Callable[[], Any],
    ) -> None:
        self.function = function
        self.instance = instance
        self.args = args
        self.kwargs = kwargs
        self.key = key
        self.lookup = lookup
        self._run = run

    def run(self) -> Any:
        """Start a fresh call, ignoring any cached value and any call in flight."""
        return self._run()

    def __repr__(self) -> str:
        return f"SyncCachedCall({self.function.name}, lookup={self.lookup})"


Next = Callable[[CachedCall], Awaitable[Any]]
SyncNext = Callable[[SyncCachedCall], Any]


@runtime_checkable
class Middleware(Protocol):
    async def __call__(self, call: CachedCall, call_next: Next) -> Any: ...


@runtime_checkable
class SyncMiddleware(Protocol):
    def __call__(self, call: SyncCachedCall, call_next: SyncNext) -> Any: ...


class DontJoinOldCalls:
    """Start a fresh call instead of joining one that has been running too long."""

    def __init__(self, max_age: float = 2.0) -> None:
        self.max_age = max_age

    async def __call__(self, call: CachedCall, call_next: Next) -> Any:
        lookup = call.lookup
        if lookup.state == "in_flight" and lookup.age is not None and lookup.age > self.max_age:
            return await call.run()
        return await call_next(call)


class SyncDontJoinOldCalls:
    """:class:`DontJoinOldCalls` for :class:`~cache_cache.SyncCache`."""

    def __init__(self, max_age: float = 2.0) -> None:
        self.max_age = max_age

    def __call__(self, call: SyncCachedCall, call_next: SyncNext) -> Any:
        lookup = call.lookup
        if lookup.state == "in_flight" and lookup.age is not None and lookup.age > self.max_age:
            return call.run()
        return call_next(call)


@dataclass
class FunctionStats:
    hits: int = 0
    session_hits: int = 0
    misses: int = 0
    stale: int = 0
    stale_if_error: int = 0
    joined: int = 0
    calls: int = 0
    stores: int = 0
    bypassed: int = 0
    errors: int = 0
    batch_sizes: list[int] = field(default_factory=list)

    @property
    def hit_ratio(self) -> float | None:
        total = self.hits + self.misses + self.stale
        return None if total == 0 else (self.hits + self.stale) / total


class StatsObserver:
    """Counts events per function. Read :attr:`stats`, keyed by function name."""

    def __init__(self) -> None:
        self.stats: defaultdict[str, FunctionStats] = defaultdict(FunctionStats)

    def on_event(self, event: CacheEvent) -> None:
        if event.name is None:
            return
        stats = self.stats[event.name]
        match event.kind:
            case "hit":
                stats.hits += 1
                if event.layer == "session":
                    stats.session_hits += 1
            case "miss":
                stats.misses += 1
            case "stale":
                stats.stale += 1
            case "stale_if_error":
                stats.stale_if_error += 1
            case "joined":
                stats.joined += 1
            case "call":
                stats.calls += 1
            case "store":
                stats.stores += 1
            case "bypass":
                stats.bypassed += 1
            case "error":
                stats.errors += 1
            case "batch":
                stats.batch_sizes.append(event.size or 0)
            case _:
                pass

    def reset(self) -> None:
        self.stats.clear()


class LoggingObserver:
    """Logs every event, at DEBUG by default (errors at WARNING)."""

    def __init__(self, logger: logging.Logger | None = None, level: int = logging.DEBUG) -> None:
        self.logger = logger or logging.getLogger("cache_cache")
        self.level = level

    def on_event(self, event: CacheEvent) -> None:
        level = logging.WARNING if event.kind == "error" else self.level
        if not self.logger.isEnabledFor(level):
            return
        details = []
        if event.layer:
            details.append(f"layer={event.layer}")
        if event.age is not None:
            details.append(f"age={event.age:.3f}s")
        if event.size is not None:
            details.append(f"size={event.size}")
        if event.tags:
            details.append(f"tags={','.join(event.tags)}")
        if event.duration is not None:
            details.append(f"duration={event.duration * 1000:.1f}ms")
        if event.error is not None:
            details.append(f"error={event.error!r}")
        self.logger.log(level, "cache %s %s %s", event.kind, event.name or "-", " ".join(details))
