"""Helpers for tests: record what the cache did during a block."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from .cache import _recorders_var
from .functions import BatchedFunction, BoundCachedFunction, CachedFunction
from .hooks import CacheEvent, EventKind

__all__ = ["Recorder", "record"]

FunctionRef = (
    CachedFunction[Any, Any] | BoundCachedFunction[Any, Any] | BatchedFunction[Any, Any, Any]
)


def _resolve(function: FunctionRef) -> object:
    if isinstance(function, BoundCachedFunction):
        return function._function
    if isinstance(function, BatchedFunction):
        return function._parent
    return function


class Recorder:
    """Every event emitted while recording, plus counters per function."""

    def __init__(self) -> None:
        self.events: list[CacheEvent] = []

    def on_event(self, event: CacheEvent) -> None:
        self.events.append(event)

    def _of(self, function: FunctionRef, kind: EventKind) -> list[CacheEvent]:
        target = _resolve(function)
        return [e for e in self.events if e.kind == kind and e.function is target]

    def calls(self, function: FunctionRef) -> int:
        """How many times the function (or its bulk function) actually ran."""
        return len(self._of(function, "call"))

    def hits(self, function: FunctionRef) -> int:
        return len(self._of(function, "hit"))

    def misses(self, function: FunctionRef) -> int:
        return len(self._of(function, "miss"))

    def stale(self, function: FunctionRef) -> int:
        return len(self._of(function, "stale")) + len(self._of(function, "stale_if_error"))

    def shared(self, function: FunctionRef) -> int:
        """Callers that joined a call already in flight."""
        return len(self._of(function, "joined"))

    def batch_sizes(self, function: FunctionRef) -> list[int]:
        return [e.size or 0 for e in self._of(function, "batch")]

    def invalidated(self) -> list[str]:
        return [tag for e in self.events if e.kind == "invalidate" for tag in e.tags]

    def clear(self) -> None:
        self.events.clear()


@contextmanager
def record() -> Iterator[Recorder]:
    """Record events from this context (and tasks started from it)."""
    recorder = Recorder()
    token = _recorders_var.set((*_recorders_var.get(), recorder))
    try:
        yield recorder
    finally:
        _recorders_var.reset(token)
