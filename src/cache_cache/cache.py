"""The cache, sessions, transactions and the context they live in."""

from __future__ import annotations

import asyncio
import contextvars
import logging
import weakref
from collections import OrderedDict
from collections.abc import Callable, Coroutine, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

from .backends.base import Backend, Entry
from .backends.memory import MemoryBackend
from .errors import CacheClosedError
from .hooks import CacheEvent, Middleware, Observer
from .keys import KeyFormats
from .tags import ancestors_of_all, validate_tag

if TYPE_CHECKING:  # pragma: no cover
    from .testing import Recorder

__all__ = [
    "Cache",
    "Session",
    "Transaction",
    "configure",
    "current_cache",
    "current_session",
    "disabled",
    "invalidate",
]

logger = logging.getLogger("cache_cache")

_session_var: contextvars.ContextVar[Session | None] = contextvars.ContextVar(
    "cache_cache_session", default=None
)
_transaction_var: contextvars.ContextVar[Transaction | None] = contextvars.ContextVar(
    "cache_cache_transaction", default=None
)
_disabled_var: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "cache_cache_disabled", default=False
)
_recorders_var: contextvars.ContextVar[tuple[Recorder, ...]] = contextvars.ContextVar(
    "cache_cache_recorders", default=()
)

_open_caches: list[Cache] = []


@dataclass
class _Config:
    strict: bool = False


_config = _Config()


def configure(*, strict: bool | None = None) -> None:
    """Process-wide settings.

    Args:
        strict: when true, calling a cached function outside a session raises
            :class:`~cache_cache.NoActiveSession` instead of using the backend only.
    """
    if strict is not None:
        _config.strict = strict


def current_session() -> Session | None:
    return _session_var.get()


def current_cache() -> Cache | None:
    """The cache of the current session, else the most recently opened cache."""
    session = _session_var.get()
    if session is not None:
        return session.cache
    return _open_caches[-1] if _open_caches else None


def emit(cache: Cache | None, event: CacheEvent) -> None:
    observers: Iterable[Observer] = cache.observers if cache is not None else ()
    for observer in (*observers, *_recorders_var.get()):
        try:
            observer.on_event(event)
        except Exception:  # pragma: no cover - observers must not break calls
            logger.exception("cache-cache observer %r failed", observer)


@contextmanager
def disabled() -> Iterator[None]:
    """No caching, deduping or batching in this block: every call runs the function."""
    token = _disabled_var.set(True)
    try:
        yield
    finally:
        _disabled_var.reset(token)


async def invalidate(*tags: str) -> None:
    """Invalidate tags (and everything below them) on the current cache.

    Inside :meth:`Cache.transaction`, this is deferred until the block exits.
    Without an open cache, it does nothing.
    """
    cache = current_cache()
    if cache is not None:
        await cache.invalidate(*tags)


class Cache:
    """A long-lived cache, opened once with the application.

    Args:
        backend: where entries live. Defaults to a :class:`MemoryBackend`.
        namespace: prefix for every key and tag, so apps and versions can share a backend.
        ttl: default lifetime, in seconds, for ``@cached`` functions that don't set ``ttl``.
            ``None`` means no expiry.
        observers: receive every :class:`~cache_cache.CacheEvent`.
        middleware: wrap every cached call, outermost first.
        shutdown_timeout: on exit, seconds to wait for background refreshes before cancelling them.
        key_ignore: argument types never part of a key (database sessions, requests...).
        key_formats: how a type appears in a key, e.g. ``{Decimal: lambda d: str(d)}``.
    """

    def __init__(
        self,
        *,
        backend: Backend | None = None,
        namespace: str = "cc",
        ttl: float | None = 3600,
        observers: Sequence[Observer] = (),
        middleware: Sequence[Middleware] = (),
        shutdown_timeout: float = 5.0,
        key_ignore: Sequence[type] = (),
        key_formats: KeyFormats | None = None,
    ) -> None:
        if ":" in namespace or "|" in namespace:
            raise ValueError("namespace can't contain ':' or '|'")
        if ttl is not None and ttl <= 0:
            raise ValueError("ttl must be positive, or None for no expiry")
        self.backend: Backend = backend if backend is not None else MemoryBackend()
        self.namespace = namespace
        self.ttl = ttl
        self.observers: list[Observer] = list(observers)
        self.middleware: list[Middleware] = list(middleware)
        self.shutdown_timeout = shutdown_timeout
        self.key_ignore: tuple[type, ...] = tuple(key_ignore)
        self.key_formats: dict[type, Callable[[Any], Any]] = dict(key_formats or {})
        self._open = False
        self._sessions: weakref.WeakSet[Session] = weakref.WeakSet()
        self._background: set[asyncio.Task[Any]] = set()
        self._lookups = _LookupBatcher(self)

    # -- lifecycle -----------------------------------------------------------------

    @property
    def is_open(self) -> bool:
        return self._open

    async def open(self) -> None:
        """Open the backend and check every decorated function against this cache."""
        if self._open:
            return
        from .functions import validate_registered

        validate_registered(self)
        await self.backend.open()
        self._open = True
        _open_caches.append(self)

    async def close(self) -> None:
        """Wait for background work (up to ``shutdown_timeout``), then close the backend."""
        if not self._open:
            return
        self._open = False
        if self in _open_caches:
            _open_caches.remove(self)
        pending = [task for task in self._background if not task.done()]
        if pending:
            _done, still_running = await asyncio.wait(pending, timeout=self.shutdown_timeout)
            for task in still_running:
                task.cancel()
            if still_running:
                await asyncio.gather(*still_running, return_exceptions=True)
        await self.backend.close()

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    def session(self, *, max_entries: int | None = None) -> Session:
        """A short-lived layer for one unit of work: a request, a job, a test."""
        return Session(self, max_entries=max_entries)

    def transaction(self) -> Transaction:
        """Collect invalidations in this block and apply them when it exits."""
        return Transaction(self)

    # -- invalidation --------------------------------------------------------------

    async def invalidate(self, *tags: str) -> None:
        """Invalidate tags and everything below them. Deferred inside a transaction."""
        for tag in tags:
            validate_tag(tag)
        transaction = _transaction_var.get()
        if transaction is not None and transaction.cache is self:
            transaction.add(tags)
            return
        await self._invalidate_now(tags)

    async def _invalidate_now(self, tags: Sequence[str]) -> None:
        if not tags:
            return
        unique = list(dict.fromkeys(tags))
        await self.backend.bump_tags([self.tag_name(tag) for tag in unique])
        session = _session_var.get()
        if session is not None:
            session.invalidated.update(unique)
        for open_session in list(self._sessions):
            open_session.drop_tags(unique)
        emit(self, CacheEvent("invalidate", tags=tuple(unique)))

    # -- internals -----------------------------------------------------------------

    def tag_name(self, tag: str) -> str:
        """The tag as stored in the backend: namespaced."""
        return f"{self.namespace}|{tag}"

    def spawn_background(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        """Run work that outlives the caller (stale-while-revalidate refreshes).

        It runs outside any session or transaction: it can't write into a request
        that has finished.
        """
        context = contextvars.copy_context()
        context.run(_session_var.set, None)
        context.run(_transaction_var.set, None)
        task = asyncio.get_running_loop().create_task(coro, context=context)
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        task.add_done_callback(_consume_exception)
        return task

    async def lookup(
        self, key: str, tp: Any, tags: Sequence[str]
    ) -> tuple[Entry | None, dict[str, int] | None]:
        """Fetch an entry and the current versions of ``tags`` (namespaced ancestors).

        Lookups made in the same event-loop iteration share one backend round trip.
        """
        return await self._lookups.lookup(key, tp, tags)

    def __repr__(self) -> str:
        state = "open" if self._open else "closed"
        return f"<Cache {self.namespace!r} {type(self.backend).__name__} {state}>"


def _consume_exception(task: asyncio.Task[Any]) -> None:
    if not task.cancelled():
        task.exception()


class Session:
    """Per-unit-of-work layer, checked before the backend.

    It holds results until it closes, as a snapshot: invalidations made in this
    process drop its entries, but invalidations made by other processes don't.
    """

    def __init__(self, cache: Cache, *, max_entries: int | None = None) -> None:
        self.cache = cache
        self.max_entries = max_entries
        self.invalidated: set[str] = set()
        self._entries: OrderedDict[str, tuple[Any, frozenset[str]]] = OrderedDict()
        self._token: contextvars.Token[Session | None] | None = None

    def get(self, key: str) -> tuple[bool, Any]:
        item = self._entries.get(key)
        if item is None:
            return False, None
        self._entries.move_to_end(key)
        return True, item[0]

    def put(self, key: str, value: Any, tags: frozenset[str]) -> None:
        self._entries[key] = (value, tags)
        self._entries.move_to_end(key)
        if self.max_entries is not None:
            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)

    def drop(self, key: str) -> None:
        self._entries.pop(key, None)

    def drop_tags(self, tags: Iterable[str]) -> None:
        targets = set(tags)
        for key in [k for k, (_v, ancestors) in self._entries.items() if ancestors & targets]:
            del self._entries[key]

    def __len__(self) -> int:
        return len(self._entries)

    async def __aenter__(self) -> Self:
        if not self.cache.is_open:
            raise CacheClosedError(
                "open the cache first (`async with cache:` or `await cache.open()`)"
            )
        self._token = _session_var.set(self)
        self.cache._sessions.add(self)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._token is not None:
            _session_var.reset(self._token)
            self._token = None
        self.cache._sessions.discard(self)
        self._entries.clear()


class Transaction:
    """Defers invalidations until the block exits, typically after a database commit.

    While the block runs, entries carrying a pending tag are neither served nor
    stored, so the code reads its own writes. Nested transactions join the
    outermost one. Invalidations are applied even if the block raises: an extra
    invalidation is harmless, a missing one serves stale data.
    """

    def __init__(self, cache: Cache) -> None:
        self.cache = cache
        self.pending: list[str] = []
        self._pending_set: set[str] = set()
        self._token: contextvars.Token[Transaction | None] | None = None
        self._outer: Transaction | None = None

    def add(self, tags: Iterable[str]) -> None:
        target = self._outer or self
        for tag in tags:
            if tag not in target._pending_set:
                target._pending_set.add(tag)
                target.pending.append(tag)

    def affects(self, tag_ancestors: frozenset[str]) -> bool:
        target = self._outer or self
        return bool(target._pending_set & tag_ancestors)

    async def __aenter__(self) -> Self:
        if not self.cache.is_open:
            raise CacheClosedError("open the cache before starting a transaction")
        outer = _transaction_var.get()
        if outer is not None and outer.cache is self.cache:
            self._outer = outer._outer or outer
        self._token = _transaction_var.set(self)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._token is not None:
            _transaction_var.reset(self._token)
            self._token = None
        if self._outer is None:
            tags, self.pending = self.pending, []
            self._pending_set.clear()
            await self.cache._invalidate_now(tags)


@dataclass
class _PendingLookup:
    key: str
    type: Any
    tags: Sequence[str]
    future: asyncio.Future[tuple[Entry | None, dict[str, int] | None]]


@dataclass
class _LookupBatcher:
    cache: Cache
    pending: list[_PendingLookup] = field(default_factory=list)

    def lookup(
        self, key: str, tp: Any, tags: Sequence[str]
    ) -> asyncio.Future[tuple[Entry | None, dict[str, int] | None]]:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[tuple[Entry | None, dict[str, int] | None]] = loop.create_future()
        if not self.pending:
            loop.call_soon(self._flush)
        self.pending.append(_PendingLookup(key, tp, tags, future))
        return future

    def _flush(self) -> None:
        items, self.pending = self.pending, []
        task = asyncio.get_running_loop().create_task(self._run(items))
        self.cache._background.add(task)
        task.add_done_callback(self.cache._background.discard)

    async def _run(self, items: list[_PendingLookup]) -> None:
        keys: dict[str, Any] = {}
        for item in items:
            keys.setdefault(item.key, item.type)
        tags = list(dict.fromkeys(tag for item in items for tag in item.tags))
        backend = self.cache.backend
        entries_by_key: dict[str, Entry | None]
        versions: dict[str, int] | None
        try:
            found, found_versions = await asyncio.gather(
                backend.get_entries(list(keys), list(keys.values())),
                backend.get_tag_versions(tags),
            )
            entries_by_key = dict(zip(keys, found, strict=True))
            versions = dict(zip(tags, found_versions, strict=True))
        except Exception as exc:
            # The backend is down: behave as a miss, and don't store anything,
            # since we couldn't read the tag versions.
            logger.warning("cache-cache backend lookup failed: %r", exc)
            emit(self.cache, CacheEvent("error", error=exc))
            entries_by_key, versions = {}, None
        for item in items:
            if item.future.done():
                continue
            item_versions = None if versions is None else {tag: versions[tag] for tag in item.tags}
            item.future.set_result((entries_by_key.get(item.key), item_versions))


def namespaced_ancestors(cache: Cache, tags: Iterable[str]) -> tuple[frozenset[str], list[str]]:
    """Ancestors of ``tags`` (plain, for sessions/transactions) and namespaced (for the backend)."""
    plain = ancestors_of_all(tags)
    return frozenset(plain), [cache.tag_name(tag) for tag in plain]


def versions_match(entry: Entry, current: Mapping[str, int]) -> list[str]:
    """Namespaced tags whose version changed since the entry was computed."""
    changed = [tag for tag, version in entry.tag_versions.items() if current.get(tag) != version]
    changed.extend(tag for tag in current if tag not in entry.tag_versions)
    return changed
