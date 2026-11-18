"""The async cache, sessions, transactions and the context they live in."""

from __future__ import annotations

import asyncio
import contextvars
import logging
import re
import threading
import weakref
from collections import OrderedDict
from collections.abc import Callable, Coroutine, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal, Self

from .backends.base import Backend, DeploymentRegistry, Entry
from .backends.memory import MemoryBackend
from .deployment import Deployment, DeploymentSetting, resolve_deployment
from .errors import CacheClosedError
from .hooks import CacheEvent, Middleware, Observer
from .keys import KeyFormats
from .tags import ancestors_of_all, validate_tag

if TYPE_CHECKING:  # pragma: no cover
    from .testing import Recorder

__all__ = [
    "Cache",
    "CacheBase",
    "Session",
    "Transaction",
    "TransactionBase",
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
_transaction_var: contextvars.ContextVar[TransactionBase | None] = contextvars.ContextVar(
    "cache_cache_transaction", default=None
)
_disabled_var: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "cache_cache_disabled", default=False
)
_recorders_var: contextvars.ContextVar[tuple[Recorder, ...]] = contextvars.ContextVar(
    "cache_cache_recorders", default=()
)

_NAMESPACE = re.compile(r"[A-Za-z0-9_.-]+")

_open_caches: list[CacheBase] = []
_open_lock = threading.Lock()


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


def _current(kind: Literal["async", "sync"]) -> CacheBase | None:
    session = _session_var.get()
    if session is not None and session.cache.kind == kind:
        return session.cache
    with _open_lock:
        for cache in reversed(_open_caches):
            if cache.kind == kind:
                return cache
    return None


def current_cache() -> Cache | None:
    """The async cache of the current session, else the most recently opened async cache."""
    cache = _current("async")
    assert cache is None or isinstance(cache, Cache)
    return cache


def emit(cache: CacheBase | None, event: CacheEvent) -> None:
    observers: Iterable[Observer] = cache.observers if cache is not None else ()
    for observer in (*observers, *_recorders_var.get()):
        try:
            observer.on_event(event)
        except Exception:
            logger.exception("cache-cache observer %r failed", observer)


@contextmanager
def disabled() -> Iterator[None]:
    """No caching or deduping in this block: every call runs (batching still applies)."""
    token = _disabled_var.set(True)
    try:
        yield
    finally:
        _disabled_var.reset(token)


async def invalidate(*tags: str) -> None:
    """Invalidate tags (and everything below them) on the current async cache.

    Inside :meth:`Cache.transaction`, this is deferred until the block exits.
    Without an open cache, it does nothing. Sync code uses
    :func:`~cache_cache.invalidate_sync`.
    """
    cache = current_cache()
    if cache is not None:
        await cache.invalidate(*tags)


class CacheBase:
    """Settings and in-process state shared by :class:`Cache` and ``SyncCache``."""

    kind: Literal["async", "sync"]
    backend: Any

    def __init__(
        self,
        *,
        namespace: str,
        deployment: DeploymentSetting,
        retire_after: float,
        heartbeat_interval: float,
        ttl: float | None,
        observers: Sequence[Observer],
        key_ignore: Sequence[type],
        key_formats: KeyFormats | None,
        shutdown_timeout: float,
    ) -> None:
        if not _NAMESPACE.fullmatch(namespace):
            raise ValueError("namespace must be letters, digits, '_', '.' or '-'")
        if ttl is not None and ttl <= 0:
            raise ValueError("ttl must be positive, or None for no expiry")
        if heartbeat_interval <= 0 or retire_after < 2 * heartbeat_interval:
            raise ValueError("retire_after must be at least twice heartbeat_interval")
        self.namespace = namespace
        self.deployment_setting = deployment
        self.retire_after = retire_after
        self.heartbeat_interval = heartbeat_interval
        self.deployment: Deployment | None = None
        self.ttl = ttl
        self.observers: list[Observer] = list(observers)
        self.key_ignore: tuple[type, ...] = tuple(key_ignore)
        self.key_formats: dict[type, Callable[[Any], Any]] = dict(key_formats or {})
        self.shutdown_timeout = shutdown_timeout
        self._open = False
        self._sessions: weakref.WeakSet[Session] = weakref.WeakSet()
        self._sessions_lock = threading.Lock()

    @property
    def is_open(self) -> bool:
        return self._open

    @property
    def generation_tag(self) -> str:
        """A tag every entry depends on: bumping it clears the namespace."""
        return f"{self.namespace}#generation"

    def tag_name(self, tag: str) -> str:
        """The tag as stored in the backend: namespaced, shared by every deployment."""
        return f"{self.namespace}|{tag}"

    def _resolve_deployment(self) -> Deployment:
        """Resolved once, on the first open: a process keeps its scope across reopens."""
        if self.deployment is None:
            self.deployment = resolve_deployment(self.deployment_setting)
        return self.deployment

    @property
    def scope(self) -> str | None:
        """``"<namespace>:<deployment token>"``, the prefix of every entry key, or ``None``."""
        token = self.deployment.token if self.deployment is not None else None
        return None if token is None else f"{self.namespace}:{token}"

    @property
    def key_prefix(self) -> str:
        return self.scope or f"{self.namespace}:-"

    def _retired(self, scopes: Sequence[str]) -> list[str]:
        """Retired scopes this cache may delete: same namespace, not its own."""
        own = self.scope
        mine = f"{self.namespace}:"
        return [scope for scope in scopes if scope != own and scope.startswith(mine)]

    def _registry_enabled(self, registry_type: type) -> bool:
        return self.scope is not None and isinstance(self.backend, registry_type)

    def session(self, *, max_entries: int | None = None) -> Session:
        """A short-lived layer for one unit of work: a request, a job, a test."""
        return Session(self, max_entries=max_entries)

    def _register_open(self) -> None:
        self._open = True
        with _open_lock:
            _open_caches.append(self)

    def _register_closed(self) -> None:
        self._open = False
        with _open_lock:
            if self in _open_caches:
                _open_caches.remove(self)

    def _open_sessions(self) -> list[Session]:
        with self._sessions_lock:
            return list(self._sessions)

    def _after_invalidation(self, tags: Sequence[str]) -> None:
        session = _session_var.get()
        if session is not None and session.cache is self:
            session.invalidated.update(tags)
        for open_session in self._open_sessions():
            open_session.drop_tags(tags)
        emit(self, CacheEvent("invalidate", tags=tuple(tags)))

    def _after_clear(self) -> None:
        for open_session in self._open_sessions():
            open_session.clear()
        emit(self, CacheEvent("clear"))

    def __repr__(self) -> str:
        state = "open" if self._open else "closed"
        deployment = f" deployment={self.deployment.label!r}" if self.deployment else ""
        return (
            f"<{type(self).__name__} {self.namespace!r}{deployment} "
            f"{type(self.backend).__name__} {state}>"
        )


class Cache(CacheBase):
    """A long-lived cache for ``async`` functions, opened once with the application.

    Args:
        backend: where entries live. Defaults to a :class:`MemoryBackend`.
        namespace: prefix for every key and tag, so apps can share a backend.
        deployment: which deployment entries belong to. A new deployment never reads the
            entries of a previous one, even with the same code; tags stay shared.
            ``"auto"`` (default) reads the ID from the environment (see
            :data:`~cache_cache.deployment.DEPLOYMENT_ENV_VARS`), and falls back to this
            process with a warning. ``"process"`` scopes to this process. Any other string
            is the ID itself. ``None`` doesn't scope: entries survive deploys.
        retire_after: delete the entries of a deployment once it has had no live process
            for this many seconds (backends with a deployment registry only).
        heartbeat_interval: seconds between heartbeats of this process's deployment.
        ttl: default lifetime, in seconds, for ``@cached`` functions that don't set ``ttl``.
            ``None`` means no expiry.
        observers: receive every :class:`~cache_cache.CacheEvent`.
        middleware: wrap every cached call, outermost first.
        shutdown_timeout: on exit, seconds to wait for background refreshes before cancelling them.
        key_ignore: argument types never part of a key (database sessions, requests...).
        key_formats: how a type appears in a key, e.g. ``{Decimal: lambda d: str(d)}``.
    """

    kind: Literal["async", "sync"] = "async"

    def __init__(
        self,
        *,
        backend: Backend | None = None,
        namespace: str = "cc",
        deployment: DeploymentSetting = "auto",
        retire_after: float = 600,
        heartbeat_interval: float = 60,
        ttl: float | None = 3600,
        observers: Sequence[Observer] = (),
        middleware: Sequence[Middleware] = (),
        shutdown_timeout: float = 5.0,
        key_ignore: Sequence[type] = (),
        key_formats: KeyFormats | None = None,
    ) -> None:
        super().__init__(
            namespace=namespace,
            deployment=deployment,
            retire_after=retire_after,
            heartbeat_interval=heartbeat_interval,
            ttl=ttl,
            observers=observers,
            key_ignore=key_ignore,
            key_formats=key_formats,
            shutdown_timeout=shutdown_timeout,
        )
        self.backend: Backend = backend if backend is not None else MemoryBackend()
        self.middleware: list[Middleware] = list(middleware)
        self._maintenance: asyncio.Task[None] | None = None
        self._stop_maintenance: asyncio.Event | None = None
        self._background: set[asyncio.Task[Any]] = set()
        self._lookups = _LookupBatcher(self)

    # -- lifecycle -----------------------------------------------------------------

    async def open(self) -> None:
        """Open the backend and check every async decorated function against this cache."""
        if self._open:
            return
        from ._base import validate_registered

        self._resolve_deployment()
        validate_registered(self, "async")
        await self.backend.open()
        self._register_open()
        if self._registry_enabled(DeploymentRegistry):
            await self._heartbeat()
            self._stop_maintenance = asyncio.Event()
            self._maintenance = asyncio.get_running_loop().create_task(
                self._maintain(self._stop_maintenance)
            )

    async def close(self) -> None:
        """Wait for background work (up to ``shutdown_timeout``), then close the backend."""
        if not self._open:
            return
        self._register_closed()
        if self._maintenance is not None and self._stop_maintenance is not None:
            # Let a backend call in progress finish: cancelling it midway can leave a
            # connection in a bad state. Cancel only if it takes too long.
            self._stop_maintenance.set()
            _done, overdue = await asyncio.wait({self._maintenance}, timeout=self.shutdown_timeout)
            for task in overdue:
                task.cancel()
            await asyncio.gather(self._maintenance, return_exceptions=True)
            self._maintenance = None
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

    def transaction(self) -> Transaction:
        """Collect invalidations in this block and apply them when it exits."""
        return Transaction(self)

    # -- deployments ---------------------------------------------------------------

    async def _heartbeat(self) -> None:
        assert self.scope is not None and isinstance(self.backend, DeploymentRegistry)
        try:
            await self.backend.heartbeat(self.scope)
        except Exception as exc:
            logger.warning("cache-cache heartbeat failed: %r", exc)

    async def collect_retired(self) -> int:
        """Delete the entries of deployments with no live process for ``retire_after``.

        Runs on its own after every heartbeat; call it to collect now. Returns the
        number of entries deleted.
        """
        backend = self.backend
        if not self._registry_enabled(DeploymentRegistry):
            return 0
        assert isinstance(backend, DeploymentRegistry)
        deleted = 0
        for scope in self._retired(await backend.retired_scopes(self.retire_after)):
            count = await backend.drop_scope(scope, self.retire_after)
            deleted += count
            emit(self, CacheEvent("retire", tags=(scope,), size=count))
        return deleted

    async def _maintain(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.collect_retired()
            except Exception as exc:
                logger.warning("cache-cache: collecting retired deployments failed: %r", exc)
            try:
                await asyncio.wait_for(stop.wait(), self.heartbeat_interval)
                return
            except TimeoutError:
                pass
            await self._heartbeat()

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
        self._after_invalidation(unique)

    async def clear(self) -> None:
        """Make every entry of this namespace unreadable, in one write.

        Entries aren't deleted: they stop matching and expire with their ttl.
        """
        await self.backend.bump_tags([self.generation_tag])
        self._after_clear()

    # -- internals -----------------------------------------------------------------

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
        """Fetch an entry and the current versions of ``tags`` (namespaced).

        Lookups made in the same event-loop iteration share one backend round trip.
        """
        return await self._lookups.lookup(key, tp, tags)


def _consume_exception(task: asyncio.Task[Any]) -> None:
    if not task.cancelled():
        task.exception()


class Session:
    """Per-unit-of-work layer, checked before the backend.

    It holds results until it closes, as a snapshot: invalidations made in this
    process drop its entries, but invalidations made by other processes don't.
    Use it with ``async with`` (async cache) or ``with`` (sync cache).
    """

    def __init__(self, cache: CacheBase, *, max_entries: int | None = None) -> None:
        self.cache = cache
        self.max_entries = max_entries
        self.invalidated: set[str] = set()
        self._entries: OrderedDict[str, tuple[Any, frozenset[str]]] = OrderedDict()
        self._lock = threading.Lock()
        self._token: contextvars.Token[Session | None] | None = None

    def get(self, key: str) -> tuple[bool, Any]:
        with self._lock:
            item = self._entries.get(key)
            if item is None:
                return False, None
            self._entries.move_to_end(key)
            return True, item[0]

    def put(self, key: str, value: Any, tags: frozenset[str]) -> None:
        with self._lock:
            self._entries[key] = (value, tags)
            self._entries.move_to_end(key)
            if self.max_entries is not None:
                while len(self._entries) > self.max_entries:
                    self._entries.popitem(last=False)

    def drop(self, key: str) -> None:
        with self._lock:
            self._entries.pop(key, None)

    def drop_tags(self, tags: Iterable[str]) -> None:
        targets = set(tags)
        with self._lock:
            for key in [k for k, (_v, found) in self._entries.items() if found & targets]:
                del self._entries[key]

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def __len__(self) -> int:
        return len(self._entries)

    def _enter(self) -> Self:
        if not self.cache.is_open:
            raise CacheClosedError("open the cache first (`async with cache:` or `with cache:`)")
        self._token = _session_var.set(self)
        with self.cache._sessions_lock:
            self.cache._sessions.add(self)
        return self

    def _exit(self) -> None:
        if self._token is not None:
            _session_var.reset(self._token)
            self._token = None
        with self.cache._sessions_lock:
            self.cache._sessions.discard(self)
        self.clear()

    async def __aenter__(self) -> Self:
        return self._enter()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._exit()

    def __enter__(self) -> Self:
        return self._enter()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._exit()


class TransactionBase:
    """Defers invalidations until the block exits, typically after a database commit.

    While the block runs, entries carrying a pending tag are neither served nor
    stored, so the code reads its own writes. Nested transactions join the
    outermost one. Invalidations are applied even if the block raises: an extra
    invalidation is harmless, a missing one serves stale data.
    """

    def __init__(self, cache: CacheBase) -> None:
        self.cache = cache
        self.pending: list[str] = []
        self._pending_set: set[str] = set()
        self._token: contextvars.Token[TransactionBase | None] | None = None
        self._outer: TransactionBase | None = None

    def add(self, tags: Iterable[str]) -> None:
        target = self._outer or self
        for tag in tags:
            if tag not in target._pending_set:
                target._pending_set.add(tag)
                target.pending.append(tag)

    def affects(self, tag_ancestors: frozenset[str]) -> bool:
        target = self._outer or self
        return bool(target._pending_set & tag_ancestors)

    def _enter(self) -> None:
        if not self.cache.is_open:
            raise CacheClosedError("open the cache before starting a transaction")
        outer = _transaction_var.get()
        if outer is not None and outer.cache is self.cache:
            self._outer = outer._outer or outer
        self._token = _transaction_var.set(self)

    def _exit(self) -> list[str] | None:
        """Leave the block; return the tags to invalidate, if this is the outermost one."""
        if self._token is not None:
            _transaction_var.reset(self._token)
            self._token = None
        if self._outer is not None:
            return None
        tags, self.pending = self.pending, []
        self._pending_set.clear()
        return tags


class Transaction(TransactionBase):
    """See :class:`TransactionBase`. Used with ``async with cache.transaction():``."""

    cache: Cache

    async def __aenter__(self) -> Self:
        self._enter()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        tags = self._exit()
        if tags:
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


def namespaced_ancestors(cache: CacheBase, tags: Iterable[str]) -> tuple[frozenset[str], list[str]]:
    """Ancestors of ``tags`` (plain, for sessions/transactions) and namespaced (for the backend)."""
    plain = ancestors_of_all(tags)
    return frozenset(plain), [cache.tag_name(tag) for tag in plain]


def versions_match(entry: Entry, current: Mapping[str, int]) -> list[str]:
    """Namespaced tags whose version changed since the entry was computed."""
    changed = [tag for tag, version in entry.tag_versions.items() if current.get(tag) != version]
    changed.extend(tag for tag in current if tag not in entry.tag_versions)
    return changed
