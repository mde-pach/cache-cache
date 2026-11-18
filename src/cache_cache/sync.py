"""The sync engine: :class:`SyncCache`, and ``@cached`` / ``@dedupe`` on plain ``def`` functions.

Everything works as in the async engine except batching, which needs concurrent
calls on an event loop. Dedupe works across threads: concurrent identical calls
from several threads share one execution.
"""

from __future__ import annotations

import concurrent.futures
import contextvars
import threading
from collections.abc import Callable, Sequence
from types import TracebackType
from typing import (
    Any,
    Concatenate,
    Generic,
    Literal,
    NoReturn,
    ParamSpec,
    Self,
    TypeVar,
    cast,
    overload,
)

from . import _clock
from ._base import NO_INSTANCE, Ctx, FunctionBase, Options, validate_registered
from .backends.base import Entry, StoreItem, SyncBackend, SyncDeploymentRegistry
from .backends.memory import SyncMemoryBackend
from .cache import (
    CacheBase,
    TransactionBase,
    _config,
    _current,
    _disabled_var,
    _session_var,
    _transaction_var,
    emit,
    logger,
    versions_match,
)
from .deployment import DeploymentSetting
from .errors import ConfigurationError, NoActiveSession
from .hooks import CacheEvent, Lookup, Observer, SyncCachedCall, SyncMiddleware, SyncNext
from .keys import KeyFormats
from .tags import validate_tag

__all__ = [
    "BoundSyncCachedFunction",
    "SyncCache",
    "SyncCachedFunction",
    "SyncTransaction",
    "current_sync_cache",
    "invalidate_sync",
]

P = ParamSpec("P")
P2 = ParamSpec("P2")
R = TypeVar("R")
T = TypeVar("T")
S = TypeVar("S")


def current_sync_cache() -> SyncCache | None:
    """The sync cache of the current session, else the most recently opened sync cache."""
    cache = _current("sync")
    assert cache is None or isinstance(cache, SyncCache)
    return cache


def invalidate_sync(*tags: str) -> None:
    """:func:`~cache_cache.invalidate` for sync code: uses the current :class:`SyncCache`."""
    cache = current_sync_cache()
    if cache is not None:
        cache.invalidate(*tags)


class SyncCache(CacheBase):
    """A long-lived cache for plain (``def``) functions: Django, Flask, scripts, workers.

    It takes the same arguments as :class:`~cache_cache.Cache`, with sync backends and
    sync middleware, plus:

    Args:
        refresh_workers: threads running stale-while-revalidate refreshes.
    """

    kind: Literal["async", "sync"] = "sync"

    def __init__(
        self,
        *,
        backend: SyncBackend | None = None,
        namespace: str = "cc",
        deployment: DeploymentSetting = "auto",
        retire_after: float = 600,
        heartbeat_interval: float = 60,
        ttl: float | None = 3600,
        observers: Sequence[Observer] = (),
        middleware: Sequence[SyncMiddleware] = (),
        shutdown_timeout: float = 5.0,
        key_ignore: Sequence[type] = (),
        key_formats: KeyFormats | None = None,
        refresh_workers: int = 4,
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
        self.backend: SyncBackend = backend if backend is not None else SyncMemoryBackend()
        self.middleware: list[SyncMiddleware] = list(middleware)
        self.refresh_workers = refresh_workers
        self._executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._background: set[concurrent.futures.Future[Any]] = set()
        self._background_lock = threading.Lock()
        self._maintenance: threading.Thread | None = None
        self._stop = threading.Event()

    # -- lifecycle -----------------------------------------------------------------

    def open(self) -> None:
        """Open the backend and check every sync decorated function against this cache."""
        if self._open:
            return
        self._resolve_deployment()
        validate_registered(self, "sync")
        self.backend.open()
        self._register_open()
        if self._registry_enabled(SyncDeploymentRegistry):
            self._heartbeat()
            self._stop.clear()
            self._maintenance = threading.Thread(
                target=self._maintain, name="cache-cache-maintenance", daemon=True
            )
            self._maintenance.start()

    def close(self) -> None:
        """Wait for background refreshes (up to ``shutdown_timeout``), then close the backend."""
        if not self._open:
            return
        self._register_closed()
        if self._maintenance is not None:
            self._stop.set()
            self._maintenance.join(self.shutdown_timeout)
            self._maintenance = None
        with self._background_lock:
            pending = list(self._background)
        if pending:
            concurrent.futures.wait(pending, timeout=self.shutdown_timeout)
        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._executor = None
        self.backend.close()

    def __enter__(self) -> Self:
        self.open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def transaction(self) -> SyncTransaction:
        """Collect invalidations in this block and apply them when it exits."""
        return SyncTransaction(self)

    # -- deployments ---------------------------------------------------------------

    def _heartbeat(self) -> None:
        assert self.scope is not None and isinstance(self.backend, SyncDeploymentRegistry)
        try:
            self.backend.heartbeat(self.scope)
        except Exception as exc:
            logger.warning("cache-cache heartbeat failed: %r", exc)

    def collect_retired(self) -> int:
        """Delete the entries of deployments with no live process for ``retire_after``."""
        backend = self.backend
        if not self._registry_enabled(SyncDeploymentRegistry):
            return 0
        assert isinstance(backend, SyncDeploymentRegistry)
        deleted = 0
        for scope in self._retired(backend.retired_scopes(self.retire_after)):
            count = backend.drop_scope(scope, self.retire_after)
            deleted += count
            emit(self, CacheEvent("retire", tags=(scope,), size=count))
        return deleted

    def _maintain(self) -> None:
        while True:
            try:
                self.collect_retired()
            except Exception as exc:
                logger.warning("cache-cache: collecting retired deployments failed: %r", exc)
            if self._stop.wait(self.heartbeat_interval):
                return
            self._heartbeat()

    # -- invalidation --------------------------------------------------------------

    def invalidate(self, *tags: str) -> None:
        """Invalidate tags and everything below them. Deferred inside a transaction."""
        for tag in tags:
            validate_tag(tag)
        transaction = _transaction_var.get()
        if transaction is not None and transaction.cache is self:
            transaction.add(tags)
            return
        self._invalidate_now(tags)

    def _invalidate_now(self, tags: Sequence[str]) -> None:
        if not tags:
            return
        unique = list(dict.fromkeys(tags))
        self.backend.bump_tags([self.tag_name(tag) for tag in unique])
        self._after_invalidation(unique)

    def clear(self) -> None:
        """Make every entry of this namespace unreadable, in one write."""
        self.backend.bump_tags([self.generation_tag])
        self._after_clear()

    # -- internals -----------------------------------------------------------------

    def submit_background(self, fn: Callable[..., Any], *args: Any) -> None:
        """Run ``fn`` on a worker thread, outside any session or transaction."""
        if not self._open:
            return
        if self._executor is None:
            self._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=self.refresh_workers, thread_name_prefix="cache-cache"
            )
        context = contextvars.copy_context()
        context.run(_session_var.set, None)
        context.run(_transaction_var.set, None)
        future = self._executor.submit(context.run, fn, *args)
        with self._background_lock:
            self._background.add(future)
        future.add_done_callback(self._background_done)

    def _background_done(self, future: concurrent.futures.Future[Any]) -> None:
        with self._background_lock:
            self._background.discard(future)
        if not future.cancelled() and future.exception() is not None:
            logger.warning("cache-cache background refresh failed: %r", future.exception())

    def lookup(
        self, key: str, tp: Any, tags: Sequence[str]
    ) -> tuple[Entry | None, dict[str, int] | None]:
        """Fetch an entry and the current versions of ``tags`` (namespaced)."""
        try:
            (entry,) = self.backend.get_entries([key], [tp])
            versions = self.backend.get_tag_versions(tags)
        except Exception as exc:
            logger.warning("cache-cache backend lookup failed: %r", exc)
            emit(self, CacheEvent("error", error=exc))
            return None, None
        return entry, dict(zip(tags, versions, strict=True))


class SyncTransaction(TransactionBase):
    """See :class:`~cache_cache.cache.TransactionBase`. Used with ``with cache.transaction():``."""

    cache: SyncCache

    def __enter__(self) -> Self:
        self._enter()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        tags = self._exit()
        if tags:
            self.cache._invalidate_now(tags)


class _Flight:
    """A call in progress, shared by the threads that ask for the same key."""

    __slots__ = ("done", "error", "keep", "started", "value")

    def __init__(self) -> None:
        self.done = threading.Event()
        self.started = _clock.monotonic()
        self.value: Any = None
        self.keep = False
        self.error: BaseException | None = None

    def wait(self) -> tuple[Any, bool]:
        self.done.wait()
        if self.error is not None:
            raise self.error
        return self.value, self.keep


_flights: dict[str, _Flight] = {}
_flights_lock = threading.Lock()


class SyncCachedFunction(FunctionBase[P, R]):
    """A plain function wrapped by ``@cached`` or ``@dedupe``; the sync ``CachedFunction``."""

    kind = "sync"

    def __init__(self, fn: Callable[P, R], options: Options) -> None:
        super().__init__(fn, options)

    @overload
    def __get__(self, instance: None, owner: type | None = None) -> Self: ...

    @overload
    def __get__(
        self: SyncCachedFunction[Concatenate[S, P2], T], instance: S, owner: type | None = None
    ) -> BoundSyncCachedFunction[P2, T]: ...

    def __get__(self, instance: Any, owner: type | None = None) -> Any:
        if instance is None:
            return self
        return BoundSyncCachedFunction(self, instance)

    def batched(self, **_: Any) -> NoReturn:
        """Not available in sync code: calls run one after another, so there is nothing to merge."""
        raise ConfigurationError(
            f"{self.name}: batching needs async functions. Sync calls run one after another, "
            f"so there are no concurrent calls to merge; call the bulk function directly."
        )

    # -- calls -----------------------------------------------------------------------

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        instance, rest = self._split_instance(args)
        return cast(R, self._call(instance, rest, dict(kwargs)))

    def _invoke(self, instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        return self._fn(*self._full_args(instance, args), **kwargs)

    def _call(self, instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        if _disabled_var.get():
            return self._invoke(instance, args, kwargs)
        cache = current_sync_cache()
        if self._stores:
            if _config.strict and _session_var.get() is None:
                raise NoActiveSession(f"{self.name} was called outside a session")
            if cache is None:
                return self._invoke(instance, args, kwargs)
        if self._condition is not None and not self._condition(
            *self._full_args(instance, args), **kwargs
        ):
            emit(cache, CacheEvent("bypass", self))
            return self._invoke(instance, args, kwargs)
        ctx = self._context(cache, instance, args, kwargs)
        if self._blocked_by_transaction(ctx):
            emit(cache, CacheEvent("bypass", self, ctx.key))
            return self._invoke(instance, args, kwargs)

        lookup = self._lookup(ctx)
        call = SyncCachedCall(
            function=self,
            instance=None if instance is NO_INSTANCE else instance,
            args=args,
            kwargs=kwargs,
            key=ctx.key,
            lookup=lookup,
            run=lambda: self._run_fresh(ctx),
        )

        def final(c: SyncCachedCall) -> Any:
            return self._default(ctx, c.lookup)

        handler = _compose(cache.middleware if cache is not None else (), final)
        return handler(call)

    def _in_flight(self, key: str) -> Lookup | None:
        with _flights_lock:
            flight = _flights.get(key)
        if flight is None:
            return None
        return Lookup("in_flight", None, _clock.monotonic() - flight.started)

    def _lookup(self, ctx: Ctx) -> Lookup:
        if ctx.session is not None and self._stores:
            found, value = ctx.session.get(ctx.key)
            if found:
                ctx.entry = Entry(value, 0.0, None)
                return Lookup("hit", "session")
        in_flight = self._in_flight(ctx.key)
        if in_flight is not None:
            return in_flight
        if not self._uses_backend or ctx.cache is None:
            return Lookup("miss")
        entry, versions = ctx.cache.lookup(ctx.key, ctx.plan.static.return_type, ctx.ns_tags)
        ctx.versions = versions
        if entry is None or versions is None:
            return self._in_flight(ctx.key) or Lookup("miss")
        now = _clock.now()
        age = max(0.0, now - entry.stored_at)
        changed = versions_match(entry, versions)
        if not changed and (entry.fresh_until is None or now < entry.fresh_until):
            ctx.entry = entry
            return Lookup("hit", "backend", age)
        ctx.stale = entry
        ctx.changed = changed
        return Lookup("stale", "backend", age)

    def _default(self, ctx: Ctx, lookup: Lookup) -> Any:
        cache = ctx.cache
        if lookup.state == "hit" and ctx.entry is not None:
            emit(cache, CacheEvent("hit", self, ctx.key, lookup.layer, lookup.age))
            if lookup.layer == "backend":
                self._remember(ctx, ctx.entry.value)
            return ctx.entry.value
        if lookup.state == "stale" and self._stale_allowed(
            ctx, self._options.stale_while_revalidate
        ):
            emit(cache, CacheEvent("stale", self, ctx.key, lookup.layer, lookup.age))
            if self._in_flight(ctx.key) is None and cache is not None:
                cache.submit_background(self._background_refresh, ctx)
            assert ctx.stale is not None
            return ctx.stale.value
        with _flights_lock:
            flight = _flights.get(ctx.key)
            leader = flight is None
            if flight is None:
                flight = _flights[ctx.key] = _Flight()
        if leader:
            emit(cache, CacheEvent("miss", self, ctx.key))
            value, keep = self._lead(ctx, flight)
        else:
            emit(
                cache,
                CacheEvent("joined", self, ctx.key, age=_clock.monotonic() - flight.started),
            )
            value, keep = flight.wait()
        if keep:
            self._remember(ctx, value)
        return value

    def _lead(self, ctx: Ctx, flight: _Flight) -> tuple[Any, bool]:
        try:
            flight.value, flight.keep = self._execute(ctx)
        except BaseException as exc:
            flight.error = exc
            raise
        finally:
            with _flights_lock:
                if _flights.get(ctx.key) is flight:
                    del _flights[ctx.key]
            flight.done.set()
        return flight.value, flight.keep

    def _background_refresh(self, ctx: Ctx) -> None:
        with _flights_lock:
            if ctx.key in _flights:
                return
            flight = _flights[ctx.key] = _Flight()
        ctx.session = None
        self._lead(ctx, flight)

    def _run_fresh(self, ctx: Ctx) -> Any:
        if self._uses_backend and ctx.cache is not None and ctx.versions is None:
            ctx.versions = self._current_versions(ctx)
        flight = _Flight()
        with _flights_lock:
            _flights[ctx.key] = flight
        value, keep = self._lead(ctx, flight)
        if keep:
            self._remember(ctx, value)
        return value

    def _current_versions(self, ctx: Ctx) -> dict[str, int] | None:
        assert ctx.cache is not None
        try:
            found = ctx.cache.backend.get_tag_versions(ctx.ns_tags)
        except Exception as exc:
            emit(ctx.cache, CacheEvent("error", self, ctx.key, error=exc))
            return None
        return dict(zip(ctx.ns_tags, found, strict=True))

    def _execute(self, ctx: Ctx) -> tuple[Any, bool]:
        cache = ctx.cache
        started = _clock.monotonic()
        try:
            try:
                value = self._invoke(ctx.instance, ctx.args, ctx.kwargs)
            finally:
                emit(
                    cache, CacheEvent("call", self, ctx.key, duration=_clock.monotonic() - started)
                )
        except Exception as exc:
            emit(cache, CacheEvent("error", self, ctx.key, error=exc))
            if self._stale_allowed(ctx, self._options.stale_if_error):
                emit(cache, CacheEvent("stale_if_error", self, ctx.key, error=exc))
                assert ctx.stale is not None
                return ctx.stale.value, False
            raise
        keep = self._keep(ctx, value)
        if keep and self._uses_backend and cache is not None:
            self._store(ctx, value)
        return value, keep

    def _store(self, ctx: Ctx, value: Any) -> None:
        cache = ctx.cache
        assert cache is not None
        if ctx.versions is None or self._blocked_by_transaction(ctx):
            return
        ttl = ctx.plan.ttl
        now = _clock.now()
        entry = Entry(
            value=value,
            stored_at=now,
            fresh_until=None if ttl is None else now + ttl,
            tag_versions=dict(ctx.versions),
        )
        try:
            cache.backend.set_entries(
                [StoreItem(ctx.key, entry, self._backend_ttl(ttl), ctx.plan.static.return_type)]
            )
        except Exception as exc:
            emit(cache, CacheEvent("error", self, ctx.key, error=exc))
            return
        emit(cache, CacheEvent("store", self, ctx.key, tags=tuple(ctx.ancestors)))

    # -- per-call controls -----------------------------------------------------------

    def _require_cache(self) -> SyncCache:
        cache = current_sync_cache()
        if cache is None:
            raise RuntimeError(f"{self.name}: no open sync cache")
        return cache

    def _refresh(self, instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        cache = self._require_cache()
        ctx = self._context(cache, instance, args, kwargs)
        emit(cache, CacheEvent("miss", self, ctx.key))
        return self._run_fresh(ctx)

    def _peek(self, instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        cache = current_sync_cache()
        if cache is None or not self._stores:
            return None
        ctx = self._context(cache, instance, args, kwargs)
        if ctx.session is not None:
            found, value = ctx.session.get(ctx.key)
            if found:
                return value
        if not self._uses_backend:
            return None
        entry, versions = cache.lookup(ctx.key, ctx.plan.static.return_type, ctx.ns_tags)
        if entry is None or versions is None or versions_match(entry, versions):
            return None
        if entry.fresh_until is not None and _clock.now() >= entry.fresh_until:
            return None
        return entry.value

    def _invalidate_one(self, instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        cache = current_sync_cache()
        if cache is None:
            return
        ctx = self._context(cache, instance, args, kwargs)
        for session in cache._open_sessions():
            session.drop(ctx.key)
        if self._uses_backend:
            cache.backend.delete([ctx.key])

    def _forget(self, instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        ctx = self._context(current_sync_cache(), instance, args, kwargs)
        with _flights_lock:
            _flights.pop(ctx.key, None)

    def _prime(
        self, value: Any, instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> None:
        cache = self._require_cache()
        if not self._stores:
            raise ConfigurationError(f"{self.name}: only @cached functions can be primed")
        ctx = self._context(cache, instance, args, kwargs)
        self._remember(ctx, value)
        if self._uses_backend:
            ctx.versions = self._current_versions(ctx)
            self._store(ctx, value)

    def refresh(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """Run the function, ignoring the cache, and store the new result."""
        instance, rest = self._split_instance(args)
        return cast(R, self._refresh(instance, rest, dict(kwargs)))

    def peek(self, *args: P.args, **kwargs: P.kwargs) -> R | None:
        """The fresh cached value, or ``None``. Never calls the function."""
        instance, rest = self._split_instance(args)
        return cast("R | None", self._peek(instance, rest, dict(kwargs)))

    def invalidate(self, *args: P.args, **kwargs: P.kwargs) -> None:
        """Evict the entry for these arguments. Tags remain the main mechanism."""
        instance, rest = self._split_instance(args)
        self._invalidate_one(instance, rest, dict(kwargs))

    def forget(self, *args: P.args, **kwargs: P.kwargs) -> None:
        """The next call with these arguments doesn't join the one in flight."""
        instance, rest = self._split_instance(args)
        self._forget(instance, rest, dict(kwargs))

    def prime(self, value: R, /, *args: P.args, **kwargs: P.kwargs) -> None:
        """Store ``value`` as the result for these arguments."""
        instance, rest = self._split_instance(args)
        self._prime(value, instance, rest, dict(kwargs))


class BoundSyncCachedFunction(Generic[P, R]):
    """A cached sync method bound to an instance: ``repo.get``."""

    __slots__ = ("__self__", "_function")

    def __init__(self, function: SyncCachedFunction[Any, R], instance: Any) -> None:
        self._function = function
        self.__self__ = instance

    @property
    def __func__(self) -> SyncCachedFunction[Any, R]:
        return self._function

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return cast(R, self._function._call(self.__self__, args, dict(kwargs)))

    def refresh(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return cast(R, self._function._refresh(self.__self__, args, dict(kwargs)))

    def peek(self, *args: P.args, **kwargs: P.kwargs) -> R | None:
        return cast("R | None", self._function._peek(self.__self__, args, dict(kwargs)))

    def invalidate(self, *args: P.args, **kwargs: P.kwargs) -> None:
        self._function._invalidate_one(self.__self__, args, dict(kwargs))

    def forget(self, *args: P.args, **kwargs: P.kwargs) -> None:
        self._function._forget(self.__self__, args, dict(kwargs))

    def prime(self, value: R, /, *args: P.args, **kwargs: P.kwargs) -> None:
        self._function._prime(value, self.__self__, args, dict(kwargs))

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, BoundSyncCachedFunction)
            and other._function is self._function
            and other.__self__ is self.__self__
        )

    def __hash__(self) -> int:
        return hash((id(self._function), id(self.__self__)))

    def __repr__(self) -> str:
        return f"<bound {self._function!r} of {self.__self__!r}>"


def _compose(middleware: Sequence[SyncMiddleware], final: SyncNext) -> SyncNext:
    handler = final
    for item in reversed(middleware):
        handler = _wrap(item, handler)
    return handler


def _wrap(middleware: SyncMiddleware, call_next: SyncNext) -> SyncNext:
    def handler(call: SyncCachedCall) -> Any:
        return middleware(call, call_next)

    return handler
