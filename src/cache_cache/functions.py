"""The async engine: ``@cached`` and ``@dedupe`` on ``async def`` functions, and batching."""

from __future__ import annotations

import asyncio
import functools
import inspect
import typing
import weakref
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Concatenate, Generic, ParamSpec, Self, TypeVar, cast, overload

from . import _clock
from ._base import NO_INSTANCE, Ctx, FunctionBase, Options
from .backends.base import Entry, StoreItem
from .cache import (
    Cache,
    _config,
    _disabled_var,
    _session_var,
    current_cache,
    emit,
    versions_match,
)
from .errors import ConfigurationError, NoActiveSession
from .hooks import CachedCall, CacheEvent, Lookup, Middleware, Next
from .keys import digest

__all__ = [
    "BatchedFunction",
    "BoundBatchedFunction",
    "BoundCachedFunction",
    "CachedFunction",
]

P = ParamSpec("P")
P2 = ParamSpec("P2")
R = TypeVar("R")
T = TypeVar("T")
S = TypeVar("S")
K = TypeVar("K")


@dataclass
class _BatchSpec:
    fn: Callable[..., Awaitable[Mapping[Any, Any]]]
    on: str
    max_size: int | None
    window: float
    params: list[str] = field(default_factory=list)


@dataclass
class _InFlight:
    task: asyncio.Task[tuple[Any, bool]]
    started: float


_inflight: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[str, _InFlight]] = (
    weakref.WeakKeyDictionary()
)
_batches: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[tuple[Any, ...], _Batch]] = (
    weakref.WeakKeyDictionary()
)
_batch_tasks: set[asyncio.Task[Any]] = set()


def _loop_registry(
    registry: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[Any, Any]],
) -> dict[Any, Any]:
    loop = asyncio.get_running_loop()
    try:
        return registry[loop]
    except KeyError:
        registry[loop] = {}
        return registry[loop]


class CachedFunction(FunctionBase[P, R]):
    """An ``async`` function wrapped by ``@cached`` or ``@dedupe``.

    It keeps the function's signature. Accessed on an instance, it becomes a
    :class:`BoundCachedFunction` with the same controls.
    """

    kind = "async"

    def __init__(self, fn: Callable[P, Awaitable[R]], options: Options) -> None:
        if not inspect.iscoroutinefunction(fn):
            raise ConfigurationError(f"{fn.__qualname__}: expected an async function")
        self._batch: _BatchSpec | None = None
        super().__init__(fn, options)

    @overload
    def __get__(self, instance: None, owner: type | None = None) -> Self: ...

    @overload
    def __get__(
        self: CachedFunction[Concatenate[S, P2], T], instance: S, owner: type | None = None
    ) -> BoundCachedFunction[P2, T]: ...

    def __get__(self, instance: Any, owner: type | None = None) -> Any:
        if instance is None:
            return self
        return BoundCachedFunction(self, instance)

    def batched(
        self, *, on: str, max_size: int | None = None, window: float = 0.0
    ) -> Callable[[Callable[P2, Awaitable[Mapping[K, Any]]]], BatchedFunction[P2, K, R]]:
        """Register a bulk function: concurrent calls become one bulk call.

        Args:
            on: the parameter that varies. The bulk function takes a sequence of its values,
                at the same position, and returns a mapping from value to result.
            max_size: split larger batches into several bulk calls.
            window: seconds to wait for more calls. ``0`` (the default) sends the batch
                after the current event-loop iteration.
        """
        if self._options.mode != "cached":
            raise ConfigurationError("only @cached functions can be batched")
        if max_size is not None and max_size < 1:
            raise ValueError("max_size must be at least 1")
        if window < 0:
            raise ValueError("window can't be negative")

        def decorator(bulk: Callable[P2, Awaitable[Mapping[K, Any]]]) -> BatchedFunction[P2, K, R]:
            if not inspect.iscoroutinefunction(bulk):
                raise ConfigurationError(f"{bulk.__qualname__}: bulk functions must be async")
            spec = _BatchSpec(bulk, on, max_size, window)
            self._check_bulk_signature(spec)
            self._batch = spec
            return BatchedFunction(self, bulk)

        return decorator

    def _check_bulk_signature(self, spec: _BatchSpec) -> None:
        parent = list(inspect.signature(self._fn).parameters.values())
        bulk = list(inspect.signature(spec.fn).parameters.values())
        if self._is_method:
            parent, bulk = parent[1:], bulk[1:]
        names = [p.name for p in parent]
        if spec.on not in names:
            raise ConfigurationError(
                f"batched(on={spec.on!r}): {self._fn.__name__}() has no such parameter"
            )
        problem = len(parent) != len(bulk)
        for p, b in zip(parent, bulk, strict=False):
            if b.kind in (b.VAR_POSITIONAL, b.VAR_KEYWORD, b.POSITIONAL_ONLY):
                problem = True
            elif p.name != spec.on and p.name != b.name:
                problem = True
        if problem:
            raise ConfigurationError(
                f"{spec.fn.__qualname__}{inspect.signature(spec.fn)} must have the parameters of "
                f"{self._fn.__name__}{inspect.signature(self._fn)}, with {spec.on!r} replaced "
                f"by a sequence"
            )
        spec.params = [b.name for b in bulk]

    def _extra_static_checks(self, hints: dict[str, Any]) -> None:
        spec = self._batch
        if spec is None:
            return
        try:
            bulk_hints = typing.get_type_hints(spec.fn)
        except Exception as exc:
            raise ConfigurationError(
                f"{spec.fn.__qualname__}: can't resolve annotations ({exc})"
            ) from exc
        bulk_name = spec.params[self._params().index(spec.on)]
        annotation = bulk_hints.get(bulk_name)
        if annotation is None:
            return
        origin = typing.get_origin(annotation) or annotation
        sequence_like = isinstance(origin, type) and issubclass(
            origin, (Sequence, list, tuple, set, frozenset, typing.Iterable)
        )
        if not sequence_like or (isinstance(origin, type) and issubclass(origin, (str, bytes))):
            raise ConfigurationError(
                f"{spec.fn.__qualname__}: {bulk_name!r} must be a sequence of "
                f"{spec.on!r} values, not {annotation!r}"
            )

    # -- calls -----------------------------------------------------------------------

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        instance, rest = self._split_instance(args)
        return cast(R, await self._call(instance, rest, dict(kwargs)))

    async def _invoke(self, instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        return await self._fn(*self._full_args(instance, args), **kwargs)

    async def _uncached(self, instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        """Run without storing anything. Batched functions still go through their bulk call."""
        if self._batch is None:
            return await self._invoke(instance, args, kwargs)
        ctx = self._context(None, instance, args, kwargs)
        return await self._submit_to_batch(ctx)

    async def _call(self, instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        if _disabled_var.get():
            return await self._uncached(instance, args, kwargs)
        cache = current_cache()
        if self._stores:
            if _config.strict and _session_var.get() is None:
                raise NoActiveSession(f"{self.name} was called outside a session")
            if cache is None:
                return await self._uncached(instance, args, kwargs)
        if self._condition is not None and not self._condition(
            *self._full_args(instance, args), **kwargs
        ):
            emit(cache, CacheEvent("bypass", self))
            return await self._uncached(instance, args, kwargs)
        ctx = self._context(cache, instance, args, kwargs)
        if self._blocked_by_transaction(ctx):
            emit(cache, CacheEvent("bypass", self, ctx.key))
            return await self._uncached(instance, args, kwargs)

        lookup = await self._lookup(ctx)
        call = CachedCall(
            function=self,
            instance=None if instance is NO_INSTANCE else instance,
            args=args,
            kwargs=kwargs,
            key=ctx.key,
            lookup=lookup,
            run=lambda: self._run_fresh(ctx),
        )

        async def final(c: CachedCall) -> Any:
            return await self._default(ctx, c.lookup)

        handler = _compose(cache.middleware if cache is not None else (), final)
        return await handler(call)

    async def _lookup(self, ctx: Ctx) -> Lookup:
        if ctx.session is not None and self._stores:
            found, value = ctx.session.get(ctx.key)
            if found:
                ctx.entry = Entry(value, 0.0, None)
                return Lookup("hit", "session")
        inflight = _loop_registry(_inflight).get(ctx.key)
        if inflight is not None:
            return Lookup("in_flight", None, _clock.monotonic() - inflight.started)
        if not self._uses_backend or ctx.cache is None:
            return Lookup("miss")
        entry, versions = await ctx.cache.lookup(ctx.key, ctx.plan.static.return_type, ctx.ns_tags)
        ctx.versions = versions
        if entry is None or versions is None:
            inflight = _loop_registry(_inflight).get(ctx.key)
            if inflight is not None:
                return Lookup("in_flight", None, _clock.monotonic() - inflight.started)
            return Lookup("miss")
        now = _clock.now()
        age = max(0.0, now - entry.stored_at)
        changed = versions_match(entry, versions)
        fresh = entry.fresh_until is None or now < entry.fresh_until
        if not changed and fresh:
            ctx.entry = entry
            return Lookup("hit", "backend", age)
        ctx.stale = entry
        ctx.changed = changed
        return Lookup("stale", "backend", age)

    async def _default(self, ctx: Ctx, lookup: Lookup) -> Any:
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
            if ctx.key not in _loop_registry(_inflight) and cache is not None:
                cache.spawn_background(self._background_refresh(ctx))
            assert ctx.stale is not None
            return ctx.stale.value
        inflight = _loop_registry(_inflight).get(ctx.key)
        if inflight is not None:
            emit(
                cache,
                CacheEvent("joined", self, ctx.key, age=_clock.monotonic() - inflight.started),
            )
            task = inflight.task
        else:
            emit(cache, CacheEvent("miss", self, ctx.key))
            task = self._start(ctx)
        value, keep = await asyncio.shield(task)
        if keep:
            self._remember(ctx, value)
        return value

    async def _background_refresh(self, ctx: Ctx) -> None:
        if ctx.key in _loop_registry(_inflight):
            return
        ctx.session = None
        await self._start(ctx)

    async def _run_fresh(self, ctx: Ctx) -> Any:
        if self._uses_backend and ctx.cache is not None and ctx.versions is None:
            ctx.versions = await self._current_versions(ctx)
        value, keep = await asyncio.shield(self._start(ctx))
        if keep:
            self._remember(ctx, value)
        return value

    async def _current_versions(self, ctx: Ctx) -> dict[str, int] | None:
        assert ctx.cache is not None
        try:
            found = await ctx.cache.backend.get_tag_versions(ctx.ns_tags)
        except Exception as exc:
            emit(ctx.cache, CacheEvent("error", self, ctx.key, error=exc))
            return None
        return dict(zip(ctx.ns_tags, found, strict=True))

    def _start(self, ctx: Ctx) -> asyncio.Task[tuple[Any, bool]]:
        registry = _loop_registry(_inflight)
        task = asyncio.get_running_loop().create_task(self._execute(ctx))
        registry[ctx.key] = _InFlight(task, _clock.monotonic())

        def done(t: asyncio.Task[tuple[Any, bool]]) -> None:
            current = registry.get(ctx.key)
            if current is not None and current.task is t:
                del registry[ctx.key]
            if not t.cancelled():
                t.exception()

        task.add_done_callback(done)
        return task

    async def _execute(self, ctx: Ctx) -> tuple[Any, bool]:
        cache = ctx.cache
        started = _clock.monotonic()
        try:
            if self._batch is not None:
                value = await self._submit_to_batch(ctx)
            else:
                try:
                    value = await self._invoke(ctx.instance, ctx.args, ctx.kwargs)
                finally:
                    emit(
                        cache,
                        CacheEvent("call", self, ctx.key, duration=_clock.monotonic() - started),
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
            await self._store(ctx, value)
        return value, keep

    async def _store(self, ctx: Ctx, value: Any) -> None:
        cache = ctx.cache
        assert cache is not None
        if ctx.versions is None:
            return  # tag versions unknown: storing could hide an invalidation
        if self._blocked_by_transaction(ctx):
            return
        ttl = ctx.plan.ttl
        now = _clock.now()
        backend_ttl = self._backend_ttl(ttl)
        entry = Entry(
            value=value,
            stored_at=now,
            fresh_until=None if ttl is None else now + ttl,
            tag_versions=dict(ctx.versions),
        )
        try:
            await cache.backend.set_entries(
                [StoreItem(ctx.key, entry, backend_ttl, ctx.plan.static.return_type)]
            )
        except Exception as exc:
            emit(cache, CacheEvent("error", self, ctx.key, error=exc))
            return
        emit(cache, CacheEvent("store", self, ctx.key, tags=tuple(ctx.ancestors)))

    # -- batching --------------------------------------------------------------------

    # -- batching --------------------------------------------------------------------

    async def _submit_to_batch(self, ctx: Ctx) -> Any:
        spec = self._batch
        assert spec is not None
        plan = ctx.plan
        others = {name: value for name, value in ctx.arguments.items() if name != spec.on}
        group = digest(self._encode_arguments(plan, others))
        registry_key = (id(self), id(ctx.instance), group)
        registry = _loop_registry(_batches)
        batch = registry.get(registry_key)
        if batch is None:
            batch = _Batch(self, spec, ctx.cache, ctx.instance, others, registry, registry_key)
            registry[registry_key] = batch
        return await batch.submit(ctx.arguments[spec.on])

    async def _invoke_bulk(
        self, instance: Any, ids: list[Any], others: dict[str, Any]
    ) -> Mapping[Any, Any]:
        spec = self._batch
        assert spec is not None
        kwargs = {
            bulk_name: (ids if name == spec.on else others[name])
            for name, bulk_name in zip(self._params(), spec.params, strict=True)
        }
        full = () if instance is NO_INSTANCE else (instance,)
        return await spec.fn(*full, **kwargs)

    # -- per-call controls -----------------------------------------------------------

    def _require_cache(self) -> Cache:
        cache = current_cache()
        if cache is None:
            raise RuntimeError(f"{self.name}: no open cache")
        return cache

    async def _refresh(self, instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        cache = self._require_cache()
        ctx = self._context(cache, instance, args, kwargs)
        emit(cache, CacheEvent("miss", self, ctx.key))
        return await self._run_fresh(ctx)

    async def _peek(self, instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        cache = current_cache()
        if cache is None or not self._stores:
            return None
        ctx = self._context(cache, instance, args, kwargs)
        if ctx.session is not None:
            found, value = ctx.session.get(ctx.key)
            if found:
                return value
        if not self._uses_backend:
            return None
        entry, versions = await cache.lookup(ctx.key, ctx.plan.static.return_type, ctx.ns_tags)
        if entry is None or versions is None or versions_match(entry, versions):
            return None
        if entry.fresh_until is not None and _clock.now() >= entry.fresh_until:
            return None
        return entry.value

    async def _invalidate_one(
        self, instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> None:
        cache = current_cache()
        if cache is None:
            return
        ctx = self._context(cache, instance, args, kwargs)
        for session in list(cache._sessions):
            session.drop(ctx.key)
        if self._uses_backend:
            await cache.backend.delete([ctx.key])

    def _forget(self, instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        ctx = self._context(current_cache(), instance, args, kwargs)
        _loop_registry(_inflight).pop(ctx.key, None)

    async def _prime(
        self, value: Any, instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> None:
        cache = self._require_cache()
        if not self._stores:
            raise ConfigurationError(f"{self.name}: only @cached functions can be primed")
        ctx = self._context(cache, instance, args, kwargs)
        self._remember(ctx, value)
        if self._uses_backend:
            ctx.versions = await self._current_versions(ctx)
            await self._store(ctx, value)

    async def refresh(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """Run the function, ignoring the cache, and store the new result."""
        instance, rest = self._split_instance(args)
        return cast(R, await self._refresh(instance, rest, dict(kwargs)))

    async def peek(self, *args: P.args, **kwargs: P.kwargs) -> R | None:
        """The fresh cached value, or ``None``. Never calls the function."""
        instance, rest = self._split_instance(args)
        return cast("R | None", await self._peek(instance, rest, dict(kwargs)))

    async def invalidate(self, *args: P.args, **kwargs: P.kwargs) -> None:
        """Evict the entry for these arguments. Tags remain the main mechanism."""
        instance, rest = self._split_instance(args)
        await self._invalidate_one(instance, rest, dict(kwargs))

    def forget(self, *args: P.args, **kwargs: P.kwargs) -> None:
        """The next call with these arguments doesn't join the one in flight."""
        instance, rest = self._split_instance(args)
        self._forget(instance, rest, dict(kwargs))

    async def prime(self, value: R, /, *args: P.args, **kwargs: P.kwargs) -> None:
        """Store ``value`` as the result for these arguments, e.g. after a list query."""
        instance, rest = self._split_instance(args)
        await self._prime(value, instance, rest, dict(kwargs))


class BoundCachedFunction(Generic[P, R]):
    """A cached method bound to an instance: ``repo.get``."""

    __slots__ = ("__self__", "_function")

    def __init__(self, function: CachedFunction[Any, R], instance: Any) -> None:
        self._function = function
        self.__self__ = instance

    @property
    def __func__(self) -> CachedFunction[Any, R]:
        return self._function

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return cast(R, await self._function._call(self.__self__, args, dict(kwargs)))

    async def refresh(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return cast(R, await self._function._refresh(self.__self__, args, dict(kwargs)))

    async def peek(self, *args: P.args, **kwargs: P.kwargs) -> R | None:
        return cast("R | None", await self._function._peek(self.__self__, args, dict(kwargs)))

    async def invalidate(self, *args: P.args, **kwargs: P.kwargs) -> None:
        await self._function._invalidate_one(self.__self__, args, dict(kwargs))

    def forget(self, *args: P.args, **kwargs: P.kwargs) -> None:
        self._function._forget(self.__self__, args, dict(kwargs))

    async def prime(self, value: R, /, *args: P.args, **kwargs: P.kwargs) -> None:
        await self._function._prime(value, self.__self__, args, dict(kwargs))

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, BoundCachedFunction)
            and other._function is self._function
            and other.__self__ is self.__self__
        )

    def __hash__(self) -> int:
        return hash((id(self._function), id(self.__self__)))

    def __repr__(self) -> str:
        return f"<bound {self._function!r} of {self.__self__!r}>"


# --------------------------------------------------------------------------------------
# Batching
# --------------------------------------------------------------------------------------


class _Batch:
    def __init__(
        self,
        function: CachedFunction[Any, Any],
        spec: _BatchSpec,
        cache: Cache | None,
        instance: Any,
        others: dict[str, Any],
        registry: dict[tuple[Any, ...], _Batch],
        registry_key: tuple[Any, ...],
    ) -> None:
        self.function = function
        self.spec = spec
        self.cache = cache
        self.instance = instance
        self.others = others
        self.registry = registry
        self.registry_key = registry_key
        self.waiters: dict[Any, list[asyncio.Future[Any]]] = {}
        loop = asyncio.get_running_loop()
        if spec.window > 0:
            loop.call_later(spec.window, self._flush)
        else:
            loop.call_soon(self._flush)

    def submit(self, item: Any) -> asyncio.Future[Any]:
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self.waiters.setdefault(item, []).append(future)
        if self.spec.max_size is not None and len(self.waiters) >= self.spec.max_size:
            self._flush()
        return future

    def _flush(self) -> None:
        if self.registry.get(self.registry_key) is not self:
            return
        del self.registry[self.registry_key]
        task = asyncio.get_running_loop().create_task(self._run())
        _batch_tasks.add(task)
        task.add_done_callback(_batch_tasks.discard)

    async def _run(self) -> None:
        ids = list(self.waiters)
        size = self.spec.max_size or len(ids) or 1
        chunks = [ids[i : i + size] for i in range(0, len(ids), size)]
        await asyncio.gather(*(self._run_chunk(chunk) for chunk in chunks))

    async def _run_chunk(self, chunk: list[Any]) -> None:
        function = self.function
        emit(self.cache, CacheEvent("batch", function, size=len(chunk)))
        started = _clock.monotonic()
        try:
            mapping = await function._invoke_bulk(self.instance, chunk, self.others)
        except BaseException as exc:
            for item in chunk:
                for future in self.waiters[item]:
                    if not future.done():
                        if isinstance(exc, asyncio.CancelledError):
                            future.cancel()
                        else:
                            future.set_exception(exc)
            if not isinstance(exc, Exception):
                raise
            return
        finally:
            emit(self.cache, CacheEvent("call", function, duration=_clock.monotonic() - started))
        optional = function._static().optional
        for item in chunk:
            for future in self.waiters[item]:
                if future.done():
                    continue
                if item in mapping:
                    future.set_result(mapping[item])
                elif optional:
                    future.set_result(None)
                else:
                    future.set_exception(KeyError(item))


class BatchedFunction(Generic[P, K, R]):
    """The bulk function registered with ``@get.batched``.

    Calling it directly goes through the cache too: each id is looked up, and only
    the missing ones reach the bulk function. Missing results are left out of the
    returned mapping.
    """

    def __init__(
        self, parent: CachedFunction[Any, R], bulk: Callable[P, Awaitable[Mapping[K, Any]]]
    ) -> None:
        functools.update_wrapper(self, bulk)
        self._parent = parent
        self._bulk = bulk

    def __set_name__(self, owner: type, name: str) -> None:
        self._parent._is_method = True

    @overload
    def __get__(self, instance: None, owner: type | None = None) -> Self: ...

    @overload
    def __get__(
        self: BatchedFunction[Concatenate[S, P2], K, T], instance: S, owner: type | None = None
    ) -> BoundBatchedFunction[P2, K, T]: ...

    def __get__(self, instance: Any, owner: type | None = None) -> Any:
        if instance is None:
            return self
        return BoundBatchedFunction(self, instance)

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> dict[K, R]:
        instance, rest = self._parent._split_instance(args)
        return await self._call(instance, rest, dict(kwargs))

    async def _call(
        self, instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> dict[K, R]:
        parent = self._parent
        spec = parent._batch
        assert spec is not None
        signature = inspect.signature(self._bulk)
        full = parent._full_args(instance, args)
        bound = signature.bind(*full, **kwargs)
        bound.apply_defaults()
        arguments = dict(bound.arguments)
        if parent._is_method:
            arguments.pop(next(iter(signature.parameters)))
        mapped = {
            name: arguments[bulk_name]
            for name, bulk_name in zip(parent._params(), spec.params, strict=True)
        }
        ids = list(dict.fromkeys(mapped.pop(spec.on)))

        async def one(item: Any) -> Any:
            return await parent._call(instance, (), {**mapped, spec.on: item})

        results = await asyncio.gather(*(one(item) for item in ids), return_exceptions=True)
        found: dict[K, R] = {}
        for item, result in zip(ids, results, strict=True):
            if isinstance(result, KeyError):
                continue
            if isinstance(result, BaseException):
                raise result
            if result is None and parent._static().optional:
                continue
            found[item] = result
        return found


class BoundBatchedFunction(Generic[P, K, R]):
    __slots__ = ("__self__", "_function")

    def __init__(self, function: BatchedFunction[Any, K, R], instance: Any) -> None:
        self._function = function
        self.__self__ = instance

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> dict[K, R]:
        return await self._function._call(self.__self__, args, dict(kwargs))


def _compose(middleware: Sequence[Middleware], final: Next) -> Next:
    handler = final
    for item in reversed(middleware):
        handler = _wrap(item, handler)
    return handler


def _wrap(middleware: Middleware, call_next: Next) -> Next:
    async def handler(call: CachedCall) -> Any:
        return await middleware(call, call_next)

    return handler
