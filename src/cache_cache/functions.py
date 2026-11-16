"""``@cached``, ``@dedupe``, ``@invalidates`` and the engine behind them."""

from __future__ import annotations

import asyncio
import enum
import functools
import inspect
import types
import typing
import weakref
from collections.abc import Awaitable, Callable, Hashable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Concatenate, Generic, Literal, ParamSpec, Self, TypeVar, cast, overload

from . import _clock
from .backends.base import Entry, StoreItem
from .cache import (
    Cache,
    Session,
    Transaction,
    _config,
    _disabled_var,
    _session_var,
    _transaction_var,
    current_cache,
    emit,
    invalidate,
    namespaced_ancestors,
    versions_match,
)
from .errors import ConfigurationError, NoActiveSession
from .hooks import CachedCall, CacheEvent, Lookup, Middleware, Next
from .keys import KeyFormats, digest, encode_value, fingerprint, is_encodable
from .tags import TagTemplate

__all__ = [
    "BatchedFunction",
    "BoundBatchedFunction",
    "BoundCachedFunction",
    "CachedFunction",
    "cached",
    "dedupe",
    "invalidates",
]

P = ParamSpec("P")
P2 = ParamSpec("P2")
R = TypeVar("R")
T = TypeVar("T")
S = TypeVar("S")
K = TypeVar("K")
F = TypeVar("F", bound=Callable[..., Awaitable[Any]])


class _Default(enum.Enum):
    DEFAULT = "DEFAULT"


DEFAULT: Literal[_Default.DEFAULT] = _Default.DEFAULT
"""Sentinel: use the cache's default ``ttl``."""

_NO_INSTANCE: Any = object()

_registry: list[CachedFunction[Any, Any]] = []
_invalidations: list[InvalidationSite] = []


@dataclass(frozen=True)
class InvalidationSite:
    """Where ``@invalidates`` was used. Read by ``cache-cache check``."""

    name: str
    module: str
    templates: tuple[TagTemplate, ...]


# --------------------------------------------------------------------------------------
# Per-function configuration
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class _Options:
    kind: Literal["cached", "dedupe"]
    ttl: float | _Default | None = DEFAULT
    tags: tuple[str, ...] = ()
    key_ignore: tuple[type, ...] = ()
    key_formats: KeyFormats = field(default_factory=dict)
    stale_while_revalidate: float | None = None
    stale_if_error: float | None = None
    version: str | None = None
    session_only: bool = False


@dataclass
class _Static:
    """What can be known from the function alone, once its annotations resolve."""

    signature: inspect.Signature
    hints: dict[str, Any]
    return_type: Any
    optional: bool
    params: list[str]
    self_name: str | None


@dataclass
class _Plan:
    """The function resolved against one cache."""

    static: _Static
    ignored: frozenset[str]
    ignore_types: tuple[type, ...]
    formats: dict[type, Callable[[Any], Any]]
    ttl: float | None
    key_prefix: str


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


def _is_optional(tp: Any) -> bool:
    origin = typing.get_origin(tp)
    if origin is typing.Annotated:
        return _is_optional(typing.get_args(tp)[0])
    if origin in (typing.Union, types.UnionType):
        return type(None) in typing.get_args(tp)
    return tp is None or tp is type(None) or tp is Any


def _parameter_names(fn: Callable[..., Any], is_method: bool) -> list[str]:
    names = list(inspect.signature(fn).parameters)
    return names[1:] if is_method and names else names


def _annotation_type(annotation: Any) -> type | None:
    origin = typing.get_origin(annotation)
    if origin is typing.Annotated:
        return _annotation_type(typing.get_args(annotation)[0])
    if isinstance(annotation, type):
        return annotation
    return origin if isinstance(origin, type) else None


# --------------------------------------------------------------------------------------
# The decorated function
# --------------------------------------------------------------------------------------


@dataclass
class _Ctx:
    cache: Cache | None
    plan: _Plan
    session: Session | None
    key: str
    instance: Any
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    arguments: dict[str, Any]
    ancestors: frozenset[str] = frozenset()
    ns_tags: list[str] = field(default_factory=list)
    transaction: Transaction | None = None
    versions: dict[str, int] | None = None
    entry: Entry | None = None
    stale: Entry | None = None
    changed: list[str] = field(default_factory=list)


class CachedFunction(Generic[P, R]):
    """An ``async`` function wrapped by ``@cached`` or ``@dedupe``.

    It keeps the function's signature. Accessed on an instance, it becomes a
    :class:`BoundCachedFunction` with the same controls.
    """

    def __init__(self, fn: Callable[P, Awaitable[R]], options: _Options) -> None:
        if not inspect.iscoroutinefunction(fn):
            raise ConfigurationError(f"{fn.__qualname__}: only async functions can be cached")
        functools.update_wrapper(self, fn)
        self._fn = fn
        self._options = options
        self._is_method = _looks_like_method(fn)
        self._templates = tuple(TagTemplate(t) for t in options.tags)
        self._key_fn: Callable[..., Hashable] | None = None
        self._condition: Callable[..., bool] | None = None
        self._result_condition: Callable[..., bool] | None = None
        self._batch: _BatchSpec | None = None
        self._static_info: _Static | None = None
        self._plans: weakref.WeakKeyDictionary[Cache, _Plan] = weakref.WeakKeyDictionary()
        self._null_plan: _Plan | None = None
        self.name = f"{fn.__module__}.{fn.__qualname__}"
        self._check_options()
        self._check_tag_fields()
        _registry.append(self)

    # -- declaration -----------------------------------------------------------------

    def __set_name__(self, owner: type, name: str) -> None:
        self._is_method = True

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

    def key(self, fn: Callable[P, Hashable]) -> Callable[P, Hashable]:
        """Register a key function with the same signature as the cached function."""
        self._check_same_parameters(fn, "key function")
        self._key_fn = fn
        self._invalidate_plans()
        return fn

    def condition(self, fn: Callable[P, bool]) -> Callable[P, bool]:
        """Register a function deciding, from the arguments, whether to use the cache."""
        self._check_same_parameters(fn, "condition")
        self._condition = fn
        return fn

    def result_condition(
        self, fn: Callable[[R], bool] | Callable[[Any, R], bool]
    ) -> Callable[..., bool]:
        """Register a function deciding, from the result, whether to keep it."""
        expected = 2 if self._is_method else 1
        count = len(inspect.signature(fn).parameters)
        if count != expected:
            what = "(self, result)" if self._is_method else "(result)"
            raise ConfigurationError(
                f"result condition {fn.__qualname__} must take {what}, not {count} parameters"
            )
        self._result_condition = fn
        return fn

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
        if self._options.kind != "cached":
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

    # -- checks ----------------------------------------------------------------------

    def _check_options(self) -> None:
        options = self._options
        ttl = options.ttl
        if isinstance(ttl, (int, float)) and not isinstance(ttl, bool) and ttl <= 0:
            raise ConfigurationError(f"{self.name}: ttl must be positive, or None for no expiry")
        for label, value in (
            ("stale_while_revalidate", options.stale_while_revalidate),
            ("stale_if_error", options.stale_if_error),
        ):
            if value is None:
                continue
            if value <= 0:
                raise ConfigurationError(f"{self.name}: {label} must be positive")
            if ttl is None:
                raise ConfigurationError(f"{self.name}: {label} needs a ttl")
            if options.session_only:
                raise ConfigurationError(f"{self.name}: {label} doesn't apply to session_only")

    def _params(self) -> list[str]:
        return _parameter_names(self._fn, self._is_method)

    def _check_tag_fields(self) -> None:
        params = set(self._params())
        for template in self._templates:
            for name in template.fields:
                if name not in params:
                    raise ConfigurationError(
                        f"{template.template!r} references {name!r} but "
                        f"{self._fn.__name__}() has no such parameter"
                    )

    def _check_same_parameters(self, fn: Callable[..., Any], what: str) -> None:
        expected = list(inspect.signature(self._fn).parameters)
        actual = list(inspect.signature(fn).parameters)
        if expected != actual:
            raise ConfigurationError(
                f"{what} {fn.__qualname__}{inspect.signature(fn)} must have the same parameters as "
                f"{self._fn.__name__}{inspect.signature(self._fn)}"
            )

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

    def _static(self) -> _Static:
        if self._static_info is not None:
            return self._static_info
        signature = inspect.signature(self._fn)
        try:
            hints = typing.get_type_hints(self._fn)
        except Exception as exc:
            raise ConfigurationError(
                f"{self.name}: can't resolve annotations ({exc}). Define the types it uses "
                f"before the cache opens."
            ) from exc
        return_type = hints.get("return", Any)
        names = list(signature.parameters)
        self_name = names[0] if self._is_method and names else None
        params = names[1:] if self_name else names
        if self._batch is not None:
            self._check_bulk_annotations(hints)
        self._static_info = _Static(
            signature, hints, return_type, _is_optional(return_type), params, self_name
        )
        return self._static_info

    def _check_bulk_annotations(self, hints: dict[str, Any]) -> None:
        assert self._batch is not None
        spec = self._batch
        try:
            bulk_hints = typing.get_type_hints(spec.fn)
        except Exception as exc:
            raise ConfigurationError(
                f"{spec.fn.__qualname__}: can't resolve annotations ({exc})"
            ) from exc
        index = self._params().index(spec.on)
        bulk_name = spec.params[index]
        annotation = bulk_hints.get(bulk_name)
        if annotation is None:
            return
        origin = typing.get_origin(annotation) or annotation
        sequence_like = isinstance(origin, type) and issubclass(
            origin,
            (Sequence, list, tuple, set, frozenset, typing.Iterable),
        )
        if not sequence_like or (isinstance(origin, type) and issubclass(origin, (str, bytes))):
            raise ConfigurationError(
                f"{spec.fn.__qualname__}: {bulk_name!r} must be a sequence of "
                f"{spec.on!r} values, not {annotation!r}"
            )

    def _plan(self, cache: Cache | None) -> _Plan:
        if cache is None:
            if self._null_plan is None:
                self._null_plan = self._build_plan(None)
            return self._null_plan
        plan = self._plans.get(cache)
        if plan is None:
            plan = self._build_plan(cache)
            self._plans[cache] = plan
        return plan

    def _build_plan(self, cache: Cache | None) -> _Plan:
        static = self._static()
        options = self._options
        ignore_types = (*(cache.key_ignore if cache else ()), *options.key_ignore)
        formats: dict[type, Callable[[Any], Any]] = {
            **(cache.key_formats if cache else {}),
            **options.key_formats,
        }
        ignored: set[str] = set()
        for name in static.params:
            tp = _annotation_type(static.hints.get(name, inspect.Parameter.empty))
            if tp is not None and ignore_types and issubclass(tp, ignore_types):
                ignored.add(name)
        if self._key_fn is None:
            for name in static.params:
                if name in ignored:
                    continue
                annotation = static.hints.get(name, inspect.Parameter.empty)
                if not is_encodable(annotation, formats):
                    raise ConfigurationError(
                        f"{self.name}: parameter {name!r} of type {annotation!r} has no key "
                        f"encoding. Add the type to key_ignore or key_formats, or write a key "
                        f"function with @{self._fn.__name__}.key"
                    )
        if options.ttl is DEFAULT:
            ttl = cache.ttl if cache is not None else None
        else:
            ttl = options.ttl
        stale_window = options.stale_while_revalidate or options.stale_if_error
        if stale_window is not None and ttl is None:
            raise ConfigurationError(f"{self.name}: stale windows need a ttl")
        stores_in_backend = options.kind == "cached" and not options.session_only
        if cache is not None and stores_in_backend:
            try:
                cache.backend.check_type(static.return_type)
            except ConfigurationError as exc:
                raise ConfigurationError(f"{self.name}: {exc}") from exc
        code = fingerprint(self._fn, static.return_type, options.version)
        namespace = cache.namespace if cache is not None else "-"
        return _Plan(
            static=static,
            ignored=frozenset(ignored),
            ignore_types=ignore_types,
            formats=formats,
            ttl=ttl,
            key_prefix=f"{namespace}:{self.name}:{code}",
        )

    def _invalidate_plans(self) -> None:
        self._plans = weakref.WeakKeyDictionary()
        self._null_plan = None

    def validate(self, cache: Cache | None = None) -> None:
        """Run every check now. ``Cache.open()`` calls this for every decorated function."""
        self._plan(cache)

    # -- calls -----------------------------------------------------------------------

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        instance, rest = self._split_instance(args)
        return cast(R, await self._call(instance, rest, dict(kwargs)))

    def _split_instance(self, args: tuple[Any, ...]) -> tuple[Any, tuple[Any, ...]]:
        if self._is_method:
            if not args:
                raise TypeError(f"{self._fn.__qualname__}() missing 'self'")
            return args[0], args[1:]
        return _NO_INSTANCE, args

    def _full_args(self, instance: Any, args: tuple[Any, ...]) -> tuple[Any, ...]:
        return args if instance is _NO_INSTANCE else (instance, *args)

    async def _invoke(self, instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        return await self._fn(*self._full_args(instance, args), **kwargs)

    def _context(
        self, cache: Cache | None, instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> _Ctx:
        plan = self._plan(cache)
        static = plan.static
        full = self._full_args(instance, args)
        try:
            bound = static.signature.bind(*full, **kwargs)
        except TypeError as exc:
            raise TypeError(f"{self._fn.__qualname__}(): {exc}") from None
        bound.apply_defaults()
        arguments = dict(bound.arguments)
        self_value = arguments.pop(static.self_name) if static.self_name else _NO_INSTANCE
        if self._key_fn is not None:
            encoded: Any = encode_value(self._key_fn(*full, **kwargs), plan.formats)
        else:
            encoded = {
                name: encode_value(value, plan.formats)
                for name, value in arguments.items()
                if name not in plan.ignored
                and not (plan.ignore_types and isinstance(value, plan.ignore_types))
            }
        instance_key = getattr(self_value, "__cache_key__", None)
        if instance_key is not None:
            encoded = [encode_value(instance_key(), plan.formats), encoded]
        key = f"{plan.key_prefix}:{digest(encoded)}"
        ctx = _Ctx(
            cache=cache,
            plan=plan,
            session=_session_var.get(),
            key=key,
            instance=instance,
            args=args,
            kwargs=kwargs,
            arguments=arguments,
            transaction=_transaction_var.get(),
        )
        if cache is not None and self._templates:
            tags = [template.render(arguments) for template in self._templates]
            ctx.ancestors, ctx.ns_tags = namespaced_ancestors(cache, tags)
        if ctx.session is not None and ctx.session.cache is not cache:
            ctx.session = None
        if ctx.transaction is not None and ctx.transaction.cache is not cache:
            ctx.transaction = None
        return ctx

    @property
    def _stores(self) -> bool:
        return self._options.kind == "cached"

    @property
    def _uses_backend(self) -> bool:
        return self._stores and not self._options.session_only

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
        if ctx.transaction is not None and ctx.transaction.affects(ctx.ancestors):
            emit(cache, CacheEvent("bypass", self, ctx.key))
            return await self._uncached(instance, args, kwargs)

        lookup = await self._lookup(ctx)
        call = CachedCall(
            function=self,
            instance=None if instance is _NO_INSTANCE else instance,
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

    async def _lookup(self, ctx: _Ctx) -> Lookup:
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

    def _stale_allowed(self, ctx: _Ctx, window: float | None) -> bool:
        entry = ctx.stale
        if window is None or entry is None or ctx.cache is None:
            return False
        if ctx.session is not None and ctx.session.invalidated:
            prefix = f"{ctx.cache.namespace}|"
            changed = {tag.removeprefix(prefix) for tag in ctx.changed}
            if changed & ctx.session.invalidated:
                return False  # read your own writes
        limit = (entry.fresh_until if entry.fresh_until is not None else entry.stored_at) + window
        return _clock.now() <= limit

    def _remember(self, ctx: _Ctx, value: Any) -> None:
        if ctx.session is not None and self._stores:
            if ctx.transaction is not None and ctx.transaction.affects(ctx.ancestors):
                return
            ctx.session.put(ctx.key, value, ctx.ancestors)

    async def _default(self, ctx: _Ctx, lookup: Lookup) -> Any:
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

    async def _background_refresh(self, ctx: _Ctx) -> None:
        if ctx.key in _loop_registry(_inflight):
            return
        ctx.session = None
        await self._start(ctx)

    async def _run_fresh(self, ctx: _Ctx) -> Any:
        if self._uses_backend and ctx.cache is not None and ctx.versions is None:
            ctx.versions = await self._current_versions(ctx)
        value, keep = await asyncio.shield(self._start(ctx))
        if keep:
            self._remember(ctx, value)
        return value

    async def _current_versions(self, ctx: _Ctx) -> dict[str, int] | None:
        assert ctx.cache is not None
        try:
            found = await ctx.cache.backend.get_tag_versions(ctx.ns_tags)
        except Exception as exc:
            emit(ctx.cache, CacheEvent("error", self, ctx.key, error=exc))
            return None
        return dict(zip(ctx.ns_tags, found, strict=True))

    def _start(self, ctx: _Ctx) -> asyncio.Task[tuple[Any, bool]]:
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

    async def _execute(self, ctx: _Ctx) -> tuple[Any, bool]:
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
        keep = self._stores
        if keep and self._result_condition is not None:
            keep = bool(self._result_condition(*self._full_args(ctx.instance, (value,))))
        if keep and self._uses_backend and cache is not None:
            await self._store(ctx, value)
        return value, keep

    async def _store(self, ctx: _Ctx, value: Any) -> None:
        cache = ctx.cache
        assert cache is not None
        if ctx.versions is None:
            return  # tag versions unknown: storing could hide an invalidation
        if ctx.transaction is not None and ctx.transaction.affects(ctx.ancestors):
            return
        ttl = ctx.plan.ttl
        now = _clock.now()
        windows = [
            w for w in (self._options.stale_while_revalidate, self._options.stale_if_error) if w
        ]
        backend_ttl = None if ttl is None else ttl + max(windows, default=0.0)
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

    async def _submit_to_batch(self, ctx: _Ctx) -> Any:
        spec = self._batch
        assert spec is not None
        plan = ctx.plan
        others = {name: value for name, value in ctx.arguments.items() if name != spec.on}
        group = digest(
            {
                name: encode_value(value, plan.formats)
                for name, value in others.items()
                if name not in plan.ignored
                and not (plan.ignore_types and isinstance(value, plan.ignore_types))
            }
        )
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
        full = () if instance is _NO_INSTANCE else (instance,)
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

    def __repr__(self) -> str:
        return f"<{self._options.kind} {self.name}>"


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


# --------------------------------------------------------------------------------------
# Decorators
# --------------------------------------------------------------------------------------


def _looks_like_method(fn: Callable[..., Any]) -> bool:
    qualname = getattr(fn, "__qualname__", "")
    parts = qualname.split(".")
    return len(parts) > 1 and parts[-2] != "<locals>"


def _compose(middleware: Sequence[Middleware], final: Next) -> Next:
    handler = final
    for item in reversed(middleware):
        handler = _wrap(item, handler)
    return handler


def _wrap(middleware: Middleware, call_next: Next) -> Next:
    async def handler(call: CachedCall) -> Any:
        return await middleware(call, call_next)

    return handler


@overload
def cached(fn: Callable[P, Awaitable[R]], /) -> CachedFunction[P, R]: ...


@overload
def cached(
    *,
    ttl: float | _Default | None = DEFAULT,
    tags: Sequence[str] = (),
    key_ignore: Sequence[type] = (),
    key_formats: KeyFormats | None = None,
    stale_while_revalidate: float | None = None,
    stale_if_error: float | None = None,
    version: str | None = None,
    session_only: bool = False,
) -> Callable[[Callable[P, Awaitable[R]]], CachedFunction[P, R]]: ...


def cached(
    fn: Callable[P, Awaitable[R]] | None = None,
    /,
    *,
    ttl: float | _Default | None = DEFAULT,
    tags: Sequence[str] = (),
    key_ignore: Sequence[type] = (),
    key_formats: KeyFormats | None = None,
    stale_while_revalidate: float | None = None,
    stale_if_error: float | None = None,
    version: str | None = None,
    session_only: bool = False,
) -> CachedFunction[P, R] | Callable[[Callable[P, Awaitable[R]]], CachedFunction[P, R]]:
    """Reuse results for the session and in the backend.

    Args:
        ttl: seconds the backend entry stays fresh. Omitted: the cache's default.
            ``None``: no expiry (tags still invalidate it).
        tags: tag templates, ``{name}`` refers to a parameter.
        key_ignore: argument types left out of the key, on top of the cache's.
        key_formats: how a type appears in the key, on top of the cache's.
        stale_while_revalidate: seconds after expiry or invalidation during which the
            old value is served while a refresh runs in the background.
        stale_if_error: seconds after expiry or invalidation during which the old value
            is served if the function raises.
        version: bump it when something the function calls has changed.
        session_only: never use the backend: results live until the session closes.
    """
    options = _Options(
        kind="cached",
        ttl=ttl,
        tags=tuple(tags),
        key_ignore=tuple(key_ignore),
        key_formats=dict(key_formats or {}),
        stale_while_revalidate=stale_while_revalidate,
        stale_if_error=stale_if_error,
        version=version,
        session_only=session_only,
    )
    if fn is not None:
        return CachedFunction(fn, options)

    def decorator(function: Callable[P, Awaitable[R]]) -> CachedFunction[P, R]:
        return CachedFunction(function, options)

    return decorator


@overload
def dedupe(fn: Callable[P, Awaitable[R]], /) -> CachedFunction[P, R]: ...


@overload
def dedupe(
    *, key_ignore: Sequence[type] = (), key_formats: KeyFormats | None = None
) -> Callable[[Callable[P, Awaitable[R]]], CachedFunction[P, R]]: ...


def dedupe(
    fn: Callable[P, Awaitable[R]] | None = None,
    /,
    *,
    key_ignore: Sequence[type] = (),
    key_formats: KeyFormats | None = None,
) -> CachedFunction[P, R] | Callable[[Callable[P, Awaitable[R]]], CachedFunction[P, R]]:
    """Concurrent identical calls share one execution. Nothing is stored.

    Dedupe is per process and works even without an open cache.
    """
    options = _Options(
        kind="dedupe", key_ignore=tuple(key_ignore), key_formats=dict(key_formats or {})
    )
    if fn is not None:
        return CachedFunction(fn, options)

    def decorator(function: Callable[P, Awaitable[R]]) -> CachedFunction[P, R]:
        return CachedFunction(function, options)

    return decorator


def invalidates(*templates: str) -> Callable[[F], F]:
    """After the function returns, invalidate these tags (rendered from its arguments)."""
    parsed = tuple(TagTemplate(t) for t in templates)

    def decorator(fn: F) -> F:
        if not inspect.iscoroutinefunction(fn):
            raise ConfigurationError(f"{fn.__qualname__}: @invalidates needs an async function")
        signature = inspect.signature(fn)
        for template in parsed:
            for name in template.fields:
                if name not in signature.parameters:
                    raise ConfigurationError(
                        f"{template.template!r} references {name!r} but "
                        f"{fn.__name__}() has no such parameter"
                    )

        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = await fn(*args, **kwargs)
            bound = signature.bind(*args, **kwargs)
            bound.apply_defaults()
            await invalidate(*(template.render(bound.arguments) for template in parsed))
            return result

        _invalidations.append(
            InvalidationSite(f"{fn.__module__}.{fn.__qualname__}", fn.__module__, parsed)
        )
        return cast(F, wrapper)

    return decorator


def registered_functions() -> list[CachedFunction[Any, Any]]:
    return list(_registry)


def registered_invalidations() -> list[InvalidationSite]:
    return list(_invalidations)


def validate_registered(cache: Cache | None) -> None:
    """Check every decorated function against ``cache``; raise one error listing all problems."""
    problems: list[str] = []
    for function in list(_registry):
        try:
            function.validate(cache)
        except ConfigurationError as exc:
            problems.append(str(exc))
    if problems:
        raise ConfigurationError("\n".join(problems))
