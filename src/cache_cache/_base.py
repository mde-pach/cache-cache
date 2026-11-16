"""What the async and sync engines share: declaration, checks, plans and keys."""

from __future__ import annotations

import enum
import functools
import inspect
import types
import typing
import weakref
from collections.abc import Callable, Hashable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, Literal, ParamSpec, TypeVar

from . import _clock
from .backends.base import Entry
from .cache import CacheBase, Session, _session_var, _transaction_var, namespaced_ancestors
from .errors import ConfigurationError
from .keys import KeyFormats, digest, encode_value, is_encodable
from .tags import TagTemplate

if TYPE_CHECKING:  # pragma: no cover
    from .cache import TransactionBase

P = ParamSpec("P")
R = TypeVar("R")


class _Default(enum.Enum):
    DEFAULT = "DEFAULT"


DEFAULT: Literal[_Default.DEFAULT] = _Default.DEFAULT
"""Sentinel: use the cache's default ``ttl``."""

NO_INSTANCE: Any = object()

Kind = Literal["async", "sync"]

_registry: list[FunctionBase[Any, Any]] = []
_invalidations: list[InvalidationSite] = []


@dataclass(frozen=True)
class InvalidationSite:
    """Where ``@invalidates`` was used. Read by ``cache-cache check``."""

    name: str
    module: str
    templates: tuple[TagTemplate, ...]


@dataclass(frozen=True)
class Options:
    mode: Literal["cached", "dedupe"]
    ttl: float | _Default | None = DEFAULT
    tags: tuple[str, ...] = ()
    key_ignore: tuple[type, ...] = ()
    key_formats: KeyFormats = field(default_factory=dict)
    stale_while_revalidate: float | None = None
    stale_if_error: float | None = None
    version: str | None = None
    session_only: bool = False


@dataclass
class Static:
    """What can be known from the function alone, once its annotations resolve."""

    signature: inspect.Signature
    hints: dict[str, Any]
    return_type: Any
    optional: bool
    params: list[str]
    self_name: str | None


@dataclass
class Plan:
    """The function resolved against one cache."""

    static: Static
    ignored: frozenset[str]
    ignore_types: tuple[type, ...]
    formats: dict[type, Callable[[Any], Any]]
    ttl: float | None
    key_prefix: str


@dataclass
class Ctx:
    """One call, resolved: its key, its tags and what the lookup found."""

    cache: Any
    plan: Plan
    session: Session | None
    key: str
    instance: Any
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    arguments: dict[str, Any]
    ancestors: frozenset[str] = frozenset()
    ns_tags: list[str] = field(default_factory=list)
    transaction: TransactionBase | None = None
    versions: dict[str, int] | None = None
    entry: Entry | None = None
    stale: Entry | None = None
    changed: list[str] = field(default_factory=list)


def is_optional(tp: Any) -> bool:
    origin = typing.get_origin(tp)
    if origin is typing.Annotated:
        return is_optional(typing.get_args(tp)[0])
    if origin in (typing.Union, types.UnionType):
        return type(None) in typing.get_args(tp)
    return tp is None or tp is type(None) or tp is Any


def _annotation_type(annotation: Any) -> type | None:
    origin = typing.get_origin(annotation)
    if origin is typing.Annotated:
        return _annotation_type(typing.get_args(annotation)[0])
    if isinstance(annotation, type):
        return annotation
    return origin if isinstance(origin, type) else None


def looks_like_method(fn: Callable[..., Any]) -> bool:
    parts = getattr(fn, "__qualname__", "").split(".")
    return len(parts) > 1 and parts[-2] != "<locals>"


def check_template_fields(
    templates: Sequence[TagTemplate], fn: Callable[..., Any], params: Sequence[str]
) -> None:
    for template in templates:
        for name in template.fields:
            if name not in params:
                raise ConfigurationError(
                    f"{template.template!r} references {name!r} but "
                    f"{fn.__name__}() has no such parameter"
                )


class FunctionBase(Generic[P, R]):
    """Declaration and key building for a decorated function, whatever its engine."""

    kind: Kind

    def __init__(self, fn: Callable[..., Any], options: Options) -> None:
        functools.update_wrapper(typing.cast(Any, self), fn)
        self._fn = fn
        self._options = options
        self._is_method = looks_like_method(fn)
        self._templates = tuple(TagTemplate(t) for t in options.tags)
        self._key_fn: Callable[..., Hashable] | None = None
        self._condition: Callable[..., bool] | None = None
        self._result_condition: Callable[..., bool] | None = None
        self._static_info: Static | None = None
        self._plans: weakref.WeakKeyDictionary[CacheBase, Plan] = weakref.WeakKeyDictionary()
        self._null_plan: Plan | None = None
        self.name = f"{fn.__module__}.{fn.__qualname__}"
        self._check_options()
        check_template_fields(self._templates, fn, self._params())
        _registry.append(self)

    # -- declaration -----------------------------------------------------------------

    def __set_name__(self, owner: type, name: str) -> None:
        self._is_method = True

    def key(self, fn: Callable[P, Hashable]) -> Callable[P, Hashable]:
        """Register a key function with the same signature as the cached function."""
        self._check_same_parameters(fn, "key function")
        self._key_fn = fn
        self._plans = weakref.WeakKeyDictionary()
        self._null_plan = None
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
        names = list(inspect.signature(self._fn).parameters)
        return names[1:] if self._is_method and names else names

    def _check_same_parameters(self, fn: Callable[..., Any], what: str) -> None:
        expected = list(inspect.signature(self._fn).parameters)
        actual = list(inspect.signature(fn).parameters)
        if expected != actual:
            raise ConfigurationError(
                f"{what} {fn.__qualname__}{inspect.signature(fn)} must have the same parameters as "
                f"{self._fn.__name__}{inspect.signature(self._fn)}"
            )

    def _extra_static_checks(self, hints: dict[str, Any]) -> None:
        """Engine-specific checks that need resolved annotations."""

    def _static(self) -> Static:
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
        self._extra_static_checks(hints)
        self._static_info = Static(
            signature, hints, return_type, is_optional(return_type), params, self_name
        )
        return self._static_info

    def _plan(self, cache: CacheBase | None) -> Plan:
        if cache is None:
            if self._null_plan is None:
                self._null_plan = self._build_plan(None)
            return self._null_plan
        plan = self._plans.get(cache)
        if plan is None:
            plan = self._build_plan(cache)
            self._plans[cache] = plan
        return plan

    def _build_plan(self, cache: CacheBase | None) -> Plan:
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
        ttl = (cache.ttl if cache is not None else None) if options.ttl is DEFAULT else options.ttl
        if (options.stale_while_revalidate or options.stale_if_error) and ttl is None:
            raise ConfigurationError(f"{self.name}: stale windows need a ttl")
        if cache is not None and self._uses_backend:
            try:
                cache.backend.check_type(static.return_type)
            except ConfigurationError as exc:
                raise ConfigurationError(f"{self.name}: {exc}") from exc
        if cache is None:
            prefix = f"-:-:{self.name}"
        else:
            prefix = f"{cache.namespace}:{cache.version or '-'}:{self.name}"
        return Plan(
            static=static,
            ignored=frozenset(ignored),
            ignore_types=ignore_types,
            formats=formats,
            ttl=ttl,
            key_prefix=f"{prefix}:{options.version or '-'}",
        )

    def validate(self, cache: CacheBase | None = None) -> None:
        """Run every check now. Opening a cache calls this for every decorated function."""
        self._plan(cache)

    # -- calls -----------------------------------------------------------------------

    @property
    def _stores(self) -> bool:
        return self._options.mode == "cached"

    @property
    def _uses_backend(self) -> bool:
        return self._stores and not self._options.session_only

    def _split_instance(self, args: tuple[Any, ...]) -> tuple[Any, tuple[Any, ...]]:
        if self._is_method:
            if not args:
                raise TypeError(f"{self._fn.__qualname__}() missing 'self'")
            return args[0], args[1:]
        return NO_INSTANCE, args

    def _full_args(self, instance: Any, args: tuple[Any, ...]) -> tuple[Any, ...]:
        return args if instance is NO_INSTANCE else (instance, *args)

    def _encode_arguments(self, plan: Plan, arguments: Mapping[str, Any]) -> dict[str, Any]:
        return {
            name: encode_value(value, plan.formats)
            for name, value in arguments.items()
            if name not in plan.ignored
            and not (plan.ignore_types and isinstance(value, plan.ignore_types))
        }

    def _context(
        self, cache: CacheBase | None, instance: Any, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Ctx:
        plan = self._plan(cache)
        static = plan.static
        full = self._full_args(instance, args)
        try:
            bound = static.signature.bind(*full, **kwargs)
        except TypeError as exc:
            raise TypeError(f"{self._fn.__qualname__}(): {exc}") from None
        bound.apply_defaults()
        arguments = dict(bound.arguments)
        self_value = arguments.pop(static.self_name) if static.self_name else NO_INSTANCE
        if self._key_fn is not None:
            encoded: Any = encode_value(self._key_fn(*full, **kwargs), plan.formats)
        else:
            encoded = self._encode_arguments(plan, arguments)
        instance_key = getattr(self_value, "__cache_key__", None)
        if instance_key is not None:
            encoded = [encode_value(instance_key(), plan.formats), encoded]
        session = _session_var.get()
        transaction = _transaction_var.get()
        ctx = Ctx(
            cache=cache,
            plan=plan,
            session=session if session is not None and session.cache is cache else None,
            key=f"{plan.key_prefix}:{digest(encoded)}",
            instance=instance,
            args=args,
            kwargs=kwargs,
            arguments=arguments,
            transaction=(
                transaction if transaction is not None and transaction.cache is cache else None
            ),
        )
        if cache is not None:
            tags = [template.render(arguments) for template in self._templates]
            ctx.ancestors, ns_tags = namespaced_ancestors(cache, tags)
            ctx.ns_tags = [cache.generation_tag, *ns_tags]
        return ctx

    def _stale_allowed(self, ctx: Ctx, window: float | None) -> bool:
        entry = ctx.stale
        cache = ctx.cache
        if window is None or entry is None or cache is None:
            return False
        if cache.generation_tag in ctx.changed:
            return False  # the cache was cleared
        if ctx.session is not None and ctx.session.invalidated:
            prefix = f"{cache.namespace}|"
            changed = {tag.removeprefix(prefix) for tag in ctx.changed}
            if changed & ctx.session.invalidated:
                return False  # read your own writes
        limit = (entry.fresh_until if entry.fresh_until is not None else entry.stored_at) + window
        return _clock.now() <= limit

    def _blocked_by_transaction(self, ctx: Ctx) -> bool:
        return ctx.transaction is not None and ctx.transaction.affects(ctx.ancestors)

    def _remember(self, ctx: Ctx, value: Any) -> None:
        if ctx.session is not None and self._stores and not self._blocked_by_transaction(ctx):
            ctx.session.put(ctx.key, value, ctx.ancestors)

    def _keep(self, ctx: Ctx, value: Any) -> bool:
        if not self._stores:
            return False
        if self._result_condition is None:
            return True
        return bool(self._result_condition(*self._full_args(ctx.instance, (value,))))

    def _backend_ttl(self, ttl: float | None) -> float | None:
        if ttl is None:
            return None
        windows = [self._options.stale_while_revalidate, self._options.stale_if_error]
        return ttl + max((w for w in windows if w), default=0.0)

    def __repr__(self) -> str:
        return f"<{self.kind} {self._options.mode} {self.name}>"


def registered_functions() -> list[FunctionBase[Any, Any]]:
    return list(_registry)


def registered_invalidations() -> list[InvalidationSite]:
    return list(_invalidations)


def validate_registered(cache: CacheBase | None, kind: Kind | None = None) -> None:
    """Check decorated functions (of one engine) against ``cache``; report every problem."""
    problems: list[str] = []
    for function in list(_registry):
        if kind is not None and function.kind != kind:
            continue
        try:
            function.validate(cache)
        except ConfigurationError as exc:
            problems.append(str(exc))
    if problems:
        raise ConfigurationError("\n".join(problems))
