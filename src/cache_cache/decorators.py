"""``@cached``, ``@dedupe`` and ``@invalidates``, for both ``async def`` and ``def`` functions."""

from __future__ import annotations

import functools
import inspect
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, ParamSpec, TypeVar, cast, overload

from ._base import (
    DEFAULT,
    InvalidationSite,
    Options,
    _Default,
    _invalidations,
    check_template_fields,
)
from .cache import invalidate
from .functions import CachedFunction
from .keys import KeyFormats
from .sync import SyncCachedFunction, invalidate_sync
from .tags import TagTemplate

__all__ = ["cached", "dedupe", "invalidates"]

P = ParamSpec("P")
R = TypeVar("R")
F = TypeVar("F", bound=Callable[..., Any])


def _make(fn: Callable[..., Any], options: Options) -> Any:
    if inspect.iscoroutinefunction(fn):
        return CachedFunction(fn, options)
    if not callable(fn):
        raise TypeError(f"expected a function, got {fn!r}")
    return SyncCachedFunction(fn, options)


class _Decorator:
    """What ``@cached(...)`` and ``@dedupe(...)`` return."""

    def __init__(self, options: Options) -> None:
        self._options = options

    @overload
    def __call__(self, fn: Callable[P, Awaitable[R]], /) -> CachedFunction[P, R]: ...  # type: ignore[overload-overlap]

    @overload
    def __call__(self, fn: Callable[P, R], /) -> SyncCachedFunction[P, R]: ...

    def __call__(self, fn: Callable[..., Any], /) -> Any:
        return _make(fn, self._options)


# An ``async def`` matches the first overload, a plain ``def`` the second.
@overload
def cached(fn: Callable[P, Awaitable[R]], /) -> CachedFunction[P, R]: ...  # type: ignore[overload-overlap]


@overload
def cached(fn: Callable[P, R], /) -> SyncCachedFunction[P, R]: ...


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
) -> _Decorator: ...


def cached(
    fn: Callable[..., Any] | None = None,
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
) -> Any:
    """Reuse results for the session and in the backend.

    Works on ``async def`` functions (with :class:`~cache_cache.Cache`) and on plain
    ``def`` functions (with :class:`~cache_cache.SyncCache`).

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
        version: bump it when this function's results change shape or meaning, to
            stop reading entries written before.
        session_only: never use the backend: results live until the session closes.
    """
    options = Options(
        mode="cached",
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
        return _make(fn, options)
    return _Decorator(options)


@overload
def dedupe(fn: Callable[P, Awaitable[R]], /) -> CachedFunction[P, R]: ...  # type: ignore[overload-overlap]


@overload
def dedupe(fn: Callable[P, R], /) -> SyncCachedFunction[P, R]: ...


@overload
def dedupe(
    *, key_ignore: Sequence[type] = (), key_formats: KeyFormats | None = None
) -> _Decorator: ...


def dedupe(
    fn: Callable[..., Any] | None = None,
    /,
    *,
    key_ignore: Sequence[type] = (),
    key_formats: KeyFormats | None = None,
) -> Any:
    """Concurrent identical calls share one execution. Nothing is stored.

    Async functions share across tasks, sync functions across threads. Dedupe is
    per process and works even without an open cache.
    """
    options = Options(
        mode="dedupe", key_ignore=tuple(key_ignore), key_formats=dict(key_formats or {})
    )
    if fn is not None:
        return _make(fn, options)
    return _Decorator(options)


def invalidates(*templates: str) -> Callable[[F], F]:
    """After the function returns, invalidate these tags (rendered from its arguments).

    Works on ``async def`` and ``def`` functions.
    """
    parsed = tuple(TagTemplate(t) for t in templates)

    def decorator(fn: F) -> F:
        signature = inspect.signature(fn)
        check_template_fields(parsed, fn, list(signature.parameters))

        def tags_for(args: tuple[Any, ...], kwargs: dict[str, Any]) -> list[str]:
            bound = signature.bind(*args, **kwargs)
            bound.apply_defaults()
            return [template.render(bound.arguments) for template in parsed]

        wrapper: Callable[..., Any]
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                result = await fn(*args, **kwargs)
                await invalidate(*tags_for(args, kwargs))
                return result

            wrapper = async_wrapper
        else:

            @functools.wraps(fn)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                result = fn(*args, **kwargs)
                invalidate_sync(*tags_for(args, kwargs))
                return result

            wrapper = sync_wrapper

        _invalidations.append(
            InvalidationSite(f"{fn.__module__}.{fn.__qualname__}", fn.__module__, parsed)
        )
        return cast(F, wrapper)

    return decorator
