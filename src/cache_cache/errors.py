"""Exceptions raised by cache-cache."""

from __future__ import annotations


class CacheCacheError(Exception):
    """Base class for every error raised by cache-cache."""


class ConfigurationError(CacheCacheError, TypeError):
    """A decorated function, tag, condition or key function is declared incorrectly.

    Raised when the cache opens (or on first use), and by ``cache-cache check``.
    """


class KeyEncodingError(ConfigurationError):
    """An argument value has no deterministic key encoding."""


class NoActiveSession(CacheCacheError, RuntimeError):
    """A cached function was called outside a session while ``strict=True``."""


class CacheClosedError(CacheCacheError, RuntimeError):
    """A session or transaction was opened on a cache that is not open."""
