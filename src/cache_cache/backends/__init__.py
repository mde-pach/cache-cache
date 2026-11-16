"""Storage backends."""

from .base import Backend, Entry, StoreItem, SyncBackend
from .memory import MemoryBackend, SyncMemoryBackend

__all__ = [
    "Backend",
    "Entry",
    "MemoryBackend",
    "RedisBackend",
    "StoreItem",
    "SyncBackend",
    "SyncMemoryBackend",
    "SyncRedisBackend",
]


def __getattr__(name: str) -> object:
    if name in ("RedisBackend", "SyncRedisBackend"):
        from . import redis

        return getattr(redis, name)
    raise AttributeError(name)
