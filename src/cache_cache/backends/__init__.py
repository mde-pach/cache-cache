"""Storage backends."""

from .base import (
    Backend,
    DeploymentRegistry,
    Entry,
    StoreItem,
    SyncBackend,
    SyncDeploymentRegistry,
)
from .memory import MemoryBackend, SyncMemoryBackend

__all__ = [
    "Backend",
    "DeploymentRegistry",
    "Entry",
    "MemoryBackend",
    "RedisBackend",
    "StoreItem",
    "SyncBackend",
    "SyncDeploymentRegistry",
    "SyncMemoryBackend",
    "SyncRedisBackend",
]


def __getattr__(name: str) -> object:
    if name in ("RedisBackend", "SyncRedisBackend"):
        from . import redis

        return getattr(redis, name)
    raise AttributeError(name)
