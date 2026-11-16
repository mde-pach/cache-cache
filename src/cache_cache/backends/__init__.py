"""Storage backends."""

from .base import Backend, Entry, StoreItem
from .memory import MemoryBackend

__all__ = ["Backend", "Entry", "MemoryBackend", "RedisBackend", "StoreItem"]


def __getattr__(name: str) -> object:
    if name == "RedisBackend":
        from .redis import RedisBackend

        return RedisBackend
    raise AttributeError(name)
