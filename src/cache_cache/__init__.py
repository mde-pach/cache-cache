"""cache-cache: caching with dedupe, sessions, invisible batching and hierarchical tags.

Works on ``async def`` functions (:class:`Cache`) and plain ``def`` functions (:class:`SyncCache`).
"""

from ._base import DEFAULT
from .backends import Backend, Entry, MemoryBackend, StoreItem, SyncBackend, SyncMemoryBackend
from .cache import (
    Cache,
    Session,
    Transaction,
    configure,
    current_cache,
    current_session,
    disabled,
    invalidate,
)
from .codecs import Codec, PickleCodec, PydanticCodec
from .decorators import cached, dedupe, invalidates
from .errors import (
    CacheCacheError,
    CacheClosedError,
    ConfigurationError,
    KeyEncodingError,
    NoActiveSession,
)
from .functions import BatchedFunction, BoundBatchedFunction, BoundCachedFunction, CachedFunction
from .hooks import (
    CachedCall,
    CacheEvent,
    DontJoinOldCalls,
    FunctionInfo,
    FunctionStats,
    LoggingObserver,
    Lookup,
    Middleware,
    Next,
    Observer,
    StatsObserver,
    SyncCachedCall,
    SyncDontJoinOldCalls,
    SyncMiddleware,
    SyncNext,
)
from .sync import (
    BoundSyncCachedFunction,
    SyncCache,
    SyncCachedFunction,
    SyncTransaction,
    current_sync_cache,
    invalidate_sync,
)
from .testing import Recorder, record

__version__ = "0.2.0"

__all__ = [
    "DEFAULT",
    "Backend",
    "BatchedFunction",
    "BoundBatchedFunction",
    "BoundCachedFunction",
    "BoundSyncCachedFunction",
    "Cache",
    "CacheCacheError",
    "CacheClosedError",
    "CacheEvent",
    "CachedCall",
    "CachedFunction",
    "Codec",
    "ConfigurationError",
    "DontJoinOldCalls",
    "Entry",
    "FunctionInfo",
    "FunctionStats",
    "KeyEncodingError",
    "LoggingObserver",
    "Lookup",
    "MemoryBackend",
    "Middleware",
    "Next",
    "NoActiveSession",
    "Observer",
    "PickleCodec",
    "PydanticCodec",
    "Recorder",
    "Session",
    "StatsObserver",
    "StoreItem",
    "SyncBackend",
    "SyncCache",
    "SyncCachedCall",
    "SyncCachedFunction",
    "SyncDontJoinOldCalls",
    "SyncMemoryBackend",
    "SyncMiddleware",
    "SyncNext",
    "SyncTransaction",
    "Transaction",
    "__version__",
    "cached",
    "configure",
    "current_cache",
    "current_session",
    "current_sync_cache",
    "dedupe",
    "disabled",
    "invalidate",
    "invalidate_sync",
    "invalidates",
    "record",
]


def __getattr__(name: str) -> object:
    if name in ("RedisBackend", "SyncRedisBackend"):
        from .backends import redis

        return getattr(redis, name)
    raise AttributeError(name)
