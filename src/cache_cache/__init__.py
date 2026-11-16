"""cache-cache: async caching with dedupe, sessions, invisible batching and hierarchical tags."""

from .backends import Backend, Entry, MemoryBackend, StoreItem
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
from .errors import (
    CacheCacheError,
    CacheClosedError,
    ConfigurationError,
    KeyEncodingError,
    NoActiveSession,
)
from .functions import (
    DEFAULT,
    BatchedFunction,
    BoundBatchedFunction,
    BoundCachedFunction,
    CachedFunction,
    cached,
    dedupe,
    invalidates,
)
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
)
from .testing import Recorder, record

__version__ = "0.1.0"

__all__ = [
    "DEFAULT",
    "Backend",
    "BatchedFunction",
    "BoundBatchedFunction",
    "BoundCachedFunction",
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
    "Transaction",
    "__version__",
    "cached",
    "configure",
    "current_cache",
    "current_session",
    "dedupe",
    "disabled",
    "invalidate",
    "invalidates",
    "record",
]


def __getattr__(name: str) -> object:
    if name == "RedisBackend":
        from .backends.redis import RedisBackend

        return RedisBackend
    raise AttributeError(name)
