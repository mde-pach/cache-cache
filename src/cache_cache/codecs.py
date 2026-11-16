"""Codecs turn values into bytes for backends that store bytes (such as Redis).

A codec receives the decorated function's return annotation, so it can decode into
the right type. :meth:`Codec.check` runs when the cache opens and rejects return
types the codec can't handle.
"""

from __future__ import annotations

import pickle
from typing import Any, Protocol, runtime_checkable

from .errors import ConfigurationError

__all__ = ["Codec", "PickleCodec", "PydanticCodec"]


@runtime_checkable
class Codec(Protocol):
    def encode(self, value: Any, tp: Any) -> bytes: ...

    def decode(self, data: bytes, tp: Any) -> Any: ...

    def check(self, tp: Any) -> None:
        """Raise :class:`ConfigurationError` if values of type ``tp`` can't be encoded."""
        ...


class PickleCodec:
    """Pickle. Handles almost anything, but only share a backend between trusted processes."""

    def __init__(self, protocol: int = pickle.HIGHEST_PROTOCOL) -> None:
        self.protocol = protocol

    def encode(self, value: Any, tp: Any) -> bytes:
        return pickle.dumps(value, protocol=self.protocol)

    def decode(self, data: bytes, tp: Any) -> Any:
        return pickle.loads(data)

    def check(self, tp: Any) -> None:
        return None


class PydanticCodec:
    """JSON through a pydantic ``TypeAdapter`` built from the return annotation.

    Values are validated when decoded, so an entry that no longer matches the type
    is treated as a miss rather than returned.
    """

    def __init__(self) -> None:
        try:
            from pydantic import TypeAdapter
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "PydanticCodec needs pydantic: pip install 'cache-cache[pydantic]'"
            ) from exc
        self._type_adapter = TypeAdapter
        self._adapters: dict[Any, Any] = {}

    def _adapter(self, tp: Any) -> Any:
        try:
            return self._adapters[tp]
        except (KeyError, TypeError):
            adapter = self._type_adapter(tp)
            try:
                self._adapters[tp] = adapter
            except TypeError:  # pragma: no cover - unhashable annotation
                pass
            return adapter

    def encode(self, value: Any, tp: Any) -> bytes:
        return bytes(self._adapter(tp).dump_json(value))

    def decode(self, data: bytes, tp: Any) -> Any:
        return self._adapter(tp).validate_json(data)

    def check(self, tp: Any) -> None:
        try:
            self._adapter(tp)
        except Exception as exc:
            raise ConfigurationError(f"PydanticCodec can't encode {tp!r}: {exc}") from exc
