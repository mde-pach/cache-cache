"""Deterministic cache keys.

Keys are shared between processes, so they can't use ``hash()``: it is randomized
for strings, and identity-based for most objects. Every argument is turned into a
canonical JSON structure instead, and the key is a digest of that structure.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import decimal
import enum
import hashlib
import inspect
import json
import types
import typing
import uuid
from collections.abc import Callable, Mapping
from typing import Any

from .errors import KeyEncodingError

__all__ = ["KeyFormats", "digest", "encode_value", "is_encodable"]

KeyFormats = Mapping[type, Callable[[Any], Any]]

_SIMPLE_TYPES: tuple[type, ...] = (
    type(None),
    bool,
    int,
    float,
    str,
    bytes,
    uuid.UUID,
    dt.datetime,
    dt.date,
    dt.time,
    dt.timedelta,
    decimal.Decimal,
)


def _pydantic_base() -> type | None:
    try:
        from pydantic import BaseModel
    except ImportError:  # pragma: no cover - pydantic is optional
        return None
    return BaseModel


def _format_for(tp: type, formats: KeyFormats) -> Callable[[Any], Any] | None:
    for base in tp.__mro__:
        if base in formats:
            return formats[base]
    return None


def encode_value(value: Any, formats: KeyFormats) -> Any:
    """Turn ``value`` into a canonical, JSON-serializable structure.

    Raises :class:`KeyEncodingError` when there is no deterministic encoding.
    """
    custom = _format_for(type(value), formats)
    if custom is not None:
        result = custom(value)
        if type(result) is type(value):
            raise KeyEncodingError(
                f"key format for {type(value).__qualname__} must return another type"
            )
        return ["fmt", encode_value(result, formats)]
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, enum.Enum):
        return ["enum", type(value).__qualname__, encode_value(value.value, formats)]
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return ["float", repr(value)]
    if isinstance(value, bytes):
        return ["bytes", value.hex()]
    if isinstance(value, uuid.UUID):
        return ["uuid", str(value)]
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return [type(value).__name__, value.isoformat()]
    if isinstance(value, dt.timedelta):
        return ["timedelta", value.total_seconds()]
    if isinstance(value, decimal.Decimal):
        return ["decimal", str(value)]
    if isinstance(value, (list, tuple)):
        return ["seq", [encode_value(item, formats) for item in value]]
    if isinstance(value, (set, frozenset)):
        items = [encode_value(item, formats) for item in value]
        return ["set", sorted(items, key=_canonical)]
    if isinstance(value, Mapping):
        pairs = [[encode_value(k, formats), encode_value(v, formats)] for k, v in value.items()]
        return ["map", sorted(pairs, key=lambda pair: _canonical(pair[0]))]
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return [
            "dataclass",
            type(value).__qualname__,
            {
                f.name: encode_value(getattr(value, f.name), formats)
                for f in dataclasses.fields(value)
            },
        ]
    base_model = _pydantic_base()
    if base_model is not None and isinstance(value, base_model):
        dumped = value.model_dump(mode="json")  # type: ignore[attr-defined]
        return ["model", type(value).__qualname__, encode_value(dumped, formats)]
    raise KeyEncodingError(
        f"no key encoding for {type(value).__qualname__}: add it to key_formats, "
        f"key_ignore, or write a key function"
    )


def _canonical(encoded: Any) -> str:
    return json.dumps(encoded, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(encoded: Any) -> str:
    """A short, stable digest of an encoded structure."""
    return hashlib.sha256(_canonical(encoded).encode()).hexdigest()[:32]


def is_encodable(annotation: Any, formats: KeyFormats) -> bool:
    """Whether arguments annotated with ``annotation`` can always be key-encoded.

    Unannotated parameters and ``Any`` return ``True``: they are checked per call.
    """
    if annotation is inspect.Parameter.empty or annotation is Any:
        return True
    if isinstance(annotation, typing.TypeVar):
        return True
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin is typing.Annotated:
        return is_encodable(args[0], formats)
    if origin is typing.Literal:
        return True
    if origin in (typing.Union, types.UnionType):
        return all(is_encodable(arg, formats) for arg in args)
    if origin is not None:
        if isinstance(origin, type) and _format_for(origin, formats) is not None:
            return True
        container_ok = isinstance(origin, type) and issubclass(
            origin,
            (tuple, list, set, frozenset, dict, Mapping, typing.Sequence),
        )
        if not container_ok:
            return False
        return all(arg is Ellipsis or is_encodable(arg, formats) for arg in args)
    if not isinstance(annotation, type):
        return False
    if _format_for(annotation, formats) is not None:
        return True
    if issubclass(annotation, _SIMPLE_TYPES) or issubclass(annotation, enum.Enum):
        return True
    if dataclasses.is_dataclass(annotation):
        return True
    base_model = _pydantic_base()
    if base_model is not None and issubclass(annotation, base_model):
        return True
    return issubclass(annotation, (tuple, list, set, frozenset, dict))
