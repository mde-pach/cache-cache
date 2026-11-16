import datetime as dt
import decimal
import enum
import uuid
from dataclasses import dataclass
from typing import Any, Literal

import pytest
from pydantic import BaseModel

from cache_cache.errors import KeyEncodingError
from cache_cache.keys import digest, encode_value, is_encodable


class Color(enum.Enum):
    RED = "red"


@dataclass(frozen=True)
class Query:
    text: str
    page: int


class Model(BaseModel):
    a: int


class Opaque:
    pass


def test_simple_values_encode_deterministically() -> None:
    value = {
        "s": "x",
        "i": 1,
        "f": 1.5,
        "b": True,
        "n": None,
        "bytes": b"\x00",
        "uuid": uuid.UUID(int=1),
        "dt": dt.datetime(2026, 1, 1),
        "dec": decimal.Decimal("1.10"),
        "enum": Color.RED,
        "tuple": (1, 2),
        "set": {3, 1, 2},
        "dc": Query("a", 1),
        "model": Model(a=1),
        "td": dt.timedelta(seconds=3),
    }
    assert digest(encode_value(value, {})) == digest(
        encode_value(dict(reversed(value.items())), {})
    )


def test_types_dont_collide() -> None:
    assert digest(encode_value(1, {})) != digest(encode_value("1", {}))
    assert digest(encode_value(1, {})) != digest(encode_value(1.0, {}))
    assert digest(encode_value(True, {})) != digest(encode_value(1, {}))
    assert digest(encode_value([1], {})) != digest(encode_value({1}, {}))


def test_sets_are_order_independent() -> None:
    assert encode_value({"b", "a"}, {}) == encode_value({"a", "b"}, {})


def test_unknown_types_raise() -> None:
    with pytest.raises(KeyEncodingError, match="Opaque"):
        encode_value(Opaque(), {})


def test_key_formats_apply_to_subclasses() -> None:
    formats = {decimal.Decimal: lambda d: str(d.quantize(decimal.Decimal("0.00")))}
    assert encode_value(decimal.Decimal("1.1"), formats) == encode_value(
        decimal.Decimal("1.10"), formats
    )


def test_key_format_must_change_type() -> None:
    with pytest.raises(KeyEncodingError):
        encode_value("x", {str: lambda s: s.lower()})


@pytest.mark.parametrize(
    "annotation",
    [
        int,
        str,
        int | None,
        list[int],
        dict[str, int],
        tuple[int, ...],
        Query,
        Model,
        Color,
        Literal["a"],
        Any,
        uuid.UUID,
        frozenset[str],
    ],
)
def test_encodable_annotations(annotation: object) -> None:
    assert is_encodable(annotation, {})


@pytest.mark.parametrize("annotation", [Opaque, list[Opaque], Opaque | None])
def test_unencodable_annotations(annotation: object) -> None:
    assert not is_encodable(annotation, {})
    assert is_encodable(annotation, {Opaque: lambda o: "x"}) or annotation is not Opaque
