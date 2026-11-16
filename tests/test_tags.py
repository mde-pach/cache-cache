from enum import Enum

import pytest

from cache_cache.tags import TagTemplate, ancestors, escape_segment, split_tag, validate_tag


def test_ancestors() -> None:
    assert ancestors("product:list:shoes") == ["product", "product:list", "product:list:shoes"]


def test_escaped_values_stay_one_segment() -> None:
    tag = TagTemplate("user:{name}").render({"name": "a:b"})
    assert tag == "user:a\\:b"
    assert split_tag(tag) == ["user", "a\\:b"]
    assert ancestors(tag) == ["user", tag]


def test_backslashes_are_escaped() -> None:
    assert escape_segment("a\\:b") == "a\\\\\\:b"


def test_enum_values_render_by_value() -> None:
    class Kind(Enum):
        A = "a"

    assert TagTemplate("k:{kind}").render({"kind": Kind.A}) == "k:a"


def test_attribute_access() -> None:
    class Order:
        id = 7

    template = TagTemplate("order:{order.id}")
    assert template.fields == ("order",)
    assert template.render({"order": Order()}) == "order:7"


def test_segments_mark_placeholders() -> None:
    assert TagTemplate("product:list:{category}").segments() == ["product", "list", None]


@pytest.mark.parametrize("bad", ["", "a::b", ":a", "a:"])
def test_invalid_tags(bad: str) -> None:
    with pytest.raises(ValueError):
        validate_tag(bad)


def test_positional_placeholders_rejected() -> None:
    with pytest.raises(ValueError):
        TagTemplate("product:{}")
