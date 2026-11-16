"""Hierarchical tags.

A tag is a ``:``-separated path such as ``product:list:shoes``. Invalidating a tag
invalidates every tag below it: ``product`` covers ``product:42`` and
``product:list:shoes``.

Values substituted into a template are escaped, so ``{name}`` with ``name="a:b"``
stays one segment (``a\\:b``) and can't reach into another part of the tree.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from enum import Enum
from string import Formatter
from typing import Any

__all__ = ["TagTemplate", "ancestors", "escape_segment", "split_tag", "validate_tag"]


def escape_segment(value: str) -> str:
    """Escape a value so it forms exactly one tag segment."""
    return value.replace("\\", "\\\\").replace(":", "\\:")


def split_tag(tag: str) -> list[str]:
    """Split a tag on unescaped ``:``. Segments keep their escaped form."""
    segments: list[str] = []
    current: list[str] = []
    i = 0
    while i < len(tag):
        char = tag[i]
        if char == "\\" and i + 1 < len(tag):
            current.append(tag[i : i + 2])
            i += 2
            continue
        if char == ":":
            segments.append("".join(current))
            current = []
        else:
            current.append(char)
        i += 1
    segments.append("".join(current))
    return segments


def validate_tag(tag: str) -> str:
    """Return ``tag`` unchanged, or raise ``ValueError`` if it has an empty segment."""
    if not tag or any(segment == "" for segment in split_tag(tag)):
        raise ValueError(f"invalid tag {tag!r}: tags are non-empty ':'-separated segments")
    return tag


def ancestors(tag: str) -> list[str]:
    """``"a:b:c"`` -> ``["a", "a:b", "a:b:c"]``."""
    segments = split_tag(tag)
    return [":".join(segments[: i + 1]) for i in range(len(segments))]


def ancestors_of_all(tags: Iterable[str]) -> list[str]:
    """Every ancestor of every tag, without duplicates, in a stable order."""
    seen: dict[str, None] = {}
    for tag in tags:
        for ancestor in ancestors(tag):
            seen[ancestor] = None
    return list(seen)


class _EscapingFormatter(Formatter):
    def format_field(self, value: Any, format_spec: str) -> str:
        if isinstance(value, Enum):
            value = value.value
        return escape_segment(format(value, format_spec))


_FORMATTER = _EscapingFormatter()
_PLACEHOLDER = "\x00"


class TagTemplate:
    """A tag with ``{name}`` placeholders that refer to the function's parameters.

    Attribute and item access work: ``"order:{order.id}"``.
    """

    __slots__ = ("fields", "template")

    def __init__(self, template: str) -> None:
        self.template = template
        fields: list[str] = []
        for _literal, field, _spec, _conversion in Formatter().parse(template):
            if field is None:
                continue
            root = re.split(r"[.\[]", field, maxsplit=1)[0]
            if not root or root.isdigit():
                raise ValueError(
                    f"tag {template!r}: placeholders must name a parameter, like '{{pid}}'"
                )
            fields.append(root)
        self.fields: tuple[str, ...] = tuple(fields)
        validate_tag(self.pattern())

    def render(self, arguments: Mapping[str, Any]) -> str:
        """Fill the placeholders from the bound arguments of a call."""
        return validate_tag(_FORMATTER.vformat(self.template, (), dict(arguments)))

    def pattern(self) -> str:
        """The template with each placeholder replaced by a marker character."""
        parts: list[str] = []
        for literal, field, _spec, _conversion in Formatter().parse(self.template):
            parts.append(literal)
            if field is not None:
                parts.append(_PLACEHOLDER)
        return "".join(parts)

    def segments(self) -> list[str | None]:
        """Segments of the template; ``None`` where a segment contains a placeholder."""
        return [None if _PLACEHOLDER in s else s for s in split_tag(self.pattern())]

    def __repr__(self) -> str:
        return f"TagTemplate({self.template!r})"
