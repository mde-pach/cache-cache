"""``cache-cache check``: static checks for CI.

* every decorated function is valid (tag placeholders, key and condition
  signatures, key encodings, bulk signatures);
* every invalidated tag matches a declared tag (with a "did you mean");
* warns about declared tags that nothing invalidates.
"""

from __future__ import annotations

import argparse
import ast
import difflib
import importlib
import inspect
import pkgutil
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from types import ModuleType

from ._base import registered_functions, registered_invalidations
from .errors import ConfigurationError
from .tags import TagTemplate

__all__ = ["CheckReport", "main", "run_check"]


@dataclass
class CheckReport:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def format(self) -> str:
        lines = [f"error: {e}" for e in self.errors] + [f"warning: {w}" for w in self.warnings]
        if not lines:
            lines.append("ok")
        return "\n".join(lines)


def _import_tree(name: str) -> list[ModuleType]:
    module = importlib.import_module(name)
    modules = [module]
    path = getattr(module, "__path__", None)
    if path is not None:
        for info in pkgutil.walk_packages(path, prefix=f"{name}."):
            modules.append(importlib.import_module(info.name))
    return modules


def _in_scope(module: str, roots: Sequence[str]) -> bool:
    return any(module == root or module.startswith(f"{root}.") for root in roots)


def _matches(invalidated: list[str | None], declared: list[str | None]) -> bool:
    """Whether an invalidation is a prefix of a declaration (placeholders match anything)."""
    if len(invalidated) > len(declared):
        return False
    return all(
        a is None or b is None or a == b for a, b in zip(invalidated, declared, strict=False)
    )


def _literal_invalidations(module: ModuleType) -> list[TagTemplate]:
    """String literals passed to ``invalidate(...)`` calls in the module's source."""
    try:
        tree = ast.parse(inspect.getsource(module))
    except (OSError, TypeError, SyntaxError):
        return []
    found: list[TagTemplate] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
        if name not in ("invalidate", "invalidate_sync"):
            continue
        for arg in node.args:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                try:
                    found.append(TagTemplate(arg.value))
                except ValueError:
                    continue
    return found


def run_check(*roots: str) -> CheckReport:
    """Import ``roots`` (and their submodules) and check what they declare."""
    report = CheckReport()
    modules: list[ModuleType] = []
    for root in roots:
        try:
            modules.extend(_import_tree(root))
        except Exception as exc:
            report.errors.append(f"can't import {root}: {exc!r}")
    functions = [f for f in registered_functions() if _in_scope(f.__module__, roots)]
    for function in functions:
        try:
            function.validate(None)
        except ConfigurationError as exc:
            report.errors.append(str(exc))

    declared: list[TagTemplate] = [t for f in functions for t in f._templates]
    invalidations: list[tuple[str, TagTemplate]] = [
        (site.name, t)
        for site in registered_invalidations()
        if _in_scope(site.module, roots)
        for t in site.templates
    ]
    for module in modules:
        invalidations.extend((module.__name__, t) for t in _literal_invalidations(module))

    declared_patterns = sorted({t.template for t in declared})
    for where, template in invalidations:
        if any(_matches(template.segments(), d.segments()) for d in declared):
            continue
        hint = difflib.get_close_matches(template.template, declared_patterns, n=1)
        suggestion = f', did you mean "{hint[0]}"?' if hint else ""
        report.errors.append(
            f'{where}: invalidates("{template.template}") matches no declared tag{suggestion}'
        )
    for template in declared:
        if not any(_matches(inv.segments(), template.segments()) for _w, inv in invalidations):
            report.warnings.append(f'tag "{template.template}" is never invalidated')
    report.warnings = list(dict.fromkeys(report.warnings))
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cache-cache")
    sub = parser.add_subparsers(dest="command", required=True)
    check = sub.add_parser("check", help="check decorated functions and tags")
    check.add_argument("modules", nargs="+", help="modules or packages to import")
    check.add_argument("--strict", action="store_true", help="treat warnings as errors")
    args = parser.parse_args(argv)
    sys.path.insert(0, "")
    report = run_check(*args.modules)
    print(report.format())
    failed = not report.ok or (args.strict and report.warnings)
    return 1 if failed else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
