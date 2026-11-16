"""Clocks used by the engine. Tests patch these attributes to control time."""

from __future__ import annotations

import time


def now() -> float:
    """Wall-clock time in seconds. Entries shared between processes use it."""
    return time.time()


def monotonic() -> float:
    """Monotonic time in seconds, for in-process ages."""
    return time.monotonic()
