"""Which deployment this process belongs to.

Entries are scoped to a deployment: a new deploy never reads entries written by a
previous one, even when the same code is deployed again. A restart within the same
deployment (a crash, a scale-up) keeps the scope, so the cache survives it.
"""

from __future__ import annotations

import hashlib
import logging
import os
import socket
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

__all__ = ["DEPLOYMENT_ENV_VARS", "Deployment", "DeploymentSetting", "resolve_deployment"]

logger = logging.getLogger("cache_cache")

DeploymentSetting = str | None
"""``"auto"`` (default), ``"process"``, any other string (the ID itself), or ``None`` (unscoped)."""

DEPLOYMENT_ENV_VARS: tuple[tuple[str, str], ...] = (
    ("CACHE_CACHE_DEPLOYMENT", "CACHE_CACHE_DEPLOYMENT"),
    ("VERCEL_DEPLOYMENT_ID", "Vercel"),
    ("RAILWAY_DEPLOYMENT_ID", "Railway"),
    ("HEROKU_RELEASE_VERSION", "Heroku (dyno metadata)"),
    ("FLY_IMAGE_REF", "Fly.io"),
    ("K_REVISION", "Cloud Run / Knative"),
)
"""Environment variables read by ``deployment="auto"``, in order.

Each one changes on every deploy, including a redeploy of the same code, and stays
the same when a process restarts within a deploy.
"""


@dataclass(frozen=True)
class Deployment:
    """The resolved deployment scope of a cache."""

    token: str | None
    """Short hash used in keys. ``None`` when entries aren't scoped."""
    label: str
    """What it was resolved from, for logs and ``repr``."""
    kind: Literal["detected", "explicit", "process", "unscoped"]


def _token(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


_process_identities: dict[int, str] = {}


def _process_identity() -> str:
    """Stable for the life of this process, and different in a forked child."""
    pid = os.getpid()
    identity = _process_identities.get(pid)
    if identity is None:
        identity = f"process:{socket.gethostname()}:{pid}:{uuid.uuid4().hex}"
        _process_identities[pid] = identity
    return identity


def resolve_deployment(
    setting: DeploymentSetting, environ: Mapping[str, str] | None = None
) -> Deployment:
    """Turn the ``deployment=`` setting of a cache into a :class:`Deployment`."""
    if setting is None:
        return Deployment(None, "unscoped", "unscoped")
    if setting == "process":
        return Deployment(_token(_process_identity()), "this process", "process")
    if setting != "auto":
        if not setting.strip():
            raise ValueError("deployment can't be empty; use None to disable scoping")
        return Deployment(_token(setting), setting, "explicit")
    env = os.environ if environ is None else environ
    for name, platform in DEPLOYMENT_ENV_VARS:
        value = env.get(name, "").strip()
        if value:
            return Deployment(_token(f"{name}={value}"), f"{platform}: {value}", "detected")
    logger.warning(
        "cache-cache: no deployment ID found in the environment, so entries are scoped to this "
        "process and processes don't share them. Set CACHE_CACHE_DEPLOYMENT to an ID that "
        "changes on every deploy (see %s), or pass deployment=None to keep entries across "
        "deploys.",
        ", ".join(name for name, _ in DEPLOYMENT_ENV_VARS[1:]),
    )
    return Deployment(_token(_process_identity()), "this process (no ID detected)", "process")
