"""File-based heartbeats used by Docker healthchecks."""

from __future__ import annotations

import time
from pathlib import Path


def heartbeat_path(directory: Path, name: str) -> Path:
    return directory / f".{name}.heartbeat"


def touch_heartbeat(directory: Path, name: str) -> None:
    path = heartbeat_path(directory, name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


def heartbeat_age(directory: Path, name: str) -> float | None:
    path = heartbeat_path(directory, name)
    if not path.exists():
        return None
    return time.time() - path.stat().st_mtime
