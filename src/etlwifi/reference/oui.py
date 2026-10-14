"""IEEE OUI registry: download, cache, and normalise vendor names."""

from __future__ import annotations

import logging
import os
import re
import time
import urllib.request
from pathlib import Path

import polars as pl

log = logging.getLogger(__name__)

OUI_FILENAME = "oui.csv"
USER_AGENT = "etlwifi/1.0 (+https://luwe.be)"

_SUFFIXES = re.compile(
    r"(?:[,.，]\s*|\s+)(inc|incorporated|corp|corporation|co|company|ltd|limited|llc|gmbh|ag|sa|s\.a|"
    r"bv|b\.v|nv|n\.v|plc|pte|pty|srl|s\.r\.o|sro|spa|sas|ab|oy|as|kg|technologies|technology|electronics|"
    r"communications|telecommunications|systems|international|holdings|group|"
    r"czech republic|germany|hungary|japan|china|korea|taiwan|europe|usa|america)\.?\s*$",
    re.IGNORECASE,
)


def normalize_vendor(name: str) -> str:
    cleaned = name.strip().strip('"')
    previous = None
    while previous != cleaned:
        previous = cleaned
        cleaned = _SUFFIXES.sub("", cleaned).strip(" ,.-")
    if cleaned.isupper() and len(cleaned) > 4:
        cleaned = cleaned.title()
    return cleaned or name.strip()


def registry_path(reference_dir: Path) -> Path:
    return reference_dir / OUI_FILENAME


def refresh_registry(reference_dir: Path, url: str, max_age_days: int) -> bool:
    """Download the registry when missing or stale. Never raises; returns success."""
    target = registry_path(reference_dir)
    if target.exists() and time.time() - target.stat().st_mtime < max_age_days * 86400:
        return True
    reference_dir.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".csv.tmp")
    try:
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(request, timeout=60) as response, tmp.open("wb") as handle:
            while chunk := response.read(1 << 16):
                handle.write(chunk)
        load_registry(tmp)
        os.replace(tmp, target)
        log.info("oui registry refreshed", extra={"path": str(target)})
        return True
    except Exception as exc:
        tmp.unlink(missing_ok=True)
        log.warning("oui registry refresh failed", extra={"error": str(exc)})
        return target.exists()


def load_registry(path: Path) -> pl.DataFrame:
    frame = pl.read_csv(path, infer_schema=False)
    columns = {c.lower(): c for c in frame.columns}
    assignment = columns.get("assignment")
    organization = columns.get("organization name")
    if assignment is None or organization is None:
        raise ValueError(f"unexpected OUI registry header: {frame.columns}")
    return (
        frame.select(
            pl.col(assignment).str.to_uppercase().str.strip_chars().alias("oui"),
            pl.col(organization).map_elements(normalize_vendor, return_dtype=pl.Utf8).alias("vendor"),
        )
        .filter(pl.col("oui").str.len_chars() == 6)
        .unique(subset=["oui"], keep="first")
    )


def load_vendors(reference_dir: Path) -> pl.DataFrame | None:
    path = registry_path(reference_dir)
    if not path.exists():
        return None
    try:
        return load_registry(path)
    except Exception as exc:
        log.warning("oui registry unreadable", extra={"error": str(exc)})
        return None
