from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from etlwifi.bronze.manifest import load_manifest as load_bronze_manifest
from etlwifi.bronze.manifest import manifest_path as bronze_manifest_path


@dataclass(frozen=True)
class BronzeSnapshot:
    """Current state of a bronze dataset: the latest landing of every source file."""

    dataset: str
    content_hash: str
    parts: tuple[str, ...]
    ingested_at: str
    rows: int
    sources: int


def _latest_per_source(manifest: list[dict], dataset: str) -> dict[str, dict]:
    latest: dict[str, dict] = {}
    for entry in manifest:
        if entry.get("dataset") == dataset:
            latest[entry["source_path"]] = entry
    return latest


def find_latest_bronze_snapshot(bronze_root: Path, dataset: str) -> BronzeSnapshot:
    manifest = load_bronze_manifest(bronze_manifest_path(bronze_root))
    latest = _latest_per_source(manifest, dataset)
    if not latest:
        raise FileNotFoundError(f"no bronze ingest found for dataset {dataset!r}")

    digest = hashlib.sha256()
    parts: list[str] = []
    for source in sorted(latest):
        entry = latest[source]
        digest.update(f"{source}\t{entry['content_hash']}\n".encode())
        parts.extend(entry.get("parts") or [])
    if not parts:
        raise ValueError(f"bronze dataset {dataset!r} has no rows yet")

    return BronzeSnapshot(
        dataset=dataset,
        content_hash=digest.hexdigest(),
        parts=tuple(parts),
        ingested_at=max(entry["ingested_at"] for entry in latest.values()),
        rows=sum(entry.get("rows", 0) for entry in latest.values()),
        sources=len(latest),
    )


def find_optional_bronze_snapshot(bronze_root: Path, dataset: str) -> BronzeSnapshot | None:
    try:
        return find_latest_bronze_snapshot(bronze_root, dataset)
    except (FileNotFoundError, ValueError):
        return None


def read_bronze_snapshot(bronze_root: Path, snapshot: BronzeSnapshot) -> pl.LazyFrame:
    paths = [bronze_root / part for part in snapshot.parts]
    missing = [path for path in paths if not path.exists()]
    if missing:
        raise FileNotFoundError(f"bronze part files missing: {missing[0]}")
    return pl.scan_parquet(paths)
