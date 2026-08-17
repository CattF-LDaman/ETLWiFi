from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from etlwifi.bronze.manifest import load_manifest as load_bronze_manifest
from etlwifi.bronze.manifest import manifest_path as bronze_manifest_path


@dataclass(frozen=True)
class BronzeSnapshot:
    dataset: str
    content_hash: str
    parts: tuple[str, ...]
    ingested_at: str
    rows: int


def find_latest_bronze_snapshot(bronze_root: Path, dataset: str) -> BronzeSnapshot:
    manifest = load_bronze_manifest(bronze_manifest_path(bronze_root))
    for entry in reversed(manifest):
        if entry.get("dataset") != dataset:
            continue
        parts = entry.get("parts") or []
        if not parts:
            raise ValueError(f"bronze manifest entry for {dataset!r} has no parts")
        return BronzeSnapshot(
            dataset=dataset,
            content_hash=entry["content_hash"],
            parts=tuple(parts),
            ingested_at=entry["ingested_at"],
            rows=entry["rows"],
        )
    raise FileNotFoundError(f"no bronze ingest found for dataset {dataset!r}")


def read_bronze_snapshot(bronze_root: Path, snapshot: BronzeSnapshot) -> pl.LazyFrame:
    paths = [bronze_root / part for part in snapshot.parts]
    missing = [path for path in paths if not path.exists()]
    if missing:
        raise FileNotFoundError(f"bronze part files missing: {missing[0]}")
    return pl.scan_parquet(paths)
