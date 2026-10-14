from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from etlwifi.config.defaults import (
    BRIDGE_TABLE,
    CAPTURE_DWELL_TABLE,
    CAPTURE_WINDOW_TABLE,
    DIM_TABLE,
    FACT_TABLE,
)
from etlwifi.silver.manifest import load_manifest as load_silver_manifest
from etlwifi.silver.manifest import manifest_path as silver_manifest_path


@dataclass(frozen=True)
class SilverSnapshot:
    dataset: str
    content_hash: str
    ingested_at: str
    rows_fact: int
    rows_dim: int
    parts: tuple[str, ...]


@dataclass(frozen=True)
class SilverTables:
    fact: pl.LazyFrame
    bridge: pl.LazyFrame
    dim: pl.DataFrame
    capture_windows: pl.DataFrame | None
    capture_dwell: pl.DataFrame | None


def find_latest_silver_snapshot(silver_root: Path, dataset: str) -> SilverSnapshot:
    manifest = load_silver_manifest(silver_manifest_path(silver_root))
    for entry in reversed(manifest):
        if entry.get("dataset") != dataset:
            continue
        return SilverSnapshot(
            dataset=dataset,
            content_hash=entry["bronze_content_hash"],
            ingested_at=entry["ingested_at"],
            rows_fact=entry.get("rows_fact", 0),
            rows_dim=entry.get("rows_dim", 0),
            parts=tuple(entry.get("parts") or ()),
        )
    raise FileNotFoundError(f"no silver transform found for dataset {dataset!r}")


def table_parts(root: Path, parts: tuple[str, ...], table_name: str) -> list[Path]:
    prefix = f"{table_name}/"
    return [root / part for part in parts if part.startswith(prefix)]


def read_silver_tables(silver_root: Path, snapshot: SilverSnapshot) -> SilverTables:
    def paths(table: str, *, required: bool = True) -> list[Path]:
        found = table_parts(silver_root, snapshot.parts, table)
        missing = [path for path in found if not path.exists()]
        if missing:
            raise FileNotFoundError(f"silver part missing: {missing[0]}")
        if required and not found:
            raise FileNotFoundError(f"silver table {table!r} has no parts in snapshot")
        return found

    windows = paths(CAPTURE_WINDOW_TABLE, required=False)
    dwell = paths(CAPTURE_DWELL_TABLE, required=False)
    return SilverTables(
        fact=pl.scan_parquet(paths(FACT_TABLE)),
        bridge=pl.scan_parquet(paths(BRIDGE_TABLE)),
        dim=pl.read_parquet(paths(DIM_TABLE)),
        capture_windows=pl.read_parquet(windows) if windows else None,
        capture_dwell=pl.read_parquet(dwell) if dwell else None,
    )
