from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import polars as pl

from etlwifi.bronze.hash import file_sha256
from etlwifi.bronze.manifest import (
    append_entry,
    find_ingest_by_hash,
    find_latest_ingest,
    load_manifest,
    manifest_path,
    remove_parts,
)
from etlwifi.config.defaults import DATE_PARTITION_PREFIX
from etlwifi.io.timestamps import (
    PARTITION_DATE_PATTERN,
    WINDOW_TIMESTAMP_COLUMN,
    clean_window_timestamp_expr,
    partition_date_expr,
)
from etlwifi.privacy import hash_mac, is_locally_administered, is_raw_mac, oui, require_salt
from etlwifi.schema import CAPTURE_COLUMNS, TELEMETRY_COLUMNS

TIMESTAMP_COLUMN = WINDOW_TIMESTAMP_COLUMN

SourceKind = Literal["capture", "telemetry"]

SCHEMAS: dict[SourceKind, tuple[str, ...]] = {
    "capture": CAPTURE_COLUMNS,
    "telemetry": TELEMETRY_COLUMNS,
}


@dataclass(frozen=True)
class BronzeIngestResult:
    skipped: bool
    dataset: str | None = None
    source: str | None = None
    rows: int | None = None
    partitions: int | None = None
    parts: tuple[str, ...] = ()
    manifest: str | None = None
    reason: str | None = None
    prior: dict | None = None
    replaced_parts: int = 0
    hashed_macs: int = 0


def _clean_timestamp_expr() -> pl.Expr:
    return clean_window_timestamp_expr(TIMESTAMP_COLUMN)


def _partition_date_expr() -> pl.Expr:
    return partition_date_expr(TIMESTAMP_COLUMN)


def _prepare_frame(frame: pl.DataFrame) -> pl.DataFrame:
    frame = frame.with_columns(_clean_timestamp_expr().alias(TIMESTAMP_COLUMN))
    dates = frame.select(_partition_date_expr().alias("date")).unique()
    if dates.filter(pl.col("date").is_null() | (pl.col("date") == "")).height:
        raise ValueError(f"rows with missing {TIMESTAMP_COLUMN} dates")
    if dates.filter(~pl.col("date").str.contains(PARTITION_DATE_PATTERN)).height:
        invalid = dates.filter(~pl.col("date").str.contains(PARTITION_DATE_PATTERN))
        raise ValueError(f"invalid partition dates after timestamp cleanup: {invalid}")
    return frame


def conform_columns(frame: pl.DataFrame, columns: tuple[str, ...]) -> pl.DataFrame:
    """Project onto the contract: every column present, string-typed, in contract order."""
    missing = [pl.lit(None, dtype=pl.Utf8).alias(c) for c in columns if c not in frame.columns]
    if missing:
        frame = frame.with_columns(missing)
    return frame.select(pl.col(c).cast(pl.Utf8) for c in columns)


def pseudonymise_macs(frame: pl.DataFrame, salt: str | bytes | None) -> tuple[pl.DataFrame, int]:
    """Derive randomized/OUI from raw MACs, then replace them with keyed hashes."""
    raw = [mac for mac in frame["mac"].unique().drop_nulls().to_list() if is_raw_mac(mac)]
    if not raw:
        return frame, 0
    key = require_salt(salt)
    hashed = {mac: hash_mac(mac, key) for mac in raw}
    randomized = {mac: "1" if is_locally_administered(mac) else "0" for mac in raw}
    prefixes = {mac: oui(mac) for mac in raw}

    frame = frame.with_columns(
        pl.coalesce(
            pl.col("mac_randomized"),
            pl.col("mac").replace_strict(randomized, default=None, return_dtype=pl.Utf8),
        ).alias("mac_randomized"),
        pl.coalesce(
            pl.col("oui"),
            pl.col("mac").replace_strict(prefixes, default=None, return_dtype=pl.Utf8),
        ).alias("oui"),
    ).with_columns(pl.col("mac").replace(hashed).alias("mac"))
    return frame, len(raw)


def _part_suffix(source_key: str) -> str:
    return hashlib.sha256(source_key.encode("utf-8")).hexdigest()[:8]


def ingest_csv(
    csv_path: Path,
    bronze_root: Path,
    *,
    dataset: str | None = None,
    kind: SourceKind = "capture",
    mac_salt: str | bytes | None = None,
    force: bool = False,
) -> BronzeIngestResult:
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    dataset_name = dataset or csv_path.stem
    source_key = str(csv_path.resolve())
    content_hash = file_sha256(csv_path)
    manifest = manifest_path(bronze_root)
    entries = load_manifest(manifest)

    if not force:
        prior = find_ingest_by_hash(entries, csv_path, content_hash)
        if prior is not None and prior.get("dataset") == dataset_name:
            return BronzeIngestResult(
                skipped=True,
                dataset=dataset_name,
                source=source_key,
                reason="unchanged source already ingested",
                prior=prior,
            )

    ingested_at = datetime.now(UTC)
    part_stamp = f"{ingested_at.strftime('%Y%m%dT%H%M%S%fZ')}-{_part_suffix(source_key)}"

    frame = pl.read_csv(csv_path, infer_schema=False, truncate_ragged_lines=True)
    if TIMESTAMP_COLUMN not in frame.columns:
        raise ValueError(f"{csv_path} has no '{TIMESTAMP_COLUMN}' column")

    frame = conform_columns(frame, SCHEMAS[kind])
    hashed_macs = 0
    if kind == "capture":
        frame, hashed_macs = pseudonymise_macs(frame, mac_salt)
    frame = _prepare_frame(frame) if frame.height else frame
    frame = frame.with_columns(
        pl.lit(ingested_at).alias("_ingested_at"),
        pl.lit(csv_path.name).alias("_source_file"),
    )

    dates = frame.select(_partition_date_expr().alias("date")).unique().sort("date")

    dataset_root = bronze_root / dataset_name
    parts_written: list[str] = []
    rows_written = 0

    for date_value in dates["date"].to_list():
        day_frame = frame.filter(_partition_date_expr() == date_value)
        partition_dir = dataset_root / f"{DATE_PARTITION_PREFIX}{date_value}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        part_path = partition_dir / f"part-{part_stamp}.parquet"
        day_frame.write_parquet(part_path)
        parts_written.append(str(part_path.relative_to(bronze_root)))
        rows_written += day_frame.height

    replaced_parts = 0
    latest = find_latest_ingest(entries, csv_path)
    if latest and latest.get("parts"):
        stale = [part for part in latest["parts"] if part not in parts_written]
        remove_parts(bronze_root, stale)
        replaced_parts = len(stale)

    append_entry(
        manifest,
        {
            "dataset": dataset_name,
            "kind": kind,
            "source_path": source_key,
            "source_file": csv_path.name,
            "content_hash": content_hash,
            "ingested_at": ingested_at.isoformat(),
            "rows": rows_written,
            "partitions": len(dates),
            "parts": parts_written,
            "replaced_parts": replaced_parts,
            "hashed_macs": hashed_macs,
        },
    )

    return BronzeIngestResult(
        skipped=False,
        dataset=dataset_name,
        source=source_key,
        rows=rows_written,
        partitions=len(dates),
        parts=tuple(parts_written),
        manifest=str(manifest),
        replaced_parts=replaced_parts,
        hashed_macs=hashed_macs,
    )


def ingest_sources(
    sources: Iterable[Path],
    bronze_root: Path,
    *,
    dataset: str,
    kind: SourceKind = "capture",
    mac_salt: str | bytes | None = None,
    force: bool = False,
) -> list[BronzeIngestResult]:
    return [
        ingest_csv(path, bronze_root, dataset=dataset, kind=kind, mac_salt=mac_salt, force=force)
        for path in sorted(sources)
    ]
