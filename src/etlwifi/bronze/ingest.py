from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

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

DATE_PARTITION_PREFIX = "date="
TIMESTAMP_COLUMN = "window_timestamp"
PARTITION_DATE_PATTERN = r"^\d{4}-\d{2}-\d{2}$"


@dataclass(frozen=True)
class BronzeIngestResult:
    skipped: bool
    dataset: str | None = None
    rows: int | None = None
    partitions: int | None = None
    parts: tuple[str, ...] = ()
    manifest: str | None = None
    reason: str | None = None
    prior: dict | None = None
    replaced_parts: int = 0


def _clean_timestamp_expr() -> pl.Expr:
    return pl.col(TIMESTAMP_COLUMN).str.replace_all("\x00", "").str.strip_chars()


def _partition_date_expr() -> pl.Expr:
    return _clean_timestamp_expr().str.slice(0, 10)


def _prepare_frame(frame: pl.DataFrame) -> pl.DataFrame:
    frame = frame.with_columns(_clean_timestamp_expr().alias(TIMESTAMP_COLUMN))
    dates = frame.select(_partition_date_expr().alias("date")).unique()
    if dates.filter(pl.col("date").is_null() | (pl.col("date") == "")).height:
        raise ValueError(f"rows with missing {TIMESTAMP_COLUMN} dates")
    if dates.filter(~pl.col("date").str.contains(PARTITION_DATE_PATTERN)).height:
        invalid = dates.filter(~pl.col("date").str.contains(PARTITION_DATE_PATTERN))
        raise ValueError(f"invalid partition dates after timestamp cleanup: {invalid}")
    return frame


def ingest_csv(
    csv_path: Path,
    bronze_root: Path,
    *,
    dataset: str | None = None,
    force: bool = False,
) -> BronzeIngestResult:
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    dataset_name = dataset or csv_path.stem
    content_hash = file_sha256(csv_path)
    manifest = manifest_path(bronze_root)
    entries = load_manifest(manifest)

    if not force:
        prior = find_ingest_by_hash(entries, csv_path, content_hash)
        if prior is not None:
            return BronzeIngestResult(
                skipped=True,
                reason="unchanged source already ingested",
                prior=prior,
            )

    replaced_parts = 0
    latest = find_latest_ingest(entries, csv_path)
    if latest and latest.get("parts"):
        remove_parts(bronze_root, latest["parts"])
        replaced_parts = len(latest["parts"])

    ingested_at = datetime.now(UTC)
    part_stamp = ingested_at.strftime("%Y%m%dT%H%M%SZ")

    frame = pl.read_csv(csv_path)
    if TIMESTAMP_COLUMN not in frame.columns:
        raise ValueError(f"{csv_path} has no '{TIMESTAMP_COLUMN}' column")

    frame = _prepare_frame(frame)
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

    append_entry(
        manifest,
        {
            "dataset": dataset_name,
            "source_path": str(csv_path.resolve()),
            "source_file": csv_path.name,
            "content_hash": content_hash,
            "ingested_at": ingested_at.isoformat(),
            "rows": rows_written,
            "partitions": len(dates),
            "parts": parts_written,
            "replaced_parts": replaced_parts,
        },
    )

    return BronzeIngestResult(
        skipped=False,
        dataset=dataset_name,
        rows=rows_written,
        partitions=len(dates),
        parts=tuple(parts_written),
        manifest=str(manifest),
        replaced_parts=replaced_parts,
    )
