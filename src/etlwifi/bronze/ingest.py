from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from etlwifi.bronze.manifest import append_entry, find_prior_ingest, load_manifest, manifest_path

DATE_PARTITION_PREFIX = "date="
TIMESTAMP_COLUMN = "window_timestamp"


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


def _partition_date_expr() -> pl.Expr:
    return pl.col(TIMESTAMP_COLUMN).str.slice(0, 10)


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
    source_mtime = csv_path.stat().st_mtime
    manifest = manifest_path(bronze_root)

    if not force:
        prior = find_prior_ingest(load_manifest(manifest), csv_path, source_mtime)
        if prior is not None:
            return BronzeIngestResult(
                skipped=True,
                reason="unchanged source already ingested",
                prior=prior,
            )

    ingested_at = datetime.now(UTC)
    part_stamp = ingested_at.strftime("%Y%m%dT%H%M%SZ")

    frame = pl.read_csv(csv_path)
    if TIMESTAMP_COLUMN not in frame.columns:
        raise ValueError(f"{csv_path} has no '{TIMESTAMP_COLUMN}' column")

    frame = frame.with_columns(
        pl.lit(ingested_at).alias("_ingested_at"),
        pl.lit(csv_path.name).alias("_source_file"),
    )

    dates = frame.select(_partition_date_expr().alias("date")).unique().sort("date")
    if dates.filter(pl.col("date").is_null() | (pl.col("date") == "")).height:
        raise ValueError(f"{csv_path} has rows with missing {TIMESTAMP_COLUMN} dates")

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
            "source_mtime": source_mtime,
            "ingested_at": ingested_at.isoformat(),
            "rows": rows_written,
            "partitions": len(dates),
            "parts": parts_written,
        },
    )

    return BronzeIngestResult(
        skipped=False,
        dataset=dataset_name,
        rows=rows_written,
        partitions=len(dates),
        parts=tuple(parts_written),
        manifest=str(manifest),
    )
