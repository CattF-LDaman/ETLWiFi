from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from etlwifi.config.defaults import (
    BRIDGE_TABLE,
    DATE_PARTITION_PREFIX,
    DIM_TABLE,
    FACT_TABLE,
)
from etlwifi.silver.clean import TIMESTAMP_COLUMN


def _partition_date_expr() -> pl.Expr:
    return pl.col(TIMESTAMP_COLUMN).dt.date().cast(pl.Utf8)


def write_partitioned_table(
    frame: pl.DataFrame,
    silver_root: Path,
    table_name: str,
    part_stamp: str,
) -> tuple[list[str], int]:
    parts_written: list[str] = []
    rows_written = 0
    table_root = silver_root / table_name

    dates = frame.select(_partition_date_expr().alias("date")).unique().sort("date")
    for date_value in dates["date"].to_list():
        day_frame = frame.filter(_partition_date_expr() == date_value)
        partition_dir = table_root / f"{DATE_PARTITION_PREFIX}{date_value}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        part_path = partition_dir / f"part-{part_stamp}.parquet"
        day_frame.write_parquet(part_path)
        parts_written.append(str(part_path.relative_to(silver_root)))
        rows_written += day_frame.height

    return parts_written, rows_written


def write_dim_device(
    frame: pl.DataFrame,
    silver_root: Path,
    part_stamp: str,
) -> tuple[str, int]:
    table_root = silver_root / DIM_TABLE
    table_root.mkdir(parents=True, exist_ok=True)
    part_path = table_root / f"part-{part_stamp}.parquet"
    frame.write_parquet(part_path)
    return str(part_path.relative_to(silver_root)), frame.height


def write_silver_tables(
    fact: pl.DataFrame,
    bridge: pl.DataFrame,
    dim: pl.DataFrame,
    silver_root: Path,
    *,
    ingested_at: datetime | None = None,
) -> dict:
    ingested_at = ingested_at or datetime.now(UTC)
    part_stamp = ingested_at.strftime("%Y%m%dT%H%M%SZ")

    fact_parts, fact_rows = write_partitioned_table(
        fact, silver_root, FACT_TABLE, part_stamp
    )
    bridge_parts, bridge_rows = write_partitioned_table(
        bridge, silver_root, BRIDGE_TABLE, part_stamp
    )
    dim_part, dim_rows = write_dim_device(dim, silver_root, part_stamp)

    return {
        "parts": fact_parts + bridge_parts + [dim_part],
        "rows_fact": fact_rows,
        "rows_bridge": bridge_rows,
        "rows_dim": dim_rows,
        "fact_partitions": len(fact_parts),
        "bridge_partitions": len(bridge_parts),
        "ingested_at": ingested_at.isoformat(),
    }
