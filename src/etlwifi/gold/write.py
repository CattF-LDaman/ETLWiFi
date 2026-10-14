from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from etlwifi.config.defaults import DATE_PARTITION_PREFIX


def write_date_partitioned(
    frame: pl.DataFrame,
    gold_root: Path,
    table_name: str,
    date_column: str,
    part_stamp: str,
) -> tuple[list[str], int]:
    parts_written: list[str] = []
    rows_written = 0
    table_root = gold_root / table_name

    if frame.height == 0:
        part, rows = write_single_table(frame, gold_root, table_name, part_stamp)
        return [part], rows

    keyed = frame.with_columns(pl.col(date_column).cast(pl.Utf8).alias("__partition"))
    for (date_value,), day_frame in keyed.group_by("__partition", maintain_order=True):
        partition_dir = table_root / f"{DATE_PARTITION_PREFIX}{date_value}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        part_path = partition_dir / f"part-{part_stamp}.parquet"
        day_frame.drop("__partition").write_parquet(part_path)
        parts_written.append(str(part_path.relative_to(gold_root)))
        rows_written += day_frame.height

    return parts_written, rows_written


def write_single_table(
    frame: pl.DataFrame,
    gold_root: Path,
    table_name: str,
    part_stamp: str,
) -> tuple[str, int]:
    table_root = gold_root / table_name
    table_root.mkdir(parents=True, exist_ok=True)
    part_path = table_root / f"part-{part_stamp}.parquet"
    frame.write_parquet(part_path)
    return str(part_path.relative_to(gold_root)), frame.height


def write_gold_tables(
    tables: dict[str, pl.DataFrame],
    gold_root: Path,
    *,
    date_partitioned: dict[str, str],
    ingested_at: datetime | None = None,
) -> dict:
    ingested_at = ingested_at or datetime.now(UTC)
    part_stamp = ingested_at.strftime("%Y%m%dT%H%M%S%fZ")

    parts: list[str] = []
    row_counts: dict[str, int] = {}

    for table_name, frame in tables.items():
        if table_name in date_partitioned:
            table_parts, rows = write_date_partitioned(
                frame,
                gold_root,
                table_name,
                date_partitioned[table_name],
                part_stamp,
            )
            parts.extend(table_parts)
        else:
            part, rows = write_single_table(frame, gold_root, table_name, part_stamp)
            parts.append(part)
        row_counts[table_name] = rows

    return {
        "parts": parts,
        "row_counts": row_counts,
        "ingested_at": ingested_at.isoformat(),
    }


def read_gold_table(gold_root: Path, parts: list[str] | tuple[str, ...], table_name: str) -> pl.DataFrame:
    prefix = f"{table_name}/"
    paths = [gold_root / part for part in parts if part.startswith(prefix)]
    if not paths:
        raise FileNotFoundError(f"gold table {table_name!r} not in snapshot")
    return pl.read_parquet(paths)
