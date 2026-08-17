from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from etlwifi.io.timestamps import WINDOW_TIMESTAMP_COLUMN, clean_window_timestamp_expr

TIMESTAMP_COLUMN = "timestamp"
OBSERVED_CHANNELS_COLUMN = "observed_channels"

INT_COLUMNS = (
    "packet_count",
    "total_bytes",
    "min_rssi",
    "max_rssi",
    "mgmt_pkts",
    "ctrl_pkts",
    "data_pkts",
)


@dataclass(frozen=True)
class CleanSummary:
    input_rows: int
    output_rows: int
    parse_failures: int
    duplicates_removed: int


def clean_bronze(frame: pl.LazyFrame) -> tuple[pl.DataFrame, CleanSummary]:
    input_rows = frame.select(pl.len()).collect().item()

    cleaned = (
        frame.with_columns(clean_window_timestamp_expr(WINDOW_TIMESTAMP_COLUMN))
        .with_columns(
            pl.col(WINDOW_TIMESTAMP_COLUMN)
            .str.to_datetime(time_zone="UTC", strict=False)
            .alias(TIMESTAMP_COLUMN)
        )
        .collect()
    )

    parse_failures = cleaned.filter(pl.col(TIMESTAMP_COLUMN).is_null()).height
    if parse_failures:
        cleaned = cleaned.filter(pl.col(TIMESTAMP_COLUMN).is_not_null())

    before_dedupe = cleaned.height
    cleaned = cleaned.sort([TIMESTAMP_COLUMN, "mac"]).unique(
        subset=[TIMESTAMP_COLUMN, "mac"],
        keep="last",
    )
    duplicates_removed = before_dedupe - cleaned.height

    cast_exprs = [pl.col(column).cast(pl.Int64) for column in INT_COLUMNS]
    cast_exprs.append(pl.col("avg_rssi").cast(pl.Float64))
    cleaned = cleaned.with_columns(cast_exprs)

    return cleaned, CleanSummary(
        input_rows=input_rows,
        output_rows=cleaned.height,
        parse_failures=parse_failures,
        duplicates_removed=duplicates_removed,
    )
