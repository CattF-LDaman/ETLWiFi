from __future__ import annotations

import polars as pl

from etlwifi.silver.clean import OBSERVED_CHANNELS_COLUMN, TIMESTAMP_COLUMN


def build_fact_device_window(
    frame: pl.DataFrame,
    channel_counts: pl.DataFrame,
) -> pl.DataFrame:
    return (
        frame.join(channel_counts, on=[TIMESTAMP_COLUMN, "mac"], how="left")
        .with_columns(pl.col("channel_count").fill_null(0).cast(pl.Int32))
        .select(
            TIMESTAMP_COLUMN,
            "mac",
            "packet_count",
            "total_bytes",
            "min_rssi",
            "max_rssi",
            "avg_rssi",
            "mgmt_pkts",
            "ctrl_pkts",
            "data_pkts",
            "channel_count",
            "_ingested_at",
            "_source_file",
        )
    )


def build_dim_device(fact: pl.DataFrame) -> pl.DataFrame:
    return fact.group_by("mac").agg(
        pl.col(TIMESTAMP_COLUMN).min().alias("first_seen"),
        pl.col(TIMESTAMP_COLUMN).max().alias("last_seen"),
        pl.len().alias("window_count"),
    )
