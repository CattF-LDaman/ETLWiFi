from __future__ import annotations

import polars as pl

from etlwifi.silver.clean import OBSERVED_CHANNELS_COLUMN, TIMESTAMP_COLUMN


def build_channel_tables(frame: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    channels = (
        frame.select(TIMESTAMP_COLUMN, "mac", OBSERVED_CHANNELS_COLUMN)
        .with_columns(pl.col(OBSERVED_CHANNELS_COLUMN).cast(pl.Utf8))
        .with_columns(
            pl.col(OBSERVED_CHANNELS_COLUMN)
            .str.split("|")
            .list.eval(pl.element().cast(pl.Int64, strict=False))
            .alias("channels")
        )
        .explode("channels", empty_as_null=True)
        .filter(pl.col("channels").is_not_null())
        .rename({"channels": "channel"})
        .select(TIMESTAMP_COLUMN, "mac", "channel")
    )

    channel_counts = channels.group_by(TIMESTAMP_COLUMN, "mac").agg(
        pl.len().alias("channel_count")
    )

    return channels, channel_counts
