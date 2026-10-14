from __future__ import annotations

import polars as pl

from etlwifi.gold.context import TS, GoldContext, local_time_columns

RSSI_BUCKET_EXPR = (
    pl.when(pl.col("avg_rssi") > -50)
    .then(pl.lit("very_near"))
    .when(pl.col("avg_rssi") > -65)
    .then(pl.lit("near"))
    .when(pl.col("avg_rssi") > -80)
    .then(pl.lit("far"))
    .otherwise(pl.lit("edge"))
)


def build_agg_channel_hourly(ctx: GoldContext) -> pl.DataFrame:
    traffic = (
        ctx.bridge.join(
            ctx.fact.select(TS, "mac", "packet_count", "total_bytes", "avg_rssi", "is_randomized"),
            on=[TS, "mac"],
            how="inner",
        )
        .with_columns(pl.col(TS).dt.truncate("1h").alias("hour_start"))
        .group_by("hour_start", "channel")
        .agg(
            pl.len().alias("observation_count"),
            pl.col("mac").n_unique().alias("device_count"),
            pl.col("mac").filter(~pl.col("is_randomized")).n_unique().alias("stable_devices"),
            pl.col("packet_count").sum().alias("packet_count"),
            pl.col("total_bytes").sum().alias("byte_count"),
            pl.col("avg_rssi").mean().alias("avg_rssi"),
        )
    )
    dwell = ctx.dwell.group_by(pl.col(TS).dt.truncate("1h").alias("hour_start"), "channel").agg(
        pl.col("dwell_sec").sum().alias("dwell_sec")
    )
    return (
        traffic.join(dwell, on=["hour_start", "channel"], how="left")
        .with_columns(
            (pl.col("packet_count") / pl.col("packet_count").sum().over("hour_start")).alias(
                "share_of_hourly_packets"
            ),
            pl.when(pl.col("dwell_sec") > 0)
            .then(pl.col("packet_count") / (pl.col("dwell_sec") / 60))
            .otherwise(None)
            .alias("packets_per_dwell_min"),
            *local_time_columns("hour_start", ctx.timezone),
        )
        .sort("hour_start", "channel")
    )


def _channel_entropy_frame(channel_counts: pl.DataFrame) -> pl.DataFrame:
    probs = channel_counts.with_columns((pl.col("count") / pl.col("count").sum().over("mac")).alias("p"))
    return probs.group_by("mac").agg((-(pl.col("p") * pl.col("p").log()).sum()).alias("channel_entropy"))


def build_agg_channel_device(ctx: GoldContext) -> pl.DataFrame:
    bridge = ctx.bridge
    channel_counts = bridge.group_by("mac", "channel").agg(pl.len().alias("count"))
    primary = (
        channel_counts.sort("mac", "count", descending=[False, True])
        .group_by("mac")
        .agg(pl.col("channel").first().alias("primary_channel"))
    )
    switches = (
        bridge.sort("mac", TS)
        .with_columns(
            (pl.col("channel") != pl.col("channel").shift(1).over("mac")).fill_null(False).alias("switched")
        )
        .group_by("mac")
        .agg(
            pl.col("switched").sum().alias("channel_switches"),
            pl.col(TS).dt.truncate("1h").n_unique().alias("active_hours"),
            pl.col("channel").n_unique().alias("distinct_channels"),
        )
        .with_columns(
            pl.when(pl.col("active_hours") > 0)
            .then(pl.col("channel_switches") / pl.col("active_hours"))
            .otherwise(0.0)
            .alias("channel_switch_rate")
        )
    )
    return (
        primary.join(_channel_entropy_frame(channel_counts), on="mac", how="left")
        .join(switches.select("mac", "channel_switch_rate", "distinct_channels"), on="mac", how="left")
        .with_columns(
            pl.col("channel_entropy").fill_null(0.0),
            pl.col("channel_switch_rate").fill_null(0.0),
            (pl.col("distinct_channels") > 1).alias("is_multi_channel"),
        )
        .sort("mac")
    )


def build_agg_rssi_distribution(ctx: GoldContext) -> pl.DataFrame:
    return (
        ctx.fact.with_columns(RSSI_BUCKET_EXPR.alias("rssi_bucket"))
        .group_by("local_date", "device_class", "rssi_bucket")
        .agg(
            pl.len().alias("window_count"),
            pl.col("mac").n_unique().alias("device_count"),
            pl.col("packet_count").sum().alias("packet_count"),
        )
        .sort("local_date", "device_class", "rssi_bucket")
    )
