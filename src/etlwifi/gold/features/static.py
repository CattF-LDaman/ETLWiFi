from __future__ import annotations

import math

import polars as pl

from etlwifi.config.defaults import FEATURE_MIN_WINDOWS
from etlwifi.gold.context import GoldContext

STATIC_FEATURES = (
    "window_count",
    "active_days_local",
    "presence_ratio",
    "span_days",
    "total_packets",
    "total_bytes",
    "avg_rssi",
    "windows_per_active_day",
    "packets_per_window",
    "bytes_per_packet",
    "coefficient_of_variation_packets",
    "rssi_std",
    "rssi_range",
    "strong_signal_share",
    "mgmt_ratio",
    "data_ratio",
    "ctrl_ratio",
    "probe_ratio",
    "peak_hour",
    "peak_hour_concentration",
    "overnight_share",
    "weekend_share",
    "distinct_channels",
    "channel_entropy",
    "channel_switch_rate",
    "session_count",
    "median_session_duration",
    "median_inter_arrival_sec",
    "max_gap_sec",
    "inter_arrival_cv",
)


def build_feat_device_static(device_profile: pl.DataFrame) -> pl.DataFrame:
    """One row per stable device with enough history to describe its behaviour."""
    return (
        device_profile.filter(~pl.col("is_randomized") & (pl.col("window_count") >= FEATURE_MIN_WINDOWS))
        .select(
            "mac",
            *STATIC_FEATURES,
            pl.col("total_packets").log1p().alias("log_total_packets"),
            pl.col("total_bytes").log1p().alias("log_total_bytes"),
            pl.col("window_count").log1p().alias("log_window_count"),
            (pl.col("peak_hour") * 2 * math.pi / 24).sin().alias("peak_hour_sin"),
            (pl.col("peak_hour") * 2 * math.pi / 24).cos().alias("peak_hour_cos"),
            "is_multi_channel",
            "vendor",
            "device_segment",
        )
        .sort("mac")
    )


def build_feat_device_hour_profile(ctx: GoldContext, device_profile: pl.DataFrame) -> pl.DataFrame:
    """Normalised 24h activity vectors (weekday and weekend) for behavioural clustering."""
    eligible = device_profile.filter(
        ~pl.col("is_randomized") & (pl.col("window_count") >= FEATURE_MIN_WINDOWS)
    ).select("mac", "device_segment")
    if eligible.height == 0:
        return pl.DataFrame(schema={"mac": pl.Utf8})

    counts = (
        ctx.fact.join(eligible.select("mac"), on="mac", how="semi")
        .with_columns(
            pl.when(pl.col("local_weekday").is_in([6, 7]))
            .then(pl.lit("we"))
            .otherwise(pl.lit("wd"))
            .alias("day_type")
        )
        .group_by("mac", "day_type", "local_hour")
        .agg(pl.len().alias("windows"))
        .with_columns(
            (pl.col("windows") / pl.col("windows").sum().over("mac", "day_type")).alias("share"),
            pl.format("{}_h{}", pl.col("day_type"), pl.col("local_hour").cast(pl.Utf8).str.zfill(2)).alias(
                "feature"
            ),
        )
    )
    wide = counts.pivot(on="feature", index="mac", values="share", aggregate_function="first")
    feature_columns = [f"{day}_h{hour:02d}" for day in ("wd", "we") for hour in range(24)]
    for column in feature_columns:
        if column not in wide.columns:
            wide = wide.with_columns(pl.lit(None, dtype=pl.Float64).alias(column))

    hour_dist = (
        counts.group_by("mac", "local_hour")
        .agg(pl.col("windows").sum())
        .with_columns((pl.col("windows") / pl.col("windows").sum().over("mac")).alias("p"))
    )
    entropy = hour_dist.group_by("mac").agg(
        (-(pl.col("p") * pl.col("p").log(base=2)).sum() / math.log2(24)).alias("hour_entropy_norm")
    )
    return (
        eligible.join(wide.select("mac", *feature_columns), on="mac", how="left")
        .join(entropy, on="mac", how="left")
        .with_columns(pl.col(c).fill_null(0.0) for c in feature_columns)
        .sort("mac")
    )
