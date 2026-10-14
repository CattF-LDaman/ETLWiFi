from __future__ import annotations

import math

import polars as pl

from etlwifi.config.defaults import FEATURE_MIN_WINDOWS


def build_feat_device_daily(presence_daily: pl.DataFrame, device_profile: pl.DataFrame) -> pl.DataFrame:
    """Per stable device and active day, with calendar-aware (not row-count) rolling windows."""
    eligible = device_profile.filter(
        ~pl.col("is_randomized") & (pl.col("window_count") >= FEATURE_MIN_WINDOWS)
    ).select("mac", pl.col("first_seen_date"))
    daily = presence_daily.join(eligible, on="mac", how="inner").sort("mac", "local_date")
    if daily.height == 0:
        return pl.DataFrame(schema={"local_date": pl.Date, "mac": pl.Utf8})

    rolled = daily.with_columns(
        pl.col("total_packets")
        .rolling_mean_by("local_date", window_size="7d")
        .over("mac")
        .alias("rolling_mean_packets_7d"),
        pl.col("total_packets")
        .rolling_std_by("local_date", window_size="7d")
        .over("mac")
        .alias("rolling_std_packets_7d"),
        pl.col("avg_rssi")
        .rolling_mean_by("local_date", window_size="7d")
        .over("mac")
        .alias("rolling_mean_rssi_7d"),
        pl.int_range(1, pl.len() + 1).over("mac").alias("active_day_index"),
        pl.col("local_date").diff().over("mac").dt.total_days().alias("days_since_previous_visit"),
    )
    return rolled.with_columns(
        pl.when(pl.col("rolling_std_packets_7d") > 0)
        .then(
            (pl.col("total_packets") - pl.col("rolling_mean_packets_7d")) / pl.col("rolling_std_packets_7d")
        )
        .otherwise(0.0)
        .alias("zscore_packets_vs_7d"),
        (pl.col("avg_rssi") - pl.col("rolling_mean_rssi_7d")).alias("rssi_delta_vs_7d"),
        (pl.col("local_date") - pl.col("first_seen_date")).dt.total_days().alias("days_since_first_seen"),
    ).select(
        "local_date",
        "mac",
        "window_count",
        "active_minutes",
        "session_count",
        "total_packets",
        "avg_rssi",
        "rolling_mean_packets_7d",
        "rolling_std_packets_7d",
        "rolling_mean_rssi_7d",
        "zscore_packets_vs_7d",
        "rssi_delta_vs_7d",
        "days_since_first_seen",
        "days_since_previous_visit",
        "active_day_index",
    )


def build_feat_network_hourly(network_hourly: pl.DataFrame, baseline: pl.DataFrame) -> pl.DataFrame:
    """Forecasting features on a gap-free hourly calendar, so lags are true clock lags."""
    ordered = network_hourly.sort("hour_start")
    target = pl.col("unique_devices")
    frame = ordered.with_columns(
        target.shift(1).alias("unique_devices_lag_1h"),
        target.shift(24).alias("unique_devices_lag_24h"),
        target.shift(168).alias("unique_devices_lag_168h"),
        target.rolling_mean(window_size=24, min_samples=1).alias("unique_devices_mean_24h"),
        target.rolling_mean(window_size=168, min_samples=1).alias("unique_devices_mean_7d"),
        target.rolling_std(window_size=168, min_samples=2).alias("unique_devices_std_7d"),
        pl.col("packets_per_min").shift(24).alias("packets_per_min_lag_24h"),
        pl.col("packets_per_min")
        .rolling_mean(window_size=168, min_samples=1)
        .alias("packets_per_min_mean_7d"),
        (pl.col("local_hour") * 2 * math.pi / 24).sin().alias("hour_sin"),
        (pl.col("local_hour") * 2 * math.pi / 24).cos().alias("hour_cos"),
        ((pl.col("local_weekday") - 1) * 2 * math.pi / 7).sin().alias("weekday_sin"),
        ((pl.col("local_weekday") - 1) * 2 * math.pi / 7).cos().alias("weekday_cos"),
    )
    if "unique_devices_p50" in baseline.columns:
        frame = frame.join(
            baseline.select(
                "local_weekday",
                "local_hour",
                pl.col("unique_devices_p50").alias("baseline_unique_devices"),
                pl.col("packets_per_min_p50").alias("baseline_packets_per_min"),
            ),
            on=["local_weekday", "local_hour"],
            how="left",
        )
    else:
        frame = frame.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("baseline_unique_devices"),
            pl.lit(None, dtype=pl.Float64).alias("baseline_packets_per_min"),
        )
    return frame.select(
        "hour_start",
        "local_date",
        "local_hour",
        "local_weekday",
        "is_weekend",
        "coverage_ratio",
        "unique_devices",
        "stable_devices",
        "randomized_macs",
        "estimated_devices",
        "packets_per_min",
        "unique_devices_lag_1h",
        "unique_devices_lag_24h",
        "unique_devices_lag_168h",
        "unique_devices_mean_24h",
        "unique_devices_mean_7d",
        "unique_devices_std_7d",
        "packets_per_min_lag_24h",
        "packets_per_min_mean_7d",
        "baseline_unique_devices",
        "baseline_packets_per_min",
        "hour_sin",
        "hour_cos",
        "weekday_sin",
        "weekday_cos",
    ).sort("hour_start")
