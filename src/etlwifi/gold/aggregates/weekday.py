"""Day-of-week profile: how a typical Monday differs from a typical Sunday."""

from __future__ import annotations

import polars as pl

from etlwifi.config.defaults import MIN_COVERAGE_FOR_BASELINE

WEEKDAY_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
STABLE_SEGMENTS = ("resident", "infrastructure", "regular", "passerby", "access_point")


def _quartiles(column: str, prefix: str) -> list[pl.Expr]:
    values = pl.col(column)
    return [
        values.min().alias(f"{prefix}_min"),
        values.quantile(0.25, interpolation="linear").alias(f"{prefix}_p25"),
        values.median().alias(f"{prefix}_median"),
        values.quantile(0.75, interpolation="linear").alias(f"{prefix}_p75"),
        values.max().alias(f"{prefix}_max"),
        values.mean().alias(f"{prefix}_mean"),
    ]


def _segment_days(presence_daily: pl.DataFrame, device_profile: pl.DataFrame) -> pl.DataFrame:
    stable = presence_daily.filter(pl.col("device_class") == "stable").join(
        device_profile.select("mac", "device_segment"), on="mac", how="inner"
    )
    return stable.group_by("local_date").agg(
        (pl.col("device_segment") == segment).sum().alias(f"{segment}_devices") for segment in STABLE_SEGMENTS
    )


def build_agg_weekday(
    network_daily: pl.DataFrame,
    presence_daily: pl.DataFrame,
    device_profile: pl.DataFrame,
    hour_of_week: pl.DataFrame,
) -> pl.DataFrame:
    """One row per ISO weekday, aggregated over well-covered local days only.

    Always returns seven rows so consumers can rely on the shape; weekdays without a covered
    day have ``days_observed == 0`` and null statistics.
    """
    days = (
        network_daily.filter(pl.col("coverage_ratio") >= MIN_COVERAGE_FOR_BASELINE)
        .join(_segment_days(presence_daily, device_profile), on="local_date", how="left")
        .with_columns(pl.col("local_date").dt.weekday().alias("local_weekday"))
    )
    per_day = days.group_by("local_weekday").agg(
        pl.len().alias("days_observed"),
        *_quartiles("stable_devices", "stable"),
        *_quartiles("randomized_macs", "randomized"),
        pl.col("estimated_devices").median().alias("estimated_median"),
        pl.col("estimated_devices").count().alias("estimated_days"),
        pl.col("total_packets").median().alias("packets_median"),
        pl.col("new_stable_devices").median().alias("new_stable_median"),
        pl.col("peak_concurrent_devices").median().alias("peak_concurrent_median"),
        pl.col("mean_concurrent_devices").median().alias("mean_concurrent_median"),
        pl.col("median_stable_active_minutes").median().alias("stable_active_minutes_median"),
        *[pl.col(f"{s}_devices").median().alias(f"{s}_median") for s in STABLE_SEGMENTS],
    )
    hours = hour_of_week.group_by("local_weekday").agg(
        pl.col("local_hour").sort_by("median_stable_devices").last().alias("peak_hour"),
        pl.col("median_stable_devices").max().alias("peak_hour_stable"),
        pl.col("local_hour").sort_by("median_stable_devices").first().alias("quiet_hour"),
        pl.col("median_stable_devices").min().alias("quiet_hour_stable"),
    )
    weekdays = pl.DataFrame(
        {
            "local_weekday": pl.Series(range(1, 8), dtype=pl.Int8),
            "weekday_name": list(WEEKDAY_NAMES),
        }
    )
    return (
        weekdays.join(per_day, on="local_weekday", how="left")
        .join(hours, on="local_weekday", how="left")
        .with_columns(
            pl.col("days_observed").fill_null(0),
            pl.col("local_weekday").is_in([6, 7]).alias("is_weekend"),
            (pl.col("stable_median") / pl.col("stable_median").mean()).alias("activity_index"),
            pl.col("stable_median").rank("ordinal", descending=True).cast(pl.Int8).alias("activity_rank"),
        )
        .sort("local_weekday")
    )
