from __future__ import annotations

import polars as pl

from etlwifi.gold.context import TS, GoldContext


def build_agg_capture_coverage_daily(ctx: GoldContext) -> pl.DataFrame:
    expected = ctx.calendar.group_by("local_date").agg(
        pl.len().alias("calendar_hours"),
        (pl.len() * ctx.expected_windows_per_hour).alias("expected_windows"),
        pl.col("hour_start").min().alias("_day_start"),
        (pl.col("hour_start").max() + pl.duration(hours=1)).alias("_day_end"),
    )
    window = pl.duration(seconds=ctx.window_seconds)
    # Gaps are bounded by the local day: midnight -> first window, between windows, last -> day end.
    gaps = (
        ctx.windows.select(TS, "local_date", "window_source")
        .join(expected.select("local_date", "_day_start", "_day_end"), on="local_date", how="inner")
        .sort(TS)
        .with_columns(
            (pl.col(TS) - pl.col(TS).shift(1).over("local_date") - window).alias("_between"),
        )
        .group_by("local_date")
        .agg(
            pl.len().alias("observed_windows"),
            pl.max_horizontal(
                pl.col("_between").max(),
                pl.col(TS).min() - window - pl.col("_day_start").first(),
                pl.col("_day_end").first() - pl.col(TS).max(),
            )
            .dt.total_seconds()
            .alias("_gap_sec"),
            (pl.col("window_source") == "telemetry").mean().alias("telemetry_share"),
        )
    )
    return (
        expected.join(gaps, on="local_date", how="left")
        .with_columns(
            pl.col("observed_windows").fill_null(0),
            pl.col("telemetry_share").fill_null(0.0),
            (
                pl.col("_gap_sec")
                .fill_null((pl.col("_day_end") - pl.col("_day_start")).dt.total_seconds())
                .clip(lower_bound=0)
                / 60
            ).alias("longest_gap_min"),
        )
        .with_columns(
            (pl.col("observed_windows") / pl.col("expected_windows")).clip(0.0, 1.0).alias("coverage_ratio")
        )
        .select(
            "local_date",
            "calendar_hours",
            "expected_windows",
            "observed_windows",
            "longest_gap_min",
            "telemetry_share",
            "coverage_ratio",
        )
        .sort("local_date")
    )
