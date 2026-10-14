"""Shared, pre-enriched inputs for every gold builder."""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from etlwifi.config.defaults import SESSION_GAP_SEC

TS = "timestamp"


def local_time_columns(column: str, timezone: str, prefix: str = "local") -> list[pl.Expr]:
    local = pl.col(column).dt.convert_time_zone(timezone)
    return [
        local.dt.date().alias(f"{prefix}_date"),
        local.dt.hour().cast(pl.Int8).alias(f"{prefix}_hour"),
        local.dt.weekday().cast(pl.Int8).alias(f"{prefix}_weekday"),
    ]


@dataclass(frozen=True)
class GoldContext:
    fact: pl.DataFrame
    bridge: pl.DataFrame
    dim: pl.DataFrame
    windows: pl.DataFrame
    calendar: pl.DataFrame
    dwell: pl.DataFrame
    sessions: pl.DataFrame
    timezone: str
    window_seconds: int

    @property
    def expected_windows_per_hour(self) -> float:
        return 3600 / self.window_seconds

    @property
    def has_frame_detail(self) -> bool:
        return bool(self.fact["has_detail"].any()) if self.fact.height else False


def _sessions(fact: pl.DataFrame, window_seconds: int) -> pl.DataFrame:
    ordered = (
        fact.select(TS, "mac", "packet_count", "total_bytes", "avg_rssi", "device_class")
        .sort("mac", TS)
        .with_columns(
            (pl.col(TS).diff().over("mac").dt.total_seconds() > SESSION_GAP_SEC)
            .fill_null(True)
            .cum_sum()
            .over("mac")
            .alias("session_id")
        )
    )
    return (
        ordered.group_by("mac", "session_id")
        .agg(
            pl.col("device_class").first(),
            pl.col(TS).min().alias("session_start"),
            pl.col(TS).max().alias("session_end"),
            pl.len().alias("window_count"),
            pl.col("packet_count").sum().alias("packets"),
            pl.col("total_bytes").sum().alias("bytes"),
            pl.col("avg_rssi").mean().alias("avg_rssi"),
        )
        .with_columns(
            ((pl.col("session_end") - pl.col("session_start")).dt.total_seconds() + window_seconds)
            .cast(pl.Float64)
            .alias("duration_sec")
        )
        .sort("mac", "session_start")
    )


def _observed_windows(fact: pl.DataFrame, capture_windows: pl.DataFrame | None) -> pl.DataFrame:
    inferred = fact.select(TS).unique().with_columns(pl.lit("inferred").alias("window_source"))
    if capture_windows is None or capture_windows.height == 0:
        windows = inferred
    else:
        telemetry = (
            capture_windows.select(TS).unique().with_columns(pl.lit("telemetry").alias("window_source"))
        )
        windows = pl.concat([telemetry, inferred.join(telemetry, on=TS, how="anti")])
    return windows.sort(TS)


def _calendar(windows: pl.DataFrame, timezone: str, expected_per_hour: float) -> pl.DataFrame:
    if windows.height == 0:
        return pl.DataFrame(
            schema={
                "hour_start": pl.Datetime("us", "UTC"),
                "observed_windows": pl.UInt32,
                "coverage_ratio": pl.Float64,
            }
        )
    first_day = windows.select(
        pl.col(TS).min().dt.convert_time_zone(timezone).dt.truncate("1d").dt.convert_time_zone("UTC")
    ).item()
    last_hour = windows.select(pl.col(TS).max().dt.truncate("1h")).item()
    hours = pl.datetime_range(
        first_day,
        last_hour,
        interval="1h",
        time_zone="UTC",
        eager=True,
    ).alias("hour_start")
    observed = windows.group_by(pl.col(TS).dt.truncate("1h").alias("hour_start")).agg(
        pl.len().alias("observed_windows")
    )
    return (
        pl.DataFrame(hours)
        .join(observed, on="hour_start", how="left")
        .with_columns(pl.col("observed_windows").fill_null(0))
        .with_columns(
            (pl.col("observed_windows") / expected_per_hour).clip(0.0, 1.0).alias("coverage_ratio"),
            *local_time_columns("hour_start", timezone),
        )
        .sort("hour_start")
    )


def build_context(
    fact: pl.LazyFrame,
    bridge: pl.LazyFrame,
    dim: pl.DataFrame,
    *,
    capture_windows: pl.DataFrame | None = None,
    capture_dwell: pl.DataFrame | None = None,
    timezone: str = "Europe/Brussels",
    window_seconds: int = 10,
) -> GoldContext:
    if "is_randomized" not in dim.columns:
        dim = dim.with_columns(pl.lit(False).alias("is_randomized"))
    for column, dtype in (("oui", pl.Utf8), ("vendor", pl.Utf8), ("beacon_packets", pl.Int64)):
        if column not in dim.columns:
            dim = dim.with_columns(pl.lit(None, dtype=dtype).alias(column))
    dim = dim.with_columns(
        pl.when(pl.col("is_randomized"))
        .then(pl.lit("randomized"))
        .otherwise(pl.lit("stable"))
        .alias("device_class")
    )

    fact_df = fact.collect()
    for column, dtype in (
        ("beacon_pkts", pl.Int64),
        ("probe_req_pkts", pl.Int64),
        ("probe_resp_pkts", pl.Int64),
        ("retry_pkts", pl.Int64),
        ("ap_channel", pl.Int64),
        ("ssid_hash", pl.Utf8),
        ("probe_fingerprint", pl.Utf8),
    ):
        if column not in fact_df.columns:
            fact_df = fact_df.with_columns(pl.lit(None, dtype=dtype).alias(column))

    fact_df = fact_df.join(
        dim.select("mac", "is_randomized", "device_class"), on="mac", how="left"
    ).with_columns(
        pl.col("is_randomized").fill_null(False),
        pl.col("device_class").fill_null("stable"),
        pl.col("beacon_pkts").is_not_null().alias("has_detail"),
        pl.col(TS).dt.truncate("1h").alias("hour_start"),
        *local_time_columns(TS, timezone),
    )

    windows = _observed_windows(fact_df, capture_windows)
    windows = windows.with_columns(
        pl.col(TS).dt.truncate("1h").alias("hour_start"),
        *local_time_columns(TS, timezone),
    )
    expected = 3600 / window_seconds
    dwell = (
        capture_dwell
        if capture_dwell is not None
        else pl.DataFrame(schema={TS: pl.Datetime("us", "UTC"), "channel": pl.Int64, "dwell_sec": pl.Float64})
    )

    return GoldContext(
        fact=fact_df,
        bridge=bridge.collect(),
        dim=dim,
        windows=windows,
        calendar=_calendar(windows, timezone, expected),
        dwell=dwell,
        sessions=_sessions(fact_df, window_seconds),
        timezone=timezone,
        window_seconds=window_seconds,
    )
