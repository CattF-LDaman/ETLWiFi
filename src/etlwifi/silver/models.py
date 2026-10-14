from __future__ import annotations

import polars as pl

from etlwifi.silver.clean import TIMESTAMP_COLUMN

FACT_COLUMNS = (
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
    "beacon_pkts",
    "probe_req_pkts",
    "probe_resp_pkts",
    "retry_pkts",
    "ap_channel",
    "ssid_hash",
    "probe_fingerprint",
    "channel_count",
    "_ingested_at",
    "_source_file",
)


def build_fact_device_window(
    frame: pl.DataFrame,
    channel_counts: pl.DataFrame,
) -> pl.DataFrame:
    return (
        frame.join(channel_counts, on=[TIMESTAMP_COLUMN, "mac"], how="left")
        .with_columns(pl.col("channel_count").fill_null(0).cast(pl.Int32))
        .select(FACT_COLUMNS)
    )


def build_dim_device(
    fact: pl.DataFrame,
    bridge: pl.DataFrame,
    cleaned: pl.DataFrame | None = None,
    vendors: pl.DataFrame | None = None,
) -> pl.DataFrame:
    channel_stats = bridge.group_by("mac").agg(pl.col("channel").n_unique().alias("distinct_channels"))

    dim = (
        fact.group_by("mac")
        .agg(
            pl.col(TIMESTAMP_COLUMN).min().alias("first_seen"),
            pl.col(TIMESTAMP_COLUMN).max().alias("last_seen"),
            pl.len().alias("window_count"),
            pl.col(TIMESTAMP_COLUMN).dt.date().n_unique().alias("active_days"),
            pl.col("packet_count").sum().alias("total_packets"),
            pl.col("total_bytes").sum().alias("total_bytes"),
            pl.col("avg_rssi").mean().alias("avg_rssi"),
            pl.col("beacon_pkts").sum().alias("beacon_packets"),
        )
        .join(channel_stats, on="mac", how="left")
        .with_columns(pl.col("distinct_channels").fill_null(0).cast(pl.Int32))
    )

    if cleaned is not None:
        identity = cleaned.group_by("mac").agg(
            pl.col("mac_randomized").drop_nulls().first().alias("is_randomized"),
            pl.col("oui").drop_nulls().first().alias("oui"),
        )
        dim = dim.join(identity, on="mac", how="left")
    else:
        dim = dim.with_columns(
            pl.lit(None, dtype=pl.Boolean).alias("is_randomized"),
            pl.lit(None, dtype=pl.Utf8).alias("oui"),
        )

    if vendors is not None and vendors.height:
        dim = dim.join(vendors.select("oui", "vendor"), on="oui", how="left")
    else:
        dim = dim.with_columns(pl.lit(None, dtype=pl.Utf8).alias("vendor"))

    return dim.with_columns(
        pl.col("is_randomized").fill_null(False),
        pl.when(pl.col("oui").is_not_null())
        .then(pl.col("vendor").fill_null("Unknown vendor"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("vendor"),
    )


def _parse_dwell(frame: pl.DataFrame) -> pl.DataFrame:
    return (
        frame.select(TIMESTAMP_COLUMN, pl.col("channel_dwell").cast(pl.Utf8).str.split("|"))
        .explode("channel_dwell", empty_as_null=True)
        .filter(pl.col("channel_dwell").str.contains(":"))
        .with_columns(
            pl.col("channel_dwell").str.split_exact(":", 1).struct.rename_fields(["channel", "dwell"])
        )
        .unnest("channel_dwell")
        .select(
            TIMESTAMP_COLUMN,
            pl.col("channel").cast(pl.Int64, strict=False),
            pl.col("dwell").cast(pl.Float64, strict=False).alias("dwell_sec"),
        )
        .filter(pl.col("channel").is_not_null())
    )


def build_capture_window_tables(telemetry: pl.DataFrame | None) -> tuple[pl.DataFrame, pl.DataFrame]:
    if telemetry is None or telemetry.height == 0:
        return empty_capture_window(), empty_capture_dwell()
    windows = telemetry.select(
        TIMESTAMP_COLUMN,
        "window_seconds",
        "frames_total",
        "frames_dot11",
        "frames_no_sender",
        "devices",
        pl.col("interface_type").cast(pl.Utf8),
        pl.col("capture_version").cast(pl.Utf8),
    )
    return windows, _parse_dwell(telemetry)


def empty_capture_window() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            TIMESTAMP_COLUMN: pl.Datetime("us", "UTC"),
            "window_seconds": pl.Float64,
            "frames_total": pl.Int64,
            "frames_dot11": pl.Int64,
            "frames_no_sender": pl.Int64,
            "devices": pl.Int64,
            "interface_type": pl.Utf8,
            "capture_version": pl.Utf8,
        }
    )


def empty_capture_dwell() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            TIMESTAMP_COLUMN: pl.Datetime("us", "UTC"),
            "channel": pl.Int64,
            "dwell_sec": pl.Float64,
        }
    )
