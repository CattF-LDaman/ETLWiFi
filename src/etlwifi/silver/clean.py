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
    "beacon_pkts",
    "probe_req_pkts",
    "probe_resp_pkts",
    "retry_pkts",
    "ap_channel",
)

OPTIONAL_STRING_COLUMNS = ("mac_randomized", "oui", "ssid_hash", "probe_fingerprint")


@dataclass(frozen=True)
class CleanSummary:
    input_rows: int
    output_rows: int
    parse_failures: int
    duplicates_removed: int


def _parse_timestamp(frame: pl.LazyFrame) -> pl.LazyFrame:
    return frame.with_columns(clean_window_timestamp_expr(WINDOW_TIMESTAMP_COLUMN)).with_columns(
        pl.col(WINDOW_TIMESTAMP_COLUMN).str.to_datetime(time_zone="UTC", strict=False).alias(TIMESTAMP_COLUMN)
    )


def _ensure_columns(frame: pl.LazyFrame, columns: tuple[str, ...]) -> pl.LazyFrame:
    present = set(frame.collect_schema().names())
    missing = [pl.lit(None, dtype=pl.Utf8).alias(c) for c in columns if c not in present]
    return frame.with_columns(missing) if missing else frame


def _to_int(column: str) -> pl.Expr:
    # Float detour tolerates "-75.0"-style values written by older exports.
    return pl.col(column).cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False).cast(pl.Int64)


def clean_bronze(frame: pl.LazyFrame) -> tuple[pl.DataFrame, CleanSummary]:
    frame = _ensure_columns(frame, INT_COLUMNS + OPTIONAL_STRING_COLUMNS)
    input_rows = frame.select(pl.len()).collect().item()

    cleaned = _parse_timestamp(frame).collect()

    parse_failures = cleaned.filter(pl.col(TIMESTAMP_COLUMN).is_null()).height
    if parse_failures:
        cleaned = cleaned.filter(pl.col(TIMESTAMP_COLUMN).is_not_null())

    before_dedupe = cleaned.height
    cleaned = cleaned.sort([TIMESTAMP_COLUMN, "mac"]).unique(
        subset=[TIMESTAMP_COLUMN, "mac"],
        keep="last",
        maintain_order=True,
    )
    duplicates_removed = before_dedupe - cleaned.height

    cleaned = cleaned.with_columns(
        *[_to_int(column) for column in INT_COLUMNS],
        pl.col("avg_rssi").cast(pl.Utf8).cast(pl.Float64, strict=False),
        pl.col(OBSERVED_CHANNELS_COLUMN).cast(pl.Utf8),
        pl.when(pl.col("mac_randomized").is_null())
        .then(None)
        .otherwise(pl.col("mac_randomized").cast(pl.Utf8).str.to_lowercase().is_in(["1", "true"]))
        .alias("mac_randomized"),
        *[pl.col(c).cast(pl.Utf8) for c in ("oui", "ssid_hash", "probe_fingerprint")],
    )

    return cleaned, CleanSummary(
        input_rows=input_rows,
        output_rows=cleaned.height,
        parse_failures=parse_failures,
        duplicates_removed=duplicates_removed,
    )


TELEMETRY_INT_COLUMNS = ("frames_total", "frames_dot11", "frames_no_sender", "devices")


def clean_telemetry(frame: pl.LazyFrame) -> pl.DataFrame:
    cleaned = (
        _parse_timestamp(frame)
        .filter(pl.col(TIMESTAMP_COLUMN).is_not_null())
        .with_columns(
            *[_to_int(c) for c in TELEMETRY_INT_COLUMNS],
            pl.col("window_seconds").cast(pl.Float64, strict=False),
        )
        .collect()
    )
    return cleaned.sort(TIMESTAMP_COLUMN).unique(subset=[TIMESTAMP_COLUMN], keep="last", maintain_order=True)
