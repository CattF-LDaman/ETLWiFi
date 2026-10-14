"""Near-real-time snapshot built straight from the landing zone (no pipeline run needed)."""

from __future__ import annotations

import io
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

from etlwifi.bronze.ingest import conform_columns, pseudonymise_macs
from etlwifi.config.paths import DataPaths
from etlwifi.config.settings import Settings
from etlwifi.gold.aggregates.channel import RSSI_BUCKET_EXPR
from etlwifi.schema import CAPTURE_COLUMNS, TELEMETRY_COLUMNS
from etlwifi.serving import SERVING_SCHEMA_VERSION
from etlwifi.serving.common import records, scalar

LEGACY_TAIL_BYTES = 12 * 1024 * 1024
ONLINE_FACTOR = 3
STALE_AFTER_SEC = 300
NOW_WINDOW_SEC = 60


@dataclass
class LiveContext:
    """Gold-derived reference data refreshed after every pipeline run."""

    baseline: pl.DataFrame | None = None
    known_stable_macs: set[str] = field(default_factory=set)


def _read_csv_text(text: str, columns: tuple[str, ...]) -> pl.DataFrame:
    if not text.strip():
        return conform_columns(pl.DataFrame(), columns)
    frame = pl.read_csv(io.StringIO(text), infer_schema=False, truncate_ragged_lines=True)
    return conform_columns(frame, columns)


def tail_csv(path: Path, max_bytes: int, columns: tuple[str, ...]) -> pl.DataFrame:
    """Parse only the last `max_bytes` of an append-only CSV, keeping its header."""
    size = path.stat().st_size
    with path.open("rb") as handle:
        header = handle.readline().decode("utf-8", errors="replace")
        if size <= max_bytes:
            body = handle.read().decode("utf-8", errors="replace")
        else:
            handle.seek(size - max_bytes)
            handle.readline()
            body = handle.read().decode("utf-8", errors="replace")
    return _read_csv_text(header + body, columns)


def _day_files(directory: Path, days: list) -> list[Path]:
    return [
        directory / f"{day.isoformat()}.csv"
        for day in days
        if (directory / f"{day.isoformat()}.csv").exists()
    ]


def _with_timestamp(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        pl.col("window_timestamp")
        .str.replace_all("\x00", "")
        .str.strip_chars()
        .str.to_datetime(time_zone="UTC", strict=False)
        .alias("timestamp")
    ).filter(pl.col("timestamp").is_not_null())


def load_recent_capture(
    settings: Settings, paths: DataPaths, now: datetime
) -> tuple[pl.DataFrame, pl.DataFrame, str]:
    utc_days = sorted({(now - timedelta(days=offset)).date() for offset in (0, 1)})
    capture_files = _day_files(paths.capture_landing, utc_days)
    source = "capture"
    if capture_files:
        capture = pl.concat(
            [
                conform_columns(
                    pl.read_csv(p, infer_schema=False, truncate_ragged_lines=True), CAPTURE_COLUMNS
                )
                for p in capture_files
            ]
        )
    elif settings.legacy_csv is not None and settings.legacy_csv.is_file():
        capture = tail_csv(settings.legacy_csv, LEGACY_TAIL_BYTES, CAPTURE_COLUMNS)
        source = "legacy"
    else:
        capture = conform_columns(pl.DataFrame(), CAPTURE_COLUMNS)
        source = "none"

    if capture.height:
        capture, _ = pseudonymise_macs(capture, settings.mac_hash_salt)
    capture = _with_timestamp(capture)

    telemetry_files = _day_files(paths.telemetry_landing, utc_days)
    telemetry = (
        _with_timestamp(
            pl.concat(
                [
                    conform_columns(pl.read_csv(p, infer_schema=False), TELEMETRY_COLUMNS)
                    for p in telemetry_files
                ]
            )
        )
        if telemetry_files
        else _with_timestamp(conform_columns(pl.DataFrame(), TELEMETRY_COLUMNS))
    )
    return _cast_capture(capture), telemetry, source


def _cast_capture(frame: pl.DataFrame) -> pl.DataFrame:
    ints = ("packet_count", "mgmt_pkts", "ctrl_pkts", "data_pkts", "beacon_pkts")
    return frame.with_columns(
        *[pl.col(c).cast(pl.Float64, strict=False).cast(pl.Int64) for c in ints],
        pl.col("avg_rssi").cast(pl.Float64, strict=False),
        (pl.col("mac_randomized") == "1").fill_null(False).alias("is_randomized"),
        pl.col("observed_channels").cast(pl.Utf8),
    )


def _window_series(frame: pl.DataFrame) -> pl.DataFrame:
    return (
        frame.group_by("timestamp")
        .agg(
            pl.col("mac").n_unique().alias("devices"),
            pl.col("mac").filter(~pl.col("is_randomized")).n_unique().alias("stable"),
            pl.col("mac").filter(pl.col("is_randomized")).n_unique().alias("randomized"),
            pl.col("packet_count").sum().alias("packets"),
        )
        .sort("timestamp")
    )


def _capture_health(
    capture: pl.DataFrame, telemetry: pl.DataFrame, now: datetime, window_seconds: int
) -> dict:
    stamps = telemetry["timestamp"] if telemetry.height else capture["timestamp"]
    if stamps.len() == 0:
        return {"status": "offline", "last_window_at": None, "age_sec": None, "coverage_last_hour": 0.0}
    last = stamps.max()
    age = (now - last).total_seconds()
    status = (
        "online"
        if age <= window_seconds * ONLINE_FACTOR
        else "stale"
        if age <= STALE_AFTER_SEC
        else "offline"
    )
    hour_ago = now - timedelta(hours=1)
    windows_last_hour = stamps.filter(stamps >= hour_ago).n_unique()
    health = {
        "status": status,
        "last_window_at": scalar(last),
        "age_sec": round(age, 1),
        "coverage_last_hour": scalar(min(1.0, windows_last_hour / (3600 / window_seconds))),
        "source": "telemetry" if telemetry.height else "inferred",
    }
    if telemetry.height:
        recent = telemetry.filter(pl.col("timestamp") >= now - timedelta(minutes=5))
        frames = recent["frames_total"].cast(pl.Float64, strict=False).sum()
        seconds = recent["window_seconds"].cast(pl.Float64, strict=False).sum()
        health["frames_per_sec"] = scalar(frames / seconds if seconds else None, 1)
    return health


def _now_block(capture: pl.DataFrame, now: datetime) -> dict:
    recent = capture.filter(pl.col("timestamp") >= now - timedelta(seconds=NOW_WINDOW_SEC))
    if recent.height == 0:
        return {"devices": 0, "stable": 0, "randomized": 0, "packets_per_min": 0}
    proximity = (
        recent.group_by("mac")
        .agg(pl.col("avg_rssi").mean())
        .with_columns(RSSI_BUCKET_EXPR.alias("bucket"))
        .group_by("bucket")
        .agg(pl.len().alias("devices"))
    )
    channels = (
        recent.with_columns(pl.col("observed_channels").str.split("|"))
        .explode("observed_channels", empty_as_null=True)
        .filter(pl.col("observed_channels") != "")
        .group_by(pl.col("observed_channels").cast(pl.Int64, strict=False).alias("channel"))
        .agg(pl.col("packet_count").sum().alias("packets"), pl.col("mac").n_unique().alias("devices"))
        .drop_nulls("channel")
        .sort("channel")
    )
    totals = recent.select(
        pl.col("packet_count").sum(),
        pl.col("mgmt_pkts").sum(),
        pl.col("ctrl_pkts").sum(),
        pl.col("data_pkts").sum(),
    ).row(0, named=True)
    total = totals["packet_count"] or 0
    span = max(
        NOW_WINDOW_SEC,
        (now - recent["timestamp"].min()).total_seconds() if recent.height else NOW_WINDOW_SEC,
    )
    return {
        "devices": recent["mac"].n_unique(),
        "stable": recent.filter(~pl.col("is_randomized"))["mac"].n_unique(),
        "randomized": recent.filter(pl.col("is_randomized"))["mac"].n_unique(),
        "access_points": recent.filter(pl.col("beacon_pkts") > 0)["mac"].n_unique()
        if recent["beacon_pkts"].is_not_null().any()
        else None,
        "packets_per_min": round(total / span * 60, 1),
        "proximity": {row["bucket"]: row["devices"] for row in proximity.iter_rows(named=True)},
        "channels": records(channels),
        "frame_mix": {
            k: scalar(totals[f"{k}_pkts"] / total if total else None) for k in ("mgmt", "ctrl", "data")
        },
    }


def _expected(context: LiveContext | None, now: datetime, tz: ZoneInfo) -> dict | None:
    if context is None or context.baseline is None or context.baseline.height == 0:
        return None
    local = now.astimezone(tz)
    row = context.baseline.filter(
        (pl.col("local_weekday") == local.isoweekday()) & (pl.col("local_hour") == local.hour)
    )
    if row.height == 0:
        return None
    values = row.row(0, named=True)
    return {
        "dow": local.isoweekday(),
        "hour": local.hour,
        "window_devices": {k: scalar(values.get(f"window_devices_{k}"), 1) for k in ("p10", "p50", "p90")},
        "unique_devices_hour": {
            k: scalar(values.get(f"unique_devices_{k}"), 1) for k in ("p10", "p50", "p90")
        },
        "packets_per_min": scalar(values.get("packets_per_min_p50"), 1),
    }


def _today(capture: pl.DataFrame, context: LiveContext | None, now: datetime, tz: ZoneInfo) -> dict:
    local_today = now.astimezone(tz).date()
    today = capture.with_columns(pl.col("timestamp").dt.convert_time_zone(str(tz)).alias("local_ts")).filter(
        pl.col("local_ts").dt.date() == local_today
    )
    if today.height == 0:
        return {"date": local_today.isoformat(), "unique": 0, "stable": 0, "randomized": 0, "hours": []}
    stable = set(today.filter(~pl.col("is_randomized"))["mac"].unique().to_list())
    known = context.known_stable_macs if context else set()
    hours = (
        today.group_by(pl.col("local_ts").dt.hour().alias("hour"))
        .agg(
            pl.col("mac").n_unique().alias("unique"),
            pl.col("mac").filter(~pl.col("is_randomized")).n_unique().alias("stable"),
            pl.col("mac").filter(pl.col("is_randomized")).n_unique().alias("randomized"),
            pl.col("packet_count").sum().alias("packets"),
        )
        .sort("hour")
    )
    series = _window_series(today)
    return {
        "date": local_today.isoformat(),
        "unique": today["mac"].n_unique(),
        "stable": len(stable),
        "randomized": today.filter(pl.col("is_randomized"))["mac"].n_unique(),
        "first_time_stable": len(stable - known) if known else None,
        "packets": int(today["packet_count"].sum()),
        "peak_concurrent": int(series["devices"].max()) if series.height else 0,
        "hours": records(hours),
    }


def build_live(
    settings: Settings,
    paths: DataPaths,
    context: LiveContext | None = None,
    *,
    now: datetime | None = None,
) -> dict:
    now = now or datetime.now(UTC)
    tz = ZoneInfo(settings.timezone)
    capture, telemetry, source = load_recent_capture(settings, paths, now)
    capture = capture.filter(pl.col("timestamp") <= now)

    last_hour = _window_series(capture.filter(pl.col("timestamp") >= now - timedelta(hours=1)))
    if telemetry.height:
        empty_windows = telemetry.filter(pl.col("timestamp") >= now - timedelta(hours=1)).select("timestamp")
        last_hour = (
            empty_windows.join(last_hour, on="timestamp", how="left")
            .with_columns(pl.col(c).fill_null(0) for c in ("devices", "stable", "randomized", "packets"))
            .sort("timestamp")
        )

    return {
        "schema_version": SERVING_SCHEMA_VERSION,
        "kind": "live",
        "generated_at": scalar(now),
        "timezone": settings.timezone,
        "source": source,
        "window_seconds": settings.window_seconds,
        "capture": _capture_health(capture, telemetry, now, settings.window_seconds),
        "now": _now_block(capture, now),
        "expected": _expected(context, now, tz),
        "last_hour": records(
            last_hour,
            {
                "timestamp": "t",
                "devices": "devices",
                "stable": "stable",
                "randomized": "randomized",
                "packets": "packets",
            },
        ),
        "today": _today(capture, context, now, tz),
    }
