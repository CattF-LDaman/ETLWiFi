"""Historical dashboard document built from the gold snapshot."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl

from etlwifi.config import defaults as d
from etlwifi.serving import SERVING_SCHEMA_VERSION
from etlwifi.serving.common import MIN_GROUP_SIZE, records, scalar

RECENT_DAYS = 28
HOURLY_RECENT_HOURS = 7 * 24
CHURN_DAYS = 90
TOP_VENDORS = 8


def _summary(t: dict[str, pl.DataFrame], now: datetime) -> dict:
    coverage = t[d.AGG_CAPTURE_COVERAGE_DAILY]
    daily = t[d.AGG_NETWORK_DAILY]
    profile = t[d.DIM_DEVICE_PROFILE]
    observed = daily.filter(pl.col("coverage_ratio") > 0)
    full_days = daily.filter(pl.col("coverage_ratio") >= 0.9)
    segments = profile["device_segment"].value_counts()
    seg = dict(zip(segments["device_segment"].to_list(), segments["count"].to_list(), strict=True))
    anomalies = t[d.AGG_ANOMALY_HOURLY]
    week_ago = now - timedelta(days=7)
    recent_anomalies = (
        anomalies.filter(pl.col("is_anomaly") & (pl.col("hour_start") >= week_ago)).height
        if anomalies.height and "is_anomaly" in anomalies.columns
        else 0
    )
    return {
        "first_day": scalar(observed["local_date"].min()) if observed.height else None,
        "last_day": scalar(observed["local_date"].max()) if observed.height else None,
        "days_observed": observed.height,
        "coverage_ratio": scalar(
            coverage["observed_windows"].sum() / coverage["expected_windows"].sum()
            if coverage.height
            else None
        ),
        "total_packets": int(daily["total_packets"].fill_null(0).sum()),
        "total_bytes": int(daily["total_bytes"].fill_null(0).sum()),
        "observations": int(t[d.AGG_NETWORK_WINDOW]["device_count"].sum()),
        "unique_macs": profile.height,
        "stable_devices": profile.height - seg.get("randomized", 0),
        "randomized_share": scalar(seg.get("randomized", 0) / profile.height if profile.height else None),
        "median_daily_stable_devices": scalar(full_days["stable_devices"].median()),
        "median_daily_randomized_macs": scalar(full_days["randomized_macs"].median()),
        "residents": seg.get("resident", 0) + seg.get("infrastructure", 0),
        "access_points": t[d.DIM_ACCESS_POINT].height or None,
        "anomalies_last_7d": recent_anomalies,
    }


def _daily(t: dict[str, pl.DataFrame]) -> list[dict]:
    return records(
        t[d.AGG_NETWORK_DAILY],
        {
            "local_date": "date",
            "coverage_ratio": "coverage",
            "unique_devices": "unique",
            "stable_devices": "stable",
            "randomized_macs": "randomized",
            "estimated_devices": "estimated",
            "new_stable_devices": "new_stable",
            "total_packets": "packets",
            "peak_concurrent_devices": "peak_concurrent",
            "is_weekend": "weekend",
        },
        digits=3,
    )


def _hourly_recent(t: dict[str, pl.DataFrame]) -> list[dict]:
    return records(
        t[d.AGG_NETWORK_HOURLY].sort("hour_start").tail(HOURLY_RECENT_HOURS),
        {
            "hour_start": "t",
            "local_hour": "hour",
            "coverage_ratio": "coverage",
            "unique_devices": "unique",
            "stable_devices": "stable",
            "randomized_macs": "randomized",
            "packets_per_min": "ppm",
        },
    )


def _hour_of_week(t: dict[str, pl.DataFrame]) -> list[dict]:
    return records(
        t[d.AGG_HOUR_OF_WEEK],
        {
            "local_weekday": "dow",
            "local_hour": "hour",
            "median_unique_devices": "unique",
            "median_stable_devices": "stable",
            "median_randomized_macs": "randomized",
            "median_packets_per_min": "ppm",
            "sample_hours": "samples",
        },
        digits=1,
    )


def _day_profile(t: dict[str, pl.DataFrame]) -> dict:
    how = t[d.AGG_HOUR_OF_WEEK]
    if how.height == 0:
        return {"weekday": [], "weekend": []}
    grouped = (
        how.with_columns(pl.col("local_weekday").is_in([6, 7]).alias("weekend"))
        .group_by("weekend", "local_hour")
        .agg(
            pl.col("median_stable_devices").mean().alias("stable"),
            pl.col("median_randomized_macs").mean().alias("randomized"),
            pl.col("median_packets_per_min").mean().alias("ppm"),
        )
        .sort("local_hour")
    )
    columns = {"local_hour": "hour", "stable": "stable", "randomized": "randomized", "ppm": "ppm"}
    return {
        "weekday": records(grouped.filter(~pl.col("weekend")), columns, digits=1),
        "weekend": records(grouped.filter(pl.col("weekend")), columns, digits=1),
    }


def _segments(t: dict[str, pl.DataFrame]) -> list[dict]:
    profile = t[d.DIM_DEVICE_PROFILE]
    last = profile["last_seen"].max() if profile.height else None
    frame = profile.group_by("device_segment").agg(
        pl.len().alias("devices"),
        (pl.col("last_seen") >= pl.lit(last) - pl.duration(days=7)).sum().alias("devices_last_7d")
        if last is not None
        else pl.lit(0).alias("devices_last_7d"),
        pl.col("median_session_duration").median().alias("median_session_sec"),
        pl.col("avg_rssi").median().alias("median_rssi"),
    )
    order = {
        name: i
        for i, name in enumerate(
            ("resident", "infrastructure", "access_point", "regular", "passerby", "randomized")
        )
    }
    frame = frame.with_columns(
        pl.col("device_segment").replace_strict(order, default=99, return_dtype=pl.Int32).alias("_o")
    ).sort("_o")
    return records(
        frame,
        {
            "device_segment": "segment",
            "devices": "devices",
            "devices_last_7d": "devices_last_7d",
            "median_session_sec": "median_session_sec",
            "median_rssi": "median_rssi",
        },
        1,
    )


def _vendors(t: dict[str, pl.DataFrame]) -> list[dict]:
    vendors = (
        t[d.AGG_VENDOR]
        .group_by("vendor")
        .agg(pl.col("device_count").sum(), pl.col("devices_last_7d").sum())
        .sort("device_count", descending=True)
    )
    known = vendors.filter(
        (pl.col("vendor") != "Unknown vendor") & (pl.col("device_count") >= MIN_GROUP_SIZE)
    ).head(TOP_VENDORS)
    rest = vendors.join(known.select("vendor"), on="vendor", how="anti")
    rows = records(
        known, {"vendor": "vendor", "device_count": "devices", "devices_last_7d": "devices_last_7d"}
    )
    if rest.height:
        rows.append(
            {
                "vendor": "Other / unknown",
                "devices": int(rest["device_count"].sum()),
                "devices_last_7d": int(rest["devices_last_7d"].sum()),
            }
        )
    return rows


def _recent(frame: pl.DataFrame, column: str = "local_date", days: int = RECENT_DAYS) -> pl.DataFrame:
    if frame.height == 0:
        return frame
    last = frame[column].max()
    return frame.filter(pl.col(column) > pl.lit(last) - pl.duration(days=days))


def _proximity(t: dict[str, pl.DataFrame]) -> list[dict]:
    frame = (
        _recent(t[d.AGG_RSSI_DISTRIBUTION])
        .group_by("device_class", "rssi_bucket")
        .agg(pl.col("window_count").sum())
        .with_columns(
            (pl.col("window_count") / pl.col("window_count").sum().over("device_class")).alias("share")
        )
    )
    order = {"very_near": 0, "near": 1, "far": 2, "edge": 3}
    frame = frame.with_columns(
        pl.col("rssi_bucket").replace_strict(order, default=9, return_dtype=pl.Int8).alias("_o")
    ).sort("device_class", "_o")
    return records(
        frame,
        {
            "device_class": "device_class",
            "rssi_bucket": "bucket",
            "window_count": "observations",
            "share": "share",
        },
        3,
    )


def _dwell(t: dict[str, pl.DataFrame]) -> list[dict]:
    frame = t[d.AGG_DWELL_DISTRIBUTION]
    if frame.height == 0:
        return []
    totals = (
        frame.group_by("duration_bucket", "bucket_order")
        .agg(pl.col("session_count").sum().alias("sessions"))
        .sort("bucket_order")
    )
    by_segment = frame.pivot(
        on="device_segment", index="duration_bucket", values="session_count", aggregate_function="sum"
    )
    merged = (
        totals.join(by_segment, on="duration_bucket", how="left").sort("bucket_order").drop("bucket_order")
    )
    return records(merged.rename({"duration_bucket": "bucket"}).fill_null(0))


def _churn(t: dict[str, pl.DataFrame]) -> list[dict]:
    return records(
        _recent(t[d.AGG_CHURN_DAILY], days=CHURN_DAYS),
        {
            "local_date": "date",
            "stable_devices": "stable",
            "first_time_devices": "first_time",
            "returning_devices": "returning",
            "lost_since_yesterday": "lost",
            "resident_devices": "resident",
            "randomized_macs": "randomized",
        },
    )


def _channels(t: dict[str, pl.DataFrame]) -> dict:
    hourly = _recent(t[d.AGG_CHANNEL_HOURLY])
    if hourly.height == 0:
        return {"totals": [], "by_hour": []}
    totals = (
        hourly.group_by("channel")
        .agg(
            pl.col("packet_count").sum().alias("packets"),
            pl.col("device_count").mean().alias("devices_per_hour"),
            pl.col("dwell_sec").sum().alias("dwell_sec"),
        )
        .with_columns((pl.col("packets") / pl.col("packets").sum()).alias("share"))
        .with_columns(
            pl.when(pl.col("dwell_sec") > 0)
            .then(pl.col("packets") / (pl.col("dwell_sec") / 60))
            .otherwise(None)
            .alias("packets_per_dwell_min")
        )
        .sort("channel")
    )
    by_hour = (
        hourly.group_by("local_hour", "channel")
        .agg(pl.col("share_of_hourly_packets").mean().alias("share"))
        .sort("local_hour", "channel")
    )
    return {
        "totals": records(
            totals, ["channel", "packets", "share", "devices_per_hour", "packets_per_dwell_min"], 3
        ),
        "by_hour": records(by_hour, {"local_hour": "hour", "channel": "channel", "share": "share"}, 3),
    }


def _frame_mix(t: dict[str, pl.DataFrame]) -> dict:
    hourly = _recent(t[d.AGG_NETWORK_HOURLY]).filter(pl.col("total_packets") > 0)
    if hourly.height == 0:
        return {}
    weights = pl.col("total_packets")
    row = hourly.select(
        *[(pl.col(c) * weights).sum() / weights.sum() for c in ("mgmt_share", "ctrl_share", "data_share")],
        (
            (pl.col("retry_share") * weights).sum()
            / weights.filter(pl.col("retry_share").is_not_null()).sum()
        ).alias("retry_share"),
        pl.col("probe_requests").sum(),
        pl.col("beacon_packets").sum(),
    ).row(0, named=True)
    return {k: scalar(v) for k, v in row.items()}


def _access_points(t: dict[str, pl.DataFrame]) -> dict | None:
    aps = t[d.DIM_ACCESS_POINT]
    if aps.height == 0:
        return None
    last = aps["last_seen"].max()
    current = aps.filter(pl.col("last_seen") >= pl.lit(last) - pl.duration(days=1))
    by_channel = current.group_by("ap_channel").agg(pl.len().alias("count")).sort("ap_channel")
    return {
        "total": aps.height,
        "seen_last_24h": current.height,
        "hidden_share": scalar(current["is_hidden"].mean()),
        "by_channel": records(by_channel, {"ap_channel": "channel", "count": "count"}),
        "daily": records(
            _recent(t[d.AGG_ACCESS_POINT_DAILY], days=60),
            {
                "local_date": "date",
                "access_points": "count",
                "hidden_access_points": "hidden",
                "new_access_points": "new",
            },
        ),
    }


def _anomalies(t: dict[str, pl.DataFrame]) -> dict:
    frame = t[d.AGG_ANOMALY_HOURLY]
    if frame.height == 0 or "is_anomaly" not in frame.columns:
        return {"rate": None, "recent": []}
    flagged = frame.filter(pl.col("is_anomaly")).sort("hour_start", descending=True).head(12)
    return {
        "rate": scalar(frame["is_anomaly"].mean()),
        "scored_hours": frame.height,
        "recent": records(
            flagged,
            {
                "hour_start": "t",
                "local_date": "date",
                "local_hour": "hour",
                "driver": "driver",
                "unique_devices": "unique",
                "expected_unique_devices": "expected_unique",
                "z_unique_devices": "z_unique",
                "packets_per_min": "ppm",
                "expected_packets_per_min": "expected_ppm",
                "z_packets_per_min": "z_ppm",
            },
            1,
        ),
    }


def baseline_records(baseline: pl.DataFrame) -> list[dict]:
    return records(
        baseline,
        {
            "local_weekday": "dow",
            "local_hour": "hour",
            "unique_devices_p10": "unique_p10",
            "unique_devices_p50": "unique_p50",
            "unique_devices_p90": "unique_p90",
            "packets_per_min_p50": "ppm_p50",
            "window_devices_p10": "window_p10",
            "window_devices_p50": "window_p50",
            "window_devices_p90": "window_p90",
            "sample_hours": "samples",
        },
        1,
    )


def _pipeline(gold_entry: dict, runs: list[dict]) -> dict:
    recent_runs = [
        {
            "finished_at": run.get("finished_at"),
            "status": run.get("status"),
            "durations_sec": run.get("durations_sec"),
            "bronze_rows_ingested": run.get("bronze_rows_ingested"),
            "silver_fact_rows": run.get("silver_fact_rows"),
            "gold_skipped": run.get("gold_skipped"),
        }
        for run in runs[-20:]
    ]
    successes = [r for r in runs if r.get("status") == "success"]
    return {
        "gold_built_at": gold_entry.get("ingested_at"),
        "fact_rows": gold_entry.get("rows_fact"),
        "devices": gold_entry.get("rows_dim"),
        "tables": gold_entry.get("table_rows"),
        "runs": recent_runs,
        "success_rate": scalar(len(successes) / len(runs)) if runs else None,
    }


def build_dashboard(
    tables: dict[str, pl.DataFrame],
    *,
    gold_entry: dict,
    runs: list[dict],
    timezone: str,
    now: datetime | None = None,
) -> dict:
    now = now or datetime.now(UTC)
    return {
        "schema_version": SERVING_SCHEMA_VERSION,
        "kind": "dashboard",
        "generated_at": scalar(now),
        "timezone": timezone,
        "summary": _summary(tables, now),
        "daily": _daily(tables),
        "coverage": records(
            tables[d.AGG_CAPTURE_COVERAGE_DAILY],
            {"local_date": "date", "coverage_ratio": "coverage", "longest_gap_min": "longest_gap_min"},
            3,
        ),
        "hourly_recent": _hourly_recent(tables),
        "hour_of_week": _hour_of_week(tables),
        "day_profile": _day_profile(tables),
        "baseline": baseline_records(tables[d.AGG_BASELINE_HOUR_OF_WEEK]),
        "segments": _segments(tables),
        "vendors": _vendors(tables),
        "proximity": _proximity(tables),
        "dwell": _dwell(tables),
        "churn": _churn(tables),
        "channels": _channels(tables),
        "frame_mix": _frame_mix(tables),
        "access_points": _access_points(tables),
        "anomalies": _anomalies(tables),
        "pipeline": _pipeline(gold_entry, runs),
    }
