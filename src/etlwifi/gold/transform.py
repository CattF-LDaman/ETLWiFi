from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from etlwifi.config import defaults as d
from etlwifi.gold.aggregates.channel import (
    build_agg_channel_device,
    build_agg_channel_hourly,
    build_agg_rssi_distribution,
)
from etlwifi.gold.aggregates.coverage import build_agg_capture_coverage_daily
from etlwifi.gold.aggregates.device import (
    build_agg_access_point_daily,
    build_agg_vendor,
    build_dim_access_point,
    build_dim_device_profile,
)
from etlwifi.gold.aggregates.network import (
    build_agg_anomaly_hourly,
    build_agg_baseline_hour_of_week,
    build_agg_hour_of_week,
    build_agg_network_daily,
    build_agg_network_hourly,
    build_agg_network_window,
)
from etlwifi.gold.aggregates.presence import (
    build_agg_churn_daily,
    build_agg_dwell_distribution,
    build_agg_presence_daily,
)
from etlwifi.gold.aggregates.weekday import build_agg_weekday
from etlwifi.gold.context import build_context
from etlwifi.gold.features.sessions import build_feat_co_presence_topk, build_feat_session_events
from etlwifi.gold.features.static import build_feat_device_hour_profile, build_feat_device_static
from etlwifi.gold.features.timeseries import build_feat_device_daily, build_feat_network_hourly
from etlwifi.gold.manifest import (
    append_entry,
    find_latest_transform,
    find_transform_by_hash,
    load_manifest,
    manifest_path,
    remove_parts,
)
from etlwifi.gold.read import find_latest_silver_snapshot, read_silver_tables
from etlwifi.gold.validate import GoldValidationSummary, validate_gold
from etlwifi.gold.write import write_gold_tables

DATE_PARTITIONED_TABLES = {
    d.AGG_NETWORK_WINDOW: "local_date",
    d.AGG_NETWORK_HOURLY: "local_date",
    d.AGG_CHANNEL_HOURLY: "local_date",
    d.AGG_PRESENCE_DAILY: "local_date",
    d.FEAT_DEVICE_DAILY: "local_date",
    d.FEAT_NETWORK_HOURLY: "local_date",
    d.FEAT_SESSION_EVENTS: "local_date",
}


def build_gold_tables(
    fact: pl.LazyFrame,
    bridge: pl.LazyFrame,
    dim: pl.DataFrame,
    *,
    capture_windows: pl.DataFrame | None = None,
    capture_dwell: pl.DataFrame | None = None,
    timezone: str = "Europe/Brussels",
    window_seconds: int = 10,
) -> dict[str, pl.DataFrame]:
    ctx = build_context(
        fact,
        bridge,
        dim,
        capture_windows=capture_windows,
        capture_dwell=capture_dwell,
        timezone=timezone,
        window_seconds=window_seconds,
    )

    coverage_daily = build_agg_capture_coverage_daily(ctx)
    network_window = build_agg_network_window(ctx)
    network_hourly = build_agg_network_hourly(ctx, network_window)
    presence_daily = build_agg_presence_daily(ctx)
    network_daily = build_agg_network_daily(ctx, network_window, presence_daily, coverage_daily)
    baseline = build_agg_baseline_hour_of_week(network_hourly, network_window)
    channel_device = build_agg_channel_device(ctx)
    device_profile = build_dim_device_profile(ctx, channel_device, coverage_daily)
    access_points = build_dim_access_point(ctx)
    hour_of_week = build_agg_hour_of_week(network_hourly)

    tables = {
        d.AGG_CAPTURE_COVERAGE_DAILY: coverage_daily,
        d.AGG_NETWORK_WINDOW: network_window,
        d.AGG_NETWORK_HOURLY: network_hourly,
        d.AGG_NETWORK_DAILY: network_daily,
        d.AGG_HOUR_OF_WEEK: hour_of_week,
        d.AGG_WEEKDAY: build_agg_weekday(network_daily, presence_daily, device_profile, hour_of_week),
        d.AGG_BASELINE_HOUR_OF_WEEK: baseline,
        d.AGG_ANOMALY_HOURLY: build_agg_anomaly_hourly(network_hourly),
        d.AGG_CHANNEL_HOURLY: build_agg_channel_hourly(ctx),
        d.AGG_CHANNEL_DEVICE: channel_device,
        d.AGG_RSSI_DISTRIBUTION: build_agg_rssi_distribution(ctx),
        d.AGG_PRESENCE_DAILY: presence_daily,
        d.AGG_CHURN_DAILY: build_agg_churn_daily(presence_daily),
        d.AGG_DWELL_DISTRIBUTION: build_agg_dwell_distribution(ctx, device_profile),
        d.AGG_VENDOR: build_agg_vendor(ctx, device_profile),
        d.DIM_DEVICE_PROFILE: device_profile,
        d.DIM_ACCESS_POINT: access_points,
        d.AGG_ACCESS_POINT_DAILY: build_agg_access_point_daily(ctx, access_points),
        d.FEAT_DEVICE_STATIC: build_feat_device_static(device_profile),
        d.FEAT_DEVICE_DAILY: build_feat_device_daily(presence_daily, device_profile),
        d.FEAT_DEVICE_HOUR_PROFILE: build_feat_device_hour_profile(ctx, device_profile),
        d.FEAT_NETWORK_HOURLY: build_feat_network_hourly(network_hourly, baseline),
        d.FEAT_SESSION_EVENTS: build_feat_session_events(ctx),
        d.FEAT_CO_PRESENCE_TOPK: build_feat_co_presence_topk(ctx),
    }
    assert tuple(tables) == d.GOLD_TABLES
    return tables


@dataclass(frozen=True)
class GoldTransformResult:
    skipped: bool
    dataset: str | None = None
    silver_content_hash: str | None = None
    validation: GoldValidationSummary | None = None
    table_rows: dict[str, int] | None = None
    manifest: str | None = None
    reason: str | None = None
    prior: dict | None = None
    replaced_parts: int = 0


def transform_dataset(
    dataset: str,
    silver_root: Path,
    gold_root: Path,
    *,
    timezone: str = "Europe/Brussels",
    window_seconds: int = 10,
    force: bool = False,
) -> GoldTransformResult:
    snapshot = find_latest_silver_snapshot(silver_root, dataset)
    input_hash = hashlib.sha256(
        f"{snapshot.content_hash}|{timezone}|{window_seconds}|{','.join(d.GOLD_TABLES)}".encode()
    ).hexdigest()
    manifest = manifest_path(gold_root)
    entries = load_manifest(manifest)

    if not force:
        prior = find_transform_by_hash(entries, dataset, input_hash)
        if prior is not None:
            return GoldTransformResult(
                skipped=True,
                reason="silver snapshot already transformed",
                prior=prior,
            )

    silver = read_silver_tables(silver_root, snapshot)
    tables = build_gold_tables(
        silver.fact,
        silver.bridge,
        silver.dim,
        capture_windows=silver.capture_windows,
        capture_dwell=silver.capture_dwell,
        timezone=timezone,
        window_seconds=window_seconds,
    )
    validation = validate_gold(tables, fact_rows=snapshot.rows_fact, dim_rows=silver.dim.height)

    write_result = write_gold_tables(tables, gold_root, date_partitioned=DATE_PARTITIONED_TABLES)

    replaced_parts = 0
    latest = find_latest_transform(entries, dataset)
    if latest and latest.get("parts"):
        stale = [part for part in latest["parts"] if part not in write_result["parts"]]
        remove_parts(gold_root, stale)
        replaced_parts = len(stale)

    append_entry(
        manifest,
        {
            "dataset": dataset,
            "silver_content_hash": input_hash,
            "silver_ingested_at": snapshot.ingested_at,
            "ingested_at": write_result["ingested_at"],
            "timezone": timezone,
            "rows_fact": snapshot.rows_fact,
            "rows_dim": silver.dim.height,
            "table_rows": write_result["row_counts"],
            "parts": write_result["parts"],
            "replaced_parts": replaced_parts,
        },
    )

    return GoldTransformResult(
        skipped=False,
        dataset=dataset,
        silver_content_hash=input_hash,
        validation=validation,
        table_rows=write_result["row_counts"],
        manifest=str(manifest),
        replaced_parts=replaced_parts,
    )
