from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from etlwifi.config import defaults as d
from etlwifi.gold.aggregates.device import DEVICE_SEGMENTS


@dataclass(frozen=True)
class GoldValidationSummary:
    fact_rows: int
    dim_rows: int
    table_rows: dict[str, int]


def _assert_unique_keys(frame: pl.DataFrame, keys: list[str], table_name: str) -> None:
    if frame.height and frame.select(keys).is_duplicated().any():
        raise ValueError(f"{table_name}: duplicate keys for {keys}")


def _assert_no_null_keys(frame: pl.DataFrame, keys: list[str], table_name: str) -> None:
    for key in keys:
        if frame.filter(pl.col(key).is_null()).height:
            raise ValueError(f"{table_name}: null values in key column {key!r}")


def _assert_in_unit_range(frame: pl.DataFrame, columns: list[str], table_name: str) -> None:
    for column in columns:
        if column not in frame.columns:
            continue
        if frame.filter((pl.col(column) < 0) | (pl.col(column) > 1)).height:
            raise ValueError(f"{table_name}: {column} outside [0, 1]")


UNIQUE_KEYS: dict[str, list[str]] = {
    d.AGG_CAPTURE_COVERAGE_DAILY: ["local_date"],
    d.AGG_NETWORK_WINDOW: ["timestamp"],
    d.AGG_NETWORK_HOURLY: ["hour_start"],
    d.AGG_NETWORK_DAILY: ["local_date"],
    d.AGG_HOUR_OF_WEEK: ["local_weekday", "local_hour"],
    d.AGG_WEEKDAY: ["local_weekday"],
    d.AGG_BASELINE_HOUR_OF_WEEK: ["local_weekday", "local_hour"],
    d.AGG_ANOMALY_HOURLY: ["hour_start"],
    d.AGG_CHANNEL_HOURLY: ["hour_start", "channel"],
    d.AGG_CHANNEL_DEVICE: ["mac"],
    d.AGG_PRESENCE_DAILY: ["local_date", "mac"],
    d.AGG_CHURN_DAILY: ["local_date"],
    d.DIM_DEVICE_PROFILE: ["mac"],
    d.DIM_ACCESS_POINT: ["mac"],
    d.FEAT_DEVICE_STATIC: ["mac"],
    d.FEAT_DEVICE_DAILY: ["local_date", "mac"],
    d.FEAT_DEVICE_HOUR_PROFILE: ["mac"],
    d.FEAT_NETWORK_HOURLY: ["hour_start"],
    d.FEAT_CO_PRESENCE_TOPK: ["mac", "rank"],
}

UNIT_RANGE_COLUMNS = [
    "coverage_ratio",
    "mgmt_share",
    "ctrl_share",
    "data_share",
    "retry_share",
    "presence_ratio",
    "share_of_hourly_packets",
    "jaccard",
]

RSSI_COLUMNS = {"avg_rssi", "min_rssi", "max_rssi", "p50_rssi", "p10_rssi", "p90_rssi"}


def validate_gold(
    tables: dict[str, pl.DataFrame],
    *,
    fact_rows: int,
    dim_rows: int,
) -> GoldValidationSummary:
    missing = set(d.GOLD_TABLES) - set(tables)
    if missing:
        raise ValueError(f"gold transform missing tables: {sorted(missing)}")

    for table_name, keys in UNIQUE_KEYS.items():
        frame = tables[table_name]
        if not set(keys) <= set(frame.columns):
            if frame.height:
                raise ValueError(f"{table_name}: missing key columns {keys}")
            continue
        _assert_no_null_keys(frame, keys, table_name)
        _assert_unique_keys(frame, keys, table_name)

    for table_name, frame in tables.items():
        _assert_in_unit_range(frame, UNIT_RANGE_COLUMNS, table_name)
        for column in RSSI_COLUMNS.intersection(frame.columns):
            if frame.filter((pl.col(column) < -100) | (pl.col(column) > 0)).height:
                raise ValueError(f"{table_name}: {column} outside [-100, 0]")

    network_window = tables[d.AGG_NETWORK_WINDOW]
    if network_window.height == 0:
        raise ValueError("gold transform produced zero network window rows")
    split = network_window.filter(
        pl.col("stable_devices") + pl.col("randomized_macs") != pl.col("device_count")
    )
    if split.height:
        raise ValueError(f"{d.AGG_NETWORK_WINDOW}: stable + randomized != device_count")

    daily = tables[d.AGG_NETWORK_DAILY]
    if daily.filter(pl.col("unique_devices") > dim_rows).height:
        raise ValueError(f"{d.AGG_NETWORK_DAILY}: unique_devices exceeds dim_device rows")

    weekday = tables[d.AGG_WEEKDAY]
    if weekday.height != 7:
        raise ValueError(f"{d.AGG_WEEKDAY}: expected 7 weekday rows, got {weekday.height}")
    if weekday["days_observed"].sum() > daily.height:
        raise ValueError(f"{d.AGG_WEEKDAY}: more observed days than {d.AGG_NETWORK_DAILY} rows")

    profile = tables[d.DIM_DEVICE_PROFILE]
    if profile.height != dim_rows:
        raise ValueError(f"{d.DIM_DEVICE_PROFILE}: row count {profile.height} != dim rows {dim_rows}")
    unknown = set(profile["device_segment"].unique().to_list()) - set(DEVICE_SEGMENTS)
    if unknown:
        raise ValueError(f"{d.DIM_DEVICE_PROFILE}: unexpected segments {sorted(unknown)}")
    if profile.filter(pl.col("is_randomized") != (pl.col("device_segment") == "randomized")).height:
        raise ValueError(f"{d.DIM_DEVICE_PROFILE}: randomized flag and segment disagree")

    calendar = tables[d.AGG_NETWORK_HOURLY]
    if calendar.height > 1:
        steps = calendar["hour_start"].diff().drop_nulls().unique()
        if steps.len() != 1 or steps[0].total_seconds() != 3600:
            raise ValueError(f"{d.AGG_NETWORK_HOURLY}: calendar is not gap-free hourly")

    return GoldValidationSummary(
        fact_rows=fact_rows,
        dim_rows=dim_rows,
        table_rows={name: frame.height for name, frame in tables.items()},
    )
