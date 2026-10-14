from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from etlwifi.config import defaults as d
from etlwifi.gold.aggregates.network import build_agg_anomaly_hourly
from etlwifi.gold.validate import validate_gold
from etlwifi.pipeline.run import run_pipeline_sources
from etlwifi.privacy import hash_mac
from etlwifi.schema import TELEMETRY_COLUMNS
from etlwifi.serving.common import load_gold_snapshot
from tests.conftest import (
    INFRASTRUCTURE,
    PASSERBY,
    REGULAR,
    RESIDENT,
    SALT,
    capture_rows,
    telemetry_rows,
    write_rows,
)


@pytest.fixture(scope="module")
def gold(tmp_path_factory: pytest.TempPathFactory) -> dict[str, pl.DataFrame]:
    root = tmp_path_factory.mktemp("gold")
    rows = capture_rows()
    capture = write_rows(root / "legacy.csv", rows)
    telemetry = write_rows(root / "telemetry.csv", telemetry_rows(rows), TELEMETRY_COLUMNS)
    run_pipeline_sources(
        [capture],
        root / "bronze",
        root / "silver",
        root / "gold",
        dataset="hood",
        telemetry_sources=[telemetry],
        mac_salt=SALT,
    )
    tables, _ = load_gold_snapshot(root / "gold", "hood")
    return tables


def _mac(raw: str) -> str:
    return hash_mac(raw, SALT.encode())


def test_all_tables_built_and_valid(gold: dict[str, pl.DataFrame]) -> None:
    assert list(gold) == list(d.GOLD_TABLES)
    dim_rows = gold[d.DIM_DEVICE_PROFILE].height
    summary = validate_gold(gold, fact_rows=1, dim_rows=dim_rows)
    assert summary.table_rows[d.AGG_NETWORK_WINDOW] > 0


def test_segments_follow_behavioural_rules(gold: dict[str, pl.DataFrame]) -> None:
    profile = gold[d.DIM_DEVICE_PROFILE]
    segments = dict(zip(profile["mac"], profile["device_segment"], strict=True))
    assert segments[_mac(RESIDENT)] == "resident"
    assert segments[_mac(INFRASTRUCTURE)] == "infrastructure"
    assert segments[_mac(REGULAR)] == "regular"
    assert segments[_mac(PASSERBY)] == "passerby"
    randomized = profile.filter(pl.col("is_randomized"))
    assert randomized.height > 0
    assert set(randomized["device_segment"].unique()) == {"randomized"}


def test_hourly_calendar_is_gap_free_with_coverage(gold: dict[str, pl.DataFrame]) -> None:
    hourly = gold[d.AGG_NETWORK_HOURLY].sort("hour_start")
    steps = hourly["hour_start"].diff().drop_nulls().unique()
    assert steps.to_list() == [timedelta(hours=1)]
    covered = hourly.filter(pl.col("coverage_ratio") > 0.99)
    assert covered.height == 9 * 2
    uncovered = hourly.filter(pl.col("coverage_ratio") == 0)
    assert uncovered["unique_devices"].null_count() == uncovered.height


def test_coverage_gaps_are_bounded_by_the_local_day(gold: dict[str, pl.DataFrame]) -> None:
    coverage = gold[d.AGG_CAPTURE_COVERAGE_DAILY].sort("local_date")
    # Capture runs 10:00-12:00 local: 10 h of silence before it, 12 h after (except the final partial day).
    full_days = coverage.head(coverage.height - 1)
    assert full_days["longest_gap_min"].round(0).unique().to_list() == [720.0]
    assert coverage["longest_gap_min"].max() <= 24 * 60
    assert coverage["coverage_ratio"].max() <= 1.0


def test_local_time_is_europe_brussels(gold: dict[str, pl.DataFrame]) -> None:
    hourly = gold[d.AGG_NETWORK_HOURLY].filter(pl.col("coverage_ratio") > 0)
    # Capture runs 08:00-10:00 UTC, i.e. 10:00-12:00 CEST.
    assert set(hourly["local_hour"].unique()) == {10, 11}


def test_window_split_and_churn(gold: dict[str, pl.DataFrame]) -> None:
    window = gold[d.AGG_NETWORK_WINDOW]
    assert (window["stable_devices"] + window["randomized_macs"] == window["device_count"]).all()
    churn = gold[d.AGG_CHURN_DAILY].sort("local_date")
    first = churn.row(0, named=True)
    assert first["first_time_devices"] == first["stable_devices"]
    assert churn["returning_devices"][1:].min() >= 2


def test_features_cover_long_lived_stable_devices(gold: dict[str, pl.DataFrame]) -> None:
    static = gold[d.FEAT_DEVICE_STATIC]
    assert set(static["mac"]) == {_mac(RESIDENT), _mac(INFRASTRUCTURE)}
    profile = gold[d.FEAT_DEVICE_HOUR_PROFILE].filter(pl.col("mac") == _mac(RESIDENT)).row(0, named=True)
    weekday_share = sum(profile[f"wd_h{h:02d}"] for h in range(24))
    weekend_share = sum(profile[f"we_h{h:02d}"] for h in range(24))
    assert math.isclose(weekday_share, 1.0, abs_tol=1e-6)
    assert math.isclose(weekend_share, 1.0, abs_tol=1e-6)
    sessions = gold[d.FEAT_SESSION_EVENTS]
    assert sessions.filter(pl.col("mac") == _mac(RESIDENT)).height == 9


def _hourly(weeks: int, spike_at: int | None = None) -> pl.DataFrame:
    start = datetime(2026, 6, 1, tzinfo=UTC)
    rows = []
    for i in range(weeks * 168):
        hour = i % 24
        level = 20 + 30 * math.sin(math.pi * hour / 24) ** 2
        unique = int(level * (1 + 0.05 * math.sin(i)))
        if i == spike_at:
            unique *= 8
        rows.append(
            {
                "hour_start": start + timedelta(hours=i),
                "local_date": (start + timedelta(hours=i)).date(),
                "local_hour": hour,
                "local_weekday": (start + timedelta(hours=i)).isoweekday(),
                "coverage_ratio": 1.0,
                "unique_devices": unique,
                "packets_per_min": float(unique * 10),
            }
        )
    return pl.DataFrame(rows)


def test_anomaly_detection_flags_spike_against_trailing_baseline() -> None:
    spike = 5 * 168 + 13
    scored = build_agg_anomaly_hourly(_hourly(6, spike_at=spike))
    flagged = scored.filter(pl.col("is_anomaly"))
    expected_hour = datetime(2026, 6, 1, tzinfo=UTC) + timedelta(hours=spike)
    assert flagged["hour_start"].to_list() == [expected_hour]
    assert scored["baseline_samples"].min() >= 2
    assert scored["hour_start"].min() >= datetime(2026, 6, 15, tzinfo=UTC)


def test_anomaly_ignores_low_coverage_hours() -> None:
    hourly = _hourly(4).with_columns(
        pl.when(pl.int_range(pl.len()) % 2 == 0).then(0.1).otherwise(1.0).alias("coverage_ratio")
    )
    scored = build_agg_anomaly_hourly(hourly)
    assert scored["coverage_ratio"].min() >= d.MIN_COVERAGE_FOR_BASELINE
