from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

from etlwifi.config import defaults as d
from etlwifi.gold.aggregates.network import build_agg_anomaly_hourly
from etlwifi.gold.aggregates.weekday import build_agg_weekday
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
        stable = max(1, int(unique * 0.25))
        randomized = max(0, unique - stable)
        if i == spike_at:
            stable *= 8
            unique *= 8
            randomized = max(randomized, unique - stable)
        rows.append(
            {
                "hour_start": start + timedelta(hours=i),
                "local_date": (start + timedelta(hours=i)).date(),
                "local_hour": hour,
                "local_weekday": (start + timedelta(hours=i)).isoweekday(),
                "coverage_ratio": 1.0,
                "unique_devices": unique,
                "stable_devices": stable,
                "randomized_macs": randomized,
                "packets_per_min": float(unique * 10),
            }
        )
    return pl.DataFrame(rows)


def test_unique_spike_flat_stable_not_primary_anomaly() -> None:
    hourly = _hourly(6)
    spike = 5 * 168 + 13
    hourly = hourly.with_columns(
        pl.when(pl.int_range(pl.len()) == spike)
        .then(pl.lit(500))
        .otherwise(pl.col("unique_devices"))
        .alias("unique_devices"),
        pl.when(pl.int_range(pl.len()) == spike)
        .then(pl.lit(490))
        .otherwise(pl.col("randomized_macs"))
        .alias("randomized_macs"),
    )
    scored = build_agg_anomaly_hourly(hourly)
    expected_hour = datetime(2026, 6, 1, tzinfo=UTC) + timedelta(hours=spike)
    row = scored.filter(pl.col("hour_start") == expected_hour).row(0, named=True)
    assert row["is_anomaly"] is False
    assert row["is_randomized_burst"] is True


def test_anomaly_detection_flags_spike_against_trailing_baseline() -> None:
    spike = 5 * 168 + 13
    scored = build_agg_anomaly_hourly(_hourly(6, spike_at=spike))
    flagged = scored.filter(pl.col("is_anomaly"))
    expected_hour = datetime(2026, 6, 1, tzinfo=UTC) + timedelta(hours=spike)
    assert flagged["hour_start"].to_list() == [expected_hour]
    assert scored["baseline_samples"].min() >= 2
    assert scored["hour_start"].min() >= datetime(2026, 6, 15, tzinfo=UTC)


def _weekday_inputs() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    first = date(2026, 6, 1)  # a Monday
    daily_rows, presence_rows = [], []
    for offset in range(21):
        day = first + timedelta(days=offset)
        weekday = day.isoweekday()
        busy = 40 if weekday == 3 else 10 if weekday == 7 else 20
        daily_rows.append(
            {
                "local_date": day,
                # Every fourth day is barely captured and must not count.
                "coverage_ratio": 0.2 if offset % 4 == 3 else 1.0,
                # The last day is still in progress: fully covered so far, but only 14 hours long.
                "calendar_hours": 14 if offset == 20 else 24,
                "stable_devices": busy + offset,
                "randomized_macs": 5 * busy,
                "estimated_devices": None,
                "total_packets": 100 * busy,
                "new_stable_devices": 1,
                "peak_concurrent_devices": busy // 2,
                "mean_concurrent_devices": busy / 4,
                "median_stable_active_minutes": 30.0,
            }
        )
        presence_rows += [{"local_date": day, "mac": "res", "device_class": "stable"}]
        presence_rows += [
            {"local_date": day, "mac": f"p{offset}-{i}", "device_class": "stable"} for i in range(busy)
        ]
        presence_rows += [{"local_date": day, "mac": f"r{offset}", "device_class": "randomized"}]
    presence = pl.DataFrame(presence_rows)
    profile = (
        presence.select("mac")
        .unique()
        .with_columns(
            pl.when(pl.col("mac") == "res")
            .then(pl.lit("resident"))
            .when(pl.col("mac").str.starts_with("r"))
            .then(pl.lit("randomized"))
            .otherwise(pl.lit("passerby"))
            .alias("device_segment")
        )
    )
    hour_of_week = pl.DataFrame(
        {
            "local_weekday": pl.Series([w for w in range(1, 8) for _ in range(24)], dtype=pl.Int8),
            "local_hour": pl.Series(list(range(24)) * 7, dtype=pl.Int8),
            "median_stable_devices": [float((h - 17) % 24) for _ in range(7) for h in range(24)],
        }
    )
    return pl.DataFrame(daily_rows), presence, profile, hour_of_week


def test_weekday_profile_ranks_days_from_well_covered_days_only() -> None:
    weekday = build_agg_weekday(*_weekday_inputs())
    assert weekday.height == 7
    assert weekday["weekday_name"].to_list()[0] == "Monday"
    by_day = {row["local_weekday"]: row for row in weekday.iter_rows(named=True)}
    assert sum(row["days_observed"] for row in by_day.values()) == 15
    assert by_day[7]["days_observed"] == 2
    assert by_day[3]["activity_rank"] == 1
    assert by_day[7]["activity_rank"] == 7
    assert by_day[3]["activity_index"] > 1 > by_day[7]["activity_index"]
    assert by_day[6]["is_weekend"] and not by_day[5]["is_weekend"]
    assert by_day[3]["stable_min"] <= by_day[3]["stable_p25"] <= by_day[3]["stable_median"]
    assert by_day[3]["stable_median"] <= by_day[3]["stable_p75"] <= by_day[3]["stable_max"]
    assert by_day[1]["resident_median"] == 1
    assert by_day[3]["passerby_median"] == 40
    assert by_day[1]["peak_hour"] == 16
    assert by_day[1]["quiet_hour"] == 17
    assert by_day[1]["estimated_days"] == 0


def test_weekday_profile_keeps_seven_rows_without_covered_days(gold: dict[str, pl.DataFrame]) -> None:
    weekday = gold[d.AGG_WEEKDAY]
    assert weekday["local_weekday"].to_list() == list(range(1, 8))
    assert weekday["days_observed"].sum() == 0
    assert weekday["activity_rank"].null_count() == 7


def test_anomaly_ignores_low_coverage_hours() -> None:
    hourly = _hourly(4).with_columns(
        pl.when(pl.int_range(pl.len()) % 2 == 0).then(0.1).otherwise(1.0).alias("coverage_ratio")
    )
    scored = build_agg_anomaly_hourly(hourly)
    assert scored["coverage_ratio"].min() >= d.MIN_COVERAGE_FOR_BASELINE
