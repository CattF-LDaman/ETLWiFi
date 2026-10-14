from __future__ import annotations

from pathlib import Path

import pytest

from etlwifi.config.paths import DataPaths
from etlwifi.config.settings import Settings
from etlwifi.gold.manifest import load_manifest as load_gold_manifest
from etlwifi.pipeline.run import (
    append_run_record,
    discover_sources,
    load_run_records,
    pipeline_lock,
    run_configured_pipeline,
    run_pipeline,
    run_record,
)
from etlwifi.schema import TELEMETRY_COLUMNS
from tests.conftest import SALT, capture_rows, telemetry_rows, write_rows


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    rows = capture_rows(days=3)
    legacy = write_rows(tmp_path / "my_neighborhood.csv", rows[: len(rows) // 2])
    paths = DataPaths.from_root(tmp_path / "data")
    # The landing file overlaps the legacy export, as during a shadow-mode cutover.
    write_rows(paths.capture_landing / "2026-06-02.csv", rows[len(rows) // 3 :])
    write_rows(paths.telemetry_landing / "2026-06-02.csv", telemetry_rows(rows), TELEMETRY_COLUMNS)
    return Settings(data_root=tmp_path / "data", dataset="hood", mac_hash_salt=SALT, legacy_csv=legacy)


def test_discover_sources_puts_legacy_first(settings: Settings) -> None:
    capture, telemetry = discover_sources(settings, DataPaths.from_root(settings.data_root))
    assert capture[0] == settings.legacy_csv
    assert [p.name for p in capture[1:]] == ["2026-06-02.csv"]
    assert len(telemetry) == 1


def test_configured_pipeline_is_idempotent_and_dedupes_overlap(settings: Settings) -> None:
    first = run_configured_pipeline(settings)
    second = run_configured_pipeline(settings)

    assert first.silver.validation is not None
    assert first.silver.validation.fact_rows == len(capture_rows(days=3))
    assert first.silver.validation.clean.duplicates_removed > 0
    assert first.silver.capture_windows > 0
    assert first.gold.skipped is False
    assert set(first.durations) == {"bronze", "silver", "gold"}

    assert second.bronze.skipped and second.silver.skipped and second.gold.skipped
    gold_root = DataPaths.from_root(settings.data_root).gold
    assert len(load_gold_manifest(gold_root / "_manifest.jsonl")) == 1


def test_run_records_round_trip(settings: Settings, tmp_path: Path) -> None:
    from datetime import UTC, datetime

    result = run_configured_pipeline(settings)
    state = tmp_path / "state"
    append_run_record(state, run_record(result, started_at=datetime.now(UTC)))
    append_run_record(state, run_record(None, started_at=datetime.now(UTC), error="boom"))

    records = load_run_records(state)
    assert [r["status"] for r in records] == ["success", "failed"]
    assert records[0]["gold_table_rows"]
    assert records[1]["error"] == "boom"


def test_pipeline_lock_rejects_concurrent_runs(tmp_path: Path) -> None:
    with pipeline_lock(tmp_path), pytest.raises(RuntimeError, match="in progress"), pipeline_lock(tmp_path):
        pass


def test_single_file_wrapper(tmp_path: Path) -> None:
    csv_path = write_rows(tmp_path / "sample.csv", capture_rows(days=1, hours_per_day=1))
    result = run_pipeline(
        csv_path, tmp_path / "bronze", tmp_path / "silver", tmp_path / "gold", mac_salt=SALT
    )
    assert result.dataset == "sample"
    assert result.gold.validation is not None
