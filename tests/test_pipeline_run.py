from __future__ import annotations

from pathlib import Path

from etlwifi.bronze.ingest import ingest_csv
from etlwifi.pipeline.run import run_pipeline
from etlwifi.silver.manifest import load_manifest
from etlwifi.silver.transform import transform_dataset

SAMPLE_HEADER = (
    "window_timestamp,mac,packet_count,total_bytes,min_rssi,max_rssi,avg_rssi,"
    "mgmt_pkts,ctrl_pkts,data_pkts,observed_channels\n"
)


def test_run_pipeline_end_to_end(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    bronze_root = tmp_path / "data" / "bronze"
    silver_root = tmp_path / "data" / "silver"

    csv_path.write_text(
        SAMPLE_HEADER
        + "2026-05-27T13:46:09.586928,abc,1,34,-76,-74,-75.0,1,0,0,1|6\n"
        + "2026-05-28T13:46:09.586928,def,2,68,-70,-68,-69.0,2,0,0,11\n",
        encoding="utf-8",
    )

    first = run_pipeline(csv_path, bronze_root, silver_root)
    second = run_pipeline(csv_path, bronze_root, silver_root)

    assert first.bronze.skipped is False
    assert first.silver.skipped is False
    assert first.silver.validation is not None
    assert first.silver.validation.fact_rows == 2

    assert second.bronze.skipped is True
    assert second.silver.skipped is True
    assert len(load_manifest(silver_root / "_manifest.jsonl")) == 1


def test_run_pipeline_silver_runs_after_bronze_skip(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    bronze_root = tmp_path / "data" / "bronze"
    silver_root = tmp_path / "data" / "silver"

    csv_path.write_text(
        SAMPLE_HEADER + "2026-05-27T13:46:09.586928,abc,1,34,-76,-74,-75.0,1,0,0,1\n",
        encoding="utf-8",
    )

    ingest_csv(csv_path, bronze_root)
    result = run_pipeline(csv_path, bronze_root, silver_root)

    assert result.bronze.skipped is True
    assert result.silver.skipped is False
    assert result.silver.validation is not None
    assert result.silver.validation.fact_rows == 1
