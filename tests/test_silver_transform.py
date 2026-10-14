from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from etlwifi.bronze.ingest import ingest_csv
from etlwifi.config import defaults as d
from etlwifi.privacy import hash_mac
from etlwifi.silver.manifest import load_manifest
from etlwifi.silver.read import find_latest_bronze_snapshot
from etlwifi.silver.transform import transform_dataset
from tests.conftest import INFRASTRUCTURE, RESIDENT, SALT, write_rows


def _table(silver_root: Path, name: str) -> pl.DataFrame:
    return pl.concat([pl.read_parquet(p) for p in sorted((silver_root / name).rglob("*.parquet"))])


@pytest.fixture
def reference_dir(tmp_path: Path) -> Path:
    path = tmp_path / "reference"
    path.mkdir()
    (path / "oui.csv").write_text(
        "Registry,Assignment,Organization Name,Organization Address\n"
        'MA-L,001CF0,"D-Link Corporation",Taipei\n',
        encoding="utf-8",
    )
    return path


def test_silver_builds_typed_tables_with_vendor_and_capture_windows(
    capture_csv: Path, telemetry_csv: Path, reference_dir: Path, tmp_path: Path
) -> None:
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    ingest_csv(capture_csv, bronze, dataset="hood", mac_salt=SALT)
    ingest_csv(telemetry_csv, bronze, dataset="hood_telemetry", kind="telemetry")

    result = transform_dataset(
        "hood", bronze, silver, telemetry_dataset="hood_telemetry", reference_dir=reference_dir
    )
    assert result.skipped is False
    assert result.capture_windows > 0

    fact = _table(silver, d.FACT_TABLE)
    assert fact.schema["timestamp"] == pl.Datetime("us", "UTC")
    assert fact.schema["packet_count"].is_integer()

    dim = _table(silver, d.DIM_TABLE)
    salt = SALT.encode()
    resident = dim.filter(pl.col("mac") == hash_mac(RESIDENT, salt)).row(0, named=True)
    assert resident["is_randomized"] is False
    assert resident["oui"] == "001CF0"
    assert resident["vendor"] == "D-Link"
    assert dim.filter(pl.col("is_randomized")).height > 0
    assert (
        dim.filter(pl.col("is_randomized"))["vendor"].null_count()
        == dim.filter(pl.col("is_randomized")).height
    )

    windows = _table(silver, d.CAPTURE_WINDOW_TABLE)
    assert windows.height == result.capture_windows
    dwell = _table(silver, d.CAPTURE_DWELL_TABLE)
    assert set(dwell["channel"].unique().to_list()) == {1, 6, 11}

    again = transform_dataset(
        "hood", bronze, silver, telemetry_dataset="hood_telemetry", reference_dir=reference_dir
    )
    assert again.skipped is True
    assert len(load_manifest(silver / "_manifest.jsonl")) == 1


def test_snapshot_merges_sources_and_dedupes_overlap(tmp_path: Path, sample_rows: list[dict]) -> None:
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    half = len(sample_rows) // 2
    first = write_rows(tmp_path / "a.csv", sample_rows[: half + 50])
    second = write_rows(tmp_path / "b.csv", sample_rows[half:])
    ingest_csv(first, bronze, dataset="hood", mac_salt=SALT)
    ingest_csv(second, bronze, dataset="hood", mac_salt=SALT)

    snapshot = find_latest_bronze_snapshot(bronze, "hood")
    assert snapshot.sources == 2

    result = transform_dataset("hood", bronze, silver)
    assert result.validation is not None
    assert result.validation.fact_rows == len(sample_rows)
    assert result.validation.clean.duplicates_removed == 50
    infra = _table(silver, d.DIM_TABLE).filter(pl.col("mac") == hash_mac(INFRASTRUCTURE, SALT.encode()))
    assert infra.height == 1


def test_missing_bronze_dataset_raises(tmp_path: Path) -> None:
    with pytest.raises((FileNotFoundError, ValueError)):
        transform_dataset("nope", tmp_path / "bronze", tmp_path / "silver")
