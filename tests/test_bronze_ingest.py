from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from etlwifi.bronze.ingest import TIMESTAMP_COLUMN, _prepare_frame, ingest_csv, ingest_sources
from etlwifi.bronze.manifest import load_manifest
from etlwifi.privacy import hash_mac, is_raw_mac
from etlwifi.schema import CAPTURE_COLUMNS, TELEMETRY_COLUMNS
from tests.conftest import RESIDENT, SALT, write_rows

HEADER = (
    "window_timestamp,mac,packet_count,total_bytes,min_rssi,max_rssi,avg_rssi,"
    "mgmt_pkts,ctrl_pkts,data_pkts,observed_channels\n"
)


def _read_bronze(bronze_root: Path, dataset: str) -> pl.DataFrame:
    return pl.concat([pl.read_parquet(p) for p in sorted((bronze_root / dataset).rglob("*.parquet"))])


def test_prepare_frame_strips_null_bytes_from_timestamp() -> None:
    frame = pl.DataFrame(
        {
            TIMESTAMP_COLUMN: ["2026-05-28T21:26:13.116098", "\x00\x00\x002026-05-28T21:26:13.116098"],
            "mac": ["aaa", "bbb"],
        }
    )
    assert _prepare_frame(frame)[TIMESTAMP_COLUMN].to_list() == ["2026-05-28T21:26:13.116098"] * 2


def test_prepare_frame_rejects_invalid_partition_dates() -> None:
    frame = pl.DataFrame({TIMESTAMP_COLUMN: ["not-a-date"], "mac": ["aaa"]})
    with pytest.raises(ValueError, match="invalid partition dates"):
        _prepare_frame(frame)


def test_legacy_csv_is_conformed_and_pseudonymised(capture_csv: Path, tmp_path: Path) -> None:
    bronze_root = tmp_path / "bronze"
    result = ingest_csv(capture_csv, bronze_root, dataset="hood", mac_salt=SALT)

    frame = _read_bronze(bronze_root, "hood")
    assert result.rows == frame.height
    assert result.hashed_macs == frame["mac"].n_unique()
    assert set(CAPTURE_COLUMNS) <= set(frame.columns)
    assert all(dtype == pl.Utf8 for name, dtype in frame.schema.items() if name in CAPTURE_COLUMNS)
    assert not any(is_raw_mac(mac) for mac in frame["mac"].unique().to_list())

    resident = frame.filter(pl.col("mac") == hash_mac(RESIDENT, SALT.encode()))
    assert resident.height > 0
    assert resident["mac_randomized"].unique().to_list() == ["0"]
    assert resident["oui"].unique().to_list() == ["001CF0"]
    randomized = frame.filter(pl.col("mac_randomized") == "1")
    assert randomized.height > 0
    assert randomized["oui"].null_count() == randomized.height


def test_raw_macs_without_salt_fail_closed(capture_csv: Path, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="MAC_HASH_SALT"):
        ingest_csv(capture_csv, tmp_path / "bronze", dataset="hood")
    assert not (tmp_path / "bronze" / "hood").exists()


def test_already_pseudonymised_input_needs_no_salt(tmp_path: Path) -> None:
    csv_path = tmp_path / "hashed.csv"
    csv_path.write_text(HEADER + "2026-05-27T13:46:09.586928,3f2a9c0b1d4e5f60,1,34,-76,-74,-75.0,1,0,0,1\n")
    result = ingest_csv(csv_path, tmp_path / "bronze", dataset="hood")
    assert result.rows == 1
    assert result.hashed_macs == 0


def test_ingest_skips_when_content_hash_unchanged(capture_csv: Path, tmp_path: Path) -> None:
    bronze_root = tmp_path / "bronze"
    first = ingest_csv(capture_csv, bronze_root, dataset="hood", mac_salt=SALT)
    second = ingest_csv(capture_csv, bronze_root, dataset="hood", mac_salt=SALT)
    assert first.skipped is False
    assert second.skipped is True
    assert len(load_manifest(bronze_root / "_manifest.jsonl")) == 1


def test_sources_are_tracked_independently(tmp_path: Path) -> None:
    bronze_root = tmp_path / "bronze"
    day_one = tmp_path / "landing" / "2026-06-01.csv"
    day_two = tmp_path / "landing" / "2026-06-02.csv"
    day_one.parent.mkdir()
    day_one.write_text(HEADER + "2026-06-01T10:00:00.000000,3f2a9c0b1d4e5f60,1,34,-76,-74,-75.0,1,0,0,1\n")
    day_two.write_text(HEADER + "2026-06-02T10:00:00.000000,3f2a9c0b1d4e5f60,1,34,-76,-74,-75.0,1,0,0,1\n")

    ingest_sources([day_one, day_two], bronze_root, dataset="hood")
    day_two.write_text(
        day_two.read_text() + "2026-06-02T10:00:10.000000,3f2a9c0b1d4e5f60,2,68,-76,-74,-75.0,2,0,0,6\n"
    )
    results = ingest_sources([day_one, day_two], bronze_root, dataset="hood")

    assert [r.skipped for r in results] == [True, False]
    assert results[1].replaced_parts == 1
    parts = sorted((bronze_root / "hood").rglob("*.parquet"))
    assert len(parts) == 2
    assert len({p.name for p in parts}) == 2
    assert sum(pl.read_parquet(p).height for p in parts) == 3


def test_header_only_landing_file_produces_no_parts(tmp_path: Path) -> None:
    csv_path = tmp_path / "2026-06-03.csv"
    csv_path.write_text(HEADER)
    result = ingest_csv(csv_path, tmp_path / "bronze", dataset="hood")
    assert result.rows == 0
    assert result.parts == ()


def test_telemetry_ingest_uses_its_own_contract(tmp_path: Path) -> None:
    path = write_rows(
        tmp_path / "telemetry.csv",
        [{"window_timestamp": "2026-06-01T10:00:00.000000", "window_seconds": 10, "devices": 0}],
        TELEMETRY_COLUMNS,
    )
    result = ingest_csv(path, tmp_path / "bronze", dataset="hood_telemetry", kind="telemetry")
    frame = _read_bronze(tmp_path / "bronze", "hood_telemetry")
    assert result.rows == 1
    assert set(TELEMETRY_COLUMNS) <= set(frame.columns)
    assert "mac" not in frame.columns
