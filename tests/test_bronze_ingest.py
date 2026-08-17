from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from etlwifi.bronze.hash import file_sha256
from etlwifi.bronze.ingest import TIMESTAMP_COLUMN, _prepare_frame, ingest_csv
from etlwifi.bronze.manifest import find_ingest_by_hash, load_manifest


def test_prepare_frame_strips_null_bytes_from_timestamp() -> None:
    frame = pl.DataFrame(
        {
            TIMESTAMP_COLUMN: [
                "2026-05-28T21:26:13.116098",
                "\x00\x00\x002026-05-28T21:26:13.116098",
            ],
            "mac": ["aaa", "bbb"],
        }
    )

    cleaned = _prepare_frame(frame)

    assert cleaned[TIMESTAMP_COLUMN].to_list() == [
        "2026-05-28T21:26:13.116098",
        "2026-05-28T21:26:13.116098",
    ]


def test_prepare_frame_rejects_invalid_partition_dates() -> None:
    frame = pl.DataFrame({TIMESTAMP_COLUMN: ["not-a-date"], "mac": ["aaa"]})

    with pytest.raises(ValueError, match="invalid partition dates"):
        _prepare_frame(frame)


def test_ingest_skips_when_content_hash_unchanged(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text(
        "window_timestamp,mac,packet_count,total_bytes,min_rssi,max_rssi,avg_rssi,"
        "mgmt_pkts,ctrl_pkts,data_pkts,observed_channels\n"
        "2026-05-27T13:46:09.586928,abc,1,34,-76,-74,-75.0,1,0,0,1\n",
        encoding="utf-8",
    )
    bronze_root = tmp_path / "data" / "bronze"

    first = ingest_csv(csv_path, bronze_root)
    second = ingest_csv(csv_path, bronze_root)

    assert first.skipped is False
    assert first.rows == 1
    assert second.skipped is True
    assert len(load_manifest(bronze_root / "_manifest.jsonl")) == 1


def test_ingest_replaces_prior_parts_when_content_changes(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    bronze_root = tmp_path / "data" / "bronze"

    csv_path.write_text(
        "window_timestamp,mac,packet_count,total_bytes,min_rssi,max_rssi,avg_rssi,"
        "mgmt_pkts,ctrl_pkts,data_pkts,observed_channels\n"
        "2026-05-27T13:46:09.586928,abc,1,34,-76,-74,-75.0,1,0,0,1\n",
        encoding="utf-8",
    )
    first = ingest_csv(csv_path, bronze_root)

    csv_path.write_text(
        "window_timestamp,mac,packet_count,total_bytes,min_rssi,max_rssi,avg_rssi,"
        "mgmt_pkts,ctrl_pkts,data_pkts,observed_channels\n"
        "2026-05-27T13:46:09.586928,abc,2,68,-76,-74,-75.0,2,0,0,1\n"
        "2026-05-28T13:46:09.586928,def,1,34,-76,-74,-75.0,1,0,0,1\n",
        encoding="utf-8",
    )
    second = ingest_csv(csv_path, bronze_root)

    assert second.skipped is False
    assert second.rows == 2
    assert second.replaced_parts == len(first.parts)
    first_hash = load_manifest(bronze_root / "_manifest.jsonl")[0]["content_hash"]
    second_hash = file_sha256(csv_path)
    assert second_hash != first_hash

    current_parts = list(bronze_root.rglob("*.parquet"))
    assert len(current_parts) == 2
    total_rows = sum(pl.read_parquet(part).height for part in current_parts)
    assert total_rows == 2

    manifest = load_manifest(bronze_root / "_manifest.jsonl")
    assert len(manifest) == 2
    assert find_ingest_by_hash(manifest, csv_path, file_sha256(csv_path)) is not None
