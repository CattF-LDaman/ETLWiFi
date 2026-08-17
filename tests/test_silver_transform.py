from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from etlwifi.bronze.ingest import ingest_csv
from etlwifi.io.timestamps import WINDOW_TIMESTAMP_COLUMN
from etlwifi.silver.channels import build_channel_tables
from etlwifi.silver.clean import clean_bronze
from etlwifi.silver.manifest import load_manifest
from etlwifi.silver.models import build_dim_device, build_fact_device_window
from etlwifi.silver.transform import transform_dataset
from etlwifi.silver.clean import CleanSummary
from etlwifi.silver.validate import validate_silver

SAMPLE_HEADER = (
    "window_timestamp,mac,packet_count,total_bytes,min_rssi,max_rssi,avg_rssi,"
    "mgmt_pkts,ctrl_pkts,data_pkts,observed_channels\n"
)


def _write_sample_csv(path: Path, *rows: str) -> None:
    path.write_text(SAMPLE_HEADER + "\n".join(rows) + "\n", encoding="utf-8")


def _bronze_lazy(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(rows)


def test_clean_parses_microsecond_second_and_nul_timestamps() -> None:
    frame = _bronze_lazy(
        [
            {
                WINDOW_TIMESTAMP_COLUMN: "2026-05-27T13:46:09.586928",
                "mac": "a",
                "packet_count": 1,
                "total_bytes": 34,
                "min_rssi": -76,
                "max_rssi": -74,
                "avg_rssi": -75.0,
                "mgmt_pkts": 1,
                "ctrl_pkts": 0,
                "data_pkts": 0,
                "observed_channels": "1",
                "_ingested_at": datetime.now(UTC),
                "_source_file": "sample.csv",
            },
            {
                WINDOW_TIMESTAMP_COLUMN: "2026-06-22T23:32:31",
                "mac": "b",
                "packet_count": 2,
                "total_bytes": 68,
                "min_rssi": -70,
                "max_rssi": -68,
                "avg_rssi": -69.0,
                "mgmt_pkts": 2,
                "ctrl_pkts": 0,
                "data_pkts": 0,
                "observed_channels": "6",
                "_ingested_at": datetime.now(UTC),
                "_source_file": "sample.csv",
            },
            {
                WINDOW_TIMESTAMP_COLUMN: "\x00\x002026-05-28T21:26:13.116098",
                "mac": "c",
                "packet_count": 3,
                "total_bytes": 90,
                "min_rssi": -72,
                "max_rssi": -70,
                "avg_rssi": -71.0,
                "mgmt_pkts": 3,
                "ctrl_pkts": 0,
                "data_pkts": 0,
                "observed_channels": "11",
                "_ingested_at": datetime.now(UTC),
                "_source_file": "sample.csv",
            },
        ]
    )

    cleaned, summary = clean_bronze(frame)

    assert summary.output_rows == 3
    assert summary.parse_failures == 0
    assert cleaned["timestamp"].dtype.time_zone == "UTC"


def test_channel_split_and_count() -> None:
    frame = pl.DataFrame(
        {
            "timestamp": [
                datetime(2026, 5, 27, 13, 46, 9, tzinfo=UTC),
                datetime(2026, 5, 27, 13, 46, 19, tzinfo=UTC),
            ],
            "mac": ["abc", "def"],
            "observed_channels": ["1|6", "11"],
        }
    )

    bridge, counts = build_channel_tables(frame)
    fact = build_fact_device_window(
        pl.DataFrame(
            {
                "timestamp": frame["timestamp"],
                "mac": frame["mac"],
                "packet_count": [1, 2],
                "total_bytes": [34, 68],
                "min_rssi": [-76, -70],
                "max_rssi": [-74, -68],
                "avg_rssi": [-75.0, -69.0],
                "mgmt_pkts": [1, 2],
                "ctrl_pkts": [0, 0],
                "data_pkts": [0, 0],
                "_ingested_at": [datetime.now(UTC), datetime.now(UTC)],
                "_source_file": ["sample.csv", "sample.csv"],
            }
        ),
        counts,
    )

    assert bridge.height == 3
    assert fact.filter(pl.col("mac") == "abc").item(0, "channel_count") == 2
    assert fact.filter(pl.col("mac") == "def").item(0, "channel_count") == 1


def test_dim_device_lifetime_metrics() -> None:
    timestamps = [
        datetime(2026, 5, 27, 13, 46, 9, tzinfo=UTC),
        datetime(2026, 5, 27, 13, 46, 19, tzinfo=UTC),
        datetime(2026, 5, 28, 10, 0, 0, tzinfo=UTC),
    ]
    fact = pl.DataFrame(
        {
            "timestamp": timestamps,
            "mac": ["abc", "abc", "abc"],
            "packet_count": [1, 3, 2],
            "total_bytes": [34, 90, 60],
            "min_rssi": [-76, -74, -70],
            "max_rssi": [-74, -72, -68],
            "avg_rssi": [-75.0, -73.0, -69.0],
            "mgmt_pkts": [1, 3, 2],
            "ctrl_pkts": [0, 0, 0],
            "data_pkts": [0, 0, 0],
            "channel_count": [2, 1, 1],
            "_ingested_at": [datetime.now(UTC)] * 3,
            "_source_file": ["sample.csv"] * 3,
        }
    )
    bridge = pl.DataFrame(
        {
            "timestamp": [timestamps[0], timestamps[0], timestamps[1], timestamps[2]],
            "mac": ["abc", "abc", "abc", "abc"],
            "channel": [1, 6, 1, 11],
        }
    )

    dim = build_dim_device(fact, bridge)
    row = dim.row(0, named=True)

    assert row["mac"] == "abc"
    assert row["window_count"] == 3
    assert row["active_days"] == 2
    assert row["distinct_channels"] == 3
    assert row["total_packets"] == 6
    assert row["total_bytes"] == 184
    assert row["avg_rssi"] == pytest.approx(-72.3333333)


def test_clean_dedupes_on_timestamp_mac() -> None:
    frame = _bronze_lazy(
        [
            {
                WINDOW_TIMESTAMP_COLUMN: "2026-05-27T13:46:09.586928",
                "mac": "abc",
                "packet_count": 1,
                "total_bytes": 34,
                "min_rssi": -76,
                "max_rssi": -74,
                "avg_rssi": -75.0,
                "mgmt_pkts": 1,
                "ctrl_pkts": 0,
                "data_pkts": 0,
                "observed_channels": "1",
                "_ingested_at": datetime.now(UTC),
                "_source_file": "sample.csv",
            },
            {
                WINDOW_TIMESTAMP_COLUMN: "2026-05-27T13:46:09.586928",
                "mac": "abc",
                "packet_count": 9,
                "total_bytes": 90,
                "min_rssi": -60,
                "max_rssi": -58,
                "avg_rssi": -59.0,
                "mgmt_pkts": 9,
                "ctrl_pkts": 0,
                "data_pkts": 0,
                "observed_channels": "1",
                "_ingested_at": datetime.now(UTC),
                "_source_file": "sample.csv",
            },
        ]
    )

    cleaned, summary = clean_bronze(frame)

    assert summary.duplicates_removed == 1
    assert cleaned.height == 1
    assert cleaned.item(0, "packet_count") == 9


def test_validate_rejects_bad_rssi() -> None:
    fact = pl.DataFrame(
        {
            "timestamp": [datetime(2026, 5, 27, 13, 46, 9, tzinfo=UTC)],
            "mac": ["abc"],
            "packet_count": [1],
            "total_bytes": [34],
            "min_rssi": [-76],
            "max_rssi": [-74],
            "avg_rssi": [5.0],
            "mgmt_pkts": [1],
            "ctrl_pkts": [0],
            "data_pkts": [0],
            "channel_count": [1],
            "_ingested_at": [datetime.now(UTC)],
            "_source_file": ["sample.csv"],
        }
    )
    bridge = pl.DataFrame(
        {
            "timestamp": fact["timestamp"],
            "mac": fact["mac"],
            "channel": [1],
        }
    )
    dim = build_dim_device(fact, bridge)

    with pytest.raises(ValueError, match="avg_rssi"):
        validate_silver(
            fact,
            bridge,
            dim,
            CleanSummary(
                input_rows=1,
                output_rows=1,
                parse_failures=0,
                duplicates_removed=0,
            ),
        )


def test_silver_transform_integration(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    bronze_root = tmp_path / "data" / "bronze"
    silver_root = tmp_path / "data" / "silver"

    _write_sample_csv(
        csv_path,
        "2026-05-27T13:46:09.586928,abc,1,34,-76,-74,-75.0,1,0,0,1|6",
        "2026-05-28T13:46:09.586928,def,2,68,-70,-68,-69.0,2,0,0,11",
    )
    ingest_csv(csv_path, bronze_root)

    first = transform_dataset("sample", bronze_root, silver_root)
    second = transform_dataset("sample", bronze_root, silver_root)

    assert first.skipped is False
    assert first.validation is not None
    assert first.validation.fact_rows == 2
    assert first.validation.bridge_rows == 3
    assert first.validation.dim_rows == 2
    assert second.skipped is True
    assert len(load_manifest(silver_root / "_manifest.jsonl")) == 1

    _write_sample_csv(
        csv_path,
        "2026-05-27T13:46:09.586928,abc,1,34,-76,-74,-75.0,1,0,0,1|6",
    )
    ingest_csv(csv_path, bronze_root)
    third = transform_dataset("sample", bronze_root, silver_root)

    assert third.skipped is False
    assert third.validation is not None
    assert third.validation.fact_rows == 1
    assert third.replaced_parts > 0
    assert len(load_manifest(silver_root / "_manifest.jsonl")) == 2
