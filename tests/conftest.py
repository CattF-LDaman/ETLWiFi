from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from etlwifi.schema import LEGACY_CAPTURE_COLUMNS, TELEMETRY_COLUMNS

SALT = "test-salt"
WINDOW = 10
START = datetime(2026, 6, 1, 8, 0, 0)  # naive UTC, like the legacy sniffer

RESIDENT = "00:1c:f0:aa:bb:01"
INFRASTRUCTURE = "00:1c:f0:aa:bb:02"
REGULAR = "3c:22:fb:00:00:01"
PASSERBY = "3c:22:fb:00:00:02"


def _row(ts: datetime, mac: str, rng: random.Random, *, mgmt_only: bool = False) -> dict:
    packets = rng.randint(1, 12)
    rssi = -60 if mgmt_only else rng.randint(-85, -45)
    mgmt = packets if mgmt_only else packets // 2
    return {
        "window_timestamp": ts.isoformat(timespec="microseconds"),
        "mac": mac,
        "packet_count": packets,
        "total_bytes": packets * 120,
        "min_rssi": rssi - (0 if mgmt_only else 3),
        "max_rssi": rssi + (0 if mgmt_only else 3),
        "avg_rssi": float(rssi),
        "mgmt_pkts": mgmt,
        "ctrl_pkts": 0,
        "data_pkts": packets - mgmt,
        "observed_channels": rng.choice(["1", "6", "11", "1|6"]),
    }


def capture_rows(days: int = 9, hours_per_day: int = 2, seed: int = 7) -> list[dict]:
    rng = random.Random(seed)
    rows: list[dict] = []
    windows_per_day = hours_per_day * 3600 // WINDOW
    randomized_id = 0
    for day in range(days):
        day_start = START + timedelta(days=day)
        for index in range(windows_per_day):
            ts = day_start + timedelta(seconds=index * WINDOW)
            rows.append(_row(ts, RESIDENT, rng))
            rows.append(_row(ts, INFRASTRUCTURE, rng, mgmt_only=True))
            if index == 100 and day % 3 == 0:
                rows.append(_row(ts, REGULAR, rng))
            if index == 200 and day == 3:
                rows.append(_row(ts, PASSERBY, rng))
            if index % 3 == 0:
                randomized_id += 1
                rows.append(
                    _row(
                        ts,
                        f"da:a1:{randomized_id >> 16 & 0xFF:02x}:"
                        f"{randomized_id >> 8 & 0xFF:02x}:{randomized_id & 0xFF:02x}:01",
                        rng,
                    )
                )
    return rows


def write_rows(path: Path, rows: list[dict], columns: tuple[str, ...] = LEGACY_CAPTURE_COLUMNS) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return path


def telemetry_rows(capture: list[dict]) -> list[dict]:
    windows = sorted({row["window_timestamp"] for row in capture})
    return [
        {
            "window_timestamp": ts,
            "window_seconds": WINDOW,
            "frames_total": 40,
            "frames_dot11": 38,
            "frames_no_sender": 4,
            "devices": 3,
            "channel_dwell": "1:4.00|6:4.00|11:2.00",
            "interface_type": "monitor",
            "capture_version": "2.0.0",
        }
        for ts in windows
    ]


@pytest.fixture(scope="session")
def sample_rows() -> list[dict]:
    return capture_rows()


@pytest.fixture
def capture_csv(tmp_path: Path, sample_rows: list[dict]) -> Path:
    return write_rows(tmp_path / "legacy.csv", sample_rows)


@pytest.fixture
def telemetry_csv(tmp_path: Path, sample_rows: list[dict]) -> Path:
    return write_rows(tmp_path / "telemetry.csv", telemetry_rows(sample_rows), TELEMETRY_COLUMNS)
