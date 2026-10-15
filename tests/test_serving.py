from __future__ import annotations

import gzip
import hashlib
import hmac
import json
import threading
from datetime import UTC, date, datetime, timedelta
from email.message import Message
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import ClassVar

import pytest

from etlwifi.config.paths import DataPaths
from etlwifi.config.settings import Settings
from etlwifi.pipeline.run import load_run_records, run_configured_pipeline
from etlwifi.privacy import hash_mac
from etlwifi.schema import CAPTURE_COLUMNS, TELEMETRY_COLUMNS
from etlwifi.serving import SERVING_SCHEMA_VERSION
from etlwifi.serving.common import load_gold_snapshot
from etlwifi.serving.dashboard import build_dashboard
from etlwifi.serving.live import LiveContext, build_live
from etlwifi.serving.publish import SIGNATURE_HEADER, TIMESTAMP_HEADER, Publisher, sign
from tests.conftest import RESIDENT, SALT, capture_rows, write_rows


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory) -> tuple[Settings, dict, dict]:
    root = tmp_path_factory.mktemp("serving")
    legacy = write_rows(root / "legacy.csv", capture_rows())
    settings = Settings(data_root=root / "data", dataset="hood", mac_hash_salt=SALT, legacy_csv=legacy)
    run_configured_pipeline(settings)
    tables, entry = load_gold_snapshot(DataPaths.from_root(settings.data_root).gold, "hood")
    return settings, tables, entry


def test_dashboard_document_shape(built: tuple[Settings, dict, dict]) -> None:
    settings, tables, entry = built
    doc = build_dashboard(
        tables,
        gold_entry=entry,
        runs=load_run_records(settings.data_root / "state"),
        timezone="Europe/Brussels",
    )
    assert doc["schema_version"] == SERVING_SCHEMA_VERSION
    assert doc["kind"] == "dashboard"
    assert doc["summary"]["days_observed"] == 9
    assert {s["segment"] for s in doc["segments"]} >= {"resident", "infrastructure", "randomized"}
    assert len(doc["hour_of_week"]) > 0
    assert [row["dow"] for row in doc["weekday"]] == list(range(1, 8))
    assert doc["access_points"] is None

    text = json.dumps(doc)
    json.loads(text)
    # Published documents contain aggregates only, never device pseudonyms.
    assert hash_mac(RESIDENT, SALT.encode()) not in text
    assert "mac" not in {key for row in doc["daily"] for key in row}


def _landing(settings: Settings, now: datetime) -> None:
    paths = DataPaths.from_root(settings.data_root)
    salt = SALT.encode()
    rows, telemetry = [], []
    for i in range(30):
        ts = (now - timedelta(seconds=10 * (30 - i))).replace(tzinfo=None).isoformat(timespec="microseconds")
        for mac, randomized in ((RESIDENT, 0), (f"da:a1:00:00:00:{i:02x}", 1)):
            rows.append(
                {
                    "window_timestamp": ts,
                    "mac": hash_mac(mac, salt),
                    "packet_count": 5,
                    "total_bytes": 500,
                    "min_rssi": -60,
                    "max_rssi": -50,
                    "avg_rssi": -55.0,
                    "mgmt_pkts": 3,
                    "ctrl_pkts": 0,
                    "data_pkts": 2,
                    "observed_channels": "6",
                    "mac_randomized": randomized,
                    "oui": None if randomized else "001CF0",
                    "beacon_pkts": 0,
                    "probe_req_pkts": 1,
                    "probe_resp_pkts": 0,
                    "retry_pkts": 0,
                    "ap_channel": None,
                    "ssid_hash": None,
                    "probe_fingerprint": None,
                }
            )
        telemetry.append(
            {
                "window_timestamp": ts,
                "window_seconds": 10,
                "frames_total": 12,
                "frames_dot11": 12,
                "frames_no_sender": 0,
                "devices": 2,
                "channel_dwell": "6:10.00",
                "interface_type": "monitor",
                "capture_version": "2.0.0",
            }
        )
    day = now.date().isoformat()
    write_rows(paths.capture_landing / f"{day}.csv", rows, CAPTURE_COLUMNS)
    write_rows(paths.telemetry_landing / f"{day}.csv", telemetry, TELEMETRY_COLUMNS)


def test_live_document_reads_landing_zone(built: tuple[Settings, dict, dict], tmp_path: Path) -> None:
    _, tables, _ = built
    settings = Settings(data_root=tmp_path / "data", dataset="hood", mac_hash_salt=SALT)
    now = datetime(2026, 7, 1, 12, 0, 5, tzinfo=UTC)
    _landing(settings, now)
    context = LiveContext(
        baseline=tables["agg_baseline_hour_of_week"],
        stable_first_seen_today={hash_mac(RESIDENT, SALT.encode())},
        stable_first_seen_for_date=date(2026, 7, 1),
    )

    doc = build_live(settings, DataPaths.from_root(settings.data_root), context, now=now)

    assert doc["kind"] == "live"
    assert doc["source"] == "capture"
    assert doc["capture"]["status"] == "online"
    assert doc["now"]["devices"] >= 2
    assert doc["now"]["stable"] == 1
    assert len(doc["last_hour"]) == 30
    assert doc["today"]["first_time_stable"] == 1
    assert hash_mac(RESIDENT, SALT.encode()) not in json.dumps(doc)


def test_live_first_time_stable_stale_context_date(
    built: tuple[Settings, dict, dict], tmp_path: Path
) -> None:
    _, tables, _ = built
    settings = Settings(data_root=tmp_path / "data", dataset="hood", mac_hash_salt=SALT)
    now = datetime(2026, 7, 2, 12, 0, 5, tzinfo=UTC)
    _landing(settings, now)
    context = LiveContext(
        baseline=tables["agg_baseline_hour_of_week"],
        stable_first_seen_today={hash_mac(RESIDENT, SALT.encode())},
        stable_first_seen_for_date=date(2026, 7, 1),
    )
    doc = build_live(settings, DataPaths.from_root(settings.data_root), context, now=now)
    assert doc["today"]["first_time_stable"] is None


def test_live_first_time_stable_empty_set_today(built: tuple[Settings, dict, dict], tmp_path: Path) -> None:
    _, tables, _ = built
    settings = Settings(data_root=tmp_path / "data", dataset="hood", mac_hash_salt=SALT)
    now = datetime(2026, 7, 1, 12, 0, 5, tzinfo=UTC)
    _landing(settings, now)
    context = LiveContext(
        baseline=tables["agg_baseline_hour_of_week"],
        stable_first_seen_today=set(),
        stable_first_seen_for_date=date(2026, 7, 1),
    )
    doc = build_live(settings, DataPaths.from_root(settings.data_root), context, now=now)
    assert doc["today"]["first_time_stable"] == 0


def test_live_document_without_data_reports_offline(tmp_path: Path) -> None:
    settings = Settings(data_root=tmp_path / "data", mac_hash_salt=SALT)
    doc = build_live(settings, DataPaths.from_root(settings.data_root), now=datetime.now(UTC))
    assert doc["source"] == "none"
    assert doc["capture"]["status"] == "offline"


class _Recorder(BaseHTTPRequestHandler):
    received: ClassVar[list[tuple[str, Message, bytes]]] = []
    status: ClassVar[int] = 204

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers["Content-Length"]))
        type(self).received.append((self.path, self.headers, body))
        self.send_response(type(self).status)
        self.end_headers()

    def log_message(self, *_args) -> None:
        pass


@pytest.fixture
def server():
    _Recorder.received = []
    _Recorder.status = 204
    httpd = HTTPServer(("127.0.0.1", 0), _Recorder)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_port}"
    httpd.shutdown()


def test_publisher_signs_gzipped_payload(server: str) -> None:
    assert Publisher(server, "s3cret", backoff=0).publish("live", {"kind": "live", "n": 1})

    path, headers, body = _Recorder.received[0]
    assert path == "/api/ingest/wifi/live"
    assert headers["Content-Encoding"] == "gzip"
    assert json.loads(gzip.decompress(body)) == {"kind": "live", "n": 1}
    timestamp = headers[TIMESTAMP_HEADER]
    expected = hmac.new(b"s3cret", timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    assert headers[SIGNATURE_HEADER] == f"sha256={expected}" == sign("s3cret", timestamp, body)


def test_publisher_does_not_retry_client_errors(server: str) -> None:
    _Recorder.status = 401
    assert not Publisher(server, "wrong", retries=3, backoff=0).publish("live", {})
    assert len(_Recorder.received) == 1
