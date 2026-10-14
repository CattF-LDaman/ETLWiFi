from __future__ import annotations

import csv
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from etlwifi.capture.aggregator import WindowAggregator
from etlwifi.capture.frames import FrameInfo, probe_signature
from etlwifi.capture.service import telemetry_row
from etlwifi.capture.sink import DailyCsvSink
from etlwifi.privacy import hash_mac
from etlwifi.schema import CAPTURE_COLUMNS, HIDDEN_SSID, TELEMETRY_COLUMNS

SALT = b"test-salt"
STATION = "3c:22:fb:00:00:01"
PHONE = "da:a1:00:00:00:01"
ROUTER = "00:1c:f0:aa:bb:02"


def _aggregator() -> WindowAggregator:
    return WindowAggregator(lambda mac: hash_mac(mac, SALT), SALT)


def test_aggregator_emits_contract_rows_with_hashed_macs() -> None:
    agg = _aggregator()
    agg.add(FrameInfo(STATION, 100, 2, 0, -60, retry=False), channel=6)
    agg.add(FrameInfo(STATION, 50, 0, 4, -70, retry=True, probe_signature="sig"), channel=1)
    agg.add(FrameInfo(PHONE, 80, 0, 4, None, retry=False, ssid=b"MyHomeNetwork"), channel=11)

    snapshot = agg.drain(datetime(2026, 6, 1, 12, 0, tzinfo=UTC))
    rows = {row["mac"]: row for row in snapshot.rows}

    station = rows[hash_mac(STATION, SALT)]
    assert tuple(station) == CAPTURE_COLUMNS
    assert station["packet_count"] == 2
    assert station["total_bytes"] == 150
    assert station["avg_rssi"] == -65.0
    assert station["observed_channels"] == "1|6"
    assert station["retry_pkts"] == 1
    assert station["probe_req_pkts"] == 1
    assert station["mac_randomized"] == 0
    assert station["oui"] == "3C22FB"
    assert station["probe_fingerprint"] == "sig"

    phone = rows[hash_mac(PHONE, SALT)]
    assert phone["mac_randomized"] == 1
    assert phone["oui"] is None
    assert phone["avg_rssi"] is None
    # Probe-request SSIDs disclose a client's saved networks and must never be stored.
    assert phone["ssid_hash"] is None
    assert all(STATION not in str(v) and PHONE not in str(v) for row in snapshot.rows for v in row.values())


def test_aggregator_hashes_beacon_ssids_and_resets_between_windows() -> None:
    agg = _aggregator()
    agg.add(FrameInfo(ROUTER, 200, 0, 8, -50, retry=False, ap_channel=6, ssid=b"Telenet-1234"), channel=6)
    agg.add(FrameInfo("00:1c:f0:aa:bb:03", 200, 0, 8, -55, retry=False, ssid=b""), channel=6)
    rows = {row["mac"]: row for row in agg.drain(datetime.now(UTC)).rows}

    router = rows[hash_mac(ROUTER, SALT)]
    assert router["beacon_pkts"] == 1
    assert router["ap_channel"] == 6
    assert router["ssid_hash"] not in (None, HIDDEN_SSID, "Telenet-1234")
    assert rows[hash_mac("00:1c:f0:aa:bb:03", SALT)]["ssid_hash"] == HIDDEN_SSID

    assert agg.drain(datetime.now(UTC)).rows == []


def test_frame_counters_feed_telemetry() -> None:
    agg = _aggregator()
    agg.count_frame(dot11=True, has_sender=True)
    agg.count_frame(dot11=True, has_sender=False)
    agg.count_frame(dot11=False, has_sender=False)
    snapshot = agg.drain(datetime(2026, 6, 1, 12, 0, 10, tzinfo=UTC))

    row = telemetry_row(snapshot, window_seconds=10.0, dwell={6: 4.0, 1: 6.0}, iface_type="monitor")
    assert tuple(row) == TELEMETRY_COLUMNS
    assert (row["frames_total"], row["frames_dot11"], row["frames_no_sender"]) == (3, 2, 1)
    assert row["devices"] == 0
    assert row["channel_dwell"] == "1:6.00|6:4.00"
    assert row["window_timestamp"] == "2026-06-01T12:00:10.000000"


def test_probe_signature_ignores_ssid_and_channel() -> None:
    rates = (1, b"\x02\x04\x0b\x16")
    base = probe_signature([(0, b"home"), rates, (3, b"\x06")])
    assert base == probe_signature([(0, b"other"), rates, (3, b"\x01")])
    assert base != probe_signature([(0, b"home"), (1, b"\x02\x04")])


def test_daily_sink_writes_header_once(tmp_path: Path) -> None:
    sink = DailyCsvSink(tmp_path, ("a", "b"))
    sink.write(date(2026, 6, 1), [{"a": 1, "b": None}])
    path = sink.write(date(2026, 6, 1), [{"a": 2, "b": "x"}])
    with path.open(encoding="utf-8") as handle:
        assert list(csv.reader(handle)) == [["a", "b"], ["1", ""], ["2", "x"]]
    assert path.name == "2026-06-01.csv"


def test_extract_frame_reads_beacon_and_probe_details() -> None:
    pytest.importorskip("scapy")
    from scapy.layers.dot11 import Dot11, Dot11Beacon, Dot11Elt, Dot11ProbeReq, RadioTap

    from etlwifi.capture.frames import extract_frame

    beacon = (
        RadioTap(dBm_AntSignal=-48, present="dBm_AntSignal")
        / Dot11(type=0, subtype=8, addr1="ff:ff:ff:ff:ff:ff", addr2=ROUTER, addr3=ROUTER)
        / Dot11Beacon()
        / Dot11Elt(ID=0, info=b"Telenet-1234")
        / Dot11Elt(ID=3, info=b"\x0b")
    )
    frame = extract_frame(RadioTap(bytes(beacon)))
    assert frame is not None
    assert frame.sender == ROUTER
    assert frame.is_beacon
    assert frame.rssi == -48
    assert frame.ssid == b"Telenet-1234"
    assert frame.ap_channel == 11
    assert frame.probe_signature is None

    probe = (
        RadioTap()
        / Dot11(
            type=0, subtype=4, FCfield=0x08, addr1="ff:ff:ff:ff:ff:ff", addr2=PHONE, addr3="ff:ff:ff:ff:ff:ff"
        )
        / Dot11ProbeReq()
        / Dot11Elt(ID=0, info=b"")
        / Dot11Elt(ID=1, info=b"\x02\x04\x0b\x16")
    )
    frame = extract_frame(RadioTap(bytes(probe)))
    assert frame is not None
    assert frame.is_probe_request
    assert frame.retry
    assert frame.probe_signature is not None

    assert extract_frame(RadioTap()) is None
