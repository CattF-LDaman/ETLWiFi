"""Thread-safe aggregation of frames into fixed windows of per-device rows."""

from __future__ import annotations

import threading
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from etlwifi.capture.frames import FRAME_CTRL, FRAME_DATA, FRAME_MGMT, FrameInfo
from etlwifi.privacy import is_locally_administered, keyed_hash, oui
from etlwifi.schema import CAPTURE_COLUMNS, HIDDEN_SSID


@dataclass
class DeviceStats:
    packets: int = 0
    total_bytes: int = 0
    rssi_min: int | None = None
    rssi_max: int | None = None
    rssi_sum: int = 0
    rssi_n: int = 0
    mgmt: int = 0
    ctrl: int = 0
    data: int = 0
    beacons: int = 0
    probe_requests: int = 0
    probe_responses: int = 0
    retries: int = 0
    channels: set[int] = field(default_factory=set)
    ap_channel: int | None = None
    ssid: bytes | None = None
    signatures: Counter[str] = field(default_factory=Counter)

    def add(self, frame: FrameInfo, channel: int | None) -> None:
        self.packets += 1
        self.total_bytes += frame.size
        if frame.rssi is not None:
            self.rssi_min = frame.rssi if self.rssi_min is None else min(self.rssi_min, frame.rssi)
            self.rssi_max = frame.rssi if self.rssi_max is None else max(self.rssi_max, frame.rssi)
            self.rssi_sum += frame.rssi
            self.rssi_n += 1
        if frame.frame_type == FRAME_MGMT:
            self.mgmt += 1
        elif frame.frame_type == FRAME_CTRL:
            self.ctrl += 1
        elif frame.frame_type == FRAME_DATA:
            self.data += 1
        if frame.is_beacon:
            self.beacons += 1
        elif frame.is_probe_request:
            self.probe_requests += 1
        elif frame.is_probe_response:
            self.probe_responses += 1
        if frame.retry:
            self.retries += 1
        if channel is not None:
            self.channels.add(channel)
        if frame.ap_channel is not None:
            self.ap_channel = frame.ap_channel
        # Only AP-advertised SSIDs are kept; client probe SSIDs reveal home networks.
        if frame.is_beacon and frame.ssid is not None:
            self.ssid = frame.ssid
        if frame.probe_signature:
            self.signatures[frame.probe_signature] += 1


@dataclass(frozen=True)
class WindowSnapshot:
    timestamp: datetime
    rows: list[dict[str, object]]
    frames_total: int
    frames_dot11: int
    frames_no_sender: int


def _ssid_token(ssid: bytes, salt: bytes) -> str:
    if not ssid or not ssid.strip(b"\x00"):
        return HIDDEN_SSID
    return keyed_hash(ssid.decode("utf-8", errors="replace"), salt, length=12)


class WindowAggregator:
    def __init__(self, mac_hasher: Callable[[str], str], salt: bytes) -> None:
        self._hash_mac = mac_hasher
        self._salt = salt
        self._lock = threading.Lock()
        self._devices: dict[str, DeviceStats] = {}
        self._frames_total = 0
        self._frames_dot11 = 0
        self._frames_no_sender = 0

    def count_frame(self, *, dot11: bool, has_sender: bool) -> None:
        with self._lock:
            self._frames_total += 1
            if dot11:
                self._frames_dot11 += 1
                if not has_sender:
                    self._frames_no_sender += 1

    def add(self, frame: FrameInfo, channel: int | None) -> None:
        with self._lock:
            stats = self._devices.get(frame.sender)
            if stats is None:
                stats = self._devices[frame.sender] = DeviceStats()
            stats.add(frame, channel)

    def drain(self, timestamp: datetime) -> WindowSnapshot:
        with self._lock:
            devices = self._devices
            totals = (self._frames_total, self._frames_dot11, self._frames_no_sender)
            self._devices = {}
            self._frames_total = self._frames_dot11 = self._frames_no_sender = 0

        window_ts = timestamp.replace(tzinfo=None).isoformat(timespec="microseconds")
        rows = [self._row(window_ts, mac, stats) for mac, stats in devices.items()]
        return WindowSnapshot(timestamp, rows, *totals)

    def _row(self, window_ts: str, mac: str, stats: DeviceStats) -> dict[str, object]:
        avg_rssi = round(stats.rssi_sum / stats.rssi_n, 1) if stats.rssi_n else None
        signature = stats.signatures.most_common(1)[0][0] if stats.signatures else None
        row: dict[str, object] = {
            "window_timestamp": window_ts,
            "mac": self._hash_mac(mac),
            "packet_count": stats.packets,
            "total_bytes": stats.total_bytes,
            "min_rssi": stats.rssi_min,
            "max_rssi": stats.rssi_max,
            "avg_rssi": avg_rssi,
            "mgmt_pkts": stats.mgmt,
            "ctrl_pkts": stats.ctrl,
            "data_pkts": stats.data,
            "observed_channels": "|".join(str(c) for c in sorted(stats.channels)),
            "mac_randomized": 1 if is_locally_administered(mac) else 0,
            "oui": oui(mac),
            "beacon_pkts": stats.beacons,
            "probe_req_pkts": stats.probe_requests,
            "probe_resp_pkts": stats.probe_responses,
            "retry_pkts": stats.retries,
            "ap_channel": stats.ap_channel,
            "ssid_hash": _ssid_token(stats.ssid, self._salt) if stats.ssid is not None else None,
            "probe_fingerprint": signature,
        }
        assert tuple(row) == CAPTURE_COLUMNS
        return row
