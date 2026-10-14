"""Landing-zone data contract shared by the capture service and the bronze layer."""

from __future__ import annotations

LEGACY_CAPTURE_COLUMNS: tuple[str, ...] = (
    "window_timestamp",
    "mac",
    "packet_count",
    "total_bytes",
    "min_rssi",
    "max_rssi",
    "avg_rssi",
    "mgmt_pkts",
    "ctrl_pkts",
    "data_pkts",
    "observed_channels",
)

# v2 appends columns only, so v1 exports remain a valid prefix of the contract.
CAPTURE_COLUMNS: tuple[str, ...] = (
    *LEGACY_CAPTURE_COLUMNS,
    "mac_randomized",
    "oui",
    "beacon_pkts",
    "probe_req_pkts",
    "probe_resp_pkts",
    "retry_pkts",
    "ap_channel",
    "ssid_hash",
    "probe_fingerprint",
)

TELEMETRY_COLUMNS: tuple[str, ...] = (
    "window_timestamp",
    "window_seconds",
    "frames_total",
    "frames_dot11",
    "frames_no_sender",
    "devices",
    "channel_dwell",
    "interface_type",
    "capture_version",
)

SCHEMA_VERSION = 2
HIDDEN_SSID = "hidden"
