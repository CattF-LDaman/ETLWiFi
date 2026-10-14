"""Capture service entrypoint: sniff, aggregate per window, append to the landing zone."""

from __future__ import annotations

import logging
import signal
import threading
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path

from etlwifi.capture import CAPTURE_VERSION
from etlwifi.capture.aggregator import WindowAggregator, WindowSnapshot
from etlwifi.capture.frames import extract_frame
from etlwifi.capture.radio import ChannelHopper, ensure_monitor_mode, interface_type
from etlwifi.capture.sink import DailyCsvSink
from etlwifi.config.paths import DataPaths
from etlwifi.config.settings import Settings
from etlwifi.health import touch_heartbeat
from etlwifi.privacy import hash_mac, require_salt
from etlwifi.schema import CAPTURE_COLUMNS, TELEMETRY_COLUMNS

log = logging.getLogger(__name__)

HEARTBEAT_NAME = "capture"


def _format_dwell(dwell: dict[int, float]) -> str:
    return "|".join(f"{channel}:{seconds:.2f}" for channel, seconds in sorted(dwell.items()))


def telemetry_row(
    snapshot: WindowSnapshot,
    *,
    window_seconds: float,
    dwell: dict[int, float],
    iface_type: str | None,
) -> dict[str, object]:
    return {
        "window_timestamp": snapshot.timestamp.replace(tzinfo=None).isoformat(timespec="microseconds"),
        "window_seconds": round(window_seconds, 3),
        "frames_total": snapshot.frames_total,
        "frames_dot11": snapshot.frames_dot11,
        "frames_no_sender": snapshot.frames_no_sender,
        "devices": len(snapshot.rows),
        "channel_dwell": _format_dwell(dwell),
        "interface_type": iface_type or "",
        "capture_version": CAPTURE_VERSION,
    }


def run_capture(settings: Settings) -> int:
    from scapy.all import AsyncSniffer
    from scapy.layers.dot11 import Dot11

    salt = require_salt(settings.mac_hash_salt)
    paths = DataPaths.from_root(settings.data_root)
    capture_sink = DailyCsvSink(paths.capture_landing, CAPTURE_COLUMNS)
    telemetry_sink = DailyCsvSink(paths.telemetry_landing, TELEMETRY_COLUMNS)
    heartbeat_dir: Path = paths.landing

    ensure_monitor_mode(settings.capture_interface)

    cached_hash = lru_cache(maxsize=200_000)(lambda mac: hash_mac(mac, salt))
    aggregator = WindowAggregator(cached_hash, salt)
    stop = threading.Event()
    hopper = ChannelHopper(
        settings.capture_interface,
        settings.capture_channels,
        settings.capture_hop_interval,
        stop,
    )

    def handle(pkt) -> None:
        is_dot11 = pkt.haslayer(Dot11)
        frame = extract_frame(pkt) if is_dot11 else None
        aggregator.count_frame(dot11=is_dot11, has_sender=frame is not None)
        if frame is not None:
            aggregator.add(frame, hopper.current_channel)

    def request_stop(signum, _frame) -> None:
        log.info("stop requested", extra={"signal": signum})
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    sniffer = AsyncSniffer(iface=settings.capture_interface, prn=handle, store=False)
    hopper.start()
    sniffer.start()
    log.info(
        "capture started",
        extra={
            "interface": settings.capture_interface,
            "channels": settings.capture_channels,
            "window_seconds": settings.window_seconds,
            "version": CAPTURE_VERSION,
        },
    )

    window_started = datetime.now(UTC)
    try:
        while True:
            stopping = stop.wait(settings.window_seconds)
            now = datetime.now(UTC)
            snapshot = aggregator.drain(now)
            dwell = hopper.take_dwell()
            day = now.date()
            if snapshot.rows:
                capture_sink.write(day, snapshot.rows)
            telemetry_sink.write(
                day,
                [
                    telemetry_row(
                        snapshot,
                        window_seconds=(now - window_started).total_seconds(),
                        dwell=dwell,
                        iface_type=interface_type(settings.capture_interface),
                    )
                ],
            )
            touch_heartbeat(heartbeat_dir, HEARTBEAT_NAME)
            window_started = now
            if not sniffer.running:
                log.error("sniffer thread stopped unexpectedly")
                return 1
            if stopping:
                break
    finally:
        stop.set()
        if sniffer.running:
            sniffer.stop()
        log.info("capture stopped")
    return 0
