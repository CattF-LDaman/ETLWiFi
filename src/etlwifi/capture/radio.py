"""Wireless interface control: monitor mode and channel hopping via `iw`."""

from __future__ import annotations

import logging
import os
import subprocess
import threading
import time
from collections import defaultdict

log = logging.getLogger(__name__)


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    if os.geteuid() != 0:
        args = ["sudo", "-n", *args]
    return subprocess.run(args, check=True, capture_output=True, text=True, timeout=10)


def interface_type(interface: str) -> str | None:
    try:
        output = _run(["iw", "dev", interface, "info"]).stdout
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return None
    for line in output.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] == "type":
            return parts[1]
    return None


def ensure_monitor_mode(interface: str) -> None:
    current = interface_type(interface)
    if current is None:
        raise RuntimeError(f"wireless interface {interface!r} not found")
    if current == "monitor":
        log.info("interface already in monitor mode", extra={"interface": interface})
        return
    log.info("switching interface to monitor mode", extra={"interface": interface, "from": current})
    _run(["ip", "link", "set", interface, "down"])
    _run(["iw", "dev", interface, "set", "type", "monitor"])
    _run(["ip", "link", "set", interface, "up"])
    if interface_type(interface) != "monitor":
        raise RuntimeError(f"could not put {interface!r} into monitor mode")


class ChannelHopper(threading.Thread):
    """Cycles channels and accounts how long each channel was actually listened to."""

    def __init__(
        self,
        interface: str,
        channels: tuple[int, ...],
        dwell_seconds: float,
        stop_event: threading.Event,
    ) -> None:
        super().__init__(name="channel-hopper", daemon=True)
        self.interface = interface
        self.channels = channels
        self.dwell_seconds = dwell_seconds
        self._stop_event = stop_event
        self._lock = threading.Lock()
        self._current: int | None = None
        self._since = time.monotonic()
        self._dwell: dict[int, float] = defaultdict(float)
        self._failures = 0

    @property
    def current_channel(self) -> int | None:
        return self._current

    def _switch(self, channel: int) -> None:
        now = time.monotonic()
        with self._lock:
            if self._current is not None:
                self._dwell[self._current] += now - self._since
            self._current = channel
            self._since = now

    def take_dwell(self) -> dict[int, float]:
        now = time.monotonic()
        with self._lock:
            if self._current is not None:
                self._dwell[self._current] += now - self._since
                self._since = now
            dwell = dict(self._dwell)
            self._dwell.clear()
        return dwell

    def run(self) -> None:
        while not self._stop_event.is_set():
            for channel in self.channels:
                if self._stop_event.is_set():
                    return
                try:
                    _run(["iw", "dev", self.interface, "set", "channel", str(channel)])
                    self._switch(channel)
                    self._failures = 0
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
                    self._failures += 1
                    if self._failures in (1, 10) or self._failures % 100 == 0:
                        log.warning(
                            "channel switch failed",
                            extra={"channel": channel, "failures": self._failures, "error": str(exc)},
                        )
                self._stop_event.wait(self.dwell_seconds)
