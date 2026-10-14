"""Runtime settings resolved from environment variables (and an optional .env file)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def load_dotenv(path: Path = Path(".env")) -> None:
    """Populate os.environ from a KEY=VALUE file without overriding existing values."""
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip().removeprefix("export ").strip()
        value = value.strip().strip("'\"")
        os.environ.setdefault(key, value)


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    value = _env(name)
    return int(value) if value is not None else default


def _env_float(name: str, default: float) -> float:
    value = _env(name)
    return float(value) if value is not None else default


def _env_path(name: str) -> Path | None:
    value = _env(name)
    return Path(value) if value else None


def _channels(value: str | None) -> tuple[int, ...]:
    if not value:
        return (1, 6, 11)
    return tuple(int(part) for part in value.replace(" ", "").split(",") if part)


@dataclass(frozen=True)
class Settings:
    data_root: Path = Path("data")
    dataset: str = "neighborhood"
    timezone: str = "Europe/Brussels"
    window_seconds: int = 10
    mac_hash_salt: str | None = None
    legacy_csv: Path | None = None

    capture_interface: str = "wlx001cf09e26ce"
    capture_channels: tuple[int, ...] = field(default_factory=lambda: (1, 6, 11))
    capture_hop_interval: float = 2.0

    portfolio_url: str | None = None
    ingest_secret: str | None = None
    live_interval: int = 30
    pipeline_interval: int = 900
    oui_refresh_days: int = 30
    oui_url: str = "https://standards-oui.ieee.org/oui/oui.csv"

    @property
    def telemetry_dataset(self) -> str:
        return f"{self.dataset}_telemetry"


def load_settings() -> Settings:
    load_dotenv()
    return Settings(
        data_root=Path(_env("ETLWIFI_DATA_ROOT", "data")),
        dataset=_env("ETLWIFI_DATASET", "neighborhood"),
        timezone=_env("ETLWIFI_TIMEZONE", "Europe/Brussels"),
        window_seconds=_env_int("CAPTURE_FLUSH_INTERVAL", 10),
        mac_hash_salt=_env("MAC_HASH_SALT"),
        legacy_csv=_env_path("ETLWIFI_LEGACY_CSV"),
        capture_interface=_env("CAPTURE_INTERFACE", "wlx001cf09e26ce"),
        capture_channels=_channels(_env("CAPTURE_CHANNELS")),
        capture_hop_interval=_env_float("CAPTURE_HOP_INTERVAL", 2.0),
        portfolio_url=_env("PORTFOLIO_URL"),
        ingest_secret=_env("WIFI_INGEST_SECRET"),
        live_interval=_env_int("WORKER_LIVE_INTERVAL", 30),
        pipeline_interval=_env_int("WORKER_PIPELINE_INTERVAL", 900),
        oui_refresh_days=_env_int("OUI_REFRESH_DAYS", 30),
        oui_url=_env("OUI_URL", "https://standards-oui.ieee.org/oui/oui.csv"),
    )
