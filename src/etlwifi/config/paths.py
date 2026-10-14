from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataPaths:
    root: Path
    raw: Path
    landing: Path
    bronze: Path
    silver: Path
    gold: Path
    reference: Path
    serving: Path
    state: Path

    @property
    def capture_landing(self) -> Path:
        return self.landing / "capture"

    @property
    def telemetry_landing(self) -> Path:
        return self.landing / "telemetry"

    @classmethod
    def from_root(cls, root: Path | str = "data") -> DataPaths:
        base = Path(root)
        return cls(
            root=base,
            raw=base / "raw",
            landing=base / "landing",
            bronze=base / "bronze",
            silver=base / "silver",
            gold=base / "gold",
            reference=base / "reference",
            serving=base / "serving",
            state=base / "state",
        )


def default_paths() -> DataPaths:
    from etlwifi.config.settings import load_settings

    return DataPaths.from_root(load_settings().data_root)
