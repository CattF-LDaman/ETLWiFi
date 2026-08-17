from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataPaths:
    root: Path
    raw: Path
    bronze: Path
    silver: Path
    gold: Path

    @classmethod
    def from_root(cls, root: Path | str = "data") -> DataPaths:
        base = Path(root)
        return cls(
            root=base,
            raw=base / "raw",
            bronze=base / "bronze",
            silver=base / "silver",
            gold=base / "gold",
        )


def default_paths() -> DataPaths:
    return DataPaths.from_root("data")
