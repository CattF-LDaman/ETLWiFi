"""Append-only, daily-rotated CSV files in the landing zone."""

from __future__ import annotations

import csv
import os
from collections.abc import Iterable, Mapping
from datetime import date
from pathlib import Path


class DailyCsvSink:
    def __init__(self, directory: Path, columns: tuple[str, ...]) -> None:
        self.directory = directory
        self.columns = columns
        directory.mkdir(parents=True, exist_ok=True)

    def path_for(self, day: date) -> Path:
        return self.directory / f"{day.isoformat()}.csv"

    def write(self, day: date, rows: Iterable[Mapping[str, object]]) -> Path:
        path = self.path_for(day)
        new_file = not path.exists() or path.stat().st_size == 0
        with path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=self.columns, extrasaction="raise")
            if new_file:
                writer.writeheader()
            writer.writerows({k: ("" if v is None else v) for k, v in row.items()} for row in rows)
            handle.flush()
            os.fsync(handle.fileno())
        return path
