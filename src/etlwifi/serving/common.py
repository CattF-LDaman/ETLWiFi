from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from etlwifi.config import defaults as d
from etlwifi.gold.manifest import find_latest_transform, load_manifest, manifest_path
from etlwifi.gold.write import read_gold_table

MIN_GROUP_SIZE = 3


def _jsonable(value: Any, digits: int) -> Any:
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return None
        return round(value, digits)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    return value


def records(
    frame: pl.DataFrame, columns: dict[str, str] | list[str] | None = None, digits: int = 2
) -> list[dict]:
    """DataFrame -> list of JSON-safe dicts, optionally selecting/renaming columns."""
    if columns is not None:
        mapping = columns if isinstance(columns, dict) else {c: c for c in columns}
        present = {src: dst for src, dst in mapping.items() if src in frame.columns}
        frame = frame.select([pl.col(src).alias(dst) for src, dst in present.items()])
    return [{k: _jsonable(v, digits) for k, v in row.items()} for row in frame.iter_rows(named=True)]


def scalar(value: Any, digits: int = 3) -> Any:
    return _jsonable(value, digits)


def load_gold_snapshot(gold_root: Path, dataset: str) -> tuple[dict[str, pl.DataFrame], dict]:
    entry = find_latest_transform(load_manifest(manifest_path(gold_root)), dataset)
    if entry is None:
        raise FileNotFoundError(f"no gold snapshot for dataset {dataset!r}")
    parts = entry.get("parts") or []
    tables = {name: read_gold_table(gold_root, parts, name) for name in d.GOLD_TABLES}
    return tables, entry


def write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, path)
