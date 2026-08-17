from __future__ import annotations

import json
from pathlib import Path

MANIFEST_FILENAME = "_manifest.jsonl"


def manifest_path(silver_root: Path) -> Path:
    return silver_root / MANIFEST_FILENAME


def load_manifest(path: Path) -> list[dict]:
    if not path.exists():
        return []
    entries: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            entries.append(json.loads(line))
    return entries


def find_transform_by_hash(
    manifest: list[dict],
    dataset: str,
    bronze_content_hash: str,
) -> dict | None:
    for entry in reversed(manifest):
        if entry.get("dataset") != dataset:
            continue
        if entry.get("bronze_content_hash") == bronze_content_hash:
            return entry
    return None


def find_latest_transform(manifest: list[dict], dataset: str) -> dict | None:
    for entry in reversed(manifest):
        if entry.get("dataset") == dataset:
            return entry
    return None


def remove_parts(silver_root: Path, parts: list[str]) -> None:
    for part in parts:
        path = silver_root / part
        if path.exists():
            path.unlink()


def append_entry(path: Path, entry: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, separators=(",", ":")) + "\n")
