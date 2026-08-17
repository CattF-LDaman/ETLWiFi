from __future__ import annotations

import json
from pathlib import Path

MANIFEST_FILENAME = "_manifest.jsonl"


def manifest_path(bronze_root: Path) -> Path:
    return bronze_root / MANIFEST_FILENAME


def load_manifest(path: Path) -> list[dict]:
    if not path.exists():
        return []
    entries: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            entries.append(json.loads(line))
    return entries


def find_prior_ingest(
    manifest: list[dict],
    source_path: Path,
    source_mtime: float,
) -> dict | None:
    source_key = str(source_path.resolve())
    for entry in reversed(manifest):
        if entry.get("source_path") != source_key:
            continue
        if entry.get("source_mtime") == source_mtime:
            return entry
    return None


def append_entry(path: Path, entry: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, separators=(",", ":")) + "\n")
