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


def _source_key(source_path: Path) -> str:
    return str(source_path.resolve())


def find_ingest_by_hash(
    manifest: list[dict],
    source_path: Path,
    content_hash: str,
) -> dict | None:
    source_key = _source_key(source_path)
    for entry in reversed(manifest):
        if entry.get("source_path") != source_key:
            continue
        if entry.get("content_hash") == content_hash:
            return entry
    return None


def find_latest_ingest(
    manifest: list[dict],
    source_path: Path,
) -> dict | None:
    source_key = _source_key(source_path)
    for entry in reversed(manifest):
        if entry.get("source_path") == source_key:
            return entry
    return None


def remove_parts(bronze_root: Path, parts: list[str]) -> None:
    for part in parts:
        path = bronze_root / part
        if path.exists():
            path.unlink()


def append_entry(path: Path, entry: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, separators=(",", ":")) + "\n")
