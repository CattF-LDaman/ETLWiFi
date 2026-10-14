from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from etlwifi.bronze.hash import file_sha256
from etlwifi.reference.oui import load_vendors, registry_path
from etlwifi.silver.channels import build_channel_tables
from etlwifi.silver.clean import clean_bronze, clean_telemetry
from etlwifi.silver.manifest import (
    append_entry,
    find_latest_transform,
    find_transform_by_hash,
    load_manifest,
    manifest_path,
    remove_parts,
)
from etlwifi.silver.models import (
    build_capture_window_tables,
    build_dim_device,
    build_fact_device_window,
)
from etlwifi.silver.read import (
    find_latest_bronze_snapshot,
    find_optional_bronze_snapshot,
    read_bronze_snapshot,
)
from etlwifi.silver.validate import ValidationSummary, validate_silver
from etlwifi.silver.write import write_silver_tables


@dataclass(frozen=True)
class SilverTransformResult:
    skipped: bool
    dataset: str | None = None
    bronze_content_hash: str | None = None
    validation: ValidationSummary | None = None
    fact_partitions: int | None = None
    bridge_partitions: int | None = None
    capture_windows: int = 0
    manifest: str | None = None
    reason: str | None = None
    prior: dict | None = None
    replaced_parts: int = 0


def transform_dataset(
    dataset: str,
    bronze_root: Path,
    silver_root: Path,
    *,
    telemetry_dataset: str | None = None,
    reference_dir: Path | None = None,
    force: bool = False,
) -> SilverTransformResult:
    snapshot = find_latest_bronze_snapshot(bronze_root, dataset)
    telemetry = find_optional_bronze_snapshot(bronze_root, telemetry_dataset) if telemetry_dataset else None
    registry = registry_path(reference_dir) if reference_dir else None

    digest = hashlib.sha256(snapshot.content_hash.encode())
    if telemetry is not None:
        digest.update(telemetry.content_hash.encode())
    if registry is not None and registry.exists():
        digest.update(file_sha256(registry).encode())
    input_hash = digest.hexdigest()

    manifest = manifest_path(silver_root)
    entries = load_manifest(manifest)

    if not force:
        prior = find_transform_by_hash(entries, dataset, input_hash)
        if prior is not None:
            return SilverTransformResult(
                skipped=True,
                reason="bronze snapshot already transformed",
                prior=prior,
            )

    bronze_frame = read_bronze_snapshot(bronze_root, snapshot)
    cleaned, clean_summary = clean_bronze(bronze_frame)
    bridge, channel_counts = build_channel_tables(cleaned)
    fact = build_fact_device_window(cleaned, channel_counts)
    vendors = load_vendors(reference_dir) if reference_dir else None
    dim = build_dim_device(fact, bridge, cleaned, vendors)
    telemetry_frame = clean_telemetry(read_bronze_snapshot(bronze_root, telemetry)) if telemetry else None
    capture_windows, capture_dwell = build_capture_window_tables(telemetry_frame)
    validation = validate_silver(fact, bridge, dim, clean_summary)

    write_result = write_silver_tables(
        fact,
        bridge,
        dim,
        silver_root,
        capture_windows=capture_windows,
        capture_dwell=capture_dwell,
    )

    replaced_parts = 0
    latest = find_latest_transform(entries, dataset)
    if latest and latest.get("parts"):
        stale = [part for part in latest["parts"] if part not in write_result["parts"]]
        remove_parts(silver_root, stale)
        replaced_parts = len(stale)

    append_entry(
        manifest,
        {
            "dataset": dataset,
            "bronze_content_hash": input_hash,
            "bronze_ingested_at": snapshot.ingested_at,
            "bronze_sources": snapshot.sources,
            "ingested_at": write_result["ingested_at"],
            "rows_fact": write_result["rows_fact"],
            "rows_bridge": write_result["rows_bridge"],
            "rows_dim": write_result["rows_dim"],
            "rows_capture_windows": write_result["rows_capture_windows"],
            "fact_partitions": write_result["fact_partitions"],
            "bridge_partitions": write_result["bridge_partitions"],
            "parts": write_result["parts"],
            "replaced_parts": replaced_parts,
        },
    )

    return SilverTransformResult(
        skipped=False,
        dataset=dataset,
        bronze_content_hash=input_hash,
        validation=validation,
        fact_partitions=write_result["fact_partitions"],
        bridge_partitions=write_result["bridge_partitions"],
        capture_windows=write_result["rows_capture_windows"],
        manifest=str(manifest),
        replaced_parts=replaced_parts,
    )
