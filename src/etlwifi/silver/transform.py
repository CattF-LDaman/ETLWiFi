from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from etlwifi.silver.channels import build_channel_tables
from etlwifi.silver.clean import clean_bronze
from etlwifi.silver.manifest import (
    append_entry,
    find_latest_transform,
    find_transform_by_hash,
    load_manifest,
    manifest_path,
    remove_parts,
)
from etlwifi.silver.models import build_dim_device, build_fact_device_window
from etlwifi.silver.read import find_latest_bronze_snapshot, read_bronze_snapshot
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
    manifest: str | None = None
    reason: str | None = None
    prior: dict | None = None
    replaced_parts: int = 0


def transform_dataset(
    dataset: str,
    bronze_root: Path,
    silver_root: Path,
    *,
    force: bool = False,
) -> SilverTransformResult:
    snapshot = find_latest_bronze_snapshot(bronze_root, dataset)
    manifest = manifest_path(silver_root)
    entries = load_manifest(manifest)

    if not force:
        prior = find_transform_by_hash(entries, dataset, snapshot.content_hash)
        if prior is not None:
            return SilverTransformResult(
                skipped=True,
                reason="bronze snapshot already transformed",
                prior=prior,
            )

    replaced_parts = 0
    latest = find_latest_transform(entries, dataset)
    if latest and latest.get("parts"):
        remove_parts(silver_root, latest["parts"])
        replaced_parts = len(latest["parts"])

    bronze_frame = read_bronze_snapshot(bronze_root, snapshot)
    cleaned, clean_summary = clean_bronze(bronze_frame)
    bridge, channel_counts = build_channel_tables(cleaned)
    fact = build_fact_device_window(cleaned, channel_counts)
    dim = build_dim_device(fact, bridge)
    validation = validate_silver(fact, bridge, dim, clean_summary)

    write_result = write_silver_tables(fact, bridge, dim, silver_root)

    append_entry(
        manifest,
        {
            "dataset": dataset,
            "bronze_content_hash": snapshot.content_hash,
            "bronze_ingested_at": snapshot.ingested_at,
            "ingested_at": write_result["ingested_at"],
            "rows_fact": write_result["rows_fact"],
            "rows_bridge": write_result["rows_bridge"],
            "rows_dim": write_result["rows_dim"],
            "fact_partitions": write_result["fact_partitions"],
            "bridge_partitions": write_result["bridge_partitions"],
            "parts": write_result["parts"],
            "replaced_parts": replaced_parts,
        },
    )

    return SilverTransformResult(
        skipped=False,
        dataset=dataset,
        bronze_content_hash=snapshot.content_hash,
        validation=validation,
        fact_partitions=write_result["fact_partitions"],
        bridge_partitions=write_result["bridge_partitions"],
        manifest=str(manifest),
        replaced_parts=replaced_parts,
    )
