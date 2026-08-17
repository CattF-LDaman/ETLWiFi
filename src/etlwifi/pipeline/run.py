from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from etlwifi.bronze.ingest import BronzeIngestResult, ingest_csv
from etlwifi.silver.transform import SilverTransformResult, transform_dataset


@dataclass(frozen=True)
class PipelineResult:
    dataset: str
    bronze: BronzeIngestResult
    silver: SilverTransformResult


def run_pipeline(
    csv_path: Path,
    bronze_root: Path,
    silver_root: Path,
    *,
    dataset: str | None = None,
    force: bool = False,
) -> PipelineResult:
    dataset_name = dataset or csv_path.stem

    bronze_result = ingest_csv(
        csv_path,
        bronze_root,
        dataset=dataset_name,
        force=force,
    )
    silver_result = transform_dataset(
        dataset_name,
        bronze_root,
        silver_root,
        force=force,
    )

    return PipelineResult(
        dataset=dataset_name,
        bronze=bronze_result,
        silver=silver_result,
    )
