from __future__ import annotations

import fcntl
import json
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from etlwifi.bronze.ingest import BronzeIngestResult, ingest_sources
from etlwifi.config.paths import DataPaths
from etlwifi.config.settings import Settings
from etlwifi.gold.transform import GoldTransformResult
from etlwifi.gold.transform import transform_dataset as transform_gold
from etlwifi.silver.transform import SilverTransformResult, transform_dataset

RUN_LOG = "pipeline_runs.jsonl"
LOCK_FILE = "pipeline.lock"


@dataclass(frozen=True)
class PipelineResult:
    dataset: str
    bronze: BronzeIngestResult
    silver: SilverTransformResult
    gold: GoldTransformResult
    bronze_sources: tuple[BronzeIngestResult, ...] = ()
    telemetry_sources: tuple[BronzeIngestResult, ...] = ()
    durations: dict[str, float] = field(default_factory=dict)


def _summarise_bronze(results: Sequence[BronzeIngestResult], dataset: str) -> BronzeIngestResult:
    changed = [r for r in results if not r.skipped]
    if not changed:
        prior = results[-1].prior if results else None
        return BronzeIngestResult(skipped=True, dataset=dataset, reason="all sources unchanged", prior=prior)
    return BronzeIngestResult(
        skipped=False,
        dataset=dataset,
        rows=sum(r.rows or 0 for r in changed),
        partitions=sum(r.partitions or 0 for r in changed),
        parts=tuple(p for r in changed for p in r.parts),
        manifest=changed[-1].manifest,
        replaced_parts=sum(r.replaced_parts for r in changed),
        hashed_macs=sum(r.hashed_macs for r in changed),
    )


def run_pipeline_sources(
    capture_sources: Sequence[Path],
    bronze_root: Path,
    silver_root: Path,
    gold_root: Path,
    *,
    dataset: str,
    telemetry_sources: Sequence[Path] = (),
    telemetry_dataset: str | None = None,
    reference_dir: Path | None = None,
    mac_salt: str | bytes | None = None,
    timezone: str = "Europe/Brussels",
    window_seconds: int = 10,
    force: bool = False,
) -> PipelineResult:
    if not capture_sources:
        raise FileNotFoundError("no capture sources to process")
    durations: dict[str, float] = {}

    started = time.perf_counter()
    capture_results = ingest_sources(
        capture_sources, bronze_root, dataset=dataset, kind="capture", mac_salt=mac_salt, force=force
    )
    telemetry_name = telemetry_dataset or f"{dataset}_telemetry"
    telemetry_results = ingest_sources(
        telemetry_sources, bronze_root, dataset=telemetry_name, kind="telemetry", force=force
    )
    durations["bronze"] = time.perf_counter() - started

    started = time.perf_counter()
    silver_result = transform_dataset(
        dataset,
        bronze_root,
        silver_root,
        telemetry_dataset=telemetry_name,
        reference_dir=reference_dir,
        force=force,
    )
    durations["silver"] = time.perf_counter() - started

    started = time.perf_counter()
    gold_result = transform_gold(
        dataset,
        silver_root,
        gold_root,
        timezone=timezone,
        window_seconds=window_seconds,
        force=force,
    )
    durations["gold"] = time.perf_counter() - started

    return PipelineResult(
        dataset=dataset,
        bronze=_summarise_bronze(capture_results, dataset),
        silver=silver_result,
        gold=gold_result,
        bronze_sources=tuple(capture_results),
        telemetry_sources=tuple(telemetry_results),
        durations=durations,
    )


def run_pipeline(
    csv_path: Path,
    bronze_root: Path,
    silver_root: Path,
    gold_root: Path,
    *,
    dataset: str | None = None,
    mac_salt: str | bytes | None = None,
    force: bool = False,
) -> PipelineResult:
    return run_pipeline_sources(
        [csv_path],
        bronze_root,
        silver_root,
        gold_root,
        dataset=dataset or csv_path.stem,
        mac_salt=mac_salt,
        force=force,
    )


def discover_sources(settings: Settings, paths: DataPaths) -> tuple[list[Path], list[Path]]:
    capture = sorted(paths.capture_landing.glob("*.csv")) if paths.capture_landing.exists() else []
    if settings.legacy_csv is not None and settings.legacy_csv.is_file():
        capture.insert(0, settings.legacy_csv)
    telemetry = sorted(paths.telemetry_landing.glob("*.csv")) if paths.telemetry_landing.exists() else []
    return capture, telemetry


@contextmanager
def pipeline_lock(state_dir: Path) -> Iterator[None]:
    state_dir.mkdir(parents=True, exist_ok=True)
    with (state_dir / LOCK_FILE).open("w") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("another pipeline run is in progress") from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def run_configured_pipeline(settings: Settings, *, force: bool = False) -> PipelineResult:
    paths = DataPaths.from_root(settings.data_root)
    capture, telemetry = discover_sources(settings, paths)
    with pipeline_lock(paths.state):
        return run_pipeline_sources(
            capture,
            paths.bronze,
            paths.silver,
            paths.gold,
            dataset=settings.dataset,
            telemetry_sources=telemetry,
            telemetry_dataset=settings.telemetry_dataset,
            reference_dir=paths.reference,
            mac_salt=settings.mac_hash_salt,
            timezone=settings.timezone,
            window_seconds=settings.window_seconds,
            force=force,
        )


def run_record(result: PipelineResult | None, *, started_at: datetime, error: str | None = None) -> dict:
    record: dict = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "status": "failed" if error else "success",
    }
    if error:
        record["error"] = error[:500]
    if result is not None:
        record.update(
            {
                "durations_sec": {k: round(v, 2) for k, v in result.durations.items()},
                "bronze_sources": len(result.bronze_sources),
                "bronze_changed_sources": sum(1 for r in result.bronze_sources if not r.skipped),
                "bronze_rows_ingested": result.bronze.rows or 0,
                "silver_skipped": result.silver.skipped,
                "silver_fact_rows": result.silver.validation.fact_rows if result.silver.validation else None,
                "silver_devices": result.silver.validation.dim_rows if result.silver.validation else None,
                "gold_skipped": result.gold.skipped,
                "gold_table_rows": result.gold.table_rows,
            }
        )
    return record


def append_run_record(state_dir: Path, record: dict) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    with (state_dir / RUN_LOG).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, separators=(",", ":"), default=str) + "\n")


def load_run_records(state_dir: Path, limit: int = 50) -> list[dict]:
    path = state_dir / RUN_LOG
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()[-limit:]
    return [json.loads(line) for line in lines if line.strip()]
