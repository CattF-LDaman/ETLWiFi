from __future__ import annotations

import argparse
import sys
from pathlib import Path

from etlwifi.config.paths import DataPaths
from etlwifi.config.settings import load_settings
from etlwifi.pipeline.run import PipelineResult, discover_sources, pipeline_lock, run_pipeline_sources


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "run",
        help="Run the full pipeline: bronze ingest, silver transform, then gold",
    )
    parser.add_argument(
        "sources",
        type=Path,
        nargs="*",
        help="Capture CSV files or directories (default: legacy CSV + landing zone)",
    )
    parser.add_argument(
        "--telemetry",
        type=Path,
        nargs="*",
        default=None,
        help="Telemetry CSV files or directories (default: landing zone)",
    )
    parser.add_argument(
        "--data-root", type=Path, default=None, help="Data root (default: $ETLWIFI_DATA_ROOT or data)"
    )
    parser.add_argument("--dataset", help="Dataset name (default: $ETLWIFI_DATASET or neighborhood)")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-run every stage even when content hashes match prior runs",
    )
    parser.set_defaults(handler=_run_pipeline)


def _expand(paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for path in paths:
        files.extend(sorted(path.glob("*.csv")) if path.is_dir() else [path])
    return files


def _run_pipeline(args: argparse.Namespace) -> int:
    settings = load_settings()
    paths = DataPaths.from_root(args.data_root or settings.data_root)
    dataset = args.dataset or settings.dataset

    discovered_capture, discovered_telemetry = discover_sources(settings, paths)
    capture = _expand(args.sources) if args.sources else discovered_capture
    telemetry = _expand(args.telemetry) if args.telemetry is not None else discovered_telemetry

    try:
        with pipeline_lock(paths.state):
            result = run_pipeline_sources(
                capture,
                paths.bronze,
                paths.silver,
                paths.gold,
                dataset=dataset,
                telemetry_sources=telemetry,
                telemetry_dataset=f"{dataset}_telemetry",
                reference_dir=paths.reference,
                mac_salt=settings.mac_hash_salt,
                timezone=settings.timezone,
                window_seconds=settings.window_seconds,
                force=args.force,
            )
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        print(exc, file=sys.stderr)
        return 1

    _print_result(result)
    return 0


def _print_result(result: PipelineResult) -> None:
    bronze, silver, gold = result.bronze, result.silver, result.gold
    durations = result.durations

    print(f"Pipeline complete for dataset {result.dataset!r}")
    print()
    changed = [r for r in result.bronze_sources if not r.skipped]
    print(
        f"Bronze: {len(changed)}/{len(result.bronze_sources)} capture sources changed "
        f"({durations.get('bronze', 0):.1f}s)"
    )
    if not bronze.skipped:
        print(f"  rows ingested: {bronze.rows:,}")
        if bronze.hashed_macs:
            print(f"  pseudonymised MAC addresses: {bronze.hashed_macs:,}")
    if result.telemetry_sources:
        changed_telemetry = sum(1 for r in result.telemetry_sources if not r.skipped)
        print(f"  telemetry sources changed: {changed_telemetry}/{len(result.telemetry_sources)}")

    print()
    if silver.skipped:
        print("Silver: skipped (bronze snapshot already transformed)")
    else:
        validation = silver.validation
        assert validation is not None
        print(f"Silver: transformed {validation.fact_rows:,} fact rows ({durations.get('silver', 0):.1f}s)")
        print(f"  dim_device: {validation.dim_rows:,} devices")
        if silver.capture_windows:
            print(f"  fact_capture_window: {silver.capture_windows:,} windows")
        if validation.clean.duplicates_removed:
            print(f"  duplicates removed: {validation.clean.duplicates_removed:,}")

    print()
    if gold.skipped:
        print("Gold: skipped (silver snapshot already transformed)")
    else:
        table_rows = gold.table_rows or {}
        print(f"Gold: built {len(table_rows)} tables ({durations.get('gold', 0):.1f}s)")
        for table_name, rows in table_rows.items():
            print(f"  {table_name}: {rows:,} rows")
