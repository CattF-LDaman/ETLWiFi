from __future__ import annotations

import argparse
import sys
from pathlib import Path

from etlwifi.config.paths import default_paths
from etlwifi.pipeline.run import run_pipeline


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "run",
        help="Run the full pipeline: bronze ingest then silver transform",
    )
    parser.add_argument("csv_path", type=Path, help="CSV export to process")
    parser.add_argument(
        "--bronze-root",
        type=Path,
        default=None,
        help="Bronze output root (default: data/bronze)",
    )
    parser.add_argument(
        "--silver-root",
        type=Path,
        default=None,
        help="Silver output root (default: data/silver)",
    )
    parser.add_argument(
        "--dataset",
        help="Dataset name (default: CSV stem, e.g. neighborhood)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-run both stages even when content hash matches prior runs",
    )
    parser.set_defaults(handler=_run_pipeline)


def _run_pipeline(args: argparse.Namespace) -> int:
    paths = default_paths()
    bronze_root = args.bronze_root or paths.bronze
    silver_root = args.silver_root or paths.silver

    try:
        result = run_pipeline(
            args.csv_path,
            bronze_root,
            silver_root,
            dataset=args.dataset,
            force=args.force,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(exc, file=sys.stderr)
        return 1

    bronze = result.bronze
    silver = result.silver

    print(f"Pipeline complete for dataset {result.dataset!r}")
    print()

    if bronze.skipped:
        prior = bronze.prior or {}
        print("Bronze: skipped (unchanged source)")
        print(f"  content hash: {prior.get('content_hash', 'unknown')}")
    else:
        print(f"Bronze: ingested {bronze.rows:,} rows")
        print(f"  date partitions: {bronze.partitions}")
        if bronze.replaced_parts:
            print(f"  replaced prior part files: {bronze.replaced_parts}")

    print()

    if silver.skipped:
        prior = silver.prior or {}
        print("Silver: skipped (bronze snapshot already transformed)")
        print(f"  bronze content hash: {prior.get('bronze_content_hash', 'unknown')}")
    else:
        validation = silver.validation
        assert validation is not None
        print(f"Silver: transformed {validation.fact_rows:,} fact rows")
        print(f"  fact_device_window: {silver.fact_partitions} date partitions")
        print(f"  device_window_channels: {silver.bridge_partitions} date partitions")
        print(f"  dim_device: {validation.dim_rows:,} devices")
        if validation.clean.duplicates_removed:
            print(f"  duplicates removed: {validation.clean.duplicates_removed:,}")
        if silver.replaced_parts:
            print(f"  replaced prior part files: {silver.replaced_parts}")

    return 0
