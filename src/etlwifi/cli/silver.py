from __future__ import annotations

import argparse
import sys
from pathlib import Path

from etlwifi.config.paths import default_paths
from etlwifi.silver.transform import transform_dataset


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "silver",
        help="Silver layer: clean bronze data into typed dim/fact tables",
    )
    silver_sub = parser.add_subparsers(dest="silver_command", required=True)

    transform_parser = silver_sub.add_parser(
        "transform",
        help="Transform the latest bronze snapshot into silver tables",
    )
    transform_parser.add_argument(
        "dataset",
        help="Bronze dataset name (e.g. neighborhood)",
    )
    transform_parser.add_argument(
        "--bronze-root",
        type=Path,
        default=None,
        help="Bronze input root (default: data/bronze)",
    )
    transform_parser.add_argument(
        "--silver-root",
        type=Path,
        default=None,
        help="Silver output root (default: data/silver)",
    )
    transform_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-transform even when bronze content hash matches a prior run",
    )
    transform_parser.set_defaults(handler=_run_transform)


def _run_transform(args: argparse.Namespace) -> int:
    paths = default_paths()
    bronze_root = args.bronze_root or paths.bronze
    silver_root = args.silver_root or paths.silver

    try:
        result = transform_dataset(
            args.dataset,
            bronze_root,
            silver_root,
            force=args.force,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(exc, file=sys.stderr)
        return 1

    if result.skipped:
        prior = result.prior or {}
        print(f"Skipped: {result.reason}")
        print(f"  bronze content hash: {prior.get('bronze_content_hash', 'unknown')}")
        print(f"  prior transform: {prior.get('ingested_at')} ({prior.get('rows_fact', 0):,} fact rows)")
        return 0

    validation = result.validation
    assert validation is not None
    print(f"Transformed {validation.fact_rows:,} rows -> {silver_root}")
    print(f"  fact_device_window: {result.fact_partitions} date partitions")
    print(f"  device_window_channels: {result.bridge_partitions} date partitions")
    print(f"  dim_device: {validation.dim_rows:,} devices")
    if validation.clean.duplicates_removed:
        print(f"  duplicates removed: {validation.clean.duplicates_removed:,}")
    if validation.clean.parse_failures:
        print(f"  parse failures dropped: {validation.clean.parse_failures:,}")
    if result.replaced_parts:
        print(f"  replaced prior part files: {result.replaced_parts}")
    print(f"  bronze content hash: {result.bronze_content_hash}")
    return 0
