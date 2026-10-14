from __future__ import annotations

import argparse
import sys
from pathlib import Path

from etlwifi.bronze.ingest import ingest_csv
from etlwifi.config.paths import default_paths
from etlwifi.config.settings import load_settings


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "bronze",
        help="Bronze layer: land raw CSV exports as date-partitioned Parquet",
    )
    bronze_sub = parser.add_subparsers(dest="bronze_command", required=True)

    ingest_parser = bronze_sub.add_parser(
        "ingest",
        help="Ingest a CSV export into bronze Parquet partitions",
    )
    ingest_parser.add_argument("csv_path", type=Path, help="CSV export to land")
    ingest_parser.add_argument(
        "--bronze-root",
        type=Path,
        default=None,
        help="Bronze output root (default: data/bronze)",
    )
    ingest_parser.add_argument(
        "--dataset",
        help="Dataset folder name (default: CSV stem, e.g. neighborhood)",
    )
    ingest_parser.add_argument(
        "--kind",
        choices=("capture", "telemetry"),
        default="capture",
        help="Landing contract of the file (default: capture)",
    )
    ingest_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-ingest even when content hash matches a prior landing",
    )
    ingest_parser.set_defaults(handler=_run_ingest)


def _run_ingest(args: argparse.Namespace) -> int:
    bronze_root = args.bronze_root or default_paths().bronze
    try:
        result = ingest_csv(
            args.csv_path,
            bronze_root,
            dataset=args.dataset,
            kind=args.kind,
            mac_salt=load_settings().mac_hash_salt,
            force=args.force,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(exc, file=sys.stderr)
        return 1

    if result.skipped:
        prior = result.prior or {}
        print(f"Skipped: {result.reason}")
        print(f"  content hash: {prior.get('content_hash', 'unknown')}")
        print(f"  prior ingest: {prior.get('ingested_at')} ({prior.get('rows', 0):,} rows)")
        return 0

    print(f"Ingested {result.rows:,} rows -> {bronze_root / result.dataset}")
    if result.hashed_macs:
        print(f"  pseudonymised MAC addresses: {result.hashed_macs:,}")
    print(f"  date partitions: {result.partitions}")
    if result.replaced_parts:
        print(f"  replaced prior part files: {result.replaced_parts}")
    print(f"  manifest: {result.manifest}")
    return 0
