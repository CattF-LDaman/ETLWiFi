from __future__ import annotations

import argparse
import sys
from pathlib import Path

from etlwifi.config.paths import default_paths
from etlwifi.config.settings import load_settings
from etlwifi.gold.transform import transform_dataset


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "gold",
        help="Gold layer: build aggregates and ML feature tables from silver",
    )
    gold_sub = parser.add_subparsers(dest="gold_command", required=True)

    transform_parser = gold_sub.add_parser(
        "transform",
        help="Transform the latest silver snapshot into gold tables",
    )
    transform_parser.add_argument(
        "dataset",
        help="Silver dataset name (e.g. neighborhood)",
    )
    transform_parser.add_argument(
        "--silver-root",
        type=Path,
        default=None,
        help="Silver input root (default: data/silver)",
    )
    transform_parser.add_argument(
        "--gold-root",
        type=Path,
        default=None,
        help="Gold output root (default: data/gold)",
    )
    transform_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-transform even when silver content hash matches a prior run",
    )
    transform_parser.set_defaults(handler=_run_transform)


def _run_transform(args: argparse.Namespace) -> int:
    paths = default_paths()
    silver_root = args.silver_root or paths.silver
    gold_root = args.gold_root or paths.gold
    settings = load_settings()

    try:
        result = transform_dataset(
            args.dataset,
            silver_root,
            gold_root,
            timezone=settings.timezone,
            window_seconds=settings.window_seconds,
            force=args.force,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(exc, file=sys.stderr)
        return 1

    if result.skipped:
        prior = result.prior or {}
        print(f"Skipped: {result.reason}")
        print(f"  silver content hash: {prior.get('silver_content_hash', 'unknown')}")
        print(f"  prior transform: {prior.get('ingested_at')}")
        return 0

    validation = result.validation
    assert validation is not None
    table_rows = result.table_rows or {}
    print(f"Built {len(table_rows)} gold tables from {validation.fact_rows:,} fact rows")
    for table_name, rows in sorted(table_rows.items()):
        print(f"  {table_name}: {rows:,} rows")
    if result.replaced_parts:
        print(f"  replaced prior part files: {result.replaced_parts}")
    print(f"  silver content hash: {result.silver_content_hash}")
    return 0
