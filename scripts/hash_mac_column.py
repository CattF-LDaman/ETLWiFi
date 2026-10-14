#!/usr/bin/env python3
"""Export a capture CSV with MAC addresses replaced by the pipeline's HMAC pseudonyms.

Uses the same code path as bronze ingest, so pseudonyms match the warehouse.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import polars as pl

from etlwifi.bronze.ingest import conform_columns, pseudonymise_macs
from etlwifi.config.settings import load_dotenv
from etlwifi.privacy import require_salt
from etlwifi.schema import CAPTURE_COLUMNS


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", type=Path, help="CSV file containing a mac column")
    parser.add_argument("output", type=Path, help="Destination CSV (the input is never modified)")
    args = parser.parse_args()

    load_dotenv()
    if not args.csv_path.exists():
        print(f"File not found: {args.csv_path}", file=sys.stderr)
        return 1
    try:
        salt = require_salt(os.environ.get("MAC_HASH_SALT"))
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 1

    frame = pl.read_csv(args.csv_path, infer_schema=False)
    if "mac" not in frame.columns:
        print(f"{args.csv_path} has no 'mac' column", file=sys.stderr)
        return 1
    hashed, count = pseudonymise_macs(conform_columns(frame, CAPTURE_COLUMNS), salt)
    hashed.write_csv(args.output)
    print(f"Wrote {hashed.height:,} rows -> {args.output} ({count:,} MAC addresses pseudonymised)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
