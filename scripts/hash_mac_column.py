#!/usr/bin/env python3
"""Replace raw MAC addresses with deterministic SHA-256 pseudonyms."""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import sys
import tempfile
from pathlib import Path


def normalize_mac(mac: str) -> str:
    return mac.strip().lower().replace("-", "").replace(":", "")


def hash_mac(mac: str, salt: bytes) -> str:
    normalized = normalize_mac(mac)
    digest = hashlib.sha256(salt + normalized.encode("utf-8")).hexdigest()
    return digest[:16]


def hash_csv(input_path: Path, output_path: Path | None, salt: bytes) -> int:
    cache: dict[str, str] = {}
    rows_written = 0

    with input_path.open(newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        if reader.fieldnames is None or "mac" not in reader.fieldnames:
            raise ValueError(f"{input_path} has no 'mac' column")

        target = output_path
        if target is None:
            fd, tmp_name = tempfile.mkstemp(
                dir=input_path.parent,
                prefix=f".{input_path.stem}.",
                suffix=".tmp",
            )
            os.close(fd)
            target = Path(tmp_name)

        with target.open("w", newline="", encoding="utf-8") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            writer.writeheader()

            for row in reader:
                mac = row["mac"]
                if mac not in cache:
                    cache[mac] = hash_mac(mac, salt)
                row["mac"] = cache[mac]
                writer.writerow(row)
                rows_written += 1

        if output_path is None:
            target.replace(input_path)

    return rows_written, len(cache)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", type=Path, help="CSV file containing a mac column")
    parser.add_argument(
        "--output",
        type=Path,
        help="Write to this path instead of replacing the input file",
    )
    parser.add_argument(
        "--salt",
        default=os.environ.get("MAC_HASH_SALT", "etlwifi-portfolio"),
        help="Salt for deterministic hashing (prefer MAC_HASH_SALT env var)",
    )
    args = parser.parse_args()

    if not args.csv_path.exists():
        print(f"File not found: {args.csv_path}", file=sys.stderr)
        return 1

    rows, unique_macs = hash_csv(args.csv_path, args.output, args.salt.encode("utf-8"))
    destination = args.output or args.csv_path
    print(f"Hashed {rows:,} rows -> {destination}")
    print(f"Unique device pseudonyms: {unique_macs:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
