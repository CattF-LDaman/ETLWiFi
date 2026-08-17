from __future__ import annotations

import argparse
import sys

from etlwifi import __version__
from etlwifi.cli import bronze, run, silver


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="etlwifi",
        description="ETLWiFi medallion pipeline CLI",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    subparsers = parser.add_subparsers(dest="command", required=True)
    run.register(subparsers)
    bronze.register(subparsers)
    silver.register(subparsers)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
