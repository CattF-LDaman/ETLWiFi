from __future__ import annotations

import argparse
import json
import sys

from etlwifi.config.paths import DataPaths
from etlwifi.config.settings import load_settings
from etlwifi.health import heartbeat_age
from etlwifi.logs import configure_logging


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    capture = subparsers.add_parser("capture", help="Run the passive WiFi capture service")
    capture.set_defaults(handler=_capture)

    worker = subparsers.add_parser("worker", help="Run scheduled pipeline + live publishing")
    worker.set_defaults(handler=_worker)

    serve = subparsers.add_parser("serve", help="Build serving documents once")
    serve.add_argument("kind", choices=("dashboard", "live"))
    serve.add_argument("--publish", action="store_true", help="Also push to PORTFOLIO_URL")
    serve.add_argument("--stdout", action="store_true", help="Print the document")
    serve.set_defaults(handler=_serve)

    health = subparsers.add_parser("health", help="Exit non-zero when a heartbeat is stale")
    health.add_argument("service", choices=("capture", "worker"))
    health.add_argument("--max-age", type=float, default=120.0)
    health.set_defaults(handler=_health)


def _capture(_: argparse.Namespace) -> int:
    from etlwifi.capture.service import run_capture

    configure_logging()
    return run_capture(load_settings())


def _worker(_: argparse.Namespace) -> int:
    from etlwifi.worker import run_worker

    configure_logging()
    return run_worker(load_settings())


def _serve(args: argparse.Namespace) -> int:
    from etlwifi.serving.common import write_json_atomic
    from etlwifi.serving.publish import Publisher
    from etlwifi.worker import Worker

    configure_logging()
    settings = load_settings()
    worker = Worker(settings)
    if args.kind == "dashboard":
        payload = worker.refresh_from_gold()
        if payload is None:
            print("no gold snapshot; run `etlwifi run` first", file=sys.stderr)
            return 1
    else:
        from etlwifi.serving.live import build_live

        worker.refresh_from_gold()
        payload = build_live(settings, worker.paths, worker.context)

    target = worker.paths.serving / f"{args.kind}.json"
    write_json_atomic(target, payload)
    if args.stdout:
        print(json.dumps(payload, indent=2))
    else:
        print(f"wrote {target}")
    if args.publish:
        if not (settings.portfolio_url and settings.ingest_secret):
            print("PORTFOLIO_URL and WIFI_INGEST_SECRET are required to publish", file=sys.stderr)
            return 1
        ok = Publisher(settings.portfolio_url, settings.ingest_secret).publish(args.kind, payload)
        print("published" if ok else "publish failed")
        return 0 if ok else 1
    return 0


def _health(args: argparse.Namespace) -> int:
    paths = DataPaths.from_root(load_settings().data_root)
    directory = paths.landing if args.service == "capture" else paths.state
    age = heartbeat_age(directory, args.service)
    if age is None or age > args.max_age:
        print(f"{args.service} heartbeat stale: {age}", file=sys.stderr)
        return 1
    return 0
