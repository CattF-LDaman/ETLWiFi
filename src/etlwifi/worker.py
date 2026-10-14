"""Long-running worker: scheduled pipeline runs plus a fast live-snapshot loop."""

from __future__ import annotations

import logging
import signal
import threading
import time
import traceback
from datetime import UTC, datetime

import polars as pl

from etlwifi.config import defaults as d
from etlwifi.config.paths import DataPaths
from etlwifi.config.settings import Settings
from etlwifi.health import touch_heartbeat
from etlwifi.pipeline.run import (
    append_run_record,
    load_run_records,
    run_configured_pipeline,
    run_record,
)
from etlwifi.reference.oui import refresh_registry
from etlwifi.serving.common import load_gold_snapshot, write_json_atomic
from etlwifi.serving.dashboard import build_dashboard
from etlwifi.serving.live import LiveContext, build_live
from etlwifi.serving.publish import Publisher

log = logging.getLogger(__name__)

HEARTBEAT_NAME = "worker"


class Worker:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.paths = DataPaths.from_root(settings.data_root)
        self.stop = threading.Event()
        self.context = LiveContext()
        self._context_lock = threading.Lock()
        self.publisher = (
            Publisher(settings.portfolio_url, settings.ingest_secret)
            if settings.portfolio_url and settings.ingest_secret
            else None
        )
        if self.publisher is None:
            log.warning("PORTFOLIO_URL or WIFI_INGEST_SECRET unset; writing serving files locally only")

    def _emit(self, kind: str, payload: dict) -> None:
        write_json_atomic(self.paths.serving / f"{kind}.json", payload)
        if self.publisher is not None and not self.publisher.publish(kind, payload):
            log.warning("publish did not succeed", extra={"kind": kind})

    def refresh_from_gold(self) -> dict | None:
        try:
            tables, entry = load_gold_snapshot(self.paths.gold, self.settings.dataset)
        except FileNotFoundError:
            log.info("no gold snapshot yet")
            return None
        profile = tables[d.DIM_DEVICE_PROFILE]
        with self._context_lock:
            self.context = LiveContext(
                baseline=tables[d.AGG_BASELINE_HOUR_OF_WEEK],
                known_stable_macs=set(profile.filter(~pl.col("is_randomized"))["mac"].to_list()),
            )
        return build_dashboard(
            tables,
            gold_entry=entry,
            runs=load_run_records(self.paths.state),
            timezone=self.settings.timezone,
        )

    def run_pipeline_once(self) -> None:
        started_at = datetime.now(UTC)
        result = None
        error = None
        try:
            refresh_registry(self.paths.reference, self.settings.oui_url, self.settings.oui_refresh_days)
            result = run_configured_pipeline(self.settings)
            log.info(
                "pipeline finished",
                extra={
                    "durations": {k: round(v, 2) for k, v in result.durations.items()},
                    "changed_sources": sum(1 for r in result.bronze_sources if not r.skipped),
                    "gold_skipped": result.gold.skipped,
                },
            )
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            log.error("pipeline failed", extra={"error": error, "trace": traceback.format_exc()})
        append_run_record(self.paths.state, run_record(result, started_at=started_at, error=error))

        try:
            dashboard = self.refresh_from_gold()
            if dashboard is not None:
                self._emit("dashboard", dashboard)
        except Exception:
            log.exception("dashboard build failed")

    def _pipeline_loop(self) -> None:
        while not self.stop.is_set():
            started = time.monotonic()
            self.run_pipeline_once()
            elapsed = time.monotonic() - started
            self.stop.wait(max(5.0, self.settings.pipeline_interval - elapsed))

    def run_live_once(self) -> None:
        with self._context_lock:
            context = self.context
        payload = build_live(self.settings, self.paths, context)
        self._emit("live", payload)

    def run(self) -> int:
        def request_stop(signum, _frame) -> None:
            log.info("stop requested", extra={"signal": signum})
            self.stop.set()

        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)

        try:
            self.refresh_from_gold()
        except Exception:
            log.exception("could not load existing gold snapshot")

        pipeline_thread = threading.Thread(target=self._pipeline_loop, name="pipeline", daemon=True)
        pipeline_thread.start()
        log.info(
            "worker started",
            extra={
                "live_interval": self.settings.live_interval,
                "pipeline_interval": self.settings.pipeline_interval,
                "publishing": self.publisher is not None,
            },
        )

        while not self.stop.is_set():
            try:
                self.run_live_once()
                touch_heartbeat(self.paths.state, HEARTBEAT_NAME)
            except Exception:
                log.exception("live snapshot failed")
            self.stop.wait(self.settings.live_interval)

        pipeline_thread.join(timeout=60)
        log.info("worker stopped")
        return 0


def run_worker(settings: Settings) -> int:
    return Worker(settings).run()
