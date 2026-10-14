from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC

import polars as pl

from etlwifi.config.defaults import COLLECTION_START
from etlwifi.silver.clean import TIMESTAMP_COLUMN, CleanSummary


@dataclass(frozen=True)
class ValidationSummary:
    fact_rows: int
    bridge_rows: int
    dim_rows: int
    clean: CleanSummary


def validate_silver(
    fact: pl.DataFrame,
    bridge: pl.DataFrame,
    dim: pl.DataFrame,
    clean: CleanSummary,
) -> ValidationSummary:
    if fact.filter(pl.col(TIMESTAMP_COLUMN).is_null()).height:
        raise ValueError("fact_device_window has null timestamps")

    start = COLLECTION_START.replace(tzinfo=UTC)
    before_start = fact.filter(pl.col(TIMESTAMP_COLUMN) < start)
    if before_start.height:
        raise ValueError(f"timestamps before collection start ({before_start.height} rows)")

    bad_rssi = fact.filter((pl.col("avg_rssi") < -100) | (pl.col("avg_rssi") > 0))
    if bad_rssi.height:
        raise ValueError(f"avg_rssi out of range (-100, 0): {bad_rssi.height} rows")

    bad_packets = fact.filter(pl.col("packet_count") < 0)
    if bad_packets.height:
        raise ValueError(f"packet_count < 0: {bad_packets.height} rows")

    bad_mix = fact.filter(
        (pl.col("mgmt_pkts") + pl.col("ctrl_pkts") + pl.col("data_pkts")) > pl.col("packet_count")
    )
    if bad_mix.height:
        raise ValueError(f"mgmt+ctrl+data exceeds packet_count: {bad_mix.height} rows")

    if bridge.height:
        bad_channels = bridge.filter(pl.col("channel") < 1)
        if bad_channels.height:
            raise ValueError(f"invalid channel values: {bad_channels.height} rows")

    if dim.filter(pl.col("window_count") < 1).height:
        raise ValueError("dim_device has devices with zero windows")

    return ValidationSummary(
        fact_rows=fact.height,
        bridge_rows=bridge.height,
        dim_rows=dim.height,
        clean=clean,
    )
