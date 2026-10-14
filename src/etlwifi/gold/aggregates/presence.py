from __future__ import annotations

import polars as pl

from etlwifi.gold.context import TS, GoldContext, local_time_columns

DWELL_BUCKETS: tuple[tuple[str, float], ...] = (
    ("< 1 min", 60),
    ("1-5 min", 300),
    ("5-15 min", 900),
    ("15-60 min", 3600),
    ("1-4 h", 4 * 3600),
    ("4-12 h", 12 * 3600),
    ("12 h +", float("inf")),
)


def _share(numerator: str, denominator: str = "packet_count") -> pl.Expr:
    total = pl.col(denominator).sum()
    return pl.when(total > 0).then(pl.col(numerator).sum() / total).otherwise(0.0)


def dwell_bucket_expr(column: str = "duration_sec") -> pl.Expr:
    expr = pl.when(pl.col(column) < DWELL_BUCKETS[0][1]).then(pl.lit(DWELL_BUCKETS[0][0]))
    for label, upper in DWELL_BUCKETS[1:]:
        expr = expr.when(pl.col(column) < upper).then(pl.lit(label))
    return expr.otherwise(pl.lit(DWELL_BUCKETS[-1][0]))


def build_agg_presence_daily(ctx: GoldContext) -> pl.DataFrame:
    sessions = (
        ctx.sessions.with_columns(*local_time_columns("session_start", ctx.timezone))
        .group_by("local_date", "mac")
        .agg(pl.len().alias("session_count"))
    )
    return (
        ctx.fact.group_by("local_date", "mac")
        .agg(
            pl.col("device_class").first(),
            pl.len().alias("window_count"),
            pl.col(TS).min().alias("first_seen_time"),
            pl.col(TS).max().alias("last_seen_time"),
            ((pl.col(TS).max() - pl.col(TS).min()).dt.total_seconds() / 60).alias("active_minutes"),
            pl.col("packet_count").sum().alias("total_packets"),
            pl.col("total_bytes").sum().alias("total_bytes"),
            pl.col("avg_rssi").mean().alias("avg_rssi"),
            pl.col("min_rssi").min().alias("min_rssi"),
            pl.col("max_rssi").max().alias("max_rssi"),
            _share("mgmt_pkts").alias("mgmt_share"),
            _share("data_pkts").alias("data_share"),
        )
        .join(sessions, on=["local_date", "mac"], how="left")
        .with_columns(pl.col("session_count").fill_null(1))
        .sort("local_date", "mac")
    )


def build_agg_churn_daily(presence_daily: pl.DataFrame) -> pl.DataFrame:
    """Day-over-day turnover of stable devices (randomized MACs churn by design)."""
    stable = presence_daily.filter(pl.col("device_class") == "stable")
    by_date = {
        row["local_date"]: set(row["mac"])
        for row in stable.group_by("local_date").agg(pl.col("mac")).iter_rows(named=True)
    }
    randomized = dict(
        presence_daily.filter(pl.col("device_class") == "randomized")
        .group_by("local_date")
        .agg(pl.len())
        .iter_rows()
    )

    rows: list[dict] = []
    ever_seen: set[str] = set()
    previous: set[str] = set()
    trailing: list[set[str]] = []
    for date_value in sorted(set(by_date) | set(randomized)):
        today = by_date.get(date_value, set())
        recent = set().union(*trailing) if trailing else set()
        rows.append(
            {
                "local_date": date_value,
                "stable_devices": len(today),
                "first_time_devices": len(today - ever_seen),
                "returning_devices": len(today & ever_seen),
                "seen_yesterday": len(today & previous),
                "lost_since_yesterday": len(previous - today),
                "resident_devices": len(today & recent),
                "randomized_macs": randomized.get(date_value, 0),
            }
        )
        ever_seen |= today
        previous = today
        trailing.append(today)
        if len(trailing) > 7:
            trailing.pop(0)

    if not rows:
        return pl.DataFrame(schema={"local_date": pl.Date, "stable_devices": pl.Int64})
    return pl.DataFrame(rows).with_columns(
        (pl.col("first_time_devices") - pl.col("lost_since_yesterday")).alias("net_device_change")
    )


def build_agg_dwell_distribution(ctx: GoldContext, device_profile: pl.DataFrame) -> pl.DataFrame:
    segments = device_profile.select("mac", "device_segment")
    order = {label: index for index, (label, _) in enumerate(DWELL_BUCKETS)}
    return (
        ctx.sessions.filter(pl.col("device_class") == "stable")
        .join(segments, on="mac", how="left")
        .with_columns(dwell_bucket_expr().alias("duration_bucket"))
        .group_by("duration_bucket", "device_segment")
        .agg(
            pl.len().alias("session_count"),
            pl.col("mac").n_unique().alias("device_count"),
            pl.col("duration_sec").median().alias("median_duration_sec"),
        )
        .with_columns(
            pl.col("duration_bucket").replace_strict(order, return_dtype=pl.Int8).alias("bucket_order")
        )
        .sort("bucket_order", "device_segment")
    )
