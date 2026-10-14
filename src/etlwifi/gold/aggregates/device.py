from __future__ import annotations

import polars as pl

from etlwifi.config.defaults import (
    INFRASTRUCTURE_MAX_RSSI_STD,
    INFRASTRUCTURE_MGMT_RATIO,
    REGULAR_MIN_ACTIVE_DAYS,
    RESIDENT_MIN_SPAN_DAYS,
    RESIDENT_PRESENCE_RATIO,
)
from etlwifi.gold.aggregates.channel import RSSI_BUCKET_EXPR
from etlwifi.gold.context import TS, GoldContext, local_time_columns
from etlwifi.schema import HIDDEN_SSID

DEVICE_SEGMENTS = ("access_point", "infrastructure", "resident", "regular", "passerby", "randomized")


def _ratio(numerator: str) -> pl.Expr:
    total = pl.col("packet_count").sum()
    return pl.when(total > 0).then(pl.col(numerator).sum() / total).otherwise(0.0)


def _proximity_tier_mode(fact: pl.DataFrame) -> pl.DataFrame:
    return (
        fact.with_columns(RSSI_BUCKET_EXPR.alias("rssi_bucket"))
        .group_by("mac", "rssi_bucket")
        .agg(pl.len().alias("count"))
        .sort("mac", "count", descending=[False, True])
        .group_by("mac")
        .agg(pl.col("rssi_bucket").first().alias("proximity_tier_mode"))
    )


def _session_stats(ctx: GoldContext) -> pl.DataFrame:
    gaps = (
        ctx.fact.select("mac", TS)
        .sort("mac", TS)
        .with_columns(pl.col(TS).diff().over("mac").dt.total_seconds().alias("gap_sec"))
        .filter(pl.col("gap_sec").is_not_null())
        .group_by("mac")
        .agg(
            pl.col("gap_sec").median().alias("median_inter_arrival_sec"),
            pl.col("gap_sec").max().alias("max_gap_sec"),
            pl.col("gap_sec").mean().alias("_mean_gap"),
            pl.col("gap_sec").std().alias("_std_gap"),
        )
        .with_columns(
            pl.when(pl.col("_mean_gap") > 0)
            .then(pl.col("_std_gap") / pl.col("_mean_gap"))
            .otherwise(0.0)
            .alias("inter_arrival_cv")
        )
        .drop("_mean_gap", "_std_gap")
    )
    sessions = ctx.sessions.group_by("mac").agg(
        pl.len().alias("session_count"),
        pl.col("duration_sec").median().alias("median_session_duration"),
        pl.col("duration_sec").max().alias("max_session_duration"),
    )
    return sessions.join(gaps, on="mac", how="left")


def _observed_days_since_first_seen(
    ctx: GoldContext, coverage_daily: pl.DataFrame, profile: pl.DataFrame
) -> pl.DataFrame:
    observed = (
        coverage_daily.filter(pl.col("coverage_ratio") > 0)
        .select("local_date")
        .sort("local_date")
        .with_row_index("observed_index")
    )
    total = observed.height
    first_seen = (
        profile.select("mac", "first_seen")
        .with_columns(*local_time_columns("first_seen", ctx.timezone, prefix="first_seen"))
        .sort("first_seen_date")
    )
    return (
        first_seen.join_asof(observed, left_on="first_seen_date", right_on="local_date", strategy="forward")
        .with_columns(
            (pl.lit(total) - pl.col("observed_index").cast(pl.Int64))
            .fill_null(1)
            .alias("observed_days_since_first_seen")
        )
        .select("mac", "first_seen_date", "observed_days_since_first_seen")
    )


def build_dim_device_profile(
    ctx: GoldContext,
    channel_device: pl.DataFrame,
    coverage_daily: pl.DataFrame,
) -> pl.DataFrame:
    fact = ctx.fact
    activity = (
        fact.group_by("mac")
        .agg(
            pl.col("local_date").n_unique().alias("active_days_local"),
            pl.col("packet_count").mean().alias("packets_per_window"),
            pl.col("packet_count").std().alias("_packet_std"),
            pl.col("avg_rssi").std().alias("rssi_std"),
            (pl.col("max_rssi") - pl.col("min_rssi")).mean().alias("rssi_range"),
            (pl.col("avg_rssi") > -60).mean().alias("strong_signal_share"),
            _ratio("mgmt_pkts").alias("mgmt_ratio"),
            _ratio("data_pkts").alias("data_ratio"),
            _ratio("ctrl_pkts").alias("ctrl_ratio"),
            _ratio("probe_req_pkts").alias("probe_ratio"),
            pl.col("local_hour").is_between(0, 5).mean().alias("overnight_share"),
            pl.col("local_weekday").is_in([6, 7]).mean().alias("weekend_share"),
        )
        .with_columns(
            pl.when(pl.col("packets_per_window") > 0)
            .then(pl.col("_packet_std") / pl.col("packets_per_window"))
            .otherwise(0.0)
            .alias("coefficient_of_variation_packets"),
        )
        .drop("_packet_std")
    )

    peak_hour = (
        fact.group_by("mac", "local_hour")
        .agg(pl.len().alias("hour_windows"))
        .sort("mac", "hour_windows", descending=[False, True])
        .group_by("mac")
        .agg(
            pl.col("local_hour").first().alias("peak_hour"),
            (pl.col("hour_windows").first() / pl.col("hour_windows").sum()).alias("peak_hour_concentration"),
        )
    )

    profile = (
        ctx.dim.join(activity, on="mac", how="left")
        .join(_proximity_tier_mode(fact), on="mac", how="left")
        .join(peak_hour, on="mac", how="left")
        .join(_session_stats(ctx), on="mac", how="left")
        .join(
            channel_device.select(
                "mac", "primary_channel", "channel_entropy", "channel_switch_rate", "is_multi_channel"
            ),
            on="mac",
            how="left",
        )
    )
    profile = profile.join(
        _observed_days_since_first_seen(ctx, coverage_daily, profile), on="mac", how="left"
    ).with_columns(
        (pl.col("active_days_local") / pl.col("observed_days_since_first_seen").clip(lower_bound=1))
        .clip(0.0, 1.0)
        .alias("presence_ratio"),
        ((pl.col("last_seen") - pl.col("first_seen")).dt.total_days() + 1).alias("span_days"),
        pl.when(pl.col("active_days_local") > 0)
        .then(pl.col("window_count") / pl.col("active_days_local"))
        .otherwise(0.0)
        .alias("windows_per_active_day"),
        pl.when(pl.col("total_packets") > 0)
        .then(pl.col("total_bytes") / pl.col("total_packets"))
        .otherwise(0.0)
        .alias("bytes_per_packet"),
        pl.col("rssi_std").fill_null(0.0),
        pl.col("channel_entropy").fill_null(0.0),
        pl.col("inter_arrival_cv").fill_null(0.0),
        pl.col("beacon_packets").fill_null(0),
    )
    return _assign_device_segment(profile)


def _assign_device_segment(profile: pl.DataFrame) -> pl.DataFrame:
    long_lived = (pl.col("presence_ratio") >= RESIDENT_PRESENCE_RATIO) & (
        pl.col("observed_days_since_first_seen") >= RESIDENT_MIN_SPAN_DAYS
    )
    return profile.with_columns(
        pl.when(pl.col("is_randomized"))
        .then(pl.lit("randomized"))
        .when(pl.col("beacon_packets") > 0)
        .then(pl.lit("access_point"))
        .when(
            long_lived
            & (pl.col("mgmt_ratio") >= INFRASTRUCTURE_MGMT_RATIO)
            & (pl.col("rssi_std") <= INFRASTRUCTURE_MAX_RSSI_STD)
        )
        .then(pl.lit("infrastructure"))
        .when(long_lived)
        .then(pl.lit("resident"))
        .when(pl.col("active_days_local") >= REGULAR_MIN_ACTIVE_DAYS)
        .then(pl.lit("regular"))
        .otherwise(pl.lit("passerby"))
        .alias("device_segment")
    )


def build_agg_vendor(ctx: GoldContext, device_profile: pl.DataFrame) -> pl.DataFrame:
    last_date = ctx.fact["local_date"].max() if ctx.fact.height else None
    recent = (
        ctx.fact.filter(pl.col("local_date") > pl.lit(last_date) - pl.duration(days=7))
        .select("mac")
        .unique()
        .with_columns(pl.lit(True).alias("seen_last_7d"))
        if last_date is not None
        else pl.DataFrame(schema={"mac": pl.Utf8, "seen_last_7d": pl.Boolean})
    )
    return (
        device_profile.filter(~pl.col("is_randomized"))
        .join(recent, on="mac", how="left")
        .with_columns(pl.col("vendor").fill_null("Unknown vendor"))
        .group_by("vendor", "device_segment")
        .agg(
            pl.len().alias("device_count"),
            pl.col("seen_last_7d").fill_null(False).sum().alias("devices_last_7d"),
            pl.col("window_count").sum().alias("window_count"),
        )
        .sort("device_count", descending=True)
    )


def build_dim_access_point(ctx: GoldContext) -> pl.DataFrame:
    beacons = ctx.fact.filter(pl.col("beacon_pkts") > 0)
    schema = {
        "mac": pl.Utf8,
        "first_seen": pl.Datetime("us", "UTC"),
        "last_seen": pl.Datetime("us", "UTC"),
        "beacon_windows": pl.UInt32,
        "ap_channel": pl.Int64,
        "ssid_hash": pl.Utf8,
        "is_hidden": pl.Boolean,
        "avg_rssi": pl.Float64,
        "vendor": pl.Utf8,
        "active_days": pl.UInt32,
    }
    if beacons.height == 0:
        return pl.DataFrame(schema=schema)
    return (
        beacons.group_by("mac")
        .agg(
            pl.col(TS).min().alias("first_seen"),
            pl.col(TS).max().alias("last_seen"),
            pl.len().alias("beacon_windows"),
            pl.col("ap_channel").drop_nulls().mode().first().alias("ap_channel"),
            pl.col("ssid_hash").drop_nulls().mode().first().alias("ssid_hash"),
            pl.col("avg_rssi").mean().alias("avg_rssi"),
            pl.col("local_date").n_unique().alias("active_days"),
        )
        .with_columns((pl.col("ssid_hash") == HIDDEN_SSID).fill_null(False).alias("is_hidden"))
        .join(ctx.dim.select("mac", "vendor"), on="mac", how="left")
        .select(list(schema))
        .sort("first_seen")
    )


def build_agg_access_point_daily(ctx: GoldContext, access_points: pl.DataFrame) -> pl.DataFrame:
    beacons = ctx.fact.filter(pl.col("beacon_pkts") > 0)
    if beacons.height == 0:
        return pl.DataFrame(schema={"local_date": pl.Date, "access_points": pl.UInt32})
    first_dates = access_points.select(
        "mac", "is_hidden", *local_time_columns("first_seen", ctx.timezone, prefix="first_seen")
    )
    return (
        beacons.select("local_date", "mac", "avg_rssi", "ap_channel")
        .join(first_dates.select("mac", "is_hidden", "first_seen_date"), on="mac", how="left")
        .group_by("local_date")
        .agg(
            pl.col("mac").n_unique().alias("access_points"),
            pl.col("mac").filter(pl.col("is_hidden")).n_unique().alias("hidden_access_points"),
            pl.col("mac")
            .filter(pl.col("first_seen_date") == pl.col("local_date"))
            .n_unique()
            .alias("new_access_points"),
            pl.col("avg_rssi").mean().alias("avg_rssi"),
            pl.col("ap_channel").drop_nulls().n_unique().alias("channels_in_use"),
        )
        .sort("local_date")
    )
