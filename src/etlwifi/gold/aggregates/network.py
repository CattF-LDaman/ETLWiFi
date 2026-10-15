from __future__ import annotations

import polars as pl

from etlwifi.config.defaults import (
    ANOMALY_BURST_RANDOMIZED_RATIO,
    ANOMALY_STABLE_MIN_ABS_DELTA,
    ANOMALY_Z_THRESHOLD,
    BASELINE_LOOKBACK_DAYS,
    MIN_COVERAGE_FOR_BASELINE,
)
from etlwifi.gold.context import TS, GoldContext, local_time_columns

ROBUST_Z_SCALE = 0.6745
BASELINE_MIN_SAMPLES = 3


def _share(numerator: str, denominator: str = "packet_count") -> pl.Expr:
    total = pl.col(denominator).sum()
    return pl.when(total > 0).then(pl.col(numerator).sum() / total).otherwise(None)


def _device_counts() -> list[pl.Expr]:
    randomized = pl.col("is_randomized")
    fingerprinted = randomized & pl.col("probe_fingerprint").is_not_null()
    estimated = (
        pl.col("mac").filter(~randomized).n_unique()
        + pl.col("probe_fingerprint").filter(fingerprinted).n_unique()
        + pl.col("mac").filter(randomized & pl.col("probe_fingerprint").is_null()).n_unique()
    )
    return [
        pl.col("mac").n_unique().alias("unique_devices"),
        pl.col("mac").filter(~randomized).n_unique().alias("stable_devices"),
        pl.col("mac").filter(randomized).n_unique().alias("randomized_macs"),
        pl.when(pl.col("has_detail").any()).then(estimated).otherwise(None).alias("estimated_devices"),
    ]


def _traffic() -> list[pl.Expr]:
    return [
        pl.col("packet_count").sum().alias("total_packets"),
        pl.col("total_bytes").sum().alias("total_bytes"),
        pl.col("avg_rssi").mean().alias("avg_rssi"),
        _share("mgmt_pkts").alias("mgmt_share"),
        _share("ctrl_pkts").alias("ctrl_share"),
        _share("data_pkts").alias("data_share"),
        _share("retry_pkts").alias("retry_share"),
        pl.col("beacon_pkts").sum().alias("beacon_packets"),
        pl.col("probe_req_pkts").sum().alias("probe_requests"),
    ]


def _first_seen(ctx: GoldContext) -> pl.DataFrame:
    return ctx.dim.select("mac", pl.col("first_seen").alias("device_first_seen"))


def build_agg_network_window(ctx: GoldContext) -> pl.DataFrame:
    stable_first_seen = _first_seen(ctx).join(
        ctx.dim.filter(~pl.col("is_randomized")).select("mac"), on="mac"
    )
    per_window = (
        ctx.fact.join(stable_first_seen, on="mac", how="left")
        .group_by(TS)
        .agg(
            pl.col("mac").n_unique().alias("device_count"),
            pl.col("mac").filter(~pl.col("is_randomized")).n_unique().alias("stable_devices"),
            pl.col("mac").filter(pl.col("is_randomized")).n_unique().alias("randomized_macs"),
            pl.col("packet_count").sum().alias("total_packets"),
            pl.col("total_bytes").sum().alias("total_bytes"),
            pl.col("avg_rssi").mean().alias("avg_rssi"),
            pl.col("avg_rssi").quantile(0.5).alias("p50_rssi"),
            pl.col("avg_rssi").quantile(0.1).alias("p10_rssi"),
            pl.col("avg_rssi").quantile(0.9).alias("p90_rssi"),
            _share("mgmt_pkts").alias("mgmt_share"),
            _share("ctrl_pkts").alias("ctrl_share"),
            _share("data_pkts").alias("data_share"),
            (pl.col("device_first_seen") == pl.col(TS)).sum().alias("new_stable_devices"),
        )
    )
    return (
        ctx.windows.select(TS, "window_source", "local_date", "local_hour", "local_weekday")
        .join(per_window, on=TS, how="left")
        .with_columns(
            pl.col(c).fill_null(0)
            for c in (
                "device_count",
                "stable_devices",
                "randomized_macs",
                "total_packets",
                "total_bytes",
                "new_stable_devices",
            )
        )
        .sort(TS)
    )


def build_agg_network_hourly(ctx: GoldContext, network_window: pl.DataFrame) -> pl.DataFrame:
    stable_first_seen = _first_seen(ctx).join(
        ctx.dim.filter(~pl.col("is_randomized")).select("mac"), on="mac"
    )
    hourly = (
        ctx.fact.join(stable_first_seen, on="mac", how="left")
        .group_by("hour_start")
        .agg(
            *_device_counts(),
            *_traffic(),
            pl.col("mac")
            .filter(pl.col("device_first_seen").dt.truncate("1h") == pl.col("hour_start"))
            .n_unique()
            .alias("new_stable_devices"),
        )
    )
    concurrency = network_window.group_by(pl.col(TS).dt.truncate("1h").alias("hour_start")).agg(
        pl.col("device_count").max().alias("peak_concurrent_devices"),
        pl.col("device_count").mean().alias("mean_concurrent_devices"),
    )
    count_columns = (
        "unique_devices",
        "stable_devices",
        "randomized_macs",
        "total_packets",
        "total_bytes",
        "new_stable_devices",
        "peak_concurrent_devices",
    )
    covered = pl.col("observed_windows") > 0
    return (
        ctx.calendar.join(hourly, on="hour_start", how="left")
        .join(concurrency, on="hour_start", how="left")
        .with_columns(
            pl.when(covered).then(pl.col(c).fill_null(0)).otherwise(None).alias(c) for c in count_columns
        )
        .with_columns(
            pl.when(covered)
            .then(pl.col("total_packets") / (pl.col("observed_windows") * ctx.window_seconds / 60))
            .otherwise(None)
            .alias("packets_per_min"),
            (pl.col("unique_devices") - pl.col("new_stable_devices")).alias("returning_devices"),
            pl.col("local_weekday").is_in([6, 7]).alias("is_weekend"),
        )
        .sort("hour_start")
    )


def build_agg_network_daily(
    ctx: GoldContext,
    network_window: pl.DataFrame,
    presence_daily: pl.DataFrame,
    coverage_daily: pl.DataFrame,
) -> pl.DataFrame:
    stable_first_seen = (
        _first_seen(ctx)
        .join(ctx.dim.filter(~pl.col("is_randomized")).select("mac"), on="mac")
        .with_columns(*local_time_columns("device_first_seen", ctx.timezone, prefix="first_seen"))
    )

    daily = (
        ctx.fact.join(stable_first_seen.select("mac", "first_seen_date"), on="mac", how="left")
        .group_by("local_date")
        .agg(
            *_device_counts(),
            *_traffic(),
            pl.col("mac")
            .filter(pl.col("first_seen_date") == pl.col("local_date"))
            .n_unique()
            .alias("new_stable_devices"),
            pl.col("local_hour").n_unique().alias("active_hours"),
        )
    )
    stable_activity = (
        presence_daily.filter(pl.col("device_class") == "stable")
        .group_by("local_date")
        .agg(
            pl.col("total_packets").median().alias("median_stable_device_packets"),
            pl.col("total_packets").quantile(0.9).alias("p90_stable_device_packets"),
            pl.col("active_minutes").median().alias("median_stable_active_minutes"),
        )
    )
    concurrency = network_window.group_by("local_date").agg(
        pl.col("device_count").max().alias("peak_concurrent_devices"),
        pl.col("device_count").mean().alias("mean_concurrent_devices"),
        pl.col("device_count").quantile(0.95).alias("p95_concurrent_devices"),
    )
    return (
        coverage_daily.select("local_date", "calendar_hours", "coverage_ratio", "observed_windows")
        .join(daily, on="local_date", how="left")
        .join(stable_activity, on="local_date", how="left")
        .join(concurrency, on="local_date", how="left")
        .with_columns(pl.col("local_date").dt.weekday().is_in([6, 7]).alias("is_weekend"))
        .sort("local_date")
    )


def _covered_hours(network_hourly: pl.DataFrame) -> pl.DataFrame:
    return network_hourly.filter(pl.col("coverage_ratio") >= MIN_COVERAGE_FOR_BASELINE)


def build_agg_hour_of_week(network_hourly: pl.DataFrame) -> pl.DataFrame:
    return (
        _covered_hours(network_hourly)
        .group_by("local_weekday", "local_hour")
        .agg(
            pl.col("unique_devices").mean().alias("mean_unique_devices"),
            pl.col("unique_devices").median().alias("median_unique_devices"),
            pl.col("stable_devices").median().alias("median_stable_devices"),
            pl.col("randomized_macs").median().alias("median_randomized_macs"),
            pl.col("estimated_devices").median().alias("median_estimated_devices"),
            pl.col("packets_per_min").median().alias("median_packets_per_min"),
            pl.col("mean_concurrent_devices").mean().alias("mean_concurrent_devices"),
            pl.len().alias("sample_hours"),
        )
        .sort("local_weekday", "local_hour")
    )


def _mad(column: str) -> pl.Expr:
    return (pl.col(column) - pl.col(column).median()).abs().median()


def build_agg_baseline_hour_of_week(
    network_hourly: pl.DataFrame,
    network_window: pl.DataFrame,
) -> pl.DataFrame:
    covered = _covered_hours(network_hourly)
    if covered.height == 0:
        return pl.DataFrame(
            schema={"local_weekday": pl.Int8, "local_hour": pl.Int8, "sample_hours": pl.UInt32}
        )
    slot = ["local_weekday", "local_hour"]
    last_date = covered["local_date"].max()
    recent = covered.filter(
        pl.col("local_date") > pl.lit(last_date) - pl.duration(days=BASELINE_LOOKBACK_DAYS)
    )
    recent_slots = recent.group_by(slot).agg(pl.len().alias("n")).filter(pl.col("n") >= BASELINE_MIN_SAMPLES)
    selected = pl.concat(
        [
            recent.join(recent_slots.select(slot), on=slot, how="semi"),
            covered.join(recent_slots.select(slot), on=slot, how="anti"),
        ]
    )

    hourly = selected.group_by(slot).agg(
        pl.col("unique_devices").median().alias("unique_devices_p50"),
        pl.col("unique_devices").quantile(0.1).alias("unique_devices_p10"),
        pl.col("unique_devices").quantile(0.9).alias("unique_devices_p90"),
        _mad("unique_devices").alias("unique_devices_mad"),
        pl.col("stable_devices").median().alias("stable_devices_p50"),
        pl.col("stable_devices").quantile(0.1).alias("stable_devices_p10"),
        pl.col("stable_devices").quantile(0.9).alias("stable_devices_p90"),
        pl.col("randomized_macs").median().alias("randomized_macs_p50"),
        pl.col("packets_per_min").median().alias("packets_per_min_p50"),
        pl.col("packets_per_min").quantile(0.1).alias("packets_per_min_p10"),
        pl.col("packets_per_min").quantile(0.9).alias("packets_per_min_p90"),
        _mad("packets_per_min").alias("packets_per_min_mad"),
        pl.len().alias("sample_hours"),
    )

    windows = (
        network_window.join(
            selected.select("hour_start"),
            left_on=pl.col(TS).dt.truncate("1h"),
            right_on="hour_start",
            how="semi",
        )
        .group_by("local_weekday", "local_hour")
        .agg(
            pl.col("device_count").quantile(0.1).alias("window_devices_p10"),
            pl.col("device_count").median().alias("window_devices_p50"),
            pl.col("device_count").quantile(0.9).alias("window_devices_p90"),
            pl.col("total_packets").median().alias("window_packets_p50"),
            pl.col("total_packets").quantile(0.9).alias("window_packets_p90"),
        )
    )
    return hourly.join(windows, on=["local_weekday", "local_hour"], how="left").sort(
        "local_weekday", "local_hour"
    )


ANOMALY_LOOKBACK_WEEKS = 4
ANOMALY_MIN_SAMPLES = 2


LOG_MAD_FLOOR = 0.15


def _trailing_same_hour(column: str, covered: pl.Expr) -> tuple[pl.Expr, pl.Expr, pl.Expr]:
    """Expected value and log-scale median of the same weekday-hour in previous weeks."""
    masked = pl.when(covered).then(pl.col(column)).otherwise(None)
    lags = [masked.shift(168 * week) for week in range(1, ANOMALY_LOOKBACK_WEEKS + 1)]
    history = pl.concat_list(lags).list.drop_nulls()
    log_median = pl.concat_list([lag.log1p() for lag in lags]).list.drop_nulls().list.median()
    return history.list.median(), log_median, history.list.len()


def _robust_z(actual: str, log_median: str) -> pl.Expr:
    """Log residual scaled by the pooled MAD of all residuals (a handful of weekly
    samples per slot is too few to estimate spread per hour). Counts are heavy-tailed
    because of randomized-MAC bursts, hence log space."""
    residual = pl.col(actual).log1p() - pl.col(log_median)
    spread = pl.max_horizontal(residual.abs().median(), pl.lit(LOG_MAD_FLOOR))
    return ROBUST_Z_SCALE * residual / spread


def build_agg_anomaly_hourly(network_hourly: pl.DataFrame) -> pl.DataFrame:
    """Score each covered hour against its own trailing same-hour-of-week history."""
    covered = pl.col("coverage_ratio") >= MIN_COVERAGE_FOR_BASELINE
    stable_exp, stable_med, stable_n = _trailing_same_hour("stable_devices", covered)
    ppm_exp, ppm_med, _ = _trailing_same_hour("packets_per_min", covered)
    unique_exp, unique_med, _ = _trailing_same_hour("unique_devices", covered)
    scored = (
        network_hourly.sort("hour_start")
        .with_columns(
            stable_exp.alias("expected_stable_devices"),
            stable_med.alias("_stable_log_median"),
            stable_n.alias("baseline_samples"),
            ppm_exp.alias("expected_packets_per_min"),
            ppm_med.alias("_ppm_log_median"),
            unique_exp.alias("expected_unique_devices"),
            unique_med.alias("_unique_log_median"),
        )
        .filter(covered & (pl.col("baseline_samples") >= ANOMALY_MIN_SAMPLES))
        .with_columns(
            _robust_z("stable_devices", "_stable_log_median").alias("z_stable_devices"),
            _robust_z("packets_per_min", "_ppm_log_median").alias("z_packets_per_min"),
            _robust_z("unique_devices", "_unique_log_median").alias("z_unique_devices"),
        )
    )
    stable_shift = (pl.col("stable_devices") - pl.col("expected_stable_devices")).abs()
    stable_floor = pl.max_horizontal(pl.col("expected_stable_devices").fill_null(0), pl.lit(1.0))
    return (
        scored.with_columns(
            (
                (pl.col("z_stable_devices").abs() > ANOMALY_Z_THRESHOLD)
                & (stable_shift >= ANOMALY_STABLE_MIN_ABS_DELTA)
            ).alias("_stable_anomaly"),
            (pl.col("z_packets_per_min").abs() > ANOMALY_Z_THRESHOLD).alias("_ppm_anomaly"),
            (
                (pl.col("z_unique_devices").abs() > ANOMALY_Z_THRESHOLD)
                & (pl.col("z_stable_devices").abs() <= ANOMALY_Z_THRESHOLD)
                & (pl.col("randomized_macs") >= ANOMALY_BURST_RANDOMIZED_RATIO * stable_floor)
            ).alias("is_randomized_burst"),
        )
        .with_columns(
            (pl.col("_stable_anomaly") | pl.col("_ppm_anomaly")).alias("is_anomaly"),
            pl.when(
                pl.col("_stable_anomaly")
                & (
                    ~pl.col("_ppm_anomaly")
                    | (pl.col("z_stable_devices").abs() >= pl.col("z_packets_per_min").abs())
                )
            )
            .then(pl.lit("stable_devices"))
            .otherwise(pl.lit("packets_per_min"))
            .alias("driver"),
        )
        .select(
            "hour_start",
            "local_date",
            "local_hour",
            "local_weekday",
            "coverage_ratio",
            "baseline_samples",
            "stable_devices",
            "expected_stable_devices",
            "z_stable_devices",
            "unique_devices",
            "expected_unique_devices",
            "z_unique_devices",
            "randomized_macs",
            "packets_per_min",
            "expected_packets_per_min",
            "z_packets_per_min",
            "driver",
            "is_anomaly",
            "is_randomized_burst",
        )
        .sort("hour_start")
    )
