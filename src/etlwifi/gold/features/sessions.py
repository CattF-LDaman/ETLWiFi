from __future__ import annotations

import polars as pl

from etlwifi.gold.context import TS, GoldContext, local_time_columns


def build_feat_session_events(ctx: GoldContext) -> pl.DataFrame:
    sessions = ctx.sessions.filter(pl.col("device_class") == "stable")
    if sessions.height == 0:
        return pl.DataFrame(schema={"mac": pl.Utf8, "local_date": pl.Date})

    channels = (
        ctx.bridge.join(ctx.dim.filter(~pl.col("is_randomized")).select("mac"), on="mac", how="semi")
        .sort(TS)
        .join_asof(
            sessions.select("mac", "session_id", pl.col("session_start").alias(TS)).sort(TS),
            on=TS,
            by="mac",
            strategy="backward",
            check_sortedness=False,
        )
        .group_by("mac", "session_id")
        .agg(pl.col("channel").n_unique().alias("channels_used"))
    )
    return (
        sessions.join(channels, on=["mac", "session_id"], how="left")
        .with_columns(
            pl.col("channels_used").fill_null(0),
            *local_time_columns("session_start", ctx.timezone),
        )
        .drop("device_class")
        .sort("mac", "session_start")
    )


def build_feat_co_presence_topk(
    ctx: GoldContext,
    *,
    min_windows: int = 100,
    k: int = 5,
) -> pl.DataFrame:
    """Top-k devices most often seen in the same window, with Jaccard similarity."""
    active = ctx.dim.filter(~pl.col("is_randomized") & (pl.col("window_count") >= min_windows))
    schema = {
        "mac": pl.Utf8,
        "rank": pl.UInt32,
        "cohort_mac": pl.Utf8,
        "shared_windows": pl.UInt32,
        "jaccard": pl.Float64,
    }
    if active.height < 2:
        return pl.DataFrame(schema=schema)

    presence = ctx.fact.join(active.select("mac"), on="mac", how="semi").select(TS, "mac")
    counts = presence.group_by("mac").agg(pl.len().alias("windows"))
    pairs = (
        presence.join(presence, on=TS, suffix="_other")
        .filter(pl.col("mac") != pl.col("mac_other"))
        .group_by("mac", "mac_other")
        .agg(pl.len().alias("shared_windows"))
        .join(counts, on="mac")
        .join(counts.rename({"mac": "mac_other", "windows": "windows_other"}), on="mac_other")
        .with_columns(
            (
                pl.col("shared_windows")
                / (pl.col("windows") + pl.col("windows_other") - pl.col("shared_windows"))
            ).alias("jaccard")
        )
        .sort(["mac", "shared_windows"], descending=[False, True])
        .with_columns(pl.int_range(1, pl.len() + 1).over("mac").cast(pl.UInt32).alias("rank"))
        .filter(pl.col("rank") <= k)
    )
    return pairs.select(
        "mac",
        "rank",
        pl.col("mac_other").alias("cohort_mac"),
        pl.col("shared_windows").cast(pl.UInt32),
        "jaccard",
    )
