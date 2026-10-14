from __future__ import annotations

from datetime import datetime

# Earliest expected observation time (inclusive lower bound for validation).
COLLECTION_START = datetime(2026, 5, 27)

DATE_PARTITION_PREFIX = "date="

# Silver tables
FACT_TABLE = "fact_device_window"
DIM_TABLE = "dim_device"
BRIDGE_TABLE = "device_window_channels"
CAPTURE_WINDOW_TABLE = "fact_capture_window"
CAPTURE_DWELL_TABLE = "capture_window_dwell"

# Gold: capture quality
AGG_CAPTURE_COVERAGE_DAILY = "agg_capture_coverage_daily"

# Gold: network activity
AGG_NETWORK_WINDOW = "agg_network_window"
AGG_NETWORK_HOURLY = "agg_network_hourly"
AGG_NETWORK_DAILY = "agg_network_daily"
AGG_HOUR_OF_WEEK = "agg_hour_of_week"
AGG_BASELINE_HOUR_OF_WEEK = "agg_baseline_hour_of_week"
AGG_ANOMALY_HOURLY = "agg_anomaly_hourly"

# Gold: radio
AGG_CHANNEL_HOURLY = "agg_channel_hourly"
AGG_CHANNEL_DEVICE = "agg_channel_device"
AGG_RSSI_DISTRIBUTION = "agg_rssi_distribution"

# Gold: presence and devices
AGG_PRESENCE_DAILY = "agg_presence_daily"
AGG_CHURN_DAILY = "agg_churn_daily"
AGG_DWELL_DISTRIBUTION = "agg_dwell_distribution"
AGG_VENDOR = "agg_vendor"
DIM_DEVICE_PROFILE = "dim_device_profile"
DIM_ACCESS_POINT = "dim_access_point"
AGG_ACCESS_POINT_DAILY = "agg_access_point_daily"

# Gold: ML features
FEAT_DEVICE_STATIC = "feat_device_static"
FEAT_DEVICE_DAILY = "feat_device_daily"
FEAT_DEVICE_HOUR_PROFILE = "feat_device_hour_profile"
FEAT_NETWORK_HOURLY = "feat_network_hourly"
FEAT_SESSION_EVENTS = "feat_session_events"
FEAT_CO_PRESENCE_TOPK = "feat_co_presence_topk"

GOLD_TABLES = (
    AGG_CAPTURE_COVERAGE_DAILY,
    AGG_NETWORK_WINDOW,
    AGG_NETWORK_HOURLY,
    AGG_NETWORK_DAILY,
    AGG_HOUR_OF_WEEK,
    AGG_BASELINE_HOUR_OF_WEEK,
    AGG_ANOMALY_HOURLY,
    AGG_CHANNEL_HOURLY,
    AGG_CHANNEL_DEVICE,
    AGG_RSSI_DISTRIBUTION,
    AGG_PRESENCE_DAILY,
    AGG_CHURN_DAILY,
    AGG_DWELL_DISTRIBUTION,
    AGG_VENDOR,
    DIM_DEVICE_PROFILE,
    DIM_ACCESS_POINT,
    AGG_ACCESS_POINT_DAILY,
    FEAT_DEVICE_STATIC,
    FEAT_DEVICE_DAILY,
    FEAT_DEVICE_HOUR_PROFILE,
    FEAT_NETWORK_HOURLY,
    FEAT_SESSION_EVENTS,
    FEAT_CO_PRESENCE_TOPK,
)

# Analytical thresholds (documented in README "Gold layer").
SESSION_GAP_SEC = 120
MIN_COVERAGE_FOR_BASELINE = 0.8
BASELINE_LOOKBACK_DAYS = 28
ANOMALY_Z_THRESHOLD = 3.5
RESIDENT_PRESENCE_RATIO = 0.5
RESIDENT_MIN_SPAN_DAYS = 7
REGULAR_MIN_ACTIVE_DAYS = 3
INFRASTRUCTURE_MGMT_RATIO = 0.95
INFRASTRUCTURE_MAX_RSSI_STD = 4.0
FEATURE_MIN_WINDOWS = 30
