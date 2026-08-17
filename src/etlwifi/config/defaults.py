from __future__ import annotations

from datetime import datetime

# Collection window for neighborhood dataset (inclusive bounds for validation).
COLLECTION_START = datetime(2026, 5, 27)
COLLECTION_END = datetime(2026, 8, 17, 23, 59, 59, 999999)

DATE_PARTITION_PREFIX = "date="

FACT_TABLE = "fact_device_window"
DIM_TABLE = "dim_device"
BRIDGE_TABLE = "device_window_channels"
