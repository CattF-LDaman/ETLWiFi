# ETLWiFi

Passive WiFi monitoring of my neighbourhood, built as an end-to-end data-engineering project:
a monitor-mode sniffer, a medallion pipeline (bronze → silver → gold) on Polars + Parquet, and a
serving layer that feeds a live dashboard on [luwe.be](https://luwe.be/playground/wifi_neighbourhood).

```
USB adapter (monitor mode)
   │  802.11 frames, channel hopping 1/6/11
   ▼
capture ── 10 s windows, MACs HMAC-hashed at source ──► landing/capture/YYYY-MM-DD.csv
   │                                                    landing/telemetry/YYYY-MM-DD.csv
   ▼
worker ─┬─ every 15 min: bronze → silver → gold (idempotent, content-hashed) ─► dashboard.json
        └─ every 30 s:   landing tail + gold baseline ─────────────────────────► live.json
                                   │  HMAC-signed, gzipped POST
                                   ▼
                     portfolio /api/ingest/wifi/{kind} → ECharts dashboard
```

## Privacy

No raw MAC address leaves the capture process.

- MACs are replaced by `HMAC-SHA256(MAC_HASH_SALT, mac)[:16]`. Without the key, pseudonyms cannot be
  reversed with a lookup table over the 2⁴⁸ MAC space.
- Before hashing, the capture keeps two non-identifying facts: whether the address is randomized
  (locally administered bit), and the manufacturer prefix (OUI) for non-randomized addresses.
- SSIDs from client probe requests are never stored, because they reveal the networks a phone has
  joined. Only access-point beacon SSIDs are kept, and only as keyed hashes.
- Legacy exports that contain raw MACs are hashed during bronze ingest. Ingest fails closed when
  `MAC_HASH_SALT` is missing.
- Published documents contain aggregates only, never per-device pseudonyms. Vendor groups with
  fewer than 3 devices are folded into "Other".

## Data contract

The capture CSV is v2 of the original sniffer format. It appends columns only, so v1 exports remain
valid input. Every column lands as a string in bronze and is typed in silver.

| column | meaning |
|---|---|
| `window_timestamp` | window end, naive UTC |
| `mac` | HMAC pseudonym |
| `packet_count`, `total_bytes` | frames and bytes in the window |
| `min_rssi`, `max_rssi`, `avg_rssi` | signal strength in dBm (null when the driver gives none) |
| `mgmt_pkts`, `ctrl_pkts`, `data_pkts` | frame-type mix |
| `observed_channels` | channels the device was heard on, `1\|6` |
| `mac_randomized`, `oui` | derived before hashing |
| `beacon_pkts`, `probe_req_pkts`, `probe_resp_pkts`, `retry_pkts` | management detail |
| `ap_channel`, `ssid_hash` | for access points, from beacons |
| `probe_fingerprint` | hash of probe-request capability IEs; groups randomized MACs per device model |

Telemetry writes one row per window, including empty windows: frame counters, device count,
per-channel dwell time, interface mode, and capture version. These rows tell "nobody around" apart
from "capture was down", which is what the coverage metrics are built on.

## Layers

- **Bronze**: each landing file is a source tracked by content hash in `_manifest.jsonl`. Changed
  files replace only their own parts. The dataset snapshot is the latest ingest of every source.
- **Silver**: `fact_device_window`, `device_window_channels`, `dim_device` (randomized flag, OUI,
  vendor from the IEEE registry), `fact_capture_window`, `capture_window_dwell`. Timestamps are UTC,
  and `(timestamp, mac)` is deduplicated across overlapping sources.
- **Gold**: 23 tables in local time (Europe/Brussels). Highlights:
  - `agg_network_hourly`: a gap-free hourly calendar with `coverage_ratio`. Uncovered hours are
    null, not zero.
  - `agg_baseline_hour_of_week`: p10/p50/p90 per weekday × hour over the last 28 days, falling back
    to full history for thin slots.
  - `agg_anomaly_hourly`: each covered hour against its trailing same-hour-of-week median, using a
    robust z-score on log counts (|z| > 3.5).
  - `dim_device_profile`: behavioural segments (`access_point`, `infrastructure`, `resident`,
    `regular`, `passerby`, `randomized`) from absolute, explainable rules.
  - `agg_churn_daily`, `agg_dwell_distribution`, `agg_vendor`, `agg_channel_hourly`,
    `dim_access_point`.
  - ML features: `feat_device_static`, `feat_device_hour_profile`, `feat_device_daily` (time-based
    rolling windows), `feat_network_hourly` (lags, rolling stats, cyclical encodings),
    `feat_session_events`, `feat_co_presence_topk`.

  Thresholds live in `src/etlwifi/config/defaults.py`. Every gold build is validated for unique
  keys, value ranges, a stable/randomized split that adds up, segment consistency, and a gap-free
  calendar.

Each stage is skipped when its input hash matches the previous run. A full rebuild of about 3.3 M rows
takes roughly 10 s.

## Running it

### Docker (production)

```bash
cp .env.example .env                  # set MAC_HASH_SALT and WIFI_INGEST_SECRET
sudo ./deploy/host-setup.sh           # once: NetworkManager ignores the adapter, data dirs
docker compose up -d --build worker   # pipeline + publishing
docker compose up -d --build capture  # sniffer
docker compose ps                     # health comes from the services' heartbeat files
```

`capture` runs with host networking plus `NET_ADMIN`/`NET_RAW`, and puts the adapter into monitor
mode itself. `worker` runs as uid 1000 and only needs the data directory.

### Local

```bash
conda create -n ETLWiFi python=3.12 && conda activate ETLWiFi
pip install -e ".[dev,notebooks]"
export MAC_HASH_SALT=dev-salt
etlwifi run neighborhood.csv          # or no args: legacy CSV + landing zone from settings
etlwifi serve dashboard --stdout      # build a serving document
etlwifi worker                        # scheduler, writes data/serving/*.json
```

Other commands: `etlwifi bronze ingest`, `etlwifi silver transform`, `etlwifi gold transform`,
`etlwifi capture`, `etlwifi health {capture,worker}`.

### Development

```bash
pytest
ruff check src tests scripts && ruff format --check src tests scripts
```

CI runs lint, the test suite (Python 3.11 and 3.12), and both Docker builds.

## Analysis

[`notebooks/analysis.ipynb`](notebooks/analysis.ipynb) reads the gold layer. It covers capture
coverage, randomized vs stable devices, clustering of devices by hour-of-week profile, a review of
flagged anomalies, and how well the seasonal baseline forecasts the next week.

## Layout

```
src/etlwifi/
├── capture/    sniffer: radio control, frame parsing, window aggregation, CSV sink
├── bronze/     multi-source raw landing + pseudonymisation
├── silver/     typed facts and dimensions, OUI vendor reference
├── gold/       aggregates, segments, anomaly scoring, ML features
├── serving/    dashboard/live documents and the signed publisher
├── pipeline/   orchestration, locking, run log
├── worker.py   long-running scheduler
└── cli/        `etlwifi` command
```
