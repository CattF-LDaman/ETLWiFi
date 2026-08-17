# ETLWiFi

Passive WiFi neighborhood monitoring dataset I collected at home turned into a lightweight data engineering showcase.

The full dataset is not included in this repo (download coming soon). Place `neighborhood.csv` in the project root before running the pipeline.

## Setup

```bash
pip install -r requirements.txt   # editable install of the etlwifi package
cp .env.example .env              # optional: MAC_HASH_SALT for hashing
```

## Pipeline CLI

Bronze ingest (CSV → date-partitioned Parquet under `data/bronze/`):

```bash
etlwifi bronze ingest neighborhood.csv
# or: python -m etlwifi bronze ingest neighborhood.csv
```

Re-run is idempotent (skips unchanged files). Use `--force` to append a new copy.

## Privacy utility

Hash raw MAC addresses before sharing or committing data (standalone script):

```bash
python scripts/hash_mac_column.py neighborhood.csv
```

## Package layout

```
src/etlwifi/
├── bronze/     # raw landing (Parquet)
├── silver/     # cleaned dim/fact tables
├── gold/       # aggregates & ML features
├── config/     # default data paths
└── cli/        # etlwifi command
```
