# ETLWiFi

Passive WiFi neighborhood monitoring dataset I collected at home turned into a lightweight data engineering showcase.

The full dataset is not included in this repo (download coming soon). Place `neighborhood.csv` in the project root before running the pipeline.

## Setup

```bash
pip install -r requirements.txt   # editable install of the etlwifi package
cp .env.example .env              # optional: MAC_HASH_SALT for hashing
```

## Pipeline CLI

Run the full pipeline (bronze ingest → silver transform):

```bash
etlwifi run neighborhood.csv
```

Or run each stage separately:

```bash
etlwifi bronze ingest neighborhood.csv
etlwifi silver transform neighborhood
```

Re-run is idempotent (skips when content hash matches). Updated exports replace prior parts for the same source file. Use `--force` to re-process identical content.

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
├── pipeline/   # end-to-end orchestration
├── config/     # default data paths
└── cli/        # etlwifi command
```
