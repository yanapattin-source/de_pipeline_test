# Pipeline Test — Sensor Data Pipeline

### This Readme.md Co-Writing with AI

## Video
https://drive.google.com/file/d/1LSqTL8FfqLII1qGXePAtCGAtu6UM0IP6/view?usp=sharing

A containerized data pipeline that generates sensor reading data and loads it into a PostgreSQL star schema, orchestrated by Mage.ai.

## Architecture

| Component | Technology |
|-----------|------------|
| Orchestration | Mage.ai |
| Database | PostgreSQL 16 |
| Containerization | Docker Compose |
| Data Format | Parquet |
| Language | Python 3 |

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.9+ (for data generation step only)

### 1. Generate Data

```bash
pip install -r requirements.txt
python sampledata.py
```

This creates ~44,640 Parquet files in `data_sample/` (~78M rows of sensor data for January 2023). Takes ~10-20 minutes.

### 2. Start Infrastructure

```bash
docker compose up -d --build
```

This starts:
- **PostgreSQL** on port 5432 (schema auto-created on first start)
- **Mage.ai** on port 6789 (web UI, authentication disabled for local dev)

> **Note:** Authentication is disabled via `REQUIRE_USER_AUTHENTICATION=0` in `docker-compose.yml`. Mage should open directly, and logging in is not required.

### 3. Run the Pipeline

Open Mage UI at **http://localhost:6789/overview**

If your browser is stuck on `/sign-in`, hard-refresh or clear site data for `localhost:6789`, then open `/overview` again. With auth disabled, Mage does not create a local user, so `admin@admin.com` / `admin` can fail even though the UI/API is usable without login.

1. Go to **Pipelines** → **load_sensor_data**
2. Click **Run**
3. For each day in January 2023, set the `day` runtime variable (e.g., `"2023-01-01"`)
4. Or run all 31 days via the backfill/batch run feature

Pipeline completes each day in ~1-2 minutes. Full month loads in under 30 minutes.

### 4. Verify Data

```bash
docker compose exec postgres psql -U pipeline -d sensor_db -c \
  "SELECT COUNT(*) FROM fact_sensor_reading;"
```

Expected: ~78,000,000 rows

## Database Schema

See [`diagrams/schema.md`](diagrams/schema.md) for the full ER diagram (Mermaid format).

**Star Schema:**
- `dim_department` — 100 departments
- `dim_sensor` — ~1,750 sensors (5-29 per department)
- `dim_product` — 1,000 products
- `fact_sensor_reading` — ~78M sensor readings (fact table)

## Pipeline Flow

```
[Parquet Files] → [Load Daily Batch] → [Normalize to Star Schema] → [Upsert to PostgreSQL]
     44,640 files    data_loader          transformer               data_exporter
```

**How blocks connect:**
1. **load_parquet_batch** (data_loader) — reads Parquet files for a given day, returns `{"df": DataFrame, "day": str}`
2. **normalize_sensor_data** (transformer) — receives the dict, splits into dimension + fact DataFrames
3. **load_to_postgres** (data_exporter) — receives the dict, upserts dimensions, bulk-inserts facts into PostgreSQL

Runtime variables (like `day`) are passed via `**kwargs` in each block's function signature.

## Mage Login / Authentication Troubleshooting

This repo pins Mage to `0.9.79` for reproducible behavior. The effective auth setting is controlled by the Docker environment variable in `docker-compose.yml`, not by Mage project YAML files.

Check the running server setting:

```bash
curl -s http://localhost:6789/api/statuses | python -m json.tool | grep require_user_authentication
```

Expected for local dev:

```json
"require_user_authentication": false
```

If you actually want a login screen, change `REQUIRE_USER_AUTHENTICATION` to `"1"` in `docker-compose.yml`, then restart Mage:

```bash
docker compose up -d --build --force-recreate mage
```

Default local owner credentials are configured in `docker-compose.yml`:

| Field | Value |
|-------|-------|
| Email | `admin@admin.com` |
| Username | `admin` |
| Password | `admin` |

## Extending the Pipeline

This pipeline is designed to be modular and easy to extend. Here's how to add new data sources and tables.

### Adding a New Table

1. **Add the table DDL** to `schema.sql`:

```sql
CREATE TABLE dim_location (
    location_id   SERIAL PRIMARY KEY,
    location_name VARCHAR(64) NOT NULL UNIQUE
);
```

2. **Recreate the database** (schema.sql only runs on first start):

```bash
docker compose down -v        # remove volumes (destroys existing data!)
docker compose up -d            # recreates DB with new schema
```

   Or add the table to a running database:

```bash
docker compose exec postgres psql -U pipeline -d sensor_db -c \
  "CREATE TABLE dim_location (location_id SERIAL PRIMARY KEY, location_name VARCHAR(64) NOT NULL UNIQUE);"
```

3. **Add a new Mage block** — in the Mage UI (http://localhost:6789), go to your pipeline and add/modify blocks as needed. Or create files manually:

   - `mage/sensor_pipeline/data_loaders/` — new data loader blocks
   - `mage/sensor_pipeline/transformers/` — new transformer blocks
   - `mage/sensor_pipeline/data_exporters/` — new exporter blocks

### Adding a New Data Source (CSV, JSON, API, etc.)

The data loader block pattern works with any format. Just create a new data loader:

```python
# mage/sensor_pipeline/data_loaders/load_csv_batch.py
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_csv_batch(**kwargs) -> dict:
    """Load CSV files instead of Parquet."""
    day = kwargs.get("day", "2023-01-01")
    df = pd.read_csv(f"/home/src/data_sample/{day}.csv")
    return {"df": df, "day": day}
```

Replace the Parquet data loader in your pipeline with the CSV version — no other changes needed.

### Connecting External SQL Clients

PostgreSQL is exposed on port 5432. Connect with any SQL client:

| Parameter | Value |
|-----------|-------|
| Host | `localhost` |
| Port | `5432` |
| Database | `sensor_db` |
| Username | `pipeline` |
| Password | `pipeline123` |

Works with DBeaver, pgAdmin, DataGrip, Azure Data Studio, or `psql` CLI.

### Adding a Completely New Pipeline

1. In Mage UI → **Pipelines** → **New pipeline**
2. Add data_loader, transformer, data_exporter blocks
3. Or create files in `mage/sensor_pipeline/` following the same pattern

Every pipeline is independent — you can have multiple pipelines writing to different tables in the same database.

## Stopping

```bash
docker compose down
# To remove data volumes (destroys all data!):
docker compose down -v
```

**Data persistence:** Regular `docker compose down` keeps your data. The PostgreSQL data lives in a named Docker volume (`pgdata`). Only `down -v` removes it.

## Security Note

This project uses hardcoded credentials (`pipeline123`) in `docker-compose.yml` and `load_to_postgres.py` for simplicity. These are **intentionally simple demo credentials** — not secrets worth protecting.

**If you plan to reuse or fork this project:**

1. **Change the default password** — replace `pipeline123` in these locations:
   - `docker-compose.yml` → `POSTGRES_PASSWORD` and `MAGE_DATABASE_CONNECTION_URL`
   - `mage/sensor_pipeline/data_exporters/load_to_postgres.py` → `DB_URL` fallback string
   - `mage/sensor_pipeline/io_config.yaml` → `POSTGRES_PASSWORD`

2. **Use environment variables** — create a `.env` file (already gitignored):
   ```bash
   # .env
   POSTGRES_PASSWORD=your_secure_password
   ```
   Then reference it in `docker-compose.yml`:
   ```yaml
   environment:
     POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
   ```

3. **Never commit real credentials** — the `.gitignore` already excludes `.env`

## Project Structure

```
├── docker-compose.yml     # Infrastructure definition
├── Dockerfile             # Mage.ai custom image with dependencies
├── schema.sql             # PostgreSQL DDL (auto-run on first start)
├── sampledata.py          # Data generation script
├── requirements.txt       # Python dependencies (minimum versions)
├── mage/                  # Mage.ai project
│   └── sensor_pipeline/
│       ├── __init__.py         # Project package marker
│       ├── io_config.yaml      # PostgreSQL connection config
│       ├── metadata.yaml       # Project metadata
│       ├── pipelines/
│       │   └── load_sensor_data/
│       │       ├── __init__.py          # Empty (Mage convention)
│       │       └── metadata.yaml        # Pipeline block definitions
│       ├── data_loaders/
│       │   └── load_parquet_batch.py     # Reads daily Parquet files
│       ├── transformers/
│       │   └── normalize_sensor_data.py # Splits into star schema parts
│       └── data_exporters/
│           └── load_to_postgres.py       # Upserts dimensions + bulk-inserts facts
├── diagrams/
│   └── schema.md          # ER diagram (Mermaid format)
├── Generate Data.ipynb    # Jupyter notebook version of sampledata.py
└── README.md
```
