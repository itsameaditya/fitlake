# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies (lightweight subset for local dev — no Spark/Docker needed)
pip install numpy pandas scipy click loguru streamlit plotly pytest pytest-cov

# Generate synthetic data (required before running anything)
make generate-data               # 10 users × 90 days → data/raw/
python data/generate_data.py --users 3 --days 14 --output data/raw   # smaller test set

# Run full local pipeline (no Docker)
make run-local

# Launch Streamlit dashboard (after run-local)
make dashboard-local             # opens http://localhost:8501

# Run tests
pytest tests/ -v                              # all tests
pytest tests/test_recovery_score.py -v        # single file
pytest tests/ -v -m "not integration"         # skip slow tests
pytest tests/ --cov=pipeline --cov-report=term-missing

# Lint / format
ruff check pipeline/ spark/ quality/ analytics/ tests/ data/
black pipeline/ spark/ quality/ analytics/ tests/ data/

# Full Docker stack
make infra-up                    # starts Spark, MinIO, Airflow, Marquez, Streamlit
make run-pipeline                # generate-data → bronze → silver → gold
make infra-down

# Spark jobs (requires Docker stack running)
make run-bronze / run-silver / run-gold

# Data quality checks
python quality/run_checks.py     # validates data/raw/ against physiological bounds
```

## Architecture

The pipeline follows a **medallion lakehouse** pattern: `Raw JSON → Bronze → Silver → Gold`, all stored as **Apache Iceberg tables** on MinIO (local S3-compatible storage).

### Data flow

```
data/generate_data.py
  └─ data/raw/{hrv,sleep,activity,skin_temp}.json
        └─ spark/jobs/bronze_ingestion.py   → fitlake.bronze.*  (Iceberg, MERGE INTO)
              └─ spark/jobs/silver_transformation.py → fitlake.silver.*  (dedup, quarantine)
                    └─ spark/jobs/gold_aggregation.py  → fitlake.gold.*  (scores, analytics)
                          └─ analytics/dashboard.py   (Streamlit, reads gold or raw directly)
```

### Key design decisions

**`pipeline/aggregation/` contains pure Python/Pandas logic** — no Spark dependency. The Spark Gold job (`spark/jobs/gold_aggregation.py`) calls `.toPandas()`, runs the scoring engines, then converts back to Spark DataFrames. This makes the scoring logic independently testable without a Spark cluster.

**Recovery score** (`pipeline/aggregation/recovery_score.py`) is the analytical core: 40% HRV, 25% resting HR, 25% sleep, 10% SpO₂ — all compared against each user's **personal rolling baseline** (7-day window), not population norms. Score state: Red <33, Yellow 33–65, Green ≥66.

**Schema evolution demo** — `skin_temp` data is intentionally absent for the first 30 simulated days (controlled in `generate_data.py:generate_skin_temp`). The Bronze job handles its late arrival gracefully with a try/except that creates the table only when data exists.

**Quarantine table** (`fitlake.silver.quarantine`) — Silver writes rejected records here rather than dropping them. Records fail validation against physiological bounds defined as constants at the top of `silver_transformation.py`.

**Dashboard fallback** — `analytics/dashboard.py` first looks for Gold layer output in `data/output/`, then falls back to running the pipeline inline from `data/raw/` using the Pandas scoring engines. This means the dashboard works without Docker.

### Spark + Iceberg configuration

All Spark jobs use the same `build_spark_session()` pattern reading from env vars (`MINIO_ENDPOINT`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`). The Iceberg catalog is named `fitlake` with a Hadoop catalog type pointing to `s3a://fitlake/warehouse`. The full config is also in `spark/conf/spark-defaults.conf` for Docker deployments.

### Airflow DAG structure

`airflow/dags/fitlake_pipeline.py` groups tasks into 5 `TaskGroup`s (generate → bronze → quality → silver → gold). The quality group uses `BranchPythonOperator` — pipeline continues even on quality failures (bad records go to quarantine, not pipeline halt). Silver uses `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` to handle this branch.

### Testing

Tests cover only the pure-Python `pipeline/aggregation/` layer. There are no Spark unit tests — Spark jobs are validated via the CI data-gen smoke test. The `sample_dataframe` fixture in `test_recovery_score.py` is the canonical test dataset shape.
