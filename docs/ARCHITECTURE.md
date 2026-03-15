# Architecture Deep Dive

This document explains how FitLake processes raw wearable sensor data into actionable recovery insights through a three-layer medallion architecture, and why each design decision was made.

---

## End-to-End Data Flow

```
                    ┌──────────────────────────┐
                    │   Wearable Sensor Data    │
                    │ HRV · Sleep · HR · SpO₂   │
                    └────────────┬─────────────┘
                                 │ JSON (900 records/table)
                                 ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        MinIO (S3-compatible)                           │
│                       fitlake-raw bucket                               │
│   hrv.json · sleep.json · activity.json · skin_temp.json · users.json  │
└────────────────────────────────┬───────────────────────────────────────┘
                                 │ spark-submit
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER (Raw Iceberg Tables)                       │
│                                                                             │
│  bronze_ingestion.py performs:                                               │
│  1. Schema enforcement — typed StructType per source                        │
│  2. Audit columns — _ingestion_run_id + _ingest_timestamp on every row      │
│  3. MERGE INTO upserts — re-running the same file won't create duplicates   │
│  4. Schema evolution — skin_temp table created only when data appears        │
│     (simulates adding a new sensor mid-deployment)                          │
│                                                                             │
│  Tables: fitlake.bronze.hrv_readings                                        │
│          fitlake.bronze.sleep_records                                        │
│          fitlake.bronze.activity_records                                     │
│          fitlake.bronze.skin_temp_readings (from day 30 onward)             │
│                                                                             │
│  Partitioned by: date (Iceberg hidden partitioning)                         │
│  Format: Parquet + Snappy compression                                       │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                    ┌────────────┴────────────┐
                    ▼                         ▼
          ┌─────────────────┐     ┌───────────────────────┐
          │  Quality Checks │     │  Great Expectations    │
          │  (run_checks.py)│     │  validation suites     │
          │                 │     │  physiological bounds   │
          │  18/20 passed   │     │  null checks, uniques  │
          └────────┬────────┘     └───────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER (Cleaned Iceberg Tables)                     │
│                                                                             │
│  silver_transformation.py performs:                                          │
│  1. Deduplication — ROW_NUMBER() over _ingest_timestamp DESC per key        │
│  2. Bound validation — rejects physiologically impossible values:           │
│     • HRV rMSSD must be 5–250 ms                                           │
│     • Resting HR must be 25–130 bpm                                         │
│     • SpO₂ must be 80–100%                                                  │
│     • Steps must be 0–100,000                                               │
│  3. Anomaly filtering — records flagged is_anomaly=True are excluded        │
│  4. Derived columns — total_sleep_minutes, rem_pct, deep_pct               │
│  5. Quarantine — rejected records written to fitlake.silver.quarantine      │
│     with rejection_reason for investigation, not silently dropped           │
│                                                                             │
│  Tables: fitlake.silver.hrv_clean                                           │
│          fitlake.silver.sleep_clean                                          │
│          fitlake.silver.activity_clean                                       │
│          fitlake.silver.quarantine                                           │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     GOLD LAYER (Analytics-Ready Tables)                      │
│                                                                             │
│  gold_aggregation.py joins Silver tables and runs three scoring engines:     │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Recovery Score Engine (recovery_score.py)                          │    │
│  │  Joins: hrv_clean ⟕ sleep_clean on (user_id, date)                │    │
│  │  Output: 0–100 score with Red/Yellow/Green state                    │    │
│  │  Components: HRV 40% · RHR 25% · Sleep 25% · SpO₂ 10%            │    │
│  │  Key: uses PERSONAL rolling baselines, not population norms         │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Strain Calculator (strain_calculator.py)                           │    │
│  │  Input: activity_clean zone minutes                                 │    │
│  │  Output: 0–21 strain score with exponential zone weighting          │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Sleep Analyzer (sleep_analyzer.py)                                 │    │
│  │  Input: sleep_clean                                                 │    │
│  │  Output: sleep debt, consistency score, quality tier                 │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  Tables: fitlake.gold.daily_recovery                                        │
│          fitlake.gold.user_strain_summary                                   │
│          fitlake.gold.sleep_analytics                                       │
│          fitlake.gold.cohort_benchmarks                                     │
│                                                                             │
│  Iceberg features demonstrated:                                             │
│    • Time travel: VERSION AS OF queries                                     │
│    • Snapshot expiration: retain last 3 snapshots                           │
│    • Data file compaction: rewrite_data_files()                             │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                    ┌────────────┴────────────┐
                    ▼                         ▼
          ┌─────────────────┐     ┌───────────────────────┐
          │    Streamlit     │     │    Marquez UI          │
          │    Dashboard     │     │    Data Lineage Graph  │
          │  localhost:8501  │     │    localhost:3000       │
          └─────────────────┘     └───────────────────────┘
```

---

## Recovery Score Algorithm — Mathematical Detail

The recovery score is the analytical centerpiece. It answers: **"How ready is your body to perform today?"**

### Formula

```
Recovery = (HRV_score × 0.40) + (RHR_score × 0.25) + (Sleep_score × 0.25) + (SpO₂_score × 0.10)
```

### Component 1: HRV Score (40% weight)

HRV (Heart Rate Variability) is the gold standard for autonomic nervous system recovery. We use **rMSSD** (root mean square of successive R-R interval differences), the metric validated in peer-reviewed exercise science literature.

```
z = (HRV_today − HRV_baseline_7d) / HRV_std_7d
HRV_score = 50 + 20 × tanh(1.2 × z)
```

**Why this formula:**
- The **z-score** normalizes each user against their own baseline — a 40ms rMSSD is excellent for a beginner but concerning for an athlete with a 90ms baseline
- The **tanh sigmoid** prevents extreme z-scores from producing absurd results (a sensor glitch reading 300ms doesn't give a score of 500)
- The **7-day rolling window** adapts to fitness changes over time — as a user gets fitter, their baseline rises, and the bar for "good recovery" rises with it
- The **shift(1)** excludes today's reading from its own baseline, preventing data leakage

### Component 2: Resting Heart Rate Score (25% weight)

```
RHR_score = 75 − (RHR_today − RHR_baseline_7d) × 5
```

Lower resting HR relative to personal baseline indicates better parasympathetic recovery. Each bpm below baseline adds 5 points; each bpm above subtracts 5. The 75 center point means "at baseline" = "good but not great."

### Component 3: Sleep Performance Score (25% weight)

A composite of three sub-metrics:

```
Sleep_score = Duration_score × 0.50
            + Architecture_score × 0.30
            + Efficiency_score × 0.20
```

**Duration scoring** uses a piecewise function:
- < 4h → 0 points (severely insufficient)
- 4–6h → linear ramp to 70 (insufficient)
- 6–9h → linear ramp 70–100 (optimal zone)
- \> 9h → slight penalty (oversleeping can signal illness)

**Architecture scoring** penalizes deviation from optimal sleep stage distribution:
- Optimal REM: 20–25% of total sleep (memory consolidation, emotional processing)
- Optimal Deep: 15–20% of total sleep (physical recovery, HGH release)

**Efficiency scoring** maps 70% efficiency to 0 points, 95% to 100 points.

### Component 4: SpO₂ Score (10% weight)

```
≥ 97% → 100 points (normal)
95–97% → 80 + (SpO₂ − 95) × 10
90–95% → 20 + (SpO₂ − 90) × 12 (concerning)
< 90% → escalating penalty (dangerously low)
```

### State Classification

```
Green  (≥ 66): Well-recovered — ready for high-intensity training
Yellow (33–65): Moderate recovery — light training recommended
Red    (< 33): Poor recovery — prioritize rest
```

---

## Schema Evolution Demo

Real-world wearable companies constantly add new sensors. FitLake demonstrates this with `skin_temp_delta`:

1. **Days 1–29**: The data generator produces no skin temperature data (simulating pre-deployment)
2. **Day 30+**: `skin_temp.json` begins containing records
3. **Bronze ingestion**: The job wraps the skin_temp ingest in try/except — on first encounter, it `CREATE TABLE IF NOT EXISTS`, on subsequent runs, it merges normally
4. **Iceberg handles it transparently**: No ALTER TABLE needed for existing tables. The new table coexists in the same catalog namespace

This demonstrates understanding of a critical real-world data engineering challenge: **your schema will change, and your pipeline must handle it gracefully without manual intervention.**

---

## Quarantine Pattern

Instead of silently dropping bad records (which hides data quality issues), the Silver layer writes them to `fitlake.silver.quarantine` with:
- `source_table`: which Bronze table the record came from
- `rejection_reason`: human-readable explanation (e.g., "avg_hr_bpm out of range [30,220]: 245.7")
- `raw_record`: full JSON of the original record for debugging

This means data engineers can:
1. Query the quarantine table to see rejection trends
2. Identify sensor failures or data pipeline bugs
3. Decide whether to relax bounds or fix upstream issues

---

## Orchestration: Airflow DAG

The DAG uses **TaskGroups** to visually organize the pipeline into logical stages:

```
generate_data → upload_to_minio → [bronze_layer] → [quality_checks] → [silver_layer] → [gold_layer] → notify
```

Key patterns:
- **BranchPythonOperator** in quality_checks: if quality fails, pipeline continues (quarantine handles bad data) rather than blocking the entire run
- **TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS** on Silver: ensures Silver runs even if the quality branch took the "failed" path
- **OpenLineage integration**: every Spark job emits lineage events to Marquez automatically via `spark.extraListeners`

---

## Data Lineage

Every pipeline run emits [OpenLineage](https://openlineage.io) events to Marquez. This creates a visual graph showing:
- Which raw files produced which Bronze tables
- Which Bronze tables feed into which Silver tables
- Which Silver tables are consumed by which Gold aggregations
- Job duration, success/failure status, and row counts

This is configured via two Spark properties:
```
spark.extraListeners = io.openlineage.spark.agent.OpenLineageSparkListener
spark.openlineage.transport.url = http://marquez:5000
```

No code changes needed — lineage is captured automatically from Spark's query plan.

---

## Local vs Docker Architecture

The project is designed to work in two modes:

**Local mode** (no Docker): The `pipeline/aggregation/` modules are pure Python/Pandas with zero Spark dependency. The dashboard falls back to reading `data/raw/*.json` directly and running the scoring engines inline. This means `make run-local && make dashboard-local` works on any machine with Python installed.

**Docker mode** (full stack): `docker-compose up` launches Spark (master + 2 workers), MinIO, Airflow, Marquez, and Streamlit. The Spark jobs read from MinIO's S3-compatible API using `s3a://` URIs, and Iceberg tables are stored in MinIO's `fitlake` bucket.

This dual-mode architecture was a deliberate choice: it lets anyone evaluate the code quality without needing 8GB of Docker containers, while still demonstrating the full distributed stack for those who want to see it in action.
