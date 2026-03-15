# Design Decisions

Every technology and architecture choice in FitLake was made for a specific reason. This document explains what was chosen, what alternatives were considered, and why.

---

## 1. Apache Iceberg over Delta Lake or Hudi

**Chosen:** Apache Iceberg 1.5
**Alternatives considered:** Delta Lake, Apache Hudi

**Why Iceberg:**
- **Engine-agnostic.** Iceberg works with Spark, Flink, Trino, DuckDB, Snowflake, and more — you're not locked into the Databricks ecosystem (Delta Lake) or a specific query engine
- **Schema evolution is first-class.** Iceberg handles adding, dropping, renaming, and reordering columns without rewriting data files. Delta Lake supports this too, but Iceberg's approach (tracking schema changes in metadata) is cleaner
- **Hidden partitioning.** Iceberg partitions data without exposing partition columns to queries. `WHERE date = '2024-03-01'` works directly — no need for `WHERE year=2024 AND month=3 AND day=1`
- **Time travel via snapshot IDs.** `VERSION AS OF 1` lets you query any historical state. This is critical for auditing and reproducibility in health data pipelines
- **Industry momentum.** Snowflake, AWS, Google BigQuery, and Cloudera have all adopted Iceberg as their open-table format. It's becoming the standard

**Why not Delta Lake:** Tighter coupling to Databricks/Spark ecosystem. Open-source Delta has fewer features than Databricks-managed Delta.
**Why not Hudi:** More complex configuration, primarily optimized for streaming upserts rather than batch analytics.

---

## 2. DuckDB over Snowflake for Local Analytics

**Chosen:** DuckDB 0.10
**Alternatives considered:** Snowflake, PostgreSQL, SQLite

**Why DuckDB:**
- **Free and zero-config.** No cloud account, no credit card, no cluster management
- **Iceberg-native.** DuckDB can read Iceberg tables directly via its Iceberg extension — same tables Spark writes, DuckDB reads, no ETL needed
- **Analytical SQL performance.** DuckDB is columnar and vectorized, designed for OLAP queries. It runs the Gold layer analytics queries in milliseconds on 900 records, and scales to millions
- **Snowflake-compatible SQL.** The SQL syntax is close enough that queries written for DuckDB work on Snowflake with minimal changes, making migration trivial

**Why not Snowflake:** Requires a paid account. For a portfolio project that anyone should be able to clone and run, a free local option is essential.
**Why not PostgreSQL:** Row-oriented storage is inefficient for analytical queries over wide tables with many columns.

---

## 3. MinIO over LocalStack for S3 Emulation

**Chosen:** MinIO
**Alternatives considered:** LocalStack, filesystem storage

**Why MinIO:**
- **Production-identical S3 API.** MinIO implements the exact same S3 API that AWS uses — `s3a://` URIs, multipart upload, bucket policies. Code that works against MinIO works against real S3 with zero changes
- **Single binary, lightweight.** ~60MB Docker image vs LocalStack's 1GB+
- **Built-in web console** on port 9001 for visual inspection of buckets and objects
- **Used in production** by companies running on-premise S3-compatible storage

**Why not LocalStack:** Heavier, emulates many AWS services we don't need, and the free tier has limitations.
**Why not filesystem:** Using local files would mean the Spark jobs need different I/O code for local vs cloud. With MinIO, the code is identical.

---

## 4. Exponential Zone Weighting for Strain Score

**Chosen:** Exponential weights (Zone 6 = 10×, Zone 1 = 0.25×)
**Alternatives considered:** Linear weighting, TRIMP (Training Impulse)

**Why exponential:**

```
Zone 1 (Recovery):  0.25×
Zone 2 (Light):     0.50×
Zone 3 (Moderate):  1.00×
Zone 4 (Hard):      2.50×
Zone 5 (Very Hard): 5.00×
Zone 6 (Max):       10.0×
```

The physiological cost of exercise increases **exponentially**, not linearly, with heart rate intensity. 10 minutes at 95% max HR produces far more than 3× the cardiovascular stress of 10 minutes at 60%. This is supported by:
- **Lucia's TRIMP** model (2003): exponential weighting of HR zones
- **Session RPE** research (Foster et al.): perceived exertion scales non-linearly with intensity
- **WHOOP's published methodology**: strain uses a logarithmic/exponential model

Linear weighting would undervalue high-intensity work and overvalue low-intensity volume.

---

## 5. Personal Rolling Baselines over Population Norms

**Chosen:** 7-day rolling baseline per user
**Alternatives considered:** Population percentiles, fixed thresholds, 30-day baseline

**Why personal baselines:**
- A 40ms HRV is **elite** for a 65-year-old sedentary person but **concerning** for a 25-year-old athlete
- Population norms create false positives (healthy people scored as "Red") and false negatives (unhealthy people scored as "Green")
- WHOOP's core innovation was exactly this: comparing each user to themselves, not to others

**Why 7 days, not 30?**
A 7-day window is responsive enough to detect acute training effects (a hard week immediately shifts the baseline) but stable enough to not be dominated by a single outlier day. A 30-day window would be too slow to adapt — a user returning from vacation would have an artificially low baseline for weeks.

**Why shift(1)?**
The baseline excludes today's reading to prevent data leakage. If today's HRV is included in its own baseline, the z-score is artificially compressed toward zero, reducing the algorithm's sensitivity.

---

## 6. Quarantine Table over Silent Drops

**Chosen:** Write rejected records to `fitlake.silver.quarantine` with rejection reasons
**Alternatives considered:** DROP records silently, COALESCE to default values

**Why quarantine:**
- **Observability.** You can query the quarantine table to see rejection trends over time. A spike in quarantined records might indicate a sensor firmware bug or a data pipeline issue.
- **Debuggability.** The full raw record is preserved as JSON, so you can inspect exactly what failed and why.
- **Auditability.** For health data, silently dropping records is dangerous. A missing day of data could mask a medical event.

**Why not COALESCE to defaults?** Replacing an invalid SpO₂ of 45% with a default of 97% doesn't fix the data — it creates a *lie* that could affect downstream recovery scores.

---

## 7. Airflow over Prefect for Orchestration

**Chosen:** Apache Airflow 2.9
**Alternatives considered:** Prefect, Dagster, Luigi

**Why Airflow:**
- **Industry standard.** The majority of data engineering teams use Airflow. Knowing Airflow is directly transferable to most jobs.
- **TaskGroups** provide visual pipeline organization in the UI
- **SparkSubmitOperator** integrates natively with Spark clusters
- **OpenLineage integration** is production-ready via `openlineage-airflow`
- **BranchPythonOperator** enables conditional pipeline paths (quality pass/fail branching)

**Why not Prefect:** Prefect is excellent for Python-native workflows, but has less mature Spark integration and smaller enterprise adoption. For a data engineering resume, Airflow demonstrates more relevant experience.

---

## 8. Pandas Scoring Engines with Spark Bridge

**Chosen:** Pure Pandas for `pipeline/aggregation/`, Spark `.toPandas()` bridge in Gold jobs
**Alternatives considered:** Spark UDFs, pure Spark SQL, Pandas UDFs

**Why this hybrid approach:**
- **Testability.** The scoring algorithms can be unit-tested with pytest and simple DataFrames, without launching a SparkSession. This is why we have 44 tests that run in 0.4 seconds.
- **Readability.** The recovery score formula is complex enough that expressing it in Spark SQL or UDFs would obscure the logic. Pandas makes the math transparent.
- **Portability.** The dashboard uses the same scoring code without Spark, enabling the local-mode fallback.
- **Scale path.** When data grows beyond what `.toPandas()` can handle, these functions can be wrapped as Spark Pandas UDFs (applyInPandas) with minimal refactoring — the math stays identical.

---

## 9. Synthetic Data over Real Datasets

**Chosen:** Custom synthetic generator with configurable realism
**Alternatives considered:** Kaggle Fitbit datasets, Apple Health exports

**Why synthetic:**
- **Controlled anomalies.** We inject exactly 2% anomalies to test the quality pipeline. With real data, you don't know what the "right answer" is.
- **Schema evolution demo.** We intentionally withhold skin_temp for 30 days. Real datasets don't have this.
- **Training periodization.** The generator simulates realistic 4-week training blocks. Random real-world data wouldn't show clean training patterns.
- **No privacy concerns.** Synthetic data can be committed to Git and shared freely.
- **Reproducibility.** Seeded RNG (`np.random.default_rng(42)`) means every run produces identical data.

---

## 10. Terraform with EMR Serverless over EKS

**Chosen:** AWS EMR Serverless for cloud deployment
**Alternatives considered:** EMR on EC2, EKS with Spark Operator, Glue

**Why EMR Serverless:**
- **No cluster management.** You submit jobs and AWS handles compute provisioning, scaling, and teardown. No idle cluster costs.
- **Iceberg-native.** EMR 7.0 includes Iceberg runtime out of the box — no JAR management.
- **Cost-effective for batch.** You pay only for the seconds your job runs, not for a 24/7 cluster.
- **S3 integration.** EMR Serverless reads/writes S3 natively, which is where our Iceberg tables live.

**Why not EKS:** More operational overhead (managing Kubernetes + Spark Operator). Appropriate for streaming or very frequent batch jobs, but overkill for a daily pipeline.
**Why not Glue:** Vendor lock-in, slower iteration cycle, and limited Iceberg support compared to EMR.
