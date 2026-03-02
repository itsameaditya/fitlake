"""
Silver Layer — Cleaning, Deduplication, and Validation.

Reads Bronze Iceberg tables, applies data quality rules, and writes
cleaned records to Silver Iceberg tables.

Transformations applied:
  1. Deduplication: remove duplicate records by primary key + date
  2. Outlier removal: flag/drop physiologically impossible values
  3. Type casting & null handling
  4. Derived columns: total_sleep_minutes, hr_zones_total
  5. Quarantine table: invalid records written to fitlake.silver.quarantine

Silver tables:
  fitlake.silver.hrv_clean
  fitlake.silver.sleep_clean
  fitlake.silver.activity_clean
  fitlake.silver.quarantine          ← bad records for investigation
"""

import os
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from loguru import logger


# ── Validation Bounds (physiologically based) ─────────────────────────────────

HRV_BOUNDS = {"hrv_rmssd_ms": (5, 250), "hrv_sdnn_ms": (10, 400), "respiratory_rate_brpm": (8, 30)}
SLEEP_BOUNDS = {
    "total_sleep_hours": (1.0, 14.0),
    "sleep_efficiency_pct": (20.0, 100.0),
    "spo2_avg_pct": (80.0, 100.0),
    "resting_hr_bpm": (25.0, 130.0),
}
ACTIVITY_BOUNDS = {
    "avg_hr_bpm": (30.0, 220.0),
    "peak_hr_bpm": (30.0, 225.0),
    "steps": (0, 100_000),
    "active_calories": (0, 5_000),
}


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FitLake-Silver-Transformation")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.fitlake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.fitlake.type", "hadoop")
        .config("spark.sql.catalog.fitlake.warehouse", "s3a://fitlake/warehouse")
        .config("spark.hadoop.fs.s3a.endpoint",
                os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key",
                os.getenv("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key",
                os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def create_silver_tables(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS fitlake.silver")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS fitlake.silver.hrv_clean
        USING iceberg
        PARTITIONED BY (`date`)
        AS SELECT * FROM fitlake.bronze.hrv_readings WHERE 1=0
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS fitlake.silver.sleep_clean
        USING iceberg
        PARTITIONED BY (`date`)
        AS SELECT *, CAST(NULL AS DOUBLE) AS total_sleep_minutes
        FROM fitlake.bronze.sleep_records WHERE 1=0
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS fitlake.silver.activity_clean
        USING iceberg
        PARTITIONED BY (`date`)
        AS SELECT *, CAST(NULL AS INT) AS total_zone_minutes
        FROM fitlake.bronze.activity_records WHERE 1=0
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS fitlake.silver.quarantine (
            source_table STRING,
            record_id STRING,
            user_id STRING,
            `date` STRING,
            rejection_reason STRING,
            raw_record STRING,
            quarantined_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (`date`)
    """)
    logger.info("Silver tables created/verified.")


def deduplicate(df: DataFrame, key_col: str, date_col: str = "date") -> DataFrame:
    """Keep most recently ingested record for each key."""
    window = Window.partitionBy(key_col).orderBy(F.col("_ingest_timestamp").desc())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )


def validate_bounds(
    df: DataFrame, bounds: dict, id_col: str, source_table: str
) -> tuple[DataFrame, DataFrame]:
    """
    Split DataFrame into valid records and quarantined records.
    Returns (valid_df, quarantine_df).
    """
    is_valid = F.lit(True)
    rejection_parts = []

    for col_name, (lo, hi) in bounds.items():
        if col_name not in df.columns:
            continue
        condition = F.col(col_name).between(lo, hi) | F.col(col_name).isNull()
        is_valid = is_valid & condition
        rejection_parts.append(
            F.when(~F.col(col_name).between(lo, hi),
                   F.concat(F.lit(f"{col_name} out of range [{lo},{hi}]: "),
                            F.col(col_name).cast("string")))
        )

    rejection_reason = F.concat_ws("; ", *rejection_parts) if rejection_parts else F.lit("")

    valid_df = df.filter(is_valid)
    invalid_df = (
        df.filter(~is_valid)
          .select(
              F.lit(source_table).alias("source_table"),
              F.col(id_col).alias("record_id"),
              F.col("user_id"),
              F.col("date"),
              rejection_reason.alias("rejection_reason"),
              F.to_json(F.struct(*df.columns)).alias("raw_record"),
              F.current_timestamp().alias("quarantined_at"),
          )
    )

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    logger.info(
        f"{source_table}: {valid_count} valid, {invalid_count} quarantined "
        f"({invalid_count / (valid_count + invalid_count) * 100:.1f}% rejection rate)"
    )
    return valid_df, invalid_df


def transform_hrv(df: DataFrame) -> DataFrame:
    """Silver transforms for HRV table."""
    return (
        df.withColumn("hrv_rmssd_ms", F.round(F.col("hrv_rmssd_ms"), 2))
          .withColumn("hrv_sdnn_ms", F.round(F.col("hrv_sdnn_ms"), 2))
          .withColumn("date", F.to_date(F.col("date")))
          .filter(F.col("is_anomaly") == False)  # Remove known sensor anomalies
    )


def transform_sleep(df: DataFrame) -> DataFrame:
    """Silver transforms for sleep table — add derived metrics."""
    return (
        df.withColumn("total_sleep_minutes",
                      F.round(F.col("total_sleep_hours") * 60, 0).cast("double"))
          .withColumn("rem_pct",
                      F.round(F.col("rem_sleep_minutes") / F.col("total_sleep_minutes") * 100, 1))
          .withColumn("deep_pct",
                      F.round(F.col("deep_sleep_minutes") / F.col("total_sleep_minutes") * 100, 1))
          .withColumn("date", F.to_date(F.col("date")))
          .filter(F.col("is_anomaly") == False)
    )


def transform_activity(df: DataFrame) -> DataFrame:
    """Silver transforms for activity table — compute total zone minutes."""
    zone_cols = [f"zone{i}_minutes" for i in range(1, 7)]
    total_zone_expr = sum(F.coalesce(F.col(c), F.lit(0)) for c in zone_cols)
    return (
        df.withColumn("total_zone_minutes", total_zone_expr)
          .withColumn("date", F.to_date(F.col("date")))
          .withColumn("workout_type", F.initcap(F.col("workout_type")))
    )


def write_silver(spark: SparkSession, df: DataFrame, target: str) -> None:
    df.createOrReplaceTempView("silver_data")
    spark.sql(f"""
        MERGE INTO {target} t
        USING silver_data s
        ON t.date = s.date AND t.user_id = s.user_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    logger.info(f"Written → {target}")


def run_silver_transformation(spark: SparkSession) -> None:
    """Full Silver transformation pipeline."""
    create_silver_tables(spark)
    quarantine_records = []

    # ── HRV ──────────────────────────────────────────────────────────────────
    hrv_bronze = spark.table("fitlake.bronze.hrv_readings")
    hrv_deduped = deduplicate(hrv_bronze, "reading_id")
    hrv_valid, hrv_bad = validate_bounds(hrv_deduped, HRV_BOUNDS, "reading_id", "bronze.hrv_readings")
    hrv_silver = transform_hrv(hrv_valid)
    write_silver(spark, hrv_silver, "fitlake.silver.hrv_clean")
    quarantine_records.append(hrv_bad)

    # ── Sleep ─────────────────────────────────────────────────────────────────
    sleep_bronze = spark.table("fitlake.bronze.sleep_records")
    sleep_deduped = deduplicate(sleep_bronze, "record_id")
    sleep_valid, sleep_bad = validate_bounds(sleep_deduped, SLEEP_BOUNDS, "record_id", "bronze.sleep_records")
    sleep_silver = transform_sleep(sleep_valid)
    write_silver(spark, sleep_silver, "fitlake.silver.sleep_clean")
    quarantine_records.append(sleep_bad)

    # ── Activity ──────────────────────────────────────────────────────────────
    activity_bronze = spark.table("fitlake.bronze.activity_records")
    activity_deduped = deduplicate(activity_bronze, "record_id")
    activity_valid, activity_bad = validate_bounds(
        activity_deduped, ACTIVITY_BOUNDS, "record_id", "bronze.activity_records"
    )
    activity_silver = transform_activity(activity_valid)
    write_silver(spark, activity_silver, "fitlake.silver.activity_clean")
    quarantine_records.append(activity_bad)

    # ── Write quarantine ──────────────────────────────────────────────────────
    from functools import reduce
    all_bad = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), quarantine_records)
    if all_bad.count() > 0:
        all_bad.writeTo("fitlake.silver.quarantine").append()
        logger.warning(f"Quarantine: {all_bad.count()} records written for review.")

    logger.success("Silver transformation complete.")


if __name__ == "__main__":
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    run_silver_transformation(spark)
    spark.stop()
