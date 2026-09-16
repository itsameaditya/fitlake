"""
Local pipeline runner — Gold layer without Spark.

Mirrors `spark/jobs/gold_aggregation.py` using the same pure-Pandas scoring
engines from `pipeline/aggregation/`, reading `data/raw/` and writing the Gold
outputs to `data/output/`. This is what `make run-local` drives.

The Spark job is the production path; this exists so the full pipeline can be
exercised (and the dashboard's Gold-layer path tested) with no cluster, no
Docker, and no S3.

Run: python pipeline/run_local.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from loguru import logger

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

RAW_DIR = REPO_ROOT / "data" / "raw"
OUTPUT_DIR = REPO_ROOT / "data" / "output"

SLEEP_JOIN_COLUMNS = [
    "user_id",
    "date",
    "total_sleep_hours",
    "sleep_efficiency_pct",
    "rem_sleep_minutes",
    "deep_sleep_minutes",
    "spo2_avg_pct",
    "resting_hr_bpm",
]


def _write(df: pd.DataFrame, name: str) -> None:
    """Write a Gold table as JSON records, matching the dashboard's reader."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"{name}.json"
    df.to_json(path, orient="records", indent=2)
    logger.success(f"Written → {path.relative_to(REPO_ROOT)} ({len(df):,} rows)")


def build_cohort_benchmarks(recovery_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cross-user benchmarks — the Pandas equivalent of the Spark percent_rank
    window in gold_aggregation.build_cohort_benchmarks.
    """
    benchmarks = (
        recovery_df.groupby("user_id")
        .agg(
            avg_recovery=("recovery_score", "mean"),
            avg_hrv=("hrv_rmssd", "mean"),
            avg_hrv_baseline=("hrv_baseline", "mean"),
            days_tracked=("recovery_score", "count"),
        )
        .reset_index()
    )
    # percent_rank: (rank - 1) / (n - 1), matching Spark's definition
    benchmarks["recovery_percentile"] = (
        benchmarks["avg_recovery"]
        .rank(method="min")
        .sub(1)
        .div(max(len(benchmarks) - 1, 1))
        .mul(100)
        .round(1)
    )
    return benchmarks.round(2)


def main() -> None:
    if not (RAW_DIR / "hrv.json").exists():
        logger.error(f"No data in {RAW_DIR}. Run: make generate-data")
        raise SystemExit(1)

    from pipeline.aggregation.recovery_score import compute_recovery_scores
    from pipeline.aggregation.sleep_analyzer import compute_sleep_metrics
    from pipeline.aggregation.strain_calculator import compute_strain_scores

    hrv_df = pd.read_json(RAW_DIR / "hrv.json")
    sleep_df = pd.read_json(RAW_DIR / "sleep.json")
    activity_df = pd.read_json(RAW_DIR / "activity.json")
    users_df = pd.read_json(RAW_DIR / "users.json")

    logger.info(
        f"Loaded raw: {len(hrv_df):,} hrv | {len(sleep_df):,} sleep | "
        f"{len(activity_df):,} activity | {len(users_df):,} users"
    )

    recovery = compute_recovery_scores(
        hrv_df.merge(sleep_df[SLEEP_JOIN_COLUMNS], on=["user_id", "date"])
    )
    strain = compute_strain_scores(activity_df, users_df)
    sleep_analytics = compute_sleep_metrics(sleep_df)
    benchmarks = build_cohort_benchmarks(recovery)

    _write(recovery, "daily_recovery")
    _write(strain, "strain_scores")
    _write(sleep_analytics, "sleep_analytics")
    _write(benchmarks, "cohort_benchmarks")

    logger.success("Local Gold layer complete → data/output/")


if __name__ == "__main__":
    main()
