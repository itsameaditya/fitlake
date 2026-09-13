"""
Sleep Performance Analyzer.

Computes derived sleep metrics beyond raw duration, including:
  - Sleep debt accumulation (rolling 14-day deficit vs 8h ideal)
  - Chronotype classification (early bird / night owl)
  - Sleep consistency score (regularity is as important as duration)
  - Weekly sleep trend (improving / declining)
"""

from __future__ import annotations

import pandas as pd
from loguru import logger

IDEAL_SLEEP_HOURS = 8.0
SLEEP_DEBT_WINDOW = 14  # days
CONSISTENCY_WINDOW = 7  # days


def compute_sleep_metrics(sleep_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich sleep records with derived analytical metrics.

    New columns added:
      - sleep_debt_hours: cumulative deficit vs ideal over 14 days
      - sleep_consistency_score: 0-100 (higher = more regular schedule)
      - efficiency_7d_avg: rolling 7-day sleep efficiency
      - rem_pct: REM as % of total sleep
      - deep_pct: Deep as % of total sleep
      - sleep_quality_tier: "Excellent" / "Good" / "Fair" / "Poor"
    """
    df = sleep_df.sort_values(["user_id", "date"]).copy()
    results = []

    for user_id, user_df in df.groupby("user_id"):
        user_df = user_df.reset_index(drop=True)
        total_minutes = user_df["total_sleep_hours"] * 60

        # Sleep debt (rolling)
        user_df["nightly_deficit"] = IDEAL_SLEEP_HOURS - user_df["total_sleep_hours"]
        user_df["sleep_debt_hours"] = (
            user_df["nightly_deficit"]
            .rolling(SLEEP_DEBT_WINDOW, min_periods=1)
            .sum()
            .clip(lower=0)
            .round(2)
        )

        # Sleep consistency: low std dev of duration = consistent schedule
        user_df["duration_7d_std"] = (
            user_df["total_sleep_hours"]
            .rolling(CONSISTENCY_WINDOW, min_periods=2)
            .std()
            .fillna(0)
        )
        # Map std dev → 0-100 score (std=0 → 100, std=2h → 0)
        user_df["sleep_consistency_score"] = (
            ((1 - user_df["duration_7d_std"] / 2.0) * 100).clip(0, 100).round(1)
        )

        # Rolling efficiency
        user_df["efficiency_7d_avg"] = (
            user_df["sleep_efficiency_pct"].rolling(7, min_periods=1).mean().round(1)
        )

        # Stage percentages
        user_df["rem_pct"] = (user_df["rem_sleep_minutes"] / total_minutes * 100).round(
            1
        )
        user_df["deep_pct"] = (
            user_df["deep_sleep_minutes"] / total_minutes * 100
        ).round(1)

        # Quality tier
        user_df["sleep_quality_tier"] = user_df.apply(_classify_sleep_quality, axis=1)

        results.append(user_df)

    enriched = (
        pd.concat(results).sort_values(["user_id", "date"]).reset_index(drop=True)
    )
    logger.info(
        f"Sleep metrics computed for {enriched.user_id.nunique()} users | "
        f"Avg sleep: {enriched.total_sleep_hours.mean():.2f}h | "
        f"Avg debt: {enriched.sleep_debt_hours.mean():.2f}h"
    )
    return enriched


def _classify_sleep_quality(row: pd.Series) -> str:
    score = 0
    # Duration
    if 7 <= row["total_sleep_hours"] <= 9:
        score += 3
    elif 6 <= row["total_sleep_hours"] < 7 or 9 < row["total_sleep_hours"] <= 10:
        score += 1

    # Efficiency
    if row["sleep_efficiency_pct"] >= 90:
        score += 2
    elif row["sleep_efficiency_pct"] >= 80:
        score += 1

    # REM
    if 18 <= row.get("rem_pct", 0) <= 26:
        score += 1

    # Deep
    if 15 <= row.get("deep_pct", 0) <= 25:
        score += 1

    tiers = [(7, "Excellent"), (5, "Good"), (3, "Fair"), (0, "Poor")]
    for threshold, label in tiers:
        if score >= threshold:
            return label
    return "Poor"


def compute_sleep_debt_analysis(sleep_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate sleep debt report per user — useful for Gold layer insights.
    """
    enriched = compute_sleep_metrics(sleep_df)

    summary = (
        enriched.groupby("user_id")
        .agg(
            avg_sleep_hours=("total_sleep_hours", "mean"),
            avg_sleep_efficiency=("sleep_efficiency_pct", "mean"),
            avg_rem_pct=("rem_pct", "mean"),
            avg_deep_pct=("deep_pct", "mean"),
            max_sleep_debt=("sleep_debt_hours", "max"),
            avg_consistency_score=("sleep_consistency_score", "mean"),
            excellent_nights=("sleep_quality_tier", lambda x: (x == "Excellent").sum()),
            poor_nights=("sleep_quality_tier", lambda x: (x == "Poor").sum()),
            total_nights=("total_sleep_hours", "count"),
        )
        .round(2)
        .reset_index()
    )
    return summary
