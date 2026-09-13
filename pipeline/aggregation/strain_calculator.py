"""
Strain Score Calculator.

Computes a 0–21 cardiovascular strain score per day, modeled after
the WHOOP strain metric and Borg RPE research.

The score is derived from time spent in each heart rate zone, weighted
exponentially — Zone 6 (>95% max HR) contributes ~10× more than Zone 1.

Zone thresholds (% of max HR):
  Zone 1: <50%   — Recovery / resting
  Zone 2: 50-60% — Light aerobic
  Zone 3: 60-70% — Moderate aerobic
  Zone 4: 70-80% — Anaerobic threshold
  Zone 5: 80-90% — VO2 max zone
  Zone 6: >90%   — Neuromuscular peak
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from loguru import logger

# Exponential zone weights — reflects physiological cost
ZONE_WEIGHTS = {
    1: 0.25,
    2: 0.50,
    3: 1.00,
    4: 2.50,
    5: 5.00,
    6: 10.0,
}

MAX_STRAIN = 21.0

# Weighted minutes needed to reach max strain. Chosen so the top of the
# distribution is not truncated: at 150 the hardest 4.3% of sessions all
# clipped to exactly 21.0, flattening the top of the chart into a straight
# line. At 250, clipping falls to ~1% while all five strain categories stay
# populated and "All Out" stays rare.
#
# Note this maps weighted load to strain linearly. WHOOP's published scale is
# logarithmic, so very hard sessions compress less there than here.
NORMALIZATION_FACTOR = 250.0


@dataclass
class StrainResult:
    user_id: str
    date: str
    strain_score: float  # 0-21
    strain_category: str  # "Recovery", "Light", "Moderate", "Hard", "All Out"
    total_active_minutes: int
    peak_hr_pct_max: float  # peak HR as % of max HR
    zone_breakdown: dict  # {zone: minutes}
    dominant_zone: int  # zone where most time was spent


STRAIN_CATEGORIES = [
    (0, 5, "Recovery"),
    (5, 10, "Light"),
    (10, 14, "Moderate"),
    (14, 18, "Hard"),
    (18, 21.01, "All Out"),
]


def _categorize_strain(score: float) -> str:
    for lo, hi, label in STRAIN_CATEGORIES:
        if lo <= score < hi:
            return label
    return "All Out"


def calculate_strain_score(
    zone_minutes: dict[int, float],
    peak_hr: float,
    max_hr: int,
) -> StrainResult:
    """
    Compute strain score from zone minutes.

    Uses the exponential zone-weight formula:
      raw_strain = Σ (zone_minutes[z] × ZONE_WEIGHTS[z])
      strain_score = (raw_strain / NORMALIZATION_FACTOR) × MAX_STRAIN
    """
    raw = sum(zone_minutes.get(z, 0) * w for z, w in ZONE_WEIGHTS.items())
    score = float(np.clip((raw / NORMALIZATION_FACTOR) * MAX_STRAIN, 0, MAX_STRAIN))

    total_minutes = int(sum(zone_minutes.values()))
    peak_pct = peak_hr / max_hr * 100 if max_hr > 0 else 0
    dominant_zone = max(zone_minutes, key=zone_minutes.get) if zone_minutes else 1

    return StrainResult(
        user_id="",
        date="",
        strain_score=round(score, 2),
        strain_category=_categorize_strain(score),
        total_active_minutes=total_minutes,
        peak_hr_pct_max=round(peak_pct, 1),
        zone_breakdown=zone_minutes,
        dominant_zone=dominant_zone,
    )


def compute_strain_scores(
    activity_df: pd.DataFrame, user_profiles_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Compute strain scores for all users and dates.

    Args:
        activity_df: Must include user_id, date, zone1-6_minutes, peak_hr_bpm
        user_profiles_df: Must include user_id, max_hr

    Returns:
        DataFrame with strain scores per user-day.
    """
    merged = activity_df.merge(
        user_profiles_df[["user_id", "max_hr"]], on="user_id", how="left"
    )
    merged["max_hr"] = merged["max_hr"].fillna(190)

    results = []
    for _, row in merged.iterrows():
        zone_minutes = {z: float(row.get(f"zone{z}_minutes", 0)) for z in range(1, 7)}
        result = calculate_strain_score(
            zone_minutes,
            peak_hr=row["peak_hr_bpm"],
            max_hr=int(row["max_hr"]),
        )
        result.user_id = row["user_id"]
        result.date = str(row["date"])
        results.append(
            {
                "user_id": result.user_id,
                "date": result.date,
                "strain_score": result.strain_score,
                "strain_category": result.strain_category,
                "total_active_minutes": result.total_active_minutes,
                "peak_hr_pct_max": result.peak_hr_pct_max,
                "dominant_zone": result.dominant_zone,
                **{
                    f"zone{z}_minutes": result.zone_breakdown.get(z, 0)
                    for z in range(1, 7)
                },
            }
        )

    df = pd.DataFrame(results)
    logger.info(
        f"Strain scores computed: {len(df)} records | "
        f"Avg strain: {df.strain_score.mean():.1f} | "
        f"Max strain: {df.strain_score.max():.1f}"
    )
    return df
