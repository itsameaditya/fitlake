"""
Strain Score Calculator.

Computes a 0–21 cardiovascular strain score per day, modeled after
the WHOOP strain metric and Borg RPE research.

The score is derived from time spent in each heart rate zone, weighted
exponentially — a Zone 6 minute costs 40× a Zone 1 minute.

Zone thresholds (% of max HR), matching the bands assigned in
data/generate_data.py:_estimate_hr_zones:
  Zone 1: <60%   — Recovery / resting
  Zone 2: 60-70% — Light aerobic
  Zone 3: 70-80% — Moderate aerobic
  Zone 4: 80-90% — Anaerobic threshold
  Zone 5: 90-95% — VO2 max zone
  Zone 6: >95%   — Neuromuscular peak
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

# Weighted minutes that define an all-out day — the load mapped to 21.0.
# Anchored physiologically rather than fitted to the sample: 40 minutes at
# neuromuscular peak (Zone 6, weight 10), or an equivalent mix such as 80
# minutes at VO2 max (Zone 5, weight 5).
ALL_OUT_LOAD = 400.0

# Shape of the curve below that anchor. WHOOP's published strain scale is
# logarithmic — the first hard minutes of a session move the score far more
# than the last ones — so this maps load through log1p rather than linearly.
#
# A linear map could not fit this distribution: weighted load is right-skewed
# (median workout 34, max 400), so any divisor either crushed typical sessions
# or clipped the top. At the previous 250 the median workout scored 2.9 and
# 81% of days landed in "Recovery" despite 627 of 900 containing a real
# workout, while the hardest 1.6% all clipped to exactly 21.0 — leaving more
# days at the ceiling than in "Moderate" and "Hard" combined.
#
# The knee sets how fast early load accumulates: at 15 weighted minutes a
# median aerobic session lands in "Light" and a sustained Zone 4-5 session in
# "Moderate"/"Hard", with the ceiling reached only by genuine outliers.
LOAD_KNEE = 15.0


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

    Exponential zone weights, mapped to 0–21 on a log scale:
      raw_strain    = Σ (zone_minutes[z] × ZONE_WEIGHTS[z])
      strain_score  = MAX_STRAIN × log1p(raw / LOAD_KNEE)
                                 ÷ log1p(ALL_OUT_LOAD / LOAD_KNEE)

    Zero load scores 0, ALL_OUT_LOAD scores exactly 21, and loads beyond it
    clip — so a freak session cannot drag the rest of the scale down.
    """
    raw = sum(zone_minutes.get(z, 0) * w for z, w in ZONE_WEIGHTS.items())
    normalized = np.log1p(raw / LOAD_KNEE) / np.log1p(ALL_OUT_LOAD / LOAD_KNEE)
    score = float(np.clip(normalized * MAX_STRAIN, 0, MAX_STRAIN))

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
