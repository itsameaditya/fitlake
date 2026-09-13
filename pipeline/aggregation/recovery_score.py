"""
Recovery Score Engine — the analytical core of FitLake.

Implements a science-based recovery score (0–100) modeled after peer-reviewed
HRV research and wearable recovery indices. Components:

  1. HRV Score (40%)  — rMSSD vs personal 7-day rolling baseline
  2. Resting HR Score (25%) — RHR vs personal baseline (lower = better)
  3. Sleep Performance Score (25%) — duration × efficiency × stage quality
  4. SpO2 Score (10%) — penalize low blood oxygen

References:
  - Kiviniemi et al. (2007): HRV-guided training individualization
  - Plews et al. (2013): HRV and endurance performance
  - WHOOP Research: https://www.whoop.com/thelocker/recovery-science/
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
from loguru import logger

warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

HRV_WEIGHT = 0.40
RHR_WEIGHT = 0.25
SLEEP_WEIGHT = 0.25
SPO2_WEIGHT = 0.10

HRV_BASELINE_WINDOW = 7  # days for rolling HRV baseline
RHR_BASELINE_WINDOW = 7  # days for rolling RHR baseline
MIN_BASELINE_DAYS = 3  # minimum days before scoring is meaningful

RED_THRESHOLD = 33
YELLOW_THRESHOLD = 66


@dataclass
class DailyMetrics:
    user_id: str
    date: str
    hrv_rmssd: float  # ms
    resting_hr: float  # bpm
    total_sleep_hours: float
    sleep_efficiency_pct: float
    rem_sleep_minutes: float
    deep_sleep_minutes: float
    spo2_avg_pct: float
    prev_day_strain: Optional[float] = None  # 0-21 strain score


@dataclass
class RecoveryScore:
    user_id: str
    date: str
    recovery_score: int  # 0-100
    recovery_state: str  # "Red", "Yellow", "Green"
    hrv_component: float  # 0-100
    rhr_component: float  # 0-100
    sleep_component: float  # 0-100
    spo2_component: float  # 0-100
    hrv_rmssd: float
    hrv_baseline: float
    rhr_baseline: float
    sleep_performance: float  # 0-100
    needs_more_baseline: bool  # True if < MIN_BASELINE_DAYS of data


def _hrv_score(hrv_today: float, hrv_baseline: float, hrv_std: float) -> float:
    """
    Score HRV relative to personal baseline using a standardized z-score.

    A z-score of 0 (= exactly at baseline) maps to 50 points. One SD below
    baseline lands near 21, two SD near 9 — the curve saturates past ~2 SD.

    The amplitude was previously 20, which confined this component to
    [30, 70] and contradicted the range stated on the mapping comment below.
    That capped the 40%-weighted HRV term's influence on the final score at
    +/-8 points, so no combination of inputs could reach the Red band (<33).
    """
    if hrv_std < 1:
        hrv_std = hrv_baseline * 0.10  # fallback: 10% of baseline as SD

    z = (hrv_today - hrv_baseline) / hrv_std
    # Sigmoid-like mapping: z in [-3, +3] → ~[6, 94]
    score = 50 + 45 * np.tanh(z * 0.75)
    return float(np.clip(score, 5, 98))


def _rhr_score(rhr_today: float, rhr_baseline: float) -> float:
    """
    Lower RHR relative to baseline = better recovery.
    Each bpm below baseline adds ~5 points; above subtracts.
    """
    delta = rhr_today - rhr_baseline
    score = 75 - (delta * 5.0)
    return float(np.clip(score, 5, 98))


def _sleep_performance_score(
    total_hours: float,
    efficiency_pct: float,
    rem_minutes: float,
    deep_minutes: float,
) -> float:
    """
    Composite sleep score based on duration, architecture, and efficiency.

    Duration scoring:
      < 6h  → penalty (linear from 0 at 4h to 70 at 6h)
      6-9h  → peak zone (70-100)
      > 9h  → slight penalty (oversleeping can indicate illness)
    """
    # Duration component (50%)
    if total_hours < 4:
        duration_score = 0.0
    elif total_hours < 6:
        duration_score = (total_hours - 4) / 2 * 70
    elif total_hours <= 9:
        duration_score = 70 + (total_hours - 6) / 3 * 30
    else:
        duration_score = 100 - (total_hours - 9) * 8

    # Sleep architecture component (30%)
    total_sleep_minutes = total_hours * 60
    rem_pct = rem_minutes / total_sleep_minutes * 100 if total_sleep_minutes > 0 else 0
    deep_pct = (
        deep_minutes / total_sleep_minutes * 100 if total_sleep_minutes > 0 else 0
    )

    # Optimal: 20-25% REM, 15-20% Deep
    rem_score = 100 - abs(rem_pct - 22.5) * 4
    deep_score = 100 - abs(deep_pct - 17.5) * 5
    architecture_score = (rem_score + deep_score) / 2

    # Efficiency component (20%)
    efficiency_score = (efficiency_pct - 70) / 25 * 100  # 70% eff = 0, 95% = 100

    score = (
        duration_score * 0.50
        + np.clip(architecture_score, 0, 100) * 0.30
        + np.clip(efficiency_score, 0, 100) * 0.20
    )
    return float(np.clip(score, 0, 100))


def _spo2_score(spo2: float) -> float:
    """Blood oxygen: 97-100% → 100 pts, <94% → sharp penalty."""
    if spo2 >= 97:
        return 100.0
    elif spo2 >= 95:
        return 80.0 + (spo2 - 95) * 10
    elif spo2 >= 90:
        return 20.0 + (spo2 - 90) * 12
    else:
        return max(0.0, spo2 - 80) * 2


def calculate_recovery_score(
    metrics: DailyMetrics, hrv_baseline: float, hrv_std: float, rhr_baseline: float
) -> RecoveryScore:
    """Compute a single day's recovery score for one user."""
    hrv_comp = _hrv_score(metrics.hrv_rmssd, hrv_baseline, hrv_std)
    rhr_comp = _rhr_score(metrics.resting_hr, rhr_baseline)
    sleep_comp = _sleep_performance_score(
        metrics.total_sleep_hours,
        metrics.sleep_efficiency_pct,
        metrics.rem_sleep_minutes,
        metrics.deep_sleep_minutes,
    )
    spo2_comp = _spo2_score(metrics.spo2_avg_pct)

    raw = (
        hrv_comp * HRV_WEIGHT
        + rhr_comp * RHR_WEIGHT
        + sleep_comp * SLEEP_WEIGHT
        + spo2_comp * SPO2_WEIGHT
    )

    score = int(np.clip(round(raw), 0, 100))

    if score < RED_THRESHOLD:
        state = "Red"
    elif score < YELLOW_THRESHOLD:
        state = "Yellow"
    else:
        state = "Green"

    sleep_perf = _sleep_performance_score(
        metrics.total_sleep_hours,
        metrics.sleep_efficiency_pct,
        metrics.rem_sleep_minutes,
        metrics.deep_sleep_minutes,
    )

    return RecoveryScore(
        user_id=metrics.user_id,
        date=metrics.date,
        recovery_score=score,
        recovery_state=state,
        hrv_component=round(hrv_comp, 2),
        rhr_component=round(rhr_comp, 2),
        sleep_component=round(sleep_comp, 2),
        spo2_component=round(spo2_comp, 2),
        hrv_rmssd=metrics.hrv_rmssd,
        hrv_baseline=round(hrv_baseline, 2),
        rhr_baseline=round(rhr_baseline, 2),
        sleep_performance=round(sleep_perf, 2),
        needs_more_baseline=False,
    )


def compute_recovery_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Vectorized recovery score computation for all users and dates.

    Args:
        df: Combined DataFrame with columns from hrv, sleep, and activity tables.
            Must include: user_id, date, hrv_rmssd_ms, resting_hr_bpm,
            total_sleep_hours, sleep_efficiency_pct, rem_sleep_minutes,
            deep_sleep_minutes, spo2_avg_pct

    Returns:
        DataFrame with recovery scores and components for each user-day.
    """
    df = df.sort_values(["user_id", "date"]).copy()
    results = []

    for user_id, user_df in df.groupby("user_id"):
        user_df = user_df.reset_index(drop=True)

        # Rolling baselines (personalized — key to the WHOOP approach)
        user_df["hrv_baseline"] = (
            user_df["hrv_rmssd_ms"]
            .shift(1)
            .rolling(HRV_BASELINE_WINDOW, min_periods=MIN_BASELINE_DAYS)
            .mean()
        )
        user_df["hrv_std"] = (
            user_df["hrv_rmssd_ms"]
            .shift(1)
            .rolling(HRV_BASELINE_WINDOW, min_periods=MIN_BASELINE_DAYS)
            .std()
        )
        user_df["rhr_baseline"] = (
            user_df["resting_hr_bpm"]
            .shift(1)
            .rolling(RHR_BASELINE_WINDOW, min_periods=MIN_BASELINE_DAYS)
            .mean()
        )

        for _, row in user_df.iterrows():
            if pd.isna(row.get("hrv_baseline")):
                # Not enough baseline yet — use global mean as fallback
                hrv_base = user_df["hrv_rmssd_ms"].mean()
                hrv_std = user_df["hrv_rmssd_ms"].std() or hrv_base * 0.10
                rhr_base = user_df["resting_hr_bpm"].mean()
                needs_baseline = True
            else:
                hrv_base = row["hrv_baseline"]
                hrv_std = (
                    row["hrv_std"] if not pd.isna(row["hrv_std"]) else hrv_base * 0.10
                )
                rhr_base = row["rhr_baseline"]
                needs_baseline = False

            m = DailyMetrics(
                user_id=str(user_id),
                date=str(row["date"]),
                hrv_rmssd=row["hrv_rmssd_ms"],
                resting_hr=row["resting_hr_bpm"],
                total_sleep_hours=row["total_sleep_hours"],
                sleep_efficiency_pct=row["sleep_efficiency_pct"],
                rem_sleep_minutes=row["rem_sleep_minutes"],
                deep_sleep_minutes=row["deep_sleep_minutes"],
                spo2_avg_pct=row["spo2_avg_pct"],
            )
            r = calculate_recovery_score(m, hrv_base, hrv_std, rhr_base)
            r.needs_more_baseline = needs_baseline
            results.append(r.__dict__)

        logger.debug(f"Scored {len(user_df)} days for {user_id}")

    result_df = pd.DataFrame(results)
    logger.info(
        f"Recovery scores computed: {len(result_df)} records | "
        f"Green: {(result_df.recovery_state == 'Green').sum()} | "
        f"Yellow: {(result_df.recovery_state == 'Yellow').sum()} | "
        f"Red: {(result_df.recovery_state == 'Red').sum()}"
    )
    return result_df


if __name__ == "__main__":
    # Quick smoke test
    from pathlib import Path

    raw_dir = Path("data/raw")
    if not (raw_dir / "hrv.json").exists():
        logger.warning("No data found. Run: make generate-data")
    else:
        hrv_df = pd.read_json(raw_dir / "hrv.json")
        sleep_df = pd.read_json(raw_dir / "sleep.json")

        merged = hrv_df.merge(
            sleep_df[
                [
                    "user_id",
                    "date",
                    "total_sleep_hours",
                    "sleep_efficiency_pct",
                    "rem_sleep_minutes",
                    "deep_sleep_minutes",
                    "spo2_avg_pct",
                    "resting_hr_bpm",
                ]
            ],
            on=["user_id", "date"],
        )

        scores = compute_recovery_scores(merged)
        print(scores[["user_id", "date", "recovery_score", "recovery_state"]].head(20))
