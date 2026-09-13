"""
Synthetic wearable sensor data generator.

Produces 90 days of realistic fitness data for N users, mimicking metrics
from modern wearables (Apple Watch, Garmin, WHOOP-style bands):
  - HRV (rMSSD, SDNN) measured during sleep
  - Resting heart rate & SpO2
  - Sleep stages (REM, Deep, Light, Awake)
  - Activity & strain events (heart rate zones, workouts)

Design choices for realism:
  - Each user has a unique physiological baseline (age, fitness level)
  - Day-of-week patterns (lower recovery on Mondays from weekend activity)
  - Training blocks: hard week → recovery week cycles
  - Rare anomalies (~2% of readings) to test data quality pipelines
  - Schema evolution: new metric (skin_temp_delta) added at day 30
"""

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import click
import numpy as np
import pandas as pd
from loguru import logger


class _NumpyEncoder(json.JSONEncoder):
    """Serialize numpy scalars/arrays, which json.dump rejects by default."""

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


SEED = 42
rng = np.random.default_rng(SEED)

WORKOUT_TYPES = ["Run", "Cycling", "HIIT", "Strength", "Yoga", "Rest", "Walk", "Swim"]
WORKOUT_WEIGHTS = [0.20, 0.12, 0.10, 0.18, 0.08, 0.15, 0.12, 0.05]

HR_ZONE_LABELS = {
    1: "Recovery",
    2: "Light",
    3: "Moderate",
    4: "Hard",
    5: "Very Hard",
    6: "Max",
}


@dataclass
class UserProfile:
    """Physiological baseline for a synthetic user."""

    user_id: str
    age: int
    resting_hr_baseline: float  # bpm
    hrv_baseline: float  # rMSSD ms
    max_hr: int  # 220 - age (approx)
    fitness_level: str  # "beginner", "intermediate", "athlete"
    sleep_baseline: float  # hours
    spo2_baseline: float  # %

    @classmethod
    def generate(cls, user_id: str) -> "UserProfile":
        age = int(rng.integers(20, 45))
        fitness = rng.choice(["beginner", "intermediate", "athlete"], p=[0.2, 0.5, 0.3])
        hrv_ranges = {
            "beginner": (25, 55),
            "intermediate": (45, 80),
            "athlete": (65, 110),
        }
        rhr_ranges = {
            "beginner": (60, 75),
            "intermediate": (52, 68),
            "athlete": (42, 58),
        }
        lo, hi = hrv_ranges[fitness]
        rlo, rhi = rhr_ranges[fitness]
        return cls(
            user_id=user_id,
            age=age,
            resting_hr_baseline=rng.uniform(rlo, rhi),
            hrv_baseline=rng.uniform(lo, hi),
            max_hr=220 - age,
            fitness_level=fitness,
            sleep_baseline=rng.uniform(6.5, 8.5),
            spo2_baseline=rng.uniform(96.5, 99.0),
        )


def simulate_training_load(day_index: int, profile: UserProfile) -> float:
    """
    Returns a 0-1 'training load' multiplier.
    Simulates ~3-week training blocks followed by a recovery week.
    """
    cycle_day = day_index % 28
    if cycle_day < 21:
        # Progressive overload week 1-3
        base_load = 0.4 + 0.04 * (cycle_day % 7)
    else:
        # Recovery week
        base_load = 0.15
    # Add athlete modifier
    athlete_mod = {"beginner": 0.7, "intermediate": 1.0, "athlete": 1.3}[
        profile.fitness_level
    ]
    # Weekly pattern: Mon hard, Fri-Sat hard, Sun rest
    dow = day_index % 7
    dow_pattern = [1.1, 1.0, 0.9, 1.0, 1.1, 1.2, 0.5]
    return float(np.clip(base_load * athlete_mod * dow_pattern[dow], 0, 1))


def generate_hrv_reading(
    date_: date, user: UserProfile, training_load: float, anomaly: bool = False
) -> dict:
    """HRV measured via overnight sensor (rMSSD method)."""
    # HRV decreases with higher training load, recovers during rest
    load_penalty = training_load * 0.4
    noise = rng.normal(0, user.hrv_baseline * 0.08)
    hrv_rmssd = user.hrv_baseline * (1 - load_penalty) + noise

    if anomaly:
        hrv_rmssd *= rng.uniform(0.3, 0.5)  # sensor dropout / illness

    hrv_sdnn = hrv_rmssd * rng.uniform(1.6, 2.1)  # SDNN typically 1.6-2.1x rMSSD
    resp_rate = rng.normal(14.5, 1.2)

    return {
        "reading_id": f"{user.user_id}_{date_.isoformat()}_hrv",
        "user_id": user.user_id,
        "date": date_.isoformat(),
        "hrv_rmssd_ms": round(float(np.clip(hrv_rmssd, 8, 200)), 2),
        "hrv_sdnn_ms": round(float(np.clip(hrv_sdnn, 15, 350)), 2),
        "respiratory_rate_brpm": round(float(np.clip(resp_rate, 10, 22)), 1),
        "is_anomaly": anomaly,
        "ingested_at": datetime.utcnow().isoformat(),
    }


def generate_sleep_record(
    date_: date, user: UserProfile, training_load: float, anomaly: bool = False
) -> dict:
    """Sleep architecture matching polysomnography distributions."""
    if anomaly:
        total_sleep = rng.uniform(2.5, 4.5)
    else:
        total_sleep = user.sleep_baseline + rng.normal(0, 0.7)
        total_sleep = float(np.clip(total_sleep, 3.5, 11.0))

    total_minutes = total_sleep * 60
    # Stage distribution: Deep 15-25%, REM 20-25%, Light 45-55%, Awake 5-10%
    deep_pct = rng.uniform(0.12, 0.25)
    rem_pct = rng.uniform(0.18, 0.26)
    awake_pct = rng.uniform(0.04, 0.10)
    light_pct = 1 - deep_pct - rem_pct - awake_pct

    spo2 = user.spo2_baseline + rng.normal(0, 0.5)
    if anomaly:
        spo2 -= rng.uniform(3, 7)

    sleep_efficiency = (1 - awake_pct) * 100

    return {
        "record_id": f"{user.user_id}_{date_.isoformat()}_sleep",
        "user_id": user.user_id,
        "date": date_.isoformat(),
        "total_sleep_hours": round(total_sleep, 2),
        "deep_sleep_minutes": round(total_minutes * deep_pct),
        "rem_sleep_minutes": round(total_minutes * rem_pct),
        "light_sleep_minutes": round(total_minutes * light_pct),
        "awake_minutes": round(total_minutes * awake_pct),
        "sleep_efficiency_pct": round(sleep_efficiency, 1),
        "spo2_avg_pct": round(float(np.clip(spo2, 85, 100)), 1),
        "resting_hr_bpm": round(
            user.resting_hr_baseline + rng.normal(0, 2.5) + training_load * 8, 1
        ),
        "is_anomaly": anomaly,
        "ingested_at": datetime.utcnow().isoformat(),
    }


def generate_activity_record(
    date_: date,
    user: UserProfile,
    training_load: float,
    day_index: int,
    anomaly: bool = False,
) -> dict:
    """Daily activity metrics + optional structured workout."""
    dow = date_.weekday()
    is_rest_day = (dow == 6) or (training_load < 0.2)

    non_rest = [(t, w) for t, w in zip(WORKOUT_TYPES, WORKOUT_WEIGHTS) if t != "Rest"]
    nr_types, nr_weights = zip(*non_rest)
    nr_weights_norm = [w / sum(nr_weights) for w in nr_weights]
    workout_type = "Rest" if is_rest_day else rng.choice(nr_types, p=nr_weights_norm)

    if workout_type == "Rest":
        workout_duration = 0
        avg_hr = user.resting_hr_baseline + rng.normal(5, 2)
        peak_hr = avg_hr + rng.uniform(10, 25)
        active_calories = rng.integers(50, 200)
    else:
        workout_duration = int(rng.integers(25, 90))
        # Target session-average HR as a fraction of max HR, by modality.
        # These are the averages themselves, not multipliers — a HIIT session
        # averages ~82% of max HR.
        intensity_map = {
            "Run": 0.72,
            "Cycling": 0.68,
            "HIIT": 0.82,
            "Strength": 0.65,
            "Yoga": 0.50,
            "Walk": 0.52,
            "Swim": 0.70,
        }
        # training_load modulates the session within a band around that target.
        # It must not scale intensity from zero: multiplying by the raw load
        # (~0.2-0.8) put every workout in zone 1-2 — HIIT averaged 40% of max
        # HR and a walk 25% — so zones 5 and 6 were empty across the dataset
        # and strain never exceeded 6.4 of a possible 21.
        load_modifier = 0.85 + 0.30 * training_load
        intensity = intensity_map.get(workout_type, 0.65) * load_modifier
        avg_hr = min(
            user.max_hr * 0.92,  # a session *average* cannot sit at peak
            user.max_hr * intensity * rng.uniform(0.95, 1.05),
        )
        peak_hr = min(user.max_hr * 0.98, avg_hr * rng.uniform(1.10, 1.25))
        active_calories = int(
            workout_duration * (avg_hr / 100) * 8 * rng.uniform(0.8, 1.2)
        )

    steps = (
        int(rng.integers(3000, 15000))
        if workout_type != "Rest"
        else int(rng.integers(1000, 5000))
    )
    total_calories = active_calories + int(rng.integers(1400, 2200))

    # Compute HR zone minutes (for strain calculation) before any sensor fault
    # is applied: the workout physically happened, the strap just misreported
    # it. Zone minutes therefore stay coherent while avg/peak HR do not.
    zones = _estimate_hr_zones(avg_hr, peak_hr, workout_duration, user.max_hr)

    if anomaly and workout_type != "Rest":
        # Two real optical-HR failure modes, injected deliberately so the
        # quality checks and the Silver quarantine table have something to
        # catch. Both land outside ACTIVITY_BOUNDS in silver_transformation.
        if rng.random() < 0.5:
            # Strap loses skin contact — HR reads implausibly low.
            avg_hr = rng.uniform(12, 28)
            peak_hr = avg_hr + rng.uniform(1, 5)
        else:
            # Motion artifact / cadence lock — peak spikes past any real HR.
            peak_hr = rng.uniform(230, 265)

    return {
        "record_id": f"{user.user_id}_{date_.isoformat()}_activity",
        "user_id": user.user_id,
        "date": date_.isoformat(),
        "workout_type": workout_type,
        "workout_duration_minutes": workout_duration,
        "steps": steps,
        "active_calories": active_calories,
        "total_calories": total_calories,
        "avg_hr_bpm": round(float(avg_hr), 1),
        "peak_hr_bpm": round(float(peak_hr), 1),
        "zone1_minutes": zones[1],
        "zone2_minutes": zones[2],
        "zone3_minutes": zones[3],
        "zone4_minutes": zones[4],
        "zone5_minutes": zones[5],
        "zone6_minutes": zones[6],
        "is_anomaly": anomaly,
        "ingested_at": datetime.utcnow().isoformat(),
    }


def _estimate_hr_zones(
    avg_hr: float, peak_hr: float, duration: int, max_hr: int
) -> dict:
    """Estimate time in each HR zone from average and peak HR."""
    thresholds = {
        1: max_hr * 0.50,
        2: max_hr * 0.60,
        3: max_hr * 0.70,
        4: max_hr * 0.80,
        5: max_hr * 0.90,
        6: max_hr * 0.95,
    }
    if duration == 0:
        return {i: 0 for i in range(1, 7)}

    # Distribute around the session average, with a short burst near peak.
    # Uses += rather than =, because the neighbour zones collide with avg_zone
    # at the clamp boundaries (zone 1 and zone 6) and would otherwise silently
    # overwrite the bulk allocation.
    avg_zone = next((z for z, t in reversed(thresholds.items()) if avg_hr >= t), 1)
    peak_zone = next((z for z, t in reversed(thresholds.items()) if peak_hr >= t), 1)

    distribution = {i: 0 for i in range(1, 7)}
    distribution[avg_zone] += int(duration * 0.45)
    distribution[max(1, avg_zone - 1)] += int(duration * 0.30)
    distribution[min(6, avg_zone + 1)] += int(duration * 0.15)
    # Time spent at the session's hardest effort. peak_hr was previously
    # computed and then ignored, so no session ever reached zone 5 or 6.
    distribution[peak_zone if peak_zone > avg_zone else avg_zone] += int(
        duration * 0.10
    )

    # Normalize to total duration
    total = sum(distribution.values())
    if total > 0:
        scale = duration / total
        distribution = {z: int(m * scale) for z, m in distribution.items()}
    return distribution


def generate_skin_temp(
    date_: date, user: UserProfile, day_index: int
) -> Optional[dict]:
    """
    Skin temperature delta (°C vs baseline).
    Schema evolution demo: only available from day 30 onward.
    """
    if day_index < 30:
        return None  # Simulates adding a new sensor mid-deployment

    delta = rng.normal(0, 0.3)
    # Slight fever on sick days (random ~2% chance)
    if rng.random() < 0.02:
        delta += rng.uniform(0.8, 2.0)

    return {
        "record_id": f"{user.user_id}_{date_.isoformat()}_temp",
        "user_id": user.user_id,
        "date": date_.isoformat(),
        "skin_temp_delta_c": round(float(delta), 2),
        "ingested_at": datetime.utcnow().isoformat(),
    }


def generate_dataset(n_users: int, n_days: int, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    users = [UserProfile.generate(f"user_{i:03d}") for i in range(1, n_users + 1)]
    start_date = date(2024, 1, 1)

    records = {"hrv": [], "sleep": [], "activity": [], "skin_temp": [], "users": []}

    for user in users:
        records["users"].append(
            {
                "user_id": user.user_id,
                "age": user.age,
                "fitness_level": user.fitness_level,
                "hrv_baseline": round(user.hrv_baseline, 2),
                "resting_hr_baseline": round(user.resting_hr_baseline, 2),
                "max_hr": user.max_hr,
            }
        )

        for day_idx in range(n_days):
            current_date = start_date + timedelta(days=day_idx)
            load = simulate_training_load(day_idx, user)
            anomaly = rng.random() < 0.02  # 2% anomaly rate

            records["hrv"].append(
                generate_hrv_reading(current_date, user, load, anomaly)
            )
            records["sleep"].append(
                generate_sleep_record(current_date, user, load, anomaly)
            )
            records["activity"].append(
                generate_activity_record(current_date, user, load, day_idx, anomaly)
            )

            temp = generate_skin_temp(current_date, user, day_idx)
            if temp:
                records["skin_temp"].append(temp)

        logger.info(
            f"Generated {n_days} days for {user.user_id} ({user.fitness_level})"
        )

    for name, data in records.items():
        if not data:
            continue
        path = output_dir / f"{name}.json"
        with open(path, "w") as f:
            json.dump(data, f, indent=2, cls=_NumpyEncoder)
        logger.success(f"Wrote {len(data)} records → {path}")

    # Also write as CSV for easy inspection
    for name in ["hrv", "sleep", "activity"]:
        df = pd.DataFrame(records[name])
        df.to_csv(output_dir / f"{name}.csv", index=False)

    logger.success(
        f"Dataset ready: {n_users} users × {n_days} days = "
        f"{n_users * n_days:,} records per table"
    )


@click.command()
@click.option("--users", default=10, help="Number of synthetic users")
@click.option("--days", default=90, help="Days of history to generate")
@click.option("--output", default="data/raw", help="Output directory")
def main(users: int, days: int, output: str) -> None:
    """Generate synthetic wearable data for the FitLake pipeline."""
    generate_dataset(users, days, Path(output))


if __name__ == "__main__":
    main()
