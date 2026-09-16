"""
Physiological validation bounds — one definition, imported by every layer.

A reading outside these ranges is not a signal, it is a broken sensor: an
optical HR strap that has lost skin contact reports 15 bpm, and a cadence-lock
artifact reports 260. The Silver job quarantines such records rather than
dropping them, and the dashboard's data-quality panel reports against these
same numbers.

Pure Python by design — no pandas, no pyspark — so the Spark job, the Pandas
quality gate and the hosted dashboard can all import it.

These bounds previously lived as literals inside
spark/jobs/silver_transformation.py, with a second set in
quality/run_checks.py that had already drifted: the pre-Silver gate accepted
sleep efficiency down to 0% and resting HR up to 140 bpm, while Silver
quarantined anything below 20% or above 130. The gate's rule lists still carry
their own thresholds for the not_null and unique rules; reconciling those
remains open.
"""

# (min, max) inclusive — values strictly outside the range are quarantined.

HRV_BOUNDS = {
    "hrv_rmssd_ms": (5, 250),
    "hrv_sdnn_ms": (10, 400),
    "respiratory_rate_brpm": (8, 30),
}

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

# Human-readable reason per column, shown beside a rejected record. Keyed by
# which side of the bound was breached, because the same column fails for
# opposite physical reasons — a strap reporting 18 bpm has not "exceeded any
# real maximum", it has stopped touching skin.
BOUND_REASONS = {
    "hrv_rmssd_ms": {
        "low": "rMSSD too low to come from a beating heart",
        "high": "rMSSD above any plausible autonomic range",
    },
    "hrv_sdnn_ms": {
        "low": "SDNN too low to come from a beating heart",
        "high": "SDNN above any plausible autonomic range",
    },
    "respiratory_rate_brpm": {
        "low": "respiratory rate below survivable",
        "high": "respiratory rate above survivable",
    },
    "total_sleep_hours": {
        "low": "too little sleep to be a recorded night",
        "high": "more sleep than fits in one night",
    },
    "sleep_efficiency_pct": {
        "low": "efficiency too low to represent real sleep",
        "high": "efficiency above 100%",
    },
    "spo2_avg_pct": {
        "low": "SpO₂ below what a sleeping adult sustains",
        "high": "SpO₂ above 100%",
    },
    "resting_hr_bpm": {
        "low": "resting HR below human range",
        "high": "resting HR above human range",
    },
    "avg_hr_bpm": {
        "low": "session-average HR implies lost skin contact",
        "high": "session-average HR above any real maximum",
    },
    "peak_hr_bpm": {
        "low": "peak HR implies lost skin contact",
        "high": "peak HR above any real maximum — cadence-lock artifact",
    },
    "steps": {
        "low": "negative step count",
        "high": "step count outside a physical day",
    },
    "active_calories": {
        "low": "negative active burn",
        "high": "active burn outside a physical day",
    },
}
