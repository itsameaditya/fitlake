"""
Unit tests for the Recovery Score engine.

Tests cover:
  - Edge cases (perfect recovery, worst case)
  - Component scoring functions
  - Vectorized computation on realistic data
  - Score state boundaries (Red/Yellow/Green)
"""

import pytest
import numpy as np
import pandas as pd

from pipeline.aggregation.recovery_score import (
    DailyMetrics,
    RecoveryScore,
    calculate_recovery_score,
    compute_recovery_scores,
    _hrv_score,
    _rhr_score,
    _sleep_performance_score,
    _spo2_score,
    RED_THRESHOLD,
    YELLOW_THRESHOLD,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def healthy_metrics() -> DailyMetrics:
    """A healthy day: good HRV, low RHR, great sleep, high SpO2."""
    return DailyMetrics(
        user_id="user_001",
        date="2024-03-01",
        hrv_rmssd=75.0,       # Above baseline
        resting_hr=48.0,      # Low RHR = good
        total_sleep_hours=8.5,
        sleep_efficiency_pct=93.0,
        rem_sleep_minutes=110.0,
        deep_sleep_minutes=95.0,
        spo2_avg_pct=98.5,
    )


@pytest.fixture
def poor_metrics() -> DailyMetrics:
    """A bad recovery day: low HRV, elevated RHR, poor sleep."""
    return DailyMetrics(
        user_id="user_001",
        date="2024-03-02",
        hrv_rmssd=20.0,       # Well below baseline
        resting_hr=72.0,      # Elevated RHR
        total_sleep_hours=4.5,
        sleep_efficiency_pct=65.0,
        rem_sleep_minutes=40.0,
        deep_sleep_minutes=25.0,
        spo2_avg_pct=94.5,
    )


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """30 days of data for 3 users."""
    np.random.seed(42)
    records = []
    for user_id in ["user_001", "user_002", "user_003"]:
        for day in range(30):
            date = pd.Timestamp("2024-01-01") + pd.Timedelta(days=day)
            records.append({
                "user_id": user_id,
                "date": date.date().isoformat(),
                "hrv_rmssd_ms": np.random.normal(60, 12),
                "hrv_sdnn_ms": np.random.normal(110, 20),
                "resting_hr_bpm": np.random.normal(58, 5),
                "total_sleep_hours": np.random.normal(7.5, 0.8),
                "sleep_efficiency_pct": np.random.normal(87, 5),
                "rem_sleep_minutes": np.random.normal(105, 15),
                "deep_sleep_minutes": np.random.normal(85, 12),
                "spo2_avg_pct": np.random.normal(97.5, 0.8),
            })
    return pd.DataFrame(records)


# ── Component Tests ───────────────────────────────────────────────────────────

class TestHRVScore:
    def test_at_baseline_scores_near_50(self):
        score = _hrv_score(60, 60, 10)
        assert 45 <= score <= 55

    def test_above_baseline_scores_high(self):
        score = _hrv_score(90, 60, 10)  # 3 SDs above baseline
        assert score > 65

    def test_below_baseline_scores_low(self):
        score = _hrv_score(30, 60, 10)  # 3 SDs below baseline
        assert score < 35

    def test_zero_std_falls_back_to_10pct(self):
        # Should not raise ZeroDivisionError
        score = _hrv_score(60, 60, 0)
        assert 0 <= score <= 100

    def test_score_bounded_0_to_100(self):
        for hrv, baseline, std in [(5, 80, 10), (200, 40, 8), (60, 60, 5)]:
            assert 0 <= _hrv_score(hrv, baseline, std) <= 100


class TestRHRScore:
    def test_at_baseline_scores_near_75(self):
        score = _rhr_score(60, 60)
        assert score == pytest.approx(75.0)

    def test_below_baseline_scores_high(self):
        score = _rhr_score(55, 65)  # 10 bpm below baseline
        assert score > 75

    def test_above_baseline_scores_low(self):
        score = _rhr_score(75, 60)  # 15 bpm above baseline
        assert score < 75

    def test_score_bounded(self):
        assert 0 <= _rhr_score(30, 90) <= 100
        assert 0 <= _rhr_score(130, 40) <= 100


class TestSleepScore:
    @pytest.mark.parametrize("hours,expected_min", [
        (8.0, 70),   # Ideal sleep
        (4.0, 0),    # Very short
        (6.0, 40),   # Just below ideal
    ])
    def test_duration_scoring(self, hours, expected_min):
        score = _sleep_performance_score(hours, 90, 100, 90)
        assert score >= expected_min

    def test_perfect_sleep_scores_high(self):
        score = _sleep_performance_score(8.0, 95, 110, 90)
        assert score >= 80

    def test_terrible_sleep_scores_low(self):
        score = _sleep_performance_score(3.5, 60, 20, 15)
        assert score < 40

    def test_score_bounded_0_to_100(self):
        score = _sleep_performance_score(8, 90, 100, 80)
        assert 0 <= score <= 100


class TestSpO2Score:
    @pytest.mark.parametrize("spo2,expected_min", [
        (98, 95),
        (95, 75),
        (90, 15),
        (85, 0),
    ])
    def test_spo2_thresholds(self, spo2, expected_min):
        assert _spo2_score(spo2) >= expected_min

    def test_perfect_spo2(self):
        assert _spo2_score(99) == 100.0

    def test_low_spo2_penalized(self):
        assert _spo2_score(88) < 20


# ── Integration Tests ─────────────────────────────────────────────────────────

class TestCalculateRecoveryScore:
    def test_healthy_day_is_green(self, healthy_metrics):
        result = calculate_recovery_score(healthy_metrics, 60.0, 8.0, 52.0)
        assert result.recovery_state == "Green"
        assert result.recovery_score >= YELLOW_THRESHOLD

    def test_poor_day_is_red_or_yellow(self, poor_metrics):
        result = calculate_recovery_score(poor_metrics, 65.0, 8.0, 55.0)
        assert result.recovery_state in ("Red", "Yellow")

    def test_score_is_integer(self, healthy_metrics):
        result = calculate_recovery_score(healthy_metrics, 60.0, 8.0, 52.0)
        assert isinstance(result.recovery_score, int)

    def test_score_bounded_0_to_100(self, healthy_metrics, poor_metrics):
        for metrics in [healthy_metrics, poor_metrics]:
            result = calculate_recovery_score(metrics, 60.0, 8.0, 55.0)
            assert 0 <= result.recovery_score <= 100

    def test_all_components_populated(self, healthy_metrics):
        result = calculate_recovery_score(healthy_metrics, 60.0, 8.0, 52.0)
        assert result.hrv_component >= 0
        assert result.rhr_component >= 0
        assert result.sleep_component >= 0
        assert result.spo2_component >= 0

    def test_result_user_and_date(self, healthy_metrics):
        result = calculate_recovery_score(healthy_metrics, 60.0, 8.0, 52.0)
        assert result.user_id == "user_001"
        assert result.date == "2024-03-01"


class TestComputeRecoveryScores:
    def test_returns_dataframe(self, sample_dataframe):
        result = compute_recovery_scores(sample_dataframe)
        assert isinstance(result, pd.DataFrame)

    def test_correct_row_count(self, sample_dataframe):
        result = compute_recovery_scores(sample_dataframe)
        assert len(result) == len(sample_dataframe)

    def test_all_states_valid(self, sample_dataframe):
        result = compute_recovery_scores(sample_dataframe)
        valid_states = {"Red", "Yellow", "Green"}
        assert set(result["recovery_state"].unique()).issubset(valid_states)

    def test_scores_within_bounds(self, sample_dataframe):
        result = compute_recovery_scores(sample_dataframe)
        assert result["recovery_score"].between(0, 100).all()

    def test_all_users_present(self, sample_dataframe):
        result = compute_recovery_scores(sample_dataframe)
        assert set(result["user_id"].unique()) == {"user_001", "user_002", "user_003"}

    def test_state_thresholds_consistent(self, sample_dataframe):
        result = compute_recovery_scores(sample_dataframe)
        assert (result[result["recovery_state"] == "Red"]["recovery_score"] < RED_THRESHOLD).all()
        assert (result[result["recovery_state"] == "Green"]["recovery_score"] >= YELLOW_THRESHOLD).all()

    def test_baseline_users_flagged(self, sample_dataframe):
        # Users with < MIN_BASELINE_DAYS should have needs_more_baseline=True initially
        short_df = sample_dataframe[sample_dataframe["user_id"] == "user_001"].head(2)
        result = compute_recovery_scores(short_df)
        # First records without enough baseline should be flagged
        assert "needs_more_baseline" in result.columns
