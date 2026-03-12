"""Unit tests for the Strain Score calculator."""

import pytest
import pandas as pd
import numpy as np

from pipeline.aggregation.strain_calculator import (
    calculate_strain_score,
    compute_strain_scores,
    ZONE_WEIGHTS,
    MAX_STRAIN,
)


class TestCalculateStrainScore:
    def test_rest_day_near_zero(self):
        zones = {i: 0 for i in range(1, 7)}
        result = calculate_strain_score(zones, peak_hr=60.0, max_hr=190)
        assert result.strain_score < 3

    def test_max_effort_near_21(self):
        # 2h in Zone 6 should produce very high strain
        zones = {1: 0, 2: 0, 3: 0, 4: 0, 5: 30, 6: 90}
        result = calculate_strain_score(zones, peak_hr=188.0, max_hr=190)
        assert result.strain_score > 15

    def test_moderate_workout(self):
        zones = {1: 5, 2: 20, 3: 30, 4: 15, 5: 5, 6: 0}
        result = calculate_strain_score(zones, peak_hr=155.0, max_hr=190)
        assert 5 <= result.strain_score <= 16

    def test_score_bounded_0_to_21(self):
        for z6 in [0, 50, 200, 500]:
            zones = {i: 10 for i in range(1, 6)}
            zones[6] = z6
            result = calculate_strain_score(zones, peak_hr=185.0, max_hr=190)
            assert 0 <= result.strain_score <= MAX_STRAIN

    def test_higher_zones_produce_more_strain(self):
        low_intensity = {1: 60, 2: 20, 3: 10, 4: 5, 5: 0, 6: 0}
        high_intensity = {1: 5, 2: 10, 3: 20, 4: 30, 5: 20, 6: 10}
        low = calculate_strain_score(low_intensity, 130.0, 190)
        high = calculate_strain_score(high_intensity, 175.0, 190)
        assert high.strain_score > low.strain_score

    def test_category_assignment(self):
        cases = [
            ({i: 0 for i in range(1, 7)}, "Recovery"),
            ({1: 60, 2: 30, 3: 10, 4: 0, 5: 0, 6: 0}, "Recovery"),
        ]
        for zones, _ in cases:
            result = calculate_strain_score(zones, 100.0, 190)
            assert result.strain_category in [
                "Recovery", "Light", "Moderate", "Hard", "All Out"
            ]


class TestComputeStrainScores:
    @pytest.fixture
    def activity_df(self) -> pd.DataFrame:
        np.random.seed(42)
        records = []
        for user in ["user_001", "user_002"]:
            for day in range(10):
                records.append({
                    "record_id": f"{user}_{day}",
                    "user_id": user,
                    "date": f"2024-01-{day+1:02d}",
                    "workout_type": "Run",
                    "workout_duration_minutes": 45,
                    "steps": 8000,
                    "active_calories": 400,
                    "total_calories": 2100,
                    "avg_hr_bpm": 145.0,
                    "peak_hr_bpm": 175.0,
                    **{f"zone{z}_minutes": np.random.randint(0, 20) for z in range(1, 7)},
                })
        return pd.DataFrame(records)

    @pytest.fixture
    def users_df(self) -> pd.DataFrame:
        return pd.DataFrame({
            "user_id": ["user_001", "user_002"],
            "max_hr": [185, 190],
        })

    def test_returns_dataframe(self, activity_df, users_df):
        result = compute_strain_scores(activity_df, users_df)
        assert isinstance(result, pd.DataFrame)

    def test_correct_row_count(self, activity_df, users_df):
        result = compute_strain_scores(activity_df, users_df)
        assert len(result) == len(activity_df)

    def test_all_scores_bounded(self, activity_df, users_df):
        result = compute_strain_scores(activity_df, users_df)
        assert result["strain_score"].between(0, MAX_STRAIN).all()

    def test_all_users_present(self, activity_df, users_df):
        result = compute_strain_scores(activity_df, users_df)
        assert "user_001" in result["user_id"].values
        assert "user_002" in result["user_id"].values
