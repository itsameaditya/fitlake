"""
Unit tests for the shared physiological bounds.

The bounds are imported by the Silver Spark job, the quality gate and the
dashboard's quarantine panel. They lived as separate copies before and drifted
apart, so these tests guard the properties each consumer relies on.
"""

from quality.bounds import (
    ACTIVITY_BOUNDS,
    BOUND_REASONS,
    HRV_BOUNDS,
    SLEEP_BOUNDS,
)

ALL_BOUNDS = {**HRV_BOUNDS, **SLEEP_BOUNDS, **ACTIVITY_BOUNDS}


class TestBounds:
    def test_no_column_shared_between_tables(self):
        # A merged view is only safe while the three tables name distinct
        # columns; otherwise one silently overrides another's range.
        assert len(ALL_BOUNDS) == len(HRV_BOUNDS) + len(SLEEP_BOUNDS) + len(
            ACTIVITY_BOUNDS
        )

    def test_ranges_are_ordered(self):
        for column, (lo, hi) in ALL_BOUNDS.items():
            assert lo < hi, f"{column} has min >= max"

    def test_every_bounded_column_explains_both_directions(self):
        # The dashboard looks up BOUND_REASONS[column][side] for a rejected
        # record; a missing side would render a blank explanation.
        for column in ALL_BOUNDS:
            assert column in BOUND_REASONS, f"{column} has no reason text"
            assert set(BOUND_REASONS[column]) == {
                "low",
                "high",
            }, f"{column} is missing a direction"

    def test_no_reason_without_a_bound(self):
        assert set(BOUND_REASONS) == set(ALL_BOUNDS)

    def test_reasons_are_non_empty(self):
        for column, sides in BOUND_REASONS.items():
            for side, text in sides.items():
                assert text.strip(), f"{column}/{side} is blank"
