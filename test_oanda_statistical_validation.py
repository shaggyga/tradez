import unittest
from datetime import datetime, timedelta, timezone

from trad.oanda_statistical_validation import (
    benjamini_hochberg,
    independent_time_block_means,
    multiple_testing_metrics,
    purged_before_boundary,
)


class StatisticalValidationTests(unittest.TestCase):
    def test_overlapping_forecasts_collapse_to_maturity_blocks(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        records = [
            {
                "entry_time": (base + timedelta(seconds=offset)).isoformat(),
                "endpoint_pips": value,
            }
            for offset, value in ((0, 1.0), (60, 3.0), (300, -2.0))
        ]
        self.assertEqual(
            independent_time_block_means(records, horizon_sec=300),
            [2.0, -2.0],
        )

    def test_purge_removes_labels_that_cross_split(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        records = [
            {"entry_time": (base + timedelta(seconds=offset)).isoformat()}
            for offset in (0, 200, 400)
        ]
        kept = purged_before_boundary(records, (base + timedelta(seconds=600)).timestamp(), 300)
        self.assertEqual(len(kept), 2)

    def test_more_trials_make_selection_bound_more_conservative(self):
        values = [1.0, 0.5, 1.2, -0.1, 0.8, 0.4, 1.1, 0.3] * 8
        one = multiple_testing_metrics(values, trial_count=1)
        many = multiple_testing_metrics(values, trial_count=100)
        self.assertLess(
            many["selection_adjusted_lower_mean_pips"],
            one["selection_adjusted_lower_mean_pips"],
        )
        self.assertLessEqual(
            many["deflated_sharpe_probability"],
            one["deflated_sharpe_probability"],
        )

    def test_benjamini_hochberg_preserves_order_and_monotonic_adjustment(self):
        adjusted = benjamini_hochberg([0.04, 0.001, 0.02])
        self.assertEqual(len(adjusted), 3)
        self.assertLess(adjusted[1], adjusted[2])
        self.assertLessEqual(adjusted[2], adjusted[0])


if __name__ == "__main__":
    unittest.main()
