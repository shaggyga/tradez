from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from fresh_m1_intrahour.src.forecast_simplification import (
    signal_slice_metrics,
    wilson_lower_bound,
)
from fresh_m1_intrahour.src.unified_signal_benchmark import (
    chronological_indices,
    correlation_groups,
    nested_validation_indices,
    rotation_metrics,
)


class UnifiedSignalBenchmarkTests(unittest.TestCase):
    def test_outer_and_nested_splits_purge_sixty_minute_labels(self) -> None:
        times = pd.Series(
            pd.date_range("2025-01-01", periods=300, freq="195min", tz="UTC")
        )
        train, validation, final, outer = chronological_indices(
            times, 0.2, 0.2, 60
        )
        screen, selection, inner = nested_validation_indices(
            times, validation, 60
        )
        self.assertTrue(outer["train_purge_passed"])
        self.assertTrue(outer["validation_purge_passed"])
        self.assertTrue(inner["purge_passed"])
        self.assertLess(times.iloc[train].max(), times.iloc[validation].min())
        self.assertLess(times.iloc[screen].max(), times.iloc[selection].min())
        self.assertGreater(len(final), 0)

    def test_correlation_groups_merge_sign_and_scale_duplicates(self) -> None:
        base = np.linspace(-2.0, 2.0, 200)
        values = np.column_stack(
            [base, base * 2.0, -base, np.sin(base * 3.0)]
        )
        groups, _ = correlation_groups(values, threshold=0.995)
        sizes = sorted((len(group) for group in groups), reverse=True)
        self.assertEqual(sizes, [3, 1])

    def test_rotation_metrics_respect_timestamp_quota_and_no_trade(self) -> None:
        times = np.asarray([1, 1, 1, 2, 2, 2])
        prediction = np.asarray([3.0, 2.0, -1.0, 0.1, -0.2, 0.3])
        long_net = np.asarray([4.0, 2.0, 1.0, 1.0, 1.0, 3.0])
        short_net = np.asarray([-4.0, -2.0, 2.0, -1.0, 2.0, -3.0])
        result = rotation_metrics(
            times,
            prediction,
            long_net,
            short_net,
            threshold=0.25,
        )
        self.assertEqual(result["top1"]["trades"], 2)
        self.assertEqual(result["top3"]["trades"], 4)
        self.assertEqual(result["top1"]["traded_timestamps"], 2)
        self.assertAlmostEqual(result["top1"]["total_net_pips"], 7.0)

    def test_signal_slice_metrics_separate_direction_from_cost(self) -> None:
        result = signal_slice_metrics(
            actual=np.asarray([2.0, -1.0, 3.0, -4.0]),
            prediction=np.asarray([1.0, -1.0, -1.0, 1.0]),
            long_net=np.asarray([1.0, -2.0, 2.0, -5.0]),
            short_net=np.asarray([-3.0, 0.5, -4.0, 3.0]),
        )
        self.assertEqual(result["signals"], 4)
        self.assertEqual(result["direction_correct"], 2)
        self.assertAlmostEqual(result["direction_accuracy"], 0.5)
        self.assertAlmostEqual(result["mean_executable_net_pips"], -1.875)

    def test_wilson_lower_bound_rewards_more_evidence(self) -> None:
        self.assertGreater(
            wilson_lower_bound(600, 1000),
            wilson_lower_bound(6, 10),
        )


if __name__ == "__main__":
    unittest.main()
