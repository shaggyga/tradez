import unittest

import numpy as np
import pandas as pd

from oanda_all68_pattern_research import (
    bh_adjust,
    derive_thresholds,
    direction_from_pattern,
    independent_transition_indices,
)


class All68PatternResearchTests(unittest.TestCase):
    def sample(self) -> pd.DataFrame:
        index = pd.date_range("2024-01-01", periods=20, freq="15min", tz="UTC")
        values = np.linspace(-1.0, 1.0, len(index))
        return pd.DataFrame(
            {
                "momentum_5_atr": values,
                "momentum_15_atr": values,
                "momentum_60_atr": values,
                "acceleration_15_atr": values,
                "sma7_minus_8_atr": values,
                "sma30_slope_5_atr": values,
                "rsi14_centered": values,
                "atr15_to_atr240": np.linspace(0.5, 1.5, len(index)),
                "compression_30": np.linspace(0.2, 2.0, len(index)),
                "range_position_60_centered": values,
                "range_position_240_centered": values,
                "spread_ratio_60": np.linspace(0.5, 1.5, len(index)),
                "volume_z_30": np.linspace(-1.0, 2.0, len(index)),
                "strength_gap_rank_15": values,
                "strength_gap_rank_60": values,
            },
            index=index,
        )

    def test_thresholds_are_derived_from_passed_discovery_only(self):
        discovery = self.sample()
        thresholds = derive_thresholds(discovery)
        self.assertAlmostEqual(
            thresholds["abs_momentum_15_atr_q90"],
            float(discovery["momentum_15_atr"].abs().quantile(0.90)),
        )
        self.assertAlmostEqual(
            thresholds["compression_30_q20"],
            float(discovery["compression_30"].quantile(0.20)),
        )

    def test_symmetric_continuation_emits_long_and_short(self):
        frame = self.sample()
        directions = direction_from_pattern(
            frame, "fast_momentum_continuation", derive_thresholds(frame)
        )
        self.assertIn(-1, directions)
        self.assertIn(1, directions)

    def test_missing_direction_source_never_becomes_a_signal(self):
        frame = self.sample()
        thresholds = derive_thresholds(frame)
        frame.loc[frame.index[-1], "momentum_15_atr"] = np.nan
        directions = direction_from_pattern(frame, "fast_momentum_continuation", thresholds)
        self.assertEqual(int(directions[-1]), 0)

    def test_transition_indices_apply_horizon_cooldown(self):
        directions = np.asarray([0, 1, 1, 0, 1, 0, -1, -1, 1], dtype=np.int8)
        times = pd.date_range("2025-01-01", periods=len(directions), freq="15min", tz="UTC").asi8
        selected = independent_transition_indices(directions, times, 60)
        self.assertEqual(selected.tolist(), [1, 6])

    def test_bh_adjustment_is_monotone_by_rank(self):
        adjusted = bh_adjust([0.001, 0.02, 0.03, float("nan")])
        self.assertLessEqual(adjusted[0], adjusted[1])
        self.assertLessEqual(adjusted[1], adjusted[2])
        self.assertTrue(np.isnan(adjusted[3]))


if __name__ == "__main__":
    unittest.main()
