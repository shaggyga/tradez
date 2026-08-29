from __future__ import annotations

import unittest

import numpy as np

try:
    from oanda_second_forecast import FEATURE_NAMES
    from oanda_short_horizon_model_sweep import excursion_metrics, interaction_matrix
except ModuleNotFoundError:
    from trad.oanda_second_forecast import FEATURE_NAMES
    from trad.oanda_short_horizon_model_sweep import excursion_metrics, interaction_matrix


class ShortHorizonFeatureTests(unittest.TestCase):
    def test_interaction_matrix_is_finite_and_keeps_base_features(self):
        base = np.zeros((3, len(FEATURE_NAMES)), dtype=float)
        base[:, 5:8] = 1.0
        augmented = interaction_matrix(base)

        self.assertEqual(augmented.shape, (3, len(FEATURE_NAMES) + 16))
        np.testing.assert_array_equal(augmented[:, : len(FEATURE_NAMES)], base)
        self.assertTrue(np.all(np.isfinite(augmented)))

    def test_excursion_metrics_detects_profit_given_back_before_horizon(self):
        metrics = excursion_metrics(
            prediction=np.asarray([2.0]),
            threshold=1.0,
            origins=np.asarray([0]),
            futures=np.asarray([3]),
            entry_bid=np.asarray([1.0000]),
            entry_ask=np.asarray([1.0001]),
            future_bid=np.asarray([0.9999]),
            future_ask=np.asarray([1.0000]),
            bid_high=np.asarray([1.0000, 1.0003, 1.0002, 0.9999]),
            bid_low=np.asarray([1.0000, 1.0001, 1.0000, 0.9998]),
            ask_high=np.asarray([1.0001, 1.0004, 1.0003, 1.0000]),
            ask_low=np.asarray([1.0001, 1.0002, 1.0001, 0.9999]),
            multiplier=10_000.0,
        )

        self.assertEqual(metrics["n"], 1)
        self.assertAlmostEqual(metrics["avg_mfe_pips"], 2.0)
        self.assertEqual(metrics["positive_mfe_rate"], 100.0)
        self.assertEqual(metrics["ended_nonpositive_after_positive_rate"], 100.0)


if __name__ == "__main__":
    unittest.main()
