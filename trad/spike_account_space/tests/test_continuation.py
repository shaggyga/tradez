from __future__ import annotations

import unittest

import pandas as pd

from trad.spike_account_space.continuation import (
    add_continuation_labels,
    continuation_metrics,
)


class ContinuationResearchTests(unittest.TestCase):
    def test_labels_are_relative_to_causal_trend_proxy(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "momentum_15_atr": 0.2,
                    "momentum_60_atr": 1.0,
                    "forward_long_net_pips": 5.0,
                    "forward_short_net_pips": -7.0,
                },
                {
                    "momentum_15_atr": -0.2,
                    "momentum_60_atr": -1.0,
                    "forward_long_net_pips": 4.0,
                    "forward_short_net_pips": -1.0,
                },
                {
                    "momentum_15_atr": 0.2,
                    "momentum_60_atr": 1.0,
                    "forward_long_net_pips": -0.1,
                    "forward_short_net_pips": -0.2,
                },
            ]
        )
        labeled = add_continuation_labels(frame, minimum_net_pips=0.0)
        self.assertEqual(labeled.loc[0, "continuation_label"], "continuation")
        self.assertEqual(labeled.loc[1, "continuation_label"], "reversal")
        self.assertEqual(labeled.loc[2, "continuation_label"], "no_trade")
        self.assertEqual(labeled.loc[1, "trend_proxy_side"], "short")

    def test_metrics_compare_model_to_trend_and_oracle(self) -> None:
        frame = pd.DataFrame(
            {
                "model_net_pips": [3.0, -1.0, 0.0],
                "model_takes_trade": [True, True, False],
                "continuation_net_pips": [2.0, -2.0, 1.0],
                "best_direction_net_pips": [3.0, 1.0, -1.0],
                "continuation_label": ["continuation", "reversal", "no_trade"],
                "continuation_prediction": ["continuation", "continuation", "no_trade"],
            }
        )
        metrics = continuation_metrics(frame, fold="outer_x")
        self.assertEqual(metrics["gated_rows"], 3)
        self.assertEqual(metrics["model_trades"], 2)
        self.assertAlmostEqual(metrics["model_net_pips"], 2.0)
        self.assertAlmostEqual(metrics["trend_baseline_net_pips"], 1.0)
        self.assertAlmostEqual(metrics["oracle_best_direction_net_pips"], 4.0)


if __name__ == "__main__":
    unittest.main()
