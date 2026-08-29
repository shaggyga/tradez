import unittest

from trad.oanda_pair_family_matrix import build_pair_family_matrix
from trad.oanda_practice_live_dashboard import query_pair_family_matrix


class PairFamilyMatrixTests(unittest.TestCase):
    def test_merges_strategy_and_equation_cells_without_promoting_them(self):
        exit_fit = {
            "generated_at": "2026-07-20T00:00:00+00:00",
            "prediction_quality_by_horizon": {
                "3600": {
                    "pair_family_timeframes": {
                        "USD_CAD::momentum::M5": {
                            "eligible": False,
                            "negative_evidence": False,
                            "blocked_by": ["minimum_total_samples"],
                            "sample_count": 12,
                            "minimum_total_samples": 30,
                            "evidence_strength": 0.4,
                            "independent_holdout_blocks": 2,
                            "split": "oldest 70% / newest 30%",
                            "holdout": {
                                "n": 4,
                                "avg_pips": 1.25,
                                "lower_confidence_pips": -0.5,
                                "win_rate": 75.0,
                                "median_mfe_pips": 2.2,
                                "median_mae_pips": 0.8,
                                "median_entry_spread_pips": 1.4,
                            },
                            "overall": {"n": 12},
                        }
                    }
                }
            },
        }
        timeframe = {
            "generated_utc": "2026-07-20T00:01:00+00:00",
            "pair_surfaces": {
                "EUR_USD|timeframe_equation_matrix.m5|3600": {
                    "instrument": "EUR_USD",
                    "lane_id": "timeframe_equation_matrix.m5",
                    "horizon_sec": 3600,
                    "n": 63,
                    "oos_n": 23,
                    "independent_blocks": 11,
                    "calibrated_accuracy": 0.61,
                    "calibrated_win_rate": 0.39,
                    "calibrated_average_net_pips": -0.17,
                    "lower_confidence_net_pips": -2.05,
                    "validation_ready": False,
                }
            },
        }

        payload = build_pair_family_matrix(exit_fit, timeframe)

        self.assertEqual(payload["counts"]["observed_cells"], 2)
        self.assertEqual(payload["counts"]["eligible_cells"], 0)
        by_key = {row["cell_key"]: row for row in payload["rows"]}
        strategy = by_key["USD_CAD|momentum|M5|3600"]
        equation = by_key["EUR_USD|timeframe_equation_matrix|M5|3600"]
        self.assertEqual(strategy["status"], "collecting")
        self.assertEqual(strategy["win_rate_pct"], 75.0)
        self.assertEqual(equation["direction_accuracy_pct"], 61.0)
        self.assertEqual(equation["win_rate_pct"], 39.0)
        self.assertIn("minimum_surface_evidence", equation["blocked_by"])

        page = query_pair_family_matrix(
            payload,
            {
                "pair": ["USD_CAD"],
                "sort": ["average_net_pips"],
                "direction": ["desc"],
                "limit": ["25"],
            },
        )
        self.assertEqual(page["matched_count"], 1)
        self.assertEqual(page["rows"][0]["family"], "momentum")
        self.assertEqual(page["sort"], "average_net_pips")


if __name__ == "__main__":
    unittest.main()
