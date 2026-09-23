import json
import math
import unittest

import pandas as pd

try:
    import oanda_shared_timeframe_horizon_panel as shared_panel
    from oanda_second_microstructure_panel import (
        build_panel,
        has_required_features,
    )
except ModuleNotFoundError:
    from trad import oanda_shared_timeframe_horizon_panel as shared_panel
    from trad.oanda_second_microstructure_panel import (
        build_panel,
        has_required_features,
    )


class SecondMicrostructurePanelTests(unittest.TestCase):
    def test_builds_two_cost_aware_sides_and_preserves_quote_flow(self):
        records = pd.DataFrame(
            [
                {
                    "origin_epoch": 1_800_000_000,
                    "target_epoch": 1_800_000_300,
                    "instrument": "EUR_USD",
                    "horizon_sec": 300,
                    "model_id": "ridge",
                    "model_family": "ridge_return",
                    "input_timeframe": "S1",
                    "training_timeframe": "S5",
                    "predicted_signed_pips": 1.5,
                    "entry_mid": 1.12345,
                    "spread_pips": 1.0,
                    "pip": 0.0001,
                    "actual_signed_pips": 3.0,
                    "chosen_theoretical_pips": 2.0,
                    "outcome_delay_sec": 0.2,
                    "features_json": json.dumps(
                        {
                            "return_5_pips": 0.7,
                            "return_30_pips": 1.1,
                            "return_60_pips": 1.3,
                            "volatility_60_pips": 0.8,
                            "spread_pips": 1.0,
                            "spread_ratio_60": 1.25,
                            "activity_30": 18,
                            "activity_ratio_30_60": 1.4,
                            "quote_flow_imbalance_5": 0.35,
                        }
                    ),
                }
            ]
        )

        panel, summary = build_panel(records)

        self.assertEqual(summary["events"], 1)
        self.assertEqual(len(panel), 2)
        long_row = panel[panel["direction"] == "LONG"].iloc[0]
        short_row = panel[panel["direction"] == "SHORT"].iloc[0]
        self.assertAlmostEqual(long_row["inferred_round_trip_cost_pips"], 1.0)
        self.assertAlmostEqual(long_row["realized_net_pips"], 2.0)
        self.assertAlmostEqual(short_row["realized_net_pips"], -4.0)
        self.assertAlmostEqual(long_row["quote_flow_imbalance_5"], 0.35)
        self.assertEqual(long_row["input_timeframe"], "S1")
        self.assertEqual(shared_panel.validate_panel(panel)["events"], 1)

    def test_rejects_impossible_negative_inferred_cost(self):
        records = pd.DataFrame(
            [
                {
                    "origin_epoch": 1_800_000_000,
                    "instrument": "EUR_USD",
                    "horizon_sec": 60,
                    "model_id": "ridge",
                    "predicted_signed_pips": 1.0,
                    "actual_signed_pips": 1.0,
                    "chosen_theoretical_pips": 2.0,
                    "spread_pips": 1.0,
                    "features_json": "{}",
                }
            ]
        )

        panel, summary = build_panel(records)

        self.assertTrue(panel.empty)
        self.assertEqual(summary["invalid_cost_rows"], 1)

    def test_optional_microstructure_is_part_of_model_space(self):
        self.assertIn(
            "quote_flow_imbalance_30",
            shared_panel.MODEL_NUMERIC_FEATURES,
        )
        self.assertIn(
            "activity_30",
            shared_panel.MODEL_NUMERIC_FEATURES,
        )
        self.assertTrue(math.isfinite(1.0))

    def test_required_feature_filter_rejects_legacy_snapshot(self):
        self.assertTrue(
            has_required_features(
                '{"activity_5": 3.0, "tick_imbalance_5": -0.5}',
                ("activity_5", "tick_imbalance_5"),
            )
        )
        self.assertFalse(
            has_required_features(
                '{"activity_5": 3.0}',
                ("activity_5", "tick_imbalance_5"),
            )
        )

    def test_duplicate_event_uses_smallest_outcome_delay(self):
        common = {
            "origin_epoch": 1_800_000_000,
            "instrument": "EUR_USD",
            "horizon_sec": 60,
            "model_id": "ridge",
            "predicted_signed_pips": 1.0,
            "spread_pips": 1.0,
            "features_json": "{}",
        }
        records = pd.DataFrame(
            [
                {
                    **common,
                    "actual_signed_pips": -2.0,
                    "chosen_theoretical_pips": -3.0,
                    "outcome_delay_sec": 0.8,
                },
                {
                    **common,
                    "actual_signed_pips": 2.0,
                    "chosen_theoretical_pips": 1.0,
                    "outcome_delay_sec": 0.1,
                },
            ]
        )

        panel, summary = build_panel(records)

        self.assertEqual(summary["duplicate_rows"], 1)
        self.assertEqual(summary["conflicting_outcomes"], 1)
        self.assertAlmostEqual(panel["market_mid_move_pips"].iloc[0], 2.0)

    def test_sql_prededup_metadata_preserves_source_and_conflict_audit(self):
        records = pd.DataFrame(
            [
                {
                    "origin_epoch": 1_800_000_000,
                    "instrument": "EUR_USD",
                    "horizon_sec": 60,
                    "model_id": "selected-in-sql",
                    "predicted_signed_pips": 1.0,
                    "actual_signed_pips": 2.0,
                    "chosen_theoretical_pips": 1.0,
                    "spread_pips": 1.0,
                    "features_json": "{}",
                    "model_rows_per_event": 9,
                    "actual_signed_pips_range": 0.5,
                }
            ]
        )

        panel, summary = build_panel(records)

        self.assertEqual(summary["events"], 1)
        self.assertEqual(summary["source_rows"], 9)
        self.assertEqual(summary["duplicate_rows"], 8)
        self.assertEqual(summary["conflicting_outcomes"], 1)
        self.assertEqual(len(panel), 2)


if __name__ == "__main__":
    unittest.main()
