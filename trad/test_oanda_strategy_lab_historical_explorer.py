import unittest

from trad.oanda_strategy_lab_historical_explorer import ensemble_predictions, lane_predictions


class ExplorerPredictionTests(unittest.TestCase):
    def test_lane_prediction_joins_delayed_outcome(self):
        rows = [
            {
                "event": "shadow_signal",
                "id": "signal-1",
                "lane_id": "momentum.fast",
                "family": "momentum",
                "profile": "fast",
                "instrument": "EUR_USD",
                "direction": "buy",
                "entry_time": "2026-07-07T12:00:00Z",
                "entry_bid": 1.1,
                "entry_ask": 1.1002,
                "gates": {"spread_pips": 2.0, "signal_to_spread": 3.0},
            },
            {
                "event": "shadow_outcome",
                "id": "signal-1",
                "horizon_sec": 300,
                "theoretical_pips": 1.25,
            },
        ]
        prediction = lane_predictions(rows)[0]
        self.assertEqual(prediction["model_key"], "lane:momentum.fast")
        self.assertEqual(prediction["status"], "accepted")
        self.assertEqual(prediction["outcomes"]["300"], 1.25)

    def test_ensemble_prediction_preserves_vote_metadata(self):
        rows = [
            {
                "event": "ensemble_signal",
                "id": "ensemble-1",
                "ensemble": "accepted_unanimous_2",
                "instrument": "EUR_USD",
                "direction": "sell",
                "entry_time": "2026-07-07T12:00:00Z",
                "entry_bid": 1.1,
                "entry_ask": 1.1002,
                "agreement": 1.0,
                "voter_count": 2,
                "vote_level": "family",
            },
            {
                "event": "ensemble_outcome",
                "ensemble_id": "ensemble-1",
                "horizon_sec": 180,
                "theoretical_pips": -0.5,
            },
        ]
        prediction = ensemble_predictions(rows)[0]
        self.assertEqual(prediction["model_key"], "ensemble:accepted_unanimous_2")
        self.assertEqual(prediction["voter_count"], 2)
        self.assertEqual(prediction["outcomes"]["180"], -0.5)

    def test_pattern_prediction_preserves_coefficient_and_realized_diagnostic(self):
        rows = [
            {
                "event": "shadow_miss",
                "id": "pattern-1",
                "lane_id": "pattern_count_forecast.fast",
                "family": "pattern_count_forecast",
                "profile": "fast",
                "instrument": "EUR_USD",
                "direction": "buy",
                "entry_time": "2026-07-07T12:00:00Z",
                "entry_bid": 1.1,
                "entry_ask": 1.1001,
                "signal": {
                    "pattern_forecast": {
                        "pattern": "U D U",
                        "probability_up": 0.508,
                        "expected_signed_move_pips": 0.1,
                        "expected_abs_move_pips": 1.4,
                        "movement_coefficient": 1.25,
                        "historical_pattern_count": 800,
                        "live_pattern_count": 4,
                    }
                },
            },
            {
                "event": "shadow_outcome",
                "id": "pattern-1",
                "horizon_sec": 60,
                "theoretical_pips": 0.5,
                "pattern_prediction": {"pattern": "U D U"},
                "actual_signed_move_pips": 0.6,
                "actual_abs_move_pips": 0.6,
                "actual_movement_coefficient": 0.55,
                "direction_correct": True,
            },
        ]
        prediction = lane_predictions(rows)[0]
        self.assertEqual(prediction["movement_coefficient"], 1.25)
        self.assertEqual(prediction["historical_pattern_count"], 800)
        self.assertTrue(prediction["pattern_outcomes"]["60"]["direction_correct"])


if __name__ == "__main__":
    unittest.main()
