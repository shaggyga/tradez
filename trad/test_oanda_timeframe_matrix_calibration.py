import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from trad.oanda_shadow_outcome_store import ShadowOutcomeStore
from trad.oanda_timeframe_matrix_calibration import SurfaceStats, TimeframeMatrixCalibrator


class TimeframeMatrixCalibrationTests(unittest.TestCase):
    def test_long_horizon_readiness_uses_non_overlapping_maturity_blocks(self):
        stats = SurfaceStats(
            scope="global",
            surface_key="timeframe_equation_matrix.m1|86400",
            lane_id="timeframe_equation_matrix.m1",
            instrument="",
            horizon_sec=86400,
        )
        for index in range(140):
            hour = index % 10
            stats.observe(
                raw_probability_up=0.8,
                actual_up=1.0,
                long_net_pips=2.0,
                short_net_pips=-2.0,
                observed_utc=f"2026-07-01T{hour:02d}:00:00+00:00",
            )
        state = stats.state()
        self.assertEqual(state["observed_hour_blocks"], 10)
        self.assertEqual(state["independent_blocks"], 1)
        self.assertEqual(state["independence_block_sec"], 86400)
        self.assertFalse(state["validation_ready"])

    def test_incremental_row_order_diagnostics_cannot_claim_readiness(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "outcomes.sqlite"
            store = ShadowOutcomeStore(source, batch_size=64, flush_sec=100)
            for index in range(140):
                up = index % 4 != 0
                entry_bid = 1.1000
                entry_ask = 1.1001
                exit_bid = 1.1003 if up else 1.0998
                exit_ask = exit_bid + 0.0001
                store.observe(
                    {
                        "id": f"event-{index}",
                        "horizon_sec": 300,
                        "lane_id": "timeframe_equation_matrix.m1",
                        "family": "timeframe_equation_matrix",
                        "profile": "continuous",
                        "model_id": "timeframe_equation_matrix.m1",
                        "input_timeframe": "M1",
                        "training_timeframe": "rolling_live_equation",
                        "kind": "signal",
                        "instrument": "EUR_USD",
                        "direction": "buy",
                        "entry_bid": entry_bid,
                        "entry_ask": entry_ask,
                        "exit_bid": exit_bid,
                        "exit_ask": exit_ask,
                        "pip": 0.0001,
                        "timeframe_prediction": {
                            "probability_up": 0.8 if up else 0.2,
                        },
                    }
                )
            store.close()
            connection = sqlite3.connect(source)
            connection.execute(
                "UPDATE outcomes SET observed_utc = "
                "printf('2026-07-%02dT%02d:00:00+00:00', "
                "1 + (row_id / 24), row_id % 24)"
            )
            connection.commit()
            connection.close()

            calibrator = TimeframeMatrixCalibrator(
                source,
                root / "calibration.sqlite",
                root / "calibration.json",
                batch_size=50,
            )
            first = calibrator.run_once()
            second = calibrator.run_once()
            calibrator.close()

            key = "EUR_USD|timeframe_equation_matrix.m1|300"
            surface = first["pair_surfaces"][key]
            self.assertEqual(surface["n"], 140)
            self.assertGreater(surface["oos_n"], 80)
            self.assertFalse(surface["account_eligible"])
            self.assertFalse(surface["proof_eligible"])
            self.assertFalse(surface["validation_ready"])
            self.assertFalse(surface["issue_time_availability_verified"])
            self.assertEqual(surface["status"], "diagnostic_replay")
            self.assertEqual(first["ready_surface_count"], 0)
            self.assertFalse(first["proof_eligible"])
            self.assertEqual(second["processed_rows_this_run"], 0)
            self.assertEqual(second["last_source_row_id"], 140)
            saved = json.loads((root / "calibration.json").read_text(encoding="utf-8"))
            self.assertIn(key, saved["pair_surfaces"])

    def test_good_replay_numbers_remain_diagnostic_even_when_old_thresholds_pass(self):
        stats = SurfaceStats("pair", "EUR_USD|lane|300", "lane", "EUR_USD", 300)
        for index in range(140):
            stats.observe(raw_probability_up=0.8, actual_up=1.0,
                          long_net_pips=2.0, short_net_pips=-2.0,
                          observed_utc=f"2026-07-{1 + index // 24:02d}T{index % 24:02d}:00:00+00:00")
        state = stats.state()
        self.assertTrue(state["diagnostic_thresholds_met"])
        self.assertFalse(state["validation_ready"])
        self.assertFalse(state["proof_eligible"])
        self.assertEqual(state["n"], 140)
        self.assertEqual(state["oos_n"], 100)
        self.assertGreater(state["calibrated_accuracy"], 0.99)


if __name__ == "__main__":
    unittest.main()
