import unittest
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace

from trad.oanda_timeframe_horizon_matrix import (
    TIMEFRAME_SECONDS,
    TimeframeHorizonMatrix,
)


class TimeframeHorizonMatrixTests(unittest.TestCase):
    def test_live_minimum_timeframe_disables_subminute_inputs(self):
        matrix = TimeframeHorizonMatrix(minimum_timeframe_sec=60)
        series = [1.1000 + index * 0.0001 for index in range(40)]
        features = {
            "candle_time": "2026-07-16T10:00:00Z",
            "closes": series,
            "m5_closes": series,
            "m10_closes": series,
            "m15_closes": series,
            "m30_closes": series,
            "h1_closes": series,
            "h2_closes": series,
            "h3_closes": series,
            "h4_closes": series,
        }
        pending = []
        emitted = matrix.emit(
            {"EUR_USD": features},
            {
                "EUR_USD": SimpleNamespace(
                    bid=1.10065,
                    ask=1.10075,
                    time="2026-07-16T10:00:00Z",
                )
            },
            {"EUR_USD": 0.0001},
            [60, 300],
            pending,
            lambda *_args, **_kwargs: None,
        )

        expected = {
            label for label, seconds in TIMEFRAME_SECONDS.items() if seconds >= 60
        }
        self.assertEqual(emitted, len(expected))
        self.assertEqual({row["input_timeframe"] for row in pending}, expected)
        self.assertEqual(set(matrix.summary([60, 300])["input_timeframes"]), expected)

    def test_every_timeframe_emits_a_multi_horizon_shadow_equation(self):
        matrix = TimeframeHorizonMatrix()
        for epoch in range(1000, 1700):
            mid = 1.1000 + (epoch - 1000) * 0.000001
            matrix.observe(
                {
                    "EUR_USD": SimpleNamespace(
                        bid=mid - 0.00005,
                        ask=mid + 0.00005,
                        time=f"t-{epoch}",
                    )
                },
                now_epoch=epoch,
            )
        series = [1.1000 + index * 0.0001 for index in range(40)]
        features = {
            "candle_time": "2026-07-16T10:00:00Z",
            "closes": series,
            "m5_closes": series,
            "m10_closes": series,
            "m15_closes": series,
            "m30_closes": series,
            "h1_closes": series,
            "h2_closes": series,
            "h3_closes": series,
            "h4_closes": series,
        }
        pending = []
        candidates = []
        logs = []
        emitted = matrix.emit(
            {"EUR_USD": features},
            {
                "EUR_USD": SimpleNamespace(
                    bid=1.10065,
                    ask=1.10075,
                    time="2026-07-16T10:00:00Z",
                )
            },
            {"EUR_USD": 0.0001},
            [60, 300],
            pending,
            lambda event, **fields: logs.append({"event": event, **fields}),
            candidate_sink=candidates,
        )
        self.assertEqual(emitted, len(TIMEFRAME_SECONDS))
        self.assertEqual(len(pending), len(TIMEFRAME_SECONDS))
        self.assertEqual(
            {row["input_timeframe"] for row in pending},
            set(TIMEFRAME_SECONDS),
        )
        self.assertTrue(all(set(row["forecast_curve"]) == {"60", "300"} for row in pending))
        self.assertTrue(all(not row["track_exit_path"] for row in pending))
        self.assertTrue(all(row["blocked_reason"] == "research_only" for row in pending))
        self.assertEqual(len(candidates), len(TIMEFRAME_SECONDS))
        self.assertEqual(
            {row["input_timeframe"] for row in candidates},
            set(TIMEFRAME_SECONDS),
        )
        self.assertTrue(all(not row["account_eligible"] for row in candidates))
        self.assertTrue(all(set(row["forecast_curve"]) == {"60", "300"} for row in candidates))
        self.assertEqual(logs[-1]["event"], "timeframe_matrix_forecast_batch")

    def test_native_outcome_mode_keeps_one_nearest_horizon_per_timeframe(self):
        matrix = TimeframeHorizonMatrix(
            minimum_timeframe_sec=60,
            outcome_horizon_mode="native",
        )
        series = [1.1000 + index * 0.0001 for index in range(40)]
        features = {
            "candle_time": "2026-07-16T10:00:00Z",
            "closes": series,
            "m5_closes": series,
            "m10_closes": series,
            "m15_closes": series,
            "m30_closes": series,
            "h1_closes": series,
            "h2_closes": series,
            "h3_closes": series,
            "h4_closes": series,
        }
        pending = []
        matrix.emit(
            {"EUR_USD": features},
            {"EUR_USD": SimpleNamespace(bid=1.10065, ask=1.10075, time="2026-07-16T10:00:00Z")},
            {"EUR_USD": 0.0001},
            [60, 300, 900, 3600, 14400],
            pending,
            lambda *_args, **_kwargs: None,
        )
        self.assertTrue(pending)
        self.assertTrue(all(len(row["forecast_curve"]) == 1 for row in pending))
        self.assertEqual(matrix.summary([60, 300])["outcome_horizon_mode"], "native")

    def test_applies_pair_calibration_without_enabling_execution(self):
        with tempfile.TemporaryDirectory() as temporary:
            state_path = Path(temporary) / "calibration.json"
            bins = [
                {"posterior_up": 0.10 + index * 0.08}
                for index in range(10)
            ]
            state_path.write_text(
                json.dumps(
                    {
                        "surface_count": 1,
                        "ready_surface_count": 1,
                        "pair_surfaces": {
                            "EUR_USD|timeframe_equation_matrix.m1|300": {
                                "n": 200,
                                "validation_ready": True,
                                "calibrated_brier": 0.22,
                                "bins": bins,
                            }
                        },
                        "global_surfaces": {},
                    }
                ),
                encoding="utf-8",
            )
            matrix = TimeframeHorizonMatrix(calibration_state_path=state_path)
            forecast = {"probability_up": 0.76}
            calibrated = matrix._apply_calibration(
                forecast,
                instrument="EUR_USD",
                timeframe="M1",
                horizon_sec=300,
            )
            self.assertEqual(calibrated["calibration_scope"], "pair")
            self.assertEqual(calibrated["calibration_n"], 200)
            self.assertTrue(calibrated["calibration_ready"])
            self.assertFalse(calibrated["account_eligible"])
            self.assertNotEqual(
                calibrated["raw_probability_up"],
                calibrated["probability_up"],
            )


if __name__ == "__main__":
    unittest.main()
