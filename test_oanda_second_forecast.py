from __future__ import annotations

import json
import sqlite3
import tempfile
import time
import unittest
from dataclasses import dataclass
from pathlib import Path

try:
    from oanda_second_forecast import (
        DEFAULT_HORIZONS_SEC,
        FEATURE_NAMES,
        INPUT_TIMEFRAME,
        MODEL_FAMILY,
        SecondFeatureEngine,
        SecondForecastRuntime,
        SecondForecastStore,
        augment_cross_sectional_microstructure,
        matrix_lane_id,
    )
except ModuleNotFoundError:
    from trad.oanda_second_forecast import (
        DEFAULT_HORIZONS_SEC,
        FEATURE_NAMES,
        INPUT_TIMEFRAME,
        MODEL_FAMILY,
        SecondFeatureEngine,
        SecondForecastRuntime,
        SecondForecastStore,
        augment_cross_sectional_microstructure,
        matrix_lane_id,
    )

try:
    from oanda_second_forecast_runner import parse_args as parse_runner_args
except ModuleNotFoundError:
    from trad.oanda_second_forecast_runner import parse_args as parse_runner_args


@dataclass
class FakeQuote:
    bid: float
    ask: float
    time: str = "2026-07-15T12:00:00Z"
    tradeable: bool = True
    local_age_sec: float = 0.0


def model_payload() -> dict[str, object]:
    profile = {
        "score_threshold": 0.0,
        "max_spread_pips": 3.0,
        "historical_gate_passed": False,
    }
    coefficients = [0.0] * len(FEATURE_NAMES)
    coefficients[0] = 1.0
    return {
        "schema_version": 1,
        "fitted_utc": "2026-07-15T12:00:00+00:00",
        "feature_names": list(FEATURE_NAMES),
        "models": {
            "EUR_USD": {
                "60": {
                    "model_id": "second_ridge.EUR_USD.h60",
                    "feature_means": [0.0] * len(FEATURE_NAMES),
                    "feature_scales": [1.0] * len(FEATURE_NAMES),
                    "coefficients": coefficients,
                    "intercept": 0.0,
                    "magnitude_calibration": 1.0,
                    "residual_std_pips": 1.0,
                    "profiles": {name: dict(profile) for name in ("fast", "balanced", "strict")},
                }
            }
        },
    }


class SecondFeatureEngineTests(unittest.TestCase):
    def test_hot_runner_accepts_shared_profit_management_options(self):
        args = parse_runner_args(
            [
                "--execution-trailing-stop-r",
                "0.45",
                "--execution-trailing-activation-spread-multiple",
                "0.25",
                "--execution-profit-lock-trigger-pips",
                "2.0",
                "--execution-profit-lock-floor-pips",
                "0.2",
                "--execution-profit-lock-spread-multiple",
                "1.5",
                "--execution-profit-lock-step-pips",
                "0.25",
            ]
        )
        self.assertEqual(args.execution_trailing_stop_r, 0.45)
        self.assertEqual(args.execution_profit_lock_step_pips, 0.25)

    def test_s1_ridge_uses_unified_matrix_dimensions(self):
        self.assertEqual(
            DEFAULT_HORIZONS_SEC,
            (
                15,
                30,
                60,
                120,
                180,
                300,
                600,
                900,
                1800,
                3600,
                7200,
                10800,
                14400,
            ),
        )
        self.assertEqual(matrix_lane_id("fast"), "ridge_return.s1.fast")
        self.assertEqual(MODEL_FAMILY, "ridge_return")
        self.assertEqual(INPUT_TIMEFRAME, "S1")

    def test_features_are_causal_and_ready_after_sixty_seconds(self):
        engine = SecondFeatureEngine()
        ready = {}
        for second in range(61):
            bid = 1.10000 + second * 0.00001
            ready = engine.observe(
                {"EUR_USD": FakeQuote(bid, bid + 0.00010)},
                {
                    "EUR_USD": {
                        "version": second * 4,
                        "depth_imbalance": 0.2,
                        "bid_top_liquidity": 100 + second,
                        "ask_top_liquidity": 100,
                    }
                },
                {"EUR_USD": 0.0001},
                now_epoch=1_700_000_000 + second,
            )
        features = ready["EUR_USD"]
        self.assertAlmostEqual(features["return_5_pips"], 0.5, places=6)
        self.assertAlmostEqual(features["return_60_pips"], 6.0, places=6)
        self.assertAlmostEqual(features["spread_pips"], 1.0, places=6)
        self.assertEqual(features["activity_30"], 120)
        self.assertAlmostEqual(features["depth_imbalance"], 0.2)
        self.assertGreater(features["quote_flow_imbalance_5"], 0.0)
        self.assertGreater(features["quote_flow_imbalance_30"], 0.0)
        self.assertIn("microprice_offset_mean_30", features)
        self.assertEqual(features["activity_5"], 20)
        self.assertAlmostEqual(features["tick_imbalance_5"], 1.0)
        self.assertAlmostEqual(features["tick_imbalance_30"], 1.0)
        self.assertAlmostEqual(features["depth_top_imbalance"], 60.0 / 260.0)
        self.assertGreater(features["depth_log_total_liquidity"], 0.0)
        self.assertIn("spread_volatility_30", features)

    def test_live_volatility_uses_s5_aligned_returns(self):
        engine = SecondFeatureEngine()
        intrabar_offsets = (0.0, 0.5, -0.5, 0.75, -0.25)
        ready = {}
        for second in range(61):
            five_second_bar = second // 5
            bid = 1.10000 + (five_second_bar + intrabar_offsets[second % 5]) * 0.00010
            ready = engine.observe(
                {"EUR_USD": FakeQuote(bid, bid + 0.00010)},
                {"EUR_USD": {"version": second * 4}},
                {"EUR_USD": 0.0001},
                now_epoch=1_700_000_000 + second,
            )

        features = ready["EUR_USD"]
        self.assertAlmostEqual(features["volatility_30_pips"], 0.0, places=6)
        self.assertAlmostEqual(features["volatility_60_pips"], 0.0, places=6)

    def test_bounded_processing_gap_preserves_causal_feature_history(self):
        engine = SecondFeatureEngine(max_fill_gap_sec=60)
        base = 1_700_000_000
        for second in range(55):
            engine.observe(
                {"EUR_USD": FakeQuote(1.1000, 1.1001)},
                {"EUR_USD": {"version": second}},
                {"EUR_USD": 0.0001},
                now_epoch=base + second,
            )

        ready = engine.observe(
            {"EUR_USD": FakeQuote(1.1002, 1.1003)},
            {"EUR_USD": {"version": 70}},
            {"EUR_USD": 0.0001},
            now_epoch=base + 70,
        )

        self.assertIn("EUR_USD", ready)
        self.assertEqual(len(engine.points["EUR_USD"]), 71)
        self.assertEqual(ready["EUR_USD"]["activity_30"], 30)

    def test_cross_pair_features_are_leave_one_pair_out(self):
        def feature(
            return_5: float,
            return_30: float,
            return_60: float,
            quote_flow: float,
        ) -> dict[str, float]:
            return {
                "return_1_pips": return_5 / 5.0,
                "return_3_pips": return_5 * 0.6,
                "return_5_pips": return_5,
                "return_30_pips": return_30,
                "return_60_pips": return_60,
                "quote_flow_imbalance_1": quote_flow,
                "quote_flow_imbalance_5": quote_flow,
                "quote_flow_imbalance_30": quote_flow,
            }

        first = {
            "EUR_USD": feature(1.0, 2.0, 3.0, 0.1),
            "GBP_USD": feature(2.0, 3.0, 4.0, 0.2),
            "EUR_GBP": feature(-1.0, -1.0, -1.0, -0.1),
        }
        augment_cross_sectional_microstructure(first)
        peer_return = first["EUR_USD"]["cross_strength_r3"]
        peer_flow = first["EUR_USD"]["cross_quote_flow_strength_5"]

        second = {
            "EUR_USD": feature(100.0, 200.0, 300.0, 1.0),
            "GBP_USD": feature(2.0, 3.0, 4.0, 0.2),
            "EUR_GBP": feature(-1.0, -1.0, -1.0, -0.1),
        }
        augment_cross_sectional_microstructure(second)

        self.assertEqual(second["EUR_USD"]["cross_sample_count"], 1.0)
        self.assertEqual(second["EUR_USD"]["cross_quote_flow_sample_count"], 1.0)
        self.assertAlmostEqual(second["EUR_USD"]["cross_strength_r3"], peer_return)
        self.assertAlmostEqual(
            second["EUR_USD"]["cross_quote_flow_strength_5"],
            peer_flow,
        )


class SecondForecastRuntimeTests(unittest.TestCase):
    def test_hot_runtime_never_opens_research_database(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            model = root / "model.json"
            database = root / "must_not_exist.sqlite"
            model.write_text(json.dumps(model_payload()), encoding="utf-8")
            runtime = SecondForecastRuntime(
                model,
                database,
                root / "hot.json",
                execution_horizons={60},
                research_enabled=False,
            )
            pending: list[dict[str, object]] = []
            callbacks: list[list[dict[str, object]]] = []
            for second in range(61):
                bid = 1.10000 + second * 0.00001
                runtime.evaluate(
                    {"EUR_USD": FakeQuote(bid, bid + 0.00010)},
                    {"EUR_USD": {"version": second * 4}},
                    {"EUR_USD": 0.0001},
                    pending,
                    lambda event, **fields: None,
                    now_epoch=1_700_000_000 + second,
                    candidate_callback=lambda rows: callbacks.append(rows),
                )
            self.assertFalse(database.exists())
            self.assertEqual(pending, [])
            self.assertEqual(len(callbacks), 1)
            self.assertEqual(len(callbacks[0]), 3)
            latest = list(runtime.top_forecasts)
            runtime.evaluate(
                {"EUR_USD": FakeQuote(1.10067, 1.10077)},
                {"EUR_USD": {"version": 268}},
                {"EUR_USD": 0.0001},
                pending,
                lambda event, **fields: None,
                now_epoch=1_700_000_067,
            )
            self.assertTrue(latest)
            self.assertNotEqual(runtime.top_forecasts, latest)
            self.assertTrue(runtime.top_forecasts_updated_utc)
            self.assertFalse(database.exists())
            runtime.close()

    def test_runtime_submits_candidates_before_persistence_and_tracks_one_horizon(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            model = root / "model.json"
            model.write_text(json.dumps(model_payload()), encoding="utf-8")
            runtime = SecondForecastRuntime(
                model,
                root / "forecast.sqlite",
                root / "state.json",
                sample_sec=1,
                signal_interval_sec=1,
                execution_horizons={60},
            )
            pending: list[dict[str, object]] = []
            callbacks: list[list[dict[str, object]]] = []
            events: list[str] = []
            for second in range(61):
                bid = 1.10000 + second * 0.00001
                runtime.evaluate(
                    {"EUR_USD": FakeQuote(bid, bid + 0.00010)},
                    {"EUR_USD": {"version": second * 4}},
                    {"EUR_USD": 0.0001},
                    pending,
                    lambda event, **fields: events.append(event),
                    now_epoch=1_700_000_000 + second,
                    candidate_callback=lambda rows: callbacks.append(rows),
                )
            self.assertEqual(len(callbacks), 1)
            self.assertEqual(len(callbacks[0]), 3)
            self.assertEqual({row["forecast_horizon_sec"] for row in callbacks[0]}, {60})
            self.assertEqual({row["lane_id"] for row in callbacks[0]}, {"ridge_return.s1.fast", "ridge_return.s1.balanced", "ridge_return.s1.strict"})
            self.assertEqual({row["input_timeframe"] for row in callbacks[0]}, {"S1"})
            self.assertEqual(len(pending), 3)
            self.assertEqual({row["remaining_horizons"][0] for row in pending}, {60.0})
            self.assertIn("second_shadow_signal", events)
            runtime.close()

    def test_runtime_propagates_research_only_model_eligibility(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            model = root / "model.json"
            payload = model_payload()
            payload["models"]["EUR_USD"]["60"].update(
                {
                    "fit_provenance": "global_pooled_proxy",
                    "pair_specific_evidence": False,
                    "account_eligible": False,
                }
            )
            model.write_text(json.dumps(payload), encoding="utf-8")
            runtime = SecondForecastRuntime(
                model,
                root / "unused.sqlite",
                root / "state.json",
                execution_horizons={60},
                research_enabled=False,
            )
            callbacks: list[list[dict[str, object]]] = []
            for second in range(61):
                bid = 1.10000 + second * 0.00001
                runtime.evaluate(
                    {"EUR_USD": FakeQuote(bid, bid + 0.00010)},
                    {"EUR_USD": {"version": second * 4}},
                    {"EUR_USD": 0.0001},
                    [],
                    lambda event, **fields: None,
                    now_epoch=1_700_000_000 + second,
                    candidate_callback=lambda rows: callbacks.append(rows),
                )
            self.assertEqual(len(callbacks), 1)
            self.assertTrue(
                all(not row["account_eligible"] for row in callbacks[0])
            )
            self.assertFalse(runtime.top_forecasts[0]["account_eligible"])
            self.assertEqual(
                runtime.top_forecasts[0]["fit_provenance"],
                "global_pooled_proxy",
            )
            runtime.close()

    def test_store_matures_sampled_forecast_with_executable_costs(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "forecast.sqlite"
            store = SecondForecastStore(path)
            quote = FakeQuote(1.1000, 1.1001)
            forecast = {
                "instrument": "EUR_USD",
                "model_id": "test",
                "horizon_sec": 1,
                "predicted_signed_pips": 1.0,
                "probability_up": 0.6,
                "raw_score": 1.0,
                "spread_pips": 1.0,
                "profiles": {},
            }
            event_id = store.record(forecast, quote, 0.0001, 100)
            _, sequence, pending = store.pending[0]
            store.pending[0] = (time.monotonic() - 1.0, sequence, pending)
            outcomes = store.mature({"EUR_USD": FakeQuote(1.1003, 1.1004)})
            store.flush()
            row = store.database.execute(
                "SELECT status, chosen_theoretical_pips FROM forecasts WHERE id=?", (event_id,)
            ).fetchone()
            self.assertEqual(len(outcomes), 1)
            self.assertEqual(row[0], "matured")
            self.assertAlmostEqual(row[1], 2.0, places=6)
            store.close()

    def test_store_persists_compact_microstructure_feature_snapshots(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "forecast.sqlite"
            store = SecondForecastStore(path)
            store.record_feature_snapshot(
                100,
                "EUR_USD",
                {
                    "quote_flow_imbalance_5": 0.25,
                    "depth_imbalance_delta_5": -0.1,
                },
            )
            store.flush()

            row = store.database.execute(
                """
                SELECT features_json FROM feature_snapshots
                WHERE origin_epoch=? AND instrument=?
                """,
                (100, "EUR_USD"),
            ).fetchone()

            self.assertIsNotNone(row)
            payload = json.loads(row[0])
            self.assertEqual(payload["quote_flow_imbalance_5"], 0.25)
            self.assertEqual(payload["depth_imbalance_delta_5"], -0.1)
            store.close()


if __name__ == "__main__":
    unittest.main()
