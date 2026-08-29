import tempfile
import time
import unittest
import json
from pathlib import Path

try:
    from oanda_model_gap_live_signal_producer import (
        DEFAULT_MODEL_ROOT,
        DEFAULT_REPORT,
        ModelGapArtifactProducer,
        relative_up_probability,
    )
    from oanda_signal_contribution_feed import SignalContributionFeed
    from oanda_model_predictor_auto_promotion import cell_id, state_checksum
except ModuleNotFoundError:
    from trad.oanda_model_gap_live_signal_producer import (
        DEFAULT_MODEL_ROOT,
        DEFAULT_REPORT,
        ModelGapArtifactProducer,
        relative_up_probability,
    )
    from trad.oanda_signal_contribution_feed import SignalContributionFeed
    from trad.oanda_model_predictor_auto_promotion import cell_id, state_checksum


class ModelGapLiveSignalProducerTests(unittest.TestCase):
    def test_relative_side_probabilities_are_directional(self):
        self.assertAlmostEqual(relative_up_probability(0.5, 0.5), 0.5)
        self.assertGreater(relative_up_probability(0.7, 0.4), 0.5)
        self.assertLess(relative_up_probability(0.3, 0.6), 0.5)

    def test_verified_artifacts_emit_only_reported_shadow_cells(self):
        if not DEFAULT_MODEL_ROOT.is_dir() or not DEFAULT_REPORT.is_file():
            self.skipTest("checkpoint model artifacts are unavailable")
        producer = ModelGapArtifactProducer(
            DEFAULT_MODEL_ROOT,
            DEFAULT_REPORT,
            ("catboost", "ngboost"),
        )
        if set(producer.runtimes) != {"catboost", "ngboost"}:
            self.skipTest(f"optional model runtime unavailable: {producer.load_errors}")
        features = {
            name: 0.0
            for runtime in producer.runtimes.values()
            for name in runtime.artifact.get("numeric_features") or []
        }
        features.update(
            {
                "pip": 0.0001,
                "last": 1.10005,
                "m1_atr14_pips": 5.0,
                "m5_atr14_pips": 12.0,
                "volume_ratio_12": 1.1,
                "volume_ratio_30": 1.0,
                "depth_imbalance": 0.15,
                "order_book_near_10_imbalance": -0.05,
            }
        )
        now = time.time()
        forecasts = producer.forecast(
            {
                "snapshot_id": "unit-live-1",
                "generated_epoch": now,
                "generated_utc": "2026-07-21T12:00:00+00:00",
                "instruments": {
                    "EUR_USD": {
                        "quote": {"bid": 1.1000, "ask": 1.1001},
                        "features": features,
                        "timeframe_features": {
                            "H1": {**features, "input_timeframe": "H1"},
                            "H4": {**features, "input_timeframe": "H4"},
                        },
                    }
                },
            }
        )

        self.assertEqual(len(forecasts), 4)
        self.assertEqual({row["model_id"] for row in forecasts}, {"catboost", "ngboost"})
        self.assertEqual({row["input_timeframe"] for row in forecasts}, {"H1", "H4"})
        self.assertTrue(all(not row["account_eligible"] for row in forecasts))
        self.assertTrue(
            all(
                row["producer_metadata"]["input_feature_contract"]
                == "timeframe_matched_training_semantics"
                for row in forecasts
            )
        )
        self.assertTrue(
            all(
                row["producer_metadata"][
                    "execution_microstructure_used_by_structural_model"
                ]
                is False
                for row in forecasts
            )
        )
        expected_horizons = {
            "60",
            "120",
            "180",
            "300",
            "600",
            "900",
            "1800",
            "3600",
        }
        self.assertTrue(
            all(set(row["forecast_curve"]) == expected_horizons for row in forecasts)
        )
        self.assertTrue(
            all(
                point.get("predicted_signed_pips") is None
                for row in forecasts
                for point in row["forecast_curve"].values()
            )
        )

        with tempfile.TemporaryDirectory() as temporary:
            feed = SignalContributionFeed(Path(temporary) / "signals.sqlite")
            result = feed.publish_forecasts(
                forecasts,
                "model-gap-unit",
                60.0,
                max_age_sec=60.0,
            )
            recent = feed.recent()
            coverage = feed.coverage(fresh_sec=60.0)
            feed.close()
        self.assertEqual(result["accepted"], 4)
        self.assertEqual(len(recent), 4)
        self.assertEqual(coverage["model_gap"]["fresh_live_contributors"], 2)
        self.assertTrue(all(row["research_only"] for row in recent))
        self.assertTrue(
            all(
                row["signal_provenance"]["producer_metadata"][
                    "historical_values_used_as_live_predictions"
                ]
                is False
                for row in recent
            )
        )

    def test_missing_timeframe_view_is_not_silently_substituted(self):
        if not DEFAULT_MODEL_ROOT.is_dir() or not DEFAULT_REPORT.is_file():
            self.skipTest("checkpoint model artifacts are unavailable")
        producer = ModelGapArtifactProducer(
            DEFAULT_MODEL_ROOT,
            DEFAULT_REPORT,
            ("catboost",),
        )
        if "catboost" not in producer.runtimes:
            self.skipTest(f"optional model runtime unavailable: {producer.load_errors}")
        now = time.time()
        forecasts = producer.forecast(
            {
                "snapshot_id": "unit-live-missing-view",
                "generated_epoch": now,
                "generated_utc": "2026-07-21T12:00:00+00:00",
                "instruments": {
                    "EUR_USD": {
                        "quote": {"bid": 1.1000, "ask": 1.1001},
                        "features": {"pip": 0.0001},
                        "timeframe_features": {},
                    }
                },
            }
        )

        self.assertEqual(forecasts, [])
        self.assertGreater(producer.summary()["missing_timeframe_feature_cells"], 0)

    def test_hash_bound_promotion_enables_only_exact_curve_point(self):
        if not DEFAULT_MODEL_ROOT.is_dir() or not DEFAULT_REPORT.is_file():
            self.skipTest("checkpoint model artifacts are unavailable")
        baseline = ModelGapArtifactProducer(
            DEFAULT_MODEL_ROOT,
            DEFAULT_REPORT,
            ("catboost",),
        )
        runtime = baseline.runtimes.get("catboost")
        if runtime is None:
            self.skipTest(f"optional model runtime unavailable: {baseline.load_errors}")
        promoted = {
            "cell_id": cell_id(
                "catboost", runtime.sha256, "EUR_USD", "H1", 300
            ),
            "model_id": "catboost",
            "artifact_sha256": runtime.sha256,
            "instrument": "EUR_USD",
            "input_timeframe": "H1",
            "horizon_sec": 300,
            "eligible": True,
            "conservative_expected_net_pips": 0.4,
            "independent_blocks": 12,
            "holdout_blocks": 4,
            "raw": {"n": 12, "avg": 0.5, "win_rate": 60.0},
            "training": {"n": 8, "avg": 0.4, "win_rate": 62.5},
            "holdout": {"n": 4, "avg": 0.6, "win_rate": 75.0},
        }
        now = time.time()
        state = {
            "schema_version": 1,
            "generated_epoch": now,
            "generated_utc": "2026-07-21T12:00:00+00:00",
            "status": "active",
            "account_scope": "practice_007_only",
            "real_account_authorized": False,
            "expires_after_sec": 1800,
            "eligible_cells": [promoted],
        }
        state["state_sha256"] = state_checksum(state)
        features = {
            name: 0.0 for name in runtime.artifact.get("numeric_features") or []
        }
        features.update({"pip": 0.0001, "m1_atr14_pips": 5.0})
        with tempfile.TemporaryDirectory() as temporary:
            policy = Path(temporary) / "promotion.json"
            policy.write_text(json.dumps(state), encoding="utf-8")
            producer = ModelGapArtifactProducer(
                DEFAULT_MODEL_ROOT,
                DEFAULT_REPORT,
                ("catboost",),
                promotion_state_path=policy,
            )
            forecasts = producer.forecast(
                {
                    "snapshot_id": "promoted-exact-cell",
                    "generated_epoch": now,
                    "generated_utc": "2026-07-21T12:00:00+00:00",
                    "instruments": {
                        "EUR_USD": {
                            "quote": {"bid": 1.1000, "ask": 1.1001},
                            "timeframe_features": {"H1": features},
                        }
                    },
                }
            )

        self.assertEqual(len(forecasts), 1)
        forecast = forecasts[0]
        self.assertTrue(forecast["account_eligible"])
        self.assertTrue(forecast["forecast_curve"]["300"]["account_eligible"])
        self.assertEqual(
            forecast["forecast_curve"]["300"]["projected_net_pips"], 0.4
        )
        self.assertTrue(
            all(
                not point["account_eligible"]
                for horizon, point in forecast["forecast_curve"].items()
                if horizon != "300"
            )
        )


if __name__ == "__main__":
    unittest.main()
