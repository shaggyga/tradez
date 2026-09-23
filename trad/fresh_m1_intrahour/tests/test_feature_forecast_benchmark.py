from __future__ import annotations

import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch

import joblib
import numpy as np
import pandas as pd

from fresh_m1_intrahour.src import feature_forecast_benchmark as benchmark
from fresh_m1_intrahour.src.feature_forecast_benchmark import (
    PRIMARY_TARGET,
    TemporalFold,
    add_calendar_features,
    add_causal_interactions,
    add_currency_identity_features,
    candidate_specs,
    classification_metrics,
    development_folds,
    evaluate_two_stage_forecast,
    rank_reports,
    safety_fields,
    split_temporal_fold,
    target_values,
)
from fresh_m1_intrahour.src.forecast_transformers import StableFSelector


class FeatureForecastContractTests(unittest.TestCase):
    def test_target_is_future_mid_direction(self) -> None:
        frame = pd.DataFrame({"long_gross_endpoint_pips": [2.0, -1.0, 0.0]})
        self.assertEqual(target_values(frame, PRIMARY_TARGET).tolist(), [1, 0, 0])

    def test_temporal_split_purges_labels_and_applies_embargo(self) -> None:
        times = pd.date_range("2024-12-31T18:00:00Z", periods=12, freq="h")
        frame = pd.DataFrame(
            {
                "decision_time_utc": times,
                "label_end_time_utc": times + pd.Timedelta(hours=2),
            }
        )
        fold = TemporalFold(
            "test",
            "2025-01-01T00:00:00Z",
            "2025-01-01T06:00:00Z",
        )
        train, test, audit = split_temporal_fold(
            frame,
            fold,
            embargo_minutes=60,
        )
        cutoff = fold.start - pd.Timedelta(minutes=60)
        self.assertTrue((train["label_end_time_utc"] < cutoff).all())
        self.assertTrue((test["decision_time_utc"] >= fold.start).all())
        self.assertTrue(audit["purge_passed"])

    def test_candidate_grid_and_folds_are_predeclared(self) -> None:
        specs = candidate_specs()
        self.assertEqual(len(specs), 7)
        self.assertEqual(len({spec.model_id for spec in specs}), 7)
        self.assertIn("random_forest_k100", {spec.model_id for spec in specs})
        self.assertEqual([fold.fold_id for fold in development_folds()], [
            "2025Q1",
            "2025Q2",
            "2025Q3",
            "2025Q4",
        ])

    def test_interactions_use_only_named_inputs(self) -> None:
        frame = pd.DataFrame(
            {
                "momentum_30_atr": [2.0],
                "trend_consistency_60": [3.0],
                "future_target": [99.0],
            }
        )
        output, columns = add_causal_interactions(frame)
        self.assertEqual(columns, ["causal_interaction__momentum30_x_trend_consistency"])
        self.assertEqual(float(output[columns[0]].iloc[0]), 6.0)
        self.assertNotIn("future_target", columns)

    def test_calendar_features_are_decision_time_only(self) -> None:
        frame = pd.DataFrame(
            {
                "decision_time_utc": pd.to_datetime(
                    ["2025-01-01T06:00:00Z", "2025-01-01T18:00:00Z"]
                )
            }
        )
        output, columns = add_calendar_features(frame)
        self.assertEqual(
            columns,
            [
                "calendar__hour_sin",
                "calendar__hour_cos",
                "calendar__day_of_week_sin",
                "calendar__day_of_week_cos",
            ],
        )
        self.assertTrue(np.isfinite(output[columns].to_numpy()).all())
        self.assertAlmostEqual(float(output["calendar__hour_cos"].iloc[0]), 0.0, places=6)

    def test_currency_identity_features_parse_base_and_quote(self) -> None:
        frame = pd.DataFrame({"instrument": ["EUR_USD", "USD_JPY"]})
        output, columns = add_currency_identity_features(
            frame,
            ["EUR_USD", "USD_JPY"],
        )
        self.assertIn("currency_base__EUR", columns)
        self.assertIn("currency_quote__USD", columns)
        self.assertEqual(float(output["currency_base__EUR"].iloc[0]), 1.0)
        self.assertEqual(float(output["currency_quote__USD"].iloc[0]), 1.0)
        self.assertEqual(float(output["currency_base__USD"].iloc[1]), 1.0)
        self.assertEqual(float(output["currency_quote__JPY"].iloc[1]), 1.0)

    def test_metrics_reward_calibrated_discrimination(self) -> None:
        actual = np.array([0, 0, 1, 1])
        strong = classification_metrics(
            actual,
            np.array([0.1, 0.2, 0.8, 0.9]),
            baseline_probability=0.5,
        )
        self.assertEqual(strong["roc_auc"], 1.0)
        self.assertGreater(strong["brier_skill_score"], 0.0)

    def test_ranking_is_deterministic_and_gate_first(self) -> None:
        def report(candidate: str, auc: float, gate_passes: int) -> dict:
            return {
                "candidate_id": candidate,
                "fold_count": 4,
                "aggregate_metrics": {
                    "mean_roc_auc": auc,
                    "minimum_roc_auc": auc,
                    "positive_auc_fold_count": 4,
                    "mean_balanced_accuracy": 0.53,
                    "mean_brier_skill_score": 0.01,
                    "pair_auc_above_0p5_fraction": 0.7,
                    "mean_expected_calibration_error": 0.05,
                },
            }

        ranked = rank_reports([report("b", 0.56, 0), report("a", 0.57, 0)])
        self.assertEqual(ranked[0]["candidate_id"], "a")

    def test_safety_contract_disables_all_execution(self) -> None:
        safety = safety_fields()
        for key in (
            "network_accessed",
            "credentials_accessed",
            "broker_api_used",
            "order_placement_used",
            "live_execution_enabled",
            "demo_execution_enabled",
            "oanda_execution_enabled",
            "mt5_execution_enabled",
            "production_ready",
        ):
            self.assertFalse(safety[key])

    def test_custom_selector_has_stable_joblib_module_identity(self) -> None:
        selector = StableFSelector(k=2).fit(
            np.array([[0.0, 1.0, 2.0], [1.0, 0.0, 3.0], [2.0, 1.0, 4.0], [3.0, 0.0, 5.0]]),
            np.array([0, 0, 1, 1]),
        )
        self.assertEqual(
            StableFSelector.__module__,
            "fresh_m1_intrahour.src.forecast_transformers",
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "selector.joblib"
            joblib.dump(selector, path)
            restored = joblib.load(path)
        np.testing.assert_array_equal(restored.get_support(), selector.get_support())

    @staticmethod
    def _prediction_frame(
        probabilities: list[float],
        actual: list[int],
        *,
        target_id: str,
    ) -> pd.DataFrame:
        rows = len(probabilities)
        return pd.DataFrame(
            {
                "forecast_row_id": np.arange(rows),
                "instrument": ["EUR_USD"] * rows,
                "decision_time_utc": pd.date_range("2025-01-01", periods=rows, freq="h", tz="UTC"),
                "label_end_time_utc": pd.date_range("2025-01-01T02:00:00Z", periods=rows, freq="h"),
                "momentum_30_atr": np.zeros(rows),
                "long_gross_endpoint_pips": np.ones(rows),
                "short_gross_endpoint_pips": -np.ones(rows),
                "long_endpoint_net_pips": np.ones(rows),
                "short_endpoint_net_pips": -np.ones(rows),
                "fold_id": ["fold_a"] * (rows // 2) + ["fold_b"] * (rows - rows // 2),
                "target_id": [target_id] * rows,
                "candidate_id": ["candidate"] * rows,
                "actual": actual,
                "probability": probabilities,
                "train_prior": [0.5] * rows,
            }
        )

    def test_two_stage_threshold_is_selected_on_development_predictions(self) -> None:
        actual = [1, 0, 1, 0, 1, 0, 1, 0]
        direction = self._prediction_frame(
            [0.9, 0.1, 0.49, 0.51, 0.9, 0.1, 0.49, 0.51],
            actual,
            target_id=PRIMARY_TARGET,
        )
        opportunity = self._prediction_frame(
            [0.9] * 8,
            [1] * 8,
            target_id=benchmark.OPPORTUNITY_TARGET,
        )
        diagnostic_direction = direction.copy()
        diagnostic_direction["probability"] = 1.0 - diagnostic_direction["probability"]
        diagnostic_opportunity = opportunity.copy()
        with (
            patch.object(benchmark, "TWO_STAGE_OPPORTUNITY_THRESHOLDS", (0.5,)),
            patch.object(benchmark, "TWO_STAGE_DIRECTION_CONFIDENCES", (0.0, 0.05)),
            patch.object(benchmark, "TWO_STAGE_MINIMUM_ROWS", 4),
            patch.object(benchmark, "TWO_STAGE_MINIMUM_ROWS_PER_FOLD", 2),
        ):
            report, development, diagnostic = evaluate_two_stage_forecast(
                direction,
                opportunity,
                diagnostic_direction,
                diagnostic_opportunity,
            )
        self.assertEqual(
            report["selected_development_policy"]["direction_confidence"],
            0.05,
        )
        self.assertEqual(int(development["selected_by_two_stage_policy"].sum()), 4)
        self.assertEqual(int(diagnostic["selected_by_two_stage_policy"].sum()), 4)
        self.assertFalse(report["confirmed_accurate_directional_forecast"])
        self.assertFalse(report["fresh_holdout"])


if __name__ == "__main__":
    unittest.main()
