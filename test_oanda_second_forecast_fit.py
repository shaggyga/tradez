from __future__ import annotations

import unittest

import numpy as np

try:
    from oanda_second_forecast_fit import (
        adaptive_sample_steps,
        apply_multi_horizon_smoothing,
        model_artifact_replacement_decision,
        parse_smoothing_lambdas,
        pooled_proxy_model,
        reconcile_fit_report,
        smooth_horizon_parameters,
    )
except ModuleNotFoundError:
    from trad.oanda_second_forecast_fit import (
        adaptive_sample_steps,
        apply_multi_horizon_smoothing,
        model_artifact_replacement_decision,
        parse_smoothing_lambdas,
        pooled_proxy_model,
        reconcile_fit_report,
        smooth_horizon_parameters,
    )


def draft_model(intercept: float, coefficient: float) -> dict:
    rows = 120
    entry_bid = np.full(rows, 1.1000)
    entry_ask = np.full(rows, 1.1001)
    future_bid = np.full(rows, 1.1003)
    future_ask = np.full(rows, 1.1004)
    return {
        "feature_means": [0.0],
        "feature_scales": [1.0],
        "intercept": intercept,
        "coefficients": [coefficient],
        "_selection_context": {
            "validation_x": np.ones((rows, 1)),
            "validation_actual": np.full(rows, 2.5),
            "validation_entry_bid": entry_bid,
            "validation_entry_ask": entry_ask,
            "validation_future_bid": future_bid,
            "validation_future_ask": future_ask,
            "validation_spread": np.full(rows, 1.0),
            "holdout_x": np.ones((rows, 1)),
            "holdout_actual": np.full(rows, 2.5),
            "holdout_entry_bid": entry_bid,
            "holdout_entry_ask": entry_ask,
            "holdout_future_bid": future_bid,
            "holdout_future_ask": future_ask,
            "multiplier": 10_000.0,
            "selected_orientation": "direct",
        },
    }


class SecondForecastFitTests(unittest.TestCase):
    def test_zero_lambda_preserves_independent_horizon_parameters(self):
        models = {
            "60": draft_model(1.0, 2.0),
            "300": draft_model(3.0, 4.0),
        }
        parameters = smooth_horizon_parameters(models, 0.0)
        self.assertAlmostEqual(parameters[60][0], 1.0)
        self.assertAlmostEqual(parameters[60][1][0], 2.0)
        self.assertAlmostEqual(parameters[300][0], 3.0)
        self.assertAlmostEqual(parameters[300][1][0], 4.0)

    def test_positive_lambda_blends_only_adjacent_horizon_surfaces(self):
        models = {
            "60": draft_model(0.0, 1.0),
            "300": draft_model(0.0, 3.0),
            "3600": draft_model(0.0, 9.0),
        }
        parameters = smooth_horizon_parameters(models, 0.35)
        self.assertGreater(parameters[60][1][0], 1.0)
        self.assertLess(parameters[60][1][0], 3.0)
        self.assertGreater(parameters[300][1][0], 1.0)
        self.assertLess(parameters[300][1][0], 9.0)
        self.assertGreater(parameters[3600][1][0], 3.0)
        self.assertLess(parameters[3600][1][0], 9.0)

    def test_smoothing_selection_removes_private_fit_context(self):
        models = {
            "60": draft_model(1.0, 0.0),
            "300": draft_model(1.0, 0.0),
        }
        result = apply_multi_horizon_smoothing(models, [0.0, 0.35])
        self.assertEqual(result["selected_lambda"], 0.0)
        self.assertIn("holdout_improvement", result)
        self.assertIn("holdout_improved", result)
        self.assertTrue(all("_selection_context" not in model for model in models.values()))
        self.assertTrue(all("profiles" in model for model in models.values()))

    def test_smoothing_lambda_parser_always_includes_independent_baseline(self):
        self.assertEqual(parse_smoothing_lambdas("0.75, 0.15"), [0.0, 0.15, 0.75])

    def test_adaptive_sampling_retries_denser_s5_rows_without_duplicates(self):
        self.assertEqual(adaptive_sample_steps(4), [4, 2, 1])
        self.assertEqual(adaptive_sample_steps(1), [1])

    def test_pooled_proxy_is_visible_but_never_account_eligible(self):
        donor = {
            "fit_provenance": "pair_specific",
            "feature_means": [0.0],
            "feature_scales": [1.0],
            "intercept": 1.0,
            "coefficients": [2.0],
            "magnitude_calibration": 1.0,
            "residual_std_pips": 1.0,
            "profiles": {
                profile: {
                    "score_threshold": 0.5,
                    "max_spread_pips": 2.0,
                    "holdout": {"n": 50, "lower_95_pips": 0.2},
                }
                for profile in ("fast", "balanced", "strict")
            },
        }
        proxy = pooled_proxy_model(
            "NZD_USD",
            14400,
            {
                "EUR_USD": {"14400": donor},
                "GBP_USD": {"14400": {**donor, "intercept": 2.0}},
            },
        )
        self.assertIsNotNone(proxy)
        self.assertEqual(proxy["fit_provenance"], "quote_currency_pooled_proxy")
        self.assertFalse(proxy["pair_specific_evidence"])
        self.assertFalse(proxy["account_eligible"])
        self.assertTrue(
            all(
                not profile["historical_gate_passed"]
                for profile in proxy["profiles"].values()
            )
        )

    def test_empty_unattended_fit_cannot_replace_trained_artifact(self):
        decision = model_artifact_replacement_decision(
            {
                "model_count": 0,
                "pair_specific_model_count": 0,
                "complete_horizon_pair_count": 0,
                "horizons_sec": [60, 300],
                "models": {},
            },
            {
                "model_count": 26,
                "pair_specific_model_count": 24,
                "complete_horizon_pair_count": 2,
                "horizons_sec": [60, 300],
                "models": {"EUR_USD": {"60": {}, "300": {}}},
            },
        )

        self.assertFalse(decision["replaced"])
        self.assertIn("model_count_regression", decision["blockers"])

    def test_explicit_override_allows_intentional_narrower_refit(self):
        decision = model_artifact_replacement_decision(
            {"model_count": 1, "horizons_sec": [60], "models": {}},
            {"model_count": 2, "horizons_sec": [60, 300], "models": {}},
            allow_coverage_regression=True,
        )

        self.assertTrue(decision["replaced"])
        self.assertEqual(decision["reason"], "explicit_coverage_regression_override")

    def test_rejected_fit_preserves_matching_incumbent_report(self):
        update = {
            "replaced": False,
            "reason": "incumbent_preserved",
            "candidate": {"model_count": 0},
            "incumbent": {"model_count": 26},
        }
        incumbent = {
            "fitted_utc": "2026-07-20T08:00:00+00:00",
            "model_count": 26,
            "historical_gate_passes": 4,
        }
        candidate = {
            "fitted_utc": "2026-07-21T23:00:00+00:00",
            "model_count": 0,
            "source_root": "missing-history",
            "errors": {},
        }

        report = reconcile_fit_report(candidate, incumbent, update)

        self.assertEqual(report["model_count"], 26)
        self.assertEqual(report["historical_gate_passes"], 4)
        self.assertEqual(report["latest_rejected_fit"]["coverage"]["model_count"], 0)


if __name__ == "__main__":
    unittest.main()
