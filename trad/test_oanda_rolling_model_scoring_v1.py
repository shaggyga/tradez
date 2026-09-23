"""Independent synthetic scoring checks; no models, files or live services."""
from __future__ import annotations

import copy
import hashlib
import json
import struct
import unittest
from unittest import mock

import numpy as np

from oanda_rolling_model_scoring_v1 import evaluate_predictions
import oanda_rolling_model_scoring_v1 as scoring


BASE = 1_700_006_400  # UTC midnight and exact minute boundary.


def arguments(predictions, *, offsets=None, pairs=None, actual=None):
    n = len(predictions)
    return {
        "times": BASE + np.asarray(np.arange(n) if offsets is None else offsets) * 60,
        "pairs": ["EUR_USD"] * n if pairs is None else pairs,
        "predicted_bps": np.asarray(predictions, dtype=float),
        "return_bps": np.asarray(predictions if actual is None else actual, dtype=float),
        "long_net_bps": np.full(n, 3.), "short_net_bps": np.full(n, -7.),
        "endpoint_valid": np.ones(n, dtype=bool), "strict_valid": np.ones(n, dtype=bool),
        "split_eligible": np.ones(n, dtype=bool), "entry_spread_bps": np.ones(n),
        "horizon_minutes": 1,
    }


def policy(report, name="origin_spread_threshold_nonoverlap", cohort="full_endpoint", cost="1.0"):
    return report["policies"][name]["cohorts"][cohort]["cost_scenarios"][cost]


class SameOriginScoringTests(unittest.TestCase):
    def test_both_direction_costs_and_no_change_baseline(self):
        args = arguments([5, -5, 0])
        args["long_net_bps"] = np.array([3., -7., -2.])
        args["short_net_bps"] = np.array([-7., 3., -2.])
        report = evaluate_predictions(**args)
        self.assertEqual(report["issued_forecasts"], 3)
        self.assertEqual(report["issued_predicted_flat"], 1)
        score = report["forecast_cohorts"]["full_endpoint"]
        self.assertEqual(score["mae_bps"], 0)
        self.assertAlmostEqual(score["no_change_baseline"]["mae_bps"], 10 / 3)
        self.assertAlmostEqual(score["no_change_baseline"]["rmse_bps"], np.sqrt(50 / 3))
        self.assertEqual(score["direction"]["exact_sign_accuracy_including_flat"], 1)
        for cost, expected in (("0.0", 3.), ("1.0", 2.), ("2.0", 1.)):
            self.assertEqual(policy(report, cost=cost)["scored_decisions"], 2)
            self.assertEqual(policy(report, cost=cost)["mean_net_bps"], expected)
            self.assertEqual(policy(report, cost=cost)["positive_decisions"], 2)
        decisions = report["policies"]["origin_spread_threshold_nonoverlap"]
        self.assertEqual(decisions["long_decisions"], 1)
        self.assertEqual(decisions["short_decisions"], 1)

    def test_threshold_uses_only_origin_spread_and_fixed_margin(self):
        args = arguments([2., 2.0001, 4., -4.])
        args["entry_spread_bps"] = np.array([1., 1., 3., 2.])
        args["long_net_bps"][:] = -100.
        args["short_net_bps"][:] = -200.
        before = evaluate_predictions(**args)
        self.assertEqual(before["policies"]["origin_spread_threshold"]["decisions"], 2)
        after = evaluate_predictions(**args, extra_cost_bps=50.)
        for name in before["policies"]:
            self.assertEqual(before["policies"][name]["decision_pair_clock_side_sha256"], after["policies"][name]["decision_pair_clock_side_sha256"])
            self.assertEqual(before["policies"][name]["decisions"], after["policies"][name]["decisions"])
        self.assertEqual(after["decision_rule"]["fixed_margin_bps"], 1.)
        self.assertEqual(after["evaluation_cost"]["primary_scenario_key"], "50.0")

    def test_missing_future_target_still_reserves_holding_interval(self):
        args = arguments([5, 5, 5], offsets=[0, 5, 10])
        args["horizon_minutes"] = 10
        baseline = evaluate_predictions(**args)
        args["endpoint_valid"][0] = False
        args["strict_valid"][0] = False
        for name in ("return_bps", "long_net_bps", "short_net_bps"):
            args[name][0] = np.nan
        report = evaluate_predictions(**args)
        self.assertEqual(report["issued_forecasts"], 3)
        self.assertEqual(report["issued_invalid_future_endpoints"], 1)
        self.assertEqual(report["threshold_candidates_blocked_by_holding_window"], 1)
        for name in report["policies"]:
            self.assertEqual(report["policies"][name]["decision_pair_clock_side_sha256"], baseline["policies"][name]["decision_pair_clock_side_sha256"])
        nonoverlap = report["policies"]["origin_spread_threshold_nonoverlap"]
        self.assertEqual(nonoverlap["decisions"], 2)
        self.assertEqual(nonoverlap["decisions_with_invalid_future_endpoint"], 1)
        self.assertEqual(policy(report)["scored_decisions"], 1)

    def test_comparison_and_split_masks_cannot_change_reservations(self):
        args = arguments([5, 5, 5, 5], offsets=[0, 5, 10, 15])
        args["horizon_minutes"] = 10
        baseline = evaluate_predictions(**args)
        args["split_eligible"][2] = False
        mask = np.array([False, True, True, True])
        report = evaluate_predictions(**args, comparison_origin_mask=mask)
        for name in report["policies"]:
            self.assertEqual(report["policies"][name]["decisions"], baseline["policies"][name]["decisions"])
            self.assertEqual(report["policies"][name]["decision_pair_clock_side_sha256"], baseline["policies"][name]["decision_pair_clock_side_sha256"])
        nonoverlap = report["policies"]["origin_spread_threshold_nonoverlap"]
        self.assertEqual(nonoverlap["decisions"], 2)
        self.assertEqual(nonoverlap["decisions_excluded_by_comparison_mask"], 1)
        self.assertEqual(nonoverlap["decisions_excluded_by_split_boundary"], 1)
        self.assertEqual(nonoverlap["decisions_with_invalid_future_endpoint"], 0)
        self.assertEqual(policy(report)["scored_decisions"], 0)
        self.assertEqual(sum(nonoverlap["unscored_reason_partition"].values()), 2)
        self.assertEqual(report["forecast_cohorts"]["full_endpoint"]["scored_forecasts"], 2)

    def test_gaps_and_exact_holding_boundary_do_not_imply_intermediate_entries(self):
        args = arguments([5, 5, 5, 5], offsets=[0, 5, 10, 100])
        args["horizon_minutes"] = 10
        report = evaluate_predictions(**args)
        self.assertEqual(report["policies"]["origin_spread_threshold"]["decisions"], 4)
        self.assertEqual(report["policies"]["origin_spread_threshold_nonoverlap"]["decisions"], 3)
        self.assertEqual(report["threshold_candidates_blocked_by_holding_window"], 1)

    def test_independent_pairs_can_share_clock_without_portfolio_claim(self):
        args = arguments([5, -5, 5, -5], offsets=[0, 0, 5, 5],
                         pairs=["EUR_USD", "USD_JPY", "EUR_USD", "USD_JPY"])
        args["horizon_minutes"] = 10
        report = evaluate_predictions(**args)
        self.assertEqual(report["policies"]["origin_spread_threshold_nonoverlap"]["decisions"], 2)
        self.assertIn("no fills, portfolio/account return", report["scope"])

    def test_explicit_flat_direction_accounting(self):
        args = arguments([1, 0, -1, 0], actual=[0, 1, -1, 0])
        report = evaluate_predictions(**args)
        direction = report["forecast_cohorts"]["full_endpoint"]["direction"]
        self.assertEqual(direction["predicted_flat"], 2)
        self.assertEqual(direction["actual_flat"], 2)
        self.assertEqual(direction["exact_sign_accuracy_including_flat"], .5)
        self.assertEqual(direction["direction_accuracy_on_nonflat_actuals"], .5)
        self.assertEqual(direction["predicted_flat_on_nonflat_actuals"], 1)
        self.assertEqual(direction["direction_accuracy_on_called_nonflat_outcomes"], 1.)
        self.assertEqual(direction["called_coverage_of_nonflat_actuals"], .5)

    def test_full_shared_additional_cohorts_partition_scores_without_changing_decisions(self):
        args = arguments([5, -5, 5], actual=[5, -3, -2])
        args["strict_valid"] = np.array([True, False, False])
        args["long_net_bps"] = np.array([3., -5., -4.])
        args["short_net_bps"] = np.array([-7., 1., 0.])
        report = evaluate_predictions(**args)
        forecast = report["forecast_cohorts"]
        self.assertEqual(forecast["full_endpoint"]["scored_forecasts"], 3)
        self.assertEqual(forecast["shared_strict"]["scored_forecasts"], 1)
        self.assertEqual(forecast["additional_endpoint_only"]["scored_forecasts"], 2)
        full = policy(report)["sum_unit_notional_net_bps"]
        shared = policy(report, cohort="shared_strict")["sum_unit_notional_net_bps"]
        additional = policy(report, cohort="additional_endpoint_only")["sum_unit_notional_net_bps"]
        self.assertAlmostEqual(full, shared + additional)
        self.assertEqual(report["policies"]["origin_spread_threshold_nonoverlap"]["decisions"], 3)

    def test_missing_origin_spread_blocks_threshold_but_not_forecast_or_sign_diagnostic(self):
        args = arguments([5, 5, 5])
        args["entry_spread_bps"] = np.array([np.nan, -1., 1.])
        report = evaluate_predictions(**args)
        self.assertEqual(report["issued_forecasts"], 3)
        self.assertEqual(report["issued_without_valid_origin_spread"], 2)
        self.assertEqual(report["policies"]["all_issued_sign_diagnostic"]["decisions"], 3)
        self.assertEqual(report["policies"]["origin_spread_threshold"]["decisions"], 1)

    def test_day_pair_concentration_and_leave_best_day_out(self):
        args = arguments([5, 5, 5], offsets=[0, 60, 1440], pairs=["EUR_USD", "EUR_USD", "USD_JPY"])
        args["horizon_minutes"] = 60
        args["long_net_bps"] = np.array([3., 3., 0.])
        report = evaluate_predictions(**args)
        concentration = report["policies"]["origin_spread_threshold_nonoverlap"]["cohorts"]["full_endpoint"]
        days = concentration["primary_cost_day_concentration"]
        self.assertEqual(days["observed_group_count"], 2)
        self.assertEqual(days["best_group_share_of_positive_group_net"], 1.)
        self.assertEqual(days["leave_best_group_out_mean_net_bps"], -1.)
        self.assertEqual(days["ranked_groups"][0]["scored_decisions"], 2)
        pair = concentration["primary_cost_pair_concentration"]
        self.assertEqual(pair["best_group"], "EUR_USD")
        self.assertEqual(pair["leave_best_group_out_mean_net_bps"], -1.)

    def test_unavailable_predictions_are_distinct_from_missing_outcomes(self):
        args = arguments([np.nan, np.inf, 5], actual=[1, 2, np.nan])
        args["endpoint_valid"][2] = False
        args["strict_valid"][2] = False
        report = evaluate_predictions(**args)
        self.assertEqual(report["issued_forecasts"], 1)
        self.assertEqual(report["unavailable_predictions"], 2)
        self.assertAlmostEqual(report["forecast_coverage"], 1 / 3)
        self.assertEqual(report["issued_invalid_future_endpoints"], 1)
        self.assertEqual(report["forecast_cohorts"]["full_endpoint"]["scored_forecasts"], 0)
        self.assertEqual(report["policies"]["origin_spread_threshold_nonoverlap"]["decisions"], 1)

    def test_everything_comparison_excluded_is_not_called_future_invalid(self):
        args = arguments([5, 5, 5])
        report = evaluate_predictions(**args, comparison_origin_mask=np.zeros(3, dtype=bool))
        self.assertEqual(report["issued_forecasts"], 3)
        self.assertEqual(report["issued_invalid_future_endpoints"], 0)
        self.assertEqual(report["policies"]["origin_spread_threshold_nonoverlap"]["decisions"], 3)
        self.assertEqual(report["policies"]["origin_spread_threshold_nonoverlap"]["decisions_excluded_by_comparison_mask"], 3)
        self.assertEqual(policy(report)["scored_decisions"], 0)

    def test_empty_and_all_unavailable_results_are_json_safe(self):
        for args in (arguments([]), arguments([np.nan, np.nan], actual=[1, 2])):
            report = evaluate_predictions(**args)
            json.dumps(report, allow_nan=False)
            self.assertEqual(report["issued_forecasts"], 0)
            self.assertIsNone(report["forecast_cohorts"]["full_endpoint"]["mae_bps"])
            self.assertEqual(policy(report)["scored_decisions"], 0)
            self.assertIsNone(policy(report)["positive_fraction"])

    def test_decisions_and_input_arrays_are_not_mutated_by_scoring(self):
        args = arguments([5, -5, 0])
        saved = copy.deepcopy(args)
        evaluate_predictions(**args)
        for name, value in args.items():
            np.testing.assert_array_equal(value, saved[name])

    def test_random_sparse_multicurrency_policy_against_scalar_origin_oracle(self):
        rng = np.random.default_rng(41029)
        pairs = np.repeat(["EUR_USD", "USD_JPY", "GBP_USD"], 90)
        times = np.concatenate([np.cumsum(rng.integers(1, 16, 90)) for _ in range(3)])
        order = np.lexsort((pairs, times))
        pairs, times = pairs[order], times[order]
        prediction = rng.normal(0, 5, len(times))
        prediction[rng.random(len(times)) < .12] = np.nan
        args = arguments(prediction, offsets=times, pairs=pairs, actual=rng.normal(0, 4, len(times)))
        args["horizon_minutes"] = 30
        args["entry_spread_bps"] = rng.uniform(0, 3, len(times))
        args["entry_spread_bps"][rng.random(len(times)) < .1] = np.nan
        args["long_net_bps"] = args["return_bps"] - 2
        args["short_net_bps"] = -args["return_bps"] - 2
        args["endpoint_valid"] = rng.random(len(times)) > .15
        args["strict_valid"] = args["endpoint_valid"] & (rng.random(len(times)) > .25)
        args["split_eligible"] = rng.random(len(times)) > .1
        comparison = rng.random(len(times)) > .2
        report = evaluate_predictions(**args, comparison_origin_mask=comparison)
        sign_decisions, threshold_decisions, reserved_decisions = [], [], []
        held_until = {}
        for i in range(len(times)):
            if not np.isfinite(prediction[i]) or prediction[i] == 0:
                continue
            sign_decisions.append(i)
            spread = args["entry_spread_bps"][i]
            if not np.isfinite(spread) or abs(prediction[i]) <= spread + 1:
                continue
            threshold_decisions.append(i)
            if int(args["times"][i]) >= held_until.get(pairs[i], -1):
                reserved_decisions.append(i)
                held_until[pairs[i]] = int(args["times"][i]) + 30 * 60
        by_policy = {"all_issued_sign_diagnostic": sign_decisions, "origin_spread_threshold": threshold_decisions,
                     "origin_spread_threshold_nonoverlap": reserved_decisions}
        for name, issued in by_policy.items():
            self.assertEqual(report["policies"][name]["decisions"], len(issued))
            self.assertEqual(sum(report["policies"][name]["unscored_reason_partition"].values()), len(issued))
            for cohort in ("full_endpoint", "shared_strict", "additional_endpoint_only"):
                selected = [i for i in issued if args["endpoint_valid"][i] and args["split_eligible"][i] and comparison[i]
                            and (cohort == "full_endpoint" or bool(args["strict_valid"][i]) == (cohort == "shared_strict"))]
                base_net = [float(args["long_net_bps"][i] if prediction[i] > 0 else args["short_net_bps"][i]) for i in selected]
                for cost in (0., 1., 2.):
                    score = policy(report, name=name, cohort=cohort, cost=str(cost))
                    self.assertEqual(score["scored_decisions"], len(selected))
                    expected = [value - cost for value in base_net]
                    self.assertAlmostEqual(score["sum_unit_notional_net_bps"], sum(expected))
                    if expected:
                        self.assertAlmostEqual(score["mean_net_bps"], sum(expected) / len(expected))
                    else:
                        self.assertIsNone(score["mean_net_bps"])
                    self.assertEqual(score["positive_decisions"], sum(value > 0 for value in expected))

    def test_cost_keys_are_canonical_even_for_negative_zero(self):
        report = evaluate_predictions(**arguments([5]), extra_cost_bps=-0., stress_costs_bps=(0., 1., 2., 1.))
        self.assertEqual(report["evaluation_cost"]["primary_scenario_key"], "0.0")
        self.assertEqual(list(report["policies"]["origin_spread_threshold"]["cohorts"]["full_endpoint"]["cost_scenarios"]), ["0.0", "1.0", "2.0"])

    def test_packed_decision_hash_matches_independent_struct_encoding(self):
        args = arguments([-5, 5, 5], offsets=[0, 0, 1], pairs=["USD_JPY", "EUR_USD", "EUR_USD"])
        report = evaluate_predictions(**args)
        packed = b"".join(struct.pack("<7sqb", pair, time, side) for pair, time, side in
                          [(b"EUR_USD", BASE, 1), (b"EUR_USD", BASE + 60, 1), (b"USD_JPY", BASE, -1)])
        self.assertEqual(len(packed), 16 * 3)
        self.assertEqual(report["policies"]["all_issued_sign_diagnostic"]["decision_pair_clock_side_sha256"], hashlib.sha256(packed).hexdigest())
        self.assertEqual(report["decision_digest_format"]["record_bytes"], 16)
        self.assertEqual(report["decision_digest_format"]["version"], scoring.DECISION_DIGEST_FORMAT)

    def test_pair_syntax_validated_once_per_unique_name(self):
        args = arguments([5] * 100, offsets=list(range(50)) * 2,
                         pairs=["EUR_USD"] * 50 + ["USD_JPY"] * 50)
        with mock.patch.object(scoring, "PAIR_PATTERN", wraps=scoring.PAIR_PATTERN) as pattern:
            evaluate_predictions(**args)
        self.assertEqual(pattern.fullmatch.call_count, 2)

    def test_invalid_order_shapes_masks_and_horizons_refused(self):
        changes = [
            {"times": np.array([BASE, BASE, BASE + 60])},
            {"times": np.array([BASE + 60, BASE, BASE + 120])},
            {"times": np.array([BASE, BASE + 61, BASE + 120])},
            {"times": np.array([BASE, np.nan, BASE + 120])},
            {"endpoint_valid": np.array([1, 1, 1])},
            {"strict_valid": np.array([True])},
            {"comparison_origin_mask": np.array([1, 1, 1])},
            {"predicted_bps": np.array(["up", "down", "up"])},
            {"return_bps": np.array([np.nan, 1., 1.])},
            {"endpoint_valid": np.array([False, True, True])},
            {"pairs": ["EUR_EUR", "EUR_USD", "EUR_USD"]},
            {"pairs": np.array(["EUR_USD", None, "EUR_USD"], dtype=object)},
            {"pairs": np.array([b"EUR_USD"] * 3)},
            {"horizon_minutes": True}, {"horizon_minutes": 0}, {"horizon_minutes": 1.5},
            {"fixed_margin_bps": -1}, {"extra_cost_bps": np.inf},
            {"stress_costs_bps": (0, np.nan)},
        ]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                evaluate_predictions(**dict(arguments([5, 5, 5]), **change))


if __name__ == "__main__":
    unittest.main()
