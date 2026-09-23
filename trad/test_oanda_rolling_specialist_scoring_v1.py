import copy
import json

import numpy as np
import pytest

from oanda_rolling_model_scoring_v1 import _decision_digest
from oanda_rolling_specialist_scoring_v1 import component_diagnostics, score_specialist_variant
from tools.run_rolling_model_comparison_v1 import score_variant


def fixture(h=30):
    base = 1800000000
    per_pair = 36
    clocks = np.r_[base + np.arange(24) * 60,
                   base + 86400 + np.array([0, 60, 1800, 1860, 3600, 5400]),
                   base + 172800 + np.array([0, 60, 1800, 1860, 3600, 5400])]
    split = np.tile(np.r_[np.zeros(24), np.ones(6), np.full(6, 2)], 2).astype(np.int8)
    n = len(split)
    d = {"pair_names": ["EUR_USD", "USD_JPY"], "pair_id": np.repeat([0, 1], per_pair),
         "time": np.tile(clocks, 2), "split": split, "spread": np.full(n, .8)}
    y = np.tile([0., 2., -1., 4., -2., 1.], n // 6)
    for horizon in (30, 60):
        d[f"y_{horizon}"] = y.copy()
        d[f"long_{horizon}"] = y - .2 - .5
        d[f"short_{horizon}"] = -y - .6 - .3
        d[f"valid_{horizon}"] = np.ones(n, bool)
        d[f"strict_{horizon}"] = np.arange(n) % 3 != 0
        d[f"eligible_{horizon}"] = np.ones(n, bool)
        d[f"arima_{horizon}"] = np.ones(n)
        d[f"momentum_{horizon}"] = np.ones(n)
        d[f"delay_long_{horizon}"] = d[f"long_{horizon}"] - .15
        d[f"delay_short_{horizon}"] = d[f"short_{horizon}"] - .15
        d[f"delay_valid_{horizon}"] = np.ones(n, bool)
    rows = np.flatnonzero(split > 0); count = len(rows)
    mean = np.tile([3., -3., 0., 3., 3., -3.], count // 6)
    prior = np.full(count, .2)
    entry_l, entry_s = np.full(count, .2), np.full(count, .6)
    heads = {"probability": np.full(count, .7), "direct": mean.copy(),
             "positive": np.full(count, 3.), "nonpositive": np.ones(count),
             "long_exit": np.full(count, .4), "short_exit": np.full(count, .1)}
    return d, rows, mean, prior, entry_l, entry_s, heads


def score(parts, h=30):
    d, rows, mean, prior, el, es, heads = parts
    return score_specialist_variant(d, rows, mean, h, prior, el, es, heads["long_exit"], heads["short_exit"])


def components(parts, h=30, **kwargs):
    d, rows, mean, prior, el, es, heads = parts
    return component_diagnostics(d, rows, mean, h, prior, el, es, heads, **kwargs)


def test_primary_is_identical_to_sealed_runner_and_inputs_unchanged():
    parts = fixture(); original = copy.deepcopy(parts)
    d, rows, mean, prior, *_ = parts
    result = score(parts)
    assert result["primary_current_spread"] == score_variant(d, rows, mean, 30, prior)
    for k, v in d.items():
        np.testing.assert_array_equal(v, original[0][k])
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("h", [30, 60])
def test_expected_gate_matches_legacy_asymmetric_decision_and_original_clock_reservations(h):
    parts = fixture(h); d, rows, mean, prior, el, es, heads = parts
    # Both signs, exact threshold equality, flat forecast, and opposite-side costs.
    el[:] = .25; es[:] = .5; d["spread"][:] = .75
    heads["long_exit"][:] = .25; heads["short_exit"][:] = .125
    mean[:] = np.tile([1.5, -1.625, 0., 1.501, -1.626, 3.], 4)
    result = score(parts, h)
    for split_id, split_name in ((1, "validation"), (2, "later_development_test")):
        m = d["split"][rows] == split_id; r = rows[m]
        expected_long = mean[m] - el[m] - heads["long_exit"][m]
        expected_short = -mean[m] - es[m] - heads["short_exit"][m]
        legacy = np.where((expected_long > expected_short) & (expected_long > 1.), 1,
                          np.where((expected_short > expected_long) & (expected_short > 1.), -1, 0))
        expected_cost = np.where(mean[m] > 0, el[m] + heads["long_exit"][m], es[m] + heads["short_exit"][m])
        chosen = np.where(np.abs(mean[m]) > expected_cost + 1., np.sign(mean[m]), 0.)
        np.testing.assert_array_equal(chosen, legacy)
        t = d["time"][r]; pairs = np.array(d["pair_names"])[d["pair_id"][r]]
        take = np.zeros(len(t), bool); last = {}
        for i in range(len(t)):
            if chosen[i] and (pairs[i] not in last or t[i] >= last[pairs[i]] + h * 60):
                take[i] = True; last[pairs[i]] = t[i]
        policy = result["secondary_expected_cost"][split_name]["primary"]["policies"]["expected_cost_threshold_nonoverlap"]
        assert policy["decisions"] == int(take.sum())
        assert policy["decision_pair_clock_side_sha256"] == _decision_digest(take, t, pairs, chosen)


def test_future_and_comparator_masks_never_change_either_gate_decisions():
    parts = fixture(); before = score(parts)
    d, rows, *_ = parts
    d["valid_30"][rows[::2]] = False
    d["strict_30"] &= d["valid_30"]
    d["eligible_30"][rows[::3]] = False
    d["arima_30"][rows[::4]] = np.nan
    for field in ("y_30", "long_30", "short_30"):
        d[field][rows[::2]] = np.nan
    after = score(parts)
    for gate in ("primary_current_spread", "secondary_expected_cost"):
        for split in ("validation", "later_development_test"):
            for policy in before[gate][split]["primary"]["policies"]:
                a = before[gate][split]["primary"]["policies"][policy]
                b = after[gate][split]["primary"]["policies"][policy]
                assert a["decision_pair_clock_side_sha256"] == b["decision_pair_clock_side_sha256"]
                assert a["decisions"] == b["decisions"]
            assert after[gate][split]["primary"]["issued_forecasts"] == 12


def test_delay_uses_original_expected_gate_even_with_missing_delayed_targets():
    parts = fixture(); d, rows, *_ = parts
    d["delay_valid_30"][rows[::2]] = False
    d["delay_long_30"][rows] -= 10.
    d["delay_short_30"][rows] -= 20.
    result = score(parts)
    for gate in ("primary_current_spread", "secondary_expected_cost"):
        for period in result[gate].values():
            a, b = period["primary"], period["one_minute_entry_delay"]
            for policy in a["policies"]:
                assert a["policies"][policy]["decision_pair_clock_side_sha256"] == b["policies"][policy]["decision_pair_clock_side_sha256"]
                assert a["policies"][policy]["decisions"] == b["policies"][policy]["decisions"]
    report = result["secondary_expected_cost"]["validation"]["primary"]
    assert "origin_spread_threshold" not in report["policies"]
    assert "issued_without_valid_origin_spread" not in report
    assert not report["decision_rule"]["gate_cost_is_observed_spread"]
    assert report["observed_current_spread_diagnostic"]["mean_bps"] == pytest.approx(.8)


@pytest.mark.parametrize("field,value", [("entry_long", -1.), ("entry_short", np.nan),
                                        ("long_exit", np.inf), ("short_exit", -.1)])
def test_invalid_costs_do_not_become_free_or_fall_back(field, value):
    parts = fixture()
    target = parts[4] if field == "entry_long" else parts[5] if field == "entry_short" else parts[6][field]
    target[0] = value
    with pytest.raises(ValueError, match="finite_nonnegative_cost"):
        score(parts)


def test_scalar_broadcast_wrong_population_and_bad_entry_basis_refused():
    parts = list(fixture())
    parts[4] = .2
    with pytest.raises(ValueError, match="one_dimensional"):
        score(parts)
    parts = list(fixture()); parts[1] = parts[1][:-1]
    with pytest.raises(ValueError, match="all_original"):
        score(parts)
    parts = fixture(); parts[4][0] += .1
    with pytest.raises(ValueError, match="sum_to_observed"):
        score(parts)


def test_component_flat_semantics_prior_denominators_and_conditional_errors():
    parts = fixture(); pp = np.full(len(parts[1]), .5); pp[0] = np.nan
    report = components(parts, prior_positive=pp)
    full = report["periods"]["validation"]["cohorts"]["full_endpoint"]
    assert full["scored_origins"] == 12
    assert full["actual_positive"] == 6 and full["actual_nonpositive"] == 6
    assert full["actual_flat"] == 2
    assert full["positive_conditional_magnitude"]["rows"] == 6
    assert full["nonpositive_conditional_magnitude"]["rows"] == 6
    prob = full["positive_probability"]
    assert prob["model"]["rows"] == prob["train_pair_prior"]["rows"] == 11
    assert prob["excluded_missing_train_prior"] == 1
    assert full["nonpositive_conditional_magnitude"]["mae_bps"] == pytest.approx(2 / 3)
    assert full["expected_absolute_move"]["rows"] == 12
    json.dumps(report, allow_nan=False)


def test_selection_optimism_exact_identity_unique_clocks_and_missing_future_reservation():
    parts = fixture(); d, rows, *_ = parts
    d["valid_30"][rows[0]] = False; d["strict_30"][rows[0]] = False
    scores = score(parts); diag = components(parts)
    for split in ("validation", "later_development_test"):
        for cohort in ("full_endpoint", "shared_strict", "additional_endpoint_only"):
            comp = diag["periods"][split]["cohorts"][cohort]
            for gate, policy in (("primary_current_spread", "origin_spread_threshold_nonoverlap"),
                                 ("secondary_expected_cost", "expected_cost_threshold_nonoverlap")):
                c = comp["selection"][gate]; s = scores[gate][split]["primary"]["policies"][policy]
                assert c["all_original_decisions"]["rows"] == s["decisions"]
                assert c["decision_pair_clock_side_sha256"] == s["decision_pair_clock_side_sha256"]
                assert c["scored_decisions"]["rows"] == s["cohorts"][cohort]["scored_decisions"]
                if c["scored_decisions"]["rows"]:
                    assert c["mean_expected_minus_actual_net_bps"] == pytest.approx(c["mean_selected_return_optimism_bps"] + c["mean_selected_exit_cost_optimism_bps"])
                    assert c["maximum_decomposition_residual_bps"] < 1e-8
                    assert c["mean_actual_net_after_extra_1bp_bps"] == pytest.approx(s["cohorts"][cohort]["cost_scenarios"]["1.0"]["mean_net_bps"])
    pop = diag["periods"]["validation"]["cohorts"]["full_endpoint"]["selection"]["primary_current_spread"]["all_original_decisions"]
    assert pop["rows"] > pop["unique_original_utc_decision_times"]
    assert pop["pairs"] == 2 and pop["utc_days"] == 1


def test_train_positive_prior_ignores_future_and_counts_flats_as_false():
    parts = fixture(); before = components(parts)
    # Modify assessment outcomes coherently, keeping endpoint cost identities.
    d, rows, *_ = parts
    d["y_30"][rows] = 100.
    d["long_30"][rows] = 99.3
    d["short_30"][rows] = -100.9
    after = components(parts)
    # TRAIN contains exactly half positive; its prior Brier on all-positive is .25.
    p = after["periods"]["validation"]["cohorts"]["full_endpoint"]["positive_probability"]
    assert p["train_pair_prior"]["brier"] == pytest.approx(.25)
    assert p["train_pair_prior"]["log_loss"] == pytest.approx(np.log(2))
    assert before["prior_scope"] == after["prior_scope"]


def test_negative_magnitude_out_of_range_probability_and_impossible_exit_labels_refused():
    parts = fixture(); parts[6]["positive"][0] = -1.
    with pytest.raises(ValueError, match="must_be_nonnegative"):
        components(parts)
    parts = fixture(); parts[6]["probability"][0] = 1.01
    with pytest.raises(ValueError, match="unit_interval"):
        components(parts)
    parts = fixture(); parts[0]["long_30"][parts[1][0]] += 10.
    with pytest.raises(ValueError, match="negative_actual_exit"):
        components(parts)


def test_no_decisions_keeps_forecasts_and_null_selected_metrics():
    parts = fixture(); parts[2][:] = 0.
    score_result = score(parts); diag = components(parts)
    assert score_result["primary_current_spread"]["validation"]["primary"]["issued_forecasts"] == 12
    for cohort in diag["periods"]["validation"]["cohorts"].values():
        for selection in cohort["selection"].values():
            assert selection["all_original_decisions"]["rows"] == 0
            assert selection["scored_decisions"]["rows"] == 0
            assert selection["mean_expected_minus_actual_net_bps"] is None


def test_distinct_gate_decisions_do_not_change_forecast_errors_or_actual_cost_scenarios():
    parts = fixture(); parts[2][:] = 1.7
    result = score(parts)
    for split in ("validation", "later_development_test"):
        a = result["primary_current_spread"][split]["primary"]
        b = result["secondary_expected_cost"][split]["primary"]
        assert a["forecast_cohorts"] == b["forecast_cohorts"]
        assert a["policies"]["origin_spread_threshold_nonoverlap"]["decisions"] == 0
        policy = b["policies"]["expected_cost_threshold_nonoverlap"]
        assert policy["decisions"] > 0
        costs = policy["cohorts"]["full_endpoint"]["cost_scenarios"]
        assert set(costs) == {"0.0", "1.0", "2.0"}
        assert costs["0.0"]["mean_net_bps"] - costs["1.0"]["mean_net_bps"] == pytest.approx(1.)
        assert costs["1.0"]["mean_net_bps"] - costs["2.0"]["mean_net_bps"] == pytest.approx(1.)


@pytest.mark.parametrize("change,error", [("duplicate_clock", "unique_ascending"),
                                         ("unaligned_clock", "aligned_utc"),
                                         ("integer_mask", "boolean_mask"),
                                         ("negative_pair_id", "pair_identity")])
def test_standalone_components_refuse_misaligned_population(change, error):
    parts = fixture(); d, rows, *_ = parts
    if change == "duplicate_clock": d["time"][rows[1]] = d["time"][rows[0]]
    if change == "unaligned_clock": d["time"][rows[0]] += 1
    if change == "integer_mask": d["valid_30"] = d["valid_30"].astype(int)
    if change == "negative_pair_id": d["pair_id"][rows[0]] = -1
    with pytest.raises(ValueError, match=error):
        components(parts)


def test_unavailable_mean_keeps_original_origin_and_component_denominator_explicit():
    parts = fixture(); parts[2][0] = np.nan
    result = score(parts); diagnostic = components(parts)
    p = result["primary_current_spread"]["validation"]["primary"]
    assert p["input_origins"] == 12 and p["issued_forecasts"] == 11
    assert p["unavailable_predictions"] == 1
    d = diagnostic["periods"]["validation"]
    assert d["original_origins"] == 12 and d["finite_variant_forecasts"] == 11
    assert d["cohorts"]["full_endpoint"]["scored_origins"] == 11
