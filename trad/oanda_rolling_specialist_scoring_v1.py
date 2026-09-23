"""Separate current-spread and predicted-exit-cost specialist diagnostics.

No fitting, selection or execution occurs here. Future masks affect scores only.
The sealed comparison runner supplies the unchanged primary policy. Its scorer
also supplies a separately labelled secondary policy using expected costs.
"""
from __future__ import annotations

import numpy as np

from oanda_rolling_model_scoring_v1 import PAIR_PATTERN, _decision_digest, _mean, _numeric, _rmse
from tools.run_rolling_model_comparison_v1 import score_variant, training_mask

SCHEMA = "rolling_specialist_scoring_v1_20260915"
SPLITS = ((1, "validation"), (2, "later_development_test"))
COHORTS = ("full_endpoint", "shared_strict", "additional_endpoint_only")
PROBABILITY_CLIP = 1e-12


def _assessment(data, assessment, mean, h, prior):
    if isinstance(h, (bool, np.bool_)) or not isinstance(h, (int, np.integer)) or h not in (30, 60):
        raise ValueError("specialist_horizon_must_be_30_or_60")
    rows = np.asarray(assessment)
    split = np.asarray(data["split"])
    if split.ndim != 1 or split.dtype.kind not in "iu" or not np.isin(split, [0, 1, 2]).all():
        raise ValueError("explicit_original_split_ids_required")
    expected = np.flatnonzero((split == 1) | (split == 2))
    if rows.dtype.kind not in "iu" or rows.ndim != 1 or not np.array_equal(rows, expected):
        raise ValueError("all_original_assessment_rows_in_original_order_required")
    rows = rows.astype(np.int64, copy=False)
    clocks = _numeric(data["time"], "time", len(split))
    if (not np.isfinite(clocks).all() or np.any(clocks < 0) or
            np.any(clocks > 253402300799) or np.any(clocks % 60)):
        raise ValueError("original_aligned_utc_minute_start_required")
    ids = np.asarray(data["pair_id"])
    names = data["pair_names"]
    if (ids.ndim != 1 or len(ids) != len(split) or ids.dtype.kind not in "iu" or
            any(not isinstance(pair, str) or not PAIR_PATTERN.fullmatch(pair) or pair[:3] == pair[4:] for pair in names) or
            len(set(names)) != len(names) or np.any(ids < 0) or np.any(ids >= len(names))):
        raise ValueError("explicit_valid_pair_identity_required")
    assessment_clocks, assessment_ids = clocks[rows], ids[rows]
    for pair_id in np.unique(assessment_ids):
        if np.any(np.diff(assessment_clocks[assessment_ids == pair_id]) <= 0):
            raise ValueError("unique_ascending_original_clocks_per_pair_required")
    for stem in ("arima", "momentum", "y", "long", "short"):
        value = np.asarray(data[f"{stem}_{h}"])
        if value.ndim != 1 or len(value) != len(split) or value.dtype.kind not in "iuf":
            raise ValueError("aligned_full_population_numeric_array_required:" + stem)
    for stem in ("valid", "strict", "eligible"):
        value = np.asarray(data[f"{stem}_{h}"])
        if value.ndim != 1 or len(value) != len(split) or value.dtype.kind != "b":
            raise ValueError("aligned_full_population_boolean_mask_required:" + stem)
    prediction, prior = _numeric(mean, "mean", len(rows)), _numeric(prior, "prior", len(rows))
    if np.isinf(prediction).any() or np.isinf(prior).any():
        raise ValueError("infinite_prediction_or_prior")
    common = (np.isfinite(data[f"arima_{h}"][rows]) &
              np.isfinite(data[f"momentum_{h}"][rows]) & np.isfinite(prior))
    return rows, prediction, prior, common


def _cost(value, name, n):
    value = _numeric(value, name, n)
    if not np.isfinite(value).all() or np.any(value < 0):
        raise ValueError("finite_nonnegative_cost_required:" + name)
    return value


def _costs(data, rows, mean, entry_long, entry_short, exit_long, exit_short):
    n = len(rows)
    el, es, xl, xs = (_cost(v, k, n) for k, v in (
        ("entry_long", entry_long), ("entry_short", entry_short),
        ("exit_long_pred", exit_long), ("exit_short_pred", exit_short)))
    with np.errstate(over="ignore", invalid="ignore"):
        entry_sum = el + es
        expected = np.where(mean > 0, el + xl, es + xs)
    if not np.isfinite(expected).all() or not np.isfinite(entry_sum).all():
        raise ValueError("derived_cost_overflow")
    spread = _numeric(data["spread"][rows], "observed_spread", n)
    if (not np.isfinite(spread).all() or np.any(spread < 0) or
            not np.allclose(entry_sum, spread, rtol=1e-9, atol=1e-8)):
        raise ValueError("asymmetric_entry_costs_must_sum_to_observed_current_spread")
    return el, es, xl, xs, spread, expected


def _description(values):
    return {"rows": len(values), "mean_bps": _mean(values),
            "minimum_bps": float(values.min()) if len(values) else None,
            "maximum_bps": float(values.max()) if len(values) else None}


def _relabel_expected(report, spread, expected):
    report["underlying_scorer_schema"] = report["schema"]
    report["schema"] = SCHEMA + "_expected_cost_gate"
    report["issued_without_valid_expected_gate_cost"] = report.pop("issued_without_valid_origin_spread")
    for old, new in (("origin_spread_threshold", "expected_cost_threshold"),
                     ("origin_spread_threshold_nonoverlap", "expected_cost_threshold_nonoverlap")):
        report["policies"][new] = report["policies"].pop(old)
    report["decision_rule"].update({
        "threshold": "abs(predicted_bps) > known_entry_cost_on_forecast_side_bps + predicted_exit_cost_on_forecast_side_bps + fixed_margin_bps",
        "gate_cost_is_observed_spread": False,
        "cost_source": "current known asymmetric entry cost plus same-side predicted exit cost; actual future exit cost is never an input",
        "side_rule": "sign of unchanged signed-mean forecast; nonnegative costs make this equivalent to the legacy greater-expected-net decision; exact ties wait",
    })
    report["observed_current_spread_diagnostic"] = _description(spread)
    report["expected_chosen_side_gate_cost_diagnostic"] = _description(expected)
    return report


def score_specialist_variant(data, assessment, mean, h, prior, entry_long, entry_short,
                             exit_long_pred, exit_short_pred):
    """Both policies, each with unchanged-origin primary and delayed scores.

    Every supplied vector is assessment-sized. Costs must be finite/nonnegative;
    missing predictions remain unissued, and never turn into zero forecasts.
    Primary output is exactly the existing ``score_variant`` return value.
    """
    rows, prediction, prior, _ = _assessment(data, assessment, mean, h, prior)
    _, _, _, _, spread, expected = _costs(data, rows, prediction, entry_long, entry_short,
                                          exit_long_pred, exit_short_pred)
    primary = score_variant(data, rows, prediction, h, prior)
    expected_data = dict(data)
    expected_data["spread"] = np.asarray(data["spread"], dtype=np.float64).copy()
    expected_data["spread"][rows] = expected
    secondary = score_variant(expected_data, rows, prediction, h, prior)
    for split_id, split_name in SPLITS:
        m = data["split"][rows] == split_id
        for scope in ("primary", "one_minute_entry_delay"):
            report = secondary[split_name][scope]
            if report["forecast_cohorts"] != primary[split_name][scope]["forecast_cohorts"]:
                raise ValueError("cost_gate_must_not_change_forecast_errors_or_coverage")
            _relabel_expected(report, spread[m], expected[m])
    return {"schema": SCHEMA, "primary_current_spread": primary,
            "secondary_expected_cost": secondary,
            "primary_gate": "unchanged observed current spread + fixed 1bp",
            "secondary_gate": "current same-side entry cost + predicted same-side exit cost + fixed 1bp",
            "all_original_assessment_origins": len(rows),
            "future_labels_used_for_decisions": False,
            "mean_forecasts_changed_by_cost_gate": False,
            "extra_evaluation_cost_scenarios_bps": [0., 1., 2.],
            "can_place_orders": False, "models_promoted": 0}


def _decisions(times, pairs, mean, cost, h):
    signs = np.where(np.isfinite(mean), np.sign(mean), 0.)
    candidate = np.isfinite(mean) & (np.abs(mean) > cost + 1.)
    accepted = np.zeros(len(mean), dtype=bool)
    last = {}
    for i in np.flatnonzero(candidate):
        pair = pairs[i]
        if pair not in last or times[i] >= last[pair] + h * 60:
            accepted[i] = True
            last[pair] = int(times[i])
    return accepted, signs


def _errors(predicted, actual):
    error = predicted - actual
    if not np.isfinite(error).all():
        raise ValueError("component_error_overflow")
    return {"rows": len(error), "mean_error_bps": _mean(error),
            "mae_bps": _mean(np.abs(error)), "rmse_bps": _rmse(error)}


def _probability_scores(probability, actual_positive):
    clipped = np.clip(probability, PROBABILITY_CLIP, 1. - PROBABILITY_CLIP)
    y = actual_positive.astype(np.float64)
    return {"rows": len(y), "positive_rows": int(y.sum()),
            "brier": _mean((probability - y) ** 2),
            "log_loss": _mean(-(y * np.log(clipped) + (1. - y) * np.log1p(-clipped))),
            "log_loss_clip": PROBABILITY_CLIP,
            "log_loss_clipped_rows": int(np.count_nonzero(clipped != probability))}


def _prior_positive(data, h, rows):
    mask = training_mask(data, h)
    prior = np.full(len(rows), np.nan)
    for pair_id in range(len(data["pair_names"])):
        y = data[f"y_{h}"][mask & (data["pair_id"] == pair_id)]
        if len(y) >= 20:
            prior[data["pair_id"][rows] == pair_id] = np.mean(y > 0)
    return prior


def _population(times, pairs, mask):
    t, p = times[mask], pairs[mask]
    days = t.astype("datetime64[s]").astype("datetime64[D]").astype(str)
    day_values, day_counts = np.unique(days, return_counts=True)
    pair_values, pair_counts = np.unique(p, return_counts=True)
    return {"rows": len(t), "unique_original_utc_decision_times": len(np.unique(t)),
            "utc_days": len(day_values), "pairs": len(pair_values),
            "counts_by_utc_day": dict(zip(day_values.tolist(), day_counts.tolist())),
            "counts_by_pair": dict(zip(pair_values.tolist(), pair_counts.tolist())),
            "clock_scope": "original UTC candle starts; distinct clocks/pairs are not independent observations"}


def component_diagnostics(data, assessment, mean, h, prior, entry_long, entry_short,
                          heads, *, prior_positive=None):
    """Head scores and exact selected optimism, independent of fit machinery.

    Heads: probability=P(R>0), direct, positive=E[R|R>0],
    nonpositive=E[-R|R<=0], long_exit and short_exit, all assessment-sized.
    Probability targets include flat rows as false. Conditional errors are
    posthoc label cohorts, never origin-time filters. Counts are never pooled
    across periods or cohorts. Future path membership cannot select entries.
    """
    rows, mean, prior, common = _assessment(data, assessment, mean, h, prior)
    names = ("probability", "direct", "positive", "nonpositive", "long_exit", "short_exit")
    arrays = {k: _numeric(heads[k], k, len(rows)) for k in names}
    if any(not np.isfinite(v).all() for v in arrays.values()):
        raise ValueError("finite_component_predictions_required")
    p = arrays["probability"]
    if np.any((p < 0) | (p > 1)):
        raise ValueError("probability_must_be_in_unit_interval")
    if any(np.any(arrays[k] < 0) for k in names[2:]):
        raise ValueError("conditional_magnitude_and_exit_predictions_must_be_nonnegative")
    el, es, xl, xs, spread, expected = _costs(data, rows, mean, entry_long, entry_short,
                                              arrays["long_exit"], arrays["short_exit"])
    pp = _prior_positive(data, h, rows) if prior_positive is None else _numeric(prior_positive, "prior_positive", len(rows))
    if np.isinf(pp).any() or np.any(np.isfinite(pp) & ((pp < 0) | (pp > 1))):
        raise ValueError("train_positive_prior_must_be_probability_or_missing")
    absolute_mean = p * arrays["positive"] + (1. - p) * arrays["nonpositive"]
    mixture = p * arrays["positive"] - (1. - p) * arrays["nonpositive"]
    if not np.isfinite(absolute_mean).all() or not np.isfinite(mixture).all():
        raise ValueError("derived_mixture_overflow")
    periods = {}
    for split_id, split_name in SPLITS:
        m = data["split"][rows] == split_id
        r = rows[m]; t = data["time"][r].astype(np.int64)
        pairs = np.asarray(data["pair_names"])[data["pair_id"][r]]
        y, long_net, short_net = (data[f"{k}_{h}"][r] for k in ("y", "long", "short"))
        valid = data[f"valid_{h}"][r]
        strict = data[f"strict_{h}"][r]
        eligible = data[f"eligible_{h}"][r]
        if any(np.asarray(v).dtype.kind != "b" for v in (valid, strict, eligible)) or np.any(strict & ~valid):
            raise ValueError("explicit_validity_masks_and_strict_subset_required")
        if np.any(valid & ~(np.isfinite(y) & np.isfinite(long_net) & np.isfinite(short_net))):
            raise ValueError("valid_endpoint_requires_finite_labels")
        actual_xl = y - el[m] - long_net
        actual_xs = -y - es[m] - short_net
        if np.any(valid & ((actual_xl < -1e-7) | (actual_xs < -1e-7))):
            raise ValueError("negative_actual_exit_cost_on_valid_endpoint")
        # Preserve exact reconstructed costs for the accounting identity.
        score = valid & eligible & common[m] & np.isfinite(mean[m])
        masks = {"full_endpoint": score, "shared_strict": score & strict,
                 "additional_endpoint_only": score & ~strict}
        decisions = {}
        for gate, cost in (("primary_current_spread", spread[m]), ("secondary_expected_cost", expected[m])):
            decision, signs = _decisions(t, pairs, mean[m], cost, h)
            decisions[gate] = (decision, signs)
        cohort_reports = {}
        for name, mask in masks.items():
            positive = mask & (y > 0); nonpositive = mask & (y <= 0)
            probability_mask = mask & np.isfinite(pp[m])
            probability = {"population": "positive versus nonpositive; exact flats are false; model and priors on identical rows",
                "model": _probability_scores(p[m][probability_mask], y[probability_mask] > 0),
                "train_pair_prior": _probability_scores(pp[m][probability_mask], y[probability_mask] > 0),
                "half_baseline": _probability_scores(np.full(int(probability_mask.sum()), .5), y[probability_mask] > 0),
                "excluded_missing_train_prior": int((mask & ~np.isfinite(pp[m])).sum())}
            selected_reports = {}
            for gate, (decision, signs) in decisions.items():
                selected = decision & mask
                sign = signs[selected]
                actual_exit = np.where(sign > 0, actual_xl[selected], actual_xs[selected])
                predicted_exit = np.where(sign > 0, xl[m][selected], xs[m][selected])
                entry = np.where(sign > 0, el[m][selected], es[m][selected])
                actual_net = np.where(sign > 0, long_net[selected], short_net[selected]) - 1.
                predicted_net = sign * mean[m][selected] - entry - predicted_exit - 1.
                return_optimism = sign * (mean[m][selected] - y[selected])
                cost_optimism = actual_exit - predicted_exit
                residual = (predicted_net - actual_net) - (return_optimism + cost_optimism)
                if not np.isfinite(residual).all() or not np.allclose(residual, 0., atol=1e-8, rtol=0.):
                    raise ValueError("selected_return_cost_decomposition_failed")
                selected_reports[gate] = {
                    "all_original_decisions": _population(t, pairs, decision),
                    "scored_decisions": _population(t, pairs, selected),
                    "decision_pair_clock_side_sha256": _decision_digest(decision, t, pairs, signs),
                    "mean_selected_return_optimism_bps": _mean(return_optimism),
                    "mean_selected_exit_cost_optimism_bps": _mean(cost_optimism),
                    "mean_expected_minus_actual_net_bps": _mean(predicted_net - actual_net),
                    "mean_predicted_net_after_extra_1bp_bps": _mean(predicted_net),
                    "mean_actual_net_after_extra_1bp_bps": _mean(actual_net),
                    "maximum_decomposition_residual_bps": float(np.abs(residual).max()) if len(residual) else None,
                    "expected_net_scope": "uses this variant signed mean and specialist predicted exit cost for both gate diagnostics; does not redefine the primary observed-spread gate",
                    "cost_optimism_definition": "actual selected exit cost minus predicted selected exit cost",
                    "return_optimism_definition": "selected side times (predicted signed return minus actual signed return)"}
            cohort_reports[name] = {
                "scored_origins": int(mask.sum()), "actual_positive": int(positive.sum()),
                "actual_nonpositive": int(nonpositive.sum()), "actual_flat": int((mask & (y == 0)).sum()),
                "positive_probability": probability,
                "positive_conditional_magnitude": _errors(arrays["positive"][m][positive], y[positive]),
                "nonpositive_conditional_magnitude": _errors(arrays["nonpositive"][m][nonpositive], -y[nonpositive]),
                "expected_absolute_move": _errors(absolute_mean[m][mask], np.abs(y[mask])),
                "raw_mixture_signed_mean": _errors(mixture[m][mask], y[mask]),
                "direct_signed_mean": _errors(arrays["direct"][m][mask], y[mask]),
                "variant_signed_mean": _errors(mean[m][mask], y[mask]),
                "variant_called_sign_return_optimism_bps": _mean(np.sign(mean[m][mask]) * (mean[m][mask] - y[mask])),
                "long_exit_cost": _errors(xl[m][mask], actual_xl[mask]),
                "short_exit_cost": _errors(xs[m][mask], actual_xs[mask]),
                "selection": selected_reports}
        periods[split_name] = {"original_origins": len(r), "finite_variant_forecasts": int(np.isfinite(mean[m]).sum()),
            "common_origin_support": int(common[m].sum()), "valid_endpoint_origins": int(valid.sum()),
            "split_eligible_origins": int(eligible.sum()), "cohorts": cohort_reports}
    return {"schema": SCHEMA + "_components", "horizon_minutes": h, "periods": periods,
            "decision_rule": "both gates fixed before future/cohort masks; independent per-pair nonoverlap reset at each assessment split",
            "prior_scope": "caller-supplied TRAIN pair positive probability" if prior_positive is not None else "derived equal-row mature TRAIN pair positive probability, minimum20 labels",
            "future_cohorts_are_entry_filters": False, "additional_evaluation_cost_bps": 1.,
            "scope": "historical candle endpoint diagnostics; no fit, portfolio/fill/independence or promotion claim"}
