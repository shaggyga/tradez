"""Pure same-origin forecast and fixed-horizon policy diagnostics.

Every finite prediction is issued before any future-label, split or comparison
mask is consulted. Threshold decisions use only abs(prediction), the spread at
the origin and a fixed margin. Evaluation costs never change those decisions.
Per-pair holding reservations are made on all original threshold decisions,
including decisions whose future endpoint is missing or excluded from scoring.

All endpoint results are historical bid/ask close proxies in bps of origin mid.
Additional costs are deducted once per selected round trip. Sums describe equal
unit-notional decisions, not an account/portfolio return. There is no fill,
slippage, financing, position-sizing, intrahorizon management or p-value claim.
The strict shared cohort and additional endpoint-only cohort remain separate.
"""
from __future__ import annotations

import hashlib
import math
import re

import numpy as np

SCHEMA = "rolling_model_same_origin_scoring_v1_20260915"
PAIR_PATTERN = re.compile(r"[A-Z]{3}_[A-Z]{3}")
DECISION_DIGEST_FORMAT = "pair_S7_time_le_i8_sign_i1_sorted_pair_time_v1"
DECISION_RECORD_DTYPE = np.dtype([("pair", "S7"), ("time", "<i8"), ("side", "i1")], align=False)


def _numeric(value, name, n=None):
    array = np.asarray(value)
    if array.ndim != 1 or array.dtype.kind not in "iuf" or (n is not None and len(array) != n):
        raise ValueError("one_dimensional_numeric_array_required:" + name)
    return array.astype(np.float64, copy=True)


def _boolean(value, name, n):
    array = np.asarray(value)
    if array.dtype.kind != "b" or array.ndim != 1 or len(array) != n:
        raise ValueError("explicit_boolean_mask_required:" + name)
    return array.copy()


def _nonnegative_number(value, name):
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, float, np.integer, np.floating)) or not math.isfinite(value) or value < 0:
        raise ValueError("finite_nonnegative_number_required:" + name)
    return 0. if value == 0 else float(value)


def _number(value):
    value = float(value)
    return value if math.isfinite(value) else None


def _ratio(numerator, denominator):
    return _number(numerator / denominator) if denominator else None


def _mean(values):
    return _number(np.mean(values)) if len(values) else None


def _rmse(errors):
    if not len(errors):
        return None
    scale = np.max(np.abs(errors))
    if not np.isfinite(scale):
        return None
    return 0. if scale == 0 else _number(scale * np.sqrt(np.mean((errors / scale) ** 2)))


def _direction(prediction, actual):
    predicted_sign, actual_sign = np.sign(prediction), np.sign(actual)
    nonflat_actual = actual_sign != 0
    called_nonflat = nonflat_actual & (predicted_sign != 0)
    matches = predicted_sign == actual_sign
    return {
        "rows": len(actual),
        "predicted_up": int((predicted_sign > 0).sum()), "predicted_down": int((predicted_sign < 0).sum()),
        "predicted_flat": int((predicted_sign == 0).sum()),
        "actual_up": int((actual_sign > 0).sum()), "actual_down": int((actual_sign < 0).sum()),
        "actual_flat": int((actual_sign == 0).sum()),
        "exact_sign_matches_including_flat": int(matches.sum()),
        "exact_sign_accuracy_including_flat": _ratio(int(matches.sum()), len(actual)),
        "nonflat_actual_rows": int(nonflat_actual.sum()),
        "correct_nonflat_actual_rows": int((matches & nonflat_actual).sum()),
        "direction_accuracy_on_nonflat_actuals": _ratio(int((matches & nonflat_actual).sum()), int(nonflat_actual.sum())),
        "predicted_flat_on_nonflat_actuals": int((nonflat_actual & (predicted_sign == 0)).sum()),
        "called_nonflat_outcome_rows": int(called_nonflat.sum()),
        "direction_accuracy_on_called_nonflat_outcomes": _ratio(int((matches & called_nonflat).sum()), int(called_nonflat.sum())),
        "called_coverage_of_nonflat_actuals": _ratio(int(called_nonflat.sum()), int(nonflat_actual.sum())),
        "tie_rule": "exact zero is flat; actual-flat cases excluded from directional metrics, predicted-flat cases count as misses on nonflat actuals and are excluded from called-only accuracy",
    }


def _forecast_scores(prediction, actual):
    with np.errstate(over="ignore", invalid="ignore"):
        errors = prediction - actual
        mae = _mean(np.abs(errors))
        baseline_mae = _mean(np.abs(actual))
    rmse, baseline_rmse = _rmse(errors), _rmse(actual)
    return {
        "scored_forecasts": len(actual), "mae_bps": mae, "rmse_bps": rmse,
        "derived_error_overflow_rows": int((~np.isfinite(errors)).sum()),
        "no_change_baseline": {
            "prediction_bps": 0., "same_origin_rows": len(actual),
            "mae_bps": baseline_mae, "rmse_bps": baseline_rmse,
            "exact_sign_accuracy_including_flat": _ratio(int((actual == 0).sum()), len(actual)),
            "direction_accuracy_on_nonflat_actuals": 0. if np.any(actual != 0) else None,
        },
        "mae_improvement_over_no_change_bps": _number(baseline_mae - mae) if mae is not None and baseline_mae is not None else None,
        "rmse_improvement_over_no_change_bps": _number(baseline_rmse - rmse) if rmse is not None and baseline_rmse is not None else None,
        "direction": _direction(prediction, actual),
    }


def _net_scores(values):
    positive, negative = values > 0, values < 0
    with np.errstate(over="ignore", invalid="ignore"):
        gross_positive = _number(values[positive].sum())
        gross_negative = _number(-values[negative].sum())
        total = _number(values.sum())
    return {
        "scored_decisions": len(values), "mean_net_bps": _mean(values),
        "median_net_bps": _number(np.median(values)) if len(values) else None,
        "sum_unit_notional_net_bps": total,
        "positive_decisions": int(positive.sum()), "negative_decisions": int(negative.sum()),
        "flat_decisions": int((values == 0).sum()),
        "positive_fraction": _ratio(int(positive.sum()), len(values)),
        "gross_positive_bps": gross_positive, "gross_negative_bps": gross_negative,
        "profit_factor": _ratio(gross_positive, gross_negative) if gross_positive is not None and gross_negative is not None else None,
        "sum_scope": "sum of equal unit-notional historical endpoint proxies; not account or portfolio return",
    }


def _group_summary(keys, values, label):
    groups = []
    for key in np.unique(keys):
        selected = keys == key
        subset = values[selected]
        groups.append({label: str(key), "scored_decisions": len(subset),
                       "mean_net_bps": _mean(subset), "sum_unit_notional_net_bps": _number(subset.sum())})
    groups.sort(key=lambda row: (-(row["sum_unit_notional_net_bps"] or 0.), row[label]))
    if not groups:
        return {"observed_group_count": 0, "ranked_groups": [], "best_group": None,
                "best_group_share_of_positive_group_net": None, "leave_best_group_out_mean_net_bps": None}
    best = groups[0]
    positive_total = sum(max(0., row["sum_unit_notional_net_bps"]) for row in groups if row["sum_unit_notional_net_bps"] is not None)
    other_values = values[keys != best[label]]
    return {
        "observed_group_count": len(groups), "ranked_groups": groups, "best_group": best[label],
        "positive_net_groups": sum(row["sum_unit_notional_net_bps"] is not None and row["sum_unit_notional_net_bps"] > 0 for row in groups),
        "best_group_share_of_positive_group_net": _ratio(max(0., best["sum_unit_notional_net_bps"]), positive_total) if best["sum_unit_notional_net_bps"] is not None else None,
        "leave_best_group_out_decisions": len(other_values),
        "leave_best_group_out_mean_net_bps": _mean(other_values),
        "ranking_rule": "descending sum of unit-notional net bps, ties by group key; diagnostic only",
    }


def _decision_digest(mask, times, pairs, signs):
    """SHA256 over sorted packed 16-byte records; no separators or padding.

    Versioned format: exact seven ASCII pair bytes, signed little-endian int64
    original start epoch, signed int8 side (-1/+1). Sort by pair then time.
    This intentionally replaces the earlier per-row textual hash format.
    """
    selected = np.flatnonzero(mask)
    records = np.empty(len(selected), dtype=DECISION_RECORD_DTYPE)
    records["pair"] = pairs[selected].astype("S7")
    records["time"] = times[selected]
    records["side"] = signs[selected].astype(np.int8)
    order = np.lexsort((records["time"], records["pair"]))
    return hashlib.sha256(records[order].tobytes(order="C")).hexdigest()


def evaluate_predictions(times, pairs, predicted_bps, return_bps, long_net_bps, short_net_bps,
                         endpoint_valid, strict_valid, split_eligible, entry_spread_bps, *,
                         horizon_minutes, extra_cost_bps=1.0, stress_costs_bps=(0., 1., 2.),
                         fixed_margin_bps=1.0, comparison_origin_mask=None):
    """Return JSON-safe scores with unchanged issuance/decisions across cohorts.

    ``times`` are original UTC minute starts, unique and ascending within each
    pair; different pairs may interleave. ``endpoint_valid`` asserts that all
    three supplied realized outcomes are finite. ``strict_valid`` is its subset.
    Missing predictions are not issued. A missing/negative origin spread blocks
    only the threshold policy, never forecast issuance or sign-only diagnostics.

    ``comparison_origin_mask`` is a caller-established origin-known matched
    coverage mask. It, future validity and split eligibility apply only AFTER
    decision/holding reservations, and receive separate unscored counts.

    The threshold is abs(prediction) > entry_spread_bps + fixed_margin_bps.
    This fixed origin-known margin is independent of extra_cost_bps. Primary and
    stress costs produce identical decisions, with canonical float-string keys.
    """
    raw_times = _numeric(times, "times")
    if np.any(~np.isfinite(raw_times)) or np.any(raw_times < 0) or np.any(raw_times > 253402300799) or np.any(raw_times % 60):
        raise ValueError("original_aligned_utc_minute_start_required")
    clocks = raw_times.astype(np.int64)
    n = len(clocks)
    pair_array = np.asarray(pairs)
    if pair_array.ndim != 1 or len(pair_array) != n or (n and pair_array.dtype.kind not in "UO"):
        raise ValueError("explicit_valid_currency_pairs_required")
    try:
        unique_pairs = np.unique(pair_array)
    except (TypeError, ValueError) as exc:
        raise ValueError("explicit_valid_currency_pairs_required") from exc
    if any(not isinstance(pair, str) or not PAIR_PATTERN.fullmatch(pair) or pair[:3] == pair[4:] for pair in unique_pairs):
        raise ValueError("explicit_valid_currency_pairs_required")
    pair_array = pair_array.astype(str, copy=False)
    for pair in unique_pairs:
        if np.any(np.diff(clocks[pair_array == pair]) <= 0):
            raise ValueError("unique_ascending_original_clocks_per_pair_required")
    if isinstance(horizon_minutes, (bool, np.bool_)) or not isinstance(horizon_minutes, (int, np.integer)) or not 1 <= horizon_minutes <= 525600:
        raise ValueError("positive_integer_horizon_minutes_required")
    primary_cost = _nonnegative_number(extra_cost_bps, "extra_cost_bps")
    margin = _nonnegative_number(fixed_margin_bps, "fixed_margin_bps")
    try:
        costs = sorted(set([primary_cost] + [_nonnegative_number(cost, "stress_costs_bps") for cost in stress_costs_bps]))
    except TypeError as exc:
        raise ValueError("iterable_nonnegative_stress_costs_required") from exc
    prediction = _numeric(predicted_bps, "predicted_bps", n)
    actual = _numeric(return_bps, "return_bps", n)
    long_net = _numeric(long_net_bps, "long_net_bps", n)
    short_net = _numeric(short_net_bps, "short_net_bps", n)
    spread = _numeric(entry_spread_bps, "entry_spread_bps", n)
    endpoint = _boolean(endpoint_valid, "endpoint_valid", n)
    strict = _boolean(strict_valid, "strict_valid", n)
    split = _boolean(split_eligible, "split_eligible", n)
    comparison = np.ones(n, dtype=bool) if comparison_origin_mask is None else _boolean(comparison_origin_mask, "comparison_origin_mask", n)
    if np.any(endpoint & ~(np.isfinite(actual) & np.isfinite(long_net) & np.isfinite(short_net))):
        raise ValueError("valid_endpoint_requires_finite_return_and_both_bidask_outcomes")
    if np.any(strict & ~endpoint):
        raise ValueError("strict_valid_must_be_endpoint_subset")

    # All origin decisions and holding reservations precede every scoring mask.
    issued = np.isfinite(prediction)
    signs = np.where(issued, np.sign(prediction), 0.)
    sign_decisions = issued & (signs != 0)
    spread_valid = np.isfinite(spread) & (spread >= 0)
    with np.errstate(over="ignore", invalid="ignore"):
        threshold = issued & spread_valid & (np.abs(prediction) > spread + margin)
    nonoverlap = np.zeros(n, dtype=bool)
    last_entry = {}
    for i in np.flatnonzero(threshold):
        pair = pair_array[i]
        if pair not in last_entry or clocks[i] >= last_entry[pair] + int(horizon_minutes) * 60:
            nonoverlap[i] = True
            last_entry[pair] = int(clocks[i])
    days = clocks.astype("datetime64[s]").astype("datetime64[D]").astype(str)
    scoring = endpoint & split & comparison
    cohorts = {"full_endpoint": scoring, "shared_strict": scoring & strict,
               "additional_endpoint_only": scoring & ~strict}
    forecast_scores = {name: dict(_forecast_scores(prediction[issued & mask], actual[issued & mask]),
                                  cohort_origins=int(mask.sum())) for name, mask in cohorts.items()}
    policies = {}
    for name, decision in (("all_issued_sign_diagnostic", sign_decisions),
                           ("origin_spread_threshold", threshold),
                           ("origin_spread_threshold_nonoverlap", nonoverlap)):
        cohort_scores = {}
        for cohort, mask in cohorts.items():
            selected = decision & mask
            base_net = np.where(signs[selected] > 0, long_net[selected], short_net[selected])
            scenario_scores = {}
            for cost in costs:
                with np.errstate(over="ignore", invalid="ignore"):
                    realized = base_net - cost
                if np.any(~np.isfinite(realized)):
                    raise ValueError("evaluation_cost_produces_nonfinite_net_outcome")
                scenario_scores[str(float(cost))] = _net_scores(realized)
            primary_net = base_net - primary_cost
            cohort_scores[cohort] = {
                "scored_decisions": int(selected.sum()),
                "long_scored_decisions": int((selected & (signs > 0)).sum()),
                "short_scored_decisions": int((selected & (signs < 0)).sum()),
                "direction": _direction(prediction[selected], actual[selected]),
                "cost_scenarios": scenario_scores,
                "primary_cost_day_concentration": _group_summary(days[selected], primary_net, "utc_day"),
                "primary_cost_pair_concentration": _group_summary(pair_array[selected], primary_net, "pair"),
            }
        policies[name] = {
            "decisions": int(decision.sum()), "long_decisions": int((decision & (signs > 0)).sum()),
            "short_decisions": int((decision & (signs < 0)).sum()),
            "decision_pair_clock_side_sha256": _decision_digest(decision, clocks, pair_array, signs),
            "observed_decision_utc_days": len(np.unique(days[decision])),
            "decisions_with_invalid_future_endpoint": int((decision & ~endpoint).sum()),
            "decisions_excluded_by_comparison_mask": int((decision & ~comparison).sum()),
            "decisions_excluded_by_split_boundary": int((decision & ~split).sum()),
            "unscored_reason_partition": {
                "comparison_excluded": int((decision & ~comparison).sum()),
                "split_excluded_after_comparison": int((decision & comparison & ~split).sum()),
                "invalid_future_after_comparison_and_split": int((decision & comparison & split & ~endpoint).sum()),
                "scored_full_endpoint": int((decision & scoring).sum()),
            },
            "reason_counts_note": "independent exclusion counts may overlap; unscored_reason_partition is exclusive and sums to all decisions",
            "cohorts": cohort_scores,
        }
    return {
        "schema": SCHEMA, "horizon_minutes": int(horizon_minutes),
        "input_origins": n, "observed_origin_utc_days": len(np.unique(days)), "observed_pairs": len(unique_pairs),
        "decision_digest_format": {"version": DECISION_DIGEST_FORMAT, "record_bytes": DECISION_RECORD_DTYPE.itemsize,
                                   "fields": ["seven ASCII pair bytes", "signed little-endian int64 original start epoch", "signed int8 side"],
                                   "sort": "pair ascending, then original start epoch ascending; packed records without separators or padding"},
        "issued_forecasts": int(issued.sum()), "unavailable_predictions": int((~issued).sum()),
        "forecast_coverage": _ratio(int(issued.sum()), n),
        "issued_predicted_flat": int((issued & (signs == 0)).sum()),
        "comparison_origins": int(comparison.sum()), "issued_comparison_origins": int((issued & comparison).sum()),
        "comparison_forecast_coverage": _ratio(int((issued & comparison).sum()), int(comparison.sum())),
        "endpoint_valid_origins": int(endpoint.sum()), "issued_valid_endpoints": int((issued & endpoint).sum()),
        "issued_invalid_future_endpoints": int((issued & ~endpoint).sum()),
        "issued_split_ineligible": int((issued & ~split).sum()),
        "issued_without_valid_origin_spread": int((issued & ~spread_valid).sum()),
        "threshold_candidates_blocked_by_holding_window": int((threshold & ~nonoverlap).sum()),
        "decision_rule": {"threshold": "abs(predicted_bps) > entry_spread_bps + fixed_margin_bps",
                          "fixed_margin_bps": margin, "prediction_flat_policy": "no sign or threshold decision",
                          "nonoverlap": "independent by pair; accept origin start >= last accepted origin start + horizon*60; missing/excluded future labels still reserve the full holding interval",
                          "clock_basis": "original UTC bar starts; equivalent holding separation for bar-end entries",
                          "future_label_masks_used_for_decisions": False,
                          "comparison_mask_used_for_decisions": False},
        "evaluation_cost": {"primary_extra_cost_bps": primary_cost, "primary_scenario_key": str(float(primary_cost)),
                            "stress_costs_bps": costs, "application": "deduct once from actual chosen-side bid/ask endpoint net; already includes endpoint quoted spread proxy; scenario costs never alter decisions"},
        "forecast_cohorts": forecast_scores, "policies": policies,
        "scope": "historical same-origin forecast and equal unit-notional fixed-horizon endpoint diagnostics; no fills, portfolio/account return, sizing, financing, continuous-tradability or management-path claim",
        "cohort_rule": "future endpoint validity, strict path validity, split maturity and optional matched-origin coverage affect scores only; additional endpoint-only cohort does not establish continuous tradability",
        "promotion_decision": None, "statistical_significance_claim": False,
    }
