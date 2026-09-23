"""Summarize every fixed comparison cell without fitting or rescoring.

Only a complete RESULTS.json and its hash-bound per-cell metrics are read.
All 32 learned cells and 20 controls appear in both assessment periods. No
support cutoff selects a winner, and descriptive positive-period flags cannot
promote models. Output requires an explicit new directory outside the result.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path

RESULT_SCHEMA = "rolling_model_comparison_v1_20260915"
SUMMARY_SCHEMA = "rolling_model_comparison_summary_v1_20260915"
HORIZONS = (5, 15, 30, 60)
LEARNERS = ("ridge", "hgb")
GROUPS = {"compact38": 38, "compact50": 50, "local216": 216, "combined228": 228}
CONTROLS = ("no_change", "pair_train_mean", "arima110_conditional_ols",
            "momentum_exact_past_horizon", "reversal_exact_past_horizon")
SPLITS = ("validation", "later_development_test")
COHORTS = ("full_endpoint", "shared_strict", "additional_endpoint_only")
POLICY = "origin_spread_threshold_nonoverlap"
CONTRASTS = (
    ("compact38", "compact50", "add peers to compact local inputs"),
    ("local216", "combined228", "add peers to full local inputs"),
    ("compact38", "local216", "expand local input families and lookback support"),
    ("compact50", "combined228", "expand local inputs while retaining peers"),
)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _json(raw):
    def invalid(value):
        raise ValueError("nonfinite_json_number_refused:" + value)
    return json.loads(raw, parse_constant=invalid)


def _finite(value, name):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("finite_metric_or_null_required:" + name)
    return float(value)


def _count(value, name):
    if type(value) is not int or value < 0:
        raise ValueError("nonnegative_integer_count_required:" + name)
    return value


def _difference(left, right):
    return left - right if left is not None and right is not None else None


def _percent(value):
    return value * 100 if value is not None else None


def _errors(scores):
    zero = scores["no_change_baseline"]
    direction = scores["direction"]
    n = _count(scores["scored_forecasts"], "scored_forecasts")
    if _count(zero["same_origin_rows"], "no_change_rows") != n:
        raise ValueError("no_change_baseline_same_origin_count_required")
    called = _count(direction["called_nonflat_outcome_rows"], "called_nonflat_outcome_rows")
    if called > n:
        raise ValueError("called_direction_count_exceeds_scored_forecasts")
    mae, rmse = _finite(scores["mae_bps"], "mae"), _finite(scores["rmse_bps"], "rmse")
    zero_mae, zero_rmse = _finite(zero["mae_bps"], "no_change_mae"), _finite(zero["rmse_bps"], "no_change_rmse")
    accuracy = _finite(direction["direction_accuracy_on_called_nonflat_outcomes"], "called_direction_accuracy")
    if accuracy is not None and not 0 <= accuracy <= 1:
        raise ValueError("direction_accuracy_fraction_required")
    return {
        "scored_forecasts": n, "called_nonflat_rows": called,
        "called_nonflat_direction_percent": _percent(accuracy),
        "predicted_flat_rows": _count(direction["predicted_flat"], "predicted_flat"),
        "actual_flat_rows": _count(direction["actual_flat"], "actual_flat"),
        "mae_bps": mae, "rmse_bps": rmse, "no_change_mae_bps": zero_mae, "no_change_rmse_bps": zero_rmse,
        "mae_delta_vs_no_change_bps": _difference(mae, zero_mae),
        "rmse_delta_vs_no_change_bps": _difference(rmse, zero_rmse),
    }


def _concentration(source, key):
    groups = source["ranked_groups"]
    count = _count(source["observed_group_count"], "observed_group_count")
    if len(groups) != count or len({row[key] for row in groups}) != count:
        raise ValueError("concentration_group_count_or_identity_mismatch")
    counts = [{key: row[key], "scored_decisions": _count(row["scored_decisions"], "group_scored_decisions"),
               "mean_net_bps": _finite(row["mean_net_bps"], "group_mean_net_bps"),
               "sum_unit_notional_net_bps": _finite(row["sum_unit_notional_net_bps"], "group_sum_net_bps")}
              for row in groups]
    return {"observed_group_count": count, "best_group": source["best_group"],
            "best_group_share_of_positive_group_net": _finite(source["best_group_share_of_positive_group_net"], "best_group_share"),
            "leave_best_group_out_mean_net_bps": _finite(source["leave_best_group_out_mean_net_bps"], "leave_best_group_out"),
            "group_counts_and_net": counts}


def _cohort(primary, delayed, cohort):
    forecasts = _errors(primary["forecast_cohorts"][cohort])
    own_policy = primary["policies"][POLICY]
    policy = own_policy["cohorts"][cohort]
    delayed_policy = delayed["policies"][POLICY]["cohorts"][cohort]
    scored = _count(policy["scored_decisions"], "scored_decisions")
    delay_scored = _count(delayed_policy["scored_decisions"], "delay_scored_decisions")
    decisions = _count(own_policy["decisions"], "nonoverlap_decisions")
    if scored > decisions or scored > forecasts["scored_forecasts"] or delay_scored > scored:
        raise ValueError("scored_policy_population_mismatch")
    net = {}
    for cost in ("0.0", "1.0", "2.0"):
        stats = policy["cost_scenarios"][cost]
        if _count(stats["scored_decisions"], "cost_scored_decisions") != scored:
            raise ValueError("cost_scenarios_must_preserve_decisions")
        net[cost] = {"mean_net_bps": _finite(stats["mean_net_bps"], "mean_net_bps"),
                     "profit_factor": _finite(stats["profit_factor"], "profit_factor"),
                     "positive_fraction": _finite(stats["positive_fraction"], "positive_fraction"),
                     "sum_unit_notional_net_bps": _finite(stats["sum_unit_notional_net_bps"], "sum_net_bps")}
    delayed_net = delayed_policy["cost_scenarios"]["1.0"]
    if _count(delayed_net["scored_decisions"], "delay_cost_scored_decisions") != delay_scored:
        raise ValueError("delayed_policy_count_mismatch")
    days = _concentration(policy["primary_cost_day_concentration"], "utc_day")
    pairs = _concentration(policy["primary_cost_pair_concentration"], "pair")
    if any(sum(row["scored_decisions"] for row in group["group_counts_and_net"]) != scored for group in (days, pairs)):
        raise ValueError("concentration_counts_must_cover_scored_decisions")
    return {
        "forecast_errors": forecasts, "nonoverlap_decisions_before_score_masks": decisions,
        "nonoverlap_scored_decisions": scored,
        "long_scored_decisions": _count(policy["long_scored_decisions"], "long_scored_decisions"),
        "short_scored_decisions": _count(policy["short_scored_decisions"], "short_scored_decisions"),
        "net_cost_scenarios": net,
        "one_minute_delay": {"scored_decisions": delay_scored,
                             "mean_net_1bp_bps": _finite(delayed_net["mean_net_bps"], "delayed_mean_net_bps"),
                             "profit_factor_1bp": _finite(delayed_net["profit_factor"], "delayed_profit_factor"),
                             "same_scored_count_as_primary": delay_scored == scored},
        "primary_1bp_day_concentration": days, "primary_1bp_pair_concentration": pairs,
    }


def _row(tag, entry, kind, split, source, metric_record):
    primary, delayed = source["primary"], source["one_minute_entry_delay"]
    if primary["horizon_minutes"] != entry["horizon_minutes"] or delayed["horizon_minutes"] != entry["horizon_minutes"]:
        raise ValueError("cell_and_metrics_horizon_mismatch")
    for report in (primary, delayed):
        if (report["decision_rule"]["fixed_margin_bps"] != 1. or
                report["evaluation_cost"]["primary_scenario_key"] != "1.0" or
                report["decision_rule"]["future_label_masks_used_for_decisions"] is not False or
                report["decision_rule"]["comparison_mask_used_for_decisions"] is not False):
            raise ValueError("fixed_origin_decision_and_primary_cost_contract_required")
    for policy in primary["policies"]:
        if (primary["policies"][policy]["decisions"] != delayed["policies"][policy]["decisions"] or
                primary["policies"][policy]["decision_pair_clock_side_sha256"] != delayed["policies"][policy]["decision_pair_clock_side_sha256"]):
            raise ValueError("delay_must_preserve_original_decisions")
    cohort_values = {cohort: _cohort(primary, delayed, cohort) for cohort in COHORTS}
    for field in ("scored_forecasts",):
        if cohort_values["full_endpoint"]["forecast_errors"][field] != sum(cohort_values[c]["forecast_errors"][field] for c in COHORTS[1:]):
            raise ValueError("shared_and_additional_forecast_cohorts_must_partition_full")
    if cohort_values["full_endpoint"]["nonoverlap_scored_decisions"] != sum(cohort_values[c]["nonoverlap_scored_decisions"] for c in COHORTS[1:]):
        raise ValueError("shared_and_additional_policy_cohorts_must_partition_full")
    return {
        "cell": tag, "kind": kind, "split": split, "horizon_minutes": entry["horizon_minutes"],
        "learner": entry["learner"] if kind == "learned" else entry["name"],
        "group": entry["group"] if kind == "learned" else "control",
        "registered_input_count": entry["registered_input_count"] if kind == "learned" else None,
        "training_provenance": ({key: entry["training_selection"][key] for key in ("rows", "pair_clock_sha256", "target_sha256")}
                                if kind == "learned" else entry.get("training_description")),
        "metrics_artifact": metric_record,
        "input_origins": _count(primary["input_origins"], "input_origins"),
        "issued_forecasts": _count(primary["issued_forecasts"], "issued_forecasts"),
        "comparison_origins": _count(primary["comparison_origins"], "comparison_origins"),
        "issued_comparison_origins": _count(primary["issued_comparison_origins"], "issued_comparison_origins"),
        "forecast_coverage": _finite(primary["forecast_coverage"], "forecast_coverage"),
        "common_origin_rule": "origin-known ARIMA input, exact past-horizon momentum and pair training-prior support; future endpoint/split masks apply only to scoring",
        "cohorts": cohort_values,
        "own_coverage_forecast_errors": _errors(primary["own_coverage_forecast_scores_without_comparator_restriction"]),
        "own_coverage_scope": "own issued valid/mature outcomes without comparator restriction; never substituted into common-coverage rankings or arm deltas",
        "nonoverlap_decision_sha256": primary["policies"][POLICY]["decision_pair_clock_side_sha256"],
        "nonoverlap_unscored_reason_partition": primary["policies"][POLICY]["unscored_reason_partition"],
    }


def _period_flags(periods):
    positive_periods = {cost: [s for s in SPLITS if periods[s]["nonoverlap_scored_decisions"] > 0 and
                              periods[s]["net_cost_scenarios"][cost]["mean_net_bps"] is not None and
                              periods[s]["net_cost_scenarios"][cost]["mean_net_bps"] > 0]
                        for cost in ("0.0", "1.0", "2.0")}
    delayed = all(periods[s]["one_minute_delay"]["scored_decisions"] > 0 and
                  periods[s]["one_minute_delay"]["mean_net_1bp_bps"] is not None and
                  periods[s]["one_minute_delay"]["mean_net_1bp_bps"] > 0 for s in SPLITS)
    return {
            "positive_quoted_spread_only_both_periods": len(positive_periods["0.0"]) == 2,
            "positive_net_1bp_both_periods": len(positive_periods["1.0"]) == 2,
            "positive_net_2bp_both_periods": len(positive_periods["2.0"]) == 2,
            "positive_primary_and_delay_1bp_both_periods": len(positive_periods["1.0"]) == 2 and delayed,
            "positive_periods_by_extra_cost_bps": positive_periods,
            "positive_in_any_period_by_extra_cost_bps": {cost: bool(periods) for cost, periods in positive_periods.items()},
            "positive_in_exactly_one_period_by_extra_cost_bps": {cost: len(periods) == 1 for cost, periods in positive_periods.items()},
            "minimum_scored_decisions_across_periods": min(periods[s]["nonoverlap_scored_decisions"] for s in SPLITS),
            "minimum_delay_scored_decisions_across_periods": min(periods[s]["one_minute_delay"]["scored_decisions"] for s in SPLITS),
            "observed_scored_days_by_period": {s: periods[s]["primary_1bp_day_concentration"]["observed_group_count"] for s in SPLITS},
            "observed_scored_pairs_by_period": {s: periods[s]["primary_1bp_pair_concentration"]["observed_group_count"] for s in SPLITS},
            "support_rule": "no minimum-support selection cutoff; exact counts and concentration must accompany every flag",
            "promotion": False,
    }


def _flags(rows):
    result = {}
    for tag in sorted({row["cell"] for row in rows}):
        by_cohort = {cohort: _period_flags({row["split"]: row["cohorts"][cohort] for row in rows if row["cell"] == tag})
                     for cohort in COHORTS}
        result[tag] = dict(by_cohort["full_endpoint"], by_path_cohort=by_cohort,
                           path_cohort_use="posthoc future path membership only; never an origin-time entry filter")
    return result


def _arm_deltas(rows, manifest):
    by_key = {(r["learner"], r["group"], r["horizon_minutes"], r["split"]): r for r in rows if r["kind"] == "learned"}
    result = []
    for learner in LEARNERS:
        for h in HORIZONS:
            for split in SPLITS:
                for reference_group, candidate_group, interpretation in CONTRASTS:
                    left, right = (by_key[(learner, group, h, split)] for group in (reference_group, candidate_group))
                    a, b = left["cohorts"]["full_endpoint"], right["cohorts"]["full_endpoint"]
                    train_a, train_b = (manifest["cells"][row["cell"]]["training_selection"] for row in (left, right))
                    same_train = all(train_a[key] == train_b[key] for key in ("rows", "pair_clock_sha256", "target_sha256"))
                    if not same_train:
                        raise ValueError("paired_feature_arms_require_same_training_population")
                    same_score_count = a["forecast_errors"]["scored_forecasts"] == b["forecast_errors"]["scored_forecasts"]
                    result.append({
                        "learner": learner, "horizon_minutes": h, "split": split,
                        "reference_group": reference_group, "candidate_group": candidate_group,
                        "interpretation": interpretation, "training_population_hashes_match": same_train,
                        "common_forecast_counts_match": same_score_count,
                        "reference_common_scored_forecasts": a["forecast_errors"]["scored_forecasts"],
                        "candidate_common_scored_forecasts": b["forecast_errors"]["scored_forecasts"],
                        "mae_delta_candidate_minus_reference_bps": _difference(b["forecast_errors"]["mae_bps"], a["forecast_errors"]["mae_bps"]) if same_score_count else None,
                        "rmse_delta_candidate_minus_reference_bps": _difference(b["forecast_errors"]["rmse_bps"], a["forecast_errors"]["rmse_bps"]) if same_score_count else None,
                        "reference_nonoverlap_decisions": a["nonoverlap_decisions_before_score_masks"],
                        "candidate_nonoverlap_decisions": b["nonoverlap_decisions_before_score_masks"],
                        "reference_scored_decisions": a["nonoverlap_scored_decisions"], "candidate_scored_decisions": b["nonoverlap_scored_decisions"],
                        "net_1bp_mean_delta_candidate_minus_reference_bps": _difference(b["net_cost_scenarios"]["1.0"]["mean_net_bps"], a["net_cost_scenarios"]["1.0"]["mean_net_bps"]),
                        "identical_original_policy_decisions": left["nonoverlap_decision_sha256"] == right["nonoverlap_decision_sha256"],
                        "policy_delta_scope": "difference of each arm's policy mean, not paired-trade profit difference; counts and decisions can differ",
                    })
    return result


def summarize_comparison(comparison_root):
    """Return all-cell descriptive summary; never open models or forecasts."""
    root = Path(comparison_root).resolve()
    manifest_path = root / "RESULTS.json"
    raw = manifest_path.read_bytes()
    manifest = _json(raw)
    if manifest.get("schema") != RESULT_SCHEMA or manifest.get("status") != "complete":
        raise ValueError("complete_model_comparison_required")
    expected_cells = {f"{learner}_{group}_{h}m": (learner, group, h) for learner in LEARNERS for group in GROUPS for h in HORIZONS}
    expected_controls = {f"{name}_{h}m": (name, h) for name in CONTROLS for h in HORIZONS}
    if set(manifest.get("cells", {})) != set(expected_cells) or set(manifest.get("baselines", {})) != set(expected_controls) or manifest.get("learned_fit_count") != 32:
        raise ValueError("all_32_learned_and_20_control_cells_required")
    if tuple(manifest.get("horizons", [])) != HORIZONS or set(manifest.get("groups", {})) != set(GROUPS):
        raise ValueError("fixed_horizon_and_feature_arm_contract_required")
    for tag, (learner, group, h) in expected_cells.items():
        entry = manifest["cells"][tag]
        if (entry.get("learner"), entry.get("group"), entry.get("horizon_minutes"), entry.get("registered_input_count")) != (learner, group, h, GROUPS[group]):
            raise ValueError("learned_cell_identity_mismatch:" + tag)
    for tag, (name, h) in expected_controls.items():
        entry = manifest["baselines"][tag]
        if (entry.get("name"), entry.get("horizon_minutes")) != (name, h):
            raise ValueError("control_cell_identity_mismatch:" + tag)
    rows, checked = [], []
    for kind, entries in (("learned", manifest["cells"]), ("control", manifest["baselines"])):
        for tag, entry in sorted(entries.items()):
            record = entry["metrics"]
            path = (root / record["path"]).resolve()
            if record["path"] != f"metrics/{tag}.json" or not path.is_relative_to((root / "metrics").resolve()) or not path.is_file():
                raise ValueError("contained_per_cell_metric_path_required")
            payload = path.read_bytes()
            if _sha(payload) != record["sha256"]:
                raise ValueError("cell_metric_hash_mismatch:" + tag)
            metrics = _json(payload)
            if set(metrics) != set(SPLITS):
                raise ValueError("both_assessment_periods_required:" + tag)
            checked.append((path, record["sha256"]))
            for split in SPLITS:
                rows.append(_row(tag, entry, kind, split, metrics[split], dict(record)))
    rows.sort(key=lambda row: (SPLITS.index(row["split"]), row["horizon_minutes"], row["kind"] != "learned", row["learner"], row["group"]))
    flags = _flags(rows)
    deltas = _arm_deltas(rows, manifest)
    rankings = {}
    for split in SPLITS:
        rankings[split] = {}
        for h in HORIZONS:
            group = [row for row in rows if row["split"] == split and row["horizon_minutes"] == h]
            def rank_key(row):
                value = row["cohorts"]["full_endpoint"]["net_cost_scenarios"]["1.0"]["mean_net_bps"]
                return value is None, -value if value is not None else 0., row["cell"]
            rankings[split][str(h)] = [row["cell"] for row in sorted(group, key=rank_key)]
    if _sha(manifest_path.read_bytes()) != _sha(raw) or any(_sha(path.read_bytes()) != digest for path, digest in checked):
        raise ValueError("comparison_artifacts_changed_during_summary")
    return {
        "schema": SUMMARY_SCHEMA, "comparison_root": str(root), "results_sha256": _sha(raw),
        "summary_source_sha256": _sha(Path(__file__).read_bytes()),
        "source_bindings": manifest["source_bindings"],
        "experiment_provenance": {key: manifest.get(key) for key in
                                  ("training_sample", "normalizer_scope", "matched_score_origins", "holding_scope", "pair_context", "training_weights")},
        "metrics_files_verified": len(checked), "learned_cells": 32, "control_cells": 20,
        "assessment_rows_in_summary": len(rows), "assessment_splits": list(SPLITS),
        "rows": rows, "descriptive_cross_period_flags": flags, "paired_feature_arm_deltas": deltas,
        "all_cell_net_1bp_rankings_by_split_horizon": rankings,
        "ranking_rule": "all cells, descending primary nonoverlap mean net bps at +1bp; no minimum-support filter, no selection or promotion",
        "error_delta_convention": "model error minus same-origin no-change error; negative improves. Arm deltas are candidate minus reference; negative error improves and positive policy mean improves.",
        "positive_flag_rule": "strictly positive observed policy mean with nonzero scored support in each named period; no significance or minimum-support qualification",
        "own_coverage_rule": "reported separately and never substituted for common-coverage errors",
        "models_promoted": 0, "automatic_model_selection": False,
        "untouched_confirmation": False, "can_place_orders": False,
        "limits": [
            "Validation and later_development_test are previously examined development periods, not untouched confirmation.",
            "ARIMA(1,1,0) here uses conditional OLS on log increments; it is different from the older statsmodels MLE implementation.",
            "Currency overlap makes pair results dependent; per-pair nonoverlap is not portfolio diversification or an account-return simulation.",
            "Net bps are equal unit-notional historical bid/ask endpoint proxies plus stated extra costs; they are not realized fills or dollar account returns.",
            "One-minute delay preserves original decisions but can have fewer valid outcomes; primary/delay means are not automatically a same-trade paired difference.",
            "Feature-arm policy decisions and trade counts can differ. Error comparisons use declared common coverage; matching counts alone are not an independent key audit.",
            "Strict/shared and additional endpoint-only membership uses future path completeness. These cohorts can describe results but cannot be used as origin-time entry filters.",
            "All cells remain visible, including low-support and losing cells. Positive-period flags and sorted means do not qualify a model for promotion.",
            "No refitting, outcome rescoring, model-file loading or forecast-file inspection is performed by this summary.",
        ],
    }


def _format(value, digits=3):
    return "—" if value is None else f"{value:+.{digits}f}"


def markdown_report(report):
    lines = ["# Fixed model comparison", "", "All 32 learned cells and 20 controls are shown separately for both assessment periods. These are previously examined development periods; no model is selected or promoted.", "",
        "The ARIMA control uses conditional OLS and differs from the older statsmodels MLE baseline. Pair/currency overlap remains: these endpoint bps are not portfolio or account returns.", "",
        "Errors use declared common coverage. Negative MAE/RMSE deltas improve on no-change. Policy decision counts are before scoring masks; scored counts apply endpoint, split and common-coverage requirements. Own-coverage errors and strict/additional endpoint cohorts are separate in the JSON.", ""]
    for split in SPLITS:
        lines += [f"## {split}", "", "| Cell | Common forecasts | Called direction % (n) | MAE Δ | RMSE Δ | Decisions / scored | Net +0 / +1 / +2 bps | PF +1 | Delay net +1 (n) | Days / pairs | Leave best day / pair out |", "| --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | --- | ---: | --- |"]
        for row in (r for r in report["rows"] if r["split"] == split):
            full = row["cohorts"]["full_endpoint"];errors = full["forecast_errors"];costs = full["net_cost_scenarios"]
            day, pair = full["primary_1bp_day_concentration"], full["primary_1bp_pair_concentration"]
            direction = "—" if errors["called_nonflat_direction_percent"] is None else f"{errors['called_nonflat_direction_percent']:.2f}%"
            lines.append(f"| {row['cell']} | {errors['scored_forecasts']:,} | {direction} ({errors['called_nonflat_rows']:,}) | {_format(errors['mae_delta_vs_no_change_bps'])} | {_format(errors['rmse_delta_vs_no_change_bps'])} | {full['nonoverlap_decisions_before_score_masks']:,} / {full['nonoverlap_scored_decisions']:,} | {' / '.join(_format(costs[c]['mean_net_bps']) for c in ('0.0','1.0','2.0'))} | {_format(costs['1.0']['profit_factor'],2)} | {_format(full['one_minute_delay']['mean_net_1bp_bps'])} ({full['one_minute_delay']['scored_decisions']:,}) | {day['observed_group_count']} / {pair['observed_group_count']} | {_format(day['leave_best_group_out_mean_net_bps'])} / {_format(pair['leave_best_group_out_mean_net_bps'])} |")
        lines += [""]
    lines += ["## Descriptive cross-period flags", "", "No support threshold hides cells. Positive periods at quoted spread only (+0) and extra +1/+2 bps are shown even when only one period is positive. V = validation; L = later development. Exact counts, days, pairs and concentration remain necessary context. All three path cohorts and their separate flags are retained in the JSON; future path membership cannot become an entry filter.", "", "| Cell | Positive periods +0 / +1 / +2 | Positive +0 both | Positive +1 both | Positive +2 both | Primary and delay +1 both | Minimum scored / delayed |", "| --- | --- | --- | --- | --- | --- | ---: |"]
    for tag, flags in report["descriptive_cross_period_flags"].items():
        yes = lambda value: "yes" if value else "no"
        periods = ["".join("V" if split == "validation" else "L" for split in flags["positive_periods_by_extra_cost_bps"][cost]) or "—" for cost in ("0.0", "1.0", "2.0")]
        lines.append(f"| {tag} | {' / '.join(periods)} | {yes(flags['positive_quoted_spread_only_both_periods'])} | {yes(flags['positive_net_1bp_both_periods'])} | {yes(flags['positive_net_2bp_both_periods'])} | {yes(flags['positive_primary_and_delay_1bp_both_periods'])} | {flags['minimum_scored_decisions_across_periods']:,} / {flags['minimum_delay_scored_decisions_across_periods']:,} |")
    lines += ["", "## Own-coverage errors", "", "These use each model's own valid/mature issued outcomes and are separate from common-coverage comparisons above.", "", "| Split | Cell | Common n / own n | Own MAE | Own RMSE |", "| --- | --- | ---: | ---: | ---: |"]
    for row in report["rows"]:
        own = row["own_coverage_forecast_errors"]
        lines.append(f"| {row['split']} | {row['cell']} | {row['cohorts']['full_endpoint']['forecast_errors']['scored_forecasts']:,} / {own['scored_forecasts']:,} | {_format(own['mae_bps'])} | {_format(own['rmse_bps'])} |")
    lines += ["", "## Feature-arm comparisons", "", "These compare the same learner, horizon and period. Policy differences compare each arm's own decisions; identical trade populations are not assumed.", "", "| Split | Learner / horizon | Candidate minus reference | Common n ref / cand | MAE Δ | RMSE Δ | Scored decisions ref / cand | Net +1 mean Δ |", "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |"]
    for row in report["paired_feature_arm_deltas"]:
        lines.append(f"| {row['split']} | {row['learner']} / {row['horizon_minutes']}m | {row['candidate_group']} − {row['reference_group']} | {row['reference_common_scored_forecasts']:,} / {row['candidate_common_scored_forecasts']:,} | {_format(row['mae_delta_candidate_minus_reference_bps'])} | {_format(row['rmse_delta_candidate_minus_reference_bps'])} | {row['reference_scored_decisions']:,} / {row['candidate_scored_decisions']:,} | {_format(row['net_1bp_mean_delta_candidate_minus_reference_bps'])} |")
    lines += ["", "## Limits", ""] + ["- " + item for item in report["limits"]]
    return "\n".join(lines) + "\n"


def run(args):
    comparison, output = Path(args.comparison).resolve(), Path(args.output).resolve()
    if output.exists() or output.is_relative_to(comparison):
        raise ValueError("new_separate_summary_output_required")
    report = summarize_comparison(comparison)
    report["generated_utc"] = datetime.now(timezone.utc).isoformat()
    payload = json.dumps(report, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    markdown = markdown_report(report)
    output.mkdir(parents=True, exist_ok=False)
    with (output / "COMPARISON_SUMMARY.json").open("x", encoding="utf-8") as stream:
        stream.write(payload)
    with (output / "README.md").open("x", encoding="utf-8") as stream:
        stream.write(markdown)
    return report


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--comparison", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


if __name__ == "__main__":
    result = run(parse_args())
    print(json.dumps({key: result[key] for key in ("metrics_files_verified", "learned_cells", "control_cells", "assessment_rows_in_summary")}))
