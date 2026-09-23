"""Read complete specialist metrics into a separate descriptive report.

No models, forecasts, training arrays or partial results are read. All registered
variants, gates, periods and path cohorts remain visible, without promotion.
"""
from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from tools import summarize_rolling_model_comparison_v1 as old_summary

RESULT_SCHEMA = "rolling_specialist_comparison_v1_20260915"
SUMMARY_SCHEMA = "rolling_specialist_summary_v1_20260915"
HORIZONS = (30, 60)
GROUPS = ("compact38", "compact50")
VARIANTS = ("direct", "mixture_raw", "mixture_calibrated", "direct_ridge",
            "specialist_ridge", "direct_context_hgb", "specialist_context_hgb")
GATES = ("primary_current_spread", "secondary_expected_cost")
SPLITS, COHORTS = old_summary.SPLITS, old_summary.COHORTS
POLICIES = {GATES[0]: "origin_spread_threshold_nonoverlap", GATES[1]: "expected_cost_threshold_nonoverlap"}
CONTRASTS = (("direct", "direct_ridge", "direct-only linear recalibration; unconstrained slope may reverse"),
             ("direct_ridge", "specialist_ridge", "specialist inputs beyond direct-only linear recalibration"),
             ("direct_context_hgb", "specialist_context_hgb", "specialists beyond the same context learner"),
             ("direct", "mixture_raw", "raw conditional decomposition"),
             ("direct", "mixture_calibrated", "calibrated conditional decomposition"))


def _hash(raw):
    return hashlib.sha256(raw).hexdigest()


def _load(raw):
    return old_summary._json(raw)


def _bound(root, record, expected_path, receipts):
    if record.get("path") != expected_path:
        raise ValueError("expected_metric_artifact_path_required:" + expected_path)
    path = (root / expected_path).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError("bound_artifact_must_stay_inside_result")
    raw = path.read_bytes()
    if _hash(raw) != record.get("sha256"):
        raise ValueError("bound_artifact_hash_mismatch:" + expected_path)
    receipts[path] = record["sha256"]
    return _load(raw)


def _population(source, count, label):
    if source["rows"] != count:
        raise ValueError("component_population_count_mismatch:" + label)
    for key in ("unique_original_utc_decision_times", "utc_days", "pairs"):
        value = old_summary._count(source[key], key)
        if value > count or (count > 0 and value == 0):
            raise ValueError("component_unique_count_outside_population:" + key)
    for key, total_key in (("counts_by_utc_day", "utc_days"), ("counts_by_pair", "pairs")):
        counts = source[key]
        if len(counts) != source[total_key] or sum(old_summary._count(v, key) for v in counts.values()) != count:
            raise ValueError("component_population_group_counts_mismatch:" + key)
    return copy.deepcopy(source)


def _equal_number(a, b):
    return a is None and b is None or a is not None and b is not None and math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-8)


def _component(cohort, metrics, gate, decision_hash):
    n = metrics["forecast_errors"]["scored_forecasts"]
    if (cohort["scored_origins"] != n or cohort["actual_positive"] + cohort["actual_nonpositive"] != n or
            not 0 <= cohort["actual_flat"] <= cohort["actual_nonpositive"]):
        raise ValueError("component_forecast_population_mismatch")
    selected = cohort["selection"][gate]
    if selected["decision_pair_clock_side_sha256"] != decision_hash:
        raise ValueError("component_original_decision_hash_mismatch")
    ndecision, nscored = metrics["nonoverlap_decisions_before_score_masks"], metrics["nonoverlap_scored_decisions"]
    _population(selected["all_original_decisions"], ndecision, "all_decisions")
    _population(selected["scored_decisions"], nscored, "scored_decisions")
    for population_key, concentration_key in (("utc_days", "primary_1bp_day_concentration"),
                                               ("pairs", "primary_1bp_pair_concentration")):
        if selected["scored_decisions"][population_key] != metrics[concentration_key]["observed_group_count"]:
            raise ValueError("component_scored_concentration_count_mismatch")
    if not _equal_number(selected["mean_actual_net_after_extra_1bp_bps"], metrics["net_cost_scenarios"]["1.0"]["mean_net_bps"]):
        raise ValueError("component_realized_net_mismatch")
    if nscored:
        lhs = selected["mean_expected_minus_actual_net_bps"]
        rhs = selected["mean_selected_return_optimism_bps"] + selected["mean_selected_exit_cost_optimism_bps"]
        if not _equal_number(lhs, rhs) or not 0 <= selected["maximum_decomposition_residual_bps"] <= 1e-8:
            raise ValueError("component_optimism_identity_mismatch")
    fields = ("positive_probability", "positive_conditional_magnitude", "nonpositive_conditional_magnitude",
              "expected_absolute_move", "raw_mixture_signed_mean", "direct_signed_mean", "variant_signed_mean",
              "variant_called_sign_return_optimism_bps", "long_exit_cost", "short_exit_cost")
    return {"head_probability_scope": "raw six-head probability, including in mixture_calibrated variants; use separate context probability table for calibrated probability scores",
            "actual_positive": cohort["actual_positive"], "actual_nonpositive": cohort["actual_nonpositive"],
            "actual_flat": cohort["actual_flat"], **{k: copy.deepcopy(cohort[k]) for k in fields},
            "selection": copy.deepcopy(selected)}


def _row(context, variant, gate, split, scores, diagnostics, metric_record, diagnostic_record):
    source = scores[gate][split]
    primary, delayed = source["primary"], source["one_minute_entry_delay"]
    policy = POLICIES[gate]
    for report in (primary, delayed):
        if (report["horizon_minutes"] != context["horizon_minutes"] or
                report["decision_rule"]["fixed_margin_bps"] != 1. or
                report["decision_rule"]["future_label_masks_used_for_decisions"] is not False or
                report["decision_rule"]["comparison_mask_used_for_decisions"] is not False or
                report["evaluation_cost"]["primary_scenario_key"] != "1.0"):
            raise ValueError("fixed_decision_contract_required")
        if gate == GATES[1] and (report["decision_rule"].get("gate_cost_is_observed_spread") is not False or
                                old_summary.POLICY in report["policies"]):
            raise ValueError("expected_cost_gate_must_not_be_labelled_observed_spread")
    for name in primary["policies"]:
        p, d = primary["policies"][name], delayed["policies"][name]
        if p["decisions"] != d["decisions"] or p["decision_pair_clock_side_sha256"] != d["decision_pair_clock_side_sha256"]:
            raise ValueError("delay_must_preserve_original_decisions")
    # Pure presentation adapter: reuse the established count/cost extractor.
    p = dict(primary, policies={old_summary.POLICY: primary["policies"][policy]})
    d = dict(delayed, policies={old_summary.POLICY: delayed["policies"][policy]})
    digest = primary["policies"][policy]["decision_pair_clock_side_sha256"]
    cohorts = {}
    for name in COHORTS:
        c = old_summary._cohort(p, d, name)
        c["components"] = _component(diagnostics["periods"][split]["cohorts"][name], c, gate, digest)
        cohorts[name] = c
    for accessor in (lambda c: c["forecast_errors"]["scored_forecasts"], lambda c: c["nonoverlap_scored_decisions"]):
        if accessor(cohorts[COHORTS[0]]) != sum(accessor(cohorts[c]) for c in COHORTS[1:]):
            raise ValueError("path_cohorts_must_partition_full")
    return {"context": f"{context['group']}_{context['horizon_minutes']}m", "variant": variant,
            "horizon_minutes": context["horizon_minutes"], "group": context["group"], "gate": gate, "split": split,
            "input_origins": primary["input_origins"], "issued_forecasts": primary["issued_forecasts"],
            "comparison_origins": primary["comparison_origins"], "cohorts": cohorts,
            "own_coverage_forecast_errors": old_summary._errors(primary["own_coverage_forecast_scores_without_comparator_restriction"]),
            "nonoverlap_decision_sha256": digest,
            "unscored_reason_partition": copy.deepcopy(primary["policies"][policy]["unscored_reason_partition"]),
            "metrics_artifact": dict(metric_record), "diagnostics_artifact": dict(diagnostic_record)}


def _probability(payload):
    if set(payload["periods"]) != set(SPLITS):
        raise ValueError("both_probability_periods_required")
    result = copy.deepcopy(payload)
    for split in SPLITS:
        if set(payload["periods"][split]) != set(COHORTS):
            raise ValueError("all_probability_cohorts_required")
        for cohort in COHORTS:
            scores = payload["periods"][split][cohort]
            if set(scores) != {"raw", "calibrated", "TRAIN_pair_prior"}:
                raise ValueError("raw_calibrated_prior_probability_scores_must_be_distinct")
            if len({(v["rows"], v["positive_rows"]) for v in scores.values()}) != 1:
                raise ValueError("probability_comparison_same_population_required")
            for value in scores.values():
                n = old_summary._count(value["rows"], "probability_rows")
                if not 0 <= old_summary._count(value["positive_rows"], "positive_rows") <= n:
                    raise ValueError("positive_rows_outside_probability_population")
                for key in ("brier", "log_loss"):
                    metric = old_summary._finite(value[key], key)
                    if n and (metric is None or metric < 0 or key == "brier" and metric > 1):
                        raise ValueError("probability_metric_outside_bounds")
    result["calibration_is_same_row_meta_input"] = False
    return result


def _deltas(rows):
    index = {(r["context"], r["variant"], r["gate"], r["split"]): r for r in rows}
    results = []
    for context in sorted({r["context"] for r in rows}):
        for reference, candidate, question in CONTRASTS:
            for gate in GATES:
                for split in SPLITS:
                    a, b = (index[context, variant, gate, split] for variant in (reference, candidate))
                    for cohort in COHORTS:
                        ac, bc = a["cohorts"][cohort], b["cohorts"][cohort]
                        ae, be = ac["forecast_errors"], bc["forecast_errors"]
                        same = ae["scored_forecasts"] == be["scored_forecasts"]
                        results.append({"context": context, "reference_variant": reference, "candidate_variant": candidate,
                            "question": question, "gate": gate, "split": split, "cohort": cohort,
                            "reference_scored_forecasts": ae["scored_forecasts"], "candidate_scored_forecasts": be["scored_forecasts"],
                            "same_declared_forecast_count": same,
                            "mae_delta_candidate_minus_reference_bps": old_summary._difference(be["mae_bps"], ae["mae_bps"]) if same else None,
                            "rmse_delta_candidate_minus_reference_bps": old_summary._difference(be["rmse_bps"], ae["rmse_bps"]) if same else None,
                            "reference_scored_decisions": ac["nonoverlap_scored_decisions"], "candidate_scored_decisions": bc["nonoverlap_scored_decisions"],
                            "net_mean_delta_candidate_minus_reference_by_extra_cost_bps": {
                                cost: old_summary._difference(bc["net_cost_scenarios"][cost]["mean_net_bps"], ac["net_cost_scenarios"][cost]["mean_net_bps"])
                                for cost in ("0.0", "1.0", "2.0")},
                            "same_original_decision_digest": a["nonoverlap_decision_sha256"] == b["nonoverlap_decision_sha256"],
                            "policy_scope": "each variant's own decisions; policy mean delta is not a matched-trade treatment effect"})
    return results


def _flags(rows):
    result = []
    for context, variant, gate in sorted({(r["context"], r["variant"], r["gate"]) for r in rows}):
        pair = {r["split"]: r for r in rows if (r["context"], r["variant"], r["gate"]) == (context, variant, gate)}
        result.append({"context": context, "variant": variant, "gate": gate,
                       "cohorts": {c: old_summary._period_flags({s: pair[s]["cohorts"][c] for s in SPLITS}) for c in COHORTS}})
    return result


def summarize_specialists(comparison_root, previous_comparison_root=None):
    root = Path(comparison_root).resolve()
    path = root / "RESULTS.json"; raw = path.read_bytes(); manifest = _load(raw)
    if manifest.get("schema") != RESULT_SCHEMA or manifest.get("status") != "complete":
        raise ValueError("complete_specialist_results_required")
    expected_contexts = {f"{g}_{h}m" for g in GROUPS for h in HORIZONS}
    if (set(manifest["contexts"]) != expected_contexts or manifest["completed_variants"] != 28 or
            manifest["completed_base_bundles"] != 20 or set(manifest["groups"]) != set(GROUPS) or
            set(manifest["horizons"]) != set(HORIZONS)):
        raise ValueError("all_four_contexts_28_variants_20_bundles_required")
    receipts = {path: _hash(raw)}
    rows, probabilities = [], {}
    for tag in sorted(expected_contexts):
        context = manifest["contexts"][tag]
        if tag != f"{context['group']}_{context['horizon_minutes']}m" or set(context["variants"]) != set(VARIANTS):
            raise ValueError("exact_context_and_seven_variants_required")
        record = context["probability_calibration_scores"]
        probability = _bound(root, record, f"diagnostics/{tag}_probability_calibration.json", receipts)
        probabilities[tag] = dict(_probability(probability), artifact=dict(record))
        for variant in VARIANTS:
            entry = context["variants"][variant]; vtag = tag + "_" + variant
            scores = _bound(root, entry["metrics"], f"metrics/{vtag}.json", receipts)
            diagnostic = _bound(root, entry["diagnostics"], f"diagnostics/{vtag}.json", receipts)
            if (scores.get("schema") != "rolling_specialist_scoring_v1_20260915" or
                    scores["future_labels_used_for_decisions"] is not False or
                    scores["mean_forecasts_changed_by_cost_gate"] is not False or
                    scores["all_original_assessment_origins"] != manifest["assessment_rows"] or
                    diagnostic["horizon_minutes"] != context["horizon_minutes"] or
                    diagnostic["future_cohorts_are_entry_filters"] is not False):
                raise ValueError("specialist_metric_scope_or_population_mismatch")
            for gate in GATES:
                if set(scores[gate]) != set(SPLITS):
                    raise ValueError("both_assessment_periods_required")
                if sum(scores[gate][s]["primary"]["input_origins"] for s in SPLITS) != manifest["assessment_rows"]:
                    raise ValueError("all_original_assessment_population_required")
                for split in SPLITS:
                    row = _row(context, variant, gate, split, scores, diagnostic, entry["metrics"], entry["diagnostics"])
                    for cohort in COHORTS:
                        n = row["cohorts"][cohort]["forecast_errors"]["scored_forecasts"]
                        pc = probabilities[tag]["periods"][split][cohort]
                        dc = row["cohorts"][cohort]["components"]["positive_probability"]
                        if pc["raw"]["rows"] != n or dc["model"]["rows"] != n:
                            raise ValueError("raw_probability_and_forecast_population_mismatch")
                        for key in ("brier", "log_loss"):
                            if not _equal_number(pc["raw"][key], dc["model"][key]):
                                raise ValueError("raw_head_probability_must_not_be_relabelled_calibrated")
                    rows.append(row)
            for split in SPLITS:
                if scores[GATES[0]][split]["primary"]["forecast_cohorts"] != scores[GATES[1]][split]["primary"]["forecast_cohorts"]:
                    raise ValueError("gate_change_must_preserve_forecast_errors")
    previous = Path(previous_comparison_root or manifest["previous_comparison_root"]).resolve()
    old_path = previous / "RESULTS.json"; previous_raw = old_path.read_bytes()
    if _hash(previous_raw) != manifest["previous_comparison_sha256"]:
        raise ValueError("pinned_previous_comparison_hash_required")
    previous_manifest = _load(previous_raw); receipts[old_path] = _hash(previous_raw)
    if (previous_manifest.get("status") != "complete" or previous_manifest.get("schema") != old_summary.RESULT_SCHEMA or
            previous_manifest["assessment_rows"] != manifest["assessment_rows"]):
        raise ValueError("same_completed_previous_assessment_population_required")
    comparator_rows, comparator_records = [], {}
    for h in HORIZONS:
        specifications = [(f"{learner}_{group}_{h}m", "learned") for learner in ("ridge", "hgb") for group in GROUPS]
        specifications += [(f"{name}_{h}m", "control") for name in old_summary.CONTROLS]
        for tag, kind in specifications:
            entry = previous_manifest["cells" if kind == "learned" else "baselines"][tag]
            record = entry["metrics"]
            metrics = _bound(previous, record, f"metrics/{tag}.json", receipts)
            comparator_records[tag] = dict(record)
            for split in SPLITS:
                item = old_summary._row(tag, entry, kind, split, metrics[split], record)
                item["gate"] = GATES[0]
                item["comparison_scope"] = "same observed-current-spread primary policy only; not the secondary predicted-cost gate"
                comparator_rows.append(item)
    rows.sort(key=lambda r: (GATES.index(r["gate"]), SPLITS.index(r["split"]), r["horizon_minutes"], r["group"], VARIANTS.index(r["variant"])))
    for source, digest in receipts.items():
        if _hash(source.read_bytes()) != digest:
            raise ValueError("input_artifact_changed_during_summary:" + str(source))
    return {"schema": SUMMARY_SCHEMA, "comparison_root": str(root), "results_sha256": _hash(raw),
            "previous_comparison_root": str(previous), "previous_results_sha256": _hash(previous_raw),
            "contexts": 4, "variants": 28, "specialist_gate_period_rows": len(rows),
            "previous_comparator_records": comparator_records, "previous_comparator_count": len(comparator_records),
            "previous_comparator_period_rows": len(comparator_rows),
            "hash_verified_files_including_manifests": len(receipts),
            "source_bindings": manifest.get("source_bindings", {}),
            "rows": rows, "context_probability_calibration": probabilities,
            "previous_primary_comparators": comparator_rows,
            "within_context_contrasts": _deltas(rows), "descriptive_positive_period_flags": _flags(rows),
            "automatic_selection": False, "models_promoted": 0, "can_place_orders": False,
            "limits": [
                "Previously examined development dates; these follow-up horizons were chosen from earlier development results, not untouched confirmation.",
                "New direct heads include two known asymmetric quote-cost inputs beyond compact38/50. They are not identical to older HGB direct fits.",
                "All variants, costs, periods and cohorts remain visible; no minimum-support cutoff, 50% filter, automatic winner or promotion.",
                "Both gates preserve original signed-mean forecasts. Secondary expected costs are predictions, not observed spread; comparisons with old policies use primary observed-spread only.",
                "Raw/calibrated probability scores belong to four contexts. Repeated component probability diagnostics remain raw even for calibrated-mixture mean variants.",
                "direct_ridge is unconstrained direct-only linear recalibration. Its slope may reverse the original signal; it is not necessarily shrinkage or probability calibration.",
                "Error deltas compare declared common forecast populations; policy means use each variant's own decisions, which can differ.",
                "Delay preserves original decisions but can have different valid-outcome coverage; mean differences are not automatically matched-trade effects.",
                "Future shared-path and additional-endpoint cohorts are posthoc descriptions, never entry filters.",
                "Unique clocks, days and pairs are support descriptions, not independent sample counts. Common currency exposures remain dependent.",
                "Unit-notional candle endpoint bps are not fills, portfolio returns or account dollars. No new fitting, forecast-file loading or outcome rescoring is performed.",
                "ARIMA is the aligned conditional-OLS comparator, different from older maximum-likelihood results; an inactive policy has no selected-return mean, not a trading loss."]}


def _fmt(value, digits=3):
    return "—" if value is None else f"{value:+.{digits}f}"


def markdown_report(report):
    lines = ["# Rolling specialist comparison", "", "All 28 variants, both entry rules and both development periods are retained. Every row's three path cohorts are preserved in the JSON. No candidate is selected or promoted.", "",
             "Primary uses observed current spread +1bp. Secondary uses known same-side entry cost plus predicted exit cost +1bp. Previous model comparators share only the primary gate. Costs below are actual endpoint bid/ask plus the stated extra bps.", "",
             "direct_ridge is direct-only linear recalibration with an unconstrained slope: it may reverse the original signal, not merely shrink it.", ""]
    for gate in GATES:
        for split in SPLITS:
            lines += [f"## {gate} / {split}", "", "| Context / variant | Common forecasts | Direction % | MAE / RMSE Δ vs zero | Decisions / scored | Net +0 / +1 / +2 | PF +1 | Delay +1 (n) | Times / days / pairs | Leave best day / pair | Return / cost optimism |",
                      "| --- | ---: | ---: | --- | ---: | --- | ---: | --- | ---: | --- | --- |"]
            for row in (r for r in report["rows"] if r["gate"] == gate and r["split"] == split):
                c = row["cohorts"]["full_endpoint"]; e = c["forecast_errors"]; s = c["components"]["selection"]
                pop = s["scored_decisions"]; costs = c["net_cost_scenarios"]
                lines.append(f"| {row['context']} / {row['variant']} | {e['scored_forecasts']:,} | {_fmt(e['called_nonflat_direction_percent'],2)} | {_fmt(e['mae_delta_vs_no_change_bps'])} / {_fmt(e['rmse_delta_vs_no_change_bps'])} | {c['nonoverlap_decisions_before_score_masks']:,} / {c['nonoverlap_scored_decisions']:,} | {' / '.join(_fmt(costs[k]['mean_net_bps']) for k in ('0.0','1.0','2.0'))} | {_fmt(costs['1.0']['profit_factor'],2)} | {_fmt(c['one_minute_delay']['mean_net_1bp_bps'])} ({c['one_minute_delay']['scored_decisions']:,}) | {pop['unique_original_utc_decision_times']} / {pop['utc_days']} / {pop['pairs']} | {_fmt(c['primary_1bp_day_concentration']['leave_best_group_out_mean_net_bps'])} / {_fmt(c['primary_1bp_pair_concentration']['leave_best_group_out_mean_net_bps'])} | {_fmt(s['mean_selected_return_optimism_bps'])} / {_fmt(s['mean_selected_exit_cost_optimism_bps'])} |")
            lines.append("")
    lines += ["## Probability calibration by context", "", "These are separate raw/calibrated positive-event scores; exact flats are nonpositive. Raw head scores repeated in mean-variant diagnostics must not be called calibrated.", "",
              "| Context | Period / cohort | n | Raw / calibrated / TRAIN-prior Brier | Raw / calibrated / TRAIN-prior log loss |", "| --- | --- | ---: | --- | --- |"]
    for context, payload in report["context_probability_calibration"].items():
        for split in SPLITS:
            for cohort in COHORTS:
                c = payload["periods"][split][cohort]
                lines.append(f"| {context} | {split} / {cohort} | {c['raw']['rows']:,} | {' / '.join(_fmt(c[k]['brier'],6) for k in ('raw','calibrated','TRAIN_pair_prior'))} | {' / '.join(_fmt(c[k]['log_loss'],6) for k in ('raw','calibrated','TRAIN_pair_prior'))} |")
    lines += ["", "## Positive-period cases, including quoted-only and small support", "", "V = validation; L = later development. All cells remain in the preceding tables. This list describes every cell/cohort with any positive mean at any stated cost; it is not a selection rule.", "",
              "| Context / variant | Gate / cohort | Positive periods +0 / +1 / +2 | Positive primary+delay +1 both | Minimum primary / delay n |", "| --- | --- | --- | --- | ---: |"]
    positive_count = 0
    for row in report["descriptive_positive_period_flags"]:
        for cohort, f in row["cohorts"].items():
            if not any(f["positive_in_any_period_by_extra_cost_bps"].values()):
                continue
            positive_count += 1
            periods = ["".join("V" if s == SPLITS[0] else "L" for s in f["positive_periods_by_extra_cost_bps"][k]) or "—" for k in ("0.0", "1.0", "2.0")]
            lines.append(f"| {row['context']} / {row['variant']} | {row['gate']} / {cohort} | {' / '.join(periods)} | {f['positive_primary_and_delay_1bp_both_periods']} | {f['minimum_scored_decisions_across_periods']} / {f['minimum_delay_scored_decisions_across_periods']} |")
    if not positive_count:
        lines += ["", "No positive quoted-cost mean in either period or cohort; all inactive and negative cases remain reported."]
    lines += ["", "## Within-context contrasts", "", "Negative error deltas improve errors. Net deltas compare each variant's own decisions, not identical trades. Full-endpoint rows appear here; all three cohorts remain in JSON.", "",
              "| Context | Period / gate | Candidate minus reference | MAE / RMSE Δ | Scored n reference / candidate | Net +1 mean Δ |", "| --- | --- | --- | --- | ---: | ---: |"]
    for row in report["within_context_contrasts"]:
        if row["cohort"] != "full_endpoint": continue
        lines.append(f"| {row['context']} | {row['split']} / {row['gate']} | {row['candidate_variant']} − {row['reference_variant']} | {_fmt(row['mae_delta_candidate_minus_reference_bps'])} / {_fmt(row['rmse_delta_candidate_minus_reference_bps'])} | {row['reference_scored_decisions']} / {row['candidate_scored_decisions']} | {_fmt(row['net_mean_delta_candidate_minus_reference_by_extra_cost_bps']['1.0'])} |")
    lines += ["", "## Retained previous comparators: primary observed-spread gate only", "", "Exactly 18 previous model/control records, two periods each. These are retained scores, not refits. No comparison to the secondary gate is represented as a same-policy change.", "",
              "| Previous cell | Period | Common forecasts | MAE / RMSE Δ vs zero | Scored n | Net +0 / +1 / +2 | Delay +1 (n) |", "| --- | --- | ---: | --- | ---: | --- | --- |"]
    for row in report["previous_primary_comparators"]:
        c = row["cohorts"]["full_endpoint"]; e = c["forecast_errors"]
        lines.append(f"| {row['cell']} | {row['split']} | {e['scored_forecasts']:,} | {_fmt(e['mae_delta_vs_no_change_bps'])} / {_fmt(e['rmse_delta_vs_no_change_bps'])} | {c['nonoverlap_scored_decisions']} | {' / '.join(_fmt(c['net_cost_scenarios'][k]['mean_net_bps']) for k in ('0.0','1.0','2.0'))} | {_fmt(c['one_minute_delay']['mean_net_1bp_bps'])} ({c['one_minute_delay']['scored_decisions']}) |")
    lines += ["", "## Limits", ""] + ["- " + item for item in report["limits"]]
    return "\n".join(lines) + "\n"


def run(args):
    comparison = Path(args.comparison).resolve(); output = Path(args.output).resolve()
    previous = getattr(args, "previous_comparison", None)
    if output.exists() or output.is_relative_to(comparison):
        raise ValueError("new_separate_output_required")
    report = summarize_specialists(comparison, previous)
    if output.is_relative_to(Path(report["previous_comparison_root"])):
        raise ValueError("summary_output_cannot_modify_previous_comparison")
    report["generated_utc"] = datetime.now(timezone.utc).isoformat()
    payload = json.dumps(report, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    markdown = markdown_report(report)
    output.mkdir(parents=True, exist_ok=False)
    (output / "SUMMARY.json").write_text(payload, encoding="utf-8")
    (output / "SUMMARY.md").write_text(markdown, encoding="utf-8")
    return report


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--comparison", type=Path, required=True)
    parser.add_argument("--previous-comparison", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


if __name__ == "__main__":
    result = run(parse_args())
    print(json.dumps({k: result[k] for k in ("contexts", "variants", "specialist_gate_period_rows", "previous_comparator_count", "hash_verified_files_including_manifests")}))
