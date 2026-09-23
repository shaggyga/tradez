"""Read frozen saved JSON only and summarize prediction diagnostics.

No SQLite connection, worker import, network call, or production write occurs.
Run with python -B. Weighted scores reconstruct rounded saved aggregates and
are descriptive; row counts cannot be used as independent sample sizes.
"""
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict
import hashlib
import json

ROOT = Path(r"C:\Users\zmoor\Documents\forex\trad")
OUT = Path(__file__).resolve().parent
DATA = ROOT / "data/oanda_training_manager"
sources = []

def read(relative):
    path = DATA / relative
    raw = path.read_bytes()
    sources.append({"path": str(path), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
    return json.loads(raw.decode("utf-8-sig"))

def weighted(rows, metric, weight):
    usable = [row for row in rows if row.get(metric) is not None and row.get(weight, 0) > 0]
    n = sum(row[weight] for row in usable)
    return None if not n else sum(row[metric] * row[weight] for row in usable) / n

def group_rows(rows, key):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row[key]].append(row)
    return grouped

def direction_summary(rows):
    n = sum(row["n"] for row in rows)
    return {"raw_matured_rows": n,
            "direction_hits": sum(row["direction_hits"] for row in rows),
            "direction_accuracy": sum(row["direction_hits"] for row in rows) / n,
            "after_cost_wins": sum(row["wins"] for row in rows),
            "after_cost_win_rate": sum(row["wins"] for row in rows) / n,
            "raw_pooled_average_net_pips_diagnostic_only": weighted(rows, "avg_net_pips", "n"),
            "raw_pooled_average_gross_pips_diagnostic_only": weighted(rows, "avg_gross_pips", "n")}

top = read("state/top_signal_position_ledger_v1.json")
cal = read("state/timeframe_matrix_calibration_v1.json")
matrix = read("state/pair_family_timeframe_horizon_v1.json")
lifecycle = read("state/evidence_lifecycle_v1.json")
opportunity = read("state/executable_opportunity_prospective_v1.json")
archive = read("reports/executable_opportunity_ranking/EXECUTABLE_OPPORTUNITY_RANKING_V2_20260814.json")
source = read("local_news_sentiment/causal_source_factor_response_map_latest_v8.json")
rank = read("state/source_conditioned_currency_rank_v7.json")
watch = read("state/news_technical_watchlist_v1.json")

calibration = {}
for scope in ("global_surfaces", "pair_surfaces"):
    rows = list(cal[scope].values())
    metrics = ["raw_accuracy", "calibrated_accuracy", "raw_brier", "calibrated_brier",
               "raw_average_net_pips", "calibrated_average_net_pips", "calibrated_win_rate"]
    summary = {metric: weighted(rows, metric, "oos_n") for metric in metrics}
    # Stored bins cover warmup plus OOS. Recover integer up counts only where
    # six-decimal bin-rate rounding cannot alter the nearest integer count.
    assert max(b["n"] for row in rows for b in row["bins"]) < 1_000_000
    all_up = sum(round(b["up_rate"] * b["n"]) for row in rows for b in row["bins"] if b["n"])
    n = sum(row["oos_n"] for row in rows)
    total = sum(row["n"] for row in rows)
    warmup = total - n
    up_bounds = [max(0, all_up - warmup) / n, min(n, all_up) / n]
    summary.update({"surface_count": len(rows), "overlapping_oos_row_evaluations": n,
                    "validation_ready_surfaces": sum(row["validation_ready"] for row in rows),
                    "constant_half_probability_brier_reference": 0.25,
                    "brier_improvement_vs_constant_half": 0.25 - summary["calibrated_brier"],
                    "exact_oos_class_balance_stored": False,
                    "all_rows_up_count_from_rounded_bins": all_up,
                    "excluded_warmup_rows": warmup,
                    "oos_up_rate_bounds_from_unknown_warmup_labels": up_bounds,
                    "hindsight_pooled_majority_accuracy_bounds": [max(0.5, up_bounds[0], 1-up_bounds[1]), max(up_bounds[1], 1-up_bounds[0])],
                    "independent_sample_size": None,
                    "first_and_last_forecast_issue_utc": None,
                    "evaluation_type": "completed_outcome_row_order_recalibration_replay; not verified issue_time_calibrated_forecasts",
                    "by_horizon": [{"horizon_sec": horizon, "surfaces": len(group),
                                    "overlapping_oos_row_evaluations": sum(row["oos_n"] for row in group),
                                    **{metric: weighted(group, metric, "oos_n") for metric in metrics}}
                                   for horizon, group in sorted(group_rows(rows, "horizon_sec").items())]})
    calibration[scope] = summary

families = []
for velocity in lifecycle["proof_cohort_evidence_velocity"]:
    name = velocity["family"]
    rows = [row for row in matrix["rows"] if row["family"] == name]
    assert len(rows) == 68
    families.append({"family": name, "current_prospective_lifecycle_snapshot": velocity,
                     "separate_exit_fit_holdout_summary": {
                         "evaluation_type": "oldest70pct_fit_newest30pct_calibration_holdout; separate slice, not full prospective cohort",
                         "instrument_count": len(rows), "holdout_rows": sum(row["holdout_n"] for row in rows),
                         "horizons_sec": sorted({row["horizon_sec"] for row in rows}),
                         "direction_accuracy_pct": weighted(rows, "direction_accuracy_pct", "holdout_n"),
                         "after_cost_win_rate_pct": weighted(rows, "win_rate_pct", "holdout_n"),
                         "raw_pooled_net_pips_diagnostic_only": weighted(rows, "average_net_pips", "holdout_n"),
                         "positive_mean_pair_cells": sum(row["average_net_pips"] > 0 for row in rows),
                         "eligible_cells": sum(row["eligible"] for row in rows),
                         "independent_blocks_per_pair_range": [min(row["independent_blocks"] for row in rows), max(row["independent_blocks"] for row in rows)],
                         "first_and_last_issue_utc": None,
                         "all_pair_cells": rows}})

archive_results = []
for row in archive["results"]:
    p = row["clearance"]["base_rate"]
    archive_results.append({**row,
                            "hindsight_validation_constant_base_rate_brier_reference": p*(1-p),
                            "brier_improvement_vs_hindsight_validation_base_rate": p*(1-p)-row["clearance"]["brier"],
                            "baseline_caveat": "descriptive validation prevalence, not a prefit deployable baseline"})

code_sources = []
for name, lines, purpose in [
    ("oanda_timeframe_matrix_calibration.py", "117-154, 446-489", "completed-outcome ordered replay scores before updating current label; no issue-time label cutoff"),
    ("oanda_top_signal_position_ledger.py", "918-951", "exact current measurement version, matured and maturity_valid=1 filter; direction gross>0"),
    ("oanda_executable_opportunity_ranking.py", None, "historical archive evaluation producer")]:
    path = ROOT / name
    code_sources.append({"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "relevant_lines": lines, "purpose": purpose})

result = {
    "generated_utc": datetime.now(timezone.utc).isoformat(),
    "task": "saved_prediction_quality_assessment",
    "verdict": "useful predictive edge not established; limited descriptive calibration/magnitude improvements; poor after-cost results",
    "scope": {"saved_json_only": True, "production_sqlite_connections": 0, "production_writes": 0,
              "runtime_starts": 0, "network_calls": 0, "credential_reads": 0, "new_predictions_generated": 0},
    "current_top_signal_shadow": {"generated_utc": top["generated_utc"], "measurement": top["measurement"],
                                  "first_and_last_issue_utc": None, "date_range_limitation": "aggregate first/last issue not stored in saved summary; latest80 positions are not full population",
                                  "independent_n": None, "majority_class_baseline": None, "filled_trade_win_rate": False,
                                  "overall": direction_summary(top["by_horizon"]),
                                  "by_policy_state": {key: direction_summary(rows) for key, rows in group_rows(top["by_horizon"], "policy_state").items()},
                                  "by_horizon_combining_policy_states": [{"horizon_sec": key, **direction_summary(rows)} for key, rows in sorted(group_rows(top["by_horizon"], "horizon_sec").items())],
                                  "original_horizon_policy_rows": top["by_horizon"],
                                  "maturity_integrity_all_versions": top["maturity_integrity"]},
    "equation_calibration": {"generated_utc": cal["generated_utc"], "model_family": "timeframe_equation_matrix", "current_four_families": False,
                             "account_eligible": cal["account_eligible"], "scopes_not_additive": True, **calibration},
    "four_active_families": {"lifecycle_generated_utc": lifecycle["generated_utc"], "exit_fit_matrix_generated_utc": matrix["generated_at"], "families": families},
    "lifecycle": lifecycle["lifecycle"],
    "pair_family_matrix_all_cells": matrix["counts"],
    "opportunity": {"generated_utc": opportunity["generated_utc"], "cohort_id": opportunity["cohort_id"],
                    "current_cohort": opportunity["totals"], "older_seven_cohorts_diagnostic_only": opportunity["all_cohort_diagnostics"]},
    "archive_opportunity_discovery": {"created_utc": archive["created_utc"], "cohort_id": archive["cohort_id"],
                                     "evidence_class": archive["evidence_class"], "limitations": archive["limitations"], "results": archive_results},
    "source_v8": {"generated_utc": source["generated_utc"], "cohort_id": source["cohort_id"], "contract_id": source["contract_id"],
                  "counts": source["counts"], "response_summary_is_realized_response_not_accuracy": True,
                  "actual_predictive_direction_accuracy": None, "source_forecast_inventory": rank["source_forecast_inventory"]},
    "rank_v7": {"generated_utc": rank["generated_utc"], "contract_id": rank["contract_id"], "ledger": rank["ledger"],
                "comparison_arms": rank["comparison_arms"], "source_vs_price_incremental_skill": None},
    "limitations": ["No single accuracy number is valid across all model, pair, horizon, lineage and cohort populations.",
                    "Mixed-pair raw pip means are diagnostic only; they are not currency-denominated P/L or comparable economic weights.",
                    "Do not sum correlated horizons, cross-pair factors, global/pair surfaces, repeated forecasts or effectiveN values.",
                    "Calibration replay lacks an explicit original-issue-time mature-label cutoff, so its adjusted metrics are not prospective availability proof.",
                    "An old contract or quarantine diagnostic cannot be imported into the repaired inactive V9/V8 successor evidence.",
                    "Saved JSON is not sufficient to estimate valid independent accuracy confidence intervals or exact date ranges where absent.",
                    "No common same-clock, untouched price-only and no-trade comparison with enough independent evidence establishes source incremental prediction skill."],
    "sources": sources,
    "code_sources": code_sources,
}
path = OUT / "PREDICTION_QUALITY_RECEIPT.json"
path.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")
print(json.dumps({"receipt": str(path), "input_files": len(sources), "overall_direction": result["current_top_signal_shadow"]["overall"],
                  "global_calibration": {key: calibration["global_surfaces"][key] for key in ["raw_accuracy","calibrated_accuracy","calibrated_brier","hindsight_pooled_majority_accuracy_bounds"]}}, indent=2))
