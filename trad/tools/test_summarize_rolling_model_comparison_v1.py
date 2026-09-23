"""Synthetic hashed-metric fixtures only; no fitting or real result reads."""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from tools import summarize_rolling_model_comparison_v1 as summary


def error_scores(n, mae=2., rmse=3.):
    return {"scored_forecasts": n, "mae_bps": mae if n else None, "rmse_bps": rmse if n else None,
        "no_change_baseline": {"same_origin_rows": n, "mae_bps": 2.5 if n else None, "rmse_bps": 3.5 if n else None},
        "direction": {"called_nonflat_outcome_rows": max(0, n - 1),
                      "direction_accuracy_on_called_nonflat_outcomes": .55 if n > 1 else None,
                      "predicted_flat": 0, "actual_flat": min(n, 1)}}


def concentration(n, key, value, mean):
    return {"observed_group_count": 1 if n else 0,
        "ranked_groups": [{key: value, "scored_decisions": n, "mean_net_bps": mean,
                           "sum_unit_notional_net_bps": n * mean}] if n else [],
        "best_group": value if n else None,
        "best_group_share_of_positive_group_net": 1. if n and mean > 0 else None,
        "leave_best_group_out_mean_net_bps": None}


def metrics(h, tag, *, mean_net_1bp=.5, delay_mean_1bp=.25, decisions=5, scored_counts=(3, 2, 1), forecast_counts=(10, 6, 4), mae=2.):
    result = {}
    for split in summary.SPLITS:
        reports = {}
        for phase, net1 in (("primary", mean_net_1bp), ("one_minute_entry_delay", delay_mean_1bp)):
            cohorts = {}
            forecasts = {}
            for cohort, scored, n in zip(summary.COHORTS, scored_counts, forecast_counts):
                costs = {key: {"scored_decisions": scored, "mean_net_bps": net1 + 1 - float(key) if scored else None,
                               "profit_factor": 1.5 if scored else None, "positive_fraction": .6 if scored else None,
                               "sum_unit_notional_net_bps": (net1 + 1 - float(key)) * scored} for key in ("0.0", "1.0", "2.0")}
                cohorts[cohort] = {"scored_decisions": scored, "long_scored_decisions": scored, "short_scored_decisions": 0,
                    "cost_scenarios": costs,
                    "primary_cost_day_concentration": concentration(scored, "utc_day", "2026-08-24" if split == "validation" else "2026-08-31", net1),
                    "primary_cost_pair_concentration": concentration(scored, "pair", "EUR_USD", net1)}
                forecasts[cohort] = error_scores(n, mae, mae + 1)
            reports[phase] = {"horizon_minutes": h, "input_origins": 12, "issued_forecasts": 12,
                "comparison_origins": 11, "issued_comparison_origins": 11, "forecast_coverage": 1.,
                "decision_rule": {"fixed_margin_bps": 1., "future_label_masks_used_for_decisions": False,
                                  "comparison_mask_used_for_decisions": False},
                "evaluation_cost": {"primary_scenario_key": "1.0"}, "forecast_cohorts": forecasts,
                "policies": {summary.POLICY: {"decisions": decisions, "decision_pair_clock_side_sha256": tag + ":" + split,
                    "cohorts": cohorts, "unscored_reason_partition": {"scored_full_endpoint": scored_counts[0], "other": decisions - scored_counts[0]}}},
                "own_coverage_forecast_scores_without_comparator_restriction": error_scores(12, .1, .2)}
        result[split] = reports
    return result


def write_metric(root, manifest, tag, payload):
    path = root / "metrics" / (tag + ".json")
    raw = json.dumps(payload, allow_nan=False, separators=(",", ":")).encode()
    path.write_bytes(raw)
    entries = manifest["cells"] if tag in manifest["cells"] else manifest["baselines"]
    entries[tag]["metrics"] = {"path": "metrics/" + path.name, "sha256": hashlib.sha256(raw).hexdigest()}
    (root / "RESULTS.json").write_text(json.dumps(manifest), encoding="utf-8")


def fixture(directory):
    root = Path(directory) / "comparison"
    root.mkdir(); (root / "metrics").mkdir()
    manifest = {"schema": summary.RESULT_SCHEMA, "status": "complete", "learned_fit_count": 32,
        "horizons": list(summary.HORIZONS), "groups": {name: [] for name in summary.GROUPS},
        "cells": {}, "baselines": {}, "source_bindings": {"synthetic.py": "synthetic-source-digest"}}
    for learner in summary.LEARNERS:
        for group, size in summary.GROUPS.items():
            for h in summary.HORIZONS:
                tag = f"{learner}_{group}_{h}m"
                manifest["cells"][tag] = {"learner": learner, "group": group, "horizon_minutes": h,
                    "registered_input_count": size, "training_selection": {"rows": 1000, "pair_clock_sha256": f"clocks-{h}", "target_sha256": f"labels-{h}"},
                    "model": {"path": "must_not_open_model.joblib", "sha256": "missing"},
                    "forecast": {"path": "must_not_open_forecasts.parquet", "sha256": "missing"}}
                write_metric(root, manifest, tag, metrics(h, tag, mae=2. - size / 1000))
    for name in summary.CONTROLS:
        for h in summary.HORIZONS:
            tag = f"{name}_{h}m"
            manifest["baselines"][tag] = {"name": name, "horizon_minutes": h,
                "forecast": {"path": "must_not_open_control_forecasts.parquet", "sha256": "missing"}}
            write_metric(root, manifest, tag, metrics(h, tag, mean_net_1bp=-1., delay_mean_1bp=-2.))
    return root, manifest


class ComparisonSummaryTests(unittest.TestCase):
    def test_all_cells_periods_and_small_support_preserved_without_loading_models(self):
        with tempfile.TemporaryDirectory() as directory:
            root, _ = fixture(directory)
            report = summary.summarize_comparison(root)
            self.assertEqual(report["metrics_files_verified"], 52)
            self.assertEqual(len(report["rows"]), 104)
            self.assertEqual(len(report["descriptive_cross_period_flags"]), 52)
            self.assertEqual(len(report["paired_feature_arm_deltas"]), 64)
            self.assertTrue(all(set(row["cohorts"]) == set(summary.COHORTS) for row in report["rows"]))
            self.assertEqual(report["models_promoted"], 0)
            self.assertFalse(report["automatic_model_selection"])
            flag = report["descriptive_cross_period_flags"]["ridge_compact38_5m"]
            self.assertTrue(flag["positive_net_1bp_both_periods"])
            self.assertTrue(flag["positive_quoted_spread_only_both_periods"])
            self.assertFalse(flag["positive_net_2bp_both_periods"])
            self.assertTrue(flag["positive_primary_and_delay_1bp_both_periods"])
            self.assertEqual(flag["minimum_scored_decisions_across_periods"], 3)
            self.assertFalse(flag["promotion"])
            self.assertEqual(flag["observed_scored_days_by_period"], {split: 1 for split in summary.SPLITS})
            self.assertEqual(len(report["all_cell_net_1bp_rankings_by_split_horizon"]["validation"]["5"]), 13)
            json.dumps(report, allow_nan=False)

    def test_common_errors_not_replaced_by_more_flattering_own_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            root, manifest = fixture(directory)
            before = summary.summarize_comparison(root)
            tag = "ridge_compact38_5m"
            payload = json.loads((root / "metrics" / (tag + ".json")).read_text())
            for split in summary.SPLITS:
                payload[split]["primary"]["own_coverage_forecast_scores_without_comparator_restriction"] = error_scores(12, 1000., 2000.)
            write_metric(root, manifest, tag, payload)
            after = summary.summarize_comparison(root)
            a = next(row for row in before["rows"] if row["cell"] == tag)
            b = next(row for row in after["rows"] if row["cell"] == tag)
            self.assertEqual(a["cohorts"], b["cohorts"])
            self.assertNotEqual(a["own_coverage_forecast_errors"], b["own_coverage_forecast_errors"])
            self.assertEqual(before["paired_feature_arm_deltas"], after["paired_feature_arm_deltas"])
            self.assertEqual(before["all_cell_net_1bp_rankings_by_split_horizon"], after["all_cell_net_1bp_rankings_by_split_horizon"])

    def test_arm_deltas_keep_different_decisions_and_counts_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            root, manifest = fixture(directory)
            tag = "ridge_compact50_5m"
            write_metric(root, manifest, tag, metrics(5, tag, decisions=7, scored_counts=(4, 2, 2), mae=1.5, mean_net_1bp=1.5))
            report = summary.summarize_comparison(root)
            row = next(row for row in report["paired_feature_arm_deltas"] if row["learner"] == "ridge" and row["horizon_minutes"] == 5 and row["split"] == "validation" and row["candidate_group"] == "compact50")
            self.assertEqual(row["reference_scored_decisions"], 3)
            self.assertEqual(row["candidate_scored_decisions"], 4)
            self.assertFalse(row["identical_original_policy_decisions"])
            self.assertAlmostEqual(row["mae_delta_candidate_minus_reference_bps"], 1.5 - 1.962)
            self.assertEqual(row["net_1bp_mean_delta_candidate_minus_reference_bps"], 1.)
            self.assertIn("not paired-trade", row["policy_delta_scope"])

    def test_different_common_forecast_counts_withhold_error_arm_delta_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root, manifest = fixture(directory)
            tag = "ridge_compact50_5m"
            write_metric(root, manifest, tag, metrics(5, tag, forecast_counts=(9, 6, 3)))
            report = summary.summarize_comparison(root)
            row = next(row for row in report["paired_feature_arm_deltas"] if row["learner"] == "ridge" and row["horizon_minutes"] == 5 and row["split"] == "validation" and row["candidate_group"] == "compact50")
            self.assertFalse(row["common_forecast_counts_match"])
            self.assertIsNone(row["mae_delta_candidate_minus_reference_bps"])
            self.assertEqual(len(report["rows"]), 104)

    def test_losing_second_period_or_delay_prevents_descriptive_robust_flags(self):
        with tempfile.TemporaryDirectory() as directory:
            root, manifest = fixture(directory)
            tag = "ridge_compact38_5m"
            payload = metrics(5, tag, mean_net_1bp=.5, delay_mean_1bp=-.1)
            write_metric(root, manifest, tag, payload)
            report = summary.summarize_comparison(root)
            flag = report["descriptive_cross_period_flags"][tag]
            self.assertTrue(flag["positive_net_1bp_both_periods"])
            self.assertFalse(flag["positive_primary_and_delay_1bp_both_periods"])
            payload["later_development_test"] = metrics(5, tag, mean_net_1bp=-1.)["later_development_test"]
            write_metric(root, manifest, tag, payload)
            self.assertFalse(summary.summarize_comparison(root)["descriptive_cross_period_flags"][tag]["positive_net_1bp_both_periods"])

    def test_quoted_only_single_period_potential_and_all_path_cohort_flags_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            root, manifest = fixture(directory)
            tag = "hgb_compact50_30m"
            payload = metrics(30, tag, mean_net_1bp=-3.)
            payload["later_development_test"] = metrics(30, tag, mean_net_1bp=-.1)["later_development_test"]
            write_metric(root, manifest, tag, payload)
            report = summary.summarize_comparison(root)
            flags = report["descriptive_cross_period_flags"][tag]
            self.assertEqual(flags["positive_periods_by_extra_cost_bps"]["0.0"], ["later_development_test"])
            self.assertTrue(flags["positive_in_any_period_by_extra_cost_bps"]["0.0"])
            self.assertTrue(flags["positive_in_exactly_one_period_by_extra_cost_bps"]["0.0"])
            self.assertFalse(flags["positive_quoted_spread_only_both_periods"])
            self.assertEqual(flags["positive_periods_by_extra_cost_bps"]["1.0"], [])
            self.assertEqual(set(flags["by_path_cohort"]), set(summary.COHORTS))
            self.assertIn("never an origin-time entry filter", flags["path_cohort_use"])

    def test_incomplete_or_missing_cell_or_period_refused(self):
        for failure in ("running", "missing_cell", "missing_period"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root, manifest = fixture(directory)
                tag = "ridge_compact38_5m"
                if failure == "running":
                    manifest["status"] = "running"
                elif failure == "missing_cell":
                    del manifest["cells"][tag]
                else:
                    payload = metrics(5, tag); del payload["validation"]
                    write_metric(root, manifest, tag, payload)
                (root / "RESULTS.json").write_text(json.dumps(manifest))
                with self.assertRaises(ValueError):
                    summary.summarize_comparison(root)

    def test_corrupted_metrics_and_escape_paths_refused(self):
        for failure in ("hash", "escape"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root, manifest = fixture(directory)
                tag = "ridge_compact38_5m"
                if failure == "hash":
                    with (root / "metrics" / (tag + ".json")).open("ab") as stream:
                        stream.write(b" ")
                else:
                    manifest["cells"][tag]["metrics"]["path"] = "../outside.json"
                    (root / "RESULTS.json").write_text(json.dumps(manifest))
                with self.assertRaises(ValueError):
                    summary.summarize_comparison(root)

    def test_source_population_and_delay_decision_mismatch_refused(self):
        for failure in ("training", "delay"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root, manifest = fixture(directory)
                tag = "ridge_compact50_5m"
                if failure == "training":
                    manifest["cells"][tag]["training_selection"]["target_sha256"] = "different-targets"
                    (root / "RESULTS.json").write_text(json.dumps(manifest))
                else:
                    payload = metrics(5, tag)
                    payload["validation"]["one_minute_entry_delay"]["policies"][summary.POLICY]["decision_pair_clock_side_sha256"] = "changed decisions"
                    write_metric(root, manifest, tag, payload)
                with self.assertRaises(ValueError):
                    summary.summarize_comparison(root)

    def test_new_separate_output_only_and_all_rows_appear_in_markdown(self):
        with tempfile.TemporaryDirectory() as directory:
            root, _ = fixture(directory)
            output = Path(directory) / "summary"
            report = summary.run(argparse.Namespace(comparison=root, output=output))
            self.assertEqual(sorted(path.name for path in output.iterdir()), ["COMPARISON_SUMMARY.json", "README.md"])
            self.assertEqual(json.loads((output / "COMPARISON_SUMMARY.json").read_text()), report)
            markdown = (output / "README.md").read_text()
            for tag in report["descriptive_cross_period_flags"]:
                self.assertGreaterEqual(markdown.count("| " + tag + " |"), 3)
            self.assertIn("conditional OLS", markdown)
            self.assertIn("previously examined", markdown)
            self.assertIn("not portfolio or account returns", markdown)
            for invalid in (output, root / "summary"):
                with self.assertRaises(ValueError):
                    summary.run(argparse.Namespace(comparison=root, output=invalid))


if __name__ == "__main__":
    unittest.main()
