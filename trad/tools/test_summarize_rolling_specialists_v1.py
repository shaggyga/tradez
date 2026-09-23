"""Synthetic hash-bound reports only: no fitting or actual result inspection."""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from tools import summarize_rolling_specialists_v1 as summary
from tools import test_summarize_rolling_model_comparison_v1 as old_fixture


def write(root, relative, payload):
    p = root / relative; p.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(payload, allow_nan=False, separators=(",", ":")).encode()
    p.write_bytes(raw)
    return {"path": relative, "sha256": hashlib.sha256(raw).hexdigest()}


def population(n, split):
    day = "2026-08-24" if split == "validation" else "2026-08-31"
    return {"rows": n, "unique_original_utc_decision_times": n, "utc_days": int(n > 0), "pairs": int(n > 0),
            "counts_by_utc_day": {day: n} if n else {}, "counts_by_pair": {"EUR_USD": n} if n else {}}


def diagnostic(h, tag, scores):
    report = {"horizon_minutes": h, "future_cohorts_are_entry_filters": False, "periods": {}}
    for split in summary.SPLITS:
        report["periods"][split] = {"cohorts": {}}
        for name in summary.COHORTS:
            primary = scores[summary.GATES[0]][split]["primary"]
            n = primary["forecast_cohorts"][name]["scored_forecasts"]
            c = {"scored_origins": n, "actual_positive": n // 2, "actual_nonpositive": n - n // 2, "actual_flat": min(n, 1),
                 "positive_probability": {"model": {"rows": n, "positive_rows": n // 2, "brier": .22, "log_loss": .66},
                                          "train_pair_prior": {"rows": n, "positive_rows": n // 2, "brier": .25, "log_loss": .69}},
                 "variant_called_sign_return_optimism_bps": .3, "selection": {}}
            for field in ("positive_conditional_magnitude", "nonpositive_conditional_magnitude", "expected_absolute_move",
                          "raw_mixture_signed_mean", "direct_signed_mean", "variant_signed_mean", "long_exit_cost", "short_exit_cost"):
                c[field] = {"rows": n, "mean_error_bps": .1, "mae_bps": .4, "rmse_bps": .7}
            for gate in summary.GATES:
                p = scores[gate][split]["primary"]["policies"][summary.POLICIES[gate]]
                scored = p["cohorts"][name]["scored_decisions"]
                actual = p["cohorts"][name]["cost_scenarios"]["1.0"]["mean_net_bps"]
                c["selection"][gate] = {"all_original_decisions": population(p["decisions"], split),
                    "scored_decisions": population(scored, split), "decision_pair_clock_side_sha256": p["decision_pair_clock_side_sha256"],
                    "mean_actual_net_after_extra_1bp_bps": actual,
                    "mean_predicted_net_after_extra_1bp_bps": actual + .5 if actual is not None else None,
                    "mean_expected_minus_actual_net_bps": .5 if scored else None,
                    "mean_selected_return_optimism_bps": .3 if scored else None,
                    "mean_selected_exit_cost_optimism_bps": .2 if scored else None,
                    "maximum_decomposition_residual_bps": 0. if scored else None}
            report["periods"][split]["cohorts"][name] = c
    return report


def probability():
    result = {"positive_event": "R>0; flats nonpositive", "periods": {}}
    for split in summary.SPLITS:
        result["periods"][split] = {}
        for cohort, n in zip(summary.COHORTS, (10, 6, 4)):
            result["periods"][split][cohort] = {
                "raw": {"rows": n, "positive_rows": n // 2, "brier": .22, "log_loss": .66},
                "calibrated": {"rows": n, "positive_rows": n // 2, "brier": .21, "log_loss": .65},
                "TRAIN_pair_prior": {"rows": n, "positive_rows": n // 2, "brier": .25, "log_loss": .69}}
    return result


def metric(h, tag):
    primary = old_fixture.metrics(h, tag)
    secondary = copy.deepcopy(primary)
    for split in summary.SPLITS:
        for phase in ("primary", "one_minute_entry_delay"):
            report = secondary[split][phase]
            report["decision_rule"]["gate_cost_is_observed_spread"] = False
            report["policies"][summary.POLICIES[summary.GATES[1]]] = report["policies"].pop(old_fixture.summary.POLICY)
    return {"schema": "rolling_specialist_scoring_v1_20260915", "future_labels_used_for_decisions": False,
            "mean_forecasts_changed_by_cost_gate": False, "all_original_assessment_origins": 24,
            summary.GATES[0]: primary, summary.GATES[1]: secondary}


def fixture(directory):
    previous, old_manifest = old_fixture.fixture(directory)
    old_manifest["assessment_rows"] = 24
    write(previous, "RESULTS.json", old_manifest)
    root = Path(directory) / "specialists"; root.mkdir()
    manifest = {"schema": summary.RESULT_SCHEMA, "status": "complete", "completed_variants": 28,
                "completed_base_bundles": 20, "groups": {g: [] for g in summary.GROUPS}, "horizons": list(summary.HORIZONS),
                "assessment_rows": 24, "previous_comparison_root": str(previous),
                "previous_comparison_sha256": hashlib.sha256((previous / "RESULTS.json").read_bytes()).hexdigest(), "contexts": {}}
    for group in summary.GROUPS:
        for h in summary.HORIZONS:
            tag = f"{group}_{h}m"
            context = {"group": group, "horizon_minutes": h, "variants": {},
                       "probability_calibration_scores": write(root, f"diagnostics/{tag}_probability_calibration.json", probability())}
            manifest["contexts"][tag] = context
            for variant in summary.VARIANTS:
                name = tag + "_" + variant; scores = metric(h, name)
                context["variants"][variant] = {
                    "metrics": write(root, f"metrics/{name}.json", scores),
                    "diagnostics": write(root, f"diagnostics/{name}.json", diagnostic(h, name, scores)),
                    "forecast": {"path": "never_read_forecast.parquet", "sha256": "missing"}}
    write(root, "RESULTS.json", manifest)
    return root, manifest


def mutate_variant(root, manifest, context, variant, change):
    entry = manifest["contexts"][context]["variants"][variant]
    m = json.loads((root / entry["metrics"]["path"]).read_bytes())
    d = json.loads((root / entry["diagnostics"]["path"]).read_bytes())
    change(m, d)
    entry["metrics"] = write(root, entry["metrics"]["path"], m)
    entry["diagnostics"] = write(root, entry["diagnostics"]["path"], d)
    write(root, "RESULTS.json", manifest)


class SpecialistSummaryTests(unittest.TestCase):
    def test_all_variants_gates_periods_cohorts_and_exact18_old_records_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            root, _ = fixture(directory)
            result = summary.summarize_specialists(root)
            self.assertEqual(len(result["rows"]), 112)
            self.assertEqual(len(result["within_context_contrasts"]), 240)
            self.assertEqual(len(result["previous_comparator_records"]), 18)
            self.assertEqual(len(result["previous_primary_comparators"]), 36)
            self.assertEqual(result["hash_verified_files_including_manifests"], 80)
            self.assertTrue(all(set(r["cohorts"]) == set(summary.COHORTS) for r in result["rows"]))
            self.assertEqual(len(result["descriptive_positive_period_flags"]), 56)
            self.assertFalse(result["automatic_selection"])
            self.assertEqual(result["models_promoted"], 0)
            json.dumps(result, allow_nan=False)

    def test_calibrated_probability_is_distinct_from_raw_head_diagnostics(self):
        with tempfile.TemporaryDirectory() as directory:
            root, _ = fixture(directory); result = summary.summarize_specialists(root)
            p = result["context_probability_calibration"]["compact38_30m"]["periods"]["validation"]["full_endpoint"]
            self.assertEqual(p["raw"]["brier"], .22); self.assertEqual(p["calibrated"]["brier"], .21)
            r = next(r for r in result["rows"] if r["variant"] == "mixture_calibrated")
            c = r["cohorts"]["full_endpoint"]["components"]
            self.assertEqual(c["positive_probability"]["model"]["brier"], .22)
            self.assertIn("raw", c["head_probability_scope"])

    def test_single_period_quoted_only_small_support_kept(self):
        with tempfile.TemporaryDirectory() as directory:
            root, manifest = fixture(directory)
            def change(m, d):
                for gate in summary.GATES:
                    for split, value in zip(summary.SPLITS, (-2., -.2)):
                        for phase in ("primary", "one_minute_entry_delay"):
                            p = m[gate][split][phase]["policies"][summary.POLICIES[gate]]
                            for cohort in summary.COHORTS:
                                c = p["cohorts"][cohort]
                                for cost, scenario in c["cost_scenarios"].items():
                                    scenario["mean_net_bps"] = value + 1. - float(cost)
                        for cohort in summary.COHORTS:
                            s = d["periods"][split]["cohorts"][cohort]["selection"][gate]
                            s["mean_actual_net_after_extra_1bp_bps"] = value
                            s["mean_predicted_net_after_extra_1bp_bps"] = value + .5
            mutate_variant(root, manifest, "compact38_30m", "direct", change)
            result = summary.summarize_specialists(root)
            f = next(f for f in result["descriptive_positive_period_flags"] if f["context"] == "compact38_30m" and f["variant"] == "direct" and f["gate"] == summary.GATES[0])
            for cohort in summary.COHORTS:
                flag = f["cohorts"][cohort]
                self.assertEqual(flag["positive_periods_by_extra_cost_bps"]["0.0"], ["later_development_test"])
                self.assertFalse(flag["positive_net_1bp_both_periods"])
                self.assertTrue(flag["positive_in_exactly_one_period_by_extra_cost_bps"]["0.0"])
            self.assertIn("VL", summary.markdown_report(result))

    def test_different_policy_digests_are_not_claimed_same_trades(self):
        with tempfile.TemporaryDirectory() as directory:
            root, _ = fixture(directory); result = summary.summarize_specialists(root)
            r = next(r for r in result["within_context_contrasts"] if r["candidate_variant"] == "specialist_ridge")
            self.assertFalse(r["same_original_decision_digest"])
            self.assertIn("own decisions", r["policy_scope"])
            self.assertIn("rmse_delta_candidate_minus_reference_bps", r)
            self.assertIn("direct-only linear recalibration", r["question"])
            direct = next(r for r in result["within_context_contrasts"] if r["candidate_variant"] == "direct_ridge")
            self.assertIn("unconstrained slope may reverse", direct["question"])

    def test_incomplete_and_missing_variant_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            root, manifest = fixture(directory); manifest["status"] = "running"; write(root, "RESULTS.json", manifest)
            with self.assertRaisesRegex(ValueError, "complete_specialist"):
                summary.summarize_specialists(root)
            manifest["status"] = "complete"; del manifest["contexts"]["compact38_30m"]["variants"]["direct"]
            write(root, "RESULTS.json", manifest)
            with self.assertRaisesRegex(ValueError, "seven_variants"):
                summary.summarize_specialists(root)

    def test_corrupt_metric_diagnostic_or_old_binding_refused(self):
        for kind in ("metrics", "diagnostics", "old_manifest"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root, manifest = fixture(directory)
                target = (root / manifest["contexts"]["compact38_30m"]["variants"]["direct"][kind]["path"]
                          if kind != "old_manifest" else Path(manifest["previous_comparison_root"]) / "RESULTS.json")
                target.write_text(target.read_text() + " ")
                with self.assertRaisesRegex(ValueError, "hash"):
                    summary.summarize_specialists(root)

    def test_component_mismatch_and_expected_gate_mislabel_refused(self):
        for kind in ("digest", "net", "gate"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root, manifest = fixture(directory)
                def change(m, d):
                    selection = d["periods"]["validation"]["cohorts"]["full_endpoint"]["selection"][summary.GATES[0]]
                    if kind == "digest": selection["decision_pair_clock_side_sha256"] = "different"
                    if kind == "net": selection["mean_actual_net_after_extra_1bp_bps"] += 1
                    if kind == "gate": m[summary.GATES[1]]["validation"]["primary"]["decision_rule"]["gate_cost_is_observed_spread"] = True
                mutate_variant(root, manifest, "compact38_30m", "direct", change)
                with self.assertRaises(ValueError): summary.summarize_specialists(root)

    def test_probability_denominator_or_raw_calibration_confusion_refused(self):
        for kind in ("denominator", "raw_confusion"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root, manifest = fixture(directory)
                rec = manifest["contexts"]["compact38_30m"]["probability_calibration_scores"]
                p = json.loads((root / rec["path"]).read_bytes())
                q = p["periods"]["validation"]["full_endpoint"]
                if kind == "denominator": q["calibrated"]["rows"] += 1
                if kind == "raw_confusion": q["raw"]["brier"] = q["calibrated"]["brier"]
                manifest["contexts"]["compact38_30m"]["probability_calibration_scores"] = write(root, rec["path"], p)
                write(root, "RESULTS.json", manifest)
                with self.assertRaises(ValueError): summary.summarize_specialists(root)

    def test_only_referenced18_previous_metrics_are_read(self):
        with tempfile.TemporaryDirectory() as directory:
            root, manifest = fixture(directory)
            previous = Path(manifest["previous_comparison_root"])
            (previous / "metrics/hgb_combined228_5m.json").write_text("unread irrelevant older record")
            result = summary.summarize_specialists(root)
            self.assertEqual(result["previous_comparator_count"], 18)
            self.assertTrue(all(r["gate"] == summary.GATES[0] for r in result["previous_primary_comparators"]))

    def test_new_separate_output_and_both_error_metrics_visible(self):
        with tempfile.TemporaryDirectory() as directory:
            root, manifest = fixture(directory); output = Path(directory) / "summary"
            args = argparse.Namespace(comparison=root, previous_comparison=None, output=output)
            summary.run(args)
            self.assertEqual({p.name for p in output.iterdir()}, {"SUMMARY.json", "SUMMARY.md"})
            text = (output / "SUMMARY.md").read_text()
            self.assertIn("RMSE", text); self.assertIn("MAE", text); self.assertIn("not untouched", text)
            self.assertIn("Raw/calibrated", text)
            self.assertIn("unconstrained slope", text)
            for variant in summary.VARIANTS: self.assertIn(variant, text)
            with self.assertRaisesRegex(ValueError, "new_separate"): summary.run(args)
            args.output = root / "forbidden"
            with self.assertRaisesRegex(ValueError, "new_separate"): summary.run(args)
            args.output = Path(manifest["previous_comparison_root"]) / "forbidden"
            with self.assertRaisesRegex(ValueError, "cannot_modify_previous"): summary.run(args)


if __name__ == "__main__":
    unittest.main()
