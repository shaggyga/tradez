from __future__ import annotations

import copy
from pathlib import Path
import tempfile
import unittest

import numpy as np

from tools import analyze_rolling_specialist_component_baselines_v1 as tool


def fixture():
    base = 1800000000; train_end = base + 96 * 900
    time = np.r_[base + np.arange(96) * 900, train_end + np.arange(6) * 60,
                 train_end + 86400 + np.arange(6) * 60]
    split = np.r_[np.zeros(96), np.ones(6), np.full(6, 2)].astype(np.int8)
    data = {"time": np.tile(time, 2), "split": np.tile(split, 2), "pair_id": np.repeat([0, 1], len(time)),
            "pair_names": ["EUR_USD", "USD_JPY"], "entry_long": np.full(2 * len(time), .25),
            "entry_short": np.full(2 * len(time), .5),
            "boundaries": {"start": base, "train_end": train_end, "validation_end": train_end + 86400, "end": train_end + 172800}}
    n = len(data["time"])
    y = np.tile([2., -1., 0.], n // 3)
    for h in tool.HORIZONS:
        data[f"y_{h}"] = y.copy()
        data[f"long_{h}"] = y - .25 - .5
        data[f"short_{h}"] = -y - .5 - .25
        data[f"valid_{h}"] = np.ones(n, bool)
        data[f"strict_{h}"] = np.arange(n) % 3 != 0
        bounds = np.choose(data["split"], [train_end, train_end + 86400, train_end + 172800])
        data[f"eligible_{h}"] = data["time"] + (h + 1) * 60 < bounds
        data[f"arima_{h}"] = np.ones(n); data[f"momentum_{h}"] = np.ones(n)
    rows = np.flatnonzero(data["split"] > 0)
    heads = {"probability": np.full(len(rows), .5), "positive": np.full(len(rows), 2.),
             "nonpositive": np.full(len(rows), .5), "long_exit": np.full(len(rows), .5), "short_exit": np.full(len(rows), .25)}
    return data, rows, heads


class ComponentBaselineTests(unittest.TestCase):
    def test_future_mutations_do_not_change_train_means_medians_or_hashes(self):
        data, rows, _ = fixture(); before = tool.train_baselines(data, 30)
        changed = copy.deepcopy(data)
        changed["y_30"][rows] = 1000.; changed["long_30"][rows] = 999.25; changed["short_30"][rows] = -1000.75
        self.assertEqual(before, tool.train_baselines(changed, 30))

    def test_exact_final_purge_and_flat_nonpositive_membership(self):
        data, _, _ = fixture()
        b = tool.train_baselines(data, 60)
        cutoff = data["boundaries"]["train_end"]
        expected = (data["split"] == 0) & (data["time"] + 61 * 60 < cutoff)
        self.assertEqual(b["training_selection"]["rows"], int(expected.sum()))
        self.assertLess(b["training_selection"]["maximum_target_end_epoch"], cutoff)
        target, mask = tool.component_targets(data, 60)["nonpositive_magnitude"]
        self.assertTrue(np.all(mask[data["y_60"] == 0]))
        self.assertTrue(np.all(target[data["y_60"] == 0] == 0))

    def test_all_cohorts_components_equal_matched_error_counts_and_population_hashes(self):
        data, rows, heads = fixture(); baseline = tool.train_baselines(data, 30)
        report = tool.comparison_rows(data, rows, heads, 30, "compact38", baseline)
        self.assertEqual(len(report), 30)
        for row in report:
            self.assertEqual(row["model"]["rows"], row["TRAIN_pair_mean"]["rows"])
            self.assertEqual(row["model"]["rows"], row["TRAIN_pair_median"]["rows"])
            self.assertEqual(sum(row["matched_counts_by_pair"].values()), row["matched_rows"])
            self.assertEqual(len(row["matched_pair_clock_sha256"]), 64)
        for split in ("validation", "later_development_test"):
            for component in tool.COMPONENTS:
                cells = {r["cohort"]: r for r in report if r["split"] == split and r["component"] == component}
                self.assertEqual(cells["full_endpoint"]["matched_rows"], cells["shared_strict"]["matched_rows"] + cells["additional_endpoint_only"]["matched_rows"])

    def test_conditional_train_support_exclusions_not_hidden_or_reestimated_later(self):
        data, rows, heads = fixture()
        rare = (data["split"] == 0) & (data["pair_id"] == 1)
        data["y_30"][rare] = -1.; data["long_30"][rare] = -1.75; data["short_30"][rare] = .25
        baseline = tool.train_baselines(data, 30)
        self.assertFalse(baseline["parameters"]["positive_magnitude"]["USD_JPY"]["supported"])
        report = tool.comparison_rows(data, rows, heads, 30, "compact38", baseline)
        r = next(r for r in report if r["component"] == "positive_magnitude" and r["split"] == "validation" and r["cohort"] == "full_endpoint")
        self.assertEqual(r["component_label_rows_before_baseline_support"], 4)
        self.assertEqual(r["matched_rows"], 2)
        self.assertEqual(r["excluded_insufficient_component_TRAIN_support"], 2)
        self.assertEqual(r["model_before_baseline_support"]["rows"], 4)

    def test_asymmetric_persistence_uses_matching_quote_wing(self):
        data, rows, heads = fixture()
        report = tool.comparison_rows(data, rows, heads, 30, "compact50", tool.train_baselines(data, 30))
        for r in report:
            if r["component"] == "long_exit_cost":
                self.assertEqual(r["current_quote_wing_persistence"]["current_input"], "entry_short")
                self.assertEqual(r["current_quote_wing_persistence"]["score"]["mae_bps"], 0.)
            if r["component"] == "short_exit_cost":
                self.assertEqual(r["current_quote_wing_persistence"]["current_input"], "entry_long")
                self.assertEqual(r["current_quote_wing_persistence"]["score"]["mae_bps"], 0.)

    def test_missing_future_masks_reduce_scores_not_baseline_parameters(self):
        data, rows, heads = fixture(); original = tool.train_baselines(data, 30)
        data["valid_30"][rows[0]] = False; data["strict_30"][rows[0]] = False
        for field in ("y_30", "long_30", "short_30"): data[field][rows[0]] = np.nan
        self.assertEqual(original, tool.train_baselines(data, 30))
        report = tool.comparison_rows(data, rows, heads, 30, "compact38", original)
        r = next(r for r in report if r["component"] == "expected_absolute_move" and r["split"] == "validation" and r["cohort"] == "full_endpoint")
        self.assertEqual(r["matched_rows"], 11)

    def test_bad_head_probability_infinite_cost_and_truncated_population_refused(self):
        data, rows, heads = fixture(); baseline = tool.train_baselines(data, 30)
        with self.assertRaisesRegex(ValueError, "all_original"):
            tool.comparison_rows(data, rows[:-1], heads, 30, "compact38", baseline)
        heads["probability"][0] = 1.01
        with self.assertRaisesRegex(ValueError, "unit_interval"):
            tool.comparison_rows(data, rows, heads, 30, "compact38", baseline)
        heads["probability"][0] = .5; heads["long_exit"][0] = np.inf
        with self.assertRaisesRegex(ValueError, "finite_nonnegative"):
            tool.comparison_rows(data, rows, heads, 30, "compact38", baseline)

    def test_mean_and_median_have_distinct_targets_and_no_partial_reads(self):
        score_mean = tool.errors([2., 2., 2.], [0., 0., 6.])
        score_median = tool.errors([0., 0., 0.], [0., 0., 6.])
        self.assertLess(score_mean["rmse_bps"], score_median["rmse_bps"])
        self.assertLess(score_median["mae_bps"], score_mean["mae_bps"])
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "RESULTS.json").write_text('{"status":"running"}')
            with self.assertRaisesRegex(ValueError, "complete_specialist"):
                tool.analyze(directory)


if __name__ == "__main__": unittest.main()
