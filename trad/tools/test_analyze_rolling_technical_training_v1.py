"""Synthetic training-only diagnostics; no existing dataset or raw input opened."""
from __future__ import annotations

import argparse
from contextlib import redirect_stderr
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from tools import analyze_rolling_technical_training_v1 as diagnostic
from oanda_rolling_technical_dataset_v1 import key_hash


START = 1_700_001_000  # UTC 30-minute clock boundary.


def fixture(directory, n=12):
    path = Path(directory) / "sealed_history"
    path.mkdir()
    names = [f"m1__fixture_{i}" for i in range(216)] + [f"peer__fixture_{i}" for i in range(12)]
    registry = [{"name": name, "family": "peer" if name.startswith("peer__") else "technical",
                 "model_input": True} for name in names]
    matrices, tables = [], []
    for pair, shift, multiplier in (("EUR_USD", 1., 1.), ("USD_JPY", 150., 30.)):
        matrix = np.tile(np.arange(228, dtype=float), (n, 1))
        x = np.sin(np.arange(n) / 2) + np.arange(n) * .01
        matrix[:, 0] = shift + x * multiplier
        matrix[:, 1] = 5 + matrix[:, 0] * 2
        matrix[:, 2] = np.nan
        matrix[:, 4] = shift  # Globally variable, but constant within each pair.
        matrices.append(matrix)
        columns = {"instrument": pa.array([pair] * n),
            "bar_start_epoch": pa.array(START + np.arange(n) * 1800),
            "bar_end_epoch": pa.array(START + np.arange(n) * 1800 + 60),
            "origin_split": pa.array(["train"] * n),
            "label__5m__return_bps": pa.array(np.arange(n) * 987654.)}
        columns.update({name: pa.array(matrix[:, i], from_pandas=True) for i, name in enumerate(names)})
        tables.append((pair, pa.table(columns)))
    all_rows = np.concatenate(matrices)
    stats = {}
    for i, name in enumerate(names):
        finite = all_rows[np.isfinite(all_rows[:, i]), i]
        stats[name] = {"finite_train_rows": len(finite), "missing_train_rows": 2 * n - len(finite),
            "minimum": float(finite.min()) if len(finite) else None,
            "maximum": float(finite.max()) if len(finite) else None,
            "all_missing_in_train": not len(finite),
            "constant_in_train": bool(len(finite) and finite.min() == finite.max())}
    summary = {"selection_scope": "all TRAIN origins only; no label or later-period selection",
        "features": stats, "candidate_nonconstant_inputs": [names[i] for i in (0, 1, 4)]}
    manifest = {"schema": diagnostic.SCHEMA, "status": "complete", "feature_count": 228,
        "feature_names": names, "label_names": ["label__5m__return_bps"],
        "boundaries": {"start": START, "train_end": START + (n + 1) * 1800,
                       "validation_end": START + (n + 2) * 1800, "end": START + (n + 3) * 1800},
        "partitions": [{"pair": pair, "split_counts": {"train": n}} for pair, _ in tables],
        "pairs": {pair: {"split_counts": {"train": n}} for pair, _ in tables}}
    for name, value in (("DATASET.json", manifest), ("FEATURE_REGISTRY.json", registry),
                        ("TRAIN_FEATURE_SUMMARY.json", summary)):
        (path / name).write_text(json.dumps(value), encoding="utf-8")
    return path, manifest, registry, summary, tables


def loader_for(tables, names):
    def loader(root, *, split=None, feature_names=None):
        if split != "train" or feature_names != names:
            raise AssertionError("selection accessed non-training split or implicit inputs")
        yield from tables
    return loader


class TrainingDiagnosticTests(unittest.TestCase):
    def test_pairwise_complete_counts_match_scalar_correlation_without_global_drop(self):
        x = np.array([[1., 2., np.nan], [2., 4., 8.], [3., np.nan, 7.],
                      [4., 8., 6.], [np.nan, 10., 5.], [6., 12., 4.]])
        correlations, counts = diagnostic.pairwise_complete_correlation(x)
        self.assertEqual(counts[0, 1], 4)
        self.assertEqual(counts[0, 2], 4)
        self.assertEqual(counts[1, 2], 4)
        for i in range(3):
            for j in range(3):
                valid = np.isfinite(x[:, i]) & np.isfinite(x[:, j])
                self.assertEqual(counts[i, j], valid.sum())
                self.assertAlmostEqual(correlations[i, j], np.corrcoef(x[valid, i], x[valid, j])[0, 1], places=12)

    def test_per_pair_centering_removes_cross_pair_level_artifact(self):
        within_x = np.tile([-1., -1., 1., 1.], 4)
        within_y = np.tile([-1., 1., -1., 1.], 4)
        x = np.column_stack([np.r_[within_x, within_x + 10000], np.r_[within_y, within_y + 10000]])
        pairs = np.array(["EUR_USD"] * 16 + ["USD_JPY"] * 16)
        raw_corr, _ = diagnostic.pairwise_complete_correlation(x)
        self.assertGreater(raw_corr[0, 1], .999)
        z, support = diagnostic.standardize_within_pair(x, pairs)
        adjusted, _ = diagnostic.pairwise_complete_correlation(z)
        self.assertAlmostEqual(adjusted[0, 1], 0, places=12)
        for pair in set(pairs):
            np.testing.assert_allclose(z[pairs == pair].mean(axis=0), 0, atol=1e-12)
            np.testing.assert_allclose(z[pairs == pair].std(axis=0), 1, atol=1e-12)
            self.assertEqual(support[pair]["inputs_with_finite_nonzero_sample_variance"], 2)

    def test_missing_constant_and_singleton_pair_features_are_not_imputed(self):
        x = np.array([[1., 5., np.nan, 7.], [2., 5., np.nan, np.nan], [3., 5., np.nan, np.nan]])
        z, support = diagnostic.standardize_within_pair(x, ["EUR_USD"] * 3)
        self.assertTrue(np.isnan(z[:, 1:]).all())
        self.assertEqual(support["EUR_USD"]["inputs_with_zero_sample_variance"], 1)
        self.assertEqual(support["EUR_USD"]["inputs_with_fewer_than_two_finite_samples"], 2)
        correlations, counts = diagnostic.pairwise_complete_correlation(z)
        self.assertEqual(counts[0, 1], 0)
        self.assertTrue(np.isnan(correlations[0, 1]))

    def test_constant_joint_subset_does_not_get_roundoff_correlation(self):
        x = np.column_stack([
            np.r_[np.full(1000, .37), np.arange(10), np.full(10, np.nan)],
            np.r_[np.full(1000, .51), np.full(10, np.nan), np.arange(10)]])
        z, _ = diagnostic.standardize_within_pair(x, ["EUR_USD"] * len(x))
        correlations, counts = diagnostic.pairwise_complete_correlation(z)
        self.assertEqual(counts[0, 1], 1000)
        self.assertTrue(np.isnan(correlations[0, 1]))

    def test_real_loader_joins_sidecars_and_never_opens_later_partitions(self):
        with tempfile.TemporaryDirectory() as directory:
            path, manifest, _, _, tables = fixture(directory)
            for index, (pair, table) in enumerate(tables):
                record = manifest["partitions"][index]
                for kind, selected in (("core", list(diagnostic.METADATA_COLUMNS) + manifest["feature_names"][:216] + manifest["label_names"]),
                                       ("peers", ["bar_start_epoch"] + manifest["feature_names"][216:])):
                    filename = f"{pair}_{kind}.parquet"
                    pq.write_table(table.select(selected), path / filename)
                    record[kind] = {"path": filename, "sha256": diagnostic.file_sha(path / filename)}
                record["key_sha256"] = key_hash(table["bar_start_epoch"].to_numpy())
            # These paths deliberately do not exist: a training-only loader
            # must skip this later partition before attempting any file read.
            manifest["partitions"].append({"pair": "EUR_USD", "split_counts": {"validation": 1},
                "core": {"path": "must_not_read_future_core.parquet", "sha256": "unknown"},
                "peers": {"path": "must_not_read_future_peers.parquet", "sha256": "unknown"}})
            (path / "DATASET.json").write_text(json.dumps(manifest))
            report = diagnostic.analyze_training(path)
            self.assertEqual(report["sample"]["all_training_rows_read"], 24)
            self.assertEqual(len(report["candidate_input_names"]), 3)
            self.assertEqual(report["label_columns_used"], [])

    def test_training_only_selection_ignores_all_label_values(self):
        with tempfile.TemporaryDirectory() as directory:
            path, manifest, _, _, tables = fixture(directory)
            with mock.patch.object(diagnostic, "iter_partitions", loader_for(tables, manifest["feature_names"])):
                before = diagnostic.analyze_training(path)
            changed = []
            for pair, table in tables:
                i = table.column_names.index("label__5m__return_bps")
                changed.append((pair, table.set_column(i, "label__5m__return_bps", pa.array(["future outcomes must not be parsed"] * table.num_rows))))
            with mock.patch.object(diagnostic, "iter_partitions", loader_for(changed, manifest["feature_names"])):
                after = diagnostic.analyze_training(path)
            self.assertEqual(before, after)
            self.assertEqual(before["label_columns_used"], [])
            self.assertEqual(before["candidate_input_names"], [manifest["feature_names"][i] for i in (0, 1, 4)])
            self.assertEqual(before["full_training_exclusions"][manifest["feature_names"][2]]["finite_train_rows"], 0)
            self.assertEqual(before["full_training_origin_rows"], 24)

    def test_false_train_tag_on_future_clock_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            path, manifest, _, _, tables = fixture(directory)
            pair, table = tables[0]
            shifted = manifest["boundaries"]["train_end"] + np.arange(table.num_rows) * 1800
            table = table.set_column(table.column_names.index("bar_start_epoch"), "bar_start_epoch", pa.array(shifted))
            table = table.set_column(table.column_names.index("bar_end_epoch"), "bar_end_epoch", pa.array(shifted + 60))
            with mock.patch.object(diagnostic, "iter_partitions", loader_for([(pair, table)] + tables[1:], manifest["feature_names"])):
                with self.assertRaisesRegex(ValueError, "training_only"):
                    diagnostic.analyze_training(path)

    def test_registry_label_and_categorical_input_guards(self):
        for category in ("label_name", "model_input_false", "future_information", "categorical_values"):
            with self.subTest(category=category), tempfile.TemporaryDirectory() as directory:
                path, manifest, registry, _, tables = fixture(directory)
                if category == "label_name":
                    manifest["feature_names"][0] = "label__5m__return_bps"
                    registry[0]["name"] = "label__5m__return_bps"
                elif category == "model_input_false":
                    registry[0]["model_input"] = False
                elif category == "future_information":
                    registry[0]["future_information"] = True
                else:
                    pair, table = tables[0]
                    name = manifest["feature_names"][0]
                    table = table.set_column(table.column_names.index(name), name, pa.array(["up"] * table.num_rows))
                    tables[0] = pair, table
                (path / "DATASET.json").write_text(json.dumps(manifest))
                (path / "FEATURE_REGISTRY.json").write_text(json.dumps(registry))
                with mock.patch.object(diagnostic, "iter_partitions", loader_for(tables, manifest["feature_names"])):
                    with self.assertRaises(ValueError):
                        diagnostic.analyze_training(path)

    def test_exact_training_summary_not_sample_constants_controls_exclusions(self):
        with tempfile.TemporaryDirectory() as directory:
            path, manifest, _, summary, tables = fixture(directory, n=600)
            with mock.patch.object(diagnostic, "iter_partitions", loader_for(tables, manifest["feature_names"])):
                report = diagnostic.analyze_training(path)
            first, second, pair_level = [manifest["feature_names"][i] for i in (0, 1, 4)]
            flags = report["sample_correlation_diagnostic"]["near_redundant_pairs"]
            self.assertTrue(any(row["left"] == first and row["right"] == second and row["joint_training_sample_rows"] == 1200 for row in flags))
            self.assertIn(first, report["candidate_input_names"])
            self.assertIn(second, report["candidate_input_names"])
            self.assertIn(pair_level, report["candidate_input_names"])
            self.assertEqual(report["sample_correlation_diagnostic"]["automatic_correlation_drops"], [])
            self.assertEqual(report["full_training_family_availability"]["technical"]["possible_feature_cells"], 216 * 1200)
            summary["features"][first]["constant_in_train"] = True
            (path / "TRAIN_FEATURE_SUMMARY.json").write_text(json.dumps(summary))
            with self.assertRaisesRegex(ValueError, "consistency"):
                diagnostic._contract(path)

    def test_clock_sampling_and_cap_are_value_independent_and_pair_order_stable(self):
        with tempfile.TemporaryDirectory() as directory:
            path, manifest, _, _, tables = fixture(directory, n=25)
            with mock.patch.object(diagnostic, "iter_partitions", loader_for(tables, manifest["feature_names"])):
                first = diagnostic.collect_training_sample(path, manifest, manifest["feature_names"], max_sample_rows=7)
            changed = []
            for pair, table in reversed(tables):
                name = manifest["feature_names"][0]
                changed.append((pair, table.set_column(table.column_names.index(name), name, pa.array(np.full(table.num_rows, 1e50)))))
            with mock.patch.object(diagnostic, "iter_partitions", loader_for(changed, manifest["feature_names"])):
                second = diagnostic.collect_training_sample(path, manifest, manifest["feature_names"], max_sample_rows=7)
            np.testing.assert_array_equal(first[1], second[1])
            np.testing.assert_array_equal(first[2], second[2])
            self.assertTrue((first[2] % 1800 == 0).all())
            self.assertEqual(first[3]["retained_rows"], 7)
            self.assertEqual(first[3]["clock_eligible_rows_before_cap"], 50)
            self.assertTrue(first[3]["cap_applied"])

    def test_non_clock_rows_not_selected_by_row_offset(self):
        with tempfile.TemporaryDirectory() as directory:
            path, manifest, _, _, tables = fixture(directory, n=3)
            changed = []
            for pair, table in tables:
                times = np.array([START, START + 60, START + 1800])
                table = table.set_column(table.column_names.index("bar_start_epoch"), "bar_start_epoch", pa.array(times))
                table = table.set_column(table.column_names.index("bar_end_epoch"), "bar_end_epoch", pa.array(times + 60))
                changed.append((pair, table))
            with mock.patch.object(diagnostic, "iter_partitions", loader_for(changed, manifest["feature_names"])):
                _, _, clocks, sample = diagnostic.collect_training_sample(path, manifest, manifest["feature_names"])
            self.assertEqual(clocks.tolist(), [START, START + 1800, START, START + 1800])
            self.assertEqual(sample["retained_rows"], 4)

    def test_only_explicit_new_separate_output_is_written(self):
        with tempfile.TemporaryDirectory() as directory:
            path, manifest, _, _, tables = fixture(directory)
            out = Path(directory) / "diagnostics"
            with mock.patch.object(diagnostic, "iter_partitions", loader_for(tables, manifest["feature_names"])):
                report = diagnostic.run(argparse.Namespace(dataset=path, output=out, max_sample_rows=50))
            self.assertEqual(sorted(file.name for file in out.iterdir()), ["README.md", "TRAINING_DIAGNOSTIC.json"])
            self.assertEqual(json.loads((out / "TRAINING_DIAGNOSTIC.json").read_text()), report)
            self.assertIn("predictive value", (out / "README.md").read_text())
            for rejected in (out, path / "diagnostics"):
                with self.assertRaisesRegex(ValueError, "new_separate"):
                    diagnostic.run(argparse.Namespace(dataset=path, output=rejected, max_sample_rows=50))
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                diagnostic.parse_args(["--dataset", str(path)])

    def test_incomplete_dataset_and_unbounded_sample_cap_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            path, manifest, _, _, _ = fixture(directory)
            for cap in (0, 1, 150001, True):
                with self.assertRaisesRegex(ValueError, "bounded"):
                    diagnostic.collect_training_sample(path, manifest, manifest["feature_names"], cap)
            manifest["status"] = "building"
            (path / "DATASET.json").write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "complete"):
                diagnostic.analyze_training(path)


if __name__ == "__main__":
    unittest.main()
