"""Audit family-only recovery, preserving the frozen full-grid auditor.

The only replacement inside that auditor maps sklearn's categorical-first tree
indices back to original input columns before checking removed family features.
No resume-runner import, model refit, original-artifact write or feature mutation.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import partial
import hashlib
import inspect
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import numpy as np
import sklearn
from sklearn.ensemble._hist_gradient_boosting import binning
from sklearn.utils.validation import check_array
from tools import audit_rolling_period_replication_v1 as frozen

SECTIONS = ("contexts", "pair_priors", "comparators", "component_priors")
EXPECTED_AUDITOR_SHA = "612b6ce5bf022ec5245a6e80dfcbbf83722995e676ec383a5a414c12e827b840"
EXPECTED_HELPER_SHA = "cd90626a29776bec461aeb2ff0851107dec5b82f0989ed1e61edea825da43561"
EXPECTED_LIBRARY_SHA = "ea9c9247a2a1989d05547d54d6f53d71cf108dc22e4111464aec037cfdab07a3"
EXPECTED_THRESHOLD_SHA = "ab2729a3fe8c8742e42cc69268f51f6289202fe845f486d4e8d0e0a606c3fa51"
require = frozen.require
sha = frozen.sha


def refs(value):
    result = {}
    def visit(node):
        if isinstance(node, dict):
            if "path" in node and "sha256" in node:
                name, digest = node["path"], node["sha256"]
                require(name not in result or result[name] == digest, "conflicting_recovery_reference")
                result[name] = digest
            for item in node.values(): visit(item)
        elif isinstance(node, list):
            for item in node: visit(item)
    visit(value)
    return result


def validate_recovery_reports(parent, result):
    """Compare manifests without trusting the resume runner's validator."""
    require(parent["schema"] == result["schema"] == frozen.RESULT_SCHEMA, "same_replication_schema")
    require(parent["status"] == "failed" and result["status"] == "complete", "failed_parent_complete_recovery")
    require(parent["failure"] == {"type": "ValueError", "message": "window shape cannot be larger than input array shape"}, "exact_empty_binning_parent_failure")
    require(set(parent["contexts"]) == {f"{group}_{h}m" for group in frozen.GROUPS for h in frozen.HORIZONS}
            and len(parent["comparators"]) == 18
            and set(parent["pair_priors"]) == {"30", "60"}
            and set(parent["component_priors"]) == {"30", "60"}, "complete_parent_context_population")
    for context in parent["contexts"].values():
        require(set(context["variants"]) == set(frozen.VARIANTS)
                and len(context["oof_models"]) == 4
                and set(context["meta_models"]) == set(frozen.META_ARMS), "complete_parent_context_models")
    for key, value in {"completed_base_bundles": 20, "completed_variants": 20,
                       "completed_comparator_fits": 8, "completed_family_refits": 0,
                       "completed_family_mean_variants": 2}.items():
        require(parent[key] == value, "parent_completed_main_grid:" + key)
    require(result["completed_base_bundles"] == 20 and result["completed_variants"] == 20
            and result["completed_comparator_fits"] == 8
            and result["completed_family_refits"] == 7 and result["completed_family_mean_variants"] == 16,
            "complete_recovery_counts")
    for section in SECTIONS:
        require(result[section] == parent[section], "unchanged_main_section:" + section)
    # Every original declaration stays identical except explicit run/recovery state.
    mutable = {"status", "failure", "source_bindings", "family_contexts", "completed_family_refits",
               "completed_family_mean_variants", "started_utc", "completed_utc", "elapsed_seconds",
               "output_bytes", "artifacts", "completion_input_recheck", "recovery"}
    for key in set(parent) - mutable:
        require(key in result and result[key] == parent[key], "unchanged_parent_declaration:" + key)
    for name, digest in parent["source_bindings"].items():
        require(result["source_bindings"].get(name) == digest, "inherited_source_binding:" + name)
    recovery = result["recovery"]
    require(recovery["schema"] == "rolling_family_only_recovery_v1", "recovery_schema")
    require(recovery["source_started_utc"] == parent["started_utc"], "parent_original_start_preserved")
    require(recovery["main_grid_refitted"] is False and recovery["main_forecasts_rescored"] is False
            and recovery["family_design_changed"] is False, "family_only_unchanged_design")
    require(result["can_place_orders"] is False and result["models_promoted"] == 0, "unpromoted_research")
    records = refs({key: parent[key] for key in SECTIONS})
    require(records == recovery["reused_files"] and len(records) == recovery["reused_file_count"], "exact_reuse_reference_population")
    return records


def validate_compatibility(result):
    receipt = result["recovery"]["compatibility"]
    require(receipt["schema"] == "rolling_hgb_empty_binning_compat_v1_20260915", "reviewed_empty_threshold_compatibility")
    require(receipt["sklearn_version"] == sklearn.__version__ == "1.9.0", "reviewed_library_version")
    require(receipt["binning_module_sha256"] == EXPECTED_LIBRARY_SHA
            and sha(inspect.getfile(binning)) == EXPECTED_LIBRARY_SHA, "installed_library_unchanged")
    require(receipt["threshold_function_sha256"] == EXPECTED_THRESHOLD_SHA
            and hashlib.sha256(inspect.getsource(binning._find_binning_thresholds).encode()).hexdigest() == EXPECTED_THRESHOLD_SHA,
            "original_threshold_finder_restored")
    helper = "tools/rolling_empty_binning_compat_v1.py"
    require(receipt["helper_sha256"] == result["source_bindings"][helper] == sha(ROOT / helper) == EXPECTED_HELPER_SHA,
            "exact_reviewed_compatibility_helper")
    require(receipt["restored_original_function"] is True and receipt["feature_values_imputed"] is False
            and receipt["installed_package_files_modified"] is False, "scoped_no_imputation_repair")
    require(isinstance(receipt["empty_effective_columns_handled"], int)
            and receipt["empty_effective_columns_handled"] > 0, "empty_column_repair_exercised")
    return receipt


def internal_to_original(estimator):
    """Verify saved preprocessing order, then map internal tree column IDs."""
    mask = np.asarray(estimator.is_categorical_)
    require(mask.dtype == np.bool_ and mask.tolist() == [False] * 52 + [True], "family_pair_category_only")
    preprocessor = estimator._preprocessor
    require(preprocessor.n_features_in_ == 53, "family_preprocessor_width")
    transformers = preprocessor.transformers_
    require([entry[0] for entry in transformers] == ["encoder", "numerical", "remainder"], "saved_preprocessor_order")
    numeric = transformers[1][1]
    numeric_func = numeric.func
    require(type(transformers[0][1]).__name__ == "OrdinalEncoder"
            and type(numeric).__name__ == "FunctionTransformer"
            and isinstance(numeric_func, partial) and numeric_func.func is check_array
            and not numeric_func.args
            and numeric_func.keywords == {"dtype": [np.float64], "ensure_all_finite": False}
            and numeric.kw_args is None and numeric.validate is False,
            "identity_numeric_preprocessing")
    require(np.array_equal(np.asarray(transformers[0][2]), mask)
            and np.array_equal(np.asarray(transformers[1][2]), ~mask)
            and not np.asarray(transformers[2][2]).any()
            and transformers[2][1] == "drop", "saved_preprocessor_selectors")
    slots = preprocessor.output_indices_
    require(slots["encoder"] == slice(0, 1) and slots["numerical"] == slice(1, 53)
            and slots["remainder"] == slice(0, 0), "saved_preprocessor_output_slices")
    require(np.asarray(estimator._bin_mapper.is_categorical_).tolist() == [1] + [0] * 52,
            "bin_mapper_categorical_first")
    return np.r_[np.flatnonzero(mask), np.flatnonzero(~mask)]


def verify_family_tree_features(estimator, removed):
    require(all(estimator.get_params()[key] == value for key, value in frozen.contract.BASE_RECIPE.items()),
            "family_fixed_head_recipe")
    removed = np.asarray(removed, dtype=np.int64)
    require(removed.ndim == 1 and np.all((removed >= 0) & (removed < 50)), "removed_registered_slots_only")
    mapping = internal_to_original(estimator)
    for trees in estimator._predictors:
        for tree in trees:
            internal = tree.nodes["feature_idx"][tree.nodes["is_leaf"] == 0]
            require(np.all(internal < len(mapping)), "valid_tree_internal_feature_indices")
            require(not np.isin(mapping[internal], removed).any(), "saved_family_trees_cannot_split_removed_original_columns")


@contextmanager
def corrected_tree_verifier():
    original = frozen.verify_family_tree_features
    frozen.verify_family_tree_features = verify_family_tree_features
    try:
        yield
    finally:
        frozen.verify_family_tree_features = original


def verify_recovery(root, result):
    root = Path(root).resolve()
    parent_root = Path(result["recovery"]["source_root"]).resolve()
    require(parent_root != root and parent_root.is_relative_to(ROOT / "data"), "separate_failed_parent")
    path = parent_root / "RESULTS.json"
    expected = result["recovery"]["failed_results_sha256"]
    require(sha(path) == expected, "exact_failed_parent_manifest")
    parent = json.loads(path.read_bytes())
    records = validate_recovery_reports(parent, result)
    validate_compatibility(result)
    actual = {p.relative_to(parent_root).as_posix() for sub in frozen.SUBDIRS for p in (parent_root / sub).iterdir()}
    require(actual == set(records), "all_parent_artifacts_referenced")
    for name, digest in records.items():
        frozen.checked(parent_root, name, digest)
        frozen.checked(root, name, digest)
    require(sha(path) == expected, "failed_parent_unchanged_after_checks")
    return {"parent_root": str(parent_root), "parent_results_sha256": expected,
            "reused_artifacts_verified_in_both_roots": len(records),
            "main_sections_identical": list(SECTIONS), "family_design_unchanged": True,
            "compatibility": result["recovery"]["compatibility"]}


def run(root, output):
    root, output = Path(root).resolve(), Path(output).resolve()
    base_output = output.with_name(output.stem + "_BASE_AUDIT.json")
    require(root.is_relative_to(ROOT / "data") and output.is_relative_to(ROOT / "docs/validation")
            and not output.exists() and not base_output.exists(), "new_bounded_recovery_audit_outputs")
    require(sha(frozen.__file__) == EXPECTED_AUDITOR_SHA, "frozen_auditor_source_unchanged")
    pins = {"tools/audit_rolling_family_recovery_v1.py": sha(Path(__file__)),
            "tools/test_audit_rolling_family_recovery_v1.py": sha(ROOT / "tools/test_audit_rolling_family_recovery_v1.py")}
    result_path = root / "RESULTS.json"
    result_sha = sha(result_path)
    result = json.loads(result_path.read_bytes())
    recovery = verify_recovery(root, result)
    with corrected_tree_verifier():
        base = frozen.run(root, base_output)
    require(base["status"] == "passed", "complete_base_audit_passed")
    require(sha(result_path) == result_sha, "recovered_result_unchanged")
    require(verify_recovery(root, result) == recovery, "recovery_proofs_unchanged_after_base_audit")
    require(sha(frozen.__file__) == EXPECTED_AUDITOR_SHA, "frozen_auditor_still_unchanged")
    for name, digest in pins.items():
        require(sha(ROOT / name) == digest, "wrapper_source_unchanged")
    report = dict(base)
    report.update(recovery_audit=recovery,
                  recovery_auditor_source_bindings=pins,
                  base_audit_artifact={"path": str(base_output), "sha256": sha(base_output)},
                  tree_index_mapping="Verified saved encoder/numeric selectors and output slices: internal0 is original52 pair ID, internal1..52 are original0..51.",
                  original_frozen_auditor_sha256=EXPECTED_AUDITOR_SHA,
                  recovery_audit_completed_utc=datetime.now(timezone.utc).isoformat())
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--comparison", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    answer = run(args.comparison, args.output)
    print(json.dumps({"status": answer["status"], "recovery_verified": True,
                      "six_head_bundles_replayed": answer["six_head_bundles_replayed"]}))
