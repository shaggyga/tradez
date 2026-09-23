import copy
from functools import partial
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.preprocessing import FunctionTransformer, OrdinalEncoder
from sklearn.utils.validation import check_array
from threadpoolctl import threadpool_limits

from tools import audit_rolling_family_recovery_v1 as audit
from tools.rolling_empty_binning_compat_v1 import empty_binning_compat


@pytest.fixture(autouse=True)
def bounded_threads():
    with threadpool_limits(limits=1): yield


def model_stub(internal):
    mask = np.array([False] * 52 + [True])
    pre = SimpleNamespace(n_features_in_=53,
        transformers_=[("encoder", OrdinalEncoder(), mask),
                       ("numerical", FunctionTransformer(partial(check_array, dtype=[np.float64], ensure_all_finite=False)), ~mask),
                       ("remainder", "drop", np.zeros(53, dtype=bool))],
        output_indices_={"encoder": slice(0, 1), "numerical": slice(1, 53), "remainder": slice(0, 0)})
    nodes = np.array([(internal, 0), (0, 1)], dtype=[("feature_idx", "u4"), ("is_leaf", "u1")])
    return SimpleNamespace(is_categorical_=mask, _preprocessor=pre,
        _bin_mapper=SimpleNamespace(is_categorical_=np.array([1] + [0] * 52)),
        _predictors=[[SimpleNamespace(nodes=nodes)]],
        get_params=lambda: dict(audit.frozen.contract.BASE_RECIPE))


@pytest.mark.parametrize("internal,removed", [(0, [0]), (4, [4]), (51, [49]), (52, [49])])
def test_allowed_pair_and_numeric_splits_map_to_original(internal, removed):
    model = model_stub(internal)
    assert audit.internal_to_original(model).tolist() == [52] + list(range(52))
    audit.verify_family_tree_features(model, removed)


@pytest.mark.parametrize("removed", [0, 4, 49])
def test_true_removed_original_split_is_rejected(removed):
    with pytest.raises(ValueError, match="removed_original"):
        audit.verify_family_tree_features(model_stub(removed + 1), [removed])


def test_preprocessor_order_change_is_rejected():
    model = model_stub(4)
    model._preprocessor.transformers_[0], model._preprocessor.transformers_[1] = model._preprocessor.transformers_[1], model._preprocessor.transformers_[0]
    with pytest.raises(ValueError, match="preprocessor_order"):
        audit.verify_family_tree_features(model, [4])


def test_nonidentity_numeric_transform_is_rejected():
    model = model_stub(4)
    model._preprocessor.transformers_[1][1].func = np.negative
    with pytest.raises(ValueError, match="identity_numeric"):
        audit.verify_family_tree_features(model, [4])


def test_real_sklearn_saved_preprocessor_mapping():
    rng = np.random.default_rng(222)
    X = rng.normal(size=(800, 53)); X[:, [2, 4]] = np.nan
    X[:, 52] = np.arange(len(X)) % 68
    with empty_binning_compat():
        model = HistGradientBoostingRegressor(categorical_features=[52], **audit.frozen.contract.BASE_RECIPE).fit(X, X[:, 9])
    audit.verify_family_tree_features(model, [2, 4])


def reports():
    artifact = {"path": "models/kept.joblib", "sha256": hashlib.sha256(b"reusable model bits").hexdigest()}
    parent = {
        "schema": audit.frozen.RESULT_SCHEMA, "status": "failed",
        "failure": {"type": "ValueError", "message": "window shape cannot be larger than input array shape"},
        "completed_base_bundles": 20, "completed_variants": 20, "completed_comparator_fits": 8,
        "completed_family_refits": 0, "completed_family_mean_variants": 2,
        "started_utc": "2026-09-15T20:00:00Z", "can_place_orders": False, "models_promoted": 0,
        "source_bindings": {}, "inputs_sha256": "preserved_input", "family_design": {"mask": "allNaN53"},
        "contexts": {f"{g}_{h}m": {"final_model": artifact,
            "variants": {name: {} for name in audit.frozen.VARIANTS},
            "oof_models": [{}] * 4, "meta_models": {name: {} for name in audit.frozen.META_ARMS}}
            for g in audit.frozen.GROUPS for h in audit.frozen.HORIZONS},
        "comparators": {f"control{i}": {} for i in range(18)},
        "pair_priors": {"30": {}, "60": {}}, "component_priors": {"30": {}, "60": {}},
    }
    result = copy.deepcopy(parent); result.pop("failure")
    result.update(status="complete", completed_family_refits=7, completed_family_mean_variants=16,
                  started_utc="2026-09-15T23:00:00Z")
    with empty_binning_compat() as compatibility:
        from sklearn.ensemble._hist_gradient_boosting import binning
        binning._find_binning_thresholds(np.full(3, np.nan), 255)
    result["source_bindings"]["tools/rolling_empty_binning_compat_v1.py"] = audit.EXPECTED_HELPER_SHA
    records = audit.refs({key: parent[key] for key in audit.SECTIONS})
    result["recovery"] = {"schema": "rolling_family_only_recovery_v1", "source_started_utc": parent["started_utc"],
        "main_grid_refitted": False, "main_forecasts_rescored": False, "family_design_changed": False,
        "reused_files": records, "reused_file_count": len(records), "compatibility": compatibility}
    return parent, result


def test_unchanged_parent_and_compatibility_are_accepted():
    parent, result = reports()
    assert audit.validate_recovery_reports(parent, result) == result["recovery"]["reused_files"]
    assert audit.validate_compatibility(result)["restored_original_function"]


@pytest.mark.parametrize("kind", ["main_section", "design", "input", "count", "parent_failure", "refit", "reuse_hash"])
def test_recovery_tampering_rejected(kind):
    parent, result = reports()
    if kind == "main_section": result["contexts"]["compact38_30m"]["variants"]["direct"] = {"changed": True}
    elif kind == "design": result["family_design"] = {"mask": "zero"}
    elif kind == "input": result["inputs_sha256"] = "different"
    elif kind == "count": parent["completed_base_bundles"] = 19
    elif kind == "parent_failure": parent["failure"]["message"] = "another failure"
    elif kind == "refit": result["recovery"]["main_grid_refitted"] = True
    elif kind == "reuse_hash": result["recovery"]["reused_files"]["models/kept.joblib"] = "wrong"
    with pytest.raises(ValueError): audit.validate_recovery_reports(parent, result)


@pytest.mark.parametrize("field,value", [
    ("restored_original_function", False), ("feature_values_imputed", True),
    ("installed_package_files_modified", True), ("empty_effective_columns_handled", 0),
    ("helper_sha256", "wrong"), ("threshold_function_sha256", "wrong"),
])
def test_compatibility_receipt_tampering_rejected(field, value):
    _, result = reports(); result["recovery"]["compatibility"][field] = value
    with pytest.raises(ValueError): audit.validate_compatibility(result)


def test_temporary_verifier_restored_even_on_exception():
    original = audit.frozen.verify_family_tree_features
    with pytest.raises(RuntimeError, match="sentinel"):
        with audit.corrected_tree_verifier():
            assert audit.frozen.verify_family_tree_features is audit.verify_family_tree_features
            raise RuntimeError("sentinel")
    assert audit.frozen.verify_family_tree_features is original


def test_each_reused_artifact_must_match_parent_and_copy(tmp_path, monkeypatch):
    parent, result = reports()
    actual_root = audit.ROOT
    helper = tmp_path / "tools/rolling_empty_binning_compat_v1.py"; helper.parent.mkdir()
    helper.write_bytes((actual_root / "tools/rolling_empty_binning_compat_v1.py").read_bytes())
    source, output = tmp_path / "data/parent", tmp_path / "data/recovery"
    for folder in (source, output):
        folder.mkdir(parents=True)
        for sub in audit.frozen.SUBDIRS: (folder / sub).mkdir()
        (folder / "models/kept.joblib").write_bytes(b"reusable model bits")
    manifest = source / "RESULTS.json"; manifest.write_text(json.dumps(parent))
    result["recovery"].update(source_root=str(source), failed_results_sha256=audit.sha(manifest))
    monkeypatch.setattr(audit, "ROOT", tmp_path)
    assert audit.verify_recovery(output, result)["reused_artifacts_verified_in_both_roots"] == 1
    (output / "models/kept.joblib").write_bytes(b"changed")
    with pytest.raises(ValueError): audit.verify_recovery(output, result)
