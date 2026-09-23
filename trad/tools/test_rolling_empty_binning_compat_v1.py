import hashlib
import inspect
import json
from pathlib import Path

import joblib
import numpy as np
import pytest
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.ensemble._hist_gradient_boosting import binning
from threadpoolctl import threadpool_limits

from tools import rolling_empty_binning_compat_v1 as compat


@pytest.fixture(autouse=True)
def bounded_threads():
    with threadpool_limits(limits=1):
        yield


def same_bits(a, b):
    assert a.dtype == b.dtype and a.shape == b.shape
    assert a.tobytes() == b.tobytes()


def test_installed_source_identity_and_original_failure():
    record = compat.compatibility_source_record()
    assert record["binning_module_sha256"] == compat.EXPECTED_BINNING_MODULE_SHA256
    with pytest.raises(ValueError, match="window shape"):
        binning._find_binning_thresholds(np.full(10, np.nan), 255)


@pytest.mark.parametrize("column,weights", [
    (np.array([], dtype=float), None),
    (np.full(10, np.nan), None),
    (np.full(10, np.nan), np.ones(10)),
    (np.array([1., 2., np.nan]), np.array([0., 0., 10.])),
])
def test_zero_effective_values_return_empty_float64_without_mutating(column, weights):
    before = column.tobytes()
    original = binning._find_binning_thresholds
    with compat.empty_binning_compat() as receipt:
        result = binning._find_binning_thresholds(column, 255, weights)
        same_bits(result, np.empty(0, np.float64))
        assert receipt["empty_effective_columns_handled"] == 1
    assert column.tobytes() == before
    assert binning._find_binning_thresholds is original
    assert receipt["restored_original_function"]
    json.dumps(receipt, allow_nan=False)


@pytest.mark.parametrize("kind", ["constant", "signed_zero", "few_distinct", "quantiles", "weighted", "mixed_missing", "zero_weights"])
def test_nonempty_thresholds_are_bitwise_original(kind):
    rng = np.random.default_rng(74)
    values = rng.normal(size=1200)
    weights = None
    if kind == "constant": values[:] = 7.0
    elif kind == "signed_zero": values = np.array([-0., 0., -0., 0.])
    elif kind == "few_distinct": values = np.tile([-2., 1., 3.], 400)
    elif kind == "weighted": weights = rng.uniform(.1, 3, len(values))
    elif kind == "mixed_missing": values[::3] = np.nan
    elif kind == "zero_weights":
        values[::5] = np.nan
        weights = np.ones(len(values)); weights[::3] = 0
    before = values.tobytes()
    expected = binning._find_binning_thresholds(values, 31, weights)
    with compat.empty_binning_compat() as receipt:
        actual = binning._find_binning_thresholds(values, 31, weights)
        same_bits(actual, expected)
        assert receipt["empty_effective_columns_handled"] == 0
    assert values.tobytes() == before


def test_bin_mapper_preserves_missing_bin_and_ordinary_thresholds():
    rng = np.random.default_rng(11)
    X = np.column_stack([rng.normal(size=600), np.full(600, np.nan), rng.normal(size=600)])
    before = X.tobytes()
    expected = [binning._find_binning_thresholds(X[:, i], 255) for i in (0, 2)]
    with compat.empty_binning_compat() as receipt:
        mapper = binning._BinMapper(n_bins=256, subsample=None, n_threads=2).fit(X)
        mapped = mapper.transform(X)
    same_bits(mapper.bin_thresholds_[0], expected[0])
    same_bits(mapper.bin_thresholds_[2], expected[1])
    assert mapper.bin_thresholds_[1].size == 0
    assert np.all(mapped[:, 1] == mapper.missing_values_bin_idx_)
    assert mapper.n_bins_non_missing_[1] == 1
    assert receipt["empty_effective_columns_handled"] == 1
    assert X.tobytes() == before


@pytest.mark.parametrize("classification", [False, True])
def test_saved_53_slot_model_replays_without_shim(tmp_path, classification):
    rng = np.random.default_rng(42)
    X = rng.normal(size=(1200, 53))
    removed = np.array([0, 3, 9, 24, 35, 49])
    X[:, removed] = np.nan
    X[:, 50:52] = np.abs(X[:, 50:52])
    X[:, 52] = np.arange(len(X)) % 68
    raw_y = 2 * X[:, 4] - X[:, 8] + rng.normal(size=len(X)) * .1
    y = (raw_y > 0).astype(int) if classification else raw_y
    cls = HistGradientBoostingClassifier if classification else HistGradientBoostingRegressor
    model = cls(max_iter=8, max_leaf_nodes=15, min_samples_leaf=200, l2_regularization=10,
                random_state=20260915, early_stopping=False, categorical_features=[52])
    before = X.tobytes()
    original = binning._find_binning_thresholds
    with compat.empty_binning_compat() as receipt:
        model.fit(X, y)
        expected = model.predict_proba(X) if classification else model.predict(X)
    assert binning._find_binning_thresholds is original
    assert receipt["empty_effective_columns_handled"] == len(removed)
    for stage in model._predictors:
        for tree in stage:
            nodes = tree.nodes
            # HGB moves categorical columns ahead of numeric columns internally.
            original_index = np.r_[np.flatnonzero(model.is_categorical_), np.flatnonzero(~model.is_categorical_)]
            internal = nodes["feature_idx"][~nodes["is_leaf"].astype(bool)]
            used = set(original_index[internal].tolist())
            assert not used.intersection(removed)
    path = tmp_path / "model.joblib"
    joblib.dump(model, path)
    restored = joblib.load(path)
    actual = restored.predict_proba(X) if classification else restored.predict(X)
    same_bits(actual, expected)
    assert X.tobytes() == before


def test_ordinary_fitted_model_is_bitwise_unchanged():
    rng = np.random.default_rng(93)
    X = rng.normal(size=(800, 5)); X[::5, 2] = np.nan
    y = X[:, 0] - X[:, 4]
    recipe = dict(max_iter=6, max_leaf_nodes=8, min_samples_leaf=50, early_stopping=False, random_state=44)
    expected = HistGradientBoostingRegressor(**recipe).fit(X, y)
    with compat.empty_binning_compat() as receipt:
        actual = HistGradientBoostingRegressor(**recipe).fit(X, y)
    assert receipt["empty_effective_columns_handled"] == 0
    same_bits(expected.predict(X), actual.predict(X))
    for a, b in zip(expected._predictors, actual._predictors):
        same_bits(a[0].nodes, b[0].nodes)


def test_actual_six_head_recipe_fits_masked_53_slots_and_replays(tmp_path):
    import oanda_rolling_specialists_v1 as core
    rng = np.random.default_rng(124)
    X = rng.normal(size=(1200, 53)); removed = np.array([0, 2, 11, 36, 49])
    X[:, removed] = np.nan
    X[:, 50:52] = np.abs(X[:, 50:52]); X[:, 52] = np.arange(len(X)) % 68
    y = X[:, 4] - X[:, 8]
    long_cost, short_cost = np.abs(y) + .1, np.abs(X[:, 6]) + .1
    before = X.tobytes()
    with compat.empty_binning_compat() as receipt:
        bundle = core.fit_heads(X, y, long_cost, short_cost)
        expected, clips = core.predict_heads(bundle, X)
    assert receipt["empty_effective_columns_handled"] == len(removed) * 6
    assert bundle["input_columns"] == 53 and X.tobytes() == before
    saved = tmp_path / "six_heads.joblib"; joblib.dump(bundle, saved)
    actual, restored_clips = core.predict_heads(joblib.load(saved), X)
    assert restored_clips == clips
    for name in core.HEAD_NAMES:
        same_bits(actual[name], expected[name])


def test_scope_restores_after_exception_and_nested_use():
    original = binning._find_binning_thresholds
    with pytest.raises(RuntimeError, match="intentional"):
        with compat.empty_binning_compat() as outer:
            installed = binning._find_binning_thresholds
            with compat.empty_binning_compat() as inner:
                assert inner is outer
                binning._find_binning_thresholds(np.full(3, np.nan), 255)
            assert binning._find_binning_thresholds is installed
            raise RuntimeError("intentional")
    assert binning._find_binning_thresholds is original
    assert outer["restored_original_function"]


def test_package_bytes_unchanged():
    path = Path(inspect.getfile(binning))
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    with compat.empty_binning_compat():
        binning._find_binning_thresholds(np.full(3, np.nan), 255)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


def test_unreviewed_version_refused(monkeypatch):
    original = binning._find_binning_thresholds
    monkeypatch.setattr(compat.sklearn, "__version__", "unreviewed")
    with pytest.raises(RuntimeError, match="reviewed sklearn version"):
        with compat.empty_binning_compat(): pass
    assert binning._find_binning_thresholds is original
