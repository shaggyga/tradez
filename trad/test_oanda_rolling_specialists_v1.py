import hashlib
from pathlib import Path

import joblib
import numpy as np
import pytest
from threadpoolctl import threadpool_limits

import oanda_rolling_specialists_v1 as s


@pytest.fixture(autouse=True)
def bounded_threads():
    with threadpool_limits(limits=2):
        yield


def base_data(n=1000, pair_count=3):
    rng = np.random.default_rng(19)
    x = rng.normal(size=(n, 41))
    x[::7, 0] = np.nan
    x[:, -3] = .2 + rng.random(n)
    x[:, -2] = .3 + rng.random(n)
    x[:, -1] = np.arange(n) % pair_count
    y = np.where(np.arange(n) % 2, 2. + rng.random(n), -1.-rng.random(n))
    y[::20] = 0.
    return x, y, np.full(n, .4), np.full(n, .6)


def raw_heads(n=600):
    rng = np.random.default_rng(37)
    return {
        'probability': rng.random(n), 'direct': rng.normal(size=n),
        'positive': 1.+rng.random(n), 'nonpositive': 1.+rng.random(n),
        'long_exit': .1+rng.random(n), 'short_exit': .1+rng.random(n),
    }


def meta_data(n=600):
    h = raw_heads(n)
    context = np.random.default_rng(44).normal(size=(n, 8))
    context[::11, 2] = np.nan
    meta = s.meta_features(h, context, np.full(n, .3), np.full(n, .5))
    return meta, np.arange(n) % 3, 2*h['direct'] + .4


def test_native_recipe_and_only_two_authorized_horizons():
    assert s.HORIZONS == (30, 60)
    assert s.GROUPS == ('compact38', 'compact50')
    assert s.BASE_RECIPE['early_stopping'] is False
    assert s.BASE_RECIPE['max_iter'] == 120
    assert s.BASE_RECIPE['min_samples_leaf'] == 200
    assert s.META_HGB_RECIPE['max_leaf_nodes'] == 8
    assert s.META_HGB_RECIPE['min_samples_leaf'] == 500
    assert s.META_COLUMNS['direct_ridge'] == (1,)
    assert s.META_COLUMNS['direct_context_hgb'] == (1, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)


def test_pinned_legacy_has_exact_identity_and_no_old_bundle_reuse():
    p = s.source_provenance()
    assert p['legacy_sha256'] == hashlib.sha256(Path(p['legacy_path']).read_bytes()).hexdigest()
    assert p['legacy_reuse'] == ['fit_platt', 'apply_platt', 'decide']
    assert p['legacy_fit_bundle_used'] is False
    assert s._legacy().VERSION == 'signed_cost_models_v1_20260911'


def test_changed_legacy_rejected_before_execution(tmp_path, monkeypatch):
    p = tmp_path/'untrusted.py'
    p.write_text("raise RuntimeError('must not execute')\n")
    monkeypatch.setattr(s, 'LEGACY_PATH', p)
    with pytest.raises(ValueError, match='legacy_specialist_source_changed'):
        s._legacy()


def test_exit_cost_targets_asymmetric_and_tiny_roundoff():
    y = np.array([3., -4.])
    a = np.array([.2, .4]); b = np.array([.7, .3])
    long, short, clips = s.exit_cost_targets(y, y-a-np.array([.5, -1e-8]),
                                         -y-b-np.array([.8, 1.]), a, b)
    assert np.allclose(long, [.5, 0.])
    assert np.allclose(short, [.8, 1.])
    assert clips == {'long_exit': 1, 'short_exit': 0}
    with pytest.raises(ValueError, match='exit_long_negative'):
        s.exit_cost_targets(y, y-a+.001, -y-b, a, b)


def test_head_targets_preserve_flats_and_same_underlying_row_measure(monkeypatch):
    calls = []

    class Capture:
        def __init__(self, **kwargs):
            self.params = kwargs

        def fit(self, X, y, sample_weight):
            calls.append((self.params, X.copy(), y.copy(), sample_weight.copy()))
            self.classes_ = np.array([0, 1])
            return self

    monkeypatch.setattr(s, 'HistGradientBoostingClassifier', Capture)
    monkeypatch.setattr(s, 'HistGradientBoostingRegressor', Capture)
    x, y, long, short = base_data()
    long[0] = -1e-8
    bundle = s.fit_heads(x, y, long, short, pair_count=3)
    assert bundle['training_rows'] == 1000 and bundle['flat_rows'] == 50
    assert bundle['positive_rows'] == bundle['nonpositive_rows'] == 500
    assert len(calls) == 6
    assert np.array_equal(calls[0][2], y > 0)
    assert np.array_equal(calls[1][2], y)
    assert np.array_equal(calls[2][2], y[y > 0])
    assert np.array_equal(calls[3][2], -y[y <= 0])
    assert (calls[3][2] == 0).sum() == 50
    for params, data, _, weights in calls:
        assert params['categorical_features'] == [40]
        assert np.array_equal(weights, np.ones(len(data)))
        assert np.isnan(data[:, 0]).any()
    assert calls[4][2][0] == 0
    assert bundle['label_roundoff_clips']['long_exit'] == 1


@pytest.mark.parametrize('defect', ['small_class', 'bad_pair', 'missing_cost', 'negative_cost', 'infinite_feature', 'overflow_feature', 'bad_exit', 'missing_y', 'wrong_width'])
def test_invalid_head_inputs_are_explicit_failures(defect):
    x, y, long, short = base_data()
    if defect == 'small_class':
        y[:] = -1.; y[:199] = 1.
    elif defect == 'bad_pair': x[0, -1] = .5
    elif defect == 'missing_cost': x[0, -3] = np.nan
    elif defect == 'negative_cost': x[0, -2] = -.1
    elif defect == 'infinite_feature': x[0, 2] = np.inf
    elif defect == 'overflow_feature': x[0, 2] = 1e100
    elif defect == 'bad_exit': long[0] = -2e-7
    elif defect == 'missing_y': y[0] = np.nan
    else: x = x[:, :-1]
    with pytest.raises(ValueError):
        s.fit_heads(x, y, long, short, pair_count=3)


def test_real_six_heads_native_nan_and_serialization(tmp_path):
    x, y, long, short = base_data()
    bundle = s.fit_heads(x, y, long, short, pair_count=3)
    heads, clips = s.predict_heads(bundle, x[:81])
    assert set(heads) == set(s.HEAD_NAMES)
    assert all(a.shape == (81,) and np.isfinite(a).all() for a in heads.values())
    assert np.all((heads['probability'] >= 0) & (heads['probability'] <= 1))
    assert all(n >= 0 for n in clips.values())
    path = tmp_path/'heads.joblib'; joblib.dump(bundle, path, compress=3)
    restored = joblib.load(path)
    replay, replay_clips = s.predict_heads(restored, x[:81])
    assert replay_clips == clips
    for key in heads: assert np.array_equal(heads[key], replay[key])
    assert restored['models']['probability'].is_categorical_[-1]


def test_prediction_clamps_only_magnitude_and_cost_preserves_raw_zero_one():
    class Pred:
        classes_ = np.array([0, 1])

        def predict(self, x): return np.array([-3., 2., 0.])

        def predict_proba(self, x): return np.array([[1., 0.], [.75, .25], [0., 1.]])

    bundle = {'schema': s.SCHEMA, 'kind': 'six_heads', 'pair_count': 3,
              'input_columns': 41, 'models': {n: Pred() for n in s.HEAD_NAMES}}
    x = base_data()[0][:3]
    values, clips = s.predict_heads(bundle, x)
    assert values['probability'].tolist() == [0., .25, 1.]
    assert values['direct'].tolist() == [-3., 2., 0.]
    assert all(v == 1 for v in clips.values())
    assert values['positive'].tolist() == [0., 2., 0.]


def test_raw_mixture_absolute_magnitude_and_context_order():
    h = raw_heads(3);h['probability'] = np.array([0., .25, 1.])
    h['positive'] = np.array([1., 8., 4.]);h['nonpositive'] = np.array([3., 2., 5.])
    context = np.arange(24.).reshape(3, 8);context[0, 0] = np.nan
    m = s.meta_features(h, context, np.full(3, .2), np.full(3, .3))
    assert m.shape == (3, 18) and m.dtype == np.float64
    assert m[:, 6].tolist() == [-3., .5, 4.]
    assert m[:, 7].tolist() == [3., 3.5, 4.]
    assert np.array_equal(m[:, 10:], context, equal_nan=True)
    assert np.array_equal(s._meta_matrix(m), m, equal_nan=True)


def test_calibrated_or_altered_mixture_cannot_enter_raw_meta_namespace():
    meta, _, _ = meta_data()
    meta[0, 6] += .001
    with pytest.raises(ValueError, match='raw_mixture_identity'):
        s._meta_matrix(meta)
    heads = raw_heads();heads['calibrated_probability'] = heads['probability']
    with pytest.raises(ValueError, match='exact_six_raw_heads'):
        s.meta_features(heads, np.zeros((600, 8)), np.ones(600), np.ones(600))


def test_direct_shrinkage_has_only_direct_head_and_pair_context():
    meta, ids, y = meta_data(); mask = np.ones(len(y), bool)
    fitted = s.fit_meta(meta, ids, y, arm='direct_ridge', fit_mask=mask, pair_count=3)
    assert fitted['selected_meta_names'] == ['direct']
    assert fitted['model'].n_features_in_ == 1+1+3
    before = s.predict_meta(fitted, meta, ids)
    changed = meta.copy();changed[:, 10:] = 1e6
    changed[:, 8:10] = 100.
    changed[:, 2:6] *= 2.
    changed[:, 6] = changed[:, 0]*changed[:, 2]-(1-changed[:, 0])*changed[:, 3]
    changed[:, 7] = changed[:, 0]*changed[:, 2]+(1-changed[:, 0])*changed[:, 3]
    assert np.array_equal(before, s.predict_meta(fitted, changed, ids))


def test_ridge_scaler_uses_all_issued_oof_inputs_before_label_selection():
    meta, ids, y = meta_data(); mask = np.arange(len(y)) < 300
    y[~mask] = np.nan
    meta[-1, 1] = 1000.
    one = s.fit_meta(meta, ids, y, arm='specialist_ridge', fit_mask=mask, pair_count=3)
    assert one['scaler_rows'] == one['issued_oof_rows'] == 600
    assert one['training_rows'] == 300
    assert one['scaler']['mean'][1] == meta[:, 1].mean()
    assert one['scaler']['mean'][1] != meta[mask, 1].mean()
    # Labels outside the mask never affect either preprocessing or estimator.
    changed = y.copy();changed[~mask] = 1e8
    two = s.fit_meta(meta, ids, changed, arm='specialist_ridge', fit_mask=mask, pair_count=3)
    for k in one['scaler']: assert np.array_equal(one['scaler'][k], two['scaler'][k])
    assert np.array_equal(s.predict_meta(one, meta, ids), s.predict_meta(two, meta, ids))


@pytest.mark.parametrize('arm', tuple(s.META_COLUMNS))
def test_all_four_meta_arms_save_and_recreate(arm, tmp_path):
    meta, ids, y = meta_data(1200);mask = np.ones(len(y), bool)
    fitted = s.fit_meta(meta, ids, y, arm=arm, fit_mask=mask, pair_count=3)
    prediction = s.predict_meta(fitted, meta[:51], ids[:51])
    p = tmp_path/(arm+'.joblib');joblib.dump(fitted, p, compress=3)
    assert np.array_equal(prediction, s.predict_meta(joblib.load(p), meta[:51], ids[:51]))
    assert fitted['probability_calibration_in_inputs'] is False
    if arm.endswith('hgb'):
        assert fitted['scaler'] is None
        assert fitted['model'].is_categorical_[-1]
    else:
        selected = meta[:, fitted['columns']]
        assert s._ridge_matrix(selected, ids, 3, fitted['scaler']).dtype == np.float64
        all_rows = s.predict_meta(fitted, meta, ids)
        representatives = np.array([0, 499, 1199])
        replay = s.predict_meta(fitted, meta[representatives], ids[representatives])
        assert np.allclose(replay, all_rows[representatives], rtol=1e-12, atol=1e-10)


@pytest.mark.parametrize('defect', ['future_namespace', 'infinite_context', 'missing_head', 'bad_pair', 'integer_mask', 'missing_selected_y'])
def test_meta_failures_are_not_swallowed(defect):
    meta, ids, y = meta_data();mask = np.ones(len(y), bool);arm = 'specialist_ridge'
    if defect == 'future_namespace': meta = np.column_stack((meta, y))
    elif defect == 'infinite_context': meta[0, 10] = np.inf
    elif defect == 'missing_head': meta[0, 1] = np.nan
    elif defect == 'bad_pair': ids[0] = 3
    elif defect == 'integer_mask': mask = mask.astype(np.int8)
    else: y[0] = np.nan
    with pytest.raises(ValueError):
        s.fit_meta(meta, ids, y, arm=arm, fit_mask=mask, pair_count=3)


def test_calibration_uses_raw_oof_only_includes_flats_and_separate_mixture():
    n = 800;raw = np.linspace(0., 1., n)
    y = np.r_[np.zeros(400), np.ones(400)]
    calibration = s.fit_calibration(raw, y)
    assert calibration['positive_rows'] == calibration['flat_rows'] == 400
    assert calibration['probability_scope'] == 'calibrated_on_raw_chronological_TRAIN_OOF_predictions_not_verified_future_accuracy'
    assert 0 <= calibration['slope'] <= 8
    calibrated = s.apply_calibration(raw, calibration)
    assert np.isfinite(calibrated).all() and np.all(np.diff(calibrated) >= 0)
    assert raw[0] == 0 and raw[-1] == 1
    heads = raw_heads(n);heads['probability'] = raw
    mixture = s.calibrated_mixture(heads, calibration)
    assert np.array_equal(mixture, calibrated*heads['positive']-(1-calibrated)*heads['nonpositive'])
    expected = s.meta_features(heads, np.zeros((n, 8)), np.ones(n), np.ones(n))
    assert np.array_equal(expected[:, 0], raw)
    assert 'excluded from all meta inputs' in calibration['used_for']


def test_calibration_has_no_small_class_or_bad_probability_fallback():
    with pytest.raises(ValueError, match='insufficient_calibration_classes'):
        s.fit_calibration(np.full(400, .5), np.r_[np.zeros(201), np.ones(199)])
    with pytest.raises(ValueError, match='invalid_probability'):
        s.fit_calibration(np.full(800, 1.1), np.ones(800))


def test_reused_decision_is_asymmetric_strict_margin_and_wait():
    side, long, short = s.decide(np.array([2., -2., 1.]), np.array([.2]*3),
         np.array([.1]*3), np.array([.5]*3), np.array([.3]*3), 1.)
    assert side.tolist() == [1, -1, 0]
    assert np.allclose(long, [1.3, -2.7, .3])
    assert np.allclose(short, [-2.4, 1.6, -1.4])
    side, _, _ = s.decide(np.array([1.]), np.zeros(1), np.zeros(1), np.zeros(1), np.zeros(1), 1.)
    assert side[0] == 0


def test_tampered_saved_meta_order_is_refused():
    meta, ids, y = meta_data()
    fitted = s.fit_meta(meta, ids, y, arm='direct_ridge', fit_mask=np.ones(600, bool), pair_count=3)
    fitted['columns'] = [0]
    with pytest.raises(ValueError, match='meta_column_identity_changed'):
        s.predict_meta(fitted, meta, ids)
