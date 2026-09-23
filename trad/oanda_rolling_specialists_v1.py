"""Fixed rolling specialists and causal-OOF combiners; no runtime/order hooks.

The caller owns original-clock partitions, prefix-only preprocessing, label
maturity and OOF issuance. This module never discovers data or changes a study.
"""
from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path

import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.linear_model import Ridge

from oanda_rolling_model_design_v1 import fit_normalizer, transform_inputs

SCHEMA = 'rolling_specialists_v1_20260915'
ROOT = Path(__file__).resolve().parent
HORIZONS = (30, 60)
GROUPS = ('compact38', 'compact50')
MIN_CLASS_ROWS = 200
EXIT_ROUNDOFF_TOLERANCE = 1e-7
LEGACY_PATH = ROOT.parent/'direction_decision_20260911/src/signed_cost_models_v1.py'
LEGACY_SHA256 = '8cebabbab7c6dcb32271a8fd3cbc656e912ae6a26e19a9ff1d35ad547342665c'
DESIGN_SHA256 = '5d611f957b65168c11634f68aa1a63d639997b6034dd176a3f1dd9bae3a4544d'
BASE_RECIPE = {
    'learning_rate': .05, 'max_iter': 120, 'max_leaf_nodes': 15,
    'min_samples_leaf': 200, 'l2_regularization': 10., 'max_bins': 128,
    'early_stopping': False, 'random_state': 20260915,
}
META_HGB_RECIPE = {**BASE_RECIPE, 'max_leaf_nodes': 8, 'min_samples_leaf': 500}
META_RIDGE_RECIPE = {'alpha': 1000., 'solver': 'cholesky', 'fit_intercept': True}
CALIBRATION_RECIPE = {
    'slope_bounds': [0., 8.], 'intercept_bounds': [-8., 8.],
    'logit_clip_probability': 1e-6, 'ridge_penalty': .001,
    'minimum_rows_each_class': MIN_CLASS_ROWS,
}
HEAD_NAMES = ('probability', 'direct', 'positive', 'nonpositive', 'long_exit', 'short_exit')
CONTEXT_NAMES = (
    'm1__return_15_bps', 'm1__return_60_bps', 'm1__path_efficiency_15',
    'm1__return_vol_15_pips', 'm1__return_vol_60_pips',
    'm1__spread_ratio_prior_120', 'm1__utc_hour_sin', 'm1__utc_hour_cos',
)
META_NAMES = HEAD_NAMES + ('mixture_raw', 'absolute_mean_raw', 'entry_long', 'entry_short') + CONTEXT_NAMES
META_COLUMNS = {
    'direct_ridge': (1,),
    'specialist_ridge': tuple(range(18)),
    'direct_context_hgb': (1, 8, 9) + tuple(range(10, 18)),
    'specialist_context_hgb': tuple(range(18)),
}


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def source_provenance():
    if _sha(LEGACY_PATH) != LEGACY_SHA256:
        raise ValueError('legacy_specialist_source_changed')
    if _sha(ROOT/'oanda_rolling_model_design_v1.py') != DESIGN_SHA256:
        raise ValueError('normalizer_source_changed')
    return {
        'legacy_path': str(LEGACY_PATH), 'legacy_sha256': LEGACY_SHA256,
        'legacy_reuse': ['fit_platt', 'apply_platt', 'decide'],
        'legacy_fit_bundle_used': False,
        'normalizer_source': 'oanda_rolling_model_design_v1.py',
        'normalizer_sha256': DESIGN_SHA256,
        'new_source_sha256': _sha(__file__),
    }


def _legacy():
    """Execute only the exact retained, side-effect-free definitions/imports."""
    source_provenance()
    spec = importlib.util.spec_from_file_location('_rolling_pinned_signed_cost_v1', LEGACY_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if _sha(LEGACY_PATH) != LEGACY_SHA256:
        raise ValueError('legacy_specialist_source_changed_during_load')
    return module


def _vector(value, name, rows=None, *, finite=True):
    a = np.asarray(value, dtype=np.float64)
    if a.ndim != 1 or (rows is not None and a.shape != (rows,)):
        raise ValueError(name+'_shape')
    if np.isinf(a).any() or (finite and not np.isfinite(a).all()):
        raise ValueError(name+'_nonfinite')
    return a


def _pair_ids(value, rows, pair_count):
    a = _vector(value, 'pair_id', rows)
    if not isinstance(pair_count, int) or not 1 <= pair_count <= 128:
        raise ValueError('invalid_pair_count')
    if np.any(a != np.floor(a)) or np.any(a < 0) or np.any(a >= pair_count):
        raise ValueError('invalid_categorical_pair_id')
    return a.astype(np.int64)


def _cost(value, name, rows, *, label=False):
    a = _vector(value, name, rows)
    if np.any(a < (-EXIT_ROUNDOFF_TOLERANCE if label else 0.)):
        raise ValueError(name+'_negative')
    return np.maximum(a, 0.) if label else a


def _base_matrix(X, pair_count, expected_columns=None):
    x = np.asarray(X, dtype=np.float64)
    if x.ndim != 2 or x.shape[1] not in (41, 53):
        raise ValueError('compact_registered_fields_plus_costs_pair_required')
    if expected_columns is not None and x.shape[1] != expected_columns:
        raise ValueError('head_input_width_changed')
    if np.isinf(x).any():
        raise ValueError('infinite_head_input')
    _cost(x[:, -3], 'entry_long', len(x))
    _cost(x[:, -2], 'entry_short', len(x))
    _pair_ids(x[:, -1], len(x), pair_count)
    with np.errstate(over='ignore'):
        compact = x.astype(np.float32)
    if not np.array_equal(np.isfinite(x), np.isfinite(compact)):
        raise ValueError('float32_head_input_overflow')
    return compact


def exit_cost_targets(return_bps, long_net_bps, short_net_bps, entry_long, entry_short):
    """Exact endpoint-cost accounting; labels are never model inputs."""
    y = _vector(return_bps, 'return')
    long_net = _vector(long_net_bps, 'long_net', len(y))
    short_net = _vector(short_net_bps, 'short_net', len(y))
    a = _cost(entry_long, 'entry_long', len(y))
    b = _cost(entry_short, 'entry_short', len(y))
    raw_long, raw_short = y-a-long_net, -y-b-short_net
    return (_cost(raw_long, 'exit_long', len(y), label=True),
            _cost(raw_short, 'exit_short', len(y), label=True),
            {'long_exit': int((raw_long < 0).sum()), 'short_exit': int((raw_short < 0).sum())})


def fit_heads(X, y, exit_long, exit_short, *, pair_count=68):
    """Fit six heads on a caller-supplied mature prefix; equal sampled rows.

    X ends with two raw current entry costs and a categorical pair ID. Every
    other column is a registered compact input normalized using this prefix.
    """
    provenance = source_provenance()
    x = _base_matrix(X, pair_count)
    r = _vector(y, 'return', len(x))
    raw_long = _vector(exit_long, 'exit_long', len(x))
    raw_short = _vector(exit_short, 'exit_short', len(x))
    long = _cost(raw_long, 'exit_long', len(x), label=True)
    short = _cost(raw_short, 'exit_short', len(x), label=True)
    positive = r > 0
    if min(int(positive.sum()), int((~positive).sum())) < MIN_CLASS_ROWS:
        raise ValueError('insufficient_training_sign_classes')
    categorical = [x.shape[1]-1]
    models = {}
    targets = {'probability': positive.astype(np.int8), 'direct': r,
               'positive': r[positive], 'nonpositive': -r[~positive],
               'long_exit': long, 'short_exit': short}
    for name in HEAD_NAMES:
        mask = positive if name == 'positive' else ~positive if name == 'nonpositive' else np.ones(len(r), bool)
        cls = HistGradientBoostingClassifier if name == 'probability' else HistGradientBoostingRegressor
        model = cls(loss='log_loss' if name == 'probability' else 'squared_error',
                    categorical_features=categorical, **BASE_RECIPE)
        model.fit(x[mask], targets[name], sample_weight=np.ones(int(mask.sum()), dtype=np.float64))
        models[name] = model
    return {
        'schema': SCHEMA, 'kind': 'six_heads', 'models': models,
        'input_columns': x.shape[1], 'registered_input_count': x.shape[1]-3,
        'pair_count': pair_count, 'head_names': list(HEAD_NAMES), 'recipe': dict(BASE_RECIPE),
        'training_rows': len(r), 'positive_rows': int(positive.sum()),
        'nonpositive_rows': int((~positive).sum()), 'flat_rows': int((r == 0).sum()),
        'weighting': 'equal sampled rows; branches subset the same underlying measure',
        'probability_target': 'P(return_bps > 0); flats retained as nonpositive',
        'label_roundoff_clips': {'long_exit': int((raw_long < 0).sum()), 'short_exit': int((raw_short < 0).sum())},
        'source_provenance': provenance, 'caller_owns_clock_and_prefix_contract': True,
        'can_place_orders': False,
    }


def _head_values(heads):
    if set(heads) != set(HEAD_NAMES):
        raise ValueError('exact_six_raw_heads_required')
    values = {n: _vector(heads[n], n) for n in HEAD_NAMES}
    rows = len(values['probability'])
    if any(len(a) != rows for a in values.values()):
        raise ValueError('head_shape_mismatch')
    p = values['probability']
    if np.any((p < 0) | (p > 1)):
        raise ValueError('invalid_probability')
    if any(np.any(values[n] < 0) for n in ('positive', 'nonpositive', 'long_exit', 'short_exit')):
        raise ValueError('unclipped_magnitude_or_cost')
    return values


def predict_heads(bundle, X):
    if bundle.get('schema') != SCHEMA or bundle.get('kind') != 'six_heads':
        raise ValueError('wrong_head_bundle')
    x = _base_matrix(X, bundle['pair_count'], bundle['input_columns'])
    models = bundle['models']
    if set(models) != set(HEAD_NAMES):
        raise ValueError('exact_six_models_required')
    if not np.array_equal(models['probability'].classes_, np.array([0, 1])):
        raise ValueError('positive_class_identity_required')
    values = {'probability': models['probability'].predict_proba(x)[:, 1],
              'direct': models['direct'].predict(x)}
    clips = {}
    for name in ('positive', 'nonpositive', 'long_exit', 'short_exit'):
        prediction = _vector(models[name].predict(x), name, len(x))
        clips[name] = int((prediction < 0).sum())
        values[name] = np.maximum(0., prediction)
    _head_values(values)
    return values, clips


def meta_features(heads, z_context, entry_long, entry_short):
    """Uncalibrated head outputs only; fixed 18-column meta representation."""
    v = _head_values(heads)
    p = v['probability']; rows = len(p)
    context = np.asarray(z_context, dtype=np.float64)
    if context.shape != (rows, 8) or np.isinf(context).any():
        raise ValueError('eight_causal_context_columns_required')
    a = _cost(entry_long, 'entry_long', rows); b = _cost(entry_short, 'entry_short', rows)
    mu = p*v['positive']-(1-p)*v['nonpositive']
    absolute = p*v['positive']+(1-p)*v['nonpositive']
    out = np.column_stack([*(v[n] for n in HEAD_NAMES), mu, absolute, a, b, context])
    if np.isinf(out).any():
        raise ValueError('meta_feature_overflow')
    return out


def _meta_matrix(meta):
    x = np.asarray(meta, dtype=np.float64)
    if x.ndim != 2 or x.shape[1] != len(META_NAMES):
        raise ValueError('exact_eighteen_meta_columns_required')
    if np.isinf(x).any() or not np.isfinite(x[:, :10]).all():
        raise ValueError('invalid_raw_meta_inputs')
    if np.any((x[:, 0] < 0) | (x[:, 0] > 1)) or np.any(x[:, [2, 3, 4, 5, 7, 8, 9]] < 0):
        raise ValueError('invalid_meta_probability_or_cost')
    # Prevent a calibrated or otherwise substituted mixture from being hidden
    # under the fixed raw-input namespace.
    if not np.array_equal(x[:, 6], x[:, 0]*x[:, 2]-(1-x[:, 0])*x[:, 3]):
        raise ValueError('raw_mixture_identity_required')
    if not np.array_equal(x[:, 7], x[:, 0]*x[:, 2]+(1-x[:, 0])*x[:, 3]):
        raise ValueError('raw_absolute_identity_required')
    return x


def _ridge_matrix(selected, pair_ids, pair_count, scaler):
    z = transform_inputs(selected, scaler)
    missing = ~np.isfinite(z)
    pair = np.zeros((len(z), pair_count), dtype=np.float32)
    pair[np.arange(len(z)), pair_ids] = 1.
    # Keep the bounded float32 standardized values, then use float64 BLAS for
    # stable saved-model replay across full chunks and one-row representatives.
    return np.column_stack((np.where(missing, 0., z), missing.astype(np.float32), pair)).astype(np.float64)


def fit_meta(meta, pair_ids, y, *, arm, fit_mask, pair_count=68):
    """Fit on mature OOF labels; Ridge scaler sees ALL issued OOF inputs.

    No target influences the pooled input scaler or its supported columns.
    Excluded targets may be NaN; selected targets must be finite. The caller
    must supply genuine prefix-fitted OOF predictions, never in-sample heads.
    """
    provenance = source_provenance()
    if arm not in META_COLUMNS:
        raise ValueError('unknown_meta_arm')
    x = _meta_matrix(meta); ids = _pair_ids(pair_ids, len(x), pair_count)
    target = _vector(y, 'meta_target', len(x), finite=False)
    mask = np.asarray(fit_mask)
    if mask.dtype != np.bool_ or mask.shape != (len(x),) or not mask.any():
        raise ValueError('nonempty_boolean_meta_fit_mask_required')
    if not np.isfinite(target[mask]).all():
        raise ValueError('finite_selected_meta_targets_required')
    columns = META_COLUMNS[arm]; selected = x[:, columns]
    ridge = arm.endswith('_ridge')
    scaler = fit_normalizer(selected) if ridge else None
    if ridge:
        design = _ridge_matrix(selected[mask], ids[mask], pair_count, scaler)
        model = Ridge(**META_RIDGE_RECIPE)
        recipe = dict(META_RIDGE_RECIPE)
    else:
        design = np.column_stack((selected[mask], ids[mask]))
        model = HistGradientBoostingRegressor(loss='squared_error',
                    categorical_features=[len(columns)], **META_HGB_RECIPE)
        recipe = dict(META_HGB_RECIPE)
    model.fit(design, target[mask])
    return {
        'schema': SCHEMA, 'kind': 'meta', 'arm': arm, 'model': model,
        'pair_count': pair_count, 'columns': list(columns),
        'selected_meta_names': [META_NAMES[i] for i in columns],
        'scaler': scaler, 'recipe': recipe,
        'issued_oof_rows': len(x), 'training_rows': int(mask.sum()),
        'scaler_rows': len(x) if ridge else 0,
        'scaler_scope': 'all issued OOF inputs before target filtering' if ridge else 'none; native NaN HGB',
        'probability_calibration_in_inputs': False,
        'source_provenance': provenance, 'caller_owns_causal_oof_contract': True,
        'can_place_orders': False,
    }


def predict_meta(bundle, meta, pair_ids):
    if bundle.get('schema') != SCHEMA or bundle.get('kind') != 'meta' or bundle.get('arm') not in META_COLUMNS:
        raise ValueError('wrong_meta_bundle')
    x = _meta_matrix(meta); ids = _pair_ids(pair_ids, len(x), bundle['pair_count'])
    expected = list(META_COLUMNS[bundle['arm']])
    if bundle['columns'] != expected or bundle['selected_meta_names'] != [META_NAMES[i] for i in expected]:
        raise ValueError('meta_column_identity_changed')
    selected = x[:, expected]
    if bundle['arm'].endswith('_ridge'):
        design = _ridge_matrix(selected, ids, bundle['pair_count'], bundle['scaler'])
    else:
        design = np.column_stack((selected, ids))
    return _vector(bundle['model'].predict(design), 'meta_prediction', len(x))


def fit_calibration(raw_probability, y, *, fit_mask=None):
    """Calibrate strictly-positive probability on the caller's OOF labels."""
    p = _vector(raw_probability, 'raw_probability')
    target = _vector(y, 'calibration_target', len(p), finite=False)
    if np.any((p < 0) | (p > 1)):
        raise ValueError('invalid_probability')
    mask = np.ones(len(p), dtype=bool) if fit_mask is None else np.asarray(fit_mask)
    if mask.dtype != np.bool_ or mask.shape != p.shape or not mask.any() or not np.isfinite(target[mask]).all():
        raise ValueError('invalid_calibration_fit_mask_or_targets')
    calibration = _legacy().fit_platt(p[mask], (target[mask] > 0).astype(np.int8),
                                  np.ones(int(mask.sum())), CALIBRATION_RECIPE)
    calibration.update(configuration=dict(CALIBRATION_RECIPE), source_provenance=source_provenance(),
        input_scope='raw prefix-fitted OOF probabilities only; never assessment labels',
        probability_scope='calibrated_on_raw_chronological_TRAIN_OOF_predictions_not_verified_future_accuracy',
        used_for='separate calibrated-mixture arm; excluded from all meta inputs',
        flat_rows=int((target[mask] == 0).sum()))
    return calibration


def apply_calibration(raw_probability, calibration):
    p = _vector(raw_probability, 'raw_probability')
    if np.any((p < 0) | (p > 1)):
        raise ValueError('invalid_probability')
    return _vector(_legacy().apply_platt(p, calibration), 'calibrated_probability', len(p))


def calibrated_mixture(heads, calibration):
    v = _head_values(heads); p = apply_calibration(v['probability'], calibration)
    return _vector(p*v['positive']-(1-p)*v['nonpositive'], 'calibrated_mixture', len(p))


def decide(mean_return, long_entry_cost, short_entry_cost, long_exit_cost, short_exit_cost, margin_bps):
    return _legacy().decide(mean_return, long_entry_cost, short_entry_cost,
                            long_exit_cost, short_exit_cost, margin_bps)
