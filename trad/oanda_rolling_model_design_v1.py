"""Fixed development comparison; explicit inputs and train-only transforms."""
from __future__ import annotations

import numpy as np

SCHEMA = 'rolling_model_comparison_v1_20260915'
HORIZONS = (5, 15, 30, 60)
TRAIN_SAMPLE_SECONDS = 900
SEED = 20260915
COMPACT_LOCAL = [
    'm1__return_1_bps', 'm1__return_5_bps', 'm1__return_15_bps',
    'm1__return_30_bps', 'm1__return_60_bps', 'm1__path_efficiency_15',
    'm1__path_efficiency_60', 'm1__up_fraction_15', 'm1__return_autocorr1_15',
    'm1__sign_entropy_30', 'm1__bar_range_pips', 'm1__body_to_range',
    'm1__upper_wick_fraction', 'm1__atr_14_pips', 'm1__return_vol_15_pips',
    'm1__return_vol_60_pips', 'm1__return_skew_30', 'm1__return_kurt_30',
    'm1__jump_variation_ratio_30', 'm1__rsi_14', 'm1__bollinger_z_20',
    'm1__range_position_20', 'm1__finite_ema_gap_5_pips',
    'm1__finite_ema_gap_20_pips', 'm1__finite_ema_gap_60_pips',
    'm1__finite_ema_curvature_20_pips', 'm1__sma_gap_200_pips',
    'm1__tick_activity_log1p', 'm1__tick_activity_ratio_30',
    'm1__tick_activity_z_120', 'm1__return_activity_corr_60',
    'm1__historical_spread_pips', 'm1__spread_ratio_prior_120',
    'm1__spread_drop_3_pips', 'm1__utc_hour_sin', 'm1__utc_hour_cos',
    'm1__utc_weekday_sin', 'm1__utc_weekday_cos',
]
RECIPES = {
    'ridge': {'alpha': 1000., 'solver': 'cholesky', 'fit_intercept': True},
    'hgb': {'loss': 'squared_error', 'learning_rate': .05, 'max_iter': 120,
            'max_leaf_nodes': 15, 'min_samples_leaf': 200,
            'l2_regularization': 10., 'max_bins': 128,
            'early_stopping': False, 'random_state': SEED},
}


def feature_groups(names):
    names = list(names)
    if len(names) != 228 or len(set(names)) != 228:
        raise ValueError('exact_228_registered_inputs_required')
    if any(not n.startswith(('m1__', 'peer__')) for n in names):
        raise ValueError('future_or_metadata_input_refused')
    local = [n for n in names if n.startswith('m1__')]
    peer = [n for n in names if n.startswith('peer__')]
    if len(local) != 216 or len(peer) != 12 or not set(COMPACT_LOCAL) <= set(local):
        raise ValueError('registered_local_and_peer_contract_required')
    return {'compact38': COMPACT_LOCAL.copy(), 'compact50': COMPACT_LOCAL + peer,
            'local216': local, 'combined228': names}


def fit_normalizer(train_x):
    """Per-pair full TRAIN inputs only, before supervised row selection.

    A pair/input needs20 finite training values and positive sample variance.
    Unsupported/constant pair-inputs remain missing in EVERY later period.
    No target, later-period statistic or clipping threshold is consulted.
    """
    x = np.asarray(train_x, dtype=np.float64)
    if x.ndim != 2 or not len(x):
        raise ValueError('nonempty_2d_training_inputs_required')
    finite = np.isfinite(x)
    count = finite.sum(axis=0)
    minimum = np.where(finite, x, np.inf).min(axis=0)
    maximum = np.where(finite, x, -np.inf).max(axis=0)
    mean = np.divide(np.where(finite, x, 0.).sum(axis=0), count,
                     out=np.zeros(x.shape[1]), where=count > 0)
    diff = np.where(finite, x - mean, 0.)
    variance = np.divide((diff * diff).sum(axis=0), count,
                         out=np.zeros(x.shape[1]), where=count > 0)
    scale = np.sqrt(variance)
    supported = (count >= 20) & (maximum > minimum) & np.isfinite(mean) & np.isfinite(scale) & (scale > 0)
    return {'count': count, 'mean': mean, 'scale': scale, 'supported': supported}


def transform_inputs(x, params):
    x = np.asarray(x, dtype=np.float64)
    if x.ndim != 2 or x.shape[1] != len(params['mean']):
        raise ValueError('normalizer_shape_mismatch')
    out = np.full(x.shape, np.nan, dtype=np.float64)
    supported = np.asarray(params['supported'], dtype=bool)
    with np.errstate(over='ignore', invalid='ignore', divide='ignore'):
        out[:, supported] = (x[:, supported] - params['mean'][supported]) / params['scale'][supported]
    expected_finite = np.isfinite(x) & supported[None, :]
    if np.any(expected_finite & ~np.isfinite(out)):
        raise ValueError('float64_transform_overflow')
    out[~np.isfinite(x)] = np.nan
    with np.errstate(over='ignore'):
        compact = out.astype(np.float32)
    if not np.array_equal(np.isfinite(out), np.isfinite(compact)):
        raise ValueError('float32_transform_overflow')
    return compact


def learner_inputs(z, pair_ids, learner, pair_count):
    """Add explicitly declared causal pair context; never generic metadata.

    Ridge: z=0 imputation plus one missing flag for each selected field, then
    a fixed all-universe pair one-hot vector. HGB: native NaN plus categorical
    pair id. These added representations are recorded separately from fields.
    """
    z = np.asarray(z, dtype=np.float32)
    ids = np.asarray(pair_ids)
    if (z.ndim != 2 or ids.shape != (len(z),) or ids.dtype.kind not in 'iu'
            or np.any(ids < 0) or np.any(ids >= pair_count)):
        raise ValueError('declared_pair_context_required')
    if learner == 'ridge':
        missing = ~np.isfinite(z)
        context = np.zeros((len(z), pair_count), dtype=np.float32)
        context[np.arange(len(z)), ids] = 1.
        return np.column_stack((np.where(missing, 0., z), missing.astype(np.float32), context))
    if learner == 'hgb':
        return np.column_stack((z, ids.astype(np.float32)))
    raise ValueError('unknown_fixed_learner')
