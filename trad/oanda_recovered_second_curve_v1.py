"""Read-only recovered S5 ridge inference; no fitting, orders, stores or runners.

The fixed July 31 JSON checkpoint and original feature/inference sources are
hash-bound. This adapter keeps the original S5 bar-start label convention: the
reference price is a completed bar's mid_close, first available after start+5.
The original training target is the first real bar at or after start+horizon,
with an inclusive 0–7 second delay; nominal anchors are not observed endpoints.
Computation never supplies an issue timestamp or grants forecast authority.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import statistics
import time

SCHEMA = 'recovered_second_curve_v1_20260909'
CAPTURE_SCHEMA = 'recovered_second_s5_capture_v1_20260909'
FEATURE_VERSION = 'original_s5_14_features_explicit_sampling_v1'
SAMPLING_POLICIES = ('exact_grid', 'retained_fit_window')
PRICE_CONVENTIONS = ('retained_midpoint_unknown_ingestion', 'official_midpoint', 'ba_derived_midpoint')
TRAINING_TARGET_POLICY = 'first_real_S5_bar_label_at_or_after_nominal_within_7_seconds'
ARTIFACT_SHA256 = '85ff37fd77f85fa6ae990990ead19eb7d4e20389d99b774023f32c13bf62658c'
ORIGINAL_SOURCES = {
    'oanda_second_forecast.py': '3ac9f38bbe7140aae7bf391d1778f658b599199601d811d97d3386f32d727063',
    'oanda_second_forecast_fit.py': '238b8e6d83536aa287dc6282a364ccbe7ba016f2e00221e3b95381edd8d4aba8',
}
FEATURE_NAMES = ('return_5_pips', 'return_10_pips', 'return_30_pips', 'return_60_pips',
                 'acceleration_5_30', 'volatility_30_pips', 'volatility_60_pips',
                 'range_30_pips', 'spread_pips', 'spread_ratio_60', 'activity_30',
                 'activity_ratio_30_60', 'hour_sin', 'hour_cos')
HORIZONS = (15, 30, 60, 120, 180, 300, 600, 900, 1800, 3600, 7200, 10800, 14400)
# Exact original trained-unit table from the hash-bound fit source, not a
# suffix-based broker pip guess. Callers must independently provide pip_size.
TRAINED_PIP_MINUS2 = frozenset(('AUD_JPY', 'CAD_JPY', 'CHF_JPY', 'EUR_HUF', 'EUR_JPY',
    'GBP_JPY', 'NZD_JPY', 'SGD_JPY', 'TRY_JPY', 'USD_HUF', 'USD_JPY', 'USD_THB', 'ZAR_JPY'))
MAX_INPUT_AGE_SEC = 30.0
INERT = {'research_only': True, 'account_eligible': False, 'can_place_orders': False,
         'can_promote': False, 'can_authorize': False, 'proof_eligible': False,
         'forecast_issued': False}
ROW_KEYS = frozenset(('bar_start_epoch', 'available_epoch', 'complete', 'bid_close',
    'ask_close', 'mid_close', 'mid_high', 'mid_low', 'spread_pips', 'volume'))
_VERIFIED_MODEL_BYTES = None


def _number(value, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError('nonfinite_or_nonnumeric')
    if positive and value <= 0:
        raise ValueError('nonpositive')
    return float(value)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def _hash(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _source_bindings():
    root = Path(__file__).resolve().parent
    for name, expected in ORIGINAL_SOURCES.items():
        if hashlib.sha256((root/name).read_bytes()).hexdigest() != expected:
            raise ValueError('original_source_changed:' + name)
    return dict(ORIGINAL_SOURCES)


def _identity(instrument, pip_size):
    if not isinstance(instrument, str) or not re.fullmatch(r'[A-Z]{3}_[A-Z]{3}', instrument):
        raise ValueError('invalid_instrument')
    if instrument[:3] == instrument[4:]:
        raise ValueError('identical_currencies')
    pip = _number(pip_size, positive=True)
    trained_pip = .01 if instrument in TRAINED_PIP_MINUS2 else .0001
    if pip != trained_pip:
        raise ValueError('supplied_pip_mismatches_original_training_units')
    return pip


@dataclass(frozen=True)
class _Cell:
    instrument: str
    horizon: int
    model_id: str
    means: tuple
    scales: tuple
    coefficients: tuple
    intercept: float
    calibration: float
    residual: float
    fit_provenance: str
    pair_specific: bool


@dataclass(frozen=True)
class RecoveredModel:
    artifact_sha256: str
    artifact_bytes: int
    fitted_epoch: float
    fitted_utc: str
    cells: tuple[_Cell, ...]


def load_model(path):
    """Read the one verified JSON artifact, never deserialize executable objects."""
    _source_bindings()
    with Path(path).open('rb') as handle:
        raw = handle.read(8 * 1024 * 1024 + 1)
    if len(raw) > 8 * 1024 * 1024 or hashlib.sha256(raw).hexdigest() != ARTIFACT_SHA256:
        raise ValueError('unverified_model_bytes')
    payload = json.loads(raw)
    if (payload.get('schema_version') != 1 or tuple(payload.get('feature_names', ())) != FEATURE_NAMES
            or tuple(payload.get('horizons_sec', ())) != HORIZONS):
        raise ValueError('model_schema')
    fitted = datetime.fromisoformat(payload['fitted_utc'])
    if fitted.tzinfo is None:
        raise ValueError('model_fit_timezone')
    cells = []
    for instrument, models in sorted(payload['models'].items()):
        if tuple(sorted(map(int, models))) != HORIZONS:
            raise ValueError('model_horizon_coverage')
        for horizon in HORIZONS:
            cell = models[str(horizon)]
            vectors = [tuple(_number(x) for x in cell[key])
                       for key in ('feature_means', 'feature_scales', 'coefficients')]
            if any(len(v) != 14 for v in vectors):
                raise ValueError('model_feature_shape')
            cells.append(_Cell(instrument, horizon, cell['model_id'], *vectors,
                _number(cell['intercept']), min(10., max(0., _number(cell['magnitude_calibration']))),
                max(.05, _number(cell['residual_std_pips'])), cell['fit_provenance'],
                cell.get('pair_specific_evidence') is True))
    snapshot = RecoveredModel(ARTIFACT_SHA256, len(raw), fitted.timestamp(), payload['fitted_utc'], tuple(cells))
    # Retain immutable factory-derived contents, not the caller's artifact SHA.
    # A dataclasses.replace() copy carrying changed coefficients must fail.
    global _VERIFIED_MODEL_BYTES
    verified = _canonical(asdict(snapshot))
    if _VERIFIED_MODEL_BYTES is not None and verified != _VERIFIED_MODEL_BYTES:
        raise ValueError('verified_model_factory_disagreement')
    _VERIFIED_MODEL_BYTES = verified
    return snapshot


def _features(rows, pip):
    """Same S5 14-feature mathematics as original feature_matrix, at one origin."""
    mids = [r['mid_close'] for r in rows]
    moves = [(b-a)/pip for a, b in zip(mids, mids[1:])]
    r5, r30 = (mids[-1]-mids[-2])/pip, (mids[-1]-mids[-7])/pip
    spreads = [r['spread_pips'] for r in rows[-12:]]
    activity30 = sum(r['volume'] for r in rows[-6:])
    activity60 = sum(r['volume'] for r in rows[-12:])
    hour = (rows[-1]['bar_start_epoch'] % 86400) / 3600
    return dict(zip(FEATURE_NAMES, (r5, (mids[-1]-mids[-3])/pip, r30,
        (mids[-1]-mids[0])/pip, r5-r30/6, statistics.pstdev(moves[-6:]),
        statistics.pstdev(moves), (max(r['mid_high'] for r in rows[-6:])-
        min(r['mid_low'] for r in rows[-6:]))/pip, spreads[-1],
        spreads[-1]/max(statistics.fmean(spreads), .01), activity30,
        activity30/max(activity60/2, 1), math.sin(2*math.pi*hour/24), math.cos(2*math.pi*hour/24))))


def _sampling_metadata(rows, policy):
    labels = [row['bar_start_epoch'] for row in rows]
    intervals = [b-a for a,b in zip(labels, labels[1:])]
    gaps = [{'after_label_epoch': a, 'before_label_epoch': b, 'missing_s5_intervals': int((b-a)/5)-1}
            for a,b in zip(labels, labels[1:]) if b-a != 5]
    supports = {}
    for name, count in [('return_5_pips',1),('return_10_pips',2),('return_30_pips',6),
                         ('return_60_pips',12),('volatility_30_pips',6),('volatility_60_pips',12)]:
        supports[name] = {'start_price_epoch': labels[-1-count]+5,
                         'end_price_epoch': labels[-1]+5,
                         'elapsed_sec': labels[-1]-labels[-1-count],
                         'real_increment_count': count}
    for name,count in [('range_30_pips',6),('activity_30',6),('spread_ratio_60',12)]:
        supports[name] = {'first_bar_label_epoch':labels[-count], 'last_bar_close_epoch':labels[-1]+5,
                         'elapsed_sec':labels[-1]+5-labels[-count], 'real_bar_count':count}
    supports['activity_ratio_30_60'] = {'numerator': dict(supports['activity_30']),
        'denominator': {'first_bar_label_epoch':labels[-12], 'last_bar_close_epoch':labels[-1]+5,
                        'elapsed_sec':labels[-1]+5-labels[-12], 'real_bar_count':12}}
    supports['acceleration_5_30'] = {'return_5':dict(supports['return_5_pips']),
                                   'return_30':dict(supports['return_30_pips']), 'original_divisor':6}
    supports['spread_pips'] = {'price_epoch':labels[-1]+5, 'elapsed_sec':0}
    for name in ('hour_sin','hour_cos'):
        supports[name] = {'bar_label_epoch':labels[-1], 'basis':'UTC_bar_start_including_seconds'}
    return {'policy':policy, 'original_fit_endpoint_span_min_sec':55,
        'original_fit_endpoint_span_max_sec':70, 'actual_endpoint_span_sec':labels[-1]-labels[0],
        'actual_adjacent_intervals_sec':intervals, 'gaps':gaps,
        'missing_s5_intervals':sum(gap['missing_s5_intervals'] for gap in gaps),
        'features_are_original_record_count_formulas_not_resampled':True,
        'feature_temporal_support':supports}


def _target_selection_policy(reference_label_epoch=None, horizon_sec=None):
    policy = {'name':TRAINING_TARGET_POLICY,
        'nominal_anchor':'reference_bar_start_label_plus_native_horizon_seconds',
        'selector':'first_real_bar_label_at_or_after_nominal_searchsorted_left',
        'minimum_delay_sec':0, 'maximum_delay_sec':7, 'delay_bounds_inclusive':True,
        'target_price_basis':'selected_S5_bar_mid_close',
        'target_price_epoch_offset_from_selected_label_sec':5,
        'nominal_anchor_guarantees_exact_training_endpoint':False,
        'actual_endpoint_requires_future_observation':True,
        'actual_target_label_epoch':None, 'actual_target_price_epoch':None}
    if reference_label_epoch is not None:
        nominal = reference_label_epoch+horizon_sec
        policy.update(nominal_target_label_epoch=nominal,
            nominal_target_price_epoch=nominal+5,
            admissible_target_label_epoch_bounds=[nominal,nominal+7],
            admissible_target_price_epoch_bounds=[nominal+5,nominal+12])
    return policy


def capture_s5_rows(rows, *, instrument, pip_size, source_sha256, clock=time.time,
                    scope='engineering_replay', sampling_policy='exact_grid',
                    price_convention='retained_midpoint_unknown_ingestion'):
    """Seal 13 already observed real S5 bars; retain caller's arrival clocks.

    available_epoch is the actual source/consumer observation, never a guessed
    bar-end arrival. Historical file reads should use their actual read clock.
    The engineering scope never claims original historical availability.
    """
    bindings = _source_bindings()
    observed = _number(clock(), positive=True)
    pip = _identity(instrument, pip_size)
    if scope not in ('engineering_replay', 'current_research'):
        raise ValueError('invalid_scope')
    if sampling_policy not in SAMPLING_POLICIES:
        raise ValueError('invalid_sampling_policy')
    if price_convention not in PRICE_CONVENTIONS:
        raise ValueError('invalid_price_convention')
    if scope == 'current_research' and price_convention == 'retained_midpoint_unknown_ingestion':
        raise ValueError('explicit_current_price_convention_required')
    if not isinstance(source_sha256, str) or not re.fullmatch('[0-9a-f]{64}', source_sha256):
        raise ValueError('source_sha256_required')
    if not isinstance(rows, (list, tuple)) or len(rows) != 13:
        raise ValueError('exactly_13_real_s5_rows_required')
    retained = []
    for raw in rows:
        if not isinstance(raw, dict) or set(raw) != ROW_KEYS or raw.get('complete') is not True:
            raise ValueError('row_schema_or_incomplete')
        row = {key: _number(raw[key]) for key in ROW_KEYS if key != 'complete'}
        row['complete'] = True
        epoch = row['bar_start_epoch']
        if epoch <= 0 or epoch % 5 != 0:
            raise ValueError('s5_grid')
        if not epoch+5 <= row['available_epoch'] <= observed:
            raise ValueError('source_availability_clock')
        if retained:
            interval = epoch - retained[-1]['bar_start_epoch']
            if interval <= 0 or (sampling_policy == 'exact_grid' and interval != 5):
                raise ValueError('missing_duplicate_or_reordered_bar')
        if not (0 < row['bid_close'] <= row['ask_close'] and row['mid_low'] > 0
                and row['mid_low'] <= row['mid_close'] <= row['mid_high']
                and row['spread_pips'] >= 0 and row['volume'] >= 0):
            raise ValueError('invalid_market_values')
        # Mid and spread are separately retained OANDA series; do not overwrite
        # them with a synthetic midpoint. Validate the cross-series constraints.
        if not row['bid_close'] <= row['mid_close'] <= row['ask_close']:
            raise ValueError('mid_outside_bid_ask')
        spread_from_quote = (row['ask_close']-row['bid_close'])/pip
        if abs(spread_from_quote-row['spread_pips']) > max(.011, 1e-7*spread_from_quote):
            raise ValueError('spread_pip_binding')
        retained.append(row)
    sampling = _sampling_metadata(retained, sampling_policy)
    if sampling_policy == 'retained_fit_window' and not 55 <= sampling['actual_endpoint_span_sec'] <= 70:
        raise ValueError('outside_original_55_to_70_second_fit_window')
    close_epoch = retained[-1]['bar_start_epoch'] + 5
    if scope == 'current_research' and not 0 <= observed-close_epoch <= MAX_INPUT_AGE_SEC:
        raise ValueError('stale_current_s5_input')
    value = {'schema_version': CAPTURE_SCHEMA, 'feature_version': FEATURE_VERSION,
        'instrument': instrument, 'pip_size': pip, 'scope': scope, 'source_sha256': source_sha256,
        'sampling_policy': sampling_policy, 'sampling_metadata': sampling,
        'price_convention': price_convention, 'historical_ingestion_equivalence_proven':False,
        'source_binding_scope': 'caller_supplied_source_digest_rows_self_sealed_not_reread_from_source',
        'original_source_bindings': bindings, 'rows': retained, 'rows_sha256': _hash(retained),
        'feature_names': list(FEATURE_NAMES), 'features': _features(retained, pip),
        'reference_label_epoch': retained[-1]['bar_start_epoch'],
        'reference_epoch': close_epoch, 'reference_price_epoch': close_epoch,
        'reference_price': retained[-1]['mid_close'],
        'reference_available_epoch': retained[-1]['available_epoch'],
        'input_available_epoch': max(row['available_epoch'] for row in retained),
        'max_bar_close_epoch': close_epoch, 'first_observed_epoch': observed,
        'timestamp_semantics': 'S5_bar_start_label_price_is_completed_mid_close',
        'training_target_policy': TRAINING_TARGET_POLICY, 'training_target_max_delay_sec':7,
        'target_selection_policy':_target_selection_policy(),
        'availability_scope': 'actual_retained_source_or_consumer_observation_not_historical_reconstruction',
        'complete_real_rows': 13, 'fabricated_rows': 0, **INERT}
    value['capture_sha256'] = _hash(value)
    return value


def validate_capture(capture):
    if not isinstance(capture, dict):
        raise ValueError('capture_object_required')
    body = {k:v for k,v in capture.items() if k != 'capture_sha256'}
    if capture.get('capture_sha256') != _hash(body):
        raise ValueError('capture_seal')
    rebuilt = capture_s5_rows(capture['rows'], instrument=capture['instrument'],
        pip_size=capture['pip_size'], source_sha256=capture['source_sha256'],
        scope=capture['scope'], clock=lambda:capture['first_observed_epoch'],
        sampling_policy=capture['sampling_policy'], price_convention=capture['price_convention'])
    if rebuilt != capture:
        raise ValueError('capture_replay_mismatch')
    return True


def predict_curve(capture, model, *, clock=time.time):
    """Return original curve nodes as computation evidence, never issue them."""
    started = _number(clock(), positive=True)
    validate_capture(capture)
    if (not isinstance(model, RecoveredModel) or model.artifact_sha256 != ARTIFACT_SHA256
            or _VERIFIED_MODEL_BYTES is None or _canonical(asdict(model)) != _VERIFIED_MODEL_BYTES):
        raise ValueError('verified_model_required')
    if started < max(capture['first_observed_epoch'], model.fitted_epoch):
        raise ValueError('computation_before_available_input_or_model')
    if capture['scope'] == 'current_research' and started-capture['max_bar_close_epoch'] > MAX_INPUT_AGE_SEC:
        raise ValueError('stale_at_computation_start')
    cells = [cell for cell in model.cells if cell.instrument == capture['instrument']]
    if tuple(cell.horizon for cell in cells) != HORIZONS:
        raise ValueError('instrument_model_unavailable')
    values = tuple(capture['features'][name] for name in FEATURE_NAMES)
    points = []
    for cell in cells:
        # Match original SecondRidgeSnapshot compilation and operation order.
        weights = [coef/max(scale, 1e-9) for coef, scale in zip(cell.coefficients, cell.scales)]
        intercept = cell.intercept-sum(mean*w for mean,w in zip(cell.means, weights))
        raw = intercept+sum(w*x for w,x in zip(weights, values))
        expected = raw*cell.calibration
        probability = 1/(1+math.exp(-max(-8., min(8., expected/cell.residual))))
        if not all(math.isfinite(x) for x in (raw, expected, probability)):
            raise ValueError('nonfinite_prediction')
        target = capture['reference_price_epoch']+cell.horizon
        points.append({'horizon_sec': cell.horizon, 'target_epoch': target,
            'target_label_epoch': capture['reference_label_epoch']+cell.horizon,
            'target_price_epoch': target,
            'target_price_available_not_before_epoch': target, 'target_price_kind': 'S5_mid_close',
            'training_target_policy':TRAINING_TARGET_POLICY, 'training_target_max_delay_sec':7,
            'training_target_price_window_end_epoch':target+7,
            'target_selection_policy':_target_selection_policy(capture['reference_label_epoch'],cell.horizon),
            'target_anchor_is_nominal_training_endpoints_may_be_delayed':True,
            'actual_future_target_epoch':None,
            'model_id': cell.model_id, 'raw_score_pips': raw, 'predicted_signed_pips': expected,
            'probability_up': probability, 'probability_scope': 'uncalibrated_retained_residual_logistic',
            'residual_std_pips': cell.residual, 'uncertainty_scope': 'retained_training_residual_not_calibrated_interval',
            'magnitude_calibration': cell.calibration, 'fit_provenance': cell.fit_provenance,
            'pair_specific_evidence': cell.pair_specific, **INERT})
    completed = _number(clock(), positive=True)
    if completed < started:
        raise ValueError('computation_clock_reversed')
    if capture['scope'] == 'current_research' and completed-capture['max_bar_close_epoch'] > MAX_INPUT_AGE_SEC:
        raise ValueError('stale_at_computation_completion')
    result = {'schema_version': SCHEMA, 'status': 'computed_not_issued',
        'instrument': capture['instrument'], 'pip_size': capture['pip_size'], 'scope': capture['scope'],
        'sampling_policy':capture['sampling_policy'], 'sampling_metadata':capture['sampling_metadata'],
        'price_convention':capture['price_convention'], 'historical_ingestion_equivalence_proven':False,
        'reference_epoch': capture['reference_epoch'], 'reference_price_epoch': capture['reference_price_epoch'],
        'reference_label_epoch': capture['reference_label_epoch'], 'reference_price': capture['reference_price'],
        'reference_available_epoch': capture['reference_available_epoch'],
        'input_available_epoch': capture['input_available_epoch'], 'first_observed_epoch': capture['first_observed_epoch'],
        'max_bar_close_epoch': capture['max_bar_close_epoch'], 'timestamp_semantics': capture['timestamp_semantics'],
        'training_target_policy':TRAINING_TARGET_POLICY, 'training_target_max_delay_sec':7,
        'target_selection_policy':_target_selection_policy(),
        'computation_started_epoch': started, 'computed_epoch': completed,
        'model_fitted_epoch': model.fitted_epoch, 'model_fitted_utc': model.fitted_utc,
        'model_sha256': model.artifact_sha256, 'model_bytes': model.artifact_bytes,
        'original_source_bindings': _source_bindings(), 'source_sha256': capture['source_sha256'],
        'capture_sha256': capture['capture_sha256'], 'feature_version': FEATURE_VERSION,
        'feature_names': list(FEATURE_NAMES), 'features_sha256': _hash(capture['features']),
        'input_after_model_fit': capture['reference_epoch'] >= model.fitted_epoch,
        'training_timeframe': 'S5', 'computed_input_timeframe': 'S5', 'original_live_input_timeframe': 'S1',
        'points': points, 'execution_profiles_included': False,
        'evaluation_scope': 'inference_compatibility_only_no_edge_or_prospective_score_claim', **INERT}
    result['result_sha256'] = _hash(result)
    return result
