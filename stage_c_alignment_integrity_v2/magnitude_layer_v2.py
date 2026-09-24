"""Matched causal ablation: signed-only versus signed/magnitude interaction Ridge."""
from collections import Counter, defaultdict
import math
import random

import numpy as np
from threadpoolctl import threadpool_limits

from contracts import fingerprint
from causal_convex_blend_v2 import validate_row as validate_signed, label

BASES = ('ridge', 'recovered_hgb')
ARMS = ('signed_only', 'magnitude_interaction')
METRICS = ('zero', 'raw_signed', *ARMS)


def join_predictions(bases, absolute, expected_scope):
    magnitude = {r['record_id']: r for r in absolute}
    if len(magnitude) != len(absolute) or set(magnitude) != {r['record_id'] for r in bases}:
        raise ValueError('magnitude_exact_base_prediction_support_required')
    result = []
    for base in bases:
        validate_signed(base, expected_scope)
        a = magnitude[base['record_id']]
        if a['forecast_id'] != fingerprint({k: v for k, v in a.items() if k != 'forecast_id'}):
            raise ValueError('magnitude_original_prediction_identity_mismatch')
        for field in ('instrument', 'decision_epoch', 'available_epoch', 'procedure', 'selected_fit_cutoff'):
            if a[field] != base[field]:
                raise ValueError('magnitude_signed_join_mismatch:'+field)
        if (a['original_signed_fit_id'] != base['selected_fit_id'] or a['signed_control_records'] != base['source_records'] or
                a['target_id'] != base['target_id'].replace('midpoint', 'absolute') or
                a['scheduled_model_ready_epoch'] != base['base_model_ready_epochs']['ridge']):
            raise ValueError('magnitude_target_model_clock_lineage_mismatch')
        values = {'ridge': a['predictions']['absolute_ridge'], 'recovered_hgb': a['predictions']['absolute_hgb']}
        row = {**base, 'magnitude_prediction_bps': values, 'absolute_forecast_id': a['forecast_id'],
               'absolute_fit_id': a['fit_id'], 'absolute_record_sha256': fingerprint(a)}
        validate_row(row, expected_scope)
        result.append(row)
    return result


def validate_row(row, scope):
    validate_signed(row, scope)
    if set(row['magnitude_prediction_bps']) != set(BASES) or any(type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in row['magnitude_prediction_bps'].values()):
        raise ValueError('finite_nonnegative_magnitude_required')
    if any(not isinstance(row[k], str) or len(row[k]) != 64 for k in ('absolute_forecast_id', 'absolute_fit_id', 'absolute_record_sha256')):
        raise ValueError('exact_absolute_source_identity_required')


def feature_vector(row, base, arm):
    signed = row['ridge_prediction_bps' if base == 'ridge' else 'recovered_hgb_prediction_bps']
    magnitude = row['magnitude_prediction_bps'][base]
    if arm == 'signed_only':
        return [signed]
    if arm != 'magnitude_interaction':
        raise ValueError('unknown_magnitude_ablation')
    values = [signed, magnitude, signed*magnitude]
    if not all(math.isfinite(v) for v in values):
        raise ValueError('nonfinite_magnitude_interaction')
    return values


def weighted_ridge(x, y, weights, ridge_lambda):
    """Existing normalized Ridge algebra, with explicit equal-origin weights."""
    x, y, w = np.asarray(x, dtype=float), np.asarray(y, dtype=float), np.asarray(weights, dtype=float)
    if (x.ndim != 2 or x.shape[0] != len(y) or w.shape != y.shape or not len(y) or
            not np.isfinite(x).all() or not np.isfinite(y).all() or not np.isfinite(w).all() or
            (w <= 0).any() or not math.isfinite(ridge_lambda) or ridge_lambda <= 0):
        raise ValueError('valid_weighted_ridge_inputs_required')
    mean = np.average(x, axis=0, weights=w)
    scale = np.sqrt(np.average((x-mean)**2, axis=0, weights=w)); scale[scale == 0] = 1
    design = np.column_stack((np.ones(len(y)), (x-mean)/scale))
    penalty = np.eye(design.shape[1])*ridge_lambda; penalty[0, 0] = 0
    with threadpool_limits(limits=1):
        coefficient = np.linalg.solve(design.T@(w[:, None]*design)+penalty, design.T@(w*y))
    if not np.isfinite(coefficient).all():
        raise ValueError('nonfinite_magnitude_layer_coefficients')
    return {'mean': mean.tolist(), 'scale': scale.tolist(), 'coefficient': coefficient.tolist(),
            'ridge_lambda': ridge_lambda, 'weight_sum': float(w.sum())}


def fit_snapshot(rows, outcomes, cutoff, expected_scope, horizon, contract):
    if type(cutoff) is not int:
        raise ValueError('integer_magnitude_layer_cutoff_required')
    eligible, seen = [], set()
    for row in rows:
        # Inspect membership/clocks first; future values never enter transforms.
        if row['record_id'] in seen:
            raise ValueError('duplicate_magnitude_layer_event')
        seen.add(row['record_id'])
        if row['decision_epoch'] >= cutoff or row['available_epoch'] > cutoff:
            continue
        outcome = label(row, outcomes, horizon)
        if outcome['available_epoch'] > cutoff:
            continue
        if outcome['value'] is None:
            continue
        validate_row(row, expected_scope)
        if type(outcome['value']) not in (int, float) or not math.isfinite(outcome['value']):
            raise ValueError('finite_mature_layer_outcome_required')
        eligible.append((row, outcome))
    eligible.sort(key=lambda item: (item[0]['decision_epoch'], item[0]['instrument']))
    counts = Counter(r['decision_epoch'] for r, _ in eligible)
    membership = [{'record_id': r['record_id'], 'origin_epoch': r['decision_epoch'],
                   'prediction_sha256': fingerprint(r), 'outcome_sha256': fingerprint(o),
                   'forecast_available_epoch': r['available_epoch'], 'outcome_available_epoch': o['available_epoch']}
                  for r, o in eligible]
    support = {'rows': len(eligible), 'distinct_origins': len(counts),
               'distinct_utc_days': len({t//86400 for t in counts}),
               'distinct_pairs': len({r['instrument'] for r, _ in eligible}),
               'training_population_sha256': fingerprint(membership),
               'maximum_outcome_available_epoch': max((o['available_epoch'] for _, o in eligible), default=None)}
    ready = all(support[k] >= contract['minimum_'+k] for k in ('distinct_origins', 'distinct_utc_days', 'distinct_pairs'))
    parameters = None
    if ready:
        weights = [len(eligible)/(len(counts)*counts[r['decision_epoch']]) for r, _ in eligible]
        y = [o['value'] for _, o in eligible]
        parameters = {base: {arm: weighted_ridge([feature_vector(r, base, arm) for r, _ in eligible], y, weights,
                                                contract['layer_fit']['ridge_lambda']) for arm in ARMS} for base in BASES}
    result = {'schema_version': 'forex_magnitude_layer_snapshot.v1', 'scope': list(expected_scope),
              'cutoff_epoch': cutoff, 'status': 'fitted' if ready else 'insufficient_distinct_support',
              'support': support, 'training_membership': membership, 'parameters': parameters,
              'contract_sha256': fingerprint(contract), 'base_models_refitted': False,
              'production_available_epoch': None}
    result['layer_id'] = fingerprint(result)
    return result


def apply_snapshot(row, snapshot, mode):
    if snapshot['layer_id'] != fingerprint({k: v for k, v in snapshot.items() if k != 'layer_id'}):
        raise ValueError('magnitude_layer_snapshot_identity_mismatch')
    validate_row(row, snapshot['scope'])
    if snapshot['cutoff_epoch'] > row['decision_epoch'] or any(x['record_id'] == row['record_id'] for x in snapshot['training_membership']):
        raise ValueError('future_or_current_magnitude_training_example')
    if mode not in ('frozen_prefix', 'expanding_prefix'):
        raise ValueError('unknown_magnitude_layer_mode')
    if snapshot['status'] != 'fitted':
        return []
    result = []
    for base in BASES:
        values = {'zero': 0.0, 'raw_signed': feature_vector(row, base, 'signed_only')[0]}
        for arm in ARMS:
            model = snapshot['parameters'][base][arm]
            x = (np.asarray(feature_vector(row, base, arm))-np.asarray(model['mean']))/np.asarray(model['scale'])
            values[arm] = float(np.r_[1, x]@np.asarray(model['coefficient']))
        if not all(math.isfinite(v) for v in values.values()):
            raise ValueError('nonfinite_magnitude_layer_output')
        result.append({**row, 'base_method': base, 'mode': mode, 'layer_id': snapshot['layer_id'],
                       'layer_cutoff_epoch': snapshot['cutoff_epoch'], 'predictions': values,
                       'outcomes_revealed': False, 'production_available_epoch': None, 'native_policy_admitted': False})
    return result


def metrics(rows):
    if not rows:
        return None
    n = len(rows)
    errors = {method: [r['predictions'][method]-r['outcome_bps'] for r in rows] for method in METRICS}
    scores = {method: {'mae_bps': math.fsum(map(abs, values))/n, 'mse_bps2': math.fsum(v*v for v in values)/n,
                       'bias_bps': math.fsum(values)/n, 'p95_absolute_error_bps': sorted(map(abs, values))[math.ceil(.95*n)-1]}
              for method, values in errors.items()}
    return {'rows': n, 'distinct_origins': len({r['decision_epoch'] for r in rows}),
            'distinct_pairs': len({r['instrument'] for r in rows}), 'support_sha256': fingerprint(sorted(r['record_id'] for r in rows)),
            'methods': scores, 'paired_deltas': {control: {metric: scores['magnitude_interaction'][metric]-scores[control][metric]
              for metric in ('mae_bps', 'mse_bps2', 'bias_bps')} for control in ('zero', 'raw_signed', 'signed_only')}}


def assessed(rows, outcomes, horizon, asof):
    result = []
    for row in rows:
        outcome = label(row, outcomes, horizon)
        if row['available_epoch'] <= asof and outcome['available_epoch'] <= asof and outcome['value'] is not None:
            if not math.isfinite(outcome['value']): raise ValueError('finite_magnitude_assessment_required')
            result.append({**row, 'outcome_bps': outcome['value']})
    return result


def score(rows, outcomes, horizon, asof):
    selected = assessed(rows, outcomes, horizon, asof)
    by_origin, by_day = defaultdict(list), defaultdict(list)
    for row in selected:
        by_origin[row['decision_epoch']].append(row); by_day[row['decision_epoch']//86400].append(row)
    return {'forecast_rows': len(rows), 'mature_rows': len(selected), 'overall': metrics(selected),
            'by_origin': [{'origin_epoch': k, 'metrics': metrics(v)} for k, v in sorted(by_origin.items())],
            'by_utc_day': [{'utc_day': k, 'metrics': metrics(v)} for k, v in sorted(by_day.items())]}


def sensitivity(rows, outcomes, horizon, asof, contract):
    selected = assessed(rows, outcomes, horizon, asof)
    grouped = defaultdict(list)
    for row in selected: grouped[row['decision_epoch']].append(row)
    origins = sorted(grouped); results = []
    for length in contract['block_lengths']:
        reason = ('insufficient_origins' if len(origins) < length else 'single_circular_block_no_resampling_variation'
                  if len(origins) == length else 'irregular_origin_grid' if any(b-a != 21600 for a, b in zip(origins, origins[1:])) else None)
        if reason:
            results.append({'block_length': length, 'status': reason, 'interval': None}); continue
        terms = []
        for origin in origins:
            part = grouped[origin]
            terms.append([len(part), *[math.fsum(abs(r['predictions']['magnitude_interaction']-r['outcome_bps'])-
                 abs(r['predictions'][control]-r['outcome_bps']) for r in part) for control in ('raw_signed', 'signed_only')]])
        rng = random.Random(contract['seed']+length); draws = [[], []]
        for _ in range(contract['draws']):
            sample = []
            while len(sample) < len(origins):
                start = rng.randrange(len(origins)); sample.extend((start+j)%len(origins) for j in range(length))
            sample = sample[:len(origins)]; denominator = sum(terms[i][0] for i in sample)
            for j in range(2): draws[j].append(math.fsum(terms[i][j+1] for i in sample)/denominator)
        intervals, degenerate = {}, []
        for control, values in zip(('raw_signed', 'signed_only'), draws):
            values.sort()
            if values[0] == values[-1]:
                intervals[control] = None; degenerate.append(control)
            else:
                intervals[control] = {'low': values[int(.025*(len(values)-1))], 'high': values[int(.975*(len(values)-1))]}
        result = {'block_length': length, 'status': 'descriptive_sensitivity', 'mae_delta_intervals': intervals,
                  'draws': contract['draws'], 'sampling': 'circular_origin_panels_row_weighted', 'independent_inference': False}
        if degenerate: result['degenerate_controls'] = degenerate
        results.append(result)
    return results
