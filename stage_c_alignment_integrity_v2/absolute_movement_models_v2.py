"""Direct magnitude fits on the original maturity-qualified technical rows."""
import hashlib
import io
import math
from collections import defaultdict

import joblib
import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits

from contracts import TrainingView, fingerprint
from fitted_consumer_v2 import fit_model
from matched_campaign_models_v2 import FEATURES, population, canonicalize_model_strings, validate_fit
from retained_signed_cost_models_v1 import fit_one

METHODS = ('zero_movement', 'past_absolute_mean', 'past_absolute_median',
           'absolute_ridge', 'absolute_hgb', 'abs_signed_ridge', 'abs_signed_hgb')


def target(minutes):
    return {'target_id': f'technical_endpoint_absolute_elapsed_{minutes}m', 'horizon_seconds': minutes * 60}


def transform_outcomes(outcomes, minutes):
    original = f'technical_endpoint_midpoint_elapsed_{minutes}m'
    result, seen = [], set()
    for row in outcomes:
        if row['target_id'] != original:
            continue
        if row['record_id'] in seen:
            raise ValueError('absolute_duplicate_original_outcome')
        seen.add(row['record_id'])
        value = row['value']
        if value is not None and (type(value) not in (int, float) or not math.isfinite(value)):
            raise ValueError('absolute_nonfinite_original_outcome')
        result.append({**row, 'target_id': target(minutes)['target_id'],
                       'value': None if value is None else abs(value),
                       'original_outcome_sha256': fingerprint(row), 'original_target_id': original})
    return result


def fit_pair(observations, outcomes, universe, minutes, cutoff, c, original):
    """Reuse exact fit machinery; preserve changed-target identity explicitly."""
    t = target(minutes)
    # Reject neither the values nor the shape of unrevealed future labels here.
    # Only clocks may decide which outcomes enter the fit-time transformation.
    labels = transform_outcomes([o for o in outcomes if o['available_epoch'] <= cutoff], minutes)
    view = TrainingView(c['training_start'], cutoff, cutoff, cutoff, c['evaluation_asof'])
    with threadpool_limits(limits=1):
        ridge = fit_model(observations, labels, universe=universe, target=t, view=view,
                          ready_epoch=cutoff+c['fit_latency_seconds'], ridge_lambda=c['ridge_lambda'],
                          min_rows=c['minimum_training_rows'])
        if ridge['status'] != 'fitted':
            raise ValueError('absolute_training_support_missing')
        rows = population(observations, labels, t, view)
        signed = population(observations, outcomes, original['target'], view)
        if fingerprint(rows) != ridge['training_population_sha256']:
            raise ValueError('absolute_ridge_tree_population_mismatch')
        if (fingerprint(signed) != original['training_population_sha256'] or
                [r['record_id'] for r, _ in rows] != [r['record_id'] for r, _ in signed]):
            raise ValueError('absolute_original_training_support_mismatch')
        if any(ridge[k] != original['ridge'][k] for k in ('mean', 'scale', 'training_rows', 'training_view')):
            raise ValueError('absolute_original_feature_statistics_mismatch')
        x = pd.DataFrame([r['features'] for r, _ in rows], columns=FEATURES)
        x['epoch'] = [r['origin_epoch'] for r, _ in rows]
        y = np.asarray([o['value'] for _, o in rows])
        tree = fit_one(x, FEATURES, y, c['hgb_parameters'], sample_weights=np.ones(len(rows)))
        canonicalize_model_strings(tree)
        buffer = io.BytesIO()
        joblib.dump(tree, buffer, compress=0, protocol=5)
    tree_bytes = buffer.getvalue()
    meta = {'schema_version': 'forex_absolute_fit_pair.v1', 'target': t, 'fit_cutoff': cutoff,
            'ready_epoch': cutoff+c['fit_latency_seconds'], 'training_view': view.identity(),
            'training_rows': len(rows), 'training_population_sha256': fingerprint(rows),
            'training_record_ids_sha256': fingerprint([r['record_id'] for r, _ in rows]),
            'original_signed_fit_id': original['fit_id'],
            'original_signed_population_sha256': fingerprint(signed),
            'original_metadata_sha256': fingerprint(original),
            'maximum_outcome_available_epoch': max(o['available_epoch'] for _, o in rows),
            'feature_schema_sha256': fingerprint(FEATURES), 'ridge': ridge,
            'history_mean': float(y.mean()), 'history_median': float(np.median(y)),
            'tree_sha256': hashlib.sha256(tree_bytes).hexdigest(), 'tree_parameters': c['hgb_parameters'],
            'equal_original_training_rows_and_feature_statistics': True,
            'projection': 'max(0, raw_prediction)', 'probability_scope': 'not_provided'}
    meta['fit_id'] = fingerprint(meta)
    return meta, tree_bytes


def predict_values(meta, tree_bytes, observations):
    """Feature-only inference. Caller authenticates artifacts before joblib load."""
    validate_fit(meta, tree_bytes)
    ridge = meta['ridge']
    if ridge['model_id'] != fingerprint({k: v for k, v in ridge.items() if k != 'model_id'}):
        raise ValueError('absolute_ridge_identity_mismatch')
    for row in observations:
        if (row['available_epoch'] > row['origin_epoch'] or row['features'] is None or
                len(row['features']) != len(FEATURES) or not np.isfinite(row['features']).all()):
            raise ValueError('absolute_prediction_requires_causal_features')
    if not observations:
        return {}
    x = np.asarray([r['features'] for r in observations])
    tree = joblib.load(io.BytesIO(tree_bytes))
    with threadpool_limits(limits=1):
        r = np.column_stack((np.ones(len(x)), (x-np.asarray(ridge['mean']))/np.asarray(ridge['scale']))) @ np.asarray(ridge['coefficient'])
        h = tree.predict(pd.DataFrame(x, columns=FEATURES))
    if not np.isfinite(r).all() or not np.isfinite(h).all():
        raise ValueError('absolute_nonfinite_prediction')
    return {o['record_id']: {'raw_absolute_ridge': float(rv), 'raw_absolute_hgb': float(hv),
            'zero_movement': 0.0, 'past_absolute_mean': meta['history_mean'],
            'past_absolute_median': meta['history_median'],
            'absolute_ridge': max(0.0, float(rv)), 'absolute_hgb': max(0.0, float(hv))}
            for o, rv, hv in zip(observations, r, h)}


def score(rows, outcomes, minutes, asof):
    """Paired support, explicit missing outcomes, and dependence-aware strata."""
    transformed = transform_outcomes(outcomes, minutes)
    labels = {r['record_id']: r for r in transformed}
    mature, coverage = [], []
    seen = set()
    for r in rows:
        if r['record_id'] in seen:
            raise ValueError('absolute_duplicate_forecast')
        seen.add(r['record_id'])
        y = labels.get(r['record_id'])
        reason = 'missing_outcome' if y is None else 'not_mature' if y['available_epoch'] > asof else 'missing_value' if y['value'] is None else 'eligible'
        coverage.append({'record_id': r['record_id'], 'reason': reason})
        if reason != 'eligible':
            continue
        if y['label_end_epoch'] != r['decision_epoch']+minutes*60 or y['available_epoch'] < y['label_end_epoch'] or r['available_epoch'] > asof:
            raise ValueError('absolute_score_clock_mismatch')
        if set(r['predictions']) != set(METHODS) or any(type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in r['predictions'].values()):
            raise ValueError('absolute_complete_nonnegative_method_predictions_required')
        mature.append((r, y['value']))
    def metrics(part):
        n = len(part)
        support = fingerprint(sorted(r['record_id'] for r, _ in part))
        return [{'method': method, 'rows': n, 'support_sha256': support,
                 'mae_bps': math.fsum(abs(r['predictions'][method]-y) for r, y in part)/n if n else None,
                 'mse_bps2': math.fsum((r['predictions'][method]-y)**2 for r, y in part)/n if n else None}
                for method in METHODS]
    groups = defaultdict(list)
    for row in mature:
        groups['origin', row[0]['decision_epoch']].append(row)
        groups['utc_day', row[0]['decision_epoch']//86400].append(row)
    result = metrics(mature)
    differences = []
    for candidate in ('absolute_ridge', 'absolute_hgb'):
        for control in ('zero_movement', 'past_absolute_mean', 'past_absolute_median', 'abs_signed_ridge', 'abs_signed_hgb'):
            a = next(r for r in result if r['method'] == candidate)
            b = next(r for r in result if r['method'] == control)
            differences.append({'candidate': candidate, 'control': control, 'rows': a['rows'],
                                'mae_delta_bps': None if not a['rows'] else a['mae_bps']-b['mae_bps'],
                                'mse_delta_bps2': None if not a['rows'] else a['mse_bps2']-b['mse_bps2']})
    return {'mature_rows': len(mature), 'metrics': result, 'paired_differences': differences,
            'outcome_coverage': coverage, 'strata': [{'kind': k[0], 'epoch_or_day': k[1], 'metrics': metrics(v)} for k, v in sorted(groups.items())],
            'scope': 'paired_inspected_development_errors; overlapping_horizons_and_shared_currencies; no_confirmation_or_significance_claim'}
