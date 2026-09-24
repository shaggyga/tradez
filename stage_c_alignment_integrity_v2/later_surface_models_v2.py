"""Frozen saved-model inference and conservative single-worker readiness."""
import io
import math
import time

import joblib
import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits

from contracts import TrainingView, fingerprint, forecast_record
from matched_campaign_models_v2 import FEATURES, validate_fit
from forecast_blend_v2 import build_chunk


def schedule(contract):
    windows = [(t, t+contract['prequential_schedule']['prediction_window_seconds']) for t in contract['prequential_origins']]
    cursor = contract['fit_cutoff']; tasks = []
    for family in ('signed', 'absolute'):
        for horizon in contract['horizons_minutes']:
            for start, end in windows:
                if cursor < end and cursor+30 > start: cursor = end
            tasks.append({'family': family, 'horizon_minutes': horizon, 'start_epoch': cursor, 'ready_epoch': cursor+30})
            cursor += 30
    prewarm = {'start_epoch': cursor, 'finish_epoch': cursor+120}
    if any(a < prewarm['finish_epoch'] and b > prewarm['start_epoch'] for a, b in windows):
        raise ValueError('later_prewarm_prediction_reservation_overlap')
    result = {'fit_tasks': tasks, 'prewarm': prewarm, 'prediction_windows': [list(x) for x in windows],
              'scope': 'counterfactual_single_worker_frozen_fits_and_measured_replay; no_historical_issuance'}
    result['schedule_id'] = fingerprint(result); return result


def horizon_order(origin, contract):
    horizons = contract['horizons_minutes']
    if origin not in contract['policy_origins']: return list(horizons)
    selected = (contract['common_target_epoch']-origin)//60
    if selected not in horizons: raise ValueError('later_exact_remaining_horizon_required')
    return [selected]+[h for h in horizons if h != selected]


class SavedModels:
    """Cache immutable fitted state only; construct every origin matrix afresh."""
    def __init__(self, artifacts, contract, reservations):
        self.models = {}; self.contract = contract; self.reservations = reservations
        self.available_epoch = reservations['prewarm']['finish_epoch']; self.loaded = 0
        start = time.monotonic()
        # Discover CPU topology during the declared prewarm, never on first use
        # inside a publication slot. This caches runtime metadata, not forecasts.
        joblib.cpu_count(only_physical_cores=True)
        expected = {(family, h) for family in ('signed', 'absolute') for h in contract['horizons_minutes']}
        if set(artifacts) != expected: raise ValueError('later_exact_saved_model_inventory_required')
        for (family, h), (meta, tree_bytes) in sorted(artifacts.items()):
            validate_fit(meta, tree_bytes)
            kind = 'midpoint' if family == 'signed' else 'absolute'
            if meta['target'] != {'target_id': f'technical_endpoint_{kind}_elapsed_{h}m', 'horizon_seconds': h*60}:
                raise ValueError('later_saved_model_target_mismatch')
            if meta['fit_cutoff'] != contract['fit_cutoff'] or meta['ready_epoch'] > reservations['prewarm']['start_epoch']:
                raise ValueError('later_model_not_ready_for_prewarm')
            if meta['feature_schema_sha256'] != fingerprint(FEATURES): raise ValueError('later_feature_schema_mismatch')
            ridge = meta['ridge']
            if ridge['model_id'] != fingerprint({k: v for k, v in ridge.items() if k != 'model_id'}):
                raise ValueError('later_saved_ridge_identity_mismatch')
            self.models[family, h] = (meta, joblib.load(io.BytesIO(tree_bytes)))
            self.loaded += 1
            if self.loaded > contract['resources']['binary_model_load_cap']: raise ValueError('later_load_cap')
        elapsed = time.monotonic()-start
        if elapsed > contract['resources']['prewarm_seconds']: raise ValueError('later_prewarm_limit')
        self.receipt = {'binary_models_loaded': self.loaded, 'elapsed_seconds': elapsed,
                        **reservations['prewarm'], 'scope': 'authenticated_fixed_weights_only_no_origin_feature_cache'}

    def predict(self, horizon, observations, origin):
        if origin < self.available_epoch: return {}
        valid = []
        for row in observations:
            if row['origin_epoch'] != origin: raise ValueError('later_exact_current_observation_required')
            if row['available_epoch'] > origin or row['features'] is None: continue
            if len(row['features']) != len(FEATURES) or not np.isfinite(row['features']).all():
                raise ValueError('later_finite_fixed_features_required')
            valid.append(row)
        if not valid: return {}
        x = np.asarray([r['features'] for r in valid]); result = {r['record_id']: {} for r in valid}
        with threadpool_limits(limits=1):
            for family in ('signed', 'absolute'):
                meta, tree = self.models[family, horizon]; ridge = meta['ridge']
                r = np.column_stack((np.ones(len(x)), (x-np.asarray(ridge['mean']))/np.asarray(ridge['scale']))) @ np.asarray(ridge['coefficient'])
                t = tree.predict(pd.DataFrame(x, columns=FEATURES))
                if not np.isfinite(r).all() or not np.isfinite(t).all(): raise ValueError('later_nonfinite_prediction')
                for row, rv, tv in zip(valid, r, t):
                    result[row['record_id']][family] = {'ridge': float(rv), 'recovered_hgb': float(tv)}
        return result


def prequential_chunk(predictor, observations, outcomes, horizon, contract):
    meta = predictor.models['signed', horizon][0]; absolute = predictor.models['absolute', horizon][0]
    wrappers, coverage, magnitudes, timing = [], [], [], []
    fields = {k: v for k, v in meta['training_view'].items() if k != 'schema'}
    for origin in contract['prequential_origins']:
        current = sorted((r for r in observations if r['origin_epoch'] == origin), key=lambda r: r['instrument'])
        available = origin+2*(horizon_order(origin, contract).index(horizon)+1)
        began = time.monotonic(); values = predictor.predict(horizon, current, origin)
        for row in current:
            feature_reason = 'missing_features' if row['features'] is None else 'feature_not_ready' if row['available_epoch'] > origin else 'eligible'
            ready = origin >= predictor.available_epoch
            reason = feature_reason if ready else 'joint_model_not_ready'
            sources = {}
            for method in ('ridge', 'recovered_hgb'):
                coverage.append({'record_id': row['record_id'], 'instrument': row['instrument'], 'decision_epoch': origin,
                    'target_id': meta['target']['target_id'], 'group': 'legacy26', 'method': method, 'procedure': 'frozen',
                    'reason': reason, 'feature_reason': feature_reason, 'selected_fit_cutoff': meta['fit_cutoff'] if ready else None,
                    'joint_model_ready_epoch': predictor.available_epoch if ready else None, 'reserved_publication_epoch': available})
                if reason != 'eligible': continue
                model_id = fingerprint({'fit_id': meta['fit_id'], 'method': method})
                f = forecast_record(forecast_id=fingerprint({'model': model_id, 'observation': row, 'schedule': predictor.reservations['schedule_id'], 'available': available}),
                    instrument=row['instrument'], decision_epoch=origin, available_epoch=available, model_id=model_id,
                    model_ready_epoch=predictor.available_epoch, training_view=TrainingView(**fields),
                    target_id=meta['target']['target_id'], prediction=values[row['record_id']]['signed'][method])
                wrapped = {'record_id': row['record_id'], 'group': 'legacy26', 'method': method, 'procedure': 'frozen',
                           'forecast': f, 'selected_fit_id': meta['fit_id'], 'selected_fit_cutoff': meta['fit_cutoff'],
                           'source_reference': {'saved_fit_id': meta['fit_id'], 'observation_sha256': fingerprint(row)}}
                wrappers.append(wrapped); sources[method] = fingerprint(wrapped)
            if reason != 'eligible': continue
            v = values[row['record_id']]
            a = {'record_id': row['record_id'], 'instrument': row['instrument'], 'decision_epoch': origin,
                 'available_epoch': available, 'procedure': 'frozen', 'selected_fit_cutoff': meta['fit_cutoff'],
                 'target_id': absolute['target']['target_id'], 'original_signed_fit_id': meta['fit_id'],
                 'signed_control_records': sources, 'scheduled_model_ready_epoch': predictor.available_epoch,
                 'fit_id': absolute['fit_id'], 'predictions': {'absolute_ridge': max(0., v['absolute']['ridge']),
                    'absolute_hgb': max(0., v['absolute']['recovered_hgb'])}, 'raw_predictions': v['absolute'],
                 'observation_sha256': fingerprint(row), 'production_available_epoch': None}
            a['forecast_id'] = fingerprint(a); magnitudes.append(a)
        elapsed = time.monotonic()-began
        if elapsed > 2: raise ValueError('later_prequential_horizon_slot_exceeded')
        timing.append({'origin_epoch': origin, 'horizon_minutes': horizon, 'elapsed_seconds': elapsed,
                       'reserved_publication_epoch': available, 'eligible_pairs': len(values), 'limit_seconds': 2})
    bases, cov = build_chunk(wrappers, coverage, outcomes, {'group': 'legacy26', 'horizon_minutes': horizon, 'procedure': 'frozen'})
    return {'horizon_minutes': horizon, 'signed_rows': wrappers, 'absolute_rows': magnitudes, 'base_rows': bases, 'coverage': cov}, timing
