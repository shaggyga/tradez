"""Causal later-frame layer fitting with explicit coverage-matched controls."""
from collections import Counter
import math
import time

from contracts import fingerprint
from magnitude_layer_v2 import fit_snapshot, apply_snapshot, BASES
from later_surface_native_v2 import NativeBatch


def layer_settings(contract):
    parent = contract['layer_contract']
    return {k: parent[k] for k in ('minimum_distinct_origins', 'minimum_distinct_pairs',
        'minimum_distinct_utc_days', 'layer_fit')} | {
        'scope': 'later_remaining_exact_horizon_frozen_bases',
        'parent_layer_contract_sha256': fingerprint(parent), 'surface_contract_sha256': fingerprint(contract)}


def frame(origin, predictor, observations, rows, outcomes, market, contract, trad):
    started = time.monotonic()
    target = contract['common_target_epoch']; horizon = (target-origin)//60
    if origin not in contract['policy_origins'] or horizon not in contract['horizons_minutes']:
        raise ValueError('later_registered_frame_required')
    current = sorted((o for o in observations if o['origin_epoch'] == origin), key=lambda o: o['instrument'])
    if [o['instrument'] for o in current] != market['universe']:
        raise ValueError('later_all68_frame_required')
    # A fresh matrix/inference demonstrates actual current conditioning after the
    # first pass generated the historical prequential training stream.
    values = predictor.predict(horizon, current, origin)
    current_rows = {r['record_id']: r for r in rows if r['decision_epoch'] == origin}
    if set(values) != set(current_rows): raise ValueError('later_fresh_prediction_support_mismatch')
    for rid, v in values.items():
        row = current_rows[rid]
        if (v['signed']['ridge'] != row['ridge_prediction_bps'] or
            v['signed']['recovered_hgb'] != row['recovered_hgb_prediction_bps'] or
            any(max(0., v['absolute'][base]) != row['magnitude_prediction_bps'][base] for base in BASES)):
            raise ValueError('later_fresh_saved_model_prediction_mismatch')
    scope = ['legacy26', f'technical_endpoint_midpoint_elapsed_{horizon}m', 'frozen']
    snapshots = {}
    for mode in ('frozen', 'expanding'):
        cutoff = contract['layer_contract']['frozen_cutoff'] if mode == 'frozen' else origin
        if cutoff > origin:
            snapshots[mode] = None
        else:
            snapshots[mode] = fit_snapshot(rows, outcomes, cutoff, scope, horizon, layer_settings(contract))
    fit_elapsed = time.monotonic()-started
    if fit_elapsed > contract['resources']['layer_fit_seconds']:
        raise ValueError(f'later_fresh_inference_and_layer_fit_reservation_exceeded:{fit_elapsed:.6f}')
    references = {r['instrument']: r for r in market['rows'] if r['price_epoch'] == origin}
    predictions, packets, coverage = [], [], []
    native = NativeBatch(trad)
    for obs in current:
        rid = obs['record_id']; pair = obs['instrument']; row = current_rows.get(rid)
        applications = {mode: {r['base_method']: r for r in apply_snapshot(row, snap, mode+'_prefix')}
            for mode, snap in snapshots.items() if row is not None and snap is not None and snap['status'] == 'fitted'}
        for base in BASES:
            for variant in contract['variants']:
                mode = None if variant == 'raw_unrestricted' else variant.rsplit('_', 1)[1]
                snapshot = snapshots.get(mode)
                reason = ('base_unavailable' if row is None else
                    'layer_phase_not_started' if mode is not None and snapshot is None else
                    'insufficient_distinct_support' if mode is not None and snapshot['status'] != 'fitted' else
                    'reference_'+references[pair]['status'] if references[pair]['status'] != 'valid_candle_close_pair' else 'eligible')
                cov = {'record_id': rid, 'instrument': pair, 'origin_epoch': origin, 'target_epoch': target,
                       'horizon_minutes': horizon, 'base_method': base, 'variant': variant, 'reason': reason,
                       'eligibility_snapshot_id': snapshot['layer_id'] if snapshot else None}
                coverage.append(cov)
                if reason != 'eligible': continue
                raw = variant.startswith('raw_')
                arm = None if raw else variant.rsplit('_', 1)[0]
                value = values[rid]['signed'][base] if raw else applications[mode][base]['predictions'][arm]
                model_id = row['base_model_ids'][base] if raw else fingerprint({'layer': snapshot['layer_id'], 'base': base, 'arm': arm})
                pred = {'record_id': rid, 'instrument': pair, 'decision_epoch': origin, 'target_epoch': target,
                    'target_id': scope[1], 'available_epoch': origin+2, 'base_method': base, 'variant': variant,
                    'prediction_bps': value, 'absolute_prediction_bps': row['magnitude_prediction_bps'][base],
                    'model_id': model_id, 'model_ready_epoch': predictor.available_epoch if raw else origin+1,
                    'computation_started_epoch': origin if raw else origin+1,
                    'signed_fit_id': row['selected_fit_id'], 'absolute_fit_id': row['absolute_fit_id'],
                    'base_prediction_sha256': fingerprint(row), 'observation_sha256': fingerprint(obs),
                    'eligibility_snapshot_id': cov['eligibility_snapshot_id'],
                    'production_available_epoch': None, 'observed_publication': False}
                pred['forecast_id'] = fingerprint(pred)
                predictions.append(pred)
                packets.append(native.prepare(pred, obs, references[pair], market['metadata'][pair], contract))
    native.verify()
    elapsed = time.monotonic()-started
    if elapsed > contract['resources']['native_build_seconds']:
        raise ValueError(f'later_complete_selected_target_preparation_reservation_exceeded:{elapsed:.6f}')
    return {'origin_epoch': origin, 'horizon_minutes': horizon, 'snapshots': snapshots,
            'predictions': predictions, 'coverage': coverage, 'packets': packets}, {
            'origin_epoch': origin, 'fresh_inference_and_layer_fit_seconds': fit_elapsed,
            'complete_selected_target_preparation_seconds': elapsed,
            'snapshot_attempts': sum(s is not None for s in snapshots.values()),
            'regression_fits': 4*sum(s is not None and s['status'] == 'fitted' for s in snapshots.values()),
            'scope': 'fresh_current_base_inference_layer_fit_application_native_preparation; no_consumer_projection'}


def assess(frames, outcomes, asof):
    groups = {}; coverage = Counter()
    for item in frames:
        coverage.update(r['reason'] for r in item['coverage'])
        for r in item['predictions']:
            o = outcomes[r['record_id'], r['target_id']]
            if o['label_end_epoch'] != r['target_epoch'] or o['available_epoch'] < o['label_end_epoch']:
                raise ValueError('later_assessment_exact_target_required')
            key = (r['base_method'], r['variant'], item['horizon_minutes'])
            group = groups.setdefault(key, {'predictions': 0, 'errors': [], 'record_ids': []})
            group['predictions'] += 1
            if o['available_epoch'] <= asof and o['value'] is not None:
                if not math.isfinite(o['value']): raise ValueError('later_nonfinite_mature_outcome')
                group['errors'].append(r['prediction_bps']-o['value']); group['record_ids'].append(r['record_id'])
    scores = []
    for key, g in sorted(groups.items()):
        e = g['errors']; n = len(e)
        scores.append({'base_method': key[0], 'variant': key[1], 'horizon_minutes': key[2],
            'prediction_rows': g['predictions'], 'mature_rows': n, 'support_sha256': fingerprint(sorted(g['record_ids'])),
            'mae_bps': math.fsum(map(abs, e))/n if n else None,
            'mse_bps2': math.fsum(x*x for x in e)/n if n else None,
            'bias_bps': math.fsum(e)/n if n else None})
    return {'scores': scores, 'coverage_reasons': dict(coverage), 'portfolio_replay': False, 'confirmation': False}
