"""Durable new-target training, saved-weight recovery and matched forecast replay."""
from collections import Counter
import json
import math
import os
from pathlib import Path
import time
import uuid

from absolute_movement_models_v2 import fit_pair, predict_values, score, METHODS
from campaign_inspector_v2 import CampaignReader
from contracts import fingerprint
from forecast_blend_v2 import build_chunk
from forecast_blend_runner_v2 import encoded, name, check_resources
from matched_campaign_models_v2 import validate_fit
from publication import RunPublisher, effective_run_identity, verify_completed_run


def fit_names(recipe):
    c = recipe['contract']['experiment']
    return [f'fit_{h}_{t}' for h in c['horizon_minutes'] for t in c['fit_cutoffs']]


def required(recipe):
    c = recipe['contract']['experiment']
    return sorted([n+s for n in fit_names(recipe) for s in ('.json', '.joblib', '.timing.json')] +
                  [prefix+name(h, p) for h in c['horizon_minutes'] for p in c['procedures']
                   for prefix in ('predictions_', 'coverage_', 'assessment_')] +
                  ['run_report.json', 'source_references.json', 'resource_receipts.json', 'prediction_resources.json'])


def scientific_names(recipe):
    return [n for n in required(recipe) if n not in ('resource_receipts.json', 'prediction_resources.json') and not n.endswith('.timing.json')]


def identity_for(recipe):
    return effective_run_identity(contract={'recipe': recipe, 'required_payloads': required(recipe)},
        dependency_hashes={**recipe['sources'], **{k: v['identity']['fingerprint'] for k, v in recipe['dependencies'].items()},
                           **{'original_metadata/'+k: v for k, v in recipe['original_metadata']['payloads'].items()}})


def validate_fit_timing(meta, row, recipe):
    if row['fit_id'] != meta['fit_id'] or not math.isfinite(row['elapsed_seconds']) or not 0 <= row['elapsed_seconds'] <= recipe['contract']['resources']['fit_pair_seconds']:
        raise ValueError('absolute_fit_timing_required')


def validate_completed(root, recipe):
    r = json.loads((root/'run_report.json').read_bytes())
    if r['fit_pairs'] != 14 or r['estimator_inventory'] != 28 or r['chunks'] != 14 or r['coverage_rows'] != 19040 or r['score_groups'] != 98:
        raise ValueError('absolute_full_declared_scope_required')
    if r['original_models_fitted'] != 0 or r['api_calls'] != 0 or r['engineering_ready'] is not False:
        raise ValueError('absolute_scope_or_promotion_violation')
    for n in fit_names(recipe):
        meta = json.loads((root/(n+'.json')).read_bytes())
        validate_fit(meta, (root/(n+'.joblib')).read_bytes())
        validate_fit_timing(meta, json.loads((root/(n+'.timing.json')).read_bytes()), recipe)
    resources = json.loads((root/'resource_receipts.json').read_bytes())['observations']
    limits = recipe['configuration']
    if not resources or resources[-1]['phase'] != 'final_precommit':
        raise ValueError('absolute_final_resource_receipt_required')
    for row in resources:
        if not (0 <= row['elapsed_seconds'] <= limits['main_wall_seconds'] and
                0 <= row['aggregate_rss_bytes'] <= limits['max_rss_bytes'] and
                0 <= row['run_disk_bytes'] <= limits['max_scratch_bytes']):
            raise ValueError('absolute_resource_receipt_limit')
    batches = json.loads((root/'prediction_resources.json').read_bytes())
    if len(batches) != 280 or len({r['batch_id'] for r in batches}) != 280 or any(not 0 <= r['elapsed_seconds'] <= 2 for r in batches):
        raise ValueError('absolute_prediction_reservation_evidence_required')


def run(paths, recipe, runs, *, resume=False, crash_after=None, reuse_only=False):
    identity = identity_for(recipe)
    publisher = RunPublisher(runs, recipe['run_id'], identity)
    if (publisher.root/'COMPLETION_MANIFEST.json').exists():
        verify_completed_run(publisher.root, identity)
        return {'actual_new_estimator_fits': 0, 'reason': 'completed_run_reused'}
    publisher.acquire(recover=resume)
    started, attempt = time.monotonic(), uuid.uuid4().hex
    fitted = 0
    def guard(phase):
        row = {**check_resources(publisher.root, started, recipe['configuration'], phase), 'attempt_id': attempt}
        with (publisher.root/'RESOURCE_ATTEMPTS.jsonl').open('ab') as f:
            f.write(encoded(row)); f.flush(); os.fsync(f.fileno())
    try:
        guard('before_inputs')
        reader = CampaignReader({k: paths[k] for k in recipe['dependencies']}, recipe['dependencies'])
        universe = recipe['configuration']['universe']
        c = recipe['contract']['experiment']
        observations, outcomes = [], []
        for pair in universe:
            part = reader.read('technical', 'pair_'+pair+'.json')
            observations.extend(part['observations']); outcomes.extend(part['outcomes'])
        by_id = {r['record_id']: r for r in observations}
        if len(by_id) != len(observations):
            raise ValueError('absolute_unique_original_observations_required')
        outcome_lookup = {(r['record_id'], r['target_id']): r for r in outcomes}
        if len(outcome_lookup) != len(outcomes):
            raise ValueError('absolute_unique_original_outcomes_required')
        for origin in c['decision_epochs']:
            if sorted(r['instrument'] for r in observations if r['origin_epoch'] == origin) != universe:
                raise ValueError('absolute_all68_original_origins_required')
        payloads, fits = [], {}
        guard('after_inputs')
        index = 0
        for h in c['horizon_minutes']:
            for cutoff in c['fit_cutoffs']:
                n = f'fit_{h}_{cutoff}'
                raw, tree, timing_raw = [publisher.read_verified_payload(n+s) for s in ('.json', '.joblib', '.timing.json')]
                original = json.loads((Path(paths['baseline_metadata'])/(n+'.json')).read_bytes())
                if raw is None or tree is None or timing_raw is None:
                    if reuse_only:
                        raise ValueError('absolute_reuse_only_missing_saved_fit')
                    before = time.monotonic()
                    meta, tree = fit_pair(observations, outcomes, universe, h, cutoff, c, original)
                    elapsed = time.monotonic()-before
                    timing = {'fit_id': meta['fit_id'], 'elapsed_seconds': elapsed, 'limit_seconds': 30,
                              'scope': 'absolute_transform_population_parity_ridge_tree_fit_serialization', 'new_estimators': 2}
                    validate_fit_timing(meta, timing, recipe)
                    raw, timing_raw = encoded(meta), encoded(timing)
                    fitted += 2
                    with (publisher.root/'FIT_ATTEMPTS.jsonl').open('ab') as f:
                        f.write(encoded({'attempt_id': attempt, **timing})); f.flush(); os.fsync(f.fileno())
                meta = json.loads(raw)
                validate_fit(meta, tree); validate_fit_timing(meta, json.loads(timing_raw), recipe)
                if meta['original_signed_fit_id'] != original['fit_id'] or meta['original_metadata_sha256'] != fingerprint(original):
                    raise ValueError('absolute_saved_fit_original_lineage_mismatch')
                for suffix, data in (('.json', raw), ('.joblib', tree), ('.timing.json', timing_raw)):
                    payloads.append(publisher.write_or_validate_payload(n+suffix, data))
                fits[h, cutoff] = meta, tree
                index += 1; guard('after_fit_'+str(index))
                if crash_after == index: os._exit(91)
        counts = Counter(); statuses = Counter(); timing_rows = []
        for h in c['horizon_minutes']:
            for procedure in c['procedures']:
                suffix = name(h, procedure)
                base_rows, cov = build_chunk(reader.read('joint', 'forecasts_legacy26_'+suffix),
                    reader.read('joint', 'coverage_legacy26_'+suffix), outcome_lookup,
                    {'group': 'legacy26', 'horizon_minutes': h, 'procedure': procedure})
                predictions = []
                for origin in c['decision_epochs']:
                    rows = [r for r in base_rows if r['decision_epoch'] == origin]
                    before = time.monotonic()
                    if rows:
                        cutoffs = {r['selected_fit_cutoff'] for r in rows}
                        if len(cutoffs) != 1: raise ValueError('absolute_unique_scheduled_fit_required')
                        meta, tree = fits[h, next(iter(cutoffs))]
                        current = [by_id[r['record_id']] for r in rows]
                        values = predict_values(meta, tree, current)
                        for row in rows:
                            if row['selected_fit_id'] != meta['original_signed_fit_id']:
                                raise ValueError('absolute_scheduled_original_fit_mismatch')
                            ready = row['base_model_ready_epochs']['ridge']
                            if meta['ready_epoch'] > ready or ready > origin or row['available_epoch'] < origin:
                                raise ValueError('absolute_replacement_schedule_clock_mismatch')
                            v = values[row['record_id']]
                            raw = {k: v.pop(k) for k in ('raw_absolute_ridge', 'raw_absolute_hgb')}
                            v.update(abs_signed_ridge=abs(row['ridge_prediction_bps']), abs_signed_hgb=abs(row['recovered_hgb_prediction_bps']))
                            record = {'record_id': row['record_id'], 'instrument': row['instrument'], 'decision_epoch': origin,
                                      'available_epoch': row['available_epoch'], 'target_id': meta['target']['target_id'],
                                      'procedure': procedure, 'fit_id': meta['fit_id'], 'selected_fit_cutoff': meta['fit_cutoff'],
                                      'scheduled_model_ready_epoch': ready, 'predictions': v, 'raw_direct_predictions': raw,
                                      'signed_control_records': row['source_records'], 'original_signed_fit_id': row['selected_fit_id'],
                                      'production_available_epoch': None, 'native_policy_admitted': False,
                                      'clock_scope': 'counterfactual_replacement_of_original_legacy26_slots'}
                            record['forecast_id'] = fingerprint(record)
                            predictions.append(record)
                    elapsed = time.monotonic()-before
                    if elapsed > recipe['contract']['resources']['prediction_batch_seconds']:
                        raise ValueError('absolute_prediction_slot_exceeded')
                    timing_rows.append({'batch_id': fingerprint([h, procedure, origin]), 'horizon_minutes': h,
                                        'procedure': procedure, 'origin_epoch': origin, 'rows': len(rows),
                                        'elapsed_seconds': elapsed, 'limit_seconds': 2,
                                        'scope': 'saved_model_validate_load_predict_and_lineage; prepared_inputs_preloaded'})
                assessment = score(predictions, outcomes, h, c['evaluation_asof'])
                coverage = [{**r, 'original_target_id': r['target_id'], 'target_id': f'technical_endpoint_absolute_elapsed_{h}m',
                             'methods': list(METHODS), 'absolute_available': r['blend_available'],
                             'production_available_epoch': None} for r in cov]
                for prefix, obj in (('predictions_', predictions), ('coverage_', coverage), ('assessment_', assessment)):
                    payloads.append(publisher.write_or_validate_payload(prefix+suffix, encoded(obj)))
                counts['chunks'] += 1; counts['coverage_rows'] += len(coverage)
                counts['prediction_rows'] += len(predictions); counts['mature_rows'] += assessment['mature_rows']
                counts['score_groups'] += len(assessment['metrics'])
                for row in predictions:
                    for k, v in row['raw_direct_predictions'].items():
                        counts[k+'_clipped'] += v < 0
                statuses.update(r['reason'] for r in coverage)
                index += 1; guard('after_chunk_'+str(counts['chunks']))
                if crash_after == index: os._exit(91)
        report = {'status': 'completed_absolute_movement_development_comparison', **dict(counts),
                  'coverage_statuses': dict(statuses), 'fit_pairs': len(fits), 'estimator_inventory': len(fits)*2,
                  'original_models_fitted': 0, 'api_calls': 0, 'selected_model': None,
                  'independent_review': False, **recipe['contract']['readiness'],
                  'limitations': ['short_previously_inspected_development', 'absolute_endpoint_not_path_movement',
                                  'hypothetical_replacement_schedule_not_historical_issuance', 'prepared_inputs_assumed_ready',
                                  'no_direction_probability_execution_or_profit_inference'],
                  'actual_fits_this_attempt': 'recorded_separately_in_attempt_receipt; inventory_not_retraining_count'}
        refs = {'dependencies': recipe['dependencies'], 'original_metadata': recipe['original_metadata'],
                'original_models_refitted': False, 'target_transform': recipe['contract']['target'],
                'readiness_schedule': reader.read('joint', 'schedule.json')['schedule_id']}
        for n, obj in (('run_report.json', report), ('source_references.json', refs)):
            payloads.append(publisher.write_or_validate_payload(n, encoded(obj)))
        old = publisher.read_verified_payload('prediction_resources.json')
        payloads.append(publisher.write_or_validate_payload('prediction_resources.json', old or encoded(timing_rows)))
        if crash_after == 0: os._exit(91)
        guard('final_precommit')
        receipts = {'scope': 'phase_boundary_samples_not_continuous_OS_quota',
                    'observations': [json.loads(line) for line in (publisher.root/'RESOURCE_ATTEMPTS.jsonl').read_text().splitlines()]}
        old = publisher.read_verified_payload('resource_receipts.json')
        payloads.append(publisher.write_or_validate_payload('resource_receipts.json', old or encoded(receipts)))
        validate_completed(publisher.root, recipe)
        check_resources(publisher.root, started, recipe['configuration'], 'before_manifest')
        publisher.complete(payloads, set(required(recipe)))
        result = {'attempt_id': attempt, 'actual_new_estimator_fits': fitted,
                  'saved_fit_pairs_reused': len(fits)-fitted//2, 'reuse_only': reuse_only,
                  'elapsed_seconds': time.monotonic()-started, 'original_estimators_refitted': 0}
        (publisher.root/('ATTEMPT_'+attempt+'.json')).write_bytes(encoded(result))
        return result
    except BaseException:
        if publisher._owner_token is not None: publisher.release()
        raise
