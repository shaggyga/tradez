"""Resumable later-target surface, saved weights and original native consumer."""
from collections import Counter
import json
import math
import os
from pathlib import Path
import time
import uuid

from absolute_movement_models_v2 import fit_pair
from campaign_inspector_v2 import CampaignReader
from contracts import fingerprint
from forecast_blend_runner_v2 import encoded, check_resources
from later_surface_models_v2 import SavedModels, schedule, prequential_chunk
from later_surface_layer_v2 import frame, assess
from later_surface_native_v2 import market_inputs, consume_verified
from magnitude_layer_v2 import join_predictions
from matched_campaign_models_v2 import validate_fit
from publication import RunPublisher, effective_run_identity, verify_completed_run


def required(recipe):
    c = recipe['contract']
    return sorted([f'absolute_{h}'+s for h in c['new_absolute_horizons'] for s in ('.json', '.joblib', '.timing.json')] +
        [f'prequential_{h}.json' for h in c['horizons_minutes']] +
        [f'frame_{t}.json' for t in c['policy_origins']] +
        ['schedule.json', 'market.json', 'assessment.json', 'consumer.json', 'run_report.json',
         'source_references.json', 'resource_receipts.json', 'timing_receipts.json'])


def scientific_names(recipe):
    return [n for n in required(recipe) if n not in ('resource_receipts.json', 'timing_receipts.json') and not n.endswith('.timing.json')]


def identity_for(recipe):
    return effective_run_identity(contract={'recipe': recipe, 'required_payloads': required(recipe)},
        dependency_hashes={**recipe['sources'], **{k: v['identity']['fingerprint'] for k, v in recipe['dependencies'].items()}})


def inputs(paths, recipe):
    reader = CampaignReader({k: paths[k] for k in recipe['dependencies']}, recipe['dependencies'])
    observations, outcomes = [], []
    for pair in recipe['configuration']['universe']:
        part = reader.read('technical', 'pair_'+pair+'.json')
        observations.extend(part['observations']); outcomes.extend(part['outcomes'])
    outcomes.extend(reader.read('remaining', 'remaining_outcomes.json'))
    lookup = {(o['record_id'], o['target_id']): o for o in outcomes}
    if len(lookup) != len(outcomes) or len({o['record_id'] for o in observations}) != len(observations):
        raise ValueError('later_unique_original_records_required')
    for t in recipe['contract']['prequential_origins']:
        if sorted(o['instrument'] for o in observations if o['origin_epoch'] == t) != recipe['configuration']['universe']:
            raise ValueError('later_all68_original_observations_required')
    return reader, observations, outcomes, lookup


def binary(reader, alias, name):
    import hashlib
    raw = (reader.paths[alias]/name).read_bytes()
    if hashlib.sha256(raw).hexdigest() != reader.dependencies[alias]['payloads'][name]:
        raise ValueError('later_original_saved_binary_changed')
    return raw


def validate_completed(root, recipe):
    read = lambda n: json.loads((root/n).read_bytes())
    report = read('run_report.json'); c = recipe['contract']
    if report['coverage_slots'] != 68*8*14 or report['new_absolute_fit_inventory'] != 5 or report['signed_refits'] != 0 or report['api_calls'] != 0:
        raise ValueError('later_full_declared_scope_required')
    for h in c['new_absolute_horizons']:
        meta = read(f'absolute_{h}.json'); validate_fit(meta, (root/f'absolute_{h}.joblib').read_bytes())
        row = read(f'absolute_{h}.timing.json')
        if row['fit_id'] != meta['fit_id'] or not 0 <= row['elapsed_seconds'] <= c['resources']['fit_pair_seconds']:
            raise ValueError('later_saved_fit_timing_invalid')
    receipts = read('timing_receipts.json')
    if receipts['prewarm']['binary_models_loaded'] != 16 or receipts['prewarm']['elapsed_seconds'] > c['resources']['prewarm_seconds']:
        raise ValueError('later_prewarm_evidence_required')
    if len(receipts['prequential']) != 160 or len(receipts['frames']) != 8:
        raise ValueError('later_full_timing_receipts_required')
    if any(not 0 <= r['elapsed_seconds'] <= 2 for r in receipts['prequential']):
        raise ValueError('later_prediction_slot_receipt_invalid')
    for r in receipts['frames']:
        if not (0 <= r['fresh_inference_and_layer_fit_seconds'] <= 1 and 0 <= r['complete_selected_target_preparation_seconds'] <= 2):
            raise ValueError('later_native_slot_receipt_invalid')
    if sum(r['snapshot_attempts'] for r in receipts['frames']) > 16 or sum(r['regression_fits'] for r in receipts['frames']) > 64:
        raise ValueError('later_layer_budget_exceeded')
    resources = read('resource_receipts.json')['observations']
    if not resources or resources[-1]['phase'] != 'final_precommit': raise ValueError('later_final_resource_receipt_required')
    for r in resources:
        if not (0 <= r['elapsed_seconds'] <= c['resources']['main_wall_seconds'] and
                0 <= r['aggregate_rss_bytes'] <= c['resources']['max_rss_bytes'] and
                0 <= r['run_disk_bytes'] <= c['resources']['max_scratch_bytes']):
            raise ValueError('later_resource_receipt_invalid')
    for origin in c['policy_origins']:
        item = read(f'frame_{origin}.json')
        if len(item['coverage']) != 952 or len(item['packets']) != len(item['predictions']):
            raise ValueError('later_complete_frame_inventory_required')
        cov = {(r['instrument'], r['base_method'], r['variant']): r for r in item['coverage']}
        if len(cov) != 952: raise ValueError('later_unique_all68_frame_coverage_required')
        for pair in recipe['configuration']['universe']:
            for base in ('ridge', 'recovered_hgb'):
                for mode in ('frozen', 'expanding'):
                    controls = [cov[pair, base, v+'_'+mode] for v in ('raw_matched', 'signed_only', 'magnitude_interaction')]
                    if len({(r['reason'], r['eligibility_snapshot_id']) for r in controls}) != 1:
                        raise ValueError('later_matched_layer_control_coverage_required')


def run(paths, recipe, runs, *, resume=False, crash_after=None, reuse_only=False):
    publisher = RunPublisher(runs, recipe['run_id'], identity_for(recipe))
    if (publisher.root/'COMPLETION_MANIFEST.json').exists():
        verify_completed_run(publisher.root, identity_for(recipe)); validate_completed(publisher.root, recipe)
        return {'actual_new_estimator_fits': 0, 'reason': 'completed_run_reused'}
    publisher.acquire(recover=resume)
    started = time.monotonic(); attempt = uuid.uuid4().hex; fitted = 0; payloads = []
    def guard(phase):
        row = {**check_resources(publisher.root, started, recipe['configuration'], phase), 'attempt_id': attempt}
        with (publisher.root/'RESOURCE_ATTEMPTS.jsonl').open('ab') as f:
            f.write(encoded(row)); f.flush(); os.fsync(f.fileno())
    def put(name, obj):
        payloads.append(publisher.write_or_validate_payload(name, encoded(obj)))
    try:
        guard('before_inputs'); reader, observations, outcomes, lookup = inputs(paths, recipe)
        c = recipe['contract']; artifacts = {}; newfit_times = []
        for h in c['horizons_minutes']:
            meta = reader.read('remaining', f'fit_{h}.json'); tree = binary(reader, 'remaining', f'fit_{h}.joblib')
            artifacts['signed', h] = meta, tree
        for index, h in enumerate(c['new_absolute_horizons'], 1):
            stem = f'absolute_{h}'
            raw, tree, timing_raw = [publisher.read_verified_payload(stem+s) for s in ('.json', '.joblib', '.timing.json')]
            if raw is None or tree is None or timing_raw is None:
                if reuse_only: raise ValueError('later_reuse_only_missing_saved_fit')
                before = time.monotonic()
                meta, tree = fit_pair(observations, outcomes, recipe['configuration']['universe'], h, c['fit_cutoff'],
                    c['original_experiment'], artifacts['signed', h][0])
                elapsed = time.monotonic()-before
                if elapsed > c['resources']['fit_pair_seconds']: raise ValueError('later_new_fit_reservation_exceeded')
                timing = {'fit_id': meta['fit_id'], 'elapsed_seconds': elapsed, 'new_estimators': 2,
                          'scope': 'matched_absolute_target_fit_and_serialization'}
                raw, timing_raw = encoded(meta), encoded(timing); fitted += 2
                with (publisher.root/'FIT_ATTEMPTS.jsonl').open('ab') as f:
                    f.write(encoded({'attempt_id': attempt, **timing})); f.flush(); os.fsync(f.fileno())
            meta = json.loads(raw); validate_fit(meta, tree)
            if meta['original_signed_fit_id'] != artifacts['signed', h][0]['fit_id']:
                raise ValueError('later_new_absolute_original_lineage_mismatch')
            for suffix, data in (('.json', raw), ('.joblib', tree), ('.timing.json', timing_raw)):
                payloads.append(publisher.write_or_validate_payload(stem+suffix, data))
            artifacts['absolute', h] = meta, tree; newfit_times.append(json.loads(timing_raw)); guard('after_fit_'+str(index))
            if crash_after == index: os._exit(91)
        for h in c['reused_absolute_horizons']:
            stem = f'fit_{h}_{c["fit_cutoff"]}'
            meta = reader.read('absolute', stem+'.json')
            if meta['original_signed_fit_id'] != artifacts['signed', h][0]['fit_id']:
                raise ValueError('later_reused_absolute_original_lineage_mismatch')
            artifacts['absolute', h] = meta, binary(reader, 'absolute', stem+'.joblib')
        reserved = schedule(c); put('schedule.json', reserved)
        predictor = SavedModels(artifacts, c, reserved)
        market = market_inputs(paths['slices'], recipe['slice_manifest'], c); put('market.json', market)
        chunks, timings = {}, []
        for index, h in enumerate(c['horizons_minutes'], 6):
            chunk, measured = prequential_chunk(predictor, observations, lookup, h, c)
            chunk['joined_rows'] = join_predictions(chunk['base_rows'], chunk['absolute_rows'],
                ['legacy26', f'technical_endpoint_midpoint_elapsed_{h}m', 'frozen'])
            put(f'prequential_{h}.json', chunk); chunks[h] = chunk; timings.extend(measured); guard('after_prequential_'+str(h))
            if crash_after == index: os._exit(91)
        frames, frame_times = [], []
        for index, origin in enumerate(c['policy_origins'], 14):
            h = (c['common_target_epoch']-origin)//60
            item, timing = frame(origin, predictor, observations, chunks[h]['joined_rows'], lookup, market, c, Path(paths['trad']))
            put(f'frame_{origin}.json', item); frames.append(item); frame_times.append(timing); guard('after_frame_'+str(origin))
            if crash_after == index: os._exit(91)
        assessment = assess(frames, lookup, c['original_experiment']['evaluation_asof']); put('assessment.json', assessment)
        by_obs = {o['record_id']: o for o in observations}
        refs = {(r['instrument'], r['price_epoch']): r for r in market['rows']}
        consumer = []; consumer_started = time.monotonic()
        for item in frames:
            for pred, packet in zip(item['predictions'], item['packets']):
                try:
                    result = consume_verified(packet, pred, by_obs[pred['record_id']], refs[pred['instrument'], pred['decision_epoch']],
                        market, c, Path(paths['trad']))
                    consumer.append({'forecast_id': pred['forecast_id'], 'status': 'admitted', 'candidate': result})
                except ValueError as exc:
                    if str(exc) != 'financing_conversion_unavailable': raise
                    consumer.append({'forecast_id': pred['forecast_id'], 'status': 'financing_conversion_unavailable', 'candidate': None})
            guard('after_consumer_'+str(item['origin_epoch']))
        consumer_elapsed = time.monotonic()-consumer_started; put('consumer.json', consumer)
        counts = Counter(r['status'] for r in consumer)
        put('run_report.json', {'status': 'completed_later_remaining_surface', 'frames': len(frames),
            'coverage_slots': sum(len(x['coverage']) for x in frames), 'native_packets': len(consumer), 'consumer_statuses': dict(counts),
            'coverage_reasons': assessment['coverage_reasons'], 'new_absolute_fit_inventory': 5,
            'saved_absolute_fit_inventory_reused': 3, 'saved_signed_fit_inventory_reused': 8,
            'signed_refits': 0, 'api_calls': 0, 'portfolio_replay': False, 'confirmation': False,
            'actual_new_fits': 'separate_attempt_receipt; inventory_is_not_refit_count', **c['readiness']})
        put('source_references.json', {'dependencies': {k: v['identity']['fingerprint'] for k, v in recipe['dependencies'].items()},
            'source_hashes': recipe['sources'], 'saved_fit_ids': {family+'_'+str(h): meta['fit_id'] for (family, h), (meta, _) in artifacts.items()},
            'slice_manifest_sha256': c['predecessors']['slices_manifest_sha256'], 'contract_sha256': fingerprint(c)})
        timing = {'prewarm': predictor.receipt, 'prequential': timings, 'frames': frame_times,
                  'consumer_elapsed_seconds': consumer_elapsed, 'consumer_scope': 'outside_native_build_clock_interface_only'}
        old = publisher.read_verified_payload('timing_receipts.json')
        payloads.append(publisher.write_or_validate_payload('timing_receipts.json', old or encoded(timing)))
        guard('final_precommit')
        receipts = {'scope': 'phase_boundary_samples_not_continuous_OS_quota', 'observations':
            [json.loads(line) for line in (publisher.root/'RESOURCE_ATTEMPTS.jsonl').read_text().splitlines()]}
        old = publisher.read_verified_payload('resource_receipts.json')
        payloads.append(publisher.write_or_validate_payload('resource_receipts.json', old or encoded(receipts)))
        validate_completed(publisher.root, recipe); publisher.complete(payloads, set(required(recipe)))
        result = {'attempt_id': attempt, 'actual_new_estimator_fits': fitted, 'signed_refits': 0,
            'binary_models_loaded': predictor.loaded, 'reuse_only': reuse_only, 'elapsed_seconds': time.monotonic()-started,
            'layer_regression_fits': sum(t['regression_fits'] for t in frame_times)}
        (publisher.root/('ATTEMPT_'+attempt+'.json')).write_bytes(encoded(result)); return result
    except BaseException as exc:
        (publisher.root/('FAILED_ATTEMPT_'+attempt+'.json')).write_bytes(encoded({
            'attempt_id': attempt, 'reason': str(exc), 'actual_new_estimator_fits': fitted,
            'elapsed_seconds': time.monotonic()-started, 'status': 'partial_preserved'}))
        if publisher._owner_token is not None: publisher.release()
        raise
