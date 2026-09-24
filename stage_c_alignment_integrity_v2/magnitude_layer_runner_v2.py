"""All-scope causal magnitude ablation with durable checkpoints and explicit gaps."""
from collections import Counter
import json
import os
import time
import uuid

from campaign_inspector_v2 import CampaignReader
from forecast_blend_v2 import build_chunk, outcome_map
from forecast_blend_runner_v2 import HORIZONS, PROCEDURES, encoded, name, check_resources
from magnitude_layer_v2 import BASES, join_predictions, fit_snapshot, apply_snapshot, score, sensitivity
from publication import RunPublisher, effective_run_identity, verify_completed_run


def required():
    return sorted([prefix+name(h, p) for h in HORIZONS for p in PROCEDURES
                   for prefix in ('layer_', 'coverage_', 'metrics_', 'blocks_')] +
                  ['run_report.json', 'source_references.json', 'resource_receipts.json'])


def identity_for(recipe):
    return effective_run_identity(contract={'recipe': recipe, 'required_payloads': required()},
       dependency_hashes={**recipe['sources'], **{k: v['identity']['fingerprint'] for k, v in recipe['dependencies'].items()}})


def chunk(bases, coverage, absolute, outcomes, horizon, procedure, contract):
    scope = ['legacy26', f'technical_endpoint_midpoint_elapsed_{horizon}m', procedure]
    rows = join_predictions(bases, absolute, scope)
    by_origin = {t: [r for r in rows if r['decision_epoch'] == t] for t in contract['origin_epochs']}
    frozen = fit_snapshot(rows, outcomes, contract['frozen_cutoff'], scope, horizon, contract)
    snapshots, predictions, statuses = [frozen], [], {}
    for epoch in contract['origin_epochs']:
        expanding = fit_snapshot(rows, outcomes, epoch, scope, horizon, contract); snapshots.append(expanding)
        for mode, snapshot in (('frozen_prefix', frozen), ('expanding_prefix', expanding)):
            status = 'phase_not_started' if mode == 'frozen_prefix' and epoch < contract['frozen_cutoff'] else snapshot['status']
            statuses[epoch, mode] = status
            if status == 'fitted':
                for row in by_origin[epoch]: predictions.extend(apply_snapshot(row, snapshot, mode))
    cov = []
    for row in coverage:
        for mode in contract['modes']:
            status = statuses[row['decision_epoch'], mode] if row['blend_available'] else row['reason']
            for base in BASES:
                cov.append({**row, 'layer_mode': mode, 'base_method': base, 'layer_status': status,
                            'layer_available': status == 'fitted', 'production_available_epoch': None,
                            'native_policy_admitted': False})
    scores, blocks = {}, {}
    for base in BASES:
        for mode in contract['modes']:
            chosen = [r for r in predictions if r['base_method'] == base and r['mode'] == mode]
            key = base+'/'+mode
            scores[key] = score(chosen, outcomes, horizon, contract['assessment_asof'])
            blocks[key] = sensitivity(chosen, outcomes, horizon, contract['assessment_asof'], contract['uncertainty'])
    return {'scope': scope, 'snapshots': snapshots, 'predictions': predictions}, cov, scores, blocks


def validate_completed(root, recipe):
    r = json.loads((root/'run_report.json').read_bytes())
    if r['chunks'] != 14 or r['snapshot_attempts'] != 294 or r['coverage_rows'] != 76160:
        raise ValueError('magnitude_complete_declared_scope_required')
    if any(r[k] != 0 for k in ('base_models_fitted', 'base_models_loaded', 'api_calls')) or r['engineering_ready'] is not False:
        raise ValueError('magnitude_scope_or_promotion_violation')
    if r['regression_fits'] != 4*r['fitted_snapshots'] or r['regression_fits'] > recipe['configuration']['regression_fit_attempt_cap']:
        raise ValueError('magnitude_regression_inventory_mismatch')
    rows = json.loads((root/'resource_receipts.json').read_bytes())['observations']; c = recipe['configuration']
    if not rows or rows[-1]['phase'] != 'final_precommit': raise ValueError('magnitude_final_resource_receipt_required')
    for row in rows:
        if not (0 <= row['elapsed_seconds'] <= c['main_wall_seconds'] and 0 <= row['aggregate_rss_bytes'] <= c['max_rss_bytes'] and 0 <= row['run_disk_bytes'] <= c['max_scratch_bytes']):
            raise ValueError('magnitude_resource_receipt_out_of_bounds')


def run(paths, recipe, runs, *, resume=False, crash_after=None):
    identity = identity_for(recipe); publisher = RunPublisher(runs, recipe['run_id'], identity)
    if (publisher.root/'COMPLETION_MANIFEST.json').exists(): verify_completed_run(publisher.root, identity); return
    publisher.acquire(recover=resume); started, attempt = time.monotonic(), uuid.uuid4().hex
    def guard(phase):
        row = {**check_resources(publisher.root, started, recipe['configuration'], phase), 'attempt_id': attempt}
        with (publisher.root/'RESOURCE_ATTEMPTS.jsonl').open('ab') as f:
            f.write(encoded(row)); f.flush(); os.fsync(f.fileno())
    try:
        guard('before_inputs')
        reader = CampaignReader({k: paths[k] for k in recipe['dependencies']}, recipe['dependencies'])
        outcomes = outcome_map(reader, recipe['configuration']['universe'])
        payloads, counts, statuses = [], Counter(), Counter(); guard('after_inputs')
        for h in HORIZONS:
            for procedure in PROCEDURES:
                suffix = name(h, procedure)
                cached = [publisher.read_verified_payload(prefix+suffix) for prefix in ('layer_', 'coverage_', 'metrics_', 'blocks_')]
                if all(value is not None for value in cached):
                    data, coverage, metrics, blocks = [json.loads(value) for value in cached]
                else:
                    bases, cov = build_chunk(reader.read('joint', 'forecasts_legacy26_'+suffix),
                        reader.read('joint', 'coverage_legacy26_'+suffix), outcomes,
                        {'group': 'legacy26', 'horizon_minutes': h, 'procedure': procedure})
                    data, coverage, metrics, blocks = chunk(bases, cov, reader.read('absolute', 'predictions_'+suffix),
                                                           outcomes, h, procedure, recipe['contract'])
                for prefix, obj in (('layer_', data), ('coverage_', coverage), ('metrics_', metrics), ('blocks_', blocks)):
                    payloads.append(publisher.write_or_validate_payload(prefix+suffix, encoded(obj)))
                counts['chunks'] += 1; counts['coverage_rows'] += len(coverage)
                counts['snapshot_attempts'] += len(data['snapshots'])
                fitted = sum(s['status'] == 'fitted' for s in data['snapshots'])
                counts['fitted_snapshots'] += fitted; counts['regression_fits'] += 4*fitted
                counts['prediction_rows'] += len(data['predictions'])
                counts['mature_assessment_rows'] += sum(s['mature_rows'] for s in metrics.values())
                statuses.update(r['layer_status'] for r in coverage)
                guard('after_chunk_'+str(counts['chunks']))
                if crash_after == counts['chunks']: os._exit(91)
        report = {'status': 'completed_magnitude_conditioned_signed_development_diagnostic', **dict(counts),
                  'coverage_statuses': dict(statuses), 'base_models_fitted': 0, 'base_models_loaded': 0, 'api_calls': 0,
                  'selected_model': None, 'independent_review': False, **recipe['contract']['readiness'],
                  'regression_count_scope': 'completed_run_inventory; cached_chunks_not_recomputed_on_resume',
                  'limitations': ['inspected_short_development', 'modeled_clocks_not_actual_historical_issuance',
                                  'no_native_policy_admission', 'overlapping_horizons_and_shared_currencies',
                                  'no_probability_or_profit_claim']}
        refs = {'dependencies': recipe['dependencies'], 'parent_absolute_recipe_sha256': recipe['contract']['predecessors']['absolute_recipe_sha256'],
                'original_base_models_refitted': False, 'absolute_models_deserialized': False}
        for n, obj in (('run_report.json', report), ('source_references.json', refs)):
            payloads.append(publisher.write_or_validate_payload(n, encoded(obj)))
        if crash_after == 0: os._exit(91)
        guard('final_precommit')
        receipts = {'scope': 'phase_samples_not_continuous_OS_quota',
                    'observations': [json.loads(line) for line in (publisher.root/'RESOURCE_ATTEMPTS.jsonl').read_text().splitlines()]}
        old = publisher.read_verified_payload('resource_receipts.json')
        payloads.append(publisher.write_or_validate_payload('resource_receipts.json', old or encoded(receipts)))
        validate_completed(publisher.root, recipe)
        check_resources(publisher.root, started, recipe['configuration'], 'before_manifest')
        publisher.complete(payloads, set(required()))
    except BaseException:
        if publisher._owner_token is not None: publisher.release()
        raise
