"""All declared scopes with exact saved bases and explicit unavailable layers."""
from collections import Counter
import json
import os
import time
import uuid

from campaign_inspector_v2 import CampaignReader
from causal_convex_blend_v2 import fit_snapshot, apply_snapshot, score, sensitivity
from forecast_blend_v2 import build_chunk, outcome_map
from forecast_blend_runner_v2 import HORIZONS, PROCEDURES, encoded, name, check_resources, validate_resources
from publication import RunPublisher, effective_run_identity, verify_completed_run


def required():
    return sorted([prefix + name(h, p) for h in HORIZONS for p in PROCEDURES
                   for prefix in ('layer_', 'coverage_', 'metrics_', 'blocks_')] +
                  ['source_references.json', 'run_report.json', 'resource_receipts.json'])


def identity_for(recipe):
    return effective_run_identity(contract={'recipe': recipe, 'required_payloads': required()},
          dependency_hashes={**recipe['sources'], **{k: v['identity']['fingerprint'] for k, v in recipe['dependencies'].items()}})


def chunk(base_rows, base_coverage, outcomes, horizon, procedure, contract):
    expected_scope = [contract['group'], f'technical_endpoint_midpoint_elapsed_{horizon}m', procedure]
    by_origin = {epoch: [r for r in base_rows if r['decision_epoch'] == epoch] for epoch in contract['origin_epochs']}
    frozen = fit_snapshot(base_rows, outcomes, contract['frozen_cutoff'], expected_scope, horizon, contract)
    snapshots, predictions, coverage = [frozen], [], []
    statuses = {}
    for epoch in contract['origin_epochs']:
        expanding = fit_snapshot(base_rows, outcomes, epoch, expected_scope, horizon, contract)
        snapshots.append(expanding)
        for mode, snapshot in [('frozen_prefix', frozen), ('expanding_prefix', expanding)]:
            status = 'phase_not_started' if mode == 'frozen_prefix' and epoch < contract['frozen_cutoff'] else snapshot['status']
            statuses[epoch, mode] = status
            if status == 'fitted':
                predictions.extend(apply_snapshot(row, snapshot, mode) for row in by_origin[epoch])
    for original in base_coverage:
        for mode in contract['modes']:
            status = statuses[original['decision_epoch'], mode] if original['blend_available'] else original['reason']
            coverage.append({**original, 'layer_mode': mode, 'layer_status': status, 'layer_available': status == 'fitted',
                             'production_available_epoch': None, 'native_policy_admitted': False})
    scored = {mode: score([r for r in predictions if r['mode'] == mode], outcomes, horizon, contract['assessment_asof'])
              for mode in contract['modes']}
    blocks = {mode: sensitivity([r for r in predictions if r['mode'] == mode], outcomes, horizon, contract['assessment_asof'])
              for mode in contract['modes']}
    return {'scope': expected_scope, 'snapshots': snapshots, 'predictions': predictions}, coverage, scored, blocks


def validate_completed(root):
    report = json.loads((root / 'run_report.json').read_bytes())
    if report['chunks'] != 14 or report['coverage_rows'] != 38080 or report['snapshot_attempts'] != 294:
        raise ValueError('convex_complete_declared_scope_required')
    if any(report[k] != 0 for k in ['base_models_fitted', 'base_models_loaded', 'api_calls', 'new_forecasts_issued']):
        raise ValueError('convex_scope_violation')
    if report['engineering_ready'] is not False or report['demo_authorization_status'] != 'not_granted':
        raise ValueError('convex_promotion_forbidden')
    validate_resources(json.loads((root / 'resource_receipts.json').read_bytes()))


def run(paths, recipe, runs, *, resume=False, crash_after=None):
    identity = identity_for(recipe)
    publisher = RunPublisher(runs, recipe['run_id'], identity)
    if (publisher.root / 'COMPLETION_MANIFEST.json').exists():
        verify_completed_run(publisher.root, identity); return
    publisher.acquire(recover=resume)
    started, attempt = time.monotonic(), uuid.uuid4().hex
    def guard(phase):
        row = {**check_resources(publisher.root, started, recipe['configuration'], phase), 'attempt_id': attempt}
        with (publisher.root / 'RESOURCE_ATTEMPTS.jsonl').open('ab') as f:
            f.write(encoded(row)); f.flush(); os.fsync(f.fileno())
    try:
        guard('before_inputs')
        reader = CampaignReader(paths, recipe['dependencies'])
        outcomes = outcome_map(reader, recipe['configuration']['universe'])
        contract = recipe['contract']
        payloads, counts, statuses = [], Counter(), Counter()
        guard('after_inputs')
        for horizon in HORIZONS:
            for procedure in PROCEDURES:
                suffix = name(horizon, procedure)
                raw = reader.read('joint', 'forecasts_legacy26_' + suffix)
                cov = reader.read('joint', 'coverage_legacy26_' + suffix)
                pairing = {'group': 'legacy26', 'horizon_minutes': horizon, 'procedure': procedure}
                bases, base_cov = build_chunk(raw, cov, outcomes, pairing)
                data, coverage, metrics, blocks = chunk(bases, base_cov, outcomes, horizon, procedure, contract)
                for prefix, value in [('layer_', data), ('coverage_', coverage), ('metrics_', metrics), ('blocks_', blocks)]:
                    payloads.append(publisher.write_or_validate_payload(prefix + suffix, encoded(value)))
                counts['chunks'] += 1
                counts['snapshot_attempts'] += len(data['snapshots'])
                counts['scalar_snapshots_fitted'] += sum(x['status'] == 'fitted' for x in data['snapshots'])
                counts['boundary_weight_snapshots'] += sum(x['parameters'] is not None and x['parameters']['weight_at_boundary'] for x in data['snapshots'])
                counts['coverage_rows'] += len(coverage)
                counts['learned_prediction_rows'] += len(data['predictions'])
                counts['mature_assessment_rows'] += sum(x['mature_rows'] for x in metrics.values())
                counts['input_native_2s_available'] += sum(r['available_epoch'] - r['decision_epoch'] <= 2 for r in data['predictions'])
                counts['input_native_2s_stale'] += sum(r['available_epoch'] - r['decision_epoch'] > 2 for r in data['predictions'])
                statuses.update(x['layer_status'] for x in coverage)
                guard('after_chunk_' + str(counts['chunks']))
                if crash_after == counts['chunks']: os._exit(91)
        report = {'status': 'completed_causal_convex_development_diagnostic', **dict(counts),
                  'coverage_statuses': dict(statuses), 'base_models_fitted': 0, 'base_models_loaded': 0,
                  'api_calls': 0, 'new_forecasts_issued': 0, 'independent_review': False, **contract['readiness'],
                  'actual_layer_issuance_qualified': False, 'learned_layer_training': 'analytical_scalar_convex_least_squares',
                  'limitations': ['inspected_short_development_period', 'modeled_base_readiness_and_label_clocks',
                                  'no_production_layer_readiness_or_native_admission', 'shared_currency_overlapping_target_dependence',
                                  'no_winner_horizon_or_policy_selection']}
        refs = {'dependencies': recipe['dependencies'], 'base_models_refitted': False,
                'reused_fixed_blend_recipe_sha256': contract['parent_fixed_recipe_sha256']}
        for n, obj in [('run_report.json', report), ('source_references.json', refs)]:
            payloads.append(publisher.write_or_validate_payload(n, encoded(obj)))
        if crash_after == 0: os._exit(91)
        guard('final_precommit')
        receipts = {'scope': 'sampled_phase_boundaries_not_continuous_peak_or_OS_quota',
                    'observations': [json.loads(line) for line in (publisher.root / 'RESOURCE_ATTEMPTS.jsonl').read_text().splitlines()]}
        validate_resources(receipts)
        old = publisher.read_verified_payload('resource_receipts.json')
        payloads.append(publisher.write_or_validate_payload('resource_receipts.json', old or encoded(receipts)))
        validate_completed(publisher.root)
        check_resources(publisher.root, started, recipe['configuration'], 'before_manifest')
        publisher.complete(payloads, set(required()))
    except BaseException:
        if publisher._owner_token is not None: publisher.release()
        raise
