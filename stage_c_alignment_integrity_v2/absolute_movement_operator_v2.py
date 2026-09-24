"""Pinned offline operator; authenticate original inputs before numerical imports."""
import argparse
import importlib.metadata
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import forecast_blend_operator_v2 as base

PARENT = 'FORECAST_BLEND_OPERATOR_RECIPE_REVIEWED_V2.json'
PARENT_SHA = '659c63c685f5ed5f1e6e6da6180f8b9c9106fc4b54319fc86e223ffa6498a75b'
RECIPE = 'ABSOLUTE_MOVEMENT_OPERATOR_RECIPE_REVIEWED_V2.json'
SOURCES = tuple(sorted(set(base.SOURCES) | {PARENT, base.PARENT_RECIPE,
    'absolute_movement_operator_v2.py', 'absolute_movement_runner_v2.py', 'absolute_movement_models_v2.py',
    'ABSOLUTE_MOVEMENT_CONTRACT_V2.json', 'matched_campaign_models_v2.py', 'fitted_consumer_v2.py',
    'retained_signed_cost_models_v1.py'}))
sha, read = base.sha, base.read


def configuration(parent, contract):
    c, limits = contract['experiment'], contract['resources']
    count = len(c['horizon_minutes'])*len(c['fit_cutoffs'])
    if count > limits['fit_pair_count_cap'] or 2*count > limits['new_estimator_count_cap'] or limits['workers'] != 1:
        raise ValueError('absolute_fit_budget_or_worker_limit')
    return {'universe': parent['configuration']['universe'], **limits,
            'original_model_fit_count_cap': 0, 'worker_count': 1,
            'resource_checks': 'sampled_phase_boundaries_and_final_precommit_not_continuous_OS_quota'}


def recipe_for(paths):
    if set(paths) != {'joint', 'technical', 'baseline_metadata'}:
        raise ValueError('absolute_exact_input_paths_required')
    parent = base.preflight(ROOT/PARENT, PARENT_SHA, {k: paths[k] for k in ('joint', 'technical')})
    contract = read(ROOT/'ABSOLUTE_MOVEMENT_CONTRACT_V2.json')
    original = parent['dependencies']['joint']['identity']['contract']['recipe']['dependencies']['baseline']
    if original['identity']['fingerprint'] != contract['original_baseline_identity']:
        raise ValueError('absolute_original_baseline_mismatch')
    metadata = Path(paths['baseline_metadata'])
    manifest = read(metadata/'COMPLETION_MANIFEST.json')
    if manifest['run_identity'] != original['identity'] or read(metadata/'RUN_IDENTITY.json') != original['identity']:
        raise ValueError('absolute_original_metadata_identity_mismatch')
    all_payloads = {r['path']: r['sha256'] for r in manifest['payloads']}
    if len(all_payloads) != len(manifest['payloads']) or {n: h for n, h in all_payloads.items() if n not in original['excluded_timing_payloads']} != original['payloads']:
        raise ValueError('absolute_original_manifest_inventory_mismatch')
    expected = {f'fit_{h}_{t}.json': original['payloads'][f'fit_{h}_{t}.json']
                for h in contract['experiment']['horizon_minutes'] for t in contract['experiment']['fit_cutoffs']}
    if {p.name for p in metadata.iterdir()} != set(expected)|{'RUN_IDENTITY.json', 'COMPLETION_MANIFEST.json'}:
        raise ValueError('absolute_metadata_capsule_exact_inventory_required')
    if any((metadata/n).is_symlink() or sha(metadata/n) != digest for n, digest in expected.items()):
        raise ValueError('absolute_original_metadata_bytes_changed')
    env = {**parent['environment'], **{n: importlib.metadata.version(n) for n in ('pandas', 'scipy', 'scikit-learn', 'joblib', 'threadpoolctl')}}
    return {'schema_version': 'forex_absolute_movement_recipe.v1', 'run_id': 'absolute-endpoint-movement-v2',
            'sources': {n: sha(ROOT/n) for n in SOURCES}, 'dependencies': parent['dependencies'],
            'original_metadata': {'identity': original['identity']['fingerprint'], 'payloads': expected,
                                  'scope': 'selected_metadata_from_full_original_run; original_weights_not_loaded_or_refitted'},
            'environment': env, 'contract': contract,
            'configuration': configuration(parent, contract),
            'new_target_fit_allowed': True, 'original_model_refit_allowed': False, 'broker_access': False}


def preflight(path, digest, paths):
    if path.is_symlink() or sha(path) != digest:
        raise ValueError('absolute_recipe_pin_mismatch')
    r = read(path)
    if set(r['sources']) != set(SOURCES) or any(sha(ROOT/n) != h for n, h in r['sources'].items()):
        raise ValueError('absolute_source_drift_before_numerical_import')
    if r != recipe_for(paths):
        raise ValueError('absolute_input_or_environment_drift')
    return r


def operate(action, path, digest, paths, runs, crash_after=None, reuse_only=False):
    try:
        if action not in ('status', 'run', 'resume', 'verify'):
            raise ValueError('unsupported_absolute_action')
        recipe = preflight(path, digest, paths)
        from campaign_inspector_v2 import CampaignReader
        from absolute_movement_runner_v2 import identity_for, required, run, validate_completed
        from publication import verify_completed_run
        CampaignReader({k: paths[k] for k in recipe['dependencies']}, recipe['dependencies'])
        identity = identity_for(recipe)
        root = runs/recipe['run_id']
        attempt = None
        if action in ('run', 'resume'):
            attempt = run(paths, recipe, runs, resume=action == 'resume', crash_after=crash_after, reuse_only=reuse_only)
        if (root/'COMPLETION_MANIFEST.json').exists():
            manifest = verify_completed_run(root, identity)
            if {r['path'] for r in manifest['payloads']} != set(required(recipe)):
                raise ValueError('absolute_exact_payload_inventory_required')
            validate_completed(root, recipe)
            status = 'completed_verified'
        elif root.exists():
            if read(root/'RUN_IDENTITY.json') != identity:
                raise ValueError('absolute_partial_identity_mismatch')
            status = 'resumable'
        else:
            status = 'ready'
        if action == 'verify' and status != 'completed_verified':
            raise ValueError('absolute_completed_run_required')
        return {'status': status, 'run_path': str(root), 'run_identity': identity['fingerprint'],
                'recipe_sha256': digest, 'attempt': attempt, 'independent_review': False,
                **recipe['contract']['readiness'], 'next_action': 'checkpoint' if status == 'completed_verified' else 'resume' if status == 'resumable' else 'run'}
    except Exception as exc:
        return {'status': 'review_required', 'reason': str(exc), 'next_action': 'preserve_evidence_and_review'}


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('action', choices=['status', 'run', 'resume', 'verify'])
    for n in ('recipe', 'paths', 'runs-dir'): p.add_argument('--'+n, type=Path, required=True)
    p.add_argument('--recipe-sha256', required=True)
    p.add_argument('--test-crash-after', type=int)
    p.add_argument('--reuse-only', action='store_true')
    a = p.parse_args()
    result = operate(a.action, a.recipe, a.recipe_sha256, read(a.paths), a.runs_dir, a.test_crash_after, a.reuse_only)
    print(json.dumps(result, sort_keys=True))
    raise SystemExit(2 if result['status'] == 'review_required' else 0)
