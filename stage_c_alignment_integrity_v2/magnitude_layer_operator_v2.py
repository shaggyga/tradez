"""Pinned saved-prediction layer; no base fitting or deserialization."""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import absolute_movement_operator_v2 as parent

PARENT = 'ABSOLUTE_MOVEMENT_OPERATOR_RECIPE_REVIEWED_V2.json'
PARENT_SHA = 'd303ca66df8ae2fad6c63cdbcf4ae92685f4d008ee64d74affb2047e897a205b'
RECIPE = 'MAGNITUDE_LAYER_OPERATOR_RECIPE_REVIEWED_V2.json'
SOURCES = tuple(sorted(set(parent.SOURCES)|{PARENT, 'magnitude_layer_operator_v2.py', 'magnitude_layer_runner_v2.py',
    'magnitude_layer_v2.py', 'causal_convex_blend_v2.py', 'MAGNITUDE_LAYER_CONTRACT_V2.json'}))
sha, read = parent.sha, parent.read


def recipe_for(paths):
    if set(paths) != {'joint', 'technical', 'baseline_metadata', 'absolute'}:
        raise ValueError('magnitude_exact_dependency_paths_required')
    base = parent.preflight(ROOT/PARENT, PARENT_SHA, {k: paths[k] for k in ('joint', 'technical', 'baseline_metadata')})
    c = read(ROOT/'MAGNITUDE_LAYER_CONTRACT_V2.json')
    manifest = read(Path(paths['absolute'])/'COMPLETION_MANIFEST.json')
    identity = read(Path(paths['absolute'])/'RUN_IDENTITY.json')
    if (identity != manifest['run_identity'] or identity['fingerprint'] != c['predecessors']['absolute_run_identity'] or
            identity['contract']['recipe'] != base or c['predecessors']['absolute_recipe_sha256'] != PARENT_SHA):
        raise ValueError('magnitude_exact_absolute_run_identity_required')
    payloads = {r['path']: r['sha256'] for r in manifest['payloads']}
    if len(payloads) != len(manifest['payloads']) or set(payloads) != set(manifest['required_payloads']):
        raise ValueError('magnitude_complete_absolute_inventory_required')
    resources = c['resources']
    snapshots = len(c['horizons_minutes'])*len(c['procedures'])*(len(c['origin_epochs'])+1)
    if snapshots > resources['snapshot_attempt_cap'] or snapshots*4 > resources['regression_fit_attempt_cap'] or resources['workers'] != 1:
        raise ValueError('magnitude_declared_layer_budget_exceeded')
    return {'schema_version': 'forex_magnitude_layer_recipe.v1', 'run_id': 'magnitude-conditioned-signed-layer-v2',
            'sources': {n: sha(ROOT/n) for n in SOURCES}, 'environment': base['environment'], 'contract': c,
            'dependencies': {**base['dependencies'], 'absolute': {'identity': identity, 'payloads': payloads, 'excluded_timing_payloads': []}},
            'original_metadata': base['original_metadata'],
            'configuration': {'universe': base['configuration']['universe'], **resources},
            'base_model_fits_allowed': False, 'model_deserialization_allowed': False, 'broker_access': False}


def preflight(path, digest, paths):
    if path.is_symlink() or sha(path) != digest:
        raise ValueError('magnitude_recipe_pin_mismatch')
    r = read(path)
    if set(r['sources']) != set(SOURCES) or any(sha(ROOT/n) != h for n, h in r['sources'].items()):
        raise ValueError('magnitude_source_drift_before_runner_import')
    if r != recipe_for(paths):
        raise ValueError('magnitude_input_or_environment_drift')
    return r


def operate(action, path, digest, paths, runs, crash_after=None):
    try:
        if action not in ('status', 'run', 'resume', 'verify'): raise ValueError('unsupported_magnitude_action')
        recipe = preflight(path, digest, paths)
        from campaign_inspector_v2 import CampaignReader
        from magnitude_layer_runner_v2 import identity_for, required, run, validate_completed
        from publication import verify_completed_run
        CampaignReader({k: paths[k] for k in recipe['dependencies']}, recipe['dependencies'])
        identity = identity_for(recipe); root = runs/recipe['run_id']
        if action in ('run', 'resume'): run(paths, recipe, runs, resume=action == 'resume', crash_after=crash_after)
        if (root/'COMPLETION_MANIFEST.json').exists():
            manifest = verify_completed_run(root, identity)
            if {r['path'] for r in manifest['payloads']} != set(required()): raise ValueError('magnitude_exact_payload_inventory_required')
            validate_completed(root, recipe); status = 'completed_verified'
        elif root.exists():
            if read(root/'RUN_IDENTITY.json') != identity: raise ValueError('magnitude_partial_identity_mismatch')
            status = 'resumable'
        else: status = 'ready'
        if action == 'verify' and status != 'completed_verified': raise ValueError('magnitude_completed_run_required')
        return {'status': status, 'run_identity': identity['fingerprint'], 'recipe_sha256': digest, 'run_path': str(root),
                'next_action': 'checkpoint' if status == 'completed_verified' else 'resume' if status == 'resumable' else 'run',
                **recipe['contract']['readiness'], 'independent_review': False}
    except Exception as exc:
        return {'status': 'review_required', 'reason': str(exc), 'next_action': 'preserve_evidence_and_review'}


if __name__ == '__main__':
    p = argparse.ArgumentParser(); p.add_argument('action', choices=['status', 'run', 'resume', 'verify'])
    for n in ('recipe', 'paths', 'runs-dir'): p.add_argument('--'+n, type=Path, required=True)
    p.add_argument('--recipe-sha256', required=True); p.add_argument('--test-crash-after', type=int)
    a = p.parse_args(); result = operate(a.action, a.recipe, a.recipe_sha256, read(a.paths), a.runs_dir, a.test_crash_after)
    print(json.dumps(result, sort_keys=True)); raise SystemExit(2 if result['status'] == 'review_required' else 0)
