"""Frozen offline operator for the scalar prequential forecast layer."""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import forecast_blend_operator_v2 as base

PARENT = 'FORECAST_BLEND_OPERATOR_RECIPE_REVIEWED_V2.json'
PARENT_SHA = '659c63c685f5ed5f1e6e6da6180f8b9c9106fc4b54319fc86e223ffa6498a75b'
SOURCES = tuple(sorted(set(base.SOURCES) | {'causal_convex_operator_v2.py', 'causal_convex_runner_v2.py',
    'causal_convex_blend_v2.py', 'CAUSAL_CONVEX_BLEND_CONTRACT.json', PARENT, base.PARENT_RECIPE}))
sha, read = base.sha, base.read


def recipe_for(paths):
    parent = base.preflight(ROOT / PARENT, PARENT_SHA, paths)
    contract = read(ROOT / 'CAUSAL_CONVEX_BLEND_CONTRACT.json')
    if contract['parent_fixed_recipe_sha256'] != PARENT_SHA:
        raise ValueError('convex_parent_contract_mismatch')
    return {'schema_version': 'forex_causal_convex_recipe.v1', 'run_id': 'causal-convex-prequential-blend-v2',
            'sources': {n: sha(ROOT / n) for n in SOURCES}, 'dependencies': parent['dependencies'],
            'environment': parent['environment'], 'contract': contract,
            'configuration': {**parent['configuration'], **contract['resources']},
            'base_models_refitted': False, 'model_load_allowed': False, 'broker_access': False}


def preflight(path, digest, paths):
    if path.is_symlink() or sha(path) != digest:
        raise ValueError('convex_recipe_pin_mismatch')
    recipe = read(path)
    if set(recipe['sources']) != set(SOURCES) or any(sha(ROOT / n) != h for n, h in recipe['sources'].items()):
        raise ValueError('convex_source_drift_before_runner_import')
    if recipe != recipe_for(paths):
        raise ValueError('convex_input_or_environment_drift')
    return recipe


def operate(action, path, digest, paths, runs, crash_after=None):
    try:
        if action not in ('status', 'run', 'resume', 'verify'):
            raise ValueError('unsupported_convex_action')
        recipe = preflight(path, digest, paths)
        from campaign_inspector_v2 import CampaignReader
        from causal_convex_runner_v2 import identity_for, required, run, validate_completed
        from publication import verify_completed_run
        CampaignReader(paths, recipe['dependencies'])
        identity = identity_for(recipe)
        root = runs / recipe['run_id']
        if action in ('run', 'resume'):
            run(paths, recipe, runs, resume=action == 'resume', crash_after=crash_after)
        if (root / 'COMPLETION_MANIFEST.json').exists():
            manifest = verify_completed_run(root, identity)
            if {r['path'] for r in manifest['payloads']} != set(required()):
                raise ValueError('convex_exact_payload_inventory_required')
            validate_completed(root)
            status = 'completed_verified'
        elif root.exists():
            if read(root / 'RUN_IDENTITY.json') != identity:
                raise ValueError('convex_partial_identity_mismatch')
            status = 'resumable'
        else:
            status = 'ready'
        if action == 'verify' and status != 'completed_verified':
            raise ValueError('completed_convex_run_required')
        return {'status': status, 'run_path': str(root), 'run_identity': identity['fingerprint'],
                'recipe_sha256': digest, 'next_action': 'record_receipt_and_stop' if status == 'completed_verified' else
                'resume' if status == 'resumable' else 'run', **recipe['contract']['readiness'], 'independent_review': False}
    except Exception as exc:
        return {'status': 'review_required', 'reason': str(exc), 'next_action': 'preserve_evidence_and_escalate'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['status', 'run', 'resume', 'verify'])
    for name in ('recipe', 'paths', 'runs-dir'): parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--recipe-sha256', required=True)
    parser.add_argument('--test-crash-after', type=int)
    args = parser.parse_args()
    result = operate(args.action, args.recipe, args.recipe_sha256, read(args.paths), args.runs_dir, args.test_crash_after)
    print(json.dumps(result, sort_keys=True))
    raise SystemExit(2 if result['status'] == 'review_required' else 0)
