"""Pinned offline later surface operator; authenticate before numerical imports."""
import argparse
import ast
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from publication import verify_completed_run, sha256_file

CONTRACT = 'LATER_REMAINING_SURFACE_CONTRACT_V2.json'
RECIPE = 'LATER_REMAINING_SURFACE_OPERATOR_RECIPE_REVIEWED_V2.json'
read = lambda p: json.loads(Path(p).read_bytes())


def source_names():
    pending = ['later_surface_operator_v2.py', 'later_surface_runner_v2.py']
    found = set()
    while pending:
        name = pending.pop()
        if name in found: continue
        found.add(name)
        tree = ast.parse((ROOT/name).read_text(encoding='utf-8-sig'))
        for node in ast.walk(tree):
            modules = [a.name for a in node.names] if isinstance(node, ast.Import) else [node.module] if isinstance(node, ast.ImportFrom) and node.module else []
            for module in modules:
                local = module.split('.')[0]+'.py'
                if (ROOT/local).is_file(): pending.append(local)
    return sorted(found | {CONTRACT})


def recipe_for(paths):
    if set(paths) != {'technical', 'remaining', 'absolute', 'slices', 'trad'}:
        raise ValueError('later_exact_input_paths_required')
    contract = read(ROOT/CONTRACT); deps = {}
    for alias in ('technical', 'remaining', 'absolute'):
        root = Path(paths[alias]); identity = read(root/'RUN_IDENTITY.json')
        if identity['fingerprint'] != contract['predecessors'][alias+'_identity']:
            raise ValueError('later_original_run_identity_mismatch:'+alias)
        manifest = verify_completed_run(root, identity)
        deps[alias] = {'identity': identity, 'payloads': {r['path']: r['sha256'] for r in manifest['payloads']},
                       'excluded_timing_payloads': []}
    slices = Path(paths['slices']); sm = read(slices/'SLICES_MANIFEST.json')
    if sha256_file(slices/'SLICES_MANIFEST.json') != contract['predecessors']['slices_manifest_sha256']:
        raise ValueError('later_slice_manifest_mismatch')
    if len(sm['members']) != 68 or len({r['instrument'] for r in sm['members']}) != 68:
        raise ValueError('later_exact68_slice_inventory_required')
    for member in sm['members']:
        name = member['path']; path = slices/name
        if Path(name).name != name or path.is_symlink() or sha256_file(path) != member['sha256']:
            raise ValueError('later_original_slice_bytes_mismatch')
    # Native and Decimal core are pinned by the original recipe, independently
    # of the local module's transitive source inventory.
    original = ROOT/'MATCHED_REMAINING_OPERATOR_RECIPE.json'
    if sha256_file(original) != contract['predecessors']['existing_remaining_recipe_sha256']:
        raise ValueError('later_original_remaining_recipe_changed')
    from reference_accounting_adapter_v2 import reference_dependency_identity
    core = reference_dependency_identity(Path(paths['trad']))
    parent = read(original); predecessors = {**parent['predecessors'], **{k: v for k, v in core.items() if v is not None}}
    for name, digest in predecessors.items():
        if sha256_file(Path(paths['trad'])/name) != digest:
            raise ValueError('later_native_predecessor_changed:'+name)
    absent = sorted(k for k, v in core.items() if v is None)
    for name in absent:
        if (Path(paths['trad'])/name).exists(): raise ValueError('later_shadow_import_path_present')
    environment = {'python': platform.python_version(), **{name: importlib.metadata.version(name)
        for name in ('numpy', 'pandas', 'scipy', 'scikit-learn', 'joblib', 'threadpoolctl', 'psutil', 'pyarrow')}}
    return {'schema_version': 'forex_later_surface_recipe.v1', 'run_id': 'later-remaining-surface-v2',
        'contract': contract, 'dependencies': deps, 'predecessors': predecessors,
        'absent_import_paths': absent,
        'original_remaining_recipe': {'name': original.name, 'sha256': sha256_file(original)},
        'sources': {n: sha256_file(ROOT/n) for n in source_names()}, 'environment': environment,
        'configuration': {'universe': sorted(r['instrument'] for r in sm['members']), **contract['resources']},
        'slice_manifest': sm, 'broker_access': False, 'api_calls': 0}


def preflight(path, digest, paths):
    if path.is_symlink() or sha256_file(path) != digest: raise ValueError('later_recipe_pin_mismatch')
    recipe = read(path)
    if set(recipe['sources']) != set(source_names()) or any(sha256_file(ROOT/n) != h for n, h in recipe['sources'].items()):
        raise ValueError('later_source_drift_before_numerical_import')
    if recipe != recipe_for(paths): raise ValueError('later_input_or_environment_drift')
    return recipe


def operate(action, path, digest, paths, runs, crash_after=None, reuse_only=False):
    try:
        recipe = preflight(path, digest, paths)
        from later_surface_runner_v2 import identity_for, required, run, validate_completed
        identity = identity_for(recipe); root = runs/recipe['run_id']; attempt = None
        if action in ('run', 'resume'):
            attempt = run(paths, recipe, runs, resume=action == 'resume', crash_after=crash_after, reuse_only=reuse_only)
        if (root/'COMPLETION_MANIFEST.json').exists():
            manifest = verify_completed_run(root, identity)
            if {r['path'] for r in manifest['payloads']} != set(required(recipe)):
                raise ValueError('later_exact_output_inventory_required')
            validate_completed(root, recipe); status = 'completed_verified'
        elif root.exists():
            if read(root/'RUN_IDENTITY.json') != identity: raise ValueError('later_partial_identity_mismatch')
            status = 'resumable'
        else: status = 'ready'
        if action == 'verify' and status != 'completed_verified': raise ValueError('later_completed_run_required')
        return {'status': status, 'run_identity': identity['fingerprint'], 'recipe_sha256': digest,
                'run_path': str(root), 'attempt': attempt, 'independent_review': False,
                **recipe['contract']['readiness']}
    except Exception as exc:
        return {'status': 'review_required', 'reason': str(exc), 'next_action': 'preserve_evidence_and_review'}


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('action', choices=['status', 'run', 'resume', 'verify'])
    for n in ('recipe', 'paths', 'runs-dir'): p.add_argument('--'+n, type=Path, required=True)
    p.add_argument('--recipe-sha256', required=True); p.add_argument('--test-crash-after', type=int)
    p.add_argument('--reuse-only', action='store_true'); a = p.parse_args()
    result = operate(a.action, a.recipe, a.recipe_sha256, read(a.paths), a.runs_dir, a.test_crash_after, a.reuse_only)
    print(json.dumps(result, sort_keys=True)); raise SystemExit(2 if result['status'] == 'review_required' else 0)
