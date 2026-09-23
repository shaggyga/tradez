"""Frozen retained-text recipe: no collectors, databases, models or service imports."""
import argparse
import hashlib
import importlib.metadata
import json
import platform
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCES = ('macro_text_operator_v2.py', 'macro_text_layer_v2.py', 'MACRO_TEXT_RULES_V2.json', 'contracts.py', 'publication.py')
INPUTS = ('baseline.json', 'universe.json', 'predecessor.py')
REQUIRED = ('text_documents.json', 'currency_states.json', 'pair_context.json', 'run_report.json')
CUTOFFS = ['2024-07-26T18:01:00+00:00', '2026-08-16T06:23:07+00:00', '2026-08-17T08:00:00+00:00', '2026-09-22T13:00:00+00:00']


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    return json.loads(path.read_text(encoding='utf-8'))


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)+'\n').encode()


def recipe_for(inputs):
    for name in INPUTS:
        p = inputs/name
        if not p.is_file() or p.is_symlink() or p.stat().st_size > 1024*1024:
            raise ValueError('plain_bounded_text_inputs_required')
    universe = read(inputs/'universe.json')
    if len(universe) != 68 or len(set(universe)) != 68 or sorted(universe) != universe:
        raise ValueError('original_sorted_68_universe_required')
    if sha(inputs/'predecessor.py') != read(ROOT/'MACRO_TEXT_RULES_V2.json')['predecessor_sha256']:
        raise ValueError('lexical_predecessor_drift')
    return {'schema': 'macro_text_recipe.v2', 'run_id': 'retained-text-state-change',
            'sources': {n: sha(ROOT/n) for n in SOURCES}, 'inputs': {n: sha(inputs/n) for n in INPUTS},
            'environment': {'python': platform.python_version(), 'numpy': importlib.metadata.version('numpy')},
            'configuration': {'cutoffs': CUTOFFS, 'max_age_days': 35, 'network_allowed': False, 'broker_access': False,
                              'historical_forecast_admission': False, 'max_wall_seconds': 120, 'max_input_bytes': 1048576},
            'required_payloads': list(REQUIRED)}


def preflight(recipe, digest, inputs):
    if recipe.is_symlink() or sha(recipe) != digest:
        raise ValueError('macro_text_recipe_pin_mismatch')
    r = read(recipe)
    if set(r['sources']) != set(SOURCES) or any(sha(ROOT/n) != h for n,h in r['sources'].items()):
        raise ValueError('macro_text_source_drift')
    if r != recipe_for(inputs):
        raise ValueError('macro_text_input_or_environment_drift')
    return r


def identity_for(r):
    from publication import effective_run_identity
    return effective_run_identity(contract=r, dependency_hashes={**r['sources'], **r['inputs']})


def consumed(inputs, r, name):
    raw = (inputs/name).read_bytes()
    if len(raw) > 1048576 or hashlib.sha256(raw).hexdigest() != r['inputs'][name]:
        raise ValueError('macro_text_consumed_input_drift')
    return json.loads(raw)


def operate(action, recipe, digest, inputs, runs, crash_after=None):
    try:
        if action not in {'status', 'run', 'resume', 'verify'}:
            raise ValueError('unsupported_macro_text_action')
        r = preflight(recipe, digest, inputs)
        sys.path.insert(0, str(ROOT))
        from publication import RunPublisher, verify_completed_run
        from macro_text_layer_v2 import build
        identity = identity_for(r)
        root = runs/r['run_id']
        if action in {'run', 'resume'} and not (root/'COMPLETION_MANIFEST.json').exists():
            publisher = RunPublisher(runs, r['run_id'], identity)
            publisher.acquire(recover=action == 'resume')
            try:
                started = time.monotonic()
                rules = read(ROOT/'MACRO_TEXT_RULES_V2.json')['rules']
                outputs = build(consumed(inputs,r,'baseline.json'), consumed(inputs,r,'universe.json'), rules, CUTOFFS)
                if time.monotonic()-started > r['configuration']['max_wall_seconds']:
                    raise ValueError('macro_text_wall_budget_exceeded_before_publication')
                payloads = []
                for index, name in enumerate(REQUIRED, 1):
                    payloads.append(publisher.write_or_validate_payload(name, encoded(outputs[name])))
                    if crash_after == index:
                        import os
                        os._exit(91)
                if crash_after == 0:
                    import os
                    os._exit(91)
                publisher.complete(payloads, set(REQUIRED))
            except BaseException:
                if publisher._owner_token is not None:
                    publisher.release()
                raise
        if (root/'COMPLETION_MANIFEST.json').exists():
            manifest = verify_completed_run(root, identity)
            if {p['path'] for p in manifest['payloads']} != set(REQUIRED):
                raise ValueError('exact_macro_text_inventory_required')
            report = read(root/'run_report.json')
            if report['pair_rows'] != 272 or report['universe_count'] != 68 or report['base_models_fitted'] != 0:
                raise ValueError('macro_text_population_mismatch')
            status = 'completed_verified'
        elif root.exists():
            if read(root/'RUN_IDENTITY.json') != identity:
                raise ValueError('macro_text_partial_identity_mismatch')
            status = 'resumable'
        else:
            status = 'ready'
        if action == 'verify' and status != 'completed_verified':
            raise ValueError('macro_text_completion_required')
        return {'status': status, 'run_path': str(root), 'run_identity': identity['fingerprint'], 'recipe_sha256': digest,
                'next_action': 'record_receipt_and_stop' if status == 'completed_verified' else 'resume' if status == 'resumable' else 'run',
                'engineering_ready': False, 'forecast_evidence_status': 'retrospective_text_only',
                'policy_evidence_status': 'not_evaluated', 'demo_authorization_status': 'not_granted', 'independent_review': False}
    except Exception as exc:
        return {'status': 'review_required', 'reason': str(exc), 'next_action': 'preserve_evidence_and_escalate'}


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('action', choices=['status','run','resume','verify'])
    for name in ('recipe','inputs','runs-dir'):
        p.add_argument('--'+name, type=Path, required=True)
    p.add_argument('--recipe-sha256', required=True)
    p.add_argument('--test-crash-after', type=int)
    args = p.parse_args()
    result = operate(args.action,args.recipe,args.recipe_sha256,args.inputs,args.runs_dir,args.test_crash_after)
    print(json.dumps(result,sort_keys=True))
    raise SystemExit(2 if result['status'] == 'review_required' else 0)
