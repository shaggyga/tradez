"""Bounded zero-fit later policy operator over authenticated parent predictions."""
import argparse
import ast
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from contracts import fingerprint
from publication import verify_completed_run, sha256_file

RECIPE = 'LATER_LAYER_POLICY_OPERATOR_RECIPE_V2.json'
CONTRACT = 'LATER_LAYER_POLICY_CONTRACT_V2.json'
AUTHORITY = 'LATER_POLICY_AUTHORITY_V2.json'
PARENT = 'LATER_REMAINING_SURFACE_OPERATOR_RECIPE_REVIEWED_V2.json'
PARITY = ('policy_decisions.jsonl', 'policy_state.json', 'event_ledger.jsonl', 'final_state.json', 'accounting_audit.json')
read = lambda p: json.loads(Path(p).read_bytes())


def source_names():
    pending = ['later_policy_operator_v2.py', 'later_policy_fixture_v2.py', 'policy_runner_v2.py',
               'matched_policy_fixture_v2.py', 'fixed_blend_remaining_policy_v2.py', 'historical_policy_fixture_v2.py']
    found = set()
    while pending:
        name = pending.pop()
        if name in found: continue
        found.add(name)
        for node in ast.walk(ast.parse((ROOT/name).read_text(encoding='utf-8-sig'))):
            modules = [a.name for a in node.names] if isinstance(node, ast.Import) else [node.module] if isinstance(node, ast.ImportFrom) and node.module else []
            for module in modules:
                local = module.split('.')[0]+'.py'
                if (ROOT/local).is_file(): pending.append(local)
    return sorted(found | {CONTRACT, AUTHORITY, PARENT})


def load_inputs(paths):
    if set(paths) != {'surface', 'technical', 'trad'}: raise ValueError('later_policy_exact_paths_required')
    c = read(ROOT/CONTRACT); parent = read(ROOT/PARENT)
    if sha256_file(ROOT/PARENT) != c['surface_recipe_sha256']: raise ValueError('later_policy_parent_recipe_pin_mismatch')
    identities, manifests = {}, {}
    for alias in ('surface', 'technical'):
        root = Path(paths[alias]); identity = read(root/'RUN_IDENTITY.json')
        if identity['fingerprint'] != c[alias+'_identity']: raise ValueError('later_policy_original_dependency_identity_mismatch:'+alias)
        if alias == 'surface' and identity['contract']['recipe'] != parent: raise ValueError('later_policy_original_surface_recipe_required')
        manifests[alias] = verify_completed_run(root, identity); identities[alias] = identity
    def payload(alias, name):
        entries = {r['path']: r['sha256'] for r in manifests[alias]['payloads']}
        raw = (Path(paths[alias])/name).read_bytes()
        if hashlib.sha256(raw).hexdigest() != entries[name]: raise ValueError('later_policy_consumed_dependency_changed')
        return json.loads(raw)
    market = payload('surface', 'market.json')
    frames = {str(t): payload('surface', f'frame_{t}.json') for t in c['origins']}
    observations = [o for pair in c['universe'] for o in payload('technical', 'pair_'+pair+'.json')['observations'] if o['origin_epoch'] in c['origins']]
    derived = {'schema_version': 'forex_later_policy_authority.v1', 'surface_identity': identities['surface']['fingerprint'],
        'source_recipe_sha256': sha256_file(ROOT/PARENT), 'source_payloads': {r['path']: r['sha256'] for r in manifests['surface']['payloads']},
        'surface_contract': parent['contract'], 'market_metadata_sha256': fingerprint(market['metadata']), 'universe': market['universe'],
        'market_panels': {str(t): fingerprint([r for r in market['rows'] if r['price_epoch'] == t]) for t in market['price_epochs']}, 'frames': {}}
    for t in c['origins']:
        data = frames[str(t)]; groups = {}
        for method in c['methods']:
            base, variant = method.split('__')
            pred = [r for r in data['predictions'] if (r['base_method'], r['variant']) == (base, variant)]
            packets = [p for p in data['packets'] if (p['prediction']['base_method'], p['prediction']['variant']) == (base, variant)]
            coverage = [r for r in data['coverage'] if (r['base_method'], r['variant']) == (base, variant)]
            groups[method] = {'predictions_sha256': fingerprint(pred), 'packets_sha256': fingerprint(packets),
                             'coverage_sha256': fingerprint(coverage), 'packet_count': len(packets)}
        derived['frames'][str(t)] = {'observations_sha256': fingerprint(sorted([o for o in observations if o['origin_epoch'] == t], key=lambda r: r['instrument'])),
            'source_frame_sha256': derived['source_payloads'][f'frame_{t}.json'], 'groups': groups}
    if sha256_file(ROOT/AUTHORITY) != c['authority_sha256'] or derived != read(ROOT/AUTHORITY):
        raise ValueError('later_policy_authority_not_derived_from_original_run')
    return {'market': market, 'frames': frames, 'observations': observations}, {
        alias: {'identity': identities[alias], 'payloads': {r['path']: r['sha256'] for r in manifests[alias]['payloads']}}
        for alias in manifests}


def recipe_for(paths):
    _, deps = load_inputs(paths)
    from reference_accounting_adapter_v2 import reference_dependency_identity
    from native_policy_input_v2 import PREDECESSORS
    core = reference_dependency_identity(Path(paths['trad']))
    for n, h in PREDECESSORS.items():
        if sha256_file(Path(paths['trad'])/n) != h: raise ValueError('later_policy_native_dependency_changed')
    return {'schema_version': 'forex_later_policy_recipe.v1', 'contract': read(ROOT/CONTRACT),
        'dependencies': deps, 'sources': {n: sha256_file(ROOT/n) for n in source_names()},
        'predecessors': {**PREDECESSORS, **{k: v for k, v in core.items() if v is not None}},
        'absent_import_paths': sorted(k for k, v in core.items() if v is None),
        'environment': {'python': platform.python_version(), **{n: importlib.metadata.version(n)
            for n in ('numpy', 'pandas', 'scipy', 'scikit-learn', 'joblib', 'threadpoolctl', 'psutil', 'pyarrow')}},
        'base_model_fits': 0, 'base_model_loads': 0, 'layer_fits': 0, 'broker_access': False}


def preflight(path, digest, paths):
    if path.is_symlink() or sha256_file(path) != digest: raise ValueError('later_policy_recipe_pin_mismatch')
    r = read(path)
    if set(r['sources']) != set(source_names()) or any(sha256_file(ROOT/n) != h for n, h in r['sources'].items()):
        raise ValueError('later_policy_source_drift_before_replay_import')
    if r != recipe_for(paths): raise ValueError('later_policy_input_environment_drift')
    return r


def guard(runs, started, limits, phase):
    import psutil
    process = psutil.Process(); rss = sum(p.memory_info().rss for p in [process, *process.children(recursive=True)] if p.is_running())
    size = sum(p.stat().st_size for p in runs.rglob('*') if p.is_file()) if runs.exists() else 0
    free = shutil.disk_usage(runs if runs.exists() else runs.parent).free
    result = {'phase': phase, 'elapsed_seconds': time.monotonic()-started, 'aggregate_rss_bytes': rss, 'scratch_bytes': size, 'disk_free_bytes': free}
    if (result['elapsed_seconds'] > limits['main_wall_seconds'] or rss > limits['max_rss_bytes'] or
        size+1048576 > limits['max_scratch_bytes'] or free < limits['minimum_disk_free_bytes']):
        raise ValueError('later_policy_resource_limit:'+json.dumps(result))
    return result


def operate(action, path, digest, paths, runs, crash_run=None, crash_frame=None):
    try:
        started = time.monotonic(); r = preflight(path, digest, paths)
        from later_policy_fixture_v2 import fixture
        from policy_runner_v2 import run, identity_for, required
        data, _ = load_inputs(paths); c = r['contract']; trad = Path(paths['trad'])
        records = []; observations = []; runs.mkdir(parents=True, exist_ok=True)
        for method in c['methods']:
            for scenario in c['scenarios']:
                observations.append(guard(runs, started, c['resources'], 'before_'+method+'_'+scenario))
                if any(sha256_file(ROOT/n) != h for n, h in r['sources'].items()): raise ValueError('later_policy_source_changed_during_replay')
                policy, frames, coverage = fixture(data, method, scenario, trad)
                if len(coverage) != 544 or len({(x['instrument'], x['conditioning_epoch']) for x in coverage}) != 544:
                    raise ValueError('later_policy_complete_all68_coverage_required')
                admitted = sum(x['status'] == 'admitted' for x in coverage)
                refused = sum(len(f.get('historical_refusals', [])) for f in frames)
                for engine in c['engines']:
                    name = method+'-'+scenario+'-'+engine; root = runs/name; identity = identity_for(policy, frames, trad, engine)
                    if action in ('run', 'resume'):
                        run(policy, frames, run_id=name, runs_dir=runs, trad_root=trad, engine=engine,
                            resume=action == 'resume', crash_after=crash_frame if name == crash_run else None)
                    if (root/'COMPLETION_MANIFEST.json').exists():
                        manifest = verify_completed_run(root, identity)
                        if {p['path'] for p in manifest['payloads']} != required(len(frames)): raise ValueError('later_policy_exact_payload_inventory')
                        report = read(root/'run_report.json')
                        if report['accounting_oracle']['status'] != 'verified' or report['decision_count'] != 54:
                            raise ValueError('later_policy_full_accounting_required')
                        if report['native_admission']['accepted'] != admitted or report['native_admission']['refused'] != refused:
                            raise ValueError('later_policy_native_admission_mismatch')
                        if any(report['actions'][arm]['ENTER'] for arm in ('cash', 'recovered')):
                            raise ValueError('later_policy_unsupported_arm_entry')
                        # Missing terminal quotes must remain visible, never be
                        # turned into zero PnL or hidden by a future universe filter.
                        terminal = {arm: {k: info[k] for k in ('open_lot_count', 'pending_order_units', 'rejected_events')}
                                    for arm, info in report['arms'].items()}
                        status = 'completed_verified'
                    elif root.exists():
                        if read(root/'RUN_IDENTITY.json') != identity: raise ValueError('later_policy_partial_identity_mismatch')
                        status, terminal = 'resumable', None
                    else: status, terminal = 'ready', None
                    records.append({'run_id': name, 'method': method, 'scenario': scenario, 'engine': engine, 'path': str(root),
                        'status': status, 'run_identity': identity['fingerprint'], 'coverage': coverage,
                        'admitted': admitted, 'refused': refused, 'terminal_reconciliation': terminal})
                    observation = guard(runs, started, c['resources'], 'after_'+name); observations.append(observation)
                    with (runs/'OPERATOR_PROGRESS.jsonl').open('a', encoding='utf-8') as f:
                        f.write(json.dumps({'run_id': name, 'status': status, 'resources': observation})+'\n')
                pair = [x for x in records if x['method'] == method and x['scenario'] == scenario]
                if all(x['status'] == 'completed_verified' for x in pair):
                    for n in PARITY:
                        if sha256_file(Path(pair[0]['path'])/n) != sha256_file(Path(pair[1]['path'])/n):
                            raise ValueError('later_policy_engine_parity:'+n)
        complete = all(x['status'] == 'completed_verified' for x in records)
        if action == 'verify' and not complete: raise ValueError('later_policy_complete56runs_required')
        return {'status': 'completed_verified' if complete else 'resumable' if any(x['status'] == 'resumable' for x in records) else 'ready',
            'runs': records, 'recipe_sha256': digest, 'resource_observations': observations,
            'resource_scope': 'phase_boundary_samples_not_continuous_quota', 'base_model_fits': 0, 'base_model_loads': 0,
            'layer_fits': 0, 'api_calls': 0, 'independent_review': False, **c['readiness']}
    except Exception as exc:
        return {'status': 'review_required', 'reason': str(exc), 'next_action': 'preserve_partial_outputs_and_resolve'}


if __name__ == '__main__':
    p = argparse.ArgumentParser(); p.add_argument('action', choices=['status', 'run', 'resume', 'verify'])
    for n in ('recipe', 'paths', 'runs-dir'): p.add_argument('--'+n, type=Path, required=True)
    p.add_argument('--recipe-sha256', required=True); p.add_argument('--test-crash-run'); p.add_argument('--test-crash-frame', type=int)
    a = p.parse_args(); result = operate(a.action, a.recipe, a.recipe_sha256, read(a.paths), a.runs_dir, a.test_crash_run, a.test_crash_frame)
    print(json.dumps(result, sort_keys=True)); raise SystemExit(2 if result['status'] == 'review_required' else 0)
