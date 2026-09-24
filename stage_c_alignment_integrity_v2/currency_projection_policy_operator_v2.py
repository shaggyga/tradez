"""Replay the frozen 48-path currency-projection policy matrix without model work."""
import argparse
import hashlib
import json
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
CONTRACT = 'CURRENCY_PROJECTION_POLICY_CONTRACT_V2.json'
SOURCES = ('currency_projection_policy_operator_v2.py', 'currency_projection_policy_fixture_v2.py',
    'currency_projection_policy_input_v2.py', 'historical_native_input_v2.py', 'policy_runner_v2.py', CONTRACT)

def read(path): return json.loads(Path(path).read_bytes())
def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def complete_root(path, expected):
    from publication import verify_completed_run
    identity = read(path / 'RUN_IDENTITY.json')
    if identity['fingerprint'] != expected:
        raise ValueError('currency_projection_policy_parent_identity_changed')
    return verify_completed_run(path, identity)

def preflight(contract_path, contract_sha, paths):
    if sha(contract_path) != contract_sha:
        raise ValueError('currency_projection_policy_contract_pin_mismatch')
    contract = read(contract_path)
    if set(paths) != {'native', 'extension', 'trad'}:
        raise ValueError('currency_projection_policy_exact_path_map_required')
    native, extension = Path(paths['native']), Path(paths['extension'])
    nm = complete_root(native, contract['parent_native_identity'])
    em = complete_root(extension, contract['parent_extension_identity'])
    native_payloads = {x['path']: x['sha256'] for x in nm['payloads']}
    extension_payloads = {x['path']: x['sha256'] for x in em['payloads']}
    for name, cohort in contract['cohorts'].items():
        if 'market_' + name + '.json' not in extension_payloads:
            raise ValueError('currency_projection_policy_market_missing')
        for origin in cohort['origins']:
            item = f'frame_{name}_{origin}.json'
            if item not in native_payloads or sha(native / item) != native_payloads[item]:
                raise ValueError('currency_projection_policy_native_frame_missing_or_changed')
    if shutil.disk_usage(native).free < contract['resources']['reserve_bytes']:
        raise ValueError('currency_projection_policy_disk_reserve_required')
    return contract, {'native_manifest': sha(native / 'COMPLETION_MANIFEST.json'), 'extension_manifest': sha(extension / 'COMPLETION_MANIFEST.json')}

def inputs(contract, paths):
    native, extension = Path(paths['native']), Path(paths['extension'])
    frames, markets = {}, {}
    for name, cohort in contract['cohorts'].items():
        markets[name] = read(extension / ('market_' + name + '.json'))['market']
        for origin in cohort['origins']:
            frames[name, origin] = read(native / f'frame_{name}_{origin}.json')
    return frames, markets

def operate(action, contract_path, contract_sha, paths, runs, crash_run=None, crash_frame=None):
    started = time.monotonic()
    try:
        contract, parents = preflight(contract_path, contract_sha, paths)
        if action == 'status':
            return {'status': 'ready', 'parents': parents, 'expected_runs': contract['expected_run_count'],
                'base_model_fits': 0, 'base_model_loads': 0, 'layer_fits': 0, 'api_calls': 0, 'policy_replays': 0,
                'independent_review': False, **contract['readiness']}
        from currency_projection_policy_fixture_v2 import fixture
        from policy_runner_v2 import run, identity_for, required
        from publication import verify_completed_run
        frames, markets = inputs(contract, paths); records = []; runs = Path(runs); runs.mkdir(parents=True, exist_ok=True)
        for method in contract['methods']:
            for scenario in contract['scenarios']:
                policy, tape, coverage = fixture(contract, frames, markets, method, scenario, Path(paths['trad']))
                if len(coverage) != 544 or len({(x['instrument'], x['conditioning_epoch']) for x in coverage}) != 544:
                    raise ValueError('currency_projection_policy_all68_coverage_required')
                for engine in contract['engines']:
                    run_id = method + '-' + scenario + '-' + engine; root = runs / run_id
                    identity = identity_for(policy, tape, Path(paths['trad']), engine)
                    if action in ('run', 'resume'):
                        run(policy, tape, run_id=run_id, runs_dir=runs, trad_root=Path(paths['trad']), engine=engine,
                            resume=action == 'resume', crash_after=crash_frame if run_id == crash_run else None)
                    if (root / 'COMPLETION_MANIFEST.json').exists():
                        manifest = verify_completed_run(root, identity)
                        if {x['path'] for x in manifest['payloads']} != required(len(tape)):
                            raise ValueError('currency_projection_policy_payload_inventory')
                        report = read(root / 'run_report.json')
                        if report['accounting_oracle']['status'] != 'verified' or report['decision_count'] != 54:
                            raise ValueError('currency_projection_policy_accounting_gate')
                        status = 'completed_verified'
                    else: status = 'resumable' if root.exists() else 'ready'
                    records.append({'run_id': run_id, 'method': method, 'scenario': scenario, 'engine': engine, 'status': status,
                        'run_identity': identity['fingerprint'], 'coverage': coverage, 'path': str(root)})
                    if shutil.disk_usage(runs).free < contract['resources']['reserve_bytes']:
                        raise ValueError('currency_projection_policy_disk_reserve_lost')
        if len(records) != contract['expected_run_count']:
            raise ValueError('currency_projection_policy_matrix_count')
        for method in contract['methods']:
            for scenario in contract['scenarios']:
                pair = [x for x in records if x['method'] == method and x['scenario'] == scenario]
                if all(x['status'] == 'completed_verified' for x in pair):
                    for name in ('run_inputs.json', 'policy_decisions.jsonl', 'event_ledger.jsonl', 'run_report.json'):
                        if sha(Path(pair[0]['path']) / name) != sha(Path(pair[1]['path']) / name):
                            raise ValueError('currency_projection_policy_engine_parity')
        return {'status': 'completed_verified' if all(x['status'] == 'completed_verified' for x in records) else 'resumable',
            'runs': records, 'parents': parents, 'elapsed_seconds': time.monotonic() - started, 'base_model_fits': 0,
            'base_model_loads': 0, 'layer_fits': 0, 'api_calls': 0, 'policy_replays': len(records), 'independent_review': False, **contract['readiness']}
    except Exception as exc:
        return {'status': 'review_required', 'reason': str(exc), 'next_action': 'preserve_partial_outputs_and_resolve'}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('action', choices=['status', 'run', 'resume', 'verify'])
    for name in ('contract', 'paths', 'runs-dir'): parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--contract-sha256', required=True); parser.add_argument('--test-crash-run'); parser.add_argument('--test-crash-frame', type=int)
    args = parser.parse_args(); result = operate(args.action, args.contract, args.contract_sha256, read(args.paths), args.runs_dir, args.test_crash_run, args.test_crash_frame)
    print(json.dumps(result, sort_keys=True)); raise SystemExit(2 if result['status'] == 'review_required' else 0)
