"""Operate the fresh native currency-projection bridge under a frozen recipe."""
import argparse
import ast
import hashlib
import importlib
import importlib.metadata
import json
import os
import platform
import shutil
import sys
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CONTRACT = 'CURRENCY_PROJECTION_NATIVE_CONTRACT_V2.json'
RECIPE = 'CURRENCY_PROJECTION_NATIVE_OPERATOR_RECIPE_V2.json'
NATIVE_PARENT_RECIPE = 'CHRONOLOGICAL_NATIVE_OPERATOR_RECIPE_REVIEWED_V2.json'
NATIVE_PARENT_PIN = 'cbbb4293b5837939594f0e58b7a9b3023364460374e0e455648fbb7c5ec1c449'
PROJECTION_PARENT_RECIPE = 'CURRENCY_PROJECTION_OPERATOR_RECIPE_V2.json'
PROJECTION_PARENT_PIN = 'ef1b24f002891bc84408e24cf084254e27f6165c0af048c51b382bd43ca53e8d'
PACKAGES = ('numpy', 'pandas', 'pyarrow', 'psutil', 'scipy', 'scikit-learn', 'joblib', 'threadpoolctl', 'tzdata')
read = lambda p: json.loads(Path(p).read_bytes())
sha = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
encoded = lambda x: (json.dumps(x, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


def source_names():
    pending = ['currency_projection_native_operator_v2.py', 'currency_projection_native_v2.py']
    seen = set()
    while pending:
        name = pending.pop()
        if name in seen:
            continue
        seen.add(name)
        tree = ast.parse((ROOT / name).read_text(encoding='utf-8-sig'))
        for node in ast.walk(tree):
            modules = ([node.module] if isinstance(node, ast.ImportFrom) and node.module else
                       [x.name for x in node.names] if isinstance(node, ast.Import) else [])
            for module in modules:
                candidate = module.split('.')[0] + '.py'
                if (ROOT / candidate).is_file() and candidate not in seen:
                    pending.append(candidate)
    return sorted(seen | {CONTRACT, 'CURRENCY_PROJECTION_CONTRACT_V2.json',
                          'currency_projection_operator_v2.py', 'currency_projection_v2.py'})


def checked(paths, recipe, alias, name):
    desc = recipe['inputs'][alias]['files'][name]
    root = Path(paths[alias])
    item = root / name
    if Path(name).name != name or root.is_symlink() or root.is_junction() or item.is_symlink() or item.is_junction():
        raise ValueError('projection_native_plain_input_required')
    if item.stat().st_size != desc['bytes'] or desc['bytes'] > recipe['contract']['resources']['max_member_bytes']:
        raise ValueError('projection_native_input_size_changed')
    data = item.read_bytes()
    if hashlib.sha256(data).hexdigest() != desc['sha256']:
        raise ValueError('projection_native_input_hash_changed')
    return data


def load(paths, recipe, alias, name):
    return json.loads(checked(paths, recipe, alias, name))


def _validate_parent_subset(paths, recipe, alias):
    from publication import _validate_inventory, validate_run_identity
    identity = load(paths, recipe, alias, 'RUN_IDENTITY.json')
    manifest = load(paths, recipe, alias, 'COMPLETION_MANIFEST.json')
    validate_run_identity(identity)
    if identity['fingerprint'] != recipe['inputs'][alias]['original_run_identity'] or manifest['run_identity'] != identity:
        raise ValueError('projection_native_parent_identity_changed')
    _validate_inventory(manifest['required_payloads'], manifest['payloads'], identity)
    inventory = {x['path']: x for x in manifest['payloads']}
    for name, desc in recipe['inputs'][alias]['files'].items():
        if name not in ('RUN_IDENTITY.json', 'COMPLETION_MANIFEST.json') and inventory.get(name) != {'path': name, **desc}:
            raise ValueError('projection_native_parent_subset_manifest_mismatch')


def _old_paths(paths):
    return {x: paths[x] for x in ('absolute', 'chronological', 'extension', 'remaining', 'surface', 'trad')}


def _projection_paths(paths):
    return {x: paths[x] for x in ('chronological', 'extension', 'solver')}


def preflight(path, pin, paths):
    if path.is_symlink() or sha(path) != pin:
        raise ValueError('projection_native_recipe_pin_mismatch')
    recipe = read(path)
    if set(recipe['sources']) != set(source_names()) or any(sha(ROOT / n) != h for n, h in recipe['sources'].items()):
        raise ValueError('projection_native_source_changed_before_model_import')
    environment = {'python': platform.python_version(), **{n: importlib.metadata.version(n) for n in PACKAGES}}
    if recipe['environment'] != environment or recipe['contract'] != read(ROOT / CONTRACT):
        raise ValueError('projection_native_environment_contract_mismatch')
    if set(paths) != set(recipe['inputs']) | {'trad', 'solver', 'native_source'}:
        raise ValueError('projection_native_exact_path_map_required')
    if sum(x['bytes'] for d in recipe['inputs'].values() for x in d['files'].values()) > recipe['contract']['resources']['max_input_bytes']:
        raise ValueError('projection_native_input_budget')
    # The old native recipe's source closure is loaded from its reviewed snapshot,
    # not from this workspace after unrelated policy imports were added.
    native_source = Path(paths['native_source'])
    if native_source.is_symlink() or native_source.is_junction():
        raise ValueError('projection_native_parent_source_snapshot_plain_required')
    for name, digest in recipe['native_parent_source_hashes'].items():
        item = native_source / name
        if item.is_symlink() or item.stat().st_size == 0 or sha(item) != digest:
            raise ValueError('projection_native_parent_source_snapshot_changed')
    sys.path.insert(0, str(ROOT))
    sys.path.insert(0, str(native_source))
    native_parent = importlib.import_module('chronological_native_operator_v2')
    native_parent.preflight(native_source / NATIVE_PARENT_RECIPE, NATIVE_PARENT_PIN, _old_paths(paths))
    projection_recipe = ROOT / PROJECTION_PARENT_RECIPE
    if sha(projection_recipe) != PROJECTION_PARENT_PIN:
        raise ValueError('projection_native_projection_parent_recipe_changed')
    projection_parent = read(projection_recipe)
    projection_environment = {'python': platform.python_version(), **{n: importlib.metadata.version(n) for n in ('numpy', 'psutil')}}
    if (any(sha(ROOT / name) != digest for name, digest in projection_parent['sources'].items()) or
            projection_parent['environment'] != projection_environment or
            projection_parent['contract'] != read(ROOT / 'CURRENCY_PROJECTION_CONTRACT_V2.json')):
        raise ValueError('projection_native_projection_parent_source_or_environment_changed')
    for alias in ('projection', 'qualified'):
        _validate_parent_subset(paths, recipe, alias)
    for name, desc in recipe['predecessors'].items():
        item = Path(paths['trad']) / name
        if item.is_symlink() or item.stat().st_size != desc['bytes'] or sha(item) != desc['sha256']:
            raise ValueError('projection_native_external_source_changed_before_import')
    if any((Path(paths['trad']) / n).exists() for n in recipe['absent_import_paths']):
        raise ValueError('projection_native_shadow_import_present')
    for name, desc in recipe['solver_pins'].items():
        item = Path(paths['solver']) / name
        if item.is_symlink() or item.stat().st_size != desc['bytes'] or sha(item) != desc['sha256']:
            raise ValueError('projection_native_solver_changed_before_import')
    return recipe


def required(recipe):
    return sorted([f'{prefix}{frame["cohort"]}_{frame["origin_epoch"]}.json' for frame in recipe['contract']['frames']
                   for prefix in ('frame_', 'consumer_')] + ['training_proof.json', 'source_references.json', 'run_report.json'])


def identity_for(recipe):
    from publication import effective_run_identity
    return effective_run_identity(contract={'recipe': recipe, 'required_payloads': required(recipe)},
        dependency_hashes={**recipe['sources'], **{n: d['sha256'] for n, d in recipe['predecessors'].items()},
                           **{n: d['sha256'] for n, d in recipe['solver_pins'].items()},
                           **{a: d['original_run_identity'] for a, d in recipe['inputs'].items()}})


def operate(action, path, pin, paths, runs, crash_after=None):
    started = time.monotonic()
    recipe = preflight(path, pin, paths)
    contract = recipe['contract']
    from publication import RunPublisher, verify_completed_run
    identity = identity_for(recipe)
    publisher = RunPublisher(runs, recipe['run_id'], identity)
    if (publisher.root / 'COMPLETION_MANIFEST.json').exists():
        manifest = verify_completed_run(publisher.root, identity)
        return {'status': 'completed_verified', 'run_identity': identity['fingerprint'], 'payloads': len(manifest['payloads']),
                'base_model_fits': 0, 'base_model_loads': 0, 'layer_fits': 0, 'api_calls': 0, 'policy_replays': 0, 'reused_completed': True}
    if action in ('status', 'verify'):
        if action == 'verify':
            raise ValueError('projection_native_completed_run_required')
        return {'status': 'resumable' if publisher.root.exists() else 'ready', 'run_identity': identity['fingerprint']}
    publisher.acquire(recover=action == 'resume')
    payloads, timings = [], []
    def put(name, value):
        payloads.append(publisher.write_or_validate_payload(name, encoded(value)))
    def cached(name):
        value = publisher.read_verified_payload(name)
        return None if value is None else json.loads(value)
    def measure(value):
        timings.append(value)
        with (publisher.root / 'TIMING_ATTEMPTS.jsonl').open('ab') as handle:
            handle.write(encoded(value)); handle.flush(); os.fsync(handle.fileno())
    def guard(phase):
        import psutil
        value = {'phase': phase, 'elapsed_seconds': time.monotonic() - started,
                 'rss_bytes': psutil.Process().memory_info().rss,
                 'scratch_bytes': sum(x.stat().st_size for x in publisher.root.iterdir() if x.is_file()),
                 'free_bytes': shutil.disk_usage(runs).free}
        limits = contract['resources']
        if (value['elapsed_seconds'] > limits['main_wall_seconds'] or value['rss_bytes'] > limits['max_rss_bytes'] or
                value['scratch_bytes'] > limits['max_scratch_bytes'] or value['free_bytes'] < limits['minimum_disk_free_bytes']):
            raise ValueError('projection_native_resource_limit:' + json.dumps(value))
        with (publisher.root / 'RESOURCE_ATTEMPTS.jsonl').open('ab') as handle:
            handle.write(encoded(value))
        return value
    try:
        guard('before_inputs')
        old = importlib.import_module('chronological_native_operator_v2')
        from currency_projection_v2 import load_solver
        from currency_projection_native_v2 import build_frame
        from later_surface_models_v2 import SavedModels, schedule
        from later_surface_native_v2 import NativeBatch, consume_verified
        from chronological_layer_v2 import combine, verify_population
        observations, outcomes = [], []
        for pair in contract['universe']:
            part = old.load(paths, recipe['native_parent_recipe'], 'extension', 'pair_' + pair + '.json')
            observations.extend(part['observations']); outcomes.extend(part['outcomes'])
        observations_by_id = {x['record_id']: x for x in observations}
        if len(observations_by_id) != len(observations):
            raise ValueError('projection_native_unique_observations_required')
        parent_recipe = recipe['native_parent_recipe']
        artifacts, proof, rows = {}, [], {}
        parent = contract['parent_surface_contract']
        for horizon in contract['horizons_minutes']:
            signed = old.load(paths, parent_recipe, 'remaining', f'fit_{horizon}.json')
            signed_binary = old.checked(paths, parent_recipe, 'remaining', f'fit_{horizon}.joblib')
            proof.append(verify_population(signed, signed_binary, observations, outcomes))
            alias = 'surface' if horizon in parent['new_absolute_horizons'] else 'absolute'
            stem = f'absolute_{horizon}' if alias == 'surface' else f'fit_{horizon}_{parent["fit_cutoff"]}'
            absolute = old.load(paths, parent_recipe, alias, stem + '.json')
            absolute_binary = old.checked(paths, parent_recipe, alias, stem + '.joblib')
            proof.append(verify_population(absolute, absolute_binary, observations, outcomes, signed))
            artifacts['signed', horizon] = (signed, signed_binary)
            artifacts['absolute', horizon] = (absolute, absolute_binary)
            old_rows = old.load(paths, parent_recipe, 'surface', f'prequential_{horizon}.json')
            new_rows = old.load(paths, parent_recipe, 'chronological', f'new_base_{horizon}.json')
            rows[horizon] = combine(old_rows, new_rows, horizon, contract['chronological_contract'])
        put('training_proof.json', {'saved_fit_pairs': proof, 'base_refits': 0})
        reservations = old.load(paths, parent_recipe, 'surface', 'schedule.json')
        if reservations != schedule(parent):
            raise ValueError('projection_native_original_schedule_changed')
        prewarm = time.monotonic()
        predictor = SavedModels(artifacts, parent, reservations)
        native = NativeBatch(Path(paths['trad']))
        if time.monotonic() - prewarm > contract['resources']['prewarm_seconds']:
            raise ValueError('projection_native_total_prewarm_exceeded')
        solver = load_solver(paths['solver'], recipe['solver_pins'])
        measure({'phase': 'prewarm', 'elapsed_seconds': time.monotonic() - prewarm,
                 'base_model_loads': predictor.loaded, 'base_model_fits': 0, 'native_source_authenticated': True})
        frame_count, packet_count, coverage = 0, 0, Counter()
        consumer_counts, cohort_counts = Counter(), {}
        cohorts = {x['name']: x for x in contract['cohorts']}
        for descriptor in contract['frames']:
            cohort = cohorts[descriptor['cohort']]
            origin = descriptor['origin_epoch']; stem = descriptor['cohort'] + '_' + str(origin)
            item = cached('frame_' + stem + '.json')
            market = old.load(paths, parent_recipe, 'extension', 'market_' + descriptor['cohort'] + '.json')['market']
            if item is None:
                raw = old.load(paths, parent_recipe, 'chronological', descriptor['raw_frame'])
                saved = load(paths, recipe, 'projection', descriptor['projection_output'])
                qualified = load(paths, recipe, 'qualified', descriptor['qualified_native_frame'])
                item, timing = build_frame(cohort, origin, predictor, native, observations, raw, saved, qualified, market,
                                           contract['projection_contract'], solver, contract)
                measure({'phase': 'native_projection_frame', **timing})
            put('frame_' + stem + '.json', item)
            consumers = cached('consumer_' + stem + '.json')
            if consumers is None:
                references = {(x['instrument'], x['price_epoch']): x for x in market['rows']}
                consumers = []
                native_contract = {**parent, 'common_target_epoch': cohort['target'], 'policy_origins': cohort['policy_origins']}
                for prediction, packet in zip(item['predictions'], item['packets']):
                    try:
                        candidate = consume_verified(packet, prediction, observations_by_id[prediction['record_id']],
                                                     references[prediction['instrument'], origin], market, native_contract, Path(paths['trad']))
                        consumers.append({'forecast_id': prediction['forecast_id'], 'status': 'admitted', 'candidate': candidate})
                    except ValueError as exc:
                        if str(exc) != 'financing_conversion_unavailable':
                            raise
                        consumers.append({'forecast_id': prediction['forecast_id'], 'status': 'financing_conversion_unavailable', 'candidate': None})
            put('consumer_' + stem + '.json', consumers)
            if len(item['coverage']) != 408 or len(item['predictions']) != len(item['packets']) or len(consumers) != len(item['predictions']):
                raise ValueError('projection_native_complete_frame_required')
            frame_count += 1; packet_count += len(consumers)
            coverage.update(x['reason'] for x in item['coverage']); consumer_counts.update(x['status'] for x in consumers)
            cohort_counts.setdefault(descriptor['cohort'], Counter()).update(x['status'] for x in consumers)
            guard('after_' + stem)
            if crash_after == frame_count:
                os._exit(91)
        if frame_count != 16 or packet_count != 6468:
            raise ValueError('projection_native_expected_frame_or_packet_count')
        put('run_report.json', {'status': 'completed_currency_projection_native_inputs', 'frames': frame_count,
            'coverage_slots': frame_count * 408, 'native_packets': packet_count, 'coverage_reasons': dict(coverage),
            'consumer_statuses': dict(consumer_counts), 'cohort_consumer_statuses': {k: dict(v) for k, v in cohort_counts.items()},
            'base_model_fits': 0, 'base_model_loads': predictor.loaded, 'learned_layer_fits': 0,
            'api_calls': 0, 'policy_replays': 0, 'confirmation': False, 'independent_review': False, **contract['readiness']})
        put('source_references.json', {'source_hashes': recipe['sources'], 'parent_identities': contract['parent_identities'],
            'solver_pins': recipe['solver_pins'], 'native_source_hashes': recipe['predecessors'],
            'contract_sha256': __import__('contracts').fingerprint(contract)})
        if any(sha(ROOT / n) != digest for n, digest in recipe['sources'].items()):
            raise ValueError('projection_native_source_changed_during_run')
        last = guard('final_precommit')
        publisher.complete(payloads, set(required(recipe)))
        verify_completed_run(publisher.root, identity)
        return {'status': 'completed_verified', 'run_identity': identity['fingerprint'], 'payloads': len(payloads),
                'base_model_fits': 0, 'base_model_loads': predictor.loaded, 'layer_fits': 0, 'api_calls': 0,
                'policy_replays': 0, 'resource_last': last, 'phase_receipts': timings}
    finally:
        if publisher._guard_handle is not None:
            publisher.release()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['status', 'run', 'resume', 'verify'])
    for name in ('recipe', 'paths', 'runs-dir'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--recipe-sha256', required=True)
    parser.add_argument('--test-crash-after', type=int)
    args = parser.parse_args()
    try:
        result = operate(args.action, args.recipe, args.recipe_sha256, read(args.paths), args.runs_dir, args.test_crash_after)
    except Exception as exc:
        result = {'status': 'review_required', 'reason': str(exc)}
    print(json.dumps(result, sort_keys=True))
    raise SystemExit(2 if result['status'] == 'review_required' else 0)
