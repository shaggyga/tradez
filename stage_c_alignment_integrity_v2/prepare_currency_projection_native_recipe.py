"""Freeze the successor contract and recipe from already authenticated parents."""
import hashlib
import importlib.metadata
import importlib.util
import json
import platform
from pathlib import Path

ROOT = Path(__file__).resolve().parent
WORKSPACE = ROOT.parent
INTAKE = WORKSPACE / 'until8am_20260923_215844/currency_projection_native_intake/NEXT_INPUT_INTAKE.json'
OLD_RECIPE = ROOT / 'CHRONOLOGICAL_NATIVE_OPERATOR_RECIPE_REVIEWED_V2.json'
PROJECTION_CONTRACT = ROOT / 'CURRENCY_PROJECTION_CONTRACT_V2.json'
CONTRACT = ROOT / 'CURRENCY_PROJECTION_NATIVE_CONTRACT_V2.json'
RECIPE = ROOT / 'CURRENCY_PROJECTION_NATIVE_OPERATOR_RECIPE_V2.json'


def read(path):
    return json.loads(path.read_bytes())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def file_descriptor(root, name):
    item = root / name
    return {'bytes': item.stat().st_size, 'sha256': sha(item)}


def main():
    if CONTRACT.exists() or RECIPE.exists():
        raise ValueError('projection_native_contract_or_recipe_already_frozen')
    intake, old_recipe, projection_contract = read(INTAKE), read(OLD_RECIPE), read(PROJECTION_CONTRACT)
    old_contract = old_recipe['contract']
    aliases = intake['native_parent_aliases']
    qualified_root = WORKSPACE / 'until8am_20260923_215844/chronological_native_step/reviewed/runs/later-chronological-native-inputs-v2'
    projection_root = WORKSPACE / 'until8am_20260923_215844/currency_projection_step/runs/currency-factor-forecast-projection-v2'
    if not qualified_root.is_dir() or not projection_root.is_dir():
        raise ValueError('projection_native_local_parent_missing')
    external = {name: file_descriptor(WORKSPACE / 'trad', name) for name in intake['native_external_source_pins']}
    if any(external[name]['sha256'] != digest for name, digest in intake['native_external_source_pins'].items()):
        raise ValueError('projection_native_external_source_drift')
    cohorts = old_contract['cohorts']
    frames = []
    for item in intake['frames']:
        frames.append({'cohort': item['cohort'], 'origin_epoch': item['origin_epoch'], 'target_epoch': item['target_epoch'],
                       'horizon_minutes': item['horizon_minutes'], 'raw_frame': item['projection_source_frame'],
                       'projection_output': item['projection_output']['path'],
                       'qualified_native_frame': item['qualified_native_parent_frame']['path'],
                       'expected_packets': item['expected_new_packets_if_fresh_qualification_passes']})
    if len(frames) != 16 or sum(x['expected_packets'] for x in frames) != 6468:
        raise ValueError('projection_native_intake_inventory_changed')
    contract = {'schema_version': 'forex_currency_projection_native_contract.v1',
        'experiment_id': 'currency_projection_native_policy_inputs_v2',
        'design_requirements': ['8', '15.1', '16.1', '17', '18', '27.3'],
        'hypothesis': 'Fresh saved-base inference followed by the fixed currency-factor projection can be qualified as common-target native inputs without relabelling diagnostic availability.',
        'material_difference': 'Direct, full fixed currency projection and half residual inputs are freshly qualified for both existing later cohorts; no model, learned layer, policy or cost setting changes.',
        'cohorts': cohorts, 'frames': frames, 'universe': old_contract['universe'],
        'horizons_minutes': old_contract['horizons_minutes'], 'parent_surface_contract': old_contract['parent_surface_contract'],
        'chronological_contract': old_contract['chronological_contract'], 'projection_contract': projection_contract,
        'parent_identities': {**old_contract['parent_identities'], 'projection': intake['projection_run_identity'],
                              'qualified_native': intake['native_parent_run_identity']},
        'coverage': {'frames': 16, 'all68_slots': 6528, 'expected_packets_if_fresh_qualification_passes': 6468,
                     'explicit_missing_slots': 60},
        'clock': 'Fresh base inference and fixed projection must finish within one second; complete native preparation within two seconds. Diagnostic availability is retained as provenance only and never reused as a native clock.',
        'maturity': 'No new scoring or policy replay. Preserve original common targets, quote exclusions and unavailable coverage.',
        'negative_result': 'Timing, source, input, solver, base-value, coverage or consumer mismatch refuses the affected run; no backdating, refit or timing relaxation.',
        'resources': {'workers': 1, 'base_model_fit_cap': 0, 'binary_model_load_cap': 16, 'fresh_base_projection_seconds': 1,
                      'native_frame_seconds': 2, 'prewarm_seconds': 120, 'main_wall_seconds': 1500,
                      'checkpoint_wall_seconds': 1800, 'max_input_bytes': 536870912, 'max_member_bytes': 16777216,
                      'max_rss_bytes': 2147483648, 'max_scratch_bytes': 2147483648,
                      'minimum_disk_free_bytes': 8589934592},
        'readiness': {'engineering_ready': False, 'forecast_evidence_status': 'projection_development_diagnostic_only',
                      'policy_evidence_status': 'not_run_for_projection_variants', 'demo_authorization_status': 'not_granted'},
        'base_model_fits': 0, 'learned_layer_fits': 0, 'policy_replays': 0, 'api_calls': 0, 'confirmation': False,
        'independent_review': False}
    CONTRACT.write_text(json.dumps(contract, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    inputs = {alias: old_recipe['inputs'][alias] for alias in ('absolute', 'chronological', 'extension', 'remaining', 'surface')}
    inputs['projection'] = {'original_run_identity': intake['projection_run_identity'], 'files': {
        'RUN_IDENTITY.json': file_descriptor(projection_root, 'RUN_IDENTITY.json'),
        'COMPLETION_MANIFEST.json': file_descriptor(projection_root, 'COMPLETION_MANIFEST.json'),
        **{frame['projection_output']: file_descriptor(projection_root, frame['projection_output']) for frame in frames}}}
    inputs['qualified'] = {'original_run_identity': intake['native_parent_run_identity'], 'files': {
        'RUN_IDENTITY.json': file_descriptor(qualified_root, 'RUN_IDENTITY.json'),
        'COMPLETION_MANIFEST.json': file_descriptor(qualified_root, 'COMPLETION_MANIFEST.json'),
        **{frame['qualified_native_frame']: file_descriptor(qualified_root, frame['qualified_native_frame']) for frame in frames}}}
    spec = importlib.util.spec_from_file_location('currency_projection_native_recipe_source', ROOT / 'currency_projection_native_operator_v2.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    source_names = module.source_names
    environment = {'python': platform.python_version(), **{n: importlib.metadata.version(n) for n in ('numpy','pandas','pyarrow','psutil','scipy','scikit-learn','joblib','threadpoolctl','tzdata')}}
    recipe = {'schema_version': 'forex_currency_projection_native_recipe.v1', 'run_id': 'currency-projection-native-inputs-v2-r3',
              'contract': contract, 'sources': {name: sha(ROOT / name) for name in source_names()}, 'environment': environment,
              'inputs': inputs, 'native_parent_recipe': old_recipe,
              'predecessors': external,
              'native_parent_source_hashes': old_recipe['sources'],
              'absent_import_paths': intake['native_absent_import_paths'], 'solver_pins': intake['projection_solver_pins'],
              'base_model_fits': 0, 'learned_layer_fits': 0, 'policy_replays': 0, 'api_calls': 0,
              'frozen_from_intake_sha256': sha(INTAKE)}
    RECIPE.write_text(json.dumps(recipe, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(json.dumps({'status': 'FROZEN', 'contract_sha256': sha(CONTRACT), 'recipe_sha256': sha(RECIPE),
                      'frames': len(frames), 'expected_packets': 6468}, sort_keys=True))


if __name__ == '__main__':
    main()
