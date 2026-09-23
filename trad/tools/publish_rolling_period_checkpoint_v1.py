"""Build a compact, explicitly accepted two-period vault checkpoint.

Default actions are read-only. --publish is an explicit later action requiring
the final acceptance SHA256. No estimator is loaded, no fitting occurs, and
raw price/prepared/OOF/forecast rows are never copied. Existing packages are
not modified. The root README gets a pointer below its first title only after
payload and ZIP checks, preserving all of its original bytes.
"""
from __future__ import annotations

import argparse
import ast
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from tools.publish_rolling_specialist_checkpoint_v1 import checked, digest, load, relative, under

VAULT = Path(r'C:\Users\zmoor\OneDrive\thevault\projects\forex')
PACKAGE = 'ROLLING_PERIOD_REPLICATION_20260915'
ZIP_NAME = 'rolling_period_replication_source_20260915.zip'
GUIDE = 'docs/FOREX_ROLLING_PERIOD_REPLICATION_20260915.md'
LEGACY = '../direction_decision_20260911/src/signed_cost_models_v1.py'
LEGACY_SHA = '8cebabbab7c6dcb32271a8fd3cbc656e912ae6a26e19a9ff1d35ad547342665c'
DEPENDENCIES = {
    'ROLLING_SPECIALISTS_20260915': '9d3c08c687778242930847a3ef59eba1eccaf0523370f37d7ecaeb5cf3df5d03',
    'ROLLING_MODEL_COMPARISON_20260915': 'de436d7db0e79567d6071fd1f66d61804e76d5da1ccea9243c7cac17a1af939f',
    'ROLLING_TRAINING_WINDOWS_20260915': 'd9da5d6e8b4c90587ab69023737129a2ccc6e111b8b2b6205a5fbb343ada2ef3',
}
EXPECTED_CONTEXTS = {f'{g}_{h}m' for g in ('compact38', 'compact50') for h in (30, 60)}
META_ARMS = {'direct_ridge', 'direct_context_hgb'}
MEAN_ARMS = META_ARMS | {'direct', 'mixture_raw', 'mixture_calibrated'}
FAMILY_NAMES = {'full_compact50', 'drop_peers', 'drop_returns_path', 'drop_trend_oscillator',
                'drop_range_volatility', 'drop_activity_spread_history', 'drop_calendar', 'context_only'}
ROW_SUFFIXES = {'.parquet', '.csv', '.tsv', '.sqlite', '.sqlite3', '.db', '.feather', '.arrow'}
MAX_PAYLOAD = 512 * 1024**2


def _sha(value):
    if not isinstance(value, str) or len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
        raise ValueError('exact_lowercase_sha256_required')
    return value


def _project_path(project, value):
    p = Path(value)
    p = p.resolve() if p.is_absolute() else under(project, value)
    if not p.is_relative_to(project):
        raise ValueError('accepted_file_must_stay_inside_project')
    return p


def _source_path(project, name):
    name = name.replace('\\', '/')
    if name == LEGACY:
        return (project.parent/'direction_decision_20260911/src/signed_cost_models_v1.py').resolve()
    return under(project, name)


def _source_destination(name):
    return ('source/forex/direction_decision_20260911/src/signed_cost_models_v1.py'
            if name == LEGACY else 'source/forex/trad/'+relative(name))


def _merge_bindings(target, source):
    for name, value in source.items():
        name = name.replace('\\', '/')
        sha = _sha(value if isinstance(value, str) else value['sha256'])
        if name in target and target[name] != sha:
            raise ValueError('conflicting_exact_source_binding:'+name)
        target[name] = sha


def local_import_closure(project, names):
    """Find local Python dependencies without executing any project module.

    This reports dependencies; it never silently accepts their current hashes.
    Every discovered file must already have a final acceptance/source pin.
    """
    project = Path(project).resolve(); visited, pending = set(), list(names)
    while pending:
        name = pending.pop()
        if name in visited or not name.endswith('.py'):
            continue
        path = _source_path(project, name)
        tree = ast.parse(path.read_bytes(), filename=str(path)); visited.add(name)
        candidates = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                if node.level:
                    base = path.parent
                    for _ in range(node.level-1):
                        base = base.parent
                    prefix = base/((node.module or '').replace('.', '/'))
                    candidates += [prefix.with_suffix('.py'), prefix/'__init__.py']
                    candidates += [prefix/(a.name+'.py') for a in node.names if a.name != '*']
                    continue
                modules = [node.module or '']
                modules += [(node.module+'.'+a.name) for a in node.names
                            if node.module and a.name != '*']
            else:
                continue
            for module in modules:
                if not module:
                    continue
                parts = module.split('.')
                prefix = project.joinpath(*parts)
                candidates += [prefix.with_suffix('.py'), prefix/'__init__.py']
                candidates += [project.joinpath(*parts[:i])/'__init__.py' for i in range(1, len(parts))]
        for candidate in candidates:
            candidate = candidate.resolve()
            if not candidate.is_file():
                continue
            if candidate.is_relative_to(project):
                dep = candidate.relative_to(project).as_posix()
            elif candidate == _source_path(project, LEGACY):
                dep = LEGACY
            else:
                raise ValueError('local_import_escaped_allowed_source_tree:'+str(candidate))
            if dep not in visited:
                pending.append(dep)
    return visited


def _normalizers(project, record, inputs, input_sha):
    import numpy as np
    path = _project_path(project, record['path'])
    meta, receipt = load(path, _sha(record['sha256']))
    if meta['source_manifest_sha256'] != input_sha:
        raise ValueError('normalizer_source_manifest_mismatch')
    artifact = meta['artifact']; src = under(path.parent, artifact['path'])
    a_receipt = checked(src, artifact['sha256'], artifact.get('bytes'))
    required = {'count', 'mean', 'scale', 'supported', 'feature_names', 'pair_names', 'fold_cutoffs_epoch'}
    with np.load(src, allow_pickle=False) as values:
        if set(values.files) != required or set(meta['arrays']) != required:
            raise ValueError('only_compact_exact_normalizer_arrays_allowed')
        for name in required:
            x = values[name]; description = meta['arrays'][name]
            if (list(x.shape) != description['shape'] or str(x.dtype) != description['dtype']
                    or hashlib.sha256(x.tobytes(order='C')).hexdigest() != description['c_order_payload_sha256']):
                raise ValueError('normalizer_array_shape_dtype_or_payload_mismatch:'+name)
        if any(values[k].shape != (5, 68, 50) for k in ('count', 'mean', 'scale', 'supported')):
            raise ValueError('five_prefix_all68_fifty_feature_normalizers_required')
        if (values['feature_names'].tolist() != inputs['feature_names']
                or values['pair_names'].tolist() != sorted(inputs['pairs'])
                or values['fold_cutoffs_epoch'].tolist() != inputs['fold_cutoffs_epoch']):
            raise ValueError('normalizer_order_and_cutoffs_must_match_inputs')
    checked(path, receipt['sha256']); checked(src, a_receipt['sha256'])
    return receipt, a_receipt


def validate_manifest_chain(inputs, prepared, quotes, base, endpoint, boundaries):
    """Validate identities before treating metadata as recreation evidence."""
    if (any(m.get('status') != 'complete' or m.get('boundaries') != boundaries
            for m in (inputs, prepared, quotes, base)) or endpoint.get('status') != 'complete'):
        raise ValueError('input_manifest_window_identity_required')
    if (prepared['base_sha256'] != inputs['base_sha256']
            or quotes['base_manifest_sha256'] != inputs['base_sha256']
            or endpoint['base_manifest_sha256'] != inputs['base_sha256']
            or inputs['endpoint_manifest_sha256'] != prepared['overlay_sha256']
            or quotes['endpoint_manifest_sha256'] != prepared['overlay_sha256']):
        raise ValueError('endpoint_quote_prepared_base_identity_required')
    pairs = set(inputs['pairs'])
    if len(pairs) != 68 or any(set(m['pairs']) != pairs for m in (prepared, quotes, base, endpoint)):
        raise ValueError('same_all68_input_manifest_populations_required')


def static_plan():
    return {'schema': 'rolling_period_checkpoint_plan_v1', 'status': 'plan_only_no_run_results_read',
            'target_package': PACKAGE, 'writes_performed': False,
            'required_windows': 2,
            'per_window_models': {'six_head_bundles': 27, 'individual_base_estimators': 162,
                                  'meta_models': 8, 'original_recipe_comparator_models': 8,
                                  'calibrators': 4, 'pair_priors': 2, 'component_priors': 2},
            'acceptance_interface': {
                'status': 'accepted_research_checkpoint', 'can_place_orders': False, 'models_promoted': 0,
                'windows': [{'comparison_root': 'data/<run>', 'results_sha256': '<exact>',
                             'audit': {'path': 'docs/validation/.../AUDIT.json', 'sha256': '<exact>'},
                             'normalizers': {'path': 'docs/validation/.../NORMALIZERS.json', 'sha256': '<exact>'}}],
                'summary': {'path': 'docs/validation/.../summary/SUMMARY.json', 'sha256': '<exact>'},
                'source_bindings': {'module.py': '<exact>'}, 'test_bindings': {'test_module.py': '<exact>'},
                'evidence_files': {'name': {'path': 'docs/validation/...', 'sha256': '<exact>'}},
                'project_documents': {'guide': {'path': GUIDE, 'sha256': '<exact>'}},
                'config_files': {'commands': {'path': 'docs/validation/.../configs/EXECUTED.json', 'sha256': '<exact>'}}},
            'source_closure': 'all exact local Python imports, including tests/helpers and the sibling legacy source',
            'included': ['saved models and priors', 'all compact metrics/diagnostics', 'two exact normalizer extracts',
                         'accepted audits/summary/tests/documents/configs', 'source manifests and population inventories'],
            'excluded': ['raw prices/quotes', 'prepared matrices', 'OOF arrays', 'forecast Parquet', 'databases', 'credentials'],
            'predecessor_manifest_pins': DEPENDENCIES,
            'publication': 'new package only; full copy/hash and ZIP readback before inserting a pointer below the root README title; all original README bytes preserved; no cleanup',
            'cloud_sync_verified': False}


def preflight(project, vault, acceptance_path, acceptance_sha):
    project, vault, acceptance_path = map(lambda p: Path(p).resolve(), (project, vault, acceptance_path))
    acceptance_path = _project_path(project, str(acceptance_path))
    acceptance, acceptance_receipt = load(acceptance_path, _sha(acceptance_sha))
    if (not str(acceptance.get('status', '')).startswith('accepted')
            or acceptance.get('can_place_orders') is not False or acceptance.get('models_promoted') != 0):
        raise ValueError('final_unpromoted_research_acceptance_required')
    if len(acceptance.get('windows', [])) != 2:
        raise ValueError('two_accepted_complete_windows_required')
    if (vault/PACKAGE).exists():
        raise ValueError('dated_vault_package_already_exists')
    readme = checked(vault/'README.md')
    (vault/'README.md').read_bytes().decode('utf-8-sig')
    files, bindings, accepted_paths = {}, {}, {}
    manifests, windows, dependency_rows = [], [], []

    def add(path, destination, category, sha=None, byte_count=None):
        path = Path(path).resolve(); destination = relative(destination)
        suffix = path.suffix.lower()
        if (suffix in ROW_SUFFIXES or suffix == '.npz' and path.name != 'NORMALIZERS.npz'
                or suffix in {'.pem', '.key', '.pfx', '.p12', '.env'}
                or path.name.lower() in {'.env', 'credentials.json', 'secrets.json'}):
            raise ValueError('excluded_raw_or_private_artifact:'+str(path))
        value = {**checked(path, sha, byte_count), 'path': destination, 'category': category}
        if destination in files and (files[destination]['sha256'], files[destination]['bytes']) != (value['sha256'], value['bytes']):
            raise ValueError('conflicting_package_destination:'+destination)
        files.setdefault(destination, value)

    for field in ('evidence_files', 'project_documents', 'config_files'):
        if not acceptance.get(field):
            raise ValueError('acceptance_must_explicitly_bind_'+field)
        for name, rec in acceptance[field].items():
            p = _project_path(project, rec.get('path', name)); key = str(p)
            if key in accepted_paths and accepted_paths[key] != rec['sha256']:
                raise ValueError('conflicting_accepted_evidence_pin')
            accepted_paths[key] = _sha(rec['sha256'])
            add(p, 'source/forex/trad/'+p.relative_to(project).as_posix(), field, rec['sha256'], rec.get('bytes'))
    if str(project/GUIDE) not in accepted_paths:
        raise ValueError('final_dated_study_guide_required')
    add(acceptance_path, 'source/forex/trad/'+acceptance_path.relative_to(project).as_posix(), 'acceptance', acceptance_sha)

    def accepted_record(rec):
        p = _project_path(project, rec['path'])
        if accepted_paths.get(str(p)) != rec['sha256']:
            raise ValueError('required_record_missing_from_accepted_evidence:'+str(p))
        return p

    def compact_manifest(path, expected, destination):
        path = Path(path).resolve()
        if not path.is_relative_to(project.parent):
            raise ValueError('dependency_manifest_must_stay_in_forex_tree')
        obj, rec = load(path, expected)
        add(path, destination, 'source_manifest_or_population_inventory', rec['sha256'])
        manifests.append(obj)
        return obj, rec

    for i, w in enumerate(acceptance['windows']):
        run = _project_path(project, w['comparison_root'])
        results, result_rec = load(run/'RESULTS.json', _sha(w['results_sha256']))
        counters = {'completed_base_bundles': 20, 'completed_variants': 20, 'completed_family_refits': 7,
                    'completed_family_mean_variants': 16, 'completed_comparator_fits': 8}
        if (results.get('schema') != 'rolling_specialist_replication_v2_20260915' or results.get('status') != 'complete'
                or any(results.get(k) != v for k, v in counters.items())
                or set(results['contexts']) != EXPECTED_CONTEXTS or set(results['family_contexts']) != FAMILY_NAMES
                or len(results['comparators']) != 18 or results.get('can_place_orders') is not False
                or results.get('models_promoted') != 0):
            raise ValueError('complete_fixed_v2_window_required')
        b = results['boundaries']; label = datetime.fromtimestamp(b['start'], timezone.utc).strftime('%Y%m%d')+'_'+datetime.fromtimestamp(b['end'], timezone.utc).strftime('%Y%m%d')
        prefix = 'evidence/windows/'+label
        add(run/'RESULTS.json', prefix+'/comparison/RESULTS.json', 'complete_results', result_rec['sha256'])
        manifests.append(results)
        audit_path = accepted_record(w['audit']); audit, audit_rec = load(audit_path, w['audit']['sha256'])
        if audit.get('status') != 'passed' or audit.get('results_sha256') != result_rec['sha256'] or Path(audit['result_root']).resolve() != run:
            raise ValueError('passed_independent_audit_for_exact_window_required')
        manifests.append(audit)
        inputs_root = _project_path(project, results['inputs_root'])
        im, ir = compact_manifest(inputs_root/'SPECIALIST_INPUTS.json', results['inputs_sha256'], prefix+'/inputs/SPECIALIST_INPUTS.json')
        if im.get('status') != 'complete' or len(im.get('pairs', {})) != 68 or im['boundaries'] != b:
            raise ValueError('complete_same_window_all68_inputs_required')
        pm, _ = compact_manifest(_project_path(project, im['prepared_root'])/'PREPARED.json', im['prepared_sha256'], prefix+'/inputs/PREPARED.json')
        qm, _ = compact_manifest(_project_path(project, im['quote_root'])/'QUOTE_PANEL.json', im['quote_sha256'], prefix+'/inputs/QUOTE_PANEL.json')
        bm, _ = compact_manifest(_project_path(project, im['base_root'])/'DATASET.json', im['base_sha256'], prefix+'/inputs/DATASET.json')
        em, _ = compact_manifest(_project_path(project, pm['overlay'])/'ENDPOINT_DATASET.json', pm['overlay_sha256'], prefix+'/inputs/ENDPOINT_DATASET.json')
        validate_manifest_chain(im, pm, qm, bm, em, b)
        pop, pop_rec = compact_manifest(Path(bm['source_manifest']), bm['source_manifest_sha256'], prefix+'/inputs/PRICE_POPULATION_MANIFEST.json')
        metadata_path = _project_path(project, bm['pair_metadata_path'])
        if accepted_paths.get(str(metadata_path)) != bm['pair_metadata_sha256']:
            raise ValueError('exact_pair_metadata_config_must_be_accepted')
        normalizer_path = accepted_record(w['normalizers'])
        nm, na = _normalizers(project, w['normalizers'], im, ir['sha256'])
        if accepted_paths.get(na['source_path']) != na['sha256']:
            raise ValueError('normalizer_NPZ_must_be_accepted_evidence')

        def artifact(rec, category):
            p = under(run, rec['path'])
            add(p, prefix+'/comparison/'+relative(rec['path']), category, rec['sha256'], rec.get('bytes'))

        for tag, ctx in results['contexts'].items():
            if len(ctx['oof_models']) != 4 or set(ctx['meta_models']) != META_ARMS or set(ctx['variants']) != MEAN_ARMS:
                raise ValueError('complete_five_mean_replication_model_states_required')
            for rec in [*ctx['oof_models'], ctx['final_model']]:
                artifact(rec, 'six_head_bundle')
            for rec in ctx['meta_models'].values():
                artifact(rec, 'meta_model')
            artifact(ctx['calibration'], 'calibrator')
            artifact(ctx['probability_calibration_scores'], 'probability_diagnostics')
            artifact(ctx['component_baseline_scores'], 'component_baseline_diagnostics')
            for variant in ctx['variants'].values():
                artifact(variant['metrics'], 'variant_metrics'); artifact(variant['diagnostics'], 'component_diagnostics')
        anchor = results['contexts']['compact50_60m']
        for group, ctx in results['family_contexts'].items():
            if ctx['horizon_minutes'] != 60 or set(ctx['variants']) != {'direct', 'mixture_raw'}:
                raise ValueError('fixed_H1_family_scope_required')
            if group == 'full_compact50':
                if (ctx['model'] != anchor['final_model'] or ctx['head_forecasts'] != anchor['head_forecasts']
                        or ctx['component_baseline_scores'] != anchor['component_baseline_scores']
                        or any(ctx['variants'][v] != anchor['variants'][v] for v in ('direct', 'mixture_raw'))):
                    raise ValueError('full_family_anchor_must_be_exact_reuse')
                continue
            artifact(ctx['model'], 'six_head_bundle')
            artifact(ctx['component_baseline_scores'], 'component_baseline_diagnostics')
            for variant in ctx['variants'].values():
                artifact(variant['metrics'], 'variant_metrics'); artifact(variant['diagnostics'], 'component_diagnostics')
        for rec in results['comparators'].values():
            if 'model' in rec:
                artifact(rec['model'], 'original_recipe_comparator_model')
            artifact(rec['metrics'], 'comparator_metrics')
        for field in ('pair_priors', 'component_priors'):
            if set(results[field]) != {'30', '60'}:
                raise ValueError('both_horizon_prior_states_required')
            for rec in results[field].values():
                artifact(rec, field)
        # Preserve metadata for local raw dependencies; no row payload read.
        dependency_rows.append({'window': label, 'comparison_root': str(run), 'results_sha256': result_rec['sha256'],
            'inputs_root': str(inputs_root), 'inputs_sha256': ir['sha256'],
            'base_root': im['base_root'], 'base_sha256': im['base_sha256'],
            'prepared_root': im['prepared_root'], 'prepared_sha256': im['prepared_sha256'],
            'quote_root': im['quote_root'], 'quote_sha256': im['quote_sha256'],
            'endpoint_root': pm['overlay'], 'endpoint_sha256': pm['overlay_sha256'],
            'population_manifest': pop_rec, 'manifest_verification': 'exact original metadata checked; raw row payloads not rehashed or duplicated by publisher',
            'excluded_result_row_records': {name: rec for name, rec in results['artifacts'].items() if Path(name).suffix in {'.npz', '.parquet'}},
            'versions': results.get('versions', {})})
        windows.append({'label': label, 'comparison_root': str(run), 'results_sha256': result_rec['sha256'],
                        'boundaries': b, 'assessment_rows': results['assessment_rows'], 'audit_sha256': audit_rec['sha256'],
                        'input_manifest_sha256': ir['sha256'], 'normalizer_metadata_sha256': nm['sha256'],
                        'normalizer_npz_sha256': na['sha256']})

    windows.sort(key=lambda w: w['boundaries']['start'])
    if windows[0]['boundaries']['end'] > windows[1]['boundaries']['start']:
        raise ValueError('two_nonoverlapping_historical_windows_required')
    summary_path = accepted_record(acceptance['summary'])
    summary, summary_rec = load(summary_path, acceptance['summary']['sha256'])
    if (summary.get('schema') != 'rolling_period_replication_summary_v1_20260915'
            or summary.get('window_count') != 2 or summary.get('models_promoted') != 0
            or {w['results_sha256'] for w in summary['windows']} != {w['results_sha256'] for w in windows}):
        raise ValueError('complete_summary_for_the_two_accepted_windows_required')
    manifests += [summary, acceptance]
    for obj in manifests:
        _merge_bindings(bindings, obj.get('source_bindings', {}))
        _merge_bindings(bindings, obj.get('summary_source_bindings', {}))
    # Summary self-pins can be absolute. Canonicalize only exact project files.
    for name in list(bindings):
        if Path(name).is_absolute():
            canonical = _project_path(project, name).relative_to(project).as_posix()
            sha = bindings.pop(name)
            if canonical in bindings and bindings[canonical] != sha:
                raise ValueError('absolute_source_binding_conflict')
            bindings[canonical] = sha
    tests = {}; _merge_bindings(tests, acceptance.get('test_bindings', {}))
    if not tests or bindings.get(LEGACY) != LEGACY_SHA:
        raise ValueError('accepted_tests_and_exact_legacy_source_required')
    _merge_bindings(bindings, tests)
    publisher_name = 'tools/publish_rolling_period_checkpoint_v1.py'
    if publisher_name not in bindings:
        raise ValueError('final_publisher_source_must_be_acceptance_pinned')
    discovered = local_import_closure(project, bindings)
    missing = sorted(discovered-set(bindings))
    if missing:
        raise ValueError('local_imports_missing_final_source_pins:'+','.join(missing))
    for name, sha in sorted(bindings.items()):
        add(_source_path(project, name), _source_destination(name), 'accepted_test' if name in tests else 'accepted_source', sha)

    predecessors = []
    pins = acceptance.get('predecessor_manifests', DEPENDENCIES)
    if pins != DEPENDENCIES:
        raise ValueError('known_predecessor_manifest_pins_required')
    for package, expected in pins.items():
        m, rec = load(vault/package/'MANIFEST.json', expected)
        for entry in m['files']:
            checked(under(vault/package, entry['path']), entry['sha256'], entry['bytes'])
        predecessors.append({'package': package, 'manifest': rec, 'payload_files_verified': len(m['files'])})
    payload_bytes = sum(r['bytes'] for r in files.values())
    if payload_bytes > MAX_PAYLOAD:
        raise ValueError('compact_payload_budget_exceeded')
    for rec in files.values():
        checked(Path(rec['source_path']), rec['sha256'], rec['bytes'])
    checked(vault/'README.md', readme['sha256'], readme['bytes'])
    return {'schema': 'rolling_period_checkpoint_preflight_v1', 'status': 'complete_read_only_preflight',
            'project': str(project), 'vault': str(vault), 'package': PACKAGE, 'acceptance': acceptance_receipt,
            'summary_sha256': summary_rec['sha256'], 'windows': windows,
            'files': [files[k] for k in sorted(files)], 'payload_bytes': payload_bytes,
            'category_counts': dict(Counter(r['category'] for r in files.values())),
            'exact_local_python_import_closure': sorted(discovered), 'predecessors': predecessors,
            'local_data_dependencies': dependency_rows, 'vault_readme_before': readme,
            'writes_performed': False, 'cloud_sync_verified': False}


def _new_bytes(path, raw):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as f:
        f.write(raw); f.flush(); os.fsync(f.fileno())


def _json_bytes(obj):
    return (json.dumps(obj, indent=2, sort_keys=True, allow_nan=False)+'\n').encode('utf-8')


def verify_payload(package, manifest):
    names = [r['path'] for r in manifest['files']]
    if len(names) != len(set(names)):
        raise ValueError('duplicate_manifest_payload_path')
    actual = {p.relative_to(package).as_posix() for p in package.rglob('*') if p.is_file()}
    if actual-set(names)-{'MANIFEST.json', ZIP_NAME, 'PUBLICATION.json'}:
        raise ValueError('unexpected_unmanifested_package_file')
    for rec in manifest['files']:
        checked(under(package, rec['path']), rec['sha256'], rec['bytes'])
    return {'payload_files_verified': len(names), 'payload_bytes_verified': sum(r['bytes'] for r in manifest['files'])}


def create_and_verify_zip(package, manifest):
    path = package/ZIP_NAME
    expected = {r['path']: r for r in manifest['files']}
    expected['MANIFEST.json'] = checked(package/'MANIFEST.json')
    with zipfile.ZipFile(path, 'x', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        for name in sorted(expected):
            z.write(under(package, name), arcname=name)
    with zipfile.ZipFile(path, 'r') as z:
        if len(z.infolist()) != len(expected) or set(z.namelist()) != set(expected):
            raise ValueError('ZIP_must_contain_exact_manifest_and_payload')
        if z.testzip() is not None:
            raise ValueError('ZIP_CRC_failure')
        for name, rec in expected.items():
            h = hashlib.sha256(); count = 0
            with z.open(name) as f:
                for block in iter(lambda: f.read(1024*1024), b''):
                    h.update(block); count += len(block)
            if count != rec['bytes'] or h.hexdigest() != rec['sha256']:
                raise ValueError('ZIP_entry_hash_or_size_mismatch:'+name)
    return {**checked(path), 'entries': len(expected), 'crc_pass': True, 'all_entry_hashes_and_sizes_verified': True}


def append_readme(path, original_bytes, addition):
    """Append only, with original byte prefix verified before and afterward."""
    if path.read_bytes() != original_bytes:
        raise ValueError('vault_README_changed_before_append')
    with path.open('ab') as f:
        f.write(addition); f.flush(); os.fsync(f.fileno())
    after = path.read_bytes()
    if after != original_bytes+addition:
        raise ValueError('vault_README_concurrent_change_or_append_mismatch')
    return {'before_sha256': hashlib.sha256(original_bytes).hexdigest(),
            'after_sha256': hashlib.sha256(after).hexdigest(), 'original_bytes_preserved': len(original_bytes)}


def insert_readme_after_title(path, original_bytes, addition):
    """Insert below the first H1 line, preserving BOM and mixed line endings.

    A same-handle check immediately precedes the write; final exact readback
    and removal of just the insertion must reconstruct the original bytes.
    Concurrent change is an error, never a reason to overwrite or roll back
    the other writer's content. No file truncation is performed.
    """
    offset = 0
    for number, line in enumerate(original_bytes.splitlines(keepends=True)):
        candidate = line[3:] if number == 0 and line.startswith(b'\xef\xbb\xbf') else line
        offset += len(line)
        if candidate.startswith((b'# ', b'#\t')):
            break
    else:
        raise ValueError('vault_README_first_title_required')
    if not isinstance(addition, bytes) or not addition:
        raise ValueError('nonempty_readme_insertion_bytes_required')
    expected = original_bytes[:offset]+addition+original_bytes[offset:]
    with path.open('r+b') as f:
        if f.read() != original_bytes:
            raise ValueError('vault_README_changed_before_insert')
        f.seek(0)
        f.write(expected); f.flush(); os.fsync(f.fileno())
    after = path.read_bytes()
    if after != expected:
        raise ValueError('vault_README_concurrent_change_or_insert_mismatch')
    if after[:offset]+after[offset+len(addition):] != original_bytes:
        raise ValueError('vault_README_original_byte_reconstruction_failed')
    return {'before_sha256': hashlib.sha256(original_bytes).hexdigest(),
            'after_sha256': hashlib.sha256(after).hexdigest(),
            'original_bytes_preserved': len(original_bytes),
            'navigation_placement': 'immediately_after_first_title_line',
            'insertion_offset': offset, 'inserted_bytes': len(addition),
            'insertion_sha256': hashlib.sha256(addition).hexdigest(),
            'original_byte_reconstruction_verified': True}


def publish(project, vault, acceptance_path, acceptance_sha, receipt_path=None):
    plan = preflight(project, vault, acceptance_path, acceptance_sha)
    project, vault = Path(plan['project']), Path(plan['vault'])
    receipt_path = _project_path(project, str(receipt_path or Path(acceptance_path).resolve().parent/'VAULT_PUBLICATION.json'))
    if receipt_path.exists():
        raise ValueError('publication_receipt_already_exists')
    if shutil.disk_usage(vault).free < plan['payload_bytes']*2+128*1024**2:
        raise ValueError('insufficient_space_for_payload_ZIP_and_buffer')
    package = vault/PACKAGE
    if package.resolve().parent != vault or package.exists():
        raise ValueError('new_exact_dated_package_required')
    original_readme = (vault/'README.md').read_bytes()
    if hashlib.sha256(original_readme).hexdigest() != plan['vault_readme_before']['sha256']:
        raise ValueError('vault_README_changed_after_preflight')
    package.mkdir(exist_ok=False)
    copied = []
    for rec in plan['files']:
        src, dst = Path(rec['source_path']), under(package, rec['path'])
        checked(src, rec['sha256'], rec['bytes']); dst.parent.mkdir(parents=True, exist_ok=True)
        with src.open('rb') as a, dst.open('xb') as b:
            shutil.copyfileobj(a, b, length=1024*1024)
        checked(dst, rec['sha256'], rec['bytes']); checked(src, rec['sha256'], rec['bytes'])
        copied.append(dict(rec))
    guide = 'source/forex/trad/'+GUIDE
    README = ('# Rolling period replication checkpoint\n\n'
              'Two completed historical windows, independent audits and the accepted summary are retained here. '
              'Models remain research candidates; this checkpoint does not enable trading.\n\n'
              f'- [Study and results]({guide})\n'
              '- [Recreation](RECREATE.md)\n- [Exact file manifest](MANIFEST.json)\n'
              '- [Local data dependencies](DEPENDENCIES.json)\n\n'
              'This package includes saved model states, exact normalizers and the complete pinned Python source closure. '
              'Full prepared data, OOF arrays, forecasts and raw quotes/prices remain local dependencies. '
              'Local verification does not establish cloud synchronization.\n').encode()
    recreation = ('# Recreation\n\n'
                  f'The accepted [guide]({guide}) and copied executed-command/config receipts give the exact window commands and parameters.\n\n'
                  'The source tree preserves forex/trad and its sibling forex/direction_decision_20260911/src. '
                  'Use the recorded timeseries312 runtime and the package versions retained with each RESULTS.json. '
                  'Do not apply an earlier full-TRAIN normalizer to an earlier OOF prefix.\n\n'
                  'Each window retains four prefix and final six-head models, direct-only/context combiners, calibrators, '
                  'seven additional masked-family fits and the original-recipe comparators. Its exact five-prefix normalizer '
                  'NPZ and metadata are in the accepted source/forex/trad/docs/validation evidence paths. Family models keep '
                  '53 slots: 50 technical fields plus known long/short entry costs and categorical pair ID.\n\n'
                  'Restore compatible registered feature inputs and retained normalization to replay saved inference. '
                  'For complete refitting/evaluation, restore the exact local base/endpoint/quote/prepared datasets listed '
                  'in DEPENDENCIES.json, or regenerate them with the retained source manifests, inventories and commands. '
                  'The package intentionally does not duplicate those row datasets or claim that they are embedded.\n\n'
                  'Previous dated packages remain separate pinned historical references. No archived code is executed by publication.\n').encode()
    generated = {'README.md': README, 'RECREATE.md': recreation,
                 'DEPENDENCIES.json': _json_bytes({'schema': 'rolling_period_local_dependencies_v1',
                       'windows': plan['local_data_dependencies'], 'predecessors': plan['predecessors'],
                       'raw_payloads_copied': False}),
                 'before/README.md': original_readme,
                 'PUBLICATION_PLAN.json': _json_bytes(plan)}
    for name, raw in generated.items():
        p = under(package, name); _new_bytes(p, raw)
        copied.append({'path': name, 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(), 'category': 'generated_recreation_or_preserved_navigation'})
    manifest = {'schema': 'rolling_period_vault_manifest_v1', 'package': PACKAGE,
                'created_utc': datetime.now(timezone.utc).isoformat(), 'acceptance_sha256': acceptance_sha,
                'windows': plan['windows'], 'files': sorted(copied, key=lambda r: r['path']),
                'payload_bytes': sum(r['bytes'] for r in copied),
                'exclusions': [ZIP_NAME, 'PUBLICATION.json', 'manifest_self_hash'],
                'models_promoted': 0, 'can_place_orders': False, 'cloud_sync_verified': False}
    _new_bytes(package/'MANIFEST.json', _json_bytes(manifest))
    payload_check = verify_payload(package, manifest)
    z = create_and_verify_zip(package, manifest)
    manifest_receipt = checked(package/'MANIFEST.json')
    for previous in plan['predecessors']:
        rec = previous['manifest']; checked(Path(rec['source_path']), rec['sha256'], rec['bytes'])
    checked(Path(plan['acceptance']['source_path']), acceptance_sha)
    for rec in plan['files']:
        checked(Path(rec['source_path']), rec['sha256'], rec['bytes'])
    addition = (f'\n\n## Rolling period replication checkpoint — September 15, 2026\n\n'
                f'Two separate historical windows and the H1 feature-family comparison are recorded in '
                f'[{PACKAGE}]({PACKAGE}/README.md), with accepted audits, summary, saved models and recreation sources. '
                'Trading settings are unchanged. Large historical and forecast datasets remain local dependencies.\n').encode('utf-8')
    readme_check = insert_readme_after_title(vault/'README.md', original_readme, addition)
    receipt = {'schema': 'rolling_period_vault_publication_v1', 'status': 'complete_local_copy_verified',
               'created_utc': datetime.now(timezone.utc).isoformat(), 'package_root': str(package),
               'acceptance_sha256': acceptance_sha, 'manifest': manifest_receipt, 'zip': z,
               **payload_check, 'windows': plan['windows'], 'category_counts': plan['category_counts'],
               'source_closure_files': len(plan['exact_local_python_import_closure']),
               'predecessors': plan['predecessors'], 'vault_readme': readme_check,
               'before_readme_preserved': 'before/README.md', 'raw_price_prepared_OOF_forecast_rows_copied': False,
               'older_packages_modified': False, 'models_loaded_or_fitted': False,
               'cloud_sync_verified': False, 'live_collection_or_trading_changed': False,
               'external_sealing_receipts_excluded_from_manifest_and_ZIP': ['PUBLICATION.json', str(receipt_path)]}
    raw = _json_bytes(receipt)
    _new_bytes(package/'PUBLICATION.json', raw)
    _new_bytes(receipt_path, raw)
    checked(package/'PUBLICATION.json', hashlib.sha256(raw).hexdigest())
    checked(receipt_path, hashlib.sha256(raw).hexdigest())
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group()
    action.add_argument('--plan-only', action='store_true')
    action.add_argument('--preflight', action='store_true')
    action.add_argument('--publish', action='store_true')
    parser.add_argument('--project', type=Path, default=ROOT)
    parser.add_argument('--vault', type=Path, default=VAULT)
    parser.add_argument('--acceptance', type=Path)
    parser.add_argument('--acceptance-sha256')
    parser.add_argument('--receipt', type=Path)
    args = parser.parse_args()
    if args.plan_only or not (args.preflight or args.publish):
        result = static_plan()
    else:
        if not args.acceptance or not args.acceptance_sha256:
            parser.error('Final --acceptance and its exact --acceptance-sha256 are required')
        result = publish(args.project, args.vault, args.acceptance, args.acceptance_sha256, args.receipt) if args.publish else preflight(args.project, args.vault, args.acceptance, args.acceptance_sha256)
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))


if __name__ == '__main__':
    main()
