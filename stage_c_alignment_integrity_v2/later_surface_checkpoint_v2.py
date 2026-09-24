"""Portable later surface: authenticated original inputs and zero-refit restore."""
import argparse
import hashlib
import json
from pathlib import Path
import stat
import subprocess
import sys
import zipfile

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import later_surface_operator_v2 as op
from portable_checkpoint_v2 import _plain_destination, safe_member, MANIFEST, encoded
from publication import RunPublisher, sha256_file

SCHEMA = 'forex_later_surface_checkpoint.v1'


def allowlist():
    return sorted(set(op.source_names()) | {op.RECIPE, 'MATCHED_REMAINING_OPERATOR_RECIPE.json',
        'later_surface_checkpoint_v2.py', 'portable_checkpoint_v2.py', 'test_later_surface_v2.py',
        'test_magnitude_layer_v2.py', 'LATER_REMAINING_SURFACE_V2.md'})


def saved_names(recipe):
    return [f'absolute_{h}'+s for h in recipe['contract']['new_absolute_horizons'] for s in ('.json', '.joblib', '.timing.json')]


def expected_paths(recipe):
    allowed = {'source/'+n for n in allowlist()} | {'EXPECTED_REPLAY.json', 'ORIGINAL_OPERATOR_RECEIPT.json', 'CAPSULE_SCOPE.json'}
    for alias, dep in recipe['dependencies'].items():
        allowed.update('capsule/'+alias+'/'+n for n in set(dep['payloads'])|{'RUN_IDENTITY.json', 'COMPLETION_MANIFEST.json'})
    allowed.update('capsule/slices/'+r['path'] for r in recipe['slice_manifest']['members'])
    allowed.add('capsule/slices/SLICES_MANIFEST.json')
    allowed.update('trad/'+n for n in recipe['predecessors'])
    allowed.update('saved_models/'+n for n in saved_names(recipe))
    return allowed


def inspect_capsule(package, digest):
    if package.is_symlink() or package.stat().st_size > 128*1024**2 or sha256_file(package) != digest:
        raise ValueError('later_bounded_pinned_capsule_required')
    with zipfile.ZipFile(package) as z:
        infos = z.infolist(); names = [x.filename for x in infos]
        if len(infos) > 512 or len(set(names)) != len(names) or sum(x.file_size for x in infos) > 384*1024**2:
            raise ValueError('later_bounded_unique_capsule_inventory_required')
        for info in infos:
            safe_member(info.filename)
            # Explicit new schema permits the original 9.4 MB label payload.
            # Existing checkpoint schemas retain their smaller original limits.
            if info.file_size > 16*1024**2 or info.is_dir() or stat.S_ISLNK(info.external_attr>>16) or info.flag_bits&1:
                raise ValueError('later_plain_bounded_capsule_file_required')
        if MANIFEST not in names or z.getinfo(MANIFEST).file_size > 1048576:
            raise ValueError('later_bounded_manifest_required')
        manifest = json.loads(z.read(MANIFEST)); rows = manifest['members']; expected = {r['path']: r for r in rows}
        if manifest['schema_version'] != SCHEMA or manifest['source_allowlist'] != allowlist():
            raise ValueError('later_capsule_schema_allowlist_mismatch')
        if len(expected) != len(rows) or set(names) != set(expected)|{MANIFEST}:
            raise ValueError('later_exact_capsule_members_required')
        data = {}
        for n, row in expected.items():
            raw = z.read(n)
            if len(raw) != row['bytes'] or hashlib.sha256(raw).hexdigest() != row['sha256']:
                raise ValueError('later_capsule_member_changed')
            data[n] = raw
        recipe = json.loads(data['source/'+op.RECIPE])
        if set(data) != expected_paths(recipe): raise ValueError('later_declared_capsule_paths_required')
        data[MANIFEST] = z.read(MANIFEST); return data


def invoke(source, paths, runs, action, reuse_only=False):
    recipe = source/op.RECIPE
    cmd = [sys.executable, '-X', 'utf8', '-I', '-B', str(source/'later_surface_operator_v2.py'), action,
           '--recipe', str(recipe), '--recipe-sha256', sha256_file(recipe), '--paths', str(paths), '--runs-dir', str(runs)]
    if reuse_only: cmd.append('--reuse-only')
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=1000)
    if p.returncode: raise ValueError('later_checkpoint_operator_failed:'+p.stdout+p.stderr)
    receipt = json.loads(p.stdout)
    if receipt['status'] != 'completed_verified': raise ValueError('later_checkpoint_incomplete')
    actual = {x['path']: x['sha256'] for x in op.read(Path(receipt['run_path'])/'COMPLETION_MANIFEST.json')['payloads']
              if x['path'] not in ('resource_receipts.json', 'timing_receipts.json') and not x['path'].endswith('.timing.json')}
    return receipt, actual


def seed_saved_models(recipe, directory, runs):
    from later_surface_runner_v2 import identity_for
    p = RunPublisher(runs, recipe['run_id'], identity_for(recipe)); p.acquire()
    try:
        for n in saved_names(recipe): p.write_or_validate_payload(n, (directory/n).read_bytes())
    finally: p.release()


def export(package, paths, runs):
    receipt, expected = invoke(ROOT, paths, runs, 'verify')
    recipe = op.read(ROOT/op.RECIPE); inputs = op.read(paths)
    values = {'source/'+n: (ROOT/n).read_bytes() for n in allowlist()}
    for alias, dep in recipe['dependencies'].items():
        for n in set(dep['payloads'])|{'RUN_IDENTITY.json', 'COMPLETION_MANIFEST.json'}:
            values['capsule/'+alias+'/'+n] = (Path(inputs[alias])/n).read_bytes()
    for n in ['SLICES_MANIFEST.json']+[r['path'] for r in recipe['slice_manifest']['members']]:
        values['capsule/slices/'+n] = (Path(inputs['slices'])/n).read_bytes()
    for n in recipe['predecessors']: values['trad/'+n] = (Path(inputs['trad'])/n).read_bytes()
    for n in saved_names(recipe): values['saved_models/'+n] = (Path(receipt['run_path'])/n).read_bytes()
    values.update({'EXPECTED_REPLAY.json': encoded(expected), 'ORIGINAL_OPERATOR_RECEIPT.json': encoded(receipt),
        'CAPSULE_SCOPE.json': encoded({'saved_new_absolute_pairs': 5, 'saved_original_signed_pairs': 8,
            'saved_original_absolute_pairs': 3, 'restore_base_refits_allowed': False,
            'layer_recomputation': 'explicit small causal layer regression recomputation; saved base weights never refitted',
            'raw_archive_included': False, 'authenticated_original_slices_included': True,
            'restore_scope': 'same_machine_relocated_replay; other_machine_environment_qualification_separate'})})
    manifest = {'schema_version': SCHEMA, 'source_allowlist': allowlist(),
        'members': [{'path': n, 'bytes': len(b), 'sha256': hashlib.sha256(b).hexdigest()} for n, b in sorted(values.items())]}
    with zipfile.ZipFile(package, 'x', compression=zipfile.ZIP_DEFLATED) as z:
        for n, b in sorted(values.items()): z.writestr(n, b)
        z.writestr(MANIFEST, encoded(manifest))
    digest = sha256_file(package); inspect_capsule(package, digest)
    return {'status': 'VERIFIED_EXPORT', 'sha256': digest, 'scientific_payloads': len(expected),
            'files': len(values)+1, 'restore_base_refits_allowed': False}


def restore(package, digest, destination, run_tests=False):
    data = inspect_capsule(package, digest); _plain_destination(destination); destination.mkdir(parents=True, exist_ok=True)
    for n, b in data.items():
        p = destination.joinpath(*safe_member(n).parts); p.parent.mkdir(parents=True, exist_ok=True)
        with p.open('xb') as f: f.write(b)
    paths = {k: str(destination/'capsule'/k) for k in ('technical', 'remaining', 'absolute', 'slices')}
    paths['trad'] = str(destination/'trad'); pf = destination/'PATHS.json'; pf.write_bytes(encoded(paths))
    seed = ('import sys,json;from pathlib import Path;sys.path.insert(0,sys.argv[1]);'
            'from later_surface_checkpoint_v2 import seed_saved_models;'
            'seed_saved_models(json.loads(Path(sys.argv[2]).read_bytes()),Path(sys.argv[3]),Path(sys.argv[4]))')
    subprocess.run([sys.executable, '-I', '-B', '-c', seed, str(destination/'source'),
        str(destination/'source'/op.RECIPE), str(destination/'saved_models'), str(destination/'runs')], check=True, timeout=60)
    receipt, actual = invoke(destination/'source', pf, destination/'runs', 'resume', reuse_only=True)
    if actual != op.read(destination/'EXPECTED_REPLAY.json'): raise ValueError('later_relocated_payload_mismatch')
    if receipt['attempt']['actual_new_estimator_fits'] != 0 or receipt['attempt']['signed_refits'] != 0:
        raise ValueError('later_restore_must_not_refit_base')
    if run_tests:
        p = subprocess.run([sys.executable, '-X', 'utf8', '-I', '-B', '-m', 'pytest', '-q', '-p', 'no:cacheprovider',
            '--noconftest', str(destination/'source/test_later_surface_v2.py')], capture_output=True, text=True, timeout=180)
        (destination/'tests.stdout').write_text(p.stdout+p.stderr, encoding='utf-8')
        if p.returncode: raise ValueError('later_relocated_tests_failed:'+p.stdout+p.stderr)
    result = {'status': 'VERIFIED', 'checkpoint_sha256': digest, 'scientific_payloads_identical': len(actual),
        'base_estimators_refitted_in_replay': 0, 'layer_regression_recomputation': receipt['attempt']['layer_regression_fits'],
        'relocated_tests_passed': run_tests, 'operator': receipt, 'independent_review': False}
    (destination/'RESTORE_RECEIPT.json').write_bytes(encoded(result)); return result


if __name__ == '__main__':
    p = argparse.ArgumentParser(); s = p.add_subparsers(dest='action', required=True)
    e = s.add_parser('export')
    for n in ('package', 'paths', 'runs-dir'): e.add_argument('--'+n, type=Path, required=True)
    r = s.add_parser('restore')
    for n in ('package', 'destination'): r.add_argument('--'+n, type=Path, required=True)
    r.add_argument('--sha256', required=True); r.add_argument('--run-tests', action='store_true'); a = p.parse_args()
    print(json.dumps(export(a.package, a.paths, a.runs_dir) if a.action == 'export' else restore(a.package, a.sha256, a.destination, a.run_tests), sort_keys=True))
