"""Portable original-input capsule with saved absolute-model weights; no refits."""
import argparse
import hashlib
import json
import stat
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import absolute_movement_operator_v2 as op
from portable_checkpoint_v2 import _plain_destination, safe_member, MANIFEST, encoded
from publication import RunPublisher

SCHEMA = 'forex_absolute_movement_checkpoint.v1'
ALLOWLIST = tuple(sorted(set(op.SOURCES)|{op.RECIPE, 'absolute_movement_checkpoint_v2.py',
    'portable_checkpoint_v2.py', 'test_absolute_movement_v2.py', 'ABSOLUTE_MOVEMENT_V2.md'}))


def saved_names(recipe):
    c = recipe['contract']['experiment']
    return [f'fit_{h}_{t}'+s for h in c['horizon_minutes'] for t in c['fit_cutoffs']
            for s in ('.json', '.joblib', '.timing.json')]


def inspect_capsule(package, digest):
    if package.stat().st_size > 128*1024**2 or op.sha(package) != digest:
        raise ValueError('absolute_bounded_pinned_capsule_required')
    with zipfile.ZipFile(package) as z:
        infos = z.infolist(); names = [x.filename for x in infos]
        if len(infos) > 320 or len(set(names)) != len(names) or sum(x.file_size for x in infos) > 384*1024**2:
            raise ValueError('absolute_bounded_unique_capsule_inventory_required')
        for info in infos:
            safe_member(info.filename)
            if info.file_size > 8*1024**2 or info.is_dir() or stat.S_ISLNK(info.external_attr>>16) or info.flag_bits&1:
                raise ValueError('absolute_plain_bounded_capsule_file_required')
        if MANIFEST not in names or z.getinfo(MANIFEST).file_size > 1048576:
            raise ValueError('absolute_bounded_manifest_required')
        manifest = json.loads(z.read(MANIFEST))
        if manifest['schema_version'] != SCHEMA or manifest['source_allowlist'] != list(ALLOWLIST):
            raise ValueError('absolute_capsule_schema_allowlist_mismatch')
        rows = manifest['members']; expected = {r['path']: r for r in rows}
        if len(expected) != len(rows) or set(names) != set(expected)|{MANIFEST}:
            raise ValueError('absolute_exact_capsule_members_required')
        data = {}
        for n, row in expected.items():
            raw = z.read(n)
            if len(raw) != row['bytes'] or hashlib.sha256(raw).hexdigest() != row['sha256']:
                raise ValueError('absolute_capsule_member_changed')
            data[n] = raw
        recipe = json.loads(data['source/'+op.RECIPE])
        allowed = {'source/'+n for n in ALLOWLIST}|{'EXPECTED_REPLAY.json', 'ORIGINAL_OPERATOR_RECEIPT.json', 'CAPSULE_SCOPE.json'}
        for alias in ('joint', 'technical'):
            allowed.update('capsule/'+alias+'/'+n for n in set(recipe['dependencies'][alias]['payloads'])|{'RUN_IDENTITY.json', 'COMPLETION_MANIFEST.json'})
        allowed.update('capsule/baseline_metadata/'+n for n in set(recipe['original_metadata']['payloads'])|{'RUN_IDENTITY.json', 'COMPLETION_MANIFEST.json'})
        allowed.update('saved_models/'+n for n in saved_names(recipe))
        if set(data) != allowed:
            raise ValueError('absolute_declared_capsule_paths_required')
        data[MANIFEST] = z.read(MANIFEST)
        return data


def invoke(source, paths, runs, action, reuse_only=False):
    recipe = source/op.RECIPE
    cmd = [sys.executable, '-X', 'utf8', '-I', '-B', str(source/'absolute_movement_operator_v2.py'), action,
           '--recipe', str(recipe), '--recipe-sha256', op.sha(recipe), '--paths', str(paths), '--runs-dir', str(runs)]
    if reuse_only: cmd.append('--reuse-only')
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=750)
    if p.returncode: raise ValueError('absolute_checkpoint_operator_failed:'+p.stdout+p.stderr)
    receipt = json.loads(p.stdout)
    if receipt['status'] != 'completed_verified': raise ValueError('absolute_checkpoint_incomplete')
    actual = {x['path']: x['sha256'] for x in op.read(Path(receipt['run_path'])/'COMPLETION_MANIFEST.json')['payloads']
              if x['path'] not in ('resource_receipts.json', 'prediction_resources.json') and not x['path'].endswith('.timing.json')}
    return receipt, actual


def seed_saved_models(recipe, directory, runs):
    from absolute_movement_runner_v2 import identity_for
    p = RunPublisher(runs, recipe['run_id'], identity_for(recipe))
    p.acquire()
    try:
        for n in saved_names(recipe): p.write_or_validate_payload(n, (directory/n).read_bytes())
    finally:
        p.release()


def export(package, paths, runs):
    receipt, expected = invoke(ROOT, paths, runs, 'verify')
    recipe = op.read(ROOT/op.RECIPE); inputs = op.read(paths)
    values = {'source/'+n: (ROOT/n).read_bytes() for n in ALLOWLIST}
    for alias in ('joint', 'technical', 'baseline_metadata'):
        names = set(recipe['original_metadata']['payloads'] if alias == 'baseline_metadata' else recipe['dependencies'][alias]['payloads'])|{'RUN_IDENTITY.json', 'COMPLETION_MANIFEST.json'}
        for n in names: values['capsule/'+alias+'/'+n] = (Path(inputs[alias])/n).read_bytes()
    run = Path(receipt['run_path'])
    for n in saved_names(recipe): values['saved_models/'+n] = (run/n).read_bytes()
    values.update({'EXPECTED_REPLAY.json': encoded(expected), 'ORIGINAL_OPERATOR_RECEIPT.json': encoded(receipt),
                  'CAPSULE_SCOPE.json': encoded({'new_target_estimators': 28, 'saved_fit_pairs': 14,
                     'original_signed_estimators_refitted': 0, 'restore_refits_allowed': False,
                     'original_signed_model_metadata_only': True, 'original_joint_forecasts_and_technical_inputs_included': True,
                     'raw_archive_included': False, 'model_fit_timing_receipts': 'original_training_measurements_preserved; restore measures inference separately'})})
    manifest = {'schema_version': SCHEMA, 'source_allowlist': list(ALLOWLIST),
                'members': [{'path': n, 'bytes': len(b), 'sha256': hashlib.sha256(b).hexdigest()} for n, b in sorted(values.items())]}
    with zipfile.ZipFile(package, 'x', compression=zipfile.ZIP_DEFLATED) as z:
        for n, b in sorted(values.items()): z.writestr(n, b)
        z.writestr(MANIFEST, encoded(manifest))
    digest = op.sha(package); inspect_capsule(package, digest)
    return {'status': 'VERIFIED_EXPORT', 'sha256': digest, 'scientific_payloads': len(expected),
            'files': len(values)+1, 'saved_fit_pairs': 14, 'restore_refits_allowed': False}


def restore(package, digest, destination, run_tests=False):
    data = inspect_capsule(package, digest)
    _plain_destination(destination); destination.mkdir(parents=True, exist_ok=True)
    for n, b in data.items():
        p = destination.joinpath(*safe_member(n).parts); p.parent.mkdir(parents=True, exist_ok=True)
        with p.open('xb') as f: f.write(b)
    paths = {k: str(destination/'capsule'/k) for k in ('joint', 'technical', 'baseline_metadata')}
    pf = destination/'PATHS.json'; pf.write_bytes(encoded(paths))
    recipe = op.read(destination/'source'/op.RECIPE)
    # Construct the publisher identity from the exact source bundled in the capsule.
    seed = ('import sys,json;from pathlib import Path;sys.path.insert(0,sys.argv[1]);'
            'from absolute_movement_checkpoint_v2 import seed_saved_models;'
            'seed_saved_models(json.loads(Path(sys.argv[2]).read_bytes()),Path(sys.argv[3]),Path(sys.argv[4]))')
    subprocess.run([sys.executable, '-I', '-B', '-c', seed, str(destination/'source'),
                    str(destination/'source'/op.RECIPE), str(destination/'saved_models'), str(destination/'runs')], check=True, timeout=60)
    receipt, actual = invoke(destination/'source', pf, destination/'runs', 'resume', reuse_only=True)
    if actual != op.read(destination/'EXPECTED_REPLAY.json'):
        raise ValueError('absolute_relocated_payload_mismatch')
    if receipt['attempt']['actual_new_estimator_fits'] != 0 or receipt['attempt']['saved_fit_pairs_reused'] != 14:
        raise ValueError('absolute_restore_must_not_refit')
    if run_tests:
        p = subprocess.run([sys.executable, '-X', 'utf8', '-I', '-B', '-m', 'pytest', '-q', '-p', 'no:cacheprovider',
                            '--noconftest', str(destination/'source/test_absolute_movement_v2.py')], capture_output=True, text=True, timeout=180)
        (destination/'tests.stdout').write_text(p.stdout+p.stderr, encoding='utf-8')
        if p.returncode: raise ValueError('absolute_relocated_tests_failed:'+p.stdout+p.stderr)
    result = {'status': 'VERIFIED', 'checkpoint_sha256': digest, 'scientific_payloads_identical': len(actual),
              'original_models_refitted': 0, 'new_target_estimators_refitted_in_replay': 0,
              'synthetic_test_fixture_fits_separate': run_tests, 'relocated_tests_passed': run_tests,
              'operator': receipt, 'independent_review': False}
    (destination/'RESTORE_RECEIPT.json').write_bytes(encoded(result))
    return result


if __name__ == '__main__':
    p = argparse.ArgumentParser(); s = p.add_subparsers(dest='action', required=True)
    e = s.add_parser('export')
    for n in ('package', 'paths', 'runs-dir'): e.add_argument('--'+n, type=Path, required=True)
    r = s.add_parser('restore')
    for n in ('package', 'destination'): r.add_argument('--'+n, type=Path, required=True)
    r.add_argument('--sha256', required=True); r.add_argument('--run-tests', action='store_true')
    a = p.parse_args()
    print(json.dumps(export(a.package, a.paths, a.runs_dir) if a.action == 'export' else restore(a.package, a.sha256, a.destination, a.run_tests), sort_keys=True))
