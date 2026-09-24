"""Portable checkpoint for the fresh currency-projection native qualification."""
import argparse
import hashlib
import importlib.metadata
import json
import stat
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import currency_projection_native_operator_v2 as op
from portable_checkpoint_v2 import MANIFEST, encoded, safe_member, _plain_destination
from publication import sha256_file, verify_completed_run

SCHEMA = 'forex_currency_projection_native_checkpoint.v1'


def sources():
    return sorted(set(op.source_names()) | {op.RECIPE, op.PROJECTION_PARENT_RECIPE, 'currency_projection_native_checkpoint_v2.py',
        'portable_checkpoint_v2.py', 'test_currency_projection_v2.py', 'test_currency_projection_native_v2.py',
        'CURRENCY_PROJECTION_NATIVE_V2.md'})


def members(recipe):
    return ({'source/' + name for name in sources()} |
            {'inputs/' + alias + '/' + name for alias, dep in recipe['inputs'].items() for name in dep['files']} |
            {'trad/' + name for name in recipe['predecessors']} |
            {'solver/' + name for name in recipe['solver_pins']} |
            {'native_source/' + name for name in recipe['native_parent_source_hashes']} |
            {'native_source/' + op.NATIVE_PARENT_RECIPE} |
            {'EXPECTED.json', 'SCOPE.json'})


def inspect(package, pin):
    if package.is_symlink() or package.stat().st_size > 536870912 or sha256_file(package) != pin:
        raise ValueError('projection_native_checkpoint_pin_or_size')
    with zipfile.ZipFile(package) as archive:
        infos = archive.infolist(); names = [x.filename for x in infos]
        if len(names) > 512 or len(set(x.casefold() for x in names)) != len(names) or sum(x.file_size for x in infos) > 805306368:
            raise ValueError('projection_native_checkpoint_inventory_bounds')
        for info in infos:
            safe_member(info.filename)
            if info.file_size > 16777216 or info.is_dir() or stat.S_ISLNK(info.external_attr >> 16) or info.flag_bits & 1:
                raise ValueError('projection_native_checkpoint_plain_members')
        if MANIFEST not in names or archive.getinfo(MANIFEST).file_size > 1048576:
            raise ValueError('projection_native_checkpoint_manifest_bounds')
        manifest = json.loads(archive.read(MANIFEST)); records = {x['path']: x for x in manifest['members']}
        if manifest['schema_version'] != SCHEMA or len(records) != len(manifest['members']) or set(names) != set(records) | {MANIFEST}:
            raise ValueError('projection_native_checkpoint_exact_inventory')
        for name, record in records.items():
            digest, size = hashlib.sha256(), 0
            with archive.open(name) as handle:
                for block in iter(lambda: handle.read(1048576), b''):
                    size += len(block); digest.update(block)
            if size != record['bytes'] or digest.hexdigest() != record['sha256']:
                raise ValueError('projection_native_checkpoint_member_changed')
        recipe = json.loads(archive.read('source/' + op.RECIPE))
        if set(records) != members(recipe):
            raise ValueError('projection_native_checkpoint_declared_members')
        return manifest


def invoke(source, paths, runs, action):
    recipe = source / op.RECIPE
    command = [sys.executable, '-X', 'utf8', '-I', '-B', str(source / 'currency_projection_native_operator_v2.py'), action,
               '--recipe', str(recipe), '--recipe-sha256', sha256_file(recipe), '--paths', str(paths), '--runs-dir', str(runs)]
    result = subprocess.run(command, capture_output=True, text=True, timeout=1800)
    if result.returncode:
        raise ValueError('projection_native_checkpoint_operator_failed:' + result.stdout + result.stderr)
    receipt = json.loads(result.stdout)
    if receipt['status'] != 'completed_verified' or any(receipt[key] for key in ('base_model_fits', 'layer_fits', 'api_calls', 'policy_replays')):
        raise ValueError('projection_native_checkpoint_completion_required')
    root = runs / op.read(recipe)['run_id']; identity = op.read(root / 'RUN_IDENTITY.json')
    if identity['fingerprint'] != receipt['run_identity']:
        raise ValueError('projection_native_checkpoint_identity_mismatch')
    manifest = verify_completed_run(root, identity)
    return receipt, {x['path']: x['sha256'] for x in manifest['payloads']}


def export(package, paths, runs):
    receipt, expected = invoke(ROOT, paths, runs, 'verify')
    recipe, locations, records = op.read(ROOT / op.RECIPE), op.read(paths), []
    with zipfile.ZipFile(package, 'x', compression=zipfile.ZIP_DEFLATED) as archive:
        def put(name, data):
            archive.writestr(name, data)
            records.append({'path': name, 'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()})
        for name in sources():
            put('source/' + name, (ROOT / name).read_bytes())
        for alias, dep in recipe['inputs'].items():
            for name in dep['files']:
                put('inputs/' + alias + '/' + name, op.checked(locations, recipe, alias, name))
        for name, desc in recipe['predecessors'].items():
            item = Path(locations['trad']) / name
            if item.stat().st_size != desc['bytes'] or sha256_file(item) != desc['sha256']:
                raise ValueError('projection_native_checkpoint_external_source_changed')
            put('trad/' + name, item.read_bytes())
        for name, desc in recipe['solver_pins'].items():
            item = Path(locations['solver']) / name
            if item.stat().st_size != desc['bytes'] or sha256_file(item) != desc['sha256']:
                raise ValueError('projection_native_checkpoint_solver_changed')
            put('solver/' + name, item.read_bytes())
        for name, digest in recipe['native_parent_source_hashes'].items():
            item = Path(locations['native_source']) / name
            if sha256_file(item) != digest:
                raise ValueError('projection_native_checkpoint_parent_snapshot_changed')
            put('native_source/' + name, item.read_bytes())
        parent_recipe = Path(locations['native_source']) / op.NATIVE_PARENT_RECIPE
        if sha256_file(parent_recipe) != op.NATIVE_PARENT_PIN:
            raise ValueError('projection_native_checkpoint_parent_recipe_changed')
        put('native_source/' + op.NATIVE_PARENT_RECIPE, parent_recipe.read_bytes())
        put('EXPECTED.json', encoded(expected))
        put('SCOPE.json', encoded({'inputs': 'authenticated saved models, source frames, projection diagnostics and reviewed native parents',
            'base_model_fits': 0, 'base_model_loads': receipt['base_model_loads'], 'learned_layer_fits': 0,
            'policy_replays': 0, 'api_calls': 0, 'pytest_version': importlib.metadata.version('pytest'),
            'run_identity': receipt['run_identity'], 'scope': 'same-machine relocation; no cloud, other-machine or live-execution claim'}))
        archive.writestr(MANIFEST, encoded({'schema_version': SCHEMA, 'members': sorted(records, key=lambda x: x['path'])}))
    digest = sha256_file(package); inspect(package, digest)
    return {'status': 'VERIFIED_EXPORT', 'sha256': digest, 'members': len(records) + 1,
            'payloads': len(expected), 'run_identity': receipt['run_identity']}


def restore(package, pin, destination, tests=False):
    manifest = inspect(package, pin)
    _plain_destination(destination); destination.mkdir(parents=True, exist_ok=True); destination = destination.resolve()
    with zipfile.ZipFile(package) as archive:
        for name in [x['path'] for x in manifest['members']] + [MANIFEST]:
            item = destination.joinpath(*safe_member(name).parts); item.parent.mkdir(parents=True, exist_ok=True)
            with item.open('xb') as out, archive.open(name) as source:
                for block in iter(lambda: source.read(1048576), b''):
                    out.write(block)
    source = destination / 'source'; recipe = op.read(source / op.RECIPE)
    locations = {alias: str(destination / 'inputs' / alias) for alias in recipe['inputs']}
    locations.update({'trad': str(destination / 'trad'), 'solver': str(destination / 'solver'), 'native_source': str(destination / 'native_source')})
    paths = destination / 'PATHS.json'; paths.write_bytes(encoded(locations))
    receipt, actual = invoke(source, paths, destination / 'runs', 'run')
    if actual != op.read(destination / 'EXPECTED.json'):
        raise ValueError('projection_native_relocated_payload_mismatch')
    if tests:
        if importlib.metadata.version('pytest') != op.read(destination / 'SCOPE.json')['pytest_version']:
            raise ValueError('projection_native_test_environment_mismatch')
        command = [sys.executable, '-X', 'utf8', '-I', '-B', '-m', 'pytest', '-q', '-p', 'no:cacheprovider', '--noconftest',
                   str(source / 'test_currency_projection_v2.py'), str(source / 'test_currency_projection_native_v2.py')]
        result = subprocess.run(command, capture_output=True, text=True, timeout=240)
        (destination / 'tests.stdout').write_text(result.stdout + result.stderr, encoding='utf-8')
        if result.returncode:
            raise ValueError('projection_native_relocated_tests_failed:' + result.stdout + result.stderr)
    result = {'status': 'VERIFIED', 'checkpoint_sha256': pin, 'run_identity': receipt['run_identity'],
              'scientific_payloads_identical': len(actual), 'relocated_tests_passed': tests,
              'base_model_fits': 0, 'base_model_loads': receipt['base_model_loads'], 'layer_fits': 0,
              'policy_replays': 0, 'api_calls': 0, 'independent_review': False}
    (destination / 'RESTORE_RECEIPT.json').write_bytes(encoded(result))
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); sub = parser.add_subparsers(dest='action', required=True)
    export_parser = sub.add_parser('export'); restore_parser = sub.add_parser('restore')
    for name in ('package', 'paths', 'runs-dir'):
        export_parser.add_argument('--' + name, type=Path, required=True)
    for name in ('package', 'destination'):
        restore_parser.add_argument('--' + name, type=Path, required=True)
    restore_parser.add_argument('--sha256', required=True); restore_parser.add_argument('--run-tests', action='store_true')
    args = parser.parse_args()
    print(json.dumps(export(args.package, args.paths, args.runs_dir) if args.action == 'export' else
                     restore(args.package, args.sha256, args.destination, args.run_tests), sort_keys=True))
