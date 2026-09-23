"""Assemble this isolated source/artifact release after implementation freeze.

This does not edit the canonical project, activate studies or provision private
data. Existing evidence receipts are retained outside the portable source ZIP.
"""
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import sys
import zipfile

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT/'src'))


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def write(path, value, *, replace=False):
    with path.open('w' if replace else 'x', encoding='utf8') as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True)+'\n')


def main():
    if (ROOT/'RELEASE_MANIFEST.json').exists() or (ROOT/'forex_weekend_setup_20260911.zip').exists():
        raise ValueError('existing_release_requires_new_version_not_overwrite')
    from oanda_practice_trial_runner_v1 import validate_config
    from oanda_practice_trial_runtime_v2 import CANDIDATE_NAME
    from oanda_joint_price_news_forecast_study_v3 import load_registry as load_joint
    from oanda_pair_local_forecast_study_v2 import load_registry as load_price
    # Preserve historical originals before composing a disabled, verified copy.
    cfg = json.loads((ROOT/'historical_config_copies/practice007_joint_v3_20260909_v1.json').read_bytes())
    cfg.pop('config_sha256')
    cfg['enabled'] = False
    for name in ('oanda_practice_trial_runtime_v2.py', CANDIDATE_NAME):
        cfg['source_bindings'][name] = ''
    for name in cfg['source_bindings']:
        cfg['source_bindings'][name] = sha((ROOT/'src'/name).read_bytes())
    cfg['config_sha256'] = sha(json.dumps(cfg, sort_keys=True, separators=(',', ':'), allow_nan=False).encode())
    config_path = ROOT/'src/config/practice007_joint_v3_20260909_v1.json'
    write(config_path, cfg, replace=True)
    validate_config(config_path)
    # Historical finite study defaults are evidence, not restart configurations.
    for name in ('recovered_second_curve_pilot_v1_20260909.json', 'observed_curve_management_v1_20260909.json'):
        copied = (ROOT/'src/config'/name).resolve()
        config_directory = ROOT/'src/config'
        if (config_directory.resolve() != config_directory or copied.parent != config_directory
                or not (ROOT/'historical_config_copies'/name).is_file()):
            raise ValueError('archive_original_required')
        if copied.exists():
            if copied.read_bytes() != (ROOT/'historical_config_copies'/name).read_bytes():
                raise ValueError('historical_copy_changed')
            copied.unlink()
    joint = load_joint(ROOT/'src/config/joint_price_news_weekend_setup_20260911.json')
    price = load_price(ROOT/'src/config/pair_local_forecast_weekend_setup_20260911.json')
    initial = {r['relative']: r for r in json.loads((ROOT/'INITIAL_SOURCE_INVENTORY.json').read_bytes())}
    implementation = []
    for path in sorted((ROOT/'src').rglob('*')):
        if path.is_file() and path.suffix == '.py' and '__pycache__' not in path.parts:
            relative = path.relative_to(ROOT/'src').as_posix()
            if relative.startswith(('fresh_m1_intrahour/', 'validation/', 'scheduler_review_20260911/')):
                continue
            if relative not in initial or initial[relative]['sha256'] != sha(path.read_bytes()):
                implementation.append(path.relative_to(ROOT).as_posix())
    packages = {d.metadata['Name']: d.version for d in importlib.metadata.distributions() if d.metadata.get('Name')}
    write(ROOT/'RUNTIME_INVENTORY.json', {'python': platform.python_version(), 'platform': platform.platform(),
        'installed_distribution_versions': packages, 'scope': 'observed environment; no dependency installation performed'})
    required = {'python': platform.python_version(), **{p: importlib.metadata.version(p)
        for p in ('numpy', 'scikit-learn', 'scipy', 'pandas', 'joblib', 'requests', 'pytest')}}
    (ROOT/'requirements-observed.txt').write_text(''.join(f'{p}=={v}\n' for p,v in sorted(required.items()) if p != 'python'), encoding='utf8')
    source_restore = []
    for path in sorted((ROOT/'src/fresh_m1_intrahour/src').rglob('*.py')):
        raw = path.read_bytes()
        source_restore.append(dict(path=path.relative_to(ROOT).as_posix(), bytes=len(raw), sha256=sha(raw)))
    write(ROOT/'RESTORED_LEGACY_SOURCE_RECEIPT.json', {'files': source_restore,
        'scope': 'Original 36 preserved modules; copied without changing the original feature builder or retraining.'})
    selected = []
    for folder in ('src', 'models', 'historical_config_copies', 'release_evidence'):
        selected.extend(p for p in (ROOT/folder).rglob('*') if p.is_file()
            and p.suffix.lower() in {'.py', '.ps1', '.html', '.json', '.joblib', '.md', '.xml'}
            and not any(x in p.parts for x in ('__pycache__', '.pytest_cache', 'validation', 'scheduler_review_20260911'))
            and not (folder == 'src' and p.relative_to(ROOT/'src').parts[0] == 'data'))
    top_names = ['README_SETUP.md', 'SETUP_PROFILES.json', 'RUNTIME_INVENTORY.json',
        'requirements-observed.txt', 'check_weekend_setup.py', 'check_existing_opportunity.py',
        'prepare_setup_registries.py', 'assemble_release.py', 'RESTORED_LEGACY_SOURCE_RECEIPT.json',
        'HISTORICAL_CONFIGURATION_SNAPSHOT.json', 'EXISTING_ARTIFACT_RESTORATION.json',
        'CURVE_MANAGEMENT_SETUP_PROFILE_20260911.md', 'SUCCESSOR_REGISTRIES_PREPARED.json',
        'REGISTRY_READBACK_AND_DURABILITY.json', 'INTEGRATED_SETUP_FINAL_TESTS_20260911.xml',
        'SCHEDULED_CURVE_PUBLISHER_PROFILE_20260911.md',
        'SCHEDULED_CURVE_PUBLISHER_DEPENDENCY_RECEIPT_20260911.json',
        'CURVE_MANAGEMENT_SETUP_DEPENDENCY_RECEIPT_V2_20260911.json']
    selected.extend(ROOT/name for name in top_names)
    rows = []
    for path in sorted(set(selected)):
        raw = path.read_bytes()
        rows.append(dict(path=path.relative_to(ROOT).as_posix(), bytes=len(raw), sha256=sha(raw)))
    manifest = {'schema_version': 'forex_weekend_setup_release_v1_20260911',
        'created_utc': datetime.now(timezone.utc).isoformat(), 'files': rows,
        'implementation_files': implementation, 'required_runtime_versions': required,
        'source_snapshot': 'Actual uncommitted canonical source snapshot, with explicitly listed staged changes.',
        'private_state_included': False, 'credentials_included': False,
        'study_registration_counts': {'joint_pairs': len(joint['pairs']), 'price_pairs': len(price['pairs'])},
        'automatic_start': False, 'production_deployed': False, 'orders_enabled': False,
        'limits': ['Scoped source/artifact setup, not a full vault or private-data recovery.',
                   'New study ledgers require their own actual activation clocks and provisioned feed/archive inputs.',
                   'Disabled historical practice config does not authorize a new trial or resume old state with a new seal.']}
    write(ROOT/'RELEASE_MANIFEST.json', manifest)
    archive = ROOT/'forex_weekend_setup_20260911.zip'
    with zipfile.ZipFile(archive, 'x', zipfile.ZIP_DEFLATED) as bundle:
        for row in rows:
            raw = (ROOT/row['path']).read_bytes()
            if len(raw) != row['bytes'] or sha(raw) != row['sha256']:
                raise ValueError('source_changed_between_inventory_and_archive:'+row['path'])
            bundle.writestr(row['path'], raw)
        manifest_raw = (ROOT/'RELEASE_MANIFEST.json').read_bytes()
        if json.loads(manifest_raw) != manifest:
            raise ValueError('manifest_changed_before_archive')
        bundle.writestr('RELEASE_MANIFEST.json', manifest_raw)
    write(ROOT/'RELEASE_ARCHIVE_RECEIPT.json', {'archive': archive.name, 'bytes': archive.stat().st_size,
        'sha256': sha(archive.read_bytes()), 'manifest_sha256': sha((ROOT/'RELEASE_MANIFEST.json').read_bytes()),
        'files': len(rows)+1, 'implementation_files': implementation})
    print(json.dumps({'files':len(rows), 'implementation_files':len(implementation),
                      'archive_bytes':archive.stat().st_size, 'orders_enabled':False}))


if __name__ == '__main__':
    main()
