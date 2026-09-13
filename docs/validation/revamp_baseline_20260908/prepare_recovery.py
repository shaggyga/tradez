"""Create exclusive, hash-verified source/artifact recovery copies; no bot imports."""
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path, PurePosixPath
import importlib.metadata
import json
import os
import shutil
import stat
import sys
import zipfile

sys.dont_write_bytecode = True
BASE = Path(__file__).resolve().parent
LIVE = BASE.parent / 'trad'
VAULT = Path('C:/Users/zmoor/OneDrive/thevault/projects/forex')
RECOVERY = Path('D:/ForexRecovery/revamp_20260908T1353Z')
WORKSPACE = BASE / 'workspace/trad'
STARTED = datetime.now(timezone.utc).isoformat()

def digest(path):
    value = sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()

def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf-8') as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write('\n')

def safe_member(name):
    p = PurePosixPath(name)
    assert not p.is_absolute() and '\\' not in name and ':' not in name
    assert all(x not in {'..', '.', ''} and not x.endswith((' ', '.')) for x in p.parts)
    return p

copied = []
def copy_bound(source, target, *, expected=None):
    assert source.is_file() and not source.is_symlink()
    prior = source.stat()
    source_hash = digest(source)
    assert expected is None or source_hash == expected, str(source)
    target.parent.mkdir(parents=True, exist_ok=True)
    with source.open('rb') as src, target.open('xb') as dst:
        shutil.copyfileobj(src, dst, 4 * 1024 * 1024)
    assert digest(target) == source_hash
    after = source.stat()
    assert (prior.st_size, prior.st_mtime_ns) == (after.st_size, after.st_mtime_ns)
    assert digest(source) == source_hash
    row = {'source': str(source), 'backup': str(target), 'bytes': prior.st_size,
           'sha256': source_hash, 'source_unchanged_during_copy': True}
    copied.append(row)
    return row

assert shutil.disk_usage('C:/').free > 2 * 1024**3
assert shutil.disk_usage('D:/').free > 20 * 1024**3
RECOVERY.mkdir(parents=True, exist_ok=True)  # DB agent owns its separate databases subtree.
assert not (RECOVERY / 'source').exists()
assert not WORKSPACE.exists()
pointer_path = VAULT / 'source/WORKTREE_SOURCE_LATEST.json'
pointer_hash = digest(pointer_path)
pointer = json.loads(pointer_path.read_text(encoding='utf-8-sig'))
archive = VAULT / 'source' / pointer['archive']
manifest_path = VAULT / 'source' / pointer['manifest']
assert safe_member(pointer['archive']).name == pointer['archive']
assert safe_member(pointer['manifest']).name == pointer['manifest']
assert digest(archive) == pointer['archive_sha256']
assert digest(manifest_path) == pointer['manifest_sha256']
manifest = json.loads(manifest_path.read_text(encoding='utf-8-sig'))
rows = manifest['files']
expected = {row['path']: row for row in rows}
assert len(expected) == manifest['file_count'] == len(rows)
assert len({name.casefold() for name in expected}) == len(expected)
assert manifest['archive_sha256'] == pointer['archive_sha256']

# Verify current file bytes before declaring the isolated copy a baseline of C.
for name, row in expected.items():
    safe_member(name)
    path = LIVE / name
    assert path.is_file() and not path.is_symlink(), name
    assert path.stat().st_size == row['size'] and digest(path) == row['sha256'], name
for source in [pointer_path, manifest_path, archive]:
    copy_bound(source, RECOVERY / 'source/current' / source.name)

WORKSPACE.mkdir(parents=True)
compiled = 0
with zipfile.ZipFile(archive) as package:
    infos = package.infolist()
    assert len(infos) == len(expected)
    assert {info.filename for info in infos} == set(expected)
    assert package.testzip() is None
    for info in infos:
        safe_member(info.filename)
        assert not info.is_dir() and stat.S_IFMT(info.external_attr >> 16) in {0, stat.S_IFREG}
        payload = package.read(info)
        row = expected[info.filename]
        assert len(payload) == row['size'] and sha256(payload).hexdigest() == row['sha256']
        target = WORKSPACE / info.filename
        assert target.resolve().is_relative_to(WORKSPACE.resolve())
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open('xb') as stream:
            stream.write(payload)
        assert digest(target) == row['sha256']
        if target.suffix == '.py':
            compile(payload, info.filename, 'exec', dont_inherit=True)
            compiled += 1
print(json.dumps({'stage': 'current_source_recovered', 'files': len(rows), 'compiled': compiled}), flush=True)

# This imports only the already-inspected offline source auditing utility.
sys.path.insert(0, str(LIVE))
from tools.vault_worktree_snapshot import audit_payload
legacy = Path('D:/forex/trad/fresh_m1_intrahour/src')
legacy_paths = sorted(legacy.glob('*.py'))
assert len(legacy_paths) == 36
legacy_rows = []
for source in legacy_paths:
    payload = source.read_bytes()
    audit_payload('fresh_m1_intrahour/src/' + source.name, payload, set())
    compile(payload, source.name, 'exec', dont_inherit=True)
    legacy_rows.append(copy_bound(source, RECOVERY / 'source/legacy_intrahour_v5/src' / source.name))
save(RECOVERY / 'source/legacy_intrahour_v5/RECOVERY_SCOPE.json', {
    'source': str(legacy), 'schema': 'current D v5 opportunity implementation',
    'compatible_with_retained_unified_v4': 'not established',
    'files': legacy_rows, 'credential_pattern_scan': 'passed',
    'imported_into_isolated_or_live_project': False,
})
print(json.dumps({'stage': 'legacy_source_preserved', 'files': len(legacy_rows)}), flush=True)

unified = Path('C:/Users/zmoor/AppData/Local/ForexResearchData/unified_intrahour_v1')
unified_files = ['build_manifest.json', 'latest_shadow_forecast_curves.json', 'latest_validation.json',
                 'matrix_manifest.json', 'unified_feature_registry.csv', 'unified_training_matrix.parquet',
                 'models/unified_intrahour_forecast_latest.joblib']
for name in unified_files:
    copy_bound(unified / name, RECOVERY / 'artifacts/unified_intrahour_v4' / name)

droot = Path('D:/forex/trad/data/oanda_training_manager')
for name in ['models/ma_feature_grid/ma_feature_grid_latest.joblib',
             'reports/ma_feature_grid/ma_feature_grid_latest.json',
             'state/second_ridge_models_v1.json', 'reports/second_ridge_fit_v1.json']:
    copy_bound(droot / name, RECOVERY / 'artifacts/legacy_model_components' / name)
family = LIVE / 'fresh_m1_intrahour/reports/feature_forecast_full_20260713_v4'
for source in sorted(family.iterdir()):
    if source.is_file() and source.suffix in {'.json', '.md', '.joblib', '.parquet'}:
        copy_bound(source, RECOVERY / 'artifacts/feature_forecast_220_v4' / source.name)

packages = {}
for name in ['numpy', 'scipy', 'scikit-learn', 'pandas', 'pyarrow', 'joblib']:
    packages[name] = importlib.metadata.version(name)
save(BASE / 'ISOLATED_WORKSPACE.json', {
    'created_utc': datetime.now(timezone.utc).isoformat(), 'workspace': str(WORKSPACE),
    'live_source': str(LIVE), 'recovery_root': str(RECOVERY),
    'source_archive': str(RECOVERY / 'source/current' / archive.name),
    'snapshot_id': pointer['snapshot_id'], 'exact_source_members': len(rows),
    'runtime_started': False, 'private_credentials_copied': False,
    'databases_in_workspace': False, 'artifacts_loaded': False,
    'purpose': 'Isolated exact source baseline for inspection and later bounded offline changes. Runtime databases and legacy components are separately preserved; this is not a running or historically reproduced bot.',
})
for name, row in expected.items():
    assert digest(LIVE / name) == digest(WORKSPACE / name) == row['sha256']
assert digest(pointer_path) == pointer_hash
receipt = {
    'schema': 'forex_revamp_source_artifact_recovery_v1', 'status': 'passed',
    'started_utc': STARTED, 'finished_utc': datetime.now(timezone.utc).isoformat(),
    'source_snapshot_id': pointer['snapshot_id'], 'source_archive_sha256': pointer['archive_sha256'],
    'source_manifest_sha256': pointer['manifest_sha256'], 'source_files': len(rows),
    'python_syntax_checked': compiled, 'live_source_matches_snapshot': True,
    'isolated_source_matches_snapshot': True, 'workspace': str(WORKSPACE),
    'backup_files': copied, 'backup_file_count': len(copied),
    'backup_bytes': sum(row['bytes'] for row in copied),
    'current_interpreter': sys.executable, 'current_python': sys.version, 'package_metadata': packages,
    'restoration_checks': ['Source ZIP directory/CRC/hash, all members, live source and extracted copies verified',
                           'Every static backup checked against original before and after copy',
                           '36 D source modules credential-pattern scanned and syntax checked, stored separately',
                           'Large unified training matrix and actual fitted model bytes copied and fully hashed'],
    'model_deserialization': False, 'training_or_predictions': False,
    'broker_or_runtime_actions': False, 'db_snapshot_scope': 'Separate performance agent receipt',
    'limits': ['This is a local recovery checkpoint, not an independent off-device disaster backup.',
               'No compatible v4 unified source reconstruction or successful model inference is claimed.',
               'No private credentials, full raw-feed archive, or all352 databases are copied.',
               'Recorded runtime dependencies describe the present interpreter, not proof of historical model compatibility.'],
}
save(BASE / 'SOURCE_ARTIFACT_RECOVERY_20260908.json', receipt)
save(RECOVERY / 'SOURCE_ARTIFACT_RECOVERY_20260908.json', receipt)
print(json.dumps({'status': 'passed', 'source_files': len(rows), 'static_backup_files': len(copied),
                  'static_backup_bytes': receipt['backup_bytes'], 'workspace': str(WORKSPACE),
                  'recovery_root': str(RECOVERY)}), flush=True)
