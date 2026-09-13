"""Publish signals-versus-live optimization and a verified source snapshot."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import sys

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent / 'trad'
VAULT = Path(r'C:\Users\zmoor\OneDrive\thevault\projects\forex')
sys.dont_write_bytecode = True
sys.path[:0] = [str(ROOT), str(ROOT.parent)]
import forex_model_vault_sync as records
from tools import vault_worktree_snapshot as snapshot
from oanda_issue_register_validator import validate_register

def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def load(p): return json.loads(p.read_text(encoding='utf-8-sig'))
def encoded(p): return (json.dumps(p, indent=2) + '\n').encode()

receipt_path = ROOT / 'FOREX_SIGNALS_LIVE_OPTIMIZATION_VALIDATION_20260907.json'
receipt = load(receipt_path)
for row in receipt['source_bindings']:
    assert sha(ROOT / row['path']) == row['sha256'], row['path']
for name, digest in receipt['prior_receipts_unchanged'].items():
    assert sha(ROOT / name) == digest, name
register = validate_register(ROOT / 'FOREX_ISSUE_REGISTER_CURRENT.json', root=ROOT)
assert register['valid'], register
backup = VAULT / 'maintenance/before_signals_live_optimization_20260907'
backup.mkdir(parents=True, exist_ok=False)
names = ['source/WORKTREE_SOURCE_LATEST.json', 'SHARED_PROJECT_STATE_CURRENT.json',
         'maintenance/LOCAL_VERIFICATION_CURRENT.json']
for source, name in records.CANONICAL_PROJECT_RECORDS:
    target = VAULT / name
    if target.is_file() and target.read_bytes() != (ROOT.parent / source).read_bytes():
        names.append(name)
preserved = []
for name in sorted(set(names)):
    source = VAULT / name
    if not source.exists(): continue
    target = backup / name
    target.parent.mkdir(parents=True, exist_ok=True)
    raw = source.read_bytes()
    with target.open('xb') as f: f.write(raw)
    preserved.append({'path': name, 'backup': target.relative_to(VAULT).as_posix(),
                      'sha256': sha(target), 'bytes': len(raw)})
with (backup / 'PRESERVED_FILES.json').open('xb') as f:
    f.write(encoded({'files': preserved, 'deletions': 0}))
print('Previous current records preserved; publishing canonical records.', flush=True)
records.sync_canonical_project_records(ROOT.parent, VAULT)
print('Publishing and verifying source-only archive.', flush=True)
pointer = snapshot.sync_worktree_snapshot(ROOT, VAULT)
manifest = load(VAULT / 'SHARED_PROJECT_STATE_CURRENT.json')
assert manifest['record_count'] == len(records.CANONICAL_PROJECT_RECORDS) == 143
for row in manifest['records']:
    assert sha(VAULT / row['name']) == row['sha256'] == sha(ROOT.parent / row['source'])
source_manifest = load(VAULT / 'source' / pointer['manifest'])
for row in source_manifest['files']:
    assert sha(ROOT / row['path']) == row['sha256'], row['path']
for row in receipt['source_bindings']:
    assert sha(ROOT / row['path']) == row['sha256'], row['path']
verified = {
    'schema_version': 'forex_signals_live_optimization_export_verification_v1',
    'verified_utc': datetime.now(timezone.utc).isoformat(),
    'status': 'local_source_and_signals_live_records_verified_forecasts_publishing',
    'source': pointer, 'record_count': manifest['record_count'],
    'current_source_or_target_mismatches': 0,
    'signals_live_receipt_sha256': sha(receipt_path),
    'previous_records_preserved': (backup / 'PRESERVED_FILES.json').relative_to(VAULT).as_posix(),
    'credential_audit': manifest['credential_audit'],
    'issue_register_validation': register,
    'deletions': 0, 'git_commit_created': False,
    'runtime_mode': 'ResearchCollectionOnly',
    'can_place_orders': False, 'prediction_improvement_demonstrated': False,
    'limitations': [
        'Only local OneDrive bytes verified; cloud sync completion not observed.',
        'No full runtime database recovery or broad database integrity pass claimed.',
        'The source-only archive excludes study databases and WAL files; restoring an existing study requires those plus its registered contract.',
        'Fresh original-H1 outcomes are required before predictive improvement can be evaluated; publication itself is not accuracy.'
    ]
}
with (VAULT / 'maintenance/SIGNALS_LIVE_EXPORT_VERIFICATION_20260907.json').open('xb') as f:
    f.write(encoded(verified))
records.write_bytes_atomic(VAULT / 'maintenance/LOCAL_VERIFICATION_CURRENT.json', encoded(verified))
with (OUT / 'VAULT_EXPORT_VERIFICATION.json').open('xb') as f: f.write(encoded(verified))
print(json.dumps({'status': verified['status'], 'record_count': manifest['record_count'],
    'archive': pointer['archive'], 'archive_sha256': pointer['archive_sha256'],
    'source_files': source_manifest['file_count'], 'verification': pointer['verification']}))

