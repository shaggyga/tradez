"""Publish frozen restart records and a verified source-only snapshot."""
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

def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def load(p): return json.loads(p.read_text(encoding='utf-8-sig'))
def encoded(p): return (json.dumps(p, indent=2) + '\n').encode()
receipt = load(ROOT/'FOREX_RESEARCH_RESTART_VALIDATION_20260906.json')
for row in receipt['bindings']: assert sha(ROOT/row['path']) == row['sha256'], row['path']
backup = VAULT/'maintenance/before_research_restart_20260906'
backup.mkdir(parents=True, exist_ok=False)
names = ['source/WORKTREE_SOURCE_LATEST.json', 'SHARED_PROJECT_STATE_CURRENT.json', 'maintenance/LOCAL_VERIFICATION_CURRENT.json']
for source, name in records.CANONICAL_PROJECT_RECORDS:
    target = VAULT/name
    if target.is_file() and target.read_bytes() != (ROOT.parent/source).read_bytes(): names.append(name)
preserved = []
for name in sorted(set(names)):
    source = VAULT/name
    if not source.exists(): continue
    target = backup/name; target.parent.mkdir(parents=True, exist_ok=True)
    raw = source.read_bytes()
    with target.open('xb') as f: f.write(raw)
    preserved.append({'path': name, 'backup': target.relative_to(VAULT).as_posix(), 'sha256': sha(target), 'bytes': len(raw)})
with (backup/'PRESERVED_FILES.json').open('xb') as f: f.write(encoded({'files': preserved, 'deletions': 0}))
records.sync_canonical_project_records(ROOT.parent, VAULT)
pointer = snapshot.sync_worktree_snapshot(ROOT, VAULT)
manifest = load(VAULT/'SHARED_PROJECT_STATE_CURRENT.json')
for row in manifest['records']:
    assert sha(VAULT/row['name']) == row['sha256'] == sha(ROOT.parent/row['source'])
source_manifest = load(VAULT/'source'/pointer['manifest'])
for row in source_manifest['files']: assert sha(ROOT/row['path']) == row['sha256'], row['path']
verified = {
    'schema_version': 'forex_research_restart_export_verification_v1',
    'verified_utc': datetime.now(timezone.utc).isoformat(),
    'status': 'local_source_and_frozen_restart_records_verified_collection_running',
    'source': pointer, 'record_count': manifest['record_count'],
    'current_source_or_target_mismatches': 0,
    'restart_receipt_sha256': sha(ROOT/'FOREX_RESEARCH_RESTART_VALIDATION_20260906.json'),
    'previous_records_preserved': (backup/'PRESERVED_FILES.json').relative_to(VAULT).as_posix(),
    'credential_audit': manifest['credential_audit'],
    'deletions': 0, 'git_commit_created': False,
    'runtime_mode': 'ResearchCollectionOnly',
    'limitations': ['Only local OneDrive bytes verified; cloud sync completion not observed.',
        'Runtime is changing after frozen observations; no full live DB recovery or current integrity pass claimed.']
}
with (VAULT/'maintenance/RESEARCH_RESTART_EXPORT_VERIFICATION_20260906.json').open('xb') as f: f.write(encoded(verified))
records.write_bytes_atomic(VAULT/'maintenance/LOCAL_VERIFICATION_CURRENT.json', encoded(verified))
with (OUT/'VAULT_EXPORT_VERIFICATION.json').open('xb') as f: f.write(encoded(verified))
print(json.dumps({'status': verified['status'], 'record_count': manifest['record_count'],
    'archive': pointer['archive'], 'archive_sha256': pointer['archive_sha256'],
    'source_files': source_manifest['file_count'], 'verification': pointer['verification']}))
