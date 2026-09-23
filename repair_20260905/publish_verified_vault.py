"""Publish only reviewed records and source; retain previous local receipts."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import sys

OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
VAULT=Path(r'C:\Users\zmoor\OneDrive\thevault\projects\forex')
sys.dont_write_bytecode=True
sys.path[:0]=[str(ROOT),str(ROOT.parent)]
import forex_model_vault_sync as records
from tools import vault_worktree_snapshot as snapshot
from oanda_issue_register_validator import validate_register

def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def encoded(value): return (json.dumps(value,indent=2)+'\n').encode()
def load(path): return json.loads(path.read_text(encoding='utf-8-sig'))

receipt=load(ROOT/'FOREX_REPAIR_VALIDATION_20260905.json')
register_validation=validate_register(ROOT/'FOREX_ISSUE_REGISTER_CURRENT.json',root=ROOT)
assert register_validation['valid'], register_validation['errors']
for row in receipt['source_bindings']:
    assert sha(ROOT/row['path'])==row['sha256'], 'Reviewed source changed before export: '+row['path']
performance=load(ROOT/'FOREX_PERFORMANCE_AUDIT_20260905.json')
assert performance['repair_fixture_benchmarks']['execution']==load(OUT/'execution/execution_fixture_performance.json')
assert performance['repair_fixture_benchmarks']['fastlane']==load(OUT/'fastlane/performance.json')

# Preserve exactly the current files being replaced, including the original
# current-pointer verification. The old immutable source ZIP is retained.
before=VAULT/'maintenance/before_repair_20260905'
before.mkdir(parents=True,exist_ok=True)
names=['source/WORKTREE_SOURCE_LATEST.json','SHARED_PROJECT_STATE_CURRENT.json','maintenance/LOCAL_VERIFICATION_CURRENT.json']
for relative,destination in records.CANONICAL_PROJECT_RECORDS:
    target=VAULT/destination
    if target.is_file() and target.read_bytes()!=(ROOT.parent/relative).read_bytes(): names.append(destination)
backups=[]
for name in sorted(set(names)):
    source=VAULT/name
    if not source.is_file(): continue
    payload=source.read_bytes();target=before/name
    target.parent.mkdir(parents=True,exist_ok=True)
    if target.exists():
        assert target.read_bytes()==payload,'Pre-repair backup already differs: '+name
    else:
        with target.open('xb') as stream: stream.write(payload)
    backups.append({'original':name,'backup':str(target.relative_to(VAULT)).replace('\\','/'),
                    'bytes':len(payload),'sha256':hashlib.sha256(payload).hexdigest()})
backup_manifest=before/'PRESERVED_FILES.json'
if not backup_manifest.exists():
    with backup_manifest.open('xb') as stream: stream.write(encoded({'generated_utc':datetime.now(timezone.utc).isoformat(),'files':backups,'deletions':0}))

published_records=records.sync_canonical_project_records(ROOT.parent,VAULT)
pointer=snapshot.sync_worktree_snapshot(ROOT,VAULT)
manifest=load(VAULT/'SHARED_PROJECT_STATE_CURRENT.json')
for row in manifest['records']:
    assert sha(VAULT/row['name'])==row['sha256']
    assert sha(ROOT.parent/row['source'])==row['sha256']
source_manifest=load(VAULT/'source'/pointer['manifest'])
for row in source_manifest['files']:
    assert sha(ROOT/row['path'])==row['sha256'], 'Source changed after export: '+row['path']
verification={
    'schema_version':'forex_vault_repair_export_verification_v1',
    'verified_utc':datetime.now(timezone.utc).isoformat(),
    'status':'local_source_and_records_verified_runtime_stopped',
    'source':{'pointer':'source/WORKTREE_SOURCE_LATEST.json','archive':pointer['archive'],
              'archive_sha256':pointer['archive_sha256'],'manifest':pointer['manifest'],
              'manifest_sha256':pointer['manifest_sha256'],'files_verified':source_manifest['file_count'],
              'verification':pointer['verification'],'current_worktree_member_mismatches':0},
    'records':{'manifest':'SHARED_PROJECT_STATE_CURRENT.json','manifest_sha256':sha(VAULT/'SHARED_PROJECT_STATE_CURRENT.json'),
               'records_verified':manifest['record_count'],'current_source_or_target_mismatches':0,
               'credential_preflight':manifest['credential_audit']},
    'repair_receipt_sha256':sha(ROOT/'FOREX_REPAIR_VALIDATION_20260905.json'),
    'issue_register_validation':register_validation,
    'performance_companion_sha256':sha(ROOT/'FOREX_PERFORMANCE_AUDIT_20260905.json'),
    'benchmark_companion_embeddings_verified':True,
    'preserved_before_repair_manifest':'maintenance/before_repair_20260905/PRESERVED_FILES.json',
    'previous_source_archive_retained':True,'git_commit_created':False,'deletions':0,
    'runtime_restarted':False,'broker_requests':0,'production_database_writes':0,
    'limitations':['Verifies local OneDrive folder bytes; cloud synchronization completion not observed.',
                  'Offline repair validation is not a live integrity pass or prospective profitability proof.']}
dated=VAULT/'maintenance/REPAIR_EXPORT_VERIFICATION_20260905.json'
with dated.open('xb') as stream: stream.write(encoded(verification))
records.write_bytes_atomic(VAULT/'maintenance/LOCAL_VERIFICATION_CURRENT.json',encoded(verification))
(OUT/'VAULT_EXPORT_VERIFICATION.json').write_bytes(encoded(verification))
print(json.dumps(verification,indent=2))
