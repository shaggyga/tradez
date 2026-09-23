"""Publish the verified optimization handoff without replacing prior receipts."""
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

receipt=load(ROOT/'FOREX_OPTIMIZATION_VALIDATION_20260906.json')
candidate_audit=load(OUT/'credential_final_scan.json')
assert candidate_audit['passed'] and candidate_audit['finding_count']==0
diff_check=load(OUT/'scoped_diff_check.json')
assert diff_check['passed']
register_validation=validate_register(ROOT/'FOREX_ISSUE_REGISTER_CURRENT.json',root=ROOT)
assert register_validation['valid'],register_validation['errors']
for row in receipt['source_bindings']:
    assert sha(ROOT/row['path'])==row['sha256'],'Reviewed source changed before export: '+row['path']

before=VAULT/'maintenance/before_optimization_20260906'
before.mkdir(parents=True,exist_ok=True)
names=['source/WORKTREE_SOURCE_LATEST.json','SHARED_PROJECT_STATE_CURRENT.json','maintenance/LOCAL_VERIFICATION_CURRENT.json']
for relative,destination in records.CANONICAL_PROJECT_RECORDS:
    target=VAULT/destination
    if target.is_file() and target.read_bytes()!=(ROOT.parent/relative).read_bytes():names.append(destination)
backups=[]
for name in sorted(set(names)):
    source=VAULT/name
    if not source.is_file():continue
    content=source.read_bytes();target=before/name
    target.parent.mkdir(parents=True,exist_ok=True)
    if target.exists():assert target.read_bytes()==content,'Pre-optimization backup differs: '+name
    else:
        with target.open('xb') as stream:stream.write(content)
    backups.append({'original':name,'backup':str(target.relative_to(VAULT)).replace('\\','/'),
                    'bytes':len(content),'sha256':hashlib.sha256(content).hexdigest()})
backup_manifest=before/'PRESERVED_FILES.json'
if not backup_manifest.exists():
    with backup_manifest.open('xb') as stream:stream.write(encoded({'generated_utc':datetime.now(timezone.utc).isoformat(),'files':backups,'deletions':0}))

records.sync_canonical_project_records(ROOT.parent,VAULT)
pointer=snapshot.sync_worktree_snapshot(ROOT,VAULT)
manifest=load(VAULT/'SHARED_PROJECT_STATE_CURRENT.json')
for row in manifest['records']:
    assert sha(VAULT/row['name'])==row['sha256']
    assert sha(ROOT.parent/row['source'])==row['sha256']
source_manifest=load(VAULT/'source'/pointer['manifest'])
for row in source_manifest['files']:assert sha(ROOT/row['path'])==row['sha256'],'Source changed after export: '+row['path']
verification={
    'schema_version':'forex_vault_optimization_export_verification_v1',
    'verified_utc':datetime.now(timezone.utc).isoformat(),
    'status':'local_source_and_records_verified_runtime_stopped',
    'source':{'pointer':'source/WORKTREE_SOURCE_LATEST.json','archive':pointer['archive'],
              'archive_sha256':pointer['archive_sha256'],'manifest':pointer['manifest'],
              'manifest_sha256':pointer['manifest_sha256'],'files_verified':source_manifest['file_count'],
              'verification':pointer['verification'],'current_worktree_member_mismatches':0},
    'records':{'manifest':'SHARED_PROJECT_STATE_CURRENT.json','manifest_sha256':sha(VAULT/'SHARED_PROJECT_STATE_CURRENT.json'),
               'records_verified':manifest['record_count'],'current_source_or_target_mismatches':0,
               'credential_preflight':manifest['credential_audit']},
    'optimization_receipt_sha256':sha(ROOT/'FOREX_OPTIMIZATION_VALIDATION_20260906.json'),
    'candidate_credential_audit':candidate_audit,
    'scoped_diff_check':diff_check,
    'issue_register_validation':register_validation,
    'prediction_review_sha256':sha(ROOT/'FOREX_PREDICTION_QUALITY_20260906.json'),
    'preserved_before_optimization_manifest':'maintenance/before_optimization_20260906/PRESERVED_FILES.json',
    'previous_source_archives_retained':True,'git_commit_created':False,'deletions':0,
    'runtime_restarted':False,'broker_requests':0,'production_database_writes':0,
    'limitations':['Verifies local OneDrive folder bytes; cloud synchronization completion not observed.',
                  'Saved runtime snapshots remain historical; optimization does not claim live readiness or predictive alpha.']}
dated=VAULT/'maintenance/OPTIMIZATION_EXPORT_VERIFICATION_20260906.json'
with dated.open('xb') as stream:stream.write(encoded(verification))
records.write_bytes_atomic(VAULT/'maintenance/LOCAL_VERIFICATION_CURRENT.json',encoded(verification))
(OUT/'VAULT_EXPORT_VERIFICATION.json').write_bytes(encoded(verification))
print(json.dumps(verification,indent=2))
