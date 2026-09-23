"""Publish reviewed fixed-evaluation records/source without running the project."""
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

def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def load(path):return json.loads(path.read_text(encoding='utf-8-sig'))
def encoded(value):return (json.dumps(value,indent=2)+'\n').encode()
receipt=load(ROOT/'FOREX_FIXED_EVALUATION_VALIDATION_20260906.json')
candidate=load(OUT/'credential_final_scan.json')
assert candidate['passed'] and candidate['finding_count']==0
for row in receipt['source_bindings']:assert sha(ROOT/row['path'])==row['sha256'],row['path']
register=validate_register(ROOT/'FOREX_ISSUE_REGISTER_CURRENT.json',root=ROOT)
assert register['valid'],register

backup_root=VAULT/'maintenance/before_fixed_evaluation_20260906'
backup_root.mkdir(parents=True,exist_ok=True)
names=['source/WORKTREE_SOURCE_LATEST.json','SHARED_PROJECT_STATE_CURRENT.json','maintenance/LOCAL_VERIFICATION_CURRENT.json']
for relative,destination in records.CANONICAL_PROJECT_RECORDS:
    target=VAULT/destination
    if target.is_file() and target.read_bytes()!=(ROOT.parent/relative).read_bytes():names.append(destination)
backups=[]
for name in sorted(set(names)):
    source=VAULT/name
    if not source.exists():continue
    raw=source.read_bytes();target=backup_root/name
    target.parent.mkdir(parents=True,exist_ok=True)
    if target.exists():assert target.read_bytes()==raw,'Backup differs: '+name
    else:
        with target.open('xb') as stream:stream.write(raw)
    backups.append({'original':name,'backup':target.relative_to(VAULT).as_posix(),
                    'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()})
backup_manifest=backup_root/'PRESERVED_FILES.json'
if not backup_manifest.exists():
    with backup_manifest.open('xb') as stream:stream.write(encoded({'generated_utc':datetime.now(timezone.utc).isoformat(),'files':backups,'deletions':0}))
records.sync_canonical_project_records(ROOT.parent,VAULT)
pointer=snapshot.sync_worktree_snapshot(ROOT,VAULT)
manifest=load(VAULT/'SHARED_PROJECT_STATE_CURRENT.json')
for row in manifest['records']:
    assert sha(VAULT/row['name'])==row['sha256']==sha(ROOT.parent/row['source'])
source_manifest=load(VAULT/'source'/pointer['manifest'])
for row in source_manifest['files']:assert sha(ROOT/row['path'])==row['sha256'],row['path']
verified={'schema_version':'forex_vault_fixed_evaluation_export_verification_v1',
    'verified_utc':datetime.now(timezone.utc).isoformat(),'status':'local_records_and_source_verified_runtime_stopped',
    'source':{'archive':pointer['archive'],'archive_sha256':pointer['archive_sha256'],
        'manifest':pointer['manifest'],'manifest_sha256':pointer['manifest_sha256'],
        'files_verified':source_manifest['file_count'],'verification':pointer['verification'],
        'current_worktree_member_mismatches':0},
    'records':{'records_verified':manifest['record_count'],'manifest_sha256':sha(VAULT/'SHARED_PROJECT_STATE_CURRENT.json'),
        'current_source_or_target_mismatches':0,'credential_preflight':manifest['credential_audit']},
    'evaluation_receipt_sha256':sha(ROOT/'FOREX_FIXED_EVALUATION_VALIDATION_20260906.json'),
    'results_sha256':sha(ROOT/'FOREX_FIXED_EVALUATION_RESULTS_20260906.json'),
    'candidate_credential_audit':candidate,'issue_register_validation':register,
    'previous_records_preserved':backup_manifest.relative_to(VAULT).as_posix(),
    'earlier_source_archives_retained':True,'git_commit_created':False,'deletions':0,
    'runtime_restarted':False,'collection_started':False,'broker_requests':0,'production_logical_writes':0,
    'limitations':['Local OneDrive bytes verified; remote cloud sync completion not observed.',
        'Bounded data extract retained; full production DB and private credentials excluded.',
        'Engineering/archival evaluation is not prospective performance or runtime readiness.']}
dated=VAULT/'maintenance/FIXED_EVALUATION_EXPORT_VERIFICATION_20260906.json'
with dated.open('xb') as stream:stream.write(encoded(verified))
records.write_bytes_atomic(VAULT/'maintenance/LOCAL_VERIFICATION_CURRENT.json',encoded(verified))
(OUT/'VAULT_EXPORT_VERIFICATION.json').write_bytes(encoded(verified))
print(json.dumps(verified,indent=2))
