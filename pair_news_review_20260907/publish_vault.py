"""Publish the separately dated news, operational status and scoring repair record."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib, json, sys

OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
VAULT=Path(r'C:\Users\zmoor\OneDrive\thevault\projects\forex')
REPORT='docs/FOREX_OPERATIONAL_STATUS_REPAIR_20260907.md'
RECEIPT='FOREX_OPERATIONAL_STATUS_REPAIR_VALIDATION_20260907.json'
PRIOR_ARCHIVE='forex_worktree_source_885144f85e85de600e4470d8.zip'
PRIOR_ARCHIVE_SHA='dbdcd79d73a761c7b8f0e1d591d483878c05eb973b2668e0913e7e5e06e3881e'
PRIOR={
 'FOREX_PAIR_DASHBOARD_CONSISTENCY_VALIDATION_20260907.json':'b84890295124ae0dc6a0dae522f7c2aa51e0db5b55302287f9a54859ec966d59',
 'FOREX_PAIR_FORECAST_COVERAGE_VALIDATION_20260907.json':'6953f2d7259be748847ec0dd6a34abd1b2f482dd8be7aa6913bc7d906ff4cfb5',
 'FOREX_SIGNALS_LIVE_OPTIMIZATION_VALIDATION_20260907.json':'3671297affd5addd4c2e4080c66d4d74382c24678a3430d7e4694814e5993792'}
sys.dont_write_bytecode=True
sys.path[:0]=[str(ROOT),str(ROOT.parent)]
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def load(path):return json.loads(path.read_text(encoding='utf-8-sig'))
def encoded(value):return (json.dumps(value,indent=2)+'\n').encode()

def main():
    import forex_model_vault_sync as records
    from tools import vault_worktree_snapshot as snapshot
    from oanda_issue_register_validator import validate_register
    receipt=load(ROOT/RECEIPT)
    receipt_sha=sha(ROOT/RECEIPT)
    for name,value in PRIOR.items():assert sha(ROOT/name)==receipt['prior_receipts_unchanged'][name]==value
    pointer=load(VAULT/'source/WORKTREE_SOURCE_LATEST.json')
    assert pointer['archive']==PRIOR_ARCHIVE and pointer['archive_sha256']==PRIOR_ARCHIVE_SHA
    assert sha(VAULT/'source'/PRIOR_ARCHIVE)==PRIOR_ARCHIVE_SHA
    assert len(records.CANONICAL_PROJECT_RECORDS)==149
    sources=[str(source).replace('\\','/') for source,_ in records.CANONICAL_PROJECT_RECORDS]
    for name in (REPORT,RECEIPT):assert sources.count('trad/'+name)==1
    for row in receipt['source_bindings']:assert sha(ROOT/row['path'])==row['sha256'],row['path']
    register=validate_register(ROOT/'FOREX_ISSUE_REGISTER_CURRENT.json',root=ROOT)
    assert register['valid']
    backup=VAULT/'maintenance/before_operational_status_repair_20260907'
    backup.mkdir(parents=True,exist_ok=False)
    names={'source/WORKTREE_SOURCE_LATEST.json','SHARED_PROJECT_STATE_CURRENT.json','maintenance/LOCAL_VERIFICATION_CURRENT.json'}
    for source,name in records.CANONICAL_PROJECT_RECORDS:
        target=VAULT/name
        if target.is_file() and target.read_bytes()!=(ROOT.parent/source).read_bytes():names.add(name)
    preserved=[]
    for name in sorted(names):
        source=VAULT/name
        if not source.exists():continue
        target=backup/name
        target.parent.mkdir(parents=True,exist_ok=True)
        with target.open('xb') as handle:handle.write(source.read_bytes())
        preserved.append({'path':name,'sha256':sha(target)})
    (backup/'PRESERVED_FILES.json').write_bytes(encoded({'files':preserved,'deletions':0}))
    print('Previous records preserved; publishing 149 canonical records.',flush=True)
    records.sync_canonical_project_records(ROOT.parent,VAULT)
    print('Publishing and verifying source archive.',flush=True)
    pointer=snapshot.sync_worktree_snapshot(ROOT,VAULT)
    manifest=load(VAULT/'SHARED_PROJECT_STATE_CURRENT.json')
    assert manifest['record_count']==149
    for row in manifest['records']:assert sha(VAULT/row['name'])==row['sha256']==sha(ROOT.parent/row['source'])
    source_manifest=load(VAULT/'source'/pointer['manifest'])
    for row in source_manifest['files']:assert sha(ROOT/row['path'])==row['sha256'],row['path']
    for row in receipt['source_bindings']:assert sha(ROOT/row['path'])==row['sha256'],row['path']
    for name,value in PRIOR.items():assert sha(ROOT/name)==value
    assert sha(VAULT/'source'/PRIOR_ARCHIVE)==PRIOR_ARCHIVE_SHA
    assert sha(ROOT/RECEIPT)==receipt_sha
    verified={'schema_version':'forex_operational_status_repair_export_v1','verified_utc':datetime.now(timezone.utc).isoformat(),
      'status':'local_source_and_records_verified','record_count':149,'source':pointer,
      'current_source_or_target_mismatches':0,'receipt_sha256':receipt_sha,'prior_receipts_unchanged':PRIOR,
      'prior_archive_preserved':{'archive':PRIOR_ARCHIVE,'sha256':PRIOR_ARCHIVE_SHA},
      'previous_records_preserved':str((backup/'PRESERVED_FILES.json').relative_to(VAULT)),
      'credential_audit':manifest['credential_audit'],'issue_register_validation':register,
      'deletions':0,'git_commit_created':False,'research_only':True,'can_place_orders':False,
      'prediction_improvement_demonstrated':False,
      'limitations':['Local OneDrive bytes verified; cloud synchronization not observed.',
        'Source archive excludes private databases and credentials.',
        'Supplemental engineering diagnostics do not replace registered scorecards or establish trading readiness.']}
    with (VAULT/'maintenance/OPERATIONAL_STATUS_REPAIR_EXPORT_VERIFICATION_20260907.json').open('xb') as handle:handle.write(encoded(verified))
    records.write_bytes_atomic(VAULT/'maintenance/LOCAL_VERIFICATION_CURRENT.json',encoded(verified))
    with (OUT/'VAULT_EXPORT_VERIFICATION.json').open('xb') as handle:handle.write(encoded(verified))
    print(json.dumps({'status':verified['status'],'record_count':149,'archive':pointer['archive'],
      'archive_sha256':pointer['archive_sha256'],'source_files':source_manifest['file_count'],'verification':pointer['verification']}))

if __name__=='__main__':main()
