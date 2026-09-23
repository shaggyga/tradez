"""Resume the interrupted source-only export; never recopy canonical records.

Requires the separately reviewed scanner validation receipt and its exact hash.
The operational receipt, its 87 bindings, 149 copied records and original backup
must already match before source snapshot publication can begin.
"""
from pathlib import Path
from datetime import datetime, timezone
import argparse, hashlib, json, sys

sys.dont_write_bytecode = True
OUT = Path(__file__).resolve().parent
ROOT = OUT.parent/'trad'
VAULT = Path(r'C:\Users\zmoor\OneDrive\thevault\projects\forex')
RECEIPT = 'FOREX_OPERATIONAL_STATUS_REPAIR_VALIDATION_20260907.json'
RECEIPT_SHA = '7c07ec243ca3d822a4bb56118482247a84e7545c343a515214f1d340bf8c76eb'
PRIOR_ARCHIVE = 'forex_worktree_source_885144f85e85de600e4470d8.zip'
PRIOR_ARCHIVE_SHA = 'dbdcd79d73a761c7b8f0e1d591d483878c05eb973b2668e0913e7e5e06e3881e'
PRIOR = {
 'FOREX_PAIR_DASHBOARD_CONSISTENCY_VALIDATION_20260907.json':'b84890295124ae0dc6a0dae522f7c2aa51e0db5b55302287f9a54859ec966d59',
 'FOREX_PAIR_FORECAST_COVERAGE_VALIDATION_20260907.json':'6953f2d7259be748847ec0dd6a34abd1b2f482dd8be7aa6913bc7d906ff4cfb5',
 'FOREX_SIGNALS_LIVE_OPTIMIZATION_VALIDATION_20260907.json':'3671297affd5addd4c2e4080c66d4d74382c24678a3430d7e4694814e5993792',
}
BACKUP = VAULT/'maintenance/before_operational_status_repair_20260907'

def load(p): return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def encoded(v): return (json.dumps(v,indent=2)+'\n').encode('utf-8')
def bounded_path(root,name):
    p=(root/name).resolve()
    if not p.is_relative_to(root.resolve()) or p==root.resolve():
        raise ValueError('path_outside_expected_root')
    return p
def operational_bindings():
    if sha(ROOT/RECEIPT)!=RECEIPT_SHA: raise ValueError('operational_receipt_changed')
    r=load(ROOT/RECEIPT)
    if len(r['source_bindings'])!=87: raise ValueError('operational_binding_count_changed')
    for row in r['source_bindings']:
        if sha(bounded_path(ROOT,row['path']))!=row['sha256']:
            raise ValueError('operational_source_binding_changed:'+row['path'])
    for name,digest in PRIOR.items():
        if sha(ROOT/name)!=digest or r['prior_receipts_unchanged'][name]!=digest:
            raise ValueError('prior_receipt_changed:'+name)
    for name in ('FOREX_AUDIT_STATE_CURRENT.json','FOREX_COMMONS_CURRENT.json'):
        if load(ROOT/name)['latest_review']['validation_sha256']!=RECEIPT_SHA:
            raise ValueError('latest_review_receipt_binding_changed')
    return r
def canonical_records(records):
    manifest=load(VAULT/'SHARED_PROJECT_STATE_CURRENT.json')
    if len(records.CANONICAL_PROJECT_RECORDS)!=149 or manifest['record_count']!=149 or len(manifest['records'])!=149:
        raise ValueError('canonical_record_count_changed')
    expected={(str(source).replace('\\','/'),name) for source,name in records.CANONICAL_PROJECT_RECORDS}
    actual={(row['source'].replace('\\','/'),row['name']) for row in manifest['records']}
    if actual!=expected: raise ValueError('canonical_record_mapping_changed')
    for row in manifest['records']:
        if not sha(bounded_path(VAULT,row['name']))==row['sha256']==sha(bounded_path(ROOT.parent,row['source'])):
            raise ValueError('already_copied_canonical_record_changed:'+row['name'])
    return manifest
def backup_bindings():
    index=BACKUP/'PRESERVED_FILES.json'
    payload=load(index)
    if payload['deletions']!=0: raise ValueError('unexpected_backup_deletions')
    for row in payload['files']:
        if sha(bounded_path(BACKUP,row['path']))!=row['sha256']:
            raise ValueError('preserved_backup_changed:'+row['path'])
    return sha(index)
def scanner_validation(path,expected):
    path=path.resolve()
    if not path.is_relative_to(ROOT.resolve()) or sha(path)!=expected:
        raise ValueError('scanner_validation_receipt_binding_mismatch')
    value=load(path)
    if value.get('status')!='passed': raise ValueError('scanner_validation_not_passed')
    rows=value.get('source_bindings')
    if not isinstance(rows,list) or not rows: raise ValueError('scanner_validation_has_no_bindings')
    checked=[]
    for row in rows:
        p=bounded_path(ROOT,row['path'])
        if sha(p)!=row['sha256']: raise ValueError('scanner_validation_source_changed:'+row['path'])
        checked.append({'path':p.relative_to(ROOT).as_posix(),'sha256':row['sha256']})
    if not any(row['path']=='tools/credential_audit.py' for row in checked):
        raise ValueError('shared_scanner_binding_missing')
    helper_rows=[row for row in checked if Path(row['path']).name=='resume_verified_export.py']
    if len(helper_rows)!=1 or sha(Path(__file__))!=helper_rows[0]['sha256']:
        raise ValueError('executing_resume_helper_not_bound_by_scanner_receipt')
    report=value.get('report')
    if not isinstance(report,dict) or report not in checked:
        raise ValueError('scanner_report_not_source_bound')
    return {'status':'passed','validation_receipt':path.relative_to(ROOT).as_posix(),
            'validation_sha256':expected,'source_and_evidence_bindings':checked,
            'report':report,'tests':value.get('tests'),
            'scope':'Separately reviewed scan performance repair; original operational receipt and all 87 bindings preserved.'}

def maintenance_scanner_copies(scanner):
    """Publish two separately bound maintenance records outside the 149 mapping."""
    copied=[]
    sources=[scanner['report'],{'path':scanner['validation_receipt'],
                               'sha256':scanner['validation_sha256']}]
    for row in sources:
        source=bounded_path(ROOT,row['path'])
        target=bounded_path(VAULT,'maintenance/'+source.name)
        payload=source.read_bytes()
        if hashlib.sha256(payload).hexdigest()!=row['sha256']:
            raise ValueError('scanner_maintenance_source_changed')
        with target.open('xb') as handle: handle.write(payload)
        if sha(target)!=row['sha256']:
            raise ValueError('scanner_maintenance_copy_mismatch')
        copied.append({'source':row['path'],'target':target.relative_to(VAULT).as_posix(),
                       'sha256':row['sha256'],'canonical_mapping_member':False})
    return copied

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scanner-validation',type=Path,required=True)
    parser.add_argument('--scanner-validation-sha256',required=True)
    args=parser.parse_args()
    scanner=scanner_validation(args.scanner_validation,args.scanner_validation_sha256)
    sys.path[:0]=[str(ROOT),str(ROOT.parent)]
    import forex_model_vault_sync as records
    from tools import vault_worktree_snapshot as snapshot
    from oanda_issue_register_validator import validate_register
    operational_bindings()
    manifest=canonical_records(records)
    backup_sha=backup_bindings()
    pointer=load(VAULT/'source/WORKTREE_SOURCE_LATEST.json')
    if pointer['archive']!=PRIOR_ARCHIVE or pointer['archive_sha256']!=PRIOR_ARCHIVE_SHA:
        raise ValueError('preceding_source_pointer_changed')
    if sha(VAULT/'source'/PRIOR_ARCHIVE)!=PRIOR_ARCHIVE_SHA:
        raise ValueError('preceding_source_archive_changed')
    if any((VAULT/'source').glob('.worktree_snapshot_*')):
        raise ValueError('unexpected_staged_source_snapshot')
    for name in (Path(scanner['report']['path']).name,Path(scanner['validation_receipt']).name,
                 'OPERATIONAL_STATUS_REPAIR_EXPORT_VERIFICATION_20260907.json'):
        if bounded_path(VAULT,'maintenance/'+name).exists():
            raise ValueError('new_maintenance_target_already_exists:'+name)
    if (OUT/'VAULT_EXPORT_VERIFICATION.json').exists():
        raise ValueError('workspace_export_receipt_already_exists')
    register=validate_register(ROOT/'FOREX_ISSUE_REGISTER_CURRENT.json',root=ROOT)
    if not register['valid']: raise ValueError('issue_register_invalid')
    print('Resume preflight passed: all 87 bindings, 149 existing local records, preceding archive and original backup verified. Canonical copies are not repeated.',flush=True)
    print('Publishing and verifying source archive with the separately validated scanner.',flush=True)
    pointer=snapshot.sync_worktree_snapshot(ROOT,VAULT)
    source_manifest=load(VAULT/'source'/pointer['manifest'])
    for row in source_manifest['files']:
        if sha(ROOT/row['path'])!=row['sha256']: raise ValueError('snapshot_source_changed:'+row['path'])
    operational_bindings()
    manifest=canonical_records(records)
    if backup_bindings()!=backup_sha: raise ValueError('backup_index_changed')
    scanner_validation(args.scanner_validation,args.scanner_validation_sha256)
    if sha(VAULT/'source'/PRIOR_ARCHIVE)!=PRIOR_ARCHIVE_SHA:
        raise ValueError('preceding_source_archive_changed_after_export')
    scanner['verified_local_maintenance_copies']=maintenance_scanner_copies(scanner)
    verified={'schema_version':'forex_operational_status_repair_export_v1',
        'verified_utc':datetime.now(timezone.utc).isoformat(),'status':'local_source_and_records_verified',
        'record_count':149,'source':pointer,'current_source_or_target_mismatches':0,
        'receipt_sha256':RECEIPT_SHA,'prior_receipts_unchanged':PRIOR,
        'prior_archive_preserved':{'archive':PRIOR_ARCHIVE,'sha256':PRIOR_ARCHIVE_SHA},
        'previous_records_preserved':str((BACKUP/'PRESERVED_FILES.json').relative_to(VAULT)),
        'previous_records_index_sha256':backup_sha,'scanner_performance_repair':scanner,
        'resume':{'canonical_records_recopied':False,'original_backup_recreated':False,
                  'interrupted_publisher':'PUBLISHER_SCAN_INTERRUPTION.json',
                  'original_operational_receipt_unchanged':True,'operational_bindings_verified':87},
        'credential_audit':manifest['credential_audit'],'issue_register_validation':register,
        'deletions':0,'git_commit_created':False,'research_only':True,'can_place_orders':False,
        'prediction_improvement_demonstrated':False,
        'limitations':['Local OneDrive bytes verified; cloud synchronization not observed.',
            'Source archive excludes private databases and credentials.',
            'Supplemental engineering diagnostics do not replace registered scorecards or establish trading readiness.',
            'The scanner follow-up is source-bound separately; it does not rewrite the earlier operational validation.']}
    with (VAULT/'maintenance/OPERATIONAL_STATUS_REPAIR_EXPORT_VERIFICATION_20260907.json').open('xb') as handle:
        handle.write(encoded(verified))
    records.write_bytes_atomic(VAULT/'maintenance/LOCAL_VERIFICATION_CURRENT.json',encoded(verified))
    with (OUT/'VAULT_EXPORT_VERIFICATION.json').open('xb') as handle:handle.write(encoded(verified))
    print(json.dumps({'status':verified['status'],'record_count':149,'archive':pointer['archive'],
        'archive_sha256':pointer['archive_sha256'],'source_files':source_manifest['file_count'],
        'verification':pointer['verification'],'scanner_validation_sha256':args.scanner_validation_sha256}))

if __name__=='__main__':main()
