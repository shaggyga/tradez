"""Frozen, offline retained OfficialFact context recovery and inspection."""
import argparse,hashlib,json,platform,sys,sqlite3,importlib.metadata
from pathlib import Path
ROOT=Path(__file__).resolve().parent
SOURCES=('official_context_operator_v2.py','official_context_core_v2.py','contracts.py','publication.py')
REQUIRED=('official_original_snapshots.json','official_pair_context.json','macro_evidence_summary.json','run_report.json')
AUDIT_NAMES=('official_document_sla_v1.json','macro_surprise_v1.json','macro_consensus_prospective_v1.json')
CUTOFFS=['2024-07-26T18:01:00+00:00','2026-08-17T08:00:00+00:00','2026-08-17T08:10:00+00:00']
MANIFEST='config/official_fact_adapter_v5_manifest.json'
MANIFEST_SHA='3b4a6589253c8e6a6974840527760eda5b24ec87c109bb0e7c694d266b21b332'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def safe_input(root,name,limit=2097152):
    from pathlib import PurePosixPath
    q=PurePosixPath(name)
    if q.is_absolute() or any(x in ('','..','.') for x in q.parts) or '\\' in name or ':' in name:raise ValueError('official_input_relative_path_required')
    p=root.joinpath(*q.parts)
    if not p.is_file() or p.stat().st_size>limit:raise ValueError('official_input_missing_or_oversize')
    cur=p
    while cur!=root.parent:
        if cur.is_symlink() or cur.is_junction():raise ValueError('official_input_link_refused')
        cur=cur.parent
    return p
def recipe_for(retained,audit_root,universe):
    retained=Path(retained).resolve();audit_root=Path(audit_root).resolve();manifest=safe_input(retained,MANIFEST)
    if sha(manifest)!=MANIFEST_SHA:raise ValueError('original_official_manifest_hash_required')
    m=read(manifest);files={MANIFEST:MANIFEST_SHA}
    for x in m['artifacts'].values():
        p=safe_input(retained,x['relative_path'])
        if p.stat().st_size!=x['byte_length'] or sha(p)!=x['sha256']:raise ValueError('original_official_artifact_hash_required:'+x['relative_path'])
        files[x['relative_path']]=x['sha256']
    audits={name:sha(safe_input(audit_root,name,262144)) for name in AUDIT_NAMES}
    if len(universe)!=68 or len(set(universe))!=68 or universe!=sorted(universe):raise ValueError('sorted_68_original_universe_required')
    return {'schema_version':'forex_retained_official_context_recipe.v1','run_id':'retained-official-context','sources':{n:sha(ROOT/n) for n in SOURCES},
        'retained':files,'audit_inputs':audits,'environment':{'python':platform.python_version(),'numpy':importlib.metadata.version('numpy'),'sqlite':sqlite3.sqlite_version,'tzdata':importlib.metadata.version('tzdata'),'os':platform.system()},'required_payloads':list(REQUIRED),
        'configuration':{'universe':universe,'cutoffs':CUTOFFS,'network_allowed':False,'active_databases_allowed':False,'model_fit_allowed':False,'broker_access':False}}
def preflight(recipe,digest,retained,audit_root):
    if recipe.is_symlink() or sha(recipe)!=digest:raise ValueError('official_recipe_hash_mismatch')
    r=read(recipe)
    if set(r['sources'])!=set(SOURCES) or any(sha(ROOT/n)!=h for n,h in r['sources'].items()):raise ValueError('official_source_drift_before_import')
    if r!=recipe_for(retained,audit_root,r['configuration']['universe']):raise ValueError('official_dependency_environment_drift')
    return r
def identity_for(r):
    from publication import effective_run_identity
    return effective_run_identity(contract={'recipe':r},dependency_hashes={**r['sources'],**r['retained'],**r['audit_inputs']})
def operate(action,recipe,digest,retained,audit_root,runs,crash_after=None):
    try:
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_official_context_action')
        r=preflight(recipe,digest,retained,audit_root);sys.path.insert(0,str(ROOT))
        from official_context_core_v2 import build
        from publication import RunPublisher,verify_completed_run
        identity=identity_for(r);root=runs/r['run_id']
        if action in {'run','resume'} and not (root/'COMPLETION_MANIFEST.json').exists():
            import os
            p=RunPublisher(runs,r['run_id'],identity);p.acquire(recover=action=='resume')
            try:
                outputs=build(retained,audit_root,r['configuration'],r['audit_inputs']);payloads=[]
                for index,name in enumerate(REQUIRED,1):
                    payloads.append(p.write_or_validate_payload(name,(json.dumps(outputs[name],sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()))
                    if crash_after==index:os._exit(91)
                if crash_after==0:os._exit(91)
                p.complete(payloads,set(REQUIRED))
            except BaseException:
                if p._owner_token is not None:p.release()
                raise
        if (root/'COMPLETION_MANIFEST.json').exists():
            m=verify_completed_run(root,identity)
            if {x['path'] for x in m['payloads']}!=set(REQUIRED):raise ValueError('official_payload_inventory_mismatch')
            report=read(root/'run_report.json')
            if report['pair_context_rows']!=204 or report['macro_semantic_numeric_values_admitted']!=0:raise ValueError('official_context_scope_mismatch')
            status='completed_verified'
        elif root.exists():
            if read(root/'RUN_IDENTITY.json')!=identity:raise ValueError('official_partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action=='verify' and status!='completed_verified':raise ValueError('completed_official_context_required')
        return {'status':status,'run_identity':identity['fingerprint'],'run_path':str(root),'recipe_sha256':digest,
            'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run',
            'engineering_ready':False,'forecast_evidence_status':'retained_context_only_no_predictive_macro_evidence','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False}
    except Exception as e:return {'status':'review_required','reason':str(e),'next_action':'preserve_evidence_and_escalate'}

def inspect_pair(recipe,digest,retained,audit_root,runs,instrument,cutoff):
    result=operate('verify',recipe,digest,retained,audit_root,runs)
    if result['status']!='completed_verified':return result
    root=Path(result['run_path']);r=read(recipe)
    if instrument not in r['configuration']['universe'] or cutoff not in r['configuration']['cutoffs']:
        return {'status':'review_required','reason':'inspection_requires_declared_pair_and_cutoff','next_action':'use_declared_pair_and_cutoff'}
    # Verify the bytes consumed for inspection, rather than trusting an earlier read.
    expected={x['path']:x['sha256'] for x in read(root/'COMPLETION_MANIFEST.json')['payloads']}
    def consumed(name):
        raw=(root/name).read_bytes()
        if len(raw)>2097152 or hashlib.sha256(raw).hexdigest()!=expected[name]:raise ValueError('official_inspection_consumed_bytes_changed')
        return json.loads(raw)
    try:
        pair=next(x for x in consumed('official_pair_context.json') if x['instrument']==instrument and x['decision_cutoff_utc']==cutoff)
        original=next(x for x in consumed('official_original_snapshots.json') if x['cutoff']==cutoff)
        currencies=instrument.split('_');snapshot=original['original']
        return {'status':'inspection_verified','run_identity':result['run_identity'],'pair_context':pair,
            'contributing_facts':[x for x in snapshot['facts'] if x['currency'] in currencies] if snapshot else [],
            'contributing_events':[x for x in snapshot['upcoming_events'] if x['currency'] in currencies] if snapshot else [],
            'source_text_is_untrusted_data':True,'outcomes_included':False,'execution_eligible':False}
    except Exception as e:return {'status':'review_required','reason':str(e),'next_action':'preserve_evidence_and_escalate'}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify','inspect'])
    for n in ('recipe','retained','audit-inputs','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--test-crash-after',type=int);p.add_argument('--instrument');p.add_argument('--cutoff');a=p.parse_args()
    r=inspect_pair(a.recipe,a.recipe_sha256,a.retained,a.audit_inputs,a.runs_dir,a.instrument,a.cutoff) if a.action=='inspect' else operate(a.action,a.recipe,a.recipe_sha256,a.retained,a.audit_inputs,a.runs_dir,a.test_crash_after)
    print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
