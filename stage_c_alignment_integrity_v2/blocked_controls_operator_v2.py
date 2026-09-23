"""Pinned offline day-shift and positive-leak controls over preserved evidence."""
import argparse,hashlib,importlib.metadata,json,platform,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
SOURCES=('blocked_controls_operator_v2.py','blocked_controls_report_v2.py','blocked_time_controls_v2.py','leak_positive_audit_v2.py','rich_dependence_operator_v2.py','rich_dependence_report_v2.py','paired_blocks_v2.py','campaign_inspector_v2.py','rolling_registry_operator_v2.py','rolling_registry_adapter_v2.py','contracts.py','publication.py')
REQUIRED=('blocked_control_scope.json','blocked_day_shifts.json','future_leak_audit.json','run_report.json')
PREDECESSORS={'oanda_rolling_technical_features_v1.py':'074a7b4fc138ef25a1e99b503ea693e0c31bc2e51a2e8f3753aab7556a6fb657','oanda_rolling_technical_panel_v1.py':'2426d0cf85dc1f1026d9ab49b0ade8925f8ba3a23730da0440a6b4c9293fb24e'}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def recipe_for(paths,raw,trad):
    sys.path.insert(0,str(ROOT))
    from rich_dependence_operator_v2 import recipe_for as dependency_recipe
    from rolling_registry_operator_v2 import checked_bytes
    dep=dependency_recipe(paths);rich=dep['dependencies']['rich']['identity']['contract']['recipe']
    if sha(raw/'INPUT_MANIFEST.json')!=rich['raw_manifest_sha256']:raise ValueError('original_rich_raw_manifest_required')
    m=read(raw/'INPUT_MANIFEST.json')
    if sorted(x['instrument'] for x in m['members'])!=dep['configuration']['universe']:raise ValueError('original_control_all68_required')
    for row in m['members']:checked_bytes(raw,row)
    for n,h in PREDECESSORS.items():
        if sha(trad/n)!=h:raise ValueError('canonical_control_kernel_changed')
    return {'schema_version':'forex_blocked_controls_recipe.v1','sources':{n:sha(ROOT/n) for n in SOURCES},'dependencies':dep['dependencies'],'predecessors':PREDECESSORS,'raw_manifest_sha256':rich['raw_manifest_sha256'],
        'environment':{'python':platform.python_version(),**{n:importlib.metadata.version(n) for n in ('numpy','pandas','pyarrow')}},'run_id':'blocked-time-and-positive-leak-controls','required_payloads':list(REQUIRED),
        'configuration':{'universe':dep['configuration']['universe'],'leak_origins':[1721736060,1721822460],'all_nonzero_cyclic_day_shifts':True,'confirmation':False,'model_fit_allowed':False,'network_allowed':False,'broker_access':False}}
def preflight(recipe,digest,paths,raw,trad):
    if recipe.is_symlink() or sha(recipe)!=digest:raise ValueError('control_recipe_hash_mismatch')
    r=read(recipe)
    if set(r['sources'])!=set(SOURCES) or any(sha(ROOT/n)!=h for n,h in r['sources'].items()):raise ValueError('control_source_drift_before_import')
    if r!=recipe_for(paths,raw,trad):raise ValueError('control_dependency_environment_drift')
    return r
def identity_for(r):
    from publication import effective_run_identity
    return effective_run_identity(contract={'recipe':r},dependency_hashes={**r['sources'],**r['predecessors'],'raw_manifest':r['raw_manifest_sha256'],**{k:v['identity']['fingerprint'] for k,v in r['dependencies'].items()}})
def operate(action,recipe,digest,paths,raw,trad,runs,crash_after=None):
    try:
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_control_action')
        r=preflight(recipe,digest,paths,raw,trad);sys.path.insert(0,str(trad));sys.path.insert(0,str(ROOT))
        from campaign_inspector_v2 import CampaignReader
        from blocked_controls_report_v2 import build
        from publication import RunPublisher,verify_completed_run
        identity=identity_for(r);root=runs/r['run_id']
        if action in {'run','resume'} and not (root/'COMPLETION_MANIFEST.json').exists():
            import os
            p=RunPublisher(runs,r['run_id'],identity);p.acquire(recover=action=='resume')
            try:
                outputs=build(CampaignReader(paths,r['dependencies']),raw,r);payloads=[]
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
            if {x['path'] for x in m['payloads']}!=set(REQUIRED):raise ValueError('control_payload_inventory_mismatch')
            report=read(root/'run_report.json')
            if report['paired_comparisons']!=336 or report['raw_audit_rows']!=136 or report['models_fitted']!=0 or report['selected_model'] is not None:raise ValueError('control_scope_mismatch')
            status='completed_verified'
        elif root.exists():
            if read(root/'RUN_IDENTITY.json')!=identity:raise ValueError('control_partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action=='verify' and status!='completed_verified':raise ValueError('completed_controls_required')
        return {'status':status,'run_identity':identity['fingerprint'],'run_path':str(root),'recipe_sha256':digest,'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run',
            'engineering_ready':False,'forecast_evidence_status':'development_association_controls_only','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False}
    except Exception as e:return {'status':'review_required','reason':str(e),'next_action':'preserve_evidence_and_escalate'}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','paths','raw-root','trad-root','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--test-crash-after',type=int);a=p.parse_args()
    r=operate(a.action,a.recipe,a.recipe_sha256,read(a.paths),a.raw_root,a.trad_root,a.runs_dir,a.test_crash_after);print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
