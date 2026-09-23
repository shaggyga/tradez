"""Frozen canonical-rich input expansion, with original endpoint evidence kept separate."""
import argparse,hashlib,importlib.metadata,json,platform,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
SOURCES=('rich_campaign_operator_v2.py','rich_campaign_runner_v2.py','rich_campaign_prepare_v2.py','rich_campaign_consumer_v2.py','rolling_registry_adapter_v2.py','rolling_registry_operator_v2.py','contracts.py','publication.py')
PREDECESSORS={'oanda_rolling_technical_features_v1.py':'074a7b4fc138ef25a1e99b503ea693e0c31bc2e51a2e8f3753aab7556a6fb657','oanda_rolling_technical_panel_v1.py':'2426d0cf85dc1f1026d9ab49b0ade8925f8ba3a23730da0440a6b4c9293fb24e'}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def validate(root,contract,lineage,technical,trad):
    sys.path.insert(0,str(ROOT))
    from rolling_registry_operator_v2 import checked_bytes
    m=read(root/'INPUT_MANIFEST.json')
    if m['origins']!=list(range(1720396860,1722038400,21600)) or m['support_bars']!=603 or len(m['members'])!=68 or len({r['instrument'] for r in m['members']})!=68:raise ValueError('rich_campaign_all68_76origins_required')
    if m['preparation_contract_sha256']!=sha(contract) or m['preparation_source_sha256']!=sha(ROOT/'rich_campaign_prepare_v2.py'):raise ValueError('rich_preparation_identity_mismatch')
    for row in m['members']:
        if row['path']!=row['instrument']+'.parquet':raise ValueError('rich_raw_pair_identity_mismatch')
        checked_bytes(root,row)
    for n,h in PREDECESSORS.items():
        if sha(trad/n)!=h:raise ValueError('canonical_rich_predecessor_changed')
    i=read(technical/'RUN_IDENTITY.json');manifest=read(technical/'COMPLETION_MANIFEST.json')
    if manifest['run_identity']!=i:raise ValueError('original_technical_identity_mismatch')
    for row in manifest['payloads']:checked_bytes(technical,row)
    ref=read(lineage.with_name('LINEAGE_REFERENCE.json'))
    if sha(lineage)!=ref['payload_sha256']:raise ValueError('recovered_lineage_payload_changed')
    return m,i,manifest,ref
def recipe_for(root,contract,lineage,technical,trad):
    m,i,manifest,ref=validate(root,contract,lineage,technical,trad)
    return {'schema_version':'forex_rich_campaign_inputs_recipe.v1','sources':{n:sha(ROOT/n) for n in SOURCES},'predecessors':PREDECESSORS,
        'raw_manifest_sha256':sha(root/'INPUT_MANIFEST.json'),'preparation_contract_sha256':sha(contract),'lineage_sha256':sha(lineage),'lineage_reference':ref,
        'technical_identity':i,'technical_payloads':{x['path']:x['sha256'] for x in manifest['payloads']},
        'environment':{'python':platform.python_version(),**{n:importlib.metadata.version(n) for n in ('numpy','pandas','pyarrow')}},
        'universe':sorted(r['instrument'] for r in m['members']),'origins':m['origins'],'run_id':'rich-campaign-inputs','fit_allowed':False,'broker_access':False}
def preflight(recipe,digest,root,contract,lineage,technical,trad):
    if recipe.is_symlink() or sha(recipe)!=digest:raise ValueError('rich_recipe_hash_mismatch')
    r=read(recipe)
    if set(r['sources'])!=set(SOURCES) or any(sha(ROOT/n)!=h for n,h in r['sources'].items()):raise ValueError('rich_source_drift_before_import')
    if r!=recipe_for(root,contract,lineage,technical,trad):raise ValueError('rich_dependency_environment_drift')
    return r
def operate(action,recipe,digest,root,contract,lineage,technical,trad,runs,crash_after=None):
    try:
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_rich_input_action')
        r=preflight(recipe,digest,root,contract,lineage,technical,trad);sys.path.insert(0,str(trad));sys.path.insert(0,str(ROOT))
        from rich_campaign_runner_v2 import run,identity_for,required
        from publication import verify_completed_run
        verify_completed_run(technical,r['technical_identity']);i=identity_for(r);p=runs/r['run_id']
        if action in {'run','resume'}:run(root,lineage,technical,r,runs,resume=action=='resume',crash_after=crash_after)
        if (p/'COMPLETION_MANIFEST.json').exists():
            m=verify_completed_run(p,i)
            if {x['path'] for x in m['payloads']}!=set(required(r)):raise ValueError('rich_input_payload_inventory_mismatch')
            report=read(p/'run_report.json')
            if report['instruments']!=68 or report['origin_rows']!=5168 or report['feature_count']!=228 or report['models_fitted']!=0:raise ValueError('rich_input_acceptance_missing')
            status='completed_verified'
        elif p.exists():
            if read(p/'RUN_IDENTITY.json')!=i:raise ValueError('rich_partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action=='verify' and status!='completed_verified':raise ValueError('completed_rich_inputs_required')
        return {'status':status,'run_identity':i['fingerprint'],'run_path':str(p),'recipe_sha256':digest,
            'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run',
            'engineering_ready':False,'forecast_evidence_status':'matched_rich_inputs_only','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False}
    except Exception as e:return {'status':'review_required','reason':str(e),'next_action':'preserve_evidence_and_escalate'}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','input-root','preparation-contract','lineage','technical','trad-root','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--test-crash-after',type=int);a=p.parse_args()
    r=operate(a.action,a.recipe,a.recipe_sha256,a.input_root,a.preparation_contract,a.lineage,a.technical,a.trad_root,a.runs_dir,a.test_crash_after);print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
