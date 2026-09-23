"""Frozen read-only inspection and deterministic campaign report publication."""
import argparse,hashlib,importlib.metadata,json,platform,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
SOURCES=('campaign_inspector_operator_v2.py','campaign_inspector_v2.py','campaign_report_v2.py','campaign_launcher_v2.py','Run-Forex-Campaign.ps1','Run-Forex-Campaign.cmd','contracts.py','publication.py')
REQUIRED=('campaign_report.json','campaign_report.md','run_report.json')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return json.loads(p.read_text())

def recipe_for(paths):
    dependencies={}
    expected={'technical','matched','remaining'}|{'policy/'+method+'/'+scenario+'/'+engine
        for method in ('ridge','recovered_hgb') for scenario in ('candle_zero_slippage_financing','candle_one_bp_slippage_rollover') for engine in ('reference','optimized')}
    if set(paths)!=expected:raise ValueError('all_campaign_dependencies_required')
    for alias,value in paths.items():
        p=Path(value);i=read(p/'RUN_IDENTITY.json');m=read(p/'COMPLETION_MANIFEST.json')
        if m['run_identity']!=i:raise ValueError('campaign_dependency_completion_identity_mismatch')
        excluded=['fit_resources.json'] if alias in ('matched','remaining') else []
        payloads={}
        for row in m['payloads']:
            n=row['path']
            if Path(n).name!=n or n in payloads or sha(p/n)!=row['sha256']:raise ValueError('campaign_dependency_payload_invalid')
            if n not in excluded:payloads[n]=row['sha256']
        dependencies[alias]={'identity':i,'payloads':payloads,'excluded_timing_payloads':excluded}
    return {'schema_version':'forex_campaign_inspector_recipe.v1','sources':{n:sha(ROOT/n) for n in SOURCES},
        'dependencies':dependencies,'environment':{'python':platform.python_version(),'numpy':importlib.metadata.version('numpy')},
        'run_id':'matched-campaign-inspection','required_payloads':list(REQUIRED),'model_refit':False,'broker_access':False,
        'scope':'original_record_inspection_and_aggregate_development_report','outcomes_hidden_by_default':True}

def preflight(recipe,digest,paths):
    if recipe.is_symlink() or sha(recipe)!=digest:raise ValueError('inspector_recipe_hash_mismatch')
    r=read(recipe)
    if set(r['sources'])!=set(SOURCES):raise ValueError('inspector_exact_source_closure_required')
    for n,h in r['sources'].items():
        if sha(ROOT/n)!=h:raise ValueError('inspector_source_drift_before_import')
    if r!=recipe_for(paths):raise ValueError('inspector_dependency_environment_drift')
    return r

def identity_for(recipe):
    from publication import effective_run_identity
    return effective_run_identity(contract={'recipe':recipe},dependency_hashes={**recipe['sources'],
        **{'input:'+k:v['identity']['fingerprint'] for k,v in recipe['dependencies'].items()}})

def operate(action,recipe,digest,paths,runs,query=None,crash_after=None):
    try:
        if action not in {'status','run','resume','verify','inspect'}:raise ValueError('unsupported_inspector_action')
        r=preflight(recipe,digest,paths);sys.path.insert(0,str(ROOT))
        from campaign_inspector_v2 import CampaignReader
        from campaign_report_v2 import render
        from publication import RunPublisher,verify_completed_run
        reader=CampaignReader(paths,r['dependencies']);i=identity_for(r);root=runs/r['run_id']
        if action=='inspect':
            if not isinstance(query,dict) or query.get('kind') not in {'forecast','decision'}:raise ValueError('explicit_inspection_query_required')
            args={k:v for k,v in query.items() if k!='kind'}
            result=getattr(reader,query['kind'])(**args)
            return {'status':'completed_verified','inspection':result,'next_action':'review_original_record','read_only':True,'recipe_sha256':digest}
        if action in {'run','resume'} and not (root/'COMPLETION_MANIFEST.json').exists():
            import os
            p=RunPublisher(runs,r['run_id'],i);p.acquire(recover=action=='resume')
            try:
                report=reader.report()
                if len(report['original_scores'])!=56 or len(report['policies'])!=4 or report['selected_model'] is not None:
                    raise ValueError('original_report_coverage_mismatch')
                status={'status':'verified_original_campaign_report','input_dependencies':len(paths),'score_groups':56,'policy_groups':4,
                    'outcome_reveal':'aggregate_report_explicitly_reveals_development_outcomes; record_inspection_default_hidden',
                    'engineering_ready':False,'forecast_evidence_status':'bounded_development_only','policy_evidence_status':'declared_candle_scenarios_only',
                    'demo_authorization_status':'not_granted','independent_review':False,'next_item':'populated_rolling_feature_registry_reconciliation_v2'}
                encode=lambda x:(json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
                payloads=[]
                for index,(n,raw) in enumerate([('campaign_report.json',encode(report)),('campaign_report.md',render(report).encode()),('run_report.json',encode(status))],1):
                    payloads.append(p.write_or_validate_payload(n,raw))
                    if crash_after==index:os._exit(91)
                if crash_after==0:os._exit(91)
                p.complete(payloads,set(REQUIRED))
            except BaseException:
                if p._owner_token is not None:p.release()
                raise
        if (root/'COMPLETION_MANIFEST.json').exists():verify_completed_run(root,i);status='completed_verified'
        elif root.exists():
            if read(root/'RUN_IDENTITY.json')!=i:raise ValueError('inspector_partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action=='verify' and status!='completed_verified':raise ValueError('completed_inspector_report_required')
        return {'status':status,'run_path':str(root),'run_identity':i['fingerprint'],'recipe_sha256':digest,
            'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run',
            'engineering_ready':False,'demo_authorization_status':'not_granted','independent_review':False}
    except Exception as exc:return {'status':'review_required','reason':str(exc),'next_action':'preserve_evidence_and_escalate'}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify','inspect'])
    for n in ('recipe','paths','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--query',type=Path);p.add_argument('--test-crash-after',type=int,help=argparse.SUPPRESS)
    a=p.parse_args();r=operate(a.action,a.recipe,a.recipe_sha256,read(a.paths),a.runs_dir,read(a.query) if a.query else None,a.test_crash_after)
    print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
