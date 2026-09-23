"""Frozen original-record diagnostics; no estimator loading or fitting."""
import argparse,hashlib,importlib.metadata,json,platform,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
SOURCES=('rich_dependence_operator_v2.py','rich_dependence_report_v2.py','paired_blocks_v2.py','campaign_inspector_v2.py','contracts.py','publication.py')
REQUIRED=('paired_origin_panels.json','paired_comparison_scope.json','block_sensitivity.json','attempt_ledger.json','run_report.json')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def recipe_for(paths):
    if set(paths)!={'family','baseline','rich'}:raise ValueError('exact_paired_report_dependencies_required')
    dependencies={}
    for alias,value in paths.items():
        root=Path(value);i=read(root/'RUN_IDENTITY.json');m=read(root/'COMPLETION_MANIFEST.json')
        if m['run_identity']!=i:raise ValueError('paired_report_dependency_completion_mismatch')
        excluded=['fit_resources.json'] if alias in ('family','baseline') else [];payloads={};seen=set()
        for row in m['payloads']:
            n=row['path']
            if n in seen or Path(n).name!=n or (root/n).is_symlink() or sha(root/n)!=row['sha256']:raise ValueError('paired_report_dependency_payload_changed')
            seen.add(n)
            if n not in excluded:payloads[n]=row['sha256']
        dependencies[alias]={'identity':i,'payloads':payloads,'excluded_timing_payloads':excluded}
        if excluded:
            timing=read(root/'fit_resources.json');inventory=read(root/'model_inventory.json');expected_count=42 if alias=='family' else 14
            if len(timing)!=expected_count or {x['fit_id'] for x in timing}!={x['fit_id'] for x in inventory} or any(not 0<=x['elapsed_seconds']<=30 for x in timing):raise ValueError('paired_dependency_fit_resource_bounds_required')
    family=dependencies['family']['identity']['contract']['recipe']
    if family['baseline']['identity']!=dependencies['baseline']['identity'] or family['rich']['identity']!=dependencies['rich']['identity']:raise ValueError('paired_report_original_dependency_graph_required')
    c=read(Path(paths['family'])/'experiment_contract.json');universe=family['universe']
    if len(universe)!=68 or len(c['groups'])!=3 or len(c['horizon_minutes'])!=7 or len(c['methods'])!=2:raise ValueError('declared_rich_family_scope_required')
    return {'schema_version':'forex_paired_development_report_recipe.v1','sources':{n:sha(ROOT/n) for n in SOURCES},'dependencies':dependencies,
        'environment':{'python':platform.python_version(),'numpy':importlib.metadata.version('numpy')},'run_id':'rich-family-paired-diagnostics','required_payloads':list(REQUIRED),
        'configuration':{'universe':universe,'block_lengths':[4,8,20],'replicates':2000,'seed':20260922,'confirmation':False,'model_fit_allowed':False,'broker_access':False}}
def preflight(recipe,digest,paths):
    if recipe.is_symlink() or sha(recipe)!=digest:raise ValueError('paired_report_recipe_hash_mismatch')
    r=read(recipe)
    if set(r['sources'])!=set(SOURCES) or any(sha(ROOT/n)!=h for n,h in r['sources'].items()):raise ValueError('paired_report_source_drift_before_import')
    if r!=recipe_for(paths):raise ValueError('paired_report_dependency_environment_drift')
    return r
def identity_for(r):
    from publication import effective_run_identity
    return effective_run_identity(contract={'recipe':r},dependency_hashes={**r['sources'],**{k:v['identity']['fingerprint'] for k,v in r['dependencies'].items()}})
def operate(action,recipe,digest,paths,runs,crash_after=None):
    try:
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_paired_report_action')
        r=preflight(recipe,digest,paths);sys.path.insert(0,str(ROOT))
        from campaign_inspector_v2 import CampaignReader
        from rich_dependence_report_v2 import build
        from publication import RunPublisher,verify_completed_run
        i=identity_for(r);root=runs/r['run_id']
        if action in {'run','resume'} and not (root/'COMPLETION_MANIFEST.json').exists():
            import os
            p=RunPublisher(runs,r['run_id'],i);p.acquire(recover=action=='resume')
            try:
                outputs=build(CampaignReader(paths,r['dependencies']),r['configuration']);payloads=[]
                for index,name in enumerate(REQUIRED,1):
                    raw=(json.dumps(outputs[name],sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
                    payloads.append(p.write_or_validate_payload(name,raw))
                    if crash_after==index:os._exit(91)
                if crash_after==0:os._exit(91)
                p.complete(payloads,set(REQUIRED))
            except BaseException:
                if p._owner_token is not None:p.release()
                raise
        if (root/'COMPLETION_MANIFEST.json').exists():
            manifest=verify_completed_run(root,i)
            if {x['path'] for x in manifest['payloads']}!=set(REQUIRED):raise ValueError('paired_report_payload_inventory_mismatch')
            report=read(root/'run_report.json')
            if report['paired_comparisons']!=336 or report['models_fitted']!=0 or report['selected_model'] is not None or report['effective_sample_size'] is not None:raise ValueError('paired_report_acceptance_missing')
            status='completed_verified'
        elif root.exists():
            if read(root/'RUN_IDENTITY.json')!=i:raise ValueError('paired_report_partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action=='verify' and status!='completed_verified':raise ValueError('completed_paired_report_required')
        return {'status':status,'run_identity':i['fingerprint'],'run_path':str(root),'recipe_sha256':digest,'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run',
            'engineering_ready':False,'forecast_evidence_status':'paired_dependent_inspected_development_only','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False}
    except Exception as e:return {'status':'review_required','reason':str(e),'next_action':'preserve_evidence_and_escalate'}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','paths','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--test-crash-after',type=int);a=p.parse_args();r=operate(a.action,a.recipe,a.recipe_sha256,read(a.paths),a.runs_dir,a.test_crash_after);print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
