"""Pinned original-fit readiness rebuild; no fit, account or service action."""
import argparse,hashlib,importlib.metadata,json,platform,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
from matched_remaining_operator_v2 import SOURCES as OLD_SOURCES,PREDECESSORS
SOURCES=tuple(sorted(set(OLD_SOURCES)|{'remaining_readiness_operator_v2.py','remaining_readiness_runner_v2.py','remaining_readiness_native_v2.py'}))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def dependency(root,count,exclude=()):
    i=read(root/'RUN_IDENTITY.json');m=read(root/'COMPLETION_MANIFEST.json');rows=m['payloads']
    if m['run_identity']!=i or len(rows)!=count or len({x['path'] for x in rows})!=count:raise ValueError('exact_remaining_dependency_required')
    for x in rows:
        n=x['path'];p=root/n
        if Path(n).name!=n or p.is_symlink() or p.stat().st_size!=x['bytes'] or sha(p)!=x['sha256']:raise ValueError('remaining_dependency_bytes_changed')
    return {'identity':i,'payloads':{x['path']:x['sha256'] for x in rows if x['path'] not in exclude}}
def recipe_for(paths,trad):
    if set(paths)!={'remaining','technical'}:raise ValueError('exact_remaining_readiness_paths_required')
    deps={k:dependency(Path(v),23 if k=='remaining' else 70,('fit_resources.json',) if k=='remaining' else ()) for k,v in paths.items()}
    original=deps['remaining']['identity']['contract']['recipe']
    if original['technical_identity']!=deps['technical']['identity']:raise ValueError('same_remaining_technical_lineage_required')
    resources=read(Path(paths['remaining'])/'fit_resources.json')
    if {x['minutes'] for x in resources}!={360,1080,1800,2160,2520} or any(not 0<=x['elapsed_seconds']<=30 for x in resources):raise ValueError('remaining_original_fit_bounds_required')
    for n,h in PREDECESSORS.items():
        if sha(trad/n)!=h:raise ValueError('remaining_native_predecessor_changed')
    return {'schema_version':'forex_remaining_readiness_recipe.v1','sources':{n:sha(ROOT/n) for n in SOURCES},'predecessors':PREDECESSORS,'dependencies':deps,'universe':original['universe'],'environment':{'python':platform.python_version(),**{n:importlib.metadata.version(n) for n in ('numpy','pandas','pyarrow','scipy','scikit-learn','joblib','threadpoolctl')}},'run_id':'joint-ready-remaining-native-preparation','models_fitted':0,'scope':'isolated_eight_fit_modeled_readiness_original_values_unissued_native_preparation','actual_historical_issuance':False,'broker_access':False}
def preflight(recipe,digest,paths,trad):
    if recipe.is_symlink() or sha(recipe)!=digest:raise ValueError('remaining_readiness_recipe_pin_mismatch')
    r=read(recipe)
    if set(r['sources'])!=set(SOURCES) or any(sha(ROOT/n)!=h for n,h in r['sources'].items()):raise ValueError('remaining_readiness_source_drift_before_import')
    if r!=recipe_for(paths,trad):raise ValueError('remaining_readiness_input_environment_drift')
    return r
def operate(action,recipe,digest,paths,trad,runs,crash_after=None):
    try:
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_remaining_readiness_action')
        r=preflight(recipe,digest,paths,trad)
        from remaining_readiness_runner_v2 import run,identity_for,required
        from publication import verify_completed_run
        i=identity_for(r);root=runs/r['run_id']
        if action in {'run','resume'}:run(paths,trad,r,runs,action=='resume',crash_after)
        if (root/'COMPLETION_MANIFEST.json').exists():
            m=verify_completed_run(root,i)
            if {x['path'] for x in m['payloads']}!=set(required()):raise ValueError('exact_remaining_readiness_payloads_required')
            report=read(root/'run_report.json');resources=read(root/'prediction_resources.json')
            if report['coverage_rows']!=1088 or report['models_fitted']!=0 or report['joint_unready_coverage_rows']!=136:raise ValueError('all_remaining_readiness_attempts_required')
            if len(resources['origins'])!=8 or any(not 0<=x['elapsed_seconds']<=30 for x in resources['origins']) or not 0<=resources['startup_seconds']<=120:raise ValueError('remaining_readiness_resource_bounds_required')
            status='completed_verified'
        elif root.exists():
            if read(root/'RUN_IDENTITY.json')!=i:raise ValueError('remaining_readiness_partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action=='verify' and status!='completed_verified':raise ValueError('completed_remaining_readiness_required')
        return {'status':status,'run_path':str(root),'run_identity':i['fingerprint'],'recipe_sha256':digest,'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run','engineering_ready':False,'forecast_evidence_status':'isolated_modeled_joint_readiness_development','policy_evidence_status':'unissued_native_preparation_only','demo_authorization_status':'not_granted','independent_review':False}
    except Exception as exc:return {'status':'review_required','reason':str(exc),'next_action':'preserve_evidence_and_escalate'}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','paths','trad-root','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--test-crash-after',type=int);a=p.parse_args();r=operate(a.action,a.recipe,a.recipe_sha256,read(a.paths),a.trad_root,a.runs_dir,a.test_crash_after);print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
