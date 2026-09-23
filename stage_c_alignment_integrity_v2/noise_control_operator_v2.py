"""Pinned matched nuisance-control inputs/source/environment; offline only."""
import argparse,hashlib,importlib.metadata,json,platform,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
SOURCES=('matched_view_models_v2.py','noise_control_features_v2.py','rich_family_models_v2.py','rich_family_operator_v2.py','noise_control_operator_v2.py','noise_control_runner_v2.py','noise_control_models_v2.py','rich_campaign_consumer_v2.py','rolling_registry_adapter_v2.py','matched_campaign_models_v2.py','fitted_consumer_v2.py','retained_signed_cost_models_v1.py','contracts.py','publication.py')
PREDECESSORS={'oanda_rolling_technical_features_v1.py':'074a7b4fc138ef25a1e99b503ea693e0c31bc2e51a2e8f3753aab7556a6fb657','oanda_rolling_technical_panel_v1.py':'2426d0cf85dc1f1026d9ab49b0ade8925f8ba3a23730da0440a6b4c9293fb24e'}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def dependency(root,exclude_timings=False):
    i=read(root/'RUN_IDENTITY.json');m=read(root/'COMPLETION_MANIFEST.json')
    if i!=m['run_identity']:raise ValueError('rich_family_dependency_completion_mismatch')
    rows=m['payloads']
    if len({x['path'] for x in rows})!=len(rows):raise ValueError('unique_dependency_members_required')
    for x in rows:
        n=x['path'];p=root/n
        if Path(n).name!=n or p.is_symlink() or sha(p)!=x['sha256']:raise ValueError('rich_family_dependency_bytes_changed')
    return {'identity':i,'payloads':{x['path']:x['sha256'] for x in rows if not (exclude_timings and x['path']=='fit_resources.json')}}
def recipe_for(rich,baseline,trad):
    a,b=dependency(rich),dependency(baseline,exclude_timings=True)
    timing=read(baseline/'fit_resources.json')
    if len(timing)!=14 or any(not 0<=x['elapsed_seconds']<=30 for x in timing):raise ValueError('baseline_fit_resource_bounds_required')
    for n,h in PREDECESSORS.items():
        if sha(trad/n)!=h:raise ValueError('canonical_rich_source_changed')
    universe=a['identity']['contract']['recipe']['universe']
    if len(universe)!=68 or len(a['payloads'])!=210 or len(b['payloads'])!=33:raise ValueError('required_rich_and_reissued_baseline_scope')
    if b['identity']['contract']['recipe']['input_identity']!=a['identity']['contract']['recipe']['technical_identity']:raise ValueError('rich_baseline_original_input_identity_mismatch')
    if sha(ROOT/'retained_signed_cost_models_v1.py')!='8cebabbab7c6dcb32271a8fd3cbc656e912ae6a26e19a9ff1d35ad547342665c':raise ValueError('retained_preprocessor_source_changed')
    sys.path.insert(0,str(ROOT))
    from noise_control_features_v2 import GROUPS,names_for
    raw=(rich/'feature_sets.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=a['payloads']['feature_sets.json']:raise ValueError('noise_consumed_schema_changed')
    legacy=json.loads(raw)['legacy26']
    return {'schema_version':'forex_noise_control_recipe.v1','sources':{n:sha(ROOT/n) for n in SOURCES},'predecessors':PREDECESSORS,'rich':a,'baseline':b,'universe':universe,
        'legacy_features':legacy,'feature_sets':{g:names_for(g,legacy) for g in GROUPS},'environment':{'python':platform.python_version(),**{n:importlib.metadata.version(n) for n in ('numpy','pandas','scipy','scikit-learn','joblib','threadpoolctl')}},
        'run_id':'matched-noise-controls','broker_access':False,'selection':'report_all_no_promotion'}
def preflight(recipe,digest,rich,baseline,trad):
    if recipe.is_symlink() or sha(recipe)!=digest:raise ValueError('rich_family_recipe_hash_mismatch')
    r=read(recipe)
    if set(r['sources'])!=set(SOURCES) or any(sha(ROOT/n)!=h for n,h in r['sources'].items()):raise ValueError('rich_family_source_drift_before_import')
    if r!=recipe_for(rich,baseline,trad):raise ValueError('rich_family_input_environment_drift')
    return r
def operate(action,recipe,digest,rich,baseline,trad,runs,crash_after=None):
    try:
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_rich_family_action')
        r=preflight(recipe,digest,rich,baseline,trad);sys.path.insert(0,str(trad));sys.path.insert(0,str(ROOT))
        from noise_control_runner_v2 import run,identity_for,required
        from publication import verify_completed_run
        for root,key in [(rich,'rich'),(baseline,'baseline')]:verify_completed_run(root,r[key]['identity'])
        i=identity_for(r);p=runs/r['run_id']
        if action in {'run','resume'}:run(rich,baseline,r,runs,resume=action=='resume',crash_after=crash_after)
        if (p/'COMPLETION_MANIFEST.json').exists():
            m=verify_completed_run(p,i)
            if {x['path'] for x in m['payloads']}!=set(required()):raise ValueError('rich_family_payload_inventory_mismatch')
            report=read(p/'run_report.json');resources=read(p/'fit_resources.json')
            if report['models_fitted']!=112 or report['models_reused']!=28 or report['new_score_groups']!=112 or report['selected_model'] is not None:raise ValueError('rich_family_acceptance_missing')
            if len(resources)!=56 or any(not 0<=x['elapsed_seconds']<=30 for x in resources):raise ValueError('rich_fit_resource_acceptance_missing')
            status='completed_verified'
        elif p.exists():
            if read(p/'RUN_IDENTITY.json')!=i:raise ValueError('rich_family_partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action=='verify' and status!='completed_verified':raise ValueError('completed_rich_family_required')
        return {'status':status,'run_identity':i['fingerprint'],'run_path':str(p),'recipe_sha256':digest,'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run',
            'engineering_ready':False,'forecast_evidence_status':'matched_inspected_development_only','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False}
    except Exception as e:return {'status':'review_required','reason':str(e),'next_action':'preserve_evidence_and_escalate'}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','rich','baseline','trad-root','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--test-crash-after',type=int);a=p.parse_args()
    r=operate(a.action,a.recipe,a.recipe_sha256,a.rich,a.baseline,a.trad_root,a.runs_dir,a.test_crash_after);print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
