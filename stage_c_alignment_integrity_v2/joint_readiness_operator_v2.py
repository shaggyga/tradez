"""Frozen original-artifact joint scheduling and fallback operator."""
import argparse,hashlib,importlib.metadata,json,platform,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import rich_family_operator_v2 as family
import rich_dependence_operator_v2 as dependence
SOURCES=tuple(sorted(set(family.SOURCES)|set(dependence.SOURCES)|{'joint_readiness_operator_v2.py','joint_readiness_schedule_v2.py','joint_readiness_predict_v2.py','joint_readiness_runner_v2.py'}))
PREDECESSORS=family.PREDECESSORS
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def recipe_for(paths,trad):
    dependencies=dependence.recipe_for(paths)['dependencies']
    for n,h in PREDECESSORS.items():
        if sha(trad/n)!=h:raise ValueError('joint_canonical_predecessor_drift')
    c=read(Path(paths['family'])/'experiment_contract.json')
    if len(c['decision_epochs'])!=20 or c['fit_latency_seconds']!=30 or c['prediction_latency_seconds']!=2:raise ValueError('original_joint_scope_required')
    return {'schema_version':'forex_joint_readiness_recipe.v1','run_id':'joint-readiness-projection','sources':{n:sha(ROOT/n) for n in SOURCES},'predecessors':PREDECESSORS,
        'dependencies':dependencies,'environment':{'python':platform.python_version(),**{n:importlib.metadata.version(n) for n in ('numpy','pandas','scipy','scikit-learn','joblib','threadpoolctl')}},
        'configuration':{'universe':dependencies['family']['identity']['contract']['recipe']['universe'],'fit_seconds':30,'prediction_batch_seconds':2,'worker_count':1,
            'selection_clock':'original_origin','native_maximum_conditioning_age_seconds':2,'fit_allowed':False,'broker_access':False,'confirmation':False}}
def preflight(recipe,digest,paths,trad):
    if recipe.is_symlink() or sha(recipe)!=digest:raise ValueError('joint_recipe_hash_mismatch')
    r=read(recipe)
    if set(r['sources'])!=set(SOURCES) or any(sha(ROOT/n)!=h for n,h in r['sources'].items()):raise ValueError('joint_source_drift_before_import')
    if r!=recipe_for(paths,trad):raise ValueError('joint_dependency_environment_drift')
    return r
def operate(action,recipe,digest,paths,trad,runs,crash_after=None):
    try:
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_joint_readiness_action')
        r=preflight(recipe,digest,paths,trad);sys.path.insert(0,str(trad));sys.path.insert(0,str(ROOT))
        from joint_readiness_runner_v2 import run,identity_for,required
        from publication import verify_completed_run
        from contracts import fingerprint
        identity=identity_for(r);root=runs/r['run_id']
        if action in {'run','resume'}:run(paths,r,runs,resume=action=='resume',crash_after=crash_after)
        if (root/'COMPLETION_MANIFEST.json').exists():
            m=verify_completed_run(root,identity)
            if {x['path'] for x in m['payloads']}!=set(required()):raise ValueError('joint_payload_inventory_mismatch')
            report=read(root/'run_report.json');timings=read(root/'prediction_resources.json');schedule=read(root/'schedule.json')
            expected={fingerprint(x) for x in schedule['prediction_tasks']}
            if len(timings)!=1120 or {x['batch_id'] for x in timings}!=expected:raise ValueError('joint_prediction_timing_inventory_mismatch')
            for t in timings:
                if t['status']=='measured_retained_weight_prediction':
                    if type(t['elapsed_seconds']) not in (int,float) or not 0<=t['elapsed_seconds']<=2 or t['selected_fit_id'] is None:raise ValueError('joint_prediction_resource_bound_missing')
                elif t['status']!='not_run_no_ready_model' or t['elapsed_seconds'] is not None or t['selected_fit_id'] is not None:raise ValueError('joint_skipped_prediction_misrepresented')
            if report['counts']['coverage_rows']!=152320 or report['score_groups']!=112 or report['models_fitted']!=0 or report['selected_model'] is not None:raise ValueError('joint_acceptance_missing')
            status='completed_verified'
        elif root.exists():
            if read(root/'RUN_IDENTITY.json')!=identity:raise ValueError('joint_partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action=='verify' and status!='completed_verified':raise ValueError('completed_joint_projection_required')
        return {'status':status,'run_identity':identity['fingerprint'],'run_path':str(root),'recipe_sha256':digest,'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run',
            'engineering_ready':False,'forecast_evidence_status':'scheduled_inspected_development_projection','policy_evidence_status':'not_evaluated_freshness_refusals_explicit','demo_authorization_status':'not_granted','independent_review':False}
    except Exception as e:return {'status':'review_required','reason':str(e),'next_action':'preserve_evidence_and_escalate'}

def inspect_forecast(recipe,digest,paths,trad,runs,*,group,horizon,procedure,method,instrument,origin,asof):
    result=operate('verify',recipe,digest,paths,trad,runs)
    if result['status']!='completed_verified':return result
    from joint_readiness_schedule_v2 import GROUPS,HORIZONS,PROCEDURES,METHODS,visible_forecast
    from joint_readiness_runner_v2 import chunk
    if group not in GROUPS or horizon not in HORIZONS or procedure not in PROCEDURES or method not in METHODS or instrument not in read(recipe)['configuration']['universe']:
        return {'status':'review_required','reason':'declared_joint_inspection_scope_required'}
    try:
        root=Path(result['run_path']);m=read(root/'COMPLETION_MANIFEST.json');expected={x['path']:x['sha256'] for x in m['payloads']}
        def consumed(name):
            raw=(root/name).read_bytes()
            if len(raw)>64*1024*1024 or hashlib.sha256(raw).hexdigest()!=expected[name]:raise ValueError('joint_inspection_consumed_bytes_changed')
            return json.loads(raw)
        cov=[x for x in consumed('coverage_'+chunk(group,horizon,procedure)) if x['instrument']==instrument and x['decision_epoch']==origin and x['method']==method]
        if len(cov)!=1:raise ValueError('unique_declared_joint_coverage_required')
        forecasts=[x for x in consumed('forecasts_'+chunk(group,horizon,procedure)) if x['record_id']==cov[0]['record_id'] and x['method']==method]
        if len(forecasts)!=(1 if cov[0]['reason']=='eligible' else 0):raise ValueError('joint_coverage_forecast_mismatch')
        if not forecasts:return {'status':'unavailable','coverage':cov[0],'forecast':None,'outcomes_included':False}
        visible=visible_forecast(forecasts[0],asof)
        return {'status':visible['status'],'coverage':cov[0],'forecast':visible['forecast'],
            'native_freshness':visible_forecast(forecasts[0],asof,maximum_conditioning_age_seconds=2)['status'],'source_reference':forecasts[0]['source_reference'] if visible['forecast'] else None,'outcomes_included':False}
    except Exception as e:return {'status':'review_required','reason':str(e),'next_action':'preserve_evidence_and_escalate'}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify','inspect'])
    for n in ('recipe','paths','trad-root','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--test-crash-after',type=int)
    for n in ('group','procedure','method','instrument'):p.add_argument('--'+n)
    for n in ('horizon','origin','asof'):p.add_argument('--'+n,type=int)
    a=p.parse_args();paths=read(a.paths)
    r=inspect_forecast(a.recipe,a.recipe_sha256,paths,a.trad_root,a.runs_dir,group=a.group,horizon=a.horizon,procedure=a.procedure,method=a.method,instrument=a.instrument,origin=a.origin,asof=a.asof) if a.action=='inspect' else operate(a.action,a.recipe,a.recipe_sha256,paths,a.trad_root,a.runs_dir,a.test_crash_after)
    print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
