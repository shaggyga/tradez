"""Pinned whole-curve characterization; timing target cannot promote readiness."""
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import joint_readiness_operator_v2 as original
SOURCES=tuple(sorted(set(original.SOURCES)|{'curve_capacity_operator_v2.py','curve_capacity_predict_v2.py','curve_capacity_runner_v2.py'}))
sha=original.sha;read=original.read
def recipe_for(paths,trad):
    r=original.recipe_for(paths,trad)
    return {**r,'schema_version':'forex_curve_capacity_recipe.v1','run_id':'whole-curve-capacity','sources':{n:sha(ROOT/n) for n in SOURCES},'configuration':{**r['configuration'],'whole_origin_hard_cap_seconds':30,'diagnostic_target_seconds':2,'comparison_order':'alternate_reference_first_by_origin_index','measure':'model_read_hash_load_transform_predict_lineage_serialize_durable_payload','excluded':'preflight_startup_prepared_feature_materialization_measured_separately; live_feeds_native_target_rebuild_host_contention_unqualified','fit_allowed':False,'promotion_allowed':False}}
def preflight(recipe,digest,paths,trad):
    if recipe.is_symlink() or sha(recipe)!=digest:raise ValueError('curve_recipe_pin_mismatch')
    r=read(recipe)
    if set(r['sources'])!=set(SOURCES) or any(sha(ROOT/n)!=h for n,h in r['sources'].items()):raise ValueError('curve_source_drift_before_import')
    if r!=recipe_for(paths,trad):raise ValueError('curve_input_environment_drift')
    return r
def operate(action,recipe,digest,paths,trad,runs,crash_after=None):
    try:
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_curve_action')
        r=preflight(recipe,digest,paths,trad);sys.path.insert(0,str(trad));sys.path.insert(0,str(ROOT))
        from curve_capacity_runner_v2 import run,identity_for,required,validate_resources
        from publication import verify_completed_run
        i=identity_for(r);root=runs/r['run_id']
        if action in {'run','resume'}:run(paths,r,runs,resume=action=='resume',crash_after=crash_after)
        if (root/'COMPLETION_MANIFEST.json').exists():
            m=verify_completed_run(root,i)
            if {x['path'] for x in m['payloads']}!=set(required()):raise ValueError('complete_curve_inventory_required')
            report=read(root/'run_report.json');resources=read(root/'curve_resources.json');validate_resources(resources)
            if report['forecast_values_per_engine']!=143904 or report['coverage_rows_per_engine']!=152320 or report['models_fitted']!=0 or report['engineering_ready'] is not False:raise ValueError('curve_acceptance_missing')
            status='completed_verified'
        elif root.exists():
            if read(root/'RUN_IDENTITY.json')!=i:raise ValueError('curve_partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action=='verify' and status!='completed_verified':raise ValueError('completed_curve_run_required')
        return {'status':status,'run_path':str(root),'run_identity':i['fingerprint'],'recipe_sha256':digest,'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run','engineering_ready':False,'forecast_evidence_status':'retained_value_parity_not_new_forecasts','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False}
    except Exception as exc:return {'status':'review_required','reason':str(exc),'next_action':'preserve_evidence_and_escalate'}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','paths','trad-root','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--test-crash-after',type=int);a=p.parse_args()
    r=operate(a.action,a.recipe,a.recipe_sha256,read(a.paths),a.trad_root,a.runs_dir,a.test_crash_after);print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
