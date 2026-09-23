"""Original forecast/label capsule preflight; no model loading or account access."""
import argparse,hashlib,importlib.metadata,json,platform,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
SOURCES=('residual_calibration_operator_v2.py','residual_calibration_runner_v2.py','prequential_residual_calibration_v2.py','contracts.py','publication.py')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def dependency(root,count):
    identity=read(root/'RUN_IDENTITY.json');m=read(root/'COMPLETION_MANIFEST.json');rows=m['payloads']
    if m['run_identity']!=identity or len(rows)!=count or len({x['path'] for x in rows})!=count:raise ValueError('original_calibration_dependency_completion_required')
    for row in rows:
        n=row['path'];p=root/n
        if Path(n).name!=n or p.is_symlink() or p.stat().st_size!=row['bytes'] or sha(p)!=row['sha256']:raise ValueError('calibration_dependency_drift')
    return {'identity':identity,'payloads':{x['path']:x['sha256'] for x in rows}}
def recipe_for(paths):
    if set(paths)!={'joint','technical'}:raise ValueError('exact_calibration_capsule_dependencies_required')
    deps={k:dependency(Path(v),117 if k=='joint' else 70) for k,v in paths.items()};jr=deps['joint']['identity']['contract']['recipe']
    if jr['schema_version']!='forex_joint_readiness_recipe.v1':raise ValueError('original_joint_tape_required')
    rich=jr['dependencies']['rich']['identity']['contract']['recipe']
    if rich['technical_identity']!=deps['technical']['identity']:raise ValueError('same_original_calibration_outcome_population_required')
    timing=read(Path(paths['joint'])/'prediction_resources.json')
    if len(timing)!=1120 or any(x['elapsed_seconds'] is not None and not 0<=x['elapsed_seconds']<=2 for x in timing):raise ValueError('original_joint_resource_bounds_required')
    return {'schema_version':'forex_residual_calibration_recipe.v1','sources':{n:sha(ROOT/n) for n in SOURCES},'dependencies':deps,'environment':{'python':platform.python_version(),'numpy':importlib.metadata.version('numpy')},'universe':jr['configuration']['universe'],'run_id':'modeled-clock-residual-calibration','input_tier':'preserved_original_forecast_and_label_capsule','base_model_fit_allowed':False,'broker_access':False,'actual_issue_receipts_qualified':False,'confirmation':False}
def preflight(recipe,digest,paths):
    if recipe.is_symlink() or sha(recipe)!=digest:raise ValueError('calibration_recipe_pin_mismatch')
    r=read(recipe)
    if set(r['sources'])!=set(SOURCES) or any(sha(ROOT/n)!=h for n,h in r['sources'].items()):raise ValueError('calibration_source_drift_before_import')
    if r!=recipe_for(paths):raise ValueError('calibration_input_environment_drift')
    return r
def operate(action,recipe,digest,paths,runs,crash_after=None):
    try:
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_calibration_action')
        r=preflight(recipe,digest,paths);sys.path.insert(0,str(ROOT))
        from residual_calibration_runner_v2 import run,identity_for,required
        from publication import verify_completed_run
        i=identity_for(r);root=runs/r['run_id']
        if action in {'run','resume'}:run(paths,r,runs,resume=action=='resume',crash_after=crash_after)
        if (root/'COMPLETION_MANIFEST.json').exists():
            m=verify_completed_run(root,i)
            if {x['path'] for x in m['payloads']}!=set(required()):raise ValueError('calibration_complete_inventory_required')
            report=read(root/'run_report.json')
            if report['base_forecast_rows']!=143904 or report['coverage_rows']!=304640 or report['base_models_fitted']!=0 or report['score_groups']!=224:raise ValueError('calibration_all_declared_attempts_required')
            status='completed_verified'
        elif root.exists():
            if read(root/'RUN_IDENTITY.json')!=i:raise ValueError('calibration_partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action=='verify' and status!='completed_verified':raise ValueError('completed_calibration_required')
        return {'status':status,'run_path':str(root),'run_identity':i['fingerprint'],'recipe_sha256':digest,'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run','engineering_ready':False,'forecast_evidence_status':'modeled_clock_prequential_development_diagnostic','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False}
    except Exception as exc:return {'status':'review_required','reason':str(exc),'next_action':'preserve_evidence_and_escalate'}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','paths','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--test-crash-after',type=int);a=p.parse_args()
    r=operate(a.action,a.recipe,a.recipe_sha256,read(a.paths),a.runs_dir,a.test_crash_after);print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
