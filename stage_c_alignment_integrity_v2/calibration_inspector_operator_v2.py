"""Pinned lower-model operator for original-record calibration inspection."""
import argparse,hashlib,importlib.metadata,json,platform,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
SOURCES=('calibration_inspector_operator_v2.py','calibration_record_inspector_v2.py','calibration_inspector_runner_v2.py','residual_calibration_operator_v2.py','prequential_residual_calibration_v2.py','contracts.py','publication.py')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def recipe_for(paths):
    if set(paths)!={'calibration','joint','technical'}:raise ValueError('exact_inspector_dependencies_required')
    sys.path.insert(0,str(ROOT));from residual_calibration_operator_v2 import dependency
    deps={k:dependency(Path(v),{'calibration':116,'joint':117,'technical':70}[k]) for k,v in paths.items()};cal=deps['calibration']['identity']['contract']['recipe']
    if cal['schema_version']!='forex_residual_calibration_recipe.v1' or cal['dependencies']!={k:deps[k] for k in ('joint','technical')}:raise ValueError('same_original_calibration_lineage_required')
    return {'schema_version':'forex_calibration_inspector_recipe.v1','sources':{n:sha(ROOT/n) for n in SOURCES},'dependencies':deps,'environment':{'python':platform.python_version(),'numpy':importlib.metadata.version('numpy')},'universe':cal['universe'],'run_id':'calibration-original-record-inspection','any_fitting_allowed':False,'broker_access':False,'actual_issue_receipts_qualified':False}
def preflight(recipe,digest,paths):
    if recipe.is_symlink() or sha(recipe)!=digest:raise ValueError('inspector_recipe_pin_mismatch')
    r=read(recipe)
    if set(r['sources'])!=set(SOURCES) or any(sha(ROOT/n)!=h for n,h in r['sources'].items()):raise ValueError('inspector_source_drift_before_import')
    if r!=recipe_for(paths):raise ValueError('inspector_input_environment_drift')
    return r
def operate(action,recipe,digest,paths,runs,query=None,crash_after=None):
    try:
        if action not in {'status','run','resume','verify','inspect'}:raise ValueError('unsupported_inspector_action')
        r=preflight(recipe,digest,paths);sys.path.insert(0,str(ROOT))
        from calibration_inspector_runner_v2 import run,identity_for,required
        from publication import verify_completed_run
        i=identity_for(r);root=runs/r['run_id']
        if action in {'run','resume'}:run(paths,r,runs,action=='resume',crash_after)
        if (root/'COMPLETION_MANIFEST.json').exists():
            m=verify_completed_run(root,i)
            if {x['path'] for x in m['payloads']}!=set(required()):raise ValueError('exact_inspection_payloads_required')
            report=read(root/'run_report.json')
            if report['inspection_cases']!=1224 or report['instruments']!=68 or report['base_models_fitted']!=0 or report['calibrators_fitted']!=0:raise ValueError('all_prescribed_inspection_cases_required')
            status='completed_verified'
        elif root.exists():
            if read(root/'RUN_IDENTITY.json')!=i:raise ValueError('inspection_partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action in {'verify','inspect'} and status!='completed_verified':raise ValueError('completed_inspector_required')
        result={'status':status,'run_path':str(root),'run_identity':i['fingerprint'],'recipe_sha256':digest,'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run','engineering_ready':False,'independent_review':False}
        if action=='inspect':
            from calibration_record_inspector_v2 import OriginalRecordReader
            result['inspection']=OriginalRecordReader(paths,r).inspect(query)
        return result
    except Exception as exc:return {'status':'review_required','reason':str(exc),'next_action':'preserve_evidence_and_escalate'}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify','inspect'])
    for n in ('recipe','paths','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--query',type=Path);p.add_argument('--test-crash-after',type=int);a=p.parse_args()
    result=operate(a.action,a.recipe,a.recipe_sha256,read(a.paths),a.runs_dir,read(a.query) if a.query else None,a.test_crash_after);print(json.dumps(result,sort_keys=True));raise SystemExit(2 if result['status']=='review_required' else 0)
