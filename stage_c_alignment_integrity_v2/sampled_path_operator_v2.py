"""Frozen offline sampled-path reconciliation; standard-library preflight."""
import argparse,hashlib,importlib.metadata,json,platform,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
SOURCES=('sampled_path_operator_v2.py','sampled_path_runner_v2.py','sampled_path_adapter_v2.py','retained_rolling_path_labels_v1.py','causal_technical_operator_v2.py','causal_technical_adapter_v2.py','retained_direction_features_v1.py','retained_endpoint_targets_v1.py','contracts.py','publication.py')
CANONICAL_SHA='77840b13e383c3fc8d3f91c76de655ceeebc410db906b62d52c40a7844ed277d'
INPUT_SHA='e4efa61f4c8a9b4d70f4b20f5f96e1bd0a4ef8ed86049924b21e78b98700c774'

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def technical_dependency(root):
    identity=read(root/'RUN_IDENTITY.json');m=read(root/'COMPLETION_MANIFEST.json');rows=m['payloads']
    if identity!=m['run_identity'] or len(rows)!=70 or len({x['path'] for x in rows})!=70:raise ValueError('original_technical_completion_required')
    for x in rows:
        n=x['path'];p=root/n
        if Path(n).name!=n or p.is_symlink() or sha(p)!=x['sha256']:raise ValueError('original_technical_payload_drift')
    return {'identity':identity,'payloads':{x['path']:x['sha256'] for x in rows}}

def recipe_for(inputs,technical):
    sys.path.insert(0,str(ROOT));from causal_technical_operator_v2 import validate_inputs
    m=validate_inputs(inputs)
    if sha(inputs/'SLICES_MANIFEST.json')!=INPUT_SHA:raise ValueError('original_technical_slices_required')
    if sha(ROOT/'retained_rolling_path_labels_v1.py')!=CANONICAL_SHA:raise ValueError('unchanged_canonical_path_labels_required')
    d=technical_dependency(technical)
    if d['identity']['contract']['recipe']['input_manifest_sha256']!=INPUT_SHA:raise ValueError('same_original_technical_population_required')
    return {'schema_version':'forex_sampled_path_recipe.v1','sources':{n:sha(ROOT/n) for n in SOURCES},'canonical_original_name':'trad/oanda_rolling_technical_labels_v1.py','canonical_original_sha256':CANONICAL_SHA,'input_manifest_sha256':INPUT_SHA,'technical':d,'universe':sorted(x['instrument'] for x in m['members']),'environment':{'python':platform.python_version(),**{n:importlib.metadata.version(n) for n in ('numpy','pandas','pyarrow')}},'run_id':'all68-sampled-path-reconciliation','fit_allowed':False,'broker_access':False}

def preflight(recipe,digest,inputs,technical):
    if recipe.is_symlink() or sha(recipe)!=digest:raise ValueError('sampled_path_recipe_pin_mismatch')
    r=read(recipe)
    if set(r['sources'])!=set(SOURCES) or any(sha(ROOT/n)!=h for n,h in r['sources'].items()):raise ValueError('sampled_path_source_drift_before_import')
    if r!=recipe_for(inputs,technical):raise ValueError('sampled_path_input_environment_drift')
    return r

def operate(action,recipe,digest,inputs,technical,runs,crash_after=None):
    try:
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_sampled_path_action')
        r=preflight(recipe,digest,inputs,technical);sys.path.insert(0,str(ROOT))
        from sampled_path_runner_v2 import identity_for,required,run
        from publication import verify_completed_run
        verify_completed_run(technical,r['technical']['identity']);identity=identity_for(r);root=runs/r['run_id']
        if action in {'run','resume'}:run(inputs,technical,r,runs,resume=action=='resume',crash_after=crash_after)
        if (root/'COMPLETION_MANIFEST.json').exists():
            m=verify_completed_run(root,identity)
            if {x['path'] for x in m['payloads']}!=set(required(r)):raise ValueError('sampled_path_inventory_mismatch')
            report=read(root/'run_report.json')
            if report['rows']!=36176 or report['instruments']!=68 or report['models_fitted']!=0:raise ValueError('sampled_path_all68_acceptance_missing')
            status='completed_verified'
        elif root.exists():
            if read(root/'RUN_IDENTITY.json')!=identity:raise ValueError('sampled_path_partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action=='verify' and status!='completed_verified':raise ValueError('completed_sampled_path_required')
        return {'status':status,'run_path':str(root),'run_identity':identity['fingerprint'],'recipe_sha256':digest,'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run','engineering_ready':False,'forecast_evidence_status':'retrospective_target_reconciliation_only','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False}
    except Exception as exc:return {'status':'review_required','reason':str(exc),'next_action':'preserve_evidence_and_escalate'}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','input-root','technical','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--test-crash-after',type=int);a=p.parse_args()
    r=operate(a.action,a.recipe,a.recipe_sha256,a.input_root,a.technical,a.runs_dir,a.test_crash_after);print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
