"""Frozen causal feature/target preparation on authenticated compact raw slices."""
import argparse,hashlib,importlib.metadata,json,platform,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
SOURCES=('causal_technical_operator_v2.py','causal_technical_runner_v2.py','causal_technical_adapter_v2.py','retained_direction_features_v1.py','retained_endpoint_targets_v1.py','contracts.py','publication.py')
PREDECESSOR_SHA='c0bbdcd265089856532345eb490571d90228b7d967fa0409ec546fe0f239f046'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def validate_inputs(root):
    m=json.loads((root/'SLICES_MANIFEST.json').read_text());rows=m['members']
    if len(rows)!=68 or len({r['instrument'] for r in rows})!=68:raise ValueError('all68_slice_inventory_required')
    for r in rows:
        if r['path']!=r['instrument']+'.parquet' or Path(r['path']).name!=r['path']:raise ValueError('slice_path_invalid')
        p=root/r['path']
        if p.is_symlink() or p.stat().st_size!=r['bytes'] or sha(p)!=r['sha256']:raise ValueError('slice_input_drift')
    return m

def recipe_for(root):
    manifest=validate_inputs(root)
    if sha(ROOT/'retained_direction_features_v1.py')!=PREDECESSOR_SHA:raise ValueError('retained_feature_source_changed')
    if sha(ROOT/'retained_endpoint_targets_v1.py')!='79ea5bcb4206d9ae2cf8a9d79fe8efb21a3d5a74bcad553fe5f56126ff3ae67d':raise ValueError('retained_endpoint_source_changed')
    return {'schema_version':'forex_causal_technical_operator.v2','sources':{n:sha(ROOT/n) for n in SOURCES},'input_manifest_sha256':sha(root/'SLICES_MANIFEST.json'),
        'environment':{'python':platform.python_version(),**{n:importlib.metadata.version(n) for n in ('numpy','pandas','pyarrow')}},
        'universe':sorted(r['instrument'] for r in manifest['members']),'run_id':'causal-technical-inputs','scope':'all68_retrospective_development_feature_target_preparation','broker_access':False,'fit_allowed':False}

def preflight(recipe,digest,root):
    if recipe.is_symlink() or sha(recipe)!=digest:raise ValueError('frozen_recipe_hash_mismatch')
    r=json.loads(recipe.read_text())
    if r!=recipe_for(root):raise ValueError('source_input_environment_drift_before_import')
    return r

def operate(action,recipe,digest,root,runs):
    try:
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_action')
        r=preflight(recipe,digest,root);sys.path.insert(0,str(ROOT))
        from causal_technical_runner_v2 import run,identity_for
        from publication import verify_completed_run
        i=identity_for(r);p=runs/r['run_id']
        if action in {'run','resume'}:run(root,r,runs_dir=runs,resume=True)
        if (p/'COMPLETION_MANIFEST.json').exists():
            verify_completed_run(p,i);report=json.loads((p/'run_report.json').read_text())
            if report['instruments']!=68 or report['observation_rows']!=5168 or report['feature_count']!=26 or report['models_fitted']!=0:raise ValueError('technical_acceptance_missing')
            status='completed_verified'
        elif p.exists():
            if json.loads((p/'RUN_IDENTITY.json').read_text())!=i:raise ValueError('partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action=='verify' and status!='completed_verified':raise ValueError('completed_run_required')
        return {'status':status,'run_identity':i['fingerprint'],'run_path':str(p),'recipe_sha256':digest,'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run',
            'engineering_ready':False,'forecast_evidence_status':'input_preparation_only','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False}
    except Exception as exc:return {'status':'review_required','reason':str(exc),'next_action':'preserve_evidence_and_escalate'}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','input-root','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);a=p.parse_args();r=operate(a.action,a.recipe,a.recipe_sha256,a.input_root,a.runs_dir);print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
