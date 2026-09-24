"""Frozen portable successor of the retained warm-curve prototype; no fitting."""
import argparse,importlib.metadata,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import curve_capacity_operator_v2 as base
SOURCES=tuple(sorted(set(base.SOURCES)|{'warm_curve_operator_v2.py','warm_curve_runner_v2.py','warm_curve_predict_v2.py','scoped_warm_predict_v2.py'}))
sha=base.sha;read=base.read
def recipe_for(paths,trad):
    r=base.recipe_for(paths,trad)
    return {**r,'schema_version':'forex_warm_curve_recipe.v2','run_id':'whole-curve-warm-resumable',
      'environment':{**r['environment'],'psutil':importlib.metadata.version('psutil')},'sources':{n:sha(ROOT/n) for n in SOURCES},'configuration':{**r['configuration'],
      'max_rss_bytes':1024**3,'max_run_disk_bytes':2*1024**3,'resource_checks':'phase_boundaries_and_final_precommit_not_continuous_OS_quota','prewarm_reservation_seconds':120,'startup_hard_seconds':180,'total_hard_seconds':900,
      'resume':'reconstruct_legal_caches_chronologically_validate_prior_science_preserve_prior_timings',
      'original_prototype_identity':'b0c89c78e16c2eb90b7591d2b9104a7ee8bc93747b895863356040aa7876ead5'}}
def preflight(recipe,digest,paths,trad):
    if recipe.is_symlink() or sha(recipe)!=digest:raise ValueError('warm_recipe_pin_mismatch')
    r=read(recipe)
    if set(r['sources'])!=set(SOURCES) or any(sha(ROOT/n)!=h for n,h in r['sources'].items()):raise ValueError('warm_source_drift_before_import')
    if r!=recipe_for(paths,trad):raise ValueError('warm_input_environment_drift')
    return r
def operate(action,recipe,digest,paths,trad,runs,crash_after=None):
    try:
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_warm_action')
        r=preflight(recipe,digest,paths,trad);sys.path.insert(0,str(trad))
        from warm_curve_runner_v2 import run,identity_for,required,validate_completed
        from publication import verify_completed_run
        identity=identity_for(r);root=runs/r['run_id']
        if action in {'run','resume'}:run(paths,r,runs,resume=action=='resume',crash_after=crash_after)
        if (root/'COMPLETION_MANIFEST.json').exists():
            m=verify_completed_run(root,identity)
            if {x['path'] for x in m['payloads']}!=set(required()):raise ValueError('warm_payload_inventory_mismatch')
            validate_completed(root);status='completed_verified'
        elif root.exists():
            if read(root/'RUN_IDENTITY.json')!=identity:raise ValueError('warm_partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action=='verify' and status!='completed_verified':raise ValueError('completed_warm_run_required')
        return {'status':status,'run_identity':identity['fingerprint'],'run_path':str(root),'recipe_sha256':digest,
          'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run',
          'engineering_ready':False,'forecast_evidence_status':'retained_value_parity_not_new_forecasts',
          'policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False}
    except Exception as e:return {'status':'review_required','reason':str(e),'next_action':'preserve_evidence_and_escalate'}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','paths','trad-root','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--test-crash-after',type=int);a=p.parse_args()
    result=operate(a.action,a.recipe,a.recipe_sha256,read(a.paths),a.trad_root,a.runs_dir,a.test_crash_after)
    print(json.dumps(result,sort_keys=True));raise SystemExit(2 if result['status']=='review_required' else 0)
