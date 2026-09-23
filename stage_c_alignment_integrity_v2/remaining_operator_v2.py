"""Frozen offline historical operator: status/run/resume/verify; never self-approve."""
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import sys
ROOT=Path(__file__).resolve().parent
SOURCES=('remaining_operator_v2.py','remaining_runner_v2.py','remaining_horizon_v2.py','historical_inputs_v2.py','fitted_consumer_v2.py','publication.py','contracts.py')
INPUT_NAMES=('PREFLIGHT_CONTRACT.json','PREFLIGHT_REPORT.json','historical_observations.jsonl','historical_outcomes.jsonl','reused_models.json')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def recipe_for(input_root,contract_path):
    """Engineering publication helper only; no CLI action regenerates approvals."""
    return {'schema_version':'forex_remaining_operator.v1','recipe':'remaining_gross_midpoint_development_slice',
      'sources':{n:sha(ROOT/n) for n in SOURCES},'inputs':{n:sha(input_root/n) for n in INPUT_NAMES},
      'contract_sha256':sha(contract_path),'environment':{'python':platform.python_version(),'numpy':importlib.metadata.version('numpy')},
      'run_id':'remaining-development','broker_access':False,'engineering_ready':False,'demo_authorization_status':'not_granted'}

def operate(action,recipe_path,recipe_sha256,input_root,contract_path,runs_dir):
    try:
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_action')
        if recipe_path.is_symlink() or sha(recipe_path)!=recipe_sha256:raise ValueError('frozen_recipe_hash_mismatch')
        recipe=json.loads(recipe_path.read_text(encoding='utf-8'))
        if recipe!=recipe_for(input_root,contract_path):raise ValueError('source_input_config_or_environment_drift')
        sys.path.insert(0,str(ROOT))
        from remaining_runner_v2 import run,identity_for
        from publication import verify_completed_run
        c=json.loads(contract_path.read_text(encoding='utf-8'));identity=identity_for(input_root,c);root=runs_dir/recipe['run_id']
        if action in {'run','resume'}:run(input_root,c,run_id=recipe['run_id'],runs_dir=runs_dir,resume=True)
        if (root/'COMPLETION_MANIFEST.json').exists():
            verify_completed_run(root,identity);r=json.loads((root/'run_report.json').read_text())
            if r['instruments']!=68 or r['coverage_rows']!=1088 or r['policy_frames']!=0:
                raise ValueError('historical_acceptance_missing')
            status='completed_verified'
        elif root.exists():
            if json.loads((root/'RUN_IDENTITY.json').read_text())!=identity:raise ValueError('partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action=='verify' and status!='completed_verified':raise ValueError('completed_run_required')
        return {'status':status,'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run',
          'run_path':str(root),'recipe_sha256':recipe_sha256,'run_identity':identity['fingerprint'],'engineering_ready':False,
          'forecast_evidence_status':'retrospective_development_only','policy_evidence_status':'blocked','demo_authorization_status':'not_granted'}
    except Exception as exc:return {'status':'review_required','reason':str(exc),'next_action':'preserve_evidence_and_escalate'}

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['status','run','resume','verify'])
    for name in ('recipe','input-root','contract','runs-dir'):p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);a=p.parse_args()
    r=operate(a.action,a.recipe,a.recipe_sha256,a.input_root,a.contract,a.runs_dir);print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
