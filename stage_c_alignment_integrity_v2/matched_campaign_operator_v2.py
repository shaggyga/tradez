"""Frozen matched model campaign; preflight before numerical imports or fitting."""
import argparse,hashlib,importlib.metadata,json,platform,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
SOURCES=('matched_campaign_operator_v2.py','matched_campaign_runner_v2.py','matched_campaign_models_v2.py','retained_signed_cost_models_v1.py','fitted_consumer_v2.py','contracts.py','publication.py')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def recipe_for(root):
    identity=json.loads((root/'RUN_IDENTITY.json').read_text());m=json.loads((root/'COMPLETION_MANIFEST.json').read_text());universe=identity['contract']['recipe']['universe']
    expected=sorted(['pair_'+p+'.json' for p in universe]+['run_report.json','input_contract.json'])
    if len(universe)!=68 or m['required_payloads']!=expected or m['run_identity']!=identity:raise ValueError('prepared_input_completion_contract_mismatch')
    if sha(ROOT/'retained_signed_cost_models_v1.py')!='8cebabbab7c6dcb32271a8fd3cbc656e912ae6a26e19a9ff1d35ad547342665c':raise ValueError('recovered_model_source_drift')
    return {'schema_version':'forex_matched_campaign_operator.v1','sources':{n:sha(ROOT/n) for n in SOURCES},'inputs':{n:sha(root/n) for n in expected},'input_identity':identity,'universe':universe,
        'environment':{'python':platform.python_version(),**{n:importlib.metadata.version(n) for n in ('numpy','pandas','scipy','scikit-learn','joblib','threadpoolctl')}},
        'run_id':'matched-development-campaign','broker_access':False,'scope':'predeclared_matched_development_endpoint_models_no_promotion'}
def preflight(recipe,digest,root):
    if recipe.is_symlink() or sha(recipe)!=digest:raise ValueError('frozen_recipe_hash_mismatch')
    r=json.loads(recipe.read_text())
    if r!=recipe_for(root):raise ValueError('source_input_environment_drift_before_import')
    return r

def operate(action,recipe,digest,root,runs):
    try:
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_action')
        r=preflight(recipe,digest,root);sys.path.insert(0,str(ROOT))
        from matched_campaign_runner_v2 import run,identity_for
        from publication import verify_completed_run
        i=identity_for(r);p=runs/r['run_id']
        if action in {'run','resume'}:run(root,r,runs_dir=runs,resume=True)
        if (p/'COMPLETION_MANIFEST.json').exists():
            verify_completed_run(p,i);report=json.loads((p/'run_report.json').read_text());resources=json.loads((p/'fit_resources.json').read_text())
            if report['models_fitted']!=28 or report['score_groups']!=56 or report['coverage_rows']!=76160 or report['selected_model'] is not None:raise ValueError('campaign_acceptance_missing')
            if len({x['fit_id'] for x in resources})!=14 or any(not 0<=x['elapsed_seconds']<=30 for x in resources):raise ValueError('fit_resource_acceptance_missing')
            status='completed_verified'
        elif p.exists():
            if json.loads((p/'RUN_IDENTITY.json').read_text())!=i:raise ValueError('partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action=='verify' and status!='completed_verified':raise ValueError('completed_run_required')
        return {'status':status,'run_identity':i['fingerprint'],'run_path':str(p),'recipe_sha256':digest,'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run',
            'engineering_ready':False,'forecast_evidence_status':'matched_development_only','policy_evidence_status':'not_evaluated_for_this_campaign','demo_authorization_status':'not_granted','independent_review':False}
    except Exception as exc:return {'status':'review_required','reason':str(exc),'next_action':'preserve_evidence_and_escalate'}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','input-root','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);a=p.parse_args();r=operate(a.action,a.recipe,a.recipe_sha256,a.input_root,a.runs_dir);print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
