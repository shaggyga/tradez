"""Frozen matched remaining/native preparation, no account or policy execution."""
import argparse,hashlib,importlib.metadata,json,platform,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
SOURCES=('matched_remaining_operator_v2.py','matched_remaining_runner_v2.py','matched_remaining_native_v2.py',
 'matched_campaign_models_v2.py','retained_signed_cost_models_v1.py','fitted_consumer_v2.py',
 'causal_technical_adapter_v2.py','retained_direction_features_v1.py','retained_endpoint_targets_v1.py',
 'native_policy_input_v2.py','reference_accounting_adapter_v2.py','contracts.py','publication.py')
PREDECESSORS={'oanda_forecast_curve_contract_v1.py':'c03549586f922f6031790fce68a5afb5b98e4eda6157f20c4720b1adb05d0746','oanda_curve_management_adapter_v1.py':'8fa6cccf1e2525d0186ab7abe0c68fccc4800db7b785a8b9f79f28c320ad1241'}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def dependency(root,exclude=()):
    identity=json.loads((root/'RUN_IDENTITY.json').read_text());m=json.loads((root/'COMPLETION_MANIFEST.json').read_text())
    if m['run_identity']!=identity:raise ValueError('input_completion_identity_mismatch')
    names=m['required_payloads']
    if any(Path(n).name!=n or n.startswith('.') for n in names):raise ValueError('unsafe_dependency_payload')
    return identity,{n:sha(root/n) for n in names if n not in exclude}
def recipe_for(slices,technical,matched,trad):
    tm,th=dependency(technical);mm,mh=dependency(matched,('fit_resources.json',))
    s=json.loads((slices/'SLICES_MANIFEST.json').read_text());universe=tm['contract']['recipe']['universe']
    if len(s['members'])!=68 or sorted(r['instrument'] for r in s['members'])!=universe:raise ValueError('all68_slice_universe_mismatch')
    for r in s['members']:
        if Path(r['path']).name!=r['path'] or r['path']!=r['instrument']+'.parquet' or sha(slices/r['path'])!=r['sha256']:raise ValueError('slice_identity_drift')
    for n,d in PREDECESSORS.items():
        if sha(trad/n)!=d:raise ValueError('native_predecessor_drift_before_import')
    return {'schema_version':'forex_matched_remaining_native_operator.v1','sources':{n:sha(ROOT/n) for n in SOURCES},'predecessors':PREDECESSORS,
        'technical_identity':tm,'technical_payloads':th,'matched_identity':mm,'matched_scientific_payloads':mh,
        'slices_manifest_sha256':sha(slices/'SLICES_MANIFEST.json'),'universe':universe,
        'environment':{'python':platform.python_version(),**{n:importlib.metadata.version(n) for n in ('numpy','pandas','pyarrow','scipy','scikit-learn','joblib','threadpoolctl')}},
        'run_id':'matched-remaining-native-inputs','broker_access':False,'scope':'exact_fresh_remaining_forecasts_and_prepared_engineering_replay_curves'}
def preflight(recipe,digest,slices,technical,matched,trad):
    if recipe.is_symlink() or sha(recipe)!=digest:raise ValueError('frozen_recipe_hash_mismatch')
    r=json.loads(recipe.read_text())
    if r!=recipe_for(slices,technical,matched,trad):raise ValueError('source_input_environment_drift_before_import')
    return r
def operate(action,recipe,digest,slices,technical,matched,trad,runs):
    try:
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_action')
        r=preflight(recipe,digest,slices,technical,matched,trad);sys.path.insert(0,str(ROOT))
        from matched_remaining_runner_v2 import run,identity_for
        from publication import verify_completed_run
        i=identity_for(r);p=runs/r['run_id']
        if action in {'run','resume'}:run(slices,technical,matched,trad,r,runs_dir=runs,resume=True)
        if (p/'COMPLETION_MANIFEST.json').exists():
            verify_completed_run(p,i);report=json.loads((p/'run_report.json').read_text());coverage=json.loads((p/'coverage.json').read_text())
            keys={(x['record_id'],x['method'],x['procedure']) for x in coverage};eligible=sum(x['reason']=='eligible' for x in coverage)
            native_expected=sum(x['reason']=='eligible' and x['method'] in ('ridge','recovered_hgb') for x in coverage)
            if (report['new_models_fitted']!=10 or report['reused_models']!=6 or len(keys)!=2176 or len(coverage)!=2176 or
                report['forecast_rows']!=eligible or report['native_packets']!=native_expected or native_expected==0):raise ValueError('remaining_native_acceptance_missing')
            resources=json.loads((p/'fit_resources.json').read_text())
            if {x['minutes'] for x in resources}!={360,1080,1800,2160,2520} or any(not 0<=x['elapsed_seconds']<=30 for x in resources):raise ValueError('remaining_resource_acceptance_missing')
            status='completed_verified'
        elif p.exists():
            if json.loads((p/'RUN_IDENTITY.json').read_text())!=i:raise ValueError('partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action=='verify' and status!='completed_verified':raise ValueError('completed_run_required')
        return {'status':status,'run_identity':i['fingerprint'],'run_path':str(p),'recipe_sha256':digest,'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run',
            'engineering_ready':False,'forecast_evidence_status':'bounded_remaining_development','policy_evidence_status':'native_preparation_only','demo_authorization_status':'not_granted','independent_review':False}
    except Exception as exc:return {'status':'review_required','reason':str(exc),'next_action':'preserve_evidence_and_escalate'}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','slices','technical','matched','trad-root','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);a=p.parse_args();r=operate(a.action,a.recipe,a.recipe_sha256,a.slices,a.technical,a.matched,a.trad_root,a.runs_dir);print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
