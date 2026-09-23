"""Frozen offline two-model/two-scenario policy operator; no model refitting."""
import argparse,hashlib,importlib.metadata,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
EXTRA=('matched_policy_operator_v2.py','matched_policy_fixture_v2.py','matched_policy_input_v2.py',
    'historical_native_input_v2.py','historical_market_inputs_v2.py','historical_policy_fixture_v2.py',
    'remaining_horizon_v2.py','fitted_consumer_v2.py','all68_neutral_runner_v2.py','quote_input_qualification_v2.py',
    'matched_remaining_native_v2.py','matched_campaign_models_v2.py','retained_signed_cost_models_v1.py')
METHODS=('ridge','recovered_hgb')
SCENARIOS=('candle_zero_slippage_financing','candle_one_bp_slippage_rollover')
PARITY=('policy_decisions.jsonl','policy_state.json','event_ledger.jsonl','final_state.json','accounting_audit.json')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return json.loads(p.read_text())

def recipe_for(input_path,trad):
    sys.path.insert(0,str(ROOT))
    from forex_operator_v2 import approved_recipe
    return {'schema_version':'forex_matched_policy_operator.v1','base_recipe':approved_recipe(trad,'synthetic_policy_continuation'),
        'sources':{n:sha(ROOT/n) for n in EXTRA},'input_sha256':sha(input_path),'methods':list(METHODS),'scenarios':list(SCENARIOS),
        'engines':['reference','optimized'],'numerical_environment':{n:importlib.metadata.version(n) for n in ('pandas','scipy','scikit-learn','joblib','threadpoolctl')},
        'scope':'matched26_two_day_development_candle_scenario','observed_execution':False,'broker_access':False,'model_refit':False}

def preflight(recipe,digest,input_path,trad):
    if recipe.is_symlink() or sha(recipe)!=digest:raise ValueError('matched_policy_recipe_hash_mismatch')
    r=read(recipe)
    for n,h in {**r['base_recipe']['sources'],**r['sources']}.items():
        if Path(n).name!=n or sha(ROOT/n)!=h:raise ValueError('matched_policy_source_drift_before_import')
    if r!=recipe_for(input_path,trad):raise ValueError('matched_policy_input_environment_predecessor_drift')
    return r

def operate(action,recipe,digest,input_path,runs,trad):
    try:
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_action')
        r=preflight(recipe,digest,input_path,trad);sys.path.insert(0,str(ROOT))
        from matched_policy_fixture_v2 import fixture
        from policy_runner_v2 import run,identity_for
        from publication import verify_completed_run
        data=read(input_path);records=[]
        for method in METHODS:
            for scenario in SCENARIOS:
                c,frames,coverage=fixture(data,method,scenario,trad)
                if len(coverage)!=544 or len({x['instrument'] for x in coverage})!=68:raise ValueError('matched_policy_all68_coverage_missing')
                admitted=sum(x['status']=='admitted' for x in coverage)
                refused=[x for f in frames for x in f.get('historical_refusals',[])]
                if admitted+len(refused)!=538:raise ValueError('matched_policy_native_input_support_changed')
                for engine in r['engines']:
                    name=method+'-'+scenario+'-'+engine;p=runs/name;i=identity_for(c,frames,trad,engine)
                    if action in {'run','resume'}:run(c,frames,run_id=name,runs_dir=runs,trad_root=trad,engine=engine,resume=True)
                    if (p/'COMPLETION_MANIFEST.json').exists():
                        verify_completed_run(p,i);report=read(p/'run_report.json')
                        if report['accounting_oracle']['status']!='verified' or report['decision_count']!=54:raise ValueError('matched_policy_accounting_audit_missing')
                        if report['native_admission']['accepted']!=admitted or report['native_admission']['refused']!=len(refused):raise ValueError('matched_policy_admission_mismatch')
                        if any(a['open_lot_count'] or a['pending_order_units'] or a['rejected_events'] for a in report['arms'].values()):raise ValueError('matched_policy_final_reconciliation_failed')
                        if report['actions']['cash']['ENTER'] or report['actions']['recovered']['ENTER']:raise ValueError('unsupported_or_cash_arm_entered')
                        status='completed_verified'
                    elif p.exists():
                        if read(p/'RUN_IDENTITY.json')!=i:raise ValueError('matched_policy_partial_identity_mismatch')
                        status='resumable'
                    else:status='ready'
                    records.append({'method':method,'scenario':scenario,'engine':engine,'run_id':name,'path':str(p),'status':status,'coverage':len(coverage),'admitted':admitted,'refused':len(refused)})
                if all(x['status']=='completed_verified' for x in records if x['method']==method and x['scenario']==scenario):
                    for n in PARITY:
                        prefix=method+'-'+scenario+'-'
                        if (runs/(prefix+'reference')/n).read_bytes()!=(runs/(prefix+'optimized')/n).read_bytes():raise ValueError('matched_policy_engine_parity_failed:'+n)
        complete=all(x['status']=='completed_verified' for x in records)
        if action=='verify' and not complete:raise ValueError('matched_policy_completed_runs_required')
        status='completed_verified' if complete else 'resumable' if any(x['status']=='resumable' for x in records) else 'ready'
        return {'status':status,'runs':records,'recipe_sha256':digest,'next_action':'record_receipt_and_stop' if complete else 'resume' if status=='resumable' else 'run',
            'engineering_ready':False,'forecast_evidence_status':'bounded_matched_remaining_development','policy_evidence_status':'two_declared_candle_scenarios_not_observed_execution','demo_authorization_status':'not_granted','independent_review':False}
    except Exception as exc:return {'status':'review_required','reason':str(exc),'next_action':'preserve_evidence_and_escalate'}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','input','runs-dir','trad-root'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);a=p.parse_args();r=operate(a.action,a.recipe,a.recipe_sha256,a.input,a.runs_dir,a.trad_root);print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
