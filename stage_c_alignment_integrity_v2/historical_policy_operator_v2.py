"""Frozen offline candle-scenario replay through both existing accounting engines."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parent
EXTRA=('historical_policy_operator_v2.py','historical_policy_fixture_v2.py',
       'historical_native_input_v2.py','historical_market_inputs_v2.py','remaining_horizon_v2.py',
       'fitted_consumer_v2.py','all68_neutral_runner_v2.py','quote_input_qualification_v2.py')
SCENARIO_IDS=('candle_zero_slippage_financing','candle_one_bp_slippage_rollover')


def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()


def recipe_for(input_path,trad_root):
    sys.path.insert(0,str(ROOT))
    from forex_operator_v2 import approved_recipe
    base=approved_recipe(trad_root,'synthetic_policy_continuation')
    return {'schema_version':'forex_historical_policy_operator.v1','base_recipe':base,
        'sources':{name:sha(ROOT/name) for name in EXTRA},'input_sha256':sha(input_path),
        'scenario_ids':list(SCENARIO_IDS),'engines':['reference','optimized'],
        'input_tier':'historical_fitted_candle_policy_scenario.v1','broker_access':False,
        'scope':'single_development_cohort_declared_execution_scenarios_no_observed_broker_history'}


def operate(action,recipe_path,recipe_sha256,input_path,runs_dir,trad_root):
    try:
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_action')
        if recipe_path.is_symlink() or sha(recipe_path)!=recipe_sha256:raise ValueError('frozen_historical_recipe_hash_mismatch')
        recipe=json.loads(recipe_path.read_text())
        for name,digest in {**recipe['base_recipe']['sources'],**recipe['sources']}.items():
            if Path(name).name!=name or sha(ROOT/name)!=digest:raise ValueError('historical_source_drift_before_import')
        if recipe!=recipe_for(input_path,trad_root):raise ValueError('historical_input_predecessor_or_environment_drift')
        from historical_policy_fixture_v2 import fixture
        from policy_runner_v2 import run,identity_for
        from publication import verify_completed_run
        inputs=json.loads(input_path.read_text());records=[]
        for scenario in SCENARIO_IDS:
            contract,frames,coverage=fixture(inputs,scenario,trad_root)
            if len(coverage)!=544 or len({r['instrument'] for r in coverage})!=68:
                raise ValueError('all68_historical_preparation_coverage_missing')
            if sum(r['status']=='prepared' for r in coverage)!=258:
                raise ValueError('historical_prepared_packet_coverage_changed')
            for engine in recipe['engines']:
                run_id=scenario+'-'+engine;root=runs_dir/run_id
                identity=identity_for(contract,frames,trad_root,engine)
                if action in {'run','resume'}:
                    run(contract,frames,run_id=run_id,runs_dir=runs_dir,trad_root=trad_root,engine=engine,resume=True)
                if (root/'COMPLETION_MANIFEST.json').exists():
                    verify_completed_run(root,identity);report=json.loads((root/'run_report.json').read_text())
                    if report['decision_count']!=54 or report['accounting_oracle']['status']!='verified':raise ValueError('historical_policy_consumer_audit_missing')
                    # The pinned input contains one HKD_JPY origin without its
                    # base-currency conversion quote. Preserve that refusal.
                    refused=[r for f in frames for r in f.get('historical_refusals',[])]
                    if (report['native_admission']['accepted']!=257 or report['native_admission']['refused']!=1 or
                        [(r['instrument'],r['reason']) for r in refused]!=[('HKD_JPY','financing_conversion_unavailable')]):
                        raise ValueError('historical_native_candidate_coverage_missing')
                    if any(a['open_lot_count'] or a['pending_order_units'] or a['rejected_events'] for a in report['arms'].values()):raise ValueError('historical_final_reconciliation_failed')
                    if any(report['actions'][a]['ENTER']<1 for a in ('fixed_hold','naive','continuation','hysteresis')):raise ValueError('historical_entry_coverage_missing')
                    if report['actions']['recovered']['ENTER'] or report['actions']['cash']['ENTER']:raise ValueError('unsupported_or_cash_arm_entered')
                    status='completed_verified'
                elif root.exists():
                    if json.loads((root/'RUN_IDENTITY.json').read_text())!=identity:raise ValueError('historical_partial_identity_mismatch')
                    status='resumable'
                else:status='ready'
                records.append({'scenario':scenario,'engine':engine,'run_id':run_id,'path':str(root),'status':status})
            if all(r['status']=='completed_verified' for r in records if r['scenario']==scenario):
                for name in ('policy_decisions.jsonl','policy_state.json','event_ledger.jsonl','final_state.json','accounting_audit.json'):
                    if (runs_dir/(scenario+'-reference')/name).read_bytes()!=(runs_dir/(scenario+'-optimized')/name).read_bytes():raise ValueError('historical_reference_optimized_parity_failed:'+name)
        complete=all(r['status']=='completed_verified' for r in records)
        if action=='verify' and not complete:raise ValueError('historical_completed_runs_required')
        status='completed_verified' if complete else 'resumable' if any(r['status']=='resumable' for r in records) else 'ready'
        return {'status':status,'runs':records,'recipe_sha256':recipe_sha256,
            'next_action':'record_receipt_and_stop' if complete else 'resume' if status=='resumable' else 'run',
            'engineering_ready':False,'forecast_evidence_status':'preserved_gross_remaining_development_only',
            'policy_evidence_status':'two_candle_execution_scenarios_not_observed_broker_evidence',
            'demo_authorization_status':'not_granted','independent_review':False}
    except Exception as exc:
        return {'status':'review_required','reason':str(exc),'next_action':'preserve_evidence_and_escalate'}


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['status','run','resume','verify'])
    for name in ('recipe','input','runs-dir','trad-root'):p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);a=p.parse_args()
    r=operate(a.action,a.recipe,a.recipe_sha256,a.input,a.runs_dir,a.trad_root)
    print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
