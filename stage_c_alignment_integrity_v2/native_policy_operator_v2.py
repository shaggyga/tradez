"""Source-bound native-input qualification through existing policy replay/recovery."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def recipe_for(trad_root):
    from forex_operator_v2 import approved_recipe,environment
    base=approved_recipe(trad_root,'synthetic_policy_continuation')
    return {'schema_version':'forex_native_policy_operator.v1','base_recipe':base,
      'sources':{n:sha(ROOT/n) for n in ['native_policy_operator_v2.py','native_policy_fixture_v2.py','native_policy_input_v2.py']},
      'native_predecessors':{n:sha(trad_root/n) for n in ['oanda_forecast_curve_contract_v1.py','oanda_curve_management_adapter_v1.py']},
      'environment':environment(),'run_ids':{e:'native-policy-'+e for e in ['reference','optimized']},
      'input_tier':'synthetic_fresh_conditional_native_curve.v1','engineering_ready':False,'demo_authorization_status':'not_granted'}

def operate(action,recipe_path,recipe_sha256,runs_dir,trad_root):
    try:
        if recipe_path.is_symlink() or sha(recipe_path)!=recipe_sha256:raise ValueError('native_recipe_hash_mismatch')
        recipe=json.loads(recipe_path.read_text())
        # The externally pinned recipe is checked before importing any local runtime.
        for name,digest in {**recipe['base_recipe']['sources'],**recipe['sources']}.items():
            if Path(name).name!=name or sha(ROOT/name)!=digest:raise ValueError('native_source_dependency_or_environment_drift')
        for name,digest in {**recipe['base_recipe']['predecessors'],**recipe['native_predecessors']}.items():
            if sha(trad_root/name)!=digest:raise ValueError('native_source_dependency_or_environment_drift')
        if recipe!=recipe_for(trad_root):raise ValueError('native_source_dependency_or_environment_drift')
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_native_action')
        from native_policy_fixture_v2 import fixture
        from policy_runner_v2 import run,identity_for
        from publication import verify_completed_run
        contract,frames=fixture(trad_root);records=[]
        if sum(len(f.get('candidates',[])) for f in frames)!=10 or any(f.get('native_refusals') for f in frames):raise ValueError('native_fixture_admission_missing')
        for engine,run_id in recipe['run_ids'].items():
            root=runs_dir/run_id;identity=identity_for(contract,frames,trad_root,engine)
            if action in {'run','resume'}:run(contract,frames,run_id=run_id,runs_dir=runs_dir,trad_root=trad_root,engine=engine,resume=True)
            if (root/'COMPLETION_MANIFEST.json').exists():
                verify_completed_run(root,identity);report=json.loads((root/'run_report.json').read_text())
                if report['decision_count']!=30 or report['accounting_oracle']['status']!='verified':raise ValueError('native_policy_audit_missing')
                if any(r['open_lot_count'] or r['pending_order_units'] or r['rejected_events'] for r in report['arms'].values()):raise ValueError('native_final_reconciliation_failed')
                if report['actions']['recovered']['ENTER']!=0:raise ValueError('unsupported_legacy_curve_selector_was_used')
                if any(report['actions'][arm]['ENTER']<1 for arm in ['fixed_hold','continuation','hysteresis','naive']):raise ValueError('native_policy_entry_coverage_missing')
                status='completed_verified'
            elif root.exists():
                if json.loads((root/'RUN_IDENTITY.json').read_text())!=identity:raise ValueError('native_partial_identity_mismatch')
                status='resumable'
            else:status='ready'
            records.append({'engine':engine,'run_id':run_id,'path':str(root),'status':status})
        complete=all(r['status']=='completed_verified' for r in records)
        if complete:
            for name in ['policy_decisions.jsonl','policy_state.json','event_ledger.jsonl','final_state.json','accounting_audit.json']:
                if (runs_dir/'native-policy-reference'/name).read_bytes()!=(runs_dir/'native-policy-optimized'/name).read_bytes():raise ValueError('native_engine_parity_failed:'+name)
        if action=='verify' and not complete:raise ValueError('native_completed_runs_required')
        status='completed_verified' if complete else 'resumable' if any(r['status']=='resumable' for r in records) else 'ready'
        return {'status':status,'runs':records,'recipe_sha256':recipe_sha256,'next_action':'record_receipt_and_stop' if complete else 'resume' if status=='resumable' else 'run',
          'engineering_ready':False,'forecast_evidence_status':'synthetic_declared_forecasts_only','policy_evidence_status':'synthetic_native_input_integration_only',
          'demo_authorization_status':'not_granted','unsupported_arm':'recovered_selector_has_no_native_curve_score_confidence_fields'}
    except Exception as exc:return {'status':'review_required','detail':str(exc),'next_action':'preserve_evidence_and_escalate'}

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['status','run','resume','verify'])
    p.add_argument('--recipe',type=Path,required=True);p.add_argument('--recipe-sha256',required=True);p.add_argument('--runs-dir',type=Path,required=True)
    p.add_argument('--trad-root',type=Path,default=ROOT.parent/'trad');a=p.parse_args()
    r=operate(a.action,a.recipe,a.recipe_sha256,a.runs_dir,a.trad_root);print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
