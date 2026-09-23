"""Durable bounded policy replay using the existing publication/state machinery."""
import argparse
from copy import deepcopy
import json
import os
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
from policy_continuation_v2 import PolicyReplay
from policy_fixture_v2 import fixture
from accounting_event_runner_v2 import encoded, decoded, plain_bytes, restore_state, scorecard, SOURCE_NAMES
from accounting_event_audit_v2 import audit_accounting
from publication import RunPublisher,effective_run_identity,sha256_file,verify_completed_run
from reference_accounting_adapter_v2 import DEFAULT_TRAD,load_reference,reference_dependency_identity

POLICY_SOURCES=(*SOURCE_NAMES,"policy_continuation_v2.py","policy_fixture_v2.py","policy_runner_v2.py","native_policy_input_v2.py")
HISTORICAL_POLICY_SOURCES=('historical_native_input_v2.py','historical_market_inputs_v2.py',
    'historical_policy_fixture_v2.py','remaining_horizon_v2.py','fitted_consumer_v2.py',
    'all68_neutral_runner_v2.py','quote_input_qualification_v2.py')


def required(count):
    return {"run_inputs.json","policy_decisions.jsonl","policy_state.json","event_ledger.jsonl","final_state.json",
            "accounting_audit.json","run_report.json",*(f"policy_snapshot_{n:06d}.json" for n in range(1,count+1))}


def identity_for(contract,frames,trad_root=DEFAULT_TRAD,engine="reference"):
    ref=load_reference(trad_root)
    from policy_continuation_v2 import RETROSPECTIVE_POLICY_TIER
    sources=(*POLICY_SOURCES,*HISTORICAL_POLICY_SOURCES) if contract.get('input_tier')==RETROSPECTIVE_POLICY_TIER else POLICY_SOURCES
    if any(f.get('model_profile') is not None for f in frames):
        sources=(*sources,'matched_policy_input_v2.py','matched_policy_fixture_v2.py',
            'matched_remaining_native_v2.py','matched_campaign_models_v2.py','retained_signed_cost_models_v1.py')
    return effective_run_identity(contract={"schema_version":"forex_policy_run.v2","contract":contract,"engine":engine,
        "python":".".join(map(str,sys.version_info[:3])),"frame_count":len(frames),"required_payloads":sorted(required(len(frames)))},
        dependency_hashes={**{n:sha256_file(ROOT/n) for n in sources},
            "reference_closure":ref.digest(reference_dependency_identity(trad_root)),"inputs":ref.digest({"contract":contract,"frames":frames})})


def run(contract,frames,*,run_id,runs_dir,trad_root=DEFAULT_TRAD,engine="reference",resume=False,crash_after=None):
    if not isinstance(frames,list) or not 1<=len(frames)<=128:raise ValueError("bounded_policy_frames_required")
    replay=PolicyReplay(contract,trad_root=trad_root,engine=engine)
    identity=identity_for(contract,frames,trad_root,engine)
    publisher=RunPublisher(runs_dir,run_id,identity)
    if (publisher.root/'COMPLETION_MANIFEST.json').exists():
        verify_completed_run(publisher.root,identity)
        return {"status":"verified_completed","identity":identity["fingerprint"]}
    publisher.acquire(recover=resume)
    try:
        payloads=[publisher.write_or_validate_payload('run_inputs.json',plain_bytes(replay.ref,{'contract':contract,'frames':frames}))]
        start=0
        for count in range(1,len(frames)+1):
            name=f'policy_snapshot_{count:06d}.json'; raw=publisher.read_verified_payload(name)
            if raw is None:break
            saved=decoded(raw)
            if saved['identity']!=identity['fingerprint'] or saved['prefix']!=replay.ref.digest(frames[:count]) or saved['frames_processed']!=count:
                raise ValueError('policy_checkpoint_prefix_or_identity_mismatch')
            replay.events=saved['events']
            replay.rows=restore_state(replay.book,saved['accounting'],replay.events,len(replay.events),identity)
            replay.memory=saved['memory']; replay.decisions=saved['decisions']; replay.cursor=saved['cursor']
            if set(replay.memory)!=set(contract['policies']) or replay.cursor!=frames[count-1]['epoch']:
                raise ValueError('policy_checkpoint_memory_or_cursor_mismatch')
            payloads.append(publisher.write_or_validate_payload(name,raw));start=count
        for index in range(start,len(frames)):
            replay.apply(frames[index]);count=index+1
            accounting={'schema_version':'forex_accounting_state_checkpoint.v2','run_identity':identity['fingerprint'],
                'events_processed':len(replay.events),'prefix_sha256':replay.ref.digest(replay.events),'state':replay.book.state,
                'state_sha256':replay.ref.digest(replay.book.state),'ledger':replay.rows}
            saved={'identity':identity['fingerprint'],'frames_processed':count,'prefix':replay.ref.digest(frames[:count]),
                   'accounting':accounting,'events':replay.events,'memory':replay.memory,'decisions':replay.decisions,'cursor':replay.cursor}
            payloads.append(publisher.write_or_validate_payload(f'policy_snapshot_{count:06d}.json',encoded(saved)))
            if crash_after==count:os._exit(91)
        audit=audit_accounting(contract['ledger'],replay.events,replay.rows)
        actions={arm:{action:sum(d['arm']==arm and d['action']==action for d in replay.decisions)
                      for action in ('WAIT','ENTER','HOLD','EXIT','REPLACE')} for arm in contract['policies']}
        report={'schema_version':'forex_policy_report.v2','engine':engine,'status':'synthetic_policy_integration_only',
            'frames':len(frames),'event_count':len(replay.events),'decision_count':len(replay.decisions),'actions':actions,
            'accounting_oracle':audit,'arms':scorecard(replay.book,replay.rows,replay.events),
            'engineering_ready':False,'forecast_evidence_status':'no_new_model_evidence','policy_evidence_status':'synthetic_only',
            'demo_authorization_status':'not_granted','limitations':['two-instrument_fixture_not_all68_campaign',
            'declared_momentum_forecasts_not_fitted_models','frozen_spread_conversion_financing_scenario',
            'explicit_synthetic_execution_only','recovered_selector_is_diagnostic_not_common_value_policy',
            'replacement_requires_exit_fill_and_fresh_decision','no_broker_or_paid_calls']}
        if any(f.get('candidate_kind')=='curve' for f in frames):
            report['limitations']=[v for v in report['limitations'] if v!='declared_momentum_forecasts_not_fitted_models']
            report['limitations']+=['declared_native_conditional_fixture_not_fitted_models',
                'legacy_rotation_abstains_missing_native_confidence_score_no_matched_six_arm_claim']
            report['native_admission']={'input_tier':'synthetic_fresh_conditional_native_curve.v1',
                'accepted':sum(len(f.get('candidates',[])) for f in frames if f.get('candidate_kind')=='curve'),
                'refused':sum(len(f.get('native_refusals',[])) for f in frames),
                'historical_execution_qualified':False}
        from policy_continuation_v2 import RETROSPECTIVE_POLICY_TIER
        if contract['input_tier']==RETROSPECTIVE_POLICY_TIER:
            report.update(status='retrospective_candle_policy_scenario_only',
                forecast_evidence_status='preserved_fitted_gross_remaining_development_forecasts',
                policy_evidence_status='declared_execution_scenario_not_observed_broker_evidence',
                limitations=['retained_candle_quotes_not_recorded_simultaneous_executable_quotes',
                    'assumed_arrival_and_full_fill_at_next_minute_close',
                    'declared_slippage_financing_fee_and_margin_scenarios',
                    'single_development_cohort_not_confirmation_or_full_campaign',
                    'frozen_spread_conversion_and_rollover_value_assumptions',
                    'legacy_selector_abstains_without_momentum_score_confidence',
                    'no_model_refit_no_broker_or_paid_calls'])
            report['native_admission']={'input_tier':RETROSPECTIVE_POLICY_TIER,
                'accepted':sum(len(f.get('candidates',[])) for f in frames),
                'refused':sum(len(f.get('historical_refusals',[])) for f in frames),
                'historical_execution_qualified':False,'observed_publication':False}
            if any(f.get('model_profile') is not None for f in frames):
                methods={f.get('method') for f in frames};profiles={f.get('model_profile') for f in frames}
                if len(methods)!=1 or len(profiles)!=1:raise ValueError('matched_policy_run_profile_mismatch')
                report.update(model_profile=next(iter(profiles)),method=next(iter(methods)),
                    forecast_evidence_status='matched26_fitted_fresh_remaining_development_forecasts')
            report['valuation_coverage']={arm:{
                'event_marks_unavailable':sum(row['arms'][arm]['equity_usd'] is None for row in replay.rows),
                'decision_values_unavailable':sum(d['arm']==arm and d['values'].get('valuation_status')=='unavailable' for d in replay.decisions),
                'path_metrics_status':'incomplete_mark_coverage' if any(row['arms'][arm]['equity_usd'] is None for row in replay.rows) else 'observed_frame_marks_only_not_intrabar'}
                for arm in contract['policies']}
            if any(v['event_marks_unavailable'] for v in report['valuation_coverage'].values()):
                report['limitations'].append('missing_marks_preserved_no_exact_drawdown_or_continuous_risk_claim')
        output={'policy_decisions.jsonl':b''.join(plain_bytes(replay.ref,d) for d in replay.decisions),
            'event_ledger.jsonl':b''.join(plain_bytes(replay.ref,r) for r in replay.rows),
            'policy_state.json':encoded(replay.memory),'final_state.json':encoded(replay.book.state),
            'accounting_audit.json':plain_bytes(replay.ref,audit),'run_report.json':plain_bytes(replay.ref,report)}
        for name,raw in output.items():payloads.append(publisher.write_or_validate_payload(name,raw))
        if crash_after==0:os._exit(91)
        publisher.complete(payloads,required(len(frames)))
    except BaseException:
        if publisher._owner_token is not None:publisher.release()
        raise
    return {'status':'completed','identity':identity['fingerprint'],'frames':len(frames),'events':len(replay.events)}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id',required=True);p.add_argument('--runs-dir',type=Path,required=True)
    p.add_argument('--trad-root',type=Path,default=DEFAULT_TRAD);p.add_argument('--engine',choices=['reference','optimized'],default='reference')
    p.add_argument('--resume',action='store_true');p.add_argument('--input',type=Path)
    p.add_argument('--test-crash-after-frame',type=int,help=argparse.SUPPRESS)
    a=p.parse_args()
    if a.input:
        supplied=json.loads(a.input.read_text(encoding='utf-8'))
        if set(supplied)!={'contract','frames'}:raise ValueError('exact_policy_input_required')
        contract,frames=supplied['contract'],supplied['frames']
    else:contract,frames=fixture(a.trad_root)
    print(json.dumps(run(contract,frames,run_id=a.run_id,runs_dir=a.runs_dir,trad_root=a.trad_root,
        engine=a.engine,resume=a.resume,crash_after=a.test_crash_after_frame),sort_keys=True))


if __name__=='__main__':main()
