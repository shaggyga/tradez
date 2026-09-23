"""Deterministic all68 visibility receipts from existing calibration records."""
import json,os
from collections import Counter
from contracts import fingerprint
from publication import RunPublisher,effective_run_identity,verify_completed_run
from calibration_record_inspector_v2 import OriginalRecordReader,ORIGINS
PROFILES=(('legacy26',60,'frozen','ridge'),('full228_cost2',1440,'adaptive','recovered_hgb'),('compact38_cost2',7200,'frozen','ridge'))
MODES=('frozen_prefix','expanding_prefix')
def required():return ['inspection_'+str(i)+'_'+m+'.json' for i in range(3) for m in MODES]+['inspection_contract.json','run_report.json']
def encoded(v):return (json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
def contract():return {'schema_version':'forex_calibration_original_record_inspector.v1','profiles':[list(x) for x in PROFILES],'modes':list(MODES),'visibility_states':['origin_before_modeled_publication','origin_plus112_seconds','explicit_outcome_at_own_maturity'],'numerical_availability':'original_modeled_base_forecast_clock_not_actual_publication','outcomes':'explicit_reveal_and_separate_maturity_asof','fitting_allowed':False,'inspection_snapshot':'authenticated_original_bytes_cached_only_for_one_reader','independent_review':False}
def identity_for(r):return effective_run_identity(contract={'recipe':r,'required_payloads':required()},dependency_hashes={**r['sources'],**{k:v['identity']['fingerprint'] for k,v in r['dependencies'].items()}})
def query(profile,pair,mode,state):
    g,h,p,m=profile;origin=ORIGINS[-1]
    return {'group':g,'horizon_minutes':h,'base_procedure':p,'method':m,'instrument':pair,'origin_epoch':origin,'calibration_mode':mode,'asof_epoch':origin if state==0 else origin+112,'reveal_outcome':state==2,'outcome_asof_epoch':origin+h*60 if state==2 else None}
def compact(result):
    return {'query':result['query'],'status':result['status'],'outcome_status':result['outcome_status'],'inspection_sha256':fingerprint(result),'record_visible':result['record'] is not None,'snapshot_visible':result['snapshot'] is not None,'outcome_visible':result['outcome'] is not None,'actual_publication_qualified':False,'base_models_fitted':0,'calibrators_fitted':0}
def run(paths,r,runs,resume=False,crash_after=None):
    i=identity_for(r);p=RunPublisher(runs,r['run_id'],i)
    if (p.root/'COMPLETION_MANIFEST.json').exists():verify_completed_run(p.root,i);return
    p.acquire(recover=resume)
    try:
        reader=OriginalRecordReader(paths,r);payloads=[p.write_or_validate_payload('inspection_contract.json',encoded(contract()))];counts=Counter();outcomes=Counter();total=0;index=0
        for n,profile in enumerate(PROFILES):
            for mode in MODES:
                name='inspection_'+str(n)+'_'+mode+'.json';raw=p.read_verified_payload(name)
                if raw is None:raw=encoded([compact(reader.inspect(query(profile,pair,mode,state))) for pair in r['universe'] for state in range(3)])
                rows=json.loads(raw);total+=len(rows);counts.update(x['status'] for x in rows);outcomes.update(x['outcome_status'] for x in rows);payloads.append(p.write_or_validate_payload(name,raw));index+=1
                if crash_after==index:os._exit(92)
        report={'status':'completed_original_record_inspection','inspection_cases':total,'instruments':len(r['universe']),'profiles':len(PROFILES),'modes':list(MODES),'visibility_states':3,'statuses':dict(sorted(counts.items())),'outcome_statuses':dict(sorted(outcomes.items())),'base_models_fitted':0,'calibrators_fitted':0,'engineering_ready':False,'forecast_evidence_status':'original_modeled_clock_development_inspection','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False,'next_item':'native_remaining_target_readiness_rebuild_v2','limitations':['modeled_not_attested_historical_publication','no_production_calibrator_latency','dependent_inspected_development','full_native_serving_gap_remains']}
        payloads.append(p.write_or_validate_payload('run_report.json',encoded(report)))
        if crash_after==0:os._exit(92)
        p.complete(payloads,set(required()))
    except BaseException:
        if p._owner_token is not None:p.release()
        raise
