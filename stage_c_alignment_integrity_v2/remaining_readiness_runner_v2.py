"""All68/eight-origin joint-ready remaining-target preparation and measurement."""
import hashlib,json,os,time
from pathlib import Path
from publication import RunPublisher,effective_run_identity,verify_completed_run
from remaining_readiness_native_v2 import schedule,build_origin,EPOCHS,TARGET,NEW_MINUTES,REUSE_MINUTES
def encoded(v):return (json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
def required():return ['origin_'+str(t)+'.json' for t in EPOCHS]+['schedule.json','run_report.json','source_references.json','prediction_resources.json']
def identity_for(r):return effective_run_identity(contract={'recipe':r,'required_payloads':required()},dependency_hashes={**r['sources'],**r['predecessors'],**{k:v['identity']['fingerprint'] for k,v in r['dependencies'].items()}})
def consumed(paths,r,alias,name):
    if Path(name).name!=name or name not in r['dependencies'][alias]['payloads']:raise ValueError('registered_remaining_payload_required')
    p=Path(paths[alias])/name;raw=p.read_bytes()
    if p.is_symlink() or hashlib.sha256(raw).hexdigest()!=r['dependencies'][alias]['payloads'][name]:raise ValueError('remaining_consumed_bytes_changed')
    return raw
def prepare(paths,r):
    meta={h:json.loads(consumed(paths,r,'remaining',f'fit_{h}.json')) for h in sorted(NEW_MINUTES+REUSE_MINUTES)};plan=schedule(meta);observations={t:[] for t in EPOCHS}
    for pair in r['universe']:
        for o in json.loads(consumed(paths,r,'technical','pair_'+pair+'.json'))['observations']:
            if o['origin_epoch'] in observations:observations[o['origin_epoch']].append(o)
    refs=json.loads(consumed(paths,r,'remaining','references.json'));original=json.loads(consumed(paths,r,'remaining','forecasts.json'))
    return meta,plan,observations,refs,original
def run(paths,trad,r,runs,resume=False,crash_after=None):
    i=identity_for(r);p=RunPublisher(runs,r['run_id'],i)
    if (p.root/'COMPLETION_MANIFEST.json').exists():verify_completed_run(p.root,i);return
    p.acquire(recover=resume)
    try:
        started=time.monotonic();meta,plan,observations,refs,original=prepare(paths,r);startup=time.monotonic()-started
        if startup>120:raise ValueError('remaining_startup_budget_exceeded')
        payloads=[p.write_or_validate_payload('schedule.json',encoded(plan))];timings=[];coverage=forecasts=packets=unready=0
        previous=p.read_verified_payload('prediction_resources.json')
        for index,epoch in enumerate(EPOCHS):
            name='origin_'+str(epoch)+'.json';raw=p.read_verified_payload(name);start=time.monotonic();existed=raw is not None
            if raw is None:
                h=(TARGET-epoch)//60
                # No weight bytes are consumed before this model set is ready.
                tree=consumed(paths,r,'remaining',f'fit_{h}.joblib') if epoch>=plan['joint_ready_epoch'] else None
                data=build_origin(epoch,meta[h],tree,observations[epoch],refs[str(epoch)],original,plan,trad);raw=encoded(data)
            else:data=json.loads(raw)
            payloads.append(p.write_or_validate_payload(name,raw));elapsed=time.monotonic()-start
            if elapsed>30:raise ValueError('remaining_whole_origin_hard_cap_exceeded')
            timings.append({'origin_epoch':epoch,'elapsed_seconds':elapsed,'existing_payload_validation_only':existed,'includes':'selected_weight_read_hash_load_prediction_native_preparation_serialization_durable_write','within_diagnostic_2seconds':elapsed<=2});coverage+=len(data['coverage']);forecasts+=len(data['forecasts']);packets+=len(data['native_packets']);unready+=sum(x['reason']=='model_set_not_joint_ready' for x in data['coverage'])
            if crash_after==index+1:os._exit(93)
        resource=json.loads(previous) if previous is not None else {'startup_seconds':startup,'startup_includes':'original_observation_reference_forecast_and_fit_metadata_authentication_and_preparation','origins':timings,'scope':'same_host_measured_diagnostic_not_actual_historical_publication'}
        report={'status':'completed_joint_ready_remaining_native_preparation','coverage_rows':coverage,'forecast_rows':forecasts,'native_packets':packets,'joint_unready_coverage_rows':unready,'original_target_epoch':TARGET,'origin_count':8,'instruments':68,'models_fitted':0,'original_artifacts_reused':16,'readiness_scope':plan['scope'],'original_numerical_values_preserved':True,'historical_packets_unissued':True,'engineering_ready':False,'forecast_evidence_status':'isolated_modeled_joint_readiness_development','policy_evidence_status':'unissued_native_preparation_only','demo_authorization_status':'not_granted','independent_review':False,'next_item':'whole_curve_warm_start_capacity_followup_v2','limitations':['not_full_campaign_shared_worker_schedule','modeled_not_actual_original_issuance','prepared_original_feature_inputs_not_live_ingestion','native_engineering_replay_cannot_issue','full_seven_target_two_second_serving_gap_remains']}
        for name,value in [('run_report.json',report),('source_references.json',{'dependencies':r['dependencies'],'predecessors':r['predecessors'],'original_records_unchanged':True}),('prediction_resources.json',resource)]:payloads.append(p.write_or_validate_payload(name,encoded(value)))
        if crash_after==0:os._exit(93)
        p.complete(payloads,set(required()))
    except BaseException:
        if p._owner_token is not None:p.release()
        raise
