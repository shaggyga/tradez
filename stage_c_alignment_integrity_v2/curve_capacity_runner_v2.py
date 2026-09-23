"""Chronological whole-origin comparison with durable exact numerical evidence."""
from collections import defaultdict,Counter
import json,math,os,time
from contracts import fingerprint
from publication import RunPublisher,effective_run_identity,verify_completed_run
from campaign_inspector_v2 import CampaignReader
from rich_family_runner_v2 import load_inputs
from joint_readiness_schedule_v2 import schedule,select_fit,GROUPS,HORIZONS,PROCEDURES,METHODS
from joint_readiness_runner_v2 import source_rows
from joint_readiness_predict_v2 import RetainedPredictor
from curve_capacity_predict_v2 import SharedCurvePredictor
ORIGINS=tuple(range(1721606460,1722038400,21600))
ENGINES=('reference','shared')
def encoded(x):return (json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
def required():return sorted([f'{e}_{t}.json' for e in ENGINES for t in ORIGINS]+['schedule.json','run_report.json','source_references.json','curve_resources.json'])
def identity_for(r):return effective_run_identity(contract={'recipe':r,'required_payloads':required()},dependency_hashes={**r['sources'],**r['predecessors'],**{k:v['identity']['fingerprint'] for k,v in r['dependencies'].items()}})

def prepare(paths,r):
    reader=CampaignReader(paths,r['dependencies']);c=reader.read('family','experiment_contract.json')
    if c['decision_epochs']!=list(ORIGINS):raise ValueError('original_curve_origins_required')
    fits=[]
    for alias in ('baseline','family'):
        for m in reader.read(alias,'model_inventory.json'):fits.append({'group':m.get('group','legacy26'),'horizon_minutes':m['target']['horizon_seconds']//60,'fit_cutoff':m['fit_cutoff'],'fit_id':m['fit_id'],'maximum_outcome_available_epoch':m['maximum_outcome_available_epoch']})
    sched=schedule(fits,list(ORIGINS),c['fit_cutoffs']);obs,_,views=load_inputs(reader.paths['rich'],r['dependencies']['family']['identity']['contract']['recipe']);by_origin=defaultdict(list)
    for o in obs:
        if o['origin_epoch'] in ORIGINS:by_origin[o['origin_epoch']].append(o)
    for rows in by_origin.values():
        rows.sort(key=lambda o:o['instrument'])
        if [o['instrument'] for o in rows]!=r['configuration']['universe']:raise ValueError('all68_original_curve_observations_required')
    references={}
    for g in GROUPS:
        for h in HORIZONS:
            for procedure in PROCEDURES:references[g,h,procedure]=source_rows(reader,g,h,procedure)[0]
    return reader,c,sched,by_origin,views,references

def curve(predictor,origin,obs,views,sched,c,references):
    if isinstance(predictor,SharedCurvePredictor):predictor.begin_origin(origin)
    forecasts=[];coverage=[];selected=set();counts=Counter()
    for g in GROUPS:
        for h in HORIZONS:
            for procedure in PROCEDURES:
                task=select_fit(sched,g,h,procedure,origin)
                actual=predictor.predict(task,obs,views) if task is not None else {}
                if task is not None:selected.add(task['fit_id']);counts['prediction_requests']+=1
                source_procedure='frozen' if task is not None and task['fit_cutoff']==c['fit_cutoffs'][0] else 'adaptive'
                source=references[g,h,source_procedure]
                for o in obs:
                    reason='joint_model_not_ready' if task is None else 'missing_features' if o['features'] is None else 'feature_not_ready' if o['available_epoch']>origin else 'eligible'
                    for method in METHODS:
                        cov={'record_id':o['record_id'],'instrument':o['instrument'],'group':g,'horizon_minutes':h,'procedure':procedure,'method':method,'reason':reason,'selected_fit_id':task['fit_id'] if task else None}
                        coverage.append(cov)
                        if reason!='eligible':continue
                        original=source.get((o['record_id'],method));value=actual.get((o['record_id'],method))
                        if original is None or value is None:raise ValueError('original_curve_value_missing')
                        if original['forecast']['model_id']!=fingerprint({'fit_id':task['fit_id'],'method':method}):raise ValueError('curve_original_model_lineage_mismatch')
                        if not math.isclose(value,original['forecast']['prediction'],rel_tol=1e-12,abs_tol=1e-10):raise ValueError('curve_original_numerical_parity_failed')
                        forecasts.append({**cov,'prediction':value,'original_forecast_id':original['forecast']['forecast_id'],'original_record_sha256':fingerprint(original),'joint_ready_epoch':task['joint_ready_epoch']})
    expected={(o['record_id'],method) for o in obs for method in METHODS}
    if len(coverage)!=len(GROUPS)*len(HORIZONS)*len(PROCEDURES)*len(expected):raise ValueError('complete_curve_coverage_required')
    return {'origin_epoch':origin,'forecasts':forecasts,'coverage':coverage,'selected_fit_ids':sorted(selected),'prediction_requests':counts['prediction_requests'],'unique_fit_predictions':len(selected),'new_forecast_availability_declared':False}

def parity(a,b):
    if {k:v for k,v in a.items() if k!='forecasts'}!={k:v for k,v in b.items() if k!='forecasts'}:raise ValueError('curve_coverage_or_selection_mismatch')
    if len(a['forecasts'])!=len(b['forecasts']):raise ValueError('curve_prediction_count_mismatch')
    for x,y in zip(a['forecasts'],b['forecasts']):
        if {k:v for k,v in x.items() if k!='prediction'}!={k:v for k,v in y.items() if k!='prediction'} or not math.isclose(x['prediction'],y['prediction'],rel_tol=1e-12,abs_tol=1e-10):raise ValueError('reference_shared_curve_parity_failed')

def validate_resources(resources):
    rows=resources['origins']
    if len(rows)!=40 or {(x['origin_epoch'],x['engine']) for x in rows}!={(t,e) for t in ORIGINS for e in ENGINES}:raise ValueError('complete_curve_timing_inventory_required')
    for x in rows:
        if type(x['elapsed_seconds']) not in (int,float) or not 0<=x['elapsed_seconds']<=30:raise ValueError('curve_hard_resource_limit_exceeded')
        if x['diagnostic_target_met']!=(x['elapsed_seconds']<=2) or x['live_ready'] is not False:raise ValueError('curve_timing_cannot_promote_readiness')
    if not 0<=resources['startup_seconds']<=180:raise ValueError('curve_startup_resource_limit_exceeded')

def run(paths,r,runs,*,resume=False,crash_after=None):
    identity=identity_for(r);p=RunPublisher(runs,r['run_id'],identity)
    if (p.root/'COMPLETION_MANIFEST.json').exists():verify_completed_run(p.root,identity);return
    p.acquire(recover=resume)
    try:
        start=time.monotonic();reader,c,sched,by_origin,views,references=prepare(paths,r);startup=time.monotonic()-start
        payloads=[p.write_or_validate_payload('schedule.json',encoded(sched))]
        engines={'reference':RetainedPredictor(reader),'shared':SharedCurvePredictor(reader)}
        log=p.root/'CURVE_TIMINGS.jsonl';previous=[json.loads(line) for line in log.read_text(encoding='utf-8').splitlines()] if log.exists() else [];timings={(x['origin_epoch'],x['engine']):x for x in previous};totals=Counter();origin_receipts=[]
        for index,origin in enumerate(ORIGINS):
            results={};order=ENGINES if index%2==0 else ENGINES[::-1]
            for engine in order:
                name=f'{engine}_{origin}.json';raw=p.read_verified_payload(name)
                if raw is not None and (origin,engine) in timings:
                    obj=json.loads(raw);payloads.append(p.write_or_validate_payload(name,raw))
                else:
                    predictor=engines[engine];before=len(predictor.cache);already=raw is not None;start=time.monotonic()
                    obj=curve(predictor,origin,by_origin[origin],views,sched,c,references);raw=encoded(obj);payloads.append(p.write_or_validate_payload(name,raw));elapsed=time.monotonic()-start
                    if elapsed>30:raise ValueError('whole_curve_hard_cap_exceeded')
                    timing={'origin_epoch':origin,'engine':engine,'elapsed_seconds':elapsed,'diagnostic_target_met':elapsed<=2,'live_ready':False,'new_model_pairs_loaded':len(predictor.cache)-before,'model_pairs_cached_before':before,'existing_payload_validated_instead_of_new_write':already,'comparison_order':list(order),'prediction_requests':obj['prediction_requests'],'actual_unique_predictions':predictor.predict_calls if engine=='shared' else obj['prediction_requests'],'shared_transform_calls':predictor.transform_calls if engine=='shared' else None,'scope':r['configuration']['measure']}
                    with log.open('ab') as f:f.write(encoded(timing));f.flush();os.fsync(f.fileno())
                    timings[origin,engine]=timing
                results[engine]=obj
            parity(results['reference'],results['shared']);totals['forecasts']+=len(results['shared']['forecasts']);totals['coverage']+=len(results['shared']['coverage']);totals['requests']+=results['shared']['prediction_requests'];totals['unique']+=results['shared']['unique_fit_predictions']
            origin_receipts.append({'origin_epoch':origin,'coverage_rows':len(results['shared']['coverage']),'forecast_values':len(results['shared']['forecasts']),'unique_fit_predictions':results['shared']['unique_fit_predictions'],'reference_shared_parity':True})
            if crash_after==index+1:os._exit(91)
        resources={'startup_seconds':startup,'startup_scope':'authenticated dependencies, prepared feature views, original forecast indexes and schedule; excludes outer operator preflight','origins':[timings[t,e] for t in ORIGINS for e in ENGINES],'live_deadline_guarantee':False}
        prior=p.read_verified_payload('curve_resources.json')
        if prior is not None:resources=json.loads(prior)
        validate_resources(resources)
        report={'status':'completed_whole_curve_characterization','origins':origin_receipts,'forecast_values_per_engine':totals['forecasts'],'coverage_rows_per_engine':totals['coverage'],'prediction_requests_per_engine':totals['requests'],'unique_fit_origin_predictions':totals['unique'],'duplicate_fit_origin_requests_avoided':totals['requests']-totals['unique'],'model_artifacts_reused':112,'models_fitted':0,'reference_shared_parity':True,'new_forecasts_issued':0,'old_forecast_tape_unchanged':True,'engineering_ready':False,'forecast_evidence_status':'retained_value_parity_not_new_forecasts','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False,'next_item':'prequential_endpoint_error_calibration_v2','capacity_results':'see separately authenticated curve_resources.json; timings are measurements not deterministic scientific identity','limitations':['prepared_feature_inputs_not_live_ingestion','native_remaining_target_rebuild_not_implemented_here','same_host_no_OS_load_or_service_guarantee','old_two_second_native_freshness_gate_unchanged','no_parameter_selection_or_market_confirmation']}
        refs={'dependencies':{k:{'run_identity':v['identity']['fingerprint'],'payloads':v['payloads']} for k,v in r['dependencies'].items()},'source_experiments_unchanged':True,'timings_excluded_from_scientific_byte_equality':['curve_resources.json']}
        for name,obj in [('run_report.json',report),('source_references.json',refs),('curve_resources.json',resources)]:payloads.append(p.write_or_validate_payload(name,encoded(obj)))
        if crash_after==0:os._exit(91)
        p.complete(payloads,set(required()))
    except BaseException:
        if p._owner_token is not None:p.release()
        raise
