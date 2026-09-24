"""Cache reconstruction is chronological; original timing receipts are never overwritten."""
import json,math,os,time,uuid
import psutil
from collections import Counter
from publication import RunPublisher,effective_run_identity,verify_completed_run
from curve_capacity_runner_v2 import prepare,curve,parity,ORIGINS,encoded,GROUPS,HORIZONS,PROCEDURES,select_fit
from curve_capacity_predict_v2 import SharedCurvePredictor
from scoped_warm_predict_v2 import ScopedReadyWarmPredictor
ENGINES=('shared','warm')
def required():return sorted([f'{e}_{t}.json' for e in ENGINES for t in ORIGINS]+[f'timing_{t}.json' for t in ORIGINS]+['schedule.json','prewarm_plan.json','run_report.json','source_references.json','startup_resources.json','resource_receipts.json'])
def scientific_names():return [n for n in required() if not n.startswith('timing_') and n not in {'startup_resources.json','resource_receipts.json'}]
def identity_for(r):return effective_run_identity(contract={'recipe':r,'required_payloads':required()},dependency_hashes={**r['sources'],**r['predecessors'],**{k:v['identity']['fingerprint'] for k,v in r['dependencies'].items()}})
def validate_timing(row,origin):
    if row['origin_epoch']!=origin or set(row['engines'])!=set(ENGINES):raise ValueError('warm_timing_identity_mismatch')
    for e,t in row['engines'].items():
        elapsed=t['elapsed_seconds']
        if type(elapsed) not in (int,float) or not math.isfinite(elapsed) or not 0<=elapsed<=30:raise ValueError('warm_origin_resource_limit')
        if t['live_ready'] is not False or t['diagnostic_target_met']!=(elapsed<=2):raise ValueError('warm_timing_cannot_promote')
    value=row['prewarm_elapsed_seconds']
    if type(value) not in (int,float) or not math.isfinite(value) or not 0<=value<=120:raise ValueError('warm_prewarm_resource_limit')
def validate_completed(root):
    read=lambda n:json.loads((root/n).read_bytes())
    for t in ORIGINS:validate_timing(read(f'timing_{t}.json'),t)
    r=read('run_report.json')
    if r['forecast_values_per_engine']!=143904 or r['coverage_rows_per_engine']!=152320 or r['models_fitted']!=0 or r['new_forecasts_issued']!=0:raise ValueError('warm_acceptance_missing')
    validate_resource_receipts(read('resource_receipts.json'))
    s=read('startup_resources.json')['elapsed_seconds']
    if type(s) not in (int,float) or not math.isfinite(s) or not 0<=s<=180:raise ValueError('warm_startup_resource_limit')
def check_resources(root,started,configuration,phase):
    elapsed=time.monotonic()-started
    if not math.isfinite(elapsed) or not 0<=elapsed<=configuration['total_hard_seconds']:raise ValueError('warm_total_resource_limit')
    process=psutil.Process();rss=sum(p.memory_info().rss for p in [process,*process.children(recursive=True)] if p.is_running())
    disk=sum(p.stat().st_size for p in root.rglob('*') if p.is_file())
    if rss>configuration['max_rss_bytes']:raise ValueError('warm_rss_resource_limit')
    # Reserve space for bounded receipts, journal and completion manifest.
    if disk+1024*1024>configuration['max_run_disk_bytes']:raise ValueError('warm_disk_resource_limit')
    return {'phase':phase,'elapsed_seconds':elapsed,'aggregate_rss_bytes':rss,'run_disk_bytes':disk,'process_id':os.getpid()}

def validate_resource_receipts(value):
    if not value['observations'] or value['observations'][-1]['phase']!='final_precommit':raise ValueError('warm_final_resource_receipt_missing')
    for row in value['observations']:
        if not 0<=row['elapsed_seconds']<=900 or not 0<=row['aggregate_rss_bytes']<=1024**3 or not 0<=row['run_disk_bytes']<=2*1024**3:raise ValueError('warm_resource_receipt_out_of_bounds')

def selected_tasks(schedule,origin):
    found={}
    for g in GROUPS:
        for h in HORIZONS:
            for procedure in PROCEDURES:
                task=select_fit(schedule,g,h,procedure,origin)
                if task is not None:found[task['fit_id']]=task
    return [found[k] for k in sorted(found)]
def run(paths,r,runs,*,resume=False,crash_after=None):
    identity=identity_for(r);p=RunPublisher(runs,r['run_id'],identity)
    if (p.root/'COMPLETION_MANIFEST.json').exists():verify_completed_run(p.root,identity);return
    p.acquire(recover=resume);started=time.monotonic();attempt=uuid.uuid4().hex
    def guard(phase):
        row={**check_resources(p.root,started,r['configuration'],phase),'attempt_id':attempt}
        with (p.root/'RESOURCE_ATTEMPTS.jsonl').open('ab') as f:
            f.write(encoded(row));f.flush();os.fsync(f.fileno())
        return row
    try:
        guard('before_prepare')
        reader,c,sched,by_origin,views,refs=prepare(paths,r);startup=time.monotonic()-started
        guard('after_prepare')
        if startup>180:raise ValueError('warm_startup_resource_limit')
        payloads=[p.write_or_validate_payload('schedule.json',encoded(sched))]
        old=p.read_verified_payload('startup_resources.json')
        payloads.append(p.write_or_validate_payload('startup_resources.json',old or encoded({'elapsed_seconds':startup,'live_guarantee':False})))
        engines={'shared':SharedCurvePredictor(reader),'warm':ScopedReadyWarmPredictor(reader)}
        plans=[];totals=Counter()
        for index,origin in enumerate(ORIGINS):
            guard('before_origin_'+str(origin))
            # Always reconstruct every past origin in order after process death. This
            # loads only ready weights, resets numeric caches, and validates old bytes.
            prewarm=engines['warm'].prewarm(selected_tasks(sched,origin),origin-120,origin)
            plans.append({k:v for k,v in prewarm.items() if k!='elapsed_seconds'})
            timing={'origin_epoch':origin,'prewarm_elapsed_seconds':prewarm['elapsed_seconds'],'engines':{}}
            results={};order=ENGINES if index%2==0 else ENGINES[::-1]
            for e in order:
                name=f'{e}_{origin}.json';prior=p.read_verified_payload(name);predictor=engines[e]
                before=sorted(predictor.cache);begin=time.monotonic()
                obj=curve(predictor,origin,by_origin[origin],views,sched,c,refs)
                inventory=predictor.verify_controller_inventory() if e=='warm' else None
                payloads.append(p.write_or_validate_payload(name,encoded(obj)));elapsed=time.monotonic()-begin
                if elapsed>30:raise ValueError('warm_origin_resource_limit')
                timing['engines'][e]={'elapsed_seconds':elapsed,'diagnostic_target_met':elapsed<=2,'live_ready':False,
                  'cache_before':before,'cache_after':sorted(predictor.cache),'cache_available':dict(predictor.cache_available) if e=='warm' else None,
                  'payload_action':'validated_existing' if prior is not None else 'newly_written','controller_inventory':inventory}
                results[e]=obj
            parity(results['shared'],results['warm']);totals['values']+=len(results['warm']['forecasts']);totals['coverage']+=len(results['warm']['coverage'])
            old=p.read_verified_payload(f'timing_{origin}.json')
            if old is not None:
                retained=json.loads(old);validate_timing(retained,origin)
                for e in ENGINES:
                    for k in ('cache_before','cache_after','cache_available'):
                        if retained['engines'][e][k]!=timing['engines'][e][k]:raise ValueError('warm_reconstructed_cache_state_mismatch')
                payloads.append(p.write_or_validate_payload(f'timing_{origin}.json',old))
            else:
                validate_timing(timing,origin);payloads.append(p.write_or_validate_payload(f'timing_{origin}.json',encoded(timing)))
            with (p.root/'RECONSTRUCTION_LOG.jsonl').open('ab') as f:
                f.write(encoded({'process_id':os.getpid(),'origin_epoch':origin,'existing_timing_preserved':old is not None,'cache_state_verified':True,'new_or_validated_payloads':{e:timing['engines'][e]['payload_action'] for e in ENGINES}}));f.flush();os.fsync(f.fileno())
            guard('after_origin_'+str(origin))
            if crash_after==index+1:os._exit(91)
        report={'status':'completed_warm_curve_characterization','forecast_values_per_engine':totals['values'],'coverage_rows_per_engine':totals['coverage'],
          'models_fitted':0,'new_forecasts_issued':0,'engineering_ready':False,'forecast_evidence_status':'retained_value_parity_not_new_forecasts',
          'policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False,
          'original_values_selection_and_coverage_preserved':True,'live_deadline_guarantee':False,
          'limitations':['same_host_OS_cache_shared','modeled_reserved_prewarm_not_historical_availability','prepared_inputs_not_live_ingestion','native_two_second_gate_not_changed']}
        for name,obj in [('prewarm_plan.json',plans),('run_report.json',report),('source_references.json',{'dependencies':r['dependencies'],'prototype_identity':r['configuration']['original_prototype_identity']})]:payloads.append(p.write_or_validate_payload(name,encoded(obj)))
        if crash_after==0:os._exit(91)
        guard('final_precommit')
        receipts={'scope':'sampled_phase_boundaries_not_continuous_peak_or_OS_quota','observations':[json.loads(line) for line in (p.root/'RESOURCE_ATTEMPTS.jsonl').read_text().splitlines()]}
        validate_resource_receipts(receipts)
        old=p.read_verified_payload('resource_receipts.json')
        payloads.append(p.write_or_validate_payload('resource_receipts.json',old or encoded(receipts)))
        check_resources(p.root,started,r['configuration'],'before_manifest')
        p.complete(payloads,set(required()));validate_completed(p.root)
    except BaseException:
        if p._owner_token is not None:p.release()
        raise
