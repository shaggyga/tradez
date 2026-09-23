"""One bounded read-only collection baseline; no broker call or watcher."""
from pathlib import Path
from datetime import datetime,timezone
from collections import Counter
import hashlib,importlib.util,json,sys,time
sys.dont_write_bytecode=True
OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
DATA=ROOT/'data/oanda_training_manager'
sys.path[:0]=[str(ROOT),str(ROOT.parent)]
from oanda_causal_forecast_inputs import _read_tail
from oanda_causal_forecast_inputs_gap_v2 import _parse_exact_source
def iso(t):return datetime.fromtimestamp(t,timezone.utc).isoformat()
def epoch(v):
    if isinstance(v,(int,float)):return float(v)
    if isinstance(v,str):return datetime.fromisoformat(v.replace('Z','+00:00')).timestamp()
    return None
def sha(raw):return hashlib.sha256(raw).hexdigest()
def snapshot(path):
    raw=path.read_bytes()
    if len(raw)>1024*1024:raise ValueError('bounded_json_exceeded')
    return json.loads(raw),{'path':str(path),'sha256':sha(raw),'bytes':len(raw),'observed_epoch':time.time()}

def main():
    begun=time.time()
    spec=importlib.util.spec_from_file_location('readonly_process_check',OUT.parent/'pair_dashboard_consistency_20260907/verify_runtime.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    processes=module.processes()
    try:process_check={'status':'passed',**module.verify_processes(processes)}
    except Exception as exc:process_check={'status':'failed','reason':type(exc).__name__+':'+str(exc)}
    prior=json.loads((OUT.parent/'pair_dashboard_consistency_20260907/LATEST_RUNTIME_AFTER_OPERATIONAL_REPAIR_20260907.json').read_text())
    stable=lambda rows:sorted([(p['script'],p['pid'],p['parent_pid'],p['created_utc']) for p in rows if p['script'] in module.WORKERS|{module.SUPERVISOR}])
    process_check['same_process_identities_as_185143']=stable(processes)==stable(prior['processes'])
    heartbeat_paths={
      'account_snapshot':'state/account_007_dashboard_v1.json',
      'local_news_collector':'local_news_sentiment/collector_heartbeat_v1.json',
      'official_release_lane':'local_news_sentiment/official_release_fast_lane_heartbeat_v4.json',
      'official_release_mapper':'local_news_sentiment/official_release_fast_mapping_heartbeat_v3.json',
      'news_governance':'state/source_governance_news_fast_lane_v3.progress.json',
      'quote_stream':'state/practice_007_quote_stream_heartbeat_v1.json',
      'clock_monitor':'state/clock_integrity_v1.json',
      'minute_updater':'state/all68_m1_forward_update_heartbeat_v1.json',
      'project_integrity':'state/project_integrity_audit_v1.json',
      'storage_guard':'state/storage_headroom_v1.json',
      'gap_companion':'causal_forecast_study_gap_v2/heartbeat.json',
      'eurusd_companion':'causal_forecast_study_eurusd_v1/heartbeat.json',
      'pair_worker':'pair_local_forecast_study_v1/heartbeat.json'}
    allowed={'schema_version','status','phase','generated_epoch','generated_utc','updated_at','heartbeat_utc','time',
       'pid','progress_sequence','progress_age_sec','phase_age_sec','cycle_in_progress','last_progress_utc',
       'errors','heartbeat_publication_errors','last_error','last_reason','last_heartbeat_publication_error',
       'can_place_orders','can_promote','research_only','clock_sources_consistent','host_clock_synchronized',
       'clock_discontinuity_active','clock_discontinuity_detected','timestamp_normalization_trusted','reasons',
       'failures','snapshot_fresh_at_publication','publication_status'}
    hearts={}
    for name,relative in heartbeat_paths.items():
        try:
            value,binding=snapshot(DATA/relative)
            compact={k:v for k,v in value.items() if k in allowed}
            clock=next((value[k] for k in ('updated_at','heartbeat_utc','generated_epoch','generated_utc','time') if value.get(k) is not None),None)
            observed=binding['observed_epoch']
            compact['age_seconds']=observed-epoch(clock) if clock is not None else None
            details=value.get('details',{})
            compact['details']={k:v for k,v in details.items() if any(s in k.lower() for s in ('error','progress','completed','count','instrument','recovered','appended','sleep','status')) and len(str(v))<700}
            hearts[name]={'read_status':'read',**binding,'state':compact}
        except Exception as exc:hearts[name]={'read_status':'failed','reason':type(exc).__name__+':'+str(exc)}
    cycle,cycle_binding=snapshot(DATA/'state/all68_m1_forward_update_v1.json')
    receipt_rows=[];archive_rows=[]
    for pair in cycle['pairs']:
        instrument=pair['instrument'];gap=pair.get('gap_recovery',{})
        row={'instrument':instrument,'cycle_before_last':pair.get('before_last'),'cycle_after_last':pair.get('after_last'),
             'rows_appended':pair.get('rows_appended'),'error':pair.get('error'),
             'gap_status':gap.get('status'),'unresolved_recent_small_minutes':gap.get('unresolved_minutes'),
             'gap_requests':gap.get('requests'),'gap_error':gap.get('error')}
        try:
            capture=_read_tail(DATA/'candles'/f'{instrument}_M1.csv');observed=time.time()
            rows=_parse_exact_source(capture,instrument,observed)
            latest=int(max(rows));suffix=0
            while latest-suffix*60 in rows:suffix+=1
            missing=[iso(t) for t in range(latest-3600,latest+1,60) if t not in rows]
            row.update({'read_status':'coherent','observed_epoch':observed,'latest_minute_start_utc':iso(latest),
                'latest_real_close_age_seconds':observed-latest-60,'own_consecutive_real_bars':suffix,
                'present_of_last61_calendar_slots':61-len(missing),'missing_last61_minute_starts_utc':missing,
                'retained_tail_sha256':sha(capture['tail_base64'].encode()),'hash_scope':'encoded frozen-reader tail bytes; raw bytes not copied'})
        except Exception as exc:row.update({'read_status':'failed','reason':type(exc).__name__+':'+str(exc)})
        archive_rows.append(row)
        if gap.get('observation_receipt'):
            try:
                retained,binding=snapshot(Path(gap['observation_receipt']))
                response=retained['response']
                actual=sha(json.dumps(response,sort_keys=True,separators=(',',':'),allow_nan=False).encode())
                requested=retained['candle_epoch']
                returned=[epoch(c['time']) for c in response.get('candles',[])]
                receipt_rows.append({**binding,'instrument':instrument,'requested_minute_utc':iso(requested),
                   'response_observed_utc':retained['response_observed_utc'],'response_hash_matches':actual==retained['response_sha256'],
                   'broker_error':bool(response.get('_error')),'requested_minute_returned':requested in returned,
                   'returned_candle_count':len(returned),'returned_minute_utc':[iso(t) for t in returned] if instrument=='EUR_USD' else None})
            except Exception as exc:receipt_rows.append({'instrument':instrument,'read_status':'failed','reason':type(exc).__name__+':'+str(exc)})
    result={'schema':'forex_live_hour_collection_baseline_v1_20260907','observed_utc':iso(time.time()),
       'started_utc':iso(begun),'duration_seconds':time.time()-begun,'scope':'One read-only process/heartbeat/minute-collection snapshot; no ledger or scorecard queries.',
       'process_check':process_check,'processes':processes,'worker_heartbeats_and_publications':hearts,
       'updater_cycle':{**cycle_binding,'summary':{k:v for k,v in cycle.items() if k not in ('pairs','source')}},
       'archive_pairs':archive_rows,'cycle_gap_status_counts':dict(Counter(p['gap_status'] for p in archive_rows)),
       'retained_latest_recovery_receipts':receipt_rows,
       'recovery_receipt_checks':{'count':len(receipt_rows),'hash_mismatches':sum(r.get('response_hash_matches') is False for r in receipt_rows),
          'requested_minutes_absent':sum(r.get('requested_minute_returned') is False and not r.get('broker_error') for r in receipt_rows),
          'requested_minutes_returned':sum(r.get('requested_minute_returned') is True for r in receipt_rows),
          'read_failures':sum(r.get('read_status')=='failed' for r in receipt_rows)},
       'archive_summary':{'read_failures':sum(r['read_status']!='coherent' for r in archive_rows),
          'pairs_with_at_least61_consecutive_real_bars':sum(r.get('own_consecutive_real_bars',0)>=61 for r in archive_rows),
          'latest_close_age_over600_seconds_pairs':[r['instrument'] for r in archive_rows if r.get('latest_real_close_age_seconds',0)>600]},
       'watch':['Worker identity loss/restart, stale liveness clocks, or rising progress age during an active phase.',
          'Minute updater should advance completed cycles on its configured 300-second cadence; sleeping with a fresh heartbeat is expected.',
          'Differentiate recent-small-gap count from full archive completeness and from collector errors.',
          'Compare EURUSD and pair-worker error counters; the original EUR scorer duplicate-reference failure is a retained known issue.',
          '61 consecutive real bars is only an input continuity condition, not complete forecast readiness or trade authorization.'],
       'limitations':['Latest stored broker recovery responses establish omission only for their named requested minute and observation time; no fresh broker call was made.',
          'An archive gap without its own verified omission receipt is not independently classified as a provider omission here.',
          'A fresh process/heartbeat is not evidence of predictive success; performance is audited by the separate ledger baseline.'],
       'runtime_changes':False,'source_docs_vault_writes':False,'broker_requests':0,'orders':0,'watchers_started':0}
    target=OUT/'COLLECTION_OPERATIONAL_BASELINE_VERIFIED_20260907.json'
    with target.open('x',encoding='utf-8') as handle:json.dump(result,handle,indent=2,allow_nan=False)
    print(json.dumps({'path':str(target),'sha256':sha(target.read_bytes()),'observed_utc':result['observed_utc'],
        'process_check':process_check,'cycle_summary':result['updater_cycle']['summary'],
        'gap_status_counts':result['cycle_gap_status_counts'],'receipt_checks':result['recovery_receipt_checks'],
        'archive_summary':result['archive_summary'],'eurusd':next(r for r in archive_rows if r['instrument']=='EUR_USD'),
        'heartbeats':{k:{f:r.get('state',{}).get(f) for f in ('age_seconds','status','phase','errors','heartbeat_publication_errors')} for k,r in hearts.items()}},indent=2))

if __name__=='__main__':main()
