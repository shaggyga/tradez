"""Bounded read-only operational snapshots; no HTTP, broker, imports with I/O, or DB scans."""
from collections import Counter
from datetime import datetime, timezone
import csv
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
import time

ROOT=Path('C:/Users/zmoor/Documents/forex/trad')
DATA=ROOT/'data/oanda_training_manager'
OUT=Path(__file__).parent
sys.path.insert(0,str(ROOT))
from oanda_project_runtime_health import read_supervisor_observation


def sha(raw): return hashlib.sha256(raw).hexdigest()
def canonical(value): return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def epoch(value):
    try:
        if type(value) in (int,float): return float(value) if math.isfinite(value) else None
        parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
        return parsed.timestamp() if parsed.tzinfo is not None else None
    except (ValueError,TypeError,AttributeError): return None
def age(value,now):
    number=epoch(value)
    return None if number is None else now-number
def compact(value,keys): return {k:value[k] for k in keys if k in value}
def code(value):
    if value in (None,''):return value
    return value if isinstance(value,str) and re.fullmatch(r'[A-Za-z0-9_:<>=.,+/-]{1,180}',value) else 'noncode_text_omitted'
def read(path,limit=2*1024*1024):
    started=time.time()
    with path.open('rb') as f:
        before=os.fstat(f.fileno());raw=f.read(limit+1);after=os.fstat(f.fileno())
    observed=time.time()
    assert len(raw)<=limit,'byte_bound'
    assert (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)==(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns),'changed_during_read'
    return json.loads(raw),{'path':str(path),'sha256':sha(raw),'bytes':len(raw),'read_started_epoch':started,'read_completed_epoch':observed}
def artifact(path):
    raw=path.read_bytes();return {'path':str(path),'sha256':sha(raw),'bytes':len(raw)}
def file_json(path,keys):
    try:
        value,meta=read(path)
        kept=compact(value,keys)
        if 'last_error' in kept:kept['last_error']=code(kept['last_error'])
        return {**meta,'value':kept,'generated_age_sec':age(value.get('generated_epoch',value.get('generated_utc',value.get('heartbeat_utc'))),meta['read_completed_epoch'])}
    except Exception as exc:return {'path':str(path),'error_type':type(exc).__name__,'observed_epoch':time.time()}
def binding_check(name):
    value,meta=read(ROOT/'config'/name)
    return value,{**meta,'schema_version':value['schema_version'],
        'controls':compact(value,['research_only','can_place_orders','can_promote','can_authorize','execution_eligible','account_eligible','proof_eligible','orders_enabled','historical_rows_imported']),
        'files':{key:{'expected_sha256':expected,'actual_sha256':sha((ROOT/key).read_bytes())}
            for key,expected in value['source_bindings'].items()}}


def study(registry,registry_sha):
    directory=DATA/'joint_price_news_study_v3'
    first,sm=read(directory/'summary.json')
    hb,hm=read(directory/'heartbeat.json')
    second,sm2=read(directory/'summary.json')
    now=sm2['read_completed_epoch']
    full=sha(canonical(first))
    reasons=[]
    if full!=sha(canonical(second)):reasons.append('summary_replaced_during_bracket')
    if first['payload_sha256']!=sha(canonical({k:v for k,v in first.items() if k!='payload_sha256'})):reasons.append('summary_payload_seal')
    if hb.get('summary_sha256')!=full:reasons.append('summary_heartbeat_generation_mismatch')
    if first.get('registry_sha256')!=registry_sha or hb.get('registry_sha256')!=registry_sha:reasons.append('registry_identity_mismatch')
    for source in (first,hb):
        if not 0<=age(source['generated_epoch'],now)<=90:reasons.append('stale_or_future_source')
        if source.get('research_only') is not True or any(source.get(key) is not False for key in ('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible','historical_rows_imported')):reasons.append('authority_mismatch')
    rows=[];totals=Counter();valid=0;status=Counter();readiness=Counter()
    for row in first['rows']:
        pair=row['instrument'];slot=row['families']['ridge_price_news_v1'];ready=slot.get('current_readiness') or {}
        forecast=slot.get('latest_forecast');errors=[]
        active=False
        if forecast:
            try:
                if not (forecast['publication_verified'] is True and forecast['consumption_verified'] is True):errors.append('worker_verification_absent')
                if not (forecast['instrument']==pair and forecast['family']=='ridge_price_news_v1' and len(forecast['forecasts'])==1):errors.append('pair_family_mismatch')
                clocks=[forecast[k] for k in ('reference_epoch','reference_available_epoch','issued_epoch','publication_epoch','consumption_epoch')]+[slot['observed_epoch'],first['generated_epoch'],now]
                if not all(type(v) in (int,float) and math.isfinite(v) for v in clocks) or clocks!=sorted(clocks):errors.append('publication_clock_order')
                if forecast['target_epoch']!=forecast['reference_epoch']+3600:errors.append('original_h1_target_changed')
                if not 0<=now-slot['observed_epoch']<=90:errors.append('worker_row_verification_stale')
                active=not errors and forecast['target_epoch']>now
            except (KeyError,TypeError):errors.append('forecast_shape')
        valid+=active;totals.update(slot.get('counts') or {});status[slot['status']]+=1;readiness[code(ready.get('reason')) or ready.get('status','unknown')]+=1
        rows.append({'instrument':pair,'status':slot['status'],'reason':code(slot.get('reason')),'observed_epoch':slot['observed_epoch'],
            'current_readiness':{**compact(ready,['status','observed_epoch','price_bar_close_epoch','news_evidence_epoch','news_expires_epoch']),
                'reason':code(ready.get('reason')),'diagnostics':compact(ready.get('diagnostics') or {},['ready','current_real_prices','current_real_returns','mature_exact_h1_training_rows','required_ridge_training_rows','training_rows','training_news_nonzero_rows'])},
            'last_attempt':{**compact(slot.get('last_attempt') or {},['epoch','attempt_id','bucket']),'reason':code((slot.get('last_attempt') or {}).get('reason'))},
            'ledger_counts':slot.get('counts'),'active_original_h1':active,'forecast_check_reasons':errors,
            'latest_forecast':compact(forecast or {},['decision_id','reference_epoch','reference_available_epoch','issued_epoch','publication_epoch','consumption_epoch','target_epoch','forecast_sha256','publication_receipt_sha256','consumer_receipt_sha256'])})
    return {'status':'coherent_current' if not reasons else 'unavailable','reasons':reasons,'summary':sm,'heartbeat':hm,'summary_after':sm2,
        'summary_generated_epoch':first['generated_epoch'],'heartbeat_generated_epoch':hb['generated_epoch'],
        'sealed_summary_sha256':full,'heartbeat_bound_summary_sha256':hb['summary_sha256'],'observed_epoch':now,
        'summary_age_sec':now-first['generated_epoch'],'heartbeat_age_sec':now-hb['generated_epoch'],
        'producer_status_counts':dict(status),'original_h1_active_pair_count_from_retained_rows':valid,
        'counts_verified_as_current_projection':not reasons,'readiness_reason_counts':dict(readiness),
        'summed_worker_verified_ledger_counts':dict(totals),'rows':rows,
        'scope':'Snapshot seal, bracket, worker-reported verification and original target rechecked; this audit did not independently reopen each ledger.'}


def quotes_and_archive():
    value,meta=read(DATA/'state/practice_007_market_quotes_v1.json',256*1024)
    now=meta['read_completed_epoch'];retained=set(value.get('coverage',{}).get('retained_last_known_instruments') or [])
    rows=[]
    for pair,quote in value['quotes'].items():
        qage=age(quote.get('time'),now)
        status='current' if quote.get('tradeable') is True and quote.get('source')=='stream' and pair not in retained and qage is not None and 0<=qage<=60 else 'withheld'
        row={'instrument':pair,'tradeable':quote.get('tradeable'),'quote_source':quote.get('source'),'quote_market_epoch':epoch(quote.get('time')),
            'quote_age_sec':qage,'retained_only':pair in retained,'current_within60s':status=='current'}
        path=DATA/'candles'/(pair+'_M1.csv')
        try:
            with path.open('rb') as f:
                before=os.fstat(f.fileno());header=f.readline(8192);offset=max(len(header),before.st_size-8192);f.seek(offset);raw=f.read(8193);after=os.fstat(f.fileno())
            assert before.st_size==after.st_size and before.st_mtime_ns==after.st_mtime_ns,'changed_during_read'
            assert len(raw)<=8192,'tail_bound'
            if offset>len(header):raw=raw.split(b'\n',1)[1]
            if not raw.endswith(b'\n'):raw=raw[:raw.rfind(b'\n')+1]
            bars=list(csv.DictReader(io.StringIO((header+raw).decode())))
            original=bars[-1];close=epoch(original['time'])+60;observed=time.time()
            assert original['instrument']==pair and original['granularity']=='M1'
            row['archive']={'original_last_bar_label':original['time'],'last_price_close_epoch':close,'observed_epoch':observed,
                'price_age_sec':observed-close,'lag_behind_quote_sec':row['quote_market_epoch']-close if row['quote_market_epoch'] else None,
                'retained_tail_sha256':sha(header+raw),'tail_bytes':len(raw),'source_size_bytes':before.st_size}
        except Exception as exc:row['archive']={'error_type':type(exc).__name__}
        rows.append(row)
    return {**meta,'header':compact(value,['schema_version','producer','generated_utc','research_only','connection_generation','quote_count']),
        'coverage':compact(value.get('coverage',{}),['retained_last_known_instruments','tradeability_contract','current_connection_generation_instruments','market_closed_instruments']),
        'wrapper_age_sec':age(value['generated_utc'],now),'current_quote_count':sum(r['current_within60s'] for r in rows),
        'nontradeable_pairs':[r['instrument'] for r in rows if r['tradeable'] is False],
        'quote_age_summary_sec':distribution([r['quote_age_sec'] for r in rows if r['current_within60s']]),
        'current_quote_archive_age_summary_sec':distribution([r['archive']['price_age_sec'] for r in rows if r['current_within60s'] and 'price_age_sec' in r['archive']]),
        'current_quote_archive_lag_summary_sec':distribution([r['archive']['lag_behind_quote_sec'] for r in rows if r['current_within60s'] and r['archive'].get('lag_behind_quote_sec') is not None]),'rows':rows}
def distribution(values):
    return {'count':len(values),'min':min(values),'median':statistics.median(values),'max':max(values)} if values else {'count':0}


def pilot(registry):
    root=Path(registry['output_root']);paths=sorted((root/'cycles').glob('*/cycle_completed.json'))
    assert len(paths)<=200,'pilot_cycle_inventory_bound'
    counts=Counter();reasons=Counter();cycles=[]
    for path in paths:
        value,meta=read(path,128*1024)
        attempts=[attempt for pair in value['pairs'] for attempt in pair['attempts']]
        for attempt in attempts:
            counts['attempts']+=1;counts['native_nodes_in_issued_curves']+=attempt.get('native_node_count',0) if attempt.get('registry_issue_admitted') else 0
            counts['candidate_observations']+=attempt.get('candidate_count',0)
            if attempt.get('reason_code'):reasons[code(attempt['reason_code'])]+=1
        for pair in value['pairs']:
            if pair.get('reason_code'):reasons[code(pair['reason_code'])]+=1
        for key in ('curves_issued','curves_published','curves_consumed','skipped_prior_schedule_slots'):counts[key]+=value[key]
        cycles.append({'cycle_id':value['cycle_id'],'source_sha256':meta['sha256'],**compact(value,['started_epoch','completed_epoch','scheduled_cycle_epoch','start_delay_sec','curves_issued','curves_published','curves_consumed','skipped_prior_schedule_slots']),
            'pairs':[{'instrument':p['instrument'],'status':p['status'],'reason_code':code(p.get('reason_code')),
                'attempts':[compact(a,['price_convention','status','reason_code','reference_epoch','issued_epoch','completed_epoch','native_node_count','candidate_count']) for a in p['attempts']]} for p in value['pairs']]})
    sessions=[]
    for path in sorted((root/'sessions').glob('*/started.json'))[-8:]:
        value,meta=read(path)
        sessions.append({**meta,'value':compact(value,['session_id','pid','started_epoch','registry_sha256','once']),
            'stop_record_exists':(path.parent/'stopped.json').exists()})
    now=time.time()
    last10=[]
    for path in paths[-10:]:
        value,_=read(path,128*1024)
        for pair in value['pairs']:
            for attempt in pair['attempts']:
                mapping_path=path.parent/pair['instrument'].lower()/attempt['price_convention']/'source_mapping.json'
                mapping,meta=read(mapping_path,256*1024)
                feature_rows=mapping['last13_feature_rows'];labels=[row['bar_start_epoch'] for row in feature_rows]
                steps=[b-a for a,b in zip(labels,labels[1:])]
                last10.append({'cycle_id':value['cycle_id'],'instrument':pair['instrument'],'price_convention':attempt['price_convention'],
                    'capture_status':pair['status'],'attempt_status':attempt['status'],'reason_code':code(attempt.get('reason_code')),
                    'mapping_sha256':meta['sha256'],'raw_source_sha256':mapping['source_sha256'],
                    'mapped_complete_rows':len(mapping['complete_rows']),'mapped_incomplete_rows':len(mapping['incomplete_rows']),
                    'selected_real_row_count':len(labels),'first_selected_bar_label_epoch':labels[0] if labels else None,
                    'last_selected_bar_label_epoch':labels[-1] if labels else None,'original_endpoint_span_sec':labels[-1]-labels[0] if labels else None,
                    'step_distribution_sec':dict(Counter(steps)),'maximum_internal_gap_sec':max(steps) if steps else None,
                    'first_observed_epoch':mapping['first_observed_epoch'],'pair_source_original_observation_age_at_attempt':attempt['started_epoch']-mapping['first_observed_epoch'],
                    'scope':'Same existing complete source rows; source omission/elapsed-span evidence, not provider continuity guarantee.'})
    return {'observed_epoch':now,'completed_cycle_count':len(paths),'all_completed_cycles_within_bound_included':True,
        'aggregate':dict(counts),'reason_counts':dict(reasons),'last_completed_age_sec':now-cycles[-1]['completed_epoch'] if cycles else None,
        'cycle_sec':registry['cycle_sec'],'issue_cutoff_epoch':registry['issue_cutoff_epoch'],'collection_stop_epoch':registry['collection_stop_epoch'],
        'sessions':sessions,'cycles':cycles,'last10_cycle_selected_source_spans':last10,
        'scope':'Publication/candidate observations only; no fill or forecast accuracy inferred.'}


def readiness_tail():
    path=DATA/'joint_price_news_study_v3/readiness.jsonl'
    with path.open('rb') as f:
        size=os.fstat(f.fileno()).st_size;offset=max(0,size-128*1024);f.seek(offset);raw=f.read(size-offset)
    if offset:raw=raw.split(b'\n',1)[1]
    if not raw.endswith(b'\n'):raw=raw[:raw.rfind(b'\n')+1]
    rows=[json.loads(line) for line in raw.splitlines()]
    kept=[{**compact(row,['event_observed_epoch','instrument','observed_epoch','status','source_capture_sha256']),
        'reasons':[code(reason) for reason in row.get('reasons',[])]} for row in rows]
    return {'path':str(path),'observed_epoch':time.time(),'retained_tail_sha256':sha(raw),'retained_rows':len(kept),
        'rows':kept,'status_counts':dict(Counter(row['status'] for row in kept)),
        'scope':'Most recent bounded journal suffix only; no whole-lifetime failure denominator.'}


def os_processes():
    script=r'''$rows = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -match '^(python|pythonw|pwsh|powershell)(\.exe)?$' } | ForEach-Object { $m=[regex]::Match([string]$_.CommandLine,'(?i)(?:"([^"]+\.(?:py|ps1))"|([^\s"]+\.(?:py|ps1)))(?:\s|$)'); $scriptPath=if($m.Groups[1].Success){$m.Groups[1].Value}else{$m.Groups[2].Value}; if($scriptPath -like 'C:\Users\zmoor\Documents\forex\trad\*'){ [pscustomobject]@{pid=$_.ProcessId;parent_pid=$_.ParentProcessId;process_name=$_.Name;executable=$_.ExecutablePath;created_utc=$_.CreationDate.ToUniversalTime().ToString('o');script_path=$scriptPath} } }); ConvertTo-Json -InputObject $rows -Depth 4 -Compress'''
    result=subprocess.run(['powershell','-NoProfile','-Command',script],capture_output=True,text=True,encoding='utf-8',timeout=20,check=True)
    return {'observed_epoch':time.time(),'processes':json.loads(result.stdout or '[]'),
        'privacy':'Command arguments are not retained; only canonical script path, executable, process and parent identities.'}


def main(label):
    started=time.time()
    v3,bindings_v3=binding_check('joint_price_news_study_v3_20260908.json')
    preg,bindings_pilot=binding_check('recovered_second_curve_pilot_v1_20260909.json')
    registrysha=sha(canonical(v3))
    supervisor=read_supervisor_observation(DATA/'logs')
    news_keys=['schema_version','status','generated_epoch','generated_utc','heartbeat_utc','last_progress_utc','progress_sequence',
        'phase','phase_age_sec','progress_age_sec','cycle_started_utc','cycle_in_progress','classification_version','source_status',
        'errors','last_error','publication_epoch','snapshot_generated_utc','snapshot_sha256','counts','active_pair','pairs_with_forecast','heartbeat_publication_errors',
        'research_only','can_place_orders','can_promote','can_authorize','execution_eligible','account_eligible','proof_eligible','supported_decision']
    heartbeats={name:file_json(DATA/path,news_keys) for name,path in {
        'repaired_news':'local_news_sentiment_repair_v1/heartbeat.json',
        'news_collector':'local_news_sentiment/collector_heartbeat_v1.json',
        'joint_v3':'joint_price_news_study_v3/heartbeat.json',
        'quote_stream':'state/practice_007_quote_stream_heartbeat_v1.json'}.items()}
    heartbeats['archive_m1']=file_json(DATA/'state/all68_m1_forward_update_heartbeat_v1.json',
        ['schema_version','updated_at','phase','phase_age_sec','progress_age_sec','progress_sequence','progress_updated_at','status','started_at','role','details'])
    account,account_meta=read(DATA/'state/account_007_dashboard_v1.json',64*1024)
    aggregate=account['aggregate']
    integrity,integrity_meta=read(DATA/'state/project_integrity_audit_v1.json',8*1024*1024)
    result={'schema_version':'overnight_operations_observation_v1_20260909','label':label,'started_epoch':started,
        'collector_source':artifact(Path(__file__)),'runtime_health_source':artifact(ROOT/'oanda_project_runtime_health.py'),
        'frozen_registries':{'joint_v3':bindings_v3,'pilot':bindings_pilot},'supervisor':supervisor,
        'joint_study':study(v3,registrysha),'heartbeats':heartbeats,'quotes_and_archive':quotes_and_archive(),
        'pilot':pilot(preg),'os':os_processes(),'joint_readiness_tail':readiness_tail(),
        'account_observation':{**account_meta,'time':account['time'],'generated_age_sec':age(account['time'],time.time()),
            'aggregate':compact(aggregate,['account_values_current','positions_current','orders_current','openTradeCount','pendingOrderCount','snapshot_state','ok_count']),
            'position_count':sum(len(row.get('trades') or []) for row in account['accounts']),
            'scope':'Counts and observation state only; no account identifiers, credentials, NAV or trade payload retained.'},
        'integrity':{**integrity_meta,**compact(integrity,['schema_version','status','generated_utc','audit_started_utc','audit_finished_utc','publication_status','snapshot_fresh_at_publication','failures','inactive_component_failures','active_shared_or_unknown_failures','live_runtime_assertions','research_only','can_place_orders','can_promote','supported_decision']),
            'age_sec':age(integrity['generated_utc'],time.time()),'runtime_health_summary':compact(integrity.get('runtime_health') or {},['status','reasons','running_worker_count','expected_worker_count']),
            'failed_scope_rows':{key:value for key,value in integrity.get('check_scopes',{}).items() if value.get('artifact_check_passed') is False}},
        'scope':'Bounded local-file/process observation; no HTTP request, broker request, model fit, worker start, source edit or database scan.'}
    result['finished_epoch']=time.time()
    result['all_frozen_sources_match']=all(r['expected_sha256']==r['actual_sha256'] for binding in result['frozen_registries'].values() for r in binding['files'].values())
    path=OUT/('OPERATIONS_'+label.upper()+'_20260909.json')
    with path.open('x',encoding='utf-8') as stream:json.dump(result,stream,sort_keys=True,indent=2,allow_nan=False);stream.write('\n')
    print(json.dumps({'artifact':artifact(path),'supervisor':compact(supervisor,['status','reasons','running_worker_count','expected_worker_count']),
        'joint':compact(result['joint_study'],['status','reasons','producer_status_counts','original_h1_active_pair_count_from_retained_rows','readiness_reason_counts','summed_worker_verified_ledger_counts']),
        'quote_count':result['quotes_and_archive']['current_quote_count'],'archive_age':result['quotes_and_archive']['current_quote_archive_age_summary_sec'],
        'pilot':compact(result['pilot'],['completed_cycle_count','aggregate','reason_counts','last_completed_age_sec']),
        'integrity':compact(result['integrity'],['status','age_sec','active_shared_or_unknown_failures','inactive_component_failures']),
        'frozen_sources_match':result['all_frozen_sources_match']},indent=2))


if __name__=='__main__':main(sys.argv[1])
