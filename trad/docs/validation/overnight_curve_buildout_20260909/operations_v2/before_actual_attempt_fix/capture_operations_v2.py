"""Bounded operational evidence only: no GET, accuracy scoring or runtime writes."""
from collections import Counter
from datetime import datetime,timezone
import argparse
import csv
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import stat
import sys
import time
import zlib

sys.dont_write_bytecode=True
BASE=Path(__file__).resolve().parent
ROOT=Path(r'C:/Users/zmoor/Documents/forex/trad');DATA=ROOT/'data/oanda_training_manager'
OLD=BASE.parent/'operations_0551/capture_operations.py'
OLD_SHA='c239a284c2ca39a7308a489651da0d7ef5b469d11d1d6fb603bc2c01b3548886'
HEALTH_SHA='d9498a2b42ff9cde525fb9e4cc9f14e5729e8bca5f512003b69f000fbefe3eda'
QUOTE_CONTRACT_SHA='ba18acd7eeac0eaa7c5cd7f8d4505fa5494c4ad3398345f009d25670169a27ef'
OBSERVER_PINS={'oanda_joint_v3_ledger_status_observer_v1.py':'d2dc17dfee52603ae1ccf61433f4a1a73cd1650fdf44c5f1daaa60cf133d8699',
 'oanda_immutable_summary_publication_v1.py':'53abafc379216797945543464fe218511f53b17a2d0d9d9cecb700146eff4bf7'}
ACTIVATION_SHA='e3e8de4f4d1d9dcd4a2daed4cfa30a03f176fa95d0380818f87ea521766597dd'
REGISTRIES={
 'joint_v3':('joint_price_news_study_v3_20260908.json','ee075e69e80ca56dfdf45abe1f7a1a301612176e67f7622eab1adf2bd9af8771',20),
 'pilot':('recovered_second_curve_pilot_v1_20260909.json','ae64f2cae6df44dad81b46deb16e95e4d5f67dabb7fc1e65289b96ba6b0fe2b2',14),
 'paper':('observed_curve_management_v1_20260909.json','60689101fb63a8bcce2465d19f627b9b2ce357d1ee271c65298f255663d991e1',20),
 'risk':('m1_risk_distributions_v1_20260909.json','855c3bb96105c0cf0943ebbb5d28e6d10b6b9bf718573883f897161d08453674',8)}
PILOT_CYCLE_CAP=600;RISK_CYCLE_CAP=512;NEWS_COMPRESSED_CAP=8*1024*1024;NEWS_DECODED_CAP=16*1024*1024
FLAGS=('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible')
PAIRS=('EUR_USD','GBP_USD','USD_JPY')


def need(ok,reason):
    if not ok:raise ValueError(reason)


def sha(raw):return hashlib.sha256(raw).hexdigest()
def code(value):return value if type(value) is str and re.fullmatch(r'[A-Za-z0-9_:<>=.,+/-]{1,180}',value) else None
def compact(value,keys):return {key:value[key] for key in keys if key in value}


def safe(path):
    path=Path(path).absolute();need('..' not in path.parts,'path_escape')
    for part in reversed([path,*path.parents]):
        info=part.lstat();need(not stat.S_ISLNK(info.st_mode) and not getattr(info,'st_file_attributes',0)&1024,'reparse_path')
    return path


def raw_read(path,limit=2*1024*1024):
    path=safe(path);start=time.time()
    with path.open('rb') as stream:
        before=os.fstat(stream.fileno());raw=stream.read(limit+1);after=os.fstat(stream.fileno())
    current=path.stat();done=time.time()
    identity=lambda x:(x.st_dev,x.st_ino,x.st_size,x.st_mtime_ns)
    need(start<=done and len(raw)<=limit and identity(before)==identity(after)==identity(current),'read_bound_or_change')
    return raw,dict(path=str(path),sha256=sha(raw),bytes=len(raw),read_started_epoch=start,read_completed_epoch=done)


def read(path,limit=2*1024*1024):
    raw,meta=raw_read(path,limit);value=json.loads(raw,parse_constant=lambda v:(_ for _ in ()).throw(ValueError('nonfinite_json')))
    return value,meta


def directories(root,pattern,cap):
    if not root.exists():return []
    safe(root);names=[]
    for count,path in enumerate(root.iterdir(),1):
        need(count<=cap,'directory_inventory_bound');safe(path)
        need(path.is_dir() and re.fullmatch(pattern,path.name),'unexpected_directory_entry');names.append(path)
    return sorted(names,key=lambda p:p.name)


def check_flags(value):need(value.get('research_only') is True and all(value.get(k) is False for k in FLAGS),'authority_flags')


def registered_source_path(source):
    need(type(source) is str and re.fullmatch(r'(?:[a-z0-9_]+/)*[a-z0-9_]+\.py',source),'source_path')
    path=safe(ROOT/source);need(path.is_relative_to(ROOT.absolute()),'source_path_escape')
    return path


def check_bindings():
    checks={};registries={}
    for key,(name,expected,count) in REGISTRIES.items():
        value,meta=read(ROOT/'config'/name);need(meta['sha256']==expected,'registry_hash');check_flags(value)
        need(len(value['source_bindings'])==count,'registry_source_count');sources={}
        for source,want in value['source_bindings'].items():
            _,actual=raw_read(registered_source_path(source))
            need(actual['sha256']==want,'frozen_source_changed');sources[source]=actual['sha256']
        registries[key]=value;checks[key]=dict(registry=meta,source_bindings=sources)
    additional={OLD:OLD_SHA,ROOT/'oanda_project_runtime_health.py':HEALTH_SHA,
        ROOT/'oanda_research_quote_receipt_v1.py':QUOTE_CONTRACT_SHA,
        **{ROOT/name:value for name,value in OBSERVER_PINS.items()},
        DATA/'joint_price_news_study_v3/activation_receipt.json':ACTIVATION_SHA}
    own={}
    for path,want in additional.items():
        _,meta=raw_read(path);need(meta['sha256']==want,'operational_source_changed');own[str(path)]=want
    own[str(Path(__file__))]=raw_read(Path(__file__))[1]['sha256']
    return registries,checks,own


def identity(checks,own):return {'registries':{k:{'sha256':v['registry']['sha256'],'sources':v['source_bindings']} for k,v in checks.items()},'own':own}


def load_primitives():
    spec=importlib.util.spec_from_file_location('pinned_operations_primitives',OLD)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def guarded(function,*args):
    try:return function(*args)
    except Exception as error:
        return dict(status='unavailable',reason=code(str(error)) or type(error).__name__,observed_epoch=time.time())


def heartbeat(path,ops):
    value,meta=read(path,256*1024)
    kept=compact(value,['schema_version','status','generated_epoch','generated_utc','heartbeat_utc','last_progress_utc',
        'progress_sequence','phase','phase_age_sec','progress_age_sec','cycle_in_progress','classification_version','errors',
        'publication_epoch','snapshot_generated_utc','snapshot_sha256','active_pair','pairs_with_forecast','heartbeat_publication_errors',
        'updated_at','progress_updated_at','started_at','role'])
    kept['last_error']=code(value.get('last_error'))
    return {**meta,'value':kept,'age_sec':ops.age(value.get('generated_epoch',value.get('generated_utc',value.get('heartbeat_utc',value.get('updated_at')))),meta['read_completed_epoch'])}


def market(ops):
    # Reuse the exact frozen quote contract's pure full-header validation for all
    # pairs. Generation membership is quotes minus the explicit retained list.
    source=ROOT/'oanda_research_quote_receipt_v1.py'
    need(raw_read(source)[1]['sha256']==QUOTE_CONTRACT_SHA,'quote_contract_source_changed')
    spec=importlib.util.spec_from_file_location('operations_quote_contract',source)
    contract=importlib.util.module_from_spec(spec);spec.loader.exec_module(contract)
    raw,meta=raw_read(DATA/'state/practice_007_market_quotes_v1.json',256*1024)
    now=meta['read_completed_epoch'];value=contract._decode(raw)
    quotes,retained,nontrade,unknown,generated=contract._source(value,now)
    current=set(quotes)-retained;wrapper_age=now-float(generated)
    need(len(quotes)<=68,'quote_inventory')
    rows=[]
    for pair,quote in sorted(quotes.items()):
        need(re.fullmatch('[A-Z]{3}_[A-Z]{3}',pair),'quote_pair_identity')
        age=ops.age(quote.get('time'),now)
        price_valid=False
        try:
            bid,ask,pip=map(contract._number,(quote['bid'],quote['ask'],quote['pip']))
            price_valid=bid<=ask and set(quote)=={'ask','bid','pip','source','time','tradeable'}
        except (ValueError,KeyError,TypeError):pass
        eligible=(price_valid and quote.get('tradeable') is True and quote.get('source')=='stream' and pair in current and pair not in retained
            and age is not None and 0<=age<=60 and wrapper_age is not None and 0<=wrapper_age<=60)
        eligible=eligible and ops.epoch(quote.get('time'))<=float(generated)
        row=dict(instrument=pair,tradeable=quote.get('tradeable') if type(quote.get('tradeable')) is bool else None,quote_market_epoch=ops.epoch(quote.get('time')),
            quote_age_sec=age,current_within60s=eligible,retained_only=pair in retained,current_generation=pair in current)
        try:
            path=safe(DATA/'candles'/(pair+'_M1.csv'))
            with path.open('rb') as stream:
                before=os.fstat(stream.fileno());header=stream.readline(8192);offset=max(len(header),before.st_size-8192)
                stream.seek(offset);tail=stream.read(8193);after=os.fstat(stream.fileno())
            need(len(tail)<=8192 and (before.st_size,before.st_mtime_ns)==(after.st_size,after.st_mtime_ns),'archive_tail_changed')
            if offset>len(header):tail=tail.split(b'\n',1)[1]
            if not tail.endswith(b'\n'):tail=tail[:tail.rfind(b'\n')+1]
            bars=list(csv.DictReader(io.StringIO((header+tail).decode())));last=bars[-1]
            need(last['instrument']==pair and last['granularity']=='M1','archive_identity')
            close=ops.epoch(last['time'])+60;observed=time.time()
            row['archive']=dict(last_price_close_epoch=close,observed_epoch=observed,price_age_sec=observed-close,
                lag_behind_quote_sec=None if row['quote_market_epoch'] is None else row['quote_market_epoch']-close,
                tail_sha256=sha(header+tail),source_size_bytes=before.st_size,
                scope='Last complete CSV record clock only; no provider completeness or price acceptance.')
        except (ValueError,KeyError,TypeError,OSError,IndexError) as error:row['archive']={'status':'unavailable','reason':type(error).__name__}
        rows.append(row)
    return dict(source=meta,wrapper_age_sec=wrapper_age,connection_generation=value['connection_generation'],
        coverage_contract=compact(value['coverage'],['current_quote_count','current_tradeable_quote_count','current_non_tradeable_quote_count',
            'current_tradeability_unknown_count','last_known_quote_count','retained_last_known_count','tradeability_contract']),
        current_quote_count=sum(r['current_within60s'] for r in rows),rows=rows,
        archive_age_summary_sec=ops.distribution([r['archive']['price_age_sec'] for r in rows if r['current_within60s'] and 'price_age_sec' in r['archive']]))


def pilot(registry):
    root=Path(registry['output_root']);cycles=[];partial=[];counts=Counter();reasons=Counter()
    for path in directories(root/'cycles',r'cycle_[0-9]{1,30}',PILOT_CYCLE_CAP):
        if not (path/'cycle_completed.json').exists():partial.append(path.name);continue
        value,meta=read(path/'cycle_completed.json',128*1024);check_flags(value)
        need(value.get('schema_version')=='recovered_curve_pilot_cycle_v1_20260909' and value['cycle_id']==path.name
            and [p['instrument'] for p in value['pairs']]==list(PAIRS[:len(value['pairs'])]),'pilot_cycle_identity')
        attempts=[a for p in value['pairs'] for a in p['attempts']]
        need(all(type(value[k]) is int and value[k]==sum(a.get(flag) is True for a in attempts) for k,flag in
            (('curves_issued','registry_issue_admitted'),('curves_published','publication_completed'),
             ('curves_consumed','consumption_completed'))),'pilot_stage_count')
        for key in ('curves_issued','curves_published','curves_consumed','skipped_prior_schedule_slots'):
            need(type(value[key]) is int and value[key]>=0,'pilot_count');counts[key]+=value[key]
        for pair in value['pairs']:
            if pair.get('reason_code'):reasons[code(pair['reason_code']) or 'noncode_reason']+=1
            for attempt in pair['attempts']:
                counts['attempts']+=1
                if attempt.get('reason_code'):reasons[code(attempt['reason_code']) or 'noncode_reason']+=1
        need(value['started_epoch']<=value['completed_epoch']<=meta['read_completed_epoch'],'pilot_cycle_clock')
        cycles.append(dict(source=meta,pairs_observed=[p['instrument'] for p in value['pairs']],**compact(value,['cycle_id','started_epoch','completed_epoch','scheduled_cycle_epoch',
            'start_delay_sec','curves_issued','curves_published','curves_consumed','skipped_prior_schedule_slots'])))
    last=max(cycles,key=lambda r:r['completed_epoch'],default=None)
    return dict(completed_cycle_count=len(cycles),partial_cycle_names=partial,cycle_inventory_cap=PILOT_CYCLE_CAP,
        aggregate=dict(counts),reason_counts=dict(reasons),latest=last,latest_age_sec=None if last is None else time.time()-last['completed_epoch'],
        completed_cycle_sources=cycles,scope='Retained collection-stage counts only; no curve, fill or accuracy acceptance.')


def paper(registry):
    root=Path(registry['output_root']);rows=[]
    for index,nominal in enumerate(registry['episode_nominal_start_epochs'],1):
        episode_id='episode_'+str(index).zfill(2);path=root/'episodes'/episode_id
        row=dict(episode_id=episode_id,nominal_start_epoch=nominal,
            status='scheduled_not_started' if time.time()<nominal else 'missing_episode_initialization',latest_step=None)
        if path.exists():
            safe(path)
            steps=directories(path/'steps',r'step_[0-9]{3}',64);complete=[]
            for step in steps:
                if (step/'step_completed.json').exists():
                    value,meta=read(step/'step_completed.json',128*1024);check_flags(value)
                    need(value['completed_epoch']<=meta['read_completed_epoch'],'paper_future_step')
                    complete.append(dict(step_id=step.name,source=meta,**compact(value,['completed_epoch','terminal','quote_observation_count','state_sha256','settlement_sha256'])))
            row.update(status='observed_without_final_result',completed_step_markers=len(complete),uncompleted_step_directories=len(steps)-len(complete),
                latest_step=max(complete,key=lambda r:r['completed_epoch'],default=None))
            if (path/'episode_config.json').exists():
                config,meta=read(path/'episode_config.json');row['original_clock_configuration']={**meta,**compact(config,['created_epoch','start_epoch','native_target_epoch'])}
            if (path/'episode_result.json').exists():
                value,meta=read(path/'episode_result.json');check_flags(value)
                need(value['completed_epoch']<=meta['read_completed_epoch'],'paper_future_result')
                row['status']='producer_reported_'+value['status'];row['result']={**meta,**compact(value,['completed_epoch','original_target_epoch','terminal_all_flat','unresolved_virtual_positions','actual_broker_actions'])}
                row['reason_code']=code(value.get('reason_code',value.get('failure',{}).get('reason_code')))
                row['reported_missed_slots']=len(value.get('missed_slots',[]))
        rows.append(row)
    need(len(rows)==3,'paper_three_episodes')
    return dict(episodes=rows,scope='Completion markers and reported disposition only; no heavy episode replay or P&L/position aggregation.')


def risk(registry):
    root=Path(registry['output_root']);rows=[];partial=[];totals=Counter();pair_status={};errors=[]
    for path in directories(root/'cycles',r'[0-9]{1,12}',RISK_CYCLE_CAP):
        cycle=int(path.name)
        if not (path/'cycle_completed.json').exists():partial.append(cycle);continue
        value,meta=read(path/'cycle_completed.json',128*1024);check_flags(value)
        need(value['registry_sha256']==REGISTRIES['risk'][1] and value['cycle_epoch']==cycle,'risk_cycle_identity')
        need(value['started_epoch']<=value['completed_epoch']<=meta['read_completed_epoch'],'risk_cycle_clock')
        need({p['instrument'] for p in value['pairs']}==set(PAIRS) and len(value['pairs'])==3,'risk_pair_inventory')
        stage={key:sum(artifact in p.get('artifacts',{}) for p in value['pairs']) for key,artifact in
            (('retained_issue_count','issued'),('publication_count','publication'),('consumption_count','consumption'))}
        stage['complete_chain_count']=sum(p['status']=='issued_published_consumed' for p in value['pairs'])
        need(all(type(value.get(k)) is int and value[k]==v for k,v in stage.items()),'risk_stage_count')
        totals.update(stage)
        for p in value['pairs']:
            pair_status[(cycle,p['instrument'])]=p['status']
            if p.get('reason_code'):errors.append(dict(cycle_epoch=cycle,instrument=p['instrument'],reason_code=code(p['reason_code'])))
        rows.append(dict(cycle_epoch=cycle,source=meta,completed_epoch=value['completed_epoch'],stages=stage,
            pairs=[dict(instrument=p['instrument'],status=p['status'],phase=p['phase'],capture_attempted=p['capture_attempted'],capture_invoked=p.get('capture_invoked',False),reason_code=code(p.get('reason_code'))) for p in value['pairs']],
            skipped_prior_capture_cycle_epochs=value.get('skipped_prior_capture_cycle_epochs',[])))
    now=time.time();slots=[];observed={r['cycle_epoch'] for r in rows}|set(partial)
    for reference in range(int(registry['first_reference_epoch']),int(registry['last_reference_epoch'])+1,300):
        for pair in PAIRS:
            status=pair_status.get((reference,pair))
            if status:disposition='producer_reported_'+status
            elif now<reference:disposition='scheduled_not_started'
            elif now<=reference+20:disposition='publication_deadline_not_elapsed'
            elif reference not in observed:disposition='missing_entire_cycle'
            else:disposition='missing_pair_or_cycle_completion'
            slots.append(dict(instrument=pair,reference_price_epoch=reference,disposition=disposition))
    return dict(observed_epoch=now,completed_cycles=len(rows),partial_cycles=partial,stage_counts=dict(totals),cycles=rows,
        latest_completed_epoch=max((r['completed_epoch'] for r in rows),default=None),errors=errors,
        planned_issue_slots=slots,planned_status_counts=dict(Counter(r['disposition'] for r in slots)),
        scope='Producer-retained stage and schedule evidence only, not independently validated distributions or outcomes.')


def news_capacity(root):
    safe(root);chosen=None;scanned=0
    for scanned,path in enumerate(root.iterdir(),1):
        need(scanned<=20000,'news_file_inventory_bound')
        if not re.fullmatch(r'[a-f0-9]{64}\.json\.gz',path.name):continue
        safe(path);info=path.stat();key=(info.st_mtime_ns,path.name)
        if chosen is None or key>chosen[0]:chosen=(key,path)
    if chosen is None:return dict(status='unavailable',reason='no_retained_gzip_capture',files_enumerated=scanned)
    raw,meta=raw_read(chosen[1],NEWS_COMPRESSED_CAP);decoder=zlib.decompressobj(31)
    decoded=decoder.decompress(raw,NEWS_DECODED_CAP+1)
    need(len(decoded)<=NEWS_DECODED_CAP and not decoder.unconsumed_tail,'news_16mib_decode_bound')
    need(decoder.eof and not decoder.unused_data,'news_gzip_geometry')
    return dict(status='bounded_size_observation',source=meta,files_enumerated=scanned,compressed_files_read=1,
        compressed_limit_bytes=NEWS_COMPRESSED_CAP,decoded_limit_bytes=NEWS_DECODED_CAP,decoded_bytes=len(decoded),
        decoded_full_bytes_sha256=sha(decoded),decoded_headroom_bytes=NEWS_DECODED_CAP-len(decoded),
        selected_by='latest_filesystem_mtime_then_name',semantic_payload_seal_verified=False,
        scope='Bytes/hash/capacity only; no news text, inferred source availability, account data or raised cap.')


def supervised_workers(supervisor,os_status):
    expected=supervisor.get('expected_workers',[])
    need(type(expected) is list and len(expected)==17 and len(set(expected))==17
        and all(type(name) is str and re.fullmatch(r'[a-z0-9_]+',name) for name in expected),'main17_expected_inventory')
    pids={r['pid'] for r in os_status['processes']};rows={}
    for name in expected:
        original=supervisor.get('workers',{}).get(name)
        if original is None:
            rows[name]=dict(status='not_reported',running=None,pids=[],all_reported_pids_present_in_os=False)
            continue
        rows[name]={**compact(original,['running','pids','supervisor_check_ok','explicitly_inactive']),
            'status':'supervisor_reported','all_reported_pids_present_in_os':bool(original['pids']) and all(pid in pids for pid in original['pids'])}
    return rows


def collect():
    started=time.time();registries,before,own=check_bindings();ops=load_primitives()
    sys.path.insert(0,str(ROOT));import oanda_joint_v3_ledger_status_observer_v1 as observer
    need(observer.own_source_bindings()==OBSERVER_PINS,'loaded_observer_identity')
    log_count=0
    for log_count,path in enumerate((DATA/'logs').iterdir(),1):need(log_count<=4096,'supervisor_log_inventory_bound')
    supervisor=ops.read_supervisor_observation(DATA/'logs')
    os_status=ops.os_processes();main_workers=supervised_workers(supervisor,os_status)
    independent=observer.observe_joint_v3(ROOT/'config'/REGISTRIES['joint_v3'][0],DATA/'joint_price_news_study_v3',
        expected_observer_source_bindings=OBSERVER_PINS,expected_activation_sha256=ACTIVATION_SHA,time_budget_sec=8)
    result=dict(schema_version='overnight_operations_observation_v2_20260909',started_epoch=started,
        frozen_registries=before,operational_source_bindings=own,
        supervisor=compact(supervisor,['status','observed_epoch','generated_epoch','supervisor_age_sec','expected_worker_count','running_worker_count','reasons','source']),
        main17_workers=main_workers,main17_os_present_count=sum(r['all_reported_pids_present_in_os'] for r in main_workers.values()),
        os=os_status,independent_joint_ledger_observer=independent,
        original_joint_producer_diagnostics=independent['original_producer_envelope'],
        heartbeats={name:guarded(heartbeat,DATA/path,ops) for name,path in {
            'news_collector':'local_news_sentiment/collector_heartbeat_v1.json','repaired_news':'local_news_sentiment_repair_v1/heartbeat.json',
            'quote_stream':'state/practice_007_quote_stream_heartbeat_v1.json','archive_m1':'state/all68_m1_forward_update_heartbeat_v1.json'}.items()},
        quotes_and_archive=guarded(market,ops),pilot=guarded(pilot,registries['pilot']),
        paper=guarded(paper,registries['paper']),risk=guarded(risk,registries['risk']),
        news_capture_capacity=guarded(news_capacity,DATA/'joint_price_news_study_v3/news_captures'))
    _,after,after_own=check_bindings();need(identity(before,own)==identity(after,after_own),'source_or_registry_changed_during_observation')
    result.update(finished_epoch=time.time(),frozen_registries_after=after,all_frozen_sources_match=True,
        scope='Read-only operational collection, original ledger publication checks and separate producer diagnostics; no account IDs/NAV, news text, GET, model fitting, accuracy evaluation or runtime writes.')
    return result


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True);args=parser.parse_args()
    path=Path(args.output).absolute();need(path.parent==BASE and path.suffix=='.json' and not path.exists(),'fresh_external_output_required');safe(BASE)
    result=collect();raw=json.dumps(result,sort_keys=True,indent=2,allow_nan=False).encode()+b'\n'
    with path.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
    need(path.read_bytes()==raw,'output_readback')
    print(json.dumps(dict(path=str(path),sha256=sha(raw),main17_os_present_count=result['main17_os_present_count'],
        joint_current_pairs=result['independent_joint_ledger_observer']['current_forecast_pairs'],
        pilot_completed_cycles=result['pilot'].get('completed_cycle_count'),risk_stage_counts=result['risk'].get('stage_counts'))))


if __name__=='__main__':main()
