"""Two registered TRY pair ledgers and retained input evidence, read-only only."""
import base64
from collections import Counter
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time

HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[1]/'trad'
DATA=ROOT/'data'/'oanda_training_manager'
STUDY=DATA/'joint_price_news_study_v3'
sys.path.insert(0,str(ROOT))
import oanda_joint_price_news_forecast_study_v3 as worker
import oanda_causal_forecast_inputs_pair_v2 as prices
import oanda_forecast_curve_file_store_v1 as files

PAIRS=('TRY_JPY','USD_TRY')
FAMILY='ridge_price_news_v1'
REGISTRY_SHA='ee075e69e80ca56dfdf45abe1f7a1a301612176e67f7622eab1adf2bd9af8771'
TABLES=('quotes','attempts','diagnostics','inputs','forecasts','publication','consumption','entries','outcomes','exclusions')
QUERIES=[]


def need(ok,reason):
    if not ok:raise ValueError(reason)


def sha(raw):return hashlib.sha256(raw).hexdigest()


def read(path,limit):
    files._safe_components(path,require_file=True)
    start=time.time()
    with path.open('rb') as f:
        before=os.fstat(f.fileno());raw=f.read(limit+1);after=os.fstat(f.fileno())
    done=time.time();need(len(raw)<=limit,'source_byte_bound')
    need((before.st_ino,before.st_size,before.st_mtime_ns)==(after.st_ino,after.st_size,after.st_mtime_ns),
         'source_changed_during_read')
    return raw,dict(path=str(path),sha256=sha(raw),bytes=len(raw),read_started_epoch=start,read_completed_epoch=done)


def registry():
    path=ROOT/'config'/'joint_price_news_study_v3_20260908.json'
    raw,receipt=read(path,1024*1024)
    need(sha(raw)==REGISTRY_SHA,'registry_changed')
    value=worker.load_registry(path)
    need(value==json.loads(raw),'registry_read_mismatch')
    return value,receipt


def query(db,pair,sql,parameters=()):
    begun=time.time();rows=db.execute(sql,parameters).fetchall();done=time.time()
    need(len(rows)<=128,'query_row_bound')
    QUERIES.append(dict(instrument=pair,sql=sql,parameters=list(parameters),rows=len(rows),
                        read_started_epoch=begun,read_completed_epoch=done))
    return [dict(v) for v in rows]


def inspect_pair(pair,registered,activation):
    path=STUDY/'pairs'/pair/FAMILY/'study.sqlite'
    files._safe_components(path,require_file=True)
    need(path.stat().st_size<=512*1024*1024,'database_size_bound')
    start=time.time();before_queries=len(QUERIES)
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=1)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON')
        deadline=time.monotonic()+4
        db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
        db.set_authorizer(lambda action,*_:sqlite3.SQLITE_OK if action in (
            sqlite3.SQLITE_SELECT,sqlite3.SQLITE_READ,sqlite3.SQLITE_FUNCTION,sqlite3.SQLITE_TRANSACTION)
            else sqlite3.SQLITE_DENY)
        db.execute('BEGIN')
        schema=query(db,pair,"SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name")
        need(set(TABLES)|{'activation','contract','clocks'} <= {v['name'] for v in schema},'ledger_schema_missing')
        contract=query(db,pair,'SELECT sha,payload FROM contract WHERE id=1')[0]
        need(contract['sha']==registered['contract_sha256'] and
             contract['payload']==worker.encoded(registered['contract']).decode(),'registered_contract_mismatch')
        active=query(db,pair,'SELECT epoch,contract_sha FROM activation WHERE id=1')[0]
        need(active['contract_sha']==contract['sha'] and 0<active['epoch']<=start,'activation_identity')
        # The deployment receipt's exact per-ledger activation must agree.
        records=[v for v in activation['ledgers'] if v['instrument']==pair and v['family']==FAMILY]
        need(len(records)==1 and records[0]['activated_epoch']==active['epoch'],'deployment_activation_mismatch')
        counts={table:query(db,pair,f'SELECT count(*) AS n FROM {table}')[0]['n'] for table in TABLES}
        attempts=query(db,pair,'SELECT * FROM attempts ORDER BY epoch DESC LIMIT 10')
        diagnostics=query(db,pair,'SELECT attempt_id,bucket,epoch,length(payload) AS bytes,payload FROM diagnostics ORDER BY epoch DESC LIMIT 10')
        for row in diagnostics:
            need(row['bytes']<=1024*1024,'diagnostic_payload_bound')
            raw=row.pop('payload');value=json.loads(raw);row['payload_sha256']=sha(raw.encode())
            row['reason']=value.get('reason');row['input_capture_sha256']=value.get('input_capture_sha256')
            retained=value.get('retained_result',{})
            row['retained_result_status']=retained.get('status')
            row['retained_result_reasons']=retained.get('reasons',[])
        last_quotes=query(db,pair,'SELECT id,market,available FROM quotes ORDER BY available DESC LIMIT 1')
        bounds=query(db,pair,'SELECT min(epoch) AS first_epoch,max(epoch) AS last_epoch FROM attempts')[0]
        fb=query(db,pair,'SELECT min(reference) AS first_reference,max(target) AS last_target FROM forecasts')[0]
        observed=time.time()
    return dict(instrument=pair,path=str(path),database_file_bytes=path.stat().st_size,
        read_started_epoch=start,coherent_transaction_completed_epoch=observed,
        registry_membership_verified=True,contract_sha256=contract['sha'],
        cohort_id=registered['contract']['cohorts'][FAMILY],activation=active,
        schema_sha256=sha(worker.encoded(schema)),counts=counts,latest_attempts=attempts,
        latest_diagnostics=diagnostics,attempt_clock_bounds=bounds,forecast_bounds=fb,
        latest_retained_quote_clock=last_quotes,queries=len(QUERIES)-before_queries)


def readiness_tail():
    path=STUDY/'readiness.jsonl';files._safe_components(path,require_file=True)
    start=time.time()
    with path.open('rb') as f:
        before=os.fstat(f.fileno());size=before.st_size;offset=max(0,size-2*1024*1024)
        f.seek(offset);raw=f.read(size-offset);after=os.fstat(f.fileno())
    done=time.time()
    need(before.st_ino==after.st_ino and after.st_size>=size,'readiness_replaced_or_truncated')
    if offset:
        skip=raw.find(b'\n')+1;need(skip>0,'readiness_line_bound');offset+=skip;raw=raw[skip:]
    end=raw.rfind(b'\n')+1;raw=raw[:end]
    rows=raw.splitlines();need(len(rows)<=10000,'readiness_row_bound')
    selected={p:[] for p in PAIRS};all_clocks=[]
    for line in rows:
        need(len(line)<=32768,'readiness_line_bound')
        value=json.loads(line);event=value['event_observed_epoch'];need(event<=done,'future_readiness_record')
        all_clocks.append(event)
        if value['instrument'] in selected:
            selected[value['instrument']].append(dict(record=value,original_line_sha256=sha(line)))
    return dict(path=str(path),read_started_epoch=start,read_completed_epoch=done,
        captured_prefix_file_size=size,source_size_after=after.st_size,offset=offset,bytes=len(raw),
        captured_tail_sha256=sha(raw),complete_lines=len(rows),full_history_read=False,
        covered_event_epoch_min=min(all_clocks),covered_event_epoch_max=max(all_clocks),
        per_pair={p:dict(matched_records=len(v),latest=v[-1] if v else None,
                        reasons=Counter(reason for x in v for reason in x['record'].get('reasons',[]))) for p,v in selected.items()})


def candle_tail(pair):
    path=DATA/'candles'/f'{pair}_M1.csv'
    start=time.time()
    try:
        files._safe_components(path,require_file=True)
        source=prices._read_tail(path);observed=time.time()
        rows=prices._source_rows(source,observed,pair)
        stamps=list(rows)
        return dict(status='parsed_original_retained_price_tail',read_started_epoch=start,read_completed_epoch=observed,
            source={k:v for k,v in source.items() if k not in ('header_base64','tail_base64')},
            rows=len(rows),first_bar_start_epoch=min(stamps),latest_bar_start_epoch=max(stamps),
            latest_complete_price_epoch=max(stamps)+60,bar_age_at_read_sec=observed-max(stamps)-60,
            potential_exact_H1_endpoint_pairs=sum(t+3600 in rows for t in rows),
            joint_training_readiness_recomputed=False)
    except (ValueError,OSError) as error:
        return dict(status='unavailable',reason=type(error).__name__+':'+str(error),
                    read_started_epoch=start,read_completed_epoch=time.time())


def main():
    need(len(sys.argv)==2,'new_external_output_required')
    output=Path(sys.argv[1]).absolute()
    need(output.parent==HERE and output.name=='actual_audit_001' and not output.exists(),'fresh_output_required')
    own=sha(Path(__file__).read_bytes());started=time.time()
    reg,rr=registry();raw,ar=read(STUDY/'activation_receipt.json',1024*1024);activation=json.loads(raw)
    need(activation['registry_file_sha256']==REGISTRY_SHA,'activation_registry_binding')
    pairs=[inspect_pair(p,reg['pairs'][p]['families'][FAMILY],activation) for p in PAIRS]
    readiness=readiness_tail()
    price_evidence={p:candle_tail(p) for p in PAIRS}
    quote_path=DATA/'state'/'practice_007_market_quotes_v1.json'
    quote_started=time.time();snapshot,observed,source=worker.read_snapshot(quote_path)
    quote_result={}
    for pair in PAIRS:
        try:
            q=worker.pair_quote(snapshot,observed,source,pair,reg['pairs'][pair]['pip_size'])
            quote_result[pair]=dict(status='current_tradeable_quote',market_epoch=q['market_epoch'],
                observed_epoch=q['available_epoch'],age_sec=observed-q['market_epoch'])
        except ValueError as error:quote_result[pair]=dict(status='unavailable',reason=str(error))
    after,rr_after=registry();need(after==reg and own==sha(Path(__file__).read_bytes()),'source_bracket_changed')
    report=dict(schema_version='two_try_publication_audit_v1_20260909',started_epoch=started,completed_epoch=time.time(),
        helper_sha256=own,registry_before=rr,registry_after=rr_after,source_bindings=reg['source_bindings'],
        source_bindings_unchanged=True,activation_receipt=ar,ledgers=pairs,readiness_log=readiness,
        fresh_read_of_retained_candle_tails=price_evidence,
        current_quote_observation=dict(path=str(quote_path),source_sha256=source,read_started_epoch=quote_started,
            read_completed_epoch=observed,source_generated_utc=snapshot['generated_utc'],pairs=quote_result),
        exact_queries=QUERIES,bounds=dict(pairs=2,database_file_bytes_each=512*1024*1024,
            database_query_deadline_sec_each=4,rows_per_query=128,latest_attempts_or_diagnostics=10,
            readiness_tail_bytes=2*1024*1024,candle_tail_bytes_each=prices.MAX_TAIL_BYTES,
            candle_tail_rows_each=prices.MAX_SOURCE_ROWS),
        scope=['Each ledger has a separate coherent RO transaction; no global atomic snapshot.',
            'Counts/schema/contracts/activation and retained readiness are checked; no new joint fit or news reconstruction.',
            'Latest log record is an original captured readiness observation, not automatically current input readiness.',
            'Fresh local quote/candle reads do not refresh old source clocks or manufacture missing training/news support.',
            'No GET, publication, ledger constructor, checkpoint, score, source/runtime change or database write.'])
    output.mkdir();path=output/'TWO_TRY_PUBLICATION_AUDIT_20260909.json'
    encoded=worker.encoded(report)
    with path.open('xb') as f:f.write(encoded);f.flush();os.fsync(f.fileno())
    print(json.dumps(dict(path=str(path),sha256=sha(encoded),counts={v['instrument']:v['counts'] for v in pairs},
        latest_readiness={p:v['latest'] for p,v in readiness['per_pair'].items()},quotes=quote_result)))


if __name__=='__main__':main()
