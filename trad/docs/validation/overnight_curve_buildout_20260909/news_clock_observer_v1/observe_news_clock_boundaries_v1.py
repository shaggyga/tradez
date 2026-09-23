"""Passive bounded news-clock observations, private bytes and sanitized evidence.

Never calls producer run_cycle, publishes news, changes its files or requests
network data. One newly observed producer failure may trigger one query-only
article diagnostic. Later observations are not the failed worker's exact input.
"""
from collections import Counter
from contextlib import closing
import argparse
import datetime as dt
import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
import time
import traceback

sys.dont_write_bytecode=True
BASE=Path(__file__).resolve().parent
ROOT=Path(r'C:/Users/zmoor/Documents/forex/trad')
DATA=ROOT/'data/oanda_training_manager'
REGISTRY=ROOT/'config/joint_price_news_study_v3_20260908.json'
REGISTRY_SHA='ee075e69e80ca56dfdf45abe1f7a1a301612176e67f7622eab1adf2bd9af8771'
FILES={'collector_heartbeat':DATA/'local_news_sentiment/collector_heartbeat_v1.json',
       'clock_state':DATA/'state/clock_integrity_v1.json',
       'repaired_heartbeat':DATA/'local_news_sentiment_repair_v1/heartbeat.json'}
MAX_FILE_BYTES=128*1024
MAX_PRIVATE_BYTES=256*1024*1024
DURATION_SEC=600
INTERVAL_SEC=1


def need(ok,reason):
    if not ok:raise ValueError(reason)


def digest(raw):return hashlib.sha256(raw).hexdigest()
def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def reason(error):
    value=str(error)
    return value if re.fullmatch(r'(?:news|upstream|observer)_[a-z0-9_]{1,100}',value) else type(error).__name__


def safe(path):
    path=Path(path).absolute();need('..' not in path.parts,'observer_path_escape')
    for node in reversed([path,*path.parents]):
        info=node.lstat();need(not stat.S_ISLNK(info.st_mode) and not getattr(info,'st_file_attributes',0)&1024,'observer_reparse')
    return path


def read(path,cap=MAX_FILE_BYTES):
    path=safe(path);start=time.time()
    with path.open('rb') as stream:
        first=os.fstat(stream.fileno());raw=stream.read(cap+1);last=os.fstat(stream.fileno())
    current=path.stat();end=time.time();ident=lambda v:(v.st_dev,v.st_ino,v.st_size,v.st_mtime_ns)
    need(len(raw)<=cap and ident(first)==ident(last)==ident(current),'observer_read_changed_or_bound')
    need(start<=end,'observer_read_clock_regression')
    return raw,dict(read_started_epoch=start,read_completed_epoch=end,bytes=len(raw),sha256=digest(raw))


def verify_sources():
    raw,_=read(REGISTRY,2*1024*1024);need(digest(raw)==REGISTRY_SHA,'observer_registry_changed')
    registry=json.loads(raw);need(len(registry['source_bindings'])==20,'observer_source_inventory')
    for name,expected in registry['source_bindings'].items():
        need(re.fullmatch(r'(?:[a-z0-9_]+/)*[a-z0-9_]+\.py',name),'observer_source_path')
        source,_=read(ROOT/name,8*1024*1024);need(digest(source)==expected,'observer_source_changed')
    return registry['source_bindings']


class PrivateStore:
    def __init__(self,root):
        self.root=root;self.total=0;self.seen={};(root/'private_unique_bytes').mkdir()

    def retain(self,kind,raw,receipt):
        sha=digest(raw);key=(kind,sha)
        if key not in self.seen:
            need(self.total+len(raw)<=MAX_PRIVATE_BYTES,'observer_private_byte_bound')
            path=self.root/'private_unique_bytes'/(kind+'_'+sha+'.json')
            with path.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
            self.seen[key]=dict(relative_path=str(path.relative_to(self.root)),sha256=sha,bytes=len(raw),first_observed_read_completed_epoch=receipt['read_completed_epoch'])
            self.total+=len(raw)
        return dict(self.seen[key])


def clock_field(producer,value,path,present=True):
    result=dict(path=path,present=present,type=type(value).__name__)
    try:result.update(status='valid',epoch=producer.epoch(value))
    except Exception as error:result.update(status='invalid',reason_code=reason(error))
    return result


def validation(function,*args):
    try:function(*args);return dict(status='passed')
    except Exception as error:
        return dict(status='rejected',reason_code=reason(error),trace=[dict(file=Path(v.filename).name,function=v.name,line=v.lineno) for v in traceback.extract_tb(error.__traceback__)[-8:]])


def inspect(producer,kind,value,observed):
    need(type(value) is dict,'observer_json_object')
    paths={'collector_heartbeat':('generated_utc','last_progress_utc','cycle_started_utc'),
           'clock_state':('generated_utc',),'repaired_heartbeat':('generated_epoch','generated_utc','snapshot_generated_utc','publication_epoch')}[kind]
    result=dict(clock_fields=[clock_field(producer,value.get(key),key,key in value) for key in paths])
    if kind=='collector_heartbeat':
        result.update(validation=validation(producer.validate_collector_observation,value,observed),
            status=value.get('status') if value.get('status') in ('running_cycle','cycle_complete','error') else 'other_or_missing',
            phase=value.get('phase') if type(value.get('phase')) is str and re.fullmatch('[a-z_]{1,80}',value['phase']) else None,
            cycle_in_progress=value.get('cycle_in_progress') if type(value.get('cycle_in_progress')) is bool else None)
    elif kind=='clock_state':
        result.update(validation=validation(producer.validate_clock_state,value,observed),
            status=value.get('status') if value.get('status') in ('ok','mitigated','unavailable','error') else 'other_or_missing',
            nonclock_field_types={key:type(value.get(key)).__name__ for key in ('timestamp_normalization_trusted','host_clock_synchronized','broker_clock_lead_sec','broker_clock_sample_count','source_age_sec')})
    else:
        error=value.get('last_error');result.update(status=value.get('status') if value.get('status') in ('current','unavailable') else 'other_or_missing',
            errors=value.get('errors') if type(value.get('errors')) is int and value['errors']>=0 else None,
            last_error_code=error if type(error) is str and re.fullmatch(r'(?:ValueError|TypeError|KeyError):[a-z0-9_]{1,100}',error) else None,
            validation_scope='Heartbeat clock fields only; no frozen producer validator exists for this output heartbeat.')
    return result


def new_failure(baseline,value):
    current=value.get('errors')
    if type(current) is not int or current<0:return baseline,False
    return max(current,baseline) if baseline is not None else current,baseline is not None and current>baseline


def diagnose_articles(producer,store):
    """One coherent RO query, bounded schema/rows/bytes and pure article replay."""
    began=time.time();since=producer.iso(began-86400);path=safe(DATA/'local_news_sentiment/local_news_sentiment_v1.sqlite')
    fields=('event_id','published_utc','first_seen_utc','last_seen_utc','relevant','duplicate_count','payload_json')
    rows=[];total=0
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
        db.execute('PRAGMA query_only=ON')
        permitted={sqlite3.SQLITE_SELECT,sqlite3.SQLITE_READ,sqlite3.SQLITE_FUNCTION,sqlite3.SQLITE_TRANSACTION,sqlite3.SQLITE_PRAGMA}
        db.set_authorizer(lambda action,a,b,c,d: sqlite3.SQLITE_OK if action in permitted else sqlite3.SQLITE_DENY)
        deadline=time.monotonic()+5;db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
        db.execute('BEGIN')
        schema=db.execute('PRAGMA table_info(articles)').fetchall();need(set(fields)<={r[1] for r in schema},'observer_article_schema')
        count=db.execute('SELECT count(*) FROM articles WHERE '+producer._PREDICATE,(since,)).fetchone()[0]
        need(count<=producer.MAX_ROWS,'observer_article_row_bound')
        query='SELECT '+','.join(fields)+' FROM articles WHERE '+producer._PREDICATE+' ORDER BY published_utc,event_id'
        for values in db.execute(query,(since,)):
            row=dict(zip(fields,values));raw=row['payload_json'];need(type(raw) is str,'observer_article_payload_type')
            total+=len(raw.encode());need(total<=producer.MAX_SOURCE_BYTES,'observer_article_byte_bound')
            row['payload_sha256']=digest(raw.encode());rows.append(row)
        need(len(rows)==count,'observer_article_snapshot_count')
    completed=time.time();evidence=dict(schema_version=producer.EVIDENCE_SCHEMA,complete=True,predicate=producer._PREDICATE,
        database_snapshot_epoch=began,minimum_publication_utc=since,maximum_admissible_window_minutes=1440,row_count=len(rows),
        payload_bytes=total,rows=rows,rows_sha256=producer.digest(rows))
    raw=producer.encoded(evidence);need(len(raw)<=64*1024*1024,'observer_private_article_evidence_bound')
    ref=store.retain('article_diagnostic',raw,dict(read_completed_epoch=completed))
    invalid=[];invalid_count=0
    for index,row in enumerate(rows):
        for key in ('published_utc','first_seen_utc','last_seen_utc'):
            field=clock_field(producer,row.get(key),'rows['+str(index)+'].'+key,key in row)
            if field['status']=='invalid':
                invalid_count+=1
                if len(invalid)<128:invalid.append(field)
        try:payload=json.loads(row['payload_json'])
        except Exception:payload={}
        field=clock_field(producer,payload.get('published_utc'),'rows['+str(index)+'].payload.published_utc','published_utc' in payload)
        if field['status']=='invalid':
            invalid_count+=1
            if len(invalid)<128:invalid.append(field)
    asof=time.time();replay=validation(producer._rows_to_articles,evidence,asof)
    return dict(status='diagnostic_observed',database_read_started_epoch=began,database_read_completed_epoch=completed,
        pure_validation_completed_epoch=time.time(),row_count=len(rows),payload_bytes=total,private_evidence=ref,
        invalid_epoch_fields=invalid,invalid_epoch_field_count=invalid_count,field_report_cap=128,article_validation=replay,
        scope='Independent later RO snapshot and pure article validation, not the exact failed worker input; no clustering, current-news publication or event identifiers exported.')


def save(path,value):
    raw=encoded(value)+b'\n'
    with path.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
    return dict(path=str(path),sha256=digest(raw),bytes=len(raw))


def run(output):
    need(output.parent==BASE and not output.exists(),'observer_fresh_external_directory');safe(BASE)
    source=Path(__file__).read_bytes();bindings=verify_sources();sys.path.insert(0,str(ROOT))
    producer=importlib.import_module('oanda_local_news_sentiment_repair_v1');need(producer.source_bindings()=={k:bindings[k] for k in producer.SOURCE_FILES},'observer_loaded_semantic_closure')
    output.mkdir();store=PrivateStore(output);began=time.time();monotonic_start=time.monotonic();stop=monotonic_start+DURATION_SEC
    save(output/'OBSERVATION_STARTED.json',dict(schema_version='news_clock_passive_observer_v1_20260909',started_epoch=began,
        duration_sec=DURATION_SEC,interval_sec=INTERVAL_SEC,max_samples=601,registry_sha256=REGISTRY_SHA,source_bindings=bindings,
        observer_sha256=digest(source),maximum_private_bytes=MAX_PRIVATE_BYTES,max_db_diagnostics=1,GET=False,runtime_writes=False,
        scope='Private exact bytes; sanitized results. Independently observed source versions cannot be attributed as exact failed-worker inputs.'))
    samples=[];baseline=None;db_used=False;db_result=None;next_tick=monotonic_start;counts=Counter();final_status='completed'
    try:
        with (output/'sanitized_samples.jsonl').open('xb') as log:
            while time.monotonic()<stop and len(samples)<601:
                sample=dict(index=len(samples),started_epoch=time.time(),files={})
                for kind,path in FILES.items():
                    try:
                        raw,receipt=read(path);ref=store.retain(kind,raw,receipt);value=json.loads(raw)
                        checked=inspect(producer,kind,value,receipt['read_completed_epoch'])
                        sample['files'][kind]=dict(read=receipt,private_source=ref,inspection=checked)
                        counts[kind+':'+checked.get('validation',{}).get('status',checked.get('status','unknown'))]+=1
                        if kind=='repaired_heartbeat':
                            baseline,trigger=new_failure(baseline,value)
                            if trigger and not db_used:
                                db_used=True;verify_sources()
                                try:db_result=diagnose_articles(producer,store)
                                except Exception as error:db_result=dict(status='diagnostic_rejected',observed_epoch=time.time(),reason_code=reason(error),trace=[dict(file=Path(v.filename).name,function=v.name,line=v.lineno) for v in traceback.extract_tb(error.__traceback__)[-8:]])
                                db_result['trigger']=dict(repaired_heartbeat_sha256=receipt['sha256'],reported_errors=baseline,observed_read_completed_epoch=receipt['read_completed_epoch'])
                                save(output/'SANITIZED_ARTICLE_DIAGNOSTIC.json',db_result)
                    except Exception as error:sample['files'][kind]=dict(status='unavailable',observed_epoch=time.time(),reason_code=reason(error));counts[kind+':read_or_validation_failure']+=1
                sample['completed_epoch']=time.time();samples.append(sample);log.write(encoded(sample)+b'\n');log.flush();os.fsync(log.fileno())
                if len(samples)%60==0:need(verify_sources()==bindings and Path(__file__).read_bytes()==source,'observer_closure_changed')
                next_tick+=INTERVAL_SEC;time.sleep(min(max(0,next_tick-time.monotonic()),max(0,stop-time.monotonic())))
    except Exception as error:final_status='stopped_on_boundary';counts['observer:'+reason(error)]+=1
    unchanged=verify_sources()==bindings and Path(__file__).read_bytes()==source
    invalid=[dict(sample=s['index'],kind=k,read_completed_epoch=v['read']['read_completed_epoch'],source_sha256=v['read']['sha256'],field=f)
        for s in samples for k,v in s['files'].items() if 'inspection' in v for f in v['inspection']['clock_fields'] if f['status']=='invalid']
    transitions=[];prior=None
    for sample in samples:
        value=sample['files'].get('repaired_heartbeat',{});inspection=value.get('inspection',{});state=(inspection.get('status'),inspection.get('errors'),inspection.get('last_error_code'))
        if state!=prior:
            transitions.append(dict(sample=sample['index'],read=value.get('read'),status=state[0],errors=state[1],last_error_code=state[2]));prior=state
    result=dict(schema_version='news_clock_passive_observer_result_v1_20260909',status=final_status,started_epoch=began,completed_epoch=time.time(),sample_count=len(samples),
        observed_duration_sec=time.monotonic()-monotonic_start,counts=dict(counts),invalid_epoch_fields=invalid,producer_status_transitions=transitions,
        db_diagnostic_attempted=db_used,db_diagnostic=db_result,unique_private_versions=len(store.seen),private_bytes=store.total,
        all_registered_sources_unchanged=unchanged,observer_sha256=digest(source),registry_sha256=REGISTRY_SHA,
        sanitized_samples_sha256=digest((output/'sanitized_samples.jsonl').read_bytes()),
        limitations=['Polling can miss intermediate generations and does not establish which exact inputs the failed worker read.',
            'One later independent article snapshot may diagnose a still-present invalid field but cannot reconstruct an earlier overwritten version.',
            'The frozen producer withholds rather than inventing clocks. A missing/invalid epoch type is not itself evidence of host-clock drift.',
            'Private exact bytes remain local and must not be included in public/vault exports; this summary contains no article text or event IDs.'],GET=False,runtime_writes=False)
    receipt=save(output/'NEWS_CLOCK_PASSIVE_OBSERVATION_20260909.json',result)
    print(json.dumps(dict(status=final_status,receipt=receipt,samples=len(samples),invalid_epoch_fields=len(invalid),db_diagnostic_attempted=db_used,source_closure_unchanged=unchanged)),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output-directory',type=Path,required=True);args=parser.parse_args();run(args.output_directory.absolute())
