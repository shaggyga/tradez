"""Observed price + original point-in-time news captures for joint research.

The large news capture is stored once by content hash. Each pair retains its
own price bytes and a small, replayable feature projection. Historical news is
training material only; live news requires the new guarded producer snapshot
and this consumer's actual read clock. No account or broker API is imported.
"""
from __future__ import annotations
import copy
import datetime as dt
import hashlib
import gzip
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import threading
import time
import zlib
from contextlib import closing
from types import ModuleType
from bisect import bisect_left,bisect_right

import oanda_causal_forecast_inputs_pair_v2 as price_inputs
import oanda_source_governance as governance
from oanda_causal_forecast_inputs import dependency_versions

SCHEMA='causal_price_news_pair_inputs_v1_20260907'
NEWS_SCHEMA='joint_news_shared_capture_v1_20260907'
FAMILY='ridge_price_news_v1'
NUMERICAL_MODULE='oanda_joint_price_news_models_v1'
CLASSIFICATION_VERSION='local_fx_news_rules_20260907_v165_causal_member_admission'
GUARD_VERSION='causal_news_member_admission_v1_20260907'
MAX_NEWS_BYTES=16*1024*1024
MAX_STORED_NEWS_BYTES=8*1024*1024
MAX_CURRENT_NEWS_BYTES=8*1024*1024
CURRENT_SOURCE_FILES=frozenset({'oanda_local_news_sentiment.py','oanda_news_classification_contract.py','oanda_news_causal_aggregation_guard_v1.py'})
MAX_HISTORY_ROWS=5000
MAX_NEWS_AGE_SEC=300
MAX_PRICE_AGE_SEC=900
HISTORY_SEC=48*3600
TRAINING_POLICY='Retrospective original classified members after committed mapping visibility; exact real H1 labels mature by fit cutoff; no prior prospective-performance claim.'
AVAILABILITY='actual_consumer_observation_after_retained_price_and_guarded_news_reads'
REQUIRED_SOURCE_FILES=frozenset({NUMERICAL_MODULE+'.py','oanda_causal_forecast_inputs_joint_news_v1.py',
    'oanda_pair_local_models_v2.py','oanda_causal_forecast_inputs_pair_v2.py','oanda_causal_forecast_inputs.py','oanda_causal_forecast_inputs_gap_v2.py',
    'oanda_news_causal_aggregation_guard_v1.py','oanda_news_classification_contract.py','oanda_local_news_sentiment.py',
    'oanda_source_governance.py','oanda_source_governance_news_fast_lane.py'})
_LOCK=threading.RLock()
_SOURCE_CACHE={}
_MODULE_CACHE={}
_CAPTURE_CACHE={}
_NEWS_MEMORY={}
_FRAME_CACHE={}

def _encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def _digest(value):return hashlib.sha256(_encoded(value)).hexdigest()
def _clock(value):
    value=value() if callable(value) else value
    if type(value) not in (int,float) or not math.isfinite(value) or value<=0:raise ValueError('finite_positive_clock_required')
    return float(value)
def _epoch(value):
    if not isinstance(value,str):raise ValueError('news_clock_required')
    value=dt.datetime.fromisoformat(value.replace('Z','+00:00'))
    if value.tzinfo is None:raise ValueError('aware_news_clock_required')
    return _clock(value.timestamp())
def _iso(value):return dt.datetime.fromtimestamp(value,dt.timezone.utc).isoformat()
def _signature(path):
    try:s=Path(path).stat();return (s.st_ino,s.st_size,s.st_mtime_ns)
    except OSError:return None
def _read(path,limit):
    path=Path(path);before=_signature(path)
    with path.open('rb') as h:
        opened=os.fstat(h.fileno());raw=h.read(limit+1);closed=os.fstat(h.fileno())
    after=_signature(path)
    ident=lambda s:(s.st_ino,s.st_size,s.st_mtime_ns)
    if len(raw)>limit:raise ValueError('input_byte_bound')
    if before!=after or before!=ident(opened) or before!=ident(closed):raise ValueError('source_changed_during_read')
    return raw
def _source_hash(path):
    path=Path(path);signature=_signature(path);key=str(path.resolve())
    with _LOCK:
        old=_SOURCE_CACHE.get(key)
        if old and old[0]==signature:return old[1]
    sha=hashlib.sha256(_read(path,4*1024*1024)).hexdigest()
    with _LOCK:_SOURCE_CACHE[key]=(signature,sha)
    return sha
def _bindings():
    root=Path(__file__).parent
    return {name:_source_hash(root/name) for name in sorted(REQUIRED_SOURCE_FILES)}
def _module(name):
    path=Path(__file__).with_name(name+'.py');sha=_source_hash(path)
    with _LOCK:
        if (name,sha) in _MODULE_CACHE:return _MODULE_CACHE[(name,sha)]
    raw=_read(path,1024*1024)
    if hashlib.sha256(raw).hexdigest()!=sha:raise ValueError('source_changed_during_module_read')
    module=ModuleType(name);module.__file__=str(path);exec(compile(raw,str(path),'exec'),module.__dict__)
    with _LOCK:
        _MODULE_CACHE[(name,sha)]=module
        while len(_MODULE_CACHE)>4:_MODULE_CACHE.pop(next(iter(_MODULE_CACHE)))
    return module

def source_signature(candle_root,instrument):
    root=Path(candle_root).resolve().parent
    files=(Path(candle_root)/f'{instrument}_M1.csv',root/'local_news_sentiment/joint_news_current_v1.json',
           root/'state/source_governance_v1.sqlite',root/'state/source_governance_v1.sqlite-wal')
    return tuple((str(p),_signature(p)) for p in files)

def _original_known(member):
    return max(_epoch(member[key]) for key in ('published_utc','first_seen_utc','causal_known_utc','detail_available_utc','numeric_causal_known_utc','publication_clock_known_utc') if member.get(key))

def _validate_current(snapshot,bindings,guard):
    if (not isinstance(snapshot,dict) or snapshot.get('schema_version')!='joint_news_current_v1_20260907'
            or snapshot.get('classification_version')!=CLASSIFICATION_VERSION or snapshot.get('guard_version')!=GUARD_VERSION
            or snapshot.get('status')!='current' or snapshot.get('errors')!=[]):raise ValueError('current_guarded_news_unavailable')
    if snapshot.get('research_only') is not True or any(snapshot.get(k) is not False for k in ('execution_eligible','can_place_orders','can_promote')):raise ValueError('current_news_inert_flags')
    if snapshot.get('source_bindings')!={key:bindings[key] for key in CURRENT_SOURCE_FILES}:raise ValueError('current_news_source_binding_changed')
    topics=snapshot.get('topics')
    if not isinstance(topics,list) or len(topics)>128:raise ValueError('current_topic_bound')
    evidence=_epoch(snapshot.get('as_of_utc'));asof=dt.datetime.fromtimestamp(evidence,dt.timezone.utc)
    replay=guard.build_current_news_snapshot(topics,as_of=asof)
    for key in ('status','news_state','topics','topic_count','directional_topic_count','context_topic_count','errors','limits'):
        if snapshot.get(key)!=replay.get(key):raise ValueError('current_news_replayed_snapshot_mismatch:'+key)
    return topics

def _history(database,observed_limit,guard):
    rows=[];bytes_read=0;deadline=time.monotonic()+10
    with closing(sqlite3.connect(Path(database).resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as con:
        con.row_factory=sqlite3.Row;con.execute('PRAGMA query_only=ON');con.execute('BEGIN')
        con.set_progress_handler(lambda:int(time.monotonic()>deadline),10000)
        batches=con.execute('SELECT MIN(mapping_visible_utc) earliest,MAX(mapping_visible_utc) latest FROM news_fast_lane_visibility_v3').fetchone()
        if not batches or batches['earliest'] is None:raise ValueError('committed_news_history_missing')
        cutoff=_iso(observed_limit-HISTORY_SEC)
        selected=con.execute('''SELECT e.source_event_id,e.raw_payload_sha256,e.payload_json,e.effective_from_utc,
           m.raw_payload_sha256 mapped_sha,v.mapping_visible_utc,v.availability_basis,v.consumer_first_observation_required,b.contract_id
           FROM news_fast_lane_mappings_v3 m JOIN source_events e USING(source_event_id)
           JOIN news_fast_lane_batches_v3 b USING(batch_seq) JOIN news_fast_lane_visibility_v3 v USING(batch_seq)
           WHERE v.mapping_visible_utc>=? ORDER BY v.mapping_visible_utc,e.source_event_id LIMIT ?''',(cutoff,MAX_HISTORY_ROWS+1))
        for row in selected:
            if len(rows)>=MAX_HISTORY_ROWS:raise ValueError('news_history_row_bound')
            raw_bytes=row['payload_json'].encode();bytes_read+=len(raw_bytes)
            if bytes_read>64*1024*1024:raise ValueError('news_history_read_byte_bound')
            if row['mapped_sha']!=row['raw_payload_sha256'] or row['contract_id']!='news_source_governance_fast_lane_v3_committed_visibility_20260905':raise ValueError('news_mapping_binding_mismatch')
            if row['availability_basis']!='post_commit_independent_mapping_read' or row['consumer_first_observation_required']!=1:raise ValueError('news_mapping_availability_contract')
            if governance.source_event_substantive_sha256(dict(row))!=row['raw_payload_sha256']:raise ValueError('news_original_payload_hash_mismatch')
            visible=_epoch(row['mapping_visible_utc']);effective=_epoch(row['effective_from_utc'])
            if visible>observed_limit or effective>visible:raise ValueError('future_news_mapping')
            raw=json.loads(row['payload_json']).get('raw_payload')
            if not isinstance(raw,dict) or raw.get('observation_clock_trusted') is not True:raise ValueError('news_member_observation_untrusted')
            if raw.get('classification_version') not in {'local_fx_news_rules_20260904_v164_conflict_duration_recap_guard',CLASSIFICATION_VERSION}:raise ValueError('unsupported_original_news_classification')
            member={key:copy.deepcopy(raw.get(key)) for key in guard.MEMBER_KEYS}
            member['observed_available_utc']=_iso(max(visible,_epoch(raw['observed_available_utc']) if raw.get('observed_available_utc') else visible))
            rows.append({'source_event_id':row['source_event_id'],'original_payload_sha256':hashlib.sha256(raw_bytes).hexdigest(),
                         'mapping_visible_epoch':visible,'effective_epoch':effective,'member':member})
        return rows,max(_epoch(batches['earliest']),observed_limit-HISTORY_SEC)

def _unpack(raw):
    try:
        decoder=zlib.decompressobj(16+zlib.MAX_WBITS)
        unpacked=decoder.decompress(raw,MAX_NEWS_BYTES+1)
        if len(unpacked)>MAX_NEWS_BYTES or decoder.unconsumed_tail:raise ValueError('shared_news_decompression_bound')
        if not decoder.eof or decoder.unused_data:raise ValueError('shared_news_compression_geometry')
        return unpacked
    except zlib.error as exc:raise ValueError('shared_news_compression_invalid') from exc

def _store_capture(root,value):
    value={**value,'news_capture_sha256':_digest(value)};raw=_encoded(value)
    if len(raw)>MAX_NEWS_BYTES:raise ValueError('sealed_news_capture_byte_bound')
    stored=gzip.compress(raw,mtime=0)
    if len(stored)>MAX_STORED_NEWS_BYTES:raise ValueError('sealed_news_stored_byte_bound')
    root=Path(root).resolve();root.mkdir(parents=True,exist_ok=True);path=root/(value['news_capture_sha256']+'.json.gz')
    # Publication is protected within this consumer and atomic for other
    # readers. An existing content-addressed artifact is never overwritten.
    with _LOCK:
        if path.exists():
            if _unpack(_read(path,MAX_STORED_NEWS_BYTES))!=raw:raise ValueError('immutable_news_capture_collision')
        else:
            temporary=path.with_suffix('.'+str(os.getpid())+'.'+str(threading.get_ident())+'.tmp')
            try:
                with temporary.open('xb') as h:h.write(stored);h.flush();os.fsync(h.fileno())
                try:os.link(temporary,path)
                except FileExistsError:
                    if _unpack(_read(path,MAX_STORED_NEWS_BYTES))!=raw:raise ValueError('immutable_news_capture_collision')
            finally:temporary.unlink(missing_ok=True)
    return {'news_capture_path':str(path),'news_capture_sha256':value['news_capture_sha256']}

def capture_news_inputs(data_root,*,clock=time.time,current_path=None,history_path=None,storage_root=None):
    root=Path(data_root).resolve();current=Path(current_path or root/'local_news_sentiment/joint_news_current_v1.json')
    history=Path(history_path or root/'state/source_governance_v1.sqlite');storage=Path(storage_root or root/'joint_price_news_study_v1/news_captures')
    begun=_clock(clock);bindings=_bindings();key=(str(current.resolve()),_signature(current),_digest(bindings))
    with _LOCK:cached=_CAPTURE_CACHE.get(key)
    if cached and 0<=begun-cached[0]<=60:
        existing=_load_news(cached[1]);_current_clock(existing,begun);return dict(cached[1])
    snapshot_bytes=_read(current,MAX_CURRENT_NEWS_BYTES);snapshot=json.loads(snapshot_bytes);guard=_module('oanda_news_causal_aggregation_guard_v1')
    read_epoch=_clock(clock)
    topics=_validate_current(snapshot,bindings,guard)
    evidence=_epoch(snapshot.get('as_of_utc'));generated=_epoch(snapshot.get('generated_utc'))
    if read_epoch<begun or not evidence<=generated<=read_epoch or read_epoch-evidence>300:raise ValueError('current_news_stale_or_future')
    history_rows,history_start=_history(history,read_epoch,guard)
    observed=_clock(clock)
    if observed<read_epoch or observed-evidence>300:raise ValueError('news_capture_clock_or_expiry')
    members={}
    for topic in topics:
        for raw in topic['causal_aggregation_guard']['members']:
            member=copy.deepcopy(raw);identity=member.get('event_id')
            member['observed_available_utc']=_iso(max(observed,_epoch(member['observed_available_utc']) if member.get('observed_available_utc') else observed))
            if identity in members and members[identity]!=member:raise ValueError('conflicting_current_member_identity')
            members[identity]=member
    value={'schema_version':NEWS_SCHEMA,'first_observed_epoch':observed,'news_available_epoch':observed,
           'news_evidence_epoch':evidence,'news_generated_epoch':generated,'current_snapshot_sha256':hashlib.sha256(snapshot_bytes).hexdigest(),
           'current_snapshot_canonical_sha256':_digest(snapshot),
           'source_hash_scope':'Raw snapshot and historical wrapper hashes were verified/observed during ingestion; retained canonical snapshot and compact original members replay the used features, not omitted raw fields.',
           'current_snapshot':snapshot,'current_members':list(members.values()),'history':history_rows,'history_start_epoch':history_start,
           'source_bindings':bindings,'guard_version':GUARD_VERSION,'classification_version':CLASSIFICATION_VERSION,
           'training_policy':TRAINING_POLICY,'research_only':True,'can_place_orders':False,'can_promote':False,'account_eligible':False,'proof_eligible':False}
    descriptor=_store_capture(storage,value)
    with _LOCK:
        _CAPTURE_CACHE[key]=(observed,descriptor)
        while len(_CAPTURE_CACHE)>4:_CAPTURE_CACHE.pop(next(iter(_CAPTURE_CACHE)))
    return descriptor

def _load_news(descriptor):
    if not isinstance(descriptor,dict):raise ValueError('shared_news_descriptor_required')
    sha=descriptor.get('news_capture_sha256');path=Path(descriptor.get('news_capture_path','')).resolve()
    if not isinstance(sha,str) or re.fullmatch(r'[0-9a-f]{64}',sha) is None or path.name!=sha+'.json.gz':raise ValueError('shared_news_content_address_required')
    signature=_signature(path);key=(str(path),sha,signature)
    with _LOCK:cached=_NEWS_MEMORY.get(key)
    raw=cached if cached is not None else _unpack(_read(path,MAX_STORED_NEWS_BYTES));value=json.loads(raw)
    if not isinstance(value,dict) or value.get('news_capture_sha256')!=sha or _digest({k:v for k,v in value.items() if k!='news_capture_sha256'})!=sha:raise ValueError('shared_news_hash_mismatch')
    if value.get('schema_version')!=NEWS_SCHEMA or value.get('training_policy')!=TRAINING_POLICY or value.get('source_bindings')!=_bindings():raise ValueError('shared_news_source_or_policy_changed')
    if value.get('research_only') is not True or any(value.get(k) is not False for k in ('can_place_orders','can_promote','account_eligible','proof_eligible')):raise ValueError('shared_news_inert_flags')
    if cached is None:_validate_shared(value)
    with _LOCK:
        _NEWS_MEMORY[key]=raw
        while len(_NEWS_MEMORY)>4:_NEWS_MEMORY.pop(next(iter(_NEWS_MEMORY)))
    return value

def _current_clock(news,now):
    evidence=_clock(news['news_evidence_epoch']);generated=_clock(news['news_generated_epoch']);observed=_clock(news['first_observed_epoch'])
    if not evidence<=generated<=observed==_clock(news['news_available_epoch'])<=now or now-evidence>300:raise ValueError('current_news_stale_or_future')

def _validate_shared(news):
    observed=_clock(news['first_observed_epoch']);_current_clock(news,observed)
    guard=_module('oanda_news_causal_aggregation_guard_v1');snapshot=news.get('current_snapshot')
    topics=_validate_current(snapshot,news['source_bindings'],guard)
    if (_digest(snapshot)!=news.get('current_snapshot_canonical_sha256') or _epoch(snapshot.get('as_of_utc'))!=news['news_evidence_epoch'] or _epoch(snapshot.get('generated_utc'))!=news['news_generated_epoch']):raise ValueError('shared_current_snapshot_binding')
    members={};seen=set()
    for topic in topics:
        if not isinstance(topic,dict) or not isinstance(topic.get('topic_id'),str) or topic['topic_id'] in seen:raise ValueError('shared_topic_identity')
        seen.add(topic['topic_id']);guard.validate_guarded_topic(topic,as_of=dt.datetime.fromtimestamp(news['news_evidence_epoch'],dt.timezone.utc))
        for source in topic['causal_aggregation_guard']['members']:
            member=copy.deepcopy(source);identity=member.get('event_id')
            member['observed_available_utc']=_iso(max(observed,_epoch(member['observed_available_utc']) if member.get('observed_available_utc') else observed))
            if identity in members and members[identity]!=member:raise ValueError('conflicting_current_member_identity')
            members[identity]=member
    if news.get('current_members')!=list(members.values()):raise ValueError('shared_current_member_projection')
    history=news.get('history');seen=set();start=_clock(news.get('history_start_epoch'))
    if not isinstance(history,list) or len(history)>MAX_HISTORY_ROWS or start>observed:raise ValueError('shared_history_bound')
    for row in history:
        if not isinstance(row,dict) or not isinstance(row.get('source_event_id'),str) or row['source_event_id'] in seen:raise ValueError('shared_history_identity')
        seen.add(row['source_event_id']);visible=_clock(row.get('mapping_visible_epoch'));effective=_clock(row.get('effective_epoch'))
        member=row.get('member')
        if not isinstance(member,dict) or set(member)!=set(guard.MEMBER_KEYS):raise ValueError('shared_historical_member_shape')
        if (not effective<=visible<=observed or _epoch(member.get('observed_available_utc'))<visible
                or member.get('classification_version') not in {'local_fx_news_rules_20260904_v164_conflict_duration_recap_guard',CLASSIFICATION_VERSION}
                or not isinstance(row.get('original_payload_sha256'),str) or re.fullmatch('[0-9a-f]{64}',row['original_payload_sha256']) is None):raise ValueError('shared_historical_member_binding')
        _original_known(member)
        scores=member.get('currency_scores')
        if not isinstance(scores,dict) or len(scores)>32 or any(re.fullmatch('[A-Z]{3}',str(c)) is None or type(v) not in (int,float) or not math.isfinite(v) or not -1<=v<=1 for c,v in scores.items()):raise ValueError('shared_historical_score_invalid')

def _frame(news,epoch,*,live=False):
    # All currencies share one bounded admission calculation per UTC anchor.
    key=(news['news_capture_sha256'],epoch,live)
    with _LOCK:cached=_FRAME_CACHE.get(key)
    if cached is not None:return copy.deepcopy(cached)
    guard=_module('oanda_news_causal_aggregation_guard_v1');members=[];available=[]
    if live:
        candidates=[(news['first_observed_epoch'],m) for m in news['current_members']]
    else:candidates=[(max(r['mapping_visible_epoch'],_epoch(r['member']['observed_available_utc'])),r['member']) for r in news['history'] if r['mapping_visible_epoch']<=epoch]
    seen=set()
    for availability,member in candidates:
        try:
            known=_original_known(member);scores=member.get('currency_scores')
            if not isinstance(scores,dict) or not scores or known>epoch or known<epoch-3600 or availability>epoch:continue
            if any(re.fullmatch('[A-Z]{3}',str(c)) is None or type(v) not in (int,float) or not math.isfinite(v) or not -1<=v<=1 for c,v in scores.items()):raise ValueError('historical_news_score_invalid')
            identity=guard._headline(member)
            if identity in seen:continue
            seen.add(identity);members.append(member);available.append(availability)
        except (TypeError,ValueError,KeyError):continue
    groups=[]
    for member in members:
        found=next((group for group in groups if all(guard.same_claim(member,other) for other in group)),None)
        if found is None:groups.append([member])
        else:found.append(member)
    directional=[]
    for group in groups:
        original={'topic_id':'joint_claim_'+_digest(sorted(m['event_id'] for m in group))[:24]}
        topic=guard.guard_topic(original,group,as_of=dt.datetime.fromtimestamp(epoch,dt.timezone.utc))
        if topic.get('directional_publish_eligible'):
            directional.append({'scores':topic['currency_scores'],'expires_epoch':_epoch(topic['direction_expires_utc'])})
    result={'members':[{'scores':m['currency_scores'],'known_epoch':_original_known(m)} for m in members],
            'directional':directional,'available_max_epoch':max(available,default=0.),'guard_version':GUARD_VERSION}
    with _LOCK:
        _FRAME_CACHE[key]=copy.deepcopy(result)
        while len(_FRAME_CACHE)>512:_FRAME_CACHE.pop(next(iter(_FRAME_CACHE)))
    return result

def _pair_features(frame,instrument,epoch):
    base,quote=instrument.split('_')
    context=[r for r in frame['members'] if base in r['scores'] or quote in r['scores']]
    vetted=[r for r in frame['directional'] if base in r['scores'] or quote in r['scores']]
    values=[r['scores'].get(base,0)-r['scores'].get(quote,0) for r in context]
    directions=[r['scores'].get(base,0)-r['scores'].get(quote,0) for r in vetted]
    average=lambda a:sum(a)/len(a) if a else 0.
    features=[average(values),math.log1p(len(values)),average([1 if v>0 else -1 if v<0 else 0 for v in values]),
              average([min(1.,max(0.,(epoch-r['known_epoch'])/3600)) for r in context]) if context else 1.,
              average(directions),math.log1p(len(directions)),min(sum(v>0 for v in directions),sum(v<0 for v in directions))/len(directions) if directions else 0.,
              min(1.,max(0.,min((r['expires_epoch']-epoch for r in vetted),default=0.)/3600))]
    return features,min((r['expires_epoch'] for r in vetted),default=None)

def _derived(price_capture,news,instrument,pip):
    rows,_=price_inputs._verified_rows(price_capture,instrument=instrument,pip_size=pip)
    cutoff=price_capture['reference_start_epoch'];frames={};availability={}
    for t in rows:
        if t%900 or t+60<news['history_start_epoch'] or t+3600>cutoff or t+3600 not in rows:continue
        frame=_frame(news,t+60);frames[str(t)]=_pair_features(frame,instrument,t+60)[0];availability[str(t)]=frame['available_max_epoch']
    current=_frame(news,news['first_observed_epoch'],live=True);current_features,expiry=_pair_features(current,instrument,news['first_observed_epoch'])
    expires=min(news['news_evidence_epoch']+300,expiry) if expiry is not None else news['news_evidence_epoch']+300
    module=_module(NUMERICAL_MODULE)
    readiness=module.family_readiness(rows,cutoff,instrument=instrument,pip_size=pip,news_frames=frames,current_news=current_features)
    return {'reference_start_epoch':cutoff,'max_bar_close_epoch':price_capture['max_bar_close_epoch'],
            'news_frames':frames,'training_news_availability_by_epoch':availability,'current_news_features':current_features,
            'news_evidence_epoch':news['news_evidence_epoch'],'news_generated_epoch':news['news_generated_epoch'],
            'news_first_observed_epoch':news['first_observed_epoch'],'news_available_epoch':news['news_available_epoch'],
            'news_expires_epoch':expires,'family_readiness':readiness,'available_families':[FAMILY] if readiness[FAMILY]['ready'] else []}

def capture_inputs(candle_root,instrument,*,pip_size,clock=time.time,news_capture=None):
    module=_module(NUMERICAL_MODULE);instrument,pip=module.validate_identity(instrument,pip_size)
    result={'schema_version':SCHEMA,'status':'abstain','readiness_status':'abstain','reasons':[],'pairs':[instrument],
            'output_instrument':instrument,'pip_size':pip,'model_source_sha256':_source_hash(Path(__file__).with_name(NUMERICAL_MODULE+'.py')),
            'dependency_versions':dependency_versions(),'availability_semantics':AVAILABILITY,'training_policy':TRAINING_POLICY,
            'family_readiness':{},'available_families':[],'research_only':True,'account_eligible':False,'can_place_orders':False,'can_promote':False}
    try:
        descriptor=news_capture or capture_news_inputs(Path(candle_root).resolve().parent,clock=clock)
        news=_load_news(descriptor);price=price_inputs.capture_inputs(candle_root,instrument,pip_size=pip,clock=clock)
        observed=_clock(clock);result.update(descriptor,price_capture=price,first_observed_epoch=observed)
        _current_clock(news,observed)
        if price.get('status')!='ready':raise ValueError('price_input_unavailable:'+','.join(price.get('reasons',[])))
        if price['first_observed_epoch']>observed:raise ValueError('price_observed_after_joint_capture')
        result.update(_derived(price,news,instrument,pip))
        if observed>result['news_expires_epoch']:raise ValueError('current_news_original_window_expired')
        result.update(status='ready',readiness_status='ready' if result['available_families'] else 'blocked')
    except (OSError,ValueError,KeyError,TypeError,sqlite3.Error) as exc:
        result['reasons'].append(type(exc).__name__+':'+str(exc)[:300])
    result['source_capture_sha256']=_digest(result);return result

def validate_capture(capture,*,instrument=None,pip_size=None,news_capture=None):
    if capture.get('schema_version')!=SCHEMA or capture.get('status')!='ready':raise ValueError('joint_input_not_ready')
    if _digest({k:v for k,v in capture.items() if k!='source_capture_sha256'})!=capture.get('source_capture_sha256'):raise ValueError('joint_input_capture_hash_mismatch')
    module=_module(NUMERICAL_MODULE);pair,pip=module.validate_identity(capture.get('output_instrument'),capture.get('pip_size'))
    if instrument is not None and pair!=instrument or pip_size is not None and module.validate_identity(pair,pip_size)[1]!=pip:raise ValueError('joint_registered_identity_mismatch')
    if (capture.get('pairs')!=[pair] or capture.get('model_source_sha256')!=_source_hash(Path(__file__).with_name(NUMERICAL_MODULE+'.py'))
            or capture.get('dependency_versions')!=dependency_versions() or capture.get('training_policy')!=TRAINING_POLICY
            or capture.get('availability_semantics')!=AVAILABILITY or capture.get('reasons')!=[]
            or capture.get('research_only') is not True or any(capture.get(k) is not False for k in ('can_place_orders','can_promote','account_eligible'))):raise ValueError('joint_capture_binding_or_safety')
    descriptor=news_capture or capture
    if descriptor.get('news_capture_sha256')!=capture.get('news_capture_sha256'):raise ValueError('joint_shared_news_binding_mismatch')
    news=_load_news(descriptor);observed=_clock(capture['first_observed_epoch']);_current_clock(news,observed)
    price=capture['price_capture'];price_inputs.validate_capture(price,instrument=pair,pip_size=pip)
    if not price['first_observed_epoch']<=observed<=capture['news_expires_epoch']:raise ValueError('joint_observation_order_or_expiry')
    for key,value in _derived(price,news,pair,pip).items():
        if capture.get(key)!=value:raise ValueError('joint_derived_input_binding_mismatch:'+key)
    if capture.get('readiness_status')!=('ready' if capture['available_families'] else 'blocked'):raise ValueError('joint_derived_readiness_binding')

def compute_predictions(capture,*,families=None,news_capture=None,clock=time.time):
    fields=('news_capture_sha256','news_evidence_epoch','news_generated_epoch','news_first_observed_epoch','news_available_epoch','news_expires_epoch')
    output={'status':'abstain','reasons':[],'predictions':{},'family_readiness':{},'requested_families':[], 'available_families':[],
            'model_source_sha256':capture.get('model_source_sha256'),'source_capture_sha256':capture.get('source_capture_sha256'),
            'output_instrument':capture.get('output_instrument'),'pip_size':capture.get('pip_size'),'dependency_versions':dependency_versions(),
            **{key:capture.get(key) for key in fields}}
    try:
        started=_clock(clock);validate_capture(capture,news_capture=news_capture);output['computation_started_epoch']=started
        if not capture['first_observed_epoch']<=started<=capture['news_expires_epoch'] or started-capture['news_evidence_epoch']>300:raise ValueError('joint_news_expired_before_computation')
        if not 0<=started-capture['max_bar_close_epoch']<=900:raise ValueError('joint_price_stale_or_future')
        rows,_=price_inputs._verified_rows(capture['price_capture']);module=_module(NUMERICAL_MODULE);selected=module._selected(families);output['requested_families']=list(selected)
        predictions,readiness=module.predict_with_readiness(rows,capture['reference_start_epoch'],instrument=capture['output_instrument'],pip_size=capture['pip_size'],
            news_frames=capture['news_frames'],current_news=capture['current_news_features'],families=selected)
        output['family_readiness']=readiness
        for family,(expected,probability,diagnostics) in predictions.items():
            training=diagnostics['training_row_start_epochs_by_pair'][capture['output_instrument']]
            diagnostics['training_news_available_max_epoch']=max(capture['training_news_availability_by_epoch'][str(t)] for t in training)
            output['predictions'][family]={'expected_signed_pips':expected,'probability_up':probability,'side':1 if expected>0 else -1 if expected<0 else 0,'diagnostics':diagnostics}
        completed=_clock(clock);output['computed_epoch']=completed
        if completed<started or completed>capture['news_expires_epoch'] or completed-capture['news_evidence_epoch']>300 or completed-capture['max_bar_close_epoch']>900:raise ValueError('joint_input_expired_during_computation')
        output.update(status='ready' if output['predictions'] else 'abstain',available_families=list(output['predictions']))
        if not output['predictions']:output['reasons']=[FAMILY+':'+reason for reason in readiness[FAMILY]['reasons']]
    except Exception as exc:
        output.update(status='abstain',predictions={},available_families=[]);output['reasons'].append(type(exc).__name__+':'+str(exc)[:300])
    return output
