"""Bounded local quote observations; never broker fills or trading authority.

No read occurs on import. Prices preserve the exact decimal value already in
the retained JSON, not precision lost upstream. The original raw bytes and
RFC3339 timestamp remain the evidence for original numeric/time lexemes.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Context, Decimal, InvalidOperation, localcontext
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import time

VERSION='research_quote_receipt_v1_20260909'
RECEIPT_SCHEMA='local_research_quote_capture_v1_20260909'
MAP_SCHEMA='local_research_quote_map_v1_20260909'
MAX_BYTES=256*1024
MAX_QUOTES=128
DEFAULT_PATH=Path(__file__).resolve().parent/'data/oanda_training_manager/state/practice_007_market_quotes_v1.json'
METADATA={
    'EUR_USD':{'instrument':'EUR_USD','base_currency':'EUR','quote_currency':'USD','pip_size':'0.0001'},
    'GBP_USD':{'instrument':'GBP_USD','base_currency':'GBP','quote_currency':'USD','pip_size':'0.0001'},
    'USD_JPY':{'instrument':'USD_JPY','base_currency':'USD','quote_currency':'JPY','pip_size':'0.01'},
}
FLAGS={'research_only':True,'can_place_orders':False,'can_promote':False,'can_authorize':False,
       'account_eligible':False,'execution_eligible':False,'proof_eligible':False}
_TOP={'schema_version','producer','research_only','generated_utc','connection_generation','quote_count','quotes','coverage'}
_COVERAGE={'connection_generation','current_non_tradeable_instruments','current_non_tradeable_quote_count',
    'current_quote_count','current_tradeability_unknown_count','current_tradeability_unknown_instruments',
    'current_tradeable_quote_count','execution_requires_independent_freshness_check','last_known_quote_count',
    'retained_last_known_count','retained_last_known_instruments','retained_quotes_execution_eligible','tradeability_contract'}
_SHA=re.compile(r'^[a-f0-9]{64}$')
_PAIR=re.compile(r'^[A-Z]{3}_[A-Z]{3}$')
_TIME=re.compile(r'^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)$')


class QuoteReceiptError(ValueError):
    """A fixed bounded error code, never raw file or exception contents."""


def _need(condition,reason):
    if not condition:
        raise QuoteReceiptError(reason)


def _bytes(value):
    try:
        raw=json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode()
    except (TypeError,ValueError,OverflowError,RecursionError):
        raise QuoteReceiptError('quote_receipt_invalid_json') from None
    _need(len(raw)<=MAX_BYTES,'quote_receipt_byte_limit')
    return raw


def _hash(value):
    return hashlib.sha256(_bytes(value)).hexdigest()


def _epoch(value):
    _need(type(value) in (float,int) and 0<value<1e12 and math.isfinite(value),'quote_invalid_observation_clock')
    return float(value)


def _time(value):
    _need(isinstance(value,str),'quote_timestamp_invalid')
    match=_TIME.fullmatch(value)
    _need(match is not None,'quote_timestamp_invalid')
    try:
        whole=datetime.fromisoformat(match[1]+match[3].replace('Z','+00:00'))
        seconds=int(whole.timestamp())
        fraction=Decimal('0.'+(match[2] or '0'))
        with localcontext(Context(prec=64)):
            answer=Decimal(seconds)+fraction
    except (ValueError,OverflowError,InvalidOperation):
        raise QuoteReceiptError('quote_timestamp_invalid') from None
    _need(0<answer<Decimal('1e12'),'quote_timestamp_invalid')
    return answer


def _decode(raw):
    _need(type(raw) is bytes and 0<len(raw)<=MAX_BYTES,'quote_raw_byte_limit')
    def pairs(items):
        result={}
        for key,value in items:
            _need(key not in result,'quote_duplicate_json_key')
            result[key]=value
        return result
    try:
        return json.loads(raw,parse_float=Decimal,object_pairs_hook=pairs,
            parse_constant=lambda _:(_ for _ in ()).throw(QuoteReceiptError('quote_nonfinite_json')))
    except (UnicodeError,json.JSONDecodeError,RecursionError,ValueError) as exc:
        if isinstance(exc,QuoteReceiptError):
            raise
        raise QuoteReceiptError('quote_invalid_source_json') from None


def _safe(path):
    current=Path(path.anchor)
    try:
        for part in path.parts[1:]:
            current/=part
            info=current.lstat()
            _need(not stat.S_ISLNK(info.st_mode) and not
                (getattr(info,'st_file_attributes',0)&getattr(stat,'FILE_ATTRIBUTE_REPARSE_POINT',1024)),
                'quote_source_reparse_point')
            _need(stat.S_ISREG(info.st_mode) if current==path else stat.S_ISDIR(info.st_mode),'quote_source_path_type')
    except OSError:
        raise QuoteReceiptError('quote_source_unavailable') from None


def capture_quote_snapshot(path=DEFAULT_PATH, *, clock=time.time):
    """Retain exact local bytes and read clocks; no classification or broker call."""
    try:
        path=Path(os.path.abspath(os.fspath(path)))
    except (TypeError,ValueError,OSError):
        raise QuoteReceiptError('quote_source_path_invalid') from None
    started=_epoch(clock())
    _safe(path)
    try:
        before=path.lstat()
        _need(0<before.st_size<=MAX_BYTES,'quote_raw_byte_limit')
        with path.open('rb') as handle:
            opened=os.fstat(handle.fileno())
            raw=handle.read(MAX_BYTES+1)
            finished=os.fstat(handle.fileno())
        after=path.lstat()
    except OSError:
        raise QuoteReceiptError('quote_source_read_failed') from None
    _safe(path)
    identity=lambda value:(value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns)
    _need(0<len(raw)<=MAX_BYTES and len({identity(value) for value in (before,opened,finished,after)})==1,
          'quote_source_changed_during_read')
    checksum=hashlib.sha256(raw).hexdigest()
    completed=_epoch(clock())
    _need(started<=completed,'quote_read_clock_reversed')
    body=dict(schema_version=RECEIPT_SCHEMA,reader_version=VERSION,source_path=str(path),
        raw_sha256=checksum,raw_bytes=len(raw),read_started_epoch=started,read_completed_epoch=completed,
        scope='local_observation_not_broker_fill',**FLAGS)
    return {'raw_bytes':raw,'receipt':{**body,'receipt_sha256':_hash(body)}}


def _receipt(raw,value):
    keys={'schema_version','reader_version','source_path','raw_sha256','raw_bytes','read_started_epoch',
          'read_completed_epoch','scope','receipt_sha256',*FLAGS}
    _need(isinstance(value,dict) and set(value)==keys,'quote_receipt_shape')
    _need(value['schema_version']==RECEIPT_SCHEMA and value['reader_version']==VERSION and
        value['scope']=='local_observation_not_broker_fill' and all(value.get(k) is v for k,v in FLAGS.items()),
        'quote_receipt_identity_or_authority')
    _need(type(raw) is bytes and 0<len(raw)<=MAX_BYTES and type(value['raw_bytes']) is int
        and value['raw_bytes']==len(raw) and value['raw_sha256']==hashlib.sha256(raw).hexdigest(),
        'quote_receipt_raw_mismatch')
    _need(isinstance(value['source_path'],str) and len(value['source_path'])<=4096,'quote_receipt_source_path')
    _need(value['receipt_sha256']==_hash({k:v for k,v in value.items() if k!='receipt_sha256'}),'quote_receipt_hash')
    start,complete=_epoch(value['read_started_epoch']),_epoch(value['read_completed_epoch'])
    _need(start<=complete,'quote_read_clock_reversed')
    return complete


def _list(value):
    _need(isinstance(value,list) and len(value)<=MAX_QUOTES and all(isinstance(p,str) and _PAIR.fullmatch(p) for p in value)
          and len(value)==len(set(value)),'quote_coverage_pair_list')
    return set(value)


def _integer(value):
    _need(type(value) is int and 0<=value<=MAX_QUOTES,'quote_coverage_count')
    return value


def _source(value,observed):
    _need(isinstance(value,dict) and set(value)==_TOP,'quote_snapshot_shape')
    _need(type(value['schema_version']) is int and value['schema_version']==3 and
          value['producer']=='practice_007_dedicated_quote_stream' and value['research_only'] is True,
          'quote_snapshot_identity')
    generation=value['connection_generation']
    _need(type(generation) is int and 0<generation<2**31,'quote_connection_generation')
    coverage=value['coverage']
    _need(isinstance(coverage,dict) and set(coverage)==_COVERAGE,'quote_coverage_schema')
    _need(type(coverage['connection_generation']) is int and coverage['connection_generation']==generation,
          'quote_coverage_generation_mismatch')
    _need(coverage['tradeability_contract']=='oanda_client_price_status_boolean_v1'
        and coverage['execution_requires_independent_freshness_check'] is True
        and coverage['retained_quotes_execution_eligible'] is False,'quote_tradeability_contract')
    quotes=value['quotes']
    _need(isinstance(quotes,dict) and len(quotes)<=MAX_QUOTES and all(isinstance(p,str) and _PAIR.fullmatch(p) for p in quotes),
          'quote_inventory_shape')
    retained=_list(coverage['retained_last_known_instruments'])
    nontrade=_list(coverage['current_non_tradeable_instruments'])
    unknown=_list(coverage['current_tradeability_unknown_instruments'])
    _need(retained<=set(quotes) and not retained&nontrade and not retained&unknown and not nontrade&unknown,
          'quote_coverage_conflict')
    current=set(quotes)-retained
    _need(nontrade<=current and unknown<=current,'quote_coverage_conflict')
    actual_nontrade=set()
    actual_unknown=set()
    for pair in current:
        row=quotes[pair]
        _need(isinstance(row,dict),'quote_row_shape')
        if row.get('tradeable') is False:
            actual_nontrade.add(pair)
        elif row.get('tradeable') is not True:
            actual_unknown.add(pair)
    counts={'current_quote_count':len(current),'last_known_quote_count':len(quotes),
        'retained_last_known_count':len(retained),'current_non_tradeable_quote_count':len(nontrade),
        'current_tradeability_unknown_count':len(unknown),'current_tradeable_quote_count':len(current)-len(nontrade)-len(unknown)}
    _need(_integer(value['quote_count'])==len(quotes),'quote_inventory_count')
    _need(all(_integer(coverage[key])==number for key,number in counts.items())
        and actual_nontrade==nontrade and actual_unknown==unknown,'quote_coverage_count_mismatch')
    generated=_time(value['generated_utc'])
    _need(generated<=Decimal(str(observed)),'quote_snapshot_future_at_observation')
    return quotes,retained,nontrade,unknown,generated


def _number(value):
    _need(type(value) in (int,Decimal),'quote_numeric_json_required')
    answer=Decimal(value)
    _need(answer.is_finite() and 0<answer<Decimal('1e12') and len(answer.as_tuple().digits)<=32
          and -30<=answer.as_tuple().exponent<=30,'quote_price_invalid')
    return answer


def _age(later,earlier):
    with localcontext(Context(prec=64)):
        return Decimal(str(later))-earlier


def map_quote_snapshot(raw_bytes,receipt,*,decision_epoch,instruments=('EUR_USD','GBP_USD','USD_JPY'),maximum_quote_age_sec=30):
    """Map only declared pairs using actual observed availability and source clocks.

    Wrapper/receipt defects raise fixed codes; individual unavailable instruments
    return explicit refusals. A retained or unknown/nontradeable row is withheld
    regardless of its prices. Old source evidence is never refreshed by reread.
    """
    observed=_receipt(raw_bytes,receipt)
    decision=_epoch(decision_epoch)
    _need(decision>=observed,'quote_decision_precedes_observation')
    _need(type(maximum_quote_age_sec) in (int,float) and math.isfinite(maximum_quote_age_sec)
          and 0<=maximum_quote_age_sec<=60,'quote_age_policy')
    _need(isinstance(instruments,(list,tuple)) and 1<=len(instruments)<=3
          and all(isinstance(pair,str) and pair in METADATA for pair in instruments)
          and len(instruments)==len(set(instruments)),'quote_selected_inventory')
    value=_decode(raw_bytes)
    quotes,retained,nontrade,unknown,generated=_source(value,observed)
    current,refusals={},{}
    for pair in instruments:
        try:
            _need(_age(decision,generated)<=90,'quote_snapshot_stale')
            _need(pair in quotes,'quote_pair_missing')
            _need(pair not in retained,'quote_retained_previous_connection')
            _need(pair not in nontrade,'quote_nontradeable')
            _need(pair not in unknown,'quote_tradeability_unknown')
            row=quotes[pair]
            _need(set(row)=={'ask','bid','pip','source','time','tradeable'} and row['tradeable'] is True,'quote_row_shape')
            _need(row['source']=='stream','quote_not_stream_source')
            market=_time(row['time'])
            _need(market<=generated and market<=Decimal(str(observed)),'quote_market_future_at_observation')
            _need(_age(decision,market)<=Decimal(str(maximum_quote_age_sec)),'quote_market_stale')
            bid,ask,pip=map(_number,(row['bid'],row['ask'],row['pip']))
            _need(bid<=ask,'quote_crossed')
            _need(pip==Decimal(METADATA[pair]['pip_size']),'quote_pip_metadata_mismatch')
            selected=dict(instrument=pair,bid=str(bid),ask=str(ask),pip_size=str(pip),
                market_time_rfc3339=row['time'],market_epoch=float(market),available_epoch=observed,
                tradeable=True,source='stream',connection_generation=value['connection_generation'],
                raw_snapshot_sha256=receipt['raw_sha256'],capture_receipt_sha256=receipt['receipt_sha256'])
            selected['quote_id']='research_quote_v1_'+_hash(selected)
            current[pair]=selected
        except QuoteReceiptError as exc:
            refusals[pair]=str(exc)
    header=dict(schema_version=value['schema_version'],producer=value['producer'],generated_utc=value['generated_utc'],
        generated_epoch=float(generated),connection_generation=value['connection_generation'],quote_count=value['quote_count'],
        coverage=value['coverage'],raw_sha256=receipt['raw_sha256'],capture_receipt_sha256=receipt['receipt_sha256'],
        read_started_epoch=receipt['read_started_epoch'],read_completed_epoch=observed)
    body=dict(schema_version=MAP_SCHEMA,status='available' if current else 'unavailable',quotes=current,refusals=refusals,
        metadata={pair:dict(METADATA[pair]) for pair in instruments},source_header=header,
        source_header_sha256=_hash(header),decision_epoch=decision,maximum_quote_age_sec=maximum_quote_age_sec,
        scope='observed_quotes_not_broker_fills_or_execution_authority',**FLAGS)
    return {**body,'mapping_sha256':_hash(body)}
