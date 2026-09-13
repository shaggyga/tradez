"""One bounded practice M1/MBA GET and pure official-M research mapping.

Caller retains raw bytes/receipt and CSV separately. No account request, order,
runner, resampling, fill, inference or file write. Candle prices do not establish
tradeability. Original availability is the actual response read completion.
"""
from datetime import datetime, timezone
from decimal import Decimal
import csv
import hashlib
import io
import json
import math
from pathlib import Path
import re
import time
from types import MappingProxyType

import requests

SCHEMA='m1_mba_research_capture_v1_20260909'
MAPPING_SCHEMA='m1_mba_research_mapping_v1_20260909'
PAIRS=('EUR_USD','GBP_USD','USD_JPY')
HOST='https://api-fxpractice.oanda.com'
PARAMS={'granularity':'M1','price':'MBA','count':300,'smooth':'false'}
MAX_BYTES=512*1024
MAX_SECONDS=20
PARSER_NAME='oanda_live_account_readonly_status.py'
PARSER_SHA='2734dda34d532a094eff614d5fe05d01c7aa6a2ebd80ca2bb974f12d8c5d93af'
FLAGS=dict(research_only=True,orders_enabled=False,can_place_orders=False,can_promote=False,
    can_authorize=False,account_eligible=False,proof_eligible=False,execution_eligible=False,
    account_access=False,manager_activation=False,forecast_issued=False)
LIMITS=dict(response_bytes=MAX_BYTES,total_seconds=MAX_SECONDS,connect_timeout_sec=3.05,read_timeout_sec=8)
_ROOT=Path(__file__).resolve().parent
_BOUND=MappingProxyType({name:hashlib.sha256((_ROOT/name).read_bytes()).hexdigest()
    for name in (Path(__file__).name,PARSER_NAME)})
if _BOUND[PARSER_NAME]!=PARSER_SHA:
    raise ValueError('m1_credential_parser_source_changed')


def _need(ok,reason):
    if not ok:raise ValueError('m1_'+reason)


def _canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def _sha(raw):return hashlib.sha256(raw).hexdigest()
def _hash(value):return _sha(_canonical(value))
def source_bindings():return dict(_BOUND)


def verify_source_bindings_on_disk():
    """Explicit read-only lifecycle check, separate from the pure mapper."""
    _need(all(_sha((_ROOT/name).read_bytes())==sha for name,sha in _BOUND.items()),'loaded_source_changed_on_disk')
    return source_bindings()


def _clock(value):
    _need(type(value) in (int,float) and math.isfinite(value) and 0<value<10**11,'actual_clock')
    return value


def _metadata(value,instrument):
    _need(instrument in PAIRS,'instrument_allowlist')
    _need(type(value) is dict and set(value)=={'instrument','base_currency','quote_currency','pip_size'},'metadata_fields')
    _need((value['instrument'],value['base_currency'],value['quote_currency'])==(instrument,instrument[:3],instrument[4:]),'metadata_identity')
    _need(type(value['pip_size']) is str and re.fullmatch(r'0\.[0-9]{1,8}',value['pip_size']),'metadata_exact_pip')
    _need(Decimal(value['pip_size'])==Decimal('0.01' if instrument=='USD_JPY' else '0.0001'),'metadata_pip_value')
    return dict(value)


def _request(instrument):
    _need(instrument in PAIRS,'instrument_allowlist')
    return dict(method='GET',url=HOST+'/v3/instruments/'+instrument+'/candles',params=dict(PARAMS),
        request_count=1,automatic_retries=0,redirects_allowed=False)


def _price(value):
    _need(type(value) is str and len(value)<=48 and re.fullmatch(r'[0-9]+(?:\.[0-9]+)?',value),'exact_price_text')
    number=Decimal(value)
    _need(0<number<Decimal('1e12') and len(number.as_tuple().digits)<=32,'price_magnitude_or_precision')
    return number


def _label(value):
    _need(type(value) is str and re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.0{1,9})?(?:Z|\+00:00)',value),'UTC_minute_label')
    try:epoch=int(datetime.fromisoformat(value.replace('Z','+00:00')).timestamp())
    except (ValueError,OverflowError):raise ValueError('m1_UTC_minute_label') from None
    _need(epoch>0 and epoch%60==0,'minute_label_grid')
    return epoch


def _parse(raw,instrument,observed):
    _need(type(raw) is bytes and 0<len(raw)<=MAX_BYTES,'raw_byte_bound')
    def pairs(items):
        out={}
        for k,v in items:
            _need(k not in out,'duplicate_json_key');out[k]=v
        return out
    try:
        payload=json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda _:(_ for _ in ()).throw(ValueError('m1_nonfinite_json')))
    except (UnicodeError,json.JSONDecodeError,RecursionError):raise ValueError('m1_invalid_response_json') from None
    _need(type(payload) is dict and set(payload)=={'instrument','granularity','candles'} and
          payload['instrument']==instrument and payload['granularity']=='M1','response_identity')
    candles=payload['candles']
    _need(type(candles) is list and 1<=len(candles)<=300,'candle_count')
    complete=[];incomplete=[];last=None
    for candle in candles:
        _need(type(candle) is dict and set(candle)=={'complete','volume','time','bid','ask','mid'},'candle_fields')
        _need(type(candle['complete']) is bool,'explicit_complete_flag')
        label=_label(candle['time'])
        _need(last is None or label>last,'duplicate_or_unordered_candles');last=label
        _need(Decimal(label)<=Decimal(str(observed)),'future_candle_label')
        _need(type(candle['volume']) is int and 0<=candle['volume']<=10**12,'volume')
        nums={}
        for side in ('mid','bid','ask'):
            series=candle[side]
            _need(type(series) is dict and set(series)==set('ohlc'),'OHLC_fields')
            n={k:_price(series[k]) for k in 'ohlc'}
            _need(n['l']<=n['o']<=n['h'] and n['l']<=n['c']<=n['h'],'OHLC_geometry');nums[side]=n
        _need(all(nums['bid'][k]<=nums['mid'][k]<=nums['ask'][k] for k in 'ohlc'),'MBA_order')
        row=dict(instrument=instrument,bar_label_epoch=label,price_epoch=label+60,available_epoch=observed,
            time_text=candle['time'],volume=candle['volume'],complete=candle['complete'],
            **{side:dict(candle[side]) for side in ('mid','bid','ask')})
        if candle['complete']:
            _need(not incomplete,'complete_after_incomplete')
            _need(Decimal(label+60)<=Decimal(str(observed)),'future_complete_price')
            complete.append(row)
        else:incomplete.append(row)
    return complete,incomplete


def _coverage(rows):
    labels=[r['bar_label_epoch'] for r in rows]
    gaps=[dict(after_bar_label_epoch=a,before_bar_label_epoch=b,missing_m1_intervals=(b-a)//60-1)
        for a,b in zip(labels,labels[1:]) if b-a!=60]
    suffix=0
    for row in reversed(rows):
        if suffix and rows[len(rows)-suffix]['bar_label_epoch']-row['bar_label_epoch']!=60:break
        suffix+=1
    return dict(complete_provider_response=True,source_query_complete=True,
        first_complete_bar_label_epoch=labels[0] if labels else None,last_complete_bar_label_epoch=labels[-1] if labels else None,
        first_complete_price_epoch=labels[0]+60 if labels else None,last_complete_price_epoch=labels[-1]+60 if labels else None,
        complete_row_count=len(rows),gaps=gaps,missing_m1_intervals=sum(r['missing_m1_intervals'] for r in gaps),
        consecutive_complete_suffix_rows=suffix,
        scope='entire_returned_complete_domain_only;gaps_are_missing_not_filled;no_coverage_outside_first_last',
        tradeability_observed=False)


def _token(path):
    from oanda_live_account_readonly_status import read_creds,cfg_value
    text=read_creds(Path(path))
    token=cfg_value(text,'OANDA_API_KEY','OANDA_API_TOKEN')
    del text
    _need(type(token) is str and bool(token),'credential_unavailable')
    return token


def capture_once(instrument,*,credential_path,metadata,clock=time.time):
    """One GET. Retain bounded raw bytes even on validation failure; never retry.

    Failed receipts are diagnostic only and cannot be mapped. Timeouts bound
    accepted responses, not hard process cancellation during all DNS/stalls.
    """
    metadata=_metadata(metadata,instrument);bindings=verify_source_bindings_on_disk();request=_request(instrument)
    receipt=dict(schema_version=SCHEMA,status='failed',instrument=instrument,metadata=metadata,
        source_bindings=bindings,request=request,limits=dict(LIMITS),dependency_versions={'requests':requests.__version__},
        complete_provider_response=False,source_query_complete=False,request_started_epoch=None,
        request_started_utc=None,read_completed_epoch=None,first_observed_epoch=None,capture_completed_epoch=None,
        request_duration_sec=None,http_status=None,response_headers={},**FLAGS)
    body=b'';parts=bytearray();started=None;observed=None;token=None
    try:
        token=_token(credential_path)
        with requests.Session() as session:
            session.trust_env=False
            session.mount('https://',requests.adapters.HTTPAdapter(max_retries=0))
            started=_clock(clock());mono=time.monotonic()
            receipt.update(request_started_epoch=started,request_started_utc=datetime.fromtimestamp(started,timezone.utc).isoformat())
            with session.get(request['url'],params=dict(PARAMS),headers={'Authorization':'Bearer '+token},
                    timeout=(3.05,8),allow_redirects=False,stream=True) as response:
                receipt['http_status']=response.status_code
                receipt['response_headers']={key:str(response.headers[key])[:256] for key in ('Date','Content-Type','RequestID') if key in response.headers}
                for block in response.iter_content(chunk_size=8192):
                    _need(type(block) is bytes,'response_chunk_type')
                    remaining=MAX_BYTES-len(parts);parts.extend(block[:remaining])
                    _need(len(block)<=remaining and time.monotonic()-mono<=MAX_SECONDS,'response_budget')
                observed=_clock(clock());body=bytes(parts)
                receipt.update(read_completed_epoch=observed,first_observed_epoch=observed,
                    request_duration_sec=observed-started,complete_provider_response=True,source_query_complete=True)
        _need(started<=observed and observed-started<=MAX_SECONDS and time.monotonic()-mono<=MAX_SECONDS,'response_clock_budget')
        _need(receipt['http_status']==200,'http_status')
        complete,incomplete=_parse(body,instrument,observed)
        verify_source_bindings_on_disk()
        completed=_clock(clock())
        _need(observed<=completed and completed-started<=MAX_SECONDS and time.monotonic()-mono<=MAX_SECONDS,'response_clock_budget')
        receipt.update(status='captured_not_issued',capture_completed_epoch=completed,
            candle_count=len(complete)+len(incomplete),complete_candle_count=len(complete),
            incomplete_candle_count=len(incomplete),coverage=_coverage(complete))
    except Exception as error:
        body=bytes(parts)
        # Never serialize exception text or request objects containing a token.
        reason=str(error)
        receipt.update(error_type=type(error).__name__,error_reason=reason if type(error) is ValueError and re.fullmatch(r'm1_[A-Za-z0-9_]{1,80}',reason) else 'm1_request_or_validation_failed')
        try:receipt['capture_completed_epoch']=_clock(clock())
        except Exception:pass
    finally:token=None
    receipt['raw_response']=dict(sha256=_sha(body),bytes=len(body),retention_scope='complete_response' if receipt['complete_provider_response'] else 'partial_or_no_response')
    receipt['http_date_scope']='server_diagnostic_only_not_local_observation_or_tradeability'
    receipt['receipt_sha256']=_hash(receipt)
    return body,receipt


def _csv(rows):
    fields=('time','instrument','granularity','complete','open','high','low','close',
        'bid_open','bid_high','bid_low','bid_close','ask_open','ask_high','ask_low','ask_close','volume')
    buffer=io.StringIO(newline='');writer=csv.DictWriter(buffer,fieldnames=fields,lineterminator='\n');writer.writeheader()
    names=dict(o='open',h='high',l='low',c='close')
    for row in rows:
        mapped=dict(time=row['time_text'],instrument=row['instrument'],granularity='M1',complete='true',volume=row['volume'])
        for side,prefix in (('mid',''),('bid','bid_'),('ask','ask_')):
            mapped.update({prefix+name:row[side][short] for short,name in names.items()})
        writer.writerow(mapped)
    return buffer.getvalue().encode('utf-8')


def map_verified_capture(raw,receipt,metadata):
    """Replay exact retained bytes with original availability, without I/O.

    Returned canonical_csv_bytes is separately hashed; use mapping_document to
    obtain the JSON-safe sealed mapping. Neither hashes nor this replay prove
    remote authentication or unknown historical training-ingestion equivalence.
    """
    _need(type(receipt) is dict and receipt.get('schema_version')==SCHEMA and receipt.get('status')=='captured_not_issued','successful_receipt')
    keys={'schema_version','status','instrument','metadata','source_bindings','request','limits','dependency_versions',
        'complete_provider_response','source_query_complete','request_started_epoch','request_started_utc',
        'read_completed_epoch','first_observed_epoch','capture_completed_epoch','request_duration_sec','http_status',
        'response_headers','candle_count','complete_candle_count','incomplete_candle_count','coverage',
        'raw_response','http_date_scope','receipt_sha256',*FLAGS}
    _need(set(receipt)==keys,'receipt_fields')
    _need(receipt['receipt_sha256']==_hash({k:v for k,v in receipt.items() if k!='receipt_sha256'}),'receipt_seal')
    _need(all(receipt[k] is v for k,v in FLAGS.items()),'receipt_flags')
    instrument=receipt['instrument'];metadata=_metadata(metadata,instrument)
    _need(receipt['metadata']==metadata and receipt['source_bindings']==source_bindings(),'receipt_metadata_or_source_binding')
    _need(_canonical(receipt['request'])==_canonical(_request(instrument)) and _canonical(receipt['limits'])==_canonical(LIMITS),'request_binding')
    _need(type(receipt['http_status']) is int and receipt['http_status']==200 and receipt['complete_provider_response'] is True and receipt['source_query_complete'] is True,'complete_response_required')
    start=_clock(receipt['request_started_epoch']);read=_clock(receipt['read_completed_epoch']);done=_clock(receipt['capture_completed_epoch'])
    first=_clock(receipt['first_observed_epoch']);duration=receipt['request_duration_sec']
    _need(type(duration) in (int,float) and math.isfinite(duration) and duration>=0,'receipt_duration_type')
    _need(start<=read<=done and done-start<=MAX_SECONDS and first==read and
          duration==read-start,'receipt_clock_order')
    _need(receipt['request_started_utc']==datetime.fromtimestamp(start,timezone.utc).isoformat(),'receipt_clock_text')
    _need(type(raw) is bytes and receipt['raw_response']==dict(sha256=_sha(raw),bytes=len(raw),retention_scope='complete_response'),'raw_binding')
    complete,incomplete=_parse(raw,instrument,read);coverage=_coverage(complete)
    _need(receipt['coverage']==coverage and type(receipt['candle_count']) is int and receipt['candle_count']==len(complete)+len(incomplete)
          and type(receipt['complete_candle_count']) is int and receipt['complete_candle_count']==len(complete)
          and type(receipt['incomplete_candle_count']) is int and receipt['incomplete_candle_count']==len(incomplete),'coverage_or_count_binding')
    csv_bytes=_csv(complete)
    body=dict(schema_version=MAPPING_SCHEMA,instrument=instrument,metadata=metadata,source_sha256=_sha(raw),
        receipt_sha256=receipt['receipt_sha256'],source_bindings=source_bindings(),first_observed_epoch=read,
        available_epoch=read,price_convention='official_midpoint',complete_rows=complete,incomplete_rows=incomplete,
        canonical_csv=dict(sha256=_sha(csv_bytes),bytes=len(csv_bytes),row_count=len(complete),price_convention='official_midpoint'),
        coverage=coverage,status='complete_rows_available' if complete else 'no_complete_rows',
        source_attestation=dict(instrument=instrument,granularity='M1',raw_sha256=_sha(raw),receipt_sha256=receipt['receipt_sha256'],
            read_started_epoch=start,read_completed_epoch=read,complete_provider_response=True,source_query_complete=True,
            first_complete_bar_label_epoch=coverage['first_complete_bar_label_epoch'],last_complete_bar_label_epoch=coverage['last_complete_bar_label_epoch'],
            scope=coverage['scope']),
        semantics=dict(bar_label='original_provider_start_label',price_epoch='bar_label_plus60_complete_close',
            availability='original_actual_read_completion_not_replay_time',prices='original_exact_decimal_M_B_A_lexemes',
            midpoint_training_ingestion_equivalence_proven=False,original_training_ingestion_availability_proven=False,
            smoothing=False,fabricated_bars=False,aggregated_from_S5=False,inferred_spread=False,tradeability_observed=False,
            warmup='downstream_requires204_consecutive_real_bars_and_UTC_label_mod300_zero;not_imputed_here'),**FLAGS)
    return {**body,'mapping_sha256':_hash(body),'canonical_csv_bytes':csv_bytes}


def mapping_document(mapping):
    """Validate both seals and produce detached JSON-safe source evidence."""
    _need(type(mapping) is dict and mapping.get('schema_version')==MAPPING_SCHEMA,'mapping_schema')
    raw=mapping.get('canonical_csv_bytes');body={k:v for k,v in mapping.items() if k not in ('canonical_csv_bytes','mapping_sha256')}
    _need(type(raw) is bytes and type(body.get('canonical_csv')) is dict and
          body['canonical_csv'].get('sha256')==_sha(raw) and body['canonical_csv'].get('bytes')==len(raw),'CSV_binding')
    _need(all(body.get(k) is v for k,v in FLAGS.items()) and body.get('source_bindings')==source_bindings(),'mapping_flags_or_source')
    _need(mapping.get('mapping_sha256')==_hash(body),'mapping_seal')
    return json.loads(_canonical({**body,'mapping_sha256':mapping['mapping_sha256']}))
