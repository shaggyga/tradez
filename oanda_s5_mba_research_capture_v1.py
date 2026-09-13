"""Bounded practice candle GET and pure exact-text research mapping.

No file writes, account requests, order methods, runners or inference. The caller
retains returned raw bytes and receipt. A candle does not prove tradeability.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Context, Decimal, localcontext
import hashlib
import json
import math
from pathlib import Path
import re
import time

import requests

SCHEMA = 's5_mba_research_capture_v1_20260909'
MAPPING_SCHEMA = 's5_mba_research_mapping_v1_20260909'
PAIRS = ('EUR_USD', 'GBP_USD', 'USD_JPY')
CONVENTIONS = ('official_midpoint', 'ba_derived_midpoint')
HOST = 'https://api-fxpractice.oanda.com'
PARAMS = {'granularity':'S5', 'price':'MBA', 'count':120, 'smooth':'false'}
MAX_BYTES = 256 * 1024
MAX_SECONDS = 15
PARSER_NAME = 'oanda_live_account_readonly_status.py'
PARSER_SHA256 = '2734dda34d532a094eff614d5fe05d01c7aa6a2ebd80ca2bb974f12d8c5d93af'
FLAGS = {'research_only':True, 'can_place_orders':False, 'can_promote':False,
    'can_authorize':False, 'account_eligible':False, 'proof_eligible':False,
    'execution_eligible':False, 'account_access':False, 'forecast_issued':False}
SOURCE_ARITHMETIC = {'decimal_precision':192,'float_conversion':'after_exact_source_arithmetic_for_feature_input_only'}
LIMITS = {'response_bytes':MAX_BYTES, 'total_seconds':MAX_SECONDS,
          'connect_timeout_sec':3.05, 'read_timeout_sec':8}
ERROR_CODES = frozenset({'practice_credential_unavailable','non_200_response','response_budget_exceeded',
    'actual_clock_reversed','credential_parser_source_changed','invalid_actual_clock',
    'bounded_raw_bytes_required','duplicate_json_key','nonfinite_json_number','invalid_response_json',
    'response_identity_or_schema','response_candle_count','candle_schema','explicit_complete_flag_required',
    'utc_integer_s5_label_required','s5_label_grid_required','duplicate_or_unordered_source_candles',
    'future_candle_label','invalid_candle_volume','ohlc_schema','exact_decimal_ohlc_string_required',
    'nonpositive_ohlc_price','ohlc_range_mismatch','bid_mid_ask_ohlc_order',
    'complete_candle_after_incomplete','future_complete_candle'})


def _require(condition, reason):
    if not condition:
        raise ValueError(reason)


def _canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def _hash(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _clock(value):
    _require(type(value) in (int,float) and math.isfinite(value) and value>0,'invalid_actual_clock')
    return float(value)


def _sources():
    source=Path(__file__).resolve()
    parser=source.with_name(PARSER_NAME)
    parser_sha=hashlib.sha256(parser.read_bytes()).hexdigest()
    _require(parser_sha==PARSER_SHA256,'credential_parser_source_changed')
    return {source.name:hashlib.sha256(source.read_bytes()).hexdigest(),PARSER_NAME:parser_sha}


def _metadata(metadata, instrument):
    _require(instrument in PAIRS,'instrument_not_allowlisted')
    _require(isinstance(metadata,dict) and set(metadata)=={'instrument','base_currency','quote_currency','pip_size'},
             'explicit_registry_metadata_required')
    _require(metadata['instrument']==instrument and metadata['base_currency']==instrument[:3]
             and metadata['quote_currency']==instrument[4:],'metadata_identity_mismatch')
    pip=metadata['pip_size']
    _require(type(pip) in (int,float,str),'metadata_pip_required')
    _require(len(str(pip))<=64,'metadata_pip_required')
    try: decimal=Decimal(str(pip))
    except Exception: raise ValueError('metadata_pip_required') from None
    _require(decimal.is_finite() and Decimal('0')<decimal<Decimal('1')
        and len(decimal.as_tuple().digits)<=32 and -18<=decimal.as_tuple().exponent<=0,'metadata_pip_required')
    # Return the exact JSON-owned registry value, without suffix inference.
    return dict(metadata), decimal


def _request(instrument):
    _require(instrument in PAIRS,'instrument_not_allowlisted')
    return {'method':'GET','url':HOST+'/v3/instruments/'+instrument+'/candles',
        'params':dict(PARAMS),'request_count':1,'automatic_retries':0,'redirects_allowed':False}


def _price(text):
    _require(isinstance(text,str) and len(text)<=64 and re.fullmatch(r'[0-9]+(?:\.[0-9]+)?',text),
             'exact_decimal_ohlc_string_required')
    value=Decimal(text)
    _require(value.is_finite() and Decimal('1e-30')<=value<=Decimal('1e20'),'nonpositive_ohlc_price')
    return value


def _label(text):
    # Check fractional nanoseconds before datetime can discard them.
    _require(isinstance(text,str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.0{1,9})?(?:Z|\+00:00)',text),
             'utc_integer_s5_label_required')
    try: value=datetime.fromisoformat(text.replace('Z','+00:00')).timestamp()
    except ValueError: raise ValueError('utc_integer_s5_label_required') from None
    _require(value>0 and value%5==0,'s5_label_grid_required')
    return value


def _parse_response(raw, instrument, observed):
    _require(isinstance(raw,bytes) and 0<len(raw)<=MAX_BYTES,'bounded_raw_bytes_required')
    def pairs(items):
        result={}
        for key,value in items:
            _require(key not in result,'duplicate_json_key')
            result[key]=value
        return result
    try:
        payload=json.loads(raw,object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite_json_number')))
    except (UnicodeError,json.JSONDecodeError,RecursionError):
        raise ValueError('invalid_response_json') from None
    _require(isinstance(payload,dict) and set(payload)=={'instrument','granularity','candles'}
             and payload['instrument']==instrument and payload['granularity']=='S5','response_identity_or_schema')
    candles=payload['candles']
    _require(isinstance(candles,list) and 1<=len(candles)<=PARAMS['count'],'response_candle_count')
    complete=[];incomplete=[];previous=None;incomplete_seen=False
    for candle in candles:
        _require(isinstance(candle,dict) and set(candle)=={'complete','volume','time','bid','ask','mid'},'candle_schema')
        _require(type(candle['complete']) is bool,'explicit_complete_flag_required')
        epoch=_label(candle['time'])
        _require(previous is None or epoch>previous,'duplicate_or_unordered_source_candles')
        previous=epoch
        _require(epoch<=observed,'future_candle_label')
        _require(type(candle['volume']) is int and 0<=candle['volume']<=10**12,'invalid_candle_volume')
        values={}
        for side in ('bid','ask','mid'):
            series=candle[side]
            _require(isinstance(series,dict) and set(series)==set('ohlc'),'ohlc_schema')
            numbers={key:_price(series[key]) for key in 'ohlc'}
            _require(numbers['l']<=numbers['o']<=numbers['h'] and numbers['l']<=numbers['c']<=numbers['h'],
                     'ohlc_range_mismatch')
            values[side]=numbers
        _require(all(values['bid'][key]<=values['mid'][key]<=values['ask'][key] for key in 'ohlc'),
                 'bid_mid_ask_ohlc_order')
        row={'instrument':instrument,'bar_label_epoch':epoch,'price_epoch':epoch+5,
            'available_epoch':observed,'time_text':candle['time'],'volume':candle['volume'],
            'complete':candle['complete'],
            **{side:dict(candle[side]) for side in ('bid','ask','mid')}}
        if candle['complete']:
            _require(not incomplete_seen,'complete_candle_after_incomplete')
            _require(epoch+5<=observed,'future_complete_candle')
            complete.append(row)
        else:
            incomplete_seen=True
            incomplete.append(row)
    return complete,incomplete


def _coverage(complete):
    labels=[row['bar_label_epoch'] for row in complete]
    gaps=[{'after_bar_label_epoch':a,'before_bar_label_epoch':b,'missing_s5_intervals':int((b-a)/5)-1}
          for a,b in zip(labels,labels[1:]) if b-a!=5]
    return {'first_complete_bar_label_epoch':labels[0] if labels else None,
        'last_complete_bar_label_epoch':labels[-1] if labels else None,
        'first_complete_price_epoch':labels[0]+5 if labels else None,
        'last_complete_price_epoch':labels[-1]+5 if labels else None,
        'complete_provider_response':True,'source_query_complete':True,
        'scope':'returned_complete_rows_only_not_continuity_or_tradeability',
        'complete_row_count':len(complete),'gaps':gaps,
        'missing_s5_intervals':sum(gap['missing_s5_intervals'] for gap in gaps),
        'absent_intervals_are_not_zero_moves':True,'tradeability_observed':False}


def capture_once(instrument, *, credential_path, metadata, clock=time.time):
    """Return (raw bytes, sealed receipt); failures return empty bytes + receipt.

    Exactly one GET is attempted after local validation. No account ID is
    selected or sent. Error text never includes raw request/response objects.
    """
    metadata,_=_metadata(metadata,instrument)
    bindings=_sources()
    request=_request(instrument)
    started=_clock(clock());monotonic_start=time.monotonic()
    receipt={'schema_version':SCHEMA,'status':'failed','instrument':instrument,
        'metadata':metadata,'source_bindings':bindings,'request':request,'limits':dict(LIMITS),
        'request_started_epoch':started,'request_started_utc':datetime.fromtimestamp(started,timezone.utc).isoformat(),
        'dependency_versions':{'requests':requests.__version__},**FLAGS}
    body=b''
    try:
        from oanda_live_account_readonly_status import cfg_value, read_creds
        text=read_creds(Path(credential_path))
        token=cfg_value(text,'OANDA_API_KEY','OANDA_API_TOKEN')
        del text
        _require(isinstance(token,str) and bool(token),'practice_credential_unavailable')
        with requests.Session() as session:
            session.trust_env=False
            # requests' default adapters have no retries; set this explicitly.
            session.mount('https://',requests.adapters.HTTPAdapter(max_retries=0))
            with session.get(request['url'],params=dict(PARAMS),
                    headers={'Authorization':'Bearer '+token},timeout=(3.05,8),
                    allow_redirects=False,stream=True) as response:
                receipt['http_status']=response.status_code
                _require(response.status_code==200,'non_200_response')
                receipt['response_headers']={key:str(response.headers[key])[:256]
                    for key in ('Date','Content-Type','RequestID') if key in response.headers}
                parts=bytearray()
                for block in response.iter_content(chunk_size=8192):
                    parts.extend(block)
                    _require(len(parts)<=MAX_BYTES and time.monotonic()-monotonic_start<=MAX_SECONDS,'response_budget_exceeded')
                observed=_clock(clock())
        del token
        _require(time.monotonic()-monotonic_start<=MAX_SECONDS,'response_budget_exceeded')
        _require(observed>=started,'actual_clock_reversed')
        _require(observed-started<=MAX_SECONDS,'response_budget_exceeded')
        candidate=bytes(parts)
        complete,incomplete=_parse_response(candidate,instrument,observed)
        completed=_clock(clock())
        _require(completed>=observed,'actual_clock_reversed')
        _require(completed-started<=MAX_SECONDS and time.monotonic()-monotonic_start<=MAX_SECONDS,'response_budget_exceeded')
        body=candidate
        receipt.update(status='captured_not_issued',read_completed_epoch=observed,
            first_observed_epoch=observed,capture_completed_epoch=completed,
            request_duration_sec=observed-started,
            raw_response={'sha256':hashlib.sha256(body).hexdigest(),'bytes':len(body)},
            complete_provider_response=True,source_query_complete=True,
            candle_count=len(complete)+len(incomplete),complete_candle_count=len(complete),
            incomplete_candle_count=len(incomplete),coverage=_coverage(complete),
            http_date_scope='server diagnostic only; never local receipt or tradeability evidence')
    except Exception as exc:
        reason=str(exc) if isinstance(exc,ValueError) and str(exc) in ERROR_CODES else 'request_or_validation_failed'
        receipt.update(error_type=type(exc).__name__,error_reason=reason)
    receipt['receipt_sha256']=_hash(receipt)
    return body,receipt


def map_verified_capture(raw, receipt, metadata, convention):
    """Replay a complete retained response without observing or renewing it.

    Hashes bind caller-retained bytes and receipt, not remote authentication.
    Source/receipt availability remains the original read completion. The caller
    independently controls trusted receipt persistence and prospective ordering.
    """
    _require(isinstance(receipt,dict) and receipt.get('schema_version')==SCHEMA
        and receipt.get('status')=='captured_not_issued','successful_capture_receipt_required')
    keys={'schema_version','status','instrument','metadata','source_bindings','request','limits',
        'request_started_epoch','request_started_utc','dependency_versions','http_status','response_headers',
        'read_completed_epoch','first_observed_epoch','capture_completed_epoch','request_duration_sec',
        'raw_response','complete_provider_response','source_query_complete','candle_count',
        'complete_candle_count','incomplete_candle_count','coverage','http_date_scope','receipt_sha256',*FLAGS}
    _require(set(receipt)==keys,'receipt_schema')
    _require(receipt.get('receipt_sha256')==_hash({k:v for k,v in receipt.items() if k!='receipt_sha256'}),'receipt_seal_mismatch')
    _require(all(receipt.get(key) is value for key,value in FLAGS.items()),'receipt_inert_flags')
    instrument=receipt.get('instrument')
    metadata,pip=_metadata(metadata,instrument)
    _require(receipt.get('metadata')==metadata,'registry_metadata_binding_mismatch')
    _require(receipt.get('source_bindings')==_sources(),'capture_source_binding_mismatch')
    _require(receipt.get('request')==_request(instrument) and receipt.get('limits')==LIMITS,'capture_request_binding_mismatch')
    _require(receipt.get('http_status')==200 and receipt.get('complete_provider_response') is True
             and receipt.get('source_query_complete') is True,'complete_provider_response_required')
    started=_clock(receipt.get('request_started_epoch'))
    observed=_clock(receipt.get('first_observed_epoch'))
    completed=_clock(receipt.get('capture_completed_epoch'))
    _require(started<=observed<=completed and receipt.get('read_completed_epoch')==observed
             and completed-started<=MAX_SECONDS
             and receipt.get('request_duration_sec')==observed-started,'receipt_clock_binding_mismatch')
    _require(receipt['request_started_utc']==datetime.fromtimestamp(started,timezone.utc).isoformat(),
             'receipt_clock_text_mismatch')
    _require(convention in CONVENTIONS,'explicit_midpoint_convention_required')
    _require(isinstance(raw,bytes) and receipt.get('raw_response')==
        {'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)},'raw_response_binding_mismatch')
    complete,incomplete=_parse_response(raw,instrument,observed)
    coverage=_coverage(complete)
    _require(receipt.get('coverage')==coverage and receipt.get('candle_count')==len(complete)+len(incomplete)
             and receipt.get('complete_candle_count')==len(complete)
             and receipt.get('incomplete_candle_count')==len(incomplete),'source_count_or_coverage_binding_mismatch')
    features=[]
    with localcontext(Context(prec=SOURCE_ARITHMETIC['decimal_precision'])):
        if len(complete)>=13:
            for row in complete[-13:]:
                bid={key:Decimal(row['bid'][key]) for key in 'ohlc'}
                ask={key:Decimal(row['ask'][key]) for key in 'ohlc'}
                mid=({key:Decimal(row['mid'][key]) for key in 'ohlc'} if convention=='official_midpoint'
                     else {key:(bid[key]+ask[key])/2 for key in 'ohlc'})
                features.append({'bar_start_epoch':row['bar_label_epoch'],'available_epoch':row['available_epoch'],
                    'complete':True,'bid_close':float(bid['c']),'ask_close':float(ask['c']),
                    'mid_close':float(mid['c']),'mid_high':float(mid['h']),'mid_low':float(mid['l']),
                    'spread_pips':float((ask['c']-bid['c'])/pip),'volume':float(row['volume'])})
    mapping={'schema_version':MAPPING_SCHEMA,'instrument':instrument,'metadata':metadata,
        'pip_size':metadata['pip_size'],'source_sha256':hashlib.sha256(raw).hexdigest(),
        'source_receipt_sha256':receipt['receipt_sha256'],'source_bindings':dict(receipt['source_bindings']),
        'first_observed_epoch':observed,'available_epoch':observed,'price_convention':convention,
        'complete_rows':complete,'incomplete_rows':incomplete,'last13_feature_rows':features,
        'feature_rows_status':'available' if len(features)==13 else 'fewer_than_13_complete_real_bars',
        'source_arithmetic_policy':dict(SOURCE_ARITHMETIC),
        'coverage':coverage,'historical_ingestion_equivalence_proven':False,
        'source_attestation':{'instrument':instrument,'granularity':'S5',
            'raw_source_sha256':hashlib.sha256(raw).hexdigest(),'complete_provider_response':True,
            'coverage_start_label_epoch':coverage['first_complete_bar_label_epoch'],
            'coverage_end_label_epoch':coverage['last_complete_bar_label_epoch'],
            'read_started_epoch':started,'read_completed_epoch':observed,
            'read_started_semantics':'request_started_before_GET',
            'source_receipt_sha256':receipt['receipt_sha256']},
        'tradeability_scope':'not_supplied_by_candle_endpoint_requires_independent_observation',
        'provenance_scope':'raw_digest_and_receipt_replayed_remote_authentication_not_reconstructed',**FLAGS}
    mapping['mapping_sha256']=_hash(mapping)
    return mapping
