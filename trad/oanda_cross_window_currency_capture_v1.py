"""Three-peer, exact-window research features; no fits or live model injection.

The signed ATR-normalized return, base/quote legs, percentile rank, sample
dispersion and leave-self-out breadth reuse the audited unified definitions.
The universe, true endpoint selection and missingness are a NEW contract. It
does not reproduce the historical 68-pair/795-column matrix or its fallbacks.
"""
from __future__ import annotations

import base64
import csv
from decimal import Context, Decimal, localcontext
from datetime import datetime
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import stat
import time

VERSION='cross_window_currency_capture_v1_20260909'
CAPTURE_SCHEMA='three_peer_m1_capture_v1_20260909'
FEATURE_SCHEMA='three_peer_cross_window_features_v1_20260909'
PEERS=('EUR_USD','GBP_USD','USD_JPY')
WINDOWS=(1,15,60)
PIPS={'EUR_USD':'0.0001','GBP_USD':'0.0001','USD_JPY':'0.01'}
FLAGS={'research_only':True,'can_place_orders':False,'can_promote':False,'can_authorize':False,
       'account_eligible':False,'execution_eligible':False,'proof_eligible':False,'forecast_issued':False}
MAX_TAIL_BYTES=256*1024
MAX_ROWS=512
MAX_JSON_BYTES=2*1024*1024
LINEAGE={
    'normalized_return_and_currency_aggregation':{'path':'D:/forex/trad/fresh_m1_intrahour/src/unified_forecast.py',
        'sha256':'4a8ac140dc610162cfbd15177768ae6687b7962dee0104fee790d84427d47262',
        'lines':[335,448,1027,1078,1101,1165]},
    'strength_window_product_pattern':{'path':'D:/forex/trad/fresh_m1_intrahour/src/feature_forecast_benchmark.py',
        'sha256':'c7f7eb550231df2cf5d271c2afd41f5f1d1a301980e3ad511217a6ff95ba3a04','lines':[143,239]},
}
_HASH=re.compile(r'^[0-9a-f]{64}$')
_TIME=re.compile(r'^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.0{1,9})?(?:Z|\+00:00)$')
READ_ERRORS=frozenset({'cross_source_reparse_point','cross_source_path_type','cross_source_unavailable',
    'cross_header_bound','cross_source_read_failed','cross_source_changed_during_read',
    'cross_partial_tail','cross_read_clock_reversed'})


class CrossWindowError(ValueError):
    """Bounded code without source-row or raw exception contents."""


def _need(condition,reason):
    if not condition:
        raise CrossWindowError(reason)


def canonical_bytes(value):
    try:
        raw=json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
    except (ValueError,TypeError,OverflowError,RecursionError):
        raise CrossWindowError('cross_invalid_json') from None
    _need(len(raw)<=MAX_JSON_BYTES,'cross_json_byte_bound')
    return raw


def digest(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _clock(value):
    _need(type(value) in (int,float) and 0<value<1e12 and math.isfinite(value),'cross_invalid_clock')
    return float(value)


def _safe(path):
    current=Path(path.anchor)
    try:
        for part in path.parts[1:]:
            current/=part
            info=current.lstat()
            _need(not stat.S_ISLNK(info.st_mode) and not
                  (getattr(info,'st_file_attributes',0)&getattr(stat,'FILE_ATTRIBUTE_REPARSE_POINT',1024)),
                  'cross_source_reparse_point')
            _need(stat.S_ISREG(info.st_mode) if current==path else stat.S_ISDIR(info.st_mode),'cross_source_path_type')
    except OSError:
        raise CrossWindowError('cross_source_unavailable') from None


def _read_source(path,clock):
    begun=_clock(clock())
    _safe(path)
    try:
        with path.open('rb') as handle:
            before=os.fstat(handle.fileno())
            header=handle.readline(8193)
            _need(0<len(header)<=8192 and header.endswith(b'\n'),'cross_header_bound')
            offset=max(len(header),before.st_size-MAX_TAIL_BYTES)
            handle.seek(offset)
            tail=handle.read(MAX_TAIL_BYTES+1)
            after=os.fstat(handle.fileno())
        retained=path.stat()
    except OSError:
        raise CrossWindowError('cross_source_read_failed') from None
    _safe(path)
    identity=lambda value:(value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns)
    _need(len({identity(value) for value in (before,after,retained)})==1,'cross_source_changed_during_read')
    if offset>len(header):
        boundary=tail.find(b'\n')
        _need(boundary>=0,'cross_partial_tail')
        offset+=boundary+1
        tail=tail[boundary+1:]
    _need(0<len(tail)<=MAX_TAIL_BYTES and tail.endswith(b'\n'),'cross_partial_tail')
    lines=tail.splitlines(keepends=True)
    if len(lines)>MAX_ROWS:
        offset+=sum(len(row) for row in lines[:-MAX_ROWS])
        tail=b''.join(lines[-MAX_ROWS:])
    checksum=hashlib.sha256(header+tail).hexdigest()
    completed=_clock(clock())
    _need(begun<=completed,'cross_read_clock_reversed')
    return dict(status='captured',source_path=str(path),read_started_epoch=begun,read_completed_epoch=completed,
        file_size_bytes=before.st_size,tail_byte_offset=offset,header_base64=base64.b64encode(header).decode(),
        tail_base64=base64.b64encode(tail).decode(),retained_bytes_sha256=checksum,
        hash_scope='retained_header_plus_last_at_most512_complete_rows_not_entire_file')


def capture_sources(candle_root,*,clock=time.time):
    """Observe three local files once; failed peers remain explicit in the capture."""
    started=_clock(clock())
    try:
        root=Path(os.path.abspath(os.fspath(candle_root)))
    except (TypeError,ValueError,OSError):
        raise CrossWindowError('cross_root_invalid') from None
    sources={}
    for pair in PEERS:
        try:
            sources[pair]=_read_source(root/(pair+'_M1.csv'),clock)
        except CrossWindowError as exc:
            sources[pair]=dict(status='unavailable',reason=str(exc),observed_epoch=_clock(clock()))
    completed=_clock(clock())
    _need(completed>=started,'cross_capture_clock_reversed')
    body=dict(schema_version=CAPTURE_SCHEMA,version=VERSION,peers=list(PEERS),pip_sizes=dict(PIPS),
        capture_started_epoch=started,capture_completed_epoch=completed,sources=sources,
        implementation_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        lineage=LINEAGE,source_semantics='archived_M1_mid_OHLC_close_at_label_plus60_observed_now',**FLAGS)
    body=json.loads(canonical_bytes(body))
    return {**body,'capture_sha256':digest(body)}


def _price(value):
    _need(isinstance(value,str) and len(value)<=64 and re.fullmatch(r'[0-9]+(?:\.[0-9]+)?',value),'cross_price_lexeme')
    number=Decimal(value)
    _need(0<number<Decimal('1e12') and len(number.as_tuple().digits)<=32 and number.as_tuple().exponent>=-18,'cross_price_bound')
    return number


def _rows(source,pair,consumer):
    _need(isinstance(source,dict),'cross_source_shape')
    if source.get('status')=='unavailable':
        _need(set(source)=={'status','reason','observed_epoch'} and isinstance(source.get('reason'),str)
            and source['reason'] in READ_ERRORS,'cross_source_failure_code')
        _need(_clock(source.get('observed_epoch'))<=consumer,'cross_peer_observed_in_future')
        raise CrossWindowError('cross_peer_read_unavailable')
    keys={'status','source_path','read_started_epoch','read_completed_epoch','file_size_bytes','tail_byte_offset',
          'header_base64','tail_base64','retained_bytes_sha256','hash_scope'}
    _need(set(source)==keys and source['status']=='captured','cross_source_shape')
    _need(source['hash_scope']=='retained_header_plus_last_at_most512_complete_rows_not_entire_file',
          'cross_source_hash_scope')
    _need(isinstance(source['source_path'],str) and len(source['source_path'])<=4096,'cross_source_path_bound')
    begun,observed=_clock(source['read_started_epoch']),_clock(source['read_completed_epoch'])
    _need(begun<=observed<=consumer,'cross_peer_observed_in_future')
    try:
        _need(isinstance(source['header_base64'],str) and len(source['header_base64'])<=10924
              and isinstance(source['tail_base64'],str) and len(source['tail_base64'])<=4*((MAX_TAIL_BYTES+2)//3),
              'cross_source_byte_bound')
        header=base64.b64decode(source['header_base64'],validate=True)
        tail=base64.b64decode(source['tail_base64'],validate=True)
    except ValueError:
        raise CrossWindowError('cross_source_base64') from None
    _need(0<len(header)<=8192 and 0<len(tail)<=MAX_TAIL_BYTES and header.endswith(b'\n') and tail.endswith(b'\n'),
          'cross_source_byte_bound')
    _need(hashlib.sha256(header+tail).hexdigest()==source['retained_bytes_sha256'],'cross_source_hash_mismatch')
    _need(type(source['tail_byte_offset']) is int and type(source['file_size_bytes']) is int
          and source['tail_byte_offset']>=len(header)
          and source['tail_byte_offset']+len(tail)==source['file_size_bytes'],'cross_source_byte_geometry')
    try:
        reader=csv.DictReader(io.StringIO((header+tail).decode()))
        columns=reader.fieldnames
        _need(columns is not None and 7<=len(columns)<=32 and len(columns)==len(set(columns))
            and {'time','instrument','granularity','open','high','low','close'}<=set(columns),'cross_csv_schema')
        answer={}
        previous=None
        for raw in reader:
            _need(len(answer)<MAX_ROWS and None not in raw,'cross_source_row_bound')
            _need(raw['instrument']==pair and raw['granularity']=='M1','cross_row_identity')
            stamp=raw['time']
            _need(isinstance(stamp,str) and _TIME.fullmatch(stamp),'cross_minute_timestamp')
            label=datetime.fromisoformat(stamp.replace('Z','+00:00')).timestamp()
            _need(label>0 and label%60==0 and (previous is None or label>previous),'cross_duplicate_or_unordered_bar')
            previous=label
            _need(label+60<=observed,'cross_future_bar_at_observation')
            prices={key:_price(raw[key]) for key in ('open','high','low','close')}
            _need(prices['low']<=prices['open']<=prices['high'] and prices['low']<=prices['close']<=prices['high'],
                  'cross_ohlc_range')
            answer[int(label+60)]={**prices,'bar_label_epoch':int(label),'price_epoch':int(label+60),'time_text':stamp}
    except (csv.Error,UnicodeError,ValueError) as exc:
        if isinstance(exc,CrossWindowError):
            raise
        raise CrossWindowError('cross_invalid_csv') from None
    _need(bool(answer),'cross_empty_rows')
    return answer


def _text(value):
    return None if value is None else str(value)


def _mean(values):
    return sum(values,Decimal(0))/len(values) if values else None


def _reported_clock(value):
    return value if type(value) in (int,float) and 0<value<1e12 and math.isfinite(value) else None


def build_features(capture,*,common_price_epoch,consumer_epoch,expected_source_sha256,maximum_age_sec=900):
    """Pure retained-byte replay at one declared cutoff; no source reads or fits.

    Consumer observation may follow the price cutoff. It never becomes a claim
    that these rows were available at that earlier time. All window endpoints
    and ATR support must be real timestamps, with no fallback or imputation.
    """
    capture=json.loads(canonical_bytes(capture))
    keys={'schema_version','version','peers','pip_sizes','capture_started_epoch','capture_completed_epoch',
        'sources','implementation_sha256','lineage','source_semantics','capture_sha256',*FLAGS}
    _need(isinstance(capture,dict) and set(capture)==keys and capture.get('schema_version')==CAPTURE_SCHEMA
        and capture.get('version')==VERSION and capture.get('peers')==list(PEERS)
        and capture.get('pip_sizes')==PIPS and capture.get('lineage')==LINEAGE
        and capture.get('source_semantics')=='archived_M1_mid_OHLC_close_at_label_plus60_observed_now','cross_capture_identity')
    _need(all(capture.get(key) is value for key,value in FLAGS.items()),'cross_capture_authority')
    _need(isinstance(expected_source_sha256,str) and _HASH.fullmatch(expected_source_sha256)
        and capture.get('implementation_sha256')==expected_source_sha256,'cross_implementation_binding')
    _need(digest({k:v for k,v in capture.items() if k!='capture_sha256'})==capture.get('capture_sha256'),'cross_capture_seal')
    consumer=_clock(consumer_epoch)
    cutoff=_clock(common_price_epoch)
    _need(cutoff%60==0 and cutoff<=consumer,'cross_cutoff_future_or_off_grid')
    _need(type(maximum_age_sec) in (int,float) and 0<=maximum_age_sec<=900 and math.isfinite(maximum_age_sec),'cross_age_policy')
    _need(_clock(capture.get('capture_started_epoch'))<=_clock(capture.get('capture_completed_epoch'))<=consumer,
          'cross_capture_not_yet_available')
    _need(isinstance(capture.get('sources'),dict) and set(capture['sources'])==set(PEERS)
        and all(isinstance(value,dict) for value in capture['sources'].values()),'cross_peer_inventory')
    pairs={}
    with localcontext(Context(prec=96)):
        for pair in PEERS:
            source=capture['sources'][pair]
            windows={}
            support={}
            reason=None
            try:
                source_observed=_clock(source.get('read_completed_epoch',source.get('observed_epoch')))
                _need(capture['capture_started_epoch']<=source_observed<=capture['capture_completed_epoch'],
                      'cross_peer_outside_capture_observation')
                if source.get('status')=='captured':
                    _need(capture['capture_started_epoch']<=_clock(source.get('read_started_epoch')),
                          'cross_peer_outside_capture_observation')
                rows=_rows(source,pair,consumer)
                _need(consumer-cutoff<=maximum_age_sec,'cross_cutoff_stale')
                _need(cutoff in rows,'cross_common_endpoint_missing')
                atr_epochs=[int(cutoff-step*60) for step in range(14,-1,-1)]
                support={'required_atr_rows':15,'available_atr_rows':sum(epoch in rows for epoch in atr_epochs),
                    'atr_support_price_epochs':atr_epochs,'atr_normalizer_window_minutes':14,
                    'latest_source_price_epoch':max(rows),'retained_rows':len(rows)}
                atr=None
                if all(epoch in rows for epoch in atr_epochs):
                    ranges=[max(rows[b]['high']-rows[b]['low'],abs(rows[b]['high']-rows[a]['close']),
                        abs(rows[b]['low']-rows[a]['close'])) for a,b in zip(atr_epochs,atr_epochs[1:])]
                    atr=_mean(ranges)
                for minute in WINDOWS:
                    start=int(cutoff-minute*60)
                    if start not in rows:
                        windows[str(minute)]={'status':'unavailable','reason':'cross_window_start_missing',
                            'required_start_price_epoch':start,'end_price_epoch':cutoff}
                        continue
                    delta=rows[cutoff]['close']-rows[start]['close']
                    pip=Decimal(PIPS[pair])
                    body={'status':'available' if atr is not None else 'unavailable',
                        'reason':None if atr is not None else 'cross_exact_atr_support_missing',
                        'start_price_epoch':start,'end_price_epoch':cutoff,
                        'start_bar_label_epoch':rows[start]['bar_label_epoch'],'end_bar_label_epoch':rows[cutoff]['bar_label_epoch'],
                        'start_price':str(rows[start]['close']),'end_price':str(rows[cutoff]['close']),
                        'signed_native_pips':str(delta/pip),'atr14_native_pips':_text(atr/pip if atr is not None else None),
                        'normalized_return':_text(max(Decimal(-10),min(Decimal(10),delta/max(atr,Decimal('0.1')*pip))) if atr is not None else None),
                        'source_available_epoch':source['read_completed_epoch']}
                    windows[str(minute)]=body
            except CrossWindowError as exc:
                reason=str(exc)
                windows={str(minute):{'status':'unavailable','reason':reason} for minute in WINDOWS}
            pairs[pair]={'base_currency':pair[:3],'quote_currency':pair[4:],'pip_size':PIPS[pair],
                'price_orientation':'quote_currency_units_per_one_base_currency',
                'source_retained_bytes_sha256':source.get('retained_bytes_sha256'),
                'source_read_started_epoch':_reported_clock(source.get('read_started_epoch')),
                'source_read_completed_epoch':_reported_clock(source.get('read_completed_epoch',source.get('observed_epoch'))),
                'source_read_reason':source.get('reason') if isinstance(source.get('reason'),str)
                    and source['reason'] in READ_ERRORS else None, 'support':support,'windows':windows}
        aggregates={}
        for minute in WINDOWS:
            key=str(minute)
            available={pair:Decimal(pairs[pair]['windows'][key]['normalized_return']) for pair in PEERS
                if pairs[pair]['windows'][key]['status']=='available'}
            legs={currency:[] for currency in ('EUR','GBP','USD','JPY')}
            for pair,change in available.items():
                legs[pair[:3]].append((pair,change))
                legs[pair[4:]].append((pair,-change))
            strengths={currency:_mean([v for _,v in members]) for currency,members in legs.items()}
            mean=_mean(list(available.values()))
            dispersion=(sum((value-mean)**2 for value in available.values())/(len(available)-1)).sqrt() if len(available)>1 else None
            aggregates[key]={'available_pairs':list(available),'available_pair_count':len(available),
                'expected_pair_count':3,'currency_strengths':{k:_text(v) for k,v in strengths.items()},
                'currency_observed_leg_counts':{k:len(v) for k,v in legs.items()},'sample_dispersion':_text(dispersion)}
            for pair,value in available.items():
                others=[v for p,v in available.items() if p!=pair]
                base_other=[v for p,v in legs[pair[:3]] if p!=pair]
                quote_other=[v for p,v in legs[pair[4:]] if p!=pair]
                positive=lambda values:_mean([Decimal(v>0) for v in values])
                base_breadth,quote_breadth=positive(base_other),positive(quote_other)
                rank=(sum(v<value for v in available.values())+(Decimal(sum(v==value for v in available.values()))+1)/2)/len(available)
                own=pairs[pair]['windows'][key]
                own.update(base_strength=_text(strengths[pair[:3]]),quote_strength=_text(strengths[pair[4:]]),
                    strength_gap=_text(strengths[pair[:3]]-strengths[pair[4:]]),
                    percentile_rank=_text(rank),rank_denominator=len(available),
                    up_breadth_ex_self=_text(positive(others)),
                    down_breadth_ex_self=_text(positive([-v for v in others])),
                    same_direction_breadth_ex_self=_text(positive([v if value>0 else -v for v in others]) if value else None),
                    base_strengthening_breadth_ex_self=_text(base_breadth),quote_strengthening_breadth_ex_self=_text(quote_breadth),
                    currency_breadth_reason=None if base_breadth is not None and quote_breadth is not None else 'no_independent_peer_for_one_currency',
                    strength_includes_own_return=True,independent_currency_peer_counts={'base':len(base_other),'quote':len(quote_other)})
        for pair in PEERS:
            first,second=(pairs[pair]['windows'][str(minute)] for minute in (15,60))
            pairs[pair]['strength_gap15_x_strength_gap60']=str(Decimal(first['strength_gap'])*Decimal(second['strength_gap'])) \
                if first['status']==second['status']=='available' else None
    body=dict(schema_version=FEATURE_SCHEMA,version=VERSION,status='available' if all(len(v['available_pairs'])==3 for v in aggregates.values()) else 'partial' if any(v['available_pairs'] for v in aggregates.values()) else 'unavailable',
        capture_sha256=capture['capture_sha256'],implementation_sha256=expected_source_sha256,lineage=LINEAGE,
        common_price_epoch=cutoff,consumer_epoch=consumer,available_epoch=consumer,maximum_age_sec=maximum_age_sec,
        peer_universe=list(PEERS),window_minutes=list(WINDOWS),pairs=pairs,windows=aggregates,
        definition='price_delta/native_pip divided by real14-bar ATR/native_pip floor0.1; clipped[-10,10]; base+ quote-',
        universe_scope='fixed_three_pairs_four_currencies_not_historical68_pair_ranks',
        dependence={'shared_currency':'USD','usd_leg_signs':{'EUR_USD':-1,'GBP_USD':-1,'USD_JPY':1},
            'independent_trial_count':None,'note':'Shared USD legs and overlapping windows are dependent; pair count is not independent evidence.'},
        original_historical_availability_proven=False,forecast_horizon_sec=None,model_inputs_injected=False,**FLAGS)
    body=json.loads(canonical_bytes(body))
    return {**body,'features_sha256':digest(body)}
