"""Bounded local market display. Descriptive prices, never forecast/order authority."""
from __future__ import annotations
import csv
from datetime import datetime
from decimal import Context, Decimal, DecimalException, ROUND_HALF_EVEN, localcontext
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import time
from oanda_exact_price_scoring import decimal_value

SCHEMA = 'market_overview_v1'
MAX_TAIL_BYTES = 65536
MAX_ROWS = 160
MAX_QUOTE_BYTES = 262144
QUOTE_MAX_AGE_SEC = 60
ANCHOR_MAX_LAG_SEC = 90
WINDOWS = {'5m':300,'15m':900,'60m':3600}
_CACHE: dict[str,tuple[tuple,list]] = {}

def _epoch(value):
    if not isinstance(value,str): raise ValueError('timestamp_required')
    parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
    if parsed.tzinfo is None: raise ValueError('timezone_required')
    return parsed.timestamp()

def _price(value):
    try: number=decimal_value(value,'display_price')
    except (ValueError,DecimalException): raise ValueError('invalid_price') from None
    if number<=0 or not math.isfinite(float(number)) or float(number)<=0: raise ValueError('invalid_price')
    return number

def _display(value):
    number=float(value)
    if not math.isfinite(number) or (value!=0 and number==0):
        raise ValueError('unrepresentable_display_metric')
    return number

def _signature(stat):
    return (stat.st_dev,stat.st_ino,stat.st_size,stat.st_mtime_ns)

def _history(path:Path,pair:str):
    before=path.stat(); signature=_signature(before); key=str(path.resolve())
    saved=_CACHE.get(key)
    if saved and saved[0]==signature: return saved[1]
    with path.open('rb') as handle:
        header=handle.readline(8193)
        if len(header)>8192 or not header.endswith(b'\n'): raise ValueError('invalid_header')
        size=before.st_size
        offset=max(len(header),size-MAX_TAIL_BYTES)
        handle.seek(offset); raw=handle.read(MAX_TAIL_BYTES)
        if offset>len(header):
            cut=raw.find(b'\n')
            if cut<0: raise ValueError('oversized_row')
            raw=raw[cut+1:]
        if _signature(path.stat())!=signature or _signature(os.fstat(handle.fileno()))!=signature:
            raise ValueError('history_changed_during_read')
    if not raw or not raw.endswith(b'\n'): raise ValueError('partial_history')
    fields=next(csv.reader([header.decode('utf-8-sig').strip()]))
    if len(fields)!=len(set(fields)) or 'close' not in fields: raise ValueError('invalid_columns')
    clocks=[field for field in ('time','datetime') if field in fields]
    if not clocks: raise ValueError('missing_clock')
    output=[]; previous=None
    for line in raw.decode('utf-8').splitlines()[-MAX_ROWS:]:
        values=next(csv.reader([line],strict=True))
        if len(values)!=len(fields): raise ValueError('invalid_row_width')
        row=dict(zip(fields,values)); stamps=[_epoch(row[k]) for k in clocks]
        stamp=stamps[0]
        if any(t!=stamp for t in stamps) or stamp%60: raise ValueError('invalid_minute_clock')
        if previous is not None and stamp<=previous: raise ValueError('unordered_or_duplicate_history')
        previous=stamp
        if row.get('instrument',pair)!=pair or row.get('granularity','M1')!='M1': raise ValueError('wrong_history_identity')
        if row.get('complete','true').lower() not in ('true','1'): continue
        output.append((stamp+60,_price(row['close'])))
    if len(_CACHE)>=100 and key not in _CACHE: _CACHE.clear()
    _CACHE[key]=(signature,output)
    return output

def _changes(history,mid:Decimal,quote_epoch:float,pip:Decimal):
    changes={}
    for name,seconds in WINDOWS.items():
        target=quote_epoch-seconds
        anchors=[item for item in history if item[0]<=target]
        if not anchors:
            changes[name]=None; continue
        stamp,anchor=anchors[-1]
        if target-stamp>ANCHOR_MAX_LAG_SEC:
            changes[name]=None; continue
        change=mid-anchor
        changes[name]={'anchor_epoch':stamp,'anchor_mid':_display(anchor),
            'end_epoch':quote_epoch,'actual_seconds':round(quote_epoch-stamp,3),
            'price_change_pips':_display(change/pip),'return_bps':_display(change/anchor*10000),
            'semantics':'observed_midpoint_change_before_costs_not_a_forecast'}
    return changes

def _build_market_overview(data_root:Path,*,now_epoch:float|None=None)->dict:
    """Read at most100 quote rows and64KiB/160 candle rows per instrument.

    Endpoint moves tolerate explicitly bounded anchor timing differences; no
    missing candles are created. The observation time is this consumer's read,
    never an assertion of historical first availability. No database, network,
    file write, worker, trading or fitted-model operation is used.
    """
    root=Path(data_root); now=time.time() if now_epoch is None else float(now_epoch)
    output={'schema_version':SCHEMA,'generated_epoch':now,'observed_epoch':now,
        'status':'unavailable','reason':'','rows':[],'windows_sec':WINDOWS,
        'research_only':True,'can_place_orders':False,'can_promote':False,
        'semantics':'Live quotes with observed midpoint changes; technical descriptors are not model forecasts.',
        'quote_max_age_sec':QUOTE_MAX_AGE_SEC,'anchor_max_lag_sec':ANCHOR_MAX_LAG_SEC}
    try:
        if not math.isfinite(now): raise ValueError('invalid_clock')
        with (root/'state/practice_007_market_quotes_v1.json').open('rb') as f: raw=f.read(MAX_QUOTE_BYTES+1)
        observed=time.time() if now_epoch is None else now
        if len(raw)>MAX_QUOTE_BYTES: raise ValueError('oversized_quote_snapshot')
        payload=json.loads(raw)
        if not isinstance(payload,dict) or payload.get('producer')!='practice_007_dedicated_quote_stream':
            raise ValueError('unexpected_quote_producer')
        source_clock=_epoch(payload.get('generated_utc'))
        if not 0<=observed-source_clock<=QUOTE_MAX_AGE_SEC: raise ValueError('stale_or_future_quote_publication')
        quotes=payload.get('quotes'); coverage=payload.get('coverage')
        if not isinstance(quotes,dict) or len(quotes)>100 or not isinstance(coverage,dict): raise ValueError('invalid_quote_shape')
        retained=coverage.get('retained_last_known_instruments')
        if not isinstance(retained,list): raise ValueError('missing_generation_coverage')
        output.update(observed_epoch=observed,source_quote_epoch=source_clock,source_quote_sha256=hashlib.sha256(raw).hexdigest())
        for pair,quote in sorted(quotes.items()):
            if not re.fullmatch(r'[A-Z]{3}_[A-Z]{3}',pair) or not isinstance(quote,dict): continue
            row={'instrument':pair,'status':'unavailable','changes':{name:None for name in WINDOWS},
                'technical':{'status':'unavailable','as_of_epoch':None},'reason':''}
            try:
                bid,ask,pip=_price(quote.get('bid')),_price(quote.get('ask')),_price(quote.get('pip'))
                if ask<bid: raise ValueError('crossed_quote')
                stamp=_epoch(quote.get('time')); age=observed-stamp; mid=(bid+ask)/2
                row.update(bid=_display(bid),ask=_display(ask),mid=_display(mid),pip=_display(pip),
                    spread_pips=_display((ask-bid)/pip),quote_epoch=stamp,quote_age_sec=round(age,3),
                    tradeable=quote.get('tradeable') is True)
                if age<0: raise ValueError('future_quote')
                if age>QUOTE_MAX_AGE_SEC:
                    row.update(status='stale',reason='stale_quote'); output['rows'].append(row); continue
                if quote.get('source')!='stream' or pair in retained: raise ValueError('retained_or_nonstream_quote')
                if quote.get('tradeable') is not True:
                    row.update(status='closed',reason='quote_not_tradeable'); output['rows'].append(row); continue
                row['status']='current'
                try:
                    history=_history(root/'candles'/f'{pair}_M1.csv',pair)
                    row['changes']=_changes(history,mid,stamp,pip)
                    row['history_status']='available'
                except (OSError,ValueError,csv.Error,UnicodeError,DecimalException):
                    row['history_status']='unavailable'
                five,fifteen=row['changes']['5m'],row['changes']['15m']
                if five and fifteen:
                    trend=lambda value:'up' if value>0 else 'down' if value<0 else 'flat'
                    row['technical']={'status':'current','as_of_epoch':stamp,
                        'trend_5m':trend(five['return_bps']),'trend_15m':trend(fifteen['return_bps']),
                        'momentum_bps_5m':five['return_bps'],'momentum_bps_15m':fifteen['return_bps'],
                        'semantics':'price_momentum_description_not_execution_signal'}
            except (ValueError,TypeError,KeyError,OverflowError,DecimalException) as exc:
                row['reason']=str(exc) if isinstance(exc,ValueError) else 'invalid_quote'
            output['rows'].append(row)
        current=sum(row['status']=='current' for row in output['rows'])
        output.update(status='current' if current else 'no_current_tradeable_quotes',
            current_pair_count=current,quoted_pair_count=len(output['rows']),
            technical_pair_count=sum(row['technical']['status']=='current' for row in output['rows']))
    except (OSError,ValueError,TypeError,UnicodeError,OverflowError,DecimalException,RecursionError) as exc:
        output['reason']=str(exc) if isinstance(exc,ValueError) else 'quote_snapshot_unavailable'
    return output


def build_market_overview(data_root:Path,*,now_epoch:float|None=None)->dict:
    """Bounded, finite display under a context independent of the caller.

    Supplied prices are bounded decimals. Exact additive price calculations fit
    the private context; unrepresentable float metrics become unavailable rather
    than Infinity, NaN or an underflowed false-flat indicator.
    """
    with localcontext(Context(prec=4096,rounding=ROUND_HALF_EVEN)):
        return _build_market_overview(data_root,now_epoch=now_epoch)
