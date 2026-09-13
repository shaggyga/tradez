"""Pure strict M1 retrospective risk labels; no inference, execution or I/O.

CSV rows prove retained prices and maturity at capture, not original ingestion
availability or historical broker completeness. Midpoint ingestion is unknown.
Bid/ask values are never inferred. Close-path diagnostics and intrabar bounds
are distinct; OHLC cannot prove ordering, tradeability or fills.
"""
from collections import Counter
import csv
from datetime import datetime
from decimal import Context, Decimal, localcontext
import hashlib
import io
import math
import re

SCHEMA = 'strict_m1_path_risk_labels_v1_20260909'
MAX_BYTES = 20 * 1024 * 1024
MAX_ROWS = 100000
HORIZONS = (15, 30, 60)
FIELDS = ('open', 'high', 'low', 'close')
CONTINUOUS_LABELS = ('signed_terminal_bps', 'absolute_terminal_bps',
    'mid_future_range_bps', 'realized_path_variation_bps',
    'long_close_MFE_bps', 'long_close_MAE_bps', 'short_close_MFE_bps', 'short_close_MAE_bps',
    'long_envelope_MFE_bps', 'long_envelope_MAE_bps', 'short_envelope_MFE_bps', 'short_envelope_MAE_bps')
ENDPOINT_COST_LABELS = ('long_endpoint_net_bps', 'short_endpoint_net_bps', 'either_side_endpoint_positive')
ALL_LABELS = CONTINUOUS_LABELS + ENDPOINT_COST_LABELS
FLAGS = dict(research_only=True,can_place_orders=False,can_promote=False,can_authorize=False,
    account_eligible=False,execution_eligible=False,proof_eligible=False,forecast_issued=False)

def need(ok, reason):
    if not ok:
        raise ValueError(reason)

def number(value):
    need(isinstance(value,str) and len(value)<=48 and re.fullmatch(r'[0-9]+(?:\.[0-9]+)?',value), 'exact_positive_price_text_required')
    result=Decimal(value)
    need(0<result<Decimal('1e12') and len(result.as_tuple().digits)<=32, 'price_magnitude_or_precision_bound')
    return result

def prices(record, prefix=''):
    values={k:number(record[prefix+k]) for k in FIELDS}
    need(values['low']<=values['open']<=values['high'] and values['low']<=values['close']<=values['high'], 'invalid_OHLC_geometry')
    return {k:record[prefix+k] for k in FIELDS}

def log_return(a,b):
    change=float((b-a)/a)
    return math.log1p(change) if change>-.5 else math.log(float(b))-math.log(float(a))

def parse_csv(raw, instrument, *, observed_epoch):
    """Parse exact archive text. Bad BA withholds cost/path-BA labels only."""
    need(re.fullmatch(r'[A-Z]{3}_[A-Z]{3}',instrument or '') is not None and instrument[:3]!=instrument[-3:], 'instrument_identity')
    need(type(observed_epoch) in (int,float) and math.isfinite(observed_epoch) and observed_epoch>0, 'actual_capture_observation_required')
    need(type(raw) is bytes and 0<len(raw)<=MAX_BYTES and raw.endswith(b'\n'), 'source_byte_bound_or_partial_tail')
    reader=csv.DictReader(io.StringIO(raw.decode('utf-8-sig')))
    names=reader.fieldnames
    need(names and len(names)==len(set(names)) and {'time','instrument','granularity',*FIELDS}<=set(names), 'source_columns')
    rows=[];previous=None;missing=Counter()
    with localcontext(Context(prec=96)):
        for record in reader:
            need(len(rows)<MAX_ROWS and None not in record, 'source_row_bound_or_width')
            need(record['instrument']==instrument and record['granularity']=='M1', 'source_row_identity')
            stamp=record['time']
            need(isinstance(stamp,str) and re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.0{1,9})?(?:Z|\+00:00)',stamp), 'UTC_minute_label_required')
            epoch=int(datetime.fromisoformat(stamp.replace('Z','+00:00')).timestamp())
            need(epoch>0 and epoch%60==0 and (previous is None or epoch>previous), 'duplicate_or_unordered_bar')
            need(epoch+60<=observed_epoch, 'bar_future_at_capture')
            if 'complete' in record:
                need(record['complete'].lower() in ('true','1'), 'explicit_incomplete_bar')
            mid=prices(record);bid=ask=None;reason=None
            try:
                if not all(record.get(side+'_'+k) for side in ('bid','ask') for k in FIELDS):
                    raise ValueError('real_bid_ask_OHLC_unavailable')
                bid,ask=prices(record,'bid_'),prices(record,'ask_')
                need(all(number(bid[k])<=number(ask[k]) for k in FIELDS), 'crossed_bid_ask_OHLC')
            except ValueError as error:
                bid=ask=None;reason=str(error);missing[reason]+=1
            rows.append(dict(label_epoch=epoch,price_epoch=epoch+60,mid=mid,bid=bid,ask=ask,
                bid_ask_unavailable_reason=reason))
            previous=epoch
    need(rows, 'empty_source')
    return rows,dict(schema_version=SCHEMA,instrument=instrument,source_sha256=hashlib.sha256(raw).hexdigest(),
        source_bytes=len(raw),rows=len(rows),observed_epoch=observed_epoch,
        first_price_epoch=rows[0]['price_epoch'],last_price_epoch=rows[-1]['price_epoch'],
        original_ingestion_availability_proven=False,midpoint_ingestion_equivalence_proven=False,
        provider_completeness_attested=False,bid_ask_missingness=dict(missing),**FLAGS)

def contiguous_segments(rows):
    if not rows:
        return []
    starts=[0]
    for i in range(1,len(rows)):
        need(rows[i]['label_epoch']>rows[i-1]['label_epoch'], 'duplicate_or_unordered_bar')
        if rows[i]['label_epoch']-rows[i-1]['label_epoch']!=60:
            starts.append(i)
    return list(zip(starts,starts[1:]+[len(rows)]))

def past_volatility_scale(rows, origin_index, horizon_minutes):
    """Past60 actual returns only; zero volatility is an explicit refusal."""
    need(type(origin_index) is int and 0<=origin_index<len(rows) and type(horizon_minutes) is int and horizon_minutes in HORIZONS, 'volatility_index_or_horizon')
    block=rows[max(0,origin_index-60):origin_index+1]
    if len(block)!=61 or any(b['label_epoch']-a['label_epoch']!=60 for a,b in zip(block,block[1:])):
        return None,'past60_complete_support_missing'
    with localcontext(Context(prec=96)):
        values=[number(r['mid']['close']) for r in block]
        returns=[log_return(a,b) for a,b in zip(values,values[1:])]
    scale=math.sqrt(math.fsum(x*x for x in returns)/60*horizon_minutes)*10000
    if not math.isfinite(scale) or scale<=0:
        return None,'zero_or_nonfinite_past_volatility'
    return scale,None

def label_origin(rows, origin_index, horizon_minutes):
    """Exact nominal endpoint; path uses only bars after completed origin close."""
    need(type(origin_index) is int and 0<=origin_index<len(rows) and type(horizon_minutes) is int and horizon_minutes in HORIZONS, 'label_index_or_horizon')
    origin=rows[origin_index];target_label=origin['label_epoch']+60*horizon_minutes
    values={k:None for k in ALL_LABELS};reasons={};future=[];target=None
    for row in rows[origin_index+1:origin_index+horizon_minutes+1]:
        if row['label_epoch']>target_label:
            break
        future.append(row)
        if row['label_epoch']==target_label:
            target=row
            break
    meta=dict(schema_version=SCHEMA,reference_label_epoch=origin['label_epoch'],reference_price_epoch=origin['price_epoch'],
        target_label_epoch=target_label,target_price_epoch=target_label+60,horizon_minutes=horizon_minutes,
        target_selection='exact_nominal_no_nearest_or_alignment',future_path_rows=len(future),
        original_ingestion_availability_proven=False,midpoint_ingestion_equivalence_proven=False,
        target_tradeability_observed=False,broker_fill_observed=False,barrier_order_known=False,
        values=values,unavailable_reasons=reasons,**FLAGS)
    def unavailable(keys, reason):
        for key in keys:
            reasons[key]=reason
    if target is None:
        unavailable(ALL_LABELS,'exact_target_missing')
        return meta
    with localcontext(Context(prec=96)):
        reference=number(origin['mid']['close']);factor=Decimal(10000)/reference
        terminal=number(target['mid']['close'])
        signed=(terminal-reference)*factor
        values['signed_terminal_bps']=float(signed);values['absolute_terminal_bps']=float(abs(signed))
        endpoint_cost_ok=all(r['bid'] is not None and r['ask'] is not None for r in (origin,target))
        if endpoint_cost_ok:
            entry_bid=number(origin['bid']['close']);entry_ask=number(origin['ask']['close'])
            long_net=(number(target['bid']['close'])-entry_ask)*factor
            short_net=(entry_bid-number(target['ask']['close']))*factor
            values.update(long_endpoint_net_bps=float(long_net),short_endpoint_net_bps=float(short_net),
                either_side_endpoint_positive=bool(max(long_net,short_net)>0))
        else:
            unavailable(ENDPOINT_COST_LABELS,'real_bid_ask_endpoint_unavailable')
        path_keys=CONTINUOUS_LABELS[2:]
        if len(future)!=horizon_minutes or any(r['label_epoch']!=origin['label_epoch']+60*(i+1) for i,r in enumerate(future)):
            unavailable(path_keys,'complete_future_minute_path_missing')
            return meta
        values['mid_future_range_bps']=float((max(number(r['mid']['high']) for r in future)-min(number(r['mid']['low']) for r in future))*factor)
        closes=[reference]+[number(r['mid']['close']) for r in future]
        log_returns=[log_return(a,b) for a,b in zip(closes,closes[1:])]
        values['realized_path_variation_bps']=math.sqrt(math.fsum(x*x for x in log_returns))*10000
        if not all(r['bid'] is not None and r['ask'] is not None for r in [origin,*future]):
            unavailable(CONTINUOUS_LABELS[4:],'real_bid_ask_complete_path_unavailable')
            return meta
        for mode in ('close','envelope'):
            bid_upper=[entry_bid]+[number(r['bid']['high' if mode=='envelope' else 'close']) for r in future]
            bid_lower=[entry_bid]+[number(r['bid']['low' if mode=='envelope' else 'close']) for r in future]
            ask_upper=[entry_ask]+[number(r['ask']['high' if mode=='envelope' else 'close']) for r in future]
            ask_lower=[entry_ask]+[number(r['ask']['low' if mode=='envelope' else 'close']) for r in future]
            values['long_'+mode+'_MFE_bps']=float(max(Decimal(0),max(bid_upper)-entry_ask)*factor)
            values['long_'+mode+'_MAE_bps']=float(max(Decimal(0),entry_ask-min(bid_lower))*factor)
            values['short_'+mode+'_MFE_bps']=float(max(Decimal(0),entry_bid-min(ask_lower))*factor)
            values['short_'+mode+'_MAE_bps']=float(max(Decimal(0),max(ask_upper)-entry_bid)*factor)
    need(all(v is None or type(v) is bool or math.isfinite(v) for v in values.values()), 'nonfinite_label')
    return meta
