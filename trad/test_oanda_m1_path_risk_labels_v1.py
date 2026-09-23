import csv
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import io
import math

import pytest
import oanda_m1_path_risk_labels_v1 as labels

START=1780000020

def records(n=260):
    out=[]
    with localcontext() as ctx:
        ctx.prec=50
        for i in range(n):
            c=Decimal('1.2')+Decimal(i)*Decimal('.00001')
            row={'time':datetime.fromtimestamp(START+i*60,timezone.utc).isoformat(),
                'instrument':'EUR_USD','granularity':'M1','volume':'1'}
            for side,delta in (('',Decimal(0)),('bid_',Decimal('-.00005')),('ask_',Decimal('.00005'))):
                row.update({side+'open':str(c+delta),side+'close':str(c+delta),
                    side+'high':str(c+delta+Decimal('.00002')),side+'low':str(c+delta-Decimal('.00002'))})
            out.append(row)
    return out

def raw(rows):
    f=io.StringIO();w=csv.DictWriter(f,fieldnames=rows[0]);w.writeheader();w.writerows(rows)
    return f.getvalue().encode()

def parsed(rows=None):
    rows=records() if rows is None else rows
    return labels.parse_csv(raw(rows),'EUR_USD',observed_epoch=START+300*60)[0]

def test_rising_market_distinct_cost_orientation_and_spread():
    rs=parsed();origin=203;result=labels.label_origin(rs,origin,15)
    v=result['values'];reference=Decimal(rs[origin]['mid']['close']);factor=Decimal(10000)/reference
    assert v['signed_terminal_bps']==pytest.approx(float(Decimal('.00015')*factor))
    assert v['long_endpoint_net_bps']==pytest.approx(float(Decimal('.00005')*factor))
    assert v['short_endpoint_net_bps']==pytest.approx(float(Decimal('-.00025')*factor))
    assert v['long_close_MAE_bps']==pytest.approx(float(Decimal('.0001')*factor))
    assert v['long_envelope_MFE_bps']>v['long_close_MFE_bps']
    assert v['short_envelope_MAE_bps']>v['short_close_MAE_bps']
    assert v['either_side_endpoint_positive'] is True
    assert result['target_price_epoch']==rs[origin]['price_epoch']+900
    assert result['barrier_order_known'] is False and result['broker_fill_observed'] is False

def test_origin_extremes_excluded_and_future_extremes_retained():
    rows=records();base=labels.label_origin(parsed(rows),203,15)
    rows[203]['high']='9';rows[203]['low']='.1'
    rows[203]['bid_high']='8';rows[203]['ask_high']='9';rows[203]['bid_low']='0.1';rows[203]['ask_low']='0.2'
    # Decimal source format is deliberately explicit, without omitted leading zero.
    rows[203]['low']='0.1'
    assert labels.label_origin(parsed(rows),203,15)['values']==base['values']
    rows[204]['high']='2';rows[204]['bid_high']='1.99995';rows[204]['ask_high']='2.00005'
    changed=labels.label_origin(parsed(rows),203,15)['values']
    assert changed['long_close_MFE_bps']==base['values']['long_close_MFE_bps']
    assert changed['long_envelope_MFE_bps']>base['values']['long_envelope_MFE_bps']

def test_missing_interior_keeps_endpoint_but_not_path():
    rs=parsed();del rs[210]
    result=labels.label_origin(rs,203,15)
    assert result['values']['signed_terminal_bps'] is not None
    assert result['values']['long_endpoint_net_bps'] is not None
    assert result['values']['mid_future_range_bps'] is None
    assert result['unavailable_reasons']['long_close_MFE_bps']=='complete_future_minute_path_missing'

def test_missing_target_does_not_shift_to_next_row():
    rs=parsed();del rs[218]
    result=labels.label_origin(rs,203,15)
    assert all(v is None for v in result['values'].values())
    assert set(result['unavailable_reasons'].values())=={'exact_target_missing'}

def test_missing_interior_bid_ask_not_filled_but_mid_survives():
    rows=records();rows[210]['bid_close']=''
    result=labels.label_origin(parsed(rows),203,15)
    assert result['values']['mid_future_range_bps'] is not None
    assert result['values']['long_endpoint_net_bps'] is not None
    assert result['values']['long_envelope_MAE_bps'] is None

@pytest.mark.parametrize('side',['bid','ask'])
def test_missing_endpoint_cost_does_not_invent_spread(side):
    rows=records();rows[218][side+'_close']=''
    rs,audit=labels.parse_csv(raw(rows),'EUR_USD',observed_epoch=START+300*60)
    result=labels.label_origin(rs,203,15)
    assert result['values']['signed_terminal_bps'] is not None
    assert result['values']['either_side_endpoint_positive'] is None
    assert audit['bid_ask_missingness']=={'real_bid_ask_OHLC_unavailable':1}

def test_low_decimal_context_does_not_change_labels_or_scale():
    rs=parsed();expected=labels.label_origin(rs,203,30);scale=labels.past_volatility_scale(rs,203,30)
    with localcontext() as ctx:
        ctx.prec=3
        assert labels.label_origin(rs,203,30)==expected
        assert labels.past_volatility_scale(rs,203,30)==scale

def test_past_scale_ignores_all_future_changes():
    rs=parsed();expected=labels.past_volatility_scale(rs,203,60)
    for row in rs[204:]:
        row['mid']['close']='10'
    assert labels.past_volatility_scale(rs,203,60)==expected

def test_past_scale_requires_real_minutes_and_reports_zero():
    rs=parsed();del rs[180]
    assert labels.past_volatility_scale(rs,202,15)==(None,'past60_complete_support_missing')
    rs=parsed()
    for r in rs:r['mid']['close']='1.2'
    assert labels.past_volatility_scale(rs,203,15)==(None,'zero_or_nonfinite_past_volatility')

def test_variation_uses_each_return_not_endpoint_only():
    rows=records();rs=parsed(rows);reference=rs[203]['mid']['close']
    for i in range(204,219):
        rs[i]['mid']['close']=reference if i%2==0 else str(Decimal(reference)+Decimal('.001'))
    rs[218]['mid']['close']=reference
    r=labels.label_origin(rs,203,15)
    assert r['values']['signed_terminal_bps']==0
    assert r['values']['realized_path_variation_bps']>0

@pytest.mark.parametrize('change,reason',[
    ('duplicate','duplicate_or_unordered_bar'),('timezone','UTC_minute_label_required'),
    ('pair','source_row_identity'),('granularity','source_row_identity'),
    ('geometry','invalid_OHLC_geometry'),('future','bar_future_at_capture'),
    ('incomplete','explicit_incomplete_bar'),('partial','source_byte_bound_or_partial_tail')])
def test_source_integrity(change,reason):
    rows=records(5);observed=START+300*60
    if change=='duplicate':rows[2]['time']=rows[1]['time']
    if change=='timezone':rows[2]['time']=rows[2]['time'].replace('+00:00','-04:00')
    if change=='pair':rows[2]['instrument']='GBP_USD'
    if change=='granularity':rows[2]['granularity']='M5'
    if change=='geometry':rows[2]['high']='0.1'
    if change=='future':observed=START+60
    if change=='incomplete':
        for row in rows:row['complete']='false'
    payload=raw(rows)
    if change=='partial':payload=payload[:-1]
    with pytest.raises(ValueError,match=reason):labels.parse_csv(payload,'EUR_USD',observed_epoch=observed)

def test_weekend_gap_creates_separate_sessions():
    rs=parsed()
    for r in rs[100:]:r['label_epoch']+=2*86400;r['price_epoch']+=2*86400
    assert labels.contiguous_segments(rs)==[(0,100),(100,260)]
    assert labels.label_origin(rs,90,15)['values']['signed_terminal_bps'] is None

def test_capture_does_not_claim_historical_arrival_or_provider_proof():
    _,audit=labels.parse_csv(raw(records()),'EUR_USD',observed_epoch=START+300*60)
    assert audit['original_ingestion_availability_proven'] is False
    assert audit['provider_completeness_attested'] is False
    assert audit['can_place_orders'] is False

def test_large_real_price_ratio_variation_stays_finite():
    rs=parsed()
    rs[205]['mid']['close']='0.00000000000000000001'
    assert math.isfinite(labels.label_origin(rs,203,15)['values']['realized_path_variation_bps'])

@pytest.mark.parametrize('h',[15.0,True,'15'])
def test_native_horizon_type_is_explicit(h):
    with pytest.raises(ValueError):labels.label_origin(parsed(),203,h)

@pytest.mark.parametrize('h',[15,30,60])
def test_all_native_horizons_exact_and_no_input_mutation(h):
    rs=parsed(records(300));import copy
    before=copy.deepcopy(rs)
    result=labels.label_origin(rs,203,h)
    assert result['future_path_rows']==h and result['target_price_epoch']==rs[203]['price_epoch']+h*60
    assert rs==before
