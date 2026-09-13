"""Fixture-only market overview checks: no broker, study or account mutations."""
import csv,json
from datetime import datetime,timezone
from pathlib import Path
import pytest
import oanda_market_overview as subject

NOW=1788796800.0
def stamp(t):return datetime.fromtimestamp(t,timezone.utc).isoformat()
@pytest.fixture
def market(tmp_path):
    state=tmp_path/'state';state.mkdir();candles=tmp_path/'candles';candles.mkdir()
    source={'producer':'practice_007_dedicated_quote_stream','generated_utc':stamp(NOW),
        'coverage':{'retained_last_known_instruments':[]},
        'quotes':{'EUR_USD':{'bid':1.101,'ask':1.1012,'pip':.0001,'time':stamp(NOW),
                            'tradeable':True,'source':'stream'}}}
    path=state/'practice_007_market_quotes_v1.json'
    path.write_text(json.dumps(source))
    with (candles/'EUR_USD_M1.csv').open('w',newline='') as f:
        writer=csv.writer(f);writer.writerow(['time','datetime','instrument','granularity','close'])
        for n in range(160):
            t=NOW-(160-n)*60
            writer.writerow([stamp(t),stamp(t),'EUR_USD','M1','1.1'])
    return tmp_path,source,path
def get(market):return subject.build_market_overview(market[0],now_epoch=NOW)
def save(market):market[2].write_text(json.dumps(market[1]))
def test_live_moves_are_signed_midpoint_changes(market):
    result=get(market);row=result['rows'][0]
    assert result['status']=='current' and result['can_place_orders'] is False
    assert row['changes']['5m']['price_change_pips']==11
    assert row['changes']['5m']['return_bps']==10
    assert row['changes']['5m']['actual_seconds']==300
    assert row['spread_pips']==2 and row['technical']['trend_5m']=='up'
    assert result['observed_epoch']==NOW
@pytest.mark.parametrize('field,value,expected',[('time',stamp(NOW-61),'stale'),('time',stamp(NOW+1),'unavailable'),
    ('source','retained','unavailable'),('tradeable',False,'closed'),('ask',1.0,'unavailable'),('bid',True,'unavailable')])
def test_invalid_prices_never_become_live_moves(market,field,value,expected):
    market[1]['quotes']['EUR_USD'][field]=value;save(market)
    row=get(market)['rows'][0]
    assert row['status']==expected and all(v is None for v in row['changes'].values())
def test_retained_generation_is_not_current(market):
    market[1]['coverage']['retained_last_known_instruments']=['EUR_USD'];save(market)
    assert get(market)['current_pair_count']==0
@pytest.mark.parametrize('clock',[NOW-61,NOW+1])
def test_publication_clock_invalidates_snapshot(market,clock):
    market[1]['generated_utc']=stamp(clock);save(market)
    assert get(market)['status']=='unavailable'
def test_missing_history_keeps_price_but_no_invented_indicator(market):
    (market[0]/'candles/EUR_USD_M1.csv').unlink()
    row=get(market)['rows'][0]
    assert row['status']=='current' and row['technical']['status']=='unavailable'
def test_missing_anchor_is_bounded_not_forward_filled(market):
    path=market[0]/'candles/EUR_USD_M1.csv';lines=path.read_text().splitlines()
    filtered=[lines[0]]+[line for line in lines[1:] if not NOW-600<=subject._epoch(line.split(',')[0])<=NOW-300]
    path.write_text('\n'.join(filtered)+'\n')
    row=get(market)['rows'][0]
    assert row['changes']['5m'] is None and row['changes']['15m'] is not None
def test_future_bar_cannot_anchor_past_move(market):
    row=get(market)['rows'][0]
    assert all(v['anchor_epoch']<=NOW-subject.WINDOWS[k] for k,v in row['changes'].items())
def test_cache_invalidates_on_replacement(market):
    assert get(market)['rows'][0]['changes']['5m']['price_change_pips']==11
    path=market[0]/'candles/EUR_USD_M1.csv';changed=path.with_suffix('.new')
    changed.write_text(path.read_text().replace(',1.1\n',',1.1011\n'));changed.replace(path)
    assert get(market)['rows'][0]['changes']['5m']['price_change_pips']==0
def test_timestamp_duplicate_disables_history(market):
    path=market[0]/'candles/EUR_USD_M1.csv';raw=path.read_text();path.write_text(raw+raw.splitlines()[-1]+'\n')
    row=get(market)['rows'][0]
    assert row['history_status']=='unavailable'
def test_snapshot_size_bounded(market):
    market[2].write_bytes(b' '*(subject.MAX_QUOTE_BYTES+1))
    assert get(market)['reason']=='oversized_quote_snapshot'

@pytest.mark.parametrize('value',['1e400','1e-400','1e999999','9'*2000,'NaN','Infinity'])
def test_unrepresentable_decimal_quote_cannot_be_marked_current(market,value):
    market[1]['quotes']['EUR_USD'].update(bid=value,ask=value);save(market)
    result=get(market)
    assert result['rows'][0]['status']=='unavailable'
    assert result['current_pair_count']==0
    json.dumps(result,allow_nan=False)

def test_derived_spread_overflow_is_unavailable_not_infinity(market):
    market[1]['quotes']['EUR_USD'].update(bid='1e-300',ask='1e300',pip='1e-300');save(market)
    result=get(market)
    assert result['rows'][0]['status']=='unavailable'
    assert result['rows'][0]['reason']=='unrepresentable_display_metric'
    json.dumps(result,allow_nan=False)

def test_derived_change_overflow_disables_history_but_preserves_finite_quote(market,monkeypatch):
    market[1]['quotes']['EUR_USD'].update(bid='1e300',ask='1e300',pip='1e-300');save(market)
    monkeypatch.setattr(subject,'_history',lambda *args:[(NOW-3600,subject.Decimal('1e-300')),
                                                       (NOW-900,subject.Decimal('1e-300')),
                                                       (NOW-300,subject.Decimal('1e-300'))])
    result=get(market);row=result['rows'][0]
    assert row['status']=='current' and row['history_status']=='unavailable'
    assert row['technical']['status']=='unavailable'
    assert all(value is None for value in row['changes'].values())
    json.dumps(result,allow_nan=False)

def test_tiny_real_change_cannot_underflow_to_false_flat(market,monkeypatch):
    from decimal import Decimal,localcontext
    with localcontext() as context:
        context.prec=200
        quote=str(Decimal('1.1')+Decimal('1e-100'))
    market[1]['quotes']['EUR_USD'].update(bid=quote,ask=quote,pip='1e300');save(market)
    monkeypatch.setattr(subject,'_history',lambda *args:[(NOW-3600,Decimal('1.1')),
                                                       (NOW-900,Decimal('1.1')),
                                                       (NOW-300,Decimal('1.1'))])
    row=get(market)['rows'][0]
    assert row['history_status']=='unavailable' and row['technical']['status']=='unavailable'

def test_caller_decimal_precision_does_not_change_display(market):
    from decimal import localcontext
    expected=get(market)
    with localcontext() as context:
        context.prec=2
        assert get(market)==expected

def test_recursive_json_fails_closed_instead_of_crashing_dashboard(market):
    market[2].write_bytes(b'['*2000+b'0'+b']'*2000)
    result=get(market)
    assert result['status']=='unavailable' and result['rows']==[]
