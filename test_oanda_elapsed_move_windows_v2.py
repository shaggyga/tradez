"""Synthetic-only tests. Execute exact selected production AST, never project imports."""
import ast
import csv
import datetime as dt
import json
import math
import os
import statistics
import types
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo
import pytest

HERE = Path(__file__).absolute().parent
SOURCE = Path(os.environ.get('MOVE_SOURCE_DIR', str(HERE)))
UTC = dt.timezone.utc
T = dt.datetime(2026, 8, 18, 12, tzinfo=UTC)

def extract(file, names, extra=None):
    ns = dict(dt=dt, math=math, csv=csv, json=json, statistics=statistics,
        Path=Path, Any=Any, Mapping=Mapping, Sequence=Sequence, UTC=UTC,
        NEW_YORK=ZoneInfo('America/New_York'), SCHEMA_VERSION='test',
        MAX_CURRENT_QUOTE_AGE_SEC=30, DIRECTIONAL_MOVE_LOOKBACK_MINUTES=1440,
        MOVER_CHART_LOOKBACK_MINUTES=180, MOVER_CHART_MAX_POINTS=90,
        LIVE_VELOCITY_MAX_END_AGE_SEC=900)
    ns.update(extra or {})
    tree = ast.parse((SOURCE/file).read_text(encoding='utf-8'))
    selected = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert {n.name for n in selected} == set(names)
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(SOURCE/file), 'exec'), ns)
    return ns

MOVES = extract('oanda_latest_moves.py', ('safe_float','parse_time','prior_mid',
    'completed_m1_window','_completed_m1_midpoint','_sampled_bucket_observation','load_recent_history','current_quote_status','quote_contract','movement_row',
    'normalized_chart_points','directional_move_legs','completed_m1_observations',
    '_rank','build_payload','markdown_report','latest_weekly_open','cached_open_payloads'))
TECH = extract('oanda_move_first_news_case_audit.py', ('causal_m1_technical_state',))['causal_m1_technical_state']

def technical_rows(count=121, step=1):
    rows=[]
    for i in range(count):
        mid=1.1+i*step*.0001
        row={'timestamp':T+dt.timedelta(minutes=i*step), 'complete':True}
        for side, offset in [('bid',-.00005),('ask',.00005)]:
            row.update({side+'_open':mid+offset, side+'_high':mid+offset+.0001,
                side+'_low':mid+offset-.0001, side+'_close':mid+offset})
        rows.append(row)
    return rows

def technical(rows, **kwargs):
    return TECH(rows, decision_time=kwargs.get('decision', T+dt.timedelta(minutes=121, seconds=30)),
        pip_size=kwargs.get('pip', .0001))

def candles(count=62, step=1, base=1.1, delta=.0001):
    return [{'time':(T+dt.timedelta(minutes=i*step)).isoformat(), 'complete':True,
        'mid':{'c':base+i*delta},'bid':{'c':base+i*delta-.00005},
        'ask':{'c':base+i*delta+.00005}} for i in range(count)]

Q=T+dt.timedelta(minutes=62, seconds=20)
OPEN=T-dt.timedelta(days=2)
def payload(rows=None, *, quote=Q, now=Q, opening=True, tradeable=True):
    c={'recent':candles() if rows is None else rows}
    if opening is not False:
        c['open']={'time':(OPEN if opening is True else opening).isoformat(),
            'complete':True,'bid':{'o':1.1},'ask':{'o':1.1001}}
    return MOVES['build_payload'](now=now, market_open=OPEN,
        prices={'EUR_USD':{'time':quote.isoformat(),'bid':1.13,'ask':1.1301,'tradeable':tradeable}},
        candles={'EUR_USD':c}, pip_by_instrument={'EUR_USD':.0001}, failures=[])

def test_regular_grid_and_unsorted_parity():
    rows=technical_rows();result=technical(rows)
    assert result['state']=='available'
    assert [result[f'return_{n}m_pips'] for n in (5,15,60)]==[5.,15.,60.]
    assert result['sma5_minus_sma20_pips']==7.5
    assert result['sma20_minus_sma60_pips']==20
    assert result['atr14_pips']==result['mean_high_low_range_14_pips']==2
    assert result==technical(rows[::-1])
    assert result['windows']['60m']['elapsed_seconds']==3600
    assert result['historical_arrival_authenticated'] is False

def test_irregular_rows_do_not_masquerade_as_elapsed_minutes():
    result=technical(technical_rows(61,2))
    assert result['return_5m_pips'] is None and result['return_15m_pips'] is None
    assert result['return_60m_pips']==60 # Original row-offset formula returned120.
    assert not result['windows']['60m']['interior_m1_complete']
    assert result['sma5_minus_sma20_pips'] is None and result['atr14_pips'] is None
    assert result['velocity_5m_pips_per_min'] is None

@pytest.mark.parametrize('kind,reason', [('duplicate','duplicate_completed_m1_timestamp'),
    ('stale','stale_completed_m1_endpoint'),('off_grid','off_grid_m1_timestamp'),
    ('naive','invalid_m1_timestamp'),('crossed','invalid_completed_m1_ohlc_or_spread'),
    ('nan','invalid_completed_m1_price')])
def test_technical_refusals(kind,reason):
    rows=technical_rows()
    if kind=='duplicate':rows.append(dict(rows[-1]))
    if kind=='stale':rows.pop()
    if kind=='off_grid':rows[-1]['timestamp']+=dt.timedelta(seconds=1)
    if kind=='naive':rows[-1]['timestamp']=rows[-1]['timestamp'].replace(tzinfo=None)
    if kind=='crossed':rows[-1]['ask_open']=1.
    if kind=='nan':rows[-1]['bid_high']=float('nan')
    result=technical(rows)
    assert result['state']==reason

@pytest.mark.parametrize('pip',[0,-1,float('nan'),float('inf'),True])
def test_invalid_pip_refused(pip):
    assert technical(technical_rows(),pip=pip)['state']=='invalid_pip_size'

def test_technical_future_price_mutation_does_not_change_prefix():
    rows=technical_rows();before=technical(rows)
    rows.append({'timestamp':T+dt.timedelta(minutes=121),'bid_open':object()})
    assert before==technical(rows)

def test_true_range_includes_price_gaps_and_missing_close_not_fabricated():
    rows=technical_rows()
    # Each of last14 bars opens10pips above the preceding actual close.
    for i in range(106,121):
        mid=1.1+(i-106)*.001
        for side,offset in [('bid',-.00005),('ask',.00005)]:
            rows[i].update({side+'_open':mid+offset,side+'_close':mid+offset,
                side+'_high':mid+offset+.0001,side+'_low':mid+offset-.0001})
    result=technical(rows)
    assert result['atr14_pips']==11 and result['mean_high_low_range_14_pips']==2
    rows[119].pop('bid_close')
    result=technical(rows)
    assert result['atr14_pips'] is None
    assert result['feature_missing_reasons']['atr14_pips']=='missing_or_invalid_previous_close'
    assert result['mean_high_low_range_14_pips']==2

def loader_fixture():
    header='datetime,bid_open,ask_open,bid_high,ask_high,bid_low,ask_low,bid_close,ask_close'
    lines=['2026-08-18T12:00:00Z,1.1,1.1001,1.1002,1.1003,1.0998,1.0999,1.10005,1.10015',
        '2026-08-18T12:01:00Z,1.1,1.1001,1.1002,1.1003,1.0998,1.0999,,bad']
    return extract('oanda_news_feed_backtest.py',('load_candles',),{
        'recent_csv_lines':lambda *a,**k:(header,lines),'parse_timestamp':MOVES['parse_time'],
        'news':types.SimpleNamespace(safe_float=MOVES['safe_float'])})['load_candles']

def test_loader_default_parity_and_explicit_close_opt_in():
    load=loader_fixture();old=load(Path('UNOPENED_SYNTHETIC.csv'),since=T)
    assert old[0]==dict(timestamp=T,bid_open=1.1,ask_open=1.1001,bid_high=1.1002,
        ask_high=1.1003,bid_low=1.0998,ask_low=1.0999)
    new=load(Path('UNOPENED_SYNTHETIC.csv'),since=T,include_close=True)
    assert [{k:v for k,v in r.items() if not k.endswith('_close')} for r in new]==old
    assert new[0]['bid_close']==1.10005 and new[1]['bid_close'] is None and new[1]['ask_close'] is None
    tree=ast.parse((SOURCE/'oanda_move_first_news_case_audit.py').read_text())
    calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='load_candles']
    assert len(calls)==2
    assert sum(any(k.arg=='include_close' and ast.literal_eval(k.value) is True for k in call.keywords) for call in calls)==1
    # First cache fill supplies technical state; retained secondary prior-reaction call uses original API.
    assert calls[0].lineno < next(n.lineno for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='causal_m1_technical_state')

def test_completed_close_exact_endpoints_and_order():
    rows=candles();window=MOVES['completed_m1_window'](rows,quote_time=Q,minutes=5)
    assert window['state']=='available' and window['elapsed_seconds']==300
    assert window['reference_completed_utc']==(T+dt.timedelta(minutes=57)).isoformat()
    assert window['reference_bar_open_utc']==(T+dt.timedelta(minutes=56)).isoformat()
    assert window['end_completed_utc']==(T+dt.timedelta(minutes=62)).isoformat()
    assert window['end_age_at_quote_seconds']==20
    assert window==MOVES['completed_m1_window'](rows[::-1],quote_time=Q,minutes=5)
    row=payload()['rows'][0]
    assert row['move_5m_pips']==5 # Live quote1.13 must not substitute for completedclose1.1061.
    assert row['move_5m_pct']==pytest.approx(row['move_5m_bps']/100,abs=1e-5)

@pytest.mark.parametrize('kind,reason',[('reference','exact_completed_reference_missing'),
    ('end','exact_completed_end_missing'),('duplicate','duplicate_m1_endpoint'),
    ('incomplete','exact_completed_end_missing'),('crossed','invalid_m1_bid_ask'),
    ('mismatch','inconsistent_m1_midpoint')])
def test_window_missingness(kind,reason):
    rows=candles()
    if kind=='reference':rows.pop(56)
    if kind=='end':rows.pop()
    if kind=='duplicate':rows.append(dict(rows[-1]))
    if kind=='incomplete':rows[-1]['complete']=False
    if kind=='crossed':rows[-1]['ask']['c']=1
    if kind=='mismatch':rows[-1]['mid']['c']=2
    w=MOVES['completed_m1_window'](rows,quote_time=Q,minutes=5)
    assert w['state']=='unavailable' and w['missing_reason']==reason
    assert MOVES['prior_mid'](rows,quote_time=Q,minutes=5) is None

def test_future_completed_flag_does_not_override_clock():
    rows=candles();before=payload(rows)
    rows.append({'time':(T+dt.timedelta(minutes=62)).isoformat(),'complete':True,'mid':{'c':900}})
    assert before==payload(rows)

@pytest.mark.parametrize('age,tradeable,reason',[(30,True,None),(31,True,'stale_quote'),
    (-1,True,'future_quote_time'),(0,False,'nontradeable_quote'),(0,None,'quote_tradeability_unknown')])
def test_current_quote_gates_all_recent_ranks(age,tradeable,reason):
    p=payload(now=Q+dt.timedelta(seconds=age),tradeable=tradeable)
    row=p['rows'][0]
    assert row['quote_age_seconds']==age and row['quote_missing_reason']==reason
    assert bool(p['rankings']['latest_5m'])==(reason is None)
    assert row['move_5m_pips']==5 # Diagnostic evidence remains; eligibility is separate.

def test_quote_contract_never_invents_tradeability_or_event_clock(tmp_path):
    path=tmp_path/'quotes.json'
    path.write_text(json.dumps({'generated_utc':Q.isoformat(),'quotes':{
        'EUR_USD':{'bid':1.1,'ask':1.1001,'pip':.0001,'tradeable':False}}}))
    _,_,prices=MOVES['quote_contract'](path)
    assert prices['EUR_USD']['time']=='' and prices['EUR_USD']['tradeable'] is False
    assert MOVES['parse_time']('2026-08-18T12:00:00') is None

@pytest.mark.parametrize('opening',[False,OPEN+dt.timedelta(minutes=1)])
def test_current_recent_ranks_independent_of_weekly_open(opening):
    p=payload(opening=opening)
    assert len(p['rows'])==1 and len(p['rankings']['latest_5m'])==1
    assert p['rankings']['since_open_normalized']==[]
    assert p['rankings']['since_open_executable']==[]
    assert 'EUR_USD' in MOVES['markdown_report'](p)

def test_cross_pair_rank_uses_bps_not_pip_scale():
    rows=[{'instrument':'A','move_bps':4.,'move_pips':400.,'current_quote_eligible':True},
        {'instrument':'B','move_bps':-8.,'move_pips':-8.,'current_quote_eligible':True}]
    out=MOVES['_rank'](rows,'move_bps',absolute=True)
    assert [r['instrument'] for r in out]==['B','A'] and out[0]['move_bps']==-8
    assert 'rank' not in rows[0]

def test_missing_windows_are_retained_and_not_ranked():
    p=payload(candles()[:-1]);row=p['rows'][0]
    assert row['move_5m_pips'] is None and not p['rankings']['latest_5m']
    assert row['move_windows']['5m']['missing_reason']=='exact_completed_end_missing'

def test_completed_adapter_reports_end_and_never_selects_duplicate():
    rows=candles(3);rows.append(dict(rows[1]))
    out=MOVES['completed_m1_observations'](rows,asof=T+dt.timedelta(minutes=3))
    assert [r['time'] for r in out]==[(T+dt.timedelta(minutes=i)).isoformat() for i in (1,3)]
    assert out[0]['source_bar_open_utc']==T.isoformat()

def test_calendar_weekly_open_dst_unchanged():
    assert MOVES['latest_weekly_open'](dt.datetime(2026,8,17,17,tzinfo=UTC))==dt.datetime(2026,8,16,21,tzinfo=UTC)
    assert MOVES['latest_weekly_open'](dt.datetime(2026,1,5,17,tzinfo=UTC))==dt.datetime(2026,1,4,22,tzinfo=UTC)

@pytest.mark.parametrize('field',['now','market_open'])
def test_naive_build_clock_refused_before_any_calculation(field):
    kwargs=dict(now=Q,market_open=OPEN,prices={},candles={},pip_by_instrument={},failures=[])
    kwargs[field]=kwargs[field].replace(tzinfo=None)
    with pytest.raises(ValueError,match='timezone_aware'):
        MOVES['build_payload'](**kwargs)

def test_executable_cross_pair_ranking_uses_percentage_scale():
    c={};prices={}
    for pair,base,gain in [('A_B',100.,.1),('C_D',1.,.002)]:
        prices[pair]={'bid':base+gain,'ask':base+gain+.0001,'time':Q.isoformat(),'tradeable':True}
        c[pair]={'open':{'time':OPEN.isoformat(),'complete':True,
            'bid':{'o':base},'ask':{'o':base+.0001}},'recent':[]}
    p=MOVES['build_payload'](now=Q,market_open=OPEN,prices=prices,candles=c,
        pip_by_instrument={'A_B':.0001,'C_D':.0001},failures=[])
    assert p['rows'][0]['best_executable_net_pips']>p['rows'][1]['best_executable_net_pips']
    assert [r['instrument'] for r in p['rankings']['since_open_executable']]==['C_D','A_B']

def test_exact_staleness_boundary_and_nonfuture_completed_clock():
    rows=technical_rows()
    assert technical(rows,decision=T+dt.timedelta(minutes=122))['state']=='stale_completed_m1_endpoint'
    assert technical(rows,decision=T+dt.timedelta(minutes=121,seconds=59))['state']=='available'

LEGACY_MOVE_TESTS=[
    'test_latest_weekly_open_respects_new_york_dst',
    'test_movement_row_separates_mid_move_from_executable_cost',
    'test_wide_spread_move_does_not_become_executable_winner',
    'test_build_payload_has_separate_normalized_and_executable_rankings',
    'test_delayed_open_is_retained_but_excluded_from_rankings',
    'test_cached_open_requires_same_weekly_open',
    'test_directional_move_legs_preserve_rally_and_reversal_not_endpoint_only',
    'test_directional_move_legs_reject_spread_sized_noise',
    'test_live_velocity_excludes_active_leg_with_stale_candle_endpoint']

@pytest.mark.parametrize('name',LEGACY_MOVE_TESTS)
def test_retained_mover_assertions(name,tmp_path):
    tree=ast.parse((SOURCE/'test_oanda_latest_moves.py').read_text())
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in (name,'_candles','_path_row')]
    ns=dict(MOVES)
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'retained_exact_test_AST','exec'),ns)
    if name=='test_cached_open_requires_same_weekly_open':ns[name](tmp_path)
    else:ns[name]()

@pytest.mark.parametrize('name',['test_causal_m1_state_uses_only_fully_completed_bars','test_causal_m1_state_fails_closed_without_history'])
def test_retained_technical_assertions(name):
    import unittest
    tree=ast.parse((SOURCE/'test_oanda_move_first_news_case_audit.py').read_text())
    cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='MoveFirstNewsAuditTests')
    method=next(n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name==name)
    ns={'dt':dt,'audit':types.SimpleNamespace(causal_m1_technical_state=TECH)}
    exec(compile(ast.Module(body=[method],type_ignores=[]),'retained_exact_test_AST','exec'),ns)
    ns[name](unittest.TestCase())


@pytest.mark.parametrize('missing',['bid_high','ask_low'])
def test_opt_in_missing_extrema_never_substitute_open(missing):
    keys=['datetime','bid_open','ask_open','bid_high','ask_high','bid_low','ask_low','bid_close','ask_close']
    values=[T.isoformat(),'1.1','1.1001','1.1002','1.1003','1.0998','1.0999','1.10005','1.10015']
    values[keys.index(missing)]=''
    load=extract('oanda_news_feed_backtest.py',('load_candles',),{
        'recent_csv_lines':lambda *a,**k:(','.join(keys),[','.join(values)]),
        'parse_timestamp':MOVES['parse_time'],'news':types.SimpleNamespace(safe_float=MOVES['safe_float'])})['load_candles']
    old=load(Path('UNOPENED_SYNTHETIC.csv'),since=T)
    new=load(Path('UNOPENED_SYNTHETIC.csv'),since=T,include_close=True)
    assert old[0][missing]==old[0][missing.split('_')[0]+'_open']
    assert new[0][missing] is None
    result=technical(new,decision=T+dt.timedelta(minutes=1,seconds=30))
    assert result['atr14_pips'] is None and result['state']=='invalid_completed_m1_price'

def test_weekly_open_complete_flag_cannot_precede_bar_end():
    p=MOVES['build_payload'](now=OPEN+dt.timedelta(seconds=30),market_open=OPEN,
        prices={'A_B':{'time':(OPEN+dt.timedelta(seconds=30)).isoformat(),'bid':1.1,'ask':1.1002,'tradeable':True}},
        candles={'A_B':{'open':{'time':OPEN.isoformat(),'complete':True,'bid':{'o':1.1},'ask':{'o':1.1001}},'recent':[]}},
        pip_by_instrument={'A_B':.0001},failures=[])
    assert not p['rows'][0]['opening_reference_valid']
    assert not p['rankings']['since_open_normalized']

def test_direct_movement_invalid_clock_returns_no_row():
    assert MOVES['movement_row'](instrument='A_B',pip=.0001,market_open=OPEN,
        price={'time':Q.isoformat(),'bid':1.1,'ask':1.1001},candle_payload={},now=Q.replace(tzinfo=None)) is None

def sampled_history(tmp_path):
    rows=[]
    for i in range(62):
        epoch=(T+dt.timedelta(minutes=i)).timestamp()
        rows.append({'instrument':'EUR_USD','minute_epoch':int(epoch),'first_epoch':epoch+1,
            'last_epoch':epoch+(10 if i==56 else 50),'close_bid':1.1+i*.0001-.00005,
            'close_ask':1.1+i*.0001+.00005})
    path=tmp_path/'history.json';path.write_text(json.dumps({'rows':rows}))
    return path,rows

def test_actual_sampled_history_keeps_requested_and_observed_spans_separate(tmp_path):
    path,rows=sampled_history(tmp_path)
    history=MOVES['load_recent_history'](path)['EUR_USD']
    w=MOVES['completed_m1_window'](history,quote_time=Q,minutes=5)
    assert w['state']=='available' and w['price_basis']=='sampled_quote_minute_last_observation'
    assert w['requested_bucket_span_seconds']==300
    assert w['actual_price_observation_span_seconds']==340
    assert w['end_observation_age_at_quote_seconds']==30
    assert w['end_age_at_quote_seconds']==20 # Bucket-endage is distinct.
    assert w['reference_price_observed_utc']==dt.datetime.fromtimestamp(rows[56]['last_epoch'],tz=UTC).isoformat()
    assert w['end_price_observed_utc']==history[-1]['last_sample_utc']
    assert not w['historical_arrival_authenticated']
    points=MOVES['completed_m1_observations'](history,asof=Q)
    assert points[-1]['time']==history[-1]['last_sample_utc']
    assert points[-1]['time']!=points[-1]['source_bucket_end_utc']
    p=payload(history)
    assert p['rows'][0]['move_5m_pips']==5

@pytest.mark.parametrize('mutation',['missing','backward','future','out_of_bucket','bool','fractional_minute'])
def test_sample_clocks_never_fabricated_from_bucket_boundaries(tmp_path,mutation):
    path,rows=sampled_history(tmp_path);r=rows[-1]
    if mutation=='missing':r.pop('last_epoch')
    if mutation=='backward':r['last_epoch']=r['first_epoch']-1
    if mutation=='future':r['last_epoch']=Q.timestamp()+1
    if mutation=='out_of_bucket':r['last_epoch']=r['minute_epoch']+60
    if mutation=='bool':r['last_epoch']=True
    if mutation=='fractional_minute':r['minute_epoch']+=.5
    path.write_text(json.dumps({'rows':rows}))
    history=MOVES['load_recent_history'](path)['EUR_USD']
    w=MOVES['completed_m1_window'](history,quote_time=Q,minutes=5)
    assert w['state']=='unavailable'
    assert w['missing_reason'] in ('invalid_m1_timestamp','missing_or_invalid_sample_observation_clock')
    assert not any(p['time']==(T+dt.timedelta(minutes=61,seconds=50)).isoformat()
        for p in MOVES['completed_m1_observations'](history,asof=Q))

def test_sampled_and_candle_bases_cannot_be_silently_mixed(tmp_path):
    path,_=sampled_history(tmp_path);history=MOVES['load_recent_history'](path)['EUR_USD']
    history[-1]=candles()[-1]
    assert MOVES['completed_m1_window'](history,quote_time=Q,minutes=5)['missing_reason']=='mixed_m1_price_source_basis'
    regular=MOVES['completed_m1_window'](candles(),quote_time=Q,minutes=5)
    assert regular['actual_price_observation_span_seconds']==regular['requested_bucket_span_seconds']==300
    assert regular['price_basis']=='completed_m1_midpoint_close'

def test_invalid_previous_close_outside_its_extrema_cannot_create_atr():
    rows=technical_rows();rows[-2].update(bid_close=3.,ask_close=3.0001)
    result=technical(rows)
    assert result['atr14_pips'] is None and result['state']=='partial'
    assert result['feature_missing_reasons']['atr14_pips']=='missing_or_invalid_previous_close'

def test_inconsistent_midpoint_does_not_reach_chart_or_ranked_leg():
    rows=candles();bad_end=(T+dt.timedelta(minutes=62)).isoformat();rows[-1]['mid']['c']+=.004
    p=payload(rows)
    assert p['rows'][0]['move_windows']['5m']['missing_reason']=='inconsistent_m1_midpoint'
    assert all(r['end_utc']!=bad_end for r in p['rankings']['live_velocity'])
    points=MOVES['completed_m1_observations'](rows,asof=Q)
    assert all(r['time']!=bad_end for r in points)

def test_duplicate_invalid_sample_clock_does_not_select_valid_variant(tmp_path):
    path,_=sampled_history(tmp_path);history=MOVES['load_recent_history'](path)['EUR_USD']
    bad=dict(history[-1]);bad['source_last_event_epoch']=None
    points=MOVES['completed_m1_observations']([bad,*history],asof=Q)
    assert all(r['source_bar_open_utc']!=history[-1]['time'] for r in points)
