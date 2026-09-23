import copy,importlib.util,math
from datetime import datetime,timezone
from pathlib import Path
import pytest
spec=importlib.util.spec_from_file_location('brief',Path(__file__).parent/'tools/forex_market_brief_v1.py');brief=importlib.util.module_from_spec(spec);spec.loader.exec_module(brief)
BASE=int(datetime(2026,9,15,9,tzinfo=timezone.utc).timestamp())

def rows(n=80):
    out=[]
    for i in range(n):
        c=1+i*.001
        out.append({'time':brief.utc(BASE+i*300),'complete':True,'volume':5.,**{s:{'o':c,'h':c+.001,'l':c-.001,'c':c} for s in ('mid','bid','ask')}})
    return out


def test_exact_elapsed_returns_and_rising_indicators():
    data=rows();end=BASE+80*300;r=brief.technical(data,now=end+20,anchor=end)
    assert r['returns_bps']['15m']==pytest.approx((1.079/1.076-1)*10000)
    assert r['returns_bps']['60m']==pytest.approx((1.079/1.067-1)*10000)
    assert r['trend']=='up' and r['rsi14']==100 and r['atr14_bps']>0


def test_future_mutation_cannot_change_historical_features():
    data=rows();end=BASE+70*300;a=brief.technical(data,now=end+10,anchor=end)
    for r in data[70:]:r['mid']['c']=float('nan');r['complete']=False
    assert brief.technical(data,now=end+10,anchor=end)==a


def test_missing_intermediate_bar_cannot_be_row_count_return():
    data=rows();del data[-3];end=BASE+80*300
    r=brief.technical(data,now=end+10,anchor=end)
    assert r['indicator_rows']==2 and r['returns_bps']=={'15m':None,'60m':None}
    assert r['ema20'] is None and r['rsi14'] is None


@pytest.mark.parametrize('delta',[1,60])
def test_aligned_M5_clock_required(delta):
    data=rows();data[-1]['time']=brief.utc(BASE+79*300+delta)
    with pytest.raises(ValueError,match='unaligned'):
        brief.technical(data,now=BASE+81*300,anchor=BASE+81*300)


def test_missing_endpoint_and_flat_are_explicit():
    data=rows();end=BASE+81*300
    r=brief.technical(data,now=end+10,anchor=end)
    assert r['status']=='missing_analysis_endpoint' and r['returns_bps']['60m'] is None
    data=rows()
    for row in data:
        for s in ('mid','bid','ask'):row[s]={'o':1.,'h':1.,'l':1.,'c':1.}
    r=brief.technical(data,now=end,anchor=end-300)
    assert r['rsi14']==50 and r['atr14_bps']==0 and r['trend']=='mixed'


def test_registry_scope_does_not_shrink_for_sparse_histories():
    pairs=sorted(brief.metadata.verified_pip_sizes());end=BASE+80*300
    report=brief.build(pairs,{'EUR_USD':rows()},None,None,now=end+10)
    assert set(report['pairs'])==set(pairs) and report['coverage']['registered_pairs']==68
    assert report['coverage']['return60_pairs']==1
    assert report['positions']['status']=='unverified'
    tiny=brief.compact(report)
    assert tiny['EUR_USD']['pair']=='EUR_USD' and tiny['EUR_USD']['quote']=='unavailable'
    assert not tiny['top_down']
    with pytest.raises(ValueError,match='unique_68'):brief.build(pairs[:-1],{},None,None,now=end)


def test_strength_common_clock_and_cross_signs():
    pairs=sorted(brief.metadata.verified_pip_sizes());end=BASE+80*300
    report=brief.build(pairs,{'EUR_USD':rows(),'GBP_USD':rows()},None,None,now=end+10)
    strength={r['currency']:r for r in report['currency_strength']['60m']}
    assert strength['USD']['crosses']==2 and strength['USD']['mean_cross_return_bps']<0
    assert strength['EUR']['crosses']==1 and strength['EUR']['mean_cross_return_bps']>0
    assert len({r['observed_end_utc'] for r in report['pairs'].values() if r['status']=='current'})==1


def test_quote_freshness_and_tradeability_separate():
    now=BASE+80*300
    snapshot={'generated_utc':brief.utc(now),'connection_generation':1,'quotes':{'EUR_USD':{'time':brief.utc(now-10),'bid':1.,'ask':1.0002,'tradeable':False}}}
    heartbeat={'updated_at':brief.utc(now),'details':{'stream':{'connected':True,'connection_generation':1}}}
    q=brief.quote('EUR_USD',snapshot,heartbeat,now)
    assert q['status']=='not_tradeable' and q['tradeable'] is False and q['spread_bps']>0
    snapshot['quotes']['EUR_USD']['tradeable']=True
    snapshot['quotes']['EUR_USD']['time']=brief.utc(now-121)
    assert brief.quote('EUR_USD',snapshot,heartbeat,now)['status']=='stale'


def test_stale_bars_not_ranked_or_counted_as_current_features():
    pairs=sorted(brief.metadata.verified_pip_sizes());now=BASE+90*300
    report=brief.build(pairs,{'EUR_USD':rows()},None,None,now=now)
    assert report['coverage']['return60_pairs']==0 and not brief.compact(report)['top_up']


def test_shared_boundary_prefers_complete_cross_section_during_native_update():
    pairs=sorted(brief.metadata.verified_pip_sizes());end=BASE+80*300
    report=brief.build(pairs,{'EUR_USD':rows(),'GBP_USD':rows(79)},None,None,now=end+20)
    assert report['analysis_end_utc']==brief.utc(end-300)
    assert report['coverage']['return60_pairs']==2


def test_capture_observation_clock_is_taken_after_live_file_reads(monkeypatch):
    pairs=sorted(brief.metadata.verified_pip_sizes());end=BASE+80*300
    monkeypatch.setattr(brief.metadata,'verified_pip_sizes',lambda:{p:.0001 for p in pairs})
    monkeypatch.setattr(brief.candles,'read_tail',lambda *a,**k:(rows(),{}))
    snapshot={'generated_utc':brief.utc(end+15),'connection_generation':1,'quotes':{p:{'time':brief.utc(end+14),'bid':1.,'ask':1.0002,'tradeable':True} for p in pairs}}
    heartbeat={'updated_at':brief.utc(end+15),'details':{'stream':{'connected':True,'connection_generation':1}}}
    monkeypatch.setattr(brief.availability,'read_json',lambda path,limit:(snapshot if path==brief.availability.DEFAULT_QUOTES else heartbeat,{}))
    clocks=iter([end+10,end+20]);monkeypatch.setattr(brief.time,'time',lambda:next(clocks))
    result=brief.capture()
    assert result['coverage']['current_tradeable_quotes']==68
    assert result['observed_utc']==brief.utc(end+20)
