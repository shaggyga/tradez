from copy import deepcopy
from types import SimpleNamespace
import json
from pathlib import Path
import numpy as np
import pandas as pd
import pytest
from contracts import fingerprint
from causal_technical_adapter_v2 import pair_records,contract
from historical_market_inputs_v2 import record
from later_input_extension_v2 import verify_prefix,indexed,prepare_market,market_consumer


def original():
    return {'instrument':'AUD_CAD','quality':{'duplicates':0},'observations':[{'record_id':'AUD_CAD:1','features':[1.0]}],
      'outcomes':[{'record_id':'AUD_CAD:1','target_id':'h','value':2.0}],
      'path_outcomes':[{'value':3.0}],'calendar_coverage':[{'status':'blocked'}]}


@pytest.mark.parametrize('field',['observations','outcomes','path_outcomes','calendar_coverage','quality'])
def test_original_overlap_cannot_silently_change(field):
    old=original();new=deepcopy(old)
    if field=='quality':new[field]['duplicates']=1
    elif field=='outcomes':new[field][0]['value']=2.1
    elif field=='observations':new[field][0]['features']=[2.0]
    else:new[field][0]['changed']=True
    with pytest.raises(ValueError,match='extension_original'):verify_prefix(new,old,[])


def test_added_future_rows_preserve_exact_original_prefix_and_extra_target():
    old=original();new=deepcopy(old);extra={'record_id':'AUD_CAD:1','target_id':'extra','value':None}
    new['observations'].append({'record_id':'AUD_CAD:2','features':None});new['outcomes'].append(extra)
    proof=verify_prefix(new,old,[extra]);assert proof['observations_identical']==1 and proof['remaining_labels_identical']==1


def test_duplicate_or_missing_original_target_refuses():
    row={'record_id':'p:1','target_id':'h'}
    with pytest.raises(ValueError,match='duplicate'):indexed([row,row])
    old=original();new=deepcopy(old);new['outcomes']=[]
    with pytest.raises(ValueError,match='label_missing'):verify_prefix(new,old,[])


def test_future_raw_price_perturbation_cannot_change_earlier26features():
    n=5000;t=np.arange(n,dtype=np.int64)*60;mid=1+np.arange(n)*.000001+.001*np.sin(np.arange(n)/100)
    raw=pd.DataFrame({'epoch':t,'mid':mid,'bid':mid-.0001,'ask':mid+.0001})
    c={**contract(),'data_start':0,'data_end':300000,'origin_start':120060,'origin_end':240000,
        'target_minutes':[15],'endpoint_minutes':[15]}
    before=pair_records('AUD_CAD',raw,.0001,'a'*64,c)
    changed=raw.copy();cutoff=163260;mask=changed.epoch>=cutoff;factor=1+.02*(changed.loc[mask,'epoch']-cutoff+60)/(300000-cutoff)
    for name in ('mid','bid','ask'):changed.loc[mask,name]=changed.loc[mask,name]*factor
    after=pair_records('AUD_CAD',changed,.0001,'a'*64,c)
    first=[r for r in before['observations'] if r['origin_epoch']<=cutoff];second=[r for r in after['observations'] if r['origin_epoch']<=cutoff]
    assert fingerprint(first)==fingerprint(second) and all(len(r['features'])==26 for r in first)
    assert fingerprint(before['observations'])!=fingerprint(after['observations'])


def test_original_market_cannot_be_replaced(monkeypatch):
    monkeypatch.setattr('later_input_extension_v2.market_inputs',lambda *a:{'changed':True})
    with pytest.raises(ValueError,match='original_market_changed'):
        prepare_market(None,None,{'name':'original_overlap','policy_origins':[],'target':1},{'changed':False},None)


def test_missing_conversion_is_explicit_not_a_zero_financing_charge(monkeypatch):
    c=json.loads((Path(__file__).parent/'LATER_INPUT_EXTENSION_CONTRACT_V2.json').read_text());cohort=c['cohorts'][0]
    epochs=sorted(set(cohort['policy_origins']+[t+60 for t in cohort['policy_origins']]+[cohort['target']-60,cohort['target']]))
    def rates(currency,*args):
        if currency!='USD':raise ValueError('missing')
        return {'buy_currency_usd':'1','sell_currency_usd':'1'}
    monkeypatch.setattr('later_input_extension_v2.load_reference',lambda _:SimpleNamespace(usd_rates=rates))
    rows=[record(pair,t,[{'close':1.0,'bid_close':.999,'ask_close':1.001}],'a'*64) for pair in c['universe'] for t in epochs]
    result=market_consumer({'rows':rows,'universe':c['universe']},cohort,None)
    assert len(result['rows'])==1292 and not result['financing_applied']
    assert all(r['quote_available'] and not r['financing_conversion_inputs_available'] for r in result['rows'])
    assert any(r['conversions']['base'] is None for r in result['rows'])


def test_duplicate_market_point_refuses_before_consumer(monkeypatch):
    monkeypatch.setattr('later_input_extension_v2.load_reference',lambda _:None)
    point={'instrument':'AUD_CAD','price_epoch':1}
    with pytest.raises(ValueError,match='duplicate_market'):
        market_consumer({'rows':[point,point]},None,None)
