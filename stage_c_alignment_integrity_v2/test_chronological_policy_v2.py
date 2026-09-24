import copy
from decimal import Decimal
from pathlib import Path

import pytest

from contracts import fingerprint
from historical_native_input_v2 import SCENARIOS, validate_scenario
from chronological_policy_scenario_v2 import scenarios, contract, canonical_metadata, PROFILE
from chronological_policy_input_v2 import validate_frame_authority, adapted_candidates


def test_shifted_financing_and_original_scenarios_both_preserved():
    for original in SCENARIOS.values():validate_scenario(original)
    for name,scenario in scenarios().items():
        c=contract(name);validate_scenario(scenario)
        assert scenario['rollover_epoch']==c['origin_epoch']+86400+1
        old=SCENARIOS[name.removeprefix(c['cohort']+'_')]
        assert {k:v for k,v in scenario.items() if k not in ('scenario_id','rollover_epoch')}=={k:v for k,v in old.items() if k not in ('scenario_id','rollover_epoch')}


@pytest.mark.parametrize('field,value', [('rollover_epoch',0),('slippage_bps_per_leg','2'),('single_rollover_cost_bps_of_base_usd','-1')])
def test_changed_dated_economic_assumption_refuses(field,value):
    scenario=copy.deepcopy(next(iter(scenarios().values())));scenario[field]=value
    with pytest.raises(ValueError,match='frozen_retrospective'):validate_scenario(scenario)


def setup(monkeypatch,kind='decision',terminal=False):
    scenario=next(iter(scenarios().values()));c=contract(scenario['scenario_id']);origin=c['origins'][0]
    epoch=origin+2;price_epoch=origin
    if terminal:epoch=c['target_epoch']-58;price_epoch=c['target_epoch']-60
    if kind=='execution':epoch=origin+60;price_epoch=epoch
    if kind=='financing':epoch=c['rollover_epoch'];price_epoch=c['rollover_price_epoch']
    metadata={'EUR_USD':{'base_currency':'EUR','quote_currency':'USD','pip_size':Decimal('.0001'),'unit_increment':1}}
    points=[{'instrument':'EUR_USD','price_epoch':price_epoch}]
    f={'cohort':c['cohort'],'model_profile':PROFILE,'method':c['methods'][0],'scenario':scenario,'kind':kind,'epoch':epoch,
       'target_epoch':c['target_epoch'],'terminal':terminal,'market_points':points,'later_input':None,'historical_packets':[]}
    if kind=='execution':f['fills']={k:{} for k in c['original_policy_contract']['policies']}
    if kind=='financing':f.update(provenance_id='declared-single-rollover:'+fingerprint(scenario),accrual_period_id='cohort-single-scenario-rollover')
    authority={'market_metadata_sha256':fingerprint(canonical_metadata(metadata)),
               'market_panels':{str(price_epoch):fingerprint(points)}}
    monkeypatch.setattr('chronological_policy_input_v2.authority',lambda cohort:copy.deepcopy(authority))
    return f,{'metadata':metadata},authority,c


@pytest.mark.parametrize('kind', ['decision','execution','financing'])
def test_registered_frame_types_and_exact_clock(monkeypatch,kind):
    f,config,a,c=setup(monkeypatch,kind);validate_frame_authority(f,config)
    f['epoch']+=1
    with pytest.raises(ValueError):validate_frame_authority(f,config)


def test_rehashed_replacement_market_panel_cannot_enter(monkeypatch):
    f,config,a,c=setup(monkeypatch);f['market_points'][0]['reference_close']='2.0'
    with pytest.raises(ValueError,match='original_market_panel'):validate_frame_authority(f,config)


def test_wrong_pip_metadata_refuses(monkeypatch):
    f,config,a,c=setup(monkeypatch);config['metadata']['EUR_USD']['pip_size']=Decimal('.01')
    with pytest.raises(ValueError,match='original_metadata'):validate_frame_authority(f,config)


def test_terminal_is_explicitly_empty_and_cannot_add_forecast(monkeypatch):
    f,config,a,c=setup(monkeypatch,terminal=True)
    assert adapted_candidates(f,config,Path('.'))==([],[])
    f['historical_packets']=[{'forged':True}]
    with pytest.raises(ValueError,match='terminal_forecasts'):adapted_candidates(f,config,Path('.'))


def test_changed_rollover_provenance_refuses(monkeypatch):
    f,config,a,c=setup(monkeypatch,'financing');f['accrual_period_id']='another-charge'
    with pytest.raises(ValueError,match='financing_provenance'):validate_frame_authority(f,config)


def test_missing_fill_arm_refuses(monkeypatch):
    f,config,a,c=setup(monkeypatch,'execution');f['fills'].pop(next(iter(f['fills'])))
    with pytest.raises(ValueError,match='fill_arm_inventory'):validate_frame_authority(f,config)


def test_forecast_self_hash_does_not_replace_parent_authority(monkeypatch):
    f,config,a,c=setup(monkeypatch)
    data={'observations':[],'predictions':[],'coverage':[],'source_frame_sha256':'a'*64}
    a['frames']={str(c['origins'][0]):{'observations_sha256':fingerprint([]),'source_frame_sha256':'a'*64,
        'groups':{f['method']:{'predictions_sha256':fingerprint([]),'coverage_sha256':fingerprint([]),'packets_sha256':fingerprint([])}}}}
    f['later_input']=data
    f['later_input']['predictions']=[{'prediction_bps':100,'forecast_id':fingerprint({'prediction_bps':100})}]
    with pytest.raises(ValueError,match='original_forecast_authority'):adapted_candidates(f,config,Path('.'))


def test_wrong_cohort_rejected_even_with_otherwise_valid_frame(monkeypatch):
    f,config,a,c=setup(monkeypatch);f['cohort']='later_tuesday'
    with pytest.raises(ValueError,match='cohort_mismatch'):validate_frame_authority(f,config)

def test_other_cohort_scenario_cannot_be_substituted(monkeypatch):
    f,config,a,c=setup(monkeypatch)
    f['scenario']=next(v for k,v in scenarios().items() if k.startswith('later_tuesday_'))
    with pytest.raises(ValueError,match='cohort_mismatch'):validate_frame_authority(f,config)
