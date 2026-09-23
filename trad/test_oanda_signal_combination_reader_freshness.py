"""Actual original/candidate reader definitions with files and injected wall clock."""
import ast
import copy
from datetime import datetime,timezone
import hashlib
import json
import math
import os
from pathlib import Path
from types import ModuleType,SimpleNamespace
from typing import Any,Iterable

import pytest

ROOT=Path(__file__).resolve().parents[1]
T=float((1800000000//3600)*3600+100)


class Clock:
    def __init__(self,now=T):self.now=now
    def __call__(self):return self.now


def stamp(now=T):return datetime.fromtimestamp(now,timezone.utc).isoformat()


def load_reader(which='src',clock=None):
    import importlib.util
    assert which=='src'
    path=Path(__file__).with_name('oanda_signal_combination_audit.py')
    spec=importlib.util.spec_from_file_location('whole_fuzzy_reader_under_test',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    module.time=SimpleNamespace(time=clock or Clock())
    return module


def payload(module,generated=... ,v4=False):
    metric={'probability_up':.7,'weighted_support':100.,'lower_probability_edge':.1,'n':80,
            'expected_net_pips':1.5,'expected_signed_move_pips':2.,'brier':.17}
    rule={'rule_id':'fixture_rule','conditions':[{'feature':'x','operator':'>=','threshold':0.,'width':1.}],
          'predicted_direction':'buy','account_eligible':True,'forward_refit_confirmed':True,
          'horizon_sec':60,'training':metric.copy(),'holdout':metric.copy()}
    value={'schema_version':3,'generated_at':stamp() if generated is ... else generated,
           'search_config':{'chronological_validation_blocks':2},'rules':[rule]}
    if v4:
        value.update(schema_version=4,validation_contract=module.FUZZY_VALIDATION_CONTRACT)
        rule.update(validation_contract=module.FUZZY_VALIDATION_CONTRACT,selection_calibration=metric.copy(),
                    final_holdout={'probability_up':.01,'brier':.99})
        rule['holdout']={'probability_up':.02,'brier':.88}
    return value


def write(path,value):path.write_text(json.dumps(value),encoding='utf-8')


@pytest.mark.parametrize('v4',[False,True])
@pytest.mark.parametrize('case',['future','naive','missing','empty','invalid'])
@pytest.mark.parametrize('maximum',[None,60.])
def test_invalid_source_generation_clock_never_becomes_fresh(tmp_path,v4,case,maximum):
    module=load_reader();generated={'future':stamp(T+100),'naive':stamp().replace('+00:00',''),
        'missing':None,'empty':'','invalid':'not-a-time'}[case]
    value=payload(module,generated=generated,v4=v4)
    if case=='missing':value.pop('generated_at')
    path=tmp_path/'state.json';write(path,value)
    model=module.SignalCombinationModel(path,maximum_state_age_sec=maximum);result=model.predict({'x':4.})
    assert not result['ready'] and result['reason']=='unavailable_combination_model_clock'
    source=model.state['sources'][0]
    assert source['clock_valid'] is False and source['fresh'] is False
    if case=='future':assert source['age_sec']==-100








@pytest.mark.parametrize('replacement',['invalid_json','non_mapping','invalid_contract','invalid_rule_container','invalid_search_config','invalid_utf8'])
def test_valid_to_invalid_replacement_never_retains_prior_rules(tmp_path,replacement):
    module=load_reader();path=tmp_path/'state.json';write(path,payload(module,v4=True))
    model=module.SignalCombinationModel(path);assert model.predict({'x':4.})['ready']
    if replacement=='invalid_json':path.write_text('{invalid')
    elif replacement=='non_mapping':write(path,[])
    elif replacement=='invalid_utf8':path.write_bytes(b'\xff\xfe\xff')
    else:
        value=payload(module,v4=True)
        if replacement=='invalid_contract':value.pop('validation_contract')
        elif replacement=='invalid_rule_container':value['rules']=12345
        else:value['search_config']='invalid'
        write(path,value)
    assert model.predict({'x':4.})['ready'] is False and model.state['rules']==[]


def test_no_valid_source_in_mixed_inventory_clears_previous_predictions(tmp_path):
    clock=Clock();module=load_reader(clock=clock);a=tmp_path/'a.json';b=tmp_path/'b.json';c=tmp_path/'c.json'
    for path in (a,b,c):write(path,payload(module))
    model=module.SignalCombinationModel(a,[b,c],maximum_state_age_sec=60)
    assert model.predict({'x':4.})['ready']
    a.unlink();write(b,payload(module,generated=stamp(T+100)));write(c,payload(module,generated=stamp(T-100)))
    result=model.predict({'x':4.})
    assert result['ready'] is False and model.state['rules']==[] and len(model.state['sources'])==3
    assert {row['retirement_reason'] for row in model.state['sources']}=={
        'model_state_source_missing','model_generation_is_future','stale_model_state'}


def test_prediction_metadata_uses_its_own_valid_source_clock_not_invalid_latest_text(tmp_path):
    module=load_reader();a=tmp_path/'valid.json';b=tmp_path/'future.json'
    write(a,payload(module,generated=stamp(T-5)));write(b,payload(module,generated=stamp(T+999)))
    model=module.SignalCombinationModel(a,[b]);result=model.predict({'x':4.})
    assert result['ready'] and result['rule_source']=='valid.json'
    assert result['model_generated_at']==stamp(T-5)
    assert next(row for row in model.state['sources'] if row['fresh'])['age_sec']==5
    assert model.state['generated_at']==stamp(T-5)




def test_freshness_recomputed_each_prediction_without_reparsing_static_file(tmp_path):
    clock=Clock();module=load_reader(clock=clock);parses=[]
    module.json=SimpleNamespace(loads=lambda value:parses.append(1) or json.loads(value))
    path=tmp_path/'state.json';write(path,payload(module))
    model=module.SignalCombinationModel(path,maximum_state_age_sec=10)
    for age in (1,5,10):
        clock.now=T+age;result=model.predict({'x':4.});assert result['ready'] and model.state['sources'][0]['age_sec']==age
    clock.now=T+11;assert not model.predict({'x':4.})['ready']
    assert len(parses)==1


def test_transient_read_failure_recovers_without_new_mtime(tmp_path,monkeypatch):
    module=load_reader();path=tmp_path/'state.json';write(path,payload(module))
    original=type(path).read_text;fail=[True]
    def read(self,*args,**kwargs):
        if self==path and fail[0]:raise PermissionError('fixture')
        return original(self,*args,**kwargs)
    monkeypatch.setattr(type(path),'read_text',read)
    model=module.SignalCombinationModel(path)
    assert model.predict({'x':4.})['ready'] is False
    fail[0]=False
    assert model.predict({'x':4.})['ready'] is True


def test_timezone_offsets_are_parsed_as_instants(tmp_path):
    module=load_reader();value=payload(module)
    value['generated_at']=datetime.fromtimestamp(T-5,timezone.utc).astimezone(
        timezone(__import__('datetime').timedelta(hours=-4))).isoformat()
    path=tmp_path/'state.json';write(path,value)
    model=module.SignalCombinationModel(path);result=model.predict({'x':4.})
    assert result['ready'] and model.state['sources'][0]['age_sec']==5 and result['model_generated_at']==value['generated_at']


@pytest.mark.parametrize('age',[float('inf'),float('nan'),True])
def test_invalid_maximum_age_does_not_disable_temporal_policy(tmp_path,age):
    module=load_reader()
    with pytest.raises(ValueError,match='finite_maximum_model_age_required'):
        module.SignalCombinationModel(tmp_path/'missing.json',maximum_state_age_sec=age)




def test_atomic_invalid_replacement_with_preserved_size_and_mtime_is_detected(tmp_path):
    module=load_reader();path=tmp_path/'state.json';write(path,payload(module))
    model=module.SignalCombinationModel(path);assert model.predict({'x':4.})['ready']
    before=path.stat();replacement=tmp_path/'replacement.json'
    replacement.write_bytes(b'{'+b' '*(before.st_size-1))
    os.utime(replacement,ns=(before.st_atime_ns,before.st_mtime_ns))
    assert replacement.stat().st_ino!=before.st_ino
    os.replace(replacement,path)
    assert path.stat().st_mtime_ns==before.st_mtime_ns and path.stat().st_size==before.st_size
    assert not model.predict({'x':4.})['ready'] and model.state['rules']==[]


def test_slow_file_read_crossing_expiry_is_assessed_after_io(tmp_path,monkeypatch):
    clock=Clock();module=load_reader(clock=clock);path=tmp_path/'state.json'
    write(path,payload(module,generated=stamp(T-50)));original=type(path).read_text
    def slow_read(self,*args,**kwargs):
        value=original(self,*args,**kwargs)
        if self==path:clock.now=T+11
        return value
    monkeypatch.setattr(type(path),'read_text',slow_read)
    model=module.SignalCombinationModel(path,maximum_state_age_sec=60)
    assert not model.predict({'x':4.})['ready']
    assert model.state['sources'][0]['age_sec']==61 and model.state['reader_assessed_epoch']==T+11


def test_expired_artifact_cannot_revive_after_inprocess_clock_rollback(tmp_path):
    clock=Clock();module=load_reader(clock=clock);path=tmp_path/'state.json';write(path,payload(module))
    model=module.SignalCombinationModel(path,maximum_state_age_sec=10)
    assert model.predict({'x':4.})['ready']
    clock.now=T+11;assert not model.predict({'x':4.})['ready']
    clock.now=T+5;result=model.predict({'x':4.})
    assert not result['ready'] and model.state['reader_clock_reason']=='reader_wall_clock_rollback'
    assert model.state['sources'][0]['retirement_reason']=='reader_wall_clock_rollback'
    clock.now=T+12;assert not model.predict({'x':4.})['ready']
    assert model.state['reader_clock_reason']=='' and model.state['sources'][0]['retirement_reason']=='stale_model_state'


def test_rollback_during_load_preserves_the_higher_observation(tmp_path,monkeypatch):
    clock=Clock(T+5);module=load_reader(clock=clock);path=tmp_path/'state.json';write(path,payload(module))
    original=type(path).read_text
    def read(self,*args,**kwargs):
        value=original(self,*args,**kwargs)
        if self==path:clock.now=T+1
        return value
    monkeypatch.setattr(type(path),'read_text',read)
    model=module.SignalCombinationModel(path,maximum_state_age_sec=10)
    assert model.state['reader_clock_reason']=='reader_wall_clock_rollback'
    assert model._reader_wall_highwater==T+5 and not model.predict({'x':4.})['ready']


def test_invalid_assessment_clock_withholds_without_infinite_highwater(tmp_path):
    clock=Clock();module=load_reader(clock=clock);path=tmp_path/'state.json';write(path,payload(module))
    model=module.SignalCombinationModel(path)
    clock.now=float('nan');result=model.predict({'x':4.})
    assert not result['ready'] and model.state['reader_clock_reason']=='invalid_reader_wall_clock'
    assert model._reader_wall_highwater==T and model.state['reader_assessed_epoch'] is None
