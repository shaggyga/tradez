import json
import hashlib
from copy import deepcopy
from datetime import datetime,timezone
from pathlib import Path

import pytest

import oanda_observed_curve_management_worker_v1 as worker
import oanda_observed_curve_management_v1 as manager
import oanda_observed_management_io_v1 as io
import oanda_research_quote_receipt_v1 as quotes
import test_oanda_observed_curve_management_v1 as fixture


class Clock:
    def __init__(self,value):self.value=float(value)
    def __call__(self):
        self.value+=.001
        return self.value
    def sleep(self,seconds):
        assert 0<=seconds<=.5
        self.value+=seconds


def setup_step(tmp_path,monkeypatch,*,terminal=False,no_fill=False,initial_read_failure=False):
    cfg=fixture.config()
    before=manager.initial_states(cfg,clock=lambda:fixture.START-1)
    if terminal:
        _,empty,plan,pub=fixture.first_plan(tmp_path)
        before=fixture.settle(cfg,empty,plan,pub,[fixture.capture(tmp_path,fixture.START+1.3)],fixture.START+1.4)['states']
    cutoff=fixture.TARGET-1 if terminal else fixture.START
    clock=Clock(cutoff)
    events=[];original_capture=quotes.capture_quote_snapshot
    def source_provider(*,clock):
        events.append(('quote_read',clock()))
        if initial_read_failure and len(events)==1:
            raise ValueError('quote_source_unavailable')
        observed=clock()
        data=fixture.snapshot();stamp=lambda n:datetime.fromtimestamp(n,timezone.utc).isoformat()
        data['generated_utc']=stamp(observed-.1)
        for row in data['quotes'].values():row['time']=stamp(observed-.2)
        data['quotes']['GBP_USD'].update(bid=1.2500,ask=1.2502)
        if no_fill and len(events)>1:
            data['quotes']['GBP_USD']['tradeable']=False
            data['coverage'].update(current_tradeable_quote_count=2,current_non_tradeable_quote_count=1,
                current_non_tradeable_instruments=['GBP_USD'])
        path=tmp_path/('observed_'+str(len(events))+'.json')
        path.write_text(json.dumps(data))
        return original_capture(path,clock=clock)
    monkeypatch.setattr(worker,'verify_sources',lambda _:None)
    monkeypatch.setattr(worker,'latest_momentum_source',lambda *a,**k:(None,[]))
    monkeypatch.setattr(worker.files,'consume_published_curve',lambda *a,**k:
        {'curve':{},'publication':{},'consumption':{'available_epoch':clock()}})
    monkeypatch.setattr(worker.adapter,'candidate_for_target',lambda *a,**kwargs:
        fixture.candidate(cfg,{'quotes':{'GBP_USD':kwargs['quote']}},kwargs['decision_epoch']))
    monkeypatch.setattr(quotes,'capture_quote_snapshot',source_provider)
    spec={'output_root':str(tmp_path/'runtime'),'hard_stop_epoch':fixture.TARGET+15}
    registry={'output_root':str(tmp_path/'pilot'),'source_bindings':fixture.SOURCES,
        'metadata':{'GBP_USD':quotes.METADATA['GBP_USD']}}
    return spec,registry,{'descriptor':{}},cfg,before,clock,events,cutoff


def run(args,terminal=False):
    spec,registered,anchor,cfg,before,clock,events,scheduled=args
    result=worker.run_step(spec,registered,anchor,cfg,before,'episode_01','step_000',scheduled,terminal,
        clock=clock,sleep=clock.sleep)
    return result,Path(spec['output_root'])/'episodes/episode_01/steps/step_000'


def test_real_manager_plan_persisted_before_quote_and_state_durably_recorded(tmp_path,monkeypatch):
    args=setup_step(tmp_path,monkeypatch)
    (state,result),path=run(args)
    events=args[6]
    plan=json.loads((path/'plan.json').read_bytes())
    publication=json.loads((path/'plan_publication.json').read_bytes())
    persisted=json.loads((path/'settlement_publication.json').read_bytes())
    assert result['status']=='completed' and len(events)==2
    assert plan['decision_epoch']<plan['plan_created_epoch']<=publication['publication_started_epoch']
    assert publication['publication_completed_epoch']<events[1][1]
    assert state['known_epoch']==result['settlement_computed_epoch']
    assert state['known_epoch']<=persisted['publication_started_epoch']
    assert sum(s['position'] is not None for s in state['arms'].values())==2
    assert json.loads((path/'state_after.json').read_bytes())==state
    assert json.loads((path/'step_completed.json').read_bytes())['state_sha256']==state['state_sha256']
    assert result['broker_fills_performed'] is False


def test_missing_executable_observation_remains_no_fill(tmp_path,monkeypatch):
    args=setup_step(tmp_path,monkeypatch,no_fill=True)
    (state,result),path=run(args)
    assert result['selected_observation_epoch'] is None
    assert all(s['position'] is None for s in state['arms'].values())
    assert all(r['virtual_action']['status']=='no_virtual_fill' for r in result['arms'].values())
    assert 2<=len(args[6])<=14
    assert (path/'step_completed.json').exists()


def test_terminal_can_record_force_exit_without_decision_quote(tmp_path,monkeypatch):
    args=setup_step(tmp_path,monkeypatch,terminal=True,initial_read_failure=True)
    (state,result),path=run(args,terminal=True)
    assert result['terminal_all_flat'] is True
    assert all(s['position'] is None for s in state['arms'].values())
    plan=json.loads((path/'plan.json').read_bytes())
    assert plan['decision_quote_mapping'] is None
    assert plan['decision_quote_refusal']['reason_code']=='quote_source_unavailable'
    assert result['selected_observation_epoch']>=fixture.TARGET


def test_failed_plan_receipt_does_not_begin_future_quote_poll(tmp_path,monkeypatch):
    args=setup_step(tmp_path,monkeypatch)
    original=io.persist_record
    def fail(root,parts,name,value,**kwargs):
        if name=='plan_publication.json':raise OSError('synthetic_persist_failure')
        return original(root,parts,name,value,**kwargs)
    monkeypatch.setattr(io,'persist_record',fail)
    with pytest.raises(OSError,match='synthetic_persist_failure'):
        run(args)
    assert len(args[6])==1
    path=Path(args[0]['output_root'])/'episodes/episode_01/steps/step_000'
    assert (path/'plan.json').exists() and not (path/'settlement.json').exists()


def test_bound_source_change_before_publication_stops_before_future_quotes(tmp_path,monkeypatch):
    args=setup_step(tmp_path,monkeypatch)
    calls=[]
    def verify(_):
        calls.append(1)
        if len(calls)==2:raise ValueError('management_bound_source_changed')
    monkeypatch.setattr(worker,'verify_sources',verify)
    with pytest.raises(ValueError,match='bound_source_changed'):
        run(args)
    assert len(args[6])==1
    path=Path(args[0]['output_root'])/'episodes/episode_01/steps/step_000'
    assert (path/'information_frame.json').exists() and not (path/'plan.json').exists()


def test_normal_decision_missing_raw_quote_is_not_terminal_override(tmp_path,monkeypatch):
    args=setup_step(tmp_path,monkeypatch,initial_read_failure=True)
    with pytest.raises(ValueError,match='quote_source_unavailable'):
        run(args)
    assert len(args[6])==1
    assert not list(Path(args[0]['output_root']).rglob('plan.json'))


def test_wait_is_bounded_by_hard_stop():
    clock=Clock(1000)
    assert worker.wait_until(1002,1003,clock=clock,sleep=clock.sleep) is True
    assert worker.wait_until(1005,1003,clock=clock,sleep=clock.sleep) is False
    assert clock.value<1003.01


def test_anchor_selection_uses_order_and_eligibility_not_prediction(monkeypatch):
    monkeypatch.setattr(io,'recent_cycles',lambda _:['newest','previous','older'])
    calls=[]
    def anchor(root,cycle,*a,**k):
        calls.append(cycle)
        if cycle=='newest':raise ValueError('management_initial_native_h1_not_admitted')
        return {'cycle_id':cycle,'predicted_signed_pips':'-999'}
    monkeypatch.setattr(io,'read_curve_anchor',anchor)
    picked,rejections=worker.pick_anchor({'output_root':'unused','source_bindings':{},'study_id':'fixture'})
    assert picked['cycle_id']=='previous' and calls==['newest','previous'] and len(rejections)==1


def test_newest_valid_source_does_not_search_for_nicer_momentum(monkeypatch):
    monkeypatch.setattr(io,'recent_cycles',lambda _:['newest','older'])
    calls=[]
    monkeypatch.setattr(io,'read_momentum_source',lambda root,cycle,*a,**k:
        calls.append(cycle) or {'cycle_id':cycle,'raw_bytes':b'original'})
    found,rejections=worker.latest_momentum_source({'output_root':'unused','metadata':{'GBP_USD':{}}})
    assert found['cycle_id']=='newest' and calls==['newest'] and not rejections


def specimen():
    registry_path=worker.ROOT/'config/recovered_second_curve_pilot_v1_20260909.json'
    registered=json.loads(registry_path.read_bytes())
    starts=[1788940800+i*3600 for i in range(3)]
    sources={**registered['source_bindings'],**{name:'a'*64 for name in worker.NEW_SOURCES}}
    return dict(schema_version=worker.SCHEMA,study_id=worker.STUDY,created_epoch=1788939000,
        output_root=str(worker.OUTPUT_ROOT),pilot_registry_path=str(registry_path),
        pilot_registry_sha256=worker.PILOT_REGISTRY_SHA,source_bindings=sources,
        episode_nominal_start_epochs=starts,hard_stop_epoch=worker.manager.HARD_STOP_EPOCH,
        policy=deepcopy(worker.manager.POLICY),momentum_policy=deepcopy(worker.momentum.POLICY),
        legacy_policy=deepcopy(worker.LEGACY_POLICY),**worker.FLAGS)


def load_fixture(tmp_path,monkeypatch,value):
    raw=worker.contract.canonical_bytes(value)
    path=tmp_path/'registry.json';path.write_bytes(raw)
    monkeypatch.setattr(worker,'verify_sources',lambda _:None)
    return worker.load_spec(path,hashlib.sha256(raw).hexdigest())


def test_registry_is_read_only_and_binds_all_prior_sources(tmp_path,monkeypatch):
    spec,pilot=load_fixture(tmp_path,monkeypatch,specimen())
    assert len(spec['episode_nominal_start_epochs'])==3
    assert set(spec['source_bindings'])==set(pilot['source_bindings'])|set(worker.NEW_SOURCES)
    assert list(tmp_path.iterdir())==[tmp_path/'registry.json']


@pytest.mark.parametrize('change',[
    lambda s:s.update(orders_enabled=True),
    lambda s:s.update(output_root='C:/unrelated'),
    lambda s:s.update(pilot_registry_sha256='0'*64),
    lambda s:s['source_bindings'].update(unbound='a'*64),
    lambda s:s['source_bindings'].update({'oanda_second_forecast.py':'0'*64}),
    lambda s:s.update(episode_nominal_start_epochs=[1788940800,1788940860,1788948000]),
    lambda s:s.update(episode_nominal_start_epochs=[True,1788944400,1788948000]),
    lambda s:s.update(hard_stop_epoch=worker.manager.HARD_STOP_EPOCH+1),
    lambda s:s['policy'].update(maximum_quote_wait_sec=60),
    lambda s:s['momentum_policy']['weights'].__setitem__(0,'0.9'),
])
def test_registry_cannot_change_fixed_scope_or_authority(tmp_path,monkeypatch,change):
    value=specimen();change(value)
    with pytest.raises(ValueError):
        load_fixture(tmp_path,monkeypatch,value)


def test_episode_config_adds_explicit_research_integer_unit_contract(tmp_path):
    from test_oanda_observed_management_io_v1 import anchor,CYCLE
    curve,publication=anchor(tmp_path)
    node=curve['prepared_curve']['nodes'][0]
    registered={'metadata':{'GBP_USD':quotes.METADATA['GBP_USD']}}
    spec={'source_bindings':fixture.config()['source_bindings'],'hard_stop_epoch':manager.HARD_STOP_EPOCH}
    cfg=worker.make_episode_config(spec,registered,{'curve':curve,'node_id':node['node_id']},
        'unit_contract_fixture',1200,clock=lambda:1210)
    assert cfg['usd_policy']['metadata']['GBP_USD']['unit_increment']==1
    assert 'unit_increment' not in registered['metadata']['GBP_USD']
    assert manager.validate_config(cfg)['metadata']['GBP_USD']['unit_increment']==1


def test_slow_initialization_is_withheld_before_any_action(tmp_path,monkeypatch):
    from test_oanda_observed_management_io_v1 import anchor,CYCLE
    curve,_=anchor(tmp_path/'fixture')
    observed={'curve':curve,'node_id':curve['prepared_curve']['nodes'][0]['node_id'],'cycle_id':CYCLE}
    spec={'source_bindings':fixture.config()['source_bindings'],'hard_stop_epoch':manager.HARD_STOP_EPOCH,
          'output_root':str(tmp_path/'runtime')}
    registered={'metadata':{'GBP_USD':quotes.METADATA['GBP_USD']}}
    clock=Clock(1210)
    monkeypatch.setattr(worker,'verify_sources',lambda _:None)
    monkeypatch.setattr(worker,'pick_anchor',lambda *a,**k:(observed,[]))
    monkeypatch.setattr(worker,'run_step',lambda *a,**k:(_ for _ in ()).throw(AssertionError('action_after_late_init')))
    original=io.persist_record
    def slow(root,parts,name,value,**kwargs):
        result=original(root,parts,name,value,**kwargs)
        if name=='decision_schedule.json':clock.value=1271
        return result
    monkeypatch.setattr(io,'persist_record',slow)
    result=worker.run_episode(spec,registered,'episode_01',1210,clock=clock,sleep=clock.sleep)
    assert result['status']=='withheld' and result['reason_code']=='initialization_not_durable_before_first_schedule'
    assert all(s['position'] is None for s in result['initial_state']['arms'].values())
    path=tmp_path/'runtime/episodes/episode_01'
    assert (path/'initialization_publication.json').exists() and not list(path.rglob('plan.json'))
