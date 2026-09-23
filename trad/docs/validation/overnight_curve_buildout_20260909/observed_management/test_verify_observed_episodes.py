from copy import deepcopy
from datetime import datetime,timezone
from decimal import Decimal,localcontext,Context
import json
from pathlib import Path
import sys
import pytest

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
import verify_observed_episodes as v
import oanda_forecast_curve_file_store_v1 as store
import test_oanda_observed_curve_management_v1 as fixture
from test_oanda_observed_curve_management_worker_v1 import Clock


def episode(tmp_path,monkeypatch,*,predicted='100'):
    nominal=1788931800.;ref=nominal-5;target=ref+3600;sources={'fixture.py':'a'*64}
    c=v.contract
    policy=c.make_policy(native_horizons_sec=[3600],maximum_reference_age_sec=30,maximum_build_sec=10,
        maximum_issue_delay_sec=5,maximum_publication_delay_sec=5,maximum_decision_age_sec=4000,minimum_remaining_sec=0)
    prepared=c.prepare_curve(instrument='GBP_USD',pip_size='.0001',forecast_cohort='fixture_pilot/GBP_USD/official_midpoint',
        model_sha256='b'*64,feature_version='fixture',source_bindings=sources,input_capture_sha256='c'*64,
        input_available_epoch=ref+.1,reference_epoch=ref,reference_label_epoch=ref-5,reference_price='1.2500',
        reference_price_kind='mid_close',bar_duration_sec=5,model_fitted_epoch=ref-100,
        computation_started_epoch=ref+.2,computed_epoch=ref+.3,
        points=[dict(horizon_sec=3600,target_epoch=target,target_label_epoch=target-5,model_id='fixture',
            predicted_signed_pips=predicted,probability_up='.6',probability_scope='uncalibrated_original')],
        policy=policy,computation_sha256='d'*64,scope='current_research',input_context={'price_convention':'official_midpoint'},
        target_selection_policy={'kind':'first_complete_bar_at_or_after_nominal','maximum_delay_sec':7})
    curve=c.issue_curve(prepared,expected_source_bindings=sources,clock=lambda:ref+.4)
    pilot=tmp_path/'pilot';times=iter([ref+.5,ref+.6,ref+.7])
    published=store.publish_curve(pilot/'published',curve,expected_source_bindings=sources,clock=lambda:next(times))
    anchor=store.consume_published_curve(pilot/'published',published['descriptor'],expected_source_bindings=sources,clock=lambda:nominal+.1)
    anchor.update(node_id=prepared['nodes'][0]['node_id'],original_target_epoch=target,
        descriptor=published['descriptor'],cycle_id='cycle_1788931800000000')
    root=tmp_path/'runtime';spec=dict(output_root=str(root),hard_stop_epoch=target+20,
        episode_nominal_start_epochs=[nominal,nominal+3600,nominal+7200],
        source_bindings={**sources,**v.manager.REUSED_BINDINGS,v.manager.NORMALIZER_SOURCE:'e'*64})
    registered=dict(output_root=str(pilot),source_bindings=sources,study_id='fixture_pilot',
        metadata={'GBP_USD':v.quotes.METADATA['GBP_USD']})
    cfg=v.worker.make_episode_config(spec,registered,anchor,'episode_01',nominal,clock=lambda:nominal+1)
    state=v.manager.initial_states(cfg,clock=lambda:nominal+2);clock=Clock(nominal+3);parts=('episodes','episode_01')
    refs={}
    for key,name,value in [('config','episode_config.json',cfg),('anchor','initial_curve_consumption.json',anchor),('states','initial_states.json',state)]:
        refs[key]=v.io.persist_record(root,parts,name,value,clock=clock)
    slots=[];when=cfg['start_epoch']
    while when<target-1:slots.append(dict(scheduled_epoch=when,terminal=False));when+=60
    slots.append(dict(scheduled_epoch=target-1,terminal=True))
    refs['schedule']=v.io.persist_record(root,parts,'decision_schedule.json',{'slots':slots,**v.worker.FLAGS},clock=clock)
    init=v.io.persist_record(root,parts,'initialization_completed.json',dict(records=refs,
        initialized_state_sha256=state['state_sha256'],original_start_epoch=cfg['start_epoch'],observed_epoch=clock()),clock=clock)
    v.io.persist_record(root,parts,'initialization_publication.json',init,clock=clock)
    original=v.quotes.capture_quote_snapshot;calls=[]
    def capture(*,clock):
        observed=clock();data=fixture.snapshot();stamp=lambda t:datetime.fromtimestamp(t,timezone.utc).isoformat()
        data['generated_utc']=stamp(observed-.1)
        for row in data['quotes'].values():row['time']=stamp(observed-.2)
        data['quotes']['GBP_USD'].update(bid=1.2500,ask=1.2502)
        path=tmp_path/('source'+str(len(calls))+'.json');calls.append(observed);path.write_bytes(json.dumps(data).encode())
        return original(path,clock=clock)
    monkeypatch.setattr(v.worker,'verify_sources',lambda spec:None)
    monkeypatch.setattr(v.worker,'latest_momentum_source',lambda *a,**k:(None,[]))
    monkeypatch.setattr(v.quotes,'capture_quote_snapshot',capture)
    clock=Clock(cfg['start_epoch'])
    after,result=v.worker.run_step(spec,registered,anchor,cfg,state,'episode_01','step_000',cfg['start_epoch'],False,
        clock=clock,sleep=clock.sleep)
    return spec,registered,anchor,cfg,state,after,result,root,slots


def test_real_immutable_step_full_source_plan_and_accounting_replay(tmp_path,monkeypatch):
    spec,registered,anchor,cfg,state,after,result,root,slots=episode(tmp_path,monkeypatch)
    out=v.verify_episode(v.Reader(root),'episode_01',spec,registered)
    assert out['status']=='in_progress_or_partial' and out['verified_completed_steps']==1
    assert out['per_arm']['usd_curve_manager']['position_open'] is True
    assert out['matched_terminal_curve_minus_momentum_usd'] is None
    assert out['steps'][0]['curve_candidate_count']==1
    assert out['steps'][0]['independent_arithmetic']['usd_curve_manager']['open_legs']==1


@pytest.mark.parametrize('field', ['price','slippage_price','base_units','market_epoch'])
def test_independent_arithmetic_rejects_changed_leg_without_relying_on_seal(tmp_path,monkeypatch,field):
    *head,result,root,slots=episode(tmp_path,monkeypatch)
    spec,registered,anchor,cfg,state,after=head
    plan=json.loads((root/'episodes/episode_01/steps/step_000/plan.json').read_bytes())
    bad=deepcopy(result);leg=bad['arms']['usd_curve_manager']['virtual_action']['legs'][0]
    leg[field]=str(Decimal(leg[field])+1) if field in ('price','slippage_price') else leg[field]+1
    with pytest.raises(ValueError):v.independent_usd_sanity(state,plan,bad)


def test_tampered_raw_source_ref_is_detected(tmp_path,monkeypatch):
    spec,registered,*_,root,slots=episode(tmp_path,monkeypatch)
    path=root/'episodes/episode_01/steps/step_000/decision_quote_raw.json';path.write_bytes(path.read_bytes()+b' ')
    with pytest.raises(ValueError,match='file_reference_hash_or_size'):
        v.verify_episode(v.Reader(root),'episode_01',spec,registered)


def test_original_forecast_clock_cannot_change_in_retained_frame(tmp_path,monkeypatch):
    spec,registered,*_,root,slots=episode(tmp_path,monkeypatch)
    path=root/'episodes/episode_01/steps/step_000/information_frame.json';data=json.loads(path.read_bytes())
    data['curve_candidates'][0]['original_target_epoch']+=60;path.write_bytes(json.dumps(data).encode())
    with pytest.raises(ValueError,match='original_curve_candidate_replay'):
        v.verify_episode(v.Reader(root),'episode_01',spec,registered)


def test_false_completed_episode_cannot_hide_missing_slots(tmp_path,monkeypatch):
    spec,registered,anchor,cfg,state,after,result,root,slots=episode(tmp_path,monkeypatch)
    (root/'episodes/episode_01/episode_result.json').write_bytes(json.dumps(dict(status='completed',final_state=after)).encode())
    with pytest.raises(ValueError,match='completed_episode_chain_incomplete'):
        v.verify_episode(v.Reader(root),'episode_01',spec,registered)


def test_withholding_and_partial_initialization_are_not_completed_trials(tmp_path):
    root=tmp_path/'runtime';path=root/'episodes/episode_01';path.mkdir(parents=True)
    result=dict(status='withheld',reason_code='no_eligible_initial_curve');(path/'episode_result.json').write_text(json.dumps(result))
    out=v.verify_episode(v.Reader(root),'episode_01',{},{});assert out['retained_episode_result']==result
    assert 'per_arm' not in out
    (path/'episode_config.json').write_text('{}')
    out=v.verify_episode(v.Reader(root),'episode_01',{},{});assert out['status']=='initialization_partial'


@pytest.mark.parametrize('predicted',['100','-100'])
@pytest.mark.parametrize('unverified_gap',[False,True])
def test_completed_long_and_short_episodes_reconcile_original_terminal_and_exact_cost(tmp_path,monkeypatch,predicted,unverified_gap):
    spec,registered,anchor,cfg,state,after,result,root,slots=episode(tmp_path,monkeypatch,predicted=predicted)
    for index,slot in enumerate(slots[1:-1],1):
        name='step_'+str(index).zfill(3)
        v.io.persist_record(root,('episodes','episode_01'),'missed_'+name+'.json',
            dict(step_id=name,scheduled_epoch=slot['scheduled_epoch'],observed_epoch=slot['scheduled_epoch']+11,
                 reason_code='decision_schedule_slot_missed'),clock=Clock(slot['scheduled_epoch']+12))
    clock=Clock(slots[-1]['scheduled_epoch'])
    final,settlement=v.worker.run_step(spec,registered,anchor,cfg,after,'episode_01','step_'+str(len(slots)-1).zfill(3),
        slots[-1]['scheduled_epoch'],True,clock=clock,sleep=clock.sleep)
    if unverified_gap:
        (root/'episodes/episode_01/missed_step_001.json').unlink()
    else:
        v.io.persist_record(root,('episodes','episode_01'),'episode_result.json',dict(status='completed',final_state=final),clock=clock)
    with localcontext(Context(prec=2)):
        out=v.verify_episode(v.Reader(root),'episode_01',spec,registered)
    assert out['status']==('terminal_with_unverified_gaps' if unverified_gap else 'terminal_flat')
    assert out['verified_completed_steps']==2
    assert Decimal(out['per_arm']['usd_curve_manager']['realized_usd'])<0
    assert out['completed_matched_endpoint_eligible'] is (not unverified_gap)
    assert out['matched_terminal_curve_minus_momentum_usd']==(None if unverified_gap else out['per_arm']['usd_curve_manager']['realized_usd'])
    assert out['steps'][-1]['independent_arithmetic']['usd_curve_manager']['close_legs']==1


def test_valid_episode_cannot_be_rebound_to_different_registered_nominal_start(tmp_path,monkeypatch):
    spec,registered,*_,root,slots=episode(tmp_path,monkeypatch)
    spec['episode_nominal_start_epochs'][0]+=60
    with pytest.raises(ValueError,match='episode_registry_schedule_or_config_mismatch'):
        v.verify_episode(v.Reader(root),'episode_01',spec,registered)


def test_ignored_quote_filenames_are_counted_in_bounded_inventory(tmp_path,monkeypatch):
    spec,registered,*_,root,slots=episode(tmp_path,monkeypatch)
    folder=root/'episodes/episode_01/steps/step_000/quotes'
    for i in range(v.manager.MAX_OBSERVATIONS*5): (folder/('ignored'+str(i))).write_bytes(b'')
    with pytest.raises(ValueError,match='quote_file_inventory_bound'):
        v.verify_episode(v.Reader(root),'episode_01',spec,registered)


@pytest.mark.parametrize('config_exists',[False,True])
def test_claimed_completed_result_cannot_skip_original_initialization(tmp_path,config_exists):
    root=tmp_path/'runtime';path=root/'episodes/episode_01';path.mkdir(parents=True)
    (path/'episode_result.json').write_text(json.dumps({'status':'completed'}))
    if config_exists:(path/'episode_config.json').write_text('{}')
    with pytest.raises(ValueError,match='claimed_completed_episode_missing_initialization'):
        v.verify_episode(v.Reader(root),'episode_01',{}, {})
