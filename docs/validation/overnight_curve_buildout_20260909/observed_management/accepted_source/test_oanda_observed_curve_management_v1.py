from copy import deepcopy
from decimal import Decimal
from datetime import datetime, timezone
import json
import math

import pytest
import oanda_observed_curve_management_v1 as m
import oanda_curve_management_replay_v1 as old
import oanda_research_quote_receipt_v1 as q
from oanda_forecast_curve_contract_v1 import AUTHORITY, content_hash
from test_oanda_research_quote_receipt_v1 import snapshot

START=1788932002.
TARGET=START+3590
SOURCES={'fixture.py':'a'*64}


def config(start=START, target=TARGET):
    policy=dict(schema_version=old.CONFIG_SCHEMA, **old.SAFETY, account_currency='USD',maximum_open_positions=1,
        metadata={'GBP_USD':dict(base_currency='GBP',quote_currency='USD',pip_size='.0001',unit_increment=1)},
        notional_usd='2500',conversion_policy='direct_or_inverse_executable_bid_ask_no_triangulation',
        sizing_policy='fixed_usd_notional_integer_base_units_at_decision',curve_target_window_policy='nominal_management_boundary',
        maximum_entry_spread_bps='5',maximum_holding_sec=3600,cadence_sec=60,execution_delay_sec=1,
        quote_max_age_sec=5,minimum_entry_cost_ratio='1',slippage_bps_per_leg='.1',switch_incremental_hurdle_usd='.05',
        legacy_policy=dict(minimum_score_cost_ratio=1.0,rotation_improvement_multiple=1.25,maximum_holding_min=60),
        feedback_horizon_sec=3600)
    binding=dict(curve_id='fixture_curve',curve_sha256='b'*64,node_id='fixture_node',model_id='fixture_model',
        model_sha256='c'*64,forecast_cohort='synthetic_unit_only',reference_epoch=target-3600,
        issued_epoch=start-5,original_target_epoch=target,expected_terminal_price='1.2600',source_bindings=SOURCES,
        price_convention='official_midpoint')
    return dict(schema_version=m.CONFIG_SCHEMA,**m.SAFETY,study_id='synthetic_session',created_epoch=start-2,
        start_epoch=start,native_target_epoch=target,hard_stop_epoch=target+15,curve_binding=binding,
        usd_policy=policy,source_bindings={**m.REUSED_BINDINGS,**SOURCES,m.NORMALIZER_SOURCE:'e'*64},policy=deepcopy(m.POLICY),
        momentum_policy=deepcopy(m.momentum.POLICY))


def capture(tmp_path, observed, *, market=None, bid=1.2500, ask=1.2502, started=None, bad=None):
    data=snapshot();iso=lambda value:datetime.fromtimestamp(value,timezone.utc).isoformat()
    data['generated_utc']=iso(observed-.1)
    for row in data['quotes'].values():row['time']=iso(observed-1 if market is None else market)
    data['quotes']['GBP_USD'].update(bid=bid,ask=ask)
    if bad=='nontradeable':
        data['quotes']['GBP_USD']['tradeable']=False
        data['coverage'].update(current_tradeable_quote_count=2,current_non_tradeable_quote_count=1,
            current_non_tradeable_instruments=['GBP_USD'])
    if bad=='retained':
        data['coverage'].update(current_tradeable_quote_count=2,current_quote_count=2,retained_last_known_count=1,
            retained_last_known_instruments=['GBP_USD'])
    path=tmp_path/('q_'+str(observed)+'.json');path.write_bytes(json.dumps(data).encode())
    times=iter([observed-.05 if started is None else started,observed])
    return q.capture_quote_snapshot(path,clock=lambda:next(times))


def candidate(cfg, mapped, cutoff):
    quote=mapped['quotes']['GBP_USD'];mid=(Decimal(quote['bid'])+Decimal(quote['ask']))/2
    b=cfg['curve_binding'];change=Decimal(b['expected_terminal_price'])-mid
    quote_evidence={k:quote[k] for k in ('instrument','quote_id','bid','ask','market_epoch','available_epoch','tradeable')}
    quote_evidence['midpoint']=str(mid)
    body=dict(schema_version='native_curve_management_candidate_v1_20260909',status='available',scope='current_research',
        **{k:b[k] for k in m.CURVE_BINDING_KEYS},instrument='GBP_USD',
        input_context={'price_convention':'official_midpoint'},side=int(change>0)-int(change<0),
        decision_epoch=cutoff,available_epoch=quote['available_epoch'],pip_size='.0001',remaining_sec=TARGET-cutoff,
        expected_remaining_price_change=str(change),expected_remaining_move_pips=str(change/Decimal('.0001')),
        target_window_policy='nominal_management_boundary',target_is_exact=False,
        target_selection_policy={'kind':'first_complete_bar_at_or_after_nominal','maximum_delay_sec':7},
        target_price_window_end_epoch=TARGET+7,decision_quote=quote_evidence,decision_quote_sha256=content_hash(quote_evidence),
        probability_is_remaining_move_probability=False,**AUTHORITY)
    return {**body,'candidate_sha256':content_hash(body)}


def frame(tmp_path,cfg,cutoff=START,scheduled=None,terminal=False,**kwargs):
    source=capture(tmp_path,cutoff-.1,**kwargs)
    mapped=q.map_quote_snapshot(source['raw_bytes'],source['receipt'],decision_epoch=cutoff,instruments=('GBP_USD',),maximum_quote_age_sec=5)
    return dict(scheduled_epoch=cutoff if scheduled is None else scheduled,decision_epoch=cutoff,terminal=terminal,
        quote_raw=source['raw_bytes'],quote_receipt=source['receipt'],
        curve_candidates=[] if terminal or 'GBP_USD' not in mapped['quotes'] else [candidate(cfg,mapped,cutoff)])


def first_plan(tmp_path, *, cfg=None):
    cfg=config() if cfg is None else cfg
    state=m.initial_states(cfg,clock=lambda:START-1)
    plan=m.plan_actions(state,frame(tmp_path,cfg),cfg,clock=lambda:START+.2)
    publication=m.make_plan_publication(plan,started_epoch=START+.21,completed_epoch=START+.3)
    return cfg,state,plan,publication


def settle(cfg,state,plan,pub,obs,now):
    return m.settle_actions(state,plan,pub,obs,as_of_epoch=now,config=cfg,clock=lambda:now)


def test_all_arms_plan_before_observed_quote_and_exact_usd_entry(tmp_path):
    cfg,state,plan,pub=first_plan(tmp_path)
    later=capture(tmp_path,START+1.3,market=START-.5)
    out=settle(cfg,state,plan,pub,[later],START+1.4)
    assert out['status']=='completed' and out['selected_observation_epoch']==START+1.3
    assert set(out['arms'])==set(m.ARMS)
    assert plan['plan_created_epoch']>plan['decision_epoch']
    for arm in ('usd_curve_manager','curve_hold_no_rotation'):
        pos=out['states']['arms'][arm]['position']
        assert pos['base_units']==1999 and pos['entry_epoch']==START+1.3
        assert pos['original_target_epoch']==TARGET
        leg=out['arms'][arm]['virtual_action']['legs'][0]
        assert leg['market_epoch']==START-.5  # Recent older tick is never retimed.
        assert Decimal(leg['price'])==Decimal('1.250212501')
        assert out['arms'][arm]['broker_fill_observed'] is False
    assert out['states']['generation']==1 and out['states']['known_epoch']==START+1.4
    assert state['generation']==0 and state['arms']['usd_curve_manager']['position'] is None


def test_earliest_valid_observation_not_best_later_price(tmp_path):
    cfg,state,plan,pub=first_plan(tmp_path)
    early=capture(tmp_path,START+1.3,bid=1.251,ask=1.2512)
    late=capture(tmp_path,START+2.3,bid=1.249,ask=1.2492)
    a=settle(cfg,state,plan,pub,[late,early],START+2.4)
    b=settle(cfg,state,plan,pub,[early,late],START+2.4)
    assert a==b and a['selected_observation_epoch']==START+1.3


@pytest.mark.parametrize('kind',['stale','nontradeable','retained','before_latency','read_before_publication','future'])
def test_ineligible_observations_never_create_virtual_fill(tmp_path,kind):
    cfg,state,plan,pub=first_plan(tmp_path)
    kwargs={};when=START+1.3
    if kind=='stale':kwargs['market']=START-5
    if kind in ('nontradeable','retained'):kwargs['bad']=kind
    if kind=='before_latency':when=START+.9
    if kind=='read_before_publication':kwargs['started']=START+.2
    if kind=='future':when=START+20
    source=capture(tmp_path,when,**kwargs)
    out=settle(cfg,state,plan,pub,[source],START+12)
    assert out['selected_observation_epoch'] is None
    assert all(row['virtual_action']['status']=='no_virtual_fill' for row in out['arms'].values())
    assert out['source_refusals'] and out['states']['generation']==1


def test_pending_is_not_a_new_state_or_a_fill(tmp_path):
    cfg,state,plan,pub=first_plan(tmp_path)
    out=settle(cfg,state,plan,pub,[],START+2)
    assert out['status']=='pending_quote' and out['states']==state


def test_publication_delay_changes_real_window_without_backdating(tmp_path):
    cfg,state,plan,_=first_plan(tmp_path)
    pub=m.make_plan_publication(plan,started_epoch=START+.21,completed_epoch=START+3)
    early=capture(tmp_path,START+2);valid=capture(tmp_path,START+3.1)
    out=settle(cfg,state,plan,pub,[early,valid],START+3.2)
    assert out['observation_not_before_epoch']==START+3
    assert out['observation_deadline_epoch']==START+13
    assert out['selected_observation_epoch']==START+3.1


def test_replay_completed_plan_against_new_state_rejected(tmp_path):
    cfg,state,plan,pub=first_plan(tmp_path)
    out=settle(cfg,state,plan,pub,[capture(tmp_path,START+1.3)],START+1.4)
    with pytest.raises(ValueError,match='plan_state_or_config'):
        settle(cfg,out['states'],plan,pub,[],START+13)


def test_next_decision_cannot_use_not_yet_observed_settlement(tmp_path):
    cfg,state,plan,pub=first_plan(tmp_path)
    out=settle(cfg,state,plan,pub,[capture(tmp_path,START+1.3)],START+70)
    with pytest.raises(ValueError,match='state_not_known'):
        m.plan_actions(out['states'],frame(tmp_path,cfg,START+60),cfg,clock=lambda:START+71)


def test_terminal_observed_after_nominal_target_and_original_target_not_changed(tmp_path):
    cfg,state,plan,pub=first_plan(tmp_path)
    opened=settle(cfg,state,plan,pub,[capture(tmp_path,START+1.3)],START+1.4)['states']
    f=frame(tmp_path,cfg,TARGET-1,terminal=True)
    terminal=m.plan_actions(opened,f,cfg,clock=lambda:TARGET-.7)
    publication=m.make_plan_publication(terminal,started_epoch=TARGET-.6,completed_epoch=TARGET-.5)
    out=settle(cfg,opened,terminal,publication,[capture(tmp_path,TARGET+.4,bid=1.252,ask=1.2522)],TARGET+.5)
    assert out['terminal_all_flat'] is True
    assert out['observation_not_before_epoch']==TARGET+.3
    close=out['arms']['usd_curve_manager']['virtual_action']['legs'][0]
    assert close['original_entry']['original_target_epoch']==TARGET
    assert close['virtual_action_epoch']==TARGET+.4
    assert out['arms']['usd_curve_manager']['native_deadline_lateness_sec']==pytest.approx(.4)
    assert Decimal(close['realized_usd'])>0


def test_missing_terminal_quote_preserves_unresolved_position(tmp_path):
    cfg,state,plan,pub=first_plan(tmp_path)
    opened=settle(cfg,state,plan,pub,[capture(tmp_path,START+1.3)],START+1.4)['states']
    terminal=m.plan_actions(opened,frame(tmp_path,cfg,TARGET-1,terminal=True),cfg,clock=lambda:TARGET-.7)
    publication=m.make_plan_publication(terminal,started_epoch=TARGET-.6,completed_epoch=TARGET-.5)
    out=settle(cfg,opened,terminal,publication,[],TARGET+15)
    assert out['terminal_all_flat'] is False and out['states']['arms']['usd_curve_manager']['position']
    assert out['arms']['usd_curve_manager']['liquidation_mark']['liquidation_usd'] is None


def test_original_incumbent_terminal_rebases_after_price_reversal(tmp_path):
    cfg,state,plan,pub=first_plan(tmp_path)
    opened=settle(cfg,state,plan,pub,[capture(tmp_path,START+1.3)],START+1.4)['states']
    f=frame(tmp_path,cfg,START+60,bid=1.261,ask=1.2612)
    new=m.plan_actions(opened,f,cfg,clock=lambda:START+60.2)
    assert new['decisions']['usd_curve_manager']['action'] in ('exit','rotate')
    assert new['decisions']['curve_hold_no_rotation']['action']=='hold'
    assert new['decision_diagnostics']['usd_curve_manager']['forecast_side']==-1


@pytest.mark.parametrize('key,value', [('curve_sha256','d'*64),('node_id','other'),
    ('expected_terminal_price','1.2700'),('model_id','other'),('forecast_cohort','other')])
def test_later_curve_replacement_rejected_even_resealed(tmp_path,key,value):
    cfg=config();state=m.initial_states(cfg,clock=lambda:START-1);f=frame(tmp_path,cfg)
    row=f['curve_candidates'][0];row[key]=value
    row['candidate_sha256']=content_hash({k:v for k,v in row.items() if k!='candidate_sha256'})
    with pytest.raises(ValueError,match='initial_curve_replaced'):
        m.plan_actions(state,f,cfg,clock=lambda:START+.2)


def test_long_computation_does_not_claim_earlier_fill_window(tmp_path):
    cfg,state,_,_=first_plan(tmp_path)
    plan=m.plan_actions(state,frame(tmp_path,cfg),cfg,clock=lambda:START+4)
    pub=m.make_plan_publication(plan,started_epoch=START+4.1,completed_epoch=START+4.2)
    out=settle(cfg,state,plan,pub,[capture(tmp_path,START+4.8),capture(tmp_path,START+5.1)],START+5.2)
    assert out['observation_not_before_epoch']==START+5
    assert out['selected_observation_epoch']==START+5.1


@pytest.mark.parametrize('field,value',[('notional_usd','1000'),('quote_max_age_sec',30),
    ('slippage_bps_per_leg','0'),('switch_incremental_hurdle_usd','0'),('execution_delay_sec',2)])
def test_predeclared_economic_policy_cannot_drift(field,value):
    cfg=config();cfg['usd_policy'][field]=value
    with pytest.raises(ValueError):m.validate_config(cfg)


@pytest.mark.parametrize('flag',list(m.SAFETY))
def test_inert_policy_flags_strict(flag):
    cfg=config();cfg[flag]=not cfg[flag]
    with pytest.raises(ValueError,match='inert'):m.validate_config(cfg)


def test_off_grid_late_duplicate_and_future_frames_refused(tmp_path):
    cfg,state,plan,pub=first_plan(tmp_path)
    for cutoff,scheduled in ((START+11,START),(START+1,START+1),(START,START+1)):
        with pytest.raises(ValueError):
            m.plan_actions(state,frame(tmp_path,cfg,cutoff,scheduled=scheduled),cfg,clock=lambda:START+20)


def test_source_binding_and_hard_stop_strict():
    cfg=config();cfg['source_bindings']['oanda_curve_management_replay_v1.py']='f'*64
    with pytest.raises(ValueError,match='reused_mechanics'):m.validate_config(cfg)
    cfg=config();cfg['hard_stop_epoch']=m.HARD_STOP_EPOCH+1
    with pytest.raises(ValueError,match='hard_stop'):m.validate_config(cfg)


def test_real_momentum_source_replayed_into_same_usd_selector(tmp_path,monkeypatch):
    from test_oanda_curve_momentum_observation_v1 import make
    raw,receipt,consumed,args=make(monkeypatch)
    cfg=config();f=frame(tmp_path,cfg)
    mapping=q.map_quote_snapshot(f['quote_raw'],f['quote_receipt'],decision_epoch=START,instruments=('GBP_USD',),maximum_quote_age_sec=5)
    computed=m.momentum.build_momentum_candidate(raw,receipt,consumed,metadata=q.METADATA['GBP_USD'],
        decision_epoch=START,target_epoch=TARGET,quote=mapping['quotes']['GBP_USD'])
    cfg['source_bindings'].update(computed['source_bindings'])
    f['momentum_source']={'raw_bytes':raw,'receipt':receipt,'consumption':consumed}
    state=m.initial_states(cfg,clock=lambda:START-1)
    plan=m.plan_actions(state,f,cfg,clock=lambda:START+.2)
    assert plan['momentum_candidate']==computed
    assert plan['decisions']['usd_momentum_manager']['action']=='enter'
    assert plan['decisions']['legacy_momentum_reference']['action']=='enter'
    assert plan['momentum_candidate']['policy']['confidence_scope']=='uncalibrated_score_mapping_not_probability'


def test_terminal_missing_decision_quote_still_exits_only_on_valid_later_observation(tmp_path):
    cfg,state,plan,pub=first_plan(tmp_path)
    opened=settle(cfg,state,plan,pub,[capture(tmp_path,START+1.3)],START+1.4)['states']
    frame_without_quote=dict(scheduled_epoch=TARGET-1,decision_epoch=TARGET-1,terminal=True,
        quote_raw=None,quote_receipt=None,decision_quote_refusal={'reason_code':'source_read_failed'})
    terminal=m.plan_actions(opened,frame_without_quote,cfg,clock=lambda:TARGET-.7)
    assert terminal['decision_quote_mapping'] is None
    assert terminal['decision_quote_refusal']=={'reason_code':'source_read_failed'}
    assert terminal['decisions']['usd_curve_manager']['action']=='exit'
    publication=m.make_plan_publication(terminal,started_epoch=TARGET-.6,completed_epoch=TARGET-.5)
    out=settle(cfg,opened,terminal,publication,[capture(tmp_path,TARGET+.4)],TARGET+.5)
    assert out['terminal_all_flat'] and out['arms']['usd_curve_manager']['virtual_action']['legs']
    missed=settle(cfg,opened,terminal,publication,[capture(tmp_path,TARGET+.4,bad='nontradeable')],TARGET+15)
    assert missed['states']['arms']['usd_curve_manager']['position']==opened['arms']['usd_curve_manager']['position']
    assert missed['arms']['no_trade']['liquidation_mark']=={'status':'available_flat','liquidation_usd':'0'}


@pytest.mark.parametrize('terminal,refusal',[(False,{'reason_code':'missing'}),(True,None),(True,{}),(True,{'reason_code':''})])
def test_missing_decision_quote_requires_terminal_and_explicit_reason(tmp_path,terminal,refusal):
    cfg=config();state=m.initial_states(cfg,clock=lambda:START-1)
    cutoff=TARGET-1 if terminal else START
    f=dict(scheduled_epoch=cutoff,decision_epoch=cutoff,terminal=terminal,decision_quote_refusal=refusal)
    with pytest.raises(ValueError,match='terminal_only_explicit'):
        m.plan_actions(state,f,cfg,clock=lambda:cutoff+.2)


def test_settlement_actual_completion_is_state_availability_not_source_cutoff(tmp_path):
    cfg,state,plan,pub=first_plan(tmp_path)
    out=m.settle_actions(state,plan,pub,[capture(tmp_path,START+1.3)],as_of_epoch=START+1.4,
        config=cfg,clock=lambda:START+70)
    assert out['as_of_epoch']==START+1.4 and out['settlement_computed_epoch']==START+70
    assert out['states']['known_epoch']==START+70
    assert out['states']['arms']['usd_curve_manager']['position']['entry_epoch']==START+1.3
    with pytest.raises(ValueError,match='state_not_known'):
        m.plan_actions(out['states'],frame(tmp_path,cfg,START+60),cfg,clock=lambda:START+71)


@pytest.mark.parametrize('complete_delta',[-1,4000])
@pytest.mark.parametrize('pending',[False,True])
def test_settlement_completion_cannot_precede_inputs_or_exceed_stop(tmp_path,complete_delta,pending):
    cfg,state,plan,pub=first_plan(tmp_path)
    observations=[] if pending else [capture(tmp_path,START+1.3)]
    with pytest.raises(ValueError,match='settlement_computation_clock'):
        m.settle_actions(state,plan,pub,observations,as_of_epoch=START+2,config=cfg,
            clock=lambda:START+2+complete_delta)


def test_pending_records_actual_computation_but_does_not_advance_known_state(tmp_path):
    cfg,state,plan,pub=first_plan(tmp_path)
    result=m.settle_actions(state,plan,pub,[],as_of_epoch=START+2,config=cfg,clock=lambda:START+3)
    assert result['settlement_computed_epoch']==START+3 and result['states']==state


def test_complete_entry_window_after_target_refuses_even_earlier_valid_quote(tmp_path):
    cfg,state,_,_=first_plan(tmp_path)
    plan=m.plan_actions(state,frame(tmp_path,cfg),cfg,clock=lambda:TARGET-12)
    pub=m.make_plan_publication(plan,started_epoch=TARGET-11,completed_epoch=TARGET-9)
    result=settle(cfg,state,plan,pub,[capture(tmp_path,TARGET-8)],TARGET-7)
    assert result['observation_deadline_epoch']==TARGET+1
    assert result['arms']['usd_curve_manager']['virtual_action']['reason_code']=='complete_entry_observation_window_not_before_native_target'
    assert result['states']['arms']['usd_curve_manager']['position'] is None


def test_rotate_failure_does_not_close_incumbent_partially(tmp_path):
    cfg,state,plan,pub=first_plan(tmp_path)
    opened=settle(cfg,state,plan,pub,[capture(tmp_path,START+1.3)],START+1.4)['states']
    f=frame(tmp_path,cfg,START+60,bid=1.261,ask=1.2612)
    rotate=m.plan_actions(opened,f,cfg,clock=lambda:START+60.2)
    assert rotate['decisions']['usd_curve_manager']['action']=='rotate'
    pub=m.make_plan_publication(rotate,started_epoch=START+60.21,completed_epoch=START+60.3)
    result=settle(cfg,opened,rotate,pub,[capture(tmp_path,START+61.3,bid=1.261,ask=1.262)],START+61.4)
    assert result['arms']['usd_curve_manager']['virtual_action']['reason_code']=='observed_entry_spread_above_limit'
    assert result['states']['arms']['usd_curve_manager']==opened['arms']['usd_curve_manager']


def test_zero_remaining_curve_is_incumbent_value_not_missing_candidate(tmp_path):
    cfg,state,plan,pub=first_plan(tmp_path)
    opened=settle(cfg,state,plan,pub,[capture(tmp_path,START+1.3)],START+1.4)['states']
    hold=m.plan_actions(opened,frame(tmp_path,cfg,START+60,bid=1.2599,ask=1.2601),cfg,clock=lambda:START+60.2)
    assert hold['decisions']['usd_curve_manager']['action']=='hold'
    assert hold['decision_diagnostics']['usd_curve_manager']['forecast_side']==0
    assert hold['prepared_candidate_evidence']['curve'][0]['side']==0


@pytest.mark.parametrize('field,value',[('minimum_score_cost_ratio',.75),('rotation_improvement_multiple',1.0),('maximum_holding_min',10)])
def test_predeclared_legacy_reference_settings_cannot_drift(field,value):
    cfg=config();cfg['usd_policy']['legacy_policy'][field]=value
    with pytest.raises(ValueError,match='fixed_legacy'):m.validate_config(cfg)


@pytest.mark.parametrize('row',[None,{}, {'raw_bytes':None,'receipt':{}},
    {'raw_bytes':b'{}','receipt':None},{'raw_bytes':bytearray(b'{}'),'receipt':{}}])
def test_malformed_observation_batch_fails_with_explicit_bounded_reason(tmp_path,row):
    cfg,state,plan,pub=first_plan(tmp_path)
    with pytest.raises(ValueError,match='observation_raw_bytes_and_receipt_shape_required'):
        settle(cfg,state,plan,pub,[row],START+12)


@pytest.mark.parametrize('fraction',[.0059996,.1234567,.0000002,.9999999])
def test_fractional_adapter_remaining_clock_has_named_lossless_origin_bridge(tmp_path,fraction):
    cfg=config();state=m.initial_states(cfg,clock=lambda:START-1)
    cutoff=START+fraction;f=frame(tmp_path,cfg,cutoff,scheduled=START)
    original=deepcopy(f['curve_candidates'][0])
    mapped=q.map_quote_snapshot(f['quote_raw'],f['quote_receipt'],decision_epoch=cutoff,
        instruments=('GBP_USD',),maximum_quote_age_sec=5)
    old_accepted,old_refused=old.prepare_candidates([original],'curve',mapped['quotes'],cutoff,TARGET,old.validate_config(cfg['usd_policy']))
    if Decimal(str(original['remaining_sec'])) != Decimal(str(TARGET))-Decimal(str(cutoff)):
        assert not old_accepted and old_refused[0]['reason']=='remaining_clock_mismatch'
    else:
        assert old_accepted and not old_refused  # The fourth float rounds to an exact integer second.
    plan=m.plan_actions(state,f,cfg,clock=lambda:cutoff+.2)
    assert plan['decisions']['usd_curve_manager']['action']=='enter'
    assert f['curve_candidates'][0]==original==plan['curve_candidate_originals'][0]
    bridge=plan['curve_candidate_compatibility'][0]
    assert bridge['schema_version']==m.COMPATIBILITY_SCHEMA and 'candidate_sha256' not in bridge
    assert bridge['original_candidate']==original and bridge['original_candidate_sha256']==original['candidate_sha256']
    assert bridge['remaining_sec']==str(Decimal(str(TARGET))-Decimal(str(cutoff)))
    assert bridge['remaining_clock_normalization']['original_remaining_sec']==TARGET-cutoff
    assert all(bridge[k]==original[k] for k in ('decision_epoch','reference_epoch','issued_epoch',
        'available_epoch','original_target_epoch','expected_terminal_price','decision_quote','decision_quote_sha256',
        'target_selection_policy','target_window_policy','target_price_window_end_epoch','target_is_exact',
        'probability_is_remaining_move_probability'))
    m.check_seal(bridge,'compatibility_sha256')


@pytest.mark.parametrize('mutation',['next_down','next_up','decimal_text','integer','wrong_schema','broken_seal'])
def test_clock_bridge_never_repairs_corrupted_original_candidate(tmp_path,mutation):
    cfg=config();state=m.initial_states(cfg,clock=lambda:START-1)
    cutoff=START+.0059996;f=frame(tmp_path,cfg,cutoff,scheduled=START);row=f['curve_candidates'][0]
    if mutation=='next_down':row['remaining_sec']=math.nextafter(row['remaining_sec'],-math.inf)
    if mutation=='next_up':row['remaining_sec']=math.nextafter(row['remaining_sec'],math.inf)
    if mutation=='decimal_text':row['remaining_sec']=str(Decimal(str(TARGET))-Decimal(str(cutoff)))
    if mutation=='integer':row['remaining_sec']=int(row['remaining_sec'])
    if mutation=='wrong_schema':row['schema_version']='modified_native_candidate'
    if mutation=='broken_seal':row['remaining_sec']+=1
    else:row['candidate_sha256']=content_hash({k:v for k,v in row.items() if k!='candidate_sha256'})
    with pytest.raises(ValueError,match='original_adapter_remaining_clock|curve_candidate_status_or_schema|curve_candidate_seal'):
        m.plan_actions(state,f,cfg,clock=lambda:cutoff+.2)
