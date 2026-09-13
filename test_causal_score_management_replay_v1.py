from copy import deepcopy
from decimal import Decimal, localcontext
import pytest
import causal_score_gate_v1 as gate
import causal_score_management_replay_v1 as replay
import oanda_curve_management_replay_v1 as mechanics

SCHEDULE=[1000,1300,1600,1900,2200,2500]

def config():
    return {'schema_version':mechanics.CONFIG_SCHEMA,**mechanics.SAFETY,'account_currency':'USD',
        'maximum_open_positions':1,'metadata':{'EUR_USD':{'base_currency':'EUR','quote_currency':'USD','pip_size':'.0001','unit_increment':1}},
        'notional_usd':'1000','conversion_policy':'direct_or_inverse_executable_bid_ask_no_triangulation',
        'sizing_policy':'fixed_usd_notional_integer_base_units_at_decision','curve_target_window_policy':'exact_only',
        'maximum_entry_spread_bps':'10','maximum_holding_sec':3600,'cadence_sec':300,'execution_delay_sec':60,
        'quote_max_age_sec':5,'minimum_entry_cost_ratio':'1','slippage_bps_per_leg':'1',
        'switch_incremental_hurdle_usd':'0','legacy_policy':{'minimum_score_cost_ratio':.75,
        'rotation_improvement_multiple':1.25,'maximum_holding_min':60}}

def policy():
    return {'schema':gate.SCHEMA,'session_id':'synthetic-wrapper','scopes':[{
        'scope_id':'EUR_USD.origin-v1','instrument':'EUR_USD','model_id':'model','model_sha256':'a'*64,
        'forecast_cohort':'fixture','source_bindings':{'model.py':'a'*64},'horizon_sec':970,'score_recipe':'fixed_origin_score'}],
        'lookback_sec':5000,'minimum_history':2,'maximum_history':10,'maximum_seen':100,'entry_percentile':'.6'}

def quotes(epoch,bid='1.0000',ask='1.0002'):
    return {'EUR_USD':{'instrument':'EUR_USD','quote_id':'q.'+str(epoch),'market_epoch':epoch,'available_epoch':epoch,
                       'bid':bid,'ask':ask,'tradeable':True}}

def original(index):
    origin=SCHEDULE[index];reference=origin-10;target=origin+960
    return {'instrument':'EUR_USD','side':1,'curve_id':'curve.'+str(index),'curve_sha256':'b'*64,
        'node_id':'node.'+str(index),'node_sha256':'c'*64,'model_id':'model','model_sha256':'a'*64,
        'forecast_cohort':'fixture','reference_epoch':reference,'reference_price':'1.0001','pip_size':'.0001',
        'issued_epoch':origin-5,'available_epoch':origin-1,'decision_epoch':origin,'original_target_epoch':target,
        'horizon_sec':970,'remaining_sec':target-origin,'target_window_policy':'exact_only','target_is_exact':True,
        'target_selection_policy':{'kind':'exact_price_epoch','maximum_delay_sec':0},
        'target_price_window_end_epoch':target,'expected_terminal_price':'1.0101',
        'expected_remaining_price_change':'.0100','expected_remaining_move_pips':'100',
        'source_bindings':{'model.py':'a'*64},'probability_up':.6,'probability_scope':'original_interval_uncalibrated'}

def sample(index):
    raw=original(index)
    out={**deepcopy(policy()['scopes'][0]),**{key:raw[key] for key in ('curve_id','curve_sha256','node_id','node_sha256',
        'reference_epoch','original_target_epoch')},'source_available_epoch':raw['available_epoch'],'static_score':str([0,2,3][index])}
    out['sample_id']=gate.sample_identity(out);return out

def frames():
    result=[]
    with localcontext(mechanics.CTX):
        validated=mechanics.validate_config(config())
        for index,epoch in enumerate(SCHEDULE):
            source_index=min(index,2);raw=original(source_index)
            raw['decision_epoch']=epoch;raw['available_epoch']=epoch
            raw['remaining_sec']=raw['original_target_epoch']-epoch
            prepared,errors=mechanics.prepare_candidates([raw],'curve',quotes(epoch),epoch,raw['original_target_epoch'],validated)
            terminal=index==len(SCHEDULE)-1
            if not terminal:assert not errors
            result.append({'decision_epoch':epoch,'execution_epoch':epoch+60,'execution_observed_epoch':epoch+60,
                'terminal':terminal,'management_target_epoch':raw['original_target_epoch'],
                'prepared_continuation':prepared if not terminal else [],'score_records':[sample(source_index)] if not terminal else [],
                'decision_quotes':quotes(epoch),'execution_quotes':quotes(epoch+60)})
    return result

def run(value=None,cutoff=2560,**kwargs):
    return replay.replay_matched_frames(frames() if value is None else value,gate_policy=policy(),
        mechanics_config=config(),decision_epochs=SCHEDULE,starting_balance_usd='50',as_of_epoch=cutoff,**kwargs)

def test_all_arms_use_same_existing_mechanics_and_balance_added_once():
    source=frames();before=deepcopy(source);out=run(source)
    assert tuple(out['arms'])==replay.ARMS and out['pending_settlement'] is None
    assert out['rows'][0]['decision_nav']['ungated_entry']['nav_usd']==50
    assert out['rows'][0]['decisions']['ungated_entry']['action']=='enter'
    assert out['rows'][0]['decisions']['past_score_entry']['action']=='wait'
    assert out['rows'][2]['decisions']['past_score_entry']['action']=='enter'
    assert all(state['position'] is None for state in out['visible_final_states'].values())
    assert out['visible_final_states']['no_trade']['realized_usd']==0
    assert out['rows'][-1]['post_execution_nav']['no_trade']['nav_usd']==50
    assert out['visible_final_states']['past_score_entry']['realized_usd']<0
    assert source==before

def test_original_target_survives_repricing_and_prefix_does_not_force_close():
    out=run(frames()[:4],1960)
    position=out['visible_final_states']['past_score_entry']['position']
    assert position['original_target_epoch']==2560 and position['entry_epoch']==1660
    assert out['rows'][-1]['terminal'] is False

def test_longer_run_and_exact_observation_prefix_have_equal_prior_decisions():
    source=frames();full=run(source);short=run(source[:4],1960)
    assert [r['decision_prefix_sha256'] for r in full['rows'][:4]]==[r['decision_prefix_sha256'] for r in short['rows']]
    changed=deepcopy(source)
    changed[4]['execution_quotes']=quotes(2260,'9','9.0002')
    changed[5]['execution_quotes']=quotes(2560,'.2','.2002')
    mutated=run(changed)
    assert [r['decision_prefix_sha256'] for r in full['rows'][:4]]==[r['decision_prefix_sha256'] for r in mutated['rows'][:4]]

def test_future_static_score_conflict_changes_no_prior_decision():
    source=frames();baseline=run(source);source[4]['score_records'][0]['static_score']='99999'
    changed=run(source)
    assert [r['decision_prefix_sha256'] for r in baseline['rows'][:4]]==[r['decision_prefix_sha256'] for r in changed['rows'][:4]]
    assert changed['rows'][4]['score_gate']['refusals'][0]['reason']=='conflicting_static_forecast_sample'

def test_feedback_is_descriptive_and_cannot_change_state_or_decisions():
    source=frames();baseline=run(source,4000)
    source[2].update(feedback_epoch=3500,feedback_observed_epoch=3500,feedback_quotes=quotes(3500,'10','10.0002'))
    changed=run(source,4000)
    assert changed['visible_final_states']==baseline['visible_final_states']
    assert [r['decision_prefix_sha256'] for r in baseline['rows']]==[r['decision_prefix_sha256'] for r in changed['rows']]
    assert changed['rows'][2]['descriptive_feedback']['past_score_entry']['nav_usd']>1000

def test_every_arm_decision_is_frozen_before_first_execution(monkeypatch):
    count={'selected':0,'executed':0}
    original_select=replay.channels.choose_usd_action_with_channels
    original_apply=mechanics.apply_action
    def select(*args,**kwargs):
        count['selected']+=1;return original_select(*args,**kwargs)
    def apply(*args,**kwargs):
        assert count['selected']==3
        count['executed']+=1;return original_apply(*args,**kwargs)
    monkeypatch.setattr(replay.channels,'choose_usd_action_with_channels',select)
    monkeypatch.setattr(mechanics,'apply_action',apply)
    run(frames()[:1],1060)
    assert count=={'selected':3,'executed':4}

def test_late_settlement_embargoes_next_decision_and_score_observation():
    source=frames();source[0]['execution_observed_epoch']=1400
    source[1]['prepared_continuation']='must not be consumed under embargo'
    source[1]['score_records']='must not be consumed under embargo'
    out=run(source)
    assert out['rows'][1]['status']=='pending_observation_embargo'
    assert len(out['score_state']['seen'])==2
    assert out['rows'][2]['visible_states_before']['ungated_entry']['position']['entry_epoch']==1060
    assert 'decisions' not in out['rows'][1]

@pytest.mark.parametrize('cutoff',[1030,1059])
def test_unexecuted_prefix_returns_pending_without_accessing_execution_map(cutoff):
    source=frames();source[0]['execution_quotes']='must not be consumed before cutoff'
    out=run(source,cutoff)
    assert out['pending_settlement']['unresolved_execution'] is True
    assert out['visible_final_states']['ungated_entry']['position'] is None
    assert out['rows'][0]['settlement_status']=='execution_not_yet_in_observed_prefix'

def test_unobserved_execution_does_not_become_cash_or_position():
    source=frames();source[0]['execution_observed_epoch']=1200;source[0]['execution_quotes']='unobserved'
    out=run(source,1100)
    assert out['visible_final_states']['ungated_entry']['position'] is None
    assert out['pending_settlement']['unresolved_execution'] is True

def test_missing_current_nav_withholds_rotation_and_keeps_position():
    source=frames();source[3]['decision_quotes']={};source[3]['prepared_continuation']=[];source[3]['score_records']=[]
    out=run(source)
    row=out['rows'][3]
    assert row['decision_nav']['past_score_entry']['nav_usd'] is None
    assert row['decisions']['past_score_entry']['action']=='hold'
    assert row['decisions']['past_score_entry']['reason']=='current_nav_unavailable'

def test_missing_terminal_exact_mark_cannot_force_cash_realization():
    source=frames();source[-1]['decision_quotes']={};source[-1]['execution_quotes']={}
    out=run(source)
    state=out['visible_final_states']['past_score_entry']
    assert state['position'] is not None and state['realized_usd']==0
    assert out['rows'][-1]['post_execution_nav']['past_score_entry']['nav_usd'] is None
    assert out['rows'][-1]['settlements']['past_score_entry']['status']=='rejected'

def test_stale_terminal_mark_is_not_accepted_as_exact_execution():
    source=frames();source[-1]['execution_quotes']=quotes(2559)
    out=run(source)
    assert out['visible_final_states']['past_score_entry']['position'] is not None
    assert out['rows'][-1]['settlements']['past_score_entry']['reason'].startswith('missing_exact_execution_quote')

def test_repriced_prepared_economics_must_match_existing_math_exactly():
    source=frames();source[0]['prepared_continuation'][0]['score']+=1
    with pytest.raises(ValueError,match='prepared_current_economics_changed'):run(source)

def test_fixed_terminal_contract_refuses_artificial_prefix_last_row():
    source=frames()[:3];source[-1]['terminal']=True
    with pytest.raises(ValueError,match='original_terminal_identity_required'):run(source,1660)

def test_native_target_off_schedule_is_refused():
    source=frames();source[0]['management_target_epoch']+=1
    with pytest.raises(ValueError,match='native_target_off_original_grid'):run(source)

def test_zero_or_bool_starting_balance_refused():
    for balance in (0,True):
        with pytest.raises(ValueError):replay.replay_matched_frames([],gate_policy=policy(),mechanics_config=config(),
            decision_epochs=SCHEDULE,starting_balance_usd=balance,as_of_epoch=2560)

def test_unobserved_settlement_keeps_every_scheduled_embargo_inside_prefix():
    source=frames();source[0]['execution_observed_epoch']=2000
    short=run(source,1500);long=run(source)
    assert [row['decision_epoch'] for row in short['rows']]==[1000,1300]
    assert short['rows'][1]['status']=='pending_observation_embargo'
    assert [row['decision_prefix_sha256'] for row in short['rows']]==[
        row['decision_prefix_sha256'] for row in long['rows'] if row['decision_epoch']<=1500]
    assert len(short['score_state']['seen'])==1

def test_future_arrival_metadata_cannot_change_public_asof_result():
    source=frames();source[0]['execution_observed_epoch']=2000
    first=run(source,1500);source[0]['execution_observed_epoch']=2400
    second=run(source,1500)
    assert first==second
    pending=first['pending_settlement']
    assert 'observed_epoch' not in pending and 'states' not in pending
    assert pending['execution_epoch']==1060 and pending['decision_epoch']==1000

@pytest.mark.parametrize('future',[None,{'decision_epoch':False},{'decision_epoch':float('nan')}])
def test_future_frame_payload_is_not_validated_before_asof_schedule_check(future):
    source=frames();baseline=run(source,1500);source[2]=future
    assert run(source,1500)==baseline

def test_settlement_observed_exactly_at_cutoff_becomes_visible_once():
    source=frames();source[0]['execution_observed_epoch']=1400
    before=run(source,1399);at=run(source,1400)
    assert before['visible_final_states']['ungated_entry']['position'] is None
    assert before['pending_settlement'] is not None
    assert at['visible_final_states']['ungated_entry']['position']['entry_epoch']==1060
    assert at['pending_settlement'] is None


@pytest.mark.parametrize('bad_epoch',[False,float('nan'),1000,5000])
def test_unobserved_feedback_payload_cannot_change_prefix_but_visible_invalid_payload_is_refused(bad_epoch):
    source=frames();baseline=run(source,1800)
    source[0].update(feedback_epoch=bad_epoch,feedback_observed_epoch=4000,feedback_quotes='invalid')
    assert run(source,1800)==baseline
    with pytest.raises(ValueError):run(source,4000)


def test_feedback_quotes_are_consumed_only_at_receipt_boundary():
    source=frames();baseline=run(source,3999)
    source[0].update(feedback_epoch=3500,feedback_observed_epoch=4000,feedback_quotes='invalid')
    assert run(source,3999)==baseline
    with pytest.raises(ValueError,match='bounded_feedback_quotes_required'):run(source,4000)


def test_compact_receipts_reconstruct_original_gate_state_and_eligibility(monkeypatch):
    captured=[];original_build=gate.build_score_channels
    def capture(*args,**kwargs):
        scored=original_build(*args,**kwargs);captured.append(deepcopy(scored));return scored
    monkeypatch.setattr(gate,'build_score_channels',capture)
    out=run();inventory={};prior=gate.initial_state(policy())
    scored_rows=[row for row in out['rows'] if row.get('score_gate') is not None]
    assert len(scored_rows)==len(captured)
    for row,full in zip(scored_rows,captured):
        compact=row['score_gate']
        assert not {'next_score_state','entry_rows','continuation_rows'} & set(compact)
        assert compact['prior_state_sha256']==prior['state_sha256']
        for observation in compact['new_observations']:
            identity=observation['sample_id'];retained=out['score_state']['seen'][identity]
            assert gate.digest(retained['sample'])==observation['sample_sha256']
            assert retained['first_seen_decision_epoch']==observation['first_seen_decision_epoch']
            assert identity not in inventory
            inventory[identity]=deepcopy(retained)
        prior=gate._seal_state({'schema':gate.SCHEMA,'policy_sha256':gate.digest(policy()),
            'last_decision_epoch':row['decision_epoch'],'seen':deepcopy(inventory)})
        assert prior==full['next_score_state']
        assert compact['next_state_sha256']==prior['state_sha256']
        for small,large in zip(compact['percentile_receipts'],full['percentile_receipts']):
            assert 'history_sample_ids' not in small
            assert small['history_sample_ids_sha256']==gate.digest(large['history_sample_ids'])
            assert {key:value for key,value in small.items() if key!='history_sample_ids_sha256'}=={
                key:value for key,value in large.items() if key!='history_sample_ids'}
        assert compact['entry_references']==[{'instrument':item['instrument'],'candidate_id':item['candidate_id'],
            'prepared_row_sha256':gate.digest(item)} for item in full['entry_rows']]
        assert compact['continuation_rows_sha256']==gate.digest(row['prepared_continuation'])
    assert prior==out['score_state']
    assert sum(len(row['score_gate']['new_observations']) for row in scored_rows)==len(inventory)==3


def test_compact_gate_receipt_growth_is_linear_on_small_unique_sequence():
    import test_causal_score_gate_v1 as gate_fixture
    sizes=[]
    for count in (12,24):
        p=gate_fixture.policy(maximum_history=100,maximum_seen=100,lookback_sec=1000)
        state=gate.initial_state(p);retained=[]
        for index in range(count):
            scored=gate_fixture.step(state,p,index,100+index,str(index))
            retained.append(replay._compact_score_receipt(scored,state));state=scored['next_score_state']
        assert sum(len(row['new_observations']) for row in retained)==count
        assert all('history_sample_ids' not in item for row in retained for item in row['percentile_receipts'])
        sizes.append(len(gate.canonical(retained)))
    assert sizes[1] < 2.2*sizes[0]


@pytest.mark.parametrize('field,value',[('maximum_seen',4097),('maximum_history',513)])
def test_replay_policy_resource_caps_do_not_change_pure_gate_domain(field,value):
    p=policy();p.update(maximum_seen=5000,maximum_history=1000);p[field]=value
    gate.validate_policy(p)
    with pytest.raises(ValueError,match='bounded_replay_history_policy_required'):
        replay.replay_matched_frames([],gate_policy=p,mechanics_config=config(),decision_epochs=SCHEDULE,
            starting_balance_usd='50',as_of_epoch=1500)


def test_known_oversized_input_refused_before_candidate_copy_or_score_call(monkeypatch):
    source=frames();source[0]['score_records'][0]['extra']='x'*(replay.MAX_INPUT_BYTES//6+1)
    def forbidden(*args,**kwargs):raise AssertionError('copy or scoring reached')
    monkeypatch.setattr(replay,'_prepare_exact',forbidden)
    monkeypatch.setattr(gate,'build_score_channels',forbidden)
    with pytest.raises(ValueError,match='replay_byte_budget_exceeded'):run(source)


def test_visible_state_and_incremental_output_budgets_refuse_atomically(monkeypatch):
    source=frames();before=deepcopy(source)
    monkeypatch.setattr(replay,'MAX_STATE_BYTES',20)
    with pytest.raises(ValueError,match='replay_byte_budget_exceeded'):run(source)
    assert source==before
    monkeypatch.setattr(replay,'MAX_STATE_BYTES',2*1024*1024)
    monkeypatch.setattr(replay,'MAX_OUTPUT_BYTES',replay.MAX_INPUT_BYTES+100)
    with pytest.raises(ValueError,match='replay_output_budget_exceeded'):run(source)
    assert source==before


def test_tree_bounds_refuse_cycles_and_nonplain_values_before_serialization():
    cyclic=[];cyclic.append(cyclic)
    with pytest.raises(ValueError,match='bounded_replay_tree_required'):replay._estimated_bytes(cyclic,10000)
    with pytest.raises(ValueError,match='plain_replay_value_required'):replay._estimated_bytes(object(),10000)
    with pytest.raises(ValueError,match='string_replay_keys_required'):replay._estimated_bytes({1:'bad'},10000)


def test_resource_checks_do_not_consume_unobserved_feedback_or_future_frames():
    source=frames();baseline=run(source,1800)
    source[0].update(feedback_epoch=3500,feedback_observed_epoch=4000,feedback_quotes=object())
    source[3]=object()
    assert run(source,1800)==baseline


def test_exact_output_serialization_cap_is_checked_without_large_allocation(monkeypatch):
    original=gate.canonical;calls=[]
    class Oversize:
        def __len__(self):return replay.MAX_OUTPUT_BYTES+1
    def canonical(value):
        if type(value) is dict and value.get('schema')==replay.SCHEMA:
            calls.append('final_output');return Oversize()
        return original(value)
    monkeypatch.setattr(gate,'canonical',canonical)
    with pytest.raises(ValueError,match='replay_serialized_output_budget_exceeded'):run()
    assert calls==['final_output']
