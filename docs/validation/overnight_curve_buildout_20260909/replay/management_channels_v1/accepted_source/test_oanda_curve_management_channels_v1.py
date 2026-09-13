"""Pure prepared-channel semantics; no original-chain or paper-PnL claims."""
from copy import deepcopy
from decimal import Decimal, localcontext

import pytest
import oanda_curve_management_channels_v1 as m
from test_oanda_curve_management_replay_v1 import config,quotes,curve

old=m.mechanics

def prepared(changes=(('EUR_USD','.01'),),*,epoch=1000,target=1660,precision=50):
    cfg=config();qs=quotes(epoch)
    with localcontext(old.CTX) as context:
        context.prec=precision
        policy=old.validate_config(cfg)
        rows=[]
        for pair,change in changes:
            row=curve(pair,epoch,change,target)
            if Decimal(change)==0:row['side']=0
            rows.append(row)
        candidates,refusals=old.prepare_candidates(rows,'curve',qs,epoch,target,policy)
        assert not refusals
    return candidates,qs,cfg

def state(side=1,*,pair='EUR_USD',target=1660):
    return {'position':{'instrument':pair,'side':side,'base_units':500,'entry_epoch':940.,
                        'original_target_epoch':target},'realized_usd':Decimal('0')}

@pytest.mark.parametrize('changes,held',[
    ((('EUR_USD','.01'),),None),((('EUR_USD','.00001'),),None),
    ((('EUR_USD','.01'),),1),((('EUR_USD','-.01'),),1),
    ((('EUR_USD','-.01'),),-1),((('EUR_USD','.01'),),-1),
    ((('EUR_USD','.001'),('GBP_JPY','5')),1),
    ((('EUR_USD','-.001'),('GBP_JPY','-5')),-1),
])
def test_identical_channels_exact_unchanged_selector_differential(changes,held):
    rows,qs,cfg=prepared(changes);position={'position':None} if held is None else state(held)
    with localcontext(old.CTX):
        expected,diagnostics=old.choose_usd_action(position,rows,qs,1000,old.validate_config(cfg))
    result=m.choose_usd_action_with_channels(position,rows,rows,qs,1000,cfg)
    assert result['decision']==old.jsonable(expected)
    assert result['diagnostics']==old.jsonable(diagnostics)

@pytest.mark.parametrize('side,change,action',[(1,'.01','hold'),(1,'-.01','exit'),(1,'0','hold'),
                                           (-1,'-.01','hold'),(-1,'.01','exit'),(-1,'0','hold')])
def test_refused_entry_does_not_remove_signed_or_zero_continuation(side,change,action):
    rows,qs,cfg=prepared((('EUR_USD',change),))
    result=m.choose_usd_action_with_channels(state(side),[],rows,qs,1000,cfg)
    assert result['decision']['action']==action
    assert result['continuation_candidate_count']==1 and result['entry_candidate_count']==0
    assert result['diagnostics']['forecast_side']==(1 if Decimal(change)>0 else -1 if Decimal(change)<0 else 0)

def test_entry_reversal_filter_prevents_rotate_without_destroying_exit_evidence():
    rows,qs,cfg=prepared((('EUR_USD','-.01'),))
    admitted=m.choose_usd_action_with_channels(state(),rows,rows,qs,1000,cfg)
    refused=m.choose_usd_action_with_channels(state(),[],rows,qs,1000,cfg)
    assert admitted['decision']['action']=='rotate' and refused['decision']['action']=='exit'
    assert admitted['diagnostics']['hold_value_usd']==refused['diagnostics']['hold_value_usd']

def test_neutral_entry_must_be_filtered_but_valid_continuation_survives():
    rows,qs,cfg=prepared((('EUR_USD','0'),))
    with pytest.raises(ValueError,match='neutral_new_entry'):
        m.choose_usd_action_with_channels(state(),rows,rows,qs,1000,cfg)
    assert m.choose_usd_action_with_channels({'position':None},[],rows,qs,1000,cfg)['decision']['action']=='wait'

@pytest.mark.parametrize('terminal,position,expected',[(True,state(),'exit'),(True,{'position':None},'wait')])
def test_terminal_intent_survives_unavailable_forecasts_and_quotes(terminal,position,expected):
    result=m.choose_usd_action_with_channels(position,None,None,None,1660,config(),terminal=terminal)
    assert result['decision']['action']==expected and result['entry_candidate_count']==0

def test_native_deadline_exit_without_new_candidate():
    result=m.choose_usd_action_with_channels(state(),[],[],{},1600,config())
    assert result['decision']['reason']=='predeclared_holding_or_native_target_deadline'

def test_nonmatching_incumbent_original_target_cannot_borrow_later_horizon():
    rows,qs,cfg=prepared(target=1700)
    result=m.choose_usd_action_with_channels(state(),[],rows,qs,1000,cfg)
    assert result['decision']['reason']=='incumbent_native_target_incomparable'

def test_missing_continuation_explicit_exit_and_hold_only_named_policy():
    args=(state(),[],[],{},1000,config())
    assert m.choose_usd_action_with_channels(*args)['decision']['reason']=='incumbent_estimate_unavailable'
    assert m.choose_usd_action_with_channels(*args,hold_only=True)['decision']['reason']=='hold_current_no_rotation'

@pytest.mark.parametrize('damage',[lambda q:q.pop('EUR_USD'),lambda q:q['EUR_USD'].update(available_epoch=1001),
                                 lambda q:q['EUR_USD'].update(market_epoch=900),lambda q:q['EUR_USD'].update(tradeable=False)])
def test_invalid_incumbent_valuation_gives_explicit_unavailable_exit(damage):
    rows,qs,cfg=prepared();damage(qs)
    result=m.choose_usd_action_with_channels(state(),[],rows,qs,1000,cfg)
    assert result['decision']['reason']=='incumbent_valuation_unavailable'

@pytest.mark.parametrize('field,value',[('score','999'),('new_entry_net_usd','999'),('decision_epoch',999),
    ('available_epoch',1001),('original_target_epoch',1000),('kind','momentum'),('side',True),('base_units',1.5)])
def test_prepared_row_corruption_refused(field,value):
    rows,qs,cfg=prepared();rows[0][field]=value
    with pytest.raises(ValueError):m.choose_usd_action_with_channels(state(),[],rows,qs,1000,cfg)

def test_entry_is_exact_same_candidate_not_an_independent_valuation():
    rows,qs,cfg=prepared();entry=deepcopy(rows);entry[0]['source_payload']['curve_id']='different'
    with pytest.raises(ValueError,match='entry_not_exact'):
        m.choose_usd_action_with_channels(state(),entry,rows,qs,1000,cfg)

def test_duplicate_pair_and_bounded_channel_refused():
    rows,qs,cfg=prepared()
    for inventory in (rows*2,rows*69):
        with pytest.raises(ValueError):m.choose_usd_action_with_channels(state(),[],inventory,qs,1000,cfg)

def test_selection_ignores_ambient_decimal_context_and_owns_return_values():
    rows,qs,cfg=prepared();position=state();before=deepcopy((rows,qs,cfg,position))
    expected=m.choose_usd_action_with_channels(position,rows,rows,qs,1000,cfg)
    with localcontext() as context:
        context.prec=2
        actual=m.choose_usd_action_with_channels(position,rows,rows,qs,1000,cfg)
    assert actual==expected
    actual['decision']['action']='forged'
    assert m.choose_usd_action_with_channels(position,rows,rows,qs,1000,cfg)==expected
    assert (rows,qs,cfg,position)==before

def test_lower_precision_preparation_is_explicitly_refused_not_silently_repriced():
    rows,qs,cfg=prepared(precision=28)
    with pytest.raises(ValueError,match='prepared_channel_economics_changed'):
        m.choose_usd_action_with_channels(state(),[],rows,qs,1000,cfg)

def test_seal_and_inert_scope_do_not_claim_chain_or_admission_validation():
    rows,qs,cfg=prepared();result=m.choose_usd_action_with_channels(state(),[],rows,qs,1000,cfg)
    assert result['selection_sha256']==old.digest({k:v for k,v in result.items() if k!='selection_sha256'})
    assert all(result[key] is value for key,value in old.SAFETY.items())
    assert all(result[key] is False for key in ('positions_managed','manager_activation',
        'semantic_direction_admission_verified_here','original_forecast_chain_replayed_here',
        'fresh_inputs_verified','computation_clock_receipt'))

def test_original_config_and_supplied_quotes_have_distinct_bindings():
    rows,qs,cfg=prepared();position=state()
    result=m.choose_usd_action_with_channels(position,rows,rows,qs,1000,cfg)
    with localcontext(old.CTX):
        assert result['validated_config_sha256']==old.digest(old.validate_config(cfg))
    assert result['supplied_quote_inventory_sha256']==old.digest(qs)
    cfg2=deepcopy(cfg);cfg2['switch_incremental_hurdle_usd']='123'
    changed=m.choose_usd_action_with_channels(position,rows,rows,qs,1000,cfg2)
    assert result['validated_config_sha256']!=changed['validated_config_sha256']
    assert result['selection_sha256']!=changed['selection_sha256']
    qs2=deepcopy(qs);qs2['EUR_USD']['quote_id']='another_original_quote'
    terminal=m.choose_usd_action_with_channels(position,None,None,qs2,1660,cfg,terminal=True)
    assert terminal['supplied_quote_inventory_sha256']==old.digest(qs2)
