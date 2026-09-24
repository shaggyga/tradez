import pytest
from chronological_policy_report_v2 import terminal_status, paired, build


def state():
    return {'net_account_pnl_usd':'9.8','equity_usd':'10009.8','return_fraction':'.00098',
        'realized_usd':'10','financing_usd':'-.1','fees_usd':'.1','unrealized_usd':'0',
        'open_lot_count':0,'pending_order_units':0}


def test_closed_account_reconciles_fees_and_financing_once():
    assert terminal_status(state())=='closed_terminal'
    s=state();s['net_account_pnl_usd']='9.9'
    with pytest.raises(ValueError,match='reconciliation'):terminal_status(s)


def test_open_marked_pnl_includes_unrealized_without_calling_it_closed():
    s=state();s.update(net_account_pnl_usd='12.8',unrealized_usd='3',open_lot_count=1)
    assert terminal_status(s)=='marked_open_terminal'


def test_unpriced_terminal_is_never_zero_or_partial_cash_pnl():
    s=state();s.update(net_account_pnl_usd=None,equity_usd=None,return_fraction=None,unrealized_usd=None,open_lot_count=1)
    assert terminal_status(s)=='unpriced_terminal'
    s['equity_usd']='10000'
    with pytest.raises(ValueError,match='consistency'):terminal_status(s)


def test_terminal_comparison_requires_both_closed():
    a={'terminal_status':'closed_terminal','method':'a','scenario':'s','arm':'fixed_hold','net_account_pnl_usd':'2','fills':2,'filled_replacement_episodes':0,'financing_application_complete':True}
    b={**a,'method':'b','net_account_pnl_usd':'1'}
    assert paired(a,b,'matched')['net_usd_delta']=='1'
    b.update(terminal_status='unpriced_terminal',net_account_pnl_usd=None)
    result=paired(a,b,'matched');assert result['net_usd_delta'] is None and result['status']=='terminal_comparison_unavailable'


def test_closed_account_delta_does_not_hide_rejected_financing():
    a={'terminal_status':'closed_terminal','method':'a','scenario':'s','arm':'fixed_hold','net_account_pnl_usd':'2','fills':2,'filled_replacement_episodes':0,'financing_application_complete':False}
    b={**a,'method':'b','net_account_pnl_usd':'1','financing_application_complete':True}
    result=paired(a,b,'matched')
    assert result['net_usd_delta']=='1'
    assert result['status']=='descriptive_recorded_delta_financing_incomplete'
    assert not result['economic_cost_comparison_complete']


def test_partial_operator_receipt_cannot_produce_full_comparison(tmp_path):
    with pytest.raises(ValueError,match='exact_verified112run'):build(tmp_path,{'status':'completed_verified','runs':[]})


def test_rejected_financing_is_visible_even_when_amount_is_zero():
    from chronological_policy_report_v2 import execution_diagnostics
    account={'realized_usd':'0','fees_usd':'0','financing_usd':'0'}
    events=[{'arm':'cash','kind':'financing','receipt':{'status':'rejected','reason':'missing_rate'}}]
    result=execution_diagnostics([],[],events,'cash',account)
    assert result['financing_rejected']==1 and result['financing_applied']==0
    assert not result['financing_application_complete']
    events[0]['receipt']={'status':'financing_applied','amount_usd':'0'}
    result=execution_diagnostics([],[],events,'cash',account)
    assert result['financing_application_complete'] and result['financing_applied']==1


def test_selection_without_opening_is_not_a_filled_trade():
    from chronological_policy_report_v2 import execution_diagnostics
    decision={'decision_id':'d','action':'REPLACE','epoch':100,'candidate':{'instrument':'EUR_USD','side':1,'base_units':100}}
    events=[{'arm':'a','kind':'financing','receipt':{'status':'financing_applied','amount_usd':'0'}}]
    account={'realized_usd':'0','fees_usd':'0','financing_usd':'0'}
    result=execution_diagnostics([decision],[],events,'a',account)
    assert result['selected_decisions']==1 and result['selected_without_open_episode']==1 and result['filled_selected_decisions']==0
    with pytest.raises(ValueError,match='actual_fill_binding'):
        execution_diagnostics([decision],[{'entry_decision':'d'}],events,'a',account)


def test_event_components_cannot_silently_disagree_with_account():
    from chronological_policy_report_v2 import execution_diagnostics
    events=[{'arm':'a','kind':'financing','receipt':{'status':'financing_applied','amount_usd':'-1'}}]
    with pytest.raises(ValueError,match='event_account_reconciliation'):
        execution_diagnostics([],[],events,'a',{'realized_usd':'0','fees_usd':'0','financing_usd':'0'})


def opening_fixture(delay):
    d={'decision_id':'d','action':'ENTER','epoch':100,'candidate':{'instrument':'EUR_USD','side':1,'base_units':100,'original_target_epoch':100000}}
    leg={'kind':'open','instrument':'EUR_USD','side':1,'base_units':100,'realized_usd':'0','execution_epoch':100+delay,
        'position':{'candidate_id':'d-order','entry_decision_epoch':100,'entry_epoch':100+delay,'original_target_epoch':100000}}
    es=[{'arm':'a','kind':'fill','epoch':100+delay,'receipt':{'status':'filled','fee_usd':'.1','legs':[leg]}},
        {'arm':'a','kind':'financing','receipt':{'status':'financing_applied','amount_usd':'0'}}]
    return [d],[{'entry_decision':'d'}],es,{'realized_usd':'0','fees_usd':'.1','financing_usd':'0'}


@pytest.mark.parametrize('delay',[58,21658])
def test_order_bound_opening_can_follow_later_retry_tick(delay):
    from chronological_policy_report_v2 import execution_diagnostics
    ds,ep,es,state=opening_fixture(delay)
    result=execution_diagnostics(ds,ep,es,'a',state)
    assert result['filled_selected_decisions']==1 and result['delayed_open_fills']==int(delay>58)
    assert result['entry_fill_links'][0]['delay_seconds']==delay


@pytest.mark.parametrize('fault',['early','wrong_order'])
def test_loose_price_time_match_cannot_replace_order_binding(fault):
    from chronological_policy_report_v2 import execution_diagnostics
    ds,ep,es,state=opening_fixture(57 if fault=='early' else 21658)
    if fault=='wrong_order':es[0]['receipt']['legs'][0]['position']['candidate_id']='other-order'
    with pytest.raises(ValueError,match='actual_fill_binding'):execution_diagnostics(ds,ep,es,'a',state)
