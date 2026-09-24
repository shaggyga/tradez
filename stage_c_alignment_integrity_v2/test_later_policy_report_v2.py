import pytest
from later_policy_report_v2 import terminal_status, paired, build


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
    a={'terminal_status':'closed_terminal','method':'a','scenario':'s','arm':'fixed_hold','net_account_pnl_usd':'2','fills':2,'filled_replacement_episodes':0}
    b={**a,'method':'b','net_account_pnl_usd':'1'}
    assert paired(a,b,'matched')['net_usd_delta']=='1'
    b.update(terminal_status='unpriced_terminal',net_account_pnl_usd=None)
    result=paired(a,b,'matched');assert result['net_usd_delta'] is None and result['status']=='terminal_comparison_unavailable'


def test_partial_operator_receipt_cannot_produce_full_comparison(tmp_path):
    with pytest.raises(ValueError,match='exact_verified56run'):build(tmp_path,{'status':'completed_verified','runs':[]})
