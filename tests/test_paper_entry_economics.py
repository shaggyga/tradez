from pathlib import Path
from copy import deepcopy
from decimal import Decimal, localcontext
import sys
import json
import pytest
sys.path[:0]=[str(Path(__file__).resolve().parents[1]/'tools'),str(Path(__file__).parent)]
import forex_paper_entry_economics as m
import test_paper_quote_observation as old


def fixture(tmp_path):
    book,capture,joined=old.fixture(tmp_path)
    rows,_=m.observations.inspect(book,capture,joined)
    r=next(x['observation'] for x in rows if x['slot']=='saved/EUR_USD')
    declaration=dict(schema=m.SCHEMA,evidence_tier='declared_paper_scenario',book_id='joined_test',slot='saved/EUR_USD',
        receipt_sha256=r['receipt_sha256'],quote_id=r['quote_id'],decision_epoch=10002,terminal_epoch=10300,
        costs_available_epoch=10000,costs_valid_from_epoch=10000,costs_valid_through_epoch=10300,
        valuation_convention='original_terminal_mid_frozen_current_spread_and_conversion',
        slippage_convention='separate_cost_once',spread_in_gross=True,financing_scope='entry_through_terminal_including_rollover',
        cost_basis='Synthetic correctness fixture; not actual pricing',side=1,
        risk=dict(account_currency='USD',budget_kind='declared_paper_allocation',notional_usd='11002',maximum_notional_usd='12000',
            available_margin_usd='500',margin_rate='0.02',maximum_loss_usd='100',declared_worst_loss_usd='50'),
        future_costs_usd=dict(fees='0.2',slippage='0.1',financing_debit='0.4',financing_credit='0.1'))
    request=dict(schema=m.SCHEMA,source_bindings=m.sources(),observation_request_sha256=m.observations.paper.digest(joined),
        declarations={'saved/EUR_USD':declaration})
    return book,capture,joined,request,r


def mapped(joined):
    q=joined['combined_request']
    return m.observations.combined.quotes.map_receipts(q['quote_snapshot'],observed_epoch=10002,
        decision_epoch=10002,instruments=q['quote_snapshot']['instruments'])['quotes']


def test_actual_consumer_preserves_missing_and_no_authority(tmp_path):
    book,capture,j,r,_=fixture(tmp_path);rows,report=m.inspect(book,capture,j,r)
    assert report['statuses']=={'entry_inputs_missing':135,'declared_entry_scenario_consistent':1}
    got=next(x for x in rows if x['slot']=='saved/EUR_USD')
    assert Decimal(got['gross_usd_proxy'])==Decimal('-0.8')
    assert Decimal(got['net_usd_proxy'])==Decimal('-1.4')
    assert got['base_units']==10000 and Decimal(got['declared_margin_usd'])==Decimal('220.04')
    assert not got['margin_is_expense'] and not any(x['action_eligible'] for x in rows)


@pytest.mark.parametrize('side,mid',[(1,'1.1013'),(-1,'1.0989')])
def test_design_ten_dollar_fixtures_spread_once(tmp_path,side,mid):
    _,_,j,r,row=fixture(tmp_path);d=r['declarations']['saved/EUR_USD'];d['side']=side
    # Hand-defined arithmetic fixture; never presented as a saved forecast.
    row['candidate']['original_terminal_price_estimate']=mid
    with localcontext() as c:
        c.prec=80;out=m.evaluate(row,mapped(j),d,book_id='joined_test',decision=10002)
    assert Decimal(out['gross_usd_proxy'])==10 and Decimal(out['net_usd_proxy'])==Decimal('9.4')


@pytest.mark.parametrize('fault',['missing_financing','float_cost','negative_cost','future_cost','short_coverage','wrong_quote',
    'wrong_receipt','wrong_book','terminal','spread','slippage','no_rollover','actual_account','risk','margin','cost_basis','tier'])
def test_refusals_do_not_become_zero_cost_entries(tmp_path,fault):
    book,capture,j,r,_=fixture(tmp_path);d=r['declarations']['saved/EUR_USD']
    if fault=='missing_financing':del d['future_costs_usd']['financing_debit']
    elif fault=='float_cost':d['future_costs_usd']['fees']=0.2
    elif fault=='negative_cost':d['future_costs_usd']['fees']='-1'
    elif fault=='future_cost':d['costs_available_epoch']=10003
    elif fault=='short_coverage':d['costs_valid_through_epoch']=10299
    elif fault=='wrong_quote':d['quote_id']='wrong'
    elif fault=='wrong_receipt':d['receipt_sha256']='0'*64
    elif fault=='wrong_book':d['book_id']='other'
    elif fault=='terminal':d['terminal_epoch']=10301
    elif fault=='spread':d['spread_in_gross']=False
    elif fault=='slippage':d['slippage_convention']='already_in_price'
    elif fault=='no_rollover':d['financing_scope']='ignored'
    elif fault=='actual_account':d['risk']['budget_kind']='actual_account'
    elif fault=='risk':d['risk']['declared_worst_loss_usd']='101'
    elif fault=='margin':d['risk']['available_margin_usd']='1'
    elif fault=='cost_basis':d['cost_basis']=''
    elif fault=='tier':d['evidence_tier']='actual_costs'
    rows,_=m.inspect(book,capture,j,r);out=next(x for x in rows if x['slot']=='saved/EUR_USD')
    assert out['status']=='entry_inputs_refused' and 'net_usd_proxy' not in out and not out['action_eligible']


def test_cross_currency_uses_loss_conversion_side(tmp_path):
    _,_,j,r,row=fixture(tmp_path);q=mapped(j);d=r['declarations']['saved/EUR_USD']
    row.update(instrument='USD_JPY');row['candidate']['original_terminal_price_estimate']='149.92'
    d.update(slot='saved/USD_JPY');d['risk'].update(notional_usd='1000')
    q['USD_JPY']={**q['EUR_USD'],'instrument':'USD_JPY','quote_id':'usd_jpy_fixture','bid':'150.00','ask':'150.02'}
    row['quote_id']=q['USD_JPY']['quote_id'];d['quote_id']=row['quote_id']
    with localcontext() as c:
        c.prec=80;out=m.evaluate(row,q,d,book_id='joined_test',decision=10002)
        assert out['base_units']==1000 and Decimal(out['gross_quote_proxy'])==Decimal('-110')
        # Independent rational answer versus the reference's rounded reciprocal
        # then multiplication, both at 80-digit precision.
        assert abs(Decimal(out['gross_usd_proxy'])-Decimal('-110')/Decimal('150.00'))<Decimal('1e-78')
    assert out['conversion']['conversion_side']=='buy_loss'


def test_observation_and_population_drift_refused(tmp_path):
    book,capture,j,r,_=fixture(tmp_path);r['observation_request_sha256']='0'*64
    with pytest.raises(ValueError,match='entry_observation_binding'):m.inspect(book,capture,j,r)
    r['observation_request_sha256']=m.observations.paper.digest(j);r['declarations']['unknown']=r['declarations']['saved/EUR_USD']
    with pytest.raises(ValueError,match='entry_population'):m.inspect(book,capture,j,r)


def test_resume_and_invalid_external_pin(tmp_path):
    book,capture,j,r,_=fixture(tmp_path);jp=tmp_path/'joined.json';rp=tmp_path/'entry.json'
    jp.write_bytes(m.prior.encoded(j));rp.write_bytes(m.prior.encoded(r));pin=m.prior.sha(rp.read_bytes());out=tmp_path/'runs'
    with pytest.raises(ValueError,match='entry_request_external_hash'):m.run(book,capture,jp,rp,'0'*64,out,'bad')
    assert m.run(book,capture,jp,rp,pin,out,'resume',max_new=1)['status']=='checkpointed'
    assert m.run(book,capture,jp,rp,pin,out,'resume',resume=True)['status']=='completed'
    assert m.run(book,capture,jp,rp,pin,out,'whole')['status']=='completed'
    for p in list((out/'whole').glob('rows_*.json'))+[out/'whole/REPORT.json']:
        assert p.read_bytes()==(out/'resume'/p.name).read_bytes()
