"""Disposable SQLite fixtures only; never inspect production as a pytest action."""
import json
from pathlib import Path
import socket
import sys
import zlib

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent/'trad'))
import inspect_research_v3 as inspector
from test_oanda_causal_forecast_ledger_joint_news_v2 import (
    Clock, contract_fixture, capture_validation_seam, issued, completed, quote,
)
from oanda_causal_forecast_ledger_joint_news_v2 import CausalForecastLedger


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket.socket, 'connect', lambda *args: (_ for _ in ()).throw(AssertionError('No network')))


@pytest.fixture(params=[('EUR_USD',.0001),('AUD_JPY',.01),('HKD_JPY',.0001),('USD_HUF',.01)])
def ledger(tmp_path,request):
    clock = Clock()
    contract = contract_fixture(*request.param)
    identity = f"joint_price_news_v1_20260907.{request.param[0]}.ridge_price_news_v1.{inspector.worker.COHORT_SCOPE}.unit"
    contract.update(contract_id=identity,cohorts={'ridge_price_news_v1':identity})
    contract['evaluation_protocol'].update(contract_id=identity+'.evaluation',cohorts=contract['cohorts'])
    instance = CausalForecastLedger(tmp_path/'test.sqlite',contract,clock=clock,activate=True)
    yield instance,clock,contract
    instance.close()


def inspect(ledger):
    db,clock,contract=ledger
    return inspector.inspect_ledger(db.path,contract,db.activated_epoch,clock=clock)


def test_empty_activation_is_not_counted_as_a_forecast(ledger):
    result=inspect(ledger)
    assert result['status']=='passed' and not any(result['counts'].values())
    assert result['examples']==[] and not result['latest_consumed_publication_secondary_verified']


def test_original_publication_and_news_clocks_are_preserved(ledger):
    db,clock,_=ledger
    identity,reference,capture,_=issued(db,clock)
    result=inspect(ledger)
    assert result['stages']=={'awaiting_entry':1}
    assert result['counts']['outcomes']==0
    row=result['examples'][0]
    assert row['decision_id']==identity and row['target_epoch']==reference['market_epoch']+3600
    assert row['news_clocks']['news_first_observed_epoch']==capture['news_first_observed_epoch']


def test_completed_original_outcome_is_counted_without_rescoring(ledger):
    db,clock,_=ledger
    completed(db,clock)
    result=inspect(ledger)
    assert result['counts']['outcomes']==1 and result['stages']=={'outcome':1}
    assert result['referenced_quotes_verified']==3 and result['checked_input_count']==1
    assert 'net_bps' not in result


@pytest.mark.parametrize('missing',['entry','target'])
def test_exact_endpoint_exclusions_are_retained(ledger,missing):
    db,clock,_=ledger
    _,reference,_,_=issued(db,clock)
    if missing=='entry':
        clock.advance(61)
        reason='missing_later_entry_before_deadline'
    else:
        clock.advance();quote(db,clock);db.settle()
        clock.value=reference['market_epoch']+3661
        reason='missing_quote_at_original_target'
    db.settle()
    result=inspect(ledger)
    assert result['stages']=={'excluded':1} and result['counts']['outcomes']==0
    assert result['exclusion_reasons']=={reason:1}


def test_readonly_inspection_never_instantiates_ledger_or_changes_retained_rows(ledger,monkeypatch):
    db,clock,_=ledger
    completed(db,clock)
    before=list(db.db.iterdump())
    monkeypatch.setattr(inspector.worker,'CausalForecastLedger',lambda *args,**kwargs:(_ for _ in ()).throw(AssertionError('writer prohibited')))
    inspect(ledger)
    assert list(db.db.iterdump())==before


def test_wrong_activation_rejected_before_forecast_inspection(ledger):
    db,clock,contract=ledger
    with pytest.raises(ValueError,match='activation_receipt_mismatch'):
        inspector.inspect_ledger(db.path,contract,db.activated_epoch-1,clock=clock)


def test_malformed_or_expanding_input_is_rejected(monkeypatch):
    monkeypatch.setattr(inspector,'MAX_INPUT_BYTES',32)
    with pytest.raises(ValueError,match='geometry|bound'):
        inspector.unpack_input(zlib.compress(b'a'*128))
