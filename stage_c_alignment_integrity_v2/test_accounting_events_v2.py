from copy import deepcopy
from decimal import Decimal, localcontext
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from accounting_events_v2 import EventLedger, event_contract
from accounting_event_fixtures_v2 import event, hand_oracle, lifecycle_fixture, panel


def through(epoch=1060, sequence=3):
    contract, events = lifecycle_fixture()
    book = EventLedger(contract)
    rows = book.replay([row for row in events if (row["epoch"], row["sequence"]) <= (epoch, sequence)])
    return book, rows


def test_daily_lifecycle_all_events_reconcile_with_hand_oracle():
    contract, events = lifecycle_fixture()
    untouched = deepcopy(events)
    book = EventLedger(contract)
    rows = book.replay(events)
    assert events == untouched
    assert len(rows) == 20
    assert all(row["receipt"]["status"] != "rejected" for row in rows)
    expected = hand_oracle()
    for arm in contract["arms"]:
        final = book.state["arms"][arm]
        assert {key: final[key] for key in expected[arm]} == {key: Decimal(value) for key, value in expected[arm].items()}
        assert not final["lots"]
        assert rows[-1]["arms"][arm]["used_margin_usd"] == 0
        assert rows[-1]["arms"][arm]["reserved_margin_usd"] == 0
    for row in rows:
        for values in row["arms"].values():
            assert values["cash_reconciliation_residual_usd"] == 0
            assert values["cash_usd"] == Decimal(10000) + values["realized_usd"] + values["financing_usd"] - values["fees_usd"]
            assert values["equity_usd"] == values["cash_usd"] + values["unrealized_usd"]
            assert values["free_margin_usd"] == values["equity_usd"] - values["used_margin_usd"] - values["reserved_margin_usd"]


def test_margin_reservation_is_capacity_not_a_cash_charge():
    book, rows = through(1060, 2)
    assert rows[0]["arms"]["hold"]["cash_usd"] == 10000
    assert rows[0]["arms"]["hold"]["reserved_margin_usd"] == Decimal("1100.2")
    after = rows[-1]["arms"]["hold"]
    assert after["cash_usd"] == Decimal("9999.6")  # only fee, never margin
    assert after["used_margin_usd"] == Decimal("440.08")
    assert after["reserved_margin_usd"] == Decimal("660.12")
    assert after["unrealized_usd"] == Decimal("-0.8")


def test_partial_close_keeps_remaining_units_entry_basis_and_margin():
    book, rows = through(2060, 1)
    lot = book.state["arms"]["rotate"]["lots"]["r1"]["position"]
    assert lot["base_units"] == 6000
    assert lot["entry_price"] == Decimal("1.1002")
    assert rows[-1]["arms"]["rotate"]["realized_usd"] == Decimal(4)
    assert rows[-1]["arms"]["rotate"]["used_margin_usd"] == Decimal("660.12")


def test_late_cancellation_does_not_erase_fill_or_release_filled_margin():
    book, rows = through(1150, 0)
    assert rows[-2]["receipt"]["status"] == "filled"
    assert rows[-1]["receipt"]["status"] == "late_cancel_ack_after_fill"
    assert sum(lot["position"]["base_units"] for lot in book.state["arms"]["hold"]["lots"].values()) == 10000
    assert rows[-1]["arms"]["hold"]["used_margin_usd"] == Decimal("1100.2")


def test_cancel_ack_releases_only_unfilled_reservation():
    book, _ = through(1060, 2)
    book.apply(event("cancel_request", 1070, 0, arm="hold", order_id="hold-entry"))
    row = book.apply(event("cancel_ack", 1080, 0, arm="hold", order_id="hold-entry"))
    assert row["arms"]["hold"]["reserved_margin_usd"] == 0
    assert row["arms"]["hold"]["used_margin_usd"] == Decimal("440.08")
    assert row["arms"]["hold"]["cash_usd"] == Decimal("9999.6")


def test_duplicate_fill_delivery_and_conflict_do_not_change_economics():
    contract, events = lifecycle_fixture()
    book, _ = through(1060, 2)
    fill = deepcopy(next(row for row in events if row.get("fill_id") == "h1"))
    fill.update(event_id="redelivery", epoch=1070, sequence=0)
    before = deepcopy(book.state["arms"])
    assert book.apply(fill)["receipt"]["status"] == "duplicate_fill_noop"
    assert book.state["arms"] == before
    fill.update(event_id="conflict", epoch=1080, units=3999)
    rejected = book.apply(fill)
    assert rejected["receipt"]["reason"] == "conflicting_duplicate_fill_identity"
    assert book.state["arms"] == before


@pytest.mark.parametrize("mutation", ["before_activation", "above_remaining", "missing_quote", "late_quote", "concentration", "fee_exhaustion"])
def test_invalid_fill_is_atomic(mutation):
    book, _ = through(1000, 1)
    if mutation != "before_activation":
        book.apply(event("activate", 1060, 0, arm="hold", order_id="hold-entry"))
    quotes = panel(1060)
    if mutation == "missing_quote":
        quotes.pop("EUR_USD")
    if mutation == "late_quote":
        quotes["EUR_USD"]["available_epoch"] = 1062
    if mutation == "concentration":
        book.currency_limit = Decimal("1")
    before = deepcopy(book.state["arms"])
    row = book.apply(event("fill", 1060, 1, arm="hold", order_id="hold-entry", fill_id="invalid",
                           units=10001 if mutation == "above_remaining" else 10000, filled_epoch=1060,
                           fee_usd="10000" if mutation == "fee_exhaustion" else "0", execution_evidence_id="test", quotes=quotes))
    assert row["receipt"]["status"] == "rejected"
    assert book.state["arms"] == before


def test_financing_is_signed_exact_and_duplicate_period_is_blocked():
    book, rows = through(86400, 1)
    assert rows[-2]["arms"]["hold"]["financing_usd"] == -1
    assert rows[-1]["arms"]["rotate"]["financing_usd"] == Decimal("-.6")
    before = deepcopy(book.state["arms"])
    row = book.apply(event("financing", 86401, 0, arm="hold", financing_id="different-id-same-period",
                           rates={"EUR_USD": {"long": "-.0001"}}, provenance_id="fixture",
                           accrual_period_id="fixture-first-rollover", quotes=panel(86401)))
    assert row["receipt"]["status"] == "rejected"
    assert book.state["arms"] == before


def test_unknown_financing_rate_does_not_silently_mean_zero():
    book, _ = through(1060, 3)
    before = deepcopy(book.state["arms"])
    row = book.apply(event("financing", 2000, 0, arm="hold", financing_id="missing", rates={},
                           provenance_id="fixture", accrual_period_id="period-1"))
    assert row["receipt"]["reason"] == "explicit_financing_rate_missing"
    assert book.state["arms"] == before


def test_two_arms_share_quotes_but_never_holdings_or_order_capacity():
    contract, events = lifecycle_fixture()
    book = EventLedger(contract)
    for row in events:
        other = "rotate" if row.get("arm") == "hold" else "hold"
        before = deepcopy(book.state["arms"][other])
        book.apply(row)
        assert book.state["arms"][other] == before


def test_insufficient_margin_and_concentration_veto_new_intent():
    for key, value, reason in (("initial_capital_usd", "100", "capacity"), ("maximum_gross_currency_usd", "100", "concentration")):
        contract, events = lifecycle_fixture()
        contract[key] = value
        book = EventLedger(contract)
        row = book.apply(events[0])
        assert row["receipt"]["status"] == "rejected"
        assert reason in row["receipt"]["reason"]
        assert book.state["arms"]["hold"]["orders"] == {}


def test_missing_marks_preserve_positions_and_explicit_unknown_equity():
    book, _ = through(1060, 3)
    before = deepcopy(book.state["arms"])
    row = book.apply(event("mark", 1200, 0, quotes={}))
    assert book.state["arms"] == before
    assert row["arms"]["hold"]["equity_usd"] is None
    assert row["arms"]["hold"]["risk_status"] == "unknown"


def test_reject_second_spread_fee_and_unsupported_limit_fill_assumptions():
    book, _ = through(1000, 1)
    base = event("activate", 1060, 0, arm="hold", order_id="hold-entry")
    for key in ("spread_fee_usd", "limit_touched", "fill_guaranteed"):
        with pytest.raises(ValueError, match="exact_event_schema"):
            book.apply({**base, key: "1"})


@pytest.mark.parametrize("key,value", [("real_money", True), ("broker_access", True), ("can_place_orders", True),
                                       ("input_tier", "historical_bid_ask_candles"), ("mode", "live")])
def test_live_or_unqualified_input_contracts_are_refused(key, value):
    contract = event_contract()
    contract[key] = value
    with pytest.raises(ValueError, match="unsafe_contract"):
        EventLedger(contract)


def test_unique_event_order_and_conflicting_identity_are_required():
    contract, events = lifecycle_fixture()
    book = EventLedger(contract)
    book.apply(events[0])
    unchanged = deepcopy(book.state)
    assert book.apply(events[0])["status"] == "duplicate_event_noop"
    assert book.state == unchanged
    with pytest.raises(ValueError, match="conflicting_event_id"):
        book.apply({**events[0], "notional_usd": "11000"})
    with pytest.raises(ValueError, match="clock_must_increase"):
        book.apply({**events[1], "sequence": 0})


def test_usd_jpy_design_example_uses_current_exit_conversion():
    contract = event_contract()
    book = EventLedger(contract)
    rows = [event("intent", 1000, 0, arm="hold", order_id="jpy", instrument="USD_JPY", action="open", side=1, notional_usd="1000", target_epoch=5000),
            event("activate", 1060, 0, arm="hold", order_id="jpy"),
            event("fill", 1060, 1, arm="hold", order_id="jpy", fill_id="jpy-entry", units=1000, filled_epoch=1060, fee_usd="0", execution_evidence_id="fixture"),
            event("intent", 2000, 0, arm="hold", order_id="close", instrument="USD_JPY", action="reduce", units=1000, lot_ids=["jpy-entry"]),
            event("activate", 2060, 0, arm="hold", order_id="close"),
            event("fill", 2060, 1, arm="hold", order_id="close", fill_id="jpy-exit", units=1000, filled_epoch=2060, fee_usd="0", execution_evidence_id="fixture", quotes=panel(2060, jpy_bid="150.12", jpy_ask="150.14"))]
    result = book.replay(rows)
    leg = result[-1]["receipt"]["legs"][0]
    assert leg["quote_currency_pnl"] == 100
    with localcontext(book.reference.CTX):
        assert leg["realized_usd"] == Decimal(100) * (Decimal(1) / Decimal("150.14"))
    assert leg["conversion"]["conversion_side"] == "sell_profit"


def test_missing_cross_currency_conversion_rejects_actual_open_fill():
    book = EventLedger(event_contract())
    book.apply(event("intent", 1000, 0, arm="hold", order_id="cross", instrument="GBP_JPY", action="open", side=1, notional_usd="1000", target_epoch=5000))
    book.apply(event("activate", 1060, 0, arm="hold", order_id="cross"))
    quotes = panel(1060)
    quotes.pop("USD_JPY")
    before = deepcopy(book.state["arms"])
    row = book.apply(event("fill", 1060, 1, arm="hold", order_id="cross", fill_id="cross-fill", units=793,
                           filled_epoch=1060, fee_usd="0", execution_evidence_id="fixture", quotes=quotes))
    assert row["receipt"]["status"] == "rejected"
    assert "missing_usd_conversion:JPY" in row["receipt"]["reason"]
    assert book.state["arms"] == before


def test_financing_credit_and_missing_conversion_preserve_signed_cash_rules():
    book, _ = through(1060, 3)
    before = book.state["arms"]["hold"]["cash_usd"]
    result = book.apply(event("financing", 2000, 0, arm="hold", financing_id="credit", rates={"EUR_USD": {"long": ".0001"}},
                              provenance_id="declared-positive-synthetic", accrual_period_id="credit-period"))
    assert result["receipt"]["amount_usd"] == Decimal(".4")
    assert book.state["arms"]["hold"]["cash_usd"] == before + Decimal(".4")


def test_failed_reconciliation_cannot_commit_any_state(monkeypatch):
    book = EventLedger(event_contract())
    before = deepcopy(book.state)
    def fail(*args):
        raise RuntimeError("injected_reconciliation_failure")
    monkeypatch.setattr(book, "reconcile", fail)
    with pytest.raises(RuntimeError, match="injected"):
        book.apply(event("mark", 1000, 0))
    assert book.state == before


def test_pending_orders_are_included_in_currency_concentration_admission():
    contract = event_contract()
    contract["maximum_gross_currency_usd"] = "2500"
    book = EventLedger(contract)
    first = book.apply(event("intent", 1000, 0, arm="hold", order_id="a", instrument="USD_JPY", action="open", side=1, notional_usd="1500", target_epoch=5000))
    second = book.apply(event("intent", 1000, 1, arm="hold", order_id="b", instrument="GBP_JPY", action="open", side=1, notional_usd="1500", target_epoch=5000))
    assert first["receipt"]["status"] == "accepted"
    assert second["receipt"]["status"] == "rejected"
    assert "concentration" in second["receipt"]["reason"]
    assert set(book.state["arms"]["hold"]["orders"]) == {"a"}


def test_reducing_position_is_allowed_when_currency_risk_is_breached():
    book, _ = through(1150, 0)
    book.currency_limit = Decimal("1")
    intent = event("intent", 2000, 0, arm="hold", order_id="risk-exit", instrument="EUR_USD", action="reduce", units=10000, lot_ids=["h1", "h2"])
    assert book.apply(intent)["receipt"]["status"] == "accepted"
    book.apply(event("activate", 2060, 0, arm="hold", order_id="risk-exit"))
    result = book.apply(event("fill", 2060, 1, arm="hold", order_id="risk-exit", fill_id="risk-fill", units=10000, filled_epoch=2060, fee_usd="0", execution_evidence_id="fixture"))
    assert result["receipt"]["status"] == "filled"
    assert not book.state["arms"]["hold"]["lots"]


@pytest.mark.parametrize("side,exit_bid,exit_ask", [(1,"1.1012","1.1014"), (-1,"1.0988","1.0990")])
def test_design_long_and_short_eurusd_examples_charge_spread_once(side, exit_bid, exit_ask):
    contract, _ = lifecycle_fixture()
    book = EventLedger(contract)
    events = [event("intent", 1000, 0, arm="hold", order_id="entry", instrument="EUR_USD", action="open", side=side, notional_usd="11002", target_epoch=5000),
              event("activate", 1060, 0, arm="hold", order_id="entry"),
              event("fill", 1060, 1, arm="hold", order_id="entry", fill_id="entry-fill", units=10000, filled_epoch=1060, fee_usd="0", execution_evidence_id="fixture"),
              event("intent", 2000, 0, arm="hold", order_id="exit", instrument="EUR_USD", action="reduce", units=10000, lot_ids=["entry-fill"]),
              event("activate", 2060, 0, arm="hold", order_id="exit"),
              event("fill", 2060, 1, arm="hold", order_id="exit", fill_id="exit-fill", units=10000, filled_epoch=2060, fee_usd="0", execution_evidence_id="fixture", quotes=panel(2060,exit_bid,exit_ask))]
    rows = book.replay(events)
    assert rows[-1]["receipt"]["legs"][0]["quote_currency_pnl"] == 10
    assert book.state["arms"]["hold"]["cash_usd"] == 10010


def test_holding_deadline_is_reported_without_inventing_stale_liquidation():
    book, _ = through(1150, 0)
    before = deepcopy(book.state["arms"])
    row = book.apply(event("mark", 200001, 0, quotes={}))
    assert row["arms"]["hold"]["risk_status"] == "breached"
    assert set(row["arms"]["hold"]["holding_deadline_breaches"]) == {"h1", "h2"}
    assert row["arms"]["hold"]["equity_usd"] is None
    assert book.state["arms"] == before
