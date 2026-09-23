from copy import deepcopy
from decimal import Decimal
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from accounting_events_v2 import EventLedger, event_contract
from accounting_event_fixtures_v2 import event, panel
from accounting_event_audit_v2 import audit_accounting


def resting(kind="stop", side=1, price="1.1010"):
    book = EventLedger(event_contract())
    book.apply(event("intent", 1000, 0, arm="hold", order_id="resting", instrument="EUR_USD", action="open",
                     side=side, notional_usd="1000", target_epoch=5000, order_type=kind, trigger_price=price))
    return book


def fill(book, epoch, quotes, sequence=1):
    return book.apply(event("fill", epoch, sequence, arm="hold", order_id="resting", fill_id=f"fill-{epoch}",
                            units=book.state["arms"]["hold"]["orders"]["resting"]["units"], filled_epoch=epoch,
                            fee_usd="0", execution_evidence_id="explicit-synthetic-fill", quotes=quotes))


def test_trigger_cannot_use_path_before_activation():
    book = resting()
    before = deepcopy(book.state["arms"])
    result = book.apply(event("trigger", 1020, 0, arm="hold", order_id="resting", trigger_evidence_id="too-early", quotes=panel(1020,"1.1020","1.1022")))
    assert result["receipt"]["status"] == "rejected"
    assert book.state["arms"] == before
    result = book.apply(event("activate", 1030, 0, arm="hold", order_id="resting"))
    assert result["receipt"]["status"] == "rejected"


def test_stop_gap_uses_next_explicit_quote_never_guaranteed_stop_price():
    book = resting()
    book.apply(event("activate", 1060, 0, arm="hold", order_id="resting"))
    book.apply(event("trigger", 1070, 0, arm="hold", order_id="resting", trigger_evidence_id="gap-quote", quotes=panel(1070,"1.1020","1.1022")))
    result = fill(book, 1080, panel(1080,"1.1030","1.1032"))
    assert result["receipt"]["status"] == "filled"
    assert result["receipt"]["legs"][0]["executed_price"] == Decimal("1.1032")
    assert result["receipt"]["legs"][0]["executed_price"] != Decimal("1.1010")


def test_limit_touch_is_not_a_fill_and_cannot_fill_worse_than_limit():
    book = resting("limit", price="1.09")
    book.apply(event("activate", 1060, 0, arm="hold", order_id="resting"))
    result = book.apply(event("trigger", 1070, 0, arm="hold", order_id="resting", trigger_evidence_id="touch", quotes=panel(1070,"1.0898","1.0900")))
    assert result["receipt"]["status"] == "triggered"
    assert not book.state["arms"]["hold"]["lots"]
    assert fill(book, 1080, panel(1080,"1.0900","1.0902"))["receipt"]["reason"] == "fill_price_violates_limit"
    result = fill(book, 1090, panel(1090,"1.0896","1.0898"))
    assert result["receipt"]["status"] == "filled"
    assert result["receipt"]["legs"][0]["executed_price"] == Decimal("1.0898")


def test_fill_without_trigger_is_rejected_even_with_price_touch():
    book = resting("limit", price="1.10")
    book.apply(event("activate", 1060, 0, arm="hold", order_id="resting"))
    assert fill(book, 1070, panel(1070,"1.0990","1.0992"))["receipt"]["reason"] == "resting_fill_requires_prior_trigger_evidence"


@pytest.mark.parametrize("kind,side,barrier,bid,ask,reached", [
    ("stop",1,"1.11","1.1097","1.1099",False), ("stop",1,"1.11","1.1098","1.1100",True),
    ("stop",-1,"1.09","1.0901","1.0903",False), ("stop",-1,"1.09","1.0900","1.0902",True),
    ("limit",1,"1.09","1.0899","1.0901",False), ("limit",1,"1.09","1.0898","1.0900",True),
    ("limit",-1,"1.11","1.1099","1.1101",False), ("limit",-1,"1.11","1.1100","1.1102",True),
])
def test_side_correct_trigger_boundaries(kind,side,barrier,bid,ask,reached):
    book = resting(kind,side,barrier)
    book.apply(event("activate",1060,0,arm="hold",order_id="resting"))
    row = book.apply(event("trigger",1070,0,arm="hold",order_id="resting",trigger_evidence_id="boundary",quotes=panel(1070,bid,ask)))
    assert (row["receipt"]["status"] == "triggered") == reached


def test_late_oco_cancellation_can_leave_two_fills_and_gross_currency_risk():
    contract = event_contract()
    book = EventLedger(contract)
    events = [event("intent",1000,0,arm="hold",order_id="up",instrument="EUR_USD",action="open",side=1,notional_usd="1000",target_epoch=5000,order_type="stop",trigger_price="1.1010",oco_group="bracket"),
              event("intent",1000,1,arm="hold",order_id="down",instrument="EUR_USD",action="open",side=-1,notional_usd="1000",target_epoch=5000,order_type="stop",trigger_price="1.0990",oco_group="bracket"),
              event("activate",1060,0,arm="hold",order_id="up"),event("activate",1060,1,arm="hold",order_id="down"),
              event("trigger",1070,0,arm="hold",order_id="up",trigger_evidence_id="up-observed",quotes=panel(1070,"1.1020","1.1022")),
              event("fill",1070,1,arm="hold",order_id="up",fill_id="up-fill",units=908,filled_epoch=1070,fee_usd="0",execution_evidence_id="up-fill-proof",quotes=panel(1070,"1.1020","1.1022")),
              event("cancel_request",1070,2,arm="hold",order_id="down",quotes=panel(1070,"1.1020","1.1022")),
              event("trigger",1080,0,arm="hold",order_id="down",trigger_evidence_id="down-before-ack",quotes=panel(1080,"1.0980","1.0982")),
              event("fill",1080,1,arm="hold",order_id="down",fill_id="down-fill",units=908,filled_epoch=1080,fee_usd="0",execution_evidence_id="down-fill-proof",quotes=panel(1080,"1.0980","1.0982")),
              event("cancel_ack",1090,0,arm="hold",order_id="down",quotes=panel(1090,"1.1000","1.1002"))]
    rows = book.replay(events)
    assert all(row["receipt"]["status"] != "rejected" for row in rows)
    assert rows[-1]["receipt"]["status"] == "late_cancel_ack_after_fill"
    assert len(book.state["arms"]["hold"]["lots"]) == 2
    assert rows[-1]["arms"]["hold"]["currency_exposures"]["EUR"]["net_native"] == 0
    assert rows[-1]["arms"]["hold"]["currency_exposures"]["EUR"]["gross_native"] == 1816
    assert audit_accounting(contract,events,rows)["status"] == "verified"


def test_bar_high_low_cannot_substitute_for_explicit_quote_trigger():
    book = resting()
    with pytest.raises(ValueError,match="exact_event_schema"):
        book.apply(event("trigger",1060,0,arm="hold",order_id="resting",trigger_evidence_id="bar",high="1.2",low="1.0"))
