"""Declared synthetic daily hold/reduce lifecycle, with hand-checkable amounts."""
from copy import deepcopy

from accounting_events_v2 import TIER, event_contract
from reference_accounting_adapter_v2 import quote, DEFAULT_TRAD


def panel(epoch, bid="1.1000", ask="1.1002", *, jpy_bid="150.00", jpy_ask="150.02"):
    return {"EUR_USD": quote("EUR_USD", bid, ask, epoch),
            "USD_JPY": quote("USD_JPY", jpy_bid, jpy_ask, epoch),
            "GBP_USD": quote("GBP_USD", "1.25", "1.26", epoch),
            "GBP_JPY": quote("GBP_JPY", "187.50", "189.00", epoch)}


def event(kind, epoch, sequence, *, arm=None, quotes=None, **fields):
    row = {"event_id": f"e{epoch}-{sequence}", "kind": kind, "epoch": epoch, "sequence": sequence,
           "input_tier": TIER, "quotes": deepcopy(panel(epoch) if quotes is None else quotes), **fields}
    if arm is not None:
        row["arm"] = arm
    return row


def lifecycle_fixture(trad_root=DEFAULT_TRAD):
    contract = event_contract(trad_root)
    contract["maximum_order_notional_usd"] = "20000"
    events = []
    for sequence, arm in enumerate(("hold", "rotate")):
        events.append(event("intent", 1000, sequence, arm=arm, order_id=f"{arm}-entry", instrument="EUR_USD",
                            action="open", side=1, notional_usd="11002", target_epoch=200000))
        events.append(event("activate", 1060, sequence, arm=arm, order_id=f"{arm}-entry"))
    events += [
        event("fill", 1060, 2, arm="hold", order_id="hold-entry", fill_id="h1", units=4000, filled_epoch=1060, fee_usd="0.4", execution_evidence_id="fixture-h1"),
        event("fill", 1060, 3, arm="rotate", order_id="rotate-entry", fill_id="r1", units=10000, filled_epoch=1060, fee_usd="1", execution_evidence_id="fixture-r1"),
        event("cancel_request", 1100, 0, arm="hold", order_id="hold-entry"),
        event("fill", 1140, 0, arm="hold", order_id="hold-entry", fill_id="h2", units=6000, filled_epoch=1140, fee_usd="0.6", execution_evidence_id="fixture-h2"),
        event("cancel_ack", 1150, 0, arm="hold", order_id="hold-entry"),
        event("intent", 2000, 0, arm="rotate", order_id="rotate-partial", instrument="EUR_USD", action="reduce", units=4000, lot_ids=["r1"], quotes=panel(2000, "1.1012", "1.1014")),
        event("activate", 2060, 0, arm="rotate", order_id="rotate-partial", quotes=panel(2060, "1.1012", "1.1014")),
        event("fill", 2060, 1, arm="rotate", order_id="rotate-partial", fill_id="r2", units=4000, filled_epoch=2060, fee_usd="0.4", execution_evidence_id="fixture-r2", quotes=panel(2060, "1.1012", "1.1014")),
    ]
    for sequence, arm in enumerate(("hold", "rotate")):
        events.append(event("financing", 86400, sequence, arm=arm, financing_id=f"fin-{arm}", rates={"EUR_USD": {"long": "-.0001", "short": ".00005"}}, provenance_id="declared-synthetic-rate-not-history", accrual_period_id="fixture-first-rollover", quotes=panel(86400, "1.1022", "1.1024")))
        lots = ["h1", "h2"] if arm == "hold" else ["r1"]
        units = 10000 if arm == "hold" else 6000
        events.append(event("intent", 86500, sequence, arm=arm, order_id=f"{arm}-exit", instrument="EUR_USD", action="reduce", units=units, lot_ids=lots, quotes=panel(86500, "1.1022", "1.1024")))
        events.append(event("activate", 86560, sequence, arm=arm, order_id=f"{arm}-exit", quotes=panel(86560, "1.1022", "1.1024")))
        events.append(event("fill", 86560, sequence + 2, arm=arm, order_id=f"{arm}-exit", fill_id=f"{arm}-final", units=units, filled_epoch=86560, fee_usd="1" if arm == "hold" else "0.6", execution_evidence_id=f"fixture-{arm}-exit", quotes=panel(86560, "1.1022", "1.1024")))
    events.sort(key=lambda row: (row["epoch"], row["sequence"]))
    return contract, events


def hand_oracle():
    return {"initial_capital_usd": "10000", "decision_units": 10000, "entry_ask": "1.1002",
            "reserved_margin_usd": "1100.2", "partial_fill_used_margin_usd": "440.08",
            "partial_fill_reserved_margin_usd": "660.12",
            "hold": {"realized_usd": "20", "financing_usd": "-1", "fees_usd": "2", "cash_usd": "10017"},
            "rotate": {"realized_usd": "16", "financing_usd": "-0.6", "fees_usd": "2", "cash_usd": "10013.4"},
            "scope": "synthetic_one_rollover_scenario_not_historical_strategy_evidence"}
