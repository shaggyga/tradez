from copy import deepcopy
from pathlib import Path
import sys
import pytest

sys.path.insert(0, str(Path(__file__).parent))
from accounting_events_v2 import EventLedger
from accounting_fastpath_v2 import OptimizedEventLedger
from accounting_stress_v2 import scenarios


@pytest.mark.parametrize("scenario", list(scenarios()))
def test_every_event_receipt_and_entire_state_match_reference(scenario):
    contract, events = scenarios()[scenario]
    reference, optimized = EventLedger(contract), OptimizedEventLedger(contract)
    for event in events:
        assert reference.apply(event) == optimized.apply(event)
        assert reference.state == optimized.state


@pytest.mark.parametrize("side", [1, -1])
@pytest.mark.parametrize("slippage", ["0", "1.5"])
def test_partial_lot_exit_and_missing_execution_quote_match(side, slippage):
    contract, events = scenarios()["baseline"]
    contract["reference_config"]["slippage_bps_per_leg"] = slippage
    for event in events:
        if event["kind"] == "intent" and event["action"] == "open":
            event["side"] = side
    for outage in (False, True):
        tape = deepcopy(events)
        if outage:
            tape[-1]["quotes"].pop("EUR_USD")
        reference, optimized = EventLedger(contract), OptimizedEventLedger(contract)
        for event in tape:
            assert reference.apply(event) == optimized.apply(event)
            assert reference.state == optimized.state
