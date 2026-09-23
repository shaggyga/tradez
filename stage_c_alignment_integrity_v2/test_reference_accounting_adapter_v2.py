from copy import deepcopy
from decimal import Decimal
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import reference_accounting_adapter_v2 as adapter


def execute(frames=None, contract=None):
    reference = adapter.load_reference(adapter.DEFAULT_TRAD)
    return adapter.replay_synthetic(adapter.fixture_frames() if frames is None else frames,
                                    adapter.synthetic_contract(reference) if contract is None else contract)


def test_every_event_matches_hand_calculated_balances_and_liquidation_values():
    report = execute()
    expected = {
        "fixed_hold_fixture": [("10000", "9990"), ("10000", "10020"), ("10040", "10040"), ("10040", "10040"), ("10040", "10040")],
        "rotation_fixture": [("10000", "9990"), ("10020", "10019.90"), ("10028", "10028"), ("10028", "10027.90"), ("10018", "10018")],
    }
    for arm, values in expected.items():
        rows = [row for row in report["ledger"] if row["arm"] == arm]
        assert [(Decimal(row["realized_balance_usd"]), Decimal(row["liquidation_equity_usd"])) for row in rows] == [(Decimal(a), Decimal(b)) for a, b in values]
        assert all(row["receipt"]["status"] == "applied" for row in rows)
        assert report["final_states"][arm]["position"] is None
    rotating = [row for row in report["ledger"] if row["arm"] == "rotation_fixture"]
    profit = rotating[2]["receipt"]["legs"][0]
    loss = rotating[4]["receipt"]["legs"][0]
    assert Decimal(profit["quote_currency_pnl"]) == 1000 and Decimal(profit["realized_usd"]) == 8
    assert profit["conversion"]["conversion_side"] == "sell_profit"
    assert Decimal(loss["quote_currency_pnl"]) == -1000 and Decimal(loss["realized_usd"]) == -10
    assert loss["conversion"]["conversion_side"] == "buy_loss"


def test_execution_prices_cannot_resize_decision_or_change_earlier_ledger():
    original = execute()
    frames = adapter.fixture_frames()
    frames[1]["execution_quotes"]["GBP_USD"]["ask"] = "30.01"
    changed = execute(frames)
    for old, new in zip(original["ledger"], changed["ledger"]):
        assert old["decision"] == new["decision"]
    sizing = changed["ledger"][3]["decision"]
    assert sizing["base_units"] == 500
    assert Decimal(sizing["sizing"]["decision_value_usd"]) == 1000
    assert original["ledger"][:2] == changed["ledger"][:2]


def test_policy_arm_changes_do_not_leak_into_other_arm_and_inputs_stay_unchanged():
    frames = adapter.fixture_frames()
    before = deepcopy(frames)
    baseline = execute(frames)
    assert frames == before
    frames[1]["actions"]["rotation_fixture"] = {"action": "hold"}
    modified = execute(frames)
    fixed = lambda report: [row for row in report["ledger"] if row["arm"] == "fixed_hold_fixture"]
    assert fixed(baseline) == fixed(modified)
    assert baseline["final_states"]["rotation_fixture"] != modified["final_states"]["rotation_fixture"]


def test_missing_conversion_rejects_rotation_atomically_without_fabricating_equity():
    frames = adapter.fixture_frames()
    del frames[1]["execution_quotes"]["USD_JPY"]
    report = execute(frames)
    row = report["ledger"][3]
    assert row["receipt"]["status"] == "rejected"
    assert row["before_state"] == row["after_state"]
    assert row["receipt"]["legs"] == []


@pytest.mark.parametrize("tier", ["historical_midpoint", "historical_bid_ask_original_arrival_unverified", None])
def test_actual_data_never_silently_enters_synthetic_adapter(tier):
    frames = adapter.fixture_frames()
    frames[0]["input_tier"] = tier
    with pytest.raises(ValueError, match="actual_candle_execution_unsupported"):
        execute(frames)


def test_later_quote_availability_rejects_fill_without_state_mutation():
    frames = adapter.fixture_frames()
    frames[0]["execution_quotes"]["EUR_USD"]["available_epoch"] = 1061
    report = execute(frames)
    for row in report["ledger"][:2]:
        assert row["receipt"]["status"] == "rejected"
        assert row["before_state"] == row["after_state"]


def test_caller_supplied_units_are_rejected():
    frames = adapter.fixture_frames()
    frames[0]["actions"]["fixed_hold_fixture"]["base_units"] = 99999
    with pytest.raises(ValueError, match="without_caller_sizing"):
        execute(frames)


def test_changed_reference_is_rejected_before_any_module_import(tmp_path, monkeypatch):
    source = tmp_path / "oanda_curve_management_replay_v1.py"
    source.write_text("raise RuntimeError('must never execute')\n")
    dependency = tmp_path / "src" / "forex_system" / "research" / "sequential_portfolio_replay_v1.py"
    dependency.parent.mkdir(parents=True)
    dependency.write_bytes((adapter.DEFAULT_TRAD / dependency.relative_to(tmp_path)).read_bytes())
    def refuse_import(*args):
        raise AssertionError("an unreviewed module was imported")
    monkeypatch.setattr(adapter.importlib, "import_module", refuse_import)
    with pytest.raises(ValueError, match="refused_before_import"):
        adapter.load_reference(tmp_path)
