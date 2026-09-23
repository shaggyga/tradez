from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from accounting_events_v2 import EventLedger, event_contract
from accounting_event_fixtures_v2 import lifecycle_fixture, event, panel
from accounting_event_audit_v2 import audit_accounting
from accounting_event_runner_v2 import run, scorecard, decoded
from publication import verify_completed_run, RunIdentityMismatch


def command(runs, run_id, *flags):
    return [sys.executable, "-I", "-B", str(ROOT / "accounting_event_runner_v2.py"), "--run-id", run_id, "--runs-dir", str(runs), *flags]


def completed(root):
    identity = json.loads((root / "RUN_IDENTITY.json").read_text())
    return verify_completed_run(root, identity)


@pytest.mark.parametrize("boundary", [1, 5, 8, 13, 20, 0])
@pytest.mark.parametrize("engine", ["reference", "optimized"])
def test_actual_crash_resume_matches_every_state_ledger_and_report(tmp_path, boundary, engine):
    clean = subprocess.run(command(tmp_path, "clean", "--engine", engine), capture_output=True, text=True, timeout=30)
    assert clean.returncode == 0, clean.stderr
    crash = subprocess.run(command(tmp_path, "crash", "--engine", engine, "--test-crash-after-event", str(boundary)), capture_output=True, text=True, timeout=30)
    assert crash.returncode == 91, crash.stderr
    assert not (tmp_path / "crash" / "COMPLETION_MANIFEST.json").exists()
    resumed = subprocess.run(command(tmp_path, "crash", "--engine", engine, "--resume"), capture_output=True, text=True, timeout=30)
    assert resumed.returncode == 0, resumed.stderr
    first, second = completed(tmp_path / "clean"), completed(tmp_path / "crash")
    assert first["payloads"] == second["payloads"]
    for entry in first["payloads"]:
        assert (tmp_path / "clean" / entry["path"]).read_bytes() == (tmp_path / "crash" / entry["path"]).read_bytes()
    # A verified completed invocation does not republish or change provenance.
    before = (tmp_path / "crash" / "COMPLETION_MANIFEST.json").read_bytes()
    rerun = subprocess.run(command(tmp_path, "crash", "--engine", engine, "--resume"), capture_output=True, text=True, timeout=30)
    assert rerun.returncode == 0, rerun.stderr
    assert (tmp_path / "crash" / "COMPLETION_MANIFEST.json").read_bytes() == before


def test_corrupt_state_and_changed_contract_refuse_resume(tmp_path):
    result = subprocess.run(command(tmp_path, "crash", "--test-crash-after-event", "5"), capture_output=True, text=True, timeout=30)
    assert result.returncode == 91
    contract, events = lifecycle_fixture()
    changed = deepcopy(contract)
    changed["margin_rate"] = "0.2"
    with pytest.raises(RunIdentityMismatch, match="identity"):
        run(changed, events, run_id="crash", runs_dir=tmp_path, resume=True)
    (tmp_path / "crash" / "state_000005.json").write_text("{}")
    with pytest.raises(RunIdentityMismatch, match="corrupt"):
        run(contract, events, run_id="crash", runs_dir=tmp_path, resume=True)
    assert not (tmp_path / "crash" / "COMPLETION_MANIFEST.json").exists()


def test_independent_oracle_rejects_intermediate_error_even_if_final_cash_unchanged():
    contract, events = lifecycle_fixture()
    book = EventLedger(contract)
    rows = book.replay(events)
    assert audit_accounting(contract, events, rows)["event_arm_rows_checked"] == 40
    tampered = deepcopy(rows)
    tampered[5]["arms"]["hold"]["reserved_margin_usd"] += Decimal(".01")
    assert tampered[-1] == rows[-1]
    with pytest.raises(AssertionError, match="independent_accounting_mismatch"):
        audit_accounting(contract, events, tampered)


def test_no_trade_and_no_loss_metrics_are_explicitly_undefined(tmp_path):
    contract = event_contract()
    events = [event("mark", 1000, 0)]
    run(contract, events, run_id="cash", runs_dir=tmp_path)
    report = json.loads((tmp_path / "cash" / "run_report.json").read_text())
    assert report["arms"]["hold"]["profit_factor_status"] == "undefined_no_closed_lots"
    assert report["arms"]["hold"]["gross_close_profit_factor"] is None
    assert report["arms"]["hold"]["return_fraction"] == "0"
    contract, events = lifecycle_fixture()
    run(contract, events, run_id="no-loss", runs_dir=tmp_path)
    report = json.loads((tmp_path / "no-loss" / "run_report.json").read_text())
    assert report["arms"]["hold"]["profit_factor_status"] == "undefined_no_losing_close_legs"
    assert report["arms"]["hold"]["gross_close_profit_factor"] is None


def test_split_fill_and_equivalent_full_fill_have_same_economic_state():
    contract, events = lifecycle_fixture()
    split = EventLedger(contract)
    split.replay(events)
    full_events = deepcopy(events)
    first = next(row for row in full_events if row.get("fill_id") == "h1")
    first.update(units=10000, fee_usd="1")
    full_events = [row for row in full_events if row.get("fill_id") != "h2" and row["kind"] not in {"cancel_request", "cancel_ack"}]
    for row in full_events:
        if row.get("order_id") == "hold-exit" and row["kind"] == "intent":
            row["lot_ids"] = ["h1"]
    full = EventLedger(contract)
    full.replay(full_events)
    for arm in contract["arms"]:
        for key in ("cash_usd", "realized_usd", "financing_usd", "fees_usd", "lots"):
            assert split.state["arms"][arm][key] == full.state["arms"][arm][key]


def test_recovered_fill_ids_prevent_duplicate_delivery(tmp_path):
    contract, events = lifecycle_fixture()
    run(contract, events, run_id="done", runs_dir=tmp_path)
    snapshot = decoded((tmp_path / "done" / "state_000005.json").read_bytes())
    from accounting_event_runner_v2 import restore_state, identity_for
    book = EventLedger(contract)
    restore_state(book, snapshot, events, 5, identity_for(contract, events, ROOT.parent / "trad"))
    fill = deepcopy(events[4])
    fill.update(event_id="redelivery-after-restart", epoch=1070, sequence=0)
    before = deepcopy(book.state["arms"])
    assert book.apply(fill)["receipt"]["status"] == "duplicate_fill_noop"
    assert book.state["arms"] == before
