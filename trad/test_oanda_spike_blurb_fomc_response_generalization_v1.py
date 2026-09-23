from __future__ import annotations

import ast
import sqlite3
from pathlib import Path

import pytest

import oanda_spike_blurb_fomc_response_generalization_v1 as module
import oanda_spike_blurb_rbnz_response_confirmation_v1 as rbnz


def test_schedule_is_complete_official_and_clock_exact() -> None:
    events = module.validate_schedule()
    assert len(events) == 21
    assert events[0]["event_date"] == "2024-01-31"
    assert events[-1]["event_date"] == "2026-07-29"
    assert {row["event_currency"] for row in events} == {"USD"}
    assert all(row["release_url"].startswith("https://www.federalreserve.gov/") for row in events)
    assert all(row["source_direction_assigned"] == 0 for row in events)
    assert all(row["execution_eligible"] == 0 for row in events)
    assert sum(row["decision_action"] == "cut" for row in events) == 6


def test_clocks_are_exact_trios_at_same_local_time() -> None:
    events = module.validate_schedule()
    clocks = module.build_clocks(events)
    assert len(clocks) == 63
    assert {row["day_offset"] for row in clocks} == {-7, 0, 7}
    assert all(row["clock_local"][11:16] == "14:00" for row in clocks)
    for event in events:
        rows = [row for row in clocks if row["source_event_id"] == event["event_id"]]
        assert len(rows) == 3


def test_candidate_is_identical_to_rbnz_selected_rule() -> None:
    assert module.LOCKED_ARM_ID == rbnz.LOCKED_ARM_ID
    assert module.LOCKED_ARM == rbnz.LOCKED_ARM
    assert module.HOLD_MINUTES == rbnz.HOLD_MINUTES == 15
    assert len(module.DIRECT_INSTRUMENTS) == 20
    assert all("USD" in instrument.split("_") for instrument in module.DIRECT_INSTRUMENTS)


def test_freeze_only_creates_contract_before_prices(tmp_path: Path) -> None:
    database = tmp_path / "fomc.sqlite3"
    result = module.run(database, tmp_path / "report", freeze_only=True)
    assert result["event_count"] == 21
    assert result["clock_count"] == 63
    assert result["instrument_count"] == 20
    assert result["execution_eligible"] is False
    connection = sqlite3.connect(database)
    assert connection.execute("SELECT count(*) FROM fomc_generalization_contracts").fetchone()[0] == 1
    assert connection.execute("SELECT count(*) FROM fomc_generalization_events").fetchone()[0] == 21
    assert connection.execute("SELECT count(*) FROM fomc_generalization_clocks").fetchone()[0] == 63
    assert connection.execute("SELECT count(*) FROM fomc_generalization_price_windows").fetchone()[0] == 0
    contract = connection.execute("SELECT contract_json FROM fomc_generalization_contracts").fetchone()[0]
    assert "frozen_before_any_fomc_price_query" in contract
    assert '"supported_execution_decision":"no_trade"' in contract
    connection.close()


def test_randomization_is_deterministic_and_directional() -> None:
    triples = [(5.0, -1.0, 0.0)] * 8
    first = module.monte_carlo_within_trio_pvalue(triples, draws=10_000, seed=7)
    second = module.monte_carlo_within_trio_pvalue(triples, draws=10_000, seed=7)
    assert first == second
    assert 0.0 < float(first["pvalue"]) < 0.05


def test_module_has_no_operational_or_order_surface() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    assert "requests" not in imported
    assert "subprocess" not in imported
    assert "api-fxtrade.oanda.com" not in source
    assert '"POST"' not in source
    assert "authorization_id" not in source
    assert "confirmed_candidate" not in source


def test_schedule_change_chain_rejects_tampering(monkeypatch: pytest.MonkeyPatch) -> None:
    changed = list(module.SCHEDULE)
    row = list(changed[5])
    row[3] = -0.25
    changed[5] = tuple(row)
    monkeypatch.setattr(module, "SCHEDULE", tuple(changed))
    with pytest.raises(RuntimeError, match="rate_change_inconsistent"):
        module.validate_schedule()
