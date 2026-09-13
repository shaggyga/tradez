import datetime as dt
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

import oanda_scheduled_event_factor_reaction_v1 as reaction


UTC = dt.timezone.utc
SCHEDULED = dt.datetime(2026, 9, 2, 9, 45, tzinfo=UTC)


def quote(mid: float, *, pip: float = 0.0001, spread_pips: float = 1.0) -> dict:
    half = spread_pips * pip / 2.0
    return {
        "bid": mid - half,
        "ask": mid + half,
        "pip": pip,
        "broker_price_time_utc": reaction.iso(SCHEDULED),
    }


def capture(horizon: int, quotes: dict, *, ledger_hash: str = "a" * 64) -> dict:
    completed = SCHEDULED + dt.timedelta(minutes=horizon, seconds=1)
    return {
        "capture_id": f"capture-{horizon}",
        "contract_id": reaction.INPUT_CONTRACT_ID,
        "cohort_id": reaction.INPUT_COHORT_ID,
        "timing_quality": reaction.VALID_TIMING_QUALITY,
        "invalid_reason": "",
        "retrieval_completed_utc": reaction.iso(completed),
        "response_time_utc": reaction.iso(completed),
        "direct_event_tradeable_quotes": quotes,
        "tradeable_quotes": quotes,
        "_ledger_payload_sha256": ledger_hash,
        "_computed_payload_sha256": ledger_hash,
    }


def clock() -> dict:
    return {
        "clock_id": "future-boc",
        "event_id": "boc-rate-decision",
        "event_series_id": "boc_policy_decision",
        "headline": "Bank of Canada policy decision",
        "scheduled_utc": reaction.iso(SCHEDULED),
        "driver_currency": "NZD",
    }


def weakening_captures() -> tuple[dict, dict]:
    start = {
        "AUD_NZD": quote(1.1000, spread_pips=0.5),
        "EUR_NZD": quote(1.9000, spread_pips=1.5),
        "NZD_CAD": quote(0.8200, spread_pips=1.2),
        "NZD_USD": quote(0.6100, spread_pips=1.0),
    }
    end = {
        "AUD_NZD": quote(1.1011, spread_pips=0.5),
        "EUR_NZD": quote(1.9019, spread_pips=1.5),
        "NZD_CAD": quote(0.8191, spread_pips=1.2),
        "NZD_USD": quote(0.6093, spread_pips=1.0),
    }
    return capture(0, start), capture(1, end)


def test_all_legs_agree_and_lowest_cost_pair_is_selected() -> None:
    zero, one = weakening_captures()
    result = reaction.evaluate_reaction(
        clock(), zero, one,
        observed=SCHEDULED + dt.timedelta(minutes=1, seconds=2),
    )

    assert result["status"] == "forecast"
    assert result["factor_direction"] == "weaken"
    assert result["confirming_pair_count"] == 4
    assert result["selected"]["instrument"] == "AUD_NZD"
    assert result["side"] == "long"
    assert result["decision_utc"] == reaction.iso(
        SCHEDULED + dt.timedelta(minutes=1, seconds=1)
    )
    assert result["processing_lag_sec"] == 1.0
    assert result["event_clock_assigns_no_direction"] is True
    assert result["execution_eligible"] is False


def test_split_factor_rejects_instead_of_inventing_direction() -> None:
    start = {
        "AUD_NZD": quote(1.1000),
        "EUR_NZD": quote(1.9000),
        "NZD_CAD": quote(0.8200),
        "NZD_USD": quote(0.6100),
    }
    split = {
        "AUD_NZD": quote(1.1011),
        "EUR_NZD": quote(1.8981),
        "NZD_CAD": quote(0.8191),
        "NZD_USD": quote(0.6107),
    }
    result = reaction.evaluate_reaction(
        clock(), capture(0, start), capture(1, split),
        observed=SCHEDULED + dt.timedelta(minutes=1, seconds=2),
    )

    assert result["status"] == "rejected"
    assert result["reason"] in {
        "factor_move_below_threshold",
        "insufficient_cross_pair_confirmation",
    }


def test_late_processing_and_payload_hash_mismatch_are_fail_closed() -> None:
    zero, one = weakening_captures()
    late = reaction.evaluate_reaction(
        clock(), zero, one,
        observed=SCHEDULED + dt.timedelta(minutes=1, seconds=32),
    )
    assert late["reason"] == "decision_processing_clock_too_late"

    one["_computed_payload_sha256"] = "b" * 64
    mismatch = reaction.evaluate_reaction(
        clock(), zero, one,
        observed=SCHEDULED + dt.timedelta(minutes=1, seconds=2),
    )
    assert mismatch["reason"] == "input_payload_hash_mismatch_h1"


def test_forecast_and_executable_outcome_ledgers_are_append_only(tmp_path: Path) -> None:
    zero, one = weakening_captures()
    decision = reaction.evaluate_reaction(
        clock(), zero, one,
        observed=SCHEDULED + dt.timedelta(minutes=1, seconds=2),
    )
    connection = reaction.open_database(tmp_path / "reaction.sqlite")
    inserted_decisions, inserted_forecasts = reaction.persist_decision(
        connection, decision
    )
    assert (inserted_decisions, inserted_forecasts) == (1, 4)

    selected = decision["selected"]["instrument"]
    horizon_captures = {0: zero, 1: one}
    for horizon in reaction.OUTCOME_HORIZONS_MIN:
        exit_quote = quote(1.1021 + horizon * 0.00001, spread_pips=0.5)
        horizon_captures[horizon] = capture(horizon, {selected: exit_quote})
    matured = reaction.mature_outcomes(
        connection, [{"clock_id": clock()["clock_id"], "captures": horizon_captures}]
    )
    assert matured == 4
    rows = connection.execute(
        "SELECT executable_net_pips,payload_json,payload_sha256 FROM outcomes "
        "ORDER BY horizon_min"
    ).fetchall()
    assert len(rows) == 4
    assert all(row[0] > 0.0 for row in rows)
    assert all(
        hashlib.sha256(row[1].encode("utf-8")).hexdigest() == row[2]
        for row in rows
    )
    with pytest.raises(sqlite3.IntegrityError, match="append_only"):
        connection.execute("UPDATE outcomes SET executable_net_pips=999")
    connection.close()


def test_main_source_is_research_only_and_has_no_order_path() -> None:
    source = Path(reaction.__file__).read_text(encoding="utf-8")
    assert "place_order(" not in source
    assert "client.write(" not in source
    assert "--execute" not in source
    assert '"can_place_orders": False' in source
    assert '"can_authorize": False' in source
    assert '"can_promote": False' in source


def test_frozen_config_pins_source_and_all_safety_contracts(tmp_path: Path) -> None:
    config = reaction.validate_frozen_config()
    assert config["cohort_id"] == reaction.COHORT_ID
    assert config["validated_source_sha256"] == reaction.sha256_file(
        Path(reaction.__file__)
    )
    assert config["research_only"] is True
    assert config["execution_eligible"] is False
    assert config["can_trade"] is False

    source = tmp_path / "changed.py"
    source.write_text("# changed after cohort activation\n", encoding="utf-8")
    with pytest.raises(ValueError, match="source hash mismatch"):
        reaction.validate_frozen_config(source_path=source)


def test_hidden_supervisor_runs_prospective_reaction_as_research_only() -> None:
    supervisor = (reaction.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    block = supervisor.split('-Name "scheduled_event_factor_reaction_v1"', 1)[1]
    block = block.split(
        '-Name "official_event_quote_horizon_capture_verifier_v1"', 1
    )[0]
    assert "oanda_scheduled_event_factor_reaction_v1.py" in block
    assert "scheduled_event_factor_reaction_v1_20260902b.json" in block
    assert '"--interval-sec", "2"' in block
    assert "--account-key" not in block
    assert "--execute" not in block
    assert 'ExpectedJsonValue = "scheduled_event_factor_reaction_v1"' in block
