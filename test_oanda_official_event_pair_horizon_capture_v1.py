import datetime as dt
import json
from pathlib import Path
import sqlite3

import pytest

import oanda_official_event_pair_horizon_capture_v1 as subject
import oanda_official_event_pair_horizon_capture_v1_verifier as verifier
import oanda_official_event_pair_quote_capture_v1 as entry_source


UTC = dt.timezone.utc


def _snapshot(now: dt.datetime, *, stale_instrument: str = "") -> dict:
    quotes = {}
    for index, instrument in enumerate(entry_source.EXPECTED_INSTRUMENTS):
        tick_time = now
        if instrument == stale_instrument:
            tick_time -= dt.timedelta(seconds=40)
        mid = 1.0 + index / 10_000
        quotes[instrument] = {
            "bid": mid,
            "ask": mid + 0.0002,
            "pip": 0.01 if instrument.endswith("_JPY") else 0.0001,
            "time": tick_time.isoformat(),
            "source": "stream",
            "tradeable": True,
        }
    return {
        "schema_version": 3,
        "producer": subject.SNAPSHOT_PRODUCER,
        "generated_utc": now.isoformat(),
        "quote_count": 68,
        "connection_generation": 9,
        "coverage": {
            "current_quote_count": 68,
            "connection_generation": 9,
        },
        "quotes": quotes,
    }


def _entry(event: dt.datetime, instruments=("EUR_USD", "USD_JPY")) -> dict:
    snap = _snapshot(event)
    eligible = {}
    for instrument in instruments:
        row = snap["quotes"][instrument]
        eligible[instrument] = {
            "bid": row["bid"],
            "ask": row["ask"],
            "pip": row["pip"],
            "quote_time_utc": event.isoformat(),
            "tradeable": True,
            "source": "stream",
        }
    return {
        "capture_id": "entry-1",
        "observation_id": "observation-1",
        "source_id": "official-test",
        "event_first_known_utc": event.isoformat(),
        "eligible_quotes": eligible,
    }


def _input_db(path: Path, entries: list[dict]) -> None:
    connection = sqlite3.connect(path)
    connection.execute(
        """CREATE TABLE official_event_pair_quote_capture (
        capture_id TEXT PRIMARY KEY, observation_id TEXT, source_id TEXT,
        event_first_known_utc TEXT, capture_payload_json TEXT,
        contract_id TEXT, cohort_id TEXT, research_only INTEGER)"""
    )
    for entry in entries:
        connection.execute(
            "INSERT INTO official_event_pair_quote_capture VALUES (?,?,?,?,?,?,?,1)",
            (
                entry["capture_id"], entry["observation_id"], entry["source_id"],
                entry["event_first_known_utc"], subject.canonical_json(entry),
                subject.ENTRY_CONTRACT_ID, subject.ENTRY_COHORT_ID,
            ),
        )
    connection.commit()
    connection.close()


def test_frozen_config_and_fail_closed_policy():
    payload = subject.validate_frozen_config()
    assert payload["activated_utc"] == subject.iso_utc(subject.ACTIVATED_UTC)
    assert subject.POLICY["supported_execution_decision"] == "no_trade"
    assert subject.POLICY["can_authorize"] is False
    assert subject.POLICY["can_place_orders"] is False


def test_valid_pairs_get_independent_executable_outcomes():
    event = subject.ACTIVATED_UTC + dt.timedelta(minutes=1)
    now = event + dt.timedelta(minutes=5)
    snapshot = _snapshot(now)
    header, outcomes = subject.build_attempt(_entry(event), 5, snapshot, now)
    assert header["valid_pair_count"] == 2
    assert header["invalid_pair_count"] == 0
    assert {row["instrument"] for row in outcomes} == {"EUR_USD", "USD_JPY"}
    for row in outcomes:
        assert row["valid"] is True
        assert row["buy_executable_pips"] is not None
        assert row["sell_executable_pips"] is not None
        assert len(row["cost_stress"]) == 3
        assert row["semantic_direction_claim"] == "none_both_directions_recorded"


def test_stale_pair_does_not_erase_other_pair():
    event = subject.ACTIVATED_UTC + dt.timedelta(minutes=1)
    now = event + dt.timedelta(minutes=5)
    snapshot = _snapshot(now, stale_instrument="USD_JPY")
    header, outcomes = subject.build_attempt(_entry(event), 5, snapshot, now)
    by_pair = {row["instrument"]: row for row in outcomes}
    assert by_pair["EUR_USD"]["valid"] is True
    assert by_pair["USD_JPY"]["valid"] is False
    assert by_pair["USD_JPY"]["invalid_reason"] == "exit_quote_stale"
    assert header["valid_pair_count"] == 1
    assert header["invalid_pair_count"] == 1


def test_endpoint_arithmetic_embeds_spread_once():
    event = subject.ACTIVATED_UTC + dt.timedelta(minutes=1)
    now = event + dt.timedelta(minutes=1)
    entry = _entry(event, ("EUR_USD",))
    entry["eligible_quotes"]["EUR_USD"].update(bid=1.1000, ask=1.1002, pip=0.0001)
    snapshot = _snapshot(now)
    snapshot["quotes"]["EUR_USD"].update(bid=1.1005, ask=1.1007, pip=0.0001)
    _, outcomes = subject.build_attempt(entry, 1, snapshot, now)
    row = outcomes[0]
    assert row["signed_mid_move_pips"] == pytest.approx(5.0)
    assert row["buy_executable_pips"] == pytest.approx(3.0)
    assert row["sell_executable_pips"] == pytest.approx(-7.0)
    assert row["cost_stress"][1]["buy_after_slippage_pips"] == pytest.approx(2.75)


def test_preactivation_capture_is_never_backfilled(tmp_path: Path):
    old = _entry(subject.ACTIVATED_UTC - dt.timedelta(minutes=10))
    input_db = tmp_path / "input.sqlite"
    output_db = tmp_path / "output.sqlite"
    _input_db(input_db, [old])
    output = subject.open_database(output_db)
    source = sqlite3.connect(input_db)
    try:
        result = subject.capture_due(
            output, source, _snapshot(subject.ACTIVATED_UTC),
            subject.ACTIVATED_UTC + dt.timedelta(hours=2),
        )
        assert result["preactivation_excluded"] == 1
        assert result["inserted_attempts"] == 0
    finally:
        source.close()
        output.close()


def test_due_attempt_is_terminal_and_deduplicated(tmp_path: Path):
    event = subject.ACTIVATED_UTC + dt.timedelta(minutes=1)
    entry = _entry(event)
    input_db = tmp_path / "input.sqlite"
    output_db = tmp_path / "output.sqlite"
    _input_db(input_db, [entry])
    output = subject.open_database(output_db)
    source = sqlite3.connect(input_db)
    now = event + dt.timedelta(minutes=1)
    try:
        first = subject.capture_due(output, source, _snapshot(now), now)
        second = subject.capture_due(output, source, _snapshot(now), now)
        assert first["inserted_attempts"] == 1
        assert first["inserted_outcomes"] == 2
        assert second["inserted_attempts"] == 0
        assert second["duplicates"] == 1
        assert subject.census(output)["attempts"] == 1
    finally:
        source.close()
        output.close()


def test_append_only_triggers_block_changes(tmp_path: Path):
    database = tmp_path / "output.sqlite"
    connection = subject.open_database(database)
    names = {
        row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger'"
        )
    }
    assert {
        "pair_horizon_attempt_no_update", "pair_horizon_attempt_no_delete",
        "pair_horizon_outcome_no_update", "pair_horizon_outcome_no_delete",
    } <= names
    connection.close()


def test_independent_verifier_accepts_valid_fixture(tmp_path: Path):
    event = subject.ACTIVATED_UTC + dt.timedelta(minutes=1)
    entry = _entry(event)
    input_db = tmp_path / "input.sqlite"
    output_db = tmp_path / "output.sqlite"
    state = tmp_path / "state.json"
    _input_db(input_db, [entry])
    output = subject.open_database(output_db)
    source = sqlite3.connect(input_db)
    now = event + dt.timedelta(minutes=1)
    try:
        subject.capture_due(output, source, _snapshot(now), now)
        counts = subject.census(output)
    finally:
        source.close()
        output.close()
    state.write_text(json.dumps({
        "contract_id": subject.CONTRACT_ID,
        "counts": counts,
    }), encoding="utf-8")
    result = verifier.verify(
        input_database=input_db, output_database=output_db,
        producer_state=state, observed_utc=now,
    )
    assert result["verified"] is True, result["failures"]
    assert result["counts"]["valid_outcomes"] == 2


def test_supervisor_and_vault_wiring_present():
    supervisor = (subject.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    vault = (subject.ROOT / "forex_model_vault_sync.py").read_text(encoding="utf-8")
    assert 'oanda_official_event_pair_horizon_capture_v1.py' in supervisor
    assert 'oanda_official_event_pair_horizon_capture_v1_verifier.py' in supervisor
    assert 'oanda_official_event_pair_horizon_capture_v1.py' in vault
    assert 'official_event_pair_horizon_capture_v1.json' in vault
