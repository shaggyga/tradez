import datetime as dt
import json
from pathlib import Path
import sqlite3

import pytest

import oanda_official_event_pair_quote_capture_v1 as subject
import oanda_official_event_pair_quote_capture_v1_verifier as verifier


UTC = dt.timezone.utc
NONTRADEABLE = {"EUR_TRY", "TRY_JPY", "USD_TRY"}


def _observation(at: dt.datetime, *, observation_id: str = "obs-1") -> dict:
    return {
        "observation_id": observation_id,
        "source_id": "boc_press",
        "source_contract_id": "boc_press_placeholder_handoff_v2_20260902",
        "first_seen_utc": subject.iso_utc(at),
        "prospective_observation": True,
        "listing_bootstrap": False,
        "publisher_time_eligible": True,
        "raw_payload_json": '{"headline":"fixture"}',
        "collector_contract_id": subject.REQUIRED_RAW_COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": subject.REQUIRED_RAW_COLLECTOR_COHORT_ID,
    }


def _quotes(captured: dt.datetime) -> dict:
    quotes = {}
    for index, instrument in enumerate(subject.EXPECTED_INSTRUMENTS):
        bid = 1.0 + index / 10000.0
        quotes[instrument] = {
            "bid": bid,
            "ask": bid + 0.0002,
            "pip": 0.0001,
            "source": "stream",
            "time": subject.iso_utc(captured - dt.timedelta(seconds=1)),
            "tradeable": instrument not in NONTRADEABLE,
        }
    return {
        "schema_version": subject.REQUIRED_QUOTE_SNAPSHOT_SCHEMA_VERSION,
        "producer": subject.REQUIRED_QUOTE_SNAPSHOT_PRODUCER,
        "generated_utc": subject.iso_utc(captured - dt.timedelta(milliseconds=50)),
        "connection_generation": 7,
        "quote_count": len(quotes),
        "quotes": quotes,
        "coverage": {
            "connection_generation": 7,
            "current_quote_count": len(quotes),
            "last_known_quote_count": len(quotes),
            "retained_last_known_count": 0,
            "current_tradeable_quote_count": len(quotes) - len(NONTRADEABLE),
            "current_non_tradeable_quote_count": len(NONTRADEABLE),
        },
        "transport": {"source": "sqlite_wal", "sequence": 91},
    }


def test_config_and_safety_contract_are_frozen():
    payload = subject.validate_config()
    assert payload["contract_id"] == subject.CONTRACT_ID
    assert payload["cohort_id"] == subject.COHORT_ID
    assert payload["expected_instrument_count"] == 68
    assert subject.POLICY["research_only"] is True
    assert subject.POLICY["execution_eligible"] is False
    assert subject.POLICY["can_place_orders"] is False
    assert subject.POLICY["can_authorize"] is False
    assert subject.POLICY["can_promote"] is False


def test_nontradeable_exotics_do_not_erase_valid_pair_quotes():
    event = subject.ACTIVATED_UTC + dt.timedelta(seconds=1)
    captured = event + dt.timedelta(seconds=1)
    result = subject.build_capture(_observation(event), _quotes(captured), captured)
    assert result["timing_quality"] == "prospective_per_pair_executable_quotes"
    assert result["eligible_quote_count"] == 65
    assert result["explicitly_tradeable_quote_count"] == 65
    assert result["exact_all_68_available"] is False
    assert result["pair_rows"]["USD_CAD"]["eligible"] is True
    assert result["pair_rows"]["USD_TRY"]["eligible"] is False
    assert result["pair_rows"]["USD_TRY"]["invalid_reason"] == (
        "not_explicitly_tradeable"
    )
    assert result["invalid_reason_counts"] == {"not_explicitly_tradeable": 3}


def test_one_stale_pair_does_not_erase_unrelated_pairs():
    event = subject.ACTIVATED_UTC + dt.timedelta(seconds=2)
    captured = event + dt.timedelta(seconds=1)
    payload = _quotes(captured)
    payload["quotes"]["EUR_DKK"]["time"] = subject.iso_utc(
        captured - dt.timedelta(seconds=31)
    )
    result = subject.build_capture(_observation(event), payload, captured)
    assert result["eligible_quote_count"] == 64
    assert result["pair_rows"]["EUR_DKK"]["invalid_reason"] == "stale_quote"
    assert result["pair_rows"]["EUR_USD"]["eligible"] is True
    assert result["pair_rows"]["USD_CAD"]["eligible"] is True


def test_late_detection_is_terminal_snapshot_level_failure():
    event = subject.ACTIVATED_UTC + dt.timedelta(seconds=3)
    captured = event + dt.timedelta(
        seconds=subject.MAXIMUM_DETECTION_LATENCY_SECONDS + 0.1
    )
    result = subject.build_capture(_observation(event), _quotes(captured), captured)
    assert result["timing_quality"] == "prospective_pair_quote_snapshot_invalid"
    assert result["eligible_quote_count"] == 0
    assert any(
        reason.startswith("detection_latency_seconds:")
        for reason in result["metadata_invalid_reasons"]
    )


def test_stale_snapshot_invalidates_bundle_without_hiding_pair_audit_rows():
    event = subject.ACTIVATED_UTC + dt.timedelta(seconds=4)
    captured = event + dt.timedelta(seconds=1)
    payload = _quotes(captured)
    payload["generated_utc"] = subject.iso_utc(
        captured
        - dt.timedelta(seconds=subject.MAXIMUM_SNAPSHOT_AGE_SECONDS + 0.1)
    )
    result = subject.build_capture(_observation(event), payload, captured)
    assert result["timing_quality"] == "prospective_pair_quote_snapshot_invalid"
    assert result["eligible_quote_count"] == 0
    assert len(result["pair_rows"]) == 68


def test_capture_ledger_is_append_only(tmp_path: Path):
    event = subject.ACTIVATED_UTC + dt.timedelta(seconds=5)
    captured = event + dt.timedelta(seconds=1)
    result = subject.build_capture(_observation(event), _quotes(captured), captured)
    database = tmp_path / "capture.sqlite"
    connection = subject.open_output_database(database)
    try:
        assert subject.insert_capture(connection, result) is True
        connection.commit()
        assert subject.insert_capture(connection, result) is False
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            connection.execute(
                "UPDATE official_event_pair_quote_capture "
                "SET timing_quality='rewritten' WHERE observation_id='obs-1'"
            )
        connection.rollback()
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            connection.execute(
                "DELETE FROM official_event_pair_quote_capture "
                "WHERE observation_id='obs-1'"
            )
    finally:
        connection.close()


def _raw_input_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            CREATE TABLE official_release_observation (
              observation_id TEXT PRIMARY KEY,
              source_id TEXT,
              source_contract_id TEXT,
              first_seen_utc TEXT,
              prospective_observation INTEGER,
              listing_bootstrap INTEGER,
              publisher_time_eligible INTEGER,
              raw_payload_json TEXT,
              collector_contract_id TEXT,
              collector_cohort_id TEXT
            )
            """
        )
        for index, (clock, contract, prospective) in enumerate(
            (
                (
                    subject.ACTIVATED_UTC - dt.timedelta(seconds=1),
                    subject.REQUIRED_RAW_COLLECTOR_CONTRACT_ID,
                    1,
                ),
                (
                    subject.ACTIVATED_UTC + dt.timedelta(seconds=1),
                    "foreign-contract",
                    1,
                ),
                (
                    subject.ACTIVATED_UTC + dt.timedelta(seconds=2),
                    subject.REQUIRED_RAW_COLLECTOR_CONTRACT_ID,
                    0,
                ),
                (
                    subject.ACTIVATED_UTC + dt.timedelta(seconds=3),
                    subject.REQUIRED_RAW_COLLECTOR_CONTRACT_ID,
                    1,
                ),
            )
        ):
            connection.execute(
                "INSERT INTO official_release_observation VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    f"obs-{index}",
                    "boc_press",
                    "source-contract",
                    subject.iso_utc(clock),
                    prospective,
                    0,
                    1,
                    json.dumps({"row": index}),
                    contract,
                    subject.REQUIRED_RAW_COLLECTOR_COHORT_ID,
                ),
            )
        connection.commit()
    finally:
        connection.close()


def test_pending_reader_admits_only_new_current_prospective_rows(tmp_path: Path):
    database = tmp_path / "raw.sqlite"
    _raw_input_database(database)
    rows = subject.read_pending_observations(database, set())
    assert [row["observation_id"] for row in rows] == ["obs-3"]
    assert subject.read_pending_observations(database, {"obs-3"}) == []


def test_graph_components_are_deterministic_and_connected():
    components = subject._graph_components(["EUR_USD", "USD_JPY", "AUD_NZD"])
    assert components == [["EUR", "JPY", "USD"], ["AUD", "NZD"]]


def test_supervisor_runs_capture_quietly_at_one_second_cadence():
    supervisor = (subject.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    assert '-Name "official_event_pair_quote_capture_v1"' in supervisor
    assert '-Needle "oanda_official_event_pair_quote_capture_v1.py"' in supervisor
    block = supervisor.split(
        '-Name "official_event_pair_quote_capture_v1"', 1
    )[1].split("# Complete the event-clock market path", 1)[0]
    assert '"--interval-sec", "1"' in block
    assert '"--quiet"' in block
    assert "official_event_pair_quote_capture_heartbeat_v1.json" in block
    assert '-Name "official_event_pair_quote_capture_v1_verifier"' in block
    assert (
        '-Needle "oanda_official_event_pair_quote_capture_v1_verifier.py"'
        in block
    )
    assert "official_event_pair_quote_capture_verifier_heartbeat_v1.json" in block


def _one_raw_row_database(path: Path, observation: dict) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            CREATE TABLE official_release_observation (
              observation_id TEXT PRIMARY KEY,
              source_id TEXT,
              source_contract_id TEXT,
              first_seen_utc TEXT,
              prospective_observation INTEGER,
              listing_bootstrap INTEGER,
              publisher_time_eligible INTEGER,
              raw_payload_json TEXT,
              collector_contract_id TEXT,
              collector_cohort_id TEXT
            )
            """
        )
        connection.execute(
            "INSERT INTO official_release_observation VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                observation["observation_id"],
                observation["source_id"],
                observation["source_contract_id"],
                observation["first_seen_utc"],
                1,
                0,
                1,
                observation["raw_payload_json"],
                observation["collector_contract_id"],
                observation["collector_cohort_id"],
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _producer_state(path: Path, database: Path, now: dt.datetime) -> None:
    connection = sqlite3.connect(database)
    try:
        counts = subject.diagnostics(connection)
    finally:
        connection.close()
    path.write_text(
        json.dumps(
            {
                "schema_version": subject.SCHEMA_VERSION,
                "contract_id": subject.CONTRACT_ID,
                "cohort_id": subject.COHORT_ID,
                "activated_utc": subject.iso_utc(subject.ACTIVATED_UTC),
                "generated_utc": subject.iso_utc(now),
                "status": "ok",
                "error": "",
                "counts": counts,
                "sqlite_integrity": "ok",
                "policy": dict(subject.POLICY),
            }
        ),
        encoding="utf-8",
    )


def test_independent_verifier_rebuilds_identity_lineage_and_counts(tmp_path: Path):
    event = subject.ACTIVATED_UTC + dt.timedelta(seconds=10)
    captured = event + dt.timedelta(seconds=1)
    observation = _observation(event)
    raw_database = tmp_path / "raw.sqlite"
    capture_database = tmp_path / "capture.sqlite"
    state_path = tmp_path / "state.json"
    _one_raw_row_database(raw_database, observation)
    capture = subject.build_capture(observation, _quotes(captured), captured)
    connection = subject.open_output_database(capture_database)
    try:
        assert subject.insert_capture(connection, capture) is True
        connection.commit()
    finally:
        connection.close()
    _producer_state(state_path, capture_database, captured)
    result = verifier.verify(
        raw_database=raw_database,
        capture_database=capture_database,
        capture_state_path=state_path,
        now_utc=captured,
    )
    assert result["verified"] is True
    assert result["failure_count"] == 0
    assert result["counts"] == {
        "capture_count": 1,
        "per_pair_ready_count": 1,
        "exact_all_68_count": 0,
        "eligible_pair_quote_total": 65,
    }


def test_independent_verifier_detects_row_payload_divergence(tmp_path: Path):
    event = subject.ACTIVATED_UTC + dt.timedelta(seconds=20)
    captured = event + dt.timedelta(seconds=1)
    observation = _observation(event)
    raw_database = tmp_path / "raw.sqlite"
    capture_database = tmp_path / "capture.sqlite"
    state_path = tmp_path / "state.json"
    _one_raw_row_database(raw_database, observation)
    capture = subject.build_capture(observation, _quotes(captured), captured)
    connection = subject.open_output_database(capture_database)
    try:
        subject.insert_capture(connection, capture)
        connection.commit()
        connection.execute("DROP TRIGGER pair_quote_capture_no_update")
        connection.execute(
            "UPDATE official_event_pair_quote_capture SET eligible_quote_count=64"
        )
        connection.commit()
    finally:
        connection.close()
    _producer_state(state_path, capture_database, captured)
    result = verifier.verify(
        raw_database=raw_database,
        capture_database=capture_database,
        capture_state_path=state_path,
        now_utc=captured,
    )
    assert result["verified"] is False
    assert "append_only_trigger_mismatch" in result["failures"]
    assert any("eligible_quote_count_mismatch" in row for row in result["failures"])
