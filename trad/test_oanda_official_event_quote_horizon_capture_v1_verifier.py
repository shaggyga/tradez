from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

import oanda_official_event_quote_horizon_capture_v1 as producer
import oanda_official_event_quote_horizon_capture_v1_verifier as verifier
import oanda_official_release_fast_lane as fast_lane


UTC = dt.timezone.utc


def _all68_quote_payload(at: dt.datetime) -> dict:
    quotes = {
        instrument: {
            "bid": 150.0 if instrument.endswith("_JPY") else 1.0,
            "ask": 150.01 if instrument.endswith("_JPY") else 1.0002,
            "pip": 0.01 if instrument.endswith("_JPY") else 0.0001,
            "time": fast_lane.iso_utc(at),
            "source": "practice_007_fast_executor_price_stream",
        }
        for instrument in fast_lane.EXPECTED_QUOTE_INSTRUMENTS
    }
    return {
        "schema_version": 2,
        "generated_utc": fast_lane.iso_utc(at),
        "producer": "practice_007_fast_executor_price_stream",
        "connection_generation": 41,
        "quote_count": 68,
        "quotes": quotes,
        "coverage": {
            "current_quote_count": 68,
            "last_known_quote_count": 68,
            "retained_last_known_count": 0,
            "retained_last_known_instruments": [],
            "connection_generation": 41,
            "retained_quotes_execution_eligible": False,
        },
        "transport": {"source": "sqlite_wal", "sequence": 9100},
        "research_only": True,
    }


def _seed_exact_source(
    monkeypatch: pytest.MonkeyPatch,
    input_path: Path,
) -> dt.datetime:
    monkeypatch.setattr(
        fast_lane.news,
        "prospective_clock_attestation",
        lambda clock: bool(clock.get("attested")),
    )
    event_time = max(
        fast_lane.QUOTE_CAPTURE_ACTIVATED_UTC,
        producer.ACTIVATED_UTC,
    ) + dt.timedelta(minutes=1)
    connection = fast_lane.open_database(input_path)
    try:
        inserted, duplicates, _ = fast_lane.append_observations(
            connection,
            source={
                "source_id": "fed_monetary_policy",
                "source_contract_id": "fed-test-v1",
                "source_cohort_id": "fed-test-v1",
            },
            rows=[
                {
                    "headline": "Prospective official policy decision",
                    "source_url": "https://www.federalreserve.gov/test-event",
                    "published_utc": fast_lane.iso_utc(event_time),
                    "source_direct": True,
                    "source_verified": True,
                }
            ],
            first_seen=event_time,
            listing_bootstrap=False,
            observation_clock={"attested": True, "source": "test_clock"},
            quote_snapshot_loader=lambda: _all68_quote_payload(
                event_time + dt.timedelta(seconds=3)
            ),
            quote_captured_utc=event_time + dt.timedelta(seconds=3),
        )
        assert (inserted, duplicates) == (1, 0)
    finally:
        connection.close()
    return event_time


def _capture_first_horizon(
    input_path: Path,
    output_path: Path,
    event_time: dt.datetime,
) -> dt.datetime:
    attempted = event_time + dt.timedelta(minutes=1, seconds=3)
    source = fast_lane.open_database(input_path)
    output = producer.open_database(output_path)
    try:
        result = producer.capture_due_horizons(
            output,
            source,
            _all68_quote_payload(attempted),
            attempted,
        )
        output.commit()
        assert result["exact"] == 1
    finally:
        output.close()
        source.close()
    return attempted


def _verify(
    input_path: Path,
    output_path: Path,
    observed_utc: dt.datetime,
    config_path: Path = verifier.CONFIG_PATH,
) -> dict:
    return verifier.verify_horizon_capture_database(
        input_database=input_path,
        output_database=output_path,
        config_path=config_path,
        observed_utc=observed_utc,
    )


def test_clean_exact_capture_verifies_independently(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "source.sqlite"
    output_path = tmp_path / "horizons.sqlite"
    event_time = _seed_exact_source(monkeypatch, input_path)
    _capture_first_horizon(input_path, output_path, event_time)

    result = _verify(
        input_path,
        output_path,
        event_time + dt.timedelta(minutes=1, seconds=21),
    )

    assert result["verified"] is True
    assert result["status"] == "verified"
    assert result["failures"] == []
    assert result["counts"]["horizon_attempts"] == 1
    assert result["counts"]["exact_horizon_attempts"] == 1
    assert result["counts"]["quote_components"] == 68
    assert result["counts"]["due_event_horizons"] == 1
    assert result["counts"]["due_missing_event_horizons"] == 0


def test_tampered_quote_row_fails_payload_and_arithmetic_checks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "source.sqlite"
    output_path = tmp_path / "horizons.sqlite"
    event_time = _seed_exact_source(monkeypatch, input_path)
    _capture_first_horizon(input_path, output_path, event_time)
    connection = sqlite3.connect(output_path)
    try:
        connection.execute("DROP TRIGGER horizon_quote_no_update")
        connection.execute(
            """
            UPDATE official_event_horizon_quote
               SET bid=bid+(pip/2.0)
             WHERE quote_id=(SELECT quote_id FROM official_event_horizon_quote LIMIT 1)
            """
        )
        connection.commit()
    finally:
        connection.close()

    result = _verify(
        input_path,
        output_path,
        event_time + dt.timedelta(minutes=1, seconds=21),
    )
    assert result["verified"] is False
    assert any("payload_column_mismatch:bid" in item for item in result["failures"])
    assert any("spread_pips_invalid" in item for item in result["failures"])
    assert any("horizon_quote_no_update:missing" in item for item in result["failures"])


def test_self_consistent_claimed_snapshot_hash_tamper_is_recomputed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "source.sqlite"
    output_path = tmp_path / "horizons.sqlite"
    event_time = _seed_exact_source(monkeypatch, input_path)
    _capture_first_horizon(input_path, output_path, event_time)
    connection = sqlite3.connect(output_path)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("DROP TRIGGER horizon_capture_no_update")
        row = connection.execute(
            "SELECT capture_id,payload_json FROM official_event_horizon_capture"
        ).fetchone()
        payload = json.loads(str(row["payload_json"]))
        forged_hash = "0" * 64
        payload["quote_snapshot_sha256"] = forged_hash
        payload_json = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), default=str
        )
        payload_sha256 = hashlib.sha256(payload_json.encode()).hexdigest()
        connection.execute(
            """
            UPDATE official_event_horizon_capture
               SET quote_snapshot_sha256=?,payload_json=?,payload_sha256=?
             WHERE capture_id=?
            """,
            (forged_hash, payload_json, payload_sha256, row["capture_id"]),
        )
        connection.commit()
    finally:
        connection.close()

    result = _verify(
        input_path,
        output_path,
        event_time + dt.timedelta(minutes=1, seconds=21),
    )
    assert result["verified"] is False
    assert any(
        "quote_snapshot_material_sha_mismatch" in item
        for item in result["failures"]
    )


def test_component_generation_must_equal_header_generation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "source.sqlite"
    output_path = tmp_path / "horizons.sqlite"
    event_time = _seed_exact_source(monkeypatch, input_path)
    _capture_first_horizon(input_path, output_path, event_time)
    connection = sqlite3.connect(output_path)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("DROP TRIGGER horizon_quote_no_update")
        row = connection.execute(
            "SELECT quote_id,payload_json FROM official_event_horizon_quote LIMIT 1"
        ).fetchone()
        payload = json.loads(str(row["payload_json"]))
        payload["connection_generation"] = 999
        payload_json = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), default=str
        )
        payload_sha256 = hashlib.sha256(payload_json.encode()).hexdigest()
        quote_id = "official_event_horizon_quote_" + hashlib.sha256(
            f"{payload.get('capture_id', '')}|x".encode()
        ).hexdigest()[:32]
        # The quote identity and roots also become inconsistent; the dedicated
        # relational failure must still be present independently.
        connection.execute(
            """
            UPDATE official_event_horizon_quote
               SET connection_generation=?,payload_json=?,payload_sha256=?,quote_id=?
             WHERE quote_id=?
            """,
            (999, payload_json, payload_sha256, quote_id, row["quote_id"]),
        )
        connection.commit()
    finally:
        connection.close()

    result = _verify(
        input_path,
        output_path,
        event_time + dt.timedelta(minutes=1, seconds=21),
    )
    assert result["verified"] is False
    assert any("header_generation_mismatch" in item for item in result["failures"])


def test_omitted_exact_quote_component_fails_exact_68_and_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "source.sqlite"
    output_path = tmp_path / "horizons.sqlite"
    event_time = _seed_exact_source(monkeypatch, input_path)
    _capture_first_horizon(input_path, output_path, event_time)
    connection = sqlite3.connect(output_path)
    try:
        connection.execute("DROP TRIGGER horizon_quote_no_delete")
        connection.execute(
            """
            DELETE FROM official_event_horizon_quote
             WHERE quote_id=(SELECT quote_id FROM official_event_horizon_quote LIMIT 1)
            """
        )
        connection.commit()
    finally:
        connection.close()

    result = _verify(
        input_path,
        output_path,
        event_time + dt.timedelta(minutes=1, seconds=21),
    )
    assert result["verified"] is False
    assert any("exact_component_count_invalid" in item for item in result["failures"])
    assert any("component_root_sha_mismatch" in item for item in result["failures"])


def test_unsafe_header_flag_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "source.sqlite"
    output_path = tmp_path / "horizons.sqlite"
    event_time = _seed_exact_source(monkeypatch, input_path)
    _capture_first_horizon(input_path, output_path, event_time)
    connection = sqlite3.connect(output_path)
    try:
        connection.execute("DROP TRIGGER horizon_capture_no_update")
        connection.execute("PRAGMA ignore_check_constraints=ON")
        connection.execute(
            "UPDATE official_event_horizon_capture SET can_place_orders=1"
        )
        connection.commit()
    finally:
        connection.close()

    result = _verify(
        input_path,
        output_path,
        event_time + dt.timedelta(minutes=1, seconds=21),
    )
    assert result["verified"] is False
    assert any("can_place_orders:invalid" in item for item in result["failures"])


def test_wrong_input_capture_link_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "source.sqlite"
    output_path = tmp_path / "horizons.sqlite"
    event_time = _seed_exact_source(monkeypatch, input_path)
    _capture_first_horizon(input_path, output_path, event_time)
    connection = sqlite3.connect(output_path)
    try:
        connection.execute("DROP TRIGGER horizon_capture_no_update")
        connection.execute(
            "UPDATE official_event_horizon_capture "
            "SET input_capture_id='forged-input-link'"
        )
        connection.commit()
    finally:
        connection.close()

    result = _verify(
        input_path,
        output_path,
        event_time + dt.timedelta(minutes=1, seconds=21),
    )
    assert result["verified"] is False
    assert any("input_capture_link_missing" in item for item in result["failures"])
    assert any("capture_identity_invalid" in item for item in result["failures"])


def test_capture_read_start_after_completion_is_rejected_even_if_rehashed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "source.sqlite"
    output_path = tmp_path / "horizons.sqlite"
    event_time = _seed_exact_source(monkeypatch, input_path)
    attempted = _capture_first_horizon(input_path, output_path, event_time)
    forged_start = fast_lane.iso_utc(attempted + dt.timedelta(seconds=1))
    connection = sqlite3.connect(output_path)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("DROP TRIGGER horizon_capture_no_update")
        row = connection.execute(
            "SELECT capture_id,payload_json FROM official_event_horizon_capture"
        ).fetchone()
        payload = json.loads(str(row["payload_json"]))
        payload["capture_read_started_utc"] = forged_start
        payload_json = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        payload_sha256 = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        connection.execute(
            """
            UPDATE official_event_horizon_capture
               SET capture_read_started_utc=?,payload_json=?,payload_sha256=?
             WHERE capture_id=?
            """,
            (forged_start, payload_json, payload_sha256, row["capture_id"]),
        )
        connection.commit()
    finally:
        connection.close()

    result = _verify(
        input_path,
        output_path,
        event_time + dt.timedelta(minutes=1, seconds=21),
    )
    assert result["verified"] is False
    assert any(
        "capture_read_start_after_attempt" in item
        for item in result["failures"]
    )


def test_due_but_missing_terminal_attempt_is_a_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "source.sqlite"
    output_path = tmp_path / "horizons.sqlite"
    event_time = _seed_exact_source(monkeypatch, input_path)
    connection = producer.open_database(output_path)
    connection.close()

    result = _verify(
        input_path,
        output_path,
        event_time + dt.timedelta(minutes=1, seconds=21),
    )
    assert result["verified"] is False
    assert result["counts"]["due_missing_event_horizons"] == 1
    assert any("schedule:due_attempt_missing" in item for item in result["failures"])


def test_unsafe_frozen_config_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "source.sqlite"
    output_path = tmp_path / "horizons.sqlite"
    event_time = _seed_exact_source(monkeypatch, input_path)
    _capture_first_horizon(input_path, output_path, event_time)
    config = json.loads(verifier.CONFIG_PATH.read_text(encoding="utf-8"))
    config["policy"]["can_place_orders"] = True
    config_path = tmp_path / "unsafe.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")

    result = _verify(
        input_path,
        output_path,
        event_time + dt.timedelta(minutes=1, seconds=21),
        config_path,
    )
    assert result["verified"] is False
    assert "config:policy:can_place_orders:unsafe_or_mismatched" in result["failures"]
