from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

import oanda_official_event_quote_horizon_capture_v1 as capture
import oanda_official_release_fast_lane as fast


UTC = dt.timezone.utc


def _all68_quote_payload(at: dt.datetime) -> dict:
    quotes = {
        instrument: {
            "bid": 150.0 if instrument.endswith("_JPY") else 1.0,
            "ask": 150.01 if instrument.endswith("_JPY") else 1.0002,
            "pip": 0.01 if instrument.endswith("_JPY") else 0.0001,
            "time": fast.iso_utc(at),
            "source": "practice_007_fast_executor_price_stream",
        }
        for instrument in fast.EXPECTED_QUOTE_INSTRUMENTS
    }
    return {
        "schema_version": 2,
        "generated_utc": fast.iso_utc(at),
        "producer": "practice_007_fast_executor_price_stream",
        "connection_generation": 23,
        "quote_count": 68,
        "quotes": quotes,
        "coverage": {
            "current_quote_count": 68,
            "last_known_quote_count": 68,
            "retained_last_known_count": 0,
            "retained_last_known_instruments": [],
            "connection_generation": 23,
            "retained_quotes_execution_eligible": False,
        },
        "transport": {"source": "sqlite_wal", "sequence": 9001},
        "research_only": True,
    }


def _seed_exact_entry_sidecar(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
) -> tuple[dt.datetime, str]:
    monkeypatch.setattr(
        fast.news,
        "prospective_clock_attestation",
        lambda clock: bool(clock.get("attested")),
    )
    event_time = max(
        fast.QUOTE_CAPTURE_ACTIVATED_UTC,
        capture.ACTIVATED_UTC,
    ) + dt.timedelta(minutes=1)
    input_connection = fast.open_database(path)
    try:
        inserted, duplicates, _emitted = fast.append_observations(
            input_connection,
            source={
                "source_id": "fed_monetary_policy",
                "source_contract_id": "fed-test-v1",
                "source_cohort_id": "fed-test-v1",
            },
            rows=[
                {
                    "headline": "Prospective official policy decision",
                    "source_url": "https://www.federalreserve.gov/test-event",
                    "published_utc": fast.iso_utc(event_time),
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
        row = input_connection.execute(
            """
            SELECT c.observation_id, c.timing_quality, c.proof_quote_count,
                   c.capture_contract_id, c.capture_cohort_id
              FROM official_release_quote_capture c
            """
        ).fetchone()
        assert row is not None
        assert row[1:] == (
            "prospective_exact_live_quote",
            68,
            fast.QUOTE_CAPTURE_CONTRACT_ID,
            fast.QUOTE_CAPTURE_COHORT_ID,
        )
        return event_time, str(row[0])
    finally:
        input_connection.close()


def _capture_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    connection.row_factory = sqlite3.Row
    return connection.execute(
        """
        SELECT * FROM official_event_horizon_capture
        ORDER BY horizon_min
        """
    ).fetchall()


def _create_empty_legacy_header_table(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            CREATE TABLE official_event_horizon_capture (
              capture_id TEXT PRIMARY KEY,
              input_capture_id TEXT NOT NULL,
              observation_id TEXT NOT NULL,
              event_first_known_utc TEXT NOT NULL,
              horizon_min INTEGER NOT NULL,
              target_utc TEXT NOT NULL,
              attempted_utc TEXT NOT NULL,
              attempt_delay_sec REAL NOT NULL,
              timing_quality TEXT NOT NULL,
              invalid_reason TEXT NOT NULL,
              observed_valid_quote_count INTEGER NOT NULL,
              proof_quote_count INTEGER NOT NULL,
              connection_generation INTEGER,
              quote_snapshot_generated_utc TEXT NOT NULL,
              quote_snapshot_sha256 TEXT NOT NULL,
              input_capture_payload_sha256 TEXT NOT NULL,
              component_root_sha256 TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              payload_sha256 TEXT NOT NULL,
              research_only INTEGER NOT NULL,
              execution_eligible INTEGER NOT NULL,
              can_place_orders INTEGER NOT NULL,
              can_authorize INTEGER NOT NULL,
              can_promote INTEGER NOT NULL,
              contract_id TEXT NOT NULL,
              cohort_id TEXT NOT NULL,
              UNIQUE(input_capture_id,horizon_min)
            )
            """
        )
        connection.commit()
    finally:
        connection.close()


def test_due_horizons_freeze_exact_all68_quotes_linked_to_entry_sidecar(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "raw.sqlite"
    event_time, observation_id = _seed_exact_entry_sidecar(monkeypatch, input_path)
    input_connection = fast.open_database(input_path)
    output_connection = capture.open_database(tmp_path / "horizons.sqlite")
    try:
        for horizon_min in capture.HORIZONS_MIN:
            attempted = event_time + dt.timedelta(minutes=horizon_min, seconds=3)
            capture.capture_due_horizons(
                output_connection,
                input_connection,
                _all68_quote_payload(attempted),
                attempted,
            )
        output_connection.commit()

        headers = _capture_rows(output_connection)
        assert capture.HORIZONS_MIN == (1, 5, 15, 30, 60)
        assert [row["horizon_min"] for row in headers] == list(capture.HORIZONS_MIN)
        assert {row["observation_id"] for row in headers} == {observation_id}
        assert {row["timing_quality"] for row in headers} == {
            "prospective_exact_live_quote"
        }
        assert {row["proof_quote_count"] for row in headers} == {68}
        for row in headers:
            payload = json.loads(row["payload_json"])
            material = payload["quote_snapshot_material"]
            material_json = json.dumps(
                material, sort_keys=True, separators=(",", ":"), default=str
            )
            assert hashlib.sha256(material_json.encode()).hexdigest() == row[
                "quote_snapshot_sha256"
            ]
            assert material["schema_version"] == 2
            assert material["producer"] == capture.REQUIRED_SNAPSHOT_PRODUCER
        assert all(row["research_only"] == 1 for row in headers)
        assert all(row["execution_eligible"] == 0 for row in headers)
        assert all(row["can_authorize"] == 0 for row in headers)
        assert all(row["can_promote"] == 0 for row in headers)

        quote_counts = output_connection.execute(
            """
            SELECT horizon_min, COUNT(*)
              FROM official_event_horizon_quote
             GROUP BY horizon_min
             ORDER BY horizon_min
            """
        ).fetchall()
        assert [tuple(row) for row in quote_counts] == [
            (horizon, 68) for horizon in capture.HORIZONS_MIN
        ]
        assert output_connection.execute(
            "SELECT COUNT(DISTINCT instrument) FROM official_event_horizon_quote"
        ).fetchone()[0] == 68
        assert output_connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    finally:
        output_connection.close()
        input_connection.close()


def test_not_due_does_not_insert_terminal_attempt(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "raw.sqlite"
    event_time, _ = _seed_exact_entry_sidecar(monkeypatch, input_path)
    input_connection = fast.open_database(input_path)
    output_connection = capture.open_database(tmp_path / "horizons.sqlite")
    try:
        attempted = event_time + dt.timedelta(seconds=30)
        capture.capture_due_horizons(
            output_connection,
            input_connection,
            _all68_quote_payload(attempted),
            attempted,
        )
        output_connection.commit()
        assert _capture_rows(output_connection) == []
        assert output_connection.execute(
            "SELECT COUNT(*) FROM official_event_horizon_quote"
        ).fetchone()[0] == 0
    finally:
        output_connection.close()
        input_connection.close()


def test_invalid_all68_attempt_is_terminal_and_never_recaptured(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "raw.sqlite"
    event_time, observation_id = _seed_exact_entry_sidecar(monkeypatch, input_path)
    input_connection = fast.open_database(input_path)
    output_connection = capture.open_database(tmp_path / "horizons.sqlite")
    attempted = event_time + dt.timedelta(minutes=1, seconds=3)
    incomplete = _all68_quote_payload(attempted)
    missing = fast.EXPECTED_QUOTE_INSTRUMENTS[-1]
    del incomplete["quotes"][missing]
    incomplete["quote_count"] = 67
    incomplete["coverage"]["current_quote_count"] = 67
    incomplete["coverage"]["last_known_quote_count"] = 67
    try:
        capture.capture_due_horizons(
            output_connection,
            input_connection,
            incomplete,
            attempted,
        )
        output_connection.commit()
        header = _capture_rows(output_connection)
        assert len(header) == 1
        assert header[0]["observation_id"] == observation_id
        assert header[0]["horizon_min"] == 1
        assert header[0]["timing_quality"] != "prospective_exact_live_quote"
        assert header[0]["proof_quote_count"] == 0
        assert output_connection.execute(
            "SELECT COUNT(*) FROM official_event_horizon_quote"
        ).fetchone()[0] == 0

        # A later valid payload cannot replace or supplement the first terminal
        # attempt for the same event/horizon identity.
        capture.capture_due_horizons(
            output_connection,
            input_connection,
            _all68_quote_payload(attempted + dt.timedelta(seconds=1)),
            attempted + dt.timedelta(seconds=1),
        )
        output_connection.commit()
        assert len(_capture_rows(output_connection)) == 1
        assert output_connection.execute(
            "SELECT COUNT(*) FROM official_event_horizon_quote"
        ).fetchone()[0] == 0
    finally:
        output_connection.close()
        input_connection.close()


@pytest.mark.parametrize(
    ("field_path", "bad_value"),
    [
        (("quote_count",), "68"),
        (("coverage", "current_quote_count"), "bad"),
        (("coverage", "last_known_quote_count"), 67.5),
        (("coverage", "retained_last_known_count"), "0"),
        (("connection_generation",), "23"),
        (("coverage", "connection_generation"), None),
    ],
)
def test_malformed_integer_metadata_is_terminal_and_never_replaced(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    field_path: tuple[str, ...],
    bad_value: object,
) -> None:
    input_path = tmp_path / "raw.sqlite"
    event_time, _ = _seed_exact_entry_sidecar(monkeypatch, input_path)
    input_connection = fast.open_database(input_path)
    output_connection = capture.open_database(tmp_path / "horizons.sqlite")
    attempted = event_time + dt.timedelta(minutes=1, seconds=3)
    malformed = _all68_quote_payload(attempted)
    target = malformed
    for field in field_path[:-1]:
        target = target[field]
    target[field_path[-1]] = bad_value
    try:
        first = capture.capture_due_horizons(
            output_connection,
            input_connection,
            malformed,
            attempted,
        )
        assert first["inserted"] == 1
        assert first["terminal_invalid"] == 1
        header = _capture_rows(output_connection)
        assert len(header) == 1
        assert header[0]["timing_quality"] == "prospective_quote_coverage_invalid"
        assert header[0]["proof_quote_count"] == 0
        assert output_connection.execute(
            "SELECT COUNT(*) FROM official_event_horizon_quote"
        ).fetchone()[0] == 0

        later = capture.capture_due_horizons(
            output_connection,
            input_connection,
            _all68_quote_payload(attempted + dt.timedelta(seconds=1)),
            attempted + dt.timedelta(seconds=1),
        )
        assert later["inserted"] == 0
        assert len(_capture_rows(output_connection)) == 1
        assert output_connection.execute(
            "SELECT COUNT(*) FROM official_event_horizon_quote"
        ).fetchone()[0] == 0
    finally:
        output_connection.close()
        input_connection.close()


@pytest.mark.parametrize("defect", ["schema", "producer", "source"])
def test_wrong_snapshot_provenance_is_terminal_invalid(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    defect: str,
) -> None:
    input_path = tmp_path / "raw.sqlite"
    event_time, _ = _seed_exact_entry_sidecar(monkeypatch, input_path)
    input_connection = fast.open_database(input_path)
    output_connection = capture.open_database(tmp_path / "horizons.sqlite")
    attempted = event_time + dt.timedelta(minutes=1, seconds=3)
    payload = _all68_quote_payload(attempted)
    if defect == "schema":
        payload["schema_version"] = 1
    elif defect == "producer":
        payload["producer"] = "unknown"
    else:
        payload["quotes"][capture.EXPECTED_INSTRUMENTS[0]]["source"] = ""
    try:
        result = capture.capture_due_horizons(
            output_connection, input_connection, payload, attempted
        )
        assert result["terminal_invalid"] == 1
        row = _capture_rows(output_connection)[0]
        assert row["proof_quote_count"] == 0
        assert output_connection.execute(
            "SELECT COUNT(*) FROM official_event_horizon_quote"
        ).fetchone()[0] == 0
    finally:
        output_connection.close()
        input_connection.close()


def test_duplicate_capture_identity_does_not_rebuild_or_recapture(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "raw.sqlite"
    event_time, _ = _seed_exact_entry_sidecar(monkeypatch, input_path)
    input_connection = fast.open_database(input_path)
    output_connection = capture.open_database(tmp_path / "horizons.sqlite")
    attempted = event_time + dt.timedelta(minutes=1, seconds=3)
    try:
        capture.capture_due_horizons(
            output_connection,
            input_connection,
            _all68_quote_payload(attempted),
            attempted,
        )
        output_connection.commit()

        monkeypatch.setattr(
            capture,
            "build_horizon_capture",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("duplicate attempted horizon recapture")
            ),
        )
        capture.capture_due_horizons(
            output_connection,
            input_connection,
            _all68_quote_payload(attempted + dt.timedelta(seconds=1)),
            attempted + dt.timedelta(seconds=1),
        )
        output_connection.commit()
        assert len(_capture_rows(output_connection)) == 1
        assert output_connection.execute(
            "SELECT COUNT(*) FROM official_event_horizon_quote"
        ).fetchone()[0] == 68
    finally:
        output_connection.close()
        input_connection.close()


def test_horizon_headers_and_quote_rows_are_append_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "raw.sqlite"
    event_time, _ = _seed_exact_entry_sidecar(monkeypatch, input_path)
    input_connection = fast.open_database(input_path)
    output_connection = capture.open_database(tmp_path / "horizons.sqlite")
    attempted = event_time + dt.timedelta(minutes=1, seconds=3)
    try:
        capture.capture_due_horizons(
            output_connection,
            input_connection,
            _all68_quote_payload(attempted),
            attempted,
        )
        output_connection.commit()
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            output_connection.execute(
                "UPDATE official_event_horizon_capture SET proof_quote_count=0"
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            output_connection.execute("DELETE FROM official_event_horizon_capture")
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            output_connection.execute(
                "UPDATE official_event_horizon_quote SET bid=0"
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            output_connection.execute("DELETE FROM official_event_horizon_quote")
    finally:
        output_connection.close()
        input_connection.close()


def test_executable_endpoint_math_and_slippage_are_separate() -> None:
    long_result = capture.executable_path(
        {"bid": 1.0000, "ask": 1.0002, "pip": 0.0001},
        {"bid": 1.0007, "ask": 1.0009, "pip": 0.0001},
        "buy",
        slippage_pips=0.5,
    )
    assert long_result["signed_mid_move_pips"] == pytest.approx(7.0)
    assert long_result["executable_net_pips_before_slippage"] == pytest.approx(5.0)
    assert long_result["round_trip_slippage_pips"] == pytest.approx(0.5)
    assert long_result["executable_net_pips_after_slippage"] == pytest.approx(4.5)
    assert long_result["cost_drag_pips"] == pytest.approx(2.5)

    short_result = capture.executable_path(
        {"bid": 1.0000, "ask": 1.0002, "pip": 0.0001},
        {"bid": 0.9993, "ask": 0.9995, "pip": 0.0001},
        "sell",
        slippage_pips=0.5,
    )
    assert short_result["signed_mid_move_pips"] == pytest.approx(7.0)
    assert short_result["executable_net_pips_before_slippage"] == pytest.approx(5.0)
    assert short_result["round_trip_slippage_pips"] == pytest.approx(0.5)
    assert short_result["executable_net_pips_after_slippage"] == pytest.approx(4.5)
    assert short_result["cost_drag_pips"] == pytest.approx(2.5)


def test_policy_has_no_execution_authorization_or_promotion_surface() -> None:
    assert capture.POLICY["research_only"] is True
    assert capture.POLICY["execution_eligible"] is False
    assert capture.POLICY["can_place_orders"] is False
    assert capture.POLICY["can_authorize"] is False
    assert capture.POLICY["can_promote"] is False
    assert capture.POLICY["supported_execution_decision"] == "no_trade"


@pytest.mark.parametrize("section", ["timing", "cost_stress", "policy"])
def test_producer_rejects_any_frozen_contract_drift(
    tmp_path: Path,
    section: str,
) -> None:
    config = json.loads(capture.CONFIG_PATH.read_text(encoding="utf-8"))
    if section == "timing":
        config["timing"]["maximum_attempt_delay_sec"] = 21.0
    elif section == "cost_stress":
        config["cost_stress"]["round_trip_slippage_pips"] = [0.0]
    else:
        config["policy"]["missing_or_late_capture_policy"] = "retry"
    path = tmp_path / "drifted.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    with pytest.raises(ValueError):
        capture.validate_frozen_config(path)


def test_empty_legacy_clock_schema_migrates_before_activation(tmp_path: Path) -> None:
    path = tmp_path / "empty-legacy.sqlite"
    _create_empty_legacy_header_table(path)
    connection = capture.open_database(path)
    try:
        columns = {
            str(row[1])
            for row in connection.execute(
                "PRAGMA table_info(official_event_horizon_capture)"
            )
        }
        assert "capture_read_started_utc" in columns
        assert connection.execute(
            "SELECT COUNT(*) FROM official_event_horizon_capture"
        ).fetchone()[0] == 0
    finally:
        connection.close()


def test_nonempty_legacy_clock_schema_fails_before_ddl(tmp_path: Path) -> None:
    path = tmp_path / "nonempty-legacy.sqlite"
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            CREATE TABLE official_event_horizon_capture (
              capture_id TEXT PRIMARY KEY,
              event_first_known_utc TEXT NOT NULL,
              horizon_min INTEGER NOT NULL,
              timing_quality TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "INSERT INTO official_event_horizon_capture VALUES (?,?,?,?)",
            ("legacy", "2026-08-30T00:00:00+00:00", 1, "legacy"),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(ValueError, match="nonempty horizon ledger"):
        capture.open_database(path)
    inspection = sqlite3.connect(path)
    try:
        assert inspection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'"
        ).fetchone()[0] == 0
        assert "capture_read_started_utc" not in {
            str(row[1])
            for row in inspection.execute(
                "PRAGMA table_info(official_event_horizon_capture)"
            )
        }
    finally:
        inspection.close()


def test_hidden_supervisor_manages_horizon_capture_worker() -> None:
    supervisor = (capture.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    assert '-Name "official_event_quote_horizon_capture_v1"' in supervisor
    assert 'oanda_official_event_quote_horizon_capture_v1.py' in supervisor
    assert 'official_event_quote_horizon_capture_heartbeat_v1.json' in supervisor
    assert capture.CONTRACT_ID in supervisor
    assert '-Name "official_event_quote_horizon_capture_verifier_v1"' in supervisor
    assert 'oanda_official_event_quote_horizon_capture_v1_verifier.py' in supervisor
    assert (
        'official_event_quote_horizon_capture_verifier_heartbeat_v1.json'
        in supervisor
    )


def test_missing_event_clock_raises_and_late_attempt_is_terminal_invalid() -> None:
    with pytest.raises(ValueError, match="event clock"):
        capture.build_horizon_capture(
            {
                "capture_id": "entry-1",
                "observation_id": "event-1",
                "capture_contract_id": fast.QUOTE_CAPTURE_CONTRACT_ID,
                "capture_cohort_id": fast.QUOTE_CAPTURE_COHORT_ID,
            },
            1,
            _all68_quote_payload(capture.ACTIVATED_UTC),
            capture.ACTIVATED_UTC,
        )

    event_time = capture.ACTIVATED_UTC + dt.timedelta(minutes=1)
    target = event_time + dt.timedelta(minutes=1)
    late = target + dt.timedelta(seconds=capture.MAXIMUM_ATTEMPT_DELAY_SEC + 1)
    result = capture.build_horizon_capture(
        {
            "capture_id": "entry-2",
            "observation_id": "event-2",
            "event_first_known_utc": fast.iso_utc(event_time),
            "capture_contract_id": fast.QUOTE_CAPTURE_CONTRACT_ID,
            "capture_cohort_id": fast.QUOTE_CAPTURE_COHORT_ID,
        },
        1,
        _all68_quote_payload(late),
        late,
    )
    assert result["timing_quality"] == "prospective_horizon_clock_missed"
    assert result["proof_quote_count"] == 0
    assert result["quotes"] == {}
    assert result["research_only"] is True
    assert result["can_authorize"] is False


def test_wrong_entry_capture_contract_never_enters_horizon_ledger(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "raw.sqlite"
    event_time, _ = _seed_exact_entry_sidecar(monkeypatch, input_path)
    input_connection = fast.open_database(input_path)
    output_connection = capture.open_database(tmp_path / "horizons.sqlite")
    try:
        # Simulate an externally contaminated upstream store. The consumer
        # must bind to the exact frozen entry-capture contract, independent of
        # the upstream trigger that normally makes this mutation impossible.
        input_connection.execute("DROP TRIGGER trg_fast_lane_quote_capture_no_update")
        input_connection.execute(
            """
            UPDATE official_release_quote_capture
               SET capture_contract_id='forged_entry_capture_contract'
            """
        )
        input_connection.commit()
        attempted = event_time + dt.timedelta(minutes=1, seconds=3)
        counts = capture.capture_due_horizons(
            output_connection,
            input_connection,
            _all68_quote_payload(attempted),
            attempted,
        )
        assert counts["eligible_input_captures"] == 0
        assert _capture_rows(output_connection) == []
        assert output_connection.execute(
            "SELECT COUNT(*) FROM official_event_horizon_quote"
        ).fetchone()[0] == 0
    finally:
        output_connection.close()
        input_connection.close()
