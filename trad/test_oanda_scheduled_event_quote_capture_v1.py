import datetime as dt
import json
from pathlib import Path
import sqlite3

import pytest

import oanda_scheduled_event_quote_capture_v1 as capture


UTC = dt.timezone.utc


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_atomic_json_retries_transient_windows_replace_denial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "heartbeat.json"
    original_replace = capture.os.replace
    attempts = []

    def flaky_replace(source, destination):
        attempts.append((source, destination))
        if len(attempts) < 4:
            raise PermissionError(5, "Access is denied")
        return original_replace(source, destination)

    monkeypatch.setattr(capture.os, "replace", flaky_replace)
    monkeypatch.setattr(capture.time, "sleep", lambda _seconds: None)

    capture.write_json_atomic(target, {"status": "ok", "can_place_orders": False})

    assert len(attempts) == 4
    assert json.loads(target.read_text(encoding="utf-8"))["status"] == "ok"
    assert not list(tmp_path.glob(".heartbeat.json.*.tmp"))


def preflight(now: dt.datetime) -> dict:
    return {
        "contract_id": capture.REQUIRED_PREFLIGHT_CONTRACT_ID,
        "generated_utc": capture.iso_utc(now),
        "events": [
            {
                "event_id": "rbnz-20260902",
                "event_series_id": "rbnz_policy_decision",
                "headline": "Reserve Bank of New Zealand Monetary Policy Decision",
                "scheduled_utc": "2026-09-02T02:00:00+00:00",
                "timing_precision": "minute",
                "currency": "NZD",
                "driver_currency": "NZD",
                "direct_event_currency": True,
                "policy_dependency": False,
                "research_only": True,
                "execution_eligible": False,
            },
            {
                "event_id": "rbnz-20260902",
                "event_series_id": "rbnz_policy_decision",
                "headline": "Reserve Bank of New Zealand Monetary Policy Decision",
                "scheduled_utc": "2026-09-02T02:00:00+00:00",
                "timing_precision": "minute",
                "currency": "AUD",
                "driver_currency": "NZD",
                "direct_event_currency": False,
                "policy_dependency": True,
                "research_only": True,
                "execution_eligible": False,
            },
        ],
    }


def quote_snapshot(quote_time: dt.datetime) -> dict:
    quotes = {}
    for index, instrument in enumerate(capture.EXPECTED_INSTRUMENTS):
        pip = 0.01 if instrument.endswith("JPY") else 0.0001
        bid = 1.0 + index * pip
        quotes[instrument] = {
            "bid": bid,
            "ask": bid + pip,
            "pip": pip,
            "time": capture.iso_utc(quote_time),
            "source": "oanda_practice_stream",
        }
    return {
        "schema_version": capture.REQUIRED_QUOTE_SCHEMA_VERSION,
        "producer": capture.REQUIRED_QUOTE_PRODUCER,
        "generated_utc": capture.iso_utc(quote_time),
        "quote_count": capture.EXPECTED_INSTRUMENT_COUNT,
        "connection_generation": 7,
        "coverage": {
            "current_quote_count": capture.EXPECTED_INSTRUMENT_COUNT,
            "last_known_quote_count": capture.EXPECTED_INSTRUMENT_COUNT,
            "retained_last_known_count": 0,
            "connection_generation": 7,
        },
        "quotes": quotes,
    }


def paths(tmp_path: Path) -> dict:
    return {
        "preflight_path": tmp_path / "preflight.json",
        "quote_path": tmp_path / "quotes.json",
        "database_path": tmp_path / "capture.sqlite",
        "state_path": tmp_path / "state.json",
        "heartbeat_path": tmp_path / "heartbeat.json",
    }


def test_frozen_config_is_research_only_and_all68() -> None:
    config = capture.validate_config()
    assert config["expected_instrument_count"] == 68
    assert config["horizons_min"] == [0, 1, 5, 15, 30, 60]
    assert config["policy"]["scheduled_clock_assigns_no_direction"] is True
    assert config["policy"]["execution_eligible"] is False
    assert config["policy"]["can_place_orders"] is False
    assert config["policy"]["can_authorize"] is False
    assert config["policy"]["can_promote"] is False


def test_hidden_supervisor_runs_the_scheduled_capture_without_broker_arguments() -> None:
    supervisor = (capture.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    block = supervisor.split(
        '-Name "scheduled_event_quote_capture_v1"', 1
    )[1].split(
        '-Name "scheduled_event_quote_capture_v2"', 1
    )[0]
    assert 'oanda_scheduled_event_quote_capture_v1.py' in block
    assert '"--interval-sec", "2"' in block
    assert capture.CONTRACT_ID in block
    assert "--creds" not in block
    assert "--account-key" not in block
    assert "--execute" not in block


def test_dependency_rows_share_one_registered_event_clock(tmp_path: Path) -> None:
    p = paths(tmp_path)
    now = dt.datetime(2026, 9, 2, 1, 0, tzinfo=UTC)
    write_json(p["preflight_path"], preflight(now))
    result = capture.run_cycle(now=now, quote_loader=lambda _: {}, **p)
    assert result["counts"]["registered_event_clocks"] == 1
    connection = sqlite3.connect(p["database_path"])
    try:
        row = connection.execute(
            "SELECT direct_currencies_json,affected_currencies_json "
            "FROM scheduled_event_clock"
        ).fetchone()
    finally:
        connection.close()
    assert json.loads(row[0]) == ["NZD"]
    assert json.loads(row[1]) == ["AUD", "NZD"]


def test_exact_t0_capture_freezes_all68_and_never_retries(tmp_path: Path) -> None:
    p = paths(tmp_path)
    registration = dt.datetime(2026, 9, 2, 1, 0, tzinfo=UTC)
    write_json(p["preflight_path"], preflight(registration))
    capture.run_cycle(now=registration, quote_loader=lambda _: {}, **p)
    target = dt.datetime(2026, 9, 2, 2, 0, tzinfo=UTC)
    payload = quote_snapshot(target + dt.timedelta(seconds=1))
    first = capture.run_cycle(
        now=target + dt.timedelta(seconds=2),
        quote_loader=lambda _: payload,
        **p,
    )
    assert first["counts"]["exact_all68_captures"] == 1
    assert first["counts"]["proof_quote_rows"] == 68
    second = capture.run_cycle(
        now=target + dt.timedelta(seconds=4),
        quote_loader=lambda _: payload,
        **p,
    )
    assert second["counts"]["terminal_capture_attempts"] == 1
    connection = sqlite3.connect(p["database_path"])
    try:
        row = connection.execute(
            "SELECT timing_quality,proof_quote_count,payload_json "
            "FROM scheduled_event_quote_capture"
        ).fetchone()
    finally:
        connection.close()
    assert row[0] == "prospective_exact_live_quote"
    assert row[1] == 68
    frozen = json.loads(row[2])
    assert frozen["direction_policy"] == "abstain"
    assert frozen["execution_eligible"] is False


def test_late_t0_is_terminal_invalid_and_append_only(tmp_path: Path) -> None:
    p = paths(tmp_path)
    registration = dt.datetime(2026, 9, 2, 1, 0, tzinfo=UTC)
    write_json(p["preflight_path"], preflight(registration))
    capture.run_cycle(now=registration, quote_loader=lambda _: {}, **p)
    target = dt.datetime(2026, 9, 2, 2, 0, tzinfo=UTC)
    late = capture.run_cycle(
        now=target + dt.timedelta(seconds=30),
        quote_loader=lambda _: pytest.fail("late capture must not read a quote"),
        **p,
    )
    assert late["counts"]["invalid_terminal_captures"] == 1
    connection = sqlite3.connect(p["database_path"])
    try:
        row = connection.execute(
            "SELECT capture_id,timing_quality,invalid_reason "
            "FROM scheduled_event_quote_capture"
        ).fetchone()
        assert row[1] == "prospective_clock_missed"
        assert row[2].startswith("capture_latency_seconds:")
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            connection.execute(
                "UPDATE scheduled_event_quote_capture SET invalid_reason='rewritten' "
                "WHERE capture_id=?",
                (row[0],),
            )
    finally:
        connection.close()


def test_stale_preflight_cannot_register_a_clock(tmp_path: Path) -> None:
    p = paths(tmp_path)
    generated = dt.datetime(2026, 9, 2, 1, 0, tzinfo=UTC)
    write_json(p["preflight_path"], preflight(generated))
    result = capture.run_cycle(
        now=generated + dt.timedelta(seconds=181),
        quote_loader=lambda _: {},
        **p,
    )
    assert result["counts"]["registered_event_clocks"] == 0
    assert result["registration_error"].startswith("preflight_age_seconds:")
