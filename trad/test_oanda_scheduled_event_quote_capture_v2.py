import datetime as dt
import json
from pathlib import Path
import sqlite3

import pytest

import oanda_scheduled_event_quote_capture_v2 as capture


UTC = dt.timezone.utc


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def preflight(now: dt.datetime, scheduled: dt.datetime) -> dict:
    return {
        "contract_id": capture.REQUIRED_PREFLIGHT_CONTRACT_ID,
        "generated_utc": capture.iso_utc(now),
        "events": [
            {
                "event_id": "future-rbnz",
                "event_series_id": "rbnz_policy_decision",
                "headline": "Future RBNZ policy decision",
                "scheduled_utc": capture.iso_utc(scheduled),
                "timing_precision": "minute",
                "currency": "NZD",
                "driver_currency": "NZD",
                "direct_event_currency": True,
                "research_only": True,
                "execution_eligible": False,
            }
        ],
    }


def rest_snapshot(
    target: dt.datetime, *, missing: str = "", old_tick: str = "EUR_DKK"
) -> dict:
    prices = {}
    nontradeable = {"EUR_TRY", "TRY_JPY", "USD_TRY"}
    for index, instrument in enumerate(capture.EXPECTED_INSTRUMENTS):
        if instrument == missing:
            continue
        pip = 0.01 if instrument.endswith("_JPY") else 0.0001
        bid = 1.0 + index * pip
        broker_time = (
            target - dt.timedelta(minutes=3)
            if instrument == old_tick
            else target + dt.timedelta(milliseconds=200)
        )
        prices[instrument] = {
            "bid": bid,
            "ask": bid + pip,
            "pip": pip,
            "broker_price_time_utc": capture.iso_utc(broker_time),
            "tradeable": instrument not in nontradeable,
            "status": "non-tradeable" if instrument in nontradeable else "tradeable",
            "source": "oanda_practice_rest_pricing",
        }
    return {
        "retrieval_started_utc": capture.iso_utc(target + dt.timedelta(seconds=1)),
        "retrieval_completed_utc": capture.iso_utc(target + dt.timedelta(seconds=1.5)),
        "response_time_utc": capture.iso_utc(target + dt.timedelta(seconds=1.25)),
        "response_price_count": len(prices),
        "prices": prices,
        "account_id_sha256": "a" * 64,
        "account_suffix": "-007",
        "environment": "practice",
        "request_method": "GET",
        "request_scope": "pricing_read_only",
    }


def paths(tmp_path: Path) -> dict:
    return {
        "preflight_path": tmp_path / "preflight.json",
        "database_path": tmp_path / "capture.sqlite",
        "state_path": tmp_path / "state.json",
        "heartbeat_path": tmp_path / "heartbeat.json",
    }


def test_frozen_v2_config_is_read_only_and_inert() -> None:
    config = capture.validate_config()
    assert config["expected_instrument_count"] == 68
    assert config["policy"]["quote_acquisition"].startswith("one_read_only")
    assert config["policy"]["currentness_clock"] == "pricing_response_observed_utc"
    assert config["policy"]["execution_eligible"] is False
    assert config["policy"]["can_place_orders"] is False
    assert config["policy"]["can_authorize"] is False
    assert config["policy"]["can_promote"] is False


def test_current_rest_response_accepts_old_tick_but_not_nontradeable_as_proof(
    tmp_path: Path,
) -> None:
    p = paths(tmp_path)
    registration = dt.datetime(2026, 9, 2, 2, 20, tzinfo=UTC)
    target = dt.datetime(2026, 9, 2, 3, 0, tzinfo=UTC)
    write_json(p["preflight_path"], preflight(registration, target))
    capture.run_cycle(now=registration, snapshot_loader=lambda: {}, **p)
    result = capture.run_cycle(
        now=target + dt.timedelta(seconds=2),
        snapshot_loader=lambda: rest_snapshot(target),
        **p,
    )
    assert result["counts"] == {
        "registered_event_clocks": 1,
        "terminal_capture_attempts": 1,
        "valid_current_snapshots": 1,
        "invalid_terminal_captures": 0,
        "observed_universe_rows": 68,
        "tradeable_proof_rows": 65,
        "nontradeable_observed_rows": 3,
        "direct_event_tradeable_rows": 9,
    }
    connection = sqlite3.connect(p["database_path"])
    try:
        row = connection.execute(
            "SELECT timing_quality,universe_quote_count,tradeable_proof_count,"
            "nontradeable_observed_count,direct_event_tradeable_count,payload_json "
            "FROM scheduled_event_quote_capture_v2"
        ).fetchone()
    finally:
        connection.close()
    assert row[:5] == (
        "prospective_current_oanda_pricing_snapshot", 68, 65, 3, 9
    )
    payload = json.loads(row[5])
    assert payload["quotes"]["EUR_DKK"]["broker_tick_age_sec"] > 15
    assert payload["broker_tick_age_is_diagnostic_only"] is True
    assert set(payload["nontradeable_quotes"]) == {
        "EUR_TRY", "TRY_JPY", "USD_TRY"
    }
    assert payload["direction_policy"] == "abstain"
    assert payload["execution_eligible"] is False


def test_missing_row_is_terminal_invalid_and_append_only(tmp_path: Path) -> None:
    p = paths(tmp_path)
    registration = dt.datetime(2026, 9, 2, 2, 20, tzinfo=UTC)
    target = dt.datetime(2026, 9, 2, 3, 0, tzinfo=UTC)
    write_json(p["preflight_path"], preflight(registration, target))
    capture.run_cycle(now=registration, snapshot_loader=lambda: {}, **p)
    result = capture.run_cycle(
        now=target + dt.timedelta(seconds=2),
        snapshot_loader=lambda: rest_snapshot(target, missing="EUR_DKK"),
        **p,
    )
    assert result["counts"]["valid_current_snapshots"] == 0
    assert result["counts"]["invalid_terminal_captures"] == 1
    connection = sqlite3.connect(p["database_path"])
    try:
        capture_id, reason = connection.execute(
            "SELECT capture_id,invalid_reason FROM scheduled_event_quote_capture_v2"
        ).fetchone()
        assert "instrument_set_mismatch" in reason
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            connection.execute(
                "UPDATE scheduled_event_quote_capture_v2 SET invalid_reason='changed' "
                "WHERE capture_id=?", (capture_id,)
            )
    finally:
        connection.close()


def test_post_activation_code_does_not_register_the_elapsed_rbnz_event(
    tmp_path: Path,
) -> None:
    p = paths(tmp_path)
    now = dt.datetime(2026, 9, 2, 2, 20, tzinfo=UTC)
    elapsed = dt.datetime(2026, 9, 2, 2, 0, tzinfo=UTC)
    write_json(p["preflight_path"], preflight(now, elapsed))
    result = capture.run_cycle(now=now, snapshot_loader=lambda: {}, **p)
    assert result["counts"]["registered_event_clocks"] == 0
    assert result["counts"]["terminal_capture_attempts"] == 0


def test_main_source_contains_no_order_or_promotion_call() -> None:
    source = Path(capture.__file__).read_text(encoding="utf-8")
    assert "place_order(" not in source
    assert "client.write(" not in source
    assert "--execute" not in source
    assert '"request_method": "GET"' in source
    assert '"can_place_orders": False' in source


def test_hidden_supervisor_runs_v2_as_read_only_practice_capture() -> None:
    supervisor = (capture.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    block = supervisor.split('-Name "scheduled_event_quote_capture_v2"', 1)[1]
    block = block.split(
        '-Name "official_event_quote_horizon_capture_verifier_v1"', 1
    )[0]
    assert "oanda_scheduled_event_quote_capture_v2.py" in block
    assert '"--account-key", $AccountKey' in block
    assert '"--interval-sec", "2"' in block
    assert capture.CONTRACT_ID in block
    assert "--execute" not in block
