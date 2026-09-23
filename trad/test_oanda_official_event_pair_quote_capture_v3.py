import datetime as dt
import json
from pathlib import Path
import sqlite3

import oanda_official_event_pair_quote_capture_v2 as v2
import oanda_official_event_pair_quote_capture_v3 as subject
import oanda_official_event_pair_quote_capture_v3_verifier as verifier


UTC = dt.timezone.utc


def _alias(actionable: dt.datetime) -> dict:
    canonical_event_id = "a" * 64
    raw_payload = json.dumps(
        {"url": "https://example.gov/release/1", "title": "Fixture"},
        sort_keys=True,
    )
    return {
        "alias_id": "b" * 64,
        "ledger_name": "fast_lane",
        "canonical_event_id": canonical_event_id,
        "canonical_identity_key": "url:https://example.gov/release/1",
        "source_id": "rba_media",
        "source_contract_id": "fixture-source",
        "source_first_seen_utc": subject.iso_utc(actionable),
        "row_observed_utc": subject.iso_utc(actionable),
        "actionable_event_utc": subject.iso_utc(actionable),
        "raw_payload_json": raw_payload,
        "raw_payload_sha256": v2.sha256_text(raw_payload),
        "ledger_collector_contract_id": "fixture-collector",
        "ledger_collector_cohort_id": "fixture-cohort",
    }


def _quotes(generated: dt.datetime, quote_time: dt.datetime) -> dict:
    rows = {}
    for index, instrument in enumerate(v2.EXPECTED_INSTRUMENTS):
        bid = 1.0 + index / 10000
        rows[instrument] = {
            "bid": bid,
            "ask": bid + 0.0002,
            "pip": 0.0001,
            "time": subject.iso_utc(quote_time),
            "source": "stream",
            "tradeable": True,
        }
    return {
        "schema_version": v2.quote_v1.REQUIRED_QUOTE_SNAPSHOT_SCHEMA_VERSION,
        "producer": v2.quote_v1.REQUIRED_QUOTE_SNAPSHOT_PRODUCER,
        "generated_utc": subject.iso_utc(generated),
        "quote_count": 68,
        "quotes": rows,
        "coverage": {
            "current_quote_count": 68,
            "last_known_quote_count": 68,
            "retained_last_known_count": 0,
        },
        "transport": {"source": "fixture", "sequence": 1},
    }


def test_config_pins_frozen_v2_and_read_clock_policy():
    config = subject.validate_config()
    assert config["required_v2_producer_sha256"] == subject.REQUIRED_V2_PRODUCER_SHA256
    assert subject.POLICY["capture_clock_source"] == "quote_snapshot_read_observed_utc"
    assert subject.POLICY["cycle_start_is_capture_clock"] is False
    assert subject.POLICY["capture_clock_may_precede_quote_snapshot_generated"] is False


def test_v3_uses_actual_read_clock_after_snapshot_generation():
    actionable = subject.ACTIVATED_UTC + dt.timedelta(seconds=1)
    generated = actionable + dt.timedelta(seconds=1)
    captured = actionable + dt.timedelta(seconds=2)
    payload = _quotes(generated, actionable + dt.timedelta(milliseconds=500))

    old = v2.build_capture(_alias(actionable), payload, actionable)
    assert old["quote_snapshot_age_seconds"] == -1.0

    current = subject.build_capture(_alias(actionable), payload, captured)
    assert current["captured_utc"] == subject.iso_utc(captured)
    assert current["detection_latency_seconds"] == 2.0
    assert current["quote_snapshot_age_seconds"] == 1.0
    assert current["capture_clock_source"] == "quote_snapshot_read_observed_utc"
    assert current["capture_clock_precedes_quote_snapshot_generated"] is False
    assert current["eligible_quote_count"] == 68
    assert current["exact_all_68_available"] is True


def test_v3_fails_closed_if_capture_clock_precedes_snapshot():
    actionable = subject.ACTIVATED_UTC + dt.timedelta(seconds=1)
    generated = actionable + dt.timedelta(seconds=2)
    current = subject.build_capture(
        _alias(actionable),
        _quotes(generated, actionable + dt.timedelta(milliseconds=500)),
        actionable + dt.timedelta(seconds=1),
    )
    assert current["capture_clock_precedes_quote_snapshot_generated"] is True
    assert "capture_clock_precedes_quote_snapshot_generated" in current["metadata_invalid_reasons"]
    assert current["eligible_quote_count"] == 0
    assert current["exact_all_68_available"] is False


def test_runner_replaces_cycle_start_with_post_read_clock(monkeypatch, tmp_path: Path):
    cycle_started = subject.ACTIVATED_UTC + dt.timedelta(seconds=1)
    snapshot_generated = cycle_started + dt.timedelta(seconds=1)
    read_observed = cycle_started + dt.timedelta(seconds=2)
    quote_payload = _quotes(
        snapshot_generated, cycle_started + dt.timedelta(milliseconds=500)
    )

    def fake_base_run_cycle(**_kwargs):
        capture = v2.build_capture(_alias(cycle_started), quote_payload, cycle_started)
        return {
            "schema_version": subject.SCHEMA_VERSION,
            "contract_id": subject.CONTRACT_ID,
            "cohort_id": subject.COHORT_ID,
            "activated_utc": subject.iso_utc(subject.ACTIVATED_UTC),
            "status": "ok",
            "error": "",
            "counts": {"capture_count": 1},
            "capture_for_test": capture,
        }

    monkeypatch.setattr(subject, "_BASE_RUN_CYCLE", fake_base_run_cycle)
    monkeypatch.setattr(subject, "utc_now", lambda: read_observed)
    result = subject.run_cycle(
        snapshot_path=tmp_path / "state.json",
        heartbeat_path=tmp_path / "heartbeat.json",
    )
    capture = result["capture_for_test"]
    assert capture["captured_utc"] == subject.iso_utc(read_observed)
    assert capture["quote_snapshot_age_seconds"] == 1.0
    assert capture["detection_latency_seconds"] == 2.0


def test_independent_read_clock_check_accepts_good_and_rejects_bad(tmp_path: Path):
    actionable = subject.ACTIVATED_UTC + dt.timedelta(seconds=1)
    generated = actionable + dt.timedelta(seconds=1)
    captured = actionable + dt.timedelta(seconds=2)
    payload = subject.build_capture(
        _alias(actionable),
        _quotes(generated, actionable + dt.timedelta(milliseconds=500)),
        captured,
    )
    database = tmp_path / "capture.sqlite"
    connection = sqlite3.connect(database)
    connection.execute(
        "CREATE TABLE official_event_pair_quote_capture("
        "capture_id TEXT,captured_utc TEXT,actionable_event_utc TEXT,capture_payload_json TEXT)"
    )
    connection.execute(
        "INSERT INTO official_event_pair_quote_capture VALUES(?,?,?,?)",
        (
            payload["capture_id"],
            payload["captured_utc"],
            payload["actionable_event_utc"],
            json.dumps(payload, sort_keys=True),
        ),
    )
    connection.commit()
    connection.close()
    assert verifier._read_clock_failures(database) == []

    broken = dict(payload)
    broken["quote_snapshot_read_observed_utc"] = subject.iso_utc(actionable)
    connection = sqlite3.connect(database)
    connection.execute(
        "UPDATE official_event_pair_quote_capture SET capture_payload_json=?",
        (json.dumps(broken, sort_keys=True),),
    )
    connection.commit()
    connection.close()
    assert any(
        "V3_read_observed_clock_mismatch" in item
        for item in verifier._read_clock_failures(database)
    )


def test_supervisor_retires_v2_and_tracks_v3_in_vault():
    supervisor = (subject.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    assert "v3_quote_snapshot_read_clock_cutover" in supervisor
    assert '-Name "official_event_pair_quote_capture_v3"' in supervisor
    assert '-Name "official_event_pair_quote_capture_v3_verifier"' in supervisor
    vault = (subject.ROOT / "forex_model_vault_sync.py").read_text(encoding="utf-8")
    for name in (
        "oanda_official_event_pair_quote_capture_v3.py",
        "oanda_official_event_pair_quote_capture_v3_verifier.py",
        "official_event_pair_quote_capture_v3.json",
        "test_oanda_official_event_pair_quote_capture_v3.py",
    ):
        assert name in vault
