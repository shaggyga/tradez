import datetime as dt
import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from oanda_rates_policy_repricing_shadow import clock_attestation_digest, run_once


UTC = dt.timezone.utc
TEST_CLOCK_SECRET = "test-key"


@pytest.fixture(autouse=True)
def trusted_clock_key(monkeypatch):
    monkeypatch.setenv("TEST_RATE_CLOCK_HMAC_KEY", TEST_CLOCK_SECRET)


def write_config(path, *, connected=True, causal=True):
    path.write_text(json.dumps({
        "schema_version": 2,
        "contract_id": "test_rates_v2",
        "cohort_start_utc": "2026-08-29T09:00:00Z",
        "state": "test",
        "window_tolerance_sec": 5,
        "sources": [{
            "source_id": "test_ois",
            "source_contract_id": "test_ois_v1",
            "provider": "Official Test Source",
            "currency": "NZD",
            "instrument": "NZD_OIS_1Y",
            "tenor_label": "1Y",
            "connected": connected,
            "ingest_enabled": connected,
            "causal_intraday_eligible": causal,
            "clock_contract_id": "test_rate_clock_v1",
            "allowed_clock_sources": ["trusted_test_clock"],
            "clock_attestation_hmac_env": "TEST_RATE_CLOCK_HMAC_KEY",
            "raw_archive_required": True,
            "raw_intake_root": str((path.parent / "raw_input").resolve()),
            "access_state": "test_fixture"
        }],
        "blockers": []
    }), encoding="utf-8")


def observation(source_time, rate, payload):
    timestamp = dt.datetime.fromisoformat(source_time.replace("Z", "+00:00"))
    return {
        "source_id": "test_ois",
        "source_contract_id": "test_ois_v1",
        "provider": "Official Test Source",
        "currency": "NZD",
        "instrument": "NZD_OIS_1Y",
        "tenor_label": "1Y",
        "rate_pct": rate,
        "source_timestamp_utc": source_time,
        "retrieved_utc": (timestamp + dt.timedelta(seconds=1)).isoformat(),
        "observed_utc": (timestamp + dt.timedelta(seconds=2)).isoformat(),
        "clock_source": "trusted_test_clock",
        "clock_contract_id": "test_rate_clock_v1",
        "raw_payload_sha256": hashlib.sha256(payload.encode()).hexdigest(),
        "_test_raw_payload": payload
    }


def write_jsonl(path, rows):
    ready = []
    raw_root = path.parent / "raw_input"
    raw_root.mkdir(exist_ok=True)
    for index, original in enumerate(rows):
        row = dict(original)
        payload = str(row.pop("_test_raw_payload"))
        raw_path = raw_root / f"payload_{index}.raw"
        raw_path.write_bytes(payload.encode())
        row["raw_payload_path"] = str(raw_path)
        normalized = dict(row)
        for field in ("source_timestamp_utc", "retrieved_utc", "observed_utc"):
            normalized[field] = dt.datetime.fromisoformat(
                str(row[field]).replace("Z", "+00:00")
            ).astimezone(UTC).isoformat()
        row["clock_attestation_hmac_sha256"] = clock_attestation_digest(
            normalized, TEST_CLOCK_SECRET
        )
        ready.append(row)
    path.write_text("\n".join(json.dumps(row) for row in ready) + "\n", encoding="utf-8")


def invoke(tmp_path, config, imported, **kwargs):
    return run_once(
        config_path=config,
        database_path=tmp_path / "rates.sqlite",
        output_path=tmp_path / "state.json",
        report_path=tmp_path / "report.md",
        archive_root=tmp_path / "archive",
        import_path=imported,
        **kwargs
    )


def test_prospective_replay_builds_exact_15m_and_60m_changes(tmp_path):
    config = tmp_path / "config.json"
    imported = tmp_path / "rates.jsonl"
    write_config(config)
    write_jsonl(imported, [
        observation("2026-08-29T10:00:00Z", 4.00, "a"),
        observation("2026-08-29T10:45:00Z", 4.08, "b"),
        observation("2026-08-29T11:00:00Z", 4.10, "c")
    ])
    result = invoke(
        tmp_path, config, imported,
        observed=dt.datetime(2026, 8, 29, 11, 1, tzinfo=UTC),
        decision_cutoff=dt.datetime(2026, 8, 29, 11, 1, tzinfo=UTC)
    )
    assert result["cycle"]["imported_rows"] == 3
    current = result["replay"]["observations"][0]
    assert current["change_bps_15m"] == pytest.approx(2.0)
    assert current["change_bps_60m"] == pytest.approx(10.0)
    assert current["clock_attestation_verified"] == 1
    assert current["raw_archive_sha256_verified"] == 1
    assert Path(current["raw_archive_path"]).is_file()
    assert current["execution_eligible"] is False
    assert result["supported_execution_decision"] == "no_trade"


def test_as_of_replay_cannot_see_later_retrieval(tmp_path):
    config = tmp_path / "config.json"
    imported = tmp_path / "rates.jsonl"
    write_config(config)
    write_jsonl(imported, [
        observation("2026-08-29T10:00:00Z", 4.00, "a"),
        observation("2026-08-29T11:00:00Z", 4.10, "b")
    ])
    result = invoke(
        tmp_path, config, imported,
        observed=dt.datetime(2026, 8, 29, 11, 1, tzinfo=UTC),
        decision_cutoff=dt.datetime(2026, 8, 29, 10, 30, tzinfo=UTC)
    )
    current = result["replay"]["observations"][0]
    assert current["source_timestamp_utc"] == "2026-08-29T10:00:00+00:00"
    assert current["change_bps_60m"] is None


def test_disconnected_source_rejects_import_and_never_zero_fills(tmp_path):
    config = tmp_path / "config.json"
    imported = tmp_path / "rates.jsonl"
    write_config(config, connected=False)
    write_jsonl(imported, [observation("2026-08-29T10:00:00Z", 4.00, "a")])
    result = invoke(
        tmp_path, config, imported,
        observed=dt.datetime(2026, 8, 29, 11, tzinfo=UTC)
    )
    assert result["cycle"]["imported_rows"] == 0
    assert result["cycle"]["rejected_rows"] == 1
    assert result["replay"]["observations"] == []
    assert result["source_readiness"]["causal_intraday_connected_sources"] == 0


def test_daily_context_can_ingest_but_cannot_confirm_intraday(tmp_path):
    config = tmp_path / "config.json"
    imported = tmp_path / "rates.jsonl"
    write_config(config, connected=True, causal=False)
    write_jsonl(imported, [observation("2026-08-29T10:00:00Z", 4.00, "a")])
    result = invoke(
        tmp_path, config, imported,
        observed=dt.datetime(2026, 8, 29, 11, tzinfo=UTC)
    )
    assert result["totals"]["active_contract_immutable_observations"] == 1
    assert result["replay"]["observations"] == []


def test_invalid_clock_order_is_rejected(tmp_path):
    config = tmp_path / "config.json"
    imported = tmp_path / "rates.jsonl"
    write_config(config)
    row = observation("2026-08-29T10:00:00Z", 4.00, "a")
    row["retrieved_utc"] = "2026-08-29T09:59:59Z"
    write_jsonl(imported, [row])
    result = invoke(
        tmp_path, config, imported,
        observed=dt.datetime(2026, 8, 29, 11, tzinfo=UTC)
    )
    assert result["cycle"]["imported_rows"] == 0
    assert "knowledge clocks" in result["cycle"]["rejection_reasons"][0]


def test_precohort_history_is_immutable_diagnostic_not_causal(tmp_path):
    config = tmp_path / "config.json"
    imported = tmp_path / "rates.jsonl"
    write_config(config)
    write_jsonl(imported, [observation("2026-08-29T08:00:00Z", 4.00, "old")])
    result = invoke(
        tmp_path, config, imported,
        observed=dt.datetime(2026, 8, 29, 11, tzinfo=UTC)
    )
    assert result["totals"]["active_contract_immutable_observations"] == 1
    assert result["replay"]["observations"] == []


def test_self_declared_clock_trust_is_ignored_without_allowed_attestation(tmp_path):
    config = tmp_path / "config.json"
    imported = tmp_path / "rates.jsonl"
    write_config(config)
    row = observation("2026-08-29T10:00:00Z", 4.00, "untrusted")
    row["clock_source"] = "unsynchronized_host"
    row["clock_trusted"] = True
    write_jsonl(imported, [row])
    result = invoke(
        tmp_path, config, imported,
        observed=dt.datetime(2026, 8, 29, 11, tzinfo=UTC)
    )
    assert result["totals"]["active_contract_immutable_observations"] == 1
    assert result["replay"]["observations"] == []


def test_missing_independent_clock_key_keeps_row_out_of_replay(tmp_path, monkeypatch):
    config = tmp_path / "config.json"
    imported = tmp_path / "rates.jsonl"
    write_config(config)
    write_jsonl(imported, [observation("2026-08-29T10:00:00Z", 4.00, "no-key")])
    monkeypatch.delenv("TEST_RATE_CLOCK_HMAC_KEY")
    result = invoke(
        tmp_path, config, imported,
        observed=dt.datetime(2026, 8, 29, 11, tzinfo=UTC)
    )
    assert result["totals"]["active_contract_immutable_observations"] == 1
    assert result["replay"]["observations"] == []
    assert result["source_readiness"]["causal_intraday_connected_sources"] == 0


def test_missing_raw_payload_is_rejected(tmp_path):
    config = tmp_path / "config.json"
    imported = tmp_path / "rates.jsonl"
    write_config(config)
    write_jsonl(imported, [observation("2026-08-29T10:00:00Z", 4.00, "missing")])
    imported_row = json.loads(imported.read_text(encoding="utf-8"))
    Path(imported_row["raw_payload_path"]).unlink()
    result = invoke(
        tmp_path, config, imported,
        observed=dt.datetime(2026, 8, 29, 11, tzinfo=UTC)
    )
    assert result["totals"]["active_contract_immutable_observations"] == 0
    assert result["cycle"]["rejected_rows"] == 1
    assert result["replay"]["observations"] == []


def test_raw_payload_outside_configured_intake_root_is_rejected(tmp_path):
    config = tmp_path / "config.json"
    imported = tmp_path / "rates.jsonl"
    write_config(config)
    write_jsonl(imported, [observation("2026-08-29T10:00:00Z", 4.00, "outside")])
    row = json.loads(imported.read_text(encoding="utf-8"))
    outside = tmp_path / "outside.raw"
    outside.write_text("outside", encoding="utf-8")
    row["raw_payload_path"] = str(outside.resolve())
    imported.write_text(json.dumps(row) + "\n", encoding="utf-8")
    result = invoke(
        tmp_path, config, imported,
        observed=dt.datetime(2026, 8, 29, 11, tzinfo=UTC)
    )
    assert result["cycle"]["imported_rows"] == 0
    assert result["cycle"]["rejected_rows"] == 1
    assert "outside configured intake root" in result["cycle"]["rejection_reasons"][0]


def test_raw_payload_parent_traversal_is_rejected(tmp_path):
    config = tmp_path / "config.json"
    imported = tmp_path / "rates.jsonl"
    write_config(config)
    write_jsonl(imported, [observation("2026-08-29T10:00:00Z", 4.00, "traversal")])
    row = json.loads(imported.read_text(encoding="utf-8"))
    outside = tmp_path / "traversal.raw"
    outside.write_text("traversal", encoding="utf-8")
    row["raw_payload_path"] = str(tmp_path / "raw_input" / ".." / "traversal.raw")
    imported.write_text(json.dumps(row) + "\n", encoding="utf-8")
    result = invoke(
        tmp_path, config, imported,
        observed=dt.datetime(2026, 8, 29, 11, tzinfo=UTC)
    )
    assert result["cycle"]["imported_rows"] == 0
    assert result["cycle"]["rejected_rows"] == 1
    assert "outside configured intake root" in result["cycle"]["rejection_reasons"][0]


def test_replay_and_counts_exclude_superseded_configured_contract(tmp_path):
    config = tmp_path / "config.json"
    imported = tmp_path / "rates.jsonl"
    write_config(config)
    write_jsonl(imported, [observation("2026-08-29T10:00:00Z", 4.00, "old")])
    first = invoke(
        tmp_path, config, imported,
        observed=dt.datetime(2026, 8, 29, 11, tzinfo=UTC)
    )
    old_cohort = first["source_readiness"]["sources"][0]["cohort_id"]
    updated = json.loads(config.read_text(encoding="utf-8"))
    updated["sources"][0]["source_contract_id"] = "test_ois_v2"
    config.write_text(json.dumps(updated), encoding="utf-8")
    second = invoke(
        tmp_path, config, None,
        observed=dt.datetime(2026, 8, 29, 11, 1, tzinfo=UTC)
    )
    active = second["source_readiness"]["sources"][0]
    assert active["source_contract_id"] == "test_ois_v2"
    assert active["cohort_id"] != old_cohort
    assert second["replay"]["observations"] == []
    assert second["totals"] == {
        "active_contract_immutable_observations": 0,
        "all_cohort_immutable_observations": 1,
        "excluded_nonactive_contract_observations": 1,
    }


def test_future_dated_row_cannot_be_preinserted(tmp_path):
    config = tmp_path / "config.json"
    imported = tmp_path / "rates.jsonl"
    write_config(config)
    write_jsonl(imported, [observation("2026-08-29T12:00:00Z", 4.00, "future")])
    result = invoke(
        tmp_path, config, imported,
        observed=dt.datetime(2026, 8, 29, 11, tzinfo=UTC)
    )
    assert result["totals"]["active_contract_immutable_observations"] == 0
    assert result["cycle"]["rejected_rows"] == 1
    assert "later than collection cycle" in result["cycle"]["rejection_reasons"][0]


def test_duplicate_does_not_inflate_and_tables_are_append_only(tmp_path):
    config = tmp_path / "config.json"
    imported = tmp_path / "rates.jsonl"
    write_config(config)
    row = observation("2026-08-29T10:00:00Z", 4.00, "a")
    write_jsonl(imported, [row, row])
    result = invoke(
        tmp_path, config, imported,
        observed=dt.datetime(2026, 8, 29, 11, tzinfo=UTC)
    )
    assert result["cycle"]["imported_rows"] == 1
    database = sqlite3.connect(tmp_path / "rates.sqlite")
    with pytest.raises(sqlite3.IntegrityError, match="append_only"):
        database.execute("UPDATE rate_observations SET rate_pct=5")
    assert database.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    database.close()


def test_default_contract_is_fail_closed(tmp_path):
    result = run_once(
        database_path=tmp_path / "rates.sqlite",
        output_path=tmp_path / "state.json",
        report_path=tmp_path / "report.md",
        archive_root=tmp_path / "archive",
        observed=dt.datetime(2026, 8, 29, 16, tzinfo=UTC)
    )
    assert result["source_readiness"]["causal_intraday_connected_sources"] == 0
    assert result["replay"]["observations"] == []
    assert result["supported_execution_decision"] == "no_trade"
