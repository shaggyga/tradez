import datetime as dt
import json
import sqlite3

import oanda_macro_consensus_prospective as consensus
from oanda_macro_consensus_prospective import run_once


UTC = dt.timezone.utc


def test_future_consensus_is_causal_and_past_snapshot_is_retained_rejected(tmp_path):
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source_contract_id": "test_consensus_v1",
                "provider": "Test Provider",
                "endpoint": "https://invalid.example",
                "credential_environment": ["NEVER_SET_TEST_KEY"],
                "maximum_future_days": 14,
                "minimum_importance": 2,
            }
        ),
        encoding="utf-8",
    )
    raw = json.dumps(
        [
            {
                "CalendarId": "future", "Date": "2026-08-12T13:30:00Z",
                "LastUpdate": "2026-08-12T11:59:00Z", "Symbol": "US_CPI",
                "Event": "CPI", "Country": "United States", "Currency": "USD",
                "Forecast": "2.8%", "ForecastValue": 2.8, "Importance": 3,
                "SourceURL": "https://www.bls.gov/",
            },
            {
                "CalendarId": "past", "Date": "2026-08-12T11:00:00Z",
                "LastUpdate": "2026-08-12T11:01:00Z", "Symbol": "OLD",
                "Event": "Old release", "Country": "United States", "Currency": "USD",
                "Forecast": "1", "ForecastValue": 1.0, "Importance": 3,
            },
        ]
    ).encode()
    database = tmp_path / "ledger.sqlite"
    output = tmp_path / "state.json"
    projection = tmp_path / "import.jsonl"
    result = run_once(
        config_path=config, database=database, output=output,
        import_path=projection, report=tmp_path / "report.md",
        archive=tmp_path / "archive", observed=dt.datetime(2026, 8, 12, 12, tzinfo=UTC),
        raw=raw,
    )
    assert result["totals"]["causal_observations"] == 1
    assert result["totals"]["rejected_observations"] == 1
    assert result["totals"]["rejection_reasons"] == {"not_captured_before_release": 1}
    lines = [json.loads(line) for line in projection.read_text(encoding="utf-8").splitlines()]
    assert len(lines) == 2
    assert next(row for row in lines if row["event_series_id"] == "US_CPI")["source_verified"]
    db = sqlite3.connect(database)
    assert db.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    try:
        db.execute("DELETE FROM consensus_observations")
    except sqlite3.IntegrityError as exc:
        assert "append_only" in str(exc)
    else:
        raise AssertionError("consensus evidence was mutable")
    db.close()

    # A later poll of the identical provider snapshot does not inflate the
    # observation count. Only a changed consensus value/version can do so.
    repeated = run_once(
        config_path=config, database=database, output=output,
        import_path=projection, report=tmp_path / "report.md",
        archive=tmp_path / "archive", observed=dt.datetime(2026, 8, 12, 12, 1, tzinfo=UTC),
        raw=raw,
    )
    assert repeated["totals"]["observations"] == 2
    assert repeated["cycle"]["inserted"] == 0


def test_missing_credential_fails_closed_without_creating_consensus(tmp_path, monkeypatch):
    monkeypatch.delenv("NEVER_SET_TEST_KEY", raising=False)
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "source_contract_id": "blocked", "provider": "Provider",
                "endpoint": "https://invalid.example",
                "credential_environment": ["NEVER_SET_TEST_KEY"],
            }
        ),
        encoding="utf-8",
    )
    result = run_once(
        config_path=config, database=tmp_path / "db.sqlite",
        output=tmp_path / "state.json", import_path=tmp_path / "import.jsonl",
        report=tmp_path / "report.md", archive=tmp_path / "archive",
        observed=dt.datetime(2026, 8, 12, 12, tzinfo=UTC),
    )
    assert result["status"] == "blocked_missing_trading_economics_api_key"
    assert result["execution_eligible"] is False
    assert not (tmp_path / "db.sqlite").exists()


def test_fetch_failure_is_sanitized_and_does_not_create_evidence(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_TE_KEY", "client:secret")
    monkeypatch.setattr(
        consensus,
        "fetch",
        lambda *_: (_ for _ in ()).throw(RuntimeError("secret URL must not persist")),
    )
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "source_contract_id": "test", "provider": "Provider",
                "endpoint": "https://invalid.example",
                "credential_environment": ["TEST_TE_KEY"],
                "poll_interval_sec": 7200,
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "state.json"
    result = run_once(
        config_path=config, database=tmp_path / "db.sqlite", output=output,
        import_path=tmp_path / "import.jsonl", report=tmp_path / "report.md",
        archive=tmp_path / "archive",
        observed=dt.datetime(2026, 8, 12, 12, tzinfo=UTC),
    )
    serialized = output.read_text(encoding="utf-8")
    assert result["status"] == "degraded_fetch_failed"
    assert result["retry_after_sec"] == 7200
    assert result["execution_eligible"] is False
    assert "secret URL" not in serialized
    assert "client:secret" not in serialized
    assert not (tmp_path / "db.sqlite").exists()
