import datetime as dt
import json
import sqlite3

import oanda_macro_consensus_prospective as consensus
from oanda_macro_consensus_prospective import run_once


UTC = dt.timezone.utc


def test_production_config_is_v2_with_exact_deployment_activation():
    config = consensus.read_json(consensus.CONFIG)
    assert config["schema_version"] == 2
    assert config["source_contract_id"] == (
        "trading_economics_pre_release_consensus_v2_20260829"
    )
    assert config["cohort_start_utc"] == "2026-08-29T16:05:00Z"
    assert consensus.parse_time(config["cohort_start_utc"]) is not None


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
                "require_exact_release_time": True,
                "source_verified": True,
                "cohort_start_utc": "2026-08-12T00:00:00Z",
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
                "DateSpan": 0,
                "SourceURL": "https://www.bls.gov/",
            },
            {
                "CalendarId": "past", "Date": "2026-08-12T11:00:00Z",
                "LastUpdate": "2026-08-12T11:01:00Z", "Symbol": "OLD",
                "Event": "Old release", "Country": "United States", "Currency": "USD",
                "Forecast": "1", "ForecastValue": 1.0, "Importance": 3,
                "DateSpan": 0,
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
    assert result["cohort"]["cohort_id"].startswith(
        "macro_consensus_prospective_v2.discovery.20260829."
    )
    assert result["cohort"]["cohort_start_utc"] == "2026-08-12T00:00:00Z"
    assert result["cohort"]["capture_contract_id"] == consensus.CAPTURE_CONTRACT_ID
    assert (
        result["cohort"]["observation_clock_contract_id"]
        == consensus.OBSERVATION_TIME_CONTRACT_ID
    )
    assert result["totals"]["rejected_observations"] == 1
    assert result["totals"]["rejection_reasons"] == {"not_captured_before_release": 1}
    lines = [json.loads(line) for line in projection.read_text(encoding="utf-8").splitlines()]
    assert len(lines) == 2
    causal = next(row for row in lines if row["event_series_id"] == "US_CPI")
    assert causal["source_verified"]
    assert causal["capture_clock_trusted"]
    assert causal["response_completed_utc"] == causal["captured_utc"]
    assert causal["request_started_utc"] <= causal["response_completed_utc"]
    assert causal["release_time_precision"] == "exact"
    assert causal["actual_present_at_capture"] is False
    assert len(causal["provider_snapshot_sha256"]) == 64
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


def test_request_start_cannot_make_response_received_after_release_causal(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("TEST_TE_KEY", "present")
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "source_contract_id": "response_boundary_v2",
                "provider": "Test Provider",
                "endpoint": "https://invalid.example",
                "credential_environment": ["TEST_TE_KEY"],
                "maximum_future_days": 14,
                "minimum_importance": 2,
                "require_exact_release_time": True,
                "source_verified": True,
                "cohort_start_utc": "2026-08-12T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    times = iter(
        (
            dt.datetime(2026, 8, 12, 12, 0, 0, tzinfo=UTC),
            dt.datetime(2026, 8, 12, 12, 0, 10, tzinfo=UTC),
        )
    )

    def clock(_now):
        value = next(times)
        return value, {
            "contract_id": consensus.OBSERVATION_TIME_CONTRACT_ID,
            "trusted_for_prospective_evidence": True,
        }

    monkeypatch.setattr(consensus, "normalized_observation_time", clock)
    monkeypatch.setattr(
        consensus,
        "fetch",
        lambda *_: json.dumps(
            [
                {
                    "CalendarId": "boundary",
                    "Date": "2026-08-12T12:00:05Z",
                    "LastUpdate": "2026-08-12T11:59:00Z",
                    "Symbol": "US_CPI",
                    "Event": "CPI",
                    "Country": "United States",
                    "Currency": "USD",
                    "Forecast": "2.8%",
                    "ForecastValue": 2.8,
                    "Importance": 3,
                    "DateSpan": 0,
                }
            ]
        ).encode(),
    )
    result = run_once(
        config_path=config,
        database=tmp_path / "db.sqlite",
        output=tmp_path / "state.json",
        import_path=tmp_path / "import.jsonl",
        report=tmp_path / "report.md",
        archive=tmp_path / "archive",
    )
    assert result["request_started_utc"].endswith("12:00:00+00:00")
    assert result["response_completed_utc"].endswith("12:00:10+00:00")
    assert result["totals"]["causal_observations"] == 0
    assert result["totals"]["rejection_reasons"] == {
        "not_captured_before_release": 1
    }


def test_untrusted_capture_clock_exact_schedule_and_existing_actual_fail_closed(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("TEST_TE_KEY", "present")
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "source_contract_id": "trusted-clock-v2",
                "provider": "Test Provider",
                "endpoint": "https://invalid.example",
                "credential_environment": ["TEST_TE_KEY"],
                "minimum_importance": 2,
                "require_exact_release_time": True,
                "source_verified": True,
                "cohort_start_utc": "2026-08-12T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    clocks = iter(
        (
            (
                dt.datetime(2026, 8, 12, 12, 0, tzinfo=UTC),
                {
                    "contract_id": consensus.OBSERVATION_TIME_CONTRACT_ID,
                    "trusted_for_prospective_evidence": True,
                },
            ),
            (
                dt.datetime(2026, 8, 12, 12, 0, 1, tzinfo=UTC),
                {
                    "contract_id": consensus.OBSERVATION_TIME_CONTRACT_ID,
                    "trusted_for_prospective_evidence": False,
                },
            ),
        )
    )
    monkeypatch.setattr(consensus, "normalized_observation_time", lambda _: next(clocks))
    monkeypatch.setattr(
        consensus,
        "fetch",
        lambda *_: json.dumps(
            [
                {
                    "CalendarId": "future",
                    "Date": "2026-08-12T13:30:00Z",
                    "LastUpdate": "2026-08-12T11:59:00Z",
                    "Symbol": "US_CPI",
                    "ForecastValue": 2.8,
                    "Importance": 3,
                    "DateSpan": 0,
                }
            ]
        ).encode(),
    )
    result = run_once(
        config_path=config,
        database=tmp_path / "db.sqlite",
        output=tmp_path / "state.json",
        import_path=tmp_path / "import.jsonl",
        report=tmp_path / "report.md",
        archive=tmp_path / "archive",
    )
    assert result["totals"]["rejection_reasons"] == {"capture_clock_untrusted": 1}


def test_pre_activation_capture_is_retained_but_never_causal(tmp_path):
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "source_contract_id": "activation-v2",
                "cohort_start_utc": "2026-08-12T12:05:00Z",
                "provider": "Test Provider",
                "endpoint": "https://invalid.example",
                "credential_environment": ["NEVER_SET_TEST_KEY"],
                "minimum_importance": 2,
                "require_exact_release_time": True,
                "source_verified": True,
            }
        ),
        encoding="utf-8",
    )
    raw = json.dumps(
        [
            {
                "CalendarId": "future",
                "Date": "2026-08-12T13:30:00Z",
                "LastUpdate": "2026-08-12T11:59:00Z",
                "Symbol": "US_CPI",
                "ForecastValue": 2.8,
                "Importance": 3,
                "DateSpan": 0,
            }
        ]
    ).encode()
    result = run_once(
        config_path=config,
        database=tmp_path / "db.sqlite",
        output=tmp_path / "state.json",
        import_path=tmp_path / "import.jsonl",
        report=tmp_path / "report.md",
        archive=tmp_path / "archive",
        observed=dt.datetime(2026, 8, 12, 12, tzinfo=UTC),
        raw=raw,
    )
    assert result["totals"]["causal_observations"] == 0
    assert result["totals"]["rejection_reasons"] == {
        "captured_before_cohort_activation": 1
    }


def test_calendar_id_and_provider_last_update_are_mandatory(tmp_path):
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "source_contract_id": "provider-version-v2",
                "cohort_start_utc": "2026-08-12T00:00:00Z",
                "provider": "Test Provider",
                "endpoint": "https://invalid.example",
                "credential_environment": ["NEVER_SET_TEST_KEY"],
                "minimum_importance": 2,
                "require_exact_release_time": True,
                "source_verified": True,
            }
        ),
        encoding="utf-8",
    )
    common = {
        "Date": "2026-08-12T13:30:00Z",
        "Symbol": "US_CPI",
        "ForecastValue": 2.8,
        "Importance": 3,
        "DateSpan": 0,
    }
    raw = json.dumps(
        [
            {**common, "LastUpdate": "2026-08-12T11:59:00Z"},
            {**common, "CalendarId": "future"},
        ]
    ).encode()
    result = run_once(
        config_path=config,
        database=tmp_path / "db.sqlite",
        output=tmp_path / "state.json",
        import_path=tmp_path / "import.jsonl",
        report=tmp_path / "report.md",
        archive=tmp_path / "archive",
        observed=dt.datetime(2026, 8, 12, 12, tzinfo=UTC),
        raw=raw,
    )
    assert result["totals"]["causal_observations"] == 0
    assert result["totals"]["rejection_reasons"] == {
        "missing_provider_calendar_id": 1,
        "missing_provider_last_update": 1,
    }


def test_request_clock_cannot_begin_after_response_clock(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_TE_KEY", "present")
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "source_contract_id": "chronology-v2",
                "cohort_start_utc": "2026-08-12T00:00:00Z",
                "provider": "Test Provider",
                "endpoint": "https://invalid.example",
                "credential_environment": ["TEST_TE_KEY"],
                "minimum_importance": 2,
                "require_exact_release_time": True,
                "source_verified": True,
            }
        ),
        encoding="utf-8",
    )
    clocks = iter(
        (
            dt.datetime(2026, 8, 12, 12, 0, 10, tzinfo=UTC),
            dt.datetime(2026, 8, 12, 12, 0, 0, tzinfo=UTC),
        )
    )
    monkeypatch.setattr(
        consensus,
        "normalized_observation_time",
        lambda _: (
            next(clocks),
            {
                "contract_id": consensus.OBSERVATION_TIME_CONTRACT_ID,
                "trusted_for_prospective_evidence": True,
            },
        ),
    )
    monkeypatch.setattr(
        consensus,
        "fetch",
        lambda *_: json.dumps(
            [
                {
                    "CalendarId": "future",
                    "Date": "2026-08-12T13:30:00Z",
                    "LastUpdate": "2026-08-12T11:59:00Z",
                    "Symbol": "US_CPI",
                    "ForecastValue": 2.8,
                    "Importance": 3,
                    "DateSpan": 0,
                }
            ]
        ).encode(),
    )
    result = run_once(
        config_path=config,
        database=tmp_path / "db.sqlite",
        output=tmp_path / "state.json",
        import_path=tmp_path / "import.jsonl",
        report=tmp_path / "report.md",
        archive=tmp_path / "archive",
    )
    assert result["totals"]["rejection_reasons"] == {
        "request_started_after_response_completed": 1
    }


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
