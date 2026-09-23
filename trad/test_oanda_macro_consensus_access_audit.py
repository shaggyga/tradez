import hashlib
import json

import oanda_macro_consensus_access_audit as audit


def test_finnhub_forbidden_is_explicit_not_misreported_as_missing_key():
    assert audit.classify_finnhub(403, None) == (
        "credential_present_but_premium_calendar_forbidden"
    )


def test_finnhub_http_200_requires_at_least_one_future_numeric_estimate():
    assert audit.classify_finnhub(200, 12, 3) == (
        "calendar_accessible_future_estimates_present"
    )
    assert audit.classify_finnhub(200, 12, 0) == (
        "calendar_accessible_no_future_estimates_in_probe"
    )
    assert audit.classify_finnhub(200, 0, 0) == (
        "calendar_accessible_no_future_estimates_in_probe"
    )


def test_trading_economics_access_requires_future_consensus_values():
    assert audit.classify_trading_economics(200, 12, 4) == (
        "calendar_accessible_future_consensus_present"
    )
    assert audit.classify_trading_economics(200, 12, 0) == (
        "calendar_accessible_no_future_consensus_in_probe"
    )
    assert audit.classify_trading_economics(403, None, None) == (
        "credential_present_but_calendar_access_forbidden"
    )


def test_offline_audit_never_persists_credentials(tmp_path):
    output = tmp_path / "audit.json"
    report = tmp_path / "audit.md"
    secret = "sensitive-value"
    result = audit.run(
        output,
        report,
        environment={
            "FINNHUB_API_KEY": secret,
            "ALPHA_VANTAGE_API_KEY": secret,
            "FRED_API_KEY": secret,
        },
        probe_network=False,
        import_path=tmp_path / "missing.jsonl",
        collector_state_path=tmp_path / "missing.json",
    )
    assert result["secrets_persisted"] is False
    assert secret not in output.read_text()
    assert secret not in report.read_text()
    assert result["status"] == "blocked_no_permitted_pre_release_consensus_access"
    assert result["causal_consensus_ready"] is False
    assert result["import_audit"]["state"] == "absent"


def test_import_audit_separates_causal_postrelease_and_missing_provenance(tmp_path):
    path = tmp_path / "consensus.jsonl"
    archive = tmp_path / "archive"
    archive.mkdir()
    raw = json.dumps(
        [
            {
                "CalendarId": "event",
                "Date": "2026-09-01T13:30:00+00:00",
                "LastUpdate": "2026-09-01T12:58:00+00:00",
                "Symbol": "US_CPI",
                "ForecastValue": 2.8,
                "DateSpan": 0,
            }
        ]
    ).encode()
    snapshot_sha = hashlib.sha256(raw).hexdigest()
    (archive / f"{snapshot_sha}.json").write_bytes(raw)
    expected = {
        "source_contract_id": "contract",
        "cohort_id": "cohort",
        "cohort_start_utc": "2026-08-29T00:00:00+00:00",
        "capture_contract_id": "capture",
        "observation_clock_contract_id": "clock",
        "provider": "Test Provider",
    }
    base = {
        "observation_id": "one",
        "cohort_id": "cohort",
        "source_contract_id": "contract",
        "capture_contract_id": "capture",
        "provider_event_id": "event",
        "provider_event_version": "2026-09-01T12:58:00+00:00",
        "provider_snapshot_sha256": snapshot_sha,
        "event_series_id": "US_CPI",
        "scheduled_utc": "2026-09-01T13:30:00+00:00",
        "captured_utc": "2026-09-01T13:00:00+00:00",
        "request_started_utc": "2026-09-01T12:59:59+00:00",
        "response_completed_utc": "2026-09-01T13:00:00+00:00",
        "source_timestamp_utc": "2026-09-01T12:58:00+00:00",
        "capture_clock_contract_id": "clock",
        "capture_clock_trusted": True,
        "release_time_precision": "exact",
        "actual_present_at_capture": False,
        "source_verified": True,
        "consensus_value": 2.8,
    }
    post = {
        **base,
        "observation_id": "two",
        "captured_utc": "2026-09-01T13:31:00+00:00",
        "request_started_utc": "2026-09-01T13:30:59+00:00",
        "response_completed_utc": "2026-09-01T13:31:00+00:00",
    }
    wrong_contract = {**base, "observation_id": "three", "cohort_id": "other"}
    source_after_capture = {
        **base,
        "observation_id": "four",
        "source_timestamp_utc": "2026-09-01T13:01:00+00:00",
        "provider_event_version": "2026-09-01T13:01:00+00:00",
    }
    uppercase_sha = {
        **base,
        "observation_id": "five",
        "provider_snapshot_sha256": snapshot_sha.upper(),
    }
    missing_archive = {
        **base,
        "observation_id": "six",
        "provider_snapshot_sha256": "b" * 64,
    }
    missing = {"event_series_id": "US_CPI", "consensus_value": 2.8}
    path.write_text(
        "\n".join(
            json.dumps(row)
            for row in (
                base,
                post,
                wrong_contract,
                source_after_capture,
                uppercase_sha,
                missing_archive,
                missing,
            )
        )
        + "\n",
        encoding="utf-8",
    )
    result = audit.audit_import(
        path,
        audit.dt.datetime(2026, 8, 29, tzinfo=audit.UTC),
        archive_path=archive,
        expected_contract=expected,
    )
    assert result["causal_v2_eligible"] == 1
    assert result["post_release"] == 1
    assert result["missing_v2_provenance"] == 1
    assert result["frozen_contract_mismatch"] == 1
    assert result["source_timestamp_after_capture"] == 1
    assert result["invalid_snapshot_sha256"] == 1
    assert result["archive_missing_or_mismatch"] == 1
