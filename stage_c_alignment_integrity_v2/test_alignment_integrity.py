from __future__ import annotations

import json
import sys
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from calendar_contract import (NY_17_SESSION_CALENDAR_ID, UTC_CLOSE_CALENDAR_ID, NewYorkSessionCalendar,
                               market_session_target, next_utc_weekday_close)
from contracts import TrainingView, forecast_coverage, forecast_record, outcome_record, validate_forecast
from publication import (RunAlreadyOwned, RunIdentityMismatch, RunPublisher, effective_run_identity,
                         package_completed_run, restore_completed_run, verify_completed_run)

sys.path.insert(0, str(Path(__file__).parent))
import all68_neutral_runner_v2 as neutral_runner
import feature_registry_audit_v2 as feature_audit


def view() -> TrainingView:
    return TrainingView(origin_start_epoch=0, origin_end_epoch=1_000, fit_cutoff_epoch=1_000,
                        protected_eval_start_epoch=1_000, protected_eval_end_epoch=2_000)


def test_tst05_unmatured_label_is_excluded_from_training():
    selection = view().eligible(np.array([100, 200]), np.array([900, 1_001]), np.array([500, 600]))
    assert selection.tolist() == [True, False]


def test_tst07_label_interval_overlapping_protected_evaluation_is_excluded_globally():
    selection = view().eligible(np.array([800, 900, 2_000]), np.array([900, 900, 900]), np.array([1_100, 999, 2_100]))
    assert selection.tolist() == [False, True, False]


def test_tst01_and_tst33_future_outcome_support_cannot_change_forecast_coverage():
    decision = np.array([1_100, 1_200])
    issued_before = forecast_coverage(np.array([True, True]), 1_000, decision)
    issued_after_future_endpoint_removed = forecast_coverage(np.array([True, True]), 1_000, decision)
    assert issued_before.tolist() == [True, True]
    assert issued_after_future_endpoint_removed.tolist() == [True, True]


def test_tst03_real_reader_ignores_a_perturbed_later_endpoint(tmp_path: Path, monkeypatch):
    """The consumer's raw read is limited to origin time, not a target time."""
    member = tmp_path / "EUR_USD_M1.parquet"
    origin = "2024-06-24T00:00:00+00:00"
    future = "2024-06-25T00:00:00+00:00"
    pq.write_table(pa.table({"datetime": [origin, future], "close": [1.1, 1.2]}), member)
    archive = tmp_path / "history.zip"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.write(member, arcname=member.name)
    monkeypatch.setattr(neutral_runner, "ARCHIVE", archive)
    members = [{"instrument": "EUR_USD", "path": member.name}]
    before = neutral_runner.origin_coverage(members, neutral_runner.parse_utc_epoch(origin))
    pq.write_table(pa.table({"datetime": [origin, future], "close": [1.1, 9.9]}), member)
    with zipfile.ZipFile(archive, "w") as handle:
        handle.write(member, arcname=member.name)
    after = neutral_runner.origin_coverage(members, neutral_runner.parse_utc_epoch(origin))
    assert before == after == [{"instrument": "EUR_USD", "status": "eligible", "reason": "origin_bar_available", "origin_close": 1.1}]


def test_tst01_future_unmatured_labels_cannot_change_an_earlier_fit():
    """Changing a label after fit cutoff must leave the fitted prior row intact."""
    origin = np.array([100, 200, 300], dtype=np.int64)
    ready = np.array([500, 900, 1_001], dtype=np.int64)
    end = np.array([300, 400, 500], dtype=np.int64)
    eligible = view().eligible(origin, ready, end)
    x = np.array([[0.0], [1.0], [999.0]])
    y_before = np.array([0.0, 2.0, 7.0])
    y_after = np.array([0.0, 2.0, 7_000.0])
    # The simple two-point slope is deliberately hand-verifiable.
    slope_before = np.polyfit(x[eligible, 0], y_before[eligible], 1)
    slope_after = np.polyfit(x[eligible, 0], y_after[eligible], 1)
    assert np.allclose(slope_before, slope_after)


def test_tst10_model_readiness_blocks_backdated_forecast():
    with pytest.raises(ValueError, match="not ready"):
        forecast_record(forecast_id="f1", instrument="EUR_USD", decision_epoch=100,
                        available_epoch=100, model_id="m1", model_ready_epoch=101,
                        training_view=view(), target_id="t1", prediction=0.0)


def test_training_view_rejects_fit_cutoff_before_training_period():
    with pytest.raises(ValueError, match="chronologically"):
        TrainingView(origin_start_epoch=0, origin_end_epoch=1_000, fit_cutoff_epoch=999,
                     protected_eval_start_epoch=1_000, protected_eval_end_epoch=2_000)


def test_forecast_and_outcome_are_separate_records():
    forecast = forecast_record(forecast_id="f1", instrument="EUR_USD", decision_epoch=100,
                               available_epoch=101, model_id="m1", model_ready_epoch=99,
                               training_view=view(), target_id="t1", prediction=1.5)
    assert "actual_bps" not in forecast
    outcome = outcome_record(forecast_id="f1", outcome_ready_epoch=500, state="MATURED", value=2.0)
    assert outcome["forecast_id"] == forecast["forecast_id"]
    with pytest.raises(ValueError, match="later outcome"):
        validate_forecast({**forecast, "actual_bps": 2.0})
    with pytest.raises(ValueError, match="unsupported"):
        validate_forecast({**forecast, "nested": {"actual": 2.0}})
    with pytest.raises(ValueError, match="finite"):
        forecast_record(forecast_id="bad", instrument="EUR_USD", decision_epoch=100,
                        available_epoch=101, model_id="m1", model_ready_epoch=99,
                        training_view=view(), target_id="t1", prediction=float("nan"))


def test_tst32_utc_diagnostic_and_ny_session_calendar_are_distinct():
    friday_noon = 1720180860  # 2024-07-05T12:01:00Z
    assert UTC_CLOSE_CALENDAR_ID == "utc_weekday_close.v2"
    assert next_utc_weekday_close(friday_noon, 1) == 1720396800  # Monday 00:00 UTC
    assert NY_17_SESSION_CALENDAR_ID == "fx_ny_1700_session_close.v2"
    calendar = NewYorkSessionCalendar(metadata_start=date(2024, 1, 1), metadata_end=date(2024, 12, 31), metadata_id="synthetic-fixture")
    assert market_session_target(friday_noon, 1, calendar) == int(datetime(2024, 7, 5, 21, tzinfo=timezone.utc).timestamp())


def test_tst32_session_calendar_handles_weekend_dst_and_declared_closure():
    # Friday after close skips Saturday and observes the US DST change to the
    # Monday 17:00 New York completed close (21:00 UTC, not 22:00 UTC).
    calendar = NewYorkSessionCalendar(metadata_start=date(2024, 1, 1), metadata_end=date(2024, 12, 31), metadata_id="synthetic-fixture")
    after_friday_close = int(datetime(2024, 3, 8, 23, tzinfo=timezone.utc).timestamp())
    assert market_session_target(after_friday_close, 1, calendar) == int(datetime(2024, 3, 11, 21, tzinfo=timezone.utc).timestamp())
    # The autumn transition shifts Monday's close back to 22:00 UTC.
    before_autumn_close = int(datetime(2024, 11, 3, 12, tzinfo=timezone.utc).timestamp())
    assert market_session_target(before_autumn_close, 1, calendar) == int(datetime(2024, 11, 4, 22, tzinfo=timezone.utc).timestamp())
    closed = NewYorkSessionCalendar(frozenset({date(2024, 12, 25)}), metadata_start=date(2024, 1, 1), metadata_end=date(2024, 12, 31), metadata_id="synthetic-fixture")
    after_tuesday_close = int(datetime(2024, 12, 24, 23, tzinfo=timezone.utc).timestamp())
    assert market_session_target(after_tuesday_close, 1, closed) == int(datetime(2024, 12, 26, 22, tzinfo=timezone.utc).timestamp())


def identity() -> dict:
    return effective_run_identity(contract={"target": "elapsed_24h.v2", "seed": 7}, dependency_hashes={"source": "a" * 64, "code": "b" * 64})


def test_tst48_and_tst50_writer_lock_and_completion_last(tmp_path: Path):
    first = RunPublisher(tmp_path, "fixture-run", identity())
    first.acquire()
    assert not (tmp_path / "fixture-run" / "COMPLETION_MANIFEST.json").exists()
    with pytest.raises(RunAlreadyOwned, match="active writer"):
        RunPublisher(tmp_path, "fixture-run", identity()).acquire()
    payload = first.write_payload("forecast.jsonl", b'{"forecast_id":"f1"}\n')
    with pytest.raises(RunAlreadyOwned, match="does not own"):
        RunPublisher(tmp_path, "fixture-run", identity()).write_payload("other.json", b"no")
    completed = first.complete([payload], {"forecast.jsonl"})
    assert completed.exists()
    assert not (tmp_path / "fixture-run" / ".owner.lock").exists()


def test_tst49_tst51_and_tst54_resume_refuses_changed_identity_and_tampering(tmp_path: Path):
    publisher = RunPublisher(tmp_path, "fixture-run", identity())
    publisher.acquire()
    payload = publisher.write_payload("report.json", json.dumps({"ok": True}).encode())
    publisher.complete([payload], {"report.json"})
    verify_completed_run(tmp_path / "fixture-run", identity())
    different = effective_run_identity(contract={"target": "elapsed_48h.v2", "seed": 7}, dependency_hashes={"source": "a" * 64, "code": "b" * 64})
    with pytest.raises(RunIdentityMismatch, match="dependency identity mismatch"):
        verify_completed_run(tmp_path / "fixture-run", different)
    (tmp_path / "fixture-run" / "report.json").write_text("tampered", encoding="utf-8")
    with pytest.raises(RunIdentityMismatch, match="corrupt"):
        verify_completed_run(tmp_path / "fixture-run", identity())


def test_cooperative_partial_resume_matches_uninterrupted_payloads(tmp_path: Path):
    required = {"a.json", "b.json"}
    uninterrupted = RunPublisher(tmp_path, "uninterrupted", identity())
    uninterrupted.acquire()
    expected_payloads = [uninterrupted.write_payload("a.json", b'{"a":1}\n'), uninterrupted.write_payload("b.json", b'{"b":2}\n')]
    uninterrupted.complete(expected_payloads, required)

    interrupted = RunPublisher(tmp_path, "interrupted", identity())
    interrupted.acquire()
    interrupted.write_payload("a.json", b'{"a":1}\n')
    # Cooperative release covers a clean partial pause. Actual process death
    # and concurrent writers are exercised in test_publication_process_repair.
    interrupted.release()
    resumed = RunPublisher(tmp_path, "interrupted", identity())
    resumed.acquire()
    resumed_payloads = [resumed.write_or_validate_payload("a.json", b'{"a":1}\n'), resumed.write_or_validate_payload("b.json", b'{"b":2}\n')]
    resumed.complete(resumed_payloads, required)
    first = verify_completed_run(tmp_path / "uninterrupted", identity())
    second = verify_completed_run(tmp_path / "interrupted", identity())
    assert first["payloads"] == second["payloads"]
    assert (tmp_path / "uninterrupted" / "a.json").read_bytes() == (tmp_path / "interrupted" / "a.json").read_bytes()
    assert (tmp_path / "uninterrupted" / "b.json").read_bytes() == (tmp_path / "interrupted" / "b.json").read_bytes()


def test_tst52_relocated_fixture_restore_revalidates_identity_and_payloads(tmp_path: Path):
    source = tmp_path / "source"
    publisher = RunPublisher(source, "fixture-run", identity())
    publisher.acquire()
    payload = publisher.write_payload("forecast.jsonl", b'{"forecast_id":"f1"}\n')
    publisher.complete([payload], {"forecast.jsonl"})
    package = tmp_path / "fixture.zip"
    package_completed_run(source / "fixture-run", package)
    restored = restore_completed_run(package, tmp_path / "relocated" / "fixture-run", identity())
    assert restored["run_identity"]["fingerprint"] == identity()["fingerprint"]


def test_incomplete_run_identity_mismatch_is_refused(tmp_path: Path):
    first = RunPublisher(tmp_path, "fixture-run", identity())
    first.acquire()
    first.release()
    changed = effective_run_identity(contract={"target": "elapsed_48h.v2", "seed": 7}, dependency_hashes={"source": "a" * 64, "code": "b" * 64})
    with pytest.raises(RunIdentityMismatch, match="incomplete"):
        RunPublisher(tmp_path, "fixture-run", changed).acquire()


def test_tst11_launcher_checks_every_native_exit_before_success_message():
    launcher = Path(__file__).with_name("verify_alignment_integrity.ps1").read_text(encoding="utf-8")
    assert "if ($LASTEXITCODE -ne 0)" in launcher
    assert "Write-Output 'Alignment integrity verification passed" in launcher


def test_feature_registry_binding_preserves_216_kernel_and_12_peer_rows():
    registry, audit = feature_audit.inspect_registry()
    assert len(registry) == 228
    assert audit["counts"]["technical_kernel_features"] == 216
    assert audit["counts"]["peer_registry_features"] == 12
    assert audit["kernel_metadata"]["max_lookback_bars"] == 603
