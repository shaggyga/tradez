from __future__ import annotations

import json
import hashlib
import multiprocessing
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from trad.oanda_independent_evidence_verifier import (
    PUBLICATION_CONTRACT,
    VerificationPassSuperseded,
    _claim_verification_pass,
    _allocator_registry_replay,
    _current_published_state_ages,
    _edge_report_freshness,
    _independently_confirmed,
    _integrity_evidence,
    _lifecycle_database_integrity_snapshot,
    _lifecycle_publication_freshness,
    _mark_guard_failed,
    _publish_owned_final,
    _proof_registry_replay,
    _start_verification_progress_heartbeat,
    atomic_json,
    verifier_guard_path,
    verify,
)


def _lifecycle_freshness_fixture(tmp_path: Path, generated_utc: str) -> tuple[Path, Path, Path]:
    database = tmp_path / "lifecycle.sqlite"
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE hypotheses(
          hypothesis_id TEXT PRIMARY KEY,cohort_id TEXT,definition_sha256 TEXT
        );
        CREATE TABLE lifecycle_events(
          hypothesis_id TEXT,next_state TEXT,observed_utc TEXT,
          source_run_id TEXT,evidence_json TEXT
        );
        CREATE TABLE futility_retirements(hypothesis_id TEXT PRIMARY KEY);
        CREATE TABLE reconsideration_decisions(decision_id TEXT PRIMARY KEY);
        CREATE TRIGGER hypotheses_no_update BEFORE UPDATE ON hypotheses
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER hypotheses_no_delete BEFORE DELETE ON hypotheses
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER lifecycle_events_no_update BEFORE UPDATE ON lifecycle_events
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER lifecycle_events_no_delete BEFORE DELETE ON lifecycle_events
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER futility_retirements_no_update BEFORE UPDATE ON futility_retirements
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER futility_retirements_no_delete BEFORE DELETE ON futility_retirements
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        INSERT INTO hypotheses VALUES('h1','cohort','definition');
        INSERT INTO lifecycle_events VALUES(
          'h1','continue_collecting','2026-09-02T00:00:00+00:00','run','{}'
        );
        """
    )
    integrity = _lifecycle_database_integrity_snapshot(connection)
    connection.commit()
    connection.close()
    state = tmp_path / "lifecycle.json"
    state.write_text(
        json.dumps(
            {
                "generated_utc": generated_utc,
                "research_only": True,
                "can_place_orders": False,
                "can_promote": False,
                "real_money_routing": False,
                "lifecycle": {"hypothesis_count": 1, "states": integrity["current_states"]},
                "integrity": integrity,
            }
        ),
        encoding="utf-8",
    )
    worker = tmp_path / "worker.json"
    return database, state, worker


def _write_healthy_lifecycle_worker(path: Path, now: str) -> None:
    path.write_text(
        json.dumps(
            {
                "generated_utc": now,
                "last_progress_utc": now,
                "progress_age_sec": 1.0,
                "status": "running",
                "phase": "running_lifecycle",
                "errors": 0,
                "last_error": "",
                "research_only": True,
                "can_place_orders": False,
                "can_submit_orders": False,
                "can_promote": False,
                "real_money_routing": False,
            }
        ),
        encoding="utf-8",
    )


def _minimal_final_payload(ownership: dict, status: str) -> dict:
    completed = datetime.now(timezone.utc).isoformat()
    return {
        "schema_version": 2,
        "publication_contract": PUBLICATION_CONTRACT,
        "generated_utc": completed,
        "completed_verification_utc": completed,
        "verification_pass_id": ownership["pass_id"],
        "verification_owner_id": ownership["owner_id"],
        "publication_generation": ownership["generation"],
        "status": status,
        "authorization_safe": status == "match",
        "can_place_orders": False,
        "can_promote": False,
        "verified_confirmed_candidates": [],
    }


def _overlap_publication_worker(
    guard: str,
    state: str,
    report: str,
    status: str,
    ready: multiprocessing.Queue,
    release: multiprocessing.Event,
    result: multiprocessing.Queue,
) -> None:
    ownership = _claim_verification_pass(Path(guard))
    ready.put(
        (
            ownership["generation"],
            ownership["pass_id"],
            ownership["owner_id"],
        )
    )
    release.wait(10.0)
    try:
        _publish_owned_final(
            state_path=Path(state),
            report_path=Path(report),
            guard_path=Path(guard),
            ownership=ownership,
            payload=_minimal_final_payload(ownership, status),
            report=status,
        )
    except VerificationPassSuperseded:
        result.put("superseded")
    else:
        result.put("published")


def _claim_then_crash_worker(guard: str, marker: str) -> None:
    _claim_verification_pass(Path(guard))
    Path(marker).write_text("claimed", encoding="utf-8")
    os._exit(7)


def test_confirmation_requires_all_independent_facts() -> None:
    row = {
        "evidence_json": json.dumps(
            {
                "hierarchical_fdr_survivor": True,
                "untouched_confirmation_passed": True,
                "cost_stress_passed": True,
                "concentration_stress_passed": True,
                "time_uniform_lower_bound_pips": 0.8,
                "minimum_economic_edge_pips": 0.5,
            }
        )
    }
    assert _independently_confirmed(row) == (
        True,
        "independent_confirmation_facts_pass",
    )
    row["evidence_json"] = json.dumps({"untouched_confirmation_passed": True})
    passed, reason = _independently_confirmed(row)
    assert passed is False
    assert reason.startswith("missing_independent_facts:")


def test_confirmation_rejects_inadequate_lower_bound() -> None:
    row = {
        "evidence_json": json.dumps(
            {
                "hierarchical_fdr_survivor": True,
                "untouched_confirmation_passed": True,
                "cost_stress_passed": True,
                "concentration_stress_passed": True,
                "time_uniform_lower_bound_pips": 0.49,
                "minimum_economic_edge_pips": 0.5,
            }
        )
    }
    assert _independently_confirmed(row) == (
        False,
        "time_uniform_lower_bound_below_minimum_edge",
    )


def test_publication_freshness_is_read_at_gate_time(tmp_path) -> None:
    stale = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    fresh = datetime.now(timezone.utc).isoformat()
    lifecycle = tmp_path / "lifecycle.json"
    allocator = tmp_path / "allocator.json"
    edge = tmp_path / "edge.json"

    # These stand in for the pass-start payloads retained for substantive
    # reconstruction.  The live publishers refresh the files during a long
    # verifier pass, before the freshness gate is evaluated.
    pass_start_payloads = {
        "lifecycle": {"generated_utc": stale},
        "allocator": {"generated_utc": stale},
        "edge": {"generated_utc": stale},
    }
    for path in (lifecycle, allocator, edge):
        path.write_text(json.dumps({"generated_utc": fresh}), encoding="utf-8")

    ages = _current_published_state_ages(
        lifecycle_state=lifecycle,
        allocator_state=allocator,
        edge_state=edge,
    )

    assert pass_start_payloads["lifecycle"]["generated_utc"] == stale
    assert ages["lifecycle_generated_utc"] == fresh
    assert ages["allocator_generated_utc"] == fresh
    assert ages["edge_generated_utc"] == fresh
    assert 0.0 <= float(ages["lifecycle_age_sec"]) < 30.0
    assert 0.0 <= float(ages["allocator_age_sec"]) < 30.0
    assert 0.0 <= float(ages["edge_age_sec"]) < 30.0


def test_old_lifecycle_publication_is_current_only_during_exact_safe_rebuild(
    tmp_path,
) -> None:
    stale = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    now = datetime.now(timezone.utc).isoformat()
    database, state, worker = _lifecycle_freshness_fixture(tmp_path, stale)
    _write_healthy_lifecycle_worker(worker, now)

    result = _lifecycle_publication_freshness(
        lifecycle_state=state,
        lifecycle_database=database,
        worker_state=worker,
        maximum_age_sec=1800.0,
        maximum_snapshot_lag_sec=7200.0,
    )
    assert result["fresh"] is True
    assert result["reason"] == "exact_fail_closed_lifecycle_snapshot_while_rebuilding"
    assert result["handshake"]["database_integrity_match"] is True
    assert result["handshake"]["zero_confirmed_candidates"] is True


def test_old_lifecycle_publication_rejects_db_change_and_confirmation(tmp_path) -> None:
    stale = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    now = datetime.now(timezone.utc).isoformat()
    database, state, worker = _lifecycle_freshness_fixture(tmp_path, stale)
    _write_healthy_lifecycle_worker(worker, now)
    connection = sqlite3.connect(database)
    connection.execute(
        "INSERT INTO lifecycle_events VALUES(?,?,?,?,?)",
        (
            "h1",
            "confirmed_candidate",
            now,
            "new-run",
            json.dumps(
                {
                    "hierarchical_fdr_survivor": True,
                    "untouched_confirmation_passed": True,
                    "cost_stress_passed": True,
                    "concentration_stress_passed": True,
                    "time_uniform_lower_bound_pips": 1.0,
                    "minimum_economic_edge_pips": 0.5,
                }
            ),
        ),
    )
    connection.commit()
    connection.close()

    result = _lifecycle_publication_freshness(
        lifecycle_state=state,
        lifecycle_database=database,
        worker_state=worker,
        maximum_age_sec=1800.0,
        maximum_snapshot_lag_sec=7200.0,
    )
    assert result["fresh"] is False
    assert result["handshake"]["database_integrity_match"] is False
    assert result["handshake"]["zero_confirmed_candidates"] is False


def test_old_lifecycle_publication_rejects_stale_or_capable_worker(tmp_path) -> None:
    stale = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    now = datetime.now(timezone.utc).isoformat()
    database, state, worker = _lifecycle_freshness_fixture(tmp_path, stale)
    worker.write_text("{}", encoding="utf-8")

    result = _lifecycle_publication_freshness(
        lifecycle_state=state,
        lifecycle_database=database,
        worker_state=worker,
        maximum_age_sec=1800.0,
        maximum_snapshot_lag_sec=7200.0,
    )
    assert result["fresh"] is False
    assert result["handshake"]["worker_fresh"] is False

    _write_healthy_lifecycle_worker(worker, now)
    payload = json.loads(worker.read_text(encoding="utf-8"))
    payload["can_place_orders"] = True
    worker.write_text(json.dumps(payload), encoding="utf-8")
    result = _lifecycle_publication_freshness(
        lifecycle_state=state,
        lifecycle_database=database,
        worker_state=worker,
        maximum_age_sec=1800.0,
        maximum_snapshot_lag_sec=7200.0,
    )
    assert result["fresh"] is False
    assert result["handshake"]["worker_fail_closed"] is False


def test_fresh_exact_lifecycle_publication_does_not_need_worker_grace(tmp_path) -> None:
    now = datetime.now(timezone.utc).isoformat()
    database, state, worker = _lifecycle_freshness_fixture(tmp_path, now)
    worker.write_text("{}", encoding="utf-8")
    result = _lifecycle_publication_freshness(
        lifecycle_state=state,
        lifecycle_database=database,
        worker_state=worker,
        maximum_age_sec=1800.0,
        maximum_snapshot_lag_sec=7200.0,
    )
    assert result["fresh"] is True
    assert result["reason"] == "fresh_exact_lifecycle_publication"


def test_old_edge_report_is_current_only_with_exact_unchanged_handshake(tmp_path) -> None:
    stale = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    fresh = datetime.now(timezone.utc).isoformat()
    report = tmp_path / "edge.json"
    markdown = tmp_path / "edge.md"
    worker = tmp_path / "worker.json"
    checkpoint = tmp_path / "checkpoint.json"
    report.write_text(
        json.dumps(
            {
                "generated_utc": stale,
                "run_id": "edge_one",
                "research_only": True,
                "can_place_orders": False,
                "can_promote": False,
                "integrity": {"source_highwater_row_id": 42},
            }
        ),
        encoding="utf-8",
    )
    markdown.write_text("frozen report", encoding="utf-8")
    worker.write_text(
        json.dumps(
            {
                "generated_utc": fresh,
                "updated_at": fresh,
                "status": "running",
                "phase": "idle_unchanged_inputs",
                "errors": 0,
                "last_error": "",
                "details": {"input_fingerprint_sha256": "same"},
            }
        ),
        encoding="utf-8",
    )
    checkpoint.write_text(
        json.dumps(
            {
                "fingerprint_sha256": "same",
                "last_completed_report": {
                    "run_id": "edge_one",
                    "source_highwater_row_id": 42,
                },
                "output_integrity": {
                    str(report.resolve()): {
                        "size_bytes": report.stat().st_size,
                        "sha256": hashlib.sha256(report.read_bytes()).hexdigest(),
                    },
                    str(markdown.resolve()): {
                        "size_bytes": markdown.stat().st_size,
                        "sha256": hashlib.sha256(markdown.read_bytes()).hexdigest(),
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    result = _edge_report_freshness(
        edge_state=report,
        worker_state=worker,
        input_checkpoint=checkpoint,
        maximum_age_sec=1800.0,
    )
    assert result["fresh"] is True
    assert result["reason"] == "semantically_current_unchanged_inputs"

    markdown.write_text("mutated report", encoding="utf-8")
    result = _edge_report_freshness(
        edge_state=report,
        worker_state=worker,
        input_checkpoint=checkpoint,
        maximum_age_sec=1800.0,
    )
    assert result["fresh"] is False
    assert result["handshake"]["output_integrity_match"] is False


def test_old_edge_report_is_bounded_safe_while_healthy_worker_rebuilds(tmp_path) -> None:
    stale = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
    fresh = datetime.now(timezone.utc).isoformat()
    report = tmp_path / "edge.json"
    markdown = tmp_path / "edge.md"
    worker = tmp_path / "worker.json"
    checkpoint = tmp_path / "checkpoint.json"
    report.write_text(json.dumps({
        "generated_utc": stale,
        "run_id": "edge_snapshot",
        "research_only": True,
        "can_place_orders": False,
        "can_promote": False,
        "integrity": {"source_highwater_row_id": 100},
    }), encoding="utf-8")
    markdown.write_text("frozen report", encoding="utf-8")
    worker.write_text(json.dumps({
        "updated_at": fresh,
        "status": "running",
        "phase": "loading_rows",
        "progress_age_sec": 2.0,
        "errors": 0,
        "last_error": "",
        "details": {"input_fingerprint_sha256": "newer"},
    }), encoding="utf-8")
    checkpoint.write_text(json.dumps({
        "fingerprint_sha256": "snapshot",
        "last_completed_report": {
            "run_id": "edge_snapshot", "source_highwater_row_id": 100,
        },
        "output_integrity": {
            str(report.resolve()): {
                "size_bytes": report.stat().st_size,
                "sha256": hashlib.sha256(report.read_bytes()).hexdigest(),
            },
            str(markdown.resolve()): {
                "size_bytes": markdown.stat().st_size,
                "sha256": hashlib.sha256(markdown.read_bytes()).hexdigest(),
            },
        },
    }), encoding="utf-8")
    result = _edge_report_freshness(
        edge_state=report,
        worker_state=worker,
        input_checkpoint=checkpoint,
        maximum_age_sec=1800.0,
        maximum_snapshot_lag_sec=86400.0,
    )
    assert result["fresh"] is True
    assert result["reason"] == "verified_bounded_snapshot_while_catching_up"

    # A heartbeat that is alive but no longer making calculation progress
    # cannot keep an old snapshot accepted.
    payload = json.loads(worker.read_text(encoding="utf-8"))
    payload["progress_age_sec"] = 7200.0
    worker.write_text(json.dumps(payload), encoding="utf-8")
    result = _edge_report_freshness(
        edge_state=report,
        worker_state=worker,
        input_checkpoint=checkpoint,
        maximum_age_sec=1800.0,
        maximum_snapshot_lag_sec=86400.0,
    )
    assert result["fresh"] is False
    assert result["handshake"]["worker_progress_recent"] is False


def test_proof_registry_replay_rejects_published_reactivation(tmp_path) -> None:
    database = tmp_path / "proof.sqlite"
    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE proof_cohorts(cohort_id TEXT PRIMARY KEY,family TEXT NOT NULL);
        CREATE TABLE proof_cohort_transitions(
          transition_id TEXT PRIMARY KEY,family TEXT NOT NULL,
          previous_cohort_id TEXT,next_cohort_id TEXT NOT NULL);
        CREATE TRIGGER proof_cohorts_no_update BEFORE UPDATE ON proof_cohorts
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER proof_cohorts_no_delete BEFORE DELETE ON proof_cohorts
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER proof_transitions_no_update BEFORE UPDATE ON proof_cohort_transitions
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER proof_transitions_no_delete BEFORE DELETE ON proof_cohort_transitions
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        INSERT INTO proof_cohorts VALUES ('a1','family');
        INSERT INTO proof_cohorts VALUES ('a2','family');
        INSERT INTO proof_cohort_transitions VALUES ('t1','family',NULL,'a1');
        INSERT INTO proof_cohort_transitions VALUES ('t2','family','a1','a2');
        """
    )
    passed, evidence = _proof_registry_replay(
        connection, {"active_cohorts": {"family": "a1"}}
    )
    connection.close()
    assert passed is False
    assert evidence["replayed_active_cohorts"] == {"family": "a2"}
    assert "published_active_cohorts_do_not_match_transition_replay" in evidence["errors"]


def test_allocator_registry_replay_rejects_old_published_head(tmp_path) -> None:
    connection = sqlite3.connect(tmp_path / "allocator.sqlite")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE allocator_cohorts(
          cohort_id TEXT PRIMARY KEY,policy_id TEXT NOT NULL,phase TEXT NOT NULL);
        CREATE TABLE allocator_cohort_transitions(
          transition_id TEXT PRIMARY KEY,policy_id TEXT NOT NULL,
          previous_cohort_id TEXT,next_cohort_id TEXT NOT NULL);
        CREATE TABLE decisions(x TEXT); CREATE TABLE decision_candidates(x TEXT);
        CREATE TABLE comparator_selections(x TEXT);
        CREATE TRIGGER allocator_cohorts_no_update BEFORE UPDATE ON allocator_cohorts
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER allocator_cohorts_no_delete BEFORE DELETE ON allocator_cohorts
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER allocator_transitions_no_update BEFORE UPDATE ON allocator_cohort_transitions
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER allocator_transitions_no_delete BEFORE DELETE ON allocator_cohort_transitions
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER decisions_no_update BEFORE UPDATE ON decisions
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER decisions_no_delete BEFORE DELETE ON decisions
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER decision_candidates_no_update BEFORE UPDATE ON decision_candidates
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER decision_candidates_no_delete BEFORE DELETE ON decision_candidates
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER selections_no_update BEFORE UPDATE ON comparator_selections
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER selections_no_delete BEFORE DELETE ON comparator_selections
          BEGIN SELECT RAISE(ABORT,'immutable'); END;
        INSERT INTO allocator_cohorts VALUES ('p.discovery.a1','p','discovery');
        INSERT INTO allocator_cohorts VALUES ('p.discovery.a2','p','discovery');
        INSERT INTO allocator_cohort_transitions VALUES ('t1','p',NULL,'p.discovery.a1');
        INSERT INTO allocator_cohort_transitions VALUES ('t2','p','p.discovery.a1','p.discovery.a2');
        """
    )
    passed, evidence = _allocator_registry_replay(
        connection, {"cohort": {"cohort_id": "p.discovery.a1"}}
    )
    connection.close()
    assert passed is False
    assert "published_allocator_cohort_is_not_replayed_active_head" in evidence["errors"]


def test_integrity_reuses_only_recent_successful_full_check() -> None:
    connection = sqlite3.connect(":memory:")
    recent = datetime.now(timezone.utc).isoformat()
    state = {
        "checks": [
            {
                "name": "allocator_sqlite_integrity",
                "passed": True,
                "evidence": {
                    "result": "ok",
                    "completed_full_check_utc": recent,
                },
            }
        ]
    }
    passed, evidence = _integrity_evidence(
        connection,
        check_name="allocator_sqlite_integrity",
        previous_state=state,
        maximum_full_check_age_sec=3600.0,
    )
    assert passed is True
    assert evidence["verification_mode"] == "recent_full_check_plus_current_logical_rebuild"


def test_integrity_refreshes_expired_or_failed_result() -> None:
    connection = sqlite3.connect(":memory:")
    expired = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    state = {
        "checks": [
            {
                "name": "source_registry_sqlite_integrity",
                "passed": True,
                "evidence": {
                    "result": "ok",
                    "completed_full_check_utc": expired,
                },
            }
        ]
    }
    passed, evidence = _integrity_evidence(
        connection,
        check_name="source_registry_sqlite_integrity",
        previous_state=state,
        maximum_full_check_age_sec=60.0,
    )
    assert passed is True
    assert evidence["verification_mode"] == "full_sqlite_quick_check"


def test_integrity_never_migrates_generated_or_legacy_heartbeat_timestamp() -> None:
    class CountingConnection:
        calls = 0

        def execute(self, sql: str):
            assert sql == "PRAGMA quick_check"
            self.calls += 1

            class Result:
                @staticmethod
                def fetchone():
                    return ("ok",)

            return Result()

    fresh = datetime.now(timezone.utc).isoformat()
    state = {
        "generated_utc": fresh,
        "status": "verification_in_progress",
        "checks": [
            {
                "name": "allocator_sqlite_integrity",
                "passed": True,
                "evidence": {
                    "result": "ok",
                    "last_full_check_utc": fresh,
                },
            }
        ],
    }
    connection = CountingConnection()
    passed, evidence = _integrity_evidence(
        connection,
        check_name="allocator_sqlite_integrity",
        previous_state=state,
        maximum_full_check_age_sec=3600.0,
    )
    assert passed is True
    assert connection.calls == 1
    assert evidence["verification_mode"] == "full_sqlite_quick_check"
    assert "completed_full_check_utc" in evidence


def test_integrity_rejects_materially_future_completed_check_timestamp() -> None:
    class CountingConnection:
        calls = 0

        def execute(self, sql: str):
            assert sql == "PRAGMA quick_check"
            self.calls += 1

            class Result:
                @staticmethod
                def fetchone():
                    return ("ok",)

            return Result()

    future = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
    state = {
        "checks": [
            {
                "name": "allocator_sqlite_integrity",
                "passed": True,
                "evidence": {
                    "result": "ok",
                    "completed_full_check_utc": future,
                },
            }
        ]
    }
    connection = CountingConnection()
    passed, evidence = _integrity_evidence(
        connection,
        check_name="allocator_sqlite_integrity",
        previous_state=state,
        maximum_full_check_age_sec=3600.0,
    )
    assert passed is True
    assert connection.calls == 1
    assert evidence["verification_mode"] == "full_sqlite_quick_check"
    assert float(evidence["full_check_age_sec"]) == 0.0


def test_full_integrity_scan_publishes_fresh_fail_closed_progress(tmp_path) -> None:
    class SlowConnection:
        def execute(self, sql: str):
            assert sql == "PRAGMA quick_check"
            time.sleep(0.08)

            class Result:
                @staticmethod
                def fetchone():
                    return ("ok",)

            return Result()

    progress = tmp_path / "verifier.json"
    previous = {
        "generated_utc": "2026-08-17T08:12:07+00:00",
        "status": "match",
        "authorization_safe": True,
        "checks": [{"name": "old_check", "passed": True, "evidence": {}}],
    }
    passed, evidence = _integrity_evidence(
        SlowConnection(),
        check_name="source_registry_sqlite_integrity",
        previous_state=previous,
        maximum_full_check_age_sec=0.0,
        progress_state_path=progress,
        progress_heartbeat_sec=0.01,
    )
    payload = json.loads(progress.read_text(encoding="utf-8"))
    assert passed is True
    assert evidence["verification_mode"] == "full_sqlite_quick_check"
    assert payload["status"] == "verification_in_progress"
    assert payload["authorization_safe"] is False
    assert payload["can_place_orders"] is False
    assert payload["verified_confirmed_candidates"] == []
    assert payload["active_full_integrity_check"] == "source_registry_sqlite_integrity"
    assert payload["checks"] == previous["checks"]


def test_verification_pass_heartbeat_is_fresh_and_authorization_ineligible(tmp_path) -> None:
    state = tmp_path / "verifier.json"
    previous = {
        "generated_utc": "2026-08-17T08:12:07+00:00",
        "status": "match",
        "authorization_safe": True,
        "verified_confirmed_candidates": [{"governed_hypothesis_id": "old"}],
        "checks": [{"name": "old_check", "passed": True, "evidence": {}}],
    }
    stop, thread = _start_verification_progress_heartbeat(
        state_path=state,
        previous_state=previous,
        verification_started_utc="2026-08-17T09:15:00+00:00",
        heartbeat_sec=0.01,
    )
    try:
        first = json.loads(state.read_text(encoding="utf-8"))
        time.sleep(0.03)
        second = json.loads(state.read_text(encoding="utf-8"))
    finally:
        stop.set()
        thread.join(timeout=2.0)
    assert first["status"] == "verification_in_progress"
    assert second["status"] == "verification_in_progress"
    assert second["authorization_safe"] is False
    assert second["can_place_orders"] is False
    assert second["verified_confirmed_candidates"] == []
    assert second["checks"] == previous["checks"]


def test_atomic_json_supports_concurrent_heartbeat_publishers(tmp_path) -> None:
    state = tmp_path / "verifier.json"
    start = threading.Barrier(3)
    errors: list[BaseException] = []

    def publish(writer: int) -> None:
        try:
            start.wait(timeout=2.0)
            for sequence in range(200):
                atomic_json(
                    state,
                    {
                        "status": "verification_in_progress",
                        "writer": writer,
                        "sequence": sequence,
                    },
                )
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    threads = [threading.Thread(target=publish, args=(writer,)) for writer in (1, 2)]
    for thread in threads:
        thread.start()
    start.wait(timeout=2.0)
    for thread in threads:
        thread.join(timeout=10.0)

    assert errors == []
    assert all(not thread.is_alive() for thread in threads)
    payload = json.loads(state.read_text(encoding="utf-8"))
    assert payload["status"] == "verification_in_progress"
    assert payload["writer"] in {1, 2}
    assert payload["sequence"] == 199
    assert list(tmp_path.glob(".*.tmp")) == []


def test_unique_pass_identity_and_generation_are_durable(tmp_path) -> None:
    guard = tmp_path / "guard.sqlite"
    first = _claim_verification_pass(guard)
    second = _claim_verification_pass(guard)
    assert first["pass_id"] != second["pass_id"]
    assert first["owner_id"] != second["owner_id"]
    assert second["generation"] == first["generation"] + 1
    connection = sqlite3.connect(guard)
    try:
        row = connection.execute(
            "SELECT generation,pass_id,owner_id,phase,authorization_safe "
            "FROM verifier_publication_guard WHERE singleton=1"
        ).fetchone()
    finally:
        connection.close()
    assert row == (
        second["generation"],
        second["pass_id"],
        second["owner_id"],
        "verification_in_progress",
        0,
    )


def test_multiprocess_newer_mismatch_cannot_be_overwritten_by_delayed_old_success(
    tmp_path,
) -> None:
    ctx = multiprocessing.get_context("spawn")
    guard = tmp_path / "guard.sqlite"
    state = tmp_path / "verifier.json"
    report = tmp_path / "verifier.md"
    old_ready = ctx.Queue()
    old_release = ctx.Event()
    old_result = ctx.Queue()
    new_ready = ctx.Queue()
    new_release = ctx.Event()
    new_result = ctx.Queue()
    old_process = ctx.Process(
        target=_overlap_publication_worker,
        args=(
            str(guard),
            str(state),
            str(report),
            "match",
            old_ready,
            old_release,
            old_result,
        ),
    )
    old_process.start()
    old_identity = old_ready.get(timeout=10.0)
    new_process = ctx.Process(
        target=_overlap_publication_worker,
        args=(
            str(guard),
            str(state),
            str(report),
            "mismatch",
            new_ready,
            new_release,
            new_result,
        ),
    )
    new_process.start()
    new_identity = new_ready.get(timeout=10.0)
    assert new_identity[0] == old_identity[0] + 1
    new_release.set()
    new_process.join(timeout=15.0)
    assert new_process.exitcode == 0
    assert new_result.get(timeout=2.0) == "published"
    old_release.set()
    old_process.join(timeout=15.0)
    assert old_process.exitcode == 0
    assert old_result.get(timeout=2.0) == "superseded"
    payload = json.loads(state.read_text(encoding="utf-8"))
    assert payload["status"] == "mismatch"
    assert payload["authorization_safe"] is False
    assert payload["publication_generation"] == new_identity[0]
    connection = sqlite3.connect(guard)
    try:
        row = connection.execute(
            "SELECT generation,pass_id,phase,status,authorization_safe "
            "FROM verifier_publication_guard WHERE singleton=1"
        ).fetchone()
    finally:
        connection.close()
    assert row == (new_identity[0], new_identity[1], "completed", "mismatch", 0)


def test_destination_lock_failure_leaves_prior_match_ineligible(
    tmp_path, monkeypatch
) -> None:
    import trad.oanda_independent_evidence_verifier as verifier_module

    state = tmp_path / "verifier.json"
    report = tmp_path / "verifier.md"
    guard = verifier_guard_path(state)
    old = _claim_verification_pass(guard)
    _publish_owned_final(
        state_path=state,
        report_path=report,
        guard_path=guard,
        ownership=old,
        payload=_minimal_final_payload(old, "match"),
        report="old match",
    )
    old_bytes = state.read_bytes()
    new = _claim_verification_pass(guard)
    real_write = verifier_module._atomic_text_payload

    def locked_write(path: Path, payload: str) -> None:
        if path == state:
            raise PermissionError("simulated Windows destination lock")
        real_write(path, payload)

    monkeypatch.setattr(verifier_module, "_atomic_text_payload", locked_write)
    try:
        _publish_owned_final(
            state_path=state,
            report_path=report,
            guard_path=guard,
            ownership=new,
            payload=_minimal_final_payload(new, "match"),
            report="new match",
        )
    except PermissionError:
        pass
    else:  # pragma: no cover - explicit safety assertion
        raise AssertionError("simulated destination lock did not fail")
    assert state.read_bytes() == old_bytes
    connection = sqlite3.connect(guard)
    try:
        row = connection.execute(
            "SELECT generation,phase,authorization_safe,state_sha256 "
            "FROM verifier_publication_guard WHERE singleton=1"
        ).fetchone()
    finally:
        connection.close()
    assert row == (new["generation"], "verification_in_progress", 0, None)


def test_exception_stops_outer_heartbeat_and_marks_guard_failed(
    tmp_path, monkeypatch
) -> None:
    import trad.oanda_independent_evidence_verifier as verifier_module

    state = tmp_path / "verifier.json"
    report = tmp_path / "verifier.md"
    guard = verifier_guard_path(state)
    captured: dict[str, threading.Thread] = {}

    def fail_after_heartbeat(**kwargs):
        stop, thread = _start_verification_progress_heartbeat(
            state_path=kwargs["state_path"],
            previous_state=kwargs["previous_state"],
            verification_started_utc=datetime.now(timezone.utc).isoformat(),
            heartbeat_sec=0.01,
            guard_path=kwargs["publication_guard_path"],
            ownership=kwargs["ownership"],
        )
        kwargs["heartbeat_holder"].append((stop, thread))
        captured["thread"] = thread
        raise RuntimeError("forced verifier body failure")

    monkeypatch.setattr(verifier_module, "_verify_owned_pass", fail_after_heartbeat)
    try:
        verify(
            state_path=state,
            report_path=report,
            publication_guard_path=guard,
        )
    except RuntimeError as exc:
        assert str(exc) == "forced verifier body failure"
    else:  # pragma: no cover - explicit safety assertion
        raise AssertionError("forced verifier failure was swallowed")
    assert captured["thread"].is_alive() is False
    connection = sqlite3.connect(guard)
    try:
        row = connection.execute(
            "SELECT phase,status,authorization_safe,failure_reason "
            "FROM verifier_publication_guard WHERE singleton=1"
        ).fetchone()
    finally:
        connection.close()
    assert row[:3] == ("publication_failed", "mismatch", 0)
    assert "forced verifier body failure" in row[3]


def test_crashed_process_leaves_durable_fail_closed_owner(tmp_path) -> None:
    ctx = multiprocessing.get_context("spawn")
    guard = tmp_path / "guard.sqlite"
    marker = tmp_path / "claimed.marker"
    process = ctx.Process(
        target=_claim_then_crash_worker,
        args=(str(guard), str(marker)),
    )
    process.start()
    deadline = time.time() + 10.0
    while not marker.is_file() and time.time() < deadline:
        time.sleep(0.02)
    process.join(timeout=10.0)
    assert marker.is_file()
    assert process.exitcode == 7
    connection = sqlite3.connect(guard)
    try:
        row = connection.execute(
            "SELECT phase,status,authorization_safe,completed_utc "
            "FROM verifier_publication_guard WHERE singleton=1"
        ).fetchone()
    finally:
        connection.close()
    assert row == ("verification_in_progress", "verification_in_progress", 0, None)
