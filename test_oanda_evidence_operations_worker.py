from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone

from trad.oanda_evidence_operations_worker import (
    OperationsProgressHeartbeat,
    canary_authorization_payload,
    read_allocator_publication,
    report_markdown,
    synchronize_lifecycle_genealogy,
)
from trad.oanda_hypothesis_lifecycle import initialize_database, insert_hypothesis


def test_canary_authorization_never_auto_routes_confirmed_cells():
    payload = {
        "generated_utc": "2026-08-06T20:00:00+00:00",
        "lifecycle": {
            "lifecycle": {"states": {"confirmed_candidate": 2}}
        },
        "allocator": {"evidence": {"cohort_id": "allocator-1"}},
        "accounting": {
            "accounting": {
                "account_operational_continuity": {"account_state_current": True}
            },
            "routeability_sentinel": {"passed": True},
        },
    }

    authorization = canary_authorization_payload(payload)

    assert authorization["confirmed_candidate_count"] == 2
    assert authorization["entry_authorized"] is False
    assert authorization["authorized_entries"] == []
    assert authorization["auto_route"] is False
    assert authorization["reason"] == (
        "explicit_version_locked_practice_canary_activation_required"
    )


def test_allocator_publication_reader_accepts_current_fail_closed_state(tmp_path):
    path = tmp_path / "allocator.json"
    now = datetime(2026, 9, 2, 6, 0, tzinfo=timezone.utc).timestamp()
    payload = {
        "generated_utc": "2026-09-02T05:55:00+00:00",
        "research_only": True,
        "can_place_orders": False,
        "can_promote": False,
        "real_money_routing": False,
        "evidence": {"cohort_id": "allocator-proof"},
    }
    path.write_text(json.dumps(payload), encoding="utf-8")

    observed = read_allocator_publication(path, now_epoch=now)

    assert observed == payload


def test_allocator_publication_reader_rejects_stale_or_unsafe_state(tmp_path):
    path = tmp_path / "allocator.json"
    path.write_text(
        json.dumps(
            {
                "generated_utc": "2026-09-02T05:00:00+00:00",
                "research_only": True,
                "can_place_orders": True,
                "can_promote": False,
                "real_money_routing": False,
                "evidence": {"cohort_id": "must-not-propagate"},
            }
        ),
        encoding="utf-8",
    )

    observed = read_allocator_publication(
        path,
        now_epoch=datetime(2026, 9, 2, 6, 0, tzinfo=timezone.utc).timestamp(),
    )

    assert observed["status"] == "allocator_publication_missing_stale_or_unsafe"
    assert observed["evidence"] == {}
    assert observed["can_place_orders"] is False
    assert observed["can_promote"] is False
    assert observed["real_money_routing"] is False


def test_canary_authorization_fails_closed_when_account_is_unavailable():
    payload = {
        "generated_utc": "2026-08-06T20:00:00+00:00",
        "lifecycle": {"lifecycle": {"states": {"confirmed_candidate": 2}}},
        "allocator": {"evidence": {"cohort_id": "allocator-1"}},
        "accounting": {
            "accounting": {
                "account_operational_continuity": {
                    "account_state_current": False,
                    "snapshot_state": "unavailable",
                }
            },
            "routeability_sentinel": {"passed": True},
        },
    }
    authorization = canary_authorization_payload(payload)
    assert authorization["entry_authorized"] is False
    assert authorization["account_state_current"] is False
    assert authorization["reason"] == "account_state_unavailable"


def test_operations_report_exposes_entry_authorization_state():
    payload = {
        "generated_utc": "2026-08-06T20:00:00+00:00",
        "lifecycle": {
            "lifecycle": {"states": {}, "hypothesis_count": 2},
            "genealogy_sync": {
                "status": "synchronized",
                "lifecycle_hypotheses": 2,
                "genealogy_governed_cells": 2,
            },
        },
        "allocator": {"evidence": {}},
        "accounting": {"accounting": {}, "routeability_sentinel": {}},
        "opportunity_decision_level": {
            "collector_cohort_id": "collector",
            "decision_epochs": 4,
            "raw_pair_rows": 100,
            "top_one": {"matured_rows": 3, "cost_clearing_rows": 2,
                        "average_predicted_side_net_pips": -0.2},
            "exactly_three_currency_disjoint": {
                "decisions": 4, "matured_rows": 9,
                "average_predicted_side_net_pips": -0.1,
            },
        },
        "practice_canary_authorization": {
            "entry_authorized": False,
            "authorized_entries": [],
            "reason": "zero_confirmed_candidates",
        },
    }

    report = report_markdown(payload)

    assert "Governed new-entry authorization: **False**" in report
    assert "zero_confirmed_candidates" in report
    assert "Executable-opportunity decision level" in report
    assert "Lifecycle/genealogy handoff: **synchronized** (2 / 2)" in report


def test_progress_heartbeat_is_separate_and_fail_closed(tmp_path):
    path = tmp_path / "liveness.json"
    heartbeat = OperationsProgressHeartbeat(path, interval_sec=0.02)
    heartbeat.start()
    try:
        heartbeat.update("running_lifecycle", {"cycle": 1})
        time.sleep(0.04)
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["phase"] == "running_lifecycle"
        assert payload["progress_sequence"] == 1
        assert payload["research_only"] is True
        assert payload["can_place_orders"] is False
        assert payload["can_submit_orders"] is False
        assert payload["can_promote"] is False
        assert payload["real_money_routing"] is False
        assert "lifecycle" not in payload
        assert payload["progress_age_sec"] > 0
    finally:
        heartbeat.stop()


def test_progress_heartbeat_records_stage_completion_without_overwriting_report(tmp_path):
    path = tmp_path / "liveness.json"
    heartbeat = OperationsProgressHeartbeat(path, interval_sec=0.02)
    heartbeat.start()
    try:
        heartbeat.update("publishing_final_state")
        heartbeat.set_cycle_complete({"last_completed_utc": "2026-08-24T19:00:00+00:00"})
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["phase"] == "idle_between_cycles"
        assert payload["cycles"] == 1
        assert payload["details"]["last_completed_utc"] == "2026-08-24T19:00:00+00:00"
        assert payload["progress_sequence"] == 2
    finally:
        heartbeat.stop()


def test_targeted_lifecycle_genealogy_sync_reconciles_exact_counts(tmp_path):
    lifecycle_path = tmp_path / "lifecycle.sqlite"
    genealogy_path = tmp_path / "genealogy.sqlite"
    connection = initialize_database(lifecycle_path)
    try:
        insert_hypothesis(
            connection,
            {
                "cell_id": "cell-1",
                "family": "test_family",
                "instrument": "EUR_USD",
                "horizon_sec": 3600,
                "session_bucket": "london",
                "liquidity_bucket": "liquid",
            },
            evidence_contract_id="test-contract",
            cohort_id="test-cohort",
            observed_utc="2026-09-01T00:00:00+00:00",
        )
        connection.commit()
    finally:
        connection.close()

    result = synchronize_lifecycle_genealogy(lifecycle_path, genealogy_path)

    assert result["ok"] is True
    assert result["status"] == "synchronized"
    assert result["inserted_definitions"] == 1
    assert result["lifecycle_hypotheses"] == 1
    assert result["genealogy_governed_cells"] == 1
    assert result["can_place_orders"] is False


def test_targeted_lifecycle_genealogy_sync_reports_extra_governed_definition(tmp_path):
    lifecycle_path = tmp_path / "lifecycle.sqlite"
    genealogy_path = tmp_path / "genealogy.sqlite"
    connection = initialize_database(lifecycle_path)
    connection.close()
    first = synchronize_lifecycle_genealogy(lifecycle_path, genealogy_path)
    assert first["ok"] is True

    genealogy = sqlite3.connect(genealogy_path)
    try:
        columns = [
            row[1]
            for row in genealogy.execute("PRAGMA table_info(experiments)")
        ]
        values = {column: "fixture" for column in columns}
        values.update(
            {
                "hypothesis_id": "orphan-governed-cell",
                "experiment_kind": "governed_cell",
                "pre_registered": 0,
            }
        )
        genealogy.execute(
            f"INSERT INTO experiments ({','.join(columns)}) "
            f"VALUES ({','.join('?' for _ in columns)})",
            tuple(values[column] for column in columns),
        )
        genealogy.commit()
    finally:
        genealogy.close()

    result = synchronize_lifecycle_genealogy(lifecycle_path, genealogy_path)
    assert result["ok"] is False
    assert result["status"] == "count_mismatch"


def test_targeted_lifecycle_genealogy_sync_retries_only_lock_contention(tmp_path):
    lifecycle_path = tmp_path / "lifecycle.sqlite"
    genealogy_path = tmp_path / "genealogy.sqlite"
    connections = []
    sleeps = []
    progress = []
    calls = {"imports": 0}

    class Connection:
        def __init__(self):
            self.commits = 0
            self.rollbacks = 0
            self.closed = False

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

        def close(self):
            self.closed = True

    def connect(_path):
        connection = Connection()
        connections.append(connection)
        return connection

    def import_lifecycle(_connection, _source, *, imported_at):
        assert imported_at
        calls["imports"] += 1
        if calls["imports"] < 3:
            raise sqlite3.OperationalError("database is locked")
        return 1, 1

    result = synchronize_lifecycle_genealogy(
        lifecycle_path,
        genealogy_path,
        lock_retry_attempts=3,
        lock_retry_base_sec=0.25,
        lock_retry_max_sec=1.0,
        sleep_fn=sleeps.append,
        progress_callback=progress.append,
        connect_fn=connect,
        import_fn=import_lifecycle,
        count_fn=lambda *_args: 1,
    )

    assert result["ok"] is True
    assert result["lock_retry_count"] == 2
    assert result["inserted_definitions"] == 1
    assert len(connections) == 3
    assert all(connection.closed for connection in connections)
    assert [connection.rollbacks for connection in connections] == [1, 1, 0]
    assert sleeps == [0.25, 0.5]
    assert [row["genealogy_lock_retry_count"] for row in progress] == [1, 2]


def test_targeted_lifecycle_genealogy_sync_does_not_retry_other_sqlite_errors(tmp_path):
    connections = []
    sleeps = []

    class Connection:
        def rollback(self):
            pass

        def close(self):
            pass

    def connect(_path):
        connections.append(Connection())
        return connections[-1]

    def import_lifecycle(*_args, **_kwargs):
        raise sqlite3.OperationalError("disk I/O error")

    try:
        synchronize_lifecycle_genealogy(
            tmp_path / "lifecycle.sqlite",
            tmp_path / "genealogy.sqlite",
            lock_retry_attempts=3,
            sleep_fn=sleeps.append,
            connect_fn=connect,
            import_fn=import_lifecycle,
        )
    except sqlite3.OperationalError as error:
        assert str(error) == "disk I/O error"
    else:
        raise AssertionError("non-lock SQLite failure must propagate")
    assert len(connections) == 1
    assert sleeps == []


def test_targeted_lifecycle_genealogy_sync_exhausts_bounded_lock_retries(tmp_path):
    attempts = []
    sleeps = []

    class Connection:
        def rollback(self):
            pass

        def close(self):
            pass

    def connect(_path):
        attempts.append(1)
        return Connection()

    def import_lifecycle(*_args, **_kwargs):
        raise sqlite3.OperationalError("database table is locked")

    try:
        synchronize_lifecycle_genealogy(
            tmp_path / "lifecycle.sqlite",
            tmp_path / "genealogy.sqlite",
            lock_retry_attempts=3,
            lock_retry_base_sec=0.1,
            lock_retry_max_sec=0.2,
            sleep_fn=sleeps.append,
            connect_fn=connect,
            import_fn=import_lifecycle,
        )
    except sqlite3.OperationalError as error:
        assert "locked" in str(error)
    else:
        raise AssertionError("exhausted genealogy lock retries must fail closed")
    assert len(attempts) == 3
    assert sleeps == [0.1, 0.2]
