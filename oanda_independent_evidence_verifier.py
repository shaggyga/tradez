#!/usr/bin/env python3
"""Independent read-only verifier for governed Forex evidence.

This module intentionally imports no production evidence, lifecycle, allocator,
authorization, or execution calculator.  It rebuilds current lifecycle state
from append-only SQLite events and compares that result with published state.
It cannot promote a hypothesis, authorize an entry, or call a broker.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import sqlite3
import threading
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
REPORTS = ROOT / "data" / "oanda_training_manager" / "reports" / "independent_evidence_verifier"
DEFAULT_LIFECYCLE_DATABASE = STATE / "evidence_lifecycle_v1.sqlite"
DEFAULT_LIFECYCLE_STATE = STATE / "evidence_lifecycle_v1.json"
DEFAULT_EVIDENCE_OPERATIONS_WORKER_STATE = (
    STATE / "evidence_operations_worker_heartbeat_v2.json"
)
DEFAULT_EDGE_STATE = STATE / "edge_evidence_v1.json"
DEFAULT_EDGE_WORKER_STATE = STATE / "edge_evidence_worker_v1.json"
DEFAULT_EDGE_INPUT_CHECKPOINT = STATE / "edge_evidence_input_checkpoint_v1.json"
DEFAULT_PROOF_DATABASE = STATE / "proof_cohort_registry_v1.sqlite"
DEFAULT_PROOF_STATE = STATE / "proof_cohort_registry_v1.json"
DEFAULT_ALLOCATOR_DATABASE = STATE / "allocator_proof_v1.sqlite"
DEFAULT_ALLOCATOR_STATE = STATE / "allocator_proof_v1.json"
DEFAULT_AUTHORIZATION = STATE / "practice_007_governed_canary_authorization_v1.json"
DEFAULT_GENEALOGY_DATABASE = STATE / "research_genealogy_v1.sqlite"
DEFAULT_SOURCE_DATABASE = STATE / "source_governance_v1.sqlite"
DEFAULT_STATE = STATE / "independent_evidence_verifier_v1.json"
DEFAULT_REPORT = REPORTS / "INDEPENDENT_EVIDENCE_VERIFIER_CURRENT.md"
DEFAULT_FULL_INTEGRITY_INTERVAL_SEC = 21600.0
DEFAULT_MAXIMUM_EDGE_SNAPSHOT_LAG_SEC = 86400.0
DEFAULT_MAXIMUM_LIFECYCLE_SNAPSHOT_LAG_SEC = 7200.0
DEFAULT_PUBLICATION_LEASE_SEC = 90.0
MAXIMUM_COMPLETED_CHECK_FUTURE_SKEW_SEC = 60.0
PUBLICATION_CONTRACT = "independent_evidence_verifier_publication_v2"
_ATOMIC_WRITE_LOCK = threading.Lock()


class VerificationPassSuperseded(RuntimeError):
    """Raised when a newer verifier pass owns the publication generation."""


def verifier_guard_path(state_path: Path) -> Path:
    """Return the durable authorization guard paired with a verifier state."""
    return state_path.with_name(f"{state_path.stem}_guard_v2.sqlite")


def _guard_connection(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=20.0, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=20000")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS verifier_publication_guard(
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            generation INTEGER NOT NULL,
            pass_id TEXT NOT NULL,
            owner_id TEXT NOT NULL,
            phase TEXT NOT NULL,
            status TEXT NOT NULL,
            authorization_safe INTEGER NOT NULL,
            started_utc TEXT NOT NULL,
            heartbeat_utc TEXT NOT NULL,
            lease_expires_epoch REAL NOT NULL,
            completed_utc TEXT,
            state_sha256 TEXT,
            failure_reason TEXT
        )
        """
    )
    return connection


def _claim_verification_pass(
    guard_path: Path,
    *,
    lease_sec: float = DEFAULT_PUBLICATION_LEASE_SEC,
    pass_id: str | None = None,
    owner_id: str | None = None,
) -> dict[str, Any]:
    """Atomically supersede any prior pass and publish a fail-closed lease."""
    claimed_pass_id = pass_id or str(uuid.uuid4())
    claimed_owner_id = owner_id or (
        f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4()}"
    )
    started_utc = utc_now()
    lease_expires_epoch = time.time() + max(5.0, float(lease_sec))
    connection = _guard_connection(guard_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT generation FROM verifier_publication_guard WHERE singleton=1"
        ).fetchone()
        generation = int(row[0]) + 1 if row is not None else 1
        connection.execute(
            """
            INSERT INTO verifier_publication_guard(
                singleton,generation,pass_id,owner_id,phase,status,
                authorization_safe,started_utc,heartbeat_utc,
                lease_expires_epoch,completed_utc,state_sha256,failure_reason
            ) VALUES(1,?,?,?,?,?,0,?,?,?,NULL,NULL,NULL)
            ON CONFLICT(singleton) DO UPDATE SET
                generation=excluded.generation,
                pass_id=excluded.pass_id,
                owner_id=excluded.owner_id,
                phase=excluded.phase,
                status=excluded.status,
                authorization_safe=0,
                started_utc=excluded.started_utc,
                heartbeat_utc=excluded.heartbeat_utc,
                lease_expires_epoch=excluded.lease_expires_epoch,
                completed_utc=NULL,
                state_sha256=NULL,
                failure_reason=NULL
            """,
            (
                generation,
                claimed_pass_id,
                claimed_owner_id,
                "verification_in_progress",
                "verification_in_progress",
                started_utc,
                started_utc,
                lease_expires_epoch,
            ),
        )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return {
        "generation": generation,
        "pass_id": claimed_pass_id,
        "owner_id": claimed_owner_id,
        "started_utc": started_utc,
        "lease_sec": max(5.0, float(lease_sec)),
        "guard_path": str(guard_path.resolve()),
    }


def _guard_owner_matches(
    connection: sqlite3.Connection,
    ownership: dict[str, Any],
) -> bool:
    row = connection.execute(
        """
        SELECT generation,pass_id,owner_id,phase FROM verifier_publication_guard
        WHERE singleton=1
        """
    ).fetchone()
    return bool(
        row is not None
        and int(row["generation"]) == int(ownership["generation"])
        and str(row["pass_id"]) == str(ownership["pass_id"])
        and str(row["owner_id"]) == str(ownership["owner_id"])
        and str(row["phase"]) == "verification_in_progress"
    )


def _owned_progress_payload(
    *,
    previous_state: dict[str, Any],
    verification_started_utc: str,
    ownership: dict[str, Any] | None,
    active_check: str,
) -> dict[str, Any]:
    payload = {
        "schema_version": 2,
        "publication_contract": PUBLICATION_CONTRACT,
        "generated_utc": utc_now(),
        "status": "verification_in_progress",
        "authorization_safe": False,
        "can_place_orders": False,
        "can_promote": False,
        "research_only": True,
        "supported_decision": "no_trade",
        "verified_confirmed_candidates": [],
        "active_full_integrity_check": active_check,
        "verification_started_utc": verification_started_utc,
        "previous_completed_state_utc": previous_state.get(
            "completed_verification_utc"
        ),
        "checks": previous_state.get("checks", []),
    }
    if ownership is not None:
        payload.update(
            {
                "verification_pass_id": ownership["pass_id"],
                "verification_owner_id": ownership["owner_id"],
                "publication_generation": ownership["generation"],
                "publication_guard": ownership["guard_path"],
            }
        )
    return payload


def _publish_owned_progress(
    *,
    state_path: Path,
    guard_path: Path,
    ownership: dict[str, Any],
    payload: dict[str, Any],
) -> bool:
    """Serialize owner check and state replacement under the guard write lock."""
    connection = _guard_connection(guard_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        if not _guard_owner_matches(connection, ownership):
            connection.rollback()
            return False
        _atomic_text_payload(
            state_path, json.dumps(payload, indent=2, sort_keys=True)
        )
        heartbeat_utc = utc_now()
        connection.execute(
            """
            UPDATE verifier_publication_guard
            SET heartbeat_utc=?,lease_expires_epoch=?
            WHERE singleton=1 AND generation=? AND pass_id=? AND owner_id=?
            """,
            (
                heartbeat_utc,
                time.time() + float(ownership["lease_sec"]),
                int(ownership["generation"]),
                str(ownership["pass_id"]),
                str(ownership["owner_id"]),
            ),
        )
        connection.commit()
        return True
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def _publish_owned_final(
    *,
    state_path: Path,
    report_path: Path,
    guard_path: Path,
    ownership: dict[str, Any],
    payload: dict[str, Any],
    report: str,
) -> None:
    """Publish final bytes and matching guard row as one ordered CAS action."""
    state_text = json.dumps(payload, indent=2, sort_keys=True)
    state_sha256 = "sha256:" + hashlib.sha256(state_text.encode("utf-8")).hexdigest()
    connection = _guard_connection(guard_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        if not _guard_owner_matches(connection, ownership):
            connection.rollback()
            raise VerificationPassSuperseded(str(ownership["pass_id"]))
        # The durable guard is already fail-closed.  Write the non-authorizing
        # report first, then the state, and only then expose a completed match.
        # If Windows locks either destination, the transaction rolls back to
        # the already-committed in-progress guard and old success is unusable.
        _atomic_text_payload(report_path, report)
        _atomic_text_payload(state_path, state_text)
        completed_utc = str(payload["completed_verification_utc"])
        result = connection.execute(
            """
            UPDATE verifier_publication_guard
            SET phase='completed',status=?,authorization_safe=?,
                heartbeat_utc=?,lease_expires_epoch=?,completed_utc=?,
                state_sha256=?,failure_reason=NULL
            WHERE singleton=1 AND generation=? AND pass_id=? AND owner_id=?
            """,
            (
                str(payload["status"]),
                1 if payload.get("authorization_safe") is True else 0,
                completed_utc,
                time.time() + float(ownership["lease_sec"]),
                completed_utc,
                state_sha256,
                int(ownership["generation"]),
                str(ownership["pass_id"]),
                str(ownership["owner_id"]),
            ),
        )
        if result.rowcount != 1:
            connection.rollback()
            raise VerificationPassSuperseded(str(ownership["pass_id"]))
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def _mark_guard_failed(
    guard_path: Path,
    ownership: dict[str, Any],
    reason: str,
) -> bool:
    """Durably leave an owned pass authorization-ineligible after failure."""
    connection = _guard_connection(guard_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        completed_utc = utc_now()
        result = connection.execute(
            """
            UPDATE verifier_publication_guard
            SET phase='publication_failed',status='mismatch',
                authorization_safe=0,heartbeat_utc=?,completed_utc=?,
                lease_expires_epoch=?,state_sha256=NULL,failure_reason=?
            WHERE singleton=1 AND generation=? AND pass_id=? AND owner_id=?
            """,
            (
                completed_utc,
                completed_utc,
                time.time() + float(ownership["lease_sec"]),
                str(reason)[:1000],
                int(ownership["generation"]),
                str(ownership["pass_id"]),
                str(ownership["owner_id"]),
            ),
        )
        connection.commit()
        return result.rowcount == 1
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def read_json_optional(path: Path) -> dict[str, Any]:
    try:
        return read_json(path)
    except (FileNotFoundError, json.JSONDecodeError, OSError, ValueError):
        return {}


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def ro(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=20.0
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=20000")
    connection.execute("PRAGMA query_only=ON")
    return connection


def _atomic_text_payload(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # The pass-level and full-integrity heartbeats may publish concurrently.
    # A single fixed ``.tmp`` path lets one thread replace another thread's
    # temporary file, terminating both fail-closed heartbeats with a
    # FileNotFoundError while the SQLite scan is still alive.  A process,
    # thread, and nanosecond-qualified sibling keeps each atomic publication
    # independent, including during a supervised process hand-off.
    with _ATOMIC_WRITE_LOCK:
        temporary = path.with_name(
            f".{path.name}.{os.getpid()}.{threading.get_ident()}.{time.time_ns()}.tmp"
        )
        try:
            # ``newline=""`` is part of the publication contract. Windows text
            # mode otherwise translates LF to CRLF after the guard SHA has
            # already been calculated from ``payload``.  Writing the exact
            # UTF-8 bytes makes the producer hash identical to what the
            # executor later reads on every supported platform.
            with temporary.open("w", encoding="utf-8", newline="") as handle:
                handle.write(payload)
            for attempt in range(10):
                try:
                    temporary.replace(path)
                    break
                except PermissionError:
                    if attempt == 9:
                        raise
                    time.sleep(0.005)
        finally:
            temporary.unlink(missing_ok=True)


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    _atomic_text_payload(path, json.dumps(value, indent=2, sort_keys=True))


def atomic_text(path: Path, value: str) -> None:
    _atomic_text_payload(path, value)


def _check(checks: list[dict[str, Any]], name: str, passed: bool, **evidence: Any) -> None:
    checks.append({"name": name, "passed": bool(passed), "evidence": evidence})


def _age_seconds(value: Any) -> float | None:
    try:
        epoch = datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None
    return max(0.0, time.time() - epoch)


def _current_published_state_ages(
    *,
    lifecycle_state: Path,
    allocator_state: Path,
    edge_state: Path,
) -> dict[str, Any]:
    """Read publication freshness at the time the freshness gate is applied.

    A verifier pass may spend many minutes rebuilding large SQLite ledgers.  The
    payloads read at pass start must remain frozen for the substantive
    reconstruction comparisons, but their timestamps must not be reused to
    judge whether the independently refreshed live publications are current at
    pass completion.
    """
    lifecycle = read_json(lifecycle_state)
    allocator = read_json(allocator_state)
    edge = read_json(edge_state)
    return {
        "lifecycle_age_sec": _age_seconds(lifecycle.get("generated_utc")),
        "allocator_age_sec": _age_seconds(allocator.get("generated_utc")),
        "edge_age_sec": _age_seconds(edge.get("generated_utc")),
        "lifecycle_generated_utc": lifecycle.get("generated_utc"),
        "allocator_generated_utc": allocator.get("generated_utc"),
        "edge_generated_utc": edge.get("generated_utc"),
    }


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _lifecycle_database_integrity_snapshot(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    """Independently bind a lifecycle publication to exact DB high-waters."""

    current_rows = [
        {
            "hypothesis_id": str(row[0]),
            "cohort_id": None if row[1] is None else str(row[1]),
            "definition_sha256": str(row[2]),
            "event_rowid": int(row[3]),
            "next_state": str(row[4]),
            "observed_utc": str(row[5]),
            "source_run_id": str(row[6]),
            "evidence_json": str(row[7]),
        }
        for row in connection.execute(
            """
            SELECT hypothesis.hypothesis_id,hypothesis.cohort_id,
                   hypothesis.definition_sha256,event.rowid,event.next_state,
                   event.observed_utc,event.source_run_id,event.evidence_json
            FROM hypotheses AS hypothesis
            JOIN lifecycle_events AS event
              ON event.hypothesis_id=hypothesis.hypothesis_id
            JOIN (
                SELECT hypothesis_id,MAX(rowid) AS latest_rowid
                FROM lifecycle_events GROUP BY hypothesis_id
            ) AS latest ON latest.latest_rowid=event.rowid
            ORDER BY hypothesis.hypothesis_id
            """
        )
    ]

    def table_extent(table: str) -> tuple[int, int]:
        count, highwater = connection.execute(
            f"SELECT COUNT(*),COALESCE(MAX(rowid),0) FROM {table}"
        ).fetchone()
        return int(count), int(highwater)

    hypothesis_count, hypothesis_highwater = table_extent("hypotheses")
    event_count, event_highwater = table_extent("lifecycle_events")
    retirement_count, retirement_highwater = table_extent("futility_retirements")
    reconsideration_count, reconsideration_highwater = table_extent(
        "reconsideration_decisions"
    )
    current_states = Counter(row["next_state"] for row in current_rows)
    return {
        "contract": "evidence_lifecycle_publication_integrity_v1",
        "hypothesis_count": hypothesis_count,
        "hypothesis_highwater_rowid": hypothesis_highwater,
        "lifecycle_event_count": event_count,
        "lifecycle_event_highwater_rowid": event_highwater,
        "futility_retirement_count": retirement_count,
        "futility_retirement_highwater_rowid": retirement_highwater,
        "reconsideration_count": reconsideration_count,
        "reconsideration_highwater_rowid": reconsideration_highwater,
        "current_state_count": len(current_rows),
        "current_states": {
            state: int(current_states.get(state, 0))
            for state in (
                "confirmed_candidate",
                "continue_collecting",
                "futility_rejected",
            )
        },
        "current_state_sha256": _canonical_sha256(current_rows),
    }


def _lifecycle_publication_freshness(
    *,
    lifecycle_state: Path,
    lifecycle_database: Path,
    worker_state: Path,
    maximum_age_sec: float,
    maximum_snapshot_lag_sec: float = DEFAULT_MAXIMUM_LIFECYCLE_SNAPSHOT_LAG_SEC,
) -> dict[str, Any]:
    """Validate lifecycle freshness without making elapsed time a proof fact.

    A publication older than the ordinary freshness budget remains usable only
    during a bounded, healthy research-only rebuild, when an independent live
    DB fingerprint exactly matches the frozen publication and both surfaces
    remain incapable of promotion or execution.  A new row, a changed latest
    event, a confirmation, a stale heartbeat, or any error fails closed.
    """

    publication = read_json_optional(lifecycle_state)
    worker = read_json_optional(worker_state)
    publication_age = _age_seconds(publication.get("generated_utc"))
    worker_age = _age_seconds(worker.get("generated_utc") or worker.get("updated_at"))
    progress_clock_age = _age_seconds(worker.get("last_progress_utc"))
    try:
        progress_age = float(worker.get("progress_age_sec"))
    except (TypeError, ValueError):
        progress_age = float("inf")
    with ro(lifecycle_database) as connection:
        database_integrity = _lifecycle_database_integrity_snapshot(connection)
        append_only = _append_only_triggers(
            connection,
            ["hypotheses", "lifecycle_events", "futility_retirements"],
        )
    published_integrity = publication.get("integrity") or {}
    published_states = (
        (publication.get("lifecycle") or {}).get("states") or {}
    )
    database_states = database_integrity.get("current_states") or {}
    normal_phases = {
        "running_lifecycle",
        "running_allocator",
        "running_accounting",
        "running_opportunity_monitor",
        "publishing_final_state",
        "idle_between_cycles",
    }
    handshake = {
        "publication_integrity_present": bool(published_integrity),
        "database_integrity_match": bool(published_integrity)
        and published_integrity == database_integrity,
        "append_only_guards_present": bool(append_only)
        and all(append_only.values()),
        "publication_fail_closed": bool(publication.get("research_only"))
        and not bool(publication.get("can_place_orders"))
        and not bool(publication.get("can_promote"))
        and not bool(publication.get("real_money_routing")),
        "publication_states_match_integrity": {
            str(key): int(value) for key, value in published_states.items()
        }
        == {str(key): int(value) for key, value in database_states.items()},
        "zero_confirmed_candidates": int(
            database_states.get("confirmed_candidate", -1)
        )
        == 0,
        "worker_running": worker.get("status") == "running",
        "worker_phase_expected": str(worker.get("phase") or "") in normal_phases,
        "worker_fresh": worker_age is not None
        and worker_age <= maximum_age_sec,
        "worker_progress_recent": progress_age <= maximum_age_sec
        and progress_clock_age is not None
        and progress_clock_age <= maximum_age_sec,
        "worker_error_free": int(worker.get("errors") or 0) == 0
        and not str(worker.get("last_error") or ""),
        "worker_fail_closed": bool(worker.get("research_only"))
        and not bool(worker.get("can_place_orders"))
        and not bool(worker.get("can_submit_orders"))
        and not bool(worker.get("can_promote"))
        and not bool(worker.get("real_money_routing")),
        "publication_within_fresh_age": publication_age is not None
        and publication_age <= maximum_age_sec,
        "publication_within_bounded_lag": publication_age is not None
        and publication_age <= maximum_snapshot_lag_sec,
    }
    intact_publication = bool(
        handshake["publication_integrity_present"]
        and handshake["database_integrity_match"]
        and handshake["append_only_guards_present"]
        and handshake["publication_fail_closed"]
        and handshake["publication_states_match_integrity"]
    )
    fresh_publication = bool(
        intact_publication and handshake["publication_within_fresh_age"]
    )
    bounded_rebuild = bool(
        intact_publication
        and handshake["publication_within_bounded_lag"]
        and handshake["zero_confirmed_candidates"]
        and handshake["worker_running"]
        and handshake["worker_phase_expected"]
        and handshake["worker_fresh"]
        and handshake["worker_progress_recent"]
        and handshake["worker_error_free"]
        and handshake["worker_fail_closed"]
    )
    if fresh_publication:
        reason = "fresh_exact_lifecycle_publication"
    elif bounded_rebuild:
        reason = "exact_fail_closed_lifecycle_snapshot_while_rebuilding"
    else:
        reason = "stale_or_unverified_lifecycle_publication"
    return {
        "fresh": fresh_publication or bounded_rebuild,
        "reason": reason,
        "publication_age_sec": publication_age,
        "worker_age_sec": worker_age,
        "worker_progress_age_sec": progress_age,
        "maximum_snapshot_lag_sec": maximum_snapshot_lag_sec,
        "handshake": handshake,
        "published_integrity": published_integrity,
        "database_integrity": database_integrity,
        "append_only_guards": append_only,
    }


def _edge_report_freshness(
    *,
    edge_state: Path,
    worker_state: Path,
    input_checkpoint: Path,
    maximum_age_sec: float,
    maximum_snapshot_lag_sec: float = DEFAULT_MAXIMUM_EDGE_SNAPSHOT_LAG_SEC,
) -> dict[str, Any]:
    """Validate a current or bounded fail-closed edge-evidence snapshot.

    The source outcome ledger is intentionally append-only and advances while
    its multi-million-row report is being rebuilt. A byte-intact completed
    research snapshot may therefore lag a healthy worker without becoming an
    authorization-integrity failure. Lagged acceptance is bounded, requires
    recent worker progress, and requires the report itself to be incapable of
    promotion or order placement.
    """
    report = read_json_optional(edge_state)
    worker = read_json_optional(worker_state)
    checkpoint = read_json_optional(input_checkpoint)
    report_age = _age_seconds(report.get("generated_utc"))
    worker_age = _age_seconds(worker.get("updated_at") or worker.get("generated_utc"))
    worker_details = worker.get("details") or {}
    compact = checkpoint.get("last_completed_report") or {}
    output_records = checkpoint.get("output_integrity") or {}
    output_checks: dict[str, bool] = {}
    for raw_path, expected in (
        output_records.items() if isinstance(output_records, dict) else []
    ):
        path = Path(str(raw_path))
        try:
            actual_hash = file_sha256(path).removeprefix("sha256:")
            actual_size = int(path.stat().st_size)
        except (OSError, ValueError):
            output_checks[str(path)] = False
            continue
        output_checks[str(path)] = bool(
            isinstance(expected, dict)
            and expected.get("sha256") == actual_hash
            and expected.get("size_bytes") == actual_size
        )
    report_highwater = (report.get("integrity") or {}).get("source_highwater_row_id")
    phase = str(worker.get("phase") or "")
    progress_age = worker.get("progress_age_sec")
    try:
        worker_progress_recent = float(progress_age) <= float(maximum_age_sec)
    except (TypeError, ValueError):
        worker_progress_recent = False
    catchup_phases = {
        "starting",
        "starting_cycle",
        "input_snapshot_captured",
        "inventorying_source",
        "loading_rows",
        "loading_rows_complete",
        "inventorying_forecast_contract",
        "inventorying_forecast_contract_complete",
        "building_cells",
        "building_archetypes",
        "building_economic_labels",
        "building_candidate_replication",
        "building_governance",
        "assembling_report",
        "persisting_evidence",
        "writing_report",
        "idle_between_cycles",
        "idle_bounded_snapshot_lag",
    }
    handshake = {
        "worker_running": worker.get("status") == "running",
        "worker_idle_unchanged": worker.get("phase") == "idle_unchanged_inputs",
        "worker_catching_up_or_bounded": phase in catchup_phases,
        "worker_fresh": worker_age is not None and worker_age <= maximum_age_sec,
        "worker_progress_recent": worker_progress_recent,
        "worker_error_free": int(worker.get("errors") or 0) == 0
        and not str(worker.get("last_error") or ""),
        "fingerprint_match": bool(checkpoint.get("fingerprint_sha256"))
        and checkpoint.get("fingerprint_sha256")
        == worker_details.get("input_fingerprint_sha256"),
        "run_id_match": bool(report.get("run_id"))
        and report.get("run_id") == compact.get("run_id"),
        "source_highwater_match": report_highwater is not None
        and int(report_highwater) == int(compact.get("source_highwater_row_id") or -1),
        "output_integrity_present": bool(output_checks),
        "output_integrity_match": bool(output_checks) and all(output_checks.values()),
        "report_fail_closed": bool(report.get("research_only"))
        and not bool(report.get("can_place_orders"))
        and not bool(report.get("can_promote")),
        "report_within_fresh_age": report_age is not None
        and report_age <= maximum_age_sec,
        "report_within_bounded_lag": report_age is not None
        and report_age <= maximum_snapshot_lag_sec,
    }
    intact_snapshot = bool(
        handshake["run_id_match"]
        and handshake["source_highwater_match"]
        and handshake["output_integrity_present"]
        and handshake["output_integrity_match"]
        and handshake["report_fail_closed"]
    )
    fresh_publication = bool(handshake["report_within_fresh_age"] and intact_snapshot)
    unchanged_current = bool(
        intact_snapshot
        and handshake["worker_running"]
        and handshake["worker_idle_unchanged"]
        and handshake["worker_fresh"]
        and handshake["worker_error_free"]
        and handshake["fingerprint_match"]
    )
    bounded_catchup = bool(
        intact_snapshot
        and handshake["report_within_bounded_lag"]
        and handshake["worker_running"]
        and handshake["worker_catching_up_or_bounded"]
        and handshake["worker_fresh"]
        and handshake["worker_progress_recent"]
        and handshake["worker_error_free"]
    )
    current = fresh_publication or unchanged_current or bounded_catchup
    if fresh_publication:
        reason = "fresh_verified_report_publication"
    elif unchanged_current:
        reason = "semantically_current_unchanged_inputs"
    elif bounded_catchup:
        reason = "verified_bounded_snapshot_while_catching_up"
    else:
        reason = "stale_or_unverified_edge_report"
    return {
        "fresh": current,
        "reason": reason,
        "report_age_sec": report_age,
        "worker_age_sec": worker_age,
        "maximum_snapshot_lag_sec": maximum_snapshot_lag_sec,
        "handshake": handshake,
        "output_checks": output_checks,
    }


def _proof_registry_replay(
    connection: sqlite3.Connection, published_state: dict[str, Any]
) -> tuple[bool, dict[str, Any]]:
    cohorts = {
        str(row["cohort_id"]): str(row["family"])
        for row in connection.execute(
            "SELECT cohort_id,family FROM proof_cohorts ORDER BY rowid"
        )
    }
    active: dict[str, str] = {}
    incoming: Counter[str] = Counter()
    errors: list[str] = []
    transitions = list(
        connection.execute(
            """
            SELECT transition_id,family,previous_cohort_id,next_cohort_id
            FROM proof_cohort_transitions ORDER BY rowid
            """
        )
    )
    for row in transitions:
        family = str(row["family"])
        previous = None if row["previous_cohort_id"] is None else str(row["previous_cohort_id"])
        next_cohort = str(row["next_cohort_id"])
        if previous != active.get(family):
            errors.append(f"{row['transition_id']}:previous_head_mismatch")
        if cohorts.get(next_cohort) != family:
            errors.append(f"{row['transition_id']}:next_missing_or_family_mismatch")
        if previous is not None and cohorts.get(previous) != family:
            errors.append(f"{row['transition_id']}:previous_missing_or_family_mismatch")
        incoming[next_cohort] += 1
        active[family] = next_cohort
    for cohort_id in cohorts:
        if incoming[cohort_id] != 1:
            errors.append(f"{cohort_id}:incoming_transition_count={incoming[cohort_id]}")
    published_active = {
        str(key): str(value)
        for key, value in (published_state.get("active_cohorts") or {}).items()
    }
    if active != published_active:
        errors.append("published_active_cohorts_do_not_match_transition_replay")
    triggers = _append_only_triggers(
        connection, ["proof_cohorts", "proof_cohort_transitions"]
    )
    if not all(triggers.values()):
        errors.append("append_only_triggers_missing")
    return not errors, {
        "cohort_count": len(cohorts),
        "transition_count": len(transitions),
        "replayed_active_cohorts": dict(sorted(active.items())),
        "published_active_cohorts": dict(sorted(published_active.items())),
        "append_only_triggers": triggers,
        "errors": errors,
    }


def _allocator_registry_replay(
    connection: sqlite3.Connection, published_state: dict[str, Any]
) -> tuple[bool, dict[str, Any]]:
    cohorts = {
        str(row["cohort_id"]): {
            "policy_id": str(row["policy_id"]),
            "phase": str(row["phase"]),
        }
        for row in connection.execute(
            "SELECT cohort_id,policy_id,phase FROM allocator_cohorts ORDER BY rowid"
        )
    }
    active: dict[tuple[str, str], str] = {}
    incoming: Counter[str] = Counter()
    errors: list[str] = []
    transitions = list(
        connection.execute(
            """
            SELECT transition_id,policy_id,previous_cohort_id,next_cohort_id
            FROM allocator_cohort_transitions ORDER BY rowid
            """
        )
    )
    for row in transitions:
        policy_id = str(row["policy_id"])
        previous = None if row["previous_cohort_id"] is None else str(row["previous_cohort_id"])
        next_cohort = str(row["next_cohort_id"])
        next_record = cohorts.get(next_cohort)
        if next_record is None or next_record["policy_id"] != policy_id:
            errors.append(f"{row['transition_id']}:next_missing_or_policy_mismatch")
            continue
        key = (policy_id, next_record["phase"])
        if previous != active.get(key):
            errors.append(f"{row['transition_id']}:previous_head_mismatch")
        if previous is not None:
            previous_record = cohorts.get(previous)
            if previous_record is None or (
                previous_record["policy_id"], previous_record["phase"]
            ) != key:
                errors.append(f"{row['transition_id']}:previous_missing_or_scope_mismatch")
        incoming[next_cohort] += 1
        active[key] = next_cohort
    for cohort_id in cohorts:
        if incoming[cohort_id] != 1:
            errors.append(f"{cohort_id}:incoming_transition_count={incoming[cohort_id]}")
    published = published_state.get("cohort") or {}
    published_id = str(published.get("cohort_id") or "")
    if published_id:
        record = cohorts.get(published_id)
        if record is None or active.get((record["policy_id"], record["phase"])) != published_id:
            errors.append("published_allocator_cohort_is_not_replayed_active_head")
    triggers = _append_only_triggers(
        connection,
        [
            "allocator_cohorts",
            "allocator_cohort_transitions",
            "decisions",
            "decision_candidates",
            "comparator_selections",
        ],
    )
    if not all(triggers.values()):
        errors.append("append_only_triggers_missing")
    return not errors, {
        "cohort_count": len(cohorts),
        "transition_count": len(transitions),
        "replayed_active_cohorts": {
            f"{key[0]}::{key[1]}": value for key, value in sorted(active.items())
        },
        "published_cohort_id": published_id or None,
        "append_only_triggers": triggers,
        "errors": errors,
    }


def _signed_age_seconds(value: Any) -> float | None:
    """Return signed age so a future completion cannot look freshly valid."""
    try:
        epoch = datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None
    return time.time() - epoch


def _start_verification_progress_heartbeat(
    *,
    state_path: Path,
    previous_state: dict[str, Any],
    verification_started_utc: str,
    heartbeat_sec: float = 30.0,
    guard_path: Path | None = None,
    ownership: dict[str, Any] | None = None,
) -> tuple[threading.Event, threading.Thread]:
    """Keep supervision alive while the read-only verifier is rebuilding.

    The heartbeat is deliberately *not* a successful verification state.  Any
    executor or authorization reader therefore fails closed for the duration
    of the rebuild, while the supervisor can distinguish a long scan from a
    dead worker.  Prior completed checks are retained only as restart context.
    """
    stop = threading.Event()

    def publish() -> None:
        payload = _owned_progress_payload(
            previous_state=previous_state,
            verification_started_utc=verification_started_utc,
            ownership=ownership,
            active_check="verification_pass",
        )
        if guard_path is not None and ownership is not None:
            if not _publish_owned_progress(
                state_path=state_path,
                guard_path=guard_path,
                ownership=ownership,
                payload=payload,
            ):
                stop.set()
            return
        atomic_json(state_path, payload)

    def loop() -> None:
        interval = max(1.0, float(heartbeat_sec))
        while not stop.wait(interval):
            publish()

    publish()
    thread = threading.Thread(
        target=loop,
        name="independent_verifier_progress_heartbeat",
        daemon=True,
    )
    thread.start()
    return stop, thread


def _append_only_triggers(connection: sqlite3.Connection, tables: list[str]) -> dict[str, bool]:
    sql = "\n".join(
        str(row[0] or "")
        for row in connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND sql IS NOT NULL"
        )
    ).upper()
    return {
        table: (
            f"BEFORE UPDATE ON {table.upper()}" in sql
            and f"BEFORE DELETE ON {table.upper()}" in sql
        )
        for table in tables
    }


def _integrity_evidence(
    connection: sqlite3.Connection,
    *,
    check_name: str,
    previous_state: dict[str, Any],
    maximum_full_check_age_sec: float,
    progress_state_path: Path | None = None,
    progress_heartbeat_sec: float = 30.0,
) -> tuple[bool, dict[str, Any]]:
    """Run full SQLite checks on a slow cadence and reuse only a recent pass.

    Logical state and append-only guards are still rebuilt on every verifier
    pass.  Multi-gigabyte live databases made a full ``quick_check`` every five
    minutes slower than the governance freshness boundary itself.  A missing,
    expired, or failed prior check always triggers a new full scan; only a
    recent successful scan may be reused.
    """
    prior = next(
        (
            row
            for row in previous_state.get("checks", [])
            if isinstance(row, dict) and row.get("name") == check_name
        ),
        None,
    )
    prior_evidence = (prior or {}).get("evidence") or {}
    # Only an explicit timestamp written *after* PRAGMA quick_check completed
    # can authorize reuse.  generated_utc, a progress heartbeat, and the old
    # v1 last_full_check_utc field are intentionally never migrated.
    completed_full_check_utc = prior_evidence.get("completed_full_check_utc")
    age_sec = _signed_age_seconds(completed_full_check_utc)
    reusable = (
        bool((prior or {}).get("passed"))
        and prior_evidence.get("result") == "ok"
        and age_sec is not None
        and age_sec >= -MAXIMUM_COMPLETED_CHECK_FUTURE_SKEW_SEC
        and age_sec <= maximum_full_check_age_sec
    )
    if reusable:
        return True, {
            "result": "ok",
            "verification_mode": "recent_full_check_plus_current_logical_rebuild",
            "completed_full_check_utc": completed_full_check_utc,
            "full_check_age_sec": age_sec,
            "maximum_full_check_age_sec": maximum_full_check_age_sec,
        }
    verification_started_utc = utc_now()
    stop_heartbeat = threading.Event()

    def publish_progress() -> None:
        if progress_state_path is None:
            return
        # This state is intentionally authorization-ineligible.  Its sole
        # purpose is to tell supervision that a potentially long, read-only
        # SQLite integrity scan is making legitimate progress.  Retaining the
        # prior check rows lets a crash/restart resume without discarding the
        # last completed checks, but status never inherits the prior match.
        atomic_json(
            progress_state_path,
            _owned_progress_payload(
                previous_state=previous_state,
                verification_started_utc=verification_started_utc,
                ownership=None,
                active_check=check_name,
            ),
        )

    def heartbeat_loop() -> None:
        interval = max(1.0, float(progress_heartbeat_sec))
        while not stop_heartbeat.wait(interval):
            publish_progress()

    heartbeat: threading.Thread | None = None
    if progress_state_path is not None:
        publish_progress()
        heartbeat = threading.Thread(
            target=heartbeat_loop,
            name=f"{check_name}_progress_heartbeat",
            daemon=True,
        )
        heartbeat.start()
    try:
        result = str(connection.execute("PRAGMA quick_check").fetchone()[0])
    finally:
        stop_heartbeat.set()
        if heartbeat is not None:
            heartbeat.join(timeout=max(2.0, float(progress_heartbeat_sec) + 1.0))
    return result == "ok", {
        "result": result,
        "verification_mode": "full_sqlite_quick_check",
        "completed_full_check_utc": utc_now(),
        "full_check_age_sec": 0.0,
        "maximum_full_check_age_sec": maximum_full_check_age_sec,
    }


def _independently_confirmed(row: dict[str, Any]) -> tuple[bool, str]:
    """Require all proof facts in the immutable lifecycle event itself.

    A production label alone is never accepted as independent confirmation.
    Unknown or incomplete confirmation evidence fails closed.
    """
    try:
        evidence = json.loads(str(row.get("evidence_json") or "{}"))
    except json.JSONDecodeError:
        return False, "confirmation_evidence_invalid_json"
    required_true = (
        "hierarchical_fdr_survivor",
        "untouched_confirmation_passed",
        "cost_stress_passed",
        "concentration_stress_passed",
    )
    missing = [key for key in required_true if evidence.get(key) is not True]
    lower = evidence.get("time_uniform_lower_bound_pips")
    minimum = evidence.get("minimum_economic_edge_pips")
    if missing:
        return False, "missing_independent_facts:" + ",".join(missing)
    try:
        if float(lower) < float(minimum):
            return False, "time_uniform_lower_bound_below_minimum_edge"
    except (TypeError, ValueError):
        return False, "confirmation_bounds_missing"
    return True, "independent_confirmation_facts_pass"


def _verify_owned_pass(
    *,
    lifecycle_database: Path = DEFAULT_LIFECYCLE_DATABASE,
    lifecycle_state: Path = DEFAULT_LIFECYCLE_STATE,
    evidence_operations_worker_state: Path = DEFAULT_EVIDENCE_OPERATIONS_WORKER_STATE,
    edge_state: Path = DEFAULT_EDGE_STATE,
    edge_worker_state: Path = DEFAULT_EDGE_WORKER_STATE,
    edge_input_checkpoint: Path = DEFAULT_EDGE_INPUT_CHECKPOINT,
    proof_database: Path = DEFAULT_PROOF_DATABASE,
    proof_state: Path = DEFAULT_PROOF_STATE,
    allocator_database: Path = DEFAULT_ALLOCATOR_DATABASE,
    allocator_state: Path = DEFAULT_ALLOCATOR_STATE,
    authorization: Path = DEFAULT_AUTHORIZATION,
    genealogy_database: Path = DEFAULT_GENEALOGY_DATABASE,
    source_database: Path = DEFAULT_SOURCE_DATABASE,
    state_path: Path = DEFAULT_STATE,
    report_path: Path = DEFAULT_REPORT,
    maximum_state_age_sec: float = 1800.0,
    maximum_lifecycle_snapshot_lag_sec: float = DEFAULT_MAXIMUM_LIFECYCLE_SNAPSHOT_LAG_SEC,
    maximum_edge_snapshot_lag_sec: float = DEFAULT_MAXIMUM_EDGE_SNAPSHOT_LAG_SEC,
    full_integrity_interval_sec: float = DEFAULT_FULL_INTEGRITY_INTERVAL_SEC,
    publication_guard_path: Path,
    ownership: dict[str, Any],
    previous_state: dict[str, Any],
    heartbeat_holder: list[tuple[threading.Event, threading.Thread]],
) -> dict[str, Any]:
    verification_started = utc_now()
    pass_started = time.monotonic()
    stage_started = pass_started
    stage_timings_sec: dict[str, float] = {}

    def mark_stage(name: str) -> None:
        nonlocal stage_started
        now = time.monotonic()
        stage_timings_sec[name] = round(now - stage_started, 6)
        stage_started = now
        print(
            json.dumps(
                {
                    "event": "independent_verifier_stage",
                    "stage": name,
                    "elapsed_sec": round(now - pass_started, 6),
                    "observed_utc": utc_now(),
                },
                sort_keys=True,
            ),
            flush=True,
        )
    checks: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    verified_confirmed: list[dict[str, str]] = []
    progress_stop, progress_thread = _start_verification_progress_heartbeat(
        state_path=state_path,
        previous_state=previous_state,
        verification_started_utc=verification_started,
        guard_path=publication_guard_path,
        ownership=ownership,
    )
    heartbeat_holder.append((progress_stop, progress_thread))

    published_lifecycle = read_json(lifecycle_state)
    published_edge = read_json(edge_state)
    published_proof = read_json(proof_state)
    published_allocator = read_json(allocator_state)
    published_authorization = read_json(authorization)
    mark_stage("read_published_states")

    with ro(proof_database) as connection:
        passed, evidence = _integrity_evidence(
            connection,
            check_name="proof_registry_sqlite_integrity",
            previous_state=previous_state,
            maximum_full_check_age_sec=full_integrity_interval_sec,
        )
        _check(checks, "proof_registry_sqlite_integrity", passed, **evidence)
        replay_passed, replay_evidence = _proof_registry_replay(
            connection, published_proof
        )
        _check(
            checks,
            "proof_cohort_transition_replay",
            replay_passed,
            **replay_evidence,
        )
    mark_stage("replay_proof_registry")

    with ro(lifecycle_database) as connection:
        passed, evidence = _integrity_evidence(
            connection,
            check_name="lifecycle_sqlite_integrity",
            previous_state=previous_state,
            maximum_full_check_age_sec=full_integrity_interval_sec,
        )
        _check(checks, "lifecycle_sqlite_integrity", passed, **evidence)
        rows = [
            dict(row)
            for row in connection.execute(
                """
                SELECT hypothesis.hypothesis_id,hypothesis.cohort_id,
                       event.next_state,event.observed_utc,event.evidence_json
                FROM hypotheses AS hypothesis
                JOIN lifecycle_events AS event
                  ON event.hypothesis_id=hypothesis.hypothesis_id
                JOIN (
                    SELECT hypothesis_id,MAX(rowid) AS latest_rowid
                    FROM lifecycle_events GROUP BY hypothesis_id
                ) AS latest ON latest.latest_rowid=event.rowid
                """
            )
        ]
        state_counts = Counter(str(row["next_state"]) for row in rows)
        for state_name in (
            "continue_collecting",
            "futility_rejected",
            "confirmed_candidate",
        ):
            state_counts.setdefault(state_name, 0)
        hypothesis_count = int(connection.execute("SELECT COUNT(*) FROM hypotheses").fetchone()[0])
        duplicate_definitions = int(
            connection.execute(
                "SELECT COUNT(*)-(SELECT COUNT(DISTINCT hypothesis_id) FROM hypotheses) FROM hypotheses"
            ).fetchone()[0]
        )
        retirement_count = int(connection.execute("SELECT COUNT(*) FROM futility_retirements").fetchone()[0])
        retired_not_futile = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM futility_retirements AS retirement
                JOIN lifecycle_events AS event ON event.hypothesis_id=retirement.hypothesis_id
                JOIN (SELECT hypothesis_id,MAX(rowid) latest_rowid FROM lifecycle_events GROUP BY hypothesis_id) latest
                  ON latest.latest_rowid=event.rowid
                WHERE event.next_state!='futility_rejected'
                """
            ).fetchone()[0]
        )
        resurrection_count = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM lifecycle_events AS later
                JOIN (
                    SELECT hypothesis_id,MIN(rowid) retired_rowid FROM lifecycle_events
                    WHERE next_state='futility_rejected' GROUP BY hypothesis_id
                ) AS retired ON retired.hypothesis_id=later.hypothesis_id
                WHERE later.rowid>retired.retired_rowid AND later.next_state!='futility_rejected'
                """
            ).fetchone()[0]
        )
        trigger_state = _append_only_triggers(
            connection, ["hypotheses", "lifecycle_events", "futility_retirements"]
        )
        lifecycle_integrity_snapshot = _lifecycle_database_integrity_snapshot(
            connection
        )
    mark_stage("rebuild_lifecycle")

    published_counts = (
        (published_lifecycle.get("lifecycle") or {}).get("states") or {}
    )
    _check(
        checks,
        "lifecycle_current_state_rebuild",
        hypothesis_count == len(rows)
        and hypothesis_count == int((published_lifecycle.get("lifecycle") or {}).get("hypothesis_count", -1))
        and dict(state_counts) == {str(k): int(v) for k, v in published_counts.items()},
        database_hypotheses=hypothesis_count,
        rebuilt_latest_events=len(rows),
        rebuilt_states=dict(state_counts),
        published_states=published_counts,
    )
    _check(
        checks,
        "lifecycle_publication_integrity",
        bool(published_lifecycle.get("integrity"))
        and published_lifecycle.get("integrity") == lifecycle_integrity_snapshot,
        published=published_lifecycle.get("integrity"),
        rebuilt=lifecycle_integrity_snapshot,
    )
    _check(checks, "unique_hypothesis_definitions", duplicate_definitions == 0, duplicates=duplicate_definitions)
    _check(
        checks,
        "futility_retirement_permanent",
        retirement_count == int(state_counts.get("futility_rejected", 0))
        and retired_not_futile == 0
        and resurrection_count == 0,
        retirement_rows=retirement_count,
        current_futility=int(state_counts.get("futility_rejected", 0)),
        retired_not_currently_futile=retired_not_futile,
        resurrection_events=resurrection_count,
    )
    _check(checks, "lifecycle_append_only_guards", all(trigger_state.values()), tables=trigger_state)

    for row in rows:
        if row["next_state"] != "confirmed_candidate":
            continue
        passed, reason = _independently_confirmed(row)
        if passed:
            verified_confirmed.append(
                {
                    "governed_hypothesis_id": str(row["hypothesis_id"]),
                    "proof_cohort_id": str(row["cohort_id"]),
                }
            )
        else:
            warnings.append(
                {
                    "name": "unverified_production_confirmation",
                    "hypothesis_id": str(row["hypothesis_id"]),
                    "reason": reason,
                }
            )
    _check(
        checks,
        "all_confirmed_candidates_independently_supported",
        len(verified_confirmed) == int(state_counts.get("confirmed_candidate", 0)),
        production_confirmed=int(state_counts.get("confirmed_candidate", 0)),
        independently_verified=len(verified_confirmed),
    )
    mark_stage("verify_confirmations")

    with ro(allocator_database) as connection:
        passed, evidence = _integrity_evidence(
            connection,
            check_name="allocator_sqlite_integrity",
            previous_state=previous_state,
            maximum_full_check_age_sec=full_integrity_interval_sec,
        )
        _check(checks, "allocator_sqlite_integrity", passed, **evidence)
        allocator_counts = {
            "decisions": int(connection.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]),
            "candidate_outcomes": int(connection.execute("SELECT COUNT(*) FROM candidate_outcomes").fetchone()[0]),
            "confirmation_cohorts": int(connection.execute("SELECT COUNT(*) FROM allocator_confirmation_cohorts").fetchone()[0]),
        }
        latest_allocator = connection.execute(
            """
            SELECT event.cohort_id,event.next_state,event.observed_utc
            FROM allocator_lifecycle_events AS event ORDER BY event.rowid DESC LIMIT 1
            """
        ).fetchone()
        allocator_replay_passed, allocator_replay_evidence = _allocator_registry_replay(
            connection, published_allocator
        )
        _check(
            checks,
            "allocator_cohort_transition_replay",
            allocator_replay_passed,
            **allocator_replay_evidence,
        )
    mark_stage("rebuild_allocator")
    published_allocator_counts = published_allocator.get("counts") or {}
    _check(
        checks,
        "allocator_current_state_rebuild",
        allocator_counts["candidate_outcomes"] == int(published_allocator_counts.get("candidate_outcomes", -1))
        and allocator_counts["confirmation_cohorts"] == int(published_allocator_counts.get("confirmation_cohorts", -1)),
        rebuilt=allocator_counts,
        published=published_allocator_counts,
        latest_lifecycle=(dict(latest_allocator) if latest_allocator else None),
    )

    with ro(genealogy_database) as connection:
        passed, evidence = _integrity_evidence(
            connection,
            check_name="genealogy_sqlite_integrity",
            previous_state=previous_state,
            maximum_full_check_age_sec=full_integrity_interval_sec,
        )
        genealogy_ids = {
            str(row[0])
            for row in connection.execute(
                "SELECT hypothesis_id FROM experiments WHERE experiment_kind='governed_cell'"
            )
        }
        genealogy_triggers = _append_only_triggers(connection, ["experiments", "experiment_observations"])
    mark_stage("rebuild_genealogy")
    lifecycle_ids = {str(row["hypothesis_id"]) for row in rows}
    _check(checks, "genealogy_sqlite_integrity", passed, **evidence)
    _check(
        checks,
        "genealogy_covers_lifecycle",
        lifecycle_ids.issubset(genealogy_ids),
        lifecycle_count=len(lifecycle_ids),
        genealogy_governed_cell_count=len(genealogy_ids),
        missing_count=len(lifecycle_ids-genealogy_ids),
    )
    _check(checks, "genealogy_append_only_guards", all(genealogy_triggers.values()), tables=genealogy_triggers)

    with ro(source_database) as connection:
        passed, evidence = _integrity_evidence(
            connection,
            check_name="source_registry_sqlite_integrity",
            previous_state=previous_state,
            maximum_full_check_age_sec=full_integrity_interval_sec,
        )
        source_triggers = _append_only_triggers(
            connection,
            [
                "source_contracts",
                "source_events",
                "source_supersession_events",
                "source_event_quarantines",
                "source_card_snapshots",
            ],
        )
    mark_stage("verify_source_registry")
    _check(checks, "source_registry_sqlite_integrity", passed, **evidence)
    _check(checks, "source_registry_append_only_guards", all(source_triggers.values()), tables=source_triggers)

    # Reconstruction above intentionally uses the pass-start payloads.  Read
    # the publications again only for the end-of-pass freshness gate so a long
    # integrity scan cannot manufacture a stale-state mismatch after the live
    # publishers have already refreshed their files.
    current_publication_ages = _current_published_state_ages(
        lifecycle_state=lifecycle_state,
        allocator_state=allocator_state,
        edge_state=edge_state,
    )
    edge_age = current_publication_ages["edge_age_sec"]
    lifecycle_age = current_publication_ages["lifecycle_age_sec"]
    allocator_age = current_publication_ages["allocator_age_sec"]
    lifecycle_freshness = _lifecycle_publication_freshness(
        lifecycle_state=lifecycle_state,
        lifecycle_database=lifecycle_database,
        worker_state=evidence_operations_worker_state,
        maximum_age_sec=maximum_state_age_sec,
        maximum_snapshot_lag_sec=maximum_lifecycle_snapshot_lag_sec,
    )
    lifecycle_unchanged_during_verification = bool(
        lifecycle_freshness.get("database_integrity")
        == lifecycle_integrity_snapshot
    )
    _check(
        checks,
        "published_governance_state_fresh",
        bool(lifecycle_freshness["fresh"])
        and lifecycle_unchanged_during_verification
        and allocator_age is not None
        and allocator_age <= maximum_state_age_sec,
        lifecycle_age_sec=lifecycle_age,
        allocator_age_sec=allocator_age,
        lifecycle_generated_utc=current_publication_ages["lifecycle_generated_utc"],
        allocator_generated_utc=current_publication_ages["allocator_generated_utc"],
        maximum_age_sec=maximum_state_age_sec,
        lifecycle_unchanged_during_verification=lifecycle_unchanged_during_verification,
        lifecycle_freshness=lifecycle_freshness,
    )
    edge_freshness = _edge_report_freshness(
        edge_state=edge_state,
        worker_state=edge_worker_state,
        input_checkpoint=edge_input_checkpoint,
        maximum_age_sec=maximum_state_age_sec,
        maximum_snapshot_lag_sec=maximum_edge_snapshot_lag_sec,
    )
    if not edge_freshness["fresh"]:
        warnings.append(
            {
                "name": "edge_report_snapshot_stale",
                "age_sec": edge_age,
                "maximum_age_sec": maximum_state_age_sec,
                "freshness": edge_freshness,
            }
        )
    _check(
        checks,
        "edge_report_fresh_or_semantically_current",
        bool(edge_freshness["fresh"]),
        **edge_freshness,
    )
    _check(
        checks,
        "edge_report_cannot_overstate_confirmation",
        int(published_edge.get("confirmation_passing_count", -1))
        >= int(state_counts.get("confirmed_candidate", 0)),
        edge_confirmation_passing=int(published_edge.get("confirmation_passing_count", -1)),
        lifecycle_confirmed=int(state_counts.get("confirmed_candidate", 0)),
        edge_report_age_sec=edge_age,
    )

    authorized_entries = published_authorization.get("authorized_entries") or []
    verified_keys = {
        (row["governed_hypothesis_id"], row["proof_cohort_id"])
        for row in verified_confirmed
    }
    auth_keys = {
        (str(row.get("governed_hypothesis_id") or ""), str(row.get("proof_cohort_id") or ""))
        for row in authorized_entries if isinstance(row, dict)
    }
    auth_disabled_consistent = (
        not bool(published_authorization.get("entry_authorized"))
        and len(authorized_entries) == 0
    )
    auth_enabled_consistent = (
        bool(published_authorization.get("entry_authorized"))
        and bool(authorized_entries)
        and auth_keys.issubset(verified_keys)
    )
    _check(
        checks,
        "authorization_matches_independent_lifecycle",
        auth_disabled_consistent or auth_enabled_consistent,
        entry_authorized=bool(published_authorization.get("entry_authorized")),
        authorized_entry_count=len(authorized_entries),
        independently_verified_count=len(verified_confirmed),
    )

    failed = [row for row in checks if not row["passed"]]
    status = "match" if not failed else "mismatch"
    generated = utc_now()
    payload = {
        "schema_version": 2,
        "publication_contract": PUBLICATION_CONTRACT,
        "generated_utc": generated,
        "completed_verification_utc": generated,
        "verification_started_utc": verification_started,
        "verification_pass_id": ownership["pass_id"],
        "verification_owner_id": ownership["owner_id"],
        "publication_generation": ownership["generation"],
        "publication_guard": ownership["guard_path"],
        "verification_duration_sec": round(time.monotonic() - pass_started, 6),
        "stage_timings_sec": stage_timings_sec,
        "status": status,
        "authorization_safe": status == "match",
        "research_only": True,
        "read_only": True,
        "can_promote": False,
        "can_place_orders": False,
        "real_money_routing": False,
        "independent_implementation": True,
        "production_calculator_imports": [],
        "verified_confirmed_candidates": verified_confirmed,
        "rebuilt_lifecycle": {
            "hypothesis_count": hypothesis_count,
            "states": dict(sorted(state_counts.items())),
            "futility_retirements": retirement_count,
        },
        "checks": checks,
        "failed_checks": failed,
        "warnings": warnings,
        "input_fingerprints": {
            "lifecycle_state": file_sha256(lifecycle_state),
            "edge_state": file_sha256(edge_state),
            "allocator_state": file_sha256(allocator_state),
            "authorization": file_sha256(authorization),
        },
        "scope_note": (
            "Current lifecycle state, permanent futility, allocator state, append-only ledgers, "
            "genealogy coverage, published-state freshness, and authorization consistency are rebuilt. "
            "Any production confirmation must also expose all independent proof facts in its immutable "
            "lifecycle event or this verifier blocks it."
        ),
    }
    lines = [
        "# Independent evidence verifier", "", f"Generated: `{generated}`", "",
        f"Status: **{status.upper()}**", "",
        "This verifier imports no production calculator and cannot promote, authorize, or execute.", "",
        f"- Governed hypotheses rebuilt: **{hypothesis_count:,}**",
        f"- Continue / futility / confirmed: **{state_counts.get('continue_collecting',0):,} / {state_counts.get('futility_rejected',0):,} / {state_counts.get('confirmed_candidate',0):,}**",
        f"- Independently verified confirmations: **{len(verified_confirmed):,}**",
        f"- Failed checks: **{len(failed):,}**", "", "| Check | Result |", "|---|---|",
    ]
    lines.extend(f"| {row['name']} | {'PASS' if row['passed'] else 'FAIL'} |" for row in checks)
    if warnings:
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- `{row['name']}`: `{json.dumps(row, sort_keys=True)}`" for row in warnings)
    lines.append("")
    progress_stop.set()
    progress_thread.join(timeout=35.0)
    _publish_owned_final(
        state_path=state_path,
        report_path=report_path,
        guard_path=publication_guard_path,
        ownership=ownership,
        payload=payload,
        report="\n".join(lines),
    )
    return payload


def verify(
    *,
    lifecycle_database: Path = DEFAULT_LIFECYCLE_DATABASE,
    lifecycle_state: Path = DEFAULT_LIFECYCLE_STATE,
    evidence_operations_worker_state: Path = DEFAULT_EVIDENCE_OPERATIONS_WORKER_STATE,
    edge_state: Path = DEFAULT_EDGE_STATE,
    edge_worker_state: Path = DEFAULT_EDGE_WORKER_STATE,
    edge_input_checkpoint: Path = DEFAULT_EDGE_INPUT_CHECKPOINT,
    proof_database: Path = DEFAULT_PROOF_DATABASE,
    proof_state: Path = DEFAULT_PROOF_STATE,
    allocator_database: Path = DEFAULT_ALLOCATOR_DATABASE,
    allocator_state: Path = DEFAULT_ALLOCATOR_STATE,
    authorization: Path = DEFAULT_AUTHORIZATION,
    genealogy_database: Path = DEFAULT_GENEALOGY_DATABASE,
    source_database: Path = DEFAULT_SOURCE_DATABASE,
    state_path: Path = DEFAULT_STATE,
    report_path: Path = DEFAULT_REPORT,
    maximum_state_age_sec: float = 1800.0,
    maximum_lifecycle_snapshot_lag_sec: float = DEFAULT_MAXIMUM_LIFECYCLE_SNAPSHOT_LAG_SEC,
    maximum_edge_snapshot_lag_sec: float = DEFAULT_MAXIMUM_EDGE_SNAPSHOT_LAG_SEC,
    full_integrity_interval_sec: float = DEFAULT_FULL_INTEGRITY_INTERVAL_SEC,
    publication_guard_path: Path | None = None,
) -> dict[str, Any]:
    """Run one owned pass; every exceptional exit durably fails closed."""
    resolved_guard_path = publication_guard_path or verifier_guard_path(state_path)
    previous_state = read_json_optional(state_path)
    ownership = _claim_verification_pass(resolved_guard_path)
    heartbeat_holder: list[tuple[threading.Event, threading.Thread]] = []
    try:
        return _verify_owned_pass(
            lifecycle_database=lifecycle_database,
            lifecycle_state=lifecycle_state,
            evidence_operations_worker_state=evidence_operations_worker_state,
            edge_state=edge_state,
            edge_worker_state=edge_worker_state,
            edge_input_checkpoint=edge_input_checkpoint,
            proof_database=proof_database,
            proof_state=proof_state,
            allocator_database=allocator_database,
            allocator_state=allocator_state,
            authorization=authorization,
            genealogy_database=genealogy_database,
            source_database=source_database,
            state_path=state_path,
            report_path=report_path,
            maximum_state_age_sec=maximum_state_age_sec,
            maximum_lifecycle_snapshot_lag_sec=maximum_lifecycle_snapshot_lag_sec,
            maximum_edge_snapshot_lag_sec=maximum_edge_snapshot_lag_sec,
            full_integrity_interval_sec=full_integrity_interval_sec,
            publication_guard_path=resolved_guard_path,
            ownership=ownership,
            previous_state=previous_state,
            heartbeat_holder=heartbeat_holder,
        )
    except VerificationPassSuperseded:
        return {
            "schema_version": 2,
            "publication_contract": PUBLICATION_CONTRACT,
            "generated_utc": utc_now(),
            "status": "superseded",
            "authorization_safe": False,
            "can_place_orders": False,
            "can_promote": False,
            "research_only": True,
            "verification_pass_id": ownership["pass_id"],
            "verification_owner_id": ownership["owner_id"],
            "publication_generation": ownership["generation"],
            "verified_confirmed_candidates": [],
        }
    except BaseException as exc:
        _mark_guard_failed(
            resolved_guard_path,
            ownership,
            f"{type(exc).__name__}: {exc}",
        )
        raise
    finally:
        # This outer finally is the final heartbeat ownership boundary.  It is
        # intentionally outside the verification body, so any read, SQLite,
        # report, state-publication, or unexpected exception stops and joins
        # every pass heartbeat before control returns to the supervisor.
        for stop, thread in heartbeat_holder:
            stop.set()
        for _stop, thread in heartbeat_holder:
            thread.join(timeout=35.0)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lifecycle-database", type=Path, default=DEFAULT_LIFECYCLE_DATABASE)
    parser.add_argument("--lifecycle-state", type=Path, default=DEFAULT_LIFECYCLE_STATE)
    parser.add_argument(
        "--evidence-operations-worker-state",
        type=Path,
        default=DEFAULT_EVIDENCE_OPERATIONS_WORKER_STATE,
    )
    parser.add_argument("--edge-state", type=Path, default=DEFAULT_EDGE_STATE)
    parser.add_argument("--edge-worker-state", type=Path, default=DEFAULT_EDGE_WORKER_STATE)
    parser.add_argument("--edge-input-checkpoint", type=Path, default=DEFAULT_EDGE_INPUT_CHECKPOINT)
    parser.add_argument("--proof-database", type=Path, default=DEFAULT_PROOF_DATABASE)
    parser.add_argument("--proof-state", type=Path, default=DEFAULT_PROOF_STATE)
    parser.add_argument("--allocator-database", type=Path, default=DEFAULT_ALLOCATOR_DATABASE)
    parser.add_argument("--allocator-state", type=Path, default=DEFAULT_ALLOCATOR_STATE)
    parser.add_argument("--authorization", type=Path, default=DEFAULT_AUTHORIZATION)
    parser.add_argument("--genealogy-database", type=Path, default=DEFAULT_GENEALOGY_DATABASE)
    parser.add_argument("--source-database", type=Path, default=DEFAULT_SOURCE_DATABASE)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--publication-guard", type=Path)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--maximum-state-age-sec", type=float, default=1800.0)
    parser.add_argument(
        "--maximum-lifecycle-snapshot-lag-sec",
        type=float,
        default=DEFAULT_MAXIMUM_LIFECYCLE_SNAPSHOT_LAG_SEC,
    )
    parser.add_argument(
        "--maximum-edge-snapshot-lag-sec",
        type=float,
        default=DEFAULT_MAXIMUM_EDGE_SNAPSHOT_LAG_SEC,
    )
    parser.add_argument(
        "--full-integrity-interval-sec",
        type=float,
        default=DEFAULT_FULL_INTEGRITY_INTERVAL_SEC,
    )
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.time()
    last_status = "mismatch"
    while True:
        payload = verify(
            lifecycle_database=args.lifecycle_database,
            lifecycle_state=args.lifecycle_state,
            evidence_operations_worker_state=args.evidence_operations_worker_state,
            edge_state=args.edge_state,
            edge_worker_state=args.edge_worker_state,
            edge_input_checkpoint=args.edge_input_checkpoint,
            proof_database=args.proof_database,
            proof_state=args.proof_state,
            allocator_database=args.allocator_database,
            allocator_state=args.allocator_state,
            authorization=args.authorization,
            genealogy_database=args.genealogy_database,
            source_database=args.source_database,
            state_path=args.state,
            report_path=args.report,
            maximum_state_age_sec=args.maximum_state_age_sec,
            maximum_lifecycle_snapshot_lag_sec=args.maximum_lifecycle_snapshot_lag_sec,
            maximum_edge_snapshot_lag_sec=args.maximum_edge_snapshot_lag_sec,
            full_integrity_interval_sec=args.full_integrity_interval_sec,
            publication_guard_path=args.publication_guard,
        )
        last_status = str(payload["status"])
        if args.interval_sec <= 0.0 or (
            args.duration_sec > 0.0 and time.time() - started >= args.duration_sec
        ):
            break
        time.sleep(max(60.0, args.interval_sec))
    return 0 if last_status == "match" else 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["verify"]
