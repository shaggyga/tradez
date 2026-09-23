#!/usr/bin/env python3
"""Reproduce three known news-fast-lane faults using disposable fixtures only.

This is an audit diagnostic, not a policy regression test that endorses the
faulty behavior. Exit zero means the diagnostic completed; inspect each
``fault_reproduced`` field. A future fix may make those fields false.

The worker and its pure governance dependency are imported without calling
their entry points. The integrity checker, timestamp parser, and fixture
helpers are AST-isolated from canonical source; no audit/runtime main is run.
Every worker invocation supplies explicit paths inside TemporaryDirectory.
No active database, network connection, supervisor, or runtime is started.
"""

from __future__ import annotations

import ast
import datetime as dt
import json
import sqlite3
import sys
import tempfile
import time
import types
from pathlib import Path
from typing import Any
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT))

import oanda_source_governance as governance  # noqa: E402
import oanda_source_governance_news_fast_lane as fast  # noqa: E402


def load_functions(path: Path, names: set[str], namespace: dict[str, Any]) -> None:
    """Execute selected function definitions, never module top-level code."""
    parsed = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
    functions = [
        node for node in parsed.body
        if isinstance(node, ast.FunctionDef) and node.name in names
    ]
    found = {node.name for node in functions}
    if found != names:
        raise RuntimeError(f"Missing canonical function definitions: {names - found}")
    isolated = ast.Module(body=functions, type_ignores=[])
    exec(compile(isolated, str(path), "exec"), namespace)


def build_helpers() -> tuple[Any, Any, Any]:
    namespace: dict[str, Any] = {
        "Any": Any, "Path": Path, "sqlite3": sqlite3, "time": time,
        "datetime": dt.datetime, "json": json,
        "governance": governance, "fast": fast,
        "NEWS_GOVERNANCE_FAST_LANE_ACTIVATED_UTC": fast.ACTIVATED_UTC,
        "NEWS_GOVERNANCE_FAST_LANE_CONTRACT_ID": fast.CONTRACT_ID,
        "NEWS_GOVERNANCE_FAST_LANE_COHORT_ID": fast.COHORT_ID,
        "NEWS_GOVERNANCE_FAST_LANE_PRIOR_CONTRACT_ID": fast.PRIOR_CONTRACT_ID,
        "NEWS_GOVERNANCE_FAST_LANE_PRIOR_RETIREMENT_REASON": fast.PRIOR_RETIREMENT_REASON,
    }
    load_functions(
        ROOT / "oanda_project_integrity_audit.py",
        {"parse_epoch", "news_source_governance_fast_lane_integrity"}, namespace,
    )
    load_functions(
        ROOT / "test_oanda_source_governance_news_fast_lane.py",
        {"_proof_payload", "_news_database", "_registry"}, namespace,
    )
    return (
        namespace["news_source_governance_fast_lane_integrity"],
        namespace["_news_database"], namespace["_registry"],
    )


def state_and_cursor_findings(check: Any, make_news: Any, make_registry: Any) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="forex_fastlane_state_audit_") as temporary:
        root = Path(temporary)
        database, news, state = root / "registry.sqlite", root / "news.sqlite", root / "state.json"
        paths = {"news_database": news, "database_path": database, "state_path": state}
        make_registry(database)
        first_seen = (fast.ACTIVATED_UTC + dt.timedelta(seconds=1)).isoformat()
        make_news(news, first_seen=first_seen, last_seen=first_seen)
        first_at = fast.ACTIVATED_UTC + dt.timedelta(seconds=2)
        first = fast.run(**paths, now=first_at)
        if not check(first, database_path=database, cutoff_epoch=first_at.timestamp())["ok"]:
            raise RuntimeError("Baseline fixture did not produce a valid committed snapshot")
        real_atomic = fast.atomic_json
        captured: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
        next_at = first_at + dt.timedelta(seconds=30)

        def capture(path: Path, value: dict[str, Any]) -> None:
            real_atomic(path, value)
            if value.get("status") == "building":
                sampled = fast.read_json(path)
                captured.append((
                    sampled,
                    check(sampled, database_path=database, cutoff_epoch=next_at.timestamp()),
                    check(first, database_path=database, cutoff_epoch=next_at.timestamp()),
                ))

        with patch.object(fast, "atomic_json", capture):
            second = fast.run(**paths, now=next_at)
        completed_ok = check(second, database_path=database, cutoff_epoch=next_at.timestamp())["ok"]
        if not completed_ok:
            raise RuntimeError("Second baseline fixture cycle did not complete validly")
        building: dict[str, Any] = {"fault_reproduced": False, "building_publications_observed": len(captured)}
        sampled = None
        if captured:
            sampled, during, previous = captured[0]
            building.update({
                "fault_reproduced": bool(previous["ok"] and not during["ok"]),
                "prior_completed_state_ok_during_same_cycle": previous["ok"],
                "sampled_status": sampled["status"],
                "sampled_missing_fields": [key for key in (
                    "next_scan_cursor_utc", "transaction_committed",
                    "total_receipt_count", "retained_invalid_prior_receipt_count",
                ) if key not in sampled],
                "sampled_integrity_ok": during["ok"],
                "sampled_state_contract_ok": during["state_contract_ok"],
                "sampled_cutoff_valid": during["state_snapshot_cutoff_valid"],
                "database_current_receipt_count": during["database_current_receipt_count"],
                "receipt_count_at_sampled_cutoff": during["receipt_count"],
                "after_completion_ok": completed_ok,
            })

        retries: dict[str, Any] = {}
        real_articles = governance.article_events
        for failure_name, target in (
            ("input_temporarily_unavailable", "article_events"),
            ("retryable_database_error", "connect_registry"),
        ):
            real_atomic(state, second)
            failed_at = next_at + dt.timedelta(seconds=30)
            with patch.object(governance, target, side_effect=sqlite3.OperationalError("fixture-only database is locked")):
                failed = fast.run(**paths, now=failed_at)
            calls: list[dict[str, Any]] = []

            def record_articles(*args: Any, **kwargs: Any) -> Any:
                calls.append(kwargs.copy())
                return real_articles(*args, **kwargs)

            with patch.object(governance, "article_events", record_articles):
                retry = fast.run(**paths, now=failed_at + dt.timedelta(seconds=30))
            if not calls or retry["status"] != "ok":
                raise RuntimeError(f"Fixture recovery did not complete: {failure_name}")
            expected = second["next_scan_cursor_utc"]
            actual = calls[0]["changed_after_utc"]
            retries[failure_name] = {
                "fault_reproduced": bool(actual != expected and actual == fast.ACTIVATED_UTC.isoformat()),
                "failed_status": failed["status"],
                "last_successful_cursor": expected,
                "failed_state_saved_cursor": failed.get("scan_cursor_utc"),
                "next_cycle_actual_query_cursor": actual,
                "activation_cursor": fast.ACTIVATED_UTC.isoformat(),
                "re_read_already_processed_rows": retry["input_row_count"],
                "new_receipts_after_replay": retry["new_receipt_count"],
                "retry_status": retry["status"],
            }
        interrupted: dict[str, Any] = {"scenario_available": sampled is not None}
        if sampled is not None:
            real_atomic(state, sampled)
            restart = fast.run(**paths, now=next_at + dt.timedelta(seconds=90))
            interrupted.update({
                "fault_reproduced": restart["scan_cursor_utc"] != sampled["scan_cursor_utc"],
                "saved_cursor": sampled["scan_cursor_utc"],
                "restart_actual_cursor": restart["scan_cursor_utc"],
                "re_read_already_processed_rows": restart["input_row_count"],
            })
        return {"building_publication": building, "retry_cursor": retries, "interrupted_building_restart": interrupted}


def delayed_commit_finding(check: Any, make_news: Any, make_registry: Any) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="forex_fastlane_clock_audit_") as temporary:
        root = Path(temporary)
        database, news, state = root / "registry.sqlite", root / "news.sqlite", root / "state.json"
        make_registry(database)
        first_seen = (fast.ACTIVATED_UTC + dt.timedelta(seconds=1)).isoformat()
        make_news(news, first_seen=first_seen, last_seen=first_seen)
        clock = {"now": fast.ACTIVATED_UTC + dt.timedelta(seconds=2)}
        evidence: dict[str, Any] = {}

        class FakeDatetime(dt.datetime):
            @classmethod
            def now(cls, tz: Any = None) -> dt.datetime:
                return clock["now"] if tz else clock["now"].replace(tzinfo=None)

        class DelayedCommit:
            def __init__(self, connection: sqlite3.Connection):
                self.connection = connection
                self.receipt_pending = False

            def __getattr__(self, name: str) -> Any:
                return getattr(self.connection, name)

            def execute(self, sql: str, *args: Any) -> Any:
                result = self.connection.execute(sql, *args)
                if sql.strip().startswith("INSERT OR IGNORE INTO news_fast_lane_import_receipts"):
                    self.receipt_pending = True
                    evidence["availability_written_utc"] = args[0][7]
                return result

            def commit(self) -> None:
                if self.receipt_pending:
                    clock["now"] += dt.timedelta(seconds=11)
                    reader = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)
                    try:
                        evidence["visible_receipts_after_claimed_availability_before_commit"] = reader.execute(
                            "SELECT COUNT(*) FROM news_fast_lane_import_receipts"
                        ).fetchone()[0]
                    finally:
                        reader.close()
                    evidence["pre_commit_utc"] = clock["now"].isoformat()
                self.connection.commit()
                if self.receipt_pending:
                    evidence["simulated_commit_completed_utc"] = clock["now"].isoformat()
                    self.receipt_pending = False

        original_connect = governance.connect_registry

        def connect_temporary(path: Path) -> DelayedCommit:
            if path.resolve() != database.resolve():
                raise RuntimeError("Refusing a registry path outside the temporary fixture")
            return DelayedCommit(original_connect(path))

        fake_dt = types.SimpleNamespace(datetime=FakeDatetime, timedelta=dt.timedelta, timezone=dt.timezone)
        with patch.object(fast, "dt", fake_dt), patch.object(governance, "connect_registry", connect_temporary):
            completed = fast.run(news_database=news, database_path=database, state_path=state)
        if completed["status"] != "ok" or "simulated_commit_completed_utc" not in evidence:
            raise RuntimeError("Delayed-commit fixture did not complete its receipt transaction")
        diagnostics = check(completed, database_path=database, cutoff_epoch=clock["now"].timestamp())
        availability = fast.parse_time(completed.get("governance_available_utc"))
        if availability is None:
            raise RuntimeError("Completed fixture did not publish an availability timestamp")
        lag = (clock["now"] - availability).total_seconds()
        evidence.update({
            "fault_reproduced": bool(lag > 0 and evidence["visible_receipts_after_claimed_availability_before_commit"] == 0),
            "clock_delay_injected_seconds": 11, "actual_sleep_seconds": 0,
            "availability_before_commit_seconds": lag,
            "completed_status": completed["status"],
            "checker_ok_despite_early_availability": diagnostics["ok"],
            "checker_optimistic_clock_receipts": diagnostics["optimistic_clock_receipts"],
            "checker_operational_clock_violations": diagnostics["operational_clock_violations"],
            "new_receipt_count": completed["new_receipt_count"],
            "historical_receipts_affected": "not_established",
        })
        return evidence


def main() -> int:
    check, make_news, make_registry = build_helpers()
    findings = state_and_cursor_findings(check, make_news, make_registry)
    findings["availability_before_commit"] = delayed_commit_finding(check, make_news, make_registry)
    print(json.dumps({
        "diagnostic_completed": True,
        "interpretation": "fault_reproduced=true identifies a defect; exit zero does not assert implementation health",
        "scope": "temporary miniature DBs, injected IO failures, virtual 11-second commit delay; no active DB/network/runtime entry point",
        "isolation": "worker/governance imported; checker, parse_epoch and test fixture helpers AST-isolated from canonical source",
        "findings": findings,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
