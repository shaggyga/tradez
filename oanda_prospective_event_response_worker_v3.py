#!/usr/bin/env python3
"""Adaptive research-only event-response worker (not supervisor-wired).

The default is exactly one explicit cycle.  A bounded duration must be supplied
to keep running.  From target-2s through target+45s it polls/captures every
second; outside active windows it uses the frozen 15-second idle cadence while
waking early for the next active window.

The heavy news/tagger and immutable-clock producers are supervisor-owned and
independent.  This worker never runs them synchronously; it consumes only the
latest already-published immutable clock and fails closed when its <=90-second
dual-freshness contract is not satisfied.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from forex_system.ingestion.prospective_event_response_v3 import (
        load_contract,
        open_ledger,
    )
except ModuleNotFoundError:
    from src.forex_system.ingestion.prospective_event_response_v3 import (
        load_contract,
        open_ledger,
    )

import oanda_prospective_event_response_v3 as collector


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
EVENT_ROOT = DATA / "news_event_tags"
DEFAULT_EVENTS = EVENT_ROOT / "events_latest.json"
DEFAULT_MANIFEST = EVENT_ROOT / "manifest.json"
DEFAULT_EVENT_SEED = ROOT / "config" / "news_event_seed_v1.json"
DEFAULT_WORKER_STATE = DATA / "state" / "prospective_event_response_worker_v3.json"


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(dict(payload), indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def _parse_utc(value: Any) -> dt.datetime | None:
    text = str(value or "").strip().replace("Z", "+00:00")
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(dt.timezone.utc)


def pending_target_times(ledger_path: Path) -> list[dt.datetime]:
    if not ledger_path.exists():
        return []
    connection = open_ledger(ledger_path)
    try:
        rows = connection.execute(
            """
            SELECT p.target_utc
            FROM event_response_plans_v3 p
            LEFT JOIN event_response_samples_v3 s ON s.plan_id=p.plan_id
            LEFT JOIN event_response_terminal_exclusions_v3 x ON x.plan_id=p.plan_id
            WHERE s.plan_id IS NULL AND x.plan_id IS NULL
            GROUP BY p.target_utc
            ORDER BY p.target_utc
            """
        ).fetchall()
    finally:
        connection.close()
    return [parsed for row in rows if (parsed := _parse_utc(row[0])) is not None]


def choose_cadence(
    now_utc: dt.datetime,
    target_times: Sequence[dt.datetime],
    contract: Mapping[str, Any],
) -> tuple[str, float]:
    now = now_utc.astimezone(dt.timezone.utc)
    cadence = contract.get("worker_cadence") or {}
    lead = float(cadence["active_lead_sec"])
    lag = float(cadence["active_lag_sec"])
    active_poll = float(cadence["active_poll_sec"])
    idle = float(cadence["idle_cycle_sec"])
    for target in target_times:
        if target - dt.timedelta(seconds=lead) <= now <= target + dt.timedelta(seconds=lag):
            return "active_target_window", active_poll
    future_starts = [
        (target - dt.timedelta(seconds=lead) - now).total_seconds()
        for target in target_times
        if target - dt.timedelta(seconds=lead) > now
    ]
    if future_starts:
        return "idle_waiting_for_target", max(0.05, min(idle, min(future_starts)))
    return "idle_no_near_target", idle


def _active_preflight_failure_reason(exc: Exception) -> str | None:
    """Classify only missing/stale clock or market-artifact failures."""

    text = str(exc).lower()
    if isinstance(exc, FileNotFoundError):
        return "active_target_required_artifact_missing_failed_closed"
    if any(
        token in text
        for token in (
            "clock",
            "attestation",
            "snapshot",
            "source_generated",
            "no_immutable_event",
        )
    ):
        return "active_target_clock_preflight_failed_closed"
    return None


def run_worker(
    *,
    duration_sec: float = 0.0,
    contract_path: Path = collector.DEFAULT_CONTRACT,
    events_path: Path = DEFAULT_EVENTS,
    manifest_path: Path = DEFAULT_MANIFEST,
    event_seed_path: Path = DEFAULT_EVENT_SEED,
    event_clock_db: Path = collector.DEFAULT_CLOCK_DB,
    clock_integrity_path: Path = collector.DEFAULT_CLOCK_INTEGRITY,
    ledger_path: Path = collector.DEFAULT_LEDGER,
    lock_path: Path = collector.DEFAULT_LOCK,
    state_path: Path = collector.DEFAULT_STATE,
    report_path: Path = collector.DEFAULT_REPORT,
    worker_state_path: Path = DEFAULT_WORKER_STATE,
    maximum_cycles: int | None = None,
) -> list[dict[str, Any]]:
    contract = load_contract(contract_path)
    started_monotonic = time.monotonic()
    results: list[dict[str, Any]] = []
    cycle_count = 0
    while True:
        cycle_count += 1
        cycle_started = dt.datetime.now(tz=dt.timezone.utc)
        targets = pending_target_times(ledger_path)
        mode, cadence_sec = choose_cadence(cycle_started, targets, contract)
        try:
            # The heavy news/event producer and immutable-clock writer are
            # supervisor-owned independent processes.  This target worker is a
            # pure consumer: it never waits for catalog synchronization and
            # only re-reads the latest immutable snapshot/attestation through
            # collector.run_once.  Stale lineage therefore fails closed without
            # delaying the one-second capture cadence.
            result = collector.run_once(
                contract_path=contract_path,
                event_clock_db=event_clock_db,
                clock_integrity_path=clock_integrity_path,
                ledger_path=ledger_path,
                lock_path=lock_path,
                state_path=state_path,
                report_path=report_path,
                cycle_mode=mode,
                cadence_sec=cadence_sec,
            )
            result = {**result, "worker_cycle_count": cycle_count, "worker_mode": mode}
        except Exception as exc:
            transient = {
                "status": "not_applicable",
                "attempts_inserted": 0,
            }
            failure_reason = (
                _active_preflight_failure_reason(exc)
                if mode == "active_target_window"
                else None
            )
            if failure_reason is not None:
                try:
                    transient = collector.record_active_preflight_failure(
                        ledger_path=ledger_path,
                        lock_path=lock_path,
                        contract=contract,
                        observed_utc=cycle_started,
                        reason=failure_reason,
                        error=str(exc),
                    )
                except Exception as attempt_exc:
                    transient = {
                        "status": "transient_attempt_append_failed_closed",
                        "attempts_inserted": 0,
                        "error": str(attempt_exc),
                    }
            if duration_sec <= 0:
                raise
            result = {
                    "status": "research_capture_cycle_failed_closed",
                    "generated_utc": dt.datetime.now(
                        tz=dt.timezone.utc
                    ).isoformat(),
                    "mode": mode,
                    "error": str(exc),
                    "transient_attempt": transient,
                    "research_only": True,
                    "execution_eligible": False,
                    "can_place_orders": False,
                    "supported_execution_decision": "no_trade",
                    "worker_cycle_count": cycle_count,
                    "worker_mode": mode,
                }
        # Retain only the latest result.  A week-long supervisor run therefore
        # has constant memory while still emitting a live compact heartbeat.
        results[:] = [result]
        heartbeat = {
            "schema_version": "prospective_event_response_worker_v3_heartbeat_v1",
            "generated_utc": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
            "cycle_count": cycle_count,
            "mode": mode,
            "status": result.get("status", "ok"),
            "last_result": result,
            "research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
            "supported_execution_decision": "no_trade",
        }
        _atomic_write_json(worker_state_path, heartbeat)
        print(json.dumps(heartbeat, sort_keys=True), flush=True)
        if duration_sec <= 0:
            break
        if maximum_cycles is not None and cycle_count >= max(1, int(maximum_cycles)):
            break
        elapsed = time.monotonic() - started_monotonic
        if elapsed >= duration_sec:
            break
        # Recalculate the window after each cycle and never sleep past its
        # target-2s boundary.  Processing overruns produce an immediate retry.
        now = dt.datetime.now(tz=dt.timezone.utc)
        _, next_sleep = choose_cadence(now, pending_target_times(ledger_path), contract)
        remaining = max(0.0, duration_sec - (time.monotonic() - started_monotonic))
        time.sleep(min(next_sleep, remaining))
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duration-sec",
        type=float,
        default=0.0,
        help="Explicit bounded duration; zero (default) runs one cycle only.",
    )
    parser.add_argument("--contract", type=Path, default=collector.DEFAULT_CONTRACT)
    parser.add_argument("--events", type=Path, default=DEFAULT_EVENTS)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--event-seed", type=Path, default=DEFAULT_EVENT_SEED)
    parser.add_argument("--event-clock-db", type=Path, default=collector.DEFAULT_CLOCK_DB)
    parser.add_argument("--clock-integrity", type=Path, default=collector.DEFAULT_CLOCK_INTEGRITY)
    parser.add_argument("--ledger", type=Path, default=collector.DEFAULT_LEDGER)
    parser.add_argument("--lock", type=Path, default=collector.DEFAULT_LOCK)
    parser.add_argument("--state", type=Path, default=collector.DEFAULT_STATE)
    parser.add_argument("--report", type=Path, default=collector.DEFAULT_REPORT)
    parser.add_argument("--worker-state", type=Path, default=DEFAULT_WORKER_STATE)
    args = parser.parse_args()
    results = run_worker(
        duration_sec=args.duration_sec,
        contract_path=args.contract,
        events_path=args.events,
        manifest_path=args.manifest,
        event_seed_path=args.event_seed,
        event_clock_db=args.event_clock_db,
        clock_integrity_path=args.clock_integrity,
        ledger_path=args.ledger,
        lock_path=args.lock,
        state_path=args.state,
        report_path=args.report,
        worker_state_path=args.worker_state,
    )
    print(json.dumps(results[-1], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
