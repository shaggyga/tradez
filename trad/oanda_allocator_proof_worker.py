#!/usr/bin/env python3
"""Continuously refresh the frozen, broker-free allocator proof ledger.

The allocator used to run only after the much larger lifecycle rebuild.  As
that rebuild grew, the allocator publication could be stale for nearly an
hour even though its own five-minute decision contract was healthy.  This
small runner gives the unchanged allocator implementation its own clock.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from oanda_allocator_proof import (
    DEFAULT_ACCOUNT,
    DEFAULT_CONFIG,
    DEFAULT_DATABASE,
    DEFAULT_EXECUTOR,
    DEFAULT_QUOTES,
    DEFAULT_SIGNAL_FEED,
    DEFAULT_STATE,
    run_allocator_cycle,
)
from oanda_worker_heartbeat import WorkerHeartbeat


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
DEFAULT_HEARTBEAT = STATE / "allocator_proof_worker_heartbeat_v1.json"


def run_once(
    *,
    database: Path = DEFAULT_DATABASE,
    state: Path = DEFAULT_STATE,
    config: Path = DEFAULT_CONFIG,
    signal_database: Path = DEFAULT_SIGNAL_FEED,
    quote_database: Path = DEFAULT_QUOTES,
    account: Path = DEFAULT_ACCOUNT,
    executor: Path = DEFAULT_EXECUTOR,
) -> dict:
    """Run one idempotent allocator bucket using only local evidence inputs."""

    return run_allocator_cycle(
        database_path=database,
        state_path=state,
        config_path=config,
        signal_database=signal_database,
        quote_database=quote_database,
        account_path=account,
        executor_path=executor,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--heartbeat", type=Path, default=DEFAULT_HEARTBEAT)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--signal-database", type=Path, default=DEFAULT_SIGNAL_FEED)
    parser.add_argument("--quote-database", type=Path, default=DEFAULT_QUOTES)
    parser.add_argument("--account", type=Path, default=DEFAULT_ACCOUNT)
    parser.add_argument("--executor", type=Path, default=DEFAULT_EXECUTOR)
    parser.add_argument("--interval-sec", type=float, default=300.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()

    started = time.monotonic()
    cycles = 0
    errors = 0
    heartbeat = WorkerHeartbeat(
        args.heartbeat,
        worker="oanda_allocator_proof_worker",
        role="research_only_frozen_allocator_proof",
        interval_sec=15.0,
    ).start()
    try:
        while True:
            heartbeat.update(
                phase="running_cycle",
                cycles=cycles,
                errors=errors,
                research_only=True,
                can_place_orders=False,
                can_promote=False,
                real_money_routing=False,
            )
            try:
                payload = run_once(
                    database=args.database,
                    state=args.state,
                    config=args.config,
                    signal_database=args.signal_database,
                    quote_database=args.quote_database,
                    account=args.account,
                    executor=args.executor,
                )
            except Exception as exc:
                errors += 1
                heartbeat.mark_progress(
                    phase="cycle_error",
                    cycles=cycles,
                    errors=errors,
                    last_error=f"{type(exc).__name__}: {exc}",
                )
            else:
                cycles += 1
                counts = payload.get("counts") or {}
                heartbeat.mark_progress(
                    phase="idle_between_cycles",
                    cycles=cycles,
                    errors=errors,
                    last_error="",
                    allocator_generated_utc=payload.get("generated_utc"),
                    cohort_id=(payload.get("cohort") or {}).get("cohort_id"),
                    decision_status=(payload.get("last_decision") or {}).get("status"),
                    decision_count=(payload.get("evidence") or {}).get("decision_count"),
                    candidate_outcomes=counts.get("candidate_outcomes"),
                )

            if args.duration_sec > 0.0 and time.monotonic() - started >= args.duration_sec:
                break
            time.sleep(max(1.0, float(args.interval_sec)))
    finally:
        heartbeat.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["DEFAULT_HEARTBEAT", "run_once"]
