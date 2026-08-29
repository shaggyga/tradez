#!/usr/bin/env python3
"""Bounded, test-only worker harness for unregistered response V4."""

from __future__ import annotations

import datetime as dt
import json
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

import oanda_prospective_event_response_v4 as collector


def run_worker(
    *,
    duration_sec: float = 0.0,
    maximum_cycles: int | None = 1,
    contract_path: Path = collector.DEFAULT_CONTRACT,
    helper_manifest_path: Path = collector.DEFAULT_HELPER_MANIFEST,
    contract_override: Mapping[str, Any] | None = None,
    clock_snapshot_override: Mapping[str, Any] | None = None,
    quote_snapshot_override: Mapping[str, Any] | None = None,
    instrument_universe_override: Sequence[str] | None = None,
    policy_dependency_override: Mapping[str, Any] | None = None,
    ledger_path: Path = collector.DEFAULT_CANONICAL_LEDGER,
    state_path: Path = collector.DEFAULT_CANONICAL_STATE,
    report_path: Path = collector.DEFAULT_CANONICAL_REPORT,
    observed_utc_override: dt.datetime | None = None,
    test_only: bool = False,
) -> list[dict[str, Any]]:
    """Forward every dependency override unchanged and retain top-level status."""

    started = time.monotonic()
    cycle = 0
    results: list[dict[str, Any]] = []
    while True:
        cycle += 1
        result = collector.run_once(
            contract_path=contract_path,
            helper_manifest_path=helper_manifest_path,
            contract_override=contract_override,
            clock_snapshot_override=clock_snapshot_override,
            quote_snapshot_override=quote_snapshot_override,
            instrument_universe_override=instrument_universe_override,
            policy_dependency_override=policy_dependency_override,
            ledger_path=ledger_path,
            state_path=state_path,
            report_path=report_path,
            observed_utc_override=observed_utc_override,
            test_only=test_only,
        )
        if not str(result.get("status") or ""):
            raise RuntimeError("v4_worker_result_missing_top_level_status")
        result = {
            **result,
            "worker_status": "temp_v4_worker_cycle_ok",
            "worker_cycle_count": cycle,
        }
        results[:] = [result]
        if duration_sec <= 0:
            break
        if maximum_cycles is not None and cycle >= max(1, int(maximum_cycles)):
            break
        remaining = duration_sec - (time.monotonic() - started)
        if remaining <= 0:
            break
        time.sleep(min(0.05, remaining))
    return results


def main() -> int:
    raise SystemExit(
        "V4 worker is disabled/unregistered pending independent review; "
        "only programmatic temp fixtures are allowed"
    )


if __name__ == "__main__":
    main()
