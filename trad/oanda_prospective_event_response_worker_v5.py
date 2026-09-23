#!/usr/bin/env python3
"""Bounded temp-only worker harness for blocked response V5."""

from __future__ import annotations

import datetime as dt
import time
from pathlib import Path
from typing import Any, Mapping

import oanda_prospective_event_response_v5 as collector


def run_worker(
    *,
    duration_sec: float = 0.0,
    maximum_cycles: int | None = 1,
    contract_path: Path = collector.DEFAULT_CONTRACT,
    pending_closure_path: Path = collector.DEFAULT_PENDING_CLOSURE,
    contract_override: Mapping[str, Any] | None = None,
    clock_snapshot_override: Mapping[str, Any] | None = None,
    clock_provenance_override: Mapping[str, Any] | None = None,
    quote_artifact_bytes_override: bytes | None = None,
    quote_artifact_name_override: str | None = None,
    market_attestation_override: Mapping[str, Any] | None = None,
    policy_dependency_override: Mapping[str, Any] | None = None,
    cohort_identity_override: Mapping[str, Any] | None = None,
    ledger_path: Path = collector.DEFAULT_CANONICAL_LEDGER,
    state_path: Path = collector.DEFAULT_CANONICAL_STATE,
    report_path: Path = collector.DEFAULT_CANONICAL_REPORT,
    observed_utc_override: dt.datetime | None = None,
    test_only: bool = False,
) -> list[dict[str, Any]]:
    started = time.monotonic()
    cycle = 0
    latest: list[dict[str, Any]] = []
    while True:
        cycle += 1
        result = collector.run_once(
            contract_path=contract_path,
            pending_closure_path=pending_closure_path,
            contract_override=contract_override,
            clock_snapshot_override=clock_snapshot_override,
            clock_provenance_override=clock_provenance_override,
            quote_artifact_bytes_override=quote_artifact_bytes_override,
            quote_artifact_name_override=quote_artifact_name_override,
            market_attestation_override=market_attestation_override,
            policy_dependency_override=policy_dependency_override,
            cohort_identity_override=cohort_identity_override,
            ledger_path=ledger_path,
            state_path=state_path,
            report_path=report_path,
            observed_utc_override=observed_utc_override,
            test_only=test_only,
        )
        status = result.get("status")
        if status not in {"temp_v5_cycle_ok", "temp_v5_failed_closed"}:
            raise RuntimeError("v5_worker_unknown_top_level_status")
        latest[:] = [{**result, "worker_status": status, "worker_cycle_count": cycle}]
        if status == "temp_v5_failed_closed" or duration_sec <= 0:
            break
        if maximum_cycles is not None and cycle >= max(1, int(maximum_cycles)):
            break
        remaining = duration_sec - (time.monotonic() - started)
        if remaining <= 0:
            break
        time.sleep(min(0.05, remaining))
    return latest


def main() -> int:
    raise SystemExit("V5 worker is disabled/unregistered; only explicit temp fixtures are accepted")


if __name__ == "__main__":
    main()
