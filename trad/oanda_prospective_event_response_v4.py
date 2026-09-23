#!/usr/bin/env python3
"""Run one explicitly test-only prospective event-response V4 cycle.

The candidate is disabled and unregistered.  This runner therefore refuses
canonical paths and requires all market/catalog inputs as explicit overrides.
It has no broker, execution, lifecycle, promotion, or authorization imports.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from forex_system.ingestion.prospective_event_response_v4 import (
        ProspectiveEventResponseV4Error,
        build_event_plans,
        build_samples,
        insert_temp_rows,
        load_contract,
        make_temp_test_contract,
        mature_outcomes,
        open_temp_ledger,
        quote_snapshot_candidates,
        read_temp_rows,
        validate_temp_contract,
        verify_helper_closure,
    )
except ModuleNotFoundError:
    from src.forex_system.ingestion.prospective_event_response_v4 import (
        ProspectiveEventResponseV4Error,
        build_event_plans,
        build_samples,
        insert_temp_rows,
        load_contract,
        make_temp_test_contract,
        mature_outcomes,
        open_temp_ledger,
        quote_snapshot_candidates,
        read_temp_rows,
        validate_temp_contract,
        verify_helper_closure,
    )


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
DEFAULT_CONTRACT = ROOT / "config" / "prospective_event_response_capture_v4.json"
DEFAULT_HELPER_MANIFEST = ROOT / "config" / "prospective_event_response_v4_helper_closure.json"
DEFAULT_CANONICAL_LEDGER = DATA / "research_ledgers" / "prospective_event_response_v4.sqlite"
DEFAULT_CANONICAL_STATE = DATA / "state" / "prospective_event_response_v4.json"
DEFAULT_CANONICAL_REPORT = (
    DATA
    / "reports"
    / "prospective_event_response"
    / "PROSPECTIVE_EVENT_RESPONSE_V4_CURRENT.md"
)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def _refuse_canonical_path(path: Path, canonical: Path, *, field: str) -> None:
    if path.resolve() == canonical.resolve():
        raise ProspectiveEventResponseV4Error(
            f"canonical_v4_{field}_forbidden_pending_review"
        )


def _attempt_with_id(row: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result["attempt_id"] = "event_response_attempt_v4_" + _sha256_json(result)[:40]
    return result


def _report(result: Mapping[str, Any]) -> str:
    return "\n".join(
        (
            "# Prospective Event Response V4 - temp fixture only",
            "",
            f"- Status: `{result.get('status')}`",
            f"- Plans inserted: {result.get('plans_inserted', 0)}",
            f"- Samples inserted: {result.get('samples_inserted', 0)}",
            f"- Outcomes inserted: {result.get('outcomes_inserted', 0)}",
            f"- Attempts inserted: {result.get('attempts_inserted', 0)}",
            "- Registration: disabled; canonical initialization forbidden",
            "- Supported execution decision: `no_trade`",
            "",
        )
    )


def run_once(
    *,
    contract_path: Path = DEFAULT_CONTRACT,
    helper_manifest_path: Path = DEFAULT_HELPER_MANIFEST,
    contract_override: Mapping[str, Any] | None = None,
    clock_snapshot_override: Mapping[str, Any] | None = None,
    quote_snapshot_override: Mapping[str, Any] | None = None,
    instrument_universe_override: Sequence[str] | None = None,
    policy_dependency_override: Mapping[str, Any] | None = None,
    ledger_path: Path = DEFAULT_CANONICAL_LEDGER,
    state_path: Path = DEFAULT_CANONICAL_STATE,
    report_path: Path = DEFAULT_CANONICAL_REPORT,
    observed_utc_override: dt.datetime | None = None,
    test_only: bool = False,
) -> dict[str, Any]:
    if type(test_only) is not bool or test_only is not True:
        raise ProspectiveEventResponseV4Error(
            "v4_unregistered_no_canonical_run_pending_review"
        )
    _refuse_canonical_path(ledger_path, DEFAULT_CANONICAL_LEDGER, field="ledger")
    _refuse_canonical_path(state_path, DEFAULT_CANONICAL_STATE, field="state")
    _refuse_canonical_path(report_path, DEFAULT_CANONICAL_REPORT, field="report")
    verify_helper_closure(ROOT, helper_manifest_path, require_reviewed=False)

    candidate = load_contract(contract_path, allow_pending_review=True)
    now = (observed_utc_override or dt.datetime.now(tz=dt.timezone.utc)).astimezone(
        dt.timezone.utc
    )
    contract = (
        dict(contract_override)
        if contract_override is not None
        else make_temp_test_contract(candidate, cohort_start_utc=now)
    )
    validate_temp_contract(contract)
    if clock_snapshot_override is None:
        raise ProspectiveEventResponseV4Error("test_clock_snapshot_override_required")
    if quote_snapshot_override is None:
        raise ProspectiveEventResponseV4Error("test_quote_snapshot_override_required")
    if instrument_universe_override is None:
        raise ProspectiveEventResponseV4Error("test_instrument_universe_override_required")
    if policy_dependency_override is None:
        raise ProspectiveEventResponseV4Error("test_policy_dependency_override_required")

    market_attestation = quote_snapshot_override.get("market_clock_attestation")
    if not isinstance(market_attestation, Mapping):
        raise ProspectiveEventResponseV4Error("quote_market_attestation_override_required")
    quote_payload = {
        key: value
        for key, value in quote_snapshot_override.items()
        if key != "market_clock_attestation"
    }
    quote_artifact_hash = _sha256_json(quote_payload)

    try:
        planned = build_event_plans(
            clock_snapshot_override,
            contract=contract,
            instruments=instrument_universe_override,
            policy_dependency_registry=policy_dependency_override,
            planned_utc=now,
        )
    except ProspectiveEventResponseV4Error as exc:
        # Catalog planning is independent of sampling already-frozen plans.
        # A stale semantic catalog can block new admissions without blocking a
        # postbaseline endpoint whose exact event lineage is already frozen.
        planned = {
            "status": "new_event_planning_failed_closed",
            "plans": [],
            "attempts": [
                {
                    "status": "new_event_planning_failed_closed",
                    "reason": str(exc),
                    "source_ref": "semantic_catalog_planning",
                    "research_only": True,
                    "execution_eligible": False,
                    "can_place_orders": False,
                    "promotion_eligible": False,
                    "authorization_eligible": False,
                    "supported_execution_decision": "no_trade",
                }
            ],
        }
    observations = quote_snapshot_candidates(
        quote_payload,
        contract=contract,
        artifact_sha256=quote_artifact_hash,
        collector_observed_utc=now,
        market_clock_attestation=market_attestation,
    )

    connection = open_temp_ledger(
        ledger_path, canonical_path=DEFAULT_CANONICAL_LEDGER
    )
    try:
        connection.execute("BEGIN IMMEDIATE")
        plans_inserted = insert_temp_rows(connection, "plans", planned["plans"])
        planning_attempts = [_attempt_with_id(row) for row in planned["attempts"]]
        attempts_inserted = insert_temp_rows(connection, "attempts", planning_attempts)
        all_plans = read_temp_rows(connection, "plans")
        existing_samples = read_temp_rows(connection, "samples")
        sampled = build_samples(
            all_plans,
            observations,
            existing_samples=existing_samples,
            current_clock_snapshot=clock_snapshot_override,
            contract=contract,
            instruments=instrument_universe_override,
            policy_dependency_registry=policy_dependency_override,
            collector_observed_utc=now,
        )
        samples_inserted = insert_temp_rows(connection, "samples", sampled["samples"])
        sample_attempts = [_attempt_with_id(row) for row in sampled["attempts"]]
        attempts_inserted += insert_temp_rows(connection, "attempts", sample_attempts)
        all_samples = read_temp_rows(connection, "samples")
        outcomes = mature_outcomes(all_samples)
        outcomes_inserted = insert_temp_rows(connection, "outcomes", outcomes)
        connection.commit()
        total_plans = len(read_temp_rows(connection, "plans"))
        total_samples = len(read_temp_rows(connection, "samples"))
        total_outcomes = len(read_temp_rows(connection, "outcomes"))
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    result = {
        "status": "temp_v4_cycle_ok",
        "generated_utc": now.isoformat(),
        "planning_status": planned["status"],
        "sampling_status": sampled["status"],
        "plans_inserted": plans_inserted,
        "samples_inserted": samples_inserted,
        "outcomes_inserted": outcomes_inserted,
        "attempts_inserted": attempts_inserted,
        "total_plans": total_plans,
        "total_samples": total_samples,
        "total_outcomes": total_outcomes,
        "override_contract": True,
        "override_clock_snapshot": True,
        "override_quote_snapshot": True,
        "override_instrument_universe": True,
        "override_policy_dependency": True,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "promotion_eligible": False,
        "authorization_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    _atomic_text(state_path, json.dumps(result, indent=2, sort_keys=True))
    _atomic_text(report_path, _report(result))
    return result


def main() -> int:
    raise SystemExit(
        "V4 is disabled/unregistered pending independent review; canonical CLI is unavailable"
    )


if __name__ == "__main__":
    main()
