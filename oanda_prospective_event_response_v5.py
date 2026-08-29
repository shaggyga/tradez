#!/usr/bin/env python3
"""Explicitly temp-only harness for the blocked response V5 candidate."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from forex_system.ingestion.prospective_event_response_v5 import (
        ProspectiveEventResponseV5Error,
        build_event_plans,
        build_samples,
        derive_top_status,
        insert_temp_rows,
        load_contract,
        make_temp_test_contract,
        mature_outcomes,
        open_temp_ledger,
        quote_snapshot_candidates,
        read_temp_rows,
        refuse_canonical_alias,
        validate_cohort_identity,
        validate_policy_dependency_registry,
        validate_temp_contract,
        verify_pending_closure,
    )
except ModuleNotFoundError:
    from src.forex_system.ingestion.prospective_event_response_v5 import (
        ProspectiveEventResponseV5Error,
        build_event_plans,
        build_samples,
        derive_top_status,
        insert_temp_rows,
        load_contract,
        make_temp_test_contract,
        mature_outcomes,
        open_temp_ledger,
        quote_snapshot_candidates,
        read_temp_rows,
        refuse_canonical_alias,
        validate_cohort_identity,
        validate_policy_dependency_registry,
        validate_temp_contract,
        verify_pending_closure,
    )


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
DEFAULT_CONTRACT = ROOT / "config" / "prospective_event_response_capture_v5.json"
DEFAULT_PENDING_CLOSURE = ROOT / "config" / "prospective_event_response_v5_pending_closure.json"
DEFAULT_CANONICAL_LEDGER = DATA / "research_ledgers" / "prospective_event_response_v5.sqlite"
DEFAULT_CANONICAL_STATE = DATA / "state" / "prospective_event_response_v5.json"
DEFAULT_CANONICAL_REPORT = DATA / "reports" / "prospective_event_response" / "PROSPECTIVE_EVENT_RESPONSE_V5_CURRENT.md"


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True, allow_nan=False)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def _attempt_id(row: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result["attempt_id"] = "event_response_attempt_v5_" + _sha256_bytes(_canonical_json(result).encode("utf-8"))[:40]
    return result


def _bundle_source_hashes(manifest: Mapping[str, Any]) -> dict[str, str]:
    rows = manifest.get("files")
    artifacts = manifest.get("source_artifacts")
    if not isinstance(rows, list) or not isinstance(artifacts, list):
        raise ProspectiveEventResponseV5Error("pending_closure_files_missing")
    return {
        str(row["path"]): str(row["sha256"])
        for row in [*rows, *artifacts]
    }


def _report(result: Mapping[str, Any]) -> str:
    return "\n".join(
        (
            "# Prospective Event Response V5 - blocked temp fixture",
            "",
            f"- Status: `{result.get('status')}`",
            f"- Planning status: `{result.get('planning_status')}`",
            f"- Sampling status: `{result.get('sampling_status')}`",
            f"- Plans inserted: {result.get('plans_inserted', 0)}",
            f"- Samples inserted: {result.get('samples_inserted', 0)}",
            f"- Outcomes inserted: {result.get('outcomes_inserted', 0)}",
            f"- Attempts inserted: {result.get('attempts_inserted', 0)}",
            "- Dependency closure: unresolved; external approval cannot be self-asserted",
            "- Registration: disabled; canonical initialization forbidden",
            "- Execution eligibility: false",
            "- Supported execution decision: `no_trade`",
            "",
        )
    )


def run_once(
    *,
    contract_path: Path = DEFAULT_CONTRACT,
    pending_closure_path: Path = DEFAULT_PENDING_CLOSURE,
    contract_override: Mapping[str, Any] | None = None,
    clock_snapshot_override: Mapping[str, Any] | None = None,
    clock_provenance_override: Mapping[str, Any] | None = None,
    quote_artifact_bytes_override: bytes | None = None,
    quote_artifact_name_override: str | None = None,
    market_attestation_override: Mapping[str, Any] | None = None,
    policy_dependency_override: Mapping[str, Any] | None = None,
    cohort_identity_override: Mapping[str, Any] | None = None,
    ledger_path: Path = DEFAULT_CANONICAL_LEDGER,
    state_path: Path = DEFAULT_CANONICAL_STATE,
    report_path: Path = DEFAULT_CANONICAL_REPORT,
    observed_utc_override: dt.datetime | None = None,
    test_only: bool = False,
) -> dict[str, Any]:
    if type(test_only) is not bool or test_only is not True:
        raise ProspectiveEventResponseV5Error("v5_disabled_unregistered_temp_fixture_only")
    refuse_canonical_alias(ledger_path, [DEFAULT_CANONICAL_LEDGER], field="ledger")
    refuse_canonical_alias(state_path, [DEFAULT_CANONICAL_STATE], field="state")
    refuse_canonical_alias(report_path, [DEFAULT_CANONICAL_REPORT], field="report")
    manifest = verify_pending_closure(ROOT, pending_closure_path, require_reviewed=False)
    candidate = load_contract(contract_path)
    now = (observed_utc_override or dt.datetime.now(tz=dt.timezone.utc)).astimezone(dt.timezone.utc)
    contract = dict(contract_override) if contract_override is not None else make_temp_test_contract(candidate, cohort_start_utc=now)
    validate_temp_contract(contract)
    if clock_snapshot_override is None or clock_provenance_override is None:
        raise ProspectiveEventResponseV5Error("clock_snapshot_and_provenance_overrides_required")
    if quote_artifact_bytes_override is None or quote_artifact_name_override is None or market_attestation_override is None:
        raise ProspectiveEventResponseV5Error("complete_quote_artifact_and_attestation_required")
    if policy_dependency_override is None or cohort_identity_override is None:
        raise ProspectiveEventResponseV5Error("policy_and_cohort_identity_overrides_required")
    policy_hash = validate_policy_dependency_registry(policy_dependency_override)
    identity = validate_cohort_identity(cohort_identity_override, policy_registry_sha256=policy_hash)
    closure_hash = _sha256_bytes(pending_closure_path.read_bytes())
    if identity["closure_manifest_sha256"] != closure_hash:
        raise ProspectiveEventResponseV5Error("cohort_identity_closure_hash_mismatch")
    if identity["contract_payload_sha256"] != _sha256_bytes(contract_path.read_bytes()):
        raise ProspectiveEventResponseV5Error("cohort_identity_contract_hash_mismatch")
    if identity["source_dependency_sha256"] != _bundle_source_hashes(manifest):
        raise ProspectiveEventResponseV5Error("cohort_identity_source_closure_mismatch")
    planned = build_event_plans(
        clock_snapshot_override,
        clock_provenance=clock_provenance_override,
        contract=contract,
        policy_dependency_registry=policy_dependency_override,
        cohort_identity=cohort_identity_override,
        planned_utc=now,
    )
    observations = quote_snapshot_candidates(
        quote_artifact_bytes_override,
        artifact_name=quote_artifact_name_override,
        market_attestation=market_attestation_override,
        contract=contract,
        collector_observed_utc=now,
        cohort_identity_sha256=identity["cohort_identity_sha256"],
    )
    connection = open_temp_ledger(
        ledger_path,
        canonical_paths=[DEFAULT_CANONICAL_LEDGER],
        cohort_identity=cohort_identity_override,
        policy_registry_sha256=policy_hash,
    )
    try:
        connection.execute("BEGIN IMMEDIATE")
        plans_inserted = insert_temp_rows(connection, "plans", planned["plans"], cohort_identity_sha256=identity["cohort_identity_sha256"])
        planning_attempts = [_attempt_id(row) for row in planned["attempts"]]
        attempts_inserted = insert_temp_rows(connection, "attempts", planning_attempts, cohort_identity_sha256=identity["cohort_identity_sha256"])
        plans = read_temp_rows(connection, "plans", cohort_identity_sha256=identity["cohort_identity_sha256"])
        existing_samples = read_temp_rows(connection, "samples", cohort_identity_sha256=identity["cohort_identity_sha256"])
        sampled = build_samples(
            plans,
            observations,
            existing_samples=existing_samples,
            current_clock_snapshot=clock_snapshot_override,
            current_clock_provenance=clock_provenance_override,
            contract=contract,
            policy_dependency_registry=policy_dependency_override,
            cohort_identity=cohort_identity_override,
            collector_observed_utc=now,
        )
        samples_inserted = insert_temp_rows(connection, "samples", sampled["samples"], cohort_identity_sha256=identity["cohort_identity_sha256"])
        attempts_inserted += insert_temp_rows(connection, "attempts", [_attempt_id(row) for row in sampled["attempts"]], cohort_identity_sha256=identity["cohort_identity_sha256"])
        samples = read_temp_rows(connection, "samples", cohort_identity_sha256=identity["cohort_identity_sha256"])
        outcomes = mature_outcomes(samples, cohort_identity_sha256=identity["cohort_identity_sha256"])
        outcomes_inserted = insert_temp_rows(connection, "outcomes", outcomes, cohort_identity_sha256=identity["cohort_identity_sha256"])
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    result = {
        "status": derive_top_status(
            planning_status=planned["status"], sampling_status=sampled["status"]
        ),
        "generated_utc": now.isoformat(),
        "planning_status": planned["status"], "sampling_status": sampled["status"],
        "plans_inserted": plans_inserted, "samples_inserted": samples_inserted,
        "outcomes_inserted": outcomes_inserted, "attempts_inserted": attempts_inserted,
        "cohort_identity_sha256": identity["cohort_identity_sha256"],
        "dependency_bundle_state": "pending_external_approval",
        "research_only": True, "execution_eligible": False, "can_place_orders": False,
        "promotion_eligible": False, "authorization_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    _atomic_text(state_path, json.dumps(result, indent=2, sort_keys=True))
    _atomic_text(report_path, _report(result))
    return result


def main() -> int:
    raise SystemExit("V5 is disabled/unregistered and its external dependencies are unresolved")


if __name__ == "__main__":
    main()
