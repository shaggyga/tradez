"""Persistent identity and registry remediation over the frozen V4 engine.

V5 is a new, engineering-only cohort.  The V4 engine remains byte-for-byte
unchanged.  Nonempty fixture inputs are accepted by this core only when a
cohort-bound append-only identity registry proves that replay, payload, and
record identities have never been rebound to different canonical bytes.
The operational CLI keeps all nonempty inputs disabled.
"""

from __future__ import annotations

import copy
import datetime as dt
import json
import sqlite3
from pathlib import Path
from typing import Any, Mapping

from .currency_state_after_cost_counterfactual_v2 import AfterCostV2Error, _validate_manifest
from .currency_state_after_cost_counterfactual_v3 import ENVELOPES, _record_id
from .currency_state_after_cost_counterfactual_v4 import (
    AfterCostV4Error,
    build_after_cost_counterfactual_v4,
)
from ..contracts.currency_state import stable_hash


class AfterCostV5Error(ValueError):
    pass


EXPECTED_ARTIFACT_PATHS = {
    "counterfactual_v5_module": "src/forex_system/research/currency_state_after_cost_counterfactual_v5.py",
    "counterfactual_v5_registrar": "src/forex_system/research/currency_state_after_cost_registrar_v5.py",
    "counterfactual_v5_config": "config/currency_state_after_cost_counterfactual_v5.json",
    "counterfactual_v5_cli": "oanda_currency_state_after_cost_counterfactual_v5.py",
    "counterfactual_v4_module": "src/forex_system/research/currency_state_after_cost_counterfactual_v4.py",
    "counterfactual_v4_config": "config/currency_state_after_cost_counterfactual_v4.json",
    "counterfactual_v4_cli": "oanda_currency_state_after_cost_counterfactual_v4.py",
    "counterfactual_v4_manifest": "config/currency_state_after_cost_counterfactual_v4_manifest.json",
    "counterfactual_v3_module": "src/forex_system/research/currency_state_after_cost_counterfactual_v3.py",
    "counterfactual_v3_config": "config/currency_state_after_cost_counterfactual_v3.json",
    "counterfactual_v3_cli": "oanda_currency_state_after_cost_counterfactual_v3.py",
    "counterfactual_v3_manifest": "config/currency_state_after_cost_counterfactual_v3_manifest.json",
    "counterfactual_v2_engine_module": "src/forex_system/research/currency_state_after_cost_counterfactual_v2.py",
    "input_envelope_module": "src/forex_system/research/after_cost_input_envelopes_v3.py",
    "input_envelope_config": "config/after_cost_input_envelopes_v3.json",
    "response_module": "src/forex_system/research/currency_state_response_timing_arms.py",
    "response_config": "config/currency_state_response_timing_arms_v1.json",
    "response_cli": "oanda_currency_state_response_timing_arms.py",
    "currency_state_module": "src/forex_system/features/currency_state_engine.py",
    "currency_state_config": "config/currency_state_engine_v2.json",
    "currency_state_cli": "oanda_currency_state_engine.py",
    "official_context_module": "src/forex_system/features/currency_state_official_context.py",
    "official_context_cli": "oanda_currency_state_official_context.py",
    "official_fact_adapter_module": "src/forex_system/ingestion/official_fact_adapter.py",
    "official_fact_adapter_cli": "oanda_official_fact_adapter.py",
    "official_fact_adapter_contract_manifest": "config/official_fact_adapter_contract_v1.json",
    "signed_exposure_module": "src/forex_system/contracts/signed_currency_exposure.py",
    "independent_verifier_producer": "oanda_independent_evidence_verifier.py",
    "quote_producer": "oanda_practice_quote_stream.py",
    "account_producer": "oanda_account_snapshot_writer.py",
}


def _validate_contracts(
    contract: Mapping[str, Any], upstream_v4_contract: Mapping[str, Any],
) -> None:
    try:
        from .currency_state_after_cost_counterfactual_v2 import validate_contract
        validate_contract(contract)
    except AfterCostV2Error as exc:
        raise AfterCostV5Error(str(exc)) from exc
    if (
        contract.get("contract_id") != "currency_state_after_cost_counterfactual_v5_20260817"
        or contract.get("counterfactual_cohort_id")
        != "currency_state_after_cost_counterfactual_cohort_20260817e"
        or contract.get("supersedes_contract_id")
        != upstream_v4_contract.get("contract_id")
        or contract.get("supersedes_cohort_id")
        != upstream_v4_contract.get("counterfactual_cohort_id")
        or contract.get("required_upstream_v4_manifest_id")
        != upstream_v4_contract.get("required_frozen_manifest_id")
    ):
        raise AfterCostV5Error("v5 contract/cohort/upstream-v4 identity mismatch")
    shared = (
        "expected_instruments", "instrument_universe_sha256",
        "expected_horizons_sec", "expected_arm_ids",
        "forward_admissible_arm_ids", "never_forward_arm_ids",
        "required_response_contract_id", "required_response_snapshot_schema",
        "required_state_contract_id", "required_basis_eligibility_contract_id",
        "account_policy", "rotation_close_contract", "selection", "hold_switch",
    )
    if any(contract.get(field) != upstream_v4_contract.get(field) for field in shared):
        raise AfterCostV5Error("v5 changed frozen v4 research/economics behavior")
    if contract.get("operational_input_policy") != {
        "mode": "zero_input_engineering_initialization_only",
        "producer_integration_state": "producer_integration_missing",
        "nonempty_operational_input_requires_separately_reviewed_new_cohort": True,
        "core_fixture_inputs_are_test_only": True,
    }:
        raise AfterCostV5Error("v5 operational input policy changed")
    registry = contract.get("identity_registry_contract") or {}
    if (
        registry.get("schema_id") != "currency_state_after_cost_identity_registry_v5_20260817"
        or registry.get("canonical_relative_path")
        != "data/oanda_training_manager/state/currency_state_after_cost_identity_registry_v5.sqlite"
        or registry.get("append_only") is not True
        or registry.get("replay_payload_record_equivocation_fails_closed") is not True
    ):
        raise AfterCostV5Error("v5 persistent identity registry contract changed")
    genealogy = contract.get("genealogy_contract") or {}
    if (
        genealogy.get("canonical_relative_path")
        != "data/oanda_training_manager/state/research_genealogy_v1.sqlite"
        or genealogy.get("caller_override_allowed") is not False
        or genealogy.get("verify_all_experiment_columns") is not True
        or genealogy.get("recompute_definition_sha256") is not True
    ):
        raise AfterCostV5Error("v5 canonical genealogy policy changed")


def _validate_v5_manifest(
    manifest: Mapping[str, Any], contract: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        normalized = _validate_manifest(manifest, contract)
    except AfterCostV2Error as exc:
        raise AfterCostV5Error(str(exc)) from exc
    actual = {
        label: str(row.get("relative_path") or "").replace("\\", "/")
        for label, row in (manifest.get("artifacts") or {}).items()
    }
    if actual != EXPECTED_ARTIFACT_PATHS:
        raise AfterCostV5Error("v5 manifest label-to-path map mismatch")
    return normalized


def _utc_now() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat()


def initialize_identity_registry_v5(
    path: Path, *, contract: Mapping[str, Any], manifest_id: str,
) -> None:
    """Create a new cohort-bound append-only registry; never repair/replace one."""
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE registry_meta(
              singleton INTEGER PRIMARY KEY CHECK(singleton=1),
              schema_id TEXT NOT NULL, contract_id TEXT NOT NULL,
              cohort_id TEXT NOT NULL, manifest_id TEXT NOT NULL,
              created_utc TEXT NOT NULL
            );
            CREATE TABLE identity_bindings(
              namespace TEXT NOT NULL, identity_kind TEXT NOT NULL,
              identity_value TEXT NOT NULL, canonical_bytes_sha256 TEXT NOT NULL,
              first_replay_identity TEXT NOT NULL, first_seen_utc TEXT NOT NULL,
              PRIMARY KEY(namespace,identity_kind,identity_value)
            );
            CREATE TABLE replay_bindings(
              replay_identity TEXT PRIMARY KEY,
              canonical_submission_sha256 TEXT NOT NULL,
              first_seen_utc TEXT NOT NULL
            );
            CREATE TABLE replay_observations(
              observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
              replay_identity TEXT NOT NULL,
              canonical_submission_sha256 TEXT NOT NULL,
              observed_utc TEXT NOT NULL
            );
            CREATE TABLE identity_conflicts(
              conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
              namespace TEXT NOT NULL, identity_kind TEXT NOT NULL,
              identity_value TEXT NOT NULL, registered_sha256 TEXT NOT NULL,
              proposed_sha256 TEXT NOT NULL, replay_identity TEXT NOT NULL,
              observed_utc TEXT NOT NULL
            );
        """)
        for table in (
            "registry_meta", "identity_bindings", "replay_bindings",
            "replay_observations", "identity_conflicts",
        ):
            connection.executescript(f"""
                CREATE TRIGGER no_update_{table} BEFORE UPDATE ON {table}
                BEGIN SELECT RAISE(ABORT,'append-only registry'); END;
                CREATE TRIGGER no_delete_{table} BEFORE DELETE ON {table}
                BEGIN SELECT RAISE(ABORT,'append-only registry'); END;
            """)
        policy = contract["identity_registry_contract"]
        connection.execute(
            "INSERT INTO registry_meta VALUES(1,?,?,?,?,?)",
            (
                policy["schema_id"], contract["contract_id"],
                contract["counterfactual_cohort_id"], manifest_id,
                contract["cohort_start_utc"],
            ),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _verify_registry(connection: sqlite3.Connection, *, contract: Mapping[str, Any], manifest_id: str) -> None:
    expected_tables = {
        "registry_meta", "identity_bindings", "replay_bindings",
        "replay_observations", "identity_conflicts",
    }
    tables = {
        row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ) if row[0] != "sqlite_sequence"
    }
    expected_triggers = {
        f"no_{operation}_{table}"
        for table in expected_tables for operation in ("update", "delete")
    }
    triggers = {
        row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger'"
        )
    }
    if tables != expected_tables or triggers != expected_triggers:
        raise AfterCostV5Error("wrong or non-append-only v5 identity registry")
    row = connection.execute(
        "SELECT schema_id,contract_id,cohort_id,manifest_id FROM registry_meta WHERE singleton=1"
    ).fetchone()
    policy = contract["identity_registry_contract"]
    if row != (
        policy["schema_id"], contract["contract_id"],
        contract["counterfactual_cohort_id"], manifest_id,
    ):
        raise AfterCostV5Error("wrong v5 identity registry cohort or contract")


def _identity_candidates(
    envelopes: Mapping[str, Mapping[str, Any] | None],
) -> tuple[str, list[tuple[str, str, str, str]]]:
    rows: list[tuple[str, str, str, str]] = []
    submission: dict[str, Any] = {}
    for name in ENVELOPES:
        envelope = envelopes.get(name)
        if envelope is None:
            continue
        payload_id = str(envelope.get("payload_id") or "")
        envelope_sha = stable_hash(envelope)
        rows.append((name, "payload_id", payload_id, envelope_sha))
        record_rows = []
        for record in envelope.get("records") or []:
            record_id = str(_record_id(record, name) or "")
            record_sha = stable_hash(record)
            rows.append((name, "record_id", record_id, record_sha))
            record_rows.append({"record_id": record_id, "canonical_bytes_sha256": record_sha})
        submission[name] = {
            "payload_id": payload_id,
            "canonical_bytes_sha256": envelope_sha,
            "records": record_rows,
        }
    return stable_hash(submission), rows


def admit_persistent_identities_v5(
    path: Path,
    *,
    contract: Mapping[str, Any],
    manifest_id: str,
    replay_identity: str,
    envelopes: Mapping[str, Mapping[str, Any] | None],
) -> dict[str, Any]:
    if not replay_identity or len(replay_identity) > 512:
        raise AfterCostV5Error("nonempty v5 input requires bounded replay_identity")
    submission_sha, candidates = _identity_candidates(envelopes)
    if not candidates:
        raise AfterCostV5Error("identity admission called without submitted envelopes")
    now = _utc_now()
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("BEGIN IMMEDIATE")
        _verify_registry(connection, contract=contract, manifest_id=manifest_id)
        conflicts: list[tuple[str, str, str, str, str]] = []
        replay = connection.execute(
            "SELECT canonical_submission_sha256 FROM replay_bindings WHERE replay_identity=?",
            (replay_identity,),
        ).fetchone()
        if replay is not None and replay[0] != submission_sha:
            conflicts.append(("replay", "replay_identity", replay_identity, replay[0], submission_sha))
        for namespace, kind, identity, proposed_sha in candidates:
            if not identity:
                raise AfterCostV5Error("empty payload or record identity")
            existing = connection.execute(
                "SELECT canonical_bytes_sha256 FROM identity_bindings "
                "WHERE namespace=? AND identity_kind=? AND identity_value=?",
                (namespace, kind, identity),
            ).fetchone()
            if existing is not None and existing[0] != proposed_sha:
                conflicts.append((namespace, kind, identity, existing[0], proposed_sha))
        if conflicts:
            connection.executemany(
                "INSERT INTO identity_conflicts(namespace,identity_kind,identity_value,"
                "registered_sha256,proposed_sha256,replay_identity,observed_utc) "
                "VALUES(?,?,?,?,?,?,?)",
                [(*row, replay_identity, now) for row in conflicts],
            )
            connection.commit()
            raise AfterCostV5Error("persistent replay/payload/record identity equivocation")
        connection.execute(
            "INSERT OR IGNORE INTO replay_bindings VALUES(?,?,?)",
            (replay_identity, submission_sha, now),
        )
        connection.executemany(
            "INSERT OR IGNORE INTO identity_bindings VALUES(?,?,?,?,?,?)",
            [(*row, replay_identity, now) for row in candidates],
        )
        connection.execute(
            "INSERT INTO replay_observations(replay_identity,canonical_submission_sha256,observed_utc) VALUES(?,?,?)",
            (replay_identity, submission_sha, now),
        )
        connection.commit()
        return {
            "registry_schema_id": contract["identity_registry_contract"]["schema_id"],
            "registry_contract_id": contract["contract_id"],
            "registry_cohort_id": contract["counterfactual_cohort_id"],
            "replay_identity": replay_identity,
            "canonical_submission_sha256": submission_sha,
            "identity_count": len(candidates),
            "equivocation_detected": False,
        }
    except AfterCostV5Error:
        if connection.in_transaction:
            connection.rollback()
        raise
    except sqlite3.Error as exc:
        if connection.in_transaction:
            connection.rollback()
        raise AfterCostV5Error("v5 identity registry read/write failed") from exc
    finally:
        connection.close()


def build_after_cost_counterfactual_v5(
    response_snapshot: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    frozen_manifest: Mapping[str, Any],
    upstream_v4_contract: Mapping[str, Any],
    upstream_v4_manifest: Mapping[str, Any],
    upstream_v3_contract: Mapping[str, Any],
    upstream_v3_manifest: Mapping[str, Any],
    venue_quotes: Mapping[str, Any] | None = None,
    verifier_evidence: Mapping[str, Any] | None = None,
    economics_inputs: Mapping[str, Any] | None = None,
    hold_switch_inputs: Mapping[str, Any] | None = None,
    account_state: Mapping[str, Any] | None = None,
    identity_registry_path: Path | None = None,
    replay_identity: str | None = None,
) -> dict[str, Any]:
    _validate_contracts(contract, upstream_v4_contract)
    manifest = _validate_v5_manifest(frozen_manifest, contract)
    supplied = {
        "venue_quotes": venue_quotes, "verifier_evidence": verifier_evidence,
        "economics_inputs": economics_inputs, "hold_switch_inputs": hold_switch_inputs,
        "account_state": account_state,
    }
    try:
        base = build_after_cost_counterfactual_v4(
            response_snapshot,
            contract=upstream_v4_contract,
            frozen_manifest=upstream_v4_manifest,
            upstream_v3_contract=upstream_v3_contract,
            upstream_v3_manifest=upstream_v3_manifest,
            venue_quotes=venue_quotes,
            verifier_evidence=verifier_evidence,
            economics_inputs=economics_inputs,
            hold_switch_inputs=hold_switch_inputs,
            account_state=account_state,
        )
    except AfterCostV4Error as exc:
        raise AfterCostV5Error(str(exc)) from exc
    any_supplied = any(value is not None for value in supplied.values())
    if any_supplied:
        if identity_registry_path is None:
            raise AfterCostV5Error("nonempty v5 core fixture requires persistent identity registry")
        identity_state = admit_persistent_identities_v5(
            identity_registry_path,
            contract=contract,
            manifest_id=manifest["manifest_id"],
            replay_identity=str(replay_identity or ""),
            envelopes=supplied,
        )
    else:
        if identity_registry_path is not None or replay_identity is not None:
            raise AfterCostV5Error("zero-input v5 initialization cannot claim replay identity")
        identity_state = {
            "registry_schema_id": contract["identity_registry_contract"]["schema_id"],
            "registry_contract_id": contract["contract_id"],
            "registry_cohort_id": contract["counterfactual_cohort_id"],
            "replay_identity": None,
            "canonical_submission_sha256": None,
            "identity_count": 0,
            "equivocation_detected": False,
            "state": "not_opened_for_zero_input_initialization",
        }
    base.update({
        "schema_version": 5,
        "snapshot_schema": contract["snapshot_schema"],
        "counterfactual_contract_id": contract["contract_id"],
        "counterfactual_cohort_id": contract["counterfactual_cohort_id"],
        "supersedes_contract_id": contract["supersedes_contract_id"],
        "supersedes_cohort_id": contract["supersedes_cohort_id"],
        "frozen_manifest": manifest,
        "upstream_v4_contract_id": upstream_v4_contract["contract_id"],
        "upstream_v4_cohort_id": upstream_v4_contract["counterfactual_cohort_id"],
        "identity_registry_state": identity_state,
        "payload_identity_policy": copy.deepcopy(contract["payload_identity_policy"]),
    })
    base["limitations"] = [
        "v3 and v4 remain immutable and are not migrated",
        "the operational v5 publisher blocks every nonempty input",
        "core nonempty fixtures require persistent replay, payload, and record identity binding",
        "genealogy trust requires the canonical database and exact registrar-generated row",
        "all results remain research-only no_trade",
    ]
    base["decision_fingerprint"] = stable_hash({
        "records": base["records"], "allocations": base["allocations"],
        "holds": base["hold_switch_counterfactuals"],
        "identity_registry_state": identity_state,
        "supported_execution_decision": base["supported_execution_decision"],
    })
    base["snapshot_id"] = "currency_state_after_cost_v5_" + stable_hash({
        key: value for key, value in base.items() if key != "snapshot_id"
    })[:24]
    if (
        len(base["records"]) != 8 * 68 * 5
        or any(row["paper_decision"] != "no_trade" for row in base["records"])
        or base.get("supported_execution_decision") != "no_trade"
    ):
        raise AfterCostV5Error("v5 grid or safety invariant failed")
    return base


__all__ = [
    "AfterCostV5Error", "EXPECTED_ARTIFACT_PATHS",
    "initialize_identity_registry_v5", "admit_persistent_identities_v5",
    "build_after_cost_counterfactual_v5",
]
