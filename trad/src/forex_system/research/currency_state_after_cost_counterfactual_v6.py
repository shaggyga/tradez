"""Failure-atomic identity and genealogy remediation over frozen V5.

V6 is a new engineering-only cohort.  It does not mutate or re-register V3,
V4, or V5.  The operational publisher remains zero-input only.  Test-only
nonempty core calls require an independently initialized V6 registry whose
complete SQLite definition and manifest binding are verified on every replay.
"""

from __future__ import annotations

import copy
import datetime as dt
import os
import re
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence

from .currency_state_after_cost_counterfactual_v2 import AfterCostV2Error, _validate_manifest
from .currency_state_after_cost_counterfactual_v3 import ENVELOPES, _record_id
from .currency_state_after_cost_counterfactual_v4 import (
    AfterCostV4Error,
    build_after_cost_counterfactual_v4,
)
from ..contracts.currency_state import stable_hash


class AfterCostV6Error(ValueError):
    pass


SHA256_RE = re.compile(r"[0-9a-f]{64}")

EXPECTED_ARTIFACT_PATHS = {
    "counterfactual_v6_module": "src/forex_system/research/currency_state_after_cost_counterfactual_v6.py",
    "counterfactual_v6_registrar": "src/forex_system/research/currency_state_after_cost_registrar_v6.py",
    "counterfactual_v6_config": "config/currency_state_after_cost_counterfactual_v6.json",
    "counterfactual_v6_cli": "oanda_currency_state_after_cost_counterfactual_v6.py",
    "counterfactual_v6_test": "test_currency_state_after_cost_counterfactual_v6.py",
    "counterfactual_v6_proposal": "docs/CURRENCY_STATE_AFTER_COST_COUNTERFACTUAL_V6_PROPOSAL_20260817.md",
    "counterfactual_v5_module": "src/forex_system/research/currency_state_after_cost_counterfactual_v5.py",
    "counterfactual_v5_registrar": "src/forex_system/research/currency_state_after_cost_registrar_v5.py",
    "counterfactual_v5_config": "config/currency_state_after_cost_counterfactual_v5.json",
    "counterfactual_v5_cli": "oanda_currency_state_after_cost_counterfactual_v5.py",
    "counterfactual_v5_manifest": "config/currency_state_after_cost_counterfactual_v5_manifest.json",
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


REGISTRY_TABLE_SQL = {
    "registry_meta": """CREATE TABLE registry_meta(
      singleton INTEGER PRIMARY KEY CHECK(singleton=1),
      schema_id TEXT NOT NULL CHECK(length(schema_id)>0),
      contract_id TEXT NOT NULL CHECK(length(contract_id)>0),
      cohort_id TEXT NOT NULL CHECK(length(cohort_id)>0),
      manifest_id TEXT NOT NULL CHECK(length(manifest_id)>0),
      manifest_file_sha256 TEXT NOT NULL CHECK(length(manifest_file_sha256)=64 AND manifest_file_sha256 NOT GLOB '*[^0-9a-f]*'),
      manifest_semantic_sha256 TEXT NOT NULL CHECK(length(manifest_semantic_sha256)=64 AND manifest_semantic_sha256 NOT GLOB '*[^0-9a-f]*'),
      schema_sql_sha256 TEXT NOT NULL CHECK(length(schema_sql_sha256)=64 AND schema_sql_sha256 NOT GLOB '*[^0-9a-f]*'),
      created_utc TEXT NOT NULL CHECK(length(created_utc)>0)
    )""",
    "identity_bindings": """CREATE TABLE identity_bindings(
      binding_id INTEGER PRIMARY KEY,
      namespace TEXT NOT NULL CHECK(namespace IN ('venue_quotes','verifier_evidence','economics_inputs','hold_switch_inputs','account_state')),
      identity_kind TEXT NOT NULL CHECK(identity_kind IN ('payload_id','record_id')),
      identity_value TEXT NOT NULL CHECK(length(identity_value) BETWEEN 1 AND 512),
      raw_object_sha256 TEXT NOT NULL CHECK(length(raw_object_sha256)=64 AND raw_object_sha256 NOT GLOB '*[^0-9a-f]*'),
      semantic_object_sha256 TEXT NOT NULL CHECK(length(semantic_object_sha256)=64 AND semantic_object_sha256 NOT GLOB '*[^0-9a-f]*'),
      first_replay_identity TEXT NOT NULL CHECK(length(first_replay_identity) BETWEEN 1 AND 512),
      first_seen_utc TEXT NOT NULL CHECK(length(first_seen_utc)>0)
    )""",
    "replay_bindings": """CREATE TABLE replay_bindings(
      binding_id INTEGER PRIMARY KEY,
      replay_identity TEXT NOT NULL CHECK(length(replay_identity) BETWEEN 1 AND 512),
      first_raw_submission_sha256 TEXT NOT NULL CHECK(length(first_raw_submission_sha256)=64 AND first_raw_submission_sha256 NOT GLOB '*[^0-9a-f]*'),
      semantic_submission_sha256 TEXT NOT NULL CHECK(length(semantic_submission_sha256)=64 AND semantic_submission_sha256 NOT GLOB '*[^0-9a-f]*'),
      first_seen_utc TEXT NOT NULL CHECK(length(first_seen_utc)>0)
    )""",
    "replay_observations": """CREATE TABLE replay_observations(
      observation_id INTEGER PRIMARY KEY,
      replay_identity TEXT NOT NULL CHECK(length(replay_identity) BETWEEN 1 AND 512),
      raw_submission_sha256 TEXT NOT NULL CHECK(length(raw_submission_sha256)=64 AND raw_submission_sha256 NOT GLOB '*[^0-9a-f]*'),
      semantic_submission_sha256 TEXT NOT NULL CHECK(length(semantic_submission_sha256)=64 AND semantic_submission_sha256 NOT GLOB '*[^0-9a-f]*'),
      observed_utc TEXT NOT NULL CHECK(length(observed_utc)>0)
    )""",
    "identity_conflicts": """CREATE TABLE identity_conflicts(
      conflict_id INTEGER PRIMARY KEY,
      namespace TEXT NOT NULL CHECK(length(namespace)>0),
      identity_kind TEXT NOT NULL CHECK(length(identity_kind)>0),
      identity_value TEXT NOT NULL CHECK(length(identity_value) BETWEEN 1 AND 512),
      registered_semantic_sha256 TEXT NOT NULL CHECK(length(registered_semantic_sha256)=64 AND registered_semantic_sha256 NOT GLOB '*[^0-9a-f]*'),
      proposed_semantic_sha256 TEXT NOT NULL CHECK(length(proposed_semantic_sha256)=64 AND proposed_semantic_sha256 NOT GLOB '*[^0-9a-f]*'),
      replay_identity TEXT NOT NULL CHECK(length(replay_identity) BETWEEN 1 AND 512),
      observed_utc TEXT NOT NULL CHECK(length(observed_utc)>0)
    )""",
}

REGISTRY_INDEX_SQL = {
    "ux_registry_meta_manifest_binding": (
        "CREATE UNIQUE INDEX ux_registry_meta_manifest_binding ON registry_meta("
        "schema_id,contract_id,cohort_id,manifest_id,manifest_file_sha256,manifest_semantic_sha256)"
    ),
    "ux_identity_bindings_identity": (
        "CREATE UNIQUE INDEX ux_identity_bindings_identity ON identity_bindings("
        "namespace,identity_kind,identity_value)"
    ),
    "ix_identity_bindings_first_replay": (
        "CREATE INDEX ix_identity_bindings_first_replay ON identity_bindings(first_replay_identity)"
    ),
    "ux_replay_bindings_identity": (
        "CREATE UNIQUE INDEX ux_replay_bindings_identity ON replay_bindings(replay_identity)"
    ),
    "ix_replay_observations_replay": (
        "CREATE INDEX ix_replay_observations_replay ON replay_observations(replay_identity,observation_id)"
    ),
    "ix_identity_conflicts_identity": (
        "CREATE INDEX ix_identity_conflicts_identity ON identity_conflicts("
        "namespace,identity_kind,identity_value,conflict_id)"
    ),
}

REGISTRY_TRIGGER_SQL = {
    f"no_{operation}_{table}": (
        f"CREATE TRIGGER no_{operation}_{table} BEFORE {operation.upper()} ON {table} "
        "BEGIN SELECT RAISE(ABORT,'append-only v6 identity registry'); END"
    )
    for table in REGISTRY_TABLE_SQL
    for operation in ("update", "delete")
}

REGISTRY_SCHEMA_SHA256 = stable_hash({
    "tables": REGISTRY_TABLE_SQL,
    "indexes": REGISTRY_INDEX_SQL,
    "triggers": REGISTRY_TRIGGER_SQL,
})

REGISTRY_TABLE_INFO = {
    "registry_meta": (
        (0, "singleton", "INTEGER", 0, None, 1),
        (1, "schema_id", "TEXT", 1, None, 0),
        (2, "contract_id", "TEXT", 1, None, 0),
        (3, "cohort_id", "TEXT", 1, None, 0),
        (4, "manifest_id", "TEXT", 1, None, 0),
        (5, "manifest_file_sha256", "TEXT", 1, None, 0),
        (6, "manifest_semantic_sha256", "TEXT", 1, None, 0),
        (7, "schema_sql_sha256", "TEXT", 1, None, 0),
        (8, "created_utc", "TEXT", 1, None, 0),
    ),
    "identity_bindings": (
        (0, "binding_id", "INTEGER", 0, None, 1),
        (1, "namespace", "TEXT", 1, None, 0),
        (2, "identity_kind", "TEXT", 1, None, 0),
        (3, "identity_value", "TEXT", 1, None, 0),
        (4, "raw_object_sha256", "TEXT", 1, None, 0),
        (5, "semantic_object_sha256", "TEXT", 1, None, 0),
        (6, "first_replay_identity", "TEXT", 1, None, 0),
        (7, "first_seen_utc", "TEXT", 1, None, 0),
    ),
    "replay_bindings": (
        (0, "binding_id", "INTEGER", 0, None, 1),
        (1, "replay_identity", "TEXT", 1, None, 0),
        (2, "first_raw_submission_sha256", "TEXT", 1, None, 0),
        (3, "semantic_submission_sha256", "TEXT", 1, None, 0),
        (4, "first_seen_utc", "TEXT", 1, None, 0),
    ),
    "replay_observations": (
        (0, "observation_id", "INTEGER", 0, None, 1),
        (1, "replay_identity", "TEXT", 1, None, 0),
        (2, "raw_submission_sha256", "TEXT", 1, None, 0),
        (3, "semantic_submission_sha256", "TEXT", 1, None, 0),
        (4, "observed_utc", "TEXT", 1, None, 0),
    ),
    "identity_conflicts": (
        (0, "conflict_id", "INTEGER", 0, None, 1),
        (1, "namespace", "TEXT", 1, None, 0),
        (2, "identity_kind", "TEXT", 1, None, 0),
        (3, "identity_value", "TEXT", 1, None, 0),
        (4, "registered_semantic_sha256", "TEXT", 1, None, 0),
        (5, "proposed_semantic_sha256", "TEXT", 1, None, 0),
        (6, "replay_identity", "TEXT", 1, None, 0),
        (7, "observed_utc", "TEXT", 1, None, 0),
    ),
}

REGISTRY_INDEX_DEFINITIONS = {
    "registry_meta": {
        "ux_registry_meta_manifest_binding": (
            True,
            ("schema_id", "contract_id", "cohort_id", "manifest_id", "manifest_file_sha256", "manifest_semantic_sha256"),
        ),
    },
    "identity_bindings": {
        "ux_identity_bindings_identity": (
            True, ("namespace", "identity_kind", "identity_value"),
        ),
        "ix_identity_bindings_first_replay": (False, ("first_replay_identity",)),
    },
    "replay_bindings": {
        "ux_replay_bindings_identity": (True, ("replay_identity",)),
    },
    "replay_observations": {
        "ix_replay_observations_replay": (False, ("replay_identity", "observation_id")),
    },
    "identity_conflicts": {
        "ix_identity_conflicts_identity": (
            False, ("namespace", "identity_kind", "identity_value", "conflict_id"),
        ),
    },
}


def _validate_contracts(
    contract: Mapping[str, Any], upstream_v5_contract: Mapping[str, Any],
) -> None:
    try:
        from .currency_state_after_cost_counterfactual_v2 import validate_contract
        validate_contract(contract)
    except AfterCostV2Error as exc:
        raise AfterCostV6Error(str(exc)) from exc
    if (
        contract.get("contract_id") != "currency_state_after_cost_counterfactual_v6_20260817"
        or contract.get("counterfactual_cohort_id")
        != "currency_state_after_cost_counterfactual_cohort_20260817f"
        or contract.get("supersedes_contract_id") != upstream_v5_contract.get("contract_id")
        or contract.get("supersedes_cohort_id")
        != upstream_v5_contract.get("counterfactual_cohort_id")
        or contract.get("required_upstream_v5_contract_id")
        != upstream_v5_contract.get("contract_id")
        or contract.get("required_upstream_v5_cohort_id")
        != upstream_v5_contract.get("counterfactual_cohort_id")
        or contract.get("required_upstream_v5_manifest_id")
        != upstream_v5_contract.get("required_frozen_manifest_id")
    ):
        raise AfterCostV6Error("v6 contract/cohort/upstream-v5 identity mismatch")
    shared = (
        "expected_instruments", "instrument_universe_sha256",
        "expected_horizons_sec", "expected_arm_ids",
        "forward_admissible_arm_ids", "never_forward_arm_ids",
        "required_response_contract_id", "required_response_snapshot_schema",
        "required_state_contract_id", "required_basis_eligibility_contract_id",
        "canonical_envelope_contract", "quote_contract", "verifier_contract",
        "economics_contract", "account_policy", "rotation_close_contract",
        "selection", "hold_switch",
    )
    if any(contract.get(field) != upstream_v5_contract.get(field) for field in shared):
        raise AfterCostV6Error("v6 changed frozen v5 research/economics behavior")
    inherited_ancestry = (
        "required_upstream_v4_contract_id", "required_upstream_v4_cohort_id",
        "required_upstream_v4_manifest_id", "required_upstream_v3_contract_id",
        "required_upstream_v3_cohort_id", "required_upstream_v3_manifest_id",
    )
    if any(
        contract.get(field) != upstream_v5_contract.get(field)
        for field in inherited_ancestry
    ):
        raise AfterCostV6Error("v6 changed frozen v3/v4 ancestry identity")
    if contract.get("operational_input_policy") != {
        "mode": "zero_input_engineering_initialization_only",
        "producer_integration_state": "producer_integration_missing",
        "nonempty_operational_input_requires_separately_reviewed_new_cohort": True,
        "core_fixture_inputs_are_test_only": True,
    }:
        raise AfterCostV6Error("v6 operational input policy changed")
    registry = contract.get("identity_registry_contract") or {}
    if (
        registry.get("schema_id") != "currency_state_after_cost_identity_registry_v6_20260817"
        or registry.get("canonical_relative_path")
        != "data/oanda_training_manager/state/currency_state_after_cost_identity_registry_v6.sqlite"
        or registry.get("schema_sql_sha256") != REGISTRY_SCHEMA_SHA256
        or registry.get("append_only") is not True
        or registry.get("exact_sql_verified_on_every_replay") is not True
        or registry.get("failure_atomic_initialization") is not True
        or registry.get("missing_path_checks_are_noncreating") is not True
        or registry.get("batch_duplicate_keys_rejected_before_insert") is not True
        or registry.get("order_invariant_semantic_admission") is not True
        or registry.get("manifest_file_and_semantic_hashes_bound") is not True
        or registry.get("raw_and_semantic_object_hashes_retained") is not True
        or registry.get("replay_payload_record_equivocation_fails_closed") is not True
        or registry.get("wrong_registry_contract_or_cohort_fails_closed") is not True
    ):
        raise AfterCostV6Error("v6 persistent identity registry contract changed")
    genealogy = contract.get("genealogy_contract") or {}
    if (
        genealogy.get("canonical_relative_path")
        != "data/oanda_training_manager/state/research_genealogy_v1.sqlite"
        or genealogy.get("caller_override_allowed") is not False
        or genealogy.get("verify_exact_database_schema_sql") is not True
        or genealogy.get("verify_primary_key_and_unique_indexes") is not True
        or genealogy.get("verify_exact_hypothesis_cardinality") is not True
        or genealogy.get("verify_immutable_trigger_sql") is not True
        or genealogy.get("verify_all_experiment_columns") is not True
        or genealogy.get("recompute_definition_sha256") is not True
    ):
        raise AfterCostV6Error("v6 canonical genealogy policy changed")


def _validate_v6_manifest(
    manifest: Mapping[str, Any], contract: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        normalized = _validate_manifest(manifest, contract)
    except AfterCostV2Error as exc:
        raise AfterCostV6Error(str(exc)) from exc
    actual = {
        label: str(row.get("relative_path") or "").replace("\\", "/")
        for label, row in (manifest.get("artifacts") or {}).items()
    }
    if actual != EXPECTED_ARTIFACT_PATHS:
        raise AfterCostV6Error("v6 manifest label-to-path map mismatch")
    return normalized


def _sha256(value: str, label: str) -> str:
    normalized = str(value or "").lower()
    if SHA256_RE.fullmatch(normalized) is None:
        raise AfterCostV6Error(f"{label} must be a lowercase SHA-256")
    return normalized


def _utc_now() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat()


def _normalize_sql(value: str | None) -> str:
    return " ".join(str(value or "").strip().rstrip(";").split())


def _expected_registry_schema_rows() -> dict[tuple[str, str], tuple[str, str]]:
    rows: dict[tuple[str, str], tuple[str, str]] = {}
    for name, sql in REGISTRY_TABLE_SQL.items():
        rows[("table", name)] = (name, _normalize_sql(sql))
    for name, sql in REGISTRY_INDEX_SQL.items():
        table = next(
            table_name for table_name in REGISTRY_TABLE_SQL
            if f" ON {table_name}(" in sql
        )
        rows[("index", name)] = (table, _normalize_sql(sql))
    for name, sql in REGISTRY_TRIGGER_SQL.items():
        table = next(
            table_name for table_name in REGISTRY_TABLE_SQL
            if f" ON {table_name} " in sql
        )
        rows[("trigger", name)] = (table, _normalize_sql(sql))
    return rows


EXPECTED_REGISTRY_SCHEMA_ROWS = _expected_registry_schema_rows()


def _verify_registry_schema(connection: sqlite3.Connection) -> None:
    actual = {
        (str(row[0]), str(row[1])): (str(row[2]), _normalize_sql(row[3]))
        for row in connection.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_schema "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
        )
    }
    if actual != EXPECTED_REGISTRY_SCHEMA_ROWS:
        raise AfterCostV6Error("v6 identity registry exact SQL schema mismatch")
    for table, expected_info in REGISTRY_TABLE_INFO.items():
        info = tuple(tuple(row) for row in connection.execute(f"PRAGMA table_info({table})"))
        if info != expected_info:
            raise AfterCostV6Error(f"v6 identity registry table/PK mismatch: {table}")
        expected_indexes = REGISTRY_INDEX_DEFINITIONS[table]
        actual_indexes: dict[str, tuple[bool, str, bool]] = {}
        for row in connection.execute(f"PRAGMA index_list({table})"):
            actual_indexes[str(row[1])] = (bool(row[2]), str(row[3]), bool(row[4]))
        if actual_indexes != {
            name: (unique, "c", False)
            for name, (unique, _columns) in expected_indexes.items()
        }:
            raise AfterCostV6Error(f"v6 identity registry index/unique mismatch: {table}")
        for name, (_unique, expected_columns) in expected_indexes.items():
            columns = tuple(
                str(row[2]) for row in connection.execute(f"PRAGMA index_info({name})")
            )
            if columns != expected_columns:
                raise AfterCostV6Error(f"v6 identity registry index columns mismatch: {name}")
    integrity = connection.execute("PRAGMA integrity_check").fetchall()
    if integrity != [("ok",)]:
        raise AfterCostV6Error("v6 identity registry integrity check failed")


def _verify_registry(
    connection: sqlite3.Connection,
    *,
    contract: Mapping[str, Any],
    manifest_id: str,
    manifest_file_sha256: str,
    manifest_semantic_sha256: str,
) -> None:
    """Verify exact DDL, PKs, unique indexes, triggers, and metadata each time."""
    _verify_registry_schema(connection)
    rows = connection.execute(
        "SELECT singleton,schema_id,contract_id,cohort_id,manifest_id,"
        "manifest_file_sha256,manifest_semantic_sha256,schema_sql_sha256,created_utc "
        "FROM registry_meta"
    ).fetchall()
    policy = contract["identity_registry_contract"]
    expected = (
        1,
        policy["schema_id"],
        contract["contract_id"],
        contract["counterfactual_cohort_id"],
        manifest_id,
        manifest_file_sha256,
        manifest_semantic_sha256,
        REGISTRY_SCHEMA_SHA256,
        contract["cohort_start_utc"],
    )
    if rows != [expected]:
        raise AfterCostV6Error("wrong v6 identity registry manifest/cohort metadata binding")


def _open_existing_registry(path: Path) -> sqlite3.Connection:
    """Open an existing target read/write without SQLite's create behavior."""
    if path.is_symlink() or not path.is_file():
        raise AfterCostV6Error("v6 identity registry missing or not a regular file")
    try:
        connection = sqlite3.connect(
            path.resolve().as_uri() + "?mode=rw", uri=True, timeout=30.0,
        )
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection
    except sqlite3.Error as exc:
        raise AfterCostV6Error("v6 identity registry open failed") from exc


def _fsync_file(path: Path) -> None:
    # Windows' CRT rejects fsync/commit on a read-only descriptor even though
    # POSIX accepts it, so use a non-truncating read/write descriptor here.
    descriptor = os.open(str(path), os.O_RDWR | getattr(os, "O_BINARY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    """Persist a directory entry where the platform exposes directory fsync."""
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(str(path), flags)
    except OSError:
        return
    try:
        try:
            os.fsync(descriptor)
        except OSError:
            # Windows does not expose POSIX directory fsync.  The database file
            # itself was already fully synchronized before the atomic link.
            pass
    finally:
        os.close(descriptor)


def _create_registry_temp_database(
    target: Path,
    *,
    contract: Mapping[str, Any],
    manifest_id: str,
    manifest_file_sha256: str,
    manifest_semantic_sha256: str,
) -> Path:
    descriptor, raw_path = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".init.sqlite", dir=str(target.parent),
    )
    os.close(descriptor)
    temporary = Path(raw_path)
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(temporary, timeout=30.0)
        mode = str(connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0]).lower()
        if mode != "delete":
            raise AfterCostV6Error("v6 registry initialization requires DELETE journal mode")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("BEGIN EXCLUSIVE")
        for sql in REGISTRY_TABLE_SQL.values():
            connection.execute(sql)
        for sql in REGISTRY_INDEX_SQL.values():
            connection.execute(sql)
        for sql in REGISTRY_TRIGGER_SQL.values():
            connection.execute(sql)
        policy = contract["identity_registry_contract"]
        connection.execute(
            "INSERT INTO registry_meta("
            "singleton,schema_id,contract_id,cohort_id,manifest_id,"
            "manifest_file_sha256,manifest_semantic_sha256,schema_sql_sha256,created_utc"
            ") VALUES(1,?,?,?,?,?,?,?,?)",
            (
                policy["schema_id"], contract["contract_id"],
                contract["counterfactual_cohort_id"], manifest_id,
                manifest_file_sha256, manifest_semantic_sha256,
                REGISTRY_SCHEMA_SHA256, contract["cohort_start_utc"],
            ),
        )
        connection.commit()
        connection.close()
        connection = None
        _fsync_file(temporary)
        verification = _open_existing_registry(temporary)
        try:
            _verify_registry(
                verification,
                contract=contract,
                manifest_id=manifest_id,
                manifest_file_sha256=manifest_file_sha256,
                manifest_semantic_sha256=manifest_semantic_sha256,
            )
        finally:
            verification.close()
        return temporary
    except Exception:
        if connection is not None:
            try:
                connection.rollback()
            except sqlite3.Error:
                pass
            connection.close()
        temporary.unlink(missing_ok=True)
        temporary.with_name(temporary.name + "-journal").unlink(missing_ok=True)
        temporary.with_name(temporary.name + "-wal").unlink(missing_ok=True)
        temporary.with_name(temporary.name + "-shm").unlink(missing_ok=True)
        raise


def _atomic_install_no_replace(temporary: Path, target: Path) -> None:
    """Atomically publish a same-directory file without replacing a winner."""
    try:
        os.link(temporary, target)
    except FileExistsError as exc:
        raise AfterCostV6Error("v6 identity registry already exists") from exc
    except OSError as exc:
        raise AfterCostV6Error("atomic no-clobber registry install failed") from exc
    _fsync_directory(target.parent)


def initialize_identity_registry_v6(
    path: Path,
    *,
    contract: Mapping[str, Any],
    manifest_id: str,
    manifest_file_sha256: str,
    manifest_semantic_sha256: str,
) -> None:
    """Failure-atomically create one V6 registry; never repair or replace one."""
    path = Path(path)
    manifest_file_sha256 = _sha256(manifest_file_sha256, "manifest_file_sha256")
    manifest_semantic_sha256 = _sha256(
        manifest_semantic_sha256, "manifest_semantic_sha256",
    )
    policy = contract.get("identity_registry_contract") or {}
    if (
        contract.get("contract_id")
        != "currency_state_after_cost_counterfactual_v6_20260817"
        or contract.get("counterfactual_cohort_id")
        != "currency_state_after_cost_counterfactual_cohort_20260817f"
        or contract.get("cohort_start_utc") != "2026-08-17T07:00:00+00:00"
        or contract.get("required_frozen_manifest_id")
        != "currency_state_after_cost_manifest_20260817f"
        or policy.get("schema_id")
        != "currency_state_after_cost_identity_registry_v6_20260817"
        or policy.get("canonical_relative_path")
        != "data/oanda_training_manager/state/currency_state_after_cost_identity_registry_v6.sqlite"
        or policy.get("schema_sql_sha256") != REGISTRY_SCHEMA_SHA256
        or policy.get("append_only") is not True
        or policy.get("exact_sql_verified_on_every_replay") is not True
        or policy.get("failure_atomic_initialization") is not True
        or policy.get("missing_path_checks_are_noncreating") is not True
        or policy.get("batch_duplicate_keys_rejected_before_insert") is not True
        or policy.get("order_invariant_semantic_admission") is not True
        or policy.get("manifest_file_and_semantic_hashes_bound") is not True
        or manifest_id != contract.get("required_frozen_manifest_id")
    ):
        raise AfterCostV6Error("v6 registry initialization contract/manifest mismatch")
    if path.exists() or path.is_symlink():
        raise AfterCostV6Error("v6 identity registry already exists")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        temporary = _create_registry_temp_database(
            path,
            contract=contract,
            manifest_id=manifest_id,
            manifest_file_sha256=manifest_file_sha256,
            manifest_semantic_sha256=manifest_semantic_sha256,
        )
        _atomic_install_no_replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
            temporary.with_name(temporary.name + "-journal").unlink(missing_ok=True)
            temporary.with_name(temporary.name + "-wal").unlink(missing_ok=True)
            temporary.with_name(temporary.name + "-shm").unlink(missing_ok=True)


def _semantic_envelope(envelope: Mapping[str, Any], record_rows: Sequence[dict[str, str]]) -> str:
    normalized = {
        key: copy.deepcopy(value)
        for key, value in envelope.items()
        if key not in ("records", "canonical_envelope_sha256")
    }
    normalized["records"] = [
        row["record"] for row in sorted(
            record_rows,
            key=lambda row: (
                row["record_id"], row["semantic_object_sha256"], row["raw_object_sha256"],
            ),
        )
    ]
    return stable_hash(normalized)


def _identity_candidates(
    envelopes: Mapping[str, Mapping[str, Any] | None],
) -> tuple[str, str, list[tuple[str, str, str, str, str]]]:
    """Return deterministic hashes/candidates after a no-insert duplicate audit."""
    candidates: list[tuple[str, str, str, str, str]] = []
    semantic_envelopes: list[dict[str, Any]] = []
    raw_submission: dict[str, Any] = {}
    for name in sorted(ENVELOPES):
        envelope = envelopes.get(name)
        if envelope is None:
            continue
        if not isinstance(envelope, Mapping):
            raise AfterCostV6Error(f"{name} envelope is not a mapping")
        raw_submission[name] = copy.deepcopy(envelope)
        payload_id = str(envelope.get("payload_id") or "")
        if not payload_id:
            raise AfterCostV6Error("empty payload or record identity")
        record_rows: list[dict[str, Any]] = []
        for record in envelope.get("records") or []:
            record_id = str(_record_id(record, name) or "")
            if not record_id:
                raise AfterCostV6Error("empty payload or record identity")
            raw_sha = stable_hash(record)
            record_rows.append({
                "record_id": record_id,
                "raw_object_sha256": raw_sha,
                "semantic_object_sha256": raw_sha,
                "record": copy.deepcopy(record),
            })
            candidates.append((name, "record_id", record_id, raw_sha, raw_sha))
        payload_raw_sha = stable_hash(envelope)
        payload_semantic_sha = _semantic_envelope(envelope, record_rows)
        candidates.append((name, "payload_id", payload_id, payload_raw_sha, payload_semantic_sha))
        semantic_envelopes.append({
            "namespace": name,
            "payload_id": payload_id,
            "semantic_object_sha256": payload_semantic_sha,
            "records": [
                {
                    "record_id": row["record_id"],
                    "semantic_object_sha256": row["semantic_object_sha256"],
                }
                for row in sorted(
                    record_rows,
                    key=lambda row: (row["record_id"], row["semantic_object_sha256"]),
                )
            ],
        })
    if not candidates:
        raise AfterCostV6Error("identity admission called without submitted envelopes")
    seen: dict[tuple[str, str, str], tuple[str, str]] = {}
    duplicate_kinds: set[str] = set()
    for namespace, kind, identity, raw_sha, semantic_sha in candidates:
        key = (namespace, kind, identity)
        previous = seen.get(key)
        if previous is not None:
            duplicate_kinds.add("equivocal" if previous[1] != semantic_sha else "duplicate")
        else:
            seen[key] = (raw_sha, semantic_sha)
    if duplicate_kinds:
        detail = "equivocal" if "equivocal" in duplicate_kinds else "duplicate"
        raise AfterCostV6Error(
            f"{detail} identity key within submitted batch rejected before insert"
        )
    candidates.sort(key=lambda row: (row[0], row[1], row[2], row[4], row[3]))
    raw_submission_sha = stable_hash(raw_submission)
    semantic_submission_sha = stable_hash({
        "envelopes": sorted(
            semantic_envelopes,
            key=lambda row: (row["namespace"], row["payload_id"]),
        ),
    })
    return raw_submission_sha, semantic_submission_sha, candidates


def admit_persistent_identities_v6(
    path: Path,
    *,
    contract: Mapping[str, Any],
    manifest_id: str,
    manifest_file_sha256: str,
    manifest_semantic_sha256: str,
    replay_identity: str,
    envelopes: Mapping[str, Mapping[str, Any] | None],
) -> dict[str, Any]:
    if not replay_identity or len(replay_identity) > 512:
        raise AfterCostV6Error("nonempty v6 input requires bounded replay_identity")
    manifest_file_sha256 = _sha256(manifest_file_sha256, "manifest_file_sha256")
    manifest_semantic_sha256 = _sha256(
        manifest_semantic_sha256, "manifest_semantic_sha256",
    )
    # This full batch audit happens before the database is opened or any row is
    # inserted, so input order can never choose the winning canonical binding.
    raw_submission_sha, semantic_submission_sha, candidates = _identity_candidates(envelopes)
    now = _utc_now()
    connection = _open_existing_registry(Path(path))
    try:
        connection.execute("BEGIN IMMEDIATE")
        _verify_registry(
            connection,
            contract=contract,
            manifest_id=manifest_id,
            manifest_file_sha256=manifest_file_sha256,
            manifest_semantic_sha256=manifest_semantic_sha256,
        )
        conflicts: list[tuple[str, str, str, str, str]] = []
        replay = connection.execute(
            "SELECT semantic_submission_sha256 FROM replay_bindings WHERE replay_identity=?",
            (replay_identity,),
        ).fetchone()
        if replay is not None and str(replay[0]) != semantic_submission_sha:
            conflicts.append((
                "replay", "replay_identity", replay_identity,
                str(replay[0]), semantic_submission_sha,
            ))
        missing_candidates: list[tuple[str, str, str, str, str]] = []
        for namespace, kind, identity, raw_sha, semantic_sha in candidates:
            existing = connection.execute(
                "SELECT semantic_object_sha256 FROM identity_bindings "
                "WHERE namespace=? AND identity_kind=? AND identity_value=?",
                (namespace, kind, identity),
            ).fetchone()
            if existing is None:
                missing_candidates.append((namespace, kind, identity, raw_sha, semantic_sha))
            elif str(existing[0]) != semantic_sha:
                conflicts.append((
                    namespace, kind, identity, str(existing[0]), semantic_sha,
                ))
        if conflicts:
            connection.executemany(
                "INSERT INTO identity_conflicts("
                "namespace,identity_kind,identity_value,registered_semantic_sha256,"
                "proposed_semantic_sha256,replay_identity,observed_utc"
                ") VALUES(?,?,?,?,?,?,?)",
                [(*row, replay_identity, now) for row in sorted(conflicts)],
            )
            connection.commit()
            raise AfterCostV6Error("persistent replay/payload/record identity equivocation")
        if replay is None:
            connection.execute(
                "INSERT INTO replay_bindings("
                "replay_identity,first_raw_submission_sha256,semantic_submission_sha256,first_seen_utc"
                ") VALUES(?,?,?,?)",
                (replay_identity, raw_submission_sha, semantic_submission_sha, now),
            )
        connection.executemany(
            "INSERT INTO identity_bindings("
            "namespace,identity_kind,identity_value,raw_object_sha256,"
            "semantic_object_sha256,first_replay_identity,first_seen_utc"
            ") VALUES(?,?,?,?,?,?,?)",
            [(*row, replay_identity, now) for row in missing_candidates],
        )
        connection.execute(
            "INSERT INTO replay_observations("
            "replay_identity,raw_submission_sha256,semantic_submission_sha256,observed_utc"
            ") VALUES(?,?,?,?)",
            (replay_identity, raw_submission_sha, semantic_submission_sha, now),
        )
        connection.commit()
        return {
            "registry_schema_id": contract["identity_registry_contract"]["schema_id"],
            "registry_schema_sql_sha256": REGISTRY_SCHEMA_SHA256,
            "registry_contract_id": contract["contract_id"],
            "registry_cohort_id": contract["counterfactual_cohort_id"],
            "registry_manifest_id": manifest_id,
            "registry_manifest_file_sha256": manifest_file_sha256,
            "registry_manifest_semantic_sha256": manifest_semantic_sha256,
            "replay_identity": replay_identity,
            "raw_submission_sha256": raw_submission_sha,
            "semantic_submission_sha256": semantic_submission_sha,
            "identity_count": len(candidates),
            "equivocation_detected": False,
        }
    except AfterCostV6Error:
        if connection.in_transaction:
            connection.rollback()
        raise
    except sqlite3.Error as exc:
        if connection.in_transaction:
            connection.rollback()
        raise AfterCostV6Error("v6 identity registry read/write failed") from exc
    finally:
        connection.close()


def build_after_cost_counterfactual_v6(
    response_snapshot: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    frozen_manifest: Mapping[str, Any],
    frozen_manifest_file_sha256: str,
    upstream_v5_contract: Mapping[str, Any],
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
    _validate_contracts(contract, upstream_v5_contract)
    manifest = _validate_v6_manifest(frozen_manifest, contract)
    manifest_file_sha256 = _sha256(
        frozen_manifest_file_sha256, "frozen_manifest_file_sha256",
    )
    supplied = {
        "venue_quotes": venue_quotes,
        "verifier_evidence": verifier_evidence,
        "economics_inputs": economics_inputs,
        "hold_switch_inputs": hold_switch_inputs,
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
        raise AfterCostV6Error(str(exc)) from exc
    any_supplied = any(value is not None for value in supplied.values())
    if any_supplied:
        if identity_registry_path is None:
            raise AfterCostV6Error("nonempty v6 core fixture requires persistent identity registry")
        identity_state = admit_persistent_identities_v6(
            identity_registry_path,
            contract=contract,
            manifest_id=manifest["manifest_id"],
            manifest_file_sha256=manifest_file_sha256,
            manifest_semantic_sha256=manifest["manifest_sha256"],
            replay_identity=str(replay_identity or ""),
            envelopes=supplied,
        )
    else:
        if identity_registry_path is not None or replay_identity is not None:
            raise AfterCostV6Error("zero-input v6 initialization cannot claim replay identity")
        identity_state = {
            "registry_schema_id": contract["identity_registry_contract"]["schema_id"],
            "registry_schema_sql_sha256": REGISTRY_SCHEMA_SHA256,
            "registry_contract_id": contract["contract_id"],
            "registry_cohort_id": contract["counterfactual_cohort_id"],
            "registry_manifest_id": manifest["manifest_id"],
            "registry_manifest_file_sha256": manifest_file_sha256,
            "registry_manifest_semantic_sha256": manifest["manifest_sha256"],
            "replay_identity": None,
            "raw_submission_sha256": None,
            "semantic_submission_sha256": None,
            "identity_count": 0,
            "equivocation_detected": False,
            "state": "not_opened_for_zero_input_initialization",
        }
    base.update({
        "schema_version": 6,
        "snapshot_schema": contract["snapshot_schema"],
        "counterfactual_contract_id": contract["contract_id"],
        "counterfactual_cohort_id": contract["counterfactual_cohort_id"],
        "supersedes_contract_id": contract["supersedes_contract_id"],
        "supersedes_cohort_id": contract["supersedes_cohort_id"],
        "frozen_manifest": manifest,
        "frozen_manifest_file_sha256": manifest_file_sha256,
        "upstream_v5_contract_id": upstream_v5_contract["contract_id"],
        "upstream_v5_cohort_id": upstream_v5_contract["counterfactual_cohort_id"],
        "upstream_v4_contract_id": upstream_v4_contract["contract_id"],
        "upstream_v4_cohort_id": upstream_v4_contract["counterfactual_cohort_id"],
        "identity_registry_state": identity_state,
        "payload_identity_policy": copy.deepcopy(contract["payload_identity_policy"]),
    })
    base["limitations"] = [
        "v3, v4, and v5 remain immutable and are not migrated or re-registered",
        "the operational v6 publisher blocks every nonempty input",
        "core nonempty fixtures require failure-atomic persistent identity binding",
        "genealogy trust requires exact canonical schema, triggers, cardinality, and row",
        "all results remain research-only no_trade",
    ]
    base["decision_fingerprint"] = stable_hash({
        "records": base["records"],
        "allocations": base["allocations"],
        "holds": base["hold_switch_counterfactuals"],
        "identity_registry_state": identity_state,
        "supported_execution_decision": base["supported_execution_decision"],
    })
    base["snapshot_id"] = "currency_state_after_cost_v6_" + stable_hash({
        key: value for key, value in base.items() if key != "snapshot_id"
    })[:24]
    if (
        len(base["records"]) != 8 * 68 * 5
        or any(row["paper_decision"] != "no_trade" for row in base["records"])
        or base.get("supported_execution_decision") != "no_trade"
    ):
        raise AfterCostV6Error("v6 grid or safety invariant failed")
    return base


__all__ = [
    "AfterCostV6Error", "EXPECTED_ARTIFACT_PATHS", "REGISTRY_SCHEMA_SHA256",
    "REGISTRY_TABLE_SQL", "REGISTRY_INDEX_SQL", "REGISTRY_TRIGGER_SQL",
    "initialize_identity_registry_v6", "admit_persistent_identities_v6",
    "build_after_cost_counterfactual_v6",
]
