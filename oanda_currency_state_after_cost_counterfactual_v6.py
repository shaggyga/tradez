#!/usr/bin/env python3
"""Publish the isolated, engineering-blocked V6 after-cost proof grid.

The genealogy and identity-registry paths are hard constants.  The publisher
unconditionally rejects all nonempty operational envelopes.  Zero-input runs
are research-only/no-trade and never create or mutate either SQLite database.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

try:
    from forex_system.contracts.currency_state import load_contract, stable_hash
    from forex_system.research.currency_state_after_cost_counterfactual_v2 import _validate_manifest
    from forex_system.research.currency_state_after_cost_counterfactual_v6 import (
        EXPECTED_ARTIFACT_PATHS,
        build_after_cost_counterfactual_v6,
    )
    from forex_system.research.currency_state_after_cost_registrar_v6 import (
        EXPERIMENT_COLUMNS,
        canonical_json,
        registrar_definition_v6,
    )
    from forex_system.research.currency_state_response_timing_arms import build_response_timing_arms
except ModuleNotFoundError:
    from src.forex_system.contracts.currency_state import load_contract, stable_hash
    from src.forex_system.research.currency_state_after_cost_counterfactual_v2 import _validate_manifest
    from src.forex_system.research.currency_state_after_cost_counterfactual_v6 import (
        EXPECTED_ARTIFACT_PATHS,
        build_after_cost_counterfactual_v6,
    )
    from src.forex_system.research.currency_state_after_cost_registrar_v6 import (
        EXPERIMENT_COLUMNS,
        canonical_json,
        registrar_definition_v6,
    )
    from src.forex_system.research.currency_state_response_timing_arms import build_response_timing_arms


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "currency_state_after_cost_counterfactual_v6.json"
MANIFEST = ROOT / "config" / "currency_state_after_cost_counterfactual_v6_manifest.json"
UPSTREAM_V5_CONFIG = ROOT / "config" / "currency_state_after_cost_counterfactual_v5.json"
UPSTREAM_V4_CONFIG = ROOT / "config" / "currency_state_after_cost_counterfactual_v4.json"
UPSTREAM_V4_MANIFEST = ROOT / "config" / "currency_state_after_cost_counterfactual_v4_manifest.json"
UPSTREAM_V3_CONFIG = ROOT / "config" / "currency_state_after_cost_counterfactual_v3.json"
UPSTREAM_V3_MANIFEST = ROOT / "config" / "currency_state_after_cost_counterfactual_v3_manifest.json"
RESPONSE_CONFIG = ROOT / "config" / "currency_state_response_timing_arms_v1.json"
STATE_CONFIG = ROOT / "config" / "currency_state_engine_v2.json"
CONTEXT = ROOT / "data" / "oanda_training_manager" / "state" / "currency_state_official_context_v1.json"
# Security boundaries: neither path appears in run() or argparse.
GENEALOGY = ROOT / "data" / "oanda_training_manager" / "state" / "research_genealogy_v1.sqlite"
IDENTITY_REGISTRY = ROOT / "data" / "oanda_training_manager" / "state" / "currency_state_after_cost_identity_registry_v6.sqlite"
OUTPUT = ROOT / "data" / "oanda_training_manager" / "state" / "currency_state_after_cost_counterfactual_v6.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "currency_state_after_cost_counterfactual" / "CURRENCY_STATE_AFTER_COST_COUNTERFACTUAL_V6_CURRENT.md"


GENEALOGY_TABLE_SQL = {
    "experiments": """CREATE TABLE experiments (
      hypothesis_id TEXT PRIMARY KEY,
      parent_hypothesis_id TEXT,
      experiment_kind TEXT NOT NULL,
      research_generation TEXT NOT NULL,
      idea_origin TEXT NOT NULL,
      pre_registered INTEGER NOT NULL,
      data_sources_json TEXT NOT NULL,
      feature_contract_json TEXT NOT NULL,
      label_contract_json TEXT NOT NULL,
      model_contract_json TEXT NOT NULL,
      cost_contract_json TEXT NOT NULL,
      allocator_contract_json TEXT NOT NULL,
      training_period_json TEXT NOT NULL,
      selection_period_json TEXT NOT NULL,
      confirmation_period_json TEXT NOT NULL,
      all_parameters_tried_json TEXT NOT NULL,
      selection_rule TEXT NOT NULL,
      holdouts_touched_json TEXT NOT NULL,
      source_code_hash TEXT NOT NULL,
      data_snapshot_hash TEXT NOT NULL,
      definition_sha256 TEXT NOT NULL,
      created_at TEXT NOT NULL,
      definition_json TEXT NOT NULL
    )""",
    "experiment_observations": """CREATE TABLE experiment_observations (
      observation_id TEXT PRIMARY KEY,
      hypothesis_id TEXT NOT NULL,
      observed_at TEXT NOT NULL,
      source_system TEXT NOT NULL,
      result TEXT NOT NULL,
      retirement_reason TEXT,
      retired_at TEXT,
      evidence_sha256 TEXT NOT NULL,
      evidence_json TEXT NOT NULL
    )""",
    "genealogy_imports": """CREATE TABLE genealogy_imports (
      import_id TEXT PRIMARY KEY,
      imported_at TEXT NOT NULL,
      source_database TEXT NOT NULL,
      source_fingerprint TEXT NOT NULL,
      definition_count INTEGER NOT NULL,
      observation_count INTEGER NOT NULL
    )""",
}

GENEALOGY_INDEX_SQL = {
    "ix_genealogy_observations_latest": (
        "CREATE INDEX ix_genealogy_observations_latest ON "
        "experiment_observations(hypothesis_id,observed_at DESC)"
    ),
}

GENEALOGY_TRIGGER_SQL = {
    "experiments_no_update": (
        "CREATE TRIGGER experiments_no_update BEFORE UPDATE ON experiments "
        "BEGIN SELECT RAISE(ABORT,'append_only'); END"
    ),
    "experiments_no_delete": (
        "CREATE TRIGGER experiments_no_delete BEFORE DELETE ON experiments "
        "BEGIN SELECT RAISE(ABORT,'append_only'); END"
    ),
    "experiment_observations_no_update": (
        "CREATE TRIGGER experiment_observations_no_update BEFORE UPDATE ON "
        "experiment_observations BEGIN SELECT RAISE(ABORT,'append_only'); END"
    ),
    "experiment_observations_no_delete": (
        "CREATE TRIGGER experiment_observations_no_delete BEFORE DELETE ON "
        "experiment_observations BEGIN SELECT RAISE(ABORT,'append_only'); END"
    ),
}

GENEALOGY_TABLE_INFO = {
    "experiments": tuple(
        (index, name, "INTEGER" if name == "pre_registered" else "TEXT",
         0 if name in ("hypothesis_id", "parent_hypothesis_id") else 1,
         None, 1 if name == "hypothesis_id" else 0)
        for index, name in enumerate(EXPERIMENT_COLUMNS)
    ),
    "experiment_observations": (
        (0, "observation_id", "TEXT", 0, None, 1),
        (1, "hypothesis_id", "TEXT", 1, None, 0),
        (2, "observed_at", "TEXT", 1, None, 0),
        (3, "source_system", "TEXT", 1, None, 0),
        (4, "result", "TEXT", 1, None, 0),
        (5, "retirement_reason", "TEXT", 0, None, 0),
        (6, "retired_at", "TEXT", 0, None, 0),
        (7, "evidence_sha256", "TEXT", 1, None, 0),
        (8, "evidence_json", "TEXT", 1, None, 0),
    ),
    "genealogy_imports": (
        (0, "import_id", "TEXT", 0, None, 1),
        (1, "imported_at", "TEXT", 1, None, 0),
        (2, "source_database", "TEXT", 1, None, 0),
        (3, "source_fingerprint", "TEXT", 1, None, 0),
        (4, "definition_count", "INTEGER", 1, None, 0),
        (5, "observation_count", "INTEGER", 1, None, 0),
    ),
}

GENEALOGY_INDEX_DEFINITIONS = {
    "experiments": {
        "sqlite_autoindex_experiments_1": (True, "pk", ("hypothesis_id",)),
    },
    "experiment_observations": {
        "sqlite_autoindex_experiment_observations_1": (True, "pk", ("observation_id",)),
        "ix_genealogy_observations_latest": (
            False, "c", ("hypothesis_id", "observed_at"),
        ),
    },
    "genealogy_imports": {
        "sqlite_autoindex_genealogy_imports_1": (True, "pk", ("import_id",)),
    },
}


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _normalize_sql(value: str | None) -> str:
    return " ".join(str(value or "").strip().rstrip(";").split())


def _expected_genealogy_schema_rows() -> dict[tuple[str, str], tuple[str, str]]:
    rows: dict[tuple[str, str], tuple[str, str]] = {}
    for name, sql in GENEALOGY_TABLE_SQL.items():
        rows[("table", name)] = (name, _normalize_sql(sql))
    rows[("index", "ix_genealogy_observations_latest")] = (
        "experiment_observations",
        _normalize_sql(GENEALOGY_INDEX_SQL["ix_genealogy_observations_latest"]),
    )
    for name, sql in GENEALOGY_TRIGGER_SQL.items():
        table = "experiments" if name.startswith("experiments_") else "experiment_observations"
        rows[("trigger", name)] = (table, _normalize_sql(sql))
    return rows


EXPECTED_GENEALOGY_SCHEMA_ROWS = _expected_genealogy_schema_rows()


def verify_manifest_files(manifest: Mapping[str, Any]) -> None:
    actual = {
        label: str(row.get("relative_path") or "").replace("\\", "/")
        for label, row in (manifest.get("artifacts") or {}).items()
    }
    if actual != EXPECTED_ARTIFACT_PATHS:
        raise RuntimeError("canonical v6 manifest label-to-path map mismatch")
    root_resolved = ROOT.resolve()
    for label, row in manifest["artifacts"].items():
        path = (ROOT / row["relative_path"]).resolve()
        try:
            path.relative_to(root_resolved)
        except ValueError as exc:
            raise RuntimeError(f"manifest artifact escapes root: {label}") from exc
        if not path.is_file() or file_sha256(path) != row["sha256"]:
            raise RuntimeError(f"manifest artifact missing or hash mismatch: {label}")


def external_trust_anchor(
    contract: Mapping[str, Any], manifest: Mapping[str, Any],
) -> dict[str, Any]:
    normalized = _validate_manifest(manifest, contract)
    return {
        "anchor_schema": "currency_state_after_cost_v6_external_trust_anchor_v1",
        "manifest_relative_path": "config/currency_state_after_cost_counterfactual_v6_manifest.json",
        "manifest_file_sha256": file_sha256(MANIFEST),
        "manifest_id": normalized["manifest_id"],
        "manifest_semantic_sha256": normalized["manifest_sha256"],
        "artifacts": normalized["artifacts"],
        "counterfactual_v6_module_sha256": normalized["artifacts"]["counterfactual_v6_module"]["sha256"],
        "counterfactual_v6_registrar_sha256": normalized["artifacts"]["counterfactual_v6_registrar"]["sha256"],
        "counterfactual_v6_config_sha256": normalized["artifacts"]["counterfactual_v6_config"]["sha256"],
        "counterfactual_v6_cli_sha256": normalized["artifacts"]["counterfactual_v6_cli"]["sha256"],
        "counterfactual_v6_test_sha256": normalized["artifacts"]["counterfactual_v6_test"]["sha256"],
        "counterfactual_v6_proposal_sha256": normalized["artifacts"]["counterfactual_v6_proposal"]["sha256"],
    }


def _verify_genealogy_schema(connection: sqlite3.Connection) -> tuple[bool, str]:
    actual_schema = {
        (str(row[0]), str(row[1])): (str(row[2]), _normalize_sql(row[3]))
        for row in connection.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_schema "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
        )
    }
    if actual_schema != EXPECTED_GENEALOGY_SCHEMA_ROWS:
        return False, "wrong_genealogy_registry_exact_sql_schema"
    for table, expected_info in GENEALOGY_TABLE_INFO.items():
        actual_info = tuple(
            tuple(row) for row in connection.execute(f"PRAGMA table_info({table})")
        )
        if actual_info != expected_info:
            return False, "wrong_genealogy_registry_table_pk_schema"
        expected_indexes = GENEALOGY_INDEX_DEFINITIONS[table]
        actual_indexes = {
            str(row[1]): (bool(row[2]), str(row[3]), bool(row[4]))
            for row in connection.execute(f"PRAGMA index_list({table})")
        }
        if actual_indexes != {
            name: (unique, origin, False)
            for name, (unique, origin, _columns) in expected_indexes.items()
        }:
            return False, "wrong_genealogy_registry_pk_or_unique_index"
        for name, (_unique, _origin, expected_columns) in expected_indexes.items():
            actual_columns = tuple(
                str(row[2]) for row in connection.execute(f"PRAGMA index_info({name})")
            )
            if actual_columns != expected_columns:
                return False, "wrong_genealogy_registry_index_columns"
    return True, "canonical_genealogy_exact_schema_verified"


def _independently_validate_stored_definition(
    stored: Mapping[str, Any], expected: Mapping[str, Any],
) -> tuple[bool, str]:
    try:
        decoded = json.loads(str(stored["definition_json"]))
        expected_decoded = json.loads(str(expected["definition_json"]))
    except (TypeError, ValueError, json.JSONDecodeError):
        return False, "v6_genealogy_definition_json_invalid"
    try:
        if canonical_json(decoded) != stored["definition_json"]:
            return False, "v6_genealogy_definition_json_not_canonical"
        if stable_hash(decoded) != stored["definition_sha256"]:
            return False, "v6_genealogy_definition_sha256_recompute_mismatch"
        if (
            canonical_json(expected_decoded) != expected["definition_json"]
            or stable_hash(expected_decoded) != expected["definition_sha256"]
        ):
            return False, "v6_expected_registrar_definition_self_check_failed"
    except (TypeError, ValueError, OverflowError):
        return False, "v6_genealogy_definition_json_not_canonical"
    if tuple(stored) != EXPERIMENT_COLUMNS or dict(stored) != dict(expected):
        return False, "v6_genealogy_exact_23_column_registrar_definition_mismatch"
    return True, "v6_genealogy_exact_schema_cardinality_and_definition_verified"


def _read_exact_genealogy_definition(
    path: Path, expected: Mapping[str, Any],
) -> tuple[bool, str]:
    """Read-only verifier; run() never accepts or forwards a caller path."""
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        return False, "canonical_append_only_genealogy_database_missing"
    try:
        connection = sqlite3.connect(
            path.resolve().as_uri() + "?mode=ro", uri=True, timeout=30.0,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        try:
            schema_ok, schema_state = _verify_genealogy_schema(connection)
            if not schema_ok:
                return False, schema_state
            cardinality = int(connection.execute(
                "SELECT COUNT(*) FROM experiments WHERE hypothesis_id=?",
                (expected["hypothesis_id"],),
            ).fetchone()[0])
            if cardinality != 1:
                return False, "v6_genealogy_hypothesis_cardinality_not_exactly_one"
            rows = connection.execute(
                f"SELECT {','.join(EXPERIMENT_COLUMNS)} FROM experiments "
                "WHERE hypothesis_id=? ORDER BY rowid",
                (expected["hypothesis_id"],),
            ).fetchall()
        finally:
            connection.close()
    except sqlite3.Error:
        return False, "canonical_append_only_genealogy_read_failed"
    if len(rows) != 1:
        return False, "v6_genealogy_hypothesis_cardinality_not_exactly_one"
    return _independently_validate_stored_definition(dict(rows[0]), expected)


def genealogy_trust_state(
    *, contract: Mapping[str, Any], manifest: Mapping[str, Any],
) -> tuple[bool, str, dict[str, Any], dict[str, Any]]:
    anchor = external_trust_anchor(contract, manifest)
    expected = registrar_definition_v6(contract=contract, external_anchor=anchor)
    pinned = (ROOT / contract["genealogy_contract"]["canonical_relative_path"]).resolve()
    if GENEALOGY.resolve() != pinned:
        return False, "v6_canonical_genealogy_path_constant_mismatch", anchor, expected
    trusted, state = _read_exact_genealogy_definition(GENEALOGY, expected)
    return trusted, state, anchor, expected


def render(snapshot: Mapping[str, Any]) -> str:
    summary = snapshot["summary"]
    lines = [
        "# CurrencyState after-cost counterfactual V6 - current", "",
        f"Generated: `{snapshot['generated_utc']}`", "",
        "**Engineering blocked / zero evidence. Not a producer integration.**", "",
        f"- Contract/cohort: `{snapshot['counterfactual_contract_id']}` / `{snapshot['counterfactual_cohort_id']}`",
        f"- Frozen manifest: `{snapshot['frozen_manifest']['manifest_id']}` / `{snapshot['frozen_manifest']['manifest_sha256']}`",
        f"- Manifest file SHA-256: `{snapshot['frozen_manifest_file_sha256']}`",
        f"- Genealogy trust: `{snapshot['operational_trust_state']}`",
        f"- Identity registry: `{snapshot['identity_registry_state']['state']}`",
        f"- Producer integration: `{snapshot['producer_integration_state']}`",
        f"- Grid: **{summary['row_count']}** rows; economics/ranked/selectable/hold: "
        f"**{summary['economics_admissible_count']} / {summary['ranked_count']} / "
        f"{summary['selectable_count']} / {summary['hold_switch_count']}**",
        f"- Supported execution decision: **{snapshot['supported_execution_decision']}**", "",
        "## Boundaries", "",
        "- V3, V4, and V5 bytes/evidence are preserved and are not re-registered.",
        "- The genealogy path is hard-pinned and cannot be supplied by a caller.",
        "- Trust requires exact canonical SQL, PK/unique indexes, immutable triggers, one matching hypothesis row, all 23 values, canonical JSON, and a recomputed definition hash.",
        "- A future nonempty core fixture must pass order-invariant pre-insert identity admission against the failure-atomic V6 registry.",
        "- The present CLI unconditionally blocks every nonempty operational envelope.",
        "- Every row is research-only `no_trade`.", "",
        "## Blockers", "", "| Blocker | Rows |", "|---|---:|",
    ]
    lines.extend(
        f"| `{name}` | {count} |"
        for name, count in sorted(Counter(summary["blocker_counts"]).items())
    )
    return "\n".join(lines) + "\n"


def run(
    *,
    response_path: Path | None = None,
    context_path: Path = CONTEXT,
    quote_path: Path | None = None,
    verifier_path: Path | None = None,
    economics_path: Path | None = None,
    hold_path: Path | None = None,
    account_path: Path | None = None,
    output_path: Path = OUTPUT,
    report_path: Path = REPORT,
) -> dict[str, Any]:
    contract = json.loads(CONFIG.read_text(encoding="utf-8"))
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    upstream_v5_contract = json.loads(UPSTREAM_V5_CONFIG.read_text(encoding="utf-8"))
    upstream_v4_contract = json.loads(UPSTREAM_V4_CONFIG.read_text(encoding="utf-8"))
    upstream_v4_manifest = json.loads(UPSTREAM_V4_MANIFEST.read_text(encoding="utf-8"))
    upstream_v3_contract = json.loads(UPSTREAM_V3_CONFIG.read_text(encoding="utf-8"))
    upstream_v3_manifest = json.loads(UPSTREAM_V3_MANIFEST.read_text(encoding="utf-8"))
    verify_manifest_files(manifest)
    trusted, trust_state, anchor, expected_definition = genealogy_trust_state(
        contract=contract, manifest=manifest,
    )
    if any(path is not None for path in (
        quote_path, verifier_path, economics_path, hold_path, account_path,
    )):
        raise RuntimeError(
            "nonempty v6 input unconditionally blocked: this engineering cohort "
            "has no operational producer integration"
        )
    if response_path is None:
        context = json.loads(context_path.read_text(encoding="utf-8"))
        response = build_response_timing_arms(
            context,
            state_contract=load_contract(STATE_CONFIG),
            arm_contract=json.loads(RESPONSE_CONFIG.read_text(encoding="utf-8")),
        )
    else:
        response = json.loads(response_path.read_text(encoding="utf-8"))
    snapshot = build_after_cost_counterfactual_v6(
        response,
        contract=contract,
        frozen_manifest=manifest,
        frozen_manifest_file_sha256=anchor["manifest_file_sha256"],
        upstream_v5_contract=upstream_v5_contract,
        upstream_v4_contract=upstream_v4_contract,
        upstream_v4_manifest=upstream_v4_manifest,
        upstream_v3_contract=upstream_v3_contract,
        upstream_v3_manifest=upstream_v3_manifest,
    )
    if any(snapshot["summary"].get(field, 0) for field in (
        "submitted_envelope_count", "submitted_record_count",
        "economics_admissible_count", "ranked_count", "selectable_count",
        "hold_switch_count",
    )):
        raise RuntimeError("v6 zero-input initialization produced evidence")
    snapshot["generated_utc"] = dt.datetime.now(tz=dt.timezone.utc).isoformat()
    snapshot["operational_trust_state"] = trust_state
    snapshot["expected_external_trust_anchor"] = anchor
    snapshot["expected_registrar_definition"] = expected_definition
    snapshot["canonical_genealogy_relative_path"] = contract["genealogy_contract"]["canonical_relative_path"]
    snapshot["canonical_identity_registry_relative_path"] = contract["identity_registry_contract"]["canonical_relative_path"]
    snapshot["producer_integration_state"] = "producer_integration_missing"
    snapshot["registration_status"] = "engineering_blocked_zero_evidence"
    snapshot["research_only"] = True
    snapshot["execution_eligible"] = False
    snapshot["can_place_orders"] = False
    if trusted:
        snapshot["registration_status"] = "engineering_registered_zero_evidence"
    atomic_text(output_path, json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    atomic_text(report_path, render(snapshot))
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("response", "quotes", "verifier", "economics", "hold", "account"):
        parser.add_argument(f"--{name}", type=Path)
    parser.add_argument("--context", type=Path, default=CONTEXT)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--report", type=Path, default=REPORT)
    args = parser.parse_args()
    snapshot = run(
        response_path=args.response,
        context_path=args.context,
        quote_path=args.quotes,
        verifier_path=args.verifier,
        economics_path=args.economics,
        hold_path=args.hold,
        account_path=args.account,
        output_path=args.output,
        report_path=args.report,
    )
    print(json.dumps({
        "snapshot_id": snapshot["snapshot_id"],
        "cohort_id": snapshot["counterfactual_cohort_id"],
        "operational_trust_state": snapshot["operational_trust_state"],
        "producer_integration_state": snapshot["producer_integration_state"],
        **{key: snapshot["summary"][key] for key in (
            "row_count", "economics_admissible_count", "ranked_count",
            "selectable_count", "hold_switch_count",
        )},
        "supported_execution_decision": snapshot["supported_execution_decision"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
