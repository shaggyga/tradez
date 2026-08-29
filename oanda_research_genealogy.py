#!/usr/bin/env python3
"""Materialize an append-only genealogy of governed FX experiments.

This registry references the existing lifecycle/cohort databases rather than
replacing them.  Definitions never change; later results are appended as
observations.  The module is research-only and has no broker imports.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
REPORTS = ROOT / "data" / "oanda_training_manager" / "reports" / "research_genealogy"
DEFAULT_DATABASE = STATE / "research_genealogy_v1.sqlite"
DEFAULT_STATE = STATE / "research_genealogy_v1.json"
DEFAULT_REPORT = REPORTS / "RESEARCH_GENEALOGY_CURRENT.md"


# This is intentionally duplicated from the frozen v4 engine instead of being
# imported from it.  The genealogy registrar is an independent trust boundary:
# changing the engine's own idea of its manifest cannot make an altered path
# acceptable to the append-only registry.
AFTER_COST_V4_MANIFEST_RELATIVE_PATH = (
    "config/currency_state_after_cost_counterfactual_v4_manifest.json"
)
AFTER_COST_V4_FROZEN_MANIFEST_FILE_SHA256 = (
    "3a2fe88ba8e330c92082324de307db075ce63b69b7c7b7e08a1e7dfca464b906"
)
AFTER_COST_V4_FROZEN_MANIFEST_SEMANTIC_SHA256 = (
    "e76780792f8e56960df668dd2fe032bce284481deb8a1ad45dbf1e74b97bef49"
)
AFTER_COST_V4_FROZEN_MODULE_SHA256 = (
    "98010415f01fb49d82d1be1bcc873e8070a46627bc5c43ae0198df4e2c496f5e"
)
AFTER_COST_V4_FROZEN_CONFIG_SHA256 = (
    "72c7cf64e2eaa5510e981239801393a1bb512cdb1feb1ae3c4ae2daa1745a03d"
)
AFTER_COST_V4_FROZEN_CLI_SHA256 = (
    "971a8d94502d5f23b7c43b299c491b3c295f63fe5583265fc569dcee7d9de825"
)
AFTER_COST_V4_EXPECTED_ARTIFACT_PATHS = {
    "counterfactual_v4_module": "src/forex_system/research/currency_state_after_cost_counterfactual_v4.py",
    "counterfactual_v4_config": "config/currency_state_after_cost_counterfactual_v4.json",
    "counterfactual_v4_cli": "oanda_currency_state_after_cost_counterfactual_v4.py",
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


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    _replace_with_retry(temporary, path)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    _replace_with_retry(temporary, path)


def _replace_with_retry(temporary: Path, path: Path) -> None:
    for attempt in range(8):
        try:
            os.replace(temporary, path)
            return
        except PermissionError:
            if attempt == 7:
                temporary.unlink(missing_ok=True)
                raise
            time.sleep(min(0.4, 0.025 * (2**attempt)))


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS experiments (
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
        );
        CREATE TABLE IF NOT EXISTS experiment_observations (
            observation_id TEXT PRIMARY KEY,
            hypothesis_id TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            source_system TEXT NOT NULL,
            result TEXT NOT NULL,
            retirement_reason TEXT,
            retired_at TEXT,
            evidence_sha256 TEXT NOT NULL,
            evidence_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_genealogy_observations_latest
            ON experiment_observations(hypothesis_id,observed_at DESC);
        CREATE TABLE IF NOT EXISTS genealogy_imports (
            import_id TEXT PRIMARY KEY,
            imported_at TEXT NOT NULL,
            source_database TEXT NOT NULL,
            source_fingerprint TEXT NOT NULL,
            definition_count INTEGER NOT NULL,
            observation_count INTEGER NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS experiments_no_update
            BEFORE UPDATE ON experiments BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS experiments_no_delete
            BEFORE DELETE ON experiments BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS experiment_observations_no_update
            BEFORE UPDATE ON experiment_observations BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS experiment_observations_no_delete
            BEFORE DELETE ON experiment_observations BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    connection.commit()
    return connection


EXPERIMENT_COLUMNS = (
    "hypothesis_id", "parent_hypothesis_id", "experiment_kind",
    "research_generation", "idea_origin", "pre_registered",
    "data_sources_json", "feature_contract_json", "label_contract_json",
    "model_contract_json", "cost_contract_json", "allocator_contract_json",
    "training_period_json", "selection_period_json", "confirmation_period_json",
    "all_parameters_tried_json", "selection_rule", "holdouts_touched_json",
    "source_code_hash", "data_snapshot_hash", "definition_sha256", "created_at",
    "definition_json",
)


def insert_experiment(connection: sqlite3.Connection, value: dict[str, Any]) -> bool:
    existing = connection.execute(
        "SELECT definition_sha256 FROM experiments WHERE hypothesis_id=?",
        (value.get("hypothesis_id"),),
    ).fetchone()
    if existing is not None:
        if str(existing[0]) != str(value.get("definition_sha256")):
            raise ValueError("hypothesis_id definition conflict")
        return False
    before = connection.total_changes
    connection.execute(
        f"INSERT OR IGNORE INTO experiments ({','.join(EXPERIMENT_COLUMNS)}) VALUES ({','.join('?' for _ in EXPERIMENT_COLUMNS)})",
        tuple(value.get(column) for column in EXPERIMENT_COLUMNS),
    )
    return connection.total_changes > before


def observe(
    connection: sqlite3.Connection, *, hypothesis_id: str, observed_at: str,
    source_system: str, result: str, evidence: dict[str, Any],
    retirement_reason: str | None = None, retired_at: str | None = None,
) -> bool:
    evidence_sha = stable_hash(evidence)
    observation_id = "genealogy_observation_" + stable_hash(
        (hypothesis_id, observed_at, source_system, result, evidence_sha)
    )[:28]
    before = connection.total_changes
    connection.execute(
        "INSERT OR IGNORE INTO experiment_observations VALUES (?,?,?,?,?,?,?,?,?)",
        (
            observation_id, hypothesis_id, observed_at, source_system, result,
            retirement_reason, retired_at, evidence_sha, canonical_json(evidence),
        ),
    )
    return connection.total_changes > before


def ro(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=10.0
    )
    connection.row_factory = sqlite3.Row
    return connection


def cohort_parent_map(connection: sqlite3.Connection, table: str) -> dict[str, str | None]:
    output: dict[str, str | None] = {}
    for row in connection.execute(
        f"SELECT previous_cohort_id,next_cohort_id FROM {table} ORDER BY observed_utc"
    ):
        output[str(row["next_cohort_id"])] = (
            str(row["previous_cohort_id"]) if row["previous_cohort_id"] else None
        )
    return output


def import_proof_registry(
    target: sqlite3.Connection, source: Path, *, kind: str, imported_at: str
) -> tuple[int, int]:
    if not source.is_file():
        return 0, 0
    connection = ro(source)
    definitions = observations = 0
    try:
        parents = cohort_parent_map(connection, "proof_cohort_transitions")
        for row in connection.execute("SELECT * FROM proof_cohorts ORDER BY cohort_start_utc"):
            contract = json.loads(str(row["contract_json"] or "{}"))
            cohort_id = str(row["cohort_id"])
            definition = {
                "hypothesis_id": cohort_id,
                "parent_hypothesis_id": parents.get(cohort_id),
                "experiment_kind": kind,
                "research_generation": "prospective_proof_generation_20260806",
                "idea_origin": str(row["family"]),
                "pre_registered": 1,
                "data_sources_json": canonical_json(["OANDA_executable_bid_ask", "OANDA_completed_candles"]),
                "feature_contract_json": canonical_json({"feature_schema_version": row["feature_schema_version"]}),
                "label_contract_json": canonical_json({"forecast_contract_version": row["forecast_contract_version"]}),
                "model_contract_json": canonical_json(contract.get("model_specification") or contract.get("hyperparameters") or {}),
                "cost_contract_json": canonical_json({"cost_model_version": row["cost_model_version"]}),
                "allocator_contract_json": "{}",
                "training_period_json": canonical_json({"cutoff": row["training_cutoff_utc"]}),
                "selection_period_json": canonical_json({"starts": row["cohort_start_utc"]}),
                "confirmation_period_json": canonical_json({"untouched_later_cohort_required": True}),
                "all_parameters_tried_json": canonical_json(contract.get("hyperparameters") or {}),
                "selection_rule": "frozen_material_contract; no interim retuning",
                "holdouts_touched_json": canonical_json([]),
                "source_code_hash": str(row["source_sha256"]),
                "data_snapshot_hash": str(row["training_dataset_sha256"]),
                "definition_sha256": str(row["material_contract_sha256"]),
                "created_at": str(row["cohort_start_utc"]),
                "definition_json": canonical_json(contract),
            }
            definitions += int(insert_experiment(target, definition))
            observations += int(observe(
                target, hypothesis_id=cohort_id, observed_at=str(row["cohort_start_utc"]),
                source_system=source.name, result="continue_collecting",
                evidence={"family": row["family"], "cohort_id": cohort_id},
            ))
    finally:
        connection.close()
    return definitions, observations


def import_allocator(
    target: sqlite3.Connection, source: Path, *, imported_at: str
) -> tuple[int, int]:
    if not source.is_file():
        return 0, 0
    connection = ro(source)
    definitions = observations = 0
    try:
        parents = cohort_parent_map(connection, "allocator_cohort_transitions")
        latest = {
            str(row["cohort_id"]): dict(row)
            for row in connection.execute(
                """SELECT event.cohort_id,event.next_state,event.observed_utc FROM allocator_lifecycle_events event
                   JOIN (SELECT cohort_id,MAX(rowid) rowid FROM allocator_lifecycle_events GROUP BY cohort_id) last
                     ON last.rowid=event.rowid"""
            )
        }
        for row in connection.execute("SELECT * FROM allocator_cohorts ORDER BY cohort_start_utc"):
            contract = json.loads(str(row["contract_json"] or "{}"))
            cohort_id = str(row["cohort_id"])
            definition = {
                "hypothesis_id": cohort_id, "parent_hypothesis_id": parents.get(cohort_id),
                "experiment_kind": "allocator_policy", "research_generation": "allocator_proof_generation_20260806",
                "idea_origin": str(row["policy_id"]), "pre_registered": 1,
                "data_sources_json": canonical_json(["governed_candidate_set", "OANDA_executable_bid_ask"]),
                "feature_contract_json": "{}", "label_contract_json": canonical_json({"policy_outcome": "executable_bid_ask_at_declared_horizon"}),
                "model_contract_json": "{}", "cost_contract_json": canonical_json(contract.get("cost_contract") or {}),
                "allocator_contract_json": canonical_json(contract), "training_period_json": "{}",
                "selection_period_json": canonical_json({"starts": row["cohort_start_utc"]}),
                "confirmation_period_json": canonical_json({"later_untouched_confirmation_required": True}),
                "all_parameters_tried_json": canonical_json(contract),
                "selection_rule": "frozen top-one allocator with six comparator arms",
                "holdouts_touched_json": canonical_json([]), "source_code_hash": str(row["source_sha256"]),
                "data_snapshot_hash": "prospective_decision_stream", "definition_sha256": str(row["material_contract_sha256"]),
                "created_at": str(row["cohort_start_utc"]), "definition_json": canonical_json(contract),
            }
            definitions += int(insert_experiment(target, definition))
            current = latest.get(cohort_id)
            observations += int(observe(
                target, hypothesis_id=cohort_id,
                observed_at=(str(current["observed_utc"]) if current else str(row["cohort_start_utc"])),
                source_system=source.name,
                result=(str(current["next_state"]) if current else "continue_collecting"),
                evidence={"phase": row["phase"], "cohort_id": cohort_id},
            ))
    finally:
        connection.close()
    return definitions, observations


def import_lifecycle(
    target: sqlite3.Connection, source: Path, *, imported_at: str
) -> tuple[int, int]:
    if not source.is_file():
        return 0, 0
    connection = ro(source)
    definitions = observations = 0
    try:
        # The lifecycle contains tens of thousands of governed cells.  The
        # former importer issued one target lookup per definition and one
        # duplicate insert per current observation on every pass.  On the live
        # Windows database that exceeded the supervision window, so genealogy
        # never caught up.  Load the immutable append-only keys once and only
        # touch rows that are actually new or changed.
        existing_definitions = {
            str(row[0]): str(row[1])
            for row in target.execute(
                "SELECT hypothesis_id,definition_sha256 FROM experiments "
                "WHERE experiment_kind='governed_cell'"
            )
        }
        existing_observations = {
            (str(row[0]), str(row[1]), str(row[2]), str(row[3]))
            for row in target.execute(
                "SELECT hypothesis_id,observed_at,result,evidence_sha256 "
                "FROM experiment_observations WHERE source_system=?",
                (source.name,),
            )
        }
        retirement = {
            str(row["hypothesis_id"]): dict(row)
            for row in connection.execute("SELECT * FROM futility_retirements")
        }
        latest = {
            str(row["hypothesis_id"]): dict(row)
            for row in connection.execute(
                """SELECT event.* FROM lifecycle_events event
                   JOIN (SELECT hypothesis_id,MAX(rowid) rowid FROM lifecycle_events GROUP BY hypothesis_id) last
                     ON last.rowid=event.rowid"""
            )
        }
        for row in connection.execute("SELECT * FROM hypotheses ORDER BY first_seen_utc"):
            hypothesis_id = str(row["hypothesis_id"])
            definition_sha256 = str(row["definition_sha256"])
            definition_json = str(row["definition_json"] or "{}")
            existing_sha = existing_definitions.get(hypothesis_id)
            if existing_sha is not None and existing_sha != definition_sha256:
                raise ValueError("hypothesis_id definition conflict")
            if existing_sha is None:
                definition = {
                    "hypothesis_id": hypothesis_id, "parent_hypothesis_id": None,
                    "experiment_kind": "governed_cell", "research_generation": str(row["evidence_contract_id"]),
                    "idea_origin": str(row["family"]), "pre_registered": int(bool(row["cohort_id"])),
                    "data_sources_json": canonical_json(["canonical_forecast_and_outcome_ledger"]),
                    "feature_contract_json": canonical_json({"cell_id": row["cell_id"]}),
                    "label_contract_json": canonical_json({"horizon_sec": row["horizon_sec"]}),
                    "model_contract_json": canonical_json({"family": row["family"], "cohort_id": row["cohort_id"]}),
                    "cost_contract_json": canonical_json({"liquidity_bucket": row["liquidity_bucket"]}),
                    "allocator_contract_json": "{}", "training_period_json": "{}",
                    "selection_period_json": canonical_json({"first_seen": row["first_seen_utc"]}),
                    "confirmation_period_json": canonical_json({"untouched_confirmation_required": True}),
                    "all_parameters_tried_json": canonical_json({"historical_parameter_inventory": "not_reconstructed"}),
                    "selection_rule": "family x pair x horizon x session x liquidity",
                    "holdouts_touched_json": canonical_json([]), "source_code_hash": "referenced_by_evidence_contract",
                    "data_snapshot_hash": definition_sha256, "definition_sha256": definition_sha256,
                    "created_at": str(row["first_seen_utc"]), "definition_json": definition_json,
                }
                definitions += int(insert_experiment(target, definition))
                existing_definitions[hypothesis_id] = definition_sha256
            current = latest.get(hypothesis_id)
            if current:
                retired = retirement.get(hypothesis_id)
                evidence = json.loads(str(current.get("evidence_json") or "{}"))
                observation_key = (
                    hypothesis_id,
                    str(current["observed_utc"]),
                    str(current["next_state"]),
                    stable_hash(evidence),
                )
                if observation_key not in existing_observations:
                    observations += int(observe(
                        target, hypothesis_id=hypothesis_id,
                        observed_at=str(current["observed_utc"]), source_system=source.name,
                        result=str(current["next_state"]), evidence=evidence,
                        retirement_reason=(str(retired["boundary_method"]) if retired else None),
                        retired_at=(str(retired["retired_utc"]) if retired else None),
                    ))
                    existing_observations.add(observation_key)
    finally:
        connection.close()
    return definitions, observations


def import_external_discovery_report(
    target: sqlite3.Connection,
    source: Path,
    *,
    source_family: str,
) -> tuple[int, int]:
    """Register each fixed rule/horizon from a historical discovery report.

    These reports can reject or nominate a new prospective hypothesis, but
    they can never create lifecycle confirmation or execution eligibility.
    The immutable source/candle fingerprint is part of the hypothesis ID so a
    materially different replay cannot overwrite this experiment.
    """
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    summaries = payload.get("summaries") or []
    if not isinstance(summaries, list):
        return 0, 0
    source_hashes = payload.get("source_hashes") or {}
    candle_hashes = payload.get("candle_hashes") or {}
    data_snapshot_hash = stable_hash(
        {"source_hashes": source_hashes, "candle_hashes": candle_hashes}
    )
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for raw in summaries:
        if not isinstance(raw, dict):
            continue
        rule = str(raw.get("rule") or "")
        try:
            horizon = int(raw.get("horizon_trading_days"))
        except (TypeError, ValueError):
            continue
        if rule:
            grouped.setdefault((rule, horizon), []).append(dict(raw))
    definitions = observations = 0
    observed_at = str(payload.get("generated_utc") or utc_now())
    for (rule, horizon), evidence_rows in sorted(grouped.items()):
        selected = any(bool(row.get("discovery_candidate")) for row in evidence_rows)
        hypothesis_id = (
            f"{source_family}.{rule}.h{horizon}d.discovery."
            f"{data_snapshot_hash[:16]}"
        )
        definition_contract = {
            "source_family": source_family,
            "rule": rule,
            "horizon_trading_days": horizon,
            "availability_contract": payload.get("availability_contract"),
            "evidence_class": payload.get("evidence_class"),
            "proof_eligible": False,
            "execution_eligible": False,
        }
        definition_sha = stable_hash(definition_contract)
        definition = {
            "hypothesis_id": hypothesis_id,
            "parent_hypothesis_id": None,
            "experiment_kind": "external_source_historical_discovery",
            "research_generation": "orthogonal_source_discovery_20260808",
            "idea_origin": source_family,
            "pre_registered": 1,
            "data_sources_json": canonical_json(
                [source_family, "OANDA_practice_GET_only_daily_bid_ask"]
            ),
            "feature_contract_json": canonical_json(
                {"rule": rule, "availability_contract": payload.get("availability_contract")}
            ),
            "label_contract_json": canonical_json(
                {"horizon_trading_days": horizon, "target": "executable_after_cost_return"}
            ),
            "model_contract_json": canonical_json({"transparent_rule": rule}),
            "cost_contract_json": canonical_json(
                {"entry": "executable_bid_or_ask", "exit": "executable_bid_or_ask"}
            ),
            "allocator_contract_json": "{}",
            "training_period_json": "{}",
            "selection_period_json": canonical_json(payload.get("date_range") or {}),
            "confirmation_period_json": canonical_json(
                {"required": "new_immutable_prospective_source_cohort"}
            ),
            "all_parameters_tried_json": canonical_json(
                sorted({
                    (str(row.get("rule")), int(row.get("horizon_trading_days")))
                    for row in summaries
                    if isinstance(row, dict) and row.get("rule") and row.get("horizon_trading_days")
                })
            ),
            "selection_rule": "split, multiplicity, concentration, and cross-period stability gates",
            "holdouts_touched_json": canonical_json(["retrospective_holdout"]),
            "source_code_hash": "recorded_by_dated_discovery_artifact",
            "data_snapshot_hash": data_snapshot_hash,
            "definition_sha256": definition_sha,
            "created_at": observed_at,
            "definition_json": canonical_json(definition_contract),
        }
        definitions += int(insert_experiment(target, definition))
        result = (
            "historical_discovery_candidate_requires_prospective_cohort"
            if selected
            else "historical_discovery_rejected"
        )
        observations += int(observe(
            target,
            hypothesis_id=hypothesis_id,
            observed_at=observed_at,
            source_system=source.name,
            result=result,
            retirement_reason=(
                None if selected else "failed_split_multiplicity_concentration_or_cross_period_gate"
            ),
            retired_at=(None if selected else observed_at),
            evidence={
                "report_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                "evidence_rows": evidence_rows,
                "limitations": payload.get("limitations") or [],
                "supported_action": payload.get("supported_action"),
            },
        ))
    return definitions, observations


def import_internal_discovery_artifact(
    target: sqlite3.Connection,
    source: Path,
    *,
    research_id_override: str | None = None,
) -> tuple[int, int]:
    """Register a bounded internal archive discovery without implying proof."""
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    research_id = str(payload.get("research_id") or research_id_override or "")
    if not research_id:
        return 0, 0
    snapshot_hash = stable_hash({
        "config_sha256": payload.get("config_sha256") or (payload.get("contracts") or {}).get("config_sha256"),
        "source_manifest_sha256": payload.get("source_manifest_sha256") or (payload.get("contracts") or {}).get("price_manifest_sha256"),
        "source_event_highwater_utc": payload.get("source_event_highwater_utc"),
        "contract_id": payload.get("contract_id"),
    })
    hypothesis_id = f"{research_id}.archive_discovery.{snapshot_hash[:16]}"
    definition_contract = {
        "research_id": research_id,
        "evidence_class": payload.get("evidence_class"),
        "research_only": True,
        "execution_eligible": False,
        "source": source.name,
    }
    observed_at = str(payload.get("generated_utc") or utc_now())
    definition = {
        "hypothesis_id": hypothesis_id,
        "parent_hypothesis_id": None,
        "experiment_kind": "internal_archive_discovery",
        "research_generation": "proof_first_internal_discovery_20260809",
        "idea_origin": research_id,
        "pre_registered": 1,
        "data_sources_json": canonical_json(["OANDA_practice_BAM_bid_ask", "point_in_time_source_governance"]),
        "feature_contract_json": canonical_json({
            "price_features": payload.get("features") or payload.get("price_features") or [],
            "source_features": payload.get("source_features") or [],
        }),
        "label_contract_json": canonical_json({"target": "executable_after_cost_cost_clearance_or_movement_episode"}),
        "model_contract_json": canonical_json({"research_id": research_id}),
        "cost_contract_json": canonical_json({"entry_exit": "recorded_bid_ask", "proof_eligible": False}),
        "allocator_contract_json": canonical_json({"can_place_orders": False}),
        "training_period_json": canonical_json({"archive_discovery": True}),
        "selection_period_json": canonical_json({"archive_already_inspected": True}),
        "confirmation_period_json": canonical_json({"required": "new_untouched_cohort"}),
        "all_parameters_tried_json": "{}",
        "selection_rule": "predeclared archive diagnostics with candidate lock fail closed",
        "holdouts_touched_json": canonical_json(["retrospective_holdout"]),
        "source_code_hash": str(payload.get("source_code_sha256") or "recorded_in_artifact_contract"),
        "data_snapshot_hash": snapshot_hash,
        "definition_sha256": stable_hash(definition_contract),
        "created_at": observed_at,
        "definition_json": canonical_json(definition_contract),
    }
    definitions = int(insert_experiment(target, definition))
    result = "archive_discovery_recorded_no_proof_candidate"
    observations = int(observe(
        target,
        hypothesis_id=hypothesis_id,
        observed_at=observed_at,
        source_system=source.name,
        result=result,
        retirement_reason=None,
        retired_at=None,
        evidence={
            "proof_eligible": False,
            "can_place_orders": False,
            "results": payload.get("results") or payload.get("horizons") or [],
            "matched_control_comparison": payload.get("matched_control_comparison") or [],
            "technical_confirmation_by_horizon": payload.get("technical_confirmation_by_horizon") or {},
            "by_horizon": payload.get("by_horizon") or {},
        },
    ))
    return definitions, observations


def import_macro_point_in_time_validation(
    target: sqlite3.Connection,
    source: Path,
) -> tuple[int, int]:
    """Register historical initial-release checks without mutating live cohorts."""
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    observed_at = str(payload.get("generated_utc") or utc_now())
    definitions = observations = 0
    for raw in payload.get("candidates") or []:
        if not isinstance(raw, dict):
            continue
        candidate = raw.get("candidate") or {}
        candidate_id = str(candidate.get("candidate_id") or "")
        if not candidate_id:
            continue
        hypothesis_id = f"macro_point_in_time.{candidate_id}.robustness.20260816"
        contract = {
            "candidate": candidate,
            "availability_contract": "ALFRED_output_type_4_initial_release_next_day_clock",
            "historical_results_can_confirm": False,
            "execution_eligible": False,
        }
        evidence = {
            "robustness_state": raw.get("robustness_state"),
            "current_view_counterfactual": raw.get("current_view_counterfactual") or {},
            "initial_release_point_in_time": raw.get("initial_release_point_in_time") or {},
            "point_in_time_technical_aligned": raw.get("point_in_time_technical_aligned") or {},
            "prospective_cohorts_remain_unchanged": payload.get("prospective_cohorts_remain_unchanged"),
            "conclusion": payload.get("conclusion"),
        }
        definition = {
            "hypothesis_id": hypothesis_id,
            "parent_hypothesis_id": None,
            "experiment_kind": "historical_point_in_time_robustness",
            "research_generation": "macro_initial_release_validation_20260816",
            "idea_origin": str(candidate.get("signal_rule") or candidate_id),
            "pre_registered": 1,
            "data_sources_json": canonical_json([
                "FRED_ALFRED_initial_release_output_type_4",
                "OANDA_practice_GET_only_completed_bid_ask",
            ]),
            "feature_contract_json": canonical_json(candidate),
            "label_contract_json": canonical_json({
                "target": "executable_after_cost_return",
                "horizon_hours": candidate.get("horizon_hours"),
            }),
            "model_contract_json": canonical_json({"transparent_rule": candidate.get("signal_rule")}),
            "cost_contract_json": canonical_json({"entry_exit": "executable_bid_ask"}),
            "allocator_contract_json": canonical_json({"execution_eligible": False}),
            "training_period_json": "{}",
            "selection_period_json": canonical_json({"fixed_before_point_in_time_replay": True}),
            "confirmation_period_json": canonical_json({"required": "later_untouched_prospective_releases"}),
            "all_parameters_tried_json": canonical_json({
                "threshold": candidate.get("threshold"),
                "liquidity_bucket": candidate.get("liquidity_bucket"),
            }),
            "selection_rule": "fixed current-view candidate replayed on initial-release vintages",
            "holdouts_touched_json": canonical_json(["historical_initial_release_replay"]),
            "source_code_hash": "recorded_by_macro_point_in_time_validation_artifact",
            "data_snapshot_hash": stable_hash(evidence),
            "definition_sha256": stable_hash(contract),
            "created_at": observed_at,
            "definition_json": canonical_json(contract),
        }
        definitions += int(insert_experiment(target, definition))
        result = str(raw.get("robustness_state") or "unavailable")
        observations += int(observe(
            target,
            hypothesis_id=hypothesis_id,
            observed_at=observed_at,
            source_system=source.name,
            result=result,
            retirement_reason=(
                "historical_current_view_candidate_failed_initial_release_replay"
                if result.startswith("failed_") else None
            ),
            retired_at=(observed_at if result.startswith("failed_") else None),
            evidence=evidence,
        ))
    return definitions, observations


def import_zero_output_adapter_retirement(target: sqlite3.Connection) -> tuple[int, int]:
    """Formally retire the disabled intrasecond adapter without statistical claims."""
    hypothesis_id = "intrasecond_ridge.zero_valid_outputs.engineering_retired.20260809"
    contract = {
        "family": "intrasecond_ridge",
        "adapter": "second_ridge",
        "expected_runtime_output": False,
        "account_eligible": False,
        "reason": "zero valid outputs; disabled timing-only adapter",
        "reconsideration": "new validated input and output contract requires a new hypothesis ID",
    }
    definition = {
        "hypothesis_id": hypothesis_id,
        "parent_hypothesis_id": None,
        "experiment_kind": "adapter_retirement",
        "research_generation": "source_audit_closure_20260809",
        "idea_origin": "intrasecond_ridge",
        "pre_registered": 0,
        "data_sources_json": canonical_json(["quote_intensity"]),
        "feature_contract_json": "{}",
        "label_contract_json": "{}",
        "model_contract_json": canonical_json(contract),
        "cost_contract_json": "{}",
        "allocator_contract_json": canonical_json({"account_eligible": False}),
        "training_period_json": "{}",
        "selection_period_json": "{}",
        "confirmation_period_json": "{}",
        "all_parameters_tried_json": "{}",
        "selection_rule": "engineering retirement after zero valid outputs",
        "holdouts_touched_json": "[]",
        "source_code_hash": "registered_runtime_inventory",
        "data_snapshot_hash": stable_hash(contract),
        "definition_sha256": stable_hash(contract),
        "created_at": "2026-08-09T00:00:00+00:00",
        "definition_json": canonical_json(contract),
    }
    definitions = int(insert_experiment(target, definition))
    observations = int(observe(
        target,
        hypothesis_id=hypothesis_id,
        observed_at="2026-08-09T00:00:00+00:00",
        source_system="source_audit_closure",
        result="engineering_retired_zero_valid_outputs",
        retirement_reason="zero valid outputs and no active structural-entry role",
        retired_at="2026-08-09T00:00:00+00:00",
        evidence=contract,
    ))
    return definitions, observations


def import_gdelt_mapping_report(
    target: sqlite3.Connection,
    source: Path,
) -> tuple[int, int]:
    """Register the dated GDELT direction nulls and magnitude discovery.

    Direction and magnitude are deliberately separate hypotheses.  A large
    post-news move does not imply that generic article tone predicts its side.
    The dated replay can only retire/nominate discovery hypotheses; it cannot
    confirm or authorize anything.
    """
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    report_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    data_snapshot_hash = stable_hash({
        "report_sha256": report_sha,
        "candle_hashes": payload.get("candle_hashes") or {},
        "source_ids": payload.get("source_ids") or [],
    })
    observed_at = str(payload.get("generated_utc") or utc_now())
    definitions = observations = 0
    rows: list[tuple[str, str, int, dict[str, Any], bool]] = []
    for raw in payload.get("arm_summaries") or []:
        if isinstance(raw, dict) and raw.get("arm") and raw.get("horizon_minutes"):
            rows.append((
                "direction", str(raw["arm"]), int(raw["horizon_minutes"]),
                dict(raw), bool(raw.get("confirmation_eligible")),
            ))
    for raw in payload.get("attention_magnitude_summaries") or []:
        if isinstance(raw, dict) and raw.get("horizon_minutes"):
            # This was the one transparent association strong enough to earn
            # a prospective cohort, but it abstains from predicting direction.
            rows.append((
                "magnitude", "high_attention_absolute_move",
                int(raw["horizon_minutes"]), dict(raw),
                bool(raw.get("confirmation_eligible")),
            ))
    for target_kind, rule, horizon, evidence_row, eligible in rows:
        hypothesis_id = (
            f"gdelt_attention.{target_kind}.{rule}.h{horizon}m.discovery."
            f"{data_snapshot_hash[:16]}"
        )
        contract = {
            "source_family": "gdelt_attention",
            "target_kind": target_kind,
            "rule": rule,
            "horizon_minutes": horizon,
            "story_independence": "source_event_lineage_x_currency_hour",
            "proof_eligible": False,
            "execution_eligible": False,
        }
        definition = {
            "hypothesis_id": hypothesis_id,
            "parent_hypothesis_id": None,
            "experiment_kind": "external_source_historical_discovery",
            "research_generation": "orthogonal_source_discovery_20260808",
            "idea_origin": "gdelt_attention",
            "pre_registered": 1,
            "data_sources_json": canonical_json(
                ["GDELT_discovery", "OANDA_practice_GET_only_M15_bid_ask"]
            ),
            "feature_contract_json": canonical_json(
                {"rule": rule, "target_kind": target_kind, "minimum_story_count": 3}
            ),
            "label_contract_json": canonical_json({
                "horizon_minutes": horizon,
                "target": (
                    "absolute_executable_move" if target_kind == "magnitude"
                    else "executable_after_cost_direction_return"
                ),
            }),
            "model_contract_json": canonical_json({"transparent_rule": rule}),
            "cost_contract_json": canonical_json(
                {"entry": "executable_bid_or_ask", "exit": "executable_bid_or_ask"}
            ),
            "allocator_contract_json": "{}",
            "training_period_json": "{}",
            "selection_period_json": canonical_json(
                {"market_days": payload.get("market_days")}
            ),
            "confirmation_period_json": canonical_json(
                {"required": "new_immutable_prospective_source_cohort"}
            ),
            "all_parameters_tried_json": canonical_json({
                "direction_arms": sorted({
                    str(item.get("arm")) for item in payload.get("arm_summaries") or []
                    if isinstance(item, dict) and item.get("arm")
                }),
                "horizons_minutes": sorted({
                    int(item.get("horizon_minutes"))
                    for item in (payload.get("arm_summaries") or [])
                    if isinstance(item, dict) and item.get("horizon_minutes")
                }),
            }),
            "selection_rule": "day independence, BH multiplicity, and best-day exclusion",
            "holdouts_touched_json": canonical_json(["retained_historical_window"]),
            "source_code_hash": "recorded_by_dated_discovery_artifact",
            "data_snapshot_hash": data_snapshot_hash,
            "definition_sha256": stable_hash(contract),
            "created_at": observed_at,
            "definition_json": canonical_json(contract),
        }
        definitions += int(insert_experiment(target, definition))
        if target_kind == "magnitude" and not eligible:
            result = "historical_magnitude_discovery_requires_prospective_cohort"
            retirement_reason = retired_at = None
        elif eligible:
            result = "historical_discovery_candidate_requires_prospective_cohort"
            retirement_reason = retired_at = None
        else:
            result = "historical_discovery_rejected"
            retirement_reason = "failed_multiplicity_or_concentration_gate"
            retired_at = observed_at
        observations += int(observe(
            target,
            hypothesis_id=hypothesis_id,
            observed_at=observed_at,
            source_system=source.name,
            result=result,
            retirement_reason=retirement_reason,
            retired_at=retired_at,
            evidence={
                "report_sha256": report_sha,
                "evidence_row": evidence_row,
                "mapping_quality": payload.get("mapping_quality") or {},
                "limitations": payload.get("limitations") or [],
            },
        ))
    return definitions, observations


def import_prospective_source_state(
    target: sqlite3.Connection,
    source: Path,
) -> tuple[int, int]:
    """Register a source-specific prospective collector without promoting it."""
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    cohort = payload.get("cohort") or {}
    cohort_id = str(cohort.get("cohort_id") or "")
    if not cohort_id:
        return 0, 0
    contract = {
        "cohort_id": cohort_id,
        "config_sha256": cohort.get("config_sha256"),
        "collector_sha256": cohort.get("collector_sha256"),
        "direction_policy": cohort.get("direction_policy"),
        "forecasts_written_before_outcomes": cohort.get("forecasts_written_before_outcomes"),
        "late_backfill_refused": cohort.get("late_backfill_refused"),
        "execution_eligible": False,
        "supersedes_cohort_id": cohort.get("supersedes_cohort_id"),
    }
    supersedes = str(cohort.get("supersedes_cohort_id") or "")
    definition = {
        "hypothesis_id": cohort_id,
        "parent_hypothesis_id": supersedes or None,
        "experiment_kind": "external_source_prospective_collection",
        "research_generation": "orthogonal_source_prospective_20260808",
        "idea_origin": "gdelt_attention_magnitude",
        "pre_registered": 1,
        "data_sources_json": canonical_json(
            ["GDELT_point_in_time_story_lineage", "OANDA_executable_bid_ask"]
        ),
        "feature_contract_json": canonical_json(
            {"config_sha256": cohort.get("config_sha256"), "direction_policy": "abstain"}
        ),
        "label_contract_json": canonical_json(
            {"target": "absolute_move_and_cost_clearance", "exact_horizon": True}
        ),
        "model_contract_json": canonical_json(
            {"collector_sha256": cohort.get("collector_sha256")}
        ),
        "cost_contract_json": canonical_json(
            {"entry_spread_recorded": True, "best_direction_is_diagnostic_only": True}
        ),
        "allocator_contract_json": canonical_json({"direction_policy": "abstain"}),
        "training_period_json": "{}",
        "selection_period_json": "{}",
        "confirmation_period_json": canonical_json({"untouched_prospective": True}),
        "all_parameters_tried_json": canonical_json({
            "frozen_by_config_sha256": True,
            "frozen_by_collector_sha256": True,
        }),
        "selection_rule": "collect only timely completed currency-hours with >=3 independent stories",
        "holdouts_touched_json": canonical_json([]),
        "source_code_hash": str(cohort.get("collector_sha256") or ""),
        "data_snapshot_hash": str(cohort.get("config_sha256") or ""),
        "definition_sha256": stable_hash(contract),
        "created_at": str(payload.get("generated_utc") or utc_now()),
        "definition_json": canonical_json(contract),
    }
    definitions = int(insert_experiment(target, definition))
    observations = 0
    if (
        supersedes
        and supersedes != cohort_id
        and int((payload.get("totals") or {}).get("forecasts") or 0) == 0
        and target.execute(
            "SELECT 1 FROM experiments WHERE hypothesis_id=?", (supersedes,)
        ).fetchone() is not None
    ):
        observations += int(observe(
            target,
            hypothesis_id=supersedes,
            observed_at=str(payload.get("generated_utc") or utc_now()),
            source_system=source.name,
            result="engineering_superseded",
            retirement_reason=(
                "material collector/observation-time contract changed before "
                "any prospective forecast"
            ),
            retired_at=str(payload.get("generated_utc") or utc_now()),
            evidence={
                "superseded_by": cohort_id,
                "prospective_forecasts_before_supersession": 0,
            },
        ))
    evidence = {
        "status": payload.get("status"),
        "latest_quote_utc": payload.get("latest_quote_utc"),
        "decision_utc": payload.get("decision_utc"),
        "totals": payload.get("totals") or {},
        "cycle": payload.get("cycle") or {},
        "supported_execution_decision": payload.get("supported_execution_decision"),
    }
    observations += int(observe(
        target,
        hypothesis_id=cohort_id,
        observed_at=str(payload.get("generated_utc") or utc_now()),
        source_system=source.name,
        result="continue_collecting",
        evidence=evidence,
    ))
    return definitions, observations


def import_treasury_source_state(
    target: sqlite3.Connection,
    source: Path,
) -> tuple[int, int]:
    """Register the source-only Treasury cohort separately from rate rules."""
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    cohort = payload.get("cohort") or {}
    cohort_id = str(cohort.get("cohort_id") or "")
    if not cohort_id:
        return 0, 0
    contract = {
        "cohort_id": cohort_id,
        "config_sha256": cohort.get("config_sha256"),
        "collector_sha256": cohort.get("collector_sha256"),
        "source": cohort.get("source"),
        "first_seen_contract": cohort.get("first_seen_contract"),
        "bootstrap_current_view_proof_eligible": False,
        "historical_revisions_direction_eligible": False,
        "direction_policy": "abstain",
        "execution_eligible": False,
        "supersedes_cohort_id": cohort.get("supersedes_cohort_id"),
    }
    supersedes = str(cohort.get("supersedes_cohort_id") or "")
    definition = {
        "hypothesis_id": cohort_id,
        "parent_hypothesis_id": supersedes or None,
        "experiment_kind": "external_source_prospective_collection",
        "research_generation": "orthogonal_source_prospective_20260808",
        "idea_origin": "us_treasury_daily_yield_curve",
        "pre_registered": 1,
        "data_sources_json": canonical_json(["official_us_treasury_daily_par_yield_curve"]),
        "feature_contract_json": canonical_json({
            "levels": ["2Y", "10Y", "2s10s"],
            "direction_policy": "abstain",
            "config_sha256": cohort.get("config_sha256"),
        }),
        "label_contract_json": canonical_json({"target": "source_integrity_only"}),
        "model_contract_json": canonical_json({"model": "none_source_collection_only"}),
        "cost_contract_json": "{}",
        "allocator_contract_json": "{}",
        "training_period_json": "{}",
        "selection_period_json": "{}",
        "confirmation_period_json": canonical_json({
            "bootstrap_excluded": True,
            "new_yield_dates_only": True,
        }),
        "all_parameters_tried_json": canonical_json({"none": True}),
        "selection_rule": "first locally observed value for each future yield date",
        "holdouts_touched_json": canonical_json([]),
        "source_code_hash": str(cohort.get("collector_sha256") or ""),
        "data_snapshot_hash": str(cohort.get("config_sha256") or ""),
        "definition_sha256": stable_hash(contract),
        "created_at": str(payload.get("generated_utc") or utc_now()),
        "definition_json": canonical_json(contract),
    }
    definitions = int(insert_experiment(target, definition))
    observations = 0
    if (
        supersedes
        and supersedes != cohort_id
        and int((payload.get("totals") or {}).get("prospective_eligible_rows") or 0) == 0
        and target.execute(
            "SELECT 1 FROM experiments WHERE hypothesis_id=?", (supersedes,)
        ).fetchone() is not None
    ):
        observations += int(observe(
            target,
            hypothesis_id=supersedes,
            observed_at=str(payload.get("generated_utc") or utc_now()),
            source_system=source.name,
            result="engineering_superseded",
            retirement_reason=(
                "material collector contract changed before any prospective-eligible row"
            ),
            retired_at=str(payload.get("generated_utc") or utc_now()),
            evidence={
                "superseded_by": cohort_id,
                "prospective_eligible_rows_before_supersession": 0,
            },
        ))
    observations += int(observe(
        target,
        hypothesis_id=cohort_id,
        observed_at=str(payload.get("generated_utc") or utc_now()),
        source_system=source.name,
        result="continue_collecting",
        evidence={
            "status": payload.get("status"),
            "totals": payload.get("totals") or {},
            "cycle": payload.get("cycle") or {},
            "database_integrity": payload.get("database_integrity"),
            "observation_clock": payload.get("observation_clock") or {},
            "limitations": payload.get("limitations") or [],
        },
    ))
    return definitions, observations


def import_alfred_source_state(
    target: sqlite3.Connection,
    source: Path,
) -> tuple[int, int]:
    """Register the blocked or collecting point-in-time ALFRED source cohort."""
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    cohort = payload.get("cohort") or {}
    cohort_id = str(cohort.get("cohort_id") or "")
    if not cohort_id:
        return 0, 0
    supersedes = str(cohort.get("supersedes_cohort_id") or "")
    contract = {
        "cohort_id": cohort_id,
        "supersedes_cohort_id": supersedes or None,
        "config_sha256": cohort.get("config_sha256"),
        "collector_sha256": cohort.get("collector_sha256"),
        "source_contract_id": cohort.get("source_contract_id"),
        "first_seen_contract": cohort.get("first_seen_contract"),
        "initial_current_view_proof_eligible": False,
        "same_day_intrahour_replay_eligible": False,
        "direction_policy": "abstain",
        "execution_eligible": False,
    }
    definition = {
        "hypothesis_id": cohort_id,
        "parent_hypothesis_id": supersedes or None,
        "experiment_kind": "external_source_prospective_collection",
        "research_generation": "orthogonal_source_prospective_20260808",
        "idea_origin": "fred_alfred_point_in_time_macro_vintages",
        "pre_registered": 1,
        "data_sources_json": canonical_json(["official_fred_alfred_api"]),
        "feature_contract_json": canonical_json({
            "config_sha256": cohort.get("config_sha256"),
            "direction_policy": "abstain",
        }),
        "label_contract_json": canonical_json({"target": "source_integrity_only"}),
        "model_contract_json": canonical_json({"model": "none_source_collection_only"}),
        "cost_contract_json": "{}",
        "allocator_contract_json": "{}",
        "training_period_json": "{}",
        "selection_period_json": "{}",
        "confirmation_period_json": canonical_json({
            "bootstrap_excluded": True,
            "first_seen_releases_and_revisions_only": True,
            "date_only_vintages_barred_from_same_day_intrahour_replay": True,
        }),
        "all_parameters_tried_json": canonical_json({"none": True}),
        "selection_rule": "first locally observed point-in-time release or revision",
        "holdouts_touched_json": canonical_json([]),
        "source_code_hash": str(cohort.get("collector_sha256") or ""),
        "data_snapshot_hash": str(cohort.get("config_sha256") or ""),
        "definition_sha256": stable_hash(contract),
        "created_at": str(payload.get("generated_utc") or utc_now()),
        "definition_json": canonical_json(contract),
    }
    definitions = int(insert_experiment(target, definition))
    observations = int(observe(
        target,
        hypothesis_id=cohort_id,
        observed_at=str(payload.get("generated_utc") or utc_now()),
        source_system=source.name,
        result=(
            "blocked_external_credential"
            if payload.get("status") == "blocked_missing_fred_api_key"
            else "continue_collecting"
        ),
        evidence={
            "status": payload.get("status"),
            "credential_environment": payload.get("credential_environment"),
            "credential_present": payload.get("credential_present"),
            "configured_series": payload.get("configured_series"),
            "totals": payload.get("totals") or {},
            "cycle": payload.get("cycle") or {},
            "database_integrity": payload.get("database_integrity"),
            "observation_clock": payload.get("observation_clock") or {},
        },
    ))
    return definitions, observations


def import_shadow_runtime_state(
    target: sqlite3.Connection,
    source: Path,
    *,
    idea_origin: str,
    data_sources: list[str],
    label_target: str,
    selection_rule: str,
) -> tuple[int, int]:
    """Register a research-only runtime cohort that cannot promote or execute."""
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    collection = payload.get("collection_cohort") or {}
    cohort_id = str(payload.get("cohort_id") or collection.get("cohort_id") or "")
    if not cohort_id or not bool(payload.get("research_only")):
        return 0, 0
    parent = str(
        payload.get("model_cohort_id")
        or collection.get("model_cohort_id")
        or collection.get("supersedes_cohort_id")
        or ""
    )
    collector_sha = str(collection.get("collector_sha256") or "")
    contract = {
        "cohort_id": cohort_id,
        "parent_cohort_id": parent or None,
        "collector_sha256": collector_sha or None,
        "runtime_contract": payload.get("contract") or payload.get("policy") or {},
        "research_only": True,
        "execution_eligible": False,
        "can_promote": False,
        "can_place_orders": False,
    }
    definition = {
        "hypothesis_id": cohort_id,
        "parent_hypothesis_id": parent or None,
        "experiment_kind": "prospective_shadow_runtime",
        "research_generation": "proof_first_runtime_20260808",
        "idea_origin": idea_origin,
        "pre_registered": 1,
        "data_sources_json": canonical_json(data_sources),
        "feature_contract_json": canonical_json(payload.get("contract") or payload.get("policy") or {}),
        "label_contract_json": canonical_json({"target": label_target}),
        "model_contract_json": canonical_json({
            "model_cohort_id": payload.get("model_cohort_id"),
            "collector_sha256": collector_sha or None,
        }),
        "cost_contract_json": canonical_json({"executable_bid_ask_required": True}),
        "allocator_contract_json": canonical_json({"execution_eligible": False}),
        "training_period_json": "{}",
        "selection_period_json": "{}",
        "confirmation_period_json": canonical_json({"untouched_prospective": True}),
        "all_parameters_tried_json": canonical_json({"frozen_runtime_contract": True}),
        "selection_rule": selection_rule,
        "holdouts_touched_json": canonical_json([]),
        "source_code_hash": collector_sha,
        "data_snapshot_hash": stable_hash(data_sources),
        "definition_sha256": stable_hash(contract),
        "created_at": str(payload.get("generated_utc") or utc_now()),
        "definition_json": canonical_json(contract),
    }
    definitions = int(insert_experiment(target, definition))
    status = str(payload.get("status") or "unknown")
    result = (
        "closed_market_collecting"
        if status in {"market_or_source_stale", "closed_or_stale", "market_or_quote_stale"}
        else "continue_collecting"
    )
    observations = int(observe(
        target,
        hypothesis_id=cohort_id,
        observed_at=str(payload.get("generated_utc") or utc_now()),
        source_system=source.name,
        result=result,
        evidence={
            "status": status,
            "totals": payload.get("totals") or payload.get("summary") or {},
            "cycle": payload.get("cycle") or {},
            "inserted_entries": payload.get("inserted_entries"),
            "matured_entries": payload.get("matured_entries"),
            "matured_targets": payload.get("matured_targets"),
            "observation_clock": payload.get("observation_clock") or {},
            "supported_execution_decision": payload.get("supported_execution_decision", "no_trade"),
        },
    ))
    additional = payload.get("additional_shadow_cohorts") or []
    if isinstance(additional, list):
        for raw in additional:
            if not isinstance(raw, dict):
                continue
            child_id = str(raw.get("cohort_id") or "")
            if not child_id:
                continue
            child_parent = str(raw.get("parent_cohort_id") or cohort_id)
            child_contract = {
                "cohort_id": child_id,
                "parent_cohort_id": child_parent or None,
                "feature_contract": raw.get("feature_contract") or {},
                "label_contract": raw.get("label_contract") or {},
                "selection_rule": str(raw.get("selection_rule") or ""),
                "research_only": True,
                "execution_eligible": False,
                "can_promote": False,
                "can_place_orders": False,
            }
            child_definition = {
                "hypothesis_id": child_id,
                "parent_hypothesis_id": child_parent or None,
                "experiment_kind": "prospective_shadow_runtime",
                "research_generation": "proof_first_runtime_20260814",
                "idea_origin": str(raw.get("idea_origin") or idea_origin),
                "pre_registered": 1,
                "data_sources_json": canonical_json(raw.get("data_sources") or data_sources),
                "feature_contract_json": canonical_json(raw.get("feature_contract") or {}),
                "label_contract_json": canonical_json(raw.get("label_contract") or {}),
                "model_contract_json": canonical_json({"parent_runtime_cohort": cohort_id}),
                "cost_contract_json": canonical_json({"executable_bid_ask_required": True}),
                "allocator_contract_json": canonical_json({"execution_eligible": False}),
                "training_period_json": "{}",
                "selection_period_json": canonical_json({"starts": raw.get("created_at")}),
                "confirmation_period_json": canonical_json({"untouched_prospective": True}),
                "all_parameters_tried_json": canonical_json({"frozen_runtime_contract": True}),
                "selection_rule": str(raw.get("selection_rule") or "frozen"),
                "holdouts_touched_json": canonical_json([]),
                "source_code_hash": str(raw.get("source_code_hash") or collector_sha),
                "data_snapshot_hash": stable_hash(raw.get("data_sources") or data_sources),
                "definition_sha256": stable_hash(child_contract),
                "created_at": str(raw.get("created_at") or payload.get("generated_utc") or utc_now()),
                "definition_json": canonical_json(child_contract),
            }
            definitions += int(insert_experiment(target, child_definition))
            observations += int(observe(
                target,
                hypothesis_id=child_id,
                observed_at=str(payload.get("generated_utc") or utc_now()),
                source_system=source.name,
                result=(
                    "closed_market_collecting"
                    if status in {"market_or_source_stale", "closed_or_stale", "market_or_quote_stale"}
                    else "continue_collecting"
                ),
                evidence={
                    "status": status,
                    "inserted_entries": payload.get("inserted_entries"),
                    "matured_entries": payload.get("matured_entries"),
                    "diagnostics": payload.get("diagnostics") or {},
                    "observation_clock": payload.get("observation_clock") or {},
                },
            ))
    return definitions, observations


def import_model_artifact_manifest(
    target: sqlite3.Connection,
    source: Path,
    *,
    idea_origin: str,
) -> tuple[int, int]:
    """Register a frozen research model artifact before its collector child."""
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    cohort_id = str(payload.get("cohort_id") or "")
    if not cohort_id or not bool(payload.get("research_only")):
        return 0, 0
    artifact_path = source.parent / "models.joblib"
    artifact_sha = (
        hashlib.sha256(artifact_path.read_bytes()).hexdigest()
        if artifact_path.is_file() else ""
    )
    contract = {
        "manifest": payload,
        "artifact_sha256": artifact_sha,
        "execution_eligible": False,
    }
    definition = {
        "hypothesis_id": cohort_id,
        "parent_hypothesis_id": None,
        "experiment_kind": "frozen_model_artifact",
        "research_generation": "proof_first_runtime_20260808",
        "idea_origin": idea_origin,
        "pre_registered": 1,
        "data_sources_json": canonical_json(["OANDA_executable_bid_ask", "quote_intensity"]),
        "feature_contract_json": canonical_json(payload.get("feature_contract") or []),
        "label_contract_json": canonical_json({"target": "after_cost_executable_opportunity"}),
        "model_contract_json": canonical_json({"artifact_sha256": artifact_sha}),
        "cost_contract_json": canonical_json({"config_sha256": payload.get("config_sha256")}),
        "allocator_contract_json": canonical_json({"execution_eligible": False}),
        "training_period_json": canonical_json({"source_highwater_utc": payload.get("source_highwater_utc")}),
        "selection_period_json": "{}",
        "confirmation_period_json": canonical_json({"prospective_start_utc": payload.get("prospective_start_utc")}),
        "all_parameters_tried_json": canonical_json({"manifest_frozen": True}),
        "selection_rule": "frozen artifact; proof requires later prospective collection",
        "holdouts_touched_json": canonical_json([]),
        "source_code_hash": str(payload.get("source_code_sha256") or ""),
        "data_snapshot_hash": str(payload.get("config_sha256") or ""),
        "definition_sha256": stable_hash(contract),
        "created_at": str(payload.get("created_utc") or utc_now()),
        "definition_json": canonical_json(contract),
    }
    definitions = int(insert_experiment(target, definition))
    observations = int(observe(
        target,
        hypothesis_id=cohort_id,
        observed_at=str(payload.get("created_utc") or utc_now()),
        source_system=source.name,
        result="frozen_awaiting_prospective_evidence",
        evidence={
            "artifact_sha256": artifact_sha,
            "prospective_start_utc": payload.get("prospective_start_utc"),
            "source_instrument_count": payload.get("source_instrument_count"),
        },
    ))
    return definitions, observations


def observe_after_cost_definition_conflict(
    target: sqlite3.Connection,
    *,
    source: Path,
    payload: dict[str, Any],
    cohort_id: str,
    definition: dict[str, Any],
    frozen_manifest: dict[str, Any],
) -> tuple[int, int] | None:
    """Hard-block a reused after-cost cohort ID without crash-looping.

    The immutable registered definition remains authoritative. A regenerated
    state with different bytes is recorded as an incident and cannot merge,
    overwrite, promote, or authorize anything.
    """
    existing = target.execute(
        "SELECT definition_sha256,source_code_hash FROM experiments "
        "WHERE hypothesis_id=?",
        (cohort_id,),
    ).fetchone()
    if existing is None or str(existing[0]) == str(definition["definition_sha256"]):
        return None
    summary = payload.get("summary") or {}
    evidence_counts = {
        field: int(summary.get(field) or 0)
        for field in (
            "admissible_canonical_record_count",
            "economics_admissible_count",
            "ranked_count",
            "selectable_count",
            "hold_switch_count",
        )
    }
    zero_evidence = not any(evidence_counts.values())
    incident = {
        "incident": "same_hypothesis_id_definition_conflict",
        "hard_blocked": True,
        "registered_definition_sha256": str(existing[0]),
        "registered_source_code_hash": str(existing[1] or ""),
        "proposed_definition_sha256": str(definition["definition_sha256"]),
        "proposed_source_code_hash": str(definition["source_code_hash"]),
        "proposed_state_artifact_sha256": hashlib.sha256(
            source.read_bytes()
        ).hexdigest(),
        "proposed_snapshot_id": payload.get("snapshot_id"),
        "proposed_manifest_id": frozen_manifest.get("manifest_id"),
        "proposed_manifest_sha256": frozen_manifest.get("manifest_sha256"),
        "evidence_counts": evidence_counts,
        "zero_admissible_evidence": zero_evidence,
        "definition_merged_or_overwritten": False,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
        "required_action": "open_a_new_cohort_id_for_material_changes",
    }
    result = (
        "engineering_quarantined_artifact_drift_zero_evidence"
        if zero_evidence
        else "artifact_definition_conflict_hard_blocked_nonzero_evidence"
    )
    return 0, int(observe(
        target,
        hypothesis_id=cohort_id,
        observed_at=str(payload.get("generated_utc") or utc_now()),
        source_system=source.name,
        result=result,
        evidence=incident,
        retirement_reason="same_cohort_id_reused_with_different_definition",
    ))


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _after_cost_v4_external_trust_anchor(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Independently attest the complete frozen v4 artifact closure.

    Neither the state-embedded manifest nor its claimed hashes are trust
    anchors.  The canonical manifest is read from the registrar's ROOT, its
    semantic hash is reconstructed, and every exact declared artifact path is
    resolved beneath ROOT and byte-hashed before an anchor is returned.
    """
    manifest_path = ROOT / AFTER_COST_V4_MANIFEST_RELATIVE_PATH
    config_path = ROOT / AFTER_COST_V4_EXPECTED_ARTIFACT_PATHS[
        "counterfactual_v4_config"
    ]
    try:
        manifest_bytes = manifest_path.read_bytes()
        manifest = json.loads(manifest_bytes.decode("utf-8"))
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(manifest, dict) or not isinstance(config, dict):
        return None
    cohort_id = str(payload.get("counterfactual_cohort_id") or "")
    contract_id = str(payload.get("counterfactual_contract_id") or "")
    if (
        manifest.get("manifest_id") != config.get("required_frozen_manifest_id")
        or manifest.get("contract_id") != contract_id
        or manifest.get("contract_id") != config.get("contract_id")
        or manifest.get("cohort_id") != cohort_id
        or manifest.get("cohort_id") != config.get("counterfactual_cohort_id")
        or set(config.get("required_manifest_artifacts") or [])
        != set(AFTER_COST_V4_EXPECTED_ARTIFACT_PATHS)
    ):
        return None
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict) or set(artifacts) != set(
        AFTER_COST_V4_EXPECTED_ARTIFACT_PATHS
    ):
        return None
    root_resolved = ROOT.resolve()
    normalized_artifacts: dict[str, dict[str, str]] = {}
    for label in sorted(AFTER_COST_V4_EXPECTED_ARTIFACT_PATHS):
        row = artifacts.get(label)
        if not isinstance(row, dict):
            return None
        relative_path = str(row.get("relative_path") or "").replace("\\", "/")
        declared_sha256 = str(row.get("sha256") or "").lower()
        if (
            relative_path != AFTER_COST_V4_EXPECTED_ARTIFACT_PATHS[label]
            or len(declared_sha256) != 64
            or any(character not in "0123456789abcdef" for character in declared_sha256)
        ):
            return None
        artifact_path = (ROOT / relative_path).resolve()
        try:
            artifact_path.relative_to(root_resolved)
        except ValueError:
            return None
        try:
            if not artifact_path.is_file() or _sha256_file(artifact_path) != declared_sha256:
                return None
        except OSError:
            return None
        normalized_artifacts[label] = {
            "relative_path": relative_path,
            "sha256": declared_sha256,
        }
    normalized_manifest = {
        "manifest_id": manifest["manifest_id"],
        "contract_id": manifest["contract_id"],
        "cohort_id": manifest["cohort_id"],
        "artifacts": normalized_artifacts,
    }
    semantic_manifest_sha256 = stable_hash(normalized_manifest)
    normalized_manifest["manifest_sha256"] = semantic_manifest_sha256
    manifest_file_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    if (
        manifest_file_sha256 != AFTER_COST_V4_FROZEN_MANIFEST_FILE_SHA256
        or semantic_manifest_sha256
        != AFTER_COST_V4_FROZEN_MANIFEST_SEMANTIC_SHA256
        or normalized_artifacts["counterfactual_v4_module"]["sha256"]
        != AFTER_COST_V4_FROZEN_MODULE_SHA256
        or normalized_artifacts["counterfactual_v4_config"]["sha256"]
        != AFTER_COST_V4_FROZEN_CONFIG_SHA256
        or normalized_artifacts["counterfactual_v4_cli"]["sha256"]
        != AFTER_COST_V4_FROZEN_CLI_SHA256
    ):
        return None
    state_manifest = payload.get("frozen_manifest")
    if state_manifest != normalized_manifest:
        return None
    expected_anchor = {
        "anchor_schema": "currency_state_after_cost_v4_external_trust_anchor_v1",
        "manifest_relative_path": AFTER_COST_V4_MANIFEST_RELATIVE_PATH,
        "manifest_file_sha256": manifest_file_sha256,
        "manifest_id": normalized_manifest["manifest_id"],
        "manifest_sha256": semantic_manifest_sha256,
        "artifacts": normalized_artifacts,
        "counterfactual_v4_module_sha256": normalized_artifacts[
            "counterfactual_v4_module"
        ]["sha256"],
        "counterfactual_v4_config_sha256": normalized_artifacts[
            "counterfactual_v4_config"
        ]["sha256"],
        "counterfactual_v4_cli_sha256": normalized_artifacts[
            "counterfactual_v4_cli"
        ]["sha256"],
    }
    if payload.get("expected_external_trust_anchor") != expected_anchor:
        return None
    return normalized_manifest, expected_anchor


def _after_cost_v4_is_zero_evidence_engineering_state(
    payload: dict[str, Any],
) -> bool:
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return False
    required_zero_fields = (
        "submitted_envelope_count",
        "submitted_record_count",
        "submitted_canonical_record_count",
        "submitted_account_state_record_count",
        "admissible_canonical_record_count",
        "admissible_account_state_record_count",
        "admissible_quote_count",
        "admissible_verifier_count",
        "venue_quote_input_count",
        "verifier_input_count",
        "economics_input_count",
        "economics_admissible_count",
        "ranked_count",
        "selectable_count",
        "hold_switch_count",
    )
    try:
        zero_counts = all(int(summary.get(field, -1)) == 0 for field in required_zero_fields)
    except (TypeError, ValueError):
        return False
    allocations = payload.get("allocations")
    if not isinstance(allocations, dict):
        return False
    try:
        for horizons in allocations.values():
            if not isinstance(horizons, dict):
                return False
            for allocation in horizons.values():
                if (
                    not isinstance(allocation, dict)
                    or int(allocation.get("admissible_ranked_count", -1)) != 0
                    or int(allocation.get("selectable_count", -1)) != 0
                    or allocation.get("top_one") is not None
                    or (allocation.get("disjoint_basket") or [])
                    or allocation.get("execution_eligible") is not False
                    or allocation.get("can_place_orders") is not False
                    or allocation.get("supported_execution_decision") != "no_trade"
                ):
                    return False
    except (TypeError, ValueError):
        return False
    return bool(
        zero_counts
        and payload.get("registration_status") == "engineering_blocked_zero_evidence"
        and payload.get("producer_integration_state") == "producer_integration_missing"
        and payload.get("research_only") is True
        and payload.get("execution_eligible") is False
        and payload.get("can_place_orders") is False
        and payload.get("supported_execution_decision") == "no_trade"
        and not (payload.get("hold_switch_counterfactuals") or [])
    )


def import_currency_state_after_cost_state(
    target: sqlite3.Connection,
    source: Path,
    *,
    registration_status: str = "frozen",
) -> tuple[int, int]:
    """Register the frozen CurrencyState after-cost policy cohort.

    This records the counterfactual as one policy-level research hypothesis. It
    does not convert its pair/horizon rows into promotion candidates and it
    cannot authorize execution.
    """
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    if registration_status not in {"frozen", "engineering_blocked"}:
        raise ValueError("unsupported after-cost registration status")
    cohort_id = str(payload.get("counterfactual_cohort_id") or "")
    contract_id = str(payload.get("counterfactual_contract_id") or "")
    is_v3 = (
        str(payload.get("snapshot_schema") or "")
        == "currency_state_after_cost_counterfactual_snapshot_v3"
    )
    is_v4 = (
        str(payload.get("snapshot_schema") or "")
        == "currency_state_after_cost_counterfactual_snapshot_v4"
    )
    frozen_manifest = payload.get("frozen_manifest") or {}
    is_manifest_governed = bool(frozen_manifest)
    v4_external_trust_anchor: dict[str, Any] | None = None
    if is_v4:
        # V4 is a byte-attested zero-input engineering artifact, not a frozen
        # evidence cohort.  It must never fall through to the historical,
        # state-manifest-trusting path.
        if (
            registration_status != "engineering_blocked"
            or not _after_cost_v4_is_zero_evidence_engineering_state(payload)
        ):
            return 0, 0
        v4_attestation = _after_cost_v4_external_trust_anchor(payload)
        if v4_attestation is None:
            return 0, 0
        frozen_manifest, v4_external_trust_anchor = v4_attestation
        artifacts = frozen_manifest["artifacts"]
        fingerprints = {
            "counterfactual_module_sha256": artifacts[
                "counterfactual_v4_module"
            ]["sha256"],
            "counterfactual_config_file_sha256": artifacts[
                "counterfactual_v4_config"
            ]["sha256"],
            "counterfactual_cli_sha256": artifacts[
                "counterfactual_v4_cli"
            ]["sha256"],
            "independent_verifier_producer_sha256": artifacts[
                "independent_verifier_producer"
            ]["sha256"],
        }
    elif is_manifest_governed:
        if (
            frozen_manifest.get("cohort_id") != cohort_id
            or frozen_manifest.get("contract_id") != contract_id
            or not str(frozen_manifest.get("manifest_id") or "")
            or len(str(frozen_manifest.get("manifest_sha256") or "")) != 64
            or not isinstance(frozen_manifest.get("artifacts"), dict)
        ):
            return 0, 0
        artifacts = frozen_manifest["artifacts"]
        fingerprints = {
            "counterfactual_module_sha256": (
                artifacts.get("counterfactual_v3_module")
                or artifacts.get("counterfactual_module") or {}
            ).get("sha256"),
            "counterfactual_config_file_sha256": (
                artifacts.get("counterfactual_v3_config")
                or artifacts.get("counterfactual_config") or {}
            ).get("sha256"),
            "counterfactual_cli_sha256": (
                artifacts.get("counterfactual_v3_cli")
                or artifacts.get("counterfactual_cli") or {}
            ).get("sha256"),
            "independent_verifier_producer_sha256": (
                artifacts.get("independent_verifier_producer") or {}
            ).get("sha256"),
        }
    else:
        fingerprints = payload.get("artifact_fingerprints") or {}
    if (
        not cohort_id
        or not contract_id
        or payload.get("research_only") is not True
        or payload.get("execution_eligible") is not False
        or payload.get("can_place_orders") is not False
    ):
        return 0, 0
    contract = {
        "cohort_id": cohort_id,
        "contract_id": contract_id,
        "contract_sha256": payload.get("counterfactual_contract_sha256"),
        "artifact_fingerprints": fingerprints,
        "response_arm_contract_id": payload.get("response_arm_contract_id"),
        "response_snapshot_id": payload.get("response_snapshot_id"),
        "response_snapshot_sha256": payload.get("response_snapshot_sha256"),
        "currency_state_snapshot_id": payload.get("currency_state_snapshot_id"),
        "horizons_sec": payload.get("horizons_sec") or [],
        "arm_ids": payload.get("arm_ids") or [],
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
    }
    if is_v4:
        contract.update({
            "v4_external_trust_anchor": v4_external_trust_anchor,
            "v4_registration_status": payload.get("registration_status"),
            "v4_producer_integration_state": payload.get(
                "producer_integration_state"
            ),
            "v4_zero_evidence_engineering_artifact": True,
        })
    if is_manifest_governed:
        contract.update({
            "snapshot_schema": payload.get("snapshot_schema"),
            "supersedes_contract_id": payload.get("supersedes_contract_id"),
            "supersedes_cohort_id": payload.get("supersedes_cohort_id"),
            "frozen_manifest": frozen_manifest,
            "quote_envelope_present": payload.get("quote_envelope") is not None,
            "verifier_envelope_present": payload.get("verifier_envelope") is not None,
            "economics_envelope_present": payload.get("economics_envelope") is not None,
            "hold_switch_envelope_present": payload.get("hold_switch_envelope") is not None,
        })
        if is_v3:
            contract["account_envelope_present"] = bool(
                ((payload.get("canonical_input_hashes") or {}).get("envelopes") or {}).get("account_state")
            )
    data_sources = [
        "CurrencyState_v2",
        "OfficialFactAdapter",
        "versioned_response_timing_arms",
        "OANDA_executable_bid_ask",
        "locked_calibration_and_cost_envelopes_when_available",
    ]
    if is_manifest_governed:
        data_sources.extend([
            "fresh_immutable_Practice_007_venue_quote_envelope",
            "independent_verifier_evidence_envelope",
            "explicit_entry_exit_slippage_latency_rotation_cost_envelope",
            "exact_account_position_hold_switch_envelope",
        ])
    definition = {
        "hypothesis_id": cohort_id,
        "parent_hypothesis_id": payload.get("supersedes_cohort_id"),
        "experiment_kind": (
            "frozen_policy_level_counterfactual"
            if registration_status == "frozen"
            else "engineering_policy_level_counterfactual"
        ),
        "research_generation": "currency_state_branch_20260817",
        "idea_origin": "currency_state_after_cost_top_one_basket_hold_switch",
        "pre_registered": 1,
        "data_sources_json": canonical_json(data_sources),
        "feature_contract_json": canonical_json({
            "response_arm_contract_id": payload.get("response_arm_contract_id"),
            "horizons_sec": payload.get("horizons_sec") or [],
            "arm_ids": payload.get("arm_ids") or [],
        }),
        "label_contract_json": canonical_json({
            "targets": [
                "after_cost_ev",
                "cost_clearance",
                "top_one",
                "disjoint_basket",
                "hold_switch",
            ],
            "observed_price_is_never_forward_evidence": True,
        }),
        "model_contract_json": canonical_json(
            {
                "contract_id": contract_id,
                "contract_sha256": fingerprints.get(
                    "counterfactual_config_file_sha256"
                ),
                "module_sha256": fingerprints.get("counterfactual_module_sha256"),
                "frozen_manifest_id": frozen_manifest.get("manifest_id"),
                "frozen_manifest_sha256": frozen_manifest.get("manifest_sha256"),
            }
            if is_manifest_governed
            else {
                "contract_id": contract_id,
                "contract_sha256": payload.get("counterfactual_contract_sha256"),
                "module_sha256": fingerprints.get("counterfactual_module_sha256"),
            }
        ),
        "cost_contract_json": canonical_json({
            "missing_cost_policy": "unavailable_not_zero",
            "executable_bid_ask_required": True,
            "config_sha256": fingerprints.get("counterfactual_config_file_sha256"),
        }),
        "allocator_contract_json": canonical_json({
            "top_one_and_disjoint_basket": True,
            "hold_switch": True,
            "execution_eligible": False,
            "supported_execution_decision": "no_trade",
        }),
        "training_period_json": "{}",
        "selection_period_json": canonical_json({
            "decision_cutoff_utc": payload.get("decision_cutoff_utc")
        }),
        "confirmation_period_json": canonical_json({
            "untouched_prospective_required": True
        }),
        "all_parameters_tried_json": canonical_json(
            {
                "frozen_by_contract_and_byte_hash_manifest": True,
                "manifest_governed": True,
            }
            if is_manifest_governed
            else {"frozen_by_contract_and_byte_hash_manifest": True}
        ),
        "selection_rule": (
            "rank only causally grounded rows with locked probability, magnitude, "
            "executable spread, slippage, latency, and rotation economics"
        ),
        "holdouts_touched_json": canonical_json([]),
        "source_code_hash": str(fingerprints.get("counterfactual_module_sha256") or ""),
        "data_snapshot_hash": str(payload.get("response_snapshot_sha256") or ""),
        "definition_sha256": stable_hash(contract),
        "created_at": str(payload.get("generated_utc") or utc_now()),
        "definition_json": canonical_json(contract),
    }
    conflict = observe_after_cost_definition_conflict(
        target,
        source=source,
        payload=payload,
        cohort_id=cohort_id,
        definition=definition,
        frozen_manifest=frozen_manifest,
    )
    if conflict is not None:
        return conflict
    definitions = int(insert_experiment(target, definition))
    summary = payload.get("summary") or {}
    if registration_status == "engineering_blocked":
        result = "engineering_blocked_zero_evidence"
    else:
        result = (
            "frozen_awaiting_admissible_inputs"
            if int(summary.get("economics_admissible_count") or 0) == 0
            else "continue_collecting"
        )
    observation_evidence = {
        "snapshot_id": payload.get("snapshot_id"),
        "decision_cutoff_utc": payload.get("decision_cutoff_utc"),
        "summary": summary,
        "input_rejection_count": len(payload.get("input_rejections") or []),
        "supported_execution_decision": payload.get(
            "supported_execution_decision", "no_trade"
        ),
        "artifact_fingerprints": fingerprints,
    }
    if is_manifest_governed:
        observation_evidence.update({
            "frozen_manifest_id": frozen_manifest.get("manifest_id"),
            "frozen_manifest_sha256": frozen_manifest.get("manifest_sha256"),
            "supersedes_contract_id": payload.get("supersedes_contract_id"),
            "supersedes_cohort_id": payload.get("supersedes_cohort_id"),
        })
    if is_v4:
        observation_evidence["v4_external_trust_anchor"] = (
            v4_external_trust_anchor
        )
    observations = int(observe(
        target,
        hypothesis_id=cohort_id,
        observed_at=str(payload.get("generated_utc") or utc_now()),
        source_system=source.name,
        result=result,
        evidence=observation_evidence,
    ))
    return definitions, observations


def observe_currency_state_after_cost_supersession(
    target: sqlite3.Connection, prior_source: Path, replacement_source: Path,
) -> int:
    """Append a non-statistical supersession only for a zero-evidence cohort."""
    try:
        prior = json.loads(prior_source.read_text(encoding="utf-8"))
        replacement = json.loads(replacement_source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0
    prior_id = str(prior.get("counterfactual_cohort_id") or "")
    replacement_id = str(replacement.get("counterfactual_cohort_id") or "")
    if (
        not prior_id
        or not replacement_id
        or replacement.get("supersedes_cohort_id") != prior_id
        or prior.get("research_only") is not True
        or replacement.get("research_only") is not True
        or prior.get("execution_eligible") is not False
        or replacement.get("execution_eligible") is not False
    ):
        return 0
    prior_summary = prior.get("summary") or {}
    evidence_counts = [
        int(prior_summary.get(field) or 0)
        for field in (
            "economics_admissible_count", "ranked_count", "selectable_count",
            "hold_switch_count",
        )
    ]
    if any(evidence_counts):
        return 0
    input_counts = [
        int(prior_summary.get(field) or 0)
        for field in (
            "venue_quote_input_count", "verifier_input_count",
            "economics_input_count", "submitted_canonical_record_count",
        )
    ]
    if any(input_counts):
        return 0
    if any(prior.get(field) is not None for field in (
        "quote_envelope", "verifier_envelope", "economics_envelope",
        "hold_switch_envelope",
    )):
        return 0
    replacement_manifest = replacement.get("frozen_manifest") or {}
    observed_at = str(replacement.get("generated_utc") or utc_now())
    existing = target.execute(
        "SELECT 1 FROM experiment_observations "
        "WHERE hypothesis_id=? AND observed_at=? AND source_system=? AND result=? "
        "LIMIT 1",
        (
            prior_id,
            observed_at,
            replacement_source.name,
            "engineering_superseded_zero_evidence",
        ),
    ).fetchone()
    if existing is not None:
        return 0
    return int(observe(
        target,
        hypothesis_id=prior_id,
        observed_at=observed_at,
        source_system=replacement_source.name,
        result="engineering_superseded_zero_evidence",
        evidence={
            "prior_snapshot_id": prior.get("snapshot_id"),
            "replacement_snapshot_id": replacement.get("snapshot_id"),
            "replacement_contract_id": replacement.get("counterfactual_contract_id"),
            "replacement_cohort_id": replacement_id,
            "replacement_manifest_id": replacement_manifest.get("manifest_id"),
            "replacement_manifest_sha256": replacement_manifest.get("manifest_sha256"),
            "prior_evidence_counts": dict(zip(
                (
                    "economics_admissible_count", "ranked_count",
                    "selectable_count", "hold_switch_count",
                ),
                evidence_counts,
            )),
            "prior_input_counts": dict(zip(
                (
                    "venue_quote_input_count", "verifier_input_count",
                    "economics_input_count", "submitted_canonical_record_count",
                ),
                input_counts,
            )),
            "reason": "material contract and evidence-lineage hardening before admissible evidence",
            "statistical_futility_retirement": False,
            "execution_eligible": False,
            "supported_execution_decision": "no_trade",
        },
    ))


def import_counterfactual_sim_gym(
    target: sqlite3.Connection, source: Path
) -> tuple[int, int]:
    """Register immutable SIM-gym cohorts without treating replay as proof.

    The gym owns its append-only cohort lineage.  This importer is deliberately
    read-only and records each material contract as one historical diagnostic
    hypothesis.  A later cohort supersedes an earlier engineering contract; it
    never rewrites, confirms, promotes, or statistically combines the earlier
    replay.
    """
    if not source.is_file():
        return 0, 0
    connection = ro(source)
    definitions = observations = 0
    try:
        required = {"sim_cohorts", "sim_cohort_transitions"}
        present = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if not required.issubset(present):
            return 0, 0
        parents = cohort_parent_map(connection, "sim_cohort_transitions")
        cohort_rows = list(
            connection.execute("SELECT * FROM sim_cohorts ORDER BY created_utc,cohort_id")
        )
        if not cohort_rows:
            return 0, 0
        current_cohort_id = str(cohort_rows[-1]["cohort_id"])
        for row in cohort_rows:
            cohort_id = str(row["cohort_id"])
            contract = json.loads(str(row["material_contract_json"] or "{}"))
            effective = contract.get("effective_config") or {}
            source_contract = effective.get("source") or {}
            sampling = effective.get("sampling") or {}
            partitions = effective.get("historical_partitions") or {}
            signal_rules = effective.get("signal_rules") or []
            execution_grid = effective.get("execution_grid") or {}
            source_manifest = contract.get("source_manifest") or []
            definition = {
                "hypothesis_id": cohort_id,
                "parent_hypothesis_id": parents.get(cohort_id),
                "experiment_kind": "historical_counterfactual_sim",
                "research_generation": str(
                    contract.get("contract_id") or row["experiment_key"]
                ),
                "idea_origin": "high_volume_matched_virtual_order_replay",
                "pre_registered": 1,
                "data_sources_json": canonical_json(
                    [source_contract.get("kind") or "OANDA_completed_M1_bid_ask"]
                ),
                "feature_contract_json": canonical_json({
                    "knowledge_time": source_contract.get("knowledge_time"),
                    "signal_rules": signal_rules,
                    "cadence_min": sampling.get("cadence_min"),
                }),
                "label_contract_json": canonical_json({
                    "entry_quote": source_contract.get("entry_quote"),
                    "path_quote": source_contract.get("path_quote"),
                    "execution_grid": execution_grid,
                    "partitions": partitions,
                    "historical_replay_can_confirm": False,
                }),
                "model_contract_json": canonical_json({"signal_rules": signal_rules}),
                "cost_contract_json": canonical_json(effective.get("costs") or {}),
                "allocator_contract_json": canonical_json(
                    effective.get("comparators") or {}
                ),
                "training_period_json": canonical_json({
                    "training": False,
                    "historical_replay": contract.get("window") or {},
                }),
                "selection_period_json": canonical_json({
                    "diagnostic_partitions": partitions.get("labels") or [],
                    "purged": bool(partitions.get("purge_overlapping_outcomes")),
                }),
                "confirmation_period_json": canonical_json({
                    "untouched_prospective_required": True,
                    "historical_replay_can_confirm": False,
                }),
                "all_parameters_tried_json": canonical_json({
                    "signal_rules": signal_rules,
                    "execution_grid": execution_grid,
                    "comparators": effective.get("comparators") or {},
                }),
                "selection_rule": (
                    "matched as-signaled, flipped, deterministic-random, and "
                    "no-trade historical diagnostics; no historical promotion"
                ),
                "holdouts_touched_json": canonical_json(
                    partitions.get("labels") or []
                ),
                "source_code_hash": stable_hash({
                    "runner": row["runner_sha256"],
                    "core": row["core_sha256"],
                }),
                "data_snapshot_hash": stable_hash(source_manifest),
                "definition_sha256": str(row["material_contract_sha256"]),
                "created_at": str(row["created_utc"]),
                "definition_json": canonical_json(contract),
            }
            definitions += int(insert_experiment(target, definition))
            result = (
                "historical_diagnostic_current"
                if cohort_id == current_cohort_id
                else "engineering_superseded_material_contract"
            )
            evidence = {
                "cohort_id": cohort_id,
                "contract_id": contract.get("contract_id"),
                "parent_cohort_id": parents.get(cohort_id),
                "material_contract_sha256": row["material_contract_sha256"],
                "source_manifest_count": len(source_manifest),
                "historical_replay_can_confirm": False,
                "execution_eligible": False,
                "supported_decision": "no_trade",
            }
            observations += int(observe(
                target,
                hypothesis_id=cohort_id,
                observed_at=str(row["created_utc"]),
                source_system=source.name,
                result=result,
                evidence=evidence,
                retirement_reason=(
                    "material engineering contract superseded; evidence preserved"
                    if cohort_id != current_cohort_id
                    else None
                ),
                retired_at=(
                    str(cohort_rows[-1]["created_utc"])
                    if cohort_id != current_cohort_id
                    else None
                ),
            ))
    finally:
        connection.close()
    return definitions, observations


def import_sequential_deliberate_replay(
    target: sqlite3.Connection, source: Path
) -> tuple[int, int]:
    """Register the training-only deliberate-practice projection.

    The source SIM cohort remains the parent.  Practice cases and repeated
    attempts cannot become proof, so this importer never confirms or promotes.
    """
    if not source.is_file():
        return 0, 0
    connection = ro(source)
    definitions = observations = 0
    try:
        present = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if not {"replay_cohorts", "replay_snapshots"}.issubset(present):
            return 0, 0
        for row in connection.execute(
            "SELECT * FROM replay_cohorts ORDER BY created_utc,cohort_id"
        ):
            cohort_id = str(row["cohort_id"])
            parent_id = str(row["source_cohort_id"])
            contract = json.loads(str(row["contract_json"] or "{}"))
            snapshot = connection.execute(
                "SELECT statistics_json FROM replay_snapshots "
                "WHERE cohort_id=? ORDER BY generated_utc DESC LIMIT 1",
                (cohort_id,),
            ).fetchone()
            statistics = json.loads(str(snapshot[0])) if snapshot else {}
            census = statistics.get("repetition_census") or {}
            definition = {
                "hypothesis_id": cohort_id,
                "parent_hypothesis_id": parent_id,
                "experiment_kind": "historical_deliberate_practice",
                "research_generation": "sequential_deliberate_replay_v1",
                "idea_origin": "high_volume_deliberate_market_replay_with_honest_repetition_counts",
                "pre_registered": 1,
                "data_sources_json": canonical_json([
                    "independently_verified_counterfactual_sim_gym",
                ]),
                "feature_contract_json": canonical_json({
                    "global_portfolio_clock": True,
                    "future_free_situation_fingerprint": True,
                    "blind_case_presentation": True,
                    "source_cohort_id": parent_id,
                }),
                "label_contract_json": canonical_json({
                    "historical_training_discovery_only": True,
                    "variants_are_nested": True,
                    "repeat_attempts_are_not_new_market_repetitions": True,
                    "management_exit_rotation": "not_measured_in_endpoint_only_source",
                }),
                "model_contract_json": canonical_json({
                    "model": "none_case_bank_and_precommitment_journal",
                }),
                "cost_contract_json": "{}",
                "allocator_contract_json": canonical_json({
                    "one_primary_action_per_session_clock": True,
                    "actions": ["wait", "enter", "hold", "exit", "rotate"],
                    "maximum_open_positions": 1,
                }),
                "training_period_json": canonical_json({
                    "source": "all already-inspected SIM historical partitions",
                    "portfolio_decision_clocks": census.get("portfolio_decision_clocks"),
                }),
                "selection_period_json": canonical_json({
                    "training_discovery_only": True,
                }),
                "confirmation_period_json": canonical_json({
                    "none": True,
                    "later_untouched_prospective_cohort_required": True,
                }),
                "all_parameters_tried_json": canonical_json({
                    "source_virtual_variants": census.get("source_virtual_variants"),
                    "source_two_sided_outcome_parts": census.get("source_two_sided_outcome_parts"),
                }),
                "selection_rule": (
                    "one primary portfolio action per global clock; all variants and "
                    "repeat attempts remain nested training diagnostics"
                ),
                "holdouts_touched_json": canonical_json([
                    "diagnostic_early", "diagnostic_middle", "diagnostic_late",
                ]),
                "source_code_hash": stable_hash({
                    "runner": row["runner_sha256"],
                    "core": row["core_sha256"],
                }),
                "data_snapshot_hash": str(row["source_database_sha256"]),
                "definition_sha256": str(row["contract_sha256"]),
                "created_at": str(row["created_utc"]),
                "definition_json": canonical_json(contract),
            }
            definitions += int(insert_experiment(target, definition))
            observations += int(observe(
                target,
                hypothesis_id=cohort_id,
                observed_at=str(row["created_utc"]),
                source_system=source.name,
                result="historical_practice_curriculum",
                evidence={
                    "source_cohort_id": parent_id,
                    "repetition_census": census,
                    "historical_replay_can_confirm": False,
                    "execution_eligible": False,
                    "supported_decision": "no_trade",
                },
            ))
    finally:
        connection.close()
    return definitions, observations


def import_sequential_portfolio_replay(
    target: sqlite3.Connection, source_root: Path
) -> tuple[int, int]:
    """Register the verified bar-by-bar portfolio-training sidecar.

    The latest state resolves its content-addressed cohort database. Every
    session is historical training/discovery, permanently proof-ineligible.
    """
    state_path = source_root / "sequential_portfolio_replay_v1.json"
    verifier_path = source_root / "sequential_portfolio_replay_verifier_v1.json"
    if not state_path.is_file() or not verifier_path.is_file():
        return 0, 0
    state = json.loads(state_path.read_text(encoding="utf-8"))
    verifier = json.loads(verifier_path.read_text(encoding="utf-8"))
    if verifier.get("verified") is not True or verifier.get("failures"):
        return 0, 0
    source = Path(str(state.get("database") or ""))
    if not source.is_file():
        return 0, 0
    connection = ro(source)
    definitions = observations = 0
    try:
        cohort = connection.execute(
            "SELECT * FROM spr_cohorts WHERE cohort_id=?",
            (str(state["cohort_id"]),),
        ).fetchone()
        session = connection.execute(
            "SELECT * FROM spr_sessions WHERE session_id=?",
            (str(state["session_id"]),),
        ).fetchone()
        if cohort is None or session is None:
            return 0, 0
        contract = json.loads(str(cohort["contract_json"]))
        statistics = {
            "global_clock_count": state.get("global_clock_count"),
            "pair_context_count": state.get("pair_context_count"),
            "action_counts": state.get("action_counts"),
            "execution_leg_count": state.get("execution_leg_count"),
            "counterfactual_count": state.get("counterfactual_count"),
            "terminal_realized_pips": state.get("terminal_realized_pips"),
            "terminal_flat": state.get("terminal_flat"),
            "structural_component_count": state.get("structural_component_count"),
            "independent_regime_count": state.get("independent_regime_count"),
            "verifier_failures": verifier.get("failures"),
        }
        code = contract.get("code") or {}
        source_contract = contract.get("source") or {}
        definition = {
            "hypothesis_id": str(cohort["cohort_id"]),
            "parent_hypothesis_id": str(cohort["source_cohort_id"]),
            "experiment_kind": "historical_sequential_portfolio_training",
            "research_generation": "sequential_portfolio_replay_v1",
            "idea_origin": "high_volume_deliberate_practice_with_real_portfolio_state",
            "pre_registered": 1,
            "data_sources_json": canonical_json([
                "independently_verified_frozen_oanda_m1_bid_ask_archives",
            ]),
            "feature_contract_json": canonical_json({
                "completed_m1_only": True,
                "global_decision_cadence_min": 5,
                "future_free_source_slices": True,
                "four_pair_opportunity_set": True,
            }),
            "label_contract_json": canonical_json({
                "historical_training_discovery_only": True,
                "one_primary_action_per_clock": True,
                "counterfactuals_count_as_repetitions": False,
                "independent_regimes": "unknown_one_inspected_window",
            }),
            "model_contract_json": canonical_json({
                "policy_id": contract.get("config", {}).get("frozen_policy", {}).get("policy_id"),
                "policy_frozen": True,
                "training_mechanics_baseline_only": True,
            }),
            "cost_contract_json": canonical_json(
                contract.get("config", {}).get("costs") or {}
            ),
            "allocator_contract_json": canonical_json({
                "actions": ["wait", "enter", "hold", "exit", "rotate"],
                "maximum_open_positions": 1,
                "one_minute_execution_delay": True,
                "rotation_has_two_execution_legs": True,
            }),
            "training_period_json": canonical_json({
                "start_epoch": session["start_epoch"],
                "end_epoch": session["end_epoch"],
                "already_inspected": True,
            }),
            "selection_period_json": canonical_json({
                "metadata_selected_friday_overlap_block": True,
                "outcome_selected": False,
            }),
            "confirmation_period_json": canonical_json({
                "none": True,
                "later_untouched_prospective_cohort_required": True,
            }),
            "all_parameters_tried_json": canonical_json({
                "one_frozen_policy": True,
                "depth_one_counterfactuals": True,
            }),
            "selection_rule": (
                "one frozen action policy over a predeclared four-hour training "
                "session; no result from this inspected session can promote"
            ),
            "holdouts_touched_json": canonical_json(["historical_training_discovery"]),
            "source_code_hash": stable_hash({
                key: value.get("sha256") for key, value in sorted(code.items())
            }),
            "data_snapshot_hash": stable_hash(source_contract),
            "definition_sha256": str(cohort["contract_sha256"]),
            "created_at": str(cohort["created_utc"]),
            "definition_json": canonical_json(contract),
        }
        definitions += int(insert_experiment(target, definition))
        observations += int(observe(
            target,
            hypothesis_id=str(cohort["cohort_id"]),
            observed_at=str(state.get("generated_utc") or cohort["created_utc"]),
            source_system=source.name,
            result="historical_sequential_portfolio_training",
            evidence={
                **statistics,
                "source_cohort_id": cohort["source_cohort_id"],
                "proof_eligible": False,
                "execution_eligible": False,
                "supported_decision": "no_trade",
            },
        ))
    finally:
        connection.close()
    return definitions, observations


def run(
    *, database: Path = DEFAULT_DATABASE, state: Path = DEFAULT_STATE,
    report: Path = DEFAULT_REPORT, root_state: Path = STATE,
) -> dict[str, Any]:
    observed = utc_now()
    target = connect(database)
    definition_count = observation_count = 0
    sources = [
        (root_state / "proof_cohort_registry_v1.sqlite", "proof_model"),
        (root_state / "candidate_cohort_registry_v1.sqlite", "strategy_candidate"),
    ]
    try:
        for source, kind in sources:
            definitions, observations = import_proof_registry(
                target, source, kind=kind, imported_at=observed
            )
            definition_count += definitions; observation_count += observations
        definitions, observations = import_allocator(
            target, root_state / "allocator_proof_v1.sqlite", imported_at=observed
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_lifecycle(
            target, root_state / "evidence_lifecycle_v1.sqlite", imported_at=observed
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_counterfactual_sim_gym(
            target,
            root_state.parent / "research_ledgers" /
            "counterfactual_sim_gym_v1" / "counterfactual_sim_gym_v1.sqlite",
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_sequential_deliberate_replay(
            target,
            root_state.parent / "research_ledgers" /
            "sequential_deliberate_replay_v1" / "sequential_deliberate_replay_v1.sqlite",
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_sequential_portfolio_replay(
            target,
            root_state.parent / "research_ledgers" /
            "sequential_portfolio_replay_v1",
        )
        definition_count += definitions; observation_count += observations
        discovery_reports = [
            (
                root_state.parent / "reports" / "cftc_positioning" / "CFTC_POSITIONING_DISCOVERY_20260808.json",
                "cftc_tff_positioning",
            ),
            (
                root_state.parent / "reports" / "rates" / "US_TREASURY_YIELD_DISCOVERY_20260808.json",
                "us_treasury_daily_yield_curve",
            ),
        ]
        for source, family in discovery_reports:
            definitions, observations = import_external_discovery_report(
                target, source, source_family=family
            )
            definition_count += definitions; observation_count += observations
        internal_discovery_reports = [
            root_state.parent / "reports" / "cost_clearance_cross_sectional" / "COST_CLEARANCE_CROSS_SECTIONAL_20260809.json",
            root_state.parent / "reports" / "movement_news_episode_research" / "MOVEMENT_NEWS_EPISODE_RESEARCH_20260809.json",
            root_state.parent / "reports" / "news_price_incremental_value" / "NEWS_PRICE_INCREMENTAL_VALUE_20260809.json",
        ]
        for source in internal_discovery_reports:
            definitions, observations = import_internal_discovery_artifact(target, source)
            definition_count += definitions; observation_count += observations
        direct_source_discovery_reports = [
            (
                root_state.parent / "reports" / "direct_source_response" /
                "DIRECT_SOURCE_HISTORICAL_REPLAY_20260816.json",
                "direct_source_all68_historical_response_20260816",
            ),
            (
                root_state.parent / "reports" / "direct_source_response" /
                "DIRECT_SOURCE_SIMPLE_RULES_20260816.json",
                "direct_source_simple_rules_20260816",
            ),
        ]
        for source, research_id in direct_source_discovery_reports:
            definitions, observations = import_internal_discovery_artifact(
                target,
                source,
                research_id_override=research_id,
            )
            definition_count += definitions; observation_count += observations
        definitions, observations = import_macro_point_in_time_validation(
            target,
            root_state.parent / "reports" / "macro_relative_strength" /
            "MACRO_POINT_IN_TIME_VALIDATION_20260816.json",
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_zero_output_adapter_retirement(target)
        definition_count += definitions; observation_count += observations
        definitions, observations = import_gdelt_mapping_report(
            target,
            root_state.parent / "reports" / "news_mapping" / "GDELT_ATTENTION_MAPPING_AUDIT_20260808.json",
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_prospective_source_state(
            target, root_state / "gdelt_attention_magnitude_prospective_v1.json"
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_treasury_source_state(
            target, root_state / "us_treasury_yield_prospective_v1.json"
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_alfred_source_state(
            target, root_state / "alfred_vintage_prospective_v1.json"
        )
        definition_count += definitions; observation_count += observations
        runtime_states = [
            (
                root_state / "executable_opportunity_prospective_v1.json",
                "executable_opportunity_ranking",
                ["OANDA_executable_bid_ask", "quote_intensity", "frozen_model_artifact"],
                "after_cost_direction_magnitude_and_cost_clearance",
                "apply the frozen executable-opportunity gate before the exact outcome horizon",
            ),
            (
                root_state / "direct_source_response_v1.json",
                "direct_numeric_source_response",
                ["official_macro_numeric_values", "OANDA_executable_bid_ask"],
                "source_native_currency_response_and_cost_clearance",
                "abstain unless a frozen causal source rule is available",
            ),
            (
                root_state / "news_technical_watchlist_v1.json",
                "news_technical_four_arm_watchlist",
                ["point_in_time_news", "technical_signal_snapshot", "OANDA_executable_bid_ask"],
                "four_arm_after_cost_market_response",
                "keep news-only, technical-only, aligned, and conflicted arms separate",
            ),
            (
                root_state / "pure_change_strategy_space_v1.json",
                "pure_change_strategy_space",
                [
                    "OANDA_practice_BAM_candles",
                    "OANDA_quote_intensity",
                    "official_macro_and_policy_publishers",
                    "CFTC_TFF_positioning",
                    "US_Treasury_daily_curve",
                    "point_in_time_news_blurbs",
                ],
                "after_cost_5m_10m_15m_opportunity_and_rule_space",
                "measure opportunity first; select signal-unique rules on development and validation only; require untouched confirmation",
            ),
        ]
        definitions, observations = import_model_artifact_manifest(
            target,
            ROOT / "data" / "oanda_training_manager" / "model_space" /
            "proof_cohorts" / "executable_opportunity_ranking_v1" / "manifest.json",
            idea_origin="executable_opportunity_ranking",
        )
        definition_count += definitions; observation_count += observations
        after_cost_v1 = root_state / "currency_state_after_cost_counterfactual_v1.json"
        after_cost_v2 = root_state / "currency_state_after_cost_counterfactual_v2.json"
        after_cost_v3 = root_state / "currency_state_after_cost_counterfactual_v3.json"
        for after_cost_source, registration_status in (
            (after_cost_v1, "frozen"),
            (after_cost_v2, "engineering_blocked"),
            (after_cost_v3, "engineering_blocked"),
        ):
            definitions, observations = import_currency_state_after_cost_state(
                target, after_cost_source,
                registration_status=registration_status,
            )
            definition_count += definitions; observation_count += observations
        observation_count += observe_currency_state_after_cost_supersession(
            target, after_cost_v1, after_cost_v2,
        )
        observation_count += observe_currency_state_after_cost_supersession(
            target, after_cost_v2, after_cost_v3,
        )
        for source_path, origin, data_sources, target_name, rule in runtime_states:
            definitions, observations = import_shadow_runtime_state(
                target, source_path, idea_origin=origin, data_sources=data_sources,
                label_target=target_name, selection_rule=rule,
            )
            definition_count += definitions; observation_count += observations
        totals = {
            "experiments": int(target.execute("SELECT COUNT(*) FROM experiments").fetchone()[0]),
            "observations": int(target.execute("SELECT COUNT(*) FROM experiment_observations").fetchone()[0]),
            "retired": int(target.execute("SELECT COUNT(DISTINCT hypothesis_id) FROM experiment_observations WHERE result='futility_rejected'").fetchone()[0]),
            "confirmed": int(target.execute("SELECT COUNT(DISTINCT hypothesis_id) FROM experiment_observations WHERE result='confirmed_candidate'").fetchone()[0]),
        }
        kinds = [
            {"kind": str(kind), "count": int(count)}
            for kind, count in target.execute(
                "SELECT experiment_kind,COUNT(*) FROM experiments GROUP BY experiment_kind ORDER BY experiment_kind"
            )
        ]
        fingerprint = stable_hash({"totals": totals, "kinds": kinds})
        target.execute(
            "INSERT OR IGNORE INTO genealogy_imports VALUES (?,?,?,?,?,?)",
            ("genealogy_import_" + fingerprint[:24], observed, str(root_state.resolve()), fingerprint, definition_count, observation_count),
        )
        target.commit()
    finally:
        target.close()
    payload = {
        "schema_version": 1, "generated_utc": observed, "status": "ok",
        "research_only": True, "can_place_orders": False, "can_promote": False,
        "registry": str(database.resolve()), "new_definitions": definition_count,
        "new_observations": observation_count, "totals": totals, "kinds": kinds,
        "coverage_note": (
            "All current lifecycle hypotheses plus proof, candidate, allocator, and dated external-source discovery cohorts are registered. "
            "Older undocumented parameter searches are explicitly marked not_reconstructed rather than invented."
        ),
    }
    lines = [
        "# FX research genealogy", "", f"Generated: `{observed}`", "",
        "Append-only and research-only; this registry cannot promote or execute.", "",
        f"- Experiments: **{totals['experiments']:,}**",
        f"- Observations: **{totals['observations']:,}**",
        f"- Retired/confirmed: **{totals['retired']:,} / {totals['confirmed']:,}**", "",
        "| Kind | Count |", "|---|---:|",
    ]
    lines.extend(f"| {row['kind']} | {row['count']:,} |" for row in kinds)
    lines.extend(["", payload["coverage_note"], ""])
    atomic_json(state, payload); atomic_text(report, "\n".join(lines))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--root-state", type=Path, default=STATE)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.time()
    while True:
        run(database=args.database, state=args.state, report=args.report, root_state=args.root_state)
        if args.interval_sec <= 0.0 or (
            args.duration_sec > 0.0 and time.time() - started >= args.duration_sec
        ):
            break
        time.sleep(max(60.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "connect",
    "import_external_discovery_report",
    "import_gdelt_mapping_report",
    "import_prospective_source_state",
    "import_treasury_source_state",
    "import_alfred_source_state",
    "import_shadow_runtime_state",
    "import_model_artifact_manifest",
    "import_macro_point_in_time_validation",
    "import_counterfactual_sim_gym",
    "import_sequential_deliberate_replay",
    "import_sequential_portfolio_replay",
    "insert_experiment",
    "observe",
    "run",
]
