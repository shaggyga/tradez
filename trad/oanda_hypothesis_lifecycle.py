#!/usr/bin/env python3
"""Terminal lifecycle and evidence velocity for governed FX hypotheses.

This module never forecasts, promotes, routes, or submits orders.  It consumes
the frozen proof registries and edge-evidence cache, persists permanent
futility decisions, and reports effective evidence rather than wall-clock age.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from oanda_edge_evidence import (
        EvidenceThresholds,
        alpha_spending_confidence_sequence,
        build_cell_results,
        collapse_factor_episodes,
        load_rows,
        minimum_economic_edge,
        positive_concentration,
        signed_currency_factors,
    )
except ModuleNotFoundError:
    from trad.oanda_edge_evidence import (
        EvidenceThresholds,
        alpha_spending_confidence_sequence,
        build_cell_results,
        collapse_factor_episodes,
        load_rows,
        minimum_economic_edge,
        positive_concentration,
        signed_currency_factors,
    )


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
DEFAULT_DATABASE = STATE / "evidence_lifecycle_v1.sqlite"
DEFAULT_STATE = STATE / "evidence_lifecycle_v1.json"
DEFAULT_CONFIG = ROOT / "config" / "evidence_operations_v1.json"
DEFAULT_GOVERNANCE_CONFIG = ROOT / "config" / "prospective_replication_governance_v1.json"
DEFAULT_EDGE_DATABASE = STATE / "edge_evidence_v1.sqlite"
DEFAULT_SOURCE_DATABASE = STATE / "strategy_shadow_outcomes_v1.sqlite"
DEFAULT_COHORT_STATE = STATE / "proof_cohort_registry_v1.json"

TERMINAL_STATES = {
    "continue_collecting",
    "futility_rejected",
    "confirmed_candidate",
}
LIFECYCLE_INTEGRITY_CONTRACT = "evidence_lifecycle_publication_integrity_v1"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return default
    return output if math.isfinite(output) else default


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def initialize_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=60.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS hypotheses (
            hypothesis_id TEXT PRIMARY KEY,
            evidence_contract_id TEXT NOT NULL,
            cohort_id TEXT,
            cell_id TEXT NOT NULL,
            family TEXT NOT NULL,
            instrument TEXT NOT NULL,
            horizon_sec INTEGER NOT NULL,
            session_bucket TEXT NOT NULL,
            liquidity_bucket TEXT NOT NULL,
            definition_sha256 TEXT NOT NULL,
            definition_json TEXT NOT NULL,
            first_seen_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS lifecycle_events (
            event_id TEXT PRIMARY KEY,
            hypothesis_id TEXT NOT NULL,
            previous_state TEXT,
            next_state TEXT NOT NULL,
            observed_utc TEXT NOT NULL,
            source_run_id TEXT NOT NULL,
            evidence_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS futility_retirements (
            hypothesis_id TEXT PRIMARY KEY,
            retired_utc TEXT NOT NULL,
            source_run_id TEXT NOT NULL,
            boundary_method TEXT NOT NULL,
            minimum_economic_edge_pips REAL NOT NULL,
            upper_bound_pips REAL NOT NULL,
            evidence_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS reconsideration_decisions (
            decision_id TEXT PRIMARY KEY,
            retired_hypothesis_id TEXT NOT NULL,
            proposed_hypothesis_id TEXT NOT NULL,
            material_change_class TEXT NOT NULL,
            accepted INTEGER NOT NULL,
            decided_utc TEXT NOT NULL,
            decision_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS lifecycle_initializations (
            initialization_id TEXT PRIMARY KEY,
            initialized_utc TEXT NOT NULL,
            source_highwater_row_id INTEGER NOT NULL,
            cell_count INTEGER NOT NULL,
            retirement_count INTEGER NOT NULL,
            evidence_sha256 TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS predictor_space_closures (
            closure_id TEXT PRIMARY KEY,
            closed_utc TEXT NOT NULL,
            scope TEXT NOT NULL,
            conclusion TEXT NOT NULL,
            evidence_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS lifecycle_events_hypothesis_observed_idx
            ON lifecycle_events(hypothesis_id,observed_utc DESC);
        CREATE INDEX IF NOT EXISTS hypotheses_contract_cell_idx
            ON hypotheses(evidence_contract_id,cell_id);
        CREATE INDEX IF NOT EXISTS lifecycle_events_state_idx
            ON lifecycle_events(next_state,hypothesis_id);
        CREATE TRIGGER IF NOT EXISTS hypotheses_no_update
        BEFORE UPDATE ON hypotheses BEGIN
            SELECT RAISE(ABORT, 'hypotheses are immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS hypotheses_no_delete
        BEFORE DELETE ON hypotheses BEGIN
            SELECT RAISE(ABORT, 'hypotheses are immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS lifecycle_events_no_update
        BEFORE UPDATE ON lifecycle_events BEGIN
            SELECT RAISE(ABORT, 'lifecycle events are immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS lifecycle_events_no_delete
        BEFORE DELETE ON lifecycle_events BEGIN
            SELECT RAISE(ABORT, 'lifecycle events are immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS futility_retirements_no_update
        BEFORE UPDATE ON futility_retirements BEGIN
            SELECT RAISE(ABORT, 'futility retirements are permanent');
        END;
        CREATE TRIGGER IF NOT EXISTS futility_retirements_no_delete
        BEFORE DELETE ON futility_retirements BEGIN
            SELECT RAISE(ABORT, 'futility retirements are permanent');
        END;
        CREATE TRIGGER IF NOT EXISTS reconsideration_no_update
        BEFORE UPDATE ON reconsideration_decisions BEGIN
            SELECT RAISE(ABORT, 'reconsideration decisions are immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS reconsideration_no_delete
        BEFORE DELETE ON reconsideration_decisions BEGIN
            SELECT RAISE(ABORT, 'reconsideration decisions are immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS initialization_no_update
        BEFORE UPDATE ON lifecycle_initializations BEGIN
            SELECT RAISE(ABORT, 'initialization records are immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS initialization_no_delete
        BEFORE DELETE ON lifecycle_initializations BEGIN
            SELECT RAISE(ABORT, 'initialization records are immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS predictor_space_closures_no_update
        BEFORE UPDATE ON predictor_space_closures BEGIN
            SELECT RAISE(ABORT, 'predictor-space conclusions are immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS predictor_space_closures_no_delete
        BEFORE DELETE ON predictor_space_closures BEGIN
            SELECT RAISE(ABORT, 'predictor-space conclusions are immutable');
        END;
        """
    )
    connection.commit()
    return connection


def hypothesis_definition(
    row: dict[str, Any],
    *,
    evidence_contract_id: str,
    cohort_id: str | None = None,
) -> tuple[str, dict[str, Any]]:
    definition = {
        "evidence_contract_id": evidence_contract_id,
        "cohort_id": cohort_id,
        "cell_id": str(row["cell_id"]),
        "family": str(row["family"]),
        "instrument": str(row["instrument"]),
        "horizon_sec": int(row["horizon_sec"]),
        "session_bucket": str(row.get("session") or row.get("session_bucket") or "unknown"),
        "liquidity_bucket": str(row.get("liquidity_bucket") or "unknown"),
    }
    return "hypothesis_" + stable_hash(definition)[:32], definition


def current_state(connection: sqlite3.Connection, hypothesis_id: str) -> str | None:
    row = connection.execute(
        """
        SELECT next_state FROM lifecycle_events
        WHERE hypothesis_id=? ORDER BY observed_utc DESC,rowid DESC LIMIT 1
        """,
        (hypothesis_id,),
    ).fetchone()
    return None if row is None else str(row[0])


def insert_hypothesis(
    connection: sqlite3.Connection,
    row: dict[str, Any],
    *,
    evidence_contract_id: str,
    cohort_id: str | None = None,
    observed_utc: str,
) -> str:
    hypothesis_id, definition = hypothesis_definition(
        row, evidence_contract_id=evidence_contract_id, cohort_id=cohort_id
    )
    connection.execute(
        """
        INSERT OR IGNORE INTO hypotheses VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            hypothesis_id,
            evidence_contract_id,
            cohort_id,
            definition["cell_id"],
            definition["family"],
            definition["instrument"],
            definition["horizon_sec"],
            definition["session_bucket"],
            definition["liquidity_bucket"],
            stable_hash(definition),
            canonical_json(definition),
            observed_utc,
        ),
    )
    return hypothesis_id


def transition(
    connection: sqlite3.Connection,
    *,
    hypothesis_id: str,
    next_state: str,
    observed_utc: str,
    source_run_id: str,
    evidence: dict[str, Any],
) -> bool:
    if next_state not in TERMINAL_STATES:
        raise ValueError(f"unknown lifecycle state: {next_state}")
    previous = current_state(connection, hypothesis_id)
    if previous == "futility_rejected" and next_state != previous:
        return False
    if previous == next_state:
        return False
    event = {
        "hypothesis_id": hypothesis_id,
        "previous_state": previous,
        "next_state": next_state,
        "observed_utc": observed_utc,
        "source_run_id": source_run_id,
        "evidence": evidence,
    }
    connection.execute(
        "INSERT OR IGNORE INTO lifecycle_events VALUES (?,?,?,?,?,?,?)",
        (
            "lifecycle_" + stable_hash(event)[:32],
            hypothesis_id,
            previous,
            next_state,
            observed_utc,
            source_run_id,
            canonical_json(evidence),
        ),
    )
    return True


def retire(
    connection: sqlite3.Connection,
    *,
    hypothesis_id: str,
    observed_utc: str,
    source_run_id: str,
    method: str,
    minimum_edge: float,
    upper_bound: float,
    evidence: dict[str, Any],
) -> bool:
    if connection.execute(
        "SELECT 1 FROM futility_retirements WHERE hypothesis_id=?", (hypothesis_id,)
    ).fetchone():
        return False
    payload = {
        **evidence,
        "reconsideration_requires_materially_new_hypothesis": True,
        "small_hyperparameter_or_rename_does_not_reset_negative_evidence": True,
    }
    connection.execute(
        "INSERT INTO futility_retirements VALUES (?,?,?,?,?,?,?)",
        (
            hypothesis_id,
            observed_utc,
            source_run_id,
            method,
            float(minimum_edge),
            float(upper_bound),
            canonical_json(payload),
        ),
    )
    transition(
        connection,
        hypothesis_id=hypothesis_id,
        next_state="futility_rejected",
        observed_utc=observed_utc,
        source_run_id=source_run_id,
        evidence={"boundary_method": method, **payload},
    )
    return True


def bootstrap_fixed_snapshot(
    connection: sqlite3.Connection,
    *,
    source_database: Path,
    config: dict[str, Any],
    governance_config_path: Path = DEFAULT_GOVERNANCE_CONFIG,
) -> dict[str, Any]:
    fixed = ((config.get("lifecycle") or {}).get("initial_fixed_snapshot_futility") or {})
    initialization_id = "initial_" + stable_hash(fixed)[:32]
    existing = connection.execute(
        "SELECT cell_count,retirement_count,evidence_sha256 FROM lifecycle_initializations WHERE initialization_id=?",
        (initialization_id,),
    ).fetchone()
    if existing:
        return {
            "status": "already_initialized",
            "cell_count": int(existing[0]),
            "retirement_count": int(existing[1]),
            "evidence_sha256": str(existing[2]),
        }
    if not fixed.get("enabled"):
        return {"status": "disabled", "cell_count": 0, "retirement_count": 0}
    highwater = int(fixed["source_highwater_row_id"])
    rows, integrity = load_rows(
        source_database,
        slippage_pips=0.25,
        maximum_delay_sec=15.0,
        maximum_row_id=highwater,
    )
    cells, _ = build_cell_results(
        rows,
        thresholds=EvidenceThresholds(),
        slippage_pips=0.25,
    )
    expected_cells = int(fixed["expected_cell_count"])
    if len(cells) != expected_cells:
        raise RuntimeError(
            f"fixed lifecycle snapshot cell mismatch: {len(cells)} != {expected_cells}"
        )
    observed = utc_now()
    contract_id = "pre_upgrade_governed_surface_" + str(fixed["governance_sha256"])[:16]
    retirement_count = 0
    for row in cells:
        hypothesis_id = insert_hypothesis(
            connection,
            row,
            evidence_contract_id=contract_id,
            observed_utc=observed,
        )
        upper = row.get("unadjusted_upper_confidence_pips")
        minimum_edge = finite(row.get("minimum_economic_edge_pips"))
        if upper is not None and finite(upper) < minimum_edge:
            retirement_count += int(
                retire(
                    connection,
                    hypothesis_id=hypothesis_id,
                    observed_utc=observed,
                    source_run_id=str(fixed["governance_sha256"]),
                    method=str(fixed["method"]),
                    minimum_edge=minimum_edge,
                    upper_bound=finite(upper),
                    evidence={
                        "cell_id": row["cell_id"],
                        "effective_n": row["effective_n"],
                        "avg_net_pips": row["avg_net_pips"],
                        "fixed_source_highwater_row_id": highwater,
                        "fixed_governance_sha256": fixed["governance_sha256"],
                        "ordinary_repeated_checks_forbidden": True,
                        "future_retirements_require_time_uniform_boundary": True,
                    },
                )
            )
        else:
            transition(
                connection,
                hypothesis_id=hypothesis_id,
                next_state="continue_collecting",
                observed_utc=observed,
                source_run_id=str(fixed["governance_sha256"]),
                evidence={
                    "fixed_source_highwater_row_id": highwater,
                    "minimum_economic_edge_pips": minimum_edge,
                    "upper_bound_pips": upper,
                },
            )
    expected_retirements = int(fixed["expected_retirement_count"])
    if retirement_count != expected_retirements:
        connection.rollback()
        raise RuntimeError(
            "fixed lifecycle snapshot retirement mismatch: "
            f"{retirement_count} != {expected_retirements}"
        )
    digest = stable_hash(
        {
            "highwater": highwater,
            "cells": [row["cell_id"] for row in cells],
            "retired": retirement_count,
            "integrity": integrity,
        }
    )
    connection.execute(
        "INSERT INTO lifecycle_initializations VALUES (?,?,?,?,?,?)",
        (initialization_id, observed, highwater, len(cells), retirement_count, digest),
    )
    connection.commit()
    return {
        "status": "initialized",
        "cell_count": len(cells),
        "retirement_count": retirement_count,
        "evidence_sha256": digest,
    }


def ingest_current_cells(
    connection: sqlite3.Connection,
    *,
    edge_database: Path,
    config: dict[str, Any],
    progress_callback: Any = None,
) -> dict[str, Any]:
    if not edge_database.exists():
        return {"status": "edge_database_missing", "cell_count": 0}
    source = sqlite3.connect(
        f"file:{edge_database.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    source.execute("PRAGMA query_only=ON")
    try:
        required = {
            str(row[0])
            for row in source.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('current_evidence_state','current_cell_evidence')"
            )
        }
        if required != {"current_evidence_state", "current_cell_evidence"}:
            return {"status": "current_cell_cache_unavailable", "cell_count": 0}
        state = source.execute(
            "SELECT run_id,generated_utc,source_highwater_row_id,cell_count FROM current_evidence_state WHERE singleton=1"
        ).fetchone()
        if not state:
            return {"status": "current_cell_cache_unavailable", "cell_count": 0}
        rows = source.execute(
            "SELECT evidence_json FROM current_cell_evidence ORDER BY cell_id"
        ).fetchall()
    finally:
        source.close()
    run_id, generated, highwater, expected_count = state
    contract_id = "pre_upgrade_governed_surface_" + str(
        (((config.get("lifecycle") or {}).get("initial_fixed_snapshot_futility") or {}).get("governance_sha256") or "unknown")
    )[:16]
    if progress_callback is not None:
        progress_callback(
            "ingesting_current_cells",
            {"candidate_cells": len(rows), "processed_cells": 0},
        )
    transitions = new_retirements = confirmations = 0
    for processed, (payload,) in enumerate(rows, start=1):
        if progress_callback is not None and processed % 500 == 0:
            progress_callback(
                "ingesting_current_cells",
                {
                    "candidate_cells": len(rows),
                    "processed_cells": processed - 1,
                    "new_continue_events": transitions,
                    "new_futility_retirements": new_retirements,
                    "new_confirmations": confirmations,
                },
            )
        row = json.loads(str(payload))
        hypothesis_id = insert_hypothesis(
            connection,
            row,
            evidence_contract_id=contract_id,
            observed_utc=str(generated),
        )
        existing_state = current_state(connection, hypothesis_id)
        if existing_state == "futility_rejected":
            continue
        if str(row.get("graduation_stage")) == "C_locked_prospective_confirmation":
            confirmations += int(
                transition(
                    connection,
                    hypothesis_id=hypothesis_id,
                    next_state="confirmed_candidate",
                    observed_utc=str(generated),
                    source_run_id=str(run_id),
                    evidence={"cell_id": row["cell_id"], "confirmation_is_untouched": True},
                )
            )
            continue
        upper = finite(row.get("time_uniform_upper_bound_pips"), math.inf)
        minimum_edge = finite(row.get("minimum_economic_edge_pips"))
        if upper < minimum_edge:
            new_retirements += int(
                retire(
                    connection,
                    hypothesis_id=hypothesis_id,
                    observed_utc=str(generated),
                    source_run_id=str(run_id),
                    method="time_uniform_upper_bound_below_minimum_economic_edge",
                    minimum_edge=minimum_edge,
                    upper_bound=upper,
                    evidence={
                        "cell_id": row["cell_id"],
                        "effective_n": row["effective_n"],
                        "source_highwater_row_id": int(highwater),
                        "sequential_method": row.get("sequential_method"),
                    },
                )
            )
        elif existing_state is None:
            transitions += int(
                transition(
                    connection,
                    hypothesis_id=hypothesis_id,
                    next_state="continue_collecting",
                    observed_utc=str(generated),
                    source_run_id=str(run_id),
                    evidence={
                        "cell_id": row["cell_id"],
                        "effective_n": row["effective_n"],
                        "distance_to_futility_pips": round(upper - minimum_edge, 6),
                    },
                )
            )
    connection.commit()
    if progress_callback is not None:
        progress_callback(
            "current_cell_ingest_complete",
            {
                "candidate_cells": len(rows),
                "processed_cells": len(rows),
                "new_continue_events": transitions,
                "new_futility_retirements": new_retirements,
                "new_confirmations": confirmations,
            },
        )
    return {
        "status": "ok",
        "run_id": str(run_id),
        "generated_utc": str(generated),
        "source_highwater_row_id": int(highwater),
        "cell_count": len(rows),
        "expected_cell_count": int(expected_count),
        "new_continue_events": transitions,
        "new_futility_retirements": new_retirements,
        "new_confirmations": confirmations,
    }


def register_reconsideration(
    connection: sqlite3.Connection,
    *,
    retired_hypothesis_id: str,
    proposed_hypothesis_id: str,
    material_change_class: str,
    config: dict[str, Any],
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    lifecycle = config.get("lifecycle") or {}
    allowed = set(lifecycle.get("permitted_reconsideration_material_changes") or ())
    rejected = set(lifecycle.get("insufficient_reconsideration_changes") or ())
    if not connection.execute(
        "SELECT 1 FROM futility_retirements WHERE hypothesis_id=?",
        (retired_hypothesis_id,),
    ).fetchone():
        raise ValueError("reconsideration requires a permanently retired hypothesis")
    accepted = material_change_class in allowed and material_change_class not in rejected
    decision = {
        "retired_hypothesis_id": retired_hypothesis_id,
        "proposed_hypothesis_id": proposed_hypothesis_id,
        "material_change_class": material_change_class,
        "accepted": accepted,
        "details": details or {},
        "decision_policy": "renames and small hyperparameter changes cannot erase negative evidence",
    }
    connection.execute(
        "INSERT OR IGNORE INTO reconsideration_decisions VALUES (?,?,?,?,?,?,?)",
        (
            "reconsideration_" + stable_hash(decision)[:32],
            retired_hypothesis_id,
            proposed_hypothesis_id,
            material_change_class,
            int(accepted),
            utc_now(),
            canonical_json(decision),
        ),
    )
    connection.commit()
    return decision


def _cohort_rows(
    source_database: Path,
    family: str,
    cohort_id: str,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    connection = sqlite3.connect(
        f"file:{source_database.resolve().as_posix()}?mode=ro", uri=True, timeout=60.0
    )
    connection.execute("PRAGMA query_only=ON")
    forecasts = connection.execute(
        """
        SELECT event_id,recorded_utc,kind,instrument,direction,entry_time,
               entry_spread_pips,session_bucket,liquidity_bucket,track_outcome,
               blocked_reason,forecast_json
        FROM canonical_forecasts
        WHERE family=? AND json_extract(forecast_json,'$.cohort_id')=?
        ORDER BY recorded_utc,event_id
        """,
        (family, cohort_id),
    ).fetchall()
    forecast_ids = {str(row[0]) for row in forecasts}
    outcomes = connection.execute(
        """
        SELECT o.event_id,o.horizon_sec,o.theoretical_pips,o.max_favorable_pips,
               o.max_adverse_pips,o.configured_stop_hit_sec,
               o.configured_target_hit_sec,o.outcome_delay_sec
        FROM canonical_outcomes o
        JOIN canonical_forecasts f ON f.event_id=o.event_id
        WHERE f.family=? AND json_extract(f.forecast_json,'$.cohort_id')=?
        ORDER BY o.entry_time,o.event_id
        """,
        (family, cohort_id),
    ).fetchall()
    integrity = dict(
        connection.execute(
            """
            SELECT i.event_type,COUNT(*) FROM forecast_integrity_events i
            JOIN canonical_forecasts f ON f.event_id=i.event_id
            WHERE f.family=? AND json_extract(f.forecast_json,'$.cohort_id')=?
            GROUP BY i.event_type
            """,
            (family, cohort_id),
        ).fetchall()
    )
    connection.close()
    forecast_by_id = {str(row[0]): row for row in forecasts}
    result: list[dict[str, Any]] = []
    for outcome in outcomes:
        event_id = str(outcome[0])
        forecast = forecast_by_id.get(event_id)
        if forecast is None:
            continue
        try:
            entered = datetime.fromisoformat(str(forecast[5]).replace("Z", "+00:00"))
        except ValueError:
            continue
        instrument, direction = str(forecast[3]), str(forecast[4])
        executable = finite(outcome[2])
        result.append(
            {
                "event_id": event_id,
                "entry_epoch": entered.timestamp(),
                "entry_day": entered.date().isoformat(),
                "entry_week": f"{entered.isocalendar().year}-W{entered.isocalendar().week:02d}",
                "instrument": instrument,
                "direction": direction,
                "session": str(forecast[7]),
                "liquidity": str(forecast[8]),
                "horizon_sec": int(outcome[1]),
                "spread_pips": finite(forecast[6]),
                "executable_pips": executable,
                "net_pips": executable - 0.25,
                "factors": signed_currency_factors(instrument, direction),
                "max_favorable_pips": finite(outcome[3]),
                "max_adverse_pips": finite(outcome[4]),
                "configured_stop_hit_sec": outcome[5],
                "configured_target_hit_sec": outcome[6],
                "outcome_delay_sec": finite(outcome[7]),
            }
        )
    diagnostics = {
        "produced": len(forecasts),
        "tracked": sum(int(row[9] or 0) for row in forecasts),
        "miss": sum(str(row[2]) == "miss" for row in forecasts),
        "blocked": sum(bool(str(row[10] or "")) for row in forecasts),
        "direction_conflict": 0,
        "cost_blocked": 0,
        "invalid_data": sum(int(value) for value in integrity.values()),
    }
    for forecast in forecasts:
        try:
            payload = json.loads(str(forecast[11]))
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        diagnostics["direction_conflict"] += int(bool(payload.get("direction_conflict")))
        blockers = payload.get("signal_blocked_by") or []
        diagnostics["cost_blocked"] += int(
            any("cost" in str(reason) or "spread" in str(reason) for reason in blockers)
        )
    diagnostics["forecast_ids"] = len(forecast_ids)
    return result, diagnostics


def proof_cohort_velocity(
    *,
    source_database: Path,
    cohort_state_path: Path,
    governance_config_path: Path = DEFAULT_GOVERNANCE_CONFIG,
) -> list[dict[str, Any]]:
    cohort_state = read_json(cohort_state_path)
    governance = read_json(governance_config_path)
    sequential = governance.get("sequential") or {}
    cohort_specs = {
        str(row.get("cohort_id")): row for row in cohort_state.get("cohorts") or []
    }
    output: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    for family, cohort_id in sorted((cohort_state.get("active_cohorts") or {}).items()):
        specification = cohort_specs.get(str(cohort_id), {})
        rows, exclusions = _cohort_rows(source_database, str(family), str(cohort_id))
        by_horizon: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_horizon[int(row["horizon_sec"])].append(row)
        episodes: list[dict[str, Any]] = []
        for horizon, members in by_horizon.items():
            episodes.extend(collapse_factor_episodes(members, horizon))
        episodes.sort(key=lambda row: (row["entry_epoch"], row["episode_id"]))
        values = [finite(row["net_pips"]) for row in episodes]
        minimum_edges = [
            minimum_economic_edge(
                3600, str(row.get("liquidity") or "wide_gt_5"), governance
            )
            for row in episodes
        ]
        excess = [value - edge for value, edge in zip(values, minimum_edges)]
        bound_map = sequential.get("clip_pips_by_max_horizon_sec") or {}
        bound = finite(bound_map.get("3600"), 60.0)
        lcb_excess, ucb_excess = alpha_spending_confidence_sequence(
            excess,
            alpha=finite(sequential.get("alpha"), 0.01),
            bound=bound,
        )
        std = statistics.stdev(values) if len(values) >= 2 else None
        average_edge = statistics.fmean(minimum_edges) if minimum_edges else 0.0
        mde = 2.487 * std / math.sqrt(len(values)) if std is not None else None
        required = (
            int(math.ceil((2.487 * std / max(1e-9, average_edge)) ** 2))
            if std is not None and std > 0.0 and average_edge > 0.0
            else None
        )
        start_text = str(specification.get("cohort_start_utc") or "")
        try:
            started = datetime.fromisoformat(start_text.replace("Z", "+00:00"))
            elapsed_days = max(1.0 / 1440.0, (now - started).total_seconds() / 86400.0)
        except ValueError:
            elapsed_days = 0.0
        cell_groups: Counter[tuple[str, str, str]] = Counter(
            (row["instrument"], row["session"], row["liquidity"]) for row in rows
        )
        liquidity_evidence: dict[str, dict[str, Any]] = {}
        for liquidity in sorted({str(row["liquidity"]) for row in rows}):
            bucket_rows = [row for row in rows if str(row["liquidity"]) == liquidity]
            bucket_episodes: list[dict[str, Any]] = []
            for horizon in sorted({int(row["horizon_sec"]) for row in bucket_rows}):
                bucket_episodes.extend(
                    collapse_factor_episodes(
                        [row for row in bucket_rows if int(row["horizon_sec"]) == horizon],
                        horizon,
                    )
                )
            bucket_values = [finite(row["net_pips"]) for row in bucket_episodes]
            bucket_edge = minimum_economic_edge(3600, liquidity, governance)
            bucket_excess = [value - bucket_edge for value in bucket_values]
            bucket_lcb, bucket_ucb = alpha_spending_confidence_sequence(
                bucket_excess,
                alpha=finite(sequential.get("alpha"), 0.01),
                bound=bound,
            )
            liquidity_evidence[liquidity] = {
                "raw_n": len(bucket_rows),
                "effective_n": len(bucket_episodes),
                "average_after_cost_pips": round(statistics.fmean(bucket_values), 6)
                if bucket_values else None,
                "minimum_economic_edge_pips": round(bucket_edge, 6),
                "time_uniform_excess_lcb_pips": round(bucket_lcb, 6),
                "time_uniform_excess_ucb_pips": round(bucket_ucb, 6),
                "point_estimate_is_diagnostic_only": len(bucket_episodes) < 2,
            }
        state = (
            "futility_rejected"
            if episodes and ucb_excess < 0.0
            else "continue_collecting"
        )
        output.append(
            {
                "family": str(family),
                "cohort_id": str(cohort_id),
                "cohort_start_utc": start_text or None,
                "lifecycle_state": state,
                "produced_forecasts": exclusions["produced"],
                "matured_forecasts": len(rows),
                "raw_n": len(rows),
                "effective_n": len(episodes),
                "independent_episodes": len(episodes),
                "independent_currency_factor_clusters": len(episodes),
                "distinct_signed_currency_factors": len(
                    set().union(*(signed_currency_factors(row["instrument"], row["direction"]) for row in rows))
                ) if rows else 0,
                "direct_cell_count": len(cell_groups),
                "effective_observations_per_day": round(len(episodes) / elapsed_days, 6)
                if elapsed_days else 0.0,
                "forecast_production_per_day": round(exclusions["produced"] / elapsed_days, 6)
                if elapsed_days else 0.0,
                "current_after_cost_ev_pips": round(statistics.fmean(values), 6)
                if values else None,
                "minimum_economic_edge_pips": round(average_edge, 6)
                if minimum_edges else None,
                "time_uniform_excess_lcb_pips": round(lcb_excess, 6),
                "time_uniform_excess_ucb_pips": round(ucb_excess, 6),
                "minimum_detectable_effect_pips": round(mde, 6) if mde is not None else None,
                "distance_to_promotion_boundary_pips": round(lcb_excess, 6),
                "distance_to_futility_boundary_pips": round(ucb_excess, 6),
                "additional_effective_n_for_configured_power": (
                    max(0, required - len(episodes)) if required is not None else None
                ),
                "point_estimate_is_diagnostic_only": len(episodes) < 2,
                "liquidity_bucket_evidence": liquidity_evidence,
                "best_pair_profit_share": round(positive_concentration(episodes, "instrument"), 6),
                "best_session_profit_share": round(positive_concentration(episodes, "session"), 6),
                "best_day_profit_share": round(positive_concentration(episodes, "entry_day"), 6),
                "best_episode_profit_share": round(
                    max((max(0.0, value) for value in values), default=0.0)
                    / max(1e-12, sum(max(0.0, value) for value in values)),
                    6,
                ) if values else 0.0,
                "exclusions": {
                    **{key: value for key, value in exclusions.items() if key != "forecast_ids"},
                    "conflict_pct": round(100.0 * exclusions["direction_conflict"] / exclusions["produced"], 6)
                    if exclusions["produced"] else 0.0,
                    "cost_pct": round(100.0 * exclusions["cost_blocked"] / exclusions["produced"], 6)
                    if exclusions["produced"] else 0.0,
                    "invalid_pct": round(100.0 * exclusions["invalid_data"] / exclusions["produced"], 6)
                    if exclusions["produced"] else 0.0,
                },
                "pooled_model_confidence_cannot_promote": str(family)
                in {"modern_tabular_probabilistic_repaired", "cross_pair_graph_transfer"},
                "direct_cell_prospective_evidence_required": True,
                "observation_frequency_policy": "frozen_15_minute_cadence; no adaptive pair or frequency changes",
            }
        )
    return output


def lifecycle_summary(connection: sqlite3.Connection) -> dict[str, Any]:
    counts = dict(
        connection.execute(
            """
            SELECT e.next_state,COUNT(*) FROM lifecycle_events e
            JOIN (
                SELECT hypothesis_id,MAX(rowid) AS rowid
                FROM lifecycle_events GROUP BY hypothesis_id
            ) latest ON latest.rowid=e.rowid
            GROUP BY e.next_state
            """
        ).fetchall()
    )
    return {
        "hypothesis_count": int(connection.execute("SELECT COUNT(*) FROM hypotheses").fetchone()[0]),
        "states": {state: int(counts.get(state, 0)) for state in sorted(TERMINAL_STATES)},
        "permanent_futility_retirement_count": int(
            connection.execute("SELECT COUNT(*) FROM futility_retirements").fetchone()[0]
        ),
        "accepted_reconsideration_count": int(
            connection.execute("SELECT COUNT(*) FROM reconsideration_decisions WHERE accepted=1").fetchone()[0]
        ),
        "rejected_reconsideration_count": int(
            connection.execute("SELECT COUNT(*) FROM reconsideration_decisions WHERE accepted=0").fetchone()[0]
        ),
    }


def lifecycle_publication_integrity(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    """Bind the aggregate publication to the exact append-only DB state.

    Lifecycle evaluation can legitimately take longer than the verifier's
    ordinary publication-age budget.  The independent verifier may accept an
    older, fail-closed publication while that calculation is running only when
    this fingerprint still matches a fresh read of the database.  Include the
    complete latest event (including confirmation evidence) for every
    hypothesis so equal aggregate counts cannot disguise a changed candidate.
    """

    current_rows = [
        {
            "hypothesis_id": str(row[0]),
            "cohort_id": None if row[1] is None else str(row[1]),
            "definition_sha256": str(row[2]),
            "event_rowid": int(row[3]),
            "next_state": str(row[4]),
            "observed_utc": str(row[5]),
            "source_run_id": str(row[6]),
            "evidence_json": str(row[7]),
        }
        for row in connection.execute(
            """
            SELECT hypothesis.hypothesis_id,hypothesis.cohort_id,
                   hypothesis.definition_sha256,event.rowid,event.next_state,
                   event.observed_utc,event.source_run_id,event.evidence_json
            FROM hypotheses AS hypothesis
            JOIN lifecycle_events AS event
              ON event.hypothesis_id=hypothesis.hypothesis_id
            JOIN (
                SELECT hypothesis_id,MAX(rowid) AS latest_rowid
                FROM lifecycle_events GROUP BY hypothesis_id
            ) AS latest ON latest.latest_rowid=event.rowid
            ORDER BY hypothesis.hypothesis_id
            """
        )
    ]

    def table_extent(table: str) -> tuple[int, int]:
        count, highwater = connection.execute(
            f"SELECT COUNT(*),COALESCE(MAX(rowid),0) FROM {table}"
        ).fetchone()
        return int(count), int(highwater)

    hypothesis_count, hypothesis_highwater = table_extent("hypotheses")
    event_count, event_highwater = table_extent("lifecycle_events")
    retirement_count, retirement_highwater = table_extent("futility_retirements")
    reconsideration_count, reconsideration_highwater = table_extent(
        "reconsideration_decisions"
    )
    current_states = Counter(row["next_state"] for row in current_rows)
    return {
        "contract": LIFECYCLE_INTEGRITY_CONTRACT,
        "hypothesis_count": hypothesis_count,
        "hypothesis_highwater_rowid": hypothesis_highwater,
        "lifecycle_event_count": event_count,
        "lifecycle_event_highwater_rowid": event_highwater,
        "futility_retirement_count": retirement_count,
        "futility_retirement_highwater_rowid": retirement_highwater,
        "reconsideration_count": reconsideration_count,
        "reconsideration_highwater_rowid": reconsideration_highwater,
        "current_state_count": len(current_rows),
        "current_states": {
            state: int(current_states.get(state, 0))
            for state in sorted(TERMINAL_STATES)
        },
        "current_state_sha256": stable_hash(current_rows),
    }


def run_lifecycle(
    *,
    database_path: Path = DEFAULT_DATABASE,
    state_path: Path = DEFAULT_STATE,
    config_path: Path = DEFAULT_CONFIG,
    edge_database: Path = DEFAULT_EDGE_DATABASE,
    source_database: Path = DEFAULT_SOURCE_DATABASE,
    cohort_state_path: Path = DEFAULT_COHORT_STATE,
    progress_callback: Any = None,
    post_ingest_sync: Any = None,
) -> dict[str, Any]:
    config = read_json(config_path)
    connection = initialize_database(database_path)
    try:
        # The velocity pass scans the large immutable outcome surface and may
        # take many minutes.  Run it before committing any lifecycle change so
        # the independent verifier never observes a new database high-water
        # paired with the previous published state merely because this
        # read-only calculation is still running.
        if progress_callback is not None:
            progress_callback("measuring_proof_cohort_velocity", {})
        velocity = proof_cohort_velocity(
            source_database=source_database,
            cohort_state_path=cohort_state_path,
        )
        if progress_callback is not None:
            progress_callback("checking_fixed_snapshot", {})
        bootstrap = bootstrap_fixed_snapshot(
            connection,
            source_database=source_database,
            config=config,
        )
        current = ingest_current_cells(
            connection,
            edge_database=edge_database,
            config=config,
            progress_callback=progress_callback,
        )
        genealogy_sync = {
            "ok": True,
            "status": "not_requested",
        }
        if post_ingest_sync is not None:
            if progress_callback is not None:
                progress_callback("synchronizing_lifecycle_genealogy", {})
            result = post_ingest_sync(database_path)
            if not isinstance(result, dict):
                raise RuntimeError("post-ingest lifecycle sync returned no result")
            genealogy_sync = dict(result)
            if not bool(genealogy_sync.get("ok")):
                raise RuntimeError(
                    "post-ingest lifecycle genealogy synchronization failed: "
                    f"{genealogy_sync}"
                )
        if progress_callback is not None:
            progress_callback("summarizing_lifecycle", {})
        summary = lifecycle_summary(connection)
        if progress_callback is not None:
            progress_callback("fingerprinting_lifecycle_publication", {})
        integrity = lifecycle_publication_integrity(connection)
    finally:
        connection.close()
    payload = {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "phase": str(config.get("phase") or "unknown"),
        "research_only": True,
        "can_place_orders": False,
        "can_promote": False,
        "real_money_routing": False,
        "terminal_states": sorted(TERMINAL_STATES),
        "bootstrap": bootstrap,
        "current_ingest": current,
        "integrity": integrity,
        "genealogy_sync": genealogy_sync,
        "lifecycle": summary,
        "proof_cohort_evidence_velocity": velocity,
        "post_null_rule": (
            "a retired price-derived hypothesis may return only through a materially "
            "new information source, target, decision policy, causal variable, "
            "executable observation, horizon, regime, or structural relationship"
        ),
        "macro_consensus": {
            "state": "blocked_until_causally_valid_pre_release_consensus_exists",
            "post_release_backfill_forbidden": True,
        },
    }
    atomic_json(state_path, payload)
    return payload


__all__ = [
    "bootstrap_fixed_snapshot",
    "initialize_database",
    "ingest_current_cells",
    "lifecycle_publication_integrity",
    "proof_cohort_velocity",
    "register_reconsideration",
    "run_lifecycle",
]
