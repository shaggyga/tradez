#!/usr/bin/env python3
"""Run one frozen, research-only sequential portfolio replay session.

This component reads only the independently verified archived M1 sources.  It
cannot contact OANDA, publish signals, alter lifecycle state, authorize a
canary, or place an order.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterable, Mapping, Sequence

from src.forex_system.research.sequential_portfolio_replay_v1 import (
    POLICY,
    Candidate,
    Decision,
    PortfolioState,
    apply_decision,
    build_global_clocks,
    candidate_set,
    canonical_json,
    causal_snapshot,
    choose_primary_decision,
    decision_payload,
    feedback_components,
    leg_payload,
    legal_branches,
    liquidation_equity,
    load_archived_market,
    market_episode_id,
    physical_path_id,
    stable_hash,
    state_payload,
    unsigned_currency_resources,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "sequential_portfolio_replay_v1.json"
CORE_PATH = ROOT / "src" / "forex_system" / "research" / "sequential_portfolio_replay_v1.py"
VERIFIER_PATH = ROOT / "oanda_sequential_portfolio_replay_verifier.py"

TABLES = {
    "spr_cohorts",
    "spr_sessions",
    "spr_pair_contexts",
    "spr_clocks",
    "spr_decisions",
    "spr_execution_outcomes",
    "spr_execution_legs",
    "spr_counterfactuals",
    "spr_feedback",
    "spr_administrative_events",
    "spr_session_seals",
    "spr_snapshots",
    "spr_integrity_events",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso_epoch(value: str) -> int:
    return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())


def file_sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                return digest.hexdigest()
            digest.update(chunk)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(value, encoding="utf-8")
    temporary.replace(path)


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    atomic_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def _is_link(path: Path) -> bool:
    return path.is_symlink() or bool(path.exists() and path.stat().st_file_attributes & 0x400)


def safe_artifact_root(config: Mapping[str, Any]) -> Path:
    candidate = ROOT / str(config["storage"]["artifact_relative_root"])
    allowed = (ROOT / "data" / "oanda_training_manager" / "research_ledgers").resolve()
    candidate.parent.mkdir(parents=True, exist_ok=True)
    if _is_link(candidate.parent):
        raise ValueError(f"artifact parent is a link/junction: {candidate.parent}")
    resolved = candidate.resolve()
    if resolved != allowed and allowed not in resolved.parents:
        raise ValueError(f"artifact path escapes research ledger root: {candidate}")
    if candidate.exists() and _is_link(candidate):
        raise ValueError(f"artifact root is a link/junction: {candidate}")
    candidate.mkdir(parents=True, exist_ok=True)
    return candidate


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def validate_config(config: Mapping[str, Any]) -> None:
    for key, expected in POLICY.items():
        if config.get(key) != expected:
            raise ValueError(f"research isolation mismatch: {key}")
    if config.get("evidence_role") != "historical_training_discovery":
        raise ValueError("historical session must remain training/discovery")
    source = config["source"]
    session = config["session"]
    counterfactuals = config["counterfactuals"]
    if source.get("historical_source_is_already_inspected") is not True:
        raise ValueError("source must remain labeled already inspected")
    if int(session.get("maximum_open_positions") or 0) != 1:
        raise ValueError("V1 is a one-position portfolio")
    if int(session.get("decision_cadence_min") or 0) != 5:
        raise ValueError("V1 cadence is frozen at five minutes")
    if int(session.get("execution_delay_min") or 0) != 1:
        raise ValueError("V1 requires the one-minute executable baseline")
    if session.get("adaptive_sampling_forbidden") is not True or session.get("adaptive_policy_forbidden") is not True:
        raise ValueError("adaptive replay is forbidden")
    if counterfactuals.get("count_as_market_repetition") is not False:
        raise ValueError("counterfactuals cannot count as repetitions")
    costs = config["costs"]
    if abs(
        2.0 * float(costs["slippage_per_execution_leg_pips"])
        - float(costs["round_trip_slippage_pips"])
    ) > 1e-12:
        raise ValueError("per-leg slippage must reproduce frozen round-trip slippage")
    if costs.get("spread_is_paid_through_executable_bid_ask") is not True:
        raise ValueError("spread must remain embedded in executable quotes")
    if config["frozen_policy"].get("no_interim_retuning") is not True:
        raise ValueError("policy must remain frozen")


def output_connection(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=60.0, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    existing = {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    unexpected = sorted(existing - TABLES)
    if unexpected:
        connection.close()
        raise ValueError(f"refusing non-portfolio-replay database: {unexpected}")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS spr_cohorts (
          cohort_id TEXT PRIMARY KEY,created_utc TEXT NOT NULL,source_cohort_id TEXT NOT NULL,
          contract_sha256 TEXT NOT NULL,contract_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS spr_sessions (
          session_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,start_epoch INTEGER NOT NULL,
          end_epoch INTEGER NOT NULL,evidence_role TEXT NOT NULL,proof_eligible INTEGER NOT NULL,
          universe_json TEXT NOT NULL,initial_state_json TEXT NOT NULL,row_sha256 TEXT NOT NULL,
          FOREIGN KEY(cohort_id) REFERENCES spr_cohorts(cohort_id)
        );
        CREATE TABLE IF NOT EXISTS spr_pair_contexts (
          pair_context_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,session_id TEXT NOT NULL,
          clock_id TEXT NOT NULL,decision_epoch INTEGER NOT NULL,instrument TEXT NOT NULL,
          pair_alias TEXT NOT NULL,input_slice_sha256 TEXT NOT NULL,candidate_snapshot_id TEXT,
          causal_sha256 TEXT NOT NULL,causal_json TEXT NOT NULL,row_sha256 TEXT NOT NULL,
          UNIQUE(session_id,decision_epoch,instrument),
          FOREIGN KEY(session_id) REFERENCES spr_sessions(session_id)
        );
        CREATE TABLE IF NOT EXISTS spr_clocks (
          clock_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,session_id TEXT NOT NULL,
          sequence_no INTEGER NOT NULL,decision_epoch INTEGER NOT NULL,execution_epoch INTEGER NOT NULL,
          feedback_epoch INTEGER NOT NULL,market_episode_id TEXT NOT NULL,
          price_snapshot_id TEXT NOT NULL,exact_situation_id TEXT NOT NULL,
          coarse_situation_id TEXT NOT NULL,causal_sha256 TEXT NOT NULL,causal_json TEXT NOT NULL,
          row_sha256 TEXT NOT NULL,UNIQUE(session_id,sequence_no),UNIQUE(session_id,decision_epoch),
          FOREIGN KEY(session_id) REFERENCES spr_sessions(session_id)
        );
        CREATE TABLE IF NOT EXISTS spr_decisions (
          decision_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,session_id TEXT NOT NULL,
          clock_id TEXT NOT NULL,sequence_no INTEGER NOT NULL,committed_epoch INTEGER NOT NULL,
          action TEXT NOT NULL,instrument TEXT,side INTEGER,units INTEGER NOT NULL,
          position_thesis_id_before TEXT,evidence_role TEXT NOT NULL,proof_eligible INTEGER NOT NULL,
          state_before_sha256 TEXT NOT NULL,state_before_json TEXT NOT NULL,
          decision_sha256 TEXT NOT NULL,decision_json TEXT NOT NULL,row_sha256 TEXT NOT NULL,
          UNIQUE(session_id,clock_id),FOREIGN KEY(clock_id) REFERENCES spr_clocks(clock_id)
        );
        CREATE TABLE IF NOT EXISTS spr_execution_outcomes (
          outcome_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,session_id TEXT NOT NULL,
          decision_id TEXT NOT NULL,status TEXT NOT NULL,rejection_reason TEXT NOT NULL,
          realized_delta_pips REAL NOT NULL,state_after_sha256 TEXT NOT NULL,
          state_after_json TEXT NOT NULL,row_sha256 TEXT NOT NULL,
          UNIQUE(decision_id),FOREIGN KEY(decision_id) REFERENCES spr_decisions(decision_id)
        );
        CREATE TABLE IF NOT EXISTS spr_execution_legs (
          leg_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,session_id TEXT NOT NULL,
          parent_kind TEXT NOT NULL,parent_id TEXT NOT NULL,clock_id TEXT,
          leg_sequence INTEGER NOT NULL,leg_kind TEXT NOT NULL,instrument TEXT NOT NULL,
          side INTEGER NOT NULL,execution_epoch INTEGER NOT NULL,realized_pips REAL NOT NULL,
          leg_sha256 TEXT NOT NULL,leg_json TEXT NOT NULL,row_sha256 TEXT NOT NULL,
          UNIQUE(parent_id,leg_sequence)
        );
        CREATE TABLE IF NOT EXISTS spr_counterfactuals (
          counterfactual_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,session_id TEXT NOT NULL,
          decision_id TEXT NOT NULL,branch_label TEXT NOT NULL,counts_as_rep INTEGER NOT NULL,
          status TEXT NOT NULL,rejection_reason TEXT NOT NULL,terminal_equity_pips REAL NOT NULL,
          action_sha256 TEXT NOT NULL,action_json TEXT NOT NULL,result_sha256 TEXT NOT NULL,
          result_json TEXT NOT NULL,row_sha256 TEXT NOT NULL,
          UNIQUE(decision_id,branch_label),FOREIGN KEY(decision_id) REFERENCES spr_decisions(decision_id)
        );
        CREATE TABLE IF NOT EXISTS spr_feedback (
          feedback_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,session_id TEXT NOT NULL,
          decision_id TEXT NOT NULL,revealed_epoch INTEGER NOT NULL,primary_terminal_equity_pips REAL NOT NULL,
          best_alternative_equity_pips REAL NOT NULL,physical_path_id TEXT NOT NULL,
          currency_resources_json TEXT NOT NULL,components_sha256 TEXT NOT NULL,
          components_json TEXT NOT NULL,row_sha256 TEXT NOT NULL,
          UNIQUE(decision_id),FOREIGN KEY(decision_id) REFERENCES spr_decisions(decision_id)
        );
        CREATE TABLE IF NOT EXISTS spr_administrative_events (
          event_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,session_id TEXT NOT NULL,
          event_epoch INTEGER NOT NULL,event_type TEXT NOT NULL,event_sha256 TEXT NOT NULL,
          event_json TEXT NOT NULL,row_sha256 TEXT NOT NULL,
          FOREIGN KEY(session_id) REFERENCES spr_sessions(session_id)
        );
        CREATE TABLE IF NOT EXISTS spr_session_seals (
          seal_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,session_id TEXT NOT NULL,
          previous_seal_id TEXT,created_utc TEXT NOT NULL,seal_sha256 TEXT NOT NULL,
          seal_json TEXT NOT NULL,FOREIGN KEY(session_id) REFERENCES spr_sessions(session_id)
        );
        CREATE TABLE IF NOT EXISTS spr_snapshots (
          snapshot_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,session_id TEXT NOT NULL,
          generated_utc TEXT NOT NULL,statistics_sha256 TEXT NOT NULL,statistics_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS spr_integrity_events (
          integrity_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,session_id TEXT,
          observed_utc TEXT NOT NULL,severity TEXT NOT NULL,event_sha256 TEXT NOT NULL,event_json TEXT NOT NULL
        );
        """
    )
    for table in sorted(TABLES):
        connection.execute(
            f"CREATE TRIGGER IF NOT EXISTS {table}_no_update BEFORE UPDATE ON {table} BEGIN SELECT RAISE(ABORT,'append_only'); END"
        )
        connection.execute(
            f"CREATE TRIGGER IF NOT EXISTS {table}_no_delete BEFORE DELETE ON {table} BEGIN SELECT RAISE(ABORT,'append_only'); END"
        )
    return connection


def _insert_immutable(
    connection: sqlite3.Connection,
    *,
    table: str,
    key_column: str,
    key: str,
    hash_column: str,
    expected_hash: str,
    columns: Sequence[str],
    values: Sequence[Any],
) -> bool:
    row = connection.execute(
        f"SELECT {hash_column} FROM {table} WHERE {key_column}=?", (key,)
    ).fetchone()
    if row is not None:
        if str(row[0]) != str(expected_hash):
            raise ValueError(f"immutable conflict: {table}:{key}")
        return False
    connection.execute(
        f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
        tuple(values),
    )
    return True


def _table_root(
    connection: sqlite3.Connection,
    table: str,
    hash_column: str,
    session_id: str | None = None,
) -> dict[str, Any]:
    sql = f"SELECT {hash_column} FROM {table}"
    params: tuple[Any, ...] = ()
    if session_id is not None:
        sql += " WHERE session_id=?"
        params = (session_id,)
    hashes = sorted(str(row[0]) for row in connection.execute(sql, params))
    return {"count": len(hashes), "set_sha256": stable_hash(hashes)}


def _source_contract(config: Mapping[str, Any]) -> tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any]]:
    source_root = (ROOT / str(config["source"]["artifact_relative_root"])).resolve()
    state_path = source_root / str(config["source"]["state_name"])
    verifier_path = source_root / str(config["source"]["verifier_name"])
    database_path = source_root / str(config["source"]["database_name"])
    state = read_json(state_path)
    verifier = read_json(verifier_path)
    required = str(config["source"]["required_cohort_id"])
    if state.get("cohort_id") != required or verifier.get("cohort_id") != required:
        raise ValueError("source cohort mismatch")
    if config["source"].get("require_verified") is True and verifier.get("verified") is not True:
        raise ValueError("source verifier is not valid")
    if config["source"].get("require_zero_failures") is True and verifier.get("failures"):
        raise ValueError("source verifier contains failures")
    contract = {
        "source_cohort_id": required,
        "source_state_sha256": file_sha256(state_path),
        "source_verifier_sha256": file_sha256(verifier_path),
        "source_database_sha256": file_sha256(database_path),
        "source_manifest": state["source_manifest"],
        "source_window": state["window"],
    }
    return source_root, state, verifier, contract


def _archive_contract(path: Path, artifact_root: Path, label: str) -> dict[str, str]:
    digest = file_sha256(path)
    destination = artifact_root / "contracts" / f"{digest}.{label}{path.suffix}"
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if file_sha256(destination) != digest:
            raise ValueError(f"contract archive conflict: {destination}")
    else:
        destination.write_bytes(path.read_bytes())
    return {
        "current_path": str(path),
        "sha256": digest,
        "archive_relative_path": destination.relative_to(artifact_root).as_posix(),
    }


def _slice_payload(market: Any, decision_epoch: int, lookback_min: int) -> dict[str, Any]:
    last_index = market.exact_index(int(decision_epoch) - 60)
    if last_index is None:
        raise ValueError("missing causal row")
    first_index = last_index - int(lookback_min)
    rows = []
    for index in range(first_index, last_index + 1):
        rows.append(
            [
                int(market.epochs[index]),
                float(market.bid_open[index]),
                float(market.bid_high[index]),
                float(market.bid_low[index]),
                float(market.bid_close[index]),
                float(market.ask_open[index]),
                float(market.ask_high[index]),
                float(market.ask_low[index]),
                float(market.ask_close[index]),
            ]
        )
    return {
        "instrument": market.instrument,
        "first_epoch": rows[0][0],
        "last_epoch": rows[-1][0],
        "row_count": len(rows),
        "source_sha256": market.source_sha256,
        "slice_sha256": stable_hash(rows),
        "last_bid_close": rows[-1][4],
        "last_ask_close": rows[-1][8],
        "spread_pips": (rows[-1][8] - rows[-1][4]) / market.pip,
        "quote_age_seconds": int(decision_epoch) - rows[-1][0] - 60,
    }


def _register_cohort(
    connection: sqlite3.Connection,
    cohort_id: str,
    source_cohort_id: str,
    contract: Mapping[str, Any],
) -> None:
    payload = canonical_json(contract)
    digest = stable_hash(contract)
    _insert_immutable(
        connection,
        table="spr_cohorts",
        key_column="cohort_id",
        key=cohort_id,
        hash_column="contract_sha256",
        expected_hash=digest,
        columns=("cohort_id", "created_utc", "source_cohort_id", "contract_sha256", "contract_json"),
        values=(cohort_id, utc_now(), source_cohort_id, digest, payload),
    )


def _register_session(
    connection: sqlite3.Connection,
    cohort_id: str,
    session_id: str,
    start_epoch: int,
    end_epoch: int,
    instruments: Sequence[str],
) -> None:
    initial = state_payload(PortfolioState())
    identity = {
        "session_id": session_id,
        "cohort_id": cohort_id,
        "start_epoch": start_epoch,
        "end_epoch": end_epoch,
        "evidence_role": "historical_training_discovery",
        "proof_eligible": 0,
        "universe": sorted(instruments),
        "initial_state": initial,
    }
    row_hash = stable_hash(identity)
    _insert_immutable(
        connection,
        table="spr_sessions",
        key_column="session_id",
        key=session_id,
        hash_column="row_sha256",
        expected_hash=row_hash,
        columns=(
            "session_id", "cohort_id", "start_epoch", "end_epoch", "evidence_role",
            "proof_eligible", "universe_json", "initial_state_json", "row_sha256",
        ),
        values=(
            session_id, cohort_id, start_epoch, end_epoch, "historical_training_discovery",
            0, canonical_json(sorted(instruments)), canonical_json(initial), row_hash,
        ),
    )


def _record_pair_context(
    connection: sqlite3.Connection,
    *,
    cohort_id: str,
    session_id: str,
    clock_id: str,
    decision_epoch: int,
    alias: str,
    slice_payload: Mapping[str, Any],
    candidate: Candidate | None,
) -> str:
    instrument = str(slice_payload["instrument"])
    context = dict(slice_payload)
    context.update(
        {
            "pair_alias": alias,
            "candidate": asdict(candidate) if candidate is not None else None,
            "candidate_state": "eligible" if candidate is not None else "not_candidate",
        }
    )
    causal_hash = stable_hash(context)
    pair_context_id = "sprpair_" + stable_hash(clock_id, instrument, causal_hash)[:28]
    identity = {
        "pair_context_id": pair_context_id,
        "cohort_id": cohort_id,
        "session_id": session_id,
        "clock_id": clock_id,
        "decision_epoch": decision_epoch,
        "instrument": instrument,
        "pair_alias": alias,
        "input_slice_sha256": slice_payload["slice_sha256"],
        "candidate_snapshot_id": candidate.snapshot_id if candidate else None,
        "causal_sha256": causal_hash,
    }
    row_hash = stable_hash(identity)
    _insert_immutable(
        connection,
        table="spr_pair_contexts",
        key_column="pair_context_id",
        key=pair_context_id,
        hash_column="row_sha256",
        expected_hash=row_hash,
        columns=(
            "pair_context_id", "cohort_id", "session_id", "clock_id", "decision_epoch",
            "instrument", "pair_alias", "input_slice_sha256", "candidate_snapshot_id",
            "causal_sha256", "causal_json", "row_sha256",
        ),
        values=(
            pair_context_id, cohort_id, session_id, clock_id, decision_epoch,
            instrument, alias, slice_payload["slice_sha256"],
            candidate.snapshot_id if candidate else None, causal_hash,
            canonical_json(context), row_hash,
        ),
    )
    return pair_context_id


def _record_clock(
    connection: sqlite3.Connection,
    *,
    cohort_id: str,
    session_id: str,
    clock_id: str,
    sequence_no: int,
    decision_epoch: int,
    execution_epoch: int,
    feedback_epoch: int,
    episode_id: str,
    causal: Mapping[str, Any],
) -> None:
    causal_hash = stable_hash(causal)
    identity = {
        "clock_id": clock_id,
        "cohort_id": cohort_id,
        "session_id": session_id,
        "sequence_no": sequence_no,
        "decision_epoch": decision_epoch,
        "execution_epoch": execution_epoch,
        "feedback_epoch": feedback_epoch,
        "market_episode_id": episode_id,
        "price_snapshot_id": causal["price_snapshot_id"],
        "exact_situation_id": causal["exact_situation_id"],
        "coarse_situation_id": causal["coarse_situation_id"],
        "causal_sha256": causal_hash,
    }
    row_hash = stable_hash(identity)
    _insert_immutable(
        connection,
        table="spr_clocks",
        key_column="clock_id",
        key=clock_id,
        hash_column="row_sha256",
        expected_hash=row_hash,
        columns=(
            "clock_id", "cohort_id", "session_id", "sequence_no", "decision_epoch",
            "execution_epoch", "feedback_epoch", "market_episode_id", "price_snapshot_id",
            "exact_situation_id", "coarse_situation_id", "causal_sha256", "causal_json", "row_sha256",
        ),
        values=(
            clock_id, cohort_id, session_id, sequence_no, decision_epoch, execution_epoch,
            feedback_epoch, episode_id, causal["price_snapshot_id"], causal["exact_situation_id"],
            causal["coarse_situation_id"], causal_hash, canonical_json(causal), row_hash,
        ),
    )


def _record_decision(
    connection: sqlite3.Connection,
    *,
    cohort_id: str,
    session_id: str,
    clock_id: str,
    sequence_no: int,
    decision_epoch: int,
    state_before: PortfolioState,
    decision: Decision,
) -> str:
    decision_json = decision_payload(decision)
    state_json = state_payload(state_before)
    decision_hash = stable_hash(decision_json)
    state_hash = stable_hash(state_json)
    decision_id = "sprdecision_" + stable_hash(session_id, clock_id)[:28]
    identity = {
        "decision_id": decision_id,
        "cohort_id": cohort_id,
        "session_id": session_id,
        "clock_id": clock_id,
        "sequence_no": sequence_no,
        "committed_epoch": decision_epoch,
        "action": decision.action,
        "instrument": decision.instrument,
        "side": decision.side,
        "units": decision.units,
        "position_thesis_id_before": state_before.position.thesis_id if state_before.position else None,
        "evidence_role": "historical_training_discovery",
        "proof_eligible": 0,
        "state_before_sha256": state_hash,
        "decision_sha256": decision_hash,
    }
    row_hash = stable_hash(identity)
    _insert_immutable(
        connection,
        table="spr_decisions",
        key_column="decision_id",
        key=decision_id,
        hash_column="row_sha256",
        expected_hash=row_hash,
        columns=(
            "decision_id", "cohort_id", "session_id", "clock_id", "sequence_no",
            "committed_epoch", "action", "instrument", "side", "units",
            "position_thesis_id_before", "evidence_role", "proof_eligible",
            "state_before_sha256", "state_before_json", "decision_sha256",
            "decision_json", "row_sha256",
        ),
        values=(
            decision_id, cohort_id, session_id, clock_id, sequence_no, decision_epoch,
            decision.action, decision.instrument, decision.side, decision.units,
            state_before.position.thesis_id if state_before.position else None,
            "historical_training_discovery", 0, state_hash, canonical_json(state_json),
            decision_hash, canonical_json(decision_json), row_hash,
        ),
    )
    return decision_id


def _record_outcome(
    connection: sqlite3.Connection,
    *,
    cohort_id: str,
    session_id: str,
    decision_id: str,
    applied: Any,
) -> str:
    state_json = state_payload(applied.state)
    state_hash = stable_hash(state_json)
    outcome_id = "sproutcome_" + stable_hash(decision_id)[:28]
    identity = {
        "outcome_id": outcome_id,
        "cohort_id": cohort_id,
        "session_id": session_id,
        "decision_id": decision_id,
        "status": applied.status,
        "rejection_reason": applied.rejection_reason,
        "realized_delta_pips": round(float(applied.realized_delta_pips), 12),
        "state_after_sha256": state_hash,
    }
    row_hash = stable_hash(identity)
    _insert_immutable(
        connection,
        table="spr_execution_outcomes",
        key_column="outcome_id",
        key=outcome_id,
        hash_column="row_sha256",
        expected_hash=row_hash,
        columns=(
            "outcome_id", "cohort_id", "session_id", "decision_id", "status",
            "rejection_reason", "realized_delta_pips", "state_after_sha256",
            "state_after_json", "row_sha256",
        ),
        values=(
            outcome_id, cohort_id, session_id, decision_id, applied.status,
            applied.rejection_reason, float(applied.realized_delta_pips), state_hash,
            canonical_json(state_json), row_hash,
        ),
    )
    return outcome_id


def _record_leg(
    connection: sqlite3.Connection,
    *,
    cohort_id: str,
    session_id: str,
    parent_kind: str,
    parent_id: str,
    clock_id: str | None,
    sequence_no: int,
    leg: Any,
) -> str:
    payload = leg_payload(leg)
    payload_hash = stable_hash(payload)
    leg_id = "sprleg_" + stable_hash(parent_kind, parent_id, sequence_no, payload_hash)[:28]
    identity = {
        "leg_id": leg_id,
        "cohort_id": cohort_id,
        "session_id": session_id,
        "parent_kind": parent_kind,
        "parent_id": parent_id,
        "clock_id": clock_id,
        "leg_sequence": sequence_no,
        "leg_kind": leg.leg_kind,
        "instrument": leg.instrument,
        "side": leg.side,
        "execution_epoch": leg.execution_epoch,
        "realized_pips": round(float(leg.realized_pips), 12),
        "leg_sha256": payload_hash,
    }
    row_hash = stable_hash(identity)
    _insert_immutable(
        connection,
        table="spr_execution_legs",
        key_column="leg_id",
        key=leg_id,
        hash_column="row_sha256",
        expected_hash=row_hash,
        columns=(
            "leg_id", "cohort_id", "session_id", "parent_kind", "parent_id",
            "clock_id", "leg_sequence", "leg_kind", "instrument", "side",
            "execution_epoch", "realized_pips", "leg_sha256", "leg_json", "row_sha256",
        ),
        values=(
            leg_id, cohort_id, session_id, parent_kind, parent_id, clock_id,
            sequence_no, leg.leg_kind, leg.instrument, leg.side, leg.execution_epoch,
            float(leg.realized_pips), payload_hash, canonical_json(payload), row_hash,
        ),
    )
    return leg_id


def _record_counterfactual(
    connection: sqlite3.Connection,
    *,
    cohort_id: str,
    session_id: str,
    decision_id: str,
    branch: Decision,
    applied: Any,
    terminal_equity: float,
) -> str:
    action_json = decision_payload(branch)
    result_json = {
        "status": applied.status,
        "rejection_reason": applied.rejection_reason,
        "execution_legs": [leg_payload(leg) for leg in applied.legs],
        "state_after_action": state_payload(applied.state),
        "terminal_equity_pips": terminal_equity,
    }
    action_hash = stable_hash(action_json)
    result_hash = stable_hash(result_json)
    counterfactual_id = "sprcf_" + stable_hash(decision_id, branch.branch_label, action_hash)[:28]
    identity = {
        "counterfactual_id": counterfactual_id,
        "cohort_id": cohort_id,
        "session_id": session_id,
        "decision_id": decision_id,
        "branch_label": branch.branch_label,
        "counts_as_rep": 0,
        "status": applied.status,
        "rejection_reason": applied.rejection_reason,
        "terminal_equity_pips": round(float(terminal_equity), 12),
        "action_sha256": action_hash,
        "result_sha256": result_hash,
    }
    row_hash = stable_hash(identity)
    _insert_immutable(
        connection,
        table="spr_counterfactuals",
        key_column="counterfactual_id",
        key=counterfactual_id,
        hash_column="row_sha256",
        expected_hash=row_hash,
        columns=(
            "counterfactual_id", "cohort_id", "session_id", "decision_id",
            "branch_label", "counts_as_rep", "status", "rejection_reason",
            "terminal_equity_pips", "action_sha256", "action_json",
            "result_sha256", "result_json", "row_sha256",
        ),
        values=(
            counterfactual_id, cohort_id, session_id, decision_id, branch.branch_label,
            0, applied.status, applied.rejection_reason, float(terminal_equity),
            action_hash, canonical_json(action_json), result_hash,
            canonical_json(result_json), row_hash,
        ),
    )
    return counterfactual_id


def _record_feedback(
    connection: sqlite3.Connection,
    *,
    cohort_id: str,
    session_id: str,
    decision_id: str,
    feedback_epoch: int,
    primary_equity: float,
    alternative_equities: Mapping[str, float],
    physical_path: str,
    resources: Sequence[str],
    components: Mapping[str, Any],
) -> str:
    feedback_id = "sprfeedback_" + stable_hash(decision_id)[:28]
    components_hash = stable_hash(components)
    best_alternative = max(alternative_equities.values()) if alternative_equities else primary_equity
    identity = {
        "feedback_id": feedback_id,
        "cohort_id": cohort_id,
        "session_id": session_id,
        "decision_id": decision_id,
        "revealed_epoch": feedback_epoch,
        "primary_terminal_equity_pips": round(float(primary_equity), 12),
        "best_alternative_equity_pips": round(float(best_alternative), 12),
        "physical_path_id": physical_path,
        "currency_resources": sorted(resources),
        "components_sha256": components_hash,
    }
    row_hash = stable_hash(identity)
    _insert_immutable(
        connection,
        table="spr_feedback",
        key_column="feedback_id",
        key=feedback_id,
        hash_column="row_sha256",
        expected_hash=row_hash,
        columns=(
            "feedback_id", "cohort_id", "session_id", "decision_id", "revealed_epoch",
            "primary_terminal_equity_pips", "best_alternative_equity_pips",
            "physical_path_id", "currency_resources_json", "components_sha256",
            "components_json", "row_sha256",
        ),
        values=(
            feedback_id, cohort_id, session_id, decision_id, feedback_epoch,
            float(primary_equity), float(best_alternative), physical_path,
            canonical_json(sorted(resources)), components_hash,
            canonical_json(components), row_hash,
        ),
    )
    return feedback_id


def _structural_component_count(connection: sqlite3.Connection, session_id: str) -> int:
    rows = list(
        connection.execute(
            """
            SELECT d.decision_id,d.position_thesis_id_before,c.market_episode_id,
                   f.currency_resources_json,f.physical_path_id
            FROM spr_decisions d JOIN spr_clocks c ON c.clock_id=d.clock_id
            JOIN spr_feedback f ON f.decision_id=d.decision_id
            WHERE d.session_id=? ORDER BY d.sequence_no
            """,
            (session_id,),
        )
    )
    parent = list(range(len(rows)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        a, b = find(left), find(right)
        if a != b:
            parent[b] = a

    for left in range(len(rows)):
        for right in range(left + 1, len(rows)):
            a, b = rows[left], rows[right]
            same_episode = str(a["market_episode_id"]) == str(b["market_episode_id"])
            same_thesis = bool(a["position_thesis_id_before"]) and str(a["position_thesis_id_before"]) == str(b["position_thesis_id_before"])
            same_path = str(a["physical_path_id"]) != "unassigned" and str(a["physical_path_id"]) == str(b["physical_path_id"])
            if same_episode or same_thesis or same_path:
                union(left, right)
    return len({find(index) for index in range(len(rows))}) if rows else 0


def _render_report(state: Mapping[str, Any]) -> str:
    actions = state["action_counts"]
    lines = [
        "# Sequential Portfolio Replay V1",
        "",
        "Status: independently verifiable historical training/discovery pilot; never execution evidence",
        "",
        "## Result",
        "",
        f"- Cohort: `{state['cohort_id']}`",
        f"- Session: `{state['session_id']}`",
        f"- Global decisions: **{state['global_clock_count']}**",
        f"- Pair contexts: **{state['pair_context_count']}**",
        f"- Actions: wait {actions.get('wait',0)}, enter {actions.get('enter',0)}, hold {actions.get('hold',0)}, exit {actions.get('exit',0)}, rotate {actions.get('rotate',0)}",
        f"- Applied / rejected actions: **{state['applied_action_count']} / {state['rejected_action_count']}**",
        f"- Execution legs: **{state['execution_leg_count']}**; rotations use two legs",
        f"- Realized terminal result: **{state['terminal_realized_pips']:+.3f} pips**",
        f"- Counterfactual branches: **{state['counterfactual_count']}** (all `counts_as_rep=0`)",
        f"- Structural dependence components: **{state['structural_component_count']}**; independent regimes remain unknown",
        "",
        "## Interpretation",
        "",
        "This run proves mechanics, chronology, cost accounting, position state, rotation, and nested alternatives. It does not prove edge: the four-hour window was already inspected, the policy is a frozen training baseline, and all rows are permanently proof-ineligible.",
        "",
        "Spread is paid once through executable bid/ask quotes. Slippage is charged at 0.125 pip per execution leg. Entries occur at the exact one-minute-delayed open; missing or wide entry quotes reject without mutating the portfolio. Exits are never blocked by spread.",
        "",
        "Practice 007 and all governed authorization/lifecycle databases are outside this component.",
    ]
    return "\n".join(lines) + "\n"


def run(config_path: Path = DEFAULT_CONFIG) -> dict[str, Any]:
    config = read_json(config_path)
    validate_config(config)
    artifact_root = safe_artifact_root(config)
    source_root, source_state, _source_verifier, source_contract = _source_contract(config)
    archived_contracts = {
        "config": _archive_contract(config_path, artifact_root, "config"),
        "runner": _archive_contract(Path(__file__).resolve(), artifact_root, "runner"),
        "core": _archive_contract(CORE_PATH, artifact_root, "core"),
        "verifier": _archive_contract(VERIFIER_PATH, artifact_root, "verifier"),
    }
    material_contract = {
        "schema_version": 1,
        "config": config,
        "code": archived_contracts,
        "source": source_contract,
    }
    material_sha = stable_hash(material_contract)
    cohort_id = "sequential_portfolio_replay_v1." + material_sha[:20]
    instruments = sorted(str(value) for value in config["source"]["initial_instruments"])
    manifest_by_instrument = {
        str(row["instrument"]): row for row in source_state["source_manifest"]
    }
    if sorted(manifest_by_instrument) != instruments:
        raise ValueError("frozen source universe mismatch")
    markets: dict[str, Any] = {}
    for instrument in instruments:
        row = manifest_by_instrument[instrument]
        archive = source_root / str(row["archive_relative_path"])
        markets[instrument] = load_archived_market(
            archive,
            instrument=instrument,
            expected_raw_sha256=str(row["sha256"]),
            maximum_raw_bytes=int(config["source"]["maximum_archive_bytes_per_instrument"]),
        )
    start_epoch = parse_iso_epoch(config["session"]["start_utc"])
    end_epoch = parse_iso_epoch(config["session"]["end_utc_exclusive"])
    clocks = build_global_clocks(
        markets,
        start_epoch=start_epoch,
        end_epoch=end_epoch,
        cadence_min=int(config["session"]["decision_cadence_min"]),
        feature_lookback_min=int(config["session"]["feature_lookback_min"]),
        execution_delay_min=int(config["session"]["execution_delay_min"]),
        feedback_horizon_min=int(config["session"]["feedback_horizon_min"]),
    )
    if len(clocks) != int(config["session"]["expected_global_clock_count"]):
        raise ValueError(f"unexpected causal clock count: {len(clocks)}")
    universe_manifest = stable_hash(
        [(instrument, markets[instrument].source_sha256) for instrument in instruments]
    )
    session_id = "sprsession_" + stable_hash(
        cohort_id, start_epoch, end_epoch, instruments, config["frozen_policy"]["policy_id"]
    )[:28]
    aliases = {instrument: chr(ord("A") + index) for index, instrument in enumerate(instruments)}
    database = artifact_root / "cohorts" / cohort_id / str(config["storage"]["database_name"])
    connection = output_connection(database)
    try:
        connection.execute("BEGIN IMMEDIATE")
        _register_cohort(
            connection,
            cohort_id,
            str(source_state["cohort_id"]),
            material_contract,
        )
        _register_session(
            connection,
            cohort_id,
            session_id,
            start_epoch,
            end_epoch,
            instruments,
        )
        connection.commit()
        state = PortfolioState()
        action_counts: Counter[str] = Counter()
        applied_count = 0
        rejected_count = 0
        for sequence_no, decision_epoch in enumerate(clocks, start=1):
            execution_epoch = decision_epoch + int(config["session"]["execution_delay_min"]) * 60
            feedback_epoch = decision_epoch + int(config["session"]["feedback_horizon_min"]) * 60
            candidates = candidate_set(markets, decision_epoch, config)
            slices = {
                instrument: _slice_payload(
                    markets[instrument], decision_epoch, int(config["session"]["feature_lookback_min"])
                )
                for instrument in instruments
            }
            clock_id = "sprclock_" + stable_hash(
                "oanda_practice_completed_m1_global_clock_v1",
                int(decision_epoch),
                sorted((instrument, slices[instrument]["slice_sha256"]) for instrument in instruments),
                universe_manifest,
            )[:28]
            causal = causal_snapshot(markets, candidates, state, decision_epoch, config)
            causal.update(
                {
                    "clock_id": clock_id,
                    "pair_aliases": aliases,
                    "pair_input_slice_ids": {
                        instrument: slices[instrument]["slice_sha256"] for instrument in instruments
                    },
                    "universe_manifest_sha256": universe_manifest,
                }
            )
            new_position_allowed = (
                decision_epoch
                + int(config["session"]["execution_delay_min"]) * 60
                + int(config["session"]["feedback_horizon_min"]) * 60
                <= end_epoch
            )
            primary = choose_primary_decision(
                state,
                candidates,
                decision_epoch,
                config,
                new_position_allowed=new_position_allowed,
            )
            branches = legal_branches(state, primary, candidates, config)
            state_before = state
            connection.execute("BEGIN IMMEDIATE")
            try:
                episode_id = market_episode_id(
                    decision_epoch, int(config["independence"]["market_episode_minutes"])
                )
                _record_clock(
                    connection,
                    cohort_id=cohort_id,
                    session_id=session_id,
                    clock_id=clock_id,
                    sequence_no=sequence_no,
                    decision_epoch=decision_epoch,
                    execution_epoch=execution_epoch,
                    feedback_epoch=feedback_epoch,
                    episode_id=episode_id,
                    causal=causal,
                )
                candidate_by_instrument = {row.instrument: row for row in candidates}
                for instrument in instruments:
                    _record_pair_context(
                        connection,
                        cohort_id=cohort_id,
                        session_id=session_id,
                        clock_id=clock_id,
                        decision_epoch=decision_epoch,
                        alias=aliases[instrument],
                        slice_payload=slices[instrument],
                        candidate=candidate_by_instrument.get(instrument),
                    )
                decision_id = _record_decision(
                    connection,
                    cohort_id=cohort_id,
                    session_id=session_id,
                    clock_id=clock_id,
                    sequence_no=sequence_no,
                    decision_epoch=decision_epoch,
                    state_before=state_before,
                    decision=primary,
                )
                applied = apply_decision(
                    state_before,
                    primary,
                    markets,
                    execution_epoch=execution_epoch,
                    slippage_per_leg_pips=float(config["costs"]["slippage_per_execution_leg_pips"]),
                    decision_clock_id=clock_id,
                    maximum_entry_spread_pips=float(config["source"]["maximum_entry_spread_pips"]),
                )
                _record_outcome(
                    connection,
                    cohort_id=cohort_id,
                    session_id=session_id,
                    decision_id=decision_id,
                    applied=applied,
                )
                for leg_sequence, leg in enumerate(applied.legs, start=1):
                    _record_leg(
                        connection,
                        cohort_id=cohort_id,
                        session_id=session_id,
                        parent_kind="primary_decision",
                        parent_id=decision_id,
                        clock_id=clock_id,
                        sequence_no=leg_sequence,
                        leg=leg,
                    )
                primary_equity = liquidation_equity(
                    applied.state,
                    markets,
                    epoch=feedback_epoch,
                    slippage_per_leg_pips=float(config["costs"]["slippage_per_execution_leg_pips"]),
                )
                alternative_equities: dict[str, float] = {}
                for branch in branches:
                    branch_applied = apply_decision(
                        state_before,
                        branch,
                        markets,
                        execution_epoch=execution_epoch,
                        slippage_per_leg_pips=float(config["costs"]["slippage_per_execution_leg_pips"]),
                        decision_clock_id=clock_id,
                        maximum_entry_spread_pips=float(config["source"]["maximum_entry_spread_pips"]),
                    )
                    branch_equity = liquidation_equity(
                        branch_applied.state,
                        markets,
                        epoch=feedback_epoch,
                        slippage_per_leg_pips=float(config["costs"]["slippage_per_execution_leg_pips"]),
                    )
                    alternative_equities[branch.branch_label] = branch_equity
                    _record_counterfactual(
                        connection,
                        cohort_id=cohort_id,
                        session_id=session_id,
                        decision_id=decision_id,
                        branch=branch,
                        applied=branch_applied,
                        terminal_equity=branch_equity,
                    )
                chosen_candidate = next(
                    (row for row in candidates if row.snapshot_id == primary.candidate_snapshot_id),
                    None,
                )
                components = feedback_components(
                    primary=primary,
                    state_before=state_before,
                    primary_equity=primary_equity,
                    alternative_equities=alternative_equities,
                    candidate=chosen_candidate,
                )
                path_instrument = primary.instrument or (
                    state_before.position.instrument if state_before.position else None
                )
                path_id = physical_path_id(
                    path_instrument, execution_epoch, feedback_epoch, markets
                )
                _record_feedback(
                    connection,
                    cohort_id=cohort_id,
                    session_id=session_id,
                    decision_id=decision_id,
                    feedback_epoch=feedback_epoch,
                    primary_equity=primary_equity,
                    alternative_equities=alternative_equities,
                    physical_path=path_id,
                    resources=unsigned_currency_resources(path_instrument),
                    components=components,
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            state = applied.state
            action_counts[primary.action] += 1
            if applied.status == "applied":
                applied_count += 1
            else:
                rejected_count += 1
        if not state.flat:
            admin_id = "spradmin_" + stable_hash(session_id, end_epoch, "administrative_session_close")[:28]
            admin_decision = Decision(
                "exit", state.position.instrument, None, 0, None, None, None,
                "", "", "predeclared administrative session close", None,
            )
            closed = apply_decision(
                state,
                admin_decision,
                markets,
                execution_epoch=end_epoch,
                slippage_per_leg_pips=float(config["costs"]["slippage_per_execution_leg_pips"]),
                decision_clock_id=admin_id,
            )
            event = {
                "event_id": admin_id,
                "event_type": "administrative_session_close",
                "event_epoch": end_epoch,
                "not_scored_as_learner_exit": True,
                "state_before": state_payload(state),
                "state_after": state_payload(closed.state),
                "legs": [leg_payload(leg) for leg in closed.legs],
            }
            event_hash = stable_hash(event)
            row_hash = stable_hash(admin_id, cohort_id, session_id, end_epoch, event_hash)
            connection.execute("BEGIN IMMEDIATE")
            try:
                _insert_immutable(
                    connection,
                    table="spr_administrative_events",
                    key_column="event_id",
                    key=admin_id,
                    hash_column="row_sha256",
                    expected_hash=row_hash,
                    columns=(
                        "event_id", "cohort_id", "session_id", "event_epoch",
                        "event_type", "event_sha256", "event_json", "row_sha256",
                    ),
                    values=(
                        admin_id, cohort_id, session_id, end_epoch,
                        "administrative_session_close", event_hash,
                        canonical_json(event), row_hash,
                    ),
                )
                for leg_sequence, leg in enumerate(closed.legs, start=1):
                    _record_leg(
                        connection,
                        cohort_id=cohort_id,
                        session_id=session_id,
                        parent_kind="administrative_event",
                        parent_id=admin_id,
                        clock_id=None,
                        sequence_no=leg_sequence,
                        leg=leg,
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            state = closed.state
        roots = {
            "pair_contexts": _table_root(connection, "spr_pair_contexts", "row_sha256", session_id),
            "clocks": _table_root(connection, "spr_clocks", "row_sha256", session_id),
            "decisions": _table_root(connection, "spr_decisions", "row_sha256", session_id),
            "execution_outcomes": _table_root(connection, "spr_execution_outcomes", "row_sha256", session_id),
            "execution_legs": _table_root(connection, "spr_execution_legs", "row_sha256", session_id),
            "counterfactuals": _table_root(connection, "spr_counterfactuals", "row_sha256", session_id),
            "feedback": _table_root(connection, "spr_feedback", "row_sha256", session_id),
            "administrative_events": _table_root(connection, "spr_administrative_events", "row_sha256", session_id),
        }
        statistics = {
            "cohort_id": cohort_id,
            "session_id": session_id,
            "source_cohort_id": source_state["cohort_id"],
            "window": {"start_epoch": start_epoch, "end_epoch": end_epoch},
            "global_clock_count": int(connection.execute("SELECT COUNT(*) FROM spr_clocks WHERE session_id=?", (session_id,)).fetchone()[0]),
            "pair_context_count": int(connection.execute("SELECT COUNT(*) FROM spr_pair_contexts WHERE session_id=?", (session_id,)).fetchone()[0]),
            "action_counts": dict(sorted(action_counts.items())),
            "applied_action_count": applied_count,
            "rejected_action_count": rejected_count,
            "execution_leg_count": roots["execution_legs"]["count"],
            "counterfactual_count": roots["counterfactuals"]["count"],
            "terminal_realized_pips": float(state.realized_pips),
            "terminal_flat": state.flat,
            "structural_component_count": _structural_component_count(connection, session_id),
            "independent_regime_count": None,
            "independent_regime_count_state": config["independence"]["independent_regime_count_state"],
            "proof_eligible": False,
            "evidence_role": "historical_training_discovery",
            "roots": roots,
        }
        previous = connection.execute(
            "SELECT seal_id,seal_json FROM spr_session_seals WHERE session_id=? ORDER BY rowid DESC LIMIT 1",
            (session_id,),
        ).fetchone()
        previous_id = str(previous[0]) if previous else None
        seal_body = {
            "session_id": session_id,
            "cohort_id": cohort_id,
            "source_contract_sha256": stable_hash(source_contract),
            "roots": roots,
            "terminal_state": state_payload(state),
            "statistics_sha256": stable_hash(statistics),
        }
        reuse_previous = False
        if previous is not None:
            previous_payload = json.loads(str(previous["seal_json"]))
            comparable = dict(previous_payload)
            comparable.pop("previous_seal_id", None)
            reuse_previous = comparable == seal_body
        seal_payload = {"previous_seal_id": previous_id, **seal_body}
        seal_hash = stable_hash(seal_payload)
        seal_id = previous_id if reuse_previous else "sprseal_" + seal_hash[:28]
        connection.execute("BEGIN IMMEDIATE")
        try:
            if not reuse_previous:
                _insert_immutable(
                    connection,
                    table="spr_session_seals",
                    key_column="seal_id",
                    key=seal_id,
                    hash_column="seal_sha256",
                    expected_hash=seal_hash,
                    columns=(
                        "seal_id", "cohort_id", "session_id", "previous_seal_id",
                        "created_utc", "seal_sha256", "seal_json",
                    ),
                    values=(
                        seal_id, cohort_id, session_id, previous_id, utc_now(),
                        seal_hash, canonical_json(seal_payload),
                    ),
                )
            snapshot_id = "sprsnapshot_" + stable_hash(cohort_id, session_id, statistics)[:28]
            _insert_immutable(
                connection,
                table="spr_snapshots",
                key_column="snapshot_id",
                key=snapshot_id,
                hash_column="statistics_sha256",
                expected_hash=stable_hash(statistics),
                columns=(
                    "snapshot_id", "cohort_id", "session_id", "generated_utc",
                    "statistics_sha256", "statistics_json",
                ),
                values=(
                    snapshot_id, cohort_id, session_id, utc_now(),
                    stable_hash(statistics), canonical_json(statistics),
                ),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        state_output = {
            "schema_version": 1,
            "generated_utc": utc_now(),
            "research_only": True,
            "execution_eligible": False,
            "can_promote": False,
            "can_place_orders": False,
            "can_authorize": False,
            "supported_decision": "no_trade",
            "cohort_id": cohort_id,
            "session_id": session_id,
            "material_contract_sha256": material_sha,
            "database": str(database),
            "session_seal_id": seal_id,
            **statistics,
            "limitations": [
                "already-inspected historical training/discovery window; cannot confirm or promote",
                "one frozen mechanics baseline is not a profitable-model claim",
                "structural dependence components are not independent market regimes",
                "counterfactual branches are nested and never count as repetitions",
                "news, rates, levels, and positioning snapshots are explicitly unavailable in this price-only pilot",
            ],
        }
        atomic_json(artifact_root / str(config["storage"]["state_name"]), state_output)
        atomic_text(artifact_root / str(config["storage"]["report_name"]), _render_report(state_output))
        return state_output
    finally:
        connection.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    state = run(args.config)
    print(json.dumps(state, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
