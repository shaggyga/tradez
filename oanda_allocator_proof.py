#!/usr/bin/env python3
"""Frozen prospective proof ledger for the actual Practice-007 allocator policy.

The ledger observes the complete active candidate set on a fixed cadence,
records ranking and veto decisions before outcomes exist, and matures all
counterfactuals from later executable quotes.  It is intentionally broker-free
and cannot place or close orders.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
import sqlite3
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from oanda_edge_evidence import alpha_spending_confidence_sequence
except ModuleNotFoundError:
    from trad.oanda_edge_evidence import alpha_spending_confidence_sequence


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
DEFAULT_DATABASE = STATE / "allocator_proof_v1.sqlite"
DEFAULT_STATE = STATE / "allocator_proof_v1.json"
DEFAULT_CONFIG = ROOT / "config" / "evidence_operations_v1.json"
DEFAULT_SIGNAL_FEED = STATE / "practice_007_signal_feed_v1.sqlite"
DEFAULT_QUOTES = STATE / "practice_007_market_quotes_v1.json.sqlite"
DEFAULT_ACCOUNT = STATE / "account_007_dashboard_v1.json"
DEFAULT_EXECUTOR = STATE / "practice_007_top_executor_heartbeat_v1.json"

COMPARATORS = (
    "frozen_policy",
    "no_trade",
    "deterministic_random_same_eligible_set",
    "highest_raw_predicted_return",
    "lowest_cost_eligible",
    "hold_current_position",
    "strongest_constituent_family",
)


class AllocatorLineageError(RuntimeError):
    """Raised when immutable allocator cohort lineage cannot be replayed."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


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
        CREATE TABLE IF NOT EXISTS allocator_cohorts (
            cohort_id TEXT PRIMARY KEY,
            policy_id TEXT NOT NULL,
            phase TEXT NOT NULL,
            cohort_start_utc TEXT NOT NULL,
            material_contract_sha256 TEXT NOT NULL,
            source_sha256 TEXT NOT NULL,
            config_sha256 TEXT NOT NULL,
            contract_json TEXT NOT NULL,
            UNIQUE(policy_id,phase,material_contract_sha256)
        );
        CREATE TABLE IF NOT EXISTS allocator_cohort_transitions (
            transition_id TEXT PRIMARY KEY,
            policy_id TEXT NOT NULL,
            previous_cohort_id TEXT,
            next_cohort_id TEXT NOT NULL,
            observed_utc TEXT NOT NULL,
            reason TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS decisions (
            decision_id TEXT PRIMARY KEY,
            cohort_id TEXT NOT NULL,
            decision_bucket INTEGER NOT NULL,
            decision_epoch REAL NOT NULL,
            decision_utc TEXT NOT NULL,
            quote_sequence INTEGER,
            quote_published_epoch REAL,
            candidate_set_sha256 TEXT NOT NULL,
            raw_candidate_count INTEGER NOT NULL,
            policy_eligible_count INTEGER NOT NULL,
            selected_candidate_id TEXT,
            best_rejected_candidate_id TEXT,
            decision_json TEXT NOT NULL,
            UNIQUE(cohort_id,decision_bucket)
        );
        CREATE TABLE IF NOT EXISTS decision_candidates (
            decision_id TEXT NOT NULL,
            candidate_id TEXT NOT NULL,
            instrument TEXT NOT NULL,
            direction TEXT NOT NULL,
            family TEXT NOT NULL,
            horizon_sec INTEGER NOT NULL,
            target_epoch REAL NOT NULL,
            policy_eligible INTEGER NOT NULL,
            entry_bid REAL NOT NULL,
            entry_ask REAL NOT NULL,
            pip REAL NOT NULL,
            spread_pips REAL NOT NULL,
            raw_predicted_return_pips REAL NOT NULL,
            projected_net_pips REAL NOT NULL,
            candidate_json TEXT NOT NULL,
            PRIMARY KEY(decision_id,candidate_id)
        );
        CREATE TABLE IF NOT EXISTS comparator_selections (
            decision_id TEXT NOT NULL,
            comparator TEXT NOT NULL,
            candidate_id TEXT,
            selection_json TEXT NOT NULL,
            PRIMARY KEY(decision_id,comparator)
        );
        CREATE TABLE IF NOT EXISTS candidate_outcomes (
            decision_id TEXT NOT NULL,
            candidate_id TEXT NOT NULL,
            matured_utc TEXT NOT NULL,
            quote_sequence INTEGER NOT NULL,
            quote_published_epoch REAL NOT NULL,
            outcome_delay_sec REAL NOT NULL,
            exit_bid REAL NOT NULL,
            exit_ask REAL NOT NULL,
            gross_mid_pips REAL NOT NULL,
            executable_net_pips REAL NOT NULL,
            realized_cost_pips REAL NOT NULL,
            outcome_json TEXT NOT NULL,
            PRIMARY KEY(decision_id,candidate_id)
        );
        CREATE TABLE IF NOT EXISTS outcome_integrity_events (
            event_id TEXT PRIMARY KEY,
            decision_id TEXT NOT NULL,
            candidate_id TEXT NOT NULL,
            observed_utc TEXT NOT NULL,
            event_type TEXT NOT NULL,
            reason TEXT NOT NULL,
            details_json TEXT NOT NULL,
            UNIQUE(decision_id,candidate_id,event_type)
        );
        CREATE TABLE IF NOT EXISTS policy_outcomes (
            decision_id TEXT NOT NULL,
            comparator TEXT NOT NULL,
            candidate_id TEXT,
            matured_utc TEXT NOT NULL,
            executable_net_pips REAL NOT NULL,
            outcome_json TEXT NOT NULL,
            PRIMARY KEY(decision_id,comparator)
        );
        CREATE TABLE IF NOT EXISTS allocator_lifecycle_events (
            event_id TEXT PRIMARY KEY,
            cohort_id TEXT NOT NULL,
            previous_state TEXT,
            next_state TEXT NOT NULL,
            observed_utc TEXT NOT NULL,
            evidence_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS allocator_candidate_locks (
            lock_id TEXT PRIMARY KEY,
            discovery_cohort_id TEXT NOT NULL,
            locked_utc TEXT NOT NULL,
            evidence_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS allocator_confirmation_cohorts (
            confirmation_cohort_id TEXT PRIMARY KEY,
            lock_id TEXT NOT NULL,
            start_utc TEXT NOT NULL,
            state TEXT NOT NULL,
            contract_json TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS allocator_cohorts_no_update BEFORE UPDATE ON allocator_cohorts
        BEGIN SELECT RAISE(ABORT, 'allocator cohorts are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS allocator_cohorts_no_delete BEFORE DELETE ON allocator_cohorts
        BEGIN SELECT RAISE(ABORT, 'allocator cohorts are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS allocator_transitions_no_update BEFORE UPDATE ON allocator_cohort_transitions
        BEGIN SELECT RAISE(ABORT, 'allocator transitions are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS allocator_transitions_no_delete BEFORE DELETE ON allocator_cohort_transitions
        BEGIN SELECT RAISE(ABORT, 'allocator transitions are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS decisions_no_update BEFORE UPDATE ON decisions
        BEGIN SELECT RAISE(ABORT, 'allocator decisions are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS decisions_no_delete BEFORE DELETE ON decisions
        BEGIN SELECT RAISE(ABORT, 'allocator decisions are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS decision_candidates_no_update BEFORE UPDATE ON decision_candidates
        BEGIN SELECT RAISE(ABORT, 'decision candidates are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS decision_candidates_no_delete BEFORE DELETE ON decision_candidates
        BEGIN SELECT RAISE(ABORT, 'decision candidates are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS selections_no_update BEFORE UPDATE ON comparator_selections
        BEGIN SELECT RAISE(ABORT, 'comparator selections are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS selections_no_delete BEFORE DELETE ON comparator_selections
        BEGIN SELECT RAISE(ABORT, 'comparator selections are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS candidate_outcomes_no_update BEFORE UPDATE ON candidate_outcomes
        BEGIN SELECT RAISE(ABORT, 'candidate outcomes are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS candidate_outcomes_no_delete BEFORE DELETE ON candidate_outcomes
        BEGIN SELECT RAISE(ABORT, 'candidate outcomes are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS integrity_events_no_update BEFORE UPDATE ON outcome_integrity_events
        BEGIN SELECT RAISE(ABORT, 'integrity events are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS integrity_events_no_delete BEFORE DELETE ON outcome_integrity_events
        BEGIN SELECT RAISE(ABORT, 'integrity events are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS policy_outcomes_no_update BEFORE UPDATE ON policy_outcomes
        BEGIN SELECT RAISE(ABORT, 'policy outcomes are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS policy_outcomes_no_delete BEFORE DELETE ON policy_outcomes
        BEGIN SELECT RAISE(ABORT, 'policy outcomes are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS allocator_lifecycle_no_update BEFORE UPDATE ON allocator_lifecycle_events
        BEGIN SELECT RAISE(ABORT, 'allocator lifecycle is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS allocator_lifecycle_no_delete BEFORE DELETE ON allocator_lifecycle_events
        BEGIN SELECT RAISE(ABORT, 'allocator lifecycle is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS allocator_locks_no_update BEFORE UPDATE ON allocator_candidate_locks
        BEGIN SELECT RAISE(ABORT, 'allocator candidate locks are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS allocator_locks_no_delete BEFORE DELETE ON allocator_candidate_locks
        BEGIN SELECT RAISE(ABORT, 'allocator candidate locks are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS allocator_confirmation_no_update BEFORE UPDATE ON allocator_confirmation_cohorts
        BEGIN SELECT RAISE(ABORT, 'allocator confirmation cohorts are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS allocator_confirmation_no_delete BEFORE DELETE ON allocator_confirmation_cohorts
        BEGIN SELECT RAISE(ABORT, 'allocator confirmation cohorts are immutable'); END;
        """
    )
    connection.commit()
    return connection


def material_contract(config: dict[str, Any]) -> dict[str, Any]:
    allocator = config.get("allocator") or {}
    source_sha = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    config_sha = hashlib.sha256(canonical_json(config).encode("utf-8")).hexdigest()
    return {
        "phase": str(config.get("phase")),
        "policy_id": str(allocator.get("policy_id")),
        "source_sha256": "sha256:" + source_sha,
        "config_sha256": "sha256:" + config_sha,
        "decision_cadence_sec": int(allocator.get("decision_cadence_sec") or 300),
        "candidate_source": "practice_007_signal_feed_v1.active_candidates_at_cutoff",
        "quote_source": "practice_007_market_quotes_v1.quote_snapshots_v2",
        "selection": allocator.get("selection") or {},
        "comparators": allocator.get("comparators") or [],
        "sampling": allocator.get("sampling") or {},
        "modeled_slippage_pips": finite(allocator.get("modeled_slippage_pips"), 0.25),
        "minimum_incremental_edge_pips": finite(
            allocator.get("minimum_incremental_edge_pips"), 0.5
        ),
        "outcome_contract": "first_executable_quote_at_or_after_declared_horizon_with_15s_tolerance",
        "deduplication": "signed_currency_factor_and_market_episode",
        "practice_execution": False,
    }


def replay_allocator_active_cohort(
    connection: sqlite3.Connection,
    *,
    policy_id: str,
    phase: str,
) -> dict[str, Any] | None:
    """Replay one policy/phase transition chain and return its active cohort.

    Cohort definitions are immutable, but a definition alone is not evidence
    that it is currently active.  The append-only transition chain is the
    authority.  In particular, a previously superseded A contract must not be
    silently reused after an A -> B -> A configuration change.
    """

    cohort_rows = connection.execute(
        """
        SELECT cohort_id,phase,material_contract_sha256,contract_json
        FROM allocator_cohorts WHERE policy_id=?
        """,
        (policy_id,),
    ).fetchall()
    cohorts: dict[str, dict[str, Any]] = {}
    for cohort_id, cohort_phase, digest, contract_json in cohort_rows:
        try:
            contract = json.loads(str(contract_json))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise AllocatorLineageError(
                f"allocator cohort contract is unreadable: {cohort_id}"
            ) from exc
        if str(contract.get("cohort_id") or "") != str(cohort_id):
            raise AllocatorLineageError(
                f"allocator cohort contract identity mismatch: {cohort_id}"
            )
        cohorts[str(cohort_id)] = {
            "cohort_id": str(cohort_id),
            "phase": str(cohort_phase),
            "material_contract_sha256": str(digest),
            "contract": contract,
        }

    transitions = connection.execute(
        """
        SELECT rowid,transition_id,previous_cohort_id,next_cohort_id
        FROM allocator_cohort_transitions
        WHERE policy_id=? ORDER BY rowid
        """,
        (policy_id,),
    ).fetchall()
    phase_transitions: list[tuple[Any, ...]] = []
    for row in transitions:
        next_id = str(row[3])
        next_cohort = cohorts.get(next_id)
        if next_cohort is None:
            raise AllocatorLineageError(
                f"allocator transition references missing next cohort: {next_id}"
            )
        previous_id = None if row[2] is None else str(row[2])
        if previous_id is not None:
            previous_cohort = cohorts.get(previous_id)
            if previous_cohort is None:
                raise AllocatorLineageError(
                    f"allocator transition references missing previous cohort: {previous_id}"
                )
            if previous_cohort["phase"] != next_cohort["phase"]:
                raise AllocatorLineageError(
                    "allocator transition crosses cohort phases: "
                    f"{previous_id} -> {next_id}"
                )
        if next_cohort["phase"] == phase:
            phase_transitions.append(row)

    phase_cohorts = {
        cohort_id for cohort_id, row in cohorts.items() if row["phase"] == phase
    }
    active_id: str | None = None
    observed: set[str] = set()
    for _rowid, transition_id, previous_id, next_id in phase_transitions:
        previous = None if previous_id is None else str(previous_id)
        next_value = str(next_id)
        if previous != active_id:
            raise AllocatorLineageError(
                "allocator transition chain cannot be replayed at "
                f"{transition_id}: expected previous {active_id!r}, got {previous!r}"
            )
        if next_value in observed:
            raise AllocatorLineageError(
                f"allocator cohort is activated more than once: {next_value}"
            )
        observed.add(next_value)
        active_id = next_value

    if observed != phase_cohorts:
        missing = sorted(phase_cohorts - observed)
        unexpected = sorted(observed - phase_cohorts)
        raise AllocatorLineageError(
            "allocator cohorts and transition replay disagree: "
            f"missing={missing}, unexpected={unexpected}"
        )
    return None if active_id is None else cohorts[active_id]


def ensure_allocator_cohort(
    connection: sqlite3.Connection,
    config: dict[str, Any],
    *,
    phase: str = "discovery",
    start_utc: str | None = None,
) -> dict[str, Any]:
    contract = material_contract(config)
    policy_id = str(contract["policy_id"])
    contract["cohort_phase"] = phase
    digest = stable_hash(contract)
    active = replay_allocator_active_cohort(
        connection, policy_id=policy_id, phase=phase
    )
    if active is not None and active["material_contract_sha256"] == digest:
        return dict(active["contract"])
    superseded = connection.execute(
        """
        SELECT cohort_id FROM allocator_cohorts
        WHERE policy_id=? AND phase=? AND material_contract_sha256=?
        """,
        (policy_id, phase, digest),
    ).fetchone()
    if superseded is not None:
        raise AllocatorLineageError(
            "refusing to reactivate superseded allocator cohort "
            f"{superseded[0]}; a materially new cohort contract is required"
        )
    started = str(start_utc or utc_now())
    cohort_id = f"{policy_id}.{phase}.{started[:10].replace('-', '')}.{digest[:16]}"
    frozen = {
        **contract,
        "cohort_id": cohort_id,
        "cohort_start_utc": started,
        "material_contract_sha256": digest,
        "excluded_pre_cohort_decisions": True,
    }
    connection.execute(
        "INSERT INTO allocator_cohorts VALUES (?,?,?,?,?,?,?,?)",
        (
            cohort_id,
            policy_id,
            phase,
            started,
            digest,
            contract["source_sha256"],
            contract["config_sha256"],
            canonical_json(frozen),
        ),
    )
    event = {
        "policy_id": policy_id,
        "previous": None if active is None else str(active["cohort_id"]),
        "next": cohort_id,
        "observed_utc": started,
        "reason": "initial_allocator_cohort" if active is None else "material_contract_changed",
    }
    connection.execute(
        "INSERT INTO allocator_cohort_transitions VALUES (?,?,?,?,?,?)",
        (
            "allocator_transition_" + stable_hash(event)[:24],
            policy_id,
            event["previous"],
            cohort_id,
            started,
            event["reason"],
        ),
    )
    replayed = replay_allocator_active_cohort(
        connection, policy_id=policy_id, phase=phase
    )
    if replayed is None or replayed["cohort_id"] != cohort_id:
        connection.rollback()
        raise AllocatorLineageError(
            f"allocator transition replay did not activate new cohort: {cohort_id}"
        )
    connection.commit()
    return frozen


def latest_quote_snapshot(
    quote_database: Path, cutoff_epoch: float
) -> tuple[int, float, dict[str, Any]] | None:
    connection = sqlite3.connect(
        f"file:{quote_database.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    connection.execute("PRAGMA query_only=ON")
    try:
        row = connection.execute(
            """
            SELECT sequence,published_epoch,payload_json FROM quote_snapshots_v2
            WHERE published_epoch <= ? ORDER BY published_epoch DESC LIMIT 1
            """,
            (float(cutoff_epoch),),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        return None
    return int(row[0]), float(row[1]), json.loads(str(row[2]))


def active_candidates(signal_database: Path, cutoff_epoch: float) -> list[dict[str, Any]]:
    connection = sqlite3.connect(
        f"file:{signal_database.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    connection.execute("PRAGMA query_only=ON")
    try:
        rows = connection.execute(
            """
            SELECT candidate_id,published_epoch,expires_epoch,source,payload_json
            FROM candidates
            WHERE published_epoch <= ? AND expires_epoch >= ?
            ORDER BY candidate_id
            """,
            (float(cutoff_epoch), float(cutoff_epoch)),
        ).fetchall()
    finally:
        connection.close()
    output = []
    for candidate_id, published, expires, source, payload in rows:
        try:
            value = json.loads(str(payload))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(value, dict):
            continue
        output.append(
            {
                **value,
                "id": str(value.get("id") or candidate_id),
                "feed_candidate_id": str(candidate_id),
                "feed_published_epoch": finite(value.get("feed_published_epoch"), published),
                "feed_expires_epoch": float(expires),
                "feed_source": str(value.get("feed_source") or source),
            }
        )
    return output


def quote_map(payload: dict[str, Any]) -> dict[str, dict[str, float]]:
    output: dict[str, dict[str, float]] = {}
    for instrument, quote in (payload.get("quotes") or {}).items():
        bid, ask = finite(quote.get("bid")), finite(quote.get("ask"))
        pip = finite(quote.get("pip"), 0.01 if str(instrument).endswith("_JPY") else 0.0001)
        if bid <= 0.0 or ask <= bid or pip <= 0.0:
            continue
        output[str(instrument)] = {
            "bid": bid,
            "ask": ask,
            "pip": pip,
            "spread_pips": (ask - bid) / pip,
        }
    return output


def _account_state(account_payload: dict[str, Any]) -> dict[str, Any]:
    accounts = account_payload.get("accounts") or []
    account = next(
        (
            row for row in accounts
            if isinstance(row, dict)
            and str(row.get("account_id") or "").endswith("-007")
        ),
        next((row for row in accounts if isinstance(row, dict)), {}),
    )
    aggregate = (
        account_payload.get("aggregate")
        if isinstance(account_payload.get("aggregate"), dict)
        else {}
    )
    snapshot_state = str(aggregate.get("snapshot_state") or "").strip().lower()
    reasons: list[str] = []
    if not account:
        reasons.append("account_row_missing")
    if account.get("ok") is not True:
        reasons.append("account_snapshot_not_ok")
    if snapshot_state == "unavailable" or snapshot_state.startswith("retained_stale"):
        reasons.append(snapshot_state)
    if account.get("positions_current") is False or aggregate.get("positions_current") is False:
        reasons.append("positions_current_false")
    if account.get("account_values_current") is False or aggregate.get("account_values_current") is False:
        reasons.append("account_values_current_false")
    trades = account.get("trades")
    if not isinstance(trades, list):
        reasons.append("positions_missing")
    current = not reasons
    return {
        "current": current,
        "reason": "current" if current else "account_state_unavailable",
        "details": sorted(set(reasons)),
        "snapshot_state": snapshot_state or ("current" if current else "unavailable"),
        "account": account,
        "positions": list(trades) if current else None,
    }


def _account_positions(account_payload: dict[str, Any]) -> list[dict[str, Any]] | None:
    return _account_state(account_payload)["positions"]


def _reentry_reason(candidate: dict[str, Any], executor_payload: dict[str, Any]) -> str | None:
    guard = ((executor_payload.get("details") or {}).get("reentry_guard") or {})
    instrument = str(candidate.get("instrument") or "")
    direction = str(candidate.get("direction") or "").lower()
    instrument_age = finite((guard.get("instrument_exit_ages_sec") or {}).get(instrument), math.inf)
    if instrument_age < finite(guard.get("instrument_cooldown_sec"), 0.0):
        return "instrument_reentry_cooldown"
    direction_age = finite(
        (guard.get("pair_direction_exit_ages_sec") or {}).get(f"{instrument}|{direction}"),
        math.inf,
    )
    if direction_age < finite(guard.get("pair_direction_cooldown_sec"), 0.0):
        return "pair_direction_reentry_cooldown"
    return None


def compact_candidate(
    candidate: dict[str, Any],
    *,
    quotes: dict[str, dict[str, float]],
    account_payload: dict[str, Any],
    executor_payload: dict[str, Any],
    config: dict[str, Any],
    decision_epoch: float,
) -> dict[str, Any]:
    allocator = config.get("allocator") or {}
    selection = allocator.get("selection") or {}
    candidate_id = str(candidate.get("feed_candidate_id") or candidate.get("id") or "")
    instrument = str(candidate.get("instrument") or "")
    direction = str(candidate.get("direction") or "").lower()
    quote = quotes.get(instrument) or {}
    pip = finite(quote.get("pip"), finite(candidate.get("pip"), 0.0001))
    spread = finite(quote.get("spread_pips"), finite(candidate.get("spread_pips"), math.inf))
    horizon = int(
        max(
            1.0,
            finite(
                candidate.get("execution_exit_horizon_sec"),
                finite(candidate.get("execution_horizon_sec"), finite(candidate.get("forecast_horizon_sec"), 3600.0)),
            ),
        )
    )
    predicted_signed = candidate.get("predicted_signed_pips")
    raw_return = (
        (1.0 if direction == "buy" else -1.0) * finite(predicted_signed)
        if predicted_signed is not None
        else finite(candidate.get("projected_gross_movement_pips"), finite(candidate.get("projected_net_pips")))
    )
    projected_net = finite(
        candidate.get("instant_projected_net_pips"),
        finite(candidate.get("projected_net_pips")),
    )
    validation = candidate.get("execution_validation") or {}
    validated = bool(validation.get("validated", candidate.get("validated", False)))
    reasons: list[str] = []
    if not candidate_id:
        reasons.append("missing_candidate_id")
    if direction not in {"buy", "sell"}:
        reasons.append("invalid_direction")
    if not quote:
        reasons.append("missing_executable_quote")
    if bool(selection.get("require_account_eligible")) and not bool(candidate.get("account_eligible")):
        reasons.append("account_ineligible")
    if bool(selection.get("require_signal_eligible")) and not bool(candidate.get("signal_eligible")):
        reasons.append("signal_ineligible")
    if bool(selection.get("require_validation")) and not validated:
        reasons.append("unvalidated")
    if bool(selection.get("reject_direction_conflict")) and bool(candidate.get("direction_conflict")):
        reasons.append("direction_conflict")
    account_state = _account_state(account_payload)
    positions = account_state["positions"]
    if not account_state["current"]:
        reasons.append("account_state_unavailable")
    if (
        positions is not None
        and bool(selection.get("respect_capacity"))
        and len(positions) >= int(selection.get("maximum_open_positions") or 4)
    ):
        reasons.append("account_capacity")
    if (
        positions is not None
        and bool(selection.get("respect_capacity"))
        and "JPY" in instrument.split("_")
    ):
        open_jpy = sum(
            1
            for trade in positions
            if "JPY" in str(trade.get("instrument") or "").split("_")
        )
        if open_jpy >= int(selection.get("maximum_open_jpy_factor_positions") or 1):
            reasons.append("jpy_factor_capacity")
    if bool(selection.get("respect_reentry")):
        reentry = _reentry_reason(candidate, executor_payload)
        if reentry:
            reasons.append(reentry)
    if projected_net < finite(allocator.get("minimum_incremental_edge_pips"), 0.5):
        reasons.append("projected_net_below_allocator_minimum")
    return {
        "candidate_id": candidate_id,
        "source_candidate_id": str(candidate.get("id") or ""),
        "feed_source": str(candidate.get("feed_source") or ""),
        "feed_published_epoch": finite(candidate.get("feed_published_epoch")),
        "instrument": instrument,
        "direction": direction,
        "family": str(candidate.get("family") or "unknown"),
        "lane_id": str(candidate.get("lane_id") or ""),
        "model_id": str(candidate.get("model_id") or ""),
        "strategy_archetype": str(candidate.get("strategy_archetype") or ""),
        "horizon_sec": horizon,
        "target_epoch": float(decision_epoch) + horizon,
        "entry_bid": finite(quote.get("bid")),
        "entry_ask": finite(quote.get("ask")),
        "pip": pip,
        "spread_pips": spread,
        "raw_predicted_return_pips": raw_return,
        "predicted_magnitude_pips": finite(candidate.get("predicted_magnitude_pips"), abs(raw_return)),
        "probability_up": candidate.get("probability_up"),
        "signal_confidence": finite(candidate.get("signal_confidence"), 0.5),
        "projected_net_pips": projected_net,
        "cost_adjusted_rank_pips": projected_net,
        "account_eligible": bool(candidate.get("account_eligible")),
        "signal_eligible": bool(candidate.get("signal_eligible")),
        "validated": validated,
        "direction_conflict": bool(candidate.get("direction_conflict")),
        "factor_conflicts": list(candidate.get("conflicted_archetypes") or []),
        "source_blockers": list(candidate.get("signal_blocked_by") or []),
        "policy_filter_reasons": sorted(set(reasons)),
        "policy_eligible": not reasons,
        "inclusion_probability": finite(
            ((allocator.get("sampling") or {}).get("inclusion_probability")), 1.0
        ),
        "model_version": candidate.get("model_version"),
        "feature_version": candidate.get("feature_version"),
        "cost_contract": "projected_net_pips_is_producer_after_cost_score; executable_outcome_uses_bid_ask",
    }


def rank_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        finite(row.get("cost_adjusted_rank_pips")),
        finite(row.get("signal_confidence")),
        -finite(row.get("spread_pips"), math.inf),
        str(row.get("candidate_id") or ""),
    )


def comparator_choices(
    candidates: list[dict[str, Any]],
    *,
    decision_id: str,
    config: dict[str, Any],
    account_payload: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    account_state = _account_state(account_payload)
    eligible = [row for row in candidates if row["policy_eligible"]]
    ranked = sorted(eligible, key=rank_key, reverse=True)
    choices: dict[str, dict[str, Any]] = {
        name: {"candidate_id": None, "action": "no_trade", "reason": "eligible_set_empty"}
        for name in COMPARATORS
    }
    choices["no_trade"]["reason"] = "fixed_baseline"
    if not account_state["current"]:
        for name, choice in choices.items():
            if name != "no_trade":
                choice["reason"] = "account_state_unavailable"
        return choices
    if ranked:
        choices["frozen_policy"] = {
            "candidate_id": ranked[0]["candidate_id"],
            "action": "select_top_one",
            "reason": "highest_frozen_cost_adjusted_rank_after_conflict_capacity_reentry",
        }
        seeded = random.Random(int(stable_hash({"decision_id": decision_id, "arm": "random"})[:16], 16))
        random_row = eligible[seeded.randrange(len(eligible))]
        choices["deterministic_random_same_eligible_set"] = {
            "candidate_id": random_row["candidate_id"],
            "action": "select_top_one",
            "reason": "deterministic_hash_draw_with_logged_inclusion_probability",
        }
        raw = max(eligible, key=lambda row: (finite(row["raw_predicted_return_pips"]), row["candidate_id"]))
        choices["highest_raw_predicted_return"] = {
            "candidate_id": raw["candidate_id"], "action": "select_top_one", "reason": "raw_prediction_rank"
        }
        low_cost = min(eligible, key=lambda row: (finite(row["spread_pips"], math.inf), row["candidate_id"]))
        choices["lowest_cost_eligible"] = {
            "candidate_id": low_cost["candidate_id"], "action": "select_top_one", "reason": "minimum_executable_spread"
        }
        strongest = str((config.get("allocator") or {}).get("strongest_constituent_family") or "")
        family_rows = [row for row in eligible if row["family"] == strongest]
        if family_rows:
            selected = max(family_rows, key=rank_key)
            choices["strongest_constituent_family"] = {
                "candidate_id": selected["candidate_id"], "action": "select_top_one", "reason": strongest
            }
        else:
            choices["strongest_constituent_family"]["reason"] = "configured_family_absent_from_eligible_set"
    positions = account_state["positions"] or []
    if positions:
        trade = positions[0]
        instrument = str(trade.get("instrument") or "")
        direction = "buy" if finite(trade.get("currentUnits")) > 0 else "sell"
        matched = next(
            (row for row in candidates if row["instrument"] == instrument and row["direction"] == direction),
            None,
        )
        choices["hold_current_position"] = {
            "candidate_id": None if matched is None else matched["candidate_id"],
            "action": "hold" if matched is not None else "hold_unscored",
            "reason": "current_account_position",
            "trade_id": trade.get("id"),
        }
    else:
        choices["hold_current_position"]["reason"] = "account_flat"
    return choices


def record_decision(
    connection: sqlite3.Connection,
    *,
    cohort: dict[str, Any],
    config: dict[str, Any],
    signal_database: Path,
    quote_database: Path,
    account_path: Path,
    executor_path: Path,
    now_epoch: float | None = None,
) -> dict[str, Any]:
    current = float(now_epoch if now_epoch is not None else time.time())
    cadence = int((config.get("allocator") or {}).get("decision_cadence_sec") or 300)
    bucket = int(current // cadence)
    existing = connection.execute(
        "SELECT decision_id,decision_json FROM decisions WHERE cohort_id=? AND decision_bucket=?",
        (cohort["cohort_id"], bucket),
    ).fetchone()
    if existing:
        return {"status": "already_recorded", **json.loads(str(existing[1]))}
    quote_record = latest_quote_snapshot(quote_database, current)
    quote_sequence: int | None = None
    quote_published: float | None = None
    quotes: dict[str, dict[str, float]] = {}
    quote_age = math.inf
    if quote_record is not None:
        quote_sequence, quote_published, quote_payload = quote_record
        quote_age = max(0.0, current - quote_published)
        if quote_age <= finite((config.get("allocator") or {}).get("maximum_quote_age_sec"), 15.0):
            quotes = quote_map(quote_payload)
    raw = active_candidates(signal_database, current)
    account = read_json(account_path)
    account_state = _account_state(account)
    executor = read_json(executor_path)
    compact = [
        compact_candidate(
            row,
            quotes=quotes,
            account_payload=account,
            executor_payload=executor,
            config=config,
            decision_epoch=current,
        )
        for row in raw
    ]
    compact = [row for row in compact if row["candidate_id"]]
    compact.sort(key=lambda row: row["candidate_id"])
    decision_id = "allocator_decision_" + stable_hash(
        {"cohort_id": cohort["cohort_id"], "bucket": bucket}
    )[:32]
    choices = comparator_choices(
        compact,
        decision_id=decision_id,
        config=config,
        account_payload=account,
    )
    selected_id = choices["frozen_policy"].get("candidate_id")
    rejected = [row for row in compact if not row["policy_eligible"]]
    best_rejected = max(rejected, key=rank_key) if rejected else None
    reason_counts: dict[str, int] = {}
    for row in compact:
        for reason in row["policy_filter_reasons"]:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
    eligible_count = sum(bool(row["policy_eligible"]) for row in compact)
    payload = {
        "decision_id": decision_id,
        "cohort_id": cohort["cohort_id"],
        "allocator_version": cohort["policy_id"],
        "decision_bucket": bucket,
        "decision_epoch": current,
        "decision_utc": datetime.fromtimestamp(current, timezone.utc).isoformat(),
        "candidate_cutoff_epoch": current,
        "complete_prefilter_candidate_count": len(compact),
        "policy_eligible_candidate_count": eligible_count,
        "selected_candidate_id": selected_id,
        "explicit_no_trade": selected_id is None,
        "best_rejected_candidate_id": None if best_rejected is None else best_rejected["candidate_id"],
        "best_rejected_reasons": [] if best_rejected is None else best_rejected["policy_filter_reasons"],
        "filter_reason_counts": reason_counts,
        "factor_conflict_candidate_count": sum(bool(row["direction_conflict"]) for row in compact),
        "account_state_current": bool(account_state["current"]),
        "account_state": account_state["snapshot_state"],
        "account_state_reason": account_state["reason"],
        "account_state_details": account_state["details"],
        "account_open_positions": (
            len(account_state["positions"])
            if account_state["positions"] is not None
            else None
        ),
        "quote_sequence": quote_sequence,
        "quote_published_epoch": quote_published,
        "quote_age_sec": None if not math.isfinite(quote_age) else round(quote_age, 6),
        "quote_valid": bool(quotes),
        "comparators": choices,
        "sampling": (config.get("allocator") or {}).get("sampling") or {},
        "can_place_orders": False,
    }
    if eligible_count == 0:
        # An empty eligible universe is operational liveness, not an allocator
        # decision and not prospective policy evidence.  Keep the diagnostic in
        # the replaceable state publication, but do not grow any immutable
        # decision/candidate/comparator table.  Pending historical candidates
        # are still matured by ``run_allocator_cycle`` below.
        return {
            "status": "paused_no_eligible_universe",
            "decision_persisted": False,
            **payload,
        }
    connection.execute(
        "INSERT INTO decisions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            decision_id,
            cohort["cohort_id"],
            bucket,
            current,
            payload["decision_utc"],
            quote_sequence,
            quote_published,
            stable_hash(compact),
            len(compact),
            payload["policy_eligible_candidate_count"],
            selected_id,
            payload["best_rejected_candidate_id"],
            canonical_json(payload),
        ),
    )
    connection.executemany(
        "INSERT INTO decision_candidates VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                decision_id,
                row["candidate_id"],
                row["instrument"],
                row["direction"],
                row["family"],
                row["horizon_sec"],
                row["target_epoch"],
                int(row["policy_eligible"]),
                row["entry_bid"],
                row["entry_ask"],
                row["pip"],
                row["spread_pips"],
                row["raw_predicted_return_pips"],
                row["projected_net_pips"],
                canonical_json(row),
            )
            for row in compact
        ],
    )
    connection.executemany(
        "INSERT INTO comparator_selections VALUES (?,?,?,?)",
        [
            (decision_id, name, choice.get("candidate_id"), canonical_json(choice))
            for name, choice in choices.items()
        ],
    )
    connection.commit()
    return {"status": "recorded", "decision_persisted": True, **payload}


def first_quote_at_or_after(
    quote_database: Path, target_epoch: float
) -> tuple[int, float, dict[str, Any]] | None:
    connection = sqlite3.connect(
        f"file:{quote_database.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    connection.execute("PRAGMA query_only=ON")
    try:
        row = connection.execute(
            """
            SELECT sequence,published_epoch,payload_json FROM quote_snapshots_v2
            WHERE published_epoch >= ? ORDER BY published_epoch LIMIT 1
            """,
            (float(target_epoch),),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        return None
    return int(row[0]), float(row[1]), json.loads(str(row[2]))


def mature_candidates(
    connection: sqlite3.Connection,
    *,
    quote_database: Path,
    config: dict[str, Any],
    now_epoch: float | None = None,
) -> dict[str, int]:
    current = float(now_epoch if now_epoch is not None else time.time())
    maximum_delay = finite((config.get("allocator") or {}).get("maximum_outcome_delay_sec"), 15.0)
    rows = connection.execute(
        """
        SELECT c.decision_id,c.candidate_id,c.instrument,c.direction,c.target_epoch,
               c.entry_bid,c.entry_ask,c.pip,c.candidate_json
        FROM decision_candidates c
        LEFT JOIN candidate_outcomes o
          ON o.decision_id=c.decision_id AND o.candidate_id=c.candidate_id
        LEFT JOIN outcome_integrity_events i
          ON i.decision_id=c.decision_id AND i.candidate_id=c.candidate_id
        WHERE c.target_epoch <= ? AND o.candidate_id IS NULL AND i.event_id IS NULL
        ORDER BY c.target_epoch,c.decision_id,c.candidate_id
        """,
        (current,),
    ).fetchall()
    matured = invalid = 0
    snapshot_cache: dict[float, tuple[int, float, dict[str, Any]] | None] = {}
    for row in rows:
        decision_id, candidate_id, instrument, direction, target, entry_bid, entry_ask, pip, candidate_json = row
        candidate_payload = json.loads(str(candidate_json))
        if "missing_executable_quote" in set(candidate_payload.get("policy_filter_reasons") or ()):
            detail = {
                "target_epoch": float(target),
                "instrument": instrument,
                "forecast_time_reason": "missing_executable_quote",
            }
            connection.execute(
                "INSERT OR IGNORE INTO outcome_integrity_events VALUES (?,?,?,?,?,?,?)",
                (
                    "allocator_integrity_" + stable_hash({"decision": decision_id, "candidate": candidate_id, "type": "missing_entry_quote"})[:32],
                    decision_id, candidate_id, utc_now(), "missing_forecast_time_executable_quote",
                    "counterfactual outcome cannot be computed without a causal entry quote", canonical_json(detail),
                ),
            )
            invalid += 1
            continue
        target_value = float(target)
        snapshot = snapshot_cache.setdefault(
            target_value, first_quote_at_or_after(quote_database, target_value)
        )
        if snapshot is None:
            continue
        sequence, published, payload = snapshot
        delay = max(0.0, published - target_value)
        quotes = quote_map(payload)
        exit_quote = quotes.get(str(instrument))
        if delay > maximum_delay or exit_quote is None:
            detail = {
                "target_epoch": target_value,
                "quote_published_epoch": published,
                "outcome_delay_sec": delay,
                "instrument": instrument,
            }
            connection.execute(
                "INSERT OR IGNORE INTO outcome_integrity_events VALUES (?,?,?,?,?,?,?)",
                (
                    "allocator_integrity_" + stable_hash({"decision": decision_id, "candidate": candidate_id, "type": "late_or_missing_quote"})[:32],
                    decision_id,
                    candidate_id,
                    utc_now(),
                    "late_or_missing_executable_quote",
                    "outcome was not matured or backfilled",
                    canonical_json(detail),
                ),
            )
            invalid += 1
            continue
        entry_mid = (finite(entry_bid) + finite(entry_ask)) / 2.0
        exit_mid = (exit_quote["bid"] + exit_quote["ask"]) / 2.0
        sign = 1.0 if str(direction) == "buy" else -1.0
        gross = sign * (exit_mid - entry_mid) / finite(pip, 1e-12)
        executable = (
            (exit_quote["bid"] - finite(entry_ask)) / finite(pip, 1e-12)
            if str(direction) == "buy"
            else (finite(entry_bid) - exit_quote["ask"]) / finite(pip, 1e-12)
        )
        cost = gross - executable
        outcome = {
            "decision_id": decision_id,
            "candidate_id": candidate_id,
            "instrument": instrument,
            "direction": direction,
            "target_epoch": target_value,
            "quote_sequence": sequence,
            "quote_published_epoch": published,
            "outcome_delay_sec": delay,
            "gross_mid_pips": gross,
            "executable_net_pips": executable,
            "realized_cost_pips": cost,
            "candidate": candidate_payload,
        }
        connection.execute(
            "INSERT INTO candidate_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                decision_id,
                candidate_id,
                utc_now(),
                sequence,
                published,
                delay,
                exit_quote["bid"],
                exit_quote["ask"],
                gross,
                executable,
                cost,
                canonical_json(outcome),
            ),
        )
        matured += 1
    connection.execute(
        """
        INSERT OR IGNORE INTO policy_outcomes(
            decision_id,comparator,candidate_id,matured_utc,
            executable_net_pips,outcome_json
        )
        SELECT s.decision_id,s.comparator,s.candidate_id,o.matured_utc,
               o.executable_net_pips,o.outcome_json
        FROM comparator_selections s
        JOIN candidate_outcomes o
          ON o.decision_id=s.decision_id AND o.candidate_id=s.candidate_id
        """
    )
    # The no-trade baseline is paired only with a genuinely evaluable frozen
    # policy opportunity. Empty candidate sets must not manufacture effective
    # sample size through repeated zero-return timestamps.
    connection.execute(
        """
        INSERT OR IGNORE INTO policy_outcomes(
            decision_id,comparator,candidate_id,matured_utc,
            executable_net_pips,outcome_json
        )
        SELECT f.decision_id,'no_trade',NULL,f.matured_utc,0.0,
               '{"action":"no_trade","paired_with_frozen_policy_opportunity":true}'
        FROM policy_outcomes f
        WHERE f.comparator='frozen_policy'
        """
    )
    connection.commit()
    return {"pending_matured": matured, "new_integrity_exclusions": invalid}


def _bh(pvalues: dict[str, float], q: float = 0.05) -> set[str]:
    ordered = sorted((max(0.0, min(1.0, value)), key) for key, value in pvalues.items())
    cutoff = 0
    for index, (value, _) in enumerate(ordered, start=1):
        if value <= q * index / max(1, len(ordered)):
            cutoff = index
    return {key for index, (_, key) in enumerate(ordered, start=1) if index <= cutoff}


def allocator_summary(
    connection: sqlite3.Connection,
    *,
    cohort: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    allocator = config.get("allocator") or {}
    decisions = int(
        connection.execute("SELECT COUNT(*) FROM decisions WHERE cohort_id=?", (cohort["cohort_id"],)).fetchone()[0]
    )
    complete = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM decisions d
            WHERE d.cohort_id=? AND d.policy_eligible_count > 0 AND NOT EXISTS (
                SELECT 1 FROM comparator_selections s
                LEFT JOIN policy_outcomes o ON o.decision_id=s.decision_id AND o.comparator=s.comparator
                WHERE s.decision_id=d.decision_id AND s.candidate_id IS NOT NULL
                  AND o.comparator IS NULL
            )
            """,
            (cohort["cohort_id"],),
        ).fetchone()[0]
    )
    arms: dict[str, dict[str, Any]] = {}
    minimum_edge = finite(allocator.get("minimum_incremental_edge_pips"), 0.5)
    alpha = finite(allocator.get("sequential_alpha"), 0.01)
    bound = finite(allocator.get("sequential_clip_bound_pips"), 250.0)
    pvalues: dict[str, float] = {}
    for comparator in COMPARATORS:
        values = [
            float(row[0])
            for row in connection.execute(
                """
                SELECT o.executable_net_pips FROM policy_outcomes o
                JOIN decisions d ON d.decision_id=o.decision_id
                WHERE d.cohort_id=? AND o.comparator=? ORDER BY d.decision_epoch
                """,
                (cohort["cohort_id"], comparator),
            )
        ]
        lcb, ucb = alpha_spending_confidence_sequence(values, alpha=alpha, bound=bound)
        std = statistics.stdev(values) if len(values) >= 2 else 0.0
        ordinary_radius = 1.645 * std / math.sqrt(len(values)) if len(values) >= 2 else math.inf
        mean = statistics.fmean(values) if values else 0.0
        # A conservative fixed-sample one-sided normal diagnostic.  It cannot
        # promote by itself; the sequential lower bound and adjusted discovery
        # lock are also required.
        z = mean / max(1e-12, std / math.sqrt(len(values))) if len(values) >= 2 and std > 0 else 0.0
        pvalue = 0.5 * math.erfc(z / math.sqrt(2.0)) if z > 0 else 1.0
        pvalues[comparator] = pvalue
        arms[comparator] = {
            "matured_decisions": len(values),
            "average_executable_net_pips": round(mean, 6),
            "time_uniform_lower_bound_pips": round(lcb, 6),
            "time_uniform_upper_bound_pips": round(ucb, 6),
            "minimum_economic_edge_pips": minimum_edge,
            "distance_to_promotion_boundary_pips": round(lcb - minimum_edge, 6),
            "distance_to_futility_boundary_pips": round(ucb - minimum_edge, 6),
            "ordinary_upper_bound_pips": round(mean + ordinary_radius, 6)
            if math.isfinite(ordinary_radius) else None,
            "one_sided_pvalue_zero": pvalue,
        }
    adjusted = _bh({key: value for key, value in pvalues.items() if key != "no_trade"})
    for name, row in arms.items():
        row["multiplicity_adjusted_discovery_survivor"] = name in adjusted
    primary = arms["frozen_policy"]
    state = (
        "futility_rejected"
        if primary["matured_decisions"] > 0 and primary["distance_to_futility_boundary_pips"] < 0.0
        else "discovery_candidate"
        if primary["multiplicity_adjusted_discovery_survivor"]
        and primary["distance_to_promotion_boundary_pips"] > 0.0
        else "continue_collecting"
    )
    return {
        "cohort_id": cohort["cohort_id"],
        "policy_id": cohort["policy_id"],
        "cohort_start_utc": cohort["cohort_start_utc"],
        "lifecycle_state": state,
        "decision_count": decisions,
        "fully_matured_decision_count": complete,
        "arms": arms,
        "multiple_testing": {
            "method": "benjamini_hochberg_across_policy_and_comparator_arms",
            "q": 0.05,
            "survivors": sorted(adjusted),
        },
        "confirmation": {
            "required": True,
            "same_discovery_window_cannot_confirm": True,
            "practice_canary_auto_route": False,
        },
    }


def record_allocator_lifecycle(
    connection: sqlite3.Connection,
    *,
    cohort: dict[str, Any],
    summary: dict[str, Any],
    config: dict[str, Any],
) -> None:
    row = connection.execute(
        "SELECT next_state FROM allocator_lifecycle_events WHERE cohort_id=? ORDER BY observed_utc DESC,rowid DESC LIMIT 1",
        (cohort["cohort_id"],),
    ).fetchone()
    previous = None if row is None else str(row[0])
    desired = str(summary["lifecycle_state"])
    if previous == "futility_rejected":
        desired = previous
    if desired == previous:
        return
    event = {
        "cohort_id": cohort["cohort_id"],
        "previous": previous,
        "next": desired,
        "observed_utc": utc_now(),
        "evidence": summary,
    }
    connection.execute(
        "INSERT INTO allocator_lifecycle_events VALUES (?,?,?,?,?,?)",
        (
            "allocator_lifecycle_" + stable_hash(event)[:32],
            cohort["cohort_id"],
            previous,
            desired,
            event["observed_utc"],
            canonical_json(summary),
        ),
    )
    # Discovery only opens a lock.  A distinct later confirmation cohort is
    # required and is never backdated or routed automatically.
    if desired == "discovery_candidate" and not connection.execute(
        "SELECT 1 FROM allocator_candidate_locks WHERE discovery_cohort_id=?",
        (cohort["cohort_id"],),
    ).fetchone():
        lock_id = "allocator_lock_" + stable_hash(event)[:32]
        locked = event["observed_utc"]
        connection.execute(
            "INSERT INTO allocator_candidate_locks VALUES (?,?,?,?)",
            (lock_id, cohort["cohort_id"], locked, canonical_json(summary)),
        )
        confirmation = ensure_allocator_cohort(
            connection,
            config,
            phase="confirmation",
            start_utc=locked,
        )
        connection.execute(
            "INSERT OR IGNORE INTO allocator_confirmation_cohorts VALUES (?,?,?,?,?)",
            (
                confirmation["cohort_id"],
                lock_id,
                locked,
                "collecting",
                canonical_json({"discovery_decisions_excluded": True, "can_place_orders": False}),
            ),
        )
    connection.commit()


def run_allocator_cycle(
    *,
    database_path: Path = DEFAULT_DATABASE,
    state_path: Path = DEFAULT_STATE,
    config_path: Path = DEFAULT_CONFIG,
    signal_database: Path = DEFAULT_SIGNAL_FEED,
    quote_database: Path = DEFAULT_QUOTES,
    account_path: Path = DEFAULT_ACCOUNT,
    executor_path: Path = DEFAULT_EXECUTOR,
    now_epoch: float | None = None,
) -> dict[str, Any]:
    config = read_json(config_path)
    connection = initialize_database(database_path)
    try:
        cohort = ensure_allocator_cohort(connection, config)
        decision = record_decision(
            connection,
            cohort=cohort,
            config=config,
            signal_database=signal_database,
            quote_database=quote_database,
            account_path=account_path,
            executor_path=executor_path,
            now_epoch=now_epoch,
        )
        maturation = mature_candidates(
            connection,
            quote_database=quote_database,
            config=config,
            now_epoch=now_epoch,
        )
        summary = allocator_summary(connection, cohort=cohort, config=config)
        collection_state = (
            "paused_no_eligible_universe"
            if decision.get("status") == "paused_no_eligible_universe"
            else "collecting"
        )
        summary["collection_state"] = collection_state
        record_allocator_lifecycle(
            connection,
            cohort=cohort,
            summary=summary,
            config=config,
        )
        counts = {
            "candidate_outcomes": int(connection.execute("SELECT COUNT(*) FROM candidate_outcomes").fetchone()[0]),
            "integrity_exclusions": int(connection.execute("SELECT COUNT(*) FROM outcome_integrity_events").fetchone()[0]),
            "confirmation_cohorts": int(connection.execute("SELECT COUNT(*) FROM allocator_confirmation_cohorts").fetchone()[0]),
        }
    finally:
        connection.close()
    payload = {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "research_only": True,
        "can_place_orders": False,
        "can_promote": False,
        "real_money_routing": False,
        "cohort": cohort,
        "collection_state": collection_state,
        "last_decision": decision,
        "maturation": maturation,
        "evidence": summary,
        "counts": counts,
    }
    atomic_json(state_path, payload)
    return payload


__all__ = [
    "AllocatorLineageError",
    "COMPARATORS",
    "allocator_summary",
    "compact_candidate",
    "ensure_allocator_cohort",
    "initialize_database",
    "mature_candidates",
    "record_decision",
    "replay_allocator_active_cohort",
    "run_allocator_cycle",
]
