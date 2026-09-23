#!/usr/bin/env python3
"""Run the frozen, research-only high-volume counterfactual FX SIM gym.

The historical runner enumerates virtual orders against completed OANDA M1
bid/ask candles.  It stores immutable definitions, compact cell aggregates,
and bounded mistake examples.  It has no broker or operational imports and can
never place, authorize, promote, or close an order.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

try:
    from forex_system.research.counterfactual_sim_gym_v1 import (
        CONTRACT_ID,
        POLICY,
        ArmDefinition,
        PairMarket,
        SignalObservation,
        arm_as_dict,
        build_arm_definitions,
        canonical_json,
        deterministic_random_side,
        evaluate_signal,
        file_sha256,
        historical_partition_interval,
        iso,
        liquidity_bucket,
        load_pair_market,
        market_episode_id,
        session_bucket,
        signed_currency_factors,
        simulate_order,
        stable_hash,
    )
except ModuleNotFoundError:
    from src.forex_system.research.counterfactual_sim_gym_v1 import (
        CONTRACT_ID,
        POLICY,
        ArmDefinition,
        PairMarket,
        SignalObservation,
        arm_as_dict,
        build_arm_definitions,
        canonical_json,
        deterministic_random_side,
        evaluate_signal,
        file_sha256,
        historical_partition_interval,
        iso,
        liquidity_bucket,
        load_pair_market,
        market_episode_id,
        session_bucket,
        signed_currency_factors,
        simulate_order,
        stable_hash,
    )


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "counterfactual_sim_gym_v1.json"
ARTIFACT_ROOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "research_ledgers"
    / "counterfactual_sim_gym_v1"
)
SIM_TABLES = {
    "sim_cohorts",
    "sim_cohort_transitions",
    "sim_arms",
    "sim_decision_clocks",
    "sim_signal_observations",
    "sim_outcome_parts",
    "sim_intent_map",
    "sim_cell_aggregates",
    "sim_mistake_samples",
    "sim_run_snapshots",
    "sim_integrity_events",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"object JSON required: {path}")
    return value


def atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    atomic_text(path, json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def _is_link_or_junction(path: Path) -> bool:
    return path.is_symlink() or bool(getattr(path, "is_junction", lambda: False)())


def safe_artifact_path(relative: str) -> Path:
    candidate = Path(str(relative))
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError(f"artifact path must be contained and relative: {relative}")
    if ARTIFACT_ROOT.exists() and _is_link_or_junction(ARTIFACT_ROOT):
        raise ValueError(f"artifact root is link/junction: {ARTIFACT_ROOT}")
    root = ARTIFACT_ROOT.resolve()
    current = ARTIFACT_ROOT
    for part in candidate.parts[:-1]:
        current = current / part
        if current.exists() and _is_link_or_junction(current):
            raise ValueError(f"artifact path traverses link/junction: {current}")
    resolved = (ARTIFACT_ROOT / candidate).resolve()
    if not resolved.is_relative_to(root):
        raise ValueError(f"artifact path escapes dedicated research root: {relative}")
    if resolved.exists() and _is_link_or_junction(resolved):
        raise ValueError(f"artifact target is link/junction: {resolved}")
    return resolved


def _mean(values: Sequence[float]) -> float | None:
    return float(sum(values) / len(values)) if values else None


@dataclass(frozen=True)
class InfluenceObservation:
    interval_start_epoch: int
    interval_end_epoch: int
    signed_factors: frozenset[str]
    instrument: str
    value: float


@dataclass
class CellAccumulator:
    raw_n: int = 0
    win_n: int = 0
    value_sum: float = 0.0
    value_sum_sq: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    moderate_sum: float = 0.0
    severe_sum: float = 0.0
    mfe_sum: float = 0.0
    mae_sum: float = 0.0
    latency_sum: float = 0.0
    minimum_value: float = math.inf
    maximum_value: float = -math.inf
    exit_reasons: Counter[str] = field(default_factory=Counter)
    influence_observations_by_start: dict[int, list[InfluenceObservation]] = field(
        default_factory=dict
    )
    effective_next_available: dict[str, int] = field(default_factory=dict)
    effective_n: int = 0
    effective_value_sum: float = 0.0
    effective_value_sum_sq: float = 0.0
    effective_positive_total: float = 0.0
    effective_best_positive: float = 0.0
    effective_overlap_purged_n: int = 0

    def add(
        self,
        *,
        value: float,
        moderate: float,
        severe: float,
        mfe: float,
        mae: float,
        latency: float,
        exit_reason: str,
        interval_start_epoch: int,
        interval_end_epoch: int,
        signed_factors: Iterable[str],
        instrument: str,
    ) -> None:
        number = float(value)
        self.raw_n += 1
        self.win_n += int(number > 0.0)
        self.value_sum += number
        self.value_sum_sq += number * number
        self.gross_profit += max(0.0, number)
        self.gross_loss += max(0.0, -number)
        self.moderate_sum += float(moderate)
        self.severe_sum += float(severe)
        self.mfe_sum += float(mfe)
        self.mae_sum += float(mae)
        self.latency_sum += float(latency)
        self.minimum_value = min(self.minimum_value, number)
        self.maximum_value = max(self.maximum_value, number)
        self.exit_reasons[str(exit_reason)] += 1
        factor_set = frozenset(str(item).upper() for item in signed_factors if str(item))
        if not factor_set:
            raise ValueError("effective-evidence observation requires signed factors")
        if int(interval_end_epoch) <= int(interval_start_epoch):
            raise ValueError("invalid influence interval")
        observation = InfluenceObservation(
            int(interval_start_epoch),
            int(interval_end_epoch),
            factor_set,
            str(instrument).upper(),
            number,
        )
        self.influence_observations_by_start.setdefault(
            observation.interval_start_epoch, []
        ).append(observation)

    def _flush_pending_effective_components(self) -> None:
        if not self.influence_observations_by_start:
            return
        for start in sorted(self.influence_observations_by_start):
            rows = self.influence_observations_by_start[start]
            remaining = set(range(len(rows)))
            while remaining:
                seed = min(remaining)
                component = {seed}
                resources = set(rows[seed].signed_factors)
                resources.add(f"PAIR:{rows[seed].instrument}")
                remaining.remove(seed)
                changed = True
                while changed:
                    changed = False
                    for candidate in sorted(remaining):
                        candidate_resources = set(rows[candidate].signed_factors)
                        candidate_resources.add(f"PAIR:{rows[candidate].instrument}")
                        if not candidate_resources.isdisjoint(resources):
                            remaining.remove(candidate)
                            component.add(candidate)
                            resources.update(candidate_resources)
                            changed = True
                end = max(rows[index].interval_end_epoch for index in component)
                if any(
                    start < self.effective_next_available.get(resource, -2**63)
                    for resource in resources
                ):
                    self.effective_overlap_purged_n += len(component)
                    continue
                value = sum(rows[index].value for index in component) / len(component)
                self.effective_n += 1
                self.effective_value_sum += value
                self.effective_value_sum_sq += value * value
                positive = max(0.0, value)
                self.effective_positive_total += positive
                self.effective_best_positive = max(self.effective_best_positive, positive)
                for resource in resources:
                    self.effective_next_available[resource] = end
        self.influence_observations_by_start.clear()

    def payload(self) -> dict[str, Any]:
        self._flush_pending_effective_components()
        effective_n = self.effective_n
        effective_average = self.effective_value_sum / effective_n if effective_n else None
        effective_variance = (
            max(
                0.0,
                (self.effective_value_sum_sq - self.effective_value_sum**2 / effective_n)
                / (effective_n - 1),
            )
            if effective_n > 1
            else 0.0
        )
        effective_std = math.sqrt(effective_variance)
        effective_lcb = (
            effective_average - 1.96 * effective_std / math.sqrt(effective_n)
            if effective_average is not None and effective_n > 0
            else None
        )
        positive_total = self.effective_positive_total
        best_positive = self.effective_best_positive
        return {
            "raw_n": self.raw_n,
            "effective_n": effective_n,
            "effective_overlap_purged_n": self.effective_overlap_purged_n,
            "win_rate": self.win_n / self.raw_n if self.raw_n else None,
            "average_net_pips": self.value_sum / self.raw_n if self.raw_n else None,
            "no_trade_average_net_pips": 0.0,
            "average_delta_vs_no_trade_pips": self.value_sum / self.raw_n if self.raw_n else None,
            "effective_average_net_pips": effective_average,
            "unadjusted_normal_lower_95_mean_pips": effective_lcb,
            "profit_factor": self.gross_profit / self.gross_loss if self.gross_loss > 0.0 else None,
            "moderate_stress_average_net_pips": self.moderate_sum / self.raw_n if self.raw_n else None,
            "severe_stress_average_net_pips": self.severe_sum / self.raw_n if self.raw_n else None,
            "average_mfe_pips": self.mfe_sum / self.raw_n if self.raw_n else None,
            "average_mae_pips": self.mae_sum / self.raw_n if self.raw_n else None,
            "average_missed_entry_slippage_pips": self.latency_sum / self.raw_n if self.raw_n else None,
            "minimum_net_pips": self.minimum_value if self.raw_n else None,
            "maximum_net_pips": self.maximum_value if self.raw_n else None,
            "best_effective_episode_positive_share": best_positive / positive_total if positive_total > 0.0 else None,
            "exit_reasons": dict(sorted(self.exit_reasons.items())),
        }


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=60.0)
    existing_tables = {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    unexpected = sorted(existing_tables - SIM_TABLES)
    if unexpected:
        connection.close()
        raise ValueError(f"refusing non-SIM database: {path}: {unexpected}")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS sim_cohorts (
          cohort_id TEXT PRIMARY KEY,experiment_key TEXT NOT NULL,created_utc TEXT NOT NULL,
          material_contract_sha256 TEXT NOT NULL,config_sha256 TEXT NOT NULL,
          runner_sha256 TEXT NOT NULL,core_sha256 TEXT NOT NULL,material_contract_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sim_cohort_transitions (
          transition_id TEXT PRIMARY KEY,experiment_key TEXT NOT NULL,previous_cohort_id TEXT,
          next_cohort_id TEXT NOT NULL,observed_utc TEXT NOT NULL,reason TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sim_arms (
          cohort_id TEXT NOT NULL,arm_id TEXT NOT NULL,definition_sha256 TEXT NOT NULL,
          definition_json TEXT NOT NULL,PRIMARY KEY(cohort_id,arm_id),
          FOREIGN KEY(cohort_id) REFERENCES sim_cohorts(cohort_id)
        );
        CREATE TABLE IF NOT EXISTS sim_decision_clocks (
          clock_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,instrument TEXT NOT NULL,
          decision_epoch INTEGER NOT NULL,knowledge_cutoff_epoch INTEGER NOT NULL,
          source_candle_epoch INTEGER NOT NULL,session_bucket TEXT NOT NULL,
          decision_bid_close REAL NOT NULL,decision_ask_close REAL NOT NULL,
          row_sha256 TEXT NOT NULL,
          UNIQUE(cohort_id,instrument,decision_epoch),
          FOREIGN KEY(cohort_id) REFERENCES sim_cohorts(cohort_id)
        );
        CREATE TABLE IF NOT EXISTS sim_signal_observations (
          signal_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,clock_id TEXT NOT NULL,
          rule_id TEXT NOT NULL,raw_side INTEGER NOT NULL,score REAL NOT NULL,
          feature_value_pips REAL NOT NULL,knowledge_cutoff_epoch INTEGER NOT NULL,
          metadata_sha256 TEXT NOT NULL,metadata_json TEXT NOT NULL,row_sha256 TEXT NOT NULL,
          FOREIGN KEY(cohort_id) REFERENCES sim_cohorts(cohort_id),
          FOREIGN KEY(clock_id) REFERENCES sim_decision_clocks(clock_id)
        );
        CREATE TABLE IF NOT EXISTS sim_outcome_parts (
          outcome_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,clock_id TEXT NOT NULL,
          side INTEGER NOT NULL,entry_delay_min INTEGER NOT NULL,horizon_min INTEGER NOT NULL,
          exit_policy_id TEXT NOT NULL,status TEXT NOT NULL,exclusion_reason TEXT NOT NULL,
          outcome_sha256 TEXT NOT NULL,outcome_json TEXT NOT NULL,row_sha256 TEXT NOT NULL,
          FOREIGN KEY(cohort_id) REFERENCES sim_cohorts(cohort_id),
          FOREIGN KEY(clock_id) REFERENCES sim_decision_clocks(clock_id)
        );
        CREATE TABLE IF NOT EXISTS sim_intent_map (
          intent_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,signal_id TEXT NOT NULL,
          arm_id TEXT NOT NULL,outcome_id TEXT,realized_side INTEGER NOT NULL,
          partition_label TEXT,eligible_for_diagnostics INTEGER NOT NULL,
          exclusion_reason TEXT NOT NULL,signed_factor_1 TEXT NOT NULL,
          signed_factor_2 TEXT NOT NULL,row_sha256 TEXT NOT NULL,
          FOREIGN KEY(cohort_id) REFERENCES sim_cohorts(cohort_id),
          FOREIGN KEY(signal_id) REFERENCES sim_signal_observations(signal_id),
          FOREIGN KEY(cohort_id,arm_id) REFERENCES sim_arms(cohort_id,arm_id),
          FOREIGN KEY(outcome_id) REFERENCES sim_outcome_parts(outcome_id)
        );
        CREATE TABLE IF NOT EXISTS sim_cell_aggregates (
          cell_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,scope TEXT NOT NULL,
          instrument TEXT NOT NULL,arm_id TEXT NOT NULL,partition_label TEXT NOT NULL,
          session_bucket TEXT NOT NULL,liquidity_bucket TEXT NOT NULL,
          aggregate_sha256 TEXT NOT NULL,aggregate_json TEXT NOT NULL,
          FOREIGN KEY(cohort_id,arm_id) REFERENCES sim_arms(cohort_id,arm_id)
        );
        CREATE TABLE IF NOT EXISTS sim_mistake_samples (
          sample_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,cluster TEXT NOT NULL,
          severity REAL NOT NULL,decision_utc TEXT NOT NULL,instrument TEXT NOT NULL,
          arm_id TEXT NOT NULL,sample_sha256 TEXT NOT NULL,sample_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sim_run_snapshots (
          snapshot_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,generated_utc TEXT NOT NULL,
          aggregate_count INTEGER NOT NULL,aggregate_set_sha256 TEXT NOT NULL,
          statistics_sha256 TEXT NOT NULL,statistics_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sim_integrity_events (
          event_id TEXT PRIMARY KEY,cohort_id TEXT NOT NULL,observed_utc TEXT NOT NULL,
          event_type TEXT NOT NULL,details_sha256 TEXT NOT NULL,details_json TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS sim_cohorts_no_update BEFORE UPDATE ON sim_cohorts BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_cohorts_no_delete BEFORE DELETE ON sim_cohorts BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_cohort_transitions_no_update BEFORE UPDATE ON sim_cohort_transitions BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_cohort_transitions_no_delete BEFORE DELETE ON sim_cohort_transitions BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_arms_no_update BEFORE UPDATE ON sim_arms BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_arms_no_delete BEFORE DELETE ON sim_arms BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_decision_clocks_no_update BEFORE UPDATE ON sim_decision_clocks BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_decision_clocks_no_delete BEFORE DELETE ON sim_decision_clocks BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_signal_observations_no_update BEFORE UPDATE ON sim_signal_observations BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_signal_observations_no_delete BEFORE DELETE ON sim_signal_observations BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_outcome_parts_no_update BEFORE UPDATE ON sim_outcome_parts BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_outcome_parts_no_delete BEFORE DELETE ON sim_outcome_parts BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_intent_map_no_update BEFORE UPDATE ON sim_intent_map BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_intent_map_no_delete BEFORE DELETE ON sim_intent_map BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_cell_aggregates_no_update BEFORE UPDATE ON sim_cell_aggregates BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_cell_aggregates_no_delete BEFORE DELETE ON sim_cell_aggregates BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_mistake_samples_no_update BEFORE UPDATE ON sim_mistake_samples BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_mistake_samples_no_delete BEFORE DELETE ON sim_mistake_samples BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_run_snapshots_no_update BEFORE UPDATE ON sim_run_snapshots BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_run_snapshots_no_delete BEFORE DELETE ON sim_run_snapshots BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_integrity_events_no_update BEFORE UPDATE ON sim_integrity_events BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS sim_integrity_events_no_delete BEFORE DELETE ON sim_integrity_events BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    connection.commit()
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
    existing = connection.execute(
        f"SELECT {hash_column} FROM {table} WHERE {key_column}=?", (key,)
    ).fetchone()
    if existing is not None:
        if str(existing[0]) != str(expected_hash):
            raise ValueError(f"immutable conflict: {table}:{key}")
        return False
    connection.execute(
        f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
        tuple(values),
    )
    return True


def register_cohort(
    connection: sqlite3.Connection,
    *,
    cohort_id: str,
    experiment_key: str,
    contract: Mapping[str, Any],
    config_sha: str,
    runner_sha: str,
    core_sha: str,
    observed_utc: str,
) -> None:
    material_sha = stable_hash(contract)
    current = connection.execute(
        "SELECT next_cohort_id FROM sim_cohort_transitions WHERE experiment_key=? ORDER BY rowid DESC LIMIT 1",
        (experiment_key,),
    ).fetchone()
    current_id = str(current[0]) if current else None
    existing = connection.execute(
        "SELECT material_contract_sha256 FROM sim_cohorts WHERE cohort_id=?", (cohort_id,)
    ).fetchone()
    if existing is not None and str(existing[0]) != material_sha:
        raise ValueError("cohort identity collision")
    if current_id and current_id != cohort_id and existing is not None:
        raise ValueError("superseded cohort reactivation forbidden")
    if existing is None:
        connection.execute(
            "INSERT INTO sim_cohorts VALUES (?,?,?,?,?,?,?,?)",
            (
                cohort_id, experiment_key, observed_utc, material_sha, config_sha,
                runner_sha, core_sha, canonical_json(contract),
            ),
        )
    if current_id != cohort_id:
        transition_id = "simtransition_" + stable_hash(experiment_key, current_id, cohort_id)[:28]
        connection.execute(
            "INSERT INTO sim_cohort_transitions VALUES (?,?,?,?,?,?)",
            (transition_id, experiment_key, current_id, cohort_id, observed_utc, "material_contract_change" if current_id else "initial_registration"),
        )
    connection.commit()


def register_arms(connection: sqlite3.Connection, cohort_id: str, arms: Sequence[ArmDefinition]) -> None:
    for arm in arms:
        payload = arm_as_dict(arm)
        existing = connection.execute(
            "SELECT definition_sha256 FROM sim_arms WHERE cohort_id=? AND arm_id=?",
            (cohort_id, arm.arm_id),
        ).fetchone()
        if existing is not None:
            if str(existing[0]) != arm.definition_sha256:
                raise ValueError(f"arm identity conflict: {arm.arm_id}")
            continue
        connection.execute(
            "INSERT INTO sim_arms VALUES (?,?,?,?)",
            (cohort_id, arm.arm_id, arm.definition_sha256, canonical_json(payload)),
        )
    connection.commit()


def record_clock(
    connection: sqlite3.Connection,
    *,
    cohort_id: str,
    market: PairMarket,
    candle_index: int,
    decision_epoch: int,
) -> str:
    identity = {
        "cohort_id": cohort_id,
        "instrument": market.instrument,
        "decision_epoch": int(decision_epoch),
        "knowledge_cutoff_epoch": int(decision_epoch),
        "source_candle_epoch": int(market.epochs[candle_index]),
        "session_bucket": session_bucket(decision_epoch),
        "decision_bid_close": float(market.bid_close[candle_index]),
        "decision_ask_close": float(market.ask_close[candle_index]),
    }
    row_sha = stable_hash(identity)
    clock_id = "simclock_" + stable_hash(
        cohort_id, market.instrument, int(decision_epoch)
    )[:28]
    _insert_immutable(
        connection,
        table="sim_decision_clocks",
        key_column="clock_id",
        key=clock_id,
        hash_column="row_sha256",
        expected_hash=row_sha,
        columns=("clock_id", *identity.keys(), "row_sha256"),
        values=(clock_id, *identity.values(), row_sha),
    )
    return clock_id


def record_signal(
    connection: sqlite3.Connection,
    *,
    cohort_id: str,
    clock_id: str,
    observation: SignalObservation,
) -> str:
    metadata = dict(observation.metadata)
    metadata_sha = stable_hash(metadata)
    identity = {
        "cohort_id": cohort_id,
        "clock_id": clock_id,
        "rule_id": observation.rule_id,
        "raw_side": int(observation.side),
        "score": float(observation.score),
        "feature_value_pips": float(observation.feature_value_pips),
        "knowledge_cutoff_epoch": int(observation.knowledge_cutoff_epoch),
        "metadata_sha256": metadata_sha,
        "metadata_json": canonical_json(metadata),
    }
    row_sha = stable_hash(identity)
    signal_id = "simsignal_" + stable_hash(clock_id, observation.rule_id)[:28]
    _insert_immutable(
        connection,
        table="sim_signal_observations",
        key_column="signal_id",
        key=signal_id,
        hash_column="row_sha256",
        expected_hash=row_sha,
        columns=("signal_id", *identity.keys(), "row_sha256"),
        values=(signal_id, *identity.values(), row_sha),
    )
    return signal_id


def record_outcome_part(
    connection: sqlite3.Connection,
    *,
    cohort_id: str,
    clock_id: str,
    side: int,
    arm: ArmDefinition,
    result: Any,
) -> str:
    outcome_payload = asdict(result)
    outcome_sha = stable_hash(outcome_payload)
    identity = {
        "cohort_id": cohort_id,
        "clock_id": clock_id,
        "side": int(side),
        "entry_delay_min": int(arm.entry_delay_min),
        "horizon_min": int(arm.horizon_min),
        "exit_policy_id": str(arm.exit_policy.get("id") or ""),
        "status": str(result.status),
        "exclusion_reason": str(result.exclusion_reason),
        "outcome_sha256": outcome_sha,
        "outcome_json": canonical_json(outcome_payload),
    }
    row_sha = stable_hash(identity)
    outcome_id = "simoutcome_" + stable_hash(
        cohort_id,
        clock_id,
        int(side),
        int(arm.entry_delay_min),
        int(arm.horizon_min),
        arm.exit_policy,
    )[:28]
    _insert_immutable(
        connection,
        table="sim_outcome_parts",
        key_column="outcome_id",
        key=outcome_id,
        hash_column="row_sha256",
        expected_hash=row_sha,
        columns=("outcome_id", *identity.keys(), "row_sha256"),
        values=(outcome_id, *identity.values(), row_sha),
    )
    return outcome_id


def record_intent(
    connection: sqlite3.Connection,
    *,
    cohort_id: str,
    signal_id: str,
    arm_id: str,
    outcome_id: str | None,
    side: int,
    partition: str | None,
    eligible: bool,
    exclusion_reason: str,
    factors: Sequence[str],
) -> str:
    ordered_factors = tuple(sorted(str(value) for value in factors))
    if len(ordered_factors) != 2:
        raise ValueError("exactly two signed currency factors required")
    identity = {
        "cohort_id": cohort_id,
        "signal_id": signal_id,
        "arm_id": arm_id,
        "outcome_id": outcome_id,
        "realized_side": int(side),
        "partition_label": partition,
        "eligible_for_diagnostics": int(bool(eligible)),
        "exclusion_reason": str(exclusion_reason),
        "signed_factor_1": ordered_factors[0],
        "signed_factor_2": ordered_factors[1],
    }
    row_sha = stable_hash(identity)
    intent_id = "simintent_" + stable_hash(signal_id, arm_id)[:28]
    _insert_immutable(
        connection,
        table="sim_intent_map",
        key_column="intent_id",
        key=intent_id,
        hash_column="row_sha256",
        expected_hash=row_sha,
        columns=("intent_id", *identity.keys(), "row_sha256"),
        values=(intent_id, *identity.values(), row_sha),
    )
    return intent_id


def discover_paths(candle_root: Path, requested_pairs: Sequence[str] | None) -> list[Path]:
    paths = sorted(candle_root.glob("*_M1.csv"))
    if requested_pairs:
        wanted = {value.upper().replace("/", "_").replace("-", "_") for value in requested_pairs}
        paths = [path for path in paths if path.name.removesuffix("_M1.csv") in wanted]
        missing = sorted(wanted - {path.name.removesuffix("_M1.csv") for path in paths})
        if missing:
            raise ValueError(f"missing requested candle files: {missing}")
    if not paths:
        raise ValueError("no M1 candle inputs")
    return paths


def validate_effective_config(config: Mapping[str, Any]) -> None:
    for key, expected in POLICY.items():
        if config.get(key) != expected:
            raise ValueError(f"research-only policy mismatch: {key}")
    sampling = config.get("sampling") or {}
    if int(sampling.get("history_days") or 0) <= 0 or int(sampling.get("cadence_min") or 0) <= 0:
        raise ValueError("history_days and cadence_min must be positive")
    if float(sampling.get("inclusion_probability") or 0.0) != 1.0:
        raise ValueError("V1 supports inclusion_probability=1.0 only")
    if sampling.get("adaptive_sampling_forbidden") is not True:
        raise ValueError("adaptive sampling must remain forbidden")
    source = config.get("source") or {}
    if float(source.get("maximum_entry_spread_pips") or 0.0) <= 0.0:
        raise ValueError("positive entry spread cap required")
    if int(source.get("maximum_source_bytes_per_instrument") or 0) <= 0:
        raise ValueError("positive per-instrument source byte cap required")
    if int(source.get("maximum_total_source_bytes") or 0) <= 0:
        raise ValueError("positive total source byte cap required")
    if int(source.get("maximum_source_rows_per_instrument") or 0) <= 0:
        raise ValueError("positive per-instrument source row cap required")
    rules = list(config.get("signal_rules") or [])
    rule_ids = [str(row.get("id") or "") for row in rules]
    if not rule_ids or len(set(rule_ids)) != len(rule_ids) or any(not value for value in rule_ids):
        raise ValueError("unique nonempty signal rule IDs required")
    supported_rules = {"momentum", "reversion", "moving_average_spread", "level_reaction"}
    if any(str(row.get("kind") or "") not in supported_rules for row in rules):
        raise ValueError("unsupported signal rule kind")
    grid = config.get("execution_grid") or {}
    delays = [int(value) for value in grid.get("entry_delay_min") or []]
    horizons = [int(value) for value in grid.get("horizon_min") or []]
    gaps = [int(value) for value in grid.get("reentry_gap_min") or []]
    policies = list(grid.get("exit_policies") or [])
    if not delays or any(value < 0 for value in delays):
        raise ValueError("nonnegative entry delays required")
    if not horizons or any(value <= 0 for value in horizons):
        raise ValueError("positive horizons required")
    if not gaps or any(value < 0 for value in gaps):
        raise ValueError("nonnegative reentry gaps required")
    policy_ids = [str(value.get("id") or "") for value in policies]
    if (
        not policy_ids
        or any(not value for value in policy_ids)
        or len(set(policy_ids)) != len(policy_ids)
    ):
        raise ValueError("unique exit policies required")
    if any(str(value.get("kind") or "") != "endpoint" for value in policies):
        raise ValueError(
            "V1 independently verifies endpoint exits only; barrier policies require a new contract"
        )
    comparator = config.get("comparators") or {}
    matched_contract = "same delay, horizon, reentry gap, and exit policy as as_signaled"
    for comparator_id in ("flipped_direction", "deterministic_random_side"):
        definition = comparator.get(comparator_id) or {}
        if definition.get("enabled") is not True:
            raise ValueError(f"matched comparator must remain enabled: {comparator_id}")
        if definition.get("matching_contract") != matched_contract:
            raise ValueError(f"unmatched comparator contract: {comparator_id}")
    costs = config.get("costs") or {}
    slippage = [
        float(costs.get("round_trip_slippage_pips") or 0.0),
        float(costs.get("moderate_stress_slippage_pips") or 0.0),
        float(costs.get("severe_stress_slippage_pips") or 0.0),
    ]
    if slippage != sorted(slippage) or any(value < 0.0 for value in slippage):
        raise ValueError("slippage stresses must be nonnegative and monotone")
    multipliers = [float(value) for value in costs.get("spread_stress_multipliers") or []]
    if len(multipliers) < 3 or multipliers != sorted(multipliers) or multipliers[0] < 1.0:
        raise ValueError("three monotone spread stress multipliers >=1 required")
    partition = config.get("historical_partitions") or {}
    expected_embargo = max(
        int(row.get("lookback_min") or row.get("slow_min") or row.get("window_min") or 1)
        for row in rules
    ) + max(delays) + max(horizons)
    if int(partition.get("embargo_min") or -1) != expected_embargo:
        raise ValueError(
            f"embargo_min must equal active max feature+delay+horizon ({expected_embargo})"
        )
    if int((config.get("storage") or {}).get("maximum_virtual_order_intents") or 0) <= 0:
        raise ValueError("positive maximum_virtual_order_intents required")


def source_manifest(markets: Sequence[PairMarket], paths: Sequence[Path]) -> list[dict[str, Any]]:
    by_instrument = {market.instrument: market for market in markets}
    output = []
    for path in paths:
        instrument = path.name.removesuffix("_M1.csv")
        market = by_instrument[instrument]
        with path.open("rb") as source:
            prefix = source.read(int(market.source_prefix_bytes))
        if len(prefix) != int(market.source_prefix_bytes) or hashlib.sha256(prefix).hexdigest() != market.source_sha256:
            raise ValueError(f"source prefix changed before archive: {instrument}")
        compressed = gzip.compress(prefix, compresslevel=9, mtime=0)
        archive_path = safe_artifact_path(f"sources/{market.source_sha256}.csv.gz")
        if archive_path.exists():
            if archive_path.read_bytes() != compressed:
                raise ValueError(f"source archive identity conflict: {archive_path}")
        else:
            atomic_bytes(archive_path, compressed)
        try:
            recorded_path = path.resolve().relative_to(ROOT.resolve()).as_posix()
        except ValueError:
            recorded_path = str(path.resolve())
        output.append(
            {
                "instrument": instrument,
                "relative_path": recorded_path,
                "sha256": market.source_sha256,
                "pip": float(market.pip),
                "frozen_prefix_bytes": int(market.source_prefix_bytes),
                "archive_relative_path": archive_path.relative_to(ARTIFACT_ROOT.resolve()).as_posix(),
                "archive_gzip_sha256": hashlib.sha256(compressed).hexdigest(),
                "row_count": int(len(market.epochs)),
                "first_candle_utc": iso(int(market.epochs[0])),
                "last_candle_utc": iso(int(market.epochs[-1])),
            }
        )
    return output


def archive_contract_file(path: Path, label: str) -> dict[str, str]:
    payload = path.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    archive_path = safe_artifact_path(f"contracts/{digest}.{label}")
    if archive_path.exists():
        if file_sha256(archive_path) != digest:
            raise ValueError(f"contract archive identity conflict: {archive_path}")
    else:
        atomic_bytes(archive_path, payload)
    return {
        "label": label,
        "sha256": digest,
        "archive_relative_path": archive_path.relative_to(ARTIFACT_ROOT.resolve()).as_posix(),
    }


def add_cell(
    cells: dict[tuple[str, ...], CellAccumulator],
    *,
    scope: str,
    instrument: str,
    arm_id: str,
    partition: str,
    session: str,
    liquidity: str,
    result: Any,
    interval_start_epoch: int,
    interval_end_epoch: int,
    signed_factors: Sequence[str],
    source_instrument: str,
) -> None:
    key = (scope, instrument, arm_id, partition, session, liquidity)
    cell = cells.setdefault(key, CellAccumulator())
    cell.add(
        value=float(result.executable_net_pips),
        moderate=float(result.moderate_stress_net_pips),
        severe=float(result.severe_stress_net_pips),
        mfe=float(result.mfe_pips),
        mae=float(result.mae_pips),
        latency=float(result.missed_entry_slippage_pips),
        exit_reason=str(result.exit_reason),
        interval_start_epoch=interval_start_epoch,
        interval_end_epoch=interval_end_epoch,
        signed_factors=signed_factors,
        instrument=source_instrument,
    )


def insert_cells(
    connection: sqlite3.Connection,
    cohort_id: str,
    cells: Mapping[tuple[str, ...], CellAccumulator],
) -> list[str]:
    hashes: list[str] = []
    for key, accumulator in sorted(cells.items()):
        scope, instrument, arm_id, partition, session, liquidity = key
        aggregate = accumulator.payload()
        identity = {
            "cohort_id": cohort_id,
            "scope": scope,
            "instrument": instrument,
            "arm_id": arm_id,
            "partition_label": partition,
            "session_bucket": session,
            "liquidity_bucket": liquidity,
            "aggregate": aggregate,
        }
        aggregate_sha = stable_hash(identity)
        cell_id = "simcell_" + stable_hash(cohort_id, key)[:28]
        _insert_immutable(
            connection,
            table="sim_cell_aggregates",
            key_column="cell_id",
            key=cell_id,
            hash_column="aggregate_sha256",
            expected_hash=aggregate_sha,
            columns=(
                "cell_id", "cohort_id", "scope", "instrument", "arm_id",
                "partition_label", "session_bucket", "liquidity_bucket",
                "aggregate_sha256", "aggregate_json",
            ),
            values=(
                cell_id, cohort_id, scope, instrument, arm_id, partition,
                session, liquidity, aggregate_sha, canonical_json(aggregate),
            ),
        )
        hashes.append(aggregate_sha)
    connection.commit()
    return hashes


def keep_mistake_sample(
    samples: dict[str, list[tuple[float, dict[str, Any]]]],
    cluster: str,
    sample: dict[str, Any],
    maximum: int,
) -> None:
    severity = max(
        abs(float(sample.get("executable_net_pips") or 0.0)),
        abs(float(sample.get("missed_entry_slippage_pips") or 0.0)),
    )
    bucket = samples.setdefault(cluster, [])
    bucket.append((severity, sample))
    bucket.sort(key=lambda value: (-value[0], stable_hash(value[1])))
    del bucket[max(1, int(maximum)) :]


def insert_samples(
    connection: sqlite3.Connection,
    cohort_id: str,
    samples: Mapping[str, Sequence[tuple[float, Mapping[str, Any]]]],
) -> int:
    count = 0
    for cluster, rows in sorted(samples.items()):
        for severity, sample in rows:
            sample_sha = stable_hash(sample)
            sample_id = "simsample_" + stable_hash(cohort_id, cluster, sample_sha)[:28]
            count += int(
                _insert_immutable(
                    connection,
                    table="sim_mistake_samples",
                    key_column="sample_id",
                    key=sample_id,
                    hash_column="sample_sha256",
                    expected_hash=sample_sha,
                    columns=(
                        "sample_id", "cohort_id", "cluster", "severity",
                        "decision_utc", "instrument", "arm_id", "sample_sha256",
                        "sample_json",
                    ),
                    values=(
                        sample_id, cohort_id, cluster, float(severity),
                        str(sample["decision_utc"]), str(sample["instrument"]),
                        str(sample["arm_id"]), sample_sha, canonical_json(sample),
                    ),
                )
            )
    connection.commit()
    return count


def cohort_table_root(
    connection: sqlite3.Connection, table: str, hash_column: str, cohort_id: str
) -> dict[str, Any]:
    if table not in SIM_TABLES:
        raise ValueError(f"unapproved SIM table: {table}")
    hashes = sorted(
        str(row[0])
        for row in connection.execute(
            f"SELECT {hash_column} FROM {table} WHERE cohort_id=?", (cohort_id,)
        )
    )
    return {"count": len(hashes), "set_sha256": stable_hash(hashes)}


def _top_diagnostic_cells(
    connection: sqlite3.Connection,
    cohort_id: str,
    *,
    minimum_effective_n: int,
    limit: int = 20,
) -> list[dict[str, Any]]:
    rows = []
    query = """
      SELECT a.arm_id,a.definition_json,c.partition_label,c.aggregate_json
      FROM sim_cell_aggregates c JOIN sim_arms a
        ON a.cohort_id=c.cohort_id AND a.arm_id=c.arm_id
      WHERE c.cohort_id=? AND c.scope='all_pairs' AND c.instrument='ALL'
        AND c.session_bucket='all' AND c.liquidity_bucket='all'
    """
    for arm_id, definition_json, partition, aggregate_json in connection.execute(query, (cohort_id,)):
        definition = json.loads(definition_json)
        aggregate = json.loads(aggregate_json)
        rows.append({"arm_id": arm_id, "partition": partition, **definition, **aggregate})
    early_label = "diagnostic_early"
    ranked = [
        row
        for row in rows
        if row["partition"] == early_label
        and row["comparator"] == "as_signaled"
        and int(row.get("effective_n") or 0) >= int(minimum_effective_n)
    ]
    ranked.sort(
        key=lambda row: (
            float(row.get("unadjusted_normal_lower_95_mean_pips") or -math.inf),
            float(row.get("effective_average_net_pips") or -math.inf),
            int(row.get("effective_n") or 0),
        ),
        reverse=True,
    )
    by_key = {(row["arm_id"], row["partition"]): row for row in rows}
    output = []
    for rank, row in enumerate(ranked[:limit], start=1):
        output.append(
            {
                "discovery_rank": rank,
                "early": row,
                "middle": by_key.get((row["arm_id"], "diagnostic_middle")),
                "late": by_key.get((row["arm_id"], "diagnostic_late")),
                "promotion_eligible": False,
                "next_step": "new strictly later prospective cohort required",
            }
        )
    return output


def render_report(state: Mapping[str, Any]) -> str:
    lines = [
        "# Counterfactual SIM Gym V1",
        "",
        "Historical discovery diagnostics only. No broker, authorization, lifecycle, or promotion path exists.",
        "",
        f"- Cohort: `{state['cohort_id']}`",
        f"- Instruments: **{state['instrument_count']}**",
        f"- Arm definitions: **{state['arm_count']:,}**",
        f"- Causal decisions evaluated: **{state['decision_count']:,}**",
        f"- Virtual order intents: **{state['virtual_order_intent_count']:,}**",
        f"- Eligible filled virtual intents: **{state['filled_outcome_count']:,}**",
        f"- Unique reusable outcome paths: **{state['unique_outcome_part_count']:,}**",
        f"- Excluded/missing outcomes: **{state['excluded_outcome_count']:,}**",
        f"- Compact aggregate cells: **{state['aggregate_cell_count']:,}**",
        f"- Stored mistake examples: **{state['mistake_sample_count']:,}**",
        f"- Replay window: **{state['window']['start_utc']} to {state['window']['end_utc']}**",
        "- Supported operational decision: **no_trade**",
        "",
        "Raw virtual-order count is repetition volume, not independent evidence. Effective N uses earliest-finish non-overlap after same-clock signed-factor and same-pair collapse.",
        "",
        "## Discovery-ranked arms",
        "",
        "Only as-signaled arms are ranked on the early diagnostic partition. Flipped and deterministic-random arms remain matched controls. Middle and late blocks are stability diagnostics, not confirmation.",
        "",
        "| Rank | Signal | Comparator | Delay | Exit | Horizon | Gap | Early eff N | Early avg | Early unadjusted LCB | Middle avg | Late avg |",
        "|---:|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in state.get("top_diagnostic_arms") or []:
        early = item["early"]
        middle = item.get("middle") or {}
        late = item.get("late") or {}
        policy = early.get("exit_policy") or {}
        def fmt(value: Any) -> str:
            return "n/a" if value is None else f"{float(value):+.3f}"
        lines.append(
            f"| {item['discovery_rank']} | `{early['signal_rule_id']}` | `{early['comparator']}` | "
            f"{early['entry_delay_min']}m | `{policy.get('id')}` | {early['horizon_min']}m | "
            f"{early['reentry_gap_min']}m | {int(early.get('effective_n') or 0)} | "
            f"{fmt(early.get('effective_average_net_pips'))} | {fmt(early.get('unadjusted_normal_lower_95_mean_pips'))} | "
            f"{fmt(middle.get('effective_average_net_pips'))} | {fmt(late.get('effective_average_net_pips'))} |"
        )
    if not state.get("top_diagnostic_arms"):
        lines.append("| - | no cell reached the minimum diagnostic effective N | - | - | - | - | - | - | - | - | - | - |")
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- Historical replay cannot confirm or promote an arm.",
            "- M1 same-bar stop/target ambiguity is resolved adverse-first.",
            "- Entry uses the exact next permissible M1 bid/ask open; gaps are not backfilled.",
            "- Spread is embedded once through executable sides; only explicit slippage is subtracted.",
            "- The all-pairs effective count collapses shared signed currency factors, including correlated JPY propagation.",
            "- A selected arm must open a newly frozen, strictly later prospective cohort.",
            "",
        ]
    )
    return "\n".join(lines)


def run(
    *,
    config_path: Path = DEFAULT_CONFIG,
    requested_pairs: Sequence[str] | None = None,
    history_days: int | None = None,
    cadence_min: int | None = None,
) -> dict[str, Any]:
    config = read_json(config_path)
    if any(config.get(key) != expected for key, expected in POLICY.items() if key in config):
        raise ValueError("configuration contradicts research-only policy")
    effective = json.loads(canonical_json(config))
    if history_days is not None:
        effective["sampling"]["history_days"] = int(history_days)
    if cadence_min is not None:
        effective["sampling"]["cadence_min"] = int(cadence_min)
    validate_effective_config(effective)
    candle_root = ROOT / str(effective["source"]["relative_root"])
    selected_pairs = requested_pairs or tuple(effective["source"].get("initial_instruments") or ())
    paths = discover_paths(candle_root, selected_pairs)
    maximum_source_bytes = int(effective["source"]["maximum_source_bytes_per_instrument"])
    oversized = [path for path in paths if int(path.stat().st_size) > maximum_source_bytes]
    if oversized:
        raise ValueError(f"per-instrument source byte cap exceeded: {oversized}")
    total_source_bytes = sum(int(path.stat().st_size) for path in paths)
    if total_source_bytes > int(effective["source"]["maximum_total_source_bytes"]):
        raise ValueError(
            "total source byte cap exceeded: "
            f"{total_source_bytes}>{effective['source']['maximum_total_source_bytes']}"
        )
    markets = [load_pair_market(path) for path in paths]
    if any(market.source_prefix_bytes > maximum_source_bytes for market in markets):
        raise ValueError("captured per-instrument source byte cap exceeded")
    captured_total_source_bytes = sum(market.source_prefix_bytes for market in markets)
    if captured_total_source_bytes > int(effective["source"]["maximum_total_source_bytes"]):
        raise ValueError("captured total source byte cap exceeded")
    maximum_source_rows = int(effective["source"]["maximum_source_rows_per_instrument"])
    if any(len(market.epochs) > maximum_source_rows for market in markets):
        raise ValueError("per-instrument source row cap exceeded")
    common_first = max(int(market.epochs[0]) for market in markets)
    common_last = min(int(market.epochs[-1]) + 60 for market in markets)
    history_start = max(common_first, common_last - int(effective["sampling"]["history_days"]) * 86400)
    if common_last <= history_start:
        raise ValueError("empty common replay window")
    manifest = source_manifest(markets, paths)
    runner_path = Path(__file__).resolve()
    core_path = ROOT / "src" / "forex_system" / "research" / "counterfactual_sim_gym_v1.py"
    config_sha = file_sha256(config_path)
    runner_sha = file_sha256(runner_path)
    core_sha = file_sha256(core_path)
    contract_artifacts = {
        "runner": archive_contract_file(runner_path, "runner.py"),
        "core": archive_contract_file(core_path, "core.py"),
        "config": archive_contract_file(config_path, "config.json"),
    }
    arms = build_arm_definitions(effective)
    minimum_warmup = max(
        int(
            rule.get("lookback_min")
            or rule.get("slow_min")
            or rule.get("window_min")
            or 1
        )
        for rule in effective.get("signal_rules") or []
    )
    potential_clock_count = sum(
        1
        for market in markets
        for index in range(minimum_warmup, len(market.epochs))
        if history_start <= int(market.epochs[index]) + 60 < common_last
        and (int(market.epochs[index]) + 60) % (
            int(effective["sampling"]["cadence_min"]) * 60
        ) == 0
    )
    maximum_intents = int(effective["storage"]["maximum_virtual_order_intents"])
    maximum_intent_estimate = potential_clock_count * len(arms)
    if maximum_intent_estimate > maximum_intents:
        raise ValueError(
            "preflight maximum_virtual_order_intents exceeded: "
            f"{maximum_intent_estimate}>{maximum_intents}"
        )
    material_contract = {
        "contract_id": CONTRACT_ID,
        "policy": POLICY,
        "effective_config": effective,
        "source_manifest": manifest,
        "window": {"start_epoch": history_start, "end_epoch": common_last},
        "arm_definition_sha256": stable_hash([arm_as_dict(arm) for arm in arms]),
        "preflight": {
            "potential_pair_clock_count": potential_clock_count,
            "maximum_intent_estimate": maximum_intent_estimate,
            "maximum_virtual_order_intents": maximum_intents,
        },
        "runner_sha256": runner_sha,
        "core_sha256": core_sha,
        "config_sha256": config_sha,
        "contract_artifacts": contract_artifacts,
        "config_path": (
            config_path.resolve().relative_to(ROOT.resolve()).as_posix()
            if config_path.resolve().is_relative_to(ROOT.resolve())
            else str(config_path.resolve())
        ),
    }
    material_sha = stable_hash(material_contract)
    cohort_id = "counterfactual_sim_gym_v1." + material_sha[:20]
    storage = effective.get("storage") or {}
    database = safe_artifact_path(str(storage["database_relative_path"]))
    state = safe_artifact_path(str(storage["state_relative_path"]))
    report = safe_artifact_path(str(storage["report_relative_path"]))
    connection = connect(database)
    generated = utc_now()
    register_cohort(
        connection,
        cohort_id=cohort_id,
        experiment_key=str(effective["experiment_key"]),
        contract=material_contract,
        config_sha=config_sha,
        runner_sha=runner_sha,
        core_sha=core_sha,
        observed_utc=generated,
    )
    register_arms(connection, cohort_id, arms)

    rules = {str(row["id"]): dict(row) for row in effective.get("signal_rules") or []}
    arms_by_rule: dict[str, list[ArmDefinition]] = defaultdict(list)
    for arm in arms:
        arms_by_rule[arm.signal_rule_id].append(arm)
    costs = effective.get("costs") or {}
    episode_min = int(
        (effective.get("independence") or {}).get("diagnostic_market_episode_min") or 60
    )
    cadence = max(1, int(effective["sampling"]["cadence_min"])) * 60
    maximum_spread = float(effective["source"]["maximum_entry_spread_pips"])
    mistake_config = effective.get("mistake_clustering") or {}
    sample_limit = int(mistake_config.get("maximum_examples_per_cluster") or 20)
    samples: dict[str, list[tuple[float, dict[str, Any]]]] = {}
    aggregate_cells: dict[tuple[str, ...], CellAccumulator] = {}
    aggregate_hashes: list[str] = []
    counters: Counter[str] = Counter()
    last_reentry: dict[tuple[str, str, int], int] = {}
    outcome_cache: dict[tuple[Any, ...], tuple[str, Any]] = {}

    for pair_number, market in enumerate(markets, start=1):
        pair_cells: dict[tuple[str, ...], CellAccumulator] = {}
        for index in range(minimum_warmup, len(market.epochs)):
            decision_epoch = int(market.epochs[index]) + 60
            if decision_epoch < history_start or decision_epoch >= common_last or decision_epoch % cadence:
                continue
            counters["decision_count"] += 1
            clock_id = record_clock(
                connection,
                cohort_id=cohort_id,
                market=market,
                candle_index=index,
                decision_epoch=decision_epoch,
            )
            observations = []
            for rule in rules.values():
                observation = evaluate_signal(
                    market,
                    index,
                    rule,
                    round_trip_slippage_pips=float(costs["round_trip_slippage_pips"]),
                )
                if observation is not None:
                    observations.append(observation)
            for observation in observations:
                signal_id = record_signal(
                    connection,
                    cohort_id=cohort_id,
                    clock_id=clock_id,
                    observation=observation,
                )
                decision_id = signal_id
                for arm in arms_by_rule.get(observation.rule_id, ()): 
                    counters["virtual_order_intent_count"] += 1
                    maximum_intents = int(
                        (effective.get("storage") or {}).get("maximum_virtual_order_intents")
                        or 0
                    )
                    if maximum_intents and counters["virtual_order_intent_count"] > maximum_intents:
                        raise ValueError("maximum_virtual_order_intents exceeded")
                    if arm.comparator == "flipped_direction":
                        side = -observation.side
                    elif arm.comparator == "deterministic_random_side":
                        side = deterministic_random_side(decision_id)
                    else:
                        side = observation.side
                    factors = signed_currency_factors(market.instrument, side)
                    influence_end_epoch = decision_epoch + (
                        arm.entry_delay_min + arm.horizon_min
                    ) * 60
                    partition = historical_partition_interval(
                        decision_epoch,
                        influence_end_epoch,
                        history_start,
                        common_last,
                        effective,
                    )
                    reentry_key = (market.instrument, arm.arm_id, side, partition or "purged")
                    previous = last_reentry.get(reentry_key)
                    if previous is not None and decision_epoch - previous < arm.reentry_gap_min * 60:
                        counters["reentry_filtered_count"] += 1
                        record_intent(
                            connection,
                            cohort_id=cohort_id,
                            signal_id=signal_id,
                            arm_id=arm.arm_id,
                            outcome_id=None,
                            side=side,
                            partition=partition,
                            eligible=False,
                            exclusion_reason="reentry_filtered",
                            factors=factors,
                        )
                        continue
                    outcome_key = (
                        clock_id,
                        int(side),
                        int(arm.entry_delay_min),
                        int(arm.horizon_min),
                        canonical_json(arm.exit_policy),
                    )
                    cached = outcome_cache.get(outcome_key)
                    if cached is None:
                        result = simulate_order(
                            market,
                            decision_epoch=decision_epoch,
                            side=side,
                            entry_delay_min=arm.entry_delay_min,
                            horizon_min=arm.horizon_min,
                            exit_policy=arm.exit_policy,
                            round_trip_slippage_pips=float(costs["round_trip_slippage_pips"]),
                            moderate_stress_slippage_pips=float(costs["moderate_stress_slippage_pips"]),
                            severe_stress_slippage_pips=float(costs["severe_stress_slippage_pips"]),
                            maximum_entry_spread_pips=maximum_spread,
                            moderate_spread_multiplier=float(costs["spread_stress_multipliers"][1]),
                            severe_spread_multiplier=float(costs["spread_stress_multipliers"][2]),
                            latency_decay_threshold_pips=float(mistake_config["latency_decay_threshold_pips"]),
                            extension_threshold_pips=float(mistake_config["extension_threshold_pips"]),
                        )
                        outcome_id = record_outcome_part(
                            connection,
                            cohort_id=cohort_id,
                            clock_id=clock_id,
                            side=side,
                            arm=arm,
                            result=result,
                        )
                        outcome_cache[outcome_key] = (outcome_id, result)
                        counters["unique_outcome_part_count"] += 1
                    else:
                        outcome_id, result = cached
                    eligible = result.status == "filled" and partition is not None
                    exclusion_reason = str(result.exclusion_reason)
                    if partition is None:
                        counters["partition_boundary_purged_count"] += 1
                        exclusion_reason = "partition_boundary_or_embargo"
                    if result.status != "filled":
                        counters["excluded_outcome_count"] += 1
                        counters[result.exclusion_reason or "invalid_outcome"] += 1
                    record_intent(
                        connection,
                        cohort_id=cohort_id,
                        signal_id=signal_id,
                        arm_id=arm.arm_id,
                        outcome_id=outcome_id,
                        side=side,
                        partition=partition,
                        eligible=eligible,
                        exclusion_reason=exclusion_reason,
                        factors=factors,
                    )
                    if not eligible:
                        continue
                    last_reentry[reentry_key] = decision_epoch
                    counters["filled_outcome_count"] += 1
                    session = session_bucket(decision_epoch)
                    liquidity = liquidity_bucket(float(result.entry_spread_pips))
                    episode_id = market_episode_id(decision_epoch, episode_min)
                    for target, scope, instrument, session_value, liquidity_value in (
                        (pair_cells, "pair", market.instrument, session, liquidity),
                        (pair_cells, "pair", market.instrument, "all", "all"),
                        (aggregate_cells, "all_pairs", "ALL", session, liquidity),
                        (aggregate_cells, "all_pairs", "ALL", "all", "all"),
                    ):
                        add_cell(
                            target,
                            scope=scope,
                            instrument=instrument,
                            arm_id=arm.arm_id,
                            partition=partition,
                            session=session_value,
                            liquidity=liquidity_value,
                            result=result,
                            interval_start_epoch=decision_epoch,
                            interval_end_epoch=influence_end_epoch,
                            signed_factors=factors,
                            source_instrument=market.instrument,
                        )
                    sample = {
                        "decision_id": decision_id,
                        "decision_utc": iso(decision_epoch),
                        "knowledge_cutoff_utc": iso(observation.knowledge_cutoff_epoch),
                        "instrument": market.instrument,
                        "arm_id": arm.arm_id,
                        "signal_rule_id": observation.rule_id,
                        "comparator": arm.comparator,
                        "side": "long" if side > 0 else "short",
                        "entry_delay_min": arm.entry_delay_min,
                        "horizon_min": arm.horizon_min,
                        "exit_policy_id": str(arm.exit_policy.get("id")),
                        "reentry_gap_min": arm.reentry_gap_min,
                        "signal_score": observation.score,
                        "feature_value_pips": observation.feature_value_pips,
                        "entry_spread_pips": result.entry_spread_pips,
                        "gross_mid_pips": result.gross_mid_pips,
                        "executable_net_pips": result.executable_net_pips,
                        "terminal_endpoint_net_pips": result.terminal_endpoint_net_pips,
                        "mfe_pips": result.mfe_pips,
                        "mae_pips": result.mae_pips,
                        "missed_entry_slippage_pips": result.missed_entry_slippage_pips,
                        "exit_reason": result.exit_reason,
                        "market_episode_id": episode_id,
                        "signed_currency_factors": list(factors),
                        "source_sha256": market.source_sha256,
                    }
                    for cluster in result.mistake_clusters:
                        counters[f"cluster:{cluster}"] += 1
                        if cluster != "captured_after_cost":
                            keep_mistake_sample(samples, cluster, sample, sample_limit)
        aggregate_hashes.extend(insert_cells(connection, cohort_id, pair_cells))
        print(
            f"[sim-gym] {pair_number:02d}/{len(markets)} {market.instrument}: "
            f"{counters['filled_outcome_count']:,} cumulative outcomes",
            flush=True,
        )

    aggregate_hashes.extend(insert_cells(connection, cohort_id, aggregate_cells))
    inserted_samples = insert_samples(connection, cohort_id, samples)
    top = _top_diagnostic_cells(
        connection,
        cohort_id,
        minimum_effective_n=int(
            (effective.get("statistical_diagnostics") or {}).get(
                "minimum_effective_n_for_ordering"
            )
            or 10
        ),
    )
    statistics = {
        "decision_count": int(counters["decision_count"]),
        "virtual_order_intent_count": int(counters["virtual_order_intent_count"]),
        "filled_outcome_count": int(counters["filled_outcome_count"]),
        "unique_outcome_part_count": int(counters["unique_outcome_part_count"]),
        "excluded_outcome_count": int(counters["excluded_outcome_count"]),
        "reentry_filtered_count": int(counters["reentry_filtered_count"]),
        "partition_boundary_purged_count": int(counters["partition_boundary_purged_count"]),
        "mistake_cluster_counts": {
            key.removeprefix("cluster:"): int(value)
            for key, value in sorted(counters.items())
            if key.startswith("cluster:")
        },
        "exclusion_counts": {
            key: int(value)
            for key, value in sorted(counters.items())
            if key not in {
                "decision_count", "virtual_order_intent_count", "filled_outcome_count",
                "unique_outcome_part_count",
                "excluded_outcome_count", "reentry_filtered_count",
                "partition_boundary_purged_count",
            } and not key.startswith("cluster:")
        },
    }
    statistics["normalized_ledger_roots"] = {
        "arms": cohort_table_root(connection, "sim_arms", "definition_sha256", cohort_id),
        "decision_clocks": cohort_table_root(
            connection, "sim_decision_clocks", "row_sha256", cohort_id
        ),
        "signal_observations": cohort_table_root(
            connection, "sim_signal_observations", "row_sha256", cohort_id
        ),
        "outcome_parts": cohort_table_root(
            connection, "sim_outcome_parts", "row_sha256", cohort_id
        ),
        "intent_map": cohort_table_root(
            connection, "sim_intent_map", "row_sha256", cohort_id
        ),
        "cell_aggregates": {
            "count": len(aggregate_hashes),
            "set_sha256": stable_hash(sorted(aggregate_hashes)),
        },
    }
    aggregate_set_sha = stable_hash(sorted(aggregate_hashes))
    statistics_sha = stable_hash(statistics)
    snapshot_id = "simsnapshot_" + stable_hash(cohort_id, aggregate_set_sha, statistics_sha)[:28]
    existing_snapshot = connection.execute(
        "SELECT aggregate_set_sha256,statistics_sha256 FROM sim_run_snapshots WHERE snapshot_id=?",
        (snapshot_id,),
    ).fetchone()
    if existing_snapshot is None:
        connection.execute(
            "INSERT INTO sim_run_snapshots VALUES (?,?,?,?,?,?,?)",
            (
                snapshot_id, cohort_id, generated, len(aggregate_hashes),
                aggregate_set_sha, statistics_sha, canonical_json(statistics),
            ),
        )
    elif tuple(map(str, existing_snapshot)) != (aggregate_set_sha, statistics_sha):
        raise ValueError("snapshot identity conflict")
    connection.commit()
    state_payload = {
        "schema_version": 1,
        "generated_utc": generated,
        "cohort_id": cohort_id,
        "material_contract_sha256": material_sha,
        **POLICY,
        "database": str(database),
        "config": str(config_path),
        "instrument_count": len(markets),
        "instruments": [market.instrument for market in markets],
        "arm_count": len(arms),
        "aggregate_cell_count": len(aggregate_hashes),
        "mistake_sample_count": sum(len(value) for value in samples.values()),
        "mistake_sample_insert_count": inserted_samples,
        "window": {"start_utc": iso(history_start), "end_utc": iso(common_last)},
        "source_manifest": manifest,
        "snapshot_id": snapshot_id,
        "aggregate_set_sha256": aggregate_set_sha,
        "statistics_sha256": statistics_sha,
        **statistics,
        "top_diagnostic_arms": top,
        "limitations": [
            "historical discovery only; no historical result can confirm or promote",
            "zero-minute entry is an optimistic decision-boundary upper bound; one-minute delay is the executable-latency baseline",
            "V1 permits endpoint exits only; barrier exits require a later independently verified contract",
            "the initial V1 candidate adapter covers a bounded price-rule and level-reaction set",
            "portfolio rotation and source-conditioned candidate adapters remain later stages",
            "raw virtual order count is not independent evidence",
            "ordinary bounds are unadjusted and paired comparator inference remains pending",
        ],
    }
    atomic_json(state, state_payload)
    atomic_text(report, render_report(state_payload))
    connection.close()
    return state_payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--pairs", default="", help="Comma-separated deterministic subset")
    parser.add_argument("--history-days", type=int)
    parser.add_argument("--cadence-min", type=int)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pairs = [value.strip() for value in args.pairs.split(",") if value.strip()]
    result = run(
        config_path=args.config,
        requested_pairs=pairs or None,
        history_days=args.history_days,
        cadence_min=args.cadence_min,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
