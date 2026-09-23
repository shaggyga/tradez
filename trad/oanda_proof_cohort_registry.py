#!/usr/bin/env python3
"""Immutable cohort registry for prospective FX proof experiments.

The registry separates a material experiment contract from changing market
observations.  A source/model/feature/cost/promotion change creates a new cohort;
ordinary new causal data and process restarts do not.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def initialize_registry(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS proof_cohorts (
            cohort_id TEXT PRIMARY KEY,
            family TEXT NOT NULL,
            cohort_start_utc TEXT NOT NULL,
            material_contract_sha256 TEXT NOT NULL,
            source_sha256 TEXT NOT NULL,
            feature_schema_version TEXT NOT NULL,
            forecast_contract_version TEXT NOT NULL,
            cost_model_version TEXT NOT NULL,
            training_cutoff_utc TEXT NOT NULL,
            training_dataset_sha256 TEXT NOT NULL,
            pre_governance_forecast_count INTEGER NOT NULL,
            excludes_untagged_forecasts INTEGER NOT NULL,
            contract_json TEXT NOT NULL,
            UNIQUE(family, material_contract_sha256)
        );
        CREATE TABLE IF NOT EXISTS proof_cohort_transitions (
            transition_id TEXT PRIMARY KEY,
            family TEXT NOT NULL,
            previous_cohort_id TEXT,
            next_cohort_id TEXT NOT NULL,
            observed_utc TEXT NOT NULL,
            reason TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS proof_cohorts_no_update
        BEFORE UPDATE ON proof_cohorts
        BEGIN SELECT RAISE(ABORT, 'proof cohorts are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS proof_cohorts_no_delete
        BEFORE DELETE ON proof_cohorts
        BEGIN SELECT RAISE(ABORT, 'proof cohorts are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS proof_transitions_no_update
        BEFORE UPDATE ON proof_cohort_transitions
        BEGIN SELECT RAISE(ABORT, 'proof cohort transitions are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS proof_transitions_no_delete
        BEFORE DELETE ON proof_cohort_transitions
        BEGIN SELECT RAISE(ABORT, 'proof cohort transitions are immutable'); END;
        """
    )
    connection.commit()
    return connection


def material_contract(specification: dict[str, Any]) -> dict[str, Any]:
    """Return fields whose change must roll the cohort.

    Initial training provenance is recorded on creation but deliberately omitted
    here because the frozen contract explicitly permits causal rolling fitting.
    Per-forecast training cutoffs and dataset hashes remain immutable forecasts.
    """

    contract = {
        key: specification[key]
        for key in (
            "family",
            "source_sha256",
            "model_specification",
            "hyperparameters",
            "feature_schema_version",
            "training_policy",
            "forecast_contract_version",
            "cost_model_version",
            "dimensions",
            "deduplication_rules",
            "promotion_thresholds",
        )
    }
    # A superseded material contract is never silently reactivated.  When a
    # deliberately repeated hypothesis is scientifically justified, callers
    # must give it a new, explicit generation identifier so that it becomes a
    # genuinely new immutable cohort rather than erasing the intervening
    # negative or superseded evidence.
    if specification.get("cohort_generation_id"):
        contract["cohort_generation_id"] = str(specification["cohort_generation_id"])
    return contract


def verify_transition_replay(connection: sqlite3.Connection) -> dict[str, Any]:
    """Replay the append-only cohort genealogy and fail on gaps or reactivation.

    Row order, rather than user-supplied timestamps, is authoritative.  This
    makes the check robust to equal timestamps and clock corrections while
    proving that every cohort was reached exactly once from its predecessor.
    """

    cohort_rows = connection.execute(
        "SELECT rowid,cohort_id,family FROM proof_cohorts ORDER BY rowid"
    ).fetchall()
    transition_rows = connection.execute(
        """
        SELECT rowid,transition_id,family,previous_cohort_id,next_cohort_id
        FROM proof_cohort_transitions ORDER BY rowid
        """
    ).fetchall()
    cohorts = {str(row[1]): {"rowid": int(row[0]), "family": str(row[2])} for row in cohort_rows}
    incoming: dict[str, int] = {}
    active_by_family: dict[str, str] = {}
    family_transition_counts: dict[str, int] = {}
    errors: list[str] = []

    for row in transition_rows:
        transition_id = str(row[1])
        family = str(row[2])
        previous = None if row[3] is None else str(row[3])
        next_cohort = str(row[4])
        family_transition_counts[family] = family_transition_counts.get(family, 0) + 1

        next_record = cohorts.get(next_cohort)
        if next_record is None:
            errors.append(f"{transition_id}: next cohort does not exist")
            continue
        if next_record["family"] != family:
            errors.append(f"{transition_id}: next cohort family mismatch")
        incoming[next_cohort] = incoming.get(next_cohort, 0) + 1

        expected_previous = active_by_family.get(family)
        if previous != expected_previous:
            errors.append(
                f"{transition_id}: previous cohort {previous!r} does not match replay "
                f"head {expected_previous!r}"
            )
        if previous is not None:
            previous_record = cohorts.get(previous)
            if previous_record is None:
                errors.append(f"{transition_id}: previous cohort does not exist")
            elif previous_record["family"] != family:
                errors.append(f"{transition_id}: previous cohort family mismatch")
        if next_cohort == previous or next_cohort in active_by_family.values():
            errors.append(f"{transition_id}: cohort cycle or reactivation detected")
        active_by_family[family] = next_cohort

    for cohort_id, record in cohorts.items():
        count = incoming.get(cohort_id, 0)
        if count != 1:
            errors.append(f"{cohort_id}: expected one incoming transition, observed {count}")
        if record["family"] not in active_by_family:
            errors.append(f"{cohort_id}: family has no replay head")

    if errors:
        raise RuntimeError("proof cohort transition replay failed: " + "; ".join(errors[:12]))
    return {
        "status": "verified",
        "cohort_count": len(cohort_rows),
        "transition_count": len(transition_rows),
        "family_count": len(active_by_family),
        "active_by_family": dict(sorted(active_by_family.items())),
        "ordering": "sqlite_rowid_append_order",
    }


def ensure_cohorts(
    database_path: Path,
    registry_json: Path,
    specifications: list[dict[str, Any]],
) -> dict[str, str]:
    connection = initialize_registry(database_path)
    active: dict[str, str] = {}
    try:
        connection.execute("BEGIN IMMEDIATE")
        for specification in sorted(specifications, key=lambda row: str(row["family"])):
            family = str(specification["family"])
            material = material_contract(specification)
            material_sha = stable_hash(material)
            existing = connection.execute(
                "SELECT rowid,cohort_id FROM proof_cohorts WHERE family=? AND material_contract_sha256=?",
                (family, material_sha),
            ).fetchone()
            if existing:
                latest = connection.execute(
                    "SELECT rowid,cohort_id FROM proof_cohorts WHERE family=? ORDER BY rowid DESC LIMIT 1",
                    (family,),
                ).fetchone()
                if latest is None or int(existing[0]) != int(latest[0]):
                    raise RuntimeError(
                        f"refusing to reactivate superseded proof cohort {existing[1]!s} for "
                        f"{family}; create a materially new hypothesis with an explicit "
                        "cohort_generation_id"
                    )
                active[family] = str(existing[1])
                continue
            previous = connection.execute(
                "SELECT cohort_id FROM proof_cohorts WHERE family=? ORDER BY rowid DESC LIMIT 1",
                (family,),
            ).fetchone()
            start = str(specification.get("cohort_start_utc") or utc_now())
            cohort_id = f"{family}.{start[:10].replace('-', '')}.{material_sha[:16]}"
            frozen = {
                **material,
                "cohort_id": cohort_id,
                "cohort_start_utc": start,
                "initial_training_cutoff_utc": str(
                    specification.get("initial_training_cutoff_utc") or "unknown"
                ),
                "initial_training_dataset_sha256": str(
                    specification.get("initial_training_dataset_sha256") or "unknown"
                ),
                "pre_governance_forecast_count": int(
                    specification.get("pre_governance_forecast_count") or 0
                ),
                "excludes_untagged_forecasts": True,
                "material_contract_sha256": material_sha,
            }
            connection.execute(
                """
                INSERT INTO proof_cohorts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    cohort_id,
                    family,
                    start,
                    material_sha,
                    str(specification["source_sha256"]),
                    str(specification["feature_schema_version"]),
                    str(specification["forecast_contract_version"]),
                    str(specification["cost_model_version"]),
                    str(frozen["initial_training_cutoff_utc"]),
                    str(frozen["initial_training_dataset_sha256"]),
                    int(frozen["pre_governance_forecast_count"]),
                    1,
                    canonical_json(frozen),
                ),
            )
            transition = {
                "family": family,
                "previous": None if previous is None else str(previous[0]),
                "next": cohort_id,
                "observed_utc": start,
                "reason": "initial_governed_cohort" if previous is None else "material_contract_changed",
            }
            connection.execute(
                "INSERT INTO proof_cohort_transitions VALUES (?,?,?,?,?,?)",
                (
                    "transition_" + stable_hash(transition)[:24],
                    family,
                    transition["previous"],
                    cohort_id,
                    start,
                    transition["reason"],
                ),
            )
            active[family] = cohort_id
        lineage_verification = verify_transition_replay(connection)
        connection.commit()
        rows = connection.execute(
            "SELECT cohort_id,family,cohort_start_utc,material_contract_sha256,contract_json FROM proof_cohorts ORDER BY cohort_start_utc,family"
        ).fetchall()
        transitions = connection.execute(
            "SELECT transition_id,family,previous_cohort_id,next_cohort_id,observed_utc,reason FROM proof_cohort_transitions ORDER BY observed_utc,family"
        ).fetchall()
    finally:
        connection.close()
    atomic_json(
        registry_json,
        {
            "schema_version": 1,
            "generated_utc": utc_now(),
            "immutable_database": str(database_path.resolve()),
            "active_cohorts": active,
            "lineage_verification": lineage_verification,
            "cohorts": [json.loads(str(row[4])) for row in rows],
            "transitions": [
                {
                    "transition_id": row[0],
                    "family": row[1],
                    "previous_cohort_id": row[2],
                    "next_cohort_id": row[3],
                    "observed_utc": row[4],
                    "reason": row[5],
                }
                for row in transitions
            ],
            "policy": "untagged forecasts are pre-governance and never merge into governed cohorts",
            "can_place_orders": False,
            "can_promote": False,
        },
    )
    return active
