#!/usr/bin/env python3
"""One-time, explicit repair opener for the four frozen proof cohorts.

The command never builds or stores a forecast.  It only opens the explicit
``lineage_repair_20260829_v1`` cohort generation in the immutable proof registry
and publishes a machine-readable incident/quarantine audit.  The default mode
is a read-only plan; mutation requires both ``--apply`` and the exact generation
identifier as a confirmation token.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from oanda_proof_cohort_registry import (
        ensure_cohorts,
        material_contract,
        stable_hash,
        verify_transition_replay,
    )
    from oanda_proof_shadow_predictors import (
        DEFAULT_COHORT_DATABASE,
        DEFAULT_COHORT_STATE,
        DEFAULT_DATABASE,
        DEFAULT_GOVERNANCE_CONFIG,
        DEFAULT_SNAPSHOT,
        FAMILIES,
        FEATURE_WINDOWS,
        HORIZON_STEPS,
        PROOF_COHORT_GENERATION_ID,
        code_hash,
        cohort_specifications,
        m1_series,
        pre_governance_counts,
        training_dataset_fingerprints,
    )
except ModuleNotFoundError:
    from trad.oanda_proof_cohort_registry import (
        ensure_cohorts,
        material_contract,
        stable_hash,
        verify_transition_replay,
    )
    from trad.oanda_proof_shadow_predictors import (
        DEFAULT_COHORT_DATABASE,
        DEFAULT_COHORT_STATE,
        DEFAULT_DATABASE,
        DEFAULT_GOVERNANCE_CONFIG,
        DEFAULT_SNAPSHOT,
        FAMILIES,
        FEATURE_WINDOWS,
        HORIZON_STEPS,
        PROOF_COHORT_GENERATION_ID,
        code_hash,
        cohort_specifications,
        m1_series,
        pre_governance_counts,
        training_dataset_fingerprints,
    )


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
DEFAULT_AUDIT_JSON = STATE / "proof_lineage_repair_20260829_v1.json"
DEFAULT_AUDIT_MARKDOWN = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "proof_lineage_repair"
    / "PROOF_LINEAGE_REPAIR_20260829_V1.md"
)
GENERATION_ID = "lineage_repair_20260829_v1"
INCIDENT_ID = "proof_cohort_reactivation_incident_20260829_v1"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def content_sha256(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(value, encoding="utf-8")
    temporary.replace(path)


def build_current_frozen_specifications(
    *,
    snapshot_path: Path,
    governance_path: Path,
    forecast_database: Path,
    repair_started_utc: str,
) -> list[dict[str, Any]]:
    """Build registry specs from the predictor's current frozen contract only."""

    if PROOF_COHORT_GENERATION_ID != GENERATION_ID:
        raise RuntimeError(
            "predictor generation does not match this repair command: "
            f"{PROOF_COHORT_GENERATION_ID!r}"
        )
    snapshot = read_json(snapshot_path)
    governance = read_json(governance_path)
    generated = str(snapshot.get("generated_utc") or "")
    if not generated:
        raise RuntimeError("feature snapshot has no generated_utc")
    instruments = snapshot.get("instruments") or {}
    series_map = {
        str(pair): m1_series(raw)
        for pair, raw in instruments.items()
        if isinstance(raw, dict) and len(m1_series(raw)) >= 180
    }
    if series_map:
        _, pooled_hash = training_dataset_fingerprints(series_map, generated)
    else:
        # A weekend snapshot can intentionally omit completed price histories.
        # Initial training provenance is not part of the material cohort
        # contract and this command emits no forecasts.  Every later forecast
        # still carries its exact causal training cutoff/dataset hash.
        pooled_hash = "pending_first_prospective_forecast_after_lineage_repair"
    feature_version = hashlib.sha256(
        json.dumps(
            {
                "windows": FEATURE_WINDOWS,
                "horizon_steps": HORIZON_STEPS,
                "timeframe": "M1",
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    specifications = cohort_specifications(
        source_sha256=code_hash(),
        feature_version=feature_version,
        training_cutoff_utc=generated,
        training_dataset_sha256=pooled_hash,
        governance=governance,
        prior_counts=pre_governance_counts(forecast_database),
    )
    for specification in specifications:
        specification["cohort_start_utc"] = repair_started_utc
    _validate_repair_specifications(specifications)
    return specifications


def _validate_repair_specifications(specifications: list[dict[str, Any]]) -> None:
    families = [str(row.get("family") or "") for row in specifications]
    if sorted(families) != sorted(FAMILIES) or len(set(families)) != len(FAMILIES):
        raise RuntimeError(
            f"repair requires exactly the four frozen proof families: {sorted(FAMILIES)}"
        )
    wrong = [
        family
        for family, row in zip(families, specifications)
        if str(row.get("cohort_generation_id") or "") != GENERATION_ID
    ]
    if wrong:
        raise RuntimeError(f"repair generation is missing or wrong for: {sorted(wrong)}")


def _validate_distinct_paths(paths: dict[str, Path]) -> None:
    aliases: dict[str, list[str]] = {}
    for name, path in paths.items():
        aliases.setdefault(str(path.resolve()).casefold(), []).append(name)
    collisions = [names for names in aliases.values() if len(names) > 1]
    if collisions:
        raise RuntimeError(f"lineage repair paths must be distinct: {collisions}")


def _registry_snapshot(database_path: Path) -> dict[str, Any]:
    connection = sqlite3.connect(
        f"file:{database_path.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    connection.execute("PRAGMA query_only=ON")
    try:
        cohorts = connection.execute(
            "SELECT COUNT(*),COALESCE(MAX(rowid),0) FROM proof_cohorts"
        ).fetchone()
        transitions = connection.execute(
            "SELECT COUNT(*),COALESCE(MAX(rowid),0) FROM proof_cohort_transitions"
        ).fetchone()
        active = {
            str(family): str(cohort_id)
            for family, cohort_id in connection.execute(
                """
                SELECT transition.family,transition.next_cohort_id
                FROM proof_cohort_transitions AS transition
                JOIN (
                    SELECT family,MAX(rowid) AS row_id
                    FROM proof_cohort_transitions GROUP BY family
                ) AS latest ON latest.row_id=transition.rowid
                """
            )
        }
        return {
            "cohort_count": int(cohorts[0]),
            "cohort_highwater_rowid": int(cohorts[1]),
            "transition_count": int(transitions[0]),
            "transition_highwater_rowid": int(transitions[1]),
            "replayed_active_cohorts": dict(sorted(active.items())),
        }
    finally:
        connection.close()


def _forecast_snapshot(database_path: Path) -> dict[str, Any]:
    connection = sqlite3.connect(
        f"file:{database_path.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    connection.execute("PRAGMA query_only=ON")
    try:
        highwater = connection.execute(
            "SELECT COALESCE(MAX(rowid),0) FROM canonical_forecasts"
        ).fetchone()[0]
        relevant = connection.execute(
            "SELECT COUNT(*) FROM canonical_forecasts WHERE family IN (?,?,?,?)",
            FAMILIES,
        ).fetchone()[0]
        return {
            "canonical_forecast_highwater_rowid": int(highwater),
            "four_family_forecast_count": int(relevant),
        }
    finally:
        connection.close()


def discover_superseded_window_rows(
    *, registry_database: Path, forecast_database: Path
) -> list[dict[str, Any]]:
    """Find rows appended to a cohort after its immutable supersession event."""

    registry = sqlite3.connect(
        f"file:{registry_database.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    registry.execute("PRAGMA query_only=ON")
    try:
        transitions = registry.execute(
            """
            SELECT family,previous_cohort_id,next_cohort_id,observed_utc,transition_id
            FROM proof_cohort_transitions
            WHERE previous_cohort_id IS NOT NULL
              AND family IN (?,?,?,?)
            ORDER BY rowid
            """,
            FAMILIES,
        ).fetchall()
    finally:
        registry.close()

    forecast = sqlite3.connect(
        f"file:{forecast_database.resolve().as_posix()}?mode=ro", uri=True, timeout=60.0
    )
    forecast.execute("PRAGMA query_only=ON")
    quarantines: list[dict[str, Any]] = []
    try:
        forecast_columns = {
            str(row[1])
            for row in forecast.execute("PRAGMA table_info(canonical_forecasts)")
        }
        time_column = "recorded_utc" if "recorded_utc" in forecast_columns else "entry_time"
        for family, previous, successor, observed, transition_id in transitions:
            count, first_seen, last_seen = forecast.execute(
                f"""
                SELECT COUNT(*),MIN({time_column}),MAX({time_column})
                FROM canonical_forecasts
                WHERE family=? AND {time_column}>=?
                  AND json_valid(forecast_json)
                  AND json_extract(forecast_json,'$.cohort_id')=?
                """,
                (str(family), str(observed), str(previous)),
            ).fetchone()
            malformed = forecast.execute(
                f"""
                SELECT COUNT(*) FROM canonical_forecasts
                WHERE family=? AND {time_column}>=? AND NOT json_valid(forecast_json)
                """,
                (str(family), str(observed)),
            ).fetchone()[0]
            if int(count or 0) <= 0 and int(malformed or 0) <= 0:
                continue
            quarantines.append(
                {
                    "family": str(family),
                    "superseded_cohort_id": str(previous),
                    "successor_cohort_id": str(successor),
                    "supersession_transition_id": str(transition_id),
                    "supersession_observed_utc": str(observed),
                    "excluded_forecast_count": int(count),
                    "exclusion_clock": time_column,
                    "first_excluded_utc": first_seen,
                    "last_excluded_utc": last_seen,
                    "malformed_forecast_json_count": int(malformed or 0),
                    "disposition": "legacy_diagnostic_only_excluded_from_repair_generation",
                }
            )
    finally:
        forecast.close()
    return quarantines


def _active_generation_contracts(
    registry_database_path: Path,
    registry_state_path: Path,
    specifications: list[dict[str, Any]],
) -> tuple[bool, dict[str, str]]:
    state = read_json(registry_state_path)
    cohorts = {
        str(row.get("cohort_id") or ""): row
        for row in state.get("cohorts") or []
        if isinstance(row, dict)
    }
    active = {
        str(key): str(value)
        for key, value in (state.get("active_cohorts") or {}).items()
    }
    expected_hashes = {
        str(row["family"]): stable_hash(material_contract(row)) for row in specifications
    }
    state_valid = all(
        family in active
        and str((cohorts.get(active[family]) or {}).get("material_contract_sha256") or "")
        == digest
        and str((cohorts.get(active[family]) or {}).get("cohort_generation_id") or "")
        == GENERATION_ID
        for family, digest in expected_hashes.items()
    )
    connection = sqlite3.connect(
        f"file:{registry_database_path.resolve().as_posix()}?mode=ro",
        uri=True,
        timeout=30.0,
    )
    connection.execute("PRAGMA query_only=ON")
    try:
        replay = verify_transition_replay(connection)
        database_active = {
            str(family): str(cohort_id)
            for family, cohort_id in (replay.get("active_by_family") or {}).items()
        }
        database_valid = True
        generation_hashes: dict[str, set[str]] = {}
        for family, material_sha, contract_json in connection.execute(
            "SELECT family,material_contract_sha256,contract_json FROM proof_cohorts"
        ):
            contract = json.loads(str(contract_json))
            if str(contract.get("cohort_generation_id") or "") == GENERATION_ID:
                generation_hashes.setdefault(str(family), set()).add(str(material_sha))
        for family, digest in expected_hashes.items():
            cohort_id = database_active.get(family)
            if not cohort_id:
                database_valid = False
                continue
            row = connection.execute(
                "SELECT material_contract_sha256,contract_json FROM proof_cohorts WHERE cohort_id=?",
                (cohort_id,),
            ).fetchone()
            if row is None:
                database_valid = False
                continue
            contract = json.loads(str(row[1]))
            if (
                str(row[0]) != digest
                or str(contract.get("cohort_generation_id") or "") != GENERATION_ID
                or generation_hashes.get(family, set()) != {digest}
            ):
                database_valid = False
    finally:
        connection.close()
    return state_valid and database_valid and active == database_active, database_active


def _validate_generation_is_unused_or_exact(
    registry_database_path: Path, specifications: list[dict[str, Any]]
) -> None:
    expected = {
        str(row["family"]): stable_hash(material_contract(row)) for row in specifications
    }
    connection = sqlite3.connect(
        f"file:{registry_database_path.resolve().as_posix()}?mode=ro",
        uri=True,
        timeout=30.0,
    )
    connection.execute("PRAGMA query_only=ON")
    try:
        for family, material_sha, contract_json in connection.execute(
            "SELECT family,material_contract_sha256,contract_json FROM proof_cohorts"
        ):
            contract = json.loads(str(contract_json))
            if str(contract.get("cohort_generation_id") or "") != GENERATION_ID:
                continue
            if expected.get(str(family)) != str(material_sha):
                raise RuntimeError(
                    f"generation {GENERATION_ID} already exists with a different "
                    f"material contract for {family}"
                )
    finally:
        connection.close()


def _valid_existing_audit(payload: dict[str, Any]) -> bool:
    expected = str(payload.get("audit_payload_sha256") or "")
    unsigned = dict(payload)
    unsigned.pop("audit_payload_sha256", None)
    return bool(expected) and stable_hash(unsigned) == expected


def _audit_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Proof cohort lineage repair",
        "",
        f"Generated: `{payload['generated_utc']}`",
        "",
        f"Status: **{payload['status']}**",
        "",
        "This command produced no forecasts and did not rewrite any forecast or outcome row.",
        "Superseded-window rows remain immutable legacy diagnostics and are excluded from",
        f"the `{GENERATION_ID}` prospective proof generation.",
        "Counts below are a point-in-time high-water snapshot; exclusion remains open-ended",
        "for every forecast carrying a superseded cohort ID after its transition cutoff.",
        "",
        "| Family | Superseded cohort | Successor | Excluded rows | Window |",
        "|---|---|---|---:|---|",
    ]
    for row in payload.get("superseded_window_quarantines") or []:
        lines.append(
            f"| {row['family']} | `{row['superseded_cohort_id']}` | "
            f"`{row['successor_cohort_id']}` | {row['excluded_forecast_count']:,} | "
            f"{row['first_excluded_utc']} to {row['last_excluded_utc']} |"
        )
    lines.extend(
        [
            "",
            f"Total excluded legacy rows: **{payload['excluded_forecast_count']:,}**",
            f"Malformed legacy forecast payloads observed: **{payload.get('malformed_forecast_json_count', 0):,}**",
            "",
            "The new active cohort identifiers are registry-only until the predictor worker",
            "later emits genuinely prospective forecasts under those exact identifiers.",
            "",
        ]
    )
    return "\n".join(lines)


def execute_lineage_repair(
    *,
    registry_database: Path,
    registry_state: Path,
    forecast_database: Path,
    audit_json: Path,
    audit_markdown: Path,
    specifications: list[dict[str, Any]],
    repair_started_utc: str,
    apply: bool,
    confirmation_token: str = "",
) -> dict[str, Any]:
    """Plan or atomically open the repair generation; never touch forecasts."""

    _validate_repair_specifications(specifications)
    _validate_distinct_paths(
        {
            "registry_database": registry_database,
            "registry_state": registry_state,
            "forecast_database": forecast_database,
            "audit_json": audit_json,
            "audit_markdown": audit_markdown,
        }
    )
    _validate_generation_is_unused_or_exact(registry_database, specifications)
    existing_audit = read_json(audit_json)
    if audit_json.exists() and not existing_audit:
        raise RuntimeError("existing lineage repair audit is unreadable")
    already_active, current_active = _active_generation_contracts(
        registry_database, registry_state, specifications
    )
    if existing_audit:
        if (
            existing_audit.get("status") != "applied"
            or existing_audit.get("generation_id") != GENERATION_ID
            or not _valid_existing_audit(existing_audit)
        ):
            raise RuntimeError("existing lineage repair audit has an incompatible contract")
        if not already_active:
            raise RuntimeError("repair audit exists but repair generation is not active")
        expected_markdown = _audit_markdown(existing_audit)
        try:
            published_markdown = audit_markdown.read_text(encoding="utf-8")
        except OSError:
            published_markdown = ""
        if published_markdown != expected_markdown:
            atomic_text(audit_markdown, expected_markdown)
            return {
                "command_status": "repaired_audit_publication",
                "audit": existing_audit,
                "active_cohorts": current_active,
                "writes_performed": True,
            }
        return {
            "command_status": "already_applied",
            "audit": existing_audit,
            "active_cohorts": current_active,
            "writes_performed": False,
        }

    registry_before = _registry_snapshot(registry_database)
    forecast_before = _forecast_snapshot(forecast_database)
    quarantines = discover_superseded_window_rows(
        registry_database=registry_database,
        forecast_database=forecast_database,
    )
    plan = {
        "schema_version": 1,
        "incident_id": INCIDENT_ID,
        "generation_id": GENERATION_ID,
        "repair_started_utc": repair_started_utc,
        "registry_before": registry_before,
        "published_active_before": read_json(registry_state).get("active_cohorts") or {},
        "forecast_snapshot_before": forecast_before,
        "superseded_window_quarantines": quarantines,
        "excluded_forecast_count": sum(
            int(row["excluded_forecast_count"]) for row in quarantines
        ),
        "malformed_forecast_json_count": sum(
            int(row["malformed_forecast_json_count"]) for row in quarantines
        ),
        "requested_material_contract_sha256_by_family": {
            str(row["family"]): stable_hash(material_contract(row))
            for row in specifications
        },
        "quarantine_policy": {
            "legacy_rows_preserved": True,
            "legacy_rows_rewritten": False,
            "legacy_rows_deleted": False,
            "excluded_from_generation_id": GENERATION_ID,
            "new_generation_may_use_only_forecasts_tagged_with_new_active_cohort_ids": True,
        },
        "forecasts_produced": 0,
        "forecast_ledger_mutated": False,
        "can_place_orders": False,
        "can_promote": False,
    }
    if not apply:
        return {
            "command_status": "planned_not_applied",
            "plan": plan,
            "writes_performed": False,
        }
    if confirmation_token != GENERATION_ID:
        raise RuntimeError(
            f"--confirmation-token must exactly equal {GENERATION_ID!r}"
        )

    active = ensure_cohorts(registry_database, registry_state, specifications)
    valid, published_active = _active_generation_contracts(
        registry_database, registry_state, specifications
    )
    if not valid or active != published_active:
        raise RuntimeError("new repair generation did not publish as the active cohort set")
    registry_after = _registry_snapshot(registry_database)
    payload = {
        **plan,
        "status": "applied",
        "generated_utc": utc_now(),
        "active_cohorts": dict(sorted(active.items())),
        "registry_after": registry_after,
        "registry_state_sha256": content_sha256(registry_state),
        "forecast_writes_by_command": 0,
    }
    payload["audit_payload_sha256"] = stable_hash(payload)
    atomic_json(audit_json, payload)
    atomic_text(audit_markdown, _audit_markdown(payload))
    return {
        "command_status": "applied",
        "audit": payload,
        "active_cohorts": active,
        "writes_performed": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry-database", type=Path, default=DEFAULT_COHORT_DATABASE)
    parser.add_argument("--registry-state", type=Path, default=DEFAULT_COHORT_STATE)
    parser.add_argument("--forecast-database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--governance", type=Path, default=DEFAULT_GOVERNANCE_CONFIG)
    parser.add_argument("--audit-json", type=Path, default=DEFAULT_AUDIT_JSON)
    parser.add_argument("--audit-markdown", type=Path, default=DEFAULT_AUDIT_MARKDOWN)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirmation-token", default="")
    parser.add_argument("--repair-started-utc", default="")
    args = parser.parse_args()
    started = str(args.repair_started_utc or utc_now())
    specifications = build_current_frozen_specifications(
        snapshot_path=args.snapshot,
        governance_path=args.governance,
        forecast_database=args.forecast_database,
        repair_started_utc=started,
    )
    result = execute_lineage_repair(
        registry_database=args.registry_database,
        registry_state=args.registry_state,
        forecast_database=args.forecast_database,
        audit_json=args.audit_json,
        audit_markdown=args.audit_markdown,
        specifications=specifications,
        repair_started_utc=started,
        apply=bool(args.apply),
        confirmation_token=str(args.confirmation_token),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
