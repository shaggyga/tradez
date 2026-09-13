#!/usr/bin/env python3
"""Statistical governance for the frozen prospective FX proof cohorts."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROOF_FAMILIES = (
    "ridge_return_repaired",
    "modern_tabular_probabilistic_repaired",
    "cross_pair_graph_transfer",
    "probabilistic_state_space",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return default
    return output if math.isfinite(output) else default


def stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def percentile(values: list[float], probability: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = max(0.0, min(1.0, probability)) * (len(ordered) - 1)
    lower, upper = int(math.floor(position)), int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def benjamini_hochberg(pvalues: dict[str, float], q: float) -> dict[str, dict[str, Any]]:
    """Return monotone BH q-values and rejection state."""

    ordered = sorted((max(0.0, min(1.0, finite(p, 1.0))), key) for key, p in pvalues.items())
    total = len(ordered)
    if total == 0:
        return {}
    adjusted = [1.0] * total
    running = 1.0
    for index in range(total - 1, -1, -1):
        pvalue, _ = ordered[index]
        running = min(running, pvalue * total / (index + 1))
        adjusted[index] = running
    cutoff_rank = 0
    for index, (pvalue, _) in enumerate(ordered, start=1):
        if pvalue <= q * index / total:
            cutoff_rank = index
    return {
        key: {
            "pvalue": pvalue,
            "qvalue": adjusted[index],
            "passed": index + 1 <= cutoff_rank,
            "rank": index + 1,
            "tests": total,
        }
        for index, (pvalue, key) in enumerate(ordered)
    }


def apply_hierarchical_fdr(
    cells: list[dict[str, Any]],
    families: list[dict[str, Any]],
    *,
    family_q: float,
    cell_q: float,
) -> dict[str, Any]:
    family_tests = benjamini_hochberg(
        {
            f"{row['family']}|{row['horizon_sec']}": finite(
                row.get("one_sided_pvalue_zero_bounded"), 1.0
            )
            for row in families
        },
        family_q,
    )
    selected = {key for key, result in family_tests.items() if result["passed"]}
    cells_by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for cell in cells:
        family_key = f"{cell['family']}|{cell['horizon_sec']}"
        cell["family_discovery_fdr"] = family_tests.get(
            family_key,
            {"pvalue": 1.0, "qvalue": 1.0, "passed": False, "rank": 0, "tests": len(family_tests)},
        )
        cells_by_family[family_key].append(cell)
    cell_test_count = cell_pass_count = 0
    for family_key, members in cells_by_family.items():
        results = (
            benjamini_hochberg(
                {
                    str(row["cell_id"]): finite(
                        row.get("one_sided_pvalue_zero_bounded"), 1.0
                    )
                    for row in members
                },
                cell_q,
            )
            if family_key in selected
            else {}
        )
        cell_test_count += len(results)
        for row in members:
            result = results.get(
                str(row["cell_id"]),
                {"pvalue": finite(row.get("one_sided_pvalue_zero_bounded"), 1.0), "qvalue": 1.0, "passed": False, "rank": 0, "tests": len(results)},
            )
            row["cell_discovery_fdr"] = result
            row["multiplicity_survivor"] = bool(
                row["family_discovery_fdr"]["passed"] and result["passed"]
            )
            cell_pass_count += int(row["multiplicity_survivor"])
    return {
        "method": "hierarchical_benjamini_hochberg_family_then_cell",
        "family_fdr_q": family_q,
        "cell_fdr_q": cell_q,
        "family_tests": len(family_tests),
        "family_survivors": len(selected),
        "cell_tests_within_selected_families": cell_test_count,
        "cell_survivors": cell_pass_count,
        "family_results": family_tests,
    }


def evidence_census(cells: list[dict[str, Any]]) -> dict[str, Any]:
    raw = [int(row.get("raw_n") or 0) for row in cells]
    effective = [int(row.get("effective_n") or 0) for row in cells]
    percentiles = (0.0, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 1.0)

    def distribution(values: list[int]) -> dict[str, float]:
        return {
            f"p{int(probability * 100):02d}": round(percentile(values, probability), 3)
            for probability in percentiles
        }

    sample_only = 0
    for row in cells:
        failures = [
            item["gate"] for item in row.get("gate_distances", []) if not item.get("passed")
        ]
        sample_only += int(failures == ["effective_sample_size"])
    return {
        "cell_count": len(cells),
        "raw_n_distribution": distribution(raw),
        "effective_n_distribution": distribution(effective),
        "effective_n_threshold_counts": {
            str(threshold): sum(value >= threshold for value in effective)
            for threshold in (25, 50, 100, 200, 500)
        },
        "positive_point_ev_cells": sum(finite(row.get("avg_net_pips")) > 0.0 for row in cells),
        "positive_unadjusted_lcb_cells": sum(
            finite(row.get("lower_confidence_pips"), -999.0) > 0.0 for row in cells
        ),
        "multiplicity_survivor_cells": sum(bool(row.get("multiplicity_survivor")) for row in cells),
        "moderate_cost_stable_cells": sum(
            finite(
                (row.get("cost_shocks") or {}).get("total_slippage_0.50_pips", {}).get("avg_net_pips"),
                -999.0,
            )
            > 0.0
            for row in cells
        ),
        "severe_cost_stable_cells": sum(
            finite(
                (row.get("cost_shocks") or {}).get("total_slippage_1.00_pips", {}).get("avg_net_pips"),
                -999.0,
            )
            > 0.0
            for row in cells
        ),
        "blocked_only_by_sample_size_cells": sample_only,
        "economically_inadequate_with_current_power_cells": sum(
            row.get("unadjusted_upper_confidence_pips") is not None
            and finite(row.get("unadjusted_upper_confidence_pips"))
            < finite(row.get("minimum_economic_edge_pips"))
            for row in cells
        ),
        "median_minimum_detectable_edge_pips": round(
            percentile(
                [
                    finite(row.get("minimum_detectable_edge_pips"))
                    for row in cells
                    if row.get("minimum_detectable_edge_pips") is not None
                ],
                0.5,
            ),
            6,
        ),
        "median_additional_independent_episodes_for_power": round(
            percentile(
                [int(row.get("additional_effective_episodes_for_power") or 0) for row in cells],
                0.5,
            ),
            3,
        ),
    }


def cohort_evidence(source_database: Path, cohort_state_path: Path) -> dict[str, Any]:
    try:
        cohort_state = json.loads(cohort_state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        cohort_state = {}
    active = cohort_state.get("active_cohorts") or {}
    if not source_database.exists():
        return {"status": "source_database_missing", "active_cohorts": active, "cohorts": []}
    connection = sqlite3.connect(f"file:{source_database.resolve().as_posix()}?mode=ro", uri=True)
    connection.execute("PRAGMA query_only=ON")
    rows: list[dict[str, Any]] = []
    try:
        tables = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if "canonical_forecasts" not in tables:
            return {
                "status": "canonical_forecasts_unavailable",
                "active_cohorts": active,
                "cohorts": [],
                "pre_governance_untagged_forecasts": {
                    family: 0 for family in PROOF_FAMILIES
                },
                "merge_policy": "pre-governance and governed cohort evidence are never merged",
            }
        for family, cohort_id in sorted(active.items()):
            forecast = connection.execute(
                """
                SELECT COUNT(*),MIN(recorded_utc),MAX(recorded_utc),
                       COUNT(DISTINCT instrument),
                       SUM(CASE WHEN track_outcome=1 THEN 1 ELSE 0 END),
                       COUNT(DISTINCT json_extract(forecast_json,'$.training_dataset_sha256')),
                       COUNT(DISTINCT json_extract(forecast_json,'$.model_version')),
                       COUNT(DISTINCT json_extract(forecast_json,'$.feature_version'))
                FROM canonical_forecasts
                WHERE family=? AND json_extract(forecast_json,'$.cohort_id')=?
                """,
                (family, cohort_id),
            ).fetchone()
            matured = connection.execute(
                """
                SELECT COUNT(*) FROM canonical_outcomes o
                JOIN canonical_forecasts f ON f.event_id=o.event_id
                WHERE f.family=? AND json_extract(f.forecast_json,'$.cohort_id')=?
                """,
                (family, cohort_id),
            ).fetchone()[0]
            start = str(forecast[1] or "")
            end = str(forecast[2] or "")
            try:
                elapsed_hours = max(
                    1.0 / 60.0,
                    (
                        datetime.fromisoformat(end.replace("Z", "+00:00"))
                        - datetime.fromisoformat(start.replace("Z", "+00:00"))
                    ).total_seconds()
                    / 3600.0,
                )
            except (TypeError, ValueError):
                elapsed_hours = 0.0
            count = int(forecast[0] or 0)
            rows.append(
                {
                    "family": family,
                    "cohort_id": cohort_id,
                    "forecast_count": count,
                    "matured_outcome_count": int(matured or 0),
                    "maturation_rate": round(int(matured or 0) / count, 6) if count else 0.0,
                    "forecast_rate_per_hour": round(count / elapsed_hours, 3) if elapsed_hours else 0.0,
                    "pair_count": int(forecast[3] or 0),
                    "tracked_forecast_count": int(forecast[4] or 0),
                    "training_dataset_fingerprint_count": int(forecast[5] or 0),
                    "model_version_count": int(forecast[6] or 0),
                    "feature_version_count": int(forecast[7] or 0),
                    "first_forecast_utc": start or None,
                    "last_forecast_utc": end or None,
                    "direct_cell_evidence_required": True,
                    "pooled_model_confidence_cannot_promote": family
                    in {"modern_tabular_probabilistic_repaired", "cross_pair_graph_transfer"},
                }
            )
        legacy = dict(
            connection.execute(
                """
                SELECT family,COUNT(*) FROM canonical_forecasts
                WHERE family IN (?,?,?,?) AND json_extract(forecast_json,'$.cohort_id') IS NULL
                GROUP BY family
                """,
                PROOF_FAMILIES,
            ).fetchall()
        )
    finally:
        connection.close()
    return {
        "status": "collecting" if active else "cohort_registry_not_initialized",
        "active_cohorts": active,
        "cohorts": rows,
        "pre_governance_untagged_forecasts": {
            family: int(legacy.get(family, 0)) for family in PROOF_FAMILIES
        },
        "merge_policy": "pre-governance and governed cohort evidence are never merged",
    }


def initialize_governance_tables(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS locked_discovery_candidates (
            candidate_lock_id TEXT PRIMARY KEY,
            locked_utc TEXT NOT NULL,
            source_governance_sha256 TEXT NOT NULL,
            cell_id TEXT NOT NULL,
            family TEXT NOT NULL,
            instrument TEXT NOT NULL,
            horizon_sec INTEGER NOT NULL,
            session_bucket TEXT NOT NULL,
            liquidity_bucket TEXT NOT NULL,
            definition_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS confirmation_cohorts (
            confirmation_cohort_id TEXT PRIMARY KEY,
            candidate_lock_id TEXT NOT NULL,
            start_utc TEXT NOT NULL,
            end_utc TEXT,
            state TEXT NOT NULL,
            evidence_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS immutable_governance_snapshots (
            governance_sha256 TEXT PRIMARY KEY,
            first_generated_utc TEXT NOT NULL,
            source_highwater_row_id INTEGER NOT NULL,
            config_sha256 TEXT NOT NULL,
            inference_json TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS discovery_locks_no_update
        BEFORE UPDATE ON locked_discovery_candidates
        BEGIN SELECT RAISE(ABORT, 'candidate locks are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS discovery_locks_no_delete
        BEFORE DELETE ON locked_discovery_candidates
        BEGIN SELECT RAISE(ABORT, 'candidate locks are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS confirmation_cohorts_no_update
        BEFORE UPDATE ON confirmation_cohorts
        BEGIN SELECT RAISE(ABORT, 'confirmation cohorts are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS confirmation_cohorts_no_delete
        BEFORE DELETE ON confirmation_cohorts
        BEGIN SELECT RAISE(ABORT, 'confirmation cohorts are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS governance_snapshots_no_update
        BEFORE UPDATE ON immutable_governance_snapshots
        BEGIN SELECT RAISE(ABORT, 'governance snapshots are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS governance_snapshots_no_delete
        BEFORE DELETE ON immutable_governance_snapshots
        BEGIN SELECT RAISE(ABORT, 'governance snapshots are immutable'); END;
        """
    )


def candidate_definition(cell: dict[str, Any]) -> dict[str, Any]:
    """Complete frozen identity used by both discovery and registration."""
    return {key: cell.get(key) for key in (
        "cell_id", "family", "instrument", "horizon_sec", "session",
        "liquidity_bucket", "cohort_id", "minimum_economic_edge_pips",
        "sequential_method", "sequential_clip_bound_pips",
    )}


def lock_discovery_candidate(
    evidence_database: Path,
    *,
    governance_sha256: str,
    cell: dict[str, Any],
    locked_utc: str | None = None,
    observed_utc: str | None = None,
) -> str:
    """Immutably lock a selected discovery cell for a later cohort."""

    connection = sqlite3.connect(evidence_database, timeout=30.0)
    try:
        initialize_governance_tables(connection)
        snapshot = connection.execute(
            "SELECT inference_json,first_generated_utc FROM immutable_governance_snapshots WHERE governance_sha256=?",
            (governance_sha256,),
        ).fetchone()
        if not snapshot:
            raise ValueError("unknown governance snapshot")
        inference = json.loads(str(snapshot[0]))
        candidates = (
            (inference.get("graduation_ladder") or {})
            .get("B_discovery_candidate", {})
            .get("cell_ids", [])
        )
        if str(cell.get("cell_id")) not in {str(value) for value in candidates}:
            raise ValueError("cell did not pass the frozen discovery snapshot")
        definition = candidate_definition(cell)
        frozen = (inference.get("candidate_definitions") or {}).get(str(cell["cell_id"]))
        if frozen is None or definition != frozen:
            raise ValueError("candidate definition does not match frozen discovery evidence")
        if not definition.get("sequential_method") or finite(definition.get("sequential_clip_bound_pips")) <= 0.0:
            raise ValueError("candidate sequential evidence contract is incomplete")
        # observed_utc is an explicit offline-fixture clock seam. A supplied
        # lock claim cannot differ from the actual registration observation.
        when = str(observed_utc or utc_now())
        observed_time = datetime.fromisoformat(when.replace("Z", "+00:00"))
        snapshot_time = datetime.fromisoformat(str(snapshot[1]).replace("Z", "+00:00"))
        if observed_time.tzinfo is None or snapshot_time.tzinfo is None:
            raise ValueError("registration and discovery clocks must be timezone-aware")
        if observed_time < snapshot_time:
            raise ValueError("candidate lock cannot precede the frozen discovery snapshot")
        if locked_utc is not None and datetime.fromisoformat(str(locked_utc).replace("Z", "+00:00")) != observed_time:
            raise ValueError("candidate lock must equal observed registration time")
        lock_id = "candidate_lock_" + stable_hash(
            {"governance": governance_sha256, "definition": definition, "locked_utc": when}
        )[:24]
        connection.execute(
            "INSERT INTO locked_discovery_candidates VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                lock_id,
                when,
                governance_sha256,
                str(cell["cell_id"]),
                str(cell["family"]),
                str(cell["instrument"]),
                int(cell["horizon_sec"]),
                str(cell["session"]),
                str(cell["liquidity_bucket"]),
                json.dumps(definition, sort_keys=True, separators=(",", ":")),
            ),
        )
        connection.commit()
        return lock_id
    finally:
        connection.close()


def open_confirmation_cohort(
    evidence_database: Path,
    *,
    candidate_lock_id: str,
    start_utc: str,
    observed_utc: str | None = None,
) -> str:
    """Open a later untouched cohort; never backdate it into discovery evidence."""

    connection = sqlite3.connect(evidence_database, timeout=30.0)
    try:
        initialize_governance_tables(connection)
        locked = connection.execute(
            "SELECT locked_utc,definition_json FROM locked_discovery_candidates WHERE candidate_lock_id=?",
            (candidate_lock_id,),
        ).fetchone()
        if not locked:
            raise ValueError("unknown candidate lock")
        locked_time = datetime.fromisoformat(str(locked[0]).replace("Z", "+00:00"))
        start_time = datetime.fromisoformat(str(start_utc).replace("Z", "+00:00"))
        observed_time = datetime.fromisoformat(str(observed_utc or utc_now()).replace("Z", "+00:00"))
        if any(value.tzinfo is None for value in (locked_time, start_time, observed_time)):
            raise ValueError("confirmation clocks must be timezone-aware")
        if observed_time < locked_time or start_time < observed_time:
            raise ValueError("confirmation must be registered before its untouched sample starts")
        if start_time <= locked_time:
            raise ValueError("confirmation must start strictly after candidate lock")
        cohort_id = "confirmation_" + stable_hash(
            {"candidate_lock_id": candidate_lock_id, "start_utc": start_utc}
        )[:24]
        connection.execute(
            "INSERT INTO confirmation_cohorts VALUES (?,?,?,?,?,?)",
            (
                cohort_id,
                candidate_lock_id,
                start_utc,
                None,
                "collecting",
                json.dumps(
                    {
                        "discovery_evidence_excluded": True,
                        "definition": json.loads(str(locked[1])),
                        "can_place_orders": False,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ),
        )
        connection.commit()
        return cohort_id
    finally:
        connection.close()


def graduation_ladder(cells: list[dict[str, Any]], integrity: dict[str, Any]) -> dict[str, Any]:
    discovery: list[str] = []
    for row in cells:
        qualifies = (
            bool(row.get("multiplicity_survivor"))
            and finite(row.get("avg_net_pips")) >= finite(row.get("minimum_economic_edge_pips"))
            and finite(row.get("cost_clearance_probability")) > 0.5
            and finite(row.get("mfe_to_mae_ratio")) > 1.0
            and finite(row.get("time_uniform_lower_bound_pips"), -999.0) > 0.0
            and finite(row.get("expected_shortfall_pips"), -999.0)
            >= -10.0 * max(0.01, finite(row.get("minimum_economic_edge_pips")))
            and finite(row.get("best_episode_profit_share"), 1.0) <= 0.5
            and finite(
                (row.get("cost_shocks") or {}).get("total_slippage_1.00_pips", {}).get("avg_net_pips"),
                -999.0,
            )
            > 0.0
        )
        row["graduation_stage"] = "B_discovery_candidate" if qualifies else "A_contract_valid_only"
        if qualifies:
            discovery.append(str(row["cell_id"]))
    contract_valid = (
        str(integrity.get("immutability")) == "immutable_trigger_enforced"
        and int(integrity.get("duplicate_event_horizons") or 0) == 0
        and int(integrity.get("invalid_entry_times_excluded") or 0) == 0
        and int(integrity.get("invalid_boundary_times_excluded") or 0) == 0
    )
    return {
        "A_contract_validity": {
            "passed": contract_valid,
            "eligible_cell_count": len(cells) if contract_valid else 0,
        },
        "B_discovery_candidate": {
            "count": len(discovery),
            "cell_ids": discovery,
            "execution_permitted": False,
        },
        "C_locked_prospective_confirmation": {
            "count": 0,
            "reason": "no discovery candidate has been immutably locked into an untouched later cohort",
            "execution_permitted": False,
        },
        "D_practice_canary": {
            "count": 0,
            "auto_route": False,
            "purpose": "execution validation only, never profitability rediscovery",
        },
        "real_money": {"permitted": False, "outside_phase": True},
    }


def build_governance(
    *,
    cells: list[dict[str, Any]],
    families: list[dict[str, Any]],
    integrity: dict[str, Any],
    source_database: Path,
    cohort_state_path: Path,
    evidence_database: Path,
    config: dict[str, Any],
) -> dict[str, Any]:
    discovery = config.get("discovery") or {}
    multiplicity = apply_hierarchical_fdr(
        cells,
        families,
        family_q=finite(discovery.get("family_fdr_q"), 0.05),
        cell_q=finite(discovery.get("cell_fdr_q"), 0.05),
    )
    census = evidence_census(cells)
    cohorts = cohort_evidence(source_database, cohort_state_path)
    ladder = graduation_ladder(cells, integrity)
    inference = {
        "schema_version": 1,
        "research_only": True,
        "can_place_orders": False,
        "can_promote": False,
        "config_sha256": stable_hash(config),
        "source_highwater_row_id": int(integrity.get("source_highwater_row_id") or 0),
        "multiple_testing": multiplicity,
        "continuous_monitoring": {
            "method": str((config.get("sequential") or {}).get("method") or ""),
            "alpha": finite((config.get("sequential") or {}).get("alpha"), 0.01),
            "ordinary_repeated_confidence_intervals_may_not_promote": True,
        },
        "evidence_census": census,
        "proof_cohorts": cohorts,
        "graduation_ladder": ladder,
        "candidate_definitions": {
            str(row["cell_id"]): candidate_definition(row)
            for row in cells
            if row.get("graduation_stage") == "B_discovery_candidate"
        },
        "stress_contract": {
            "recorded_executable_costs": True,
            "moderate_and_severe_cost_shocks": True,
            "delayed_entry_fixed_penalties_pips": (config.get("stress") or {}).get("delayed_entry_penalties_pips") or [],
            "one_bar_latency_path_status": "requires timestamp-complete executable quote path before candidate confirmation",
            "best_pair_day_week_session_currency_episode_and_top_5pct_concentration": True,
            "macro_event_exclusion_status": "required at candidate lock; no candidate exists",
        },
    }
    fingerprint_payload = {
        "config_sha256": inference["config_sha256"],
        "source_highwater_row_id": inference["source_highwater_row_id"],
        "multiple_testing": multiplicity,
        "evidence_census": census,
        "proof_cohorts": cohorts,
        "graduation_ladder": ladder,
        "candidate_definitions": inference["candidate_definitions"],
    }
    inference["governance_sha256"] = stable_hash(fingerprint_payload)
    connection = sqlite3.connect(evidence_database, timeout=30.0)
    try:
        initialize_governance_tables(connection)
        connection.execute(
            "INSERT OR IGNORE INTO immutable_governance_snapshots VALUES (?,?,?,?,?)",
            (
                inference["governance_sha256"],
                utc_now(),
                inference["source_highwater_row_id"],
                inference["config_sha256"],
                json.dumps(inference, sort_keys=True, separators=(",", ":")),
            ),
        )
        connection.commit()
    finally:
        connection.close()
    return inference
