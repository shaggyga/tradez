#!/usr/bin/env python3
"""Read-only coverage, drift, synchronization, and operational truth audit."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from oanda_news_classification_contract import NEWS_CLASSIFICATION_VERSION
from oanda_news_collector_contract import NEWS_COLLECTOR_CONTRACT_ID
from oanda_continuous_narrative_meter_v12 import (
    BUCKET_MINUTES as CONTINUOUS_NARRATIVE_BUCKET_MINUTES,
    METER_CONTRACT_ID as CONTINUOUS_NARRATIVE_METER_CONTRACT,
    SEAL_GRACE_SECONDS as CONTINUOUS_NARRATIVE_SEAL_GRACE_SECONDS,
)
from oanda_official_release_fast_lane_contract import (
    OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_ACTIVATED_UTC,
    OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID,
    OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_PRIOR_ACTIVATED_UTC,
    OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_PRIOR_CONTRACT_ID,
    OFFICIAL_RELEASE_FAST_LANE_PRIOR_COHORT_ID,
    OFFICIAL_RELEASE_FAST_LANE_PRIOR_CONTRACT_ID,
    OFFICIAL_RELEASE_FAST_LANE_COHORT_ID,
    OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID,
)


ROOT = Path(__file__).resolve().parent
EXPECTED_CONFIGURED_NEWS_SOURCES = 102
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
DEFAULT_OUTPUT = STATE / "project_integrity_audit_v1.json"
DEFAULT_HISTORY = DATA / "logs" / "project_integrity_audit_v1.jsonl"
DEFAULT_REPORT = DATA / "reports" / "project_integrity" / "PROJECT_INTEGRITY_CURRENT.md"
LOCAL_NEWS_DATABASE = DATA / "local_news_sentiment" / "local_news_sentiment_v1.sqlite"
CURRENT_NEWS_COLLECTOR_STATE = (
    DATA / "local_news_sentiment" / "collector_state_v1.json"
)
CURRENT_CONTEXT_ARTICLES = DATA / "local_news_sentiment" / "context_articles_latest.json"
CURRENT_PERSISTENT_POLICY_STATE = (
    DATA / "local_news_sentiment" / "persistent_policy_state_v1.json"
)
CURRENT_OFFICIAL_CURRENCY_SOURCE_DEPTH = (
    DATA
    / "reports"
    / "official_currency_source_depth"
    / "OFFICIAL_CURRENCY_SOURCE_DEPTH_CURRENT.json"
)
OFFICIAL_SOURCE_DEPTH_CURRENCIES = {
    "AUD", "CAD", "CHF", "CNH", "CZK", "DKK", "EUR", "GBP", "HKD",
    "HUF", "JPY", "MXN", "NOK", "NZD", "PLN", "SEK", "SGD", "THB",
    "TRY", "USD", "ZAR",
}
OFFICIAL_SOURCE_DEPTH_CATEGORIES = {
    "inflation", "labour", "growth", "trade", "intervention_reserves",
    "market_rates", "fiscal_debt",
}
CURRENT_MOVE_FIRST_NEWS_AUDIT = (
    DATA
    / "reports"
    / "move_first_news_case_audit"
    / "MOVE_FIRST_NEWS_CASE_AUDIT_CURRENT.json"
)
CURRENT_MAJOR_MOVE_GAP_CENSUS = (
    DATA
    / "reports"
    / "major_move_gap_census"
    / "MAJOR_MOVE_GAP_CENSUS_CURRENT.json"
)
CURRENT_LIVE_MOVE_NEWS_SNAPSHOT = STATE / "live_move_news_snapshot_v7r3.json"
CURRENT_LIVE_MOVE_NEWS_OUTCOMES = STATE / "live_move_news_outcomes_v4r3.json"
CURRENT_LIVE_MOVE_NEWS_CASES = STATE / "live_move_news_cases_v7r3.sqlite"
CURRENT_CONTINUOUS_NARRATIVE_METER = (
    STATE / "continuous_narrative_meter_v12.json"
)
CONTINUOUS_NARRATIVE_METER_DATABASE = (
    STATE / "continuous_narrative_meter_v12.sqlite"
)
CURRENT_NEWS_OUTCOME_IMPROVEMENT_AUDIT = (
    STATE / "news_outcome_improvement_audit_v2.json"
)
NEWS_OUTCOME_IMPROVEMENT_DATABASE = (
    STATE / "news_outcome_improvement_audit_v2.sqlite"
)
CURRENT_LIVE_MOVE_PERSISTENT_CONTEXT = (
    STATE / "live_move_persistent_news_context_v5r3.json"
)
CURRENT_OFFICIAL_RELEASE_FAST_LANE = (
    DATA / "local_news_sentiment" / "official_release_fast_lane_latest_v4.json"
)
CURRENT_OFFICIAL_RELEASE_FAST_LANE_HEARTBEAT = (
    DATA / "local_news_sentiment" / "official_release_fast_lane_heartbeat_v4.json"
)
OFFICIAL_RELEASE_FAST_LANE_DATABASE = (
    DATA / "local_news_sentiment" / "official_release_fast_lane_v4.sqlite"
)
OFFICIAL_RELEASE_FAST_LANE_CONTRACT = OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID
SOURCE_GOVERNANCE_DATABASE = STATE / "source_governance_v1.sqlite"
CURRENT_OFFICIAL_RELEASE_FAST_MAPPING = (
    DATA / "local_news_sentiment" / "official_release_fast_mapping_latest_v3.json"
)
CURRENT_OFFICIAL_RELEASE_FAST_MAPPING_HEARTBEAT = (
    DATA / "local_news_sentiment" / "official_release_fast_mapping_heartbeat_v3.json"
)
OFFICIAL_RELEASE_FAST_MAPPING_DATABASE = (
    DATA / "local_news_sentiment" / "official_release_fast_mapping_v3.sqlite"
)
OFFICIAL_RELEASE_FAST_MAPPING_CONTRACT = (
    "official_release_fast_mapping_v3_semantic_vs_publish_gate_20260824"
)
CURRENT_OFFICIAL_RELEASE_FAST_RESPONSE_WATCH = (
    DATA / "local_news_sentiment" / "official_release_fast_response_watch_latest_v3.json"
)
CURRENT_OFFICIAL_RELEASE_FAST_RESPONSE_HEARTBEAT = (
    DATA / "local_news_sentiment" / "official_release_fast_response_watch_heartbeat_v3.json"
)
OFFICIAL_RELEASE_FAST_RESPONSE_DATABASE = (
    DATA / "local_news_sentiment" / "official_release_fast_response_watch_v3.sqlite"
)
OFFICIAL_RELEASE_FAST_RESPONSE_CONTRACT = (
    "official_release_fast_response_watch_v3_multi_horizon_exact_quote_20260825"
)
OFFICIAL_RELEASE_FAST_RESPONSE_COHORT = (
    "official_release_fast_response_watch_v3_20260825"
)
LIVE_MOVE_NEWS_SNAPSHOT_CONTRACT = (
    "live_move_news_snapshot_v7r3_transitive_factor_root_union_20260827"
)
LIVE_MOVE_NEWS_OUTCOME_CONTRACT = (
    "live_move_news_forward_outcomes_v4r3_v7r3_"
    "transitive_factor_root_union_20260827"
)
LIVE_MOVE_FACTOR_EPISODE_CONTRACT = (
    "live_factor_episode_overlap_adjacency_v3_gap60_fixed_onset5m_"
    "append_only_transitive_root_union_20260827"
)
LEGACY_LIVE_MOVE_NEWS_SNAPSHOT_CONTRACT = (
    "live_move_news_snapshot_v5_causal_factor_strength_20260827"
)
LEGACY_LIVE_MOVE_NEWS_OUTCOME_CONTRACT = (
    "live_move_news_forward_outcomes_v1_20260824"
)
NEWS_OUTCOME_IMPROVEMENT_CONTRACT = (
    "news_outcome_improvement_audit_v2_20260826"
)
LIVE_MOVE_PERSISTENT_CONTEXT_CONTRACT = (
    "live_move_persistent_news_context_v5r3_exact_v7r3_"
    "transitive_root_union_binding_20260827"
)
CURRENT_NEWS_CLASSIFICATION_VERSION = NEWS_CLASSIFICATION_VERSION
MAXIMUM_CONCURRENT_HEARTBEAT_FUTURE_SKEW_SEC = 10.0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def parse_epoch(value: Any) -> float | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def heartbeat_age_is_current(
    age_sec: float | None,
    *,
    maximum_age_sec: float,
    maximum_future_skew_sec: float = MAXIMUM_CONCURRENT_HEARTBEAT_FUTURE_SKEW_SEC,
) -> bool:
    """Accept a narrowly concurrent heartbeat write, but not a future clock."""

    return bool(
        age_sec is not None
        and -maximum_future_skew_sec <= age_sec <= maximum_age_sec
    )


def news_outcome_improvement_integrity(
    payload: dict[str, Any],
    *,
    database_path: Path,
    cutoff_epoch: float,
    maximum_future_skew_sec: float = 300.0,
) -> dict[str, Any]:
    """Verify the on-demand canonical record is immutable and inert."""

    generated_epoch = parse_epoch(payload.get("generated_utc"))
    age_sec = (
        None
        if generated_epoch is None
        else cutoff_epoch - generated_epoch
    )
    database_integrity = "missing"
    retained_diagnoses = -1
    canonical_snapshot_row: tuple[Any, ...] | None = None
    append_only_triggers_present = False
    try:
        connection = sqlite3.connect(
            f"file:{database_path.resolve().as_posix()}?mode=ro",
            uri=True,
            timeout=2.0,
        )
        try:
            database_integrity = str(
                connection.execute("PRAGMA quick_check").fetchone()[0]
            )
            retained_diagnoses = int(
                connection.execute("SELECT COUNT(*) FROM diagnoses").fetchone()[0]
            )
            canonical_snapshot_row = connection.execute(
                """
                SELECT generated_utc, diagnosis_count, queue_count
                FROM audit_snapshots
                WHERE source_fingerprint=?
                """,
                (str(payload.get("source_fingerprint") or ""),),
            ).fetchone()
            trigger_names = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='trigger'"
                ).fetchall()
            }
            append_only_triggers_present = {
                "diagnoses_no_update",
                "diagnoses_no_delete",
                "audit_snapshots_no_update",
                "audit_snapshots_no_delete",
            } <= trigger_names
        finally:
            connection.close()
    except (OSError, sqlite3.Error):
        pass
    summary = payload.get("summary") or {}
    queue = payload.get("queue") or {}
    canonical_snapshot = payload.get("canonical_snapshot") or {}
    input_contracts = payload.get("input_contracts") or {}
    reported_reasons = set((summary.get("reason_counts") or {}).keys())
    known_reasons = set(payload.get("known_reason_codes") or [])
    ok = bool(
        payload.get("schema_version") == 1
        and payload.get("contract_id") == NEWS_OUTCOME_IMPROVEMENT_CONTRACT
        and payload.get("status") == "ok"
        and payload.get("research_only") is True
        and payload.get("execution_eligible") is False
        and payload.get("can_place_orders") is False
        and payload.get("can_promote") is False
        and payload.get("can_modify_execution_policy") is False
        and payload.get("broker_access") is False
        and payload.get("refresh_mode") == "on_demand_append_only"
        and payload.get("recurring_polling") is False
        and payload.get("supported_execution_decision")
        == "no_change_diagnostic_only"
        and input_contracts.get("signal_news")
        == "practice_007_signal_news_monitor_v7"
        and input_contracts.get("mover_cases")
        == LEGACY_LIVE_MOVE_NEWS_SNAPSHOT_CONTRACT
        and input_contracts.get("mover_outcomes")
        == LEGACY_LIVE_MOVE_NEWS_OUTCOME_CONTRACT
        and payload.get("sqlite_integrity") == "ok"
        and database_integrity == "ok"
        and append_only_triggers_present
        and retained_diagnoses
        == int(summary.get("retained_diagnoses") or 0)
        and int(queue.get("queue_count") or 0)
        == len(queue.get("items") or [])
        and reported_reasons <= known_reasons
        and age_sec is not None
        and age_sec >= -maximum_future_skew_sec
        and canonical_snapshot_row is not None
        and str(payload.get("recorded_utc") or "")
        == str(canonical_snapshot_row[0])
        and str(canonical_snapshot.get("source_fingerprint") or "")
        == str(payload.get("source_fingerprint") or "")
        and int(canonical_snapshot.get("diagnosis_count") or 0)
        == int(canonical_snapshot_row[1])
        and int(summary.get("current_effective_theses") or 0)
        == int(canonical_snapshot_row[1])
        and int(canonical_snapshot.get("queue_count") or 0)
        == int(canonical_snapshot_row[2])
        and int(queue.get("queue_count") or 0)
        == int(canonical_snapshot_row[2])
    )
    return {
        "ok": ok,
        "generated_utc": payload.get("generated_utc"),
        "age_sec": age_sec,
        "database_integrity": database_integrity,
        "append_only_triggers_present": append_only_triggers_present,
        "retained_diagnoses": retained_diagnoses,
        "reported_retained_diagnoses": int(
            summary.get("retained_diagnoses") or 0
        ),
        "queue_count": int(queue.get("queue_count") or 0),
        "research_only": payload.get("research_only"),
        "execution_eligible": payload.get("execution_eligible"),
        "can_place_orders": payload.get("can_place_orders"),
        "can_promote": payload.get("can_promote"),
        "can_modify_execution_policy": payload.get(
            "can_modify_execution_policy"
        ),
    }


def official_source_depth_integrity(
    payload: dict[str, Any], *, cutoff_epoch: float
) -> dict[str, Any]:
    generated_epoch = parse_epoch(payload.get("generated_utc"))
    age_sec = (
        None
        if generated_epoch is None
        else max(0.0, cutoff_epoch - generated_epoch)
    )
    category_counts = payload.get("category_currency_counts") or {}
    rows = payload.get("currencies") or []
    currencies = {
        str(row.get("currency") or "")
        for row in rows
        if isinstance(row, dict)
    }
    weight = payload.get("currency_strength_weight")
    exact_zero_weight = (
        not isinstance(weight, bool)
        and isinstance(weight, (int, float))
        and float(weight) == 0.0
    )
    ok = bool(
        payload.get("schema_version")
        == "official_currency_source_depth_report_v1"
        and payload.get("research_only") is True
        and payload.get("execution_eligible") is False
        and exact_zero_weight
        and payload.get("currency_count") == 21
        and payload.get("complete_transport_depth_count") == 21
        and currencies == OFFICIAL_SOURCE_DEPTH_CURRENCIES
        and set(category_counts) == OFFICIAL_SOURCE_DEPTH_CATEGORIES
        and all(category_counts.get(name) == 21 for name in OFFICIAL_SOURCE_DEPTH_CATEGORIES)
        and len(rows) == 21
        and all(
            isinstance(row, dict)
            and row.get("complete_transport_depth") is True
            and row.get("missing_categories") == []
            for row in rows
        )
        and age_sec is not None
        and age_sec <= 25_200.0
    )
    return {
        "ok": ok,
        "generated_utc": payload.get("generated_utc"),
        "age_sec": age_sec,
        "currency_count": payload.get("currency_count"),
        "complete_transport_depth_count": payload.get(
            "complete_transport_depth_count"
        ),
        "category_currency_counts": category_counts,
        "currency_strength_weight": weight,
        "research_only": payload.get("research_only"),
        "execution_eligible": payload.get("execution_eligible"),
    }


def official_release_fast_lane_integrity(
    snapshot: dict[str, Any],
    heartbeat: dict[str, Any],
    *,
    database_path: Path,
    cutoff_epoch: float,
    maximum_heartbeat_age_sec: float = 180.0,
) -> dict[str, Any]:
    """Verify the official release observation lane is fresh and inert."""

    heartbeat_epoch = parse_epoch(heartbeat.get("heartbeat_utc"))
    heartbeat_age = (
        None if heartbeat_epoch is None else cutoff_epoch - heartbeat_epoch
    )
    database_integrity = "missing"
    observation_count = -1
    prospective_count = -1
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(database_path, timeout=5.0)
        database_integrity = str(
            connection.execute("PRAGMA quick_check").fetchone()[0]
        )
        row = connection.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(prospective_observation), 0)
            FROM official_release_observation
            """
        ).fetchone()
        observation_count = int(row[0])
        prospective_count = int(row[1])
    except (OSError, sqlite3.Error, TypeError, ValueError):
        database_integrity = "error"
    finally:
        if connection is not None:
            connection.close()
    policy = snapshot.get("policy") or {}
    heartbeat_policy = heartbeat.get("policy") or {}
    counts = snapshot.get("counts") or {}
    contract_ok = bool(
        snapshot.get("schema_version") == "official_release_fast_lane_v4"
        and snapshot.get("collector_contract_id")
        == OFFICIAL_RELEASE_FAST_LANE_CONTRACT
        and heartbeat.get("schema_version") == "official_release_fast_lane_v4"
        and heartbeat.get("collector_contract_id")
        == OFFICIAL_RELEASE_FAST_LANE_CONTRACT
    )
    inert_ok = bool(
        policy.get("research_only") is True
        and policy.get("execution_eligible") is False
        and policy.get("can_authorize") is False
        and policy.get("can_promote") is False
        and policy.get("broker_access") is False
        and heartbeat_policy.get("research_only") is True
        and heartbeat_policy.get("execution_eligible") is False
        and heartbeat_policy.get("can_authorize") is False
        and heartbeat_policy.get("can_promote") is False
        and heartbeat_policy.get("broker_access") is False
    )
    coverage_ok = bool(
        snapshot.get("configured_currency_count") == 21
        and isinstance(snapshot.get("configured_release_source_count"), int)
        and int(snapshot.get("configured_release_source_count") or 0) >= 21
    )
    counts_ok = bool(
        observation_count >= 0
        and 0 <= prospective_count <= observation_count
        and int(counts.get("observations") or 0) == observation_count
        and int(counts.get("prospective_observations") or 0)
        == prospective_count
    )
    freshness_ok = bool(
        heartbeat_age_is_current(
            heartbeat_age, maximum_age_sec=maximum_heartbeat_age_sec
        )
        and heartbeat.get("status") in {"running_cycle", "cycle_complete"}
    )
    return {
        "ok": bool(
            contract_ok
            and inert_ok
            and coverage_ok
            and counts_ok
            and freshness_ok
            and database_integrity == "ok"
        ),
        "contract_ok": contract_ok,
        "inert_ok": inert_ok,
        "coverage_ok": coverage_ok,
        "counts_ok": counts_ok,
        "freshness_ok": freshness_ok,
        "heartbeat_age_sec": heartbeat_age,
        "database_integrity": database_integrity,
        "observations": observation_count,
        "prospective_observations": prospective_count,
    }


def official_release_fast_mapping_integrity(
    snapshot: dict[str, Any],
    heartbeat: dict[str, Any],
    *,
    database_path: Path,
    cutoff_epoch: float,
    maximum_heartbeat_age_sec: float = 180.0,
) -> dict[str, Any]:
    """Verify the fast semantic mapper is current, reproducible and inert."""

    heartbeat_epoch = parse_epoch(heartbeat.get("heartbeat_utc"))
    heartbeat_age = (
        None if heartbeat_epoch is None else cutoff_epoch - heartbeat_epoch
    )
    snapshot_generated_utc = str(snapshot.get("generated_utc") or "")
    snapshot_epoch = parse_epoch(snapshot_generated_utc)
    snapshot_age = (
        None if snapshot_epoch is None else cutoff_epoch - snapshot_epoch
    )
    database_integrity = "missing"
    database_counts = (-1, -1, -1, -1, -1, -1)
    database_current_counts = (-1, -1, -1, -1, -1, -1)
    count_basis = "unavailable"
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(database_path, timeout=5.0)
        connection.execute("BEGIN")
        database_integrity = str(
            connection.execute("PRAGMA quick_check").fetchone()[0]
        )
        columns = {
            str(row[1])
            for row in connection.execute(
                "PRAGMA table_info(official_release_mapping)"
            )
        }
        count_sql = """
            SELECT COUNT(*),
                   COALESCE(SUM(input_prospective_observation), 0),
                   COALESCE(SUM(semantic_direction_available), 0),
                   COALESCE(SUM(prospective_semantic_candidate), 0),
                   COALESCE(SUM(publish_eligible_forward_candidate), 0),
                   COALESCE(SUM(forward_shadow_candidate), 0)
            FROM official_release_mapping
            WHERE classification_version = ? AND mapper_contract_id = ?
        """
        parameters: tuple[Any, ...] = (
            CURRENT_NEWS_CLASSIFICATION_VERSION,
            OFFICIAL_RELEASE_FAST_MAPPING_CONTRACT,
        )
        current_row = connection.execute(count_sql, parameters).fetchone()
        database_current_counts = tuple(int(value) for value in current_row)
        if "mapped_utc" not in columns or snapshot_epoch is None:
            raise ValueError("mapping_snapshot_missing_stable_count_cutoff")
        row = connection.execute(
            count_sql + " AND julianday(mapped_utc)<=julianday(?)",
            (*parameters, snapshot_generated_utc),
        ).fetchone()
        database_counts = tuple(int(value) for value in row)
        count_basis = "database_rows_mapped_at_or_before_snapshot_generated_utc"
    except (OSError, sqlite3.Error, TypeError, ValueError):
        database_integrity = "error"
    finally:
        if connection is not None:
            connection.close()
    policy = snapshot.get("policy") or {}
    heartbeat_policy = heartbeat.get("policy") or {}
    counts = snapshot.get("counts") or {}
    contract_ok = bool(
        snapshot.get("schema_version") == "official_release_fast_mapping_v3"
        and snapshot.get("mapper_contract_id")
        == OFFICIAL_RELEASE_FAST_MAPPING_CONTRACT
        and snapshot.get("required_input_contract_id")
        == OFFICIAL_RELEASE_FAST_LANE_CONTRACT
        and snapshot.get("required_classification_version")
        == CURRENT_NEWS_CLASSIFICATION_VERSION
        and heartbeat.get("schema_version")
        == "official_release_fast_mapping_v3"
        and heartbeat.get("mapper_contract_id")
        == OFFICIAL_RELEASE_FAST_MAPPING_CONTRACT
        and heartbeat.get("required_input_contract_id")
        == OFFICIAL_RELEASE_FAST_LANE_CONTRACT
        and heartbeat.get("required_classification_version")
        == CURRENT_NEWS_CLASSIFICATION_VERSION
    )
    inert_ok = bool(
        policy.get("research_only") is True
        and policy.get("execution_eligible") is False
        and policy.get("can_place_orders") is False
        and policy.get("can_authorize") is False
        and policy.get("can_promote") is False
        and policy.get("broker_access") is False
        and policy.get("watchlist_mutation") is False
        and heartbeat_policy.get("research_only") is True
        and heartbeat_policy.get("execution_eligible") is False
        and heartbeat_policy.get("can_authorize") is False
        and heartbeat_policy.get("can_promote") is False
        and heartbeat_policy.get("broker_access") is False
    )
    counts_ok = bool(
        database_counts[0] >= 0
        and 0 <= database_counts[1] <= database_counts[0]
        and 0 <= database_counts[2] <= database_counts[0]
        and 0 <= database_counts[3] <= database_counts[1]
        and 0 <= database_counts[4] <= database_counts[3]
        and database_counts[5] == database_counts[3]
        and int(counts.get("mappings") or 0) == database_counts[0]
        and int(counts.get("prospective_inputs") or 0) == database_counts[1]
        and int(counts.get("semantic_direction_available") or 0)
        == database_counts[2]
        and int(counts.get("prospective_semantic_candidates") or 0)
        == database_counts[3]
        and int(counts.get("publish_eligible_forward_candidates") or 0)
        == database_counts[4]
        and int(counts.get("forward_shadow_candidates") or 0)
        == database_counts[5]
    )
    freshness_ok = bool(
        heartbeat_age_is_current(
            heartbeat_age, maximum_age_sec=maximum_heartbeat_age_sec
        )
        and snapshot_age is not None
        and -5.0 <= snapshot_age <= maximum_heartbeat_age_sec
        and heartbeat.get("status") in {"running_cycle", "cycle_complete"}
    )
    return {
        "ok": bool(
            contract_ok
            and inert_ok
            and counts_ok
            and freshness_ok
            and database_integrity == "ok"
        ),
        "contract_ok": contract_ok,
        "inert_ok": inert_ok,
        "counts_ok": counts_ok,
        "freshness_ok": freshness_ok,
        "heartbeat_age_sec": heartbeat_age,
        "snapshot_age_sec": snapshot_age,
        "snapshot_generated_utc": snapshot_generated_utc,
        "count_basis": count_basis,
        "database_integrity": database_integrity,
        "mappings": database_counts[0],
        "prospective_inputs": database_counts[1],
        "semantic_direction_available": database_counts[2],
        "prospective_semantic_candidates": database_counts[3],
        "publish_eligible_forward_candidates": database_counts[4],
        "forward_shadow_candidates": database_counts[5],
        "current_database_mappings": database_current_counts[0],
    }


def official_release_fast_response_integrity(
    snapshot: dict[str, Any],
    heartbeat: dict[str, Any],
    *,
    database_path: Path,
    cutoff_epoch: float,
    maximum_heartbeat_age_sec: float = 180.0,
) -> dict[str, Any]:
    """Verify exact-quote official response watches remain current and inert."""

    heartbeat_epoch = parse_epoch(heartbeat.get("heartbeat_utc"))
    heartbeat_age = (
        None if heartbeat_epoch is None else cutoff_epoch - heartbeat_epoch
    )
    database_integrity = "missing"
    database_counts = (-1, -1, -1, -1, -1, -1)
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(database_path, timeout=5.0)
        database_integrity = str(
            connection.execute("PRAGMA quick_check").fetchone()[0]
        )
        watch_row = connection.execute(
            """
            SELECT COUNT(*),COUNT(DISTINCT mapping_id),
                   COUNT(DISTINCT event_factor_id),COUNT(DISTINCT instrument)
            FROM response_watch
            """
        ).fetchone()
        outcome_row = connection.execute(
            """
            SELECT COUNT(*),
                   COALESCE(SUM(CASE WHEN maturity_state='valid_exact_executable_quote'
                                     THEN 1 ELSE 0 END),0)
            FROM response_outcome
            """
        ).fetchone()
        database_counts = tuple(int(value) for value in (*watch_row, *outcome_row))
    except (OSError, sqlite3.Error, TypeError, ValueError):
        database_integrity = "error"
    finally:
        if connection is not None:
            connection.close()
    policy = snapshot.get("policy") or {}
    heartbeat_policy = heartbeat.get("policy") or {}
    counts = snapshot.get("counts") or {}
    contract_ok = bool(
        snapshot.get("schema_version") == "official_release_fast_response_watch_v3"
        and snapshot.get("contract_id") == OFFICIAL_RELEASE_FAST_RESPONSE_CONTRACT
        and snapshot.get("cohort_id") == OFFICIAL_RELEASE_FAST_RESPONSE_COHORT
        and snapshot.get("required_mapper_contract_id")
        == OFFICIAL_RELEASE_FAST_MAPPING_CONTRACT
        and snapshot.get("required_classification_version")
        == CURRENT_NEWS_CLASSIFICATION_VERSION
        and snapshot.get("horizons_min") == [1, 5, 15, 30, 60, 120]
        and heartbeat.get("schema_version")
        == "official_release_fast_response_watch_v3"
        and heartbeat.get("contract_id") == OFFICIAL_RELEASE_FAST_RESPONSE_CONTRACT
        and heartbeat.get("cohort_id") == OFFICIAL_RELEASE_FAST_RESPONSE_COHORT
    )
    inert_ok = bool(
        policy.get("research_only") is True
        and policy.get("execution_eligible") is False
        and policy.get("can_place_orders") is False
        and policy.get("can_authorize") is False
        and policy.get("can_promote") is False
        and policy.get("broker_access") is False
        and policy.get("canonical_watchlist_mutation") is False
        and policy.get("supported_decision") == "shadow_observation_only"
        and heartbeat_policy.get("research_only") is True
        and heartbeat_policy.get("execution_eligible") is False
        and heartbeat_policy.get("can_place_orders") is False
        and heartbeat_policy.get("can_authorize") is False
        and heartbeat_policy.get("can_promote") is False
        and heartbeat_policy.get("broker_access") is False
        and heartbeat_policy.get("canonical_watchlist_mutation") is False
    )
    counts_ok = bool(
        database_counts[0] >= 0
        and 0 <= database_counts[1] <= database_counts[0]
        and 0 <= database_counts[2] <= database_counts[0]
        and 0 <= database_counts[3] <= database_counts[0]
        and 0 <= database_counts[4] <= database_counts[0]
        and 0 <= database_counts[5] <= database_counts[4]
        and int(counts.get("watches") or 0) == database_counts[0]
        and int(counts.get("mapping_events") or 0) == database_counts[1]
        and int(counts.get("factor_episodes") or 0) == database_counts[2]
        and int(counts.get("instruments") or 0) == database_counts[3]
        and int(counts.get("outcomes") or 0) == database_counts[4]
        and int(counts.get("valid_outcomes") or 0) == database_counts[5]
    )
    freshness_ok = bool(
        heartbeat_age_is_current(
            heartbeat_age, maximum_age_sec=maximum_heartbeat_age_sec
        )
        and heartbeat.get("status") in {"running_cycle", "cycle_complete"}
    )
    return {
        "ok": bool(
            contract_ok
            and inert_ok
            and counts_ok
            and freshness_ok
            and database_integrity == "ok"
        ),
        "contract_ok": contract_ok,
        "inert_ok": inert_ok,
        "counts_ok": counts_ok,
        "freshness_ok": freshness_ok,
        "heartbeat_age_sec": heartbeat_age,
        "database_integrity": database_integrity,
        "watches": database_counts[0],
        "mapping_events": database_counts[1],
        "factor_episodes": database_counts[2],
        "instruments": database_counts[3],
        "outcomes": database_counts[4],
        "valid_outcomes": database_counts[5],
    }


def move_first_news_audit_is_current(
    audit: dict[str, Any],
    upstream_census: dict[str, Any],
    *,
    cutoff_epoch: float,
    maximum_age_sec: float = 28800.0,
) -> bool:
    """Require a fresh, fail-closed audit built after its move inventory."""

    generated = parse_epoch(audit.get("generated_utc"))
    upstream_generated = parse_epoch(upstream_census.get("generated_utc"))
    return bool(
        audit.get("schema_version") == 2
        and audit.get("research_id") == "move_first_news_case_audit_v2"
        and audit.get("research_only") is True
        and audit.get("execution_eligible") is False
        and audit.get("can_place_orders") is False
        and audit.get("execution_decision") == "no_trade"
        and isinstance(audit.get("case_count"), int)
        and int(audit.get("case_count") or 0) >= 0
        and generated is not None
        and upstream_generated is not None
        and generated >= upstream_generated
        and 0.0 <= cutoff_epoch - generated <= maximum_age_sec
    )


def independent_verifier_operationally_safe(
    verifier: dict[str, Any],
    lifecycle_counts: dict[str, Any],
    *,
    cutoff_epoch: float,
    maximum_previous_match_age_sec: float = 1800.0,
) -> bool:
    """Accept a completed match or a bounded, explicitly fail-closed pass.

    The independent verifier publishes progress while rebuilding its large
    read-only ledgers.  That progress state deliberately disables
    authorization.  Sampling it must not create a false integrity incident
    when a recent completed match and its passing checks are retained, there
    are no confirmed candidates, and every routing surface remains closed.
    """

    if verifier.get("status") == "match":
        return True
    if verifier.get("status") != "verification_in_progress":
        return False
    previous_completed = parse_epoch(
        verifier.get("previous_completed_state_utc")
    )
    checks = verifier.get("checks")
    return bool(
        verifier.get("authorization_safe") is False
        and verifier.get("can_place_orders") is False
        and verifier.get("can_promote") is False
        and verifier.get("supported_decision") == "no_trade"
        and not (verifier.get("verified_confirmed_candidates") or [])
        and int(lifecycle_counts.get("confirmed_candidate") or 0) == 0
        and previous_completed is not None
        and 0.0
        <= cutoff_epoch - previous_completed
        <= maximum_previous_match_age_sec
        and isinstance(checks, list)
        and bool(checks)
        and all(
            isinstance(row, dict) and row.get("passed") is True
            for row in checks
        )
    )


def live_move_forward_proof_integrity(
    snapshot: dict[str, Any],
    outcomes: dict[str, Any],
    *,
    cutoff_epoch: float,
    case_database_path: Path = CURRENT_LIVE_MOVE_NEWS_CASES,
    snapshot_maximum_age_sec: float = 360.0,
    outcome_maximum_age_sec: float = 600.0,
) -> dict[str, Any]:
    """Bind V7R3/V4R3 to exact inert clocks and nonexpansive root unions."""

    snapshot_epoch = parse_epoch(snapshot.get("generated_utc"))
    outcome_epoch = parse_epoch(outcomes.get("generated_utc"))
    snapshot_age = (
        None if snapshot_epoch is None else cutoff_epoch - snapshot_epoch
    )
    outcome_age = None if outcome_epoch is None else cutoff_epoch - outcome_epoch
    join = snapshot.get("narrative_join_contract") or {}
    factor_contract = snapshot.get("factor_episode_contract") or {}
    outcome_upstream = outcomes.get("upstream_integrity") or {}
    movers = snapshot.get("movers") or []

    def causal_provenance(row: Any) -> bool:
        if not isinstance(row, dict):
            return False
        state = row.get("continuous_narrative_state")
        provenance = row.get("continuous_narrative_provenance")
        if not isinstance(state, dict) or not isinstance(provenance, dict):
            return False
        if not (
            provenance.get("meter_contract_id")
            == CONTINUOUS_NARRATIVE_METER_CONTRACT
            and provenance.get("partial_live_excluded") is True
            and provenance.get("execution_eligible") is False
        ):
            return False
        if state.get("state_available") is False:
            return bool(
                provenance.get("meter_state_kind") == "unavailable"
                and provenance.get("meter_clock_utc") is None
                and provenance.get("meter_sealed_at_utc") is None
            )
        if state.get("state_available") is not True:
            return False
        start = parse_epoch(row.get("start_utc"))
        clock = parse_epoch(provenance.get("meter_clock_utc"))
        sealed = parse_epoch(provenance.get("meter_sealed_at_utc"))
        return bool(
            provenance.get("meter_state_kind")
            == "immutable_completed_bucket"
            and start is not None
            and clock is not None
            and sealed is not None
            and clock <= start
            and sealed <= start
        )

    mover_provenance_ok = bool(
        isinstance(movers, list) and all(causal_provenance(row) for row in movers)
    )
    database_integrity = "missing"
    database_registry_ok = False
    append_only_triggers_present = False
    database_membership_conflict_count: int | None = None
    database_membership_mismatch_count: int | None = None
    database_invalid_root_merge_count: int | None = None
    database_raw_root_count: int | None = None
    database_canonical_root_count: int | None = None
    database_root_union_cycle: bool | None = None
    try:
        connection = sqlite3.connect(
            f"file:{case_database_path.resolve().as_posix()}?mode=ro", uri=True
        )
        try:
            database_integrity = str(
                connection.execute("PRAGMA quick_check").fetchone()[0]
            )
            registry = connection.execute(
                "SELECT meter_contract_id,partial_live_excluded,research_only,"
                "execution_eligible FROM mover_case_contract_registry "
                "WHERE contract_id=?",
                (LIVE_MOVE_NEWS_SNAPSHOT_CONTRACT,),
            ).fetchone()
            database_registry_ok = bool(
                registry
                and str(registry[0]) == CONTINUOUS_NARRATIVE_METER_CONTRACT
                and int(registry[1]) == 1
                and int(registry[2]) == 1
                and int(registry[3]) == 0
            )
            factor_registry_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM factor_episode_contract_registry "
                    "WHERE factor_episode_contract_id=? AND snapshot_contract_id=? "
                    "AND research_only=1 AND execution_eligible=0",
                    (
                        LIVE_MOVE_FACTOR_EPISODE_CONTRACT,
                        LIVE_MOVE_NEWS_SNAPSHOT_CONTRACT,
                    ),
                ).fetchone()[0]
            )
            database_registry_ok = database_registry_ok and factor_registry_count == 1
            database_membership_conflict_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM factor_episode_membership_conflicts"
                ).fetchone()[0]
            )
            database_membership_mismatch_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM mover_cases c "
                    "LEFT JOIN factor_episode_membership f ON f.case_id=c.case_id "
                    "WHERE json_extract(c.case_json,'$.contract_id')=? AND ("
                    "f.case_id IS NULL OR f.factor_episode_contract_id<>? OR "
                    "f.factor_episode_id<>json_extract(c.case_json,'$.factor_episode_id') "
                    "OR f.factor_primary_token<>json_extract(c.case_json,'$.factor_primary_token'))",
                    (
                        LIVE_MOVE_NEWS_SNAPSHOT_CONTRACT,
                        LIVE_MOVE_FACTOR_EPISODE_CONTRACT,
                    ),
                ).fetchone()[0]
            )
            database_invalid_root_merge_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM factor_episode_root_merges WHERE "
                    "factor_episode_contract_id<>? OR from_root_id=into_root_id "
                    "OR factor_primary_token='' OR research_only<>1 "
                    "OR execution_eligible<>0",
                    (LIVE_MOVE_FACTOR_EPISODE_CONTRACT,),
                ).fetchone()[0]
            )
            raw_roots = {
                str(row[0])
                for row in connection.execute(
                    "SELECT factor_episode_id FROM factor_episode_membership "
                    "WHERE factor_episode_contract_id=?",
                    (LIVE_MOVE_FACTOR_EPISODE_CONTRACT,),
                ).fetchall()
            }
            edge_rows = connection.execute(
                "SELECT from_root_id,into_root_id FROM factor_episode_root_merges "
                "WHERE factor_episode_contract_id=? ORDER BY detected_utc,merge_id",
                (LIVE_MOVE_FACTOR_EPISODE_CONTRACT,),
            ).fetchall()
            edges: dict[str, str] = {}
            edge_conflict = False
            for source, target in edge_rows:
                source_text = str(source)
                target_text = str(target)
                prior = edges.get(source_text)
                edge_conflict = edge_conflict or (
                    prior is not None and prior != target_text
                )
                edges[source_text] = target_text

            def resolve_root(root: str) -> tuple[str, bool]:
                current = root
                seen: set[str] = set()
                while current in edges:
                    if current in seen:
                        return current, True
                    seen.add(current)
                    current = edges[current]
                return current, False

            graph_roots = raw_roots | set(edges) | set(edges.values())
            resolved_graph = [resolve_root(root) for root in graph_roots]
            database_root_union_cycle = edge_conflict or any(
                cycle for _, cycle in resolved_graph
            )
            resolved = [resolve_root(root) for root in raw_roots]
            database_raw_root_count = len(raw_roots)
            database_canonical_root_count = len(
                {canonical for canonical, _ in resolved}
            )
            trigger_names = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='trigger'"
                ).fetchall()
            }
            append_only_triggers_present = {
                "mover_cases_no_update",
                "mover_cases_no_delete",
                "mover_case_contract_registry_no_update",
                "mover_case_contract_registry_no_delete",
                "factor_episode_contract_registry_no_update",
                "factor_episode_contract_registry_no_delete",
                "factor_episode_membership_no_update",
                "factor_episode_membership_no_delete",
                "factor_episode_membership_conflicts_no_update",
                "factor_episode_membership_conflicts_no_delete",
                "factor_episode_root_merges_no_update",
                "factor_episode_root_merges_no_delete",
            } <= trigger_names
        finally:
            connection.close()
    except (OSError, sqlite3.Error):
        pass
    snapshot_ok = bool(
        snapshot.get("schema_version") == 7
        and snapshot.get("contract_id") == LIVE_MOVE_NEWS_SNAPSHOT_CONTRACT
        and snapshot.get("research_only") is True
        and snapshot.get("execution_eligible") is False
        and snapshot.get("can_place_orders") is False
        and snapshot.get("can_promote") is False
        and snapshot.get("broker_access") is False
        and snapshot.get("supported_decision") == "diagnostic_only"
        and isinstance(snapshot.get("mover_count"), int)
        and isinstance(snapshot.get("factor_episode_count"), int)
        and isinstance(movers, list)
        and len(movers) == int(snapshot.get("mover_count") or 0)
        and join.get("meter_contract_id")
        == CONTINUOUS_NARRATIVE_METER_CONTRACT
        and join.get("partial_live_excluded") is True
        and join.get("sealed_v12_database_only") is True
        and join.get("research_only") is True
        and join.get("execution_eligible") is False
        and factor_contract.get("contract_id")
        == LIVE_MOVE_FACTOR_EPISODE_CONTRACT
        and factor_contract.get("grouping_method")
        == (
            "fixed_onset_same_primary_interval_overlap_adjacency_"
            "append_only_transitive_root_union"
        )
        and factor_contract.get("root_union_can_increase_effective_n") is False
        and factor_contract.get("membership_rewrites_allowed") is False
        and snapshot.get("factor_episode_integrity_ok") is True
        and snapshot.get("factor_episode_membership_conflict_total") == 0
        and snapshot.get("factor_episode_graph_cycle") is False
        and all(
            isinstance(row, dict)
            and row.get("factor_episode_contract_id")
            == LIVE_MOVE_FACTOR_EPISODE_CONTRACT
            and bool(row.get("factor_episode_id"))
            and bool(row.get("factor_episode_canonical_root_id"))
            and row.get("factor_episode_membership_conflict") is False
            for row in movers
        )
        and mover_provenance_ok
        and database_integrity == "ok"
        and database_registry_ok
        and append_only_triggers_present
        and database_membership_conflict_count == 0
        and database_membership_mismatch_count == 0
        and database_invalid_root_merge_count == 0
        and database_root_union_cycle is False
        and database_raw_root_count is not None
        and database_canonical_root_count is not None
        and database_canonical_root_count <= database_raw_root_count
        and snapshot_age is not None
        and 0.0 <= snapshot_age <= snapshot_maximum_age_sec
    )
    outcomes_ok = bool(
        outcomes.get("schema_version") == 4
        and outcomes.get("contract_id") == LIVE_MOVE_NEWS_OUTCOME_CONTRACT
        and outcomes.get("case_contract_id") == LIVE_MOVE_NEWS_SNAPSHOT_CONTRACT
        and outcomes.get("factor_episode_contract_id")
        == LIVE_MOVE_FACTOR_EPISODE_CONTRACT
        and outcomes.get("research_only") is True
        and outcomes.get("execution_eligible") is False
        and outcomes.get("can_place_orders") is False
        and outcomes.get("can_promote") is False
        and outcomes.get("supported_decision") == "collect_forward_outcomes"
        and outcomes.get("sqlite_integrity") == "ok"
        and outcome_upstream.get("ok") is True
        and outcome_upstream.get("failures") == []
        and outcome_upstream.get("quick_check") == "ok"
        and outcome_upstream.get("case_contract_id")
        == LIVE_MOVE_NEWS_SNAPSHOT_CONTRACT
        and outcome_upstream.get("factor_episode_contract_id")
        == LIVE_MOVE_FACTOR_EPISODE_CONTRACT
        and int(outcome_upstream.get("membership_conflict_count") or 0) == 0
        and int(outcome_upstream.get("membership_mismatch_count") or 0) == 0
        and int(outcome_upstream.get("invalid_root_merge_count") or 0) == 0
        and outcome_upstream.get("root_union_cycle") is False
        and isinstance(outcome_upstream.get("raw_root_count"), int)
        and isinstance(outcome_upstream.get("canonical_root_count"), int)
        and int(outcome_upstream.get("canonical_root_count"))
        <= int(outcome_upstream.get("raw_root_count"))
        and isinstance(outcomes.get("case_count"), int)
        and isinstance(outcomes.get("retained_outcome_count"), int)
        and outcome_age is not None
        and 0.0 <= outcome_age <= outcome_maximum_age_sec
    )
    return {
        "ok": snapshot_ok and outcomes_ok,
        "snapshot_ok": snapshot_ok,
        "outcomes_ok": outcomes_ok,
        "snapshot_contract_id": snapshot.get("contract_id"),
        "outcome_contract_id": outcomes.get("contract_id"),
        "outcome_case_contract_id": outcomes.get("case_contract_id"),
        "snapshot_age_sec": snapshot_age,
        "outcome_age_sec": outcome_age,
        "narrative_join_ok": bool(
            join.get("meter_contract_id")
            == CONTINUOUS_NARRATIVE_METER_CONTRACT
            and join.get("partial_live_excluded") is True
            and join.get("sealed_v12_database_only") is True
        ),
        "mover_provenance_ok": mover_provenance_ok,
        "case_database_integrity": database_integrity,
        "case_database_registry_ok": database_registry_ok,
        "case_database_append_only": append_only_triggers_present,
        "factor_episode_contract_id": factor_contract.get("contract_id"),
        "case_database_membership_conflict_count": (
            database_membership_conflict_count
        ),
        "case_database_membership_mismatch_count": (
            database_membership_mismatch_count
        ),
        "case_database_invalid_root_merge_count": (
            database_invalid_root_merge_count
        ),
        "case_database_raw_root_count": database_raw_root_count,
        "case_database_canonical_root_count": database_canonical_root_count,
        "case_database_root_union_cycle": database_root_union_cycle,
        "outcome_upstream_integrity_ok": outcome_upstream.get("ok") is True,
        "retained_case_count": int(snapshot.get("retained_case_count") or 0),
        "retained_outcome_count": int(outcomes.get("retained_outcome_count") or 0),
    }


def live_move_persistent_context_integrity(
    state: dict[str, Any], *, cutoff_epoch: float, maximum_age_sec: float = 360.0
) -> dict[str, Any]:
    """Require exact, fresh, inert V5R3 root-union-bound context output."""

    generated = parse_epoch(state.get("generated_utc"))
    age = None if generated is None else cutoff_epoch - generated
    rows = state.get("movers")
    upstream = state.get("upstream_factor_history_status") or {}
    ok = bool(
        state.get("schema_version") == 5
        and state.get("contract_id") == LIVE_MOVE_PERSISTENT_CONTEXT_CONTRACT
        and state.get("input_snapshot_contract_id")
        == LIVE_MOVE_NEWS_SNAPSHOT_CONTRACT
        and state.get("expected_input_snapshot_contract_id")
        == LIVE_MOVE_NEWS_SNAPSHOT_CONTRACT
        and state.get("input_factor_episode_contract_id")
        == LIVE_MOVE_FACTOR_EPISODE_CONTRACT
        and state.get("expected_factor_episode_contract_id")
        == LIVE_MOVE_FACTOR_EPISODE_CONTRACT
        and state.get("input_integrity_ok") is True
        and state.get("input_rejection_reasons") == []
        and upstream.get("ok") is True
        and upstream.get("quick_check") == "ok"
        and upstream.get("contract_registry_ok") is True
        and upstream.get("append_only_triggers_ok") is True
        and upstream.get("current_mover_membership_ok") is True
        and int(upstream.get("membership_conflict_total") or 0) == 0
        and upstream.get("graph_cycle") is False
        and upstream.get("root_union_nonexpansive") is True
        and isinstance(upstream.get("raw_root_count"), int)
        and isinstance(upstream.get("canonical_root_count"), int)
        and int(upstream.get("canonical_root_count"))
        <= int(upstream.get("raw_root_count"))
        and state.get("research_only") is True
        and state.get("execution_eligible") is False
        and state.get("can_place_orders") is False
        and state.get("can_promote") is False
        and state.get("supported_decision") == "diagnostic_only"
        and isinstance(state.get("mover_count"), int)
        and isinstance(rows, list)
        and len(rows) == int(state.get("mover_count") or 0)
        and all(
            isinstance(row, dict)
            and row.get("factor_episode_contract_id")
            == LIVE_MOVE_FACTOR_EPISODE_CONTRACT
            and bool(row.get("factor_episode_id"))
            and bool(row.get("factor_episode_canonical_root_id"))
            and row.get("factor_episode_membership_conflict") is False
            and isinstance(row.get("persistent_active_event_count"), int)
            and isinstance(row.get("persistent_independent_story_count"), int)
            and row.get("persistent_direct_authority_side")
            in {"long", "short", "neutral"}
            and row.get("persistent_direct_authority_alignment")
            in {"aligned", "opposed", "neutral"}
            for row in rows
        )
        and age is not None
        and 0.0 <= age <= maximum_age_sec
    )
    return {
        "ok": ok,
        "contract_id": state.get("contract_id"),
        "input_snapshot_contract_id": state.get("input_snapshot_contract_id"),
        "input_factor_episode_contract_id": state.get(
            "input_factor_episode_contract_id"
        ),
        "input_integrity_ok": state.get("input_integrity_ok") is True,
        "upstream_root_union_ok": upstream.get("ok") is True,
        "upstream_raw_root_count": upstream.get("raw_root_count"),
        "upstream_canonical_root_count": upstream.get("canonical_root_count"),
        "age_sec": age,
        "mover_count": int(state.get("mover_count") or 0),
    }


def live_feature_coverage_diagnostic(
    feature_state: dict[str, Any], expected_instruments: int
) -> dict[str, Any]:
    """Explain partial feature coverage without weakening quote freshness."""
    actual = int(feature_state.get("instrument_count") or 0)
    if actual == expected_instruments:
        return {
            "status": "full",
            "accepted_instrument_count": actual,
            "expected_instrument_count": expected_instruments,
            "exclusions": [],
        }
    coverage = feature_state.get("coverage") or {}
    exclusions = coverage.get("quote_exclusions") or []
    allowed_reasons = {
        "missing_quote",
        "quote_not_tradeable",
        "quote_time_invalid",
        "stale_quote",
    }
    explained = (
        bool(coverage.get("fail_closed_on_invalid_quote"))
        and int(coverage.get("expected_feature_instrument_count") or -1)
        == expected_instruments
        and int(coverage.get("accepted_instrument_count") or -1) == actual
        and int(coverage.get("excluded_instrument_count") or -1) == len(exclusions)
        and actual + len(exclusions) == expected_instruments
        and all(
            isinstance(row, dict)
            and str(row.get("instrument") or "")
            and row.get("reason") in allowed_reasons
            for row in exclusions
        )
    )
    return {
        "status": "fail_closed_quote_exclusions" if explained else "unexpected",
        "accepted_instrument_count": actual,
        "expected_instrument_count": expected_instruments,
        "exclusions": exclusions if explained else [],
    }


def forex_market_state(now: datetime) -> str:
    now = now.astimezone(timezone.utc)
    if now.weekday() == 5 or (now.weekday() == 6 and now.hour < 21):
        return "weekend_closed"
    if now.weekday() == 4 and now.hour >= 21:
        return "weekend_closed"
    return "open_or_transition"


def availability_market_state_consistent(
    state: dict[str, Any], expected_market_state: str
) -> bool:
    """Check that active feed gaps cannot masquerade as weekend movement."""
    scopes = [state]
    comparison = state.get("comparison")
    if isinstance(comparison, dict):
        scopes.append(comparison)
    for scope in scopes:
        latest = scope.get("latest") or {}
        if latest.get("market_state") != expected_market_state:
            return False
        active_gap = scope.get("current_gap_start_epoch") is not None
        if expected_market_state == "weekend_closed" and active_gap:
            if scope.get("current_gap_classification") != "market_closed_expected":
                return False
            if scope.get("current_gap_market_movement"):
                return False
        if (
            expected_market_state != "weekend_closed"
            and scope.get("current_gap_classification") == "market_closed_expected"
        ):
            return False
    return True


def historical_archive_gap_digest_preserved(shadow_archive: dict[str, Any]) -> bool:
    """Require every known detail-gap date to retain a matching frozen digest."""
    reconciliation = shadow_archive.get("detail_reconciliation") or {}
    gap = int(
        reconciliation.get(
            "historical_missing_detail_rows",
            max(0, int(reconciliation.get("detail_gap_vs_rollup") or 0)),
        )
        or 0
    )
    if gap == 0:
        return True
    gap_dates = reconciliation.get("positive_gap_dates") or [
        row for row in (reconciliation.get("nonzero_gap_dates") or [])
        if int(row.get("detail_gap") or 0) > 0
    ]
    return bool(
        gap_dates
        and reconciliation.get("gap_dates_have_matching_frozen_snapshot") is True
        and all(
            int(row.get("detail_gap") or 0) > 0
            and row.get("snapshot_matches_rollup") is True
            and bool(row.get("frozen_snapshot_sha256"))
            for row in gap_dates
        )
    )


def structured_numeric_currency_integrity(
    path: Path, *, source_state_path: Path | None = None
) -> dict[str, Any]:
    """Verify source-native rows stay single-currency and prospectively versioned.

    Derived source lineage was added prospectively.  Articles observed before a
    source's recorded adoption clock remain immutable legacy evidence; they are
    counted explicitly rather than rewritten.  Any row observed at or after the
    adoption clock must carry the exact source contract/cohort metadata.
    """
    if not path.is_file():
        return {"status":"missing","bound_rows":0,"invalid_rows":[],"ok":False}
    try:
        connection=sqlite3.connect(
            f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=10.0
        )
        article_columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(articles)")
        }
        first_seen_expression = (
            "first_seen_utc" if "first_seen_utc" in article_columns else "NULL"
        )
        rows=connection.execute(
            f"""SELECT event_id,source_id,headline,currencies_json,
                      {first_seen_expression},
                      json_extract(payload_json,'$.source_contract_id'),
                      json_extract(payload_json,'$.source_cohort_id')
               FROM articles
               WHERE json_extract(payload_json,'$.classification_version')=?
                 AND json_extract(payload_json,'$.source_native_currency_bound')=1""",
            (CURRENT_NEWS_CLASSIFICATION_VERSION,),
        ).fetchall()
        connection.close()
    except (OSError,sqlite3.Error) as exc:
        return {"status":"error","error":str(exc)[:300],"bound_rows":0,
                "invalid_rows":[],"ok":False}
    source_state = read_json(source_state_path) if source_state_path else {}
    source_states = source_state.get("sources")
    if not isinstance(source_states, dict):
        source_states = {}
    invalid=[]
    legacy_pre_lineage=[]
    for (
        event_id, source_id, headline, currencies_json, first_seen_utc,
        source_contract_id, source_cohort_id,
    ) in rows:
        try:
            currencies=json.loads(currencies_json or "[]")
        except (TypeError,ValueError,json.JSONDecodeError):
            currencies=[]
        reasons=[]
        if len(currencies)!=1:
            reasons.append("currency_cardinality_not_one")
        missing_contract = not str(source_contract_id or "")
        missing_cohort = not str(source_cohort_id or "")
        source_runtime = source_states.get(str(source_id))
        if not isinstance(source_runtime, dict):
            source_runtime = {}
        adoption_epoch = parse_epoch(source_runtime.get("source_lineage_adopted_utc"))
        first_seen_epoch = parse_epoch(first_seen_utc)
        is_derived_legacy = bool(
            (missing_contract or missing_cohort)
            and source_runtime.get("source_contract_derived") is True
            and adoption_epoch is not None
            and first_seen_epoch is not None
            and first_seen_epoch < adoption_epoch
        )
        if is_derived_legacy:
            legacy_pre_lineage.append({
                "event_id": str(event_id),
                "source_id": str(source_id),
                "headline": str(headline),
                "first_seen_utc": str(first_seen_utc or ""),
                "source_lineage_adopted_utc": str(
                    source_runtime.get("source_lineage_adopted_utc") or ""
                ),
            })
        else:
            if missing_contract:
                reasons.append("missing_source_contract_id")
            if missing_cohort:
                reasons.append("missing_source_cohort_id")
        if reasons:
            invalid.append({"event_id":str(event_id),"source_id":str(source_id),
                            "headline":str(headline),"currencies":currencies,
                            "reasons":reasons})
    return {"status":"ok","classification_version":CURRENT_NEWS_CLASSIFICATION_VERSION,
            "bound_rows":len(rows),"invalid_rows":invalid[:20],
            "invalid_row_count":len(invalid),
            "legacy_pre_lineage_rows":legacy_pre_lineage[:20],
            "legacy_pre_lineage_count":len(legacy_pre_lineage),
            "prospective_lineage_enforced":bool(source_state_path),
            "ok":bool(rows) and not invalid}


def official_detail_quality_integrity(path: Path) -> dict[str, Any]:
    """Reject enriched official details that are only transport/error shells.

    The current context artifact is deliberately used instead of rescanning the
    full news database on every audit cycle. Historical rows remain immutable;
    this check protects the live semantic surface consumed by research workers.
    """
    state = read_json(path)
    articles = state.get("articles")
    if not isinstance(articles, list):
        return {
            "status": "missing_or_invalid",
            "article_count": 0,
            "enriched_detail_count": 0,
            "invalid_rows": [],
            "invalid_row_count": 0,
            "ok": False,
        }
    markers = (
        "maintenance sorry, this service is currently unavailable",
        "this service is currently unavailable",
        "access denied",
        "request unsuccessful",
        "page not found",
    )
    enriched_count = 0
    invalid: list[dict[str, str]] = []
    for raw in articles:
        if not isinstance(raw, dict) or raw.get("detail_enriched") is not True:
            continue
        enriched_count += 1
        summary = " ".join(str(raw.get("summary") or "").lower().split())
        if len(summary) > 1_000 or not any(marker in summary for marker in markers):
            continue
        invalid.append(
            {
                "event_id": str(raw.get("event_id") or ""),
                "source_id": str(raw.get("source_id") or ""),
                "headline": str(raw.get("headline") or ""),
                "matched_quality_marker": next(
                    marker for marker in markers if marker in summary
                ),
            }
        )
    return {
        "status": "ok" if not invalid else "invalid_enriched_detail",
        "generated_utc": state.get("generated_utc"),
        "article_count": len(articles),
        "enriched_detail_count": enriched_count,
        "invalid_rows": invalid[:20],
        "invalid_row_count": len(invalid),
        "ok": not invalid,
    }


def current_source_provenance_integrity(path: Path) -> dict[str, Any]:
    """Require anything labeled direct to have verified source provenance."""
    state = read_json(path)
    articles = state.get("articles")
    if not isinstance(articles, list):
        return {
            "status": "missing_or_invalid",
            "article_count": 0,
            "direct_article_count": 0,
            "invalid_rows": [],
            "invalid_row_count": 0,
            "ok": False,
        }
    direct_count = 0
    invalid: list[dict[str, str]] = []
    for raw in articles:
        if not isinstance(raw, dict) or raw.get("source_direct") is not True:
            continue
        direct_count += 1
        if raw.get("source_verified") is True:
            continue
        invalid.append(
            {
                "event_id": str(raw.get("event_id") or ""),
                "source_id": str(raw.get("source_id") or ""),
                "headline": str(raw.get("headline") or ""),
            }
        )
    return {
        "status": "ok" if not invalid else "unverified_source_labeled_direct",
        "generated_utc": state.get("generated_utc"),
        "article_count": len(articles),
        "direct_article_count": direct_count,
        "invalid_rows": invalid[:20],
        "invalid_row_count": len(invalid),
        "ok": not invalid,
    }


def persistent_policy_state_integrity(path: Path) -> dict[str, Any]:
    """Ensure event clocks cannot masquerade as completed policy states."""

    state = read_json(path)
    currencies = state.get("currencies")
    if not isinstance(currencies, dict):
        return {
            "status": "missing_or_invalid",
            "currency_count": 0,
            "invalid_rows": [],
            "invalid_row_count": 0,
            "ok": False,
        }
    invalid: list[dict[str, str]] = []
    schedule_phrases = (
        "decision calendar",
        "policy calendar",
        "decision dates",
        "meeting dates",
        "schedule for policy",
        "schedule of policy",
        "schedule for monetary policy",
        "schedule of monetary policy",
    )
    for currency, raw in currencies.items():
        row = raw if isinstance(raw, dict) else {}
        source_id = str(row.get("source_id") or "").lower()
        text = " ".join(
            str(row.get(field) or "").lower()
            for field in ("headline", "summary", "policy_document_type")
        )
        reasons: list[str] = []
        if "policy_decision_calendar" in source_id or any(
            phrase in text for phrase in schedule_phrases
        ):
            reasons.append("schedule_used_as_policy_state")
        if not str(row.get("event_id") or ""):
            reasons.append("missing_event_id")
        if not str(row.get("known_utc") or ""):
            reasons.append("missing_known_utc")
        if row.get("policy_stance_bearing_eligible") is not True:
            reasons.append("not_stance_bearing_eligible")
        if reasons:
            invalid.append(
                {
                    "currency": str(currency),
                    "source_id": str(row.get("source_id") or ""),
                    "headline": str(row.get("headline") or ""),
                    "reasons": ",".join(reasons),
                }
            )
    schema_current = (
        state.get("schema_version")
        == "persistent_policy_state_v3_stance_bearing_only"
    )
    return {
        "status": "ok" if schema_current and currencies and not invalid else "invalid",
        "schema_version": state.get("schema_version"),
        "generated_utc": state.get("generated_utc"),
        "currency_count": len(currencies),
        "document_count": int(state.get("document_count") or 0),
        "excluded_policy_clock_count": int(
            state.get("excluded_policy_clock_count") or 0
        ),
        "excluded_non_stance_policy_count": int(
            state.get("excluded_non_stance_policy_count") or 0
        ),
        "invalid_rows": invalid[:20],
        "invalid_row_count": len(invalid),
        "ok": bool(schema_current and currencies and not invalid),
    }


def news_consumer_classification_bound(state: dict[str, Any]) -> bool:
    top_level = str(state.get("required_news_classification_version") or "")
    diagnostic = str(
        (state.get("diagnostics") or {}).get(
            "required_news_classification_version"
        )
        or ""
    )
    return bool(
        top_level == CURRENT_NEWS_CLASSIFICATION_VERSION
        and diagnostic == CURRENT_NEWS_CLASSIFICATION_VERSION
    )


def executable_opportunity_proof_is_fail_closed(
    state: dict[str, Any], *, market_state: str
) -> bool:
    """Accept active proof or the exact retired drain-only lifecycle.

    The executable-opportunity producer no longer issues forecasts. It is
    intentionally retained only to mature forecasts written before retirement.
    Its drain status is safe only while future production is disabled, pending
    outcomes remain preserved, and every execution/promotion surface is closed.
    """

    contract = state.get("contract") or {}
    runtime_mode = str(state.get("runtime_mode") or "").strip().lower()
    status = str(state.get("status") or "").strip().lower()
    common_safe = bool(
        state.get("research_only")
        and not state.get("execution_eligible")
        and not state.get("can_place_orders")
        and not state.get("can_promote")
        and not (state.get("artifact_failures") or [])
        and state.get("supported_execution_decision") == "no_trade"
    )
    if not common_safe:
        return False

    if runtime_mode in {"mature_only", "drain_only", "retired"}:
        return bool(
            status in {"draining_pending_outcomes", "drained_retired"}
            and contract.get("future_forecast_production") is False
            and contract.get("pending_forecasts_preserved_to_maturity") is True
        )

    allowed_active_statuses = (
        {"ok", "market_or_source_stale"}
        if market_state == "weekend_closed"
        else {"ok"}
    )
    return bool(
        status in allowed_active_statuses
        and runtime_mode == "produce_and_mature"
        and contract.get("future_forecast_production") is True
    )


def source_governance_collector_bound(state: dict[str, Any]) -> bool:
    """Validate collector identity for terminal and honest in-progress states.

    A governance rebuild can overlap the integrity audit. Its progress payload
    deliberately reports ``building_governance`` while rebinding the current
    collector contract. Build completion and collector identity are different
    facts; this check measures the latter and remains fail-closed on an
    operational capability or unknown state.
    """

    return (
        state.get("status") in {"ok", "building_governance"}
        and state.get("research_only") is True
        and state.get("can_place_orders") is False
        and state.get("real_money_routing") is False
        and str(state.get("news_collector_contract_id") or "")
        == NEWS_COLLECTOR_CONTRACT_ID
        and str(state.get("news_collector_cohort_id") or "")
        == NEWS_COLLECTOR_CONTRACT_ID
    )


def source_governance_fast_lane_adapter_integrity(
    state: dict[str, Any], *, database_path: Path
) -> dict[str, Any]:
    """Verify the fast-lane adapter is prospective, append-only, and inert."""

    adapter = state.get("official_fast_lane_source_governance_adapter") or {}
    activated = OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_ACTIVATED_UTC.isoformat()
    state_contract_ok = bool(
        state.get("status") in {"ok", "building_governance"}
        and state.get("research_only") is True
        and state.get("can_place_orders") is False
        and state.get("real_money_routing") is False
        and str(
            state.get("official_fast_lane_governance_adapter_contract_id") or ""
        )
        == OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID
        and str(
            state.get("official_fast_lane_governance_adapter_activated_utc") or ""
        )
        == activated
    )
    adapter_contract_ok = bool(
        adapter.get("status") == "ok"
        and adapter.get("adapter_contract_id")
        == OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID
        and adapter.get("adapter_activated_utc") == activated
        and adapter.get("upstream_collector_contract_id")
        == OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID
        and adapter.get("upstream_collector_cohort_id")
        == OFFICIAL_RELEASE_FAST_LANE_COHORT_ID
        and int(adapter.get("allowed_official_source_count") or 0) >= 21
        and int(adapter.get("active_source_lineage_count") or 0)
        == int(adapter.get("allowed_official_source_count") or 0)
    )
    inert_ok = bool(
        adapter.get("research_only") is True
        and adapter.get("execution_eligible") is False
        and adapter.get("can_place_orders") is False
        and adapter.get("can_authorize") is False
    )
    diagnostics: dict[str, Any] = {
        "database_integrity": "missing",
        "receipt_count": -1,
        "active_receipt_count": -1,
        "retained_prior_receipt_count": -1,
        "unknown_adapter_receipt_count": -1,
        "preactivation_receipts": -1,
        "backdated_effective_receipts": -1,
        "contract_or_inert_violations": -1,
        "invalid_material_kind_receipts": -1,
        "listing_clock_violations": -1,
        "later_material_clock_violations": -1,
        "cross_kind_hash_collisions": -1,
        "orphan_or_mismatched_source_events": -1,
        "append_only_triggers_present": False,
    }
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(
            f"file:{database_path.resolve().as_posix()}?mode=ro",
            uri=True,
            timeout=5.0,
        )
        diagnostics["database_integrity"] = str(
            connection.execute("PRAGMA quick_check").fetchone()[0]
        )
        required_columns = {
            "receipt_id", "observation_id", "source_id", "provider_event_id",
            "material_kind", "material_sha256", "source_event_id",
            "input_first_seen_utc", "effective_from_utc", "imported_utc",
            "adapter_contract_id", "adapter_activated_utc",
            "upstream_collector_contract_id", "upstream_collector_cohort_id",
            "upstream_source_contract_id", "upstream_source_cohort_id",
            "research_only", "execution_eligible", "can_authorize",
        }
        actual_columns = {
            str(row[1])
            for row in connection.execute(
                "PRAGMA table_info(official_fast_lane_governance_imports)"
            ).fetchall()
        }
        diagnostics["schema_ok"] = required_columns <= actual_columns
        if diagnostics["schema_ok"]:
            table = "official_fast_lane_governance_imports"
            diagnostics["receipt_count"] = int(
                connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            )
            diagnostics["active_receipt_count"] = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE adapter_contract_id=?",
                    (OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID,),
                ).fetchone()[0]
            )
            diagnostics["retained_prior_receipt_count"] = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE adapter_contract_id=?",
                    (OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_PRIOR_CONTRACT_ID,),
                ).fetchone()[0]
            )
            diagnostics["unknown_adapter_receipt_count"] = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {table} "
                    "WHERE adapter_contract_id NOT IN (?,?)",
                    (
                        OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID,
                        OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_PRIOR_CONTRACT_ID,
                    ),
                ).fetchone()[0]
            )
            diagnostics["preactivation_receipts"] = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {table} "
                    "WHERE (adapter_contract_id=? AND "
                    "julianday(input_first_seen_utc)<julianday(?)) OR "
                    "(adapter_contract_id=? AND "
                    "julianday(input_first_seen_utc)<julianday(?))",
                    (
                        OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID,
                        activated,
                        OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_PRIOR_CONTRACT_ID,
                        OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_PRIOR_ACTIVATED_UTC.isoformat(),
                    ),
                ).fetchone()[0]
            )
            diagnostics["backdated_effective_receipts"] = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {table} "
                    "WHERE julianday(effective_from_utc) "
                    "< julianday(input_first_seen_utc)"
                ).fetchone()[0]
            )
            diagnostics["contract_or_inert_violations"] = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE "
                    "NOT ((adapter_contract_id=? AND adapter_activated_utc=? "
                    "AND upstream_collector_contract_id=? "
                    "AND upstream_collector_cohort_id=?) OR "
                    "(adapter_contract_id=? AND adapter_activated_utc=? "
                    "AND upstream_collector_contract_id=? "
                    "AND upstream_collector_cohort_id=?)) OR "
                    "upstream_source_contract_id='' OR upstream_source_cohort_id='' OR "
                    "research_only<>1 OR execution_eligible<>0 OR can_authorize<>0",
                    (
                        OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID,
                        activated,
                        OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID,
                        OFFICIAL_RELEASE_FAST_LANE_COHORT_ID,
                        OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_PRIOR_CONTRACT_ID,
                        OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_PRIOR_ACTIVATED_UTC.isoformat(),
                        OFFICIAL_RELEASE_FAST_LANE_PRIOR_CONTRACT_ID,
                        OFFICIAL_RELEASE_FAST_LANE_PRIOR_COHORT_ID,
                    ),
                ).fetchone()[0]
            )
            diagnostics["invalid_material_kind_receipts"] = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE material_kind NOT IN "
                    "('listing_observation','official_body_detail',"
                    "'official_pdf_attachment')"
                ).fetchone()[0]
            )
            diagnostics["listing_clock_violations"] = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {table} "
                    "WHERE material_kind='listing_observation' AND "
                    "ABS(julianday(effective_from_utc)-"
                    "julianday(input_first_seen_utc))>0.000000001"
                ).fetchone()[0]
            )
            diagnostics["later_material_clock_violations"] = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {table} "
                    "WHERE material_kind<>'listing_observation' AND "
                    "julianday(effective_from_utc)<julianday(input_first_seen_utc)"
                ).fetchone()[0]
            )
            diagnostics["cross_kind_hash_collisions"] = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM (SELECT observation_id,material_sha256 "
                    f"FROM {table} GROUP BY observation_id,material_sha256 "
                    "HAVING COUNT(DISTINCT material_kind)>1)"
                ).fetchone()[0]
            )
            diagnostics["orphan_or_mismatched_source_events"] = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {table} AS receipt "
                    "LEFT JOIN source_events AS event "
                    "ON event.source_event_id=receipt.source_event_id "
                    "WHERE event.source_event_id IS NULL "
                    "OR event.source_id<>receipt.source_id "
                    "OR event.provider_event_id<>receipt.provider_event_id "
                    "OR event.raw_payload_sha256<>receipt.material_sha256 "
                    "OR julianday(event.effective_from_utc)<>"
                    "julianday(receipt.effective_from_utc) "
                    "OR json_extract(event.payload_json,"
                    "'$.raw_payload.adapter_contract_id')<>"
                    "receipt.adapter_contract_id "
                    "OR json_extract(event.payload_json,"
                    "'$.raw_payload.research_only')<>1 "
                    "OR json_extract(event.payload_json,"
                    "'$.raw_payload.execution_eligible')<>0 "
                    "OR json_extract(event.payload_json,"
                    "'$.raw_payload.can_authorize')<>0",
                ).fetchone()[0]
            )
            triggers = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='trigger'"
                ).fetchall()
            }
            diagnostics["append_only_triggers_present"] = {
                "fast_lane_imports_no_update", "fast_lane_imports_no_delete"
            } <= triggers
    except (OSError, sqlite3.Error, TypeError, ValueError):
        diagnostics["database_integrity"] = "error"
        diagnostics["schema_ok"] = False
    finally:
        if connection is not None:
            connection.close()

    database_ok = bool(
        diagnostics.get("database_integrity") == "ok"
        and diagnostics.get("schema_ok") is True
        and diagnostics.get("receipt_count", -1) >= 0
        and all(
            diagnostics.get(key) == 0
            for key in (
                "preactivation_receipts", "backdated_effective_receipts",
                "unknown_adapter_receipt_count",
                "contract_or_inert_violations", "invalid_material_kind_receipts",
                "listing_clock_violations", "later_material_clock_violations",
                "cross_kind_hash_collisions", "orphan_or_mismatched_source_events",
            )
        )
        and diagnostics.get("append_only_triggers_present") is True
    )
    return {
        "ok": bool(state_contract_ok and adapter_contract_ok and inert_ok and database_ok),
        "state_contract_ok": state_contract_ok,
        "adapter_contract_ok": adapter_contract_ok,
        "inert_ok": inert_ok,
        **diagnostics,
    }


def synchronized_quote_audit(quotes: dict[str, Any], *, cutoff_epoch: float) -> dict[str, Any]:
    rows: list[tuple[str, float, float, float]] = []
    future: list[str] = []
    invalid: list[str] = []
    for instrument, raw in quotes.items():
        if not isinstance(raw, dict):
            invalid.append(str(instrument)); continue
        epoch = parse_epoch(raw.get("time"))
        try:
            bid, ask = float(raw.get("bid")), float(raw.get("ask"))
        except (TypeError, ValueError):
            invalid.append(str(instrument)); continue
        if epoch is None or bid <= 0.0 or ask < bid:
            invalid.append(str(instrument)); continue
        if epoch > cutoff_epoch + 60.0:
            future.append(str(instrument)); continue
        rows.append((str(instrument), epoch, bid, ask))
    rows.sort(key=lambda row: row[0])
    canonical = json.dumps(rows, separators=(",", ":"), ensure_ascii=True)
    epochs = [row[1] for row in rows]
    return {
        "valid_instruments": len(rows),
        "invalid_instruments": sorted(invalid),
        "future_instruments": sorted(future),
        "maximum_quote_age_sec": max((cutoff_epoch-e for e in epochs), default=None),
        "cross_pair_event_time_dispersion_sec": max(epochs)-min(epochs) if epochs else None,
        "snapshot_sha256": "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        "ingestion_order_invariant": True,
    }


def cross_pair_snapshot_routable(
    quote_audit: dict[str, Any],
    quote_state: dict[str, Any],
    *,
    cutoff_epoch: float,
    market_state: str,
    maximum_snapshot_age_sec: float = 15.0,
) -> bool:
    """Validate the published cache, not each instrument's last-change time.

    OANDA streams a price row when that instrument changes.  A thin pair can
    therefore have an older provider event timestamp while still belonging to
    a newly published, current 68-pair cache from a healthy stream.  Requiring
    every last-change timestamp to be within a few seconds creates a permanent
    false failure and confuses event-time dispersion with transport freshness.
    The dispersion remains in ``synchronized_quote_audit`` as a diagnostic;
    routability is governed by a fresh snapshot publication, current coverage,
    valid prices, and the absence of research-only retained quotes.
    """
    if market_state == "weekend_closed":
        return True
    coverage = quote_state.get("coverage") or {}
    snapshot_epoch = parse_epoch(quote_state.get("generated_utc"))
    snapshot_age = (
        None if snapshot_epoch is None else max(0.0, cutoff_epoch - snapshot_epoch)
    )
    return bool(
        quote_audit.get("valid_instruments") == 68
        and int(coverage.get("current_quote_count") or 0) == 68
        and int(coverage.get("retained_last_known_count") or 0) == 0
        and not (quote_audit.get("invalid_instruments") or [])
        and not (quote_audit.get("future_instruments") or [])
        and quote_audit.get("ingestion_order_invariant") is True
        and snapshot_age is not None
        and snapshot_age <= maximum_snapshot_age_sec
    )


def collector_cohort_bound(state: dict[str, Any]) -> bool:
    """Require a collector-defined cohort when the runtime promises immutability."""
    cohort = state.get("collection_cohort") or state.get("cohort") or {}
    cohort_id = str(state.get("cohort_id") or cohort.get("cohort_id") or "")
    definition_sha = str(cohort.get("cohort_definition_sha256") or "")
    collector_sha = str(cohort.get("collector_sha256") or "")
    requires_new = bool(
        cohort.get("material_change_requires_new_cohort")
        or cohort.get("material_collector_change_requires_new_cohort")
    )
    if not requires_new:
        return False
    return bool(
        cohort_id and definition_sha and collector_sha
        and cohort_id.endswith(definition_sha[:16])
    )


def latest_sealable_clock(
    cutoff_epoch: float,
    *,
    bucket_minutes: int,
    grace_seconds: int,
) -> datetime | None:
    """Latest bucket whose close plus contract grace is known at cutoff."""

    if bucket_minutes <= 0 or grace_seconds < 0:
        return None
    width = bucket_minutes * 60
    adjusted = int(cutoff_epoch) - grace_seconds
    return datetime.fromtimestamp((adjusted // width) * width, timezone.utc)


def continuous_narrative_meter_integrity(
    state: dict[str, Any],
    *,
    database_path: Path,
    cutoff_epoch: float,
    maximum_age_sec: float = 240.0,
) -> dict[str, Any]:
    """Require the V12 meter to expose only sealed immutable proof rows."""

    generated_epoch = parse_epoch(state.get("generated_utc"))
    sealed_epoch = parse_epoch(state.get("sealed_clock_utc"))
    partial = state.get("partial_live") or {}
    partial_epoch = parse_epoch(partial.get("bucket_end_utc"))
    age = None if generated_epoch is None else cutoff_epoch - generated_epoch
    database_integrity = "missing"
    contract_row: tuple[Any, ...] | None = None
    latest_seal: str | None = None
    seal_count = -1
    incomplete_seals = -1
    late_arrival_count = -1
    seal_gap_count = -1
    premature_seal_count = -1
    expected_sealed_clock: datetime | None = None
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(database_path, timeout=5.0)
        database_integrity = str(
            connection.execute("PRAGMA quick_check").fetchone()[0]
        )
        contract_row = connection.execute(
            "SELECT meter_contract_id,bucket_minutes,seal_grace_seconds,research_only,"
            "execution_eligible FROM meter_contract_registry"
        ).fetchone()
        bucket_minutes = int(contract_row[1]) if contract_row else 0
        grace_seconds = int(contract_row[2]) if contract_row else -1
        expected_sealed_clock = latest_sealable_clock(
            cutoff_epoch,
            bucket_minutes=bucket_minutes,
            grace_seconds=grace_seconds,
        )
        expected_text = (
            expected_sealed_clock.isoformat() if expected_sealed_clock else ""
        )
        cutoff_text = datetime.fromtimestamp(
            cutoff_epoch, timezone.utc
        ).isoformat()
        latest_seal, seal_count = connection.execute(
            "SELECT MAX(clock_utc),COUNT(*) FROM bucket_seals "
            "WHERE meter_contract_id=? "
            "AND julianday(clock_utc)<=julianday(?) "
            "AND julianday(sealed_at_utc)<=julianday(?)",
            (
                CONTINUOUS_NARRATIVE_METER_CONTRACT,
                expected_text,
                cutoff_text,
            ),
        ).fetchone()
        premature_seal_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM bucket_seals WHERE meter_contract_id=? "
                "AND julianday(clock_utc)>julianday(?) "
                "AND julianday(sealed_at_utc)<=julianday(?)",
                (
                    CONTINUOUS_NARRATIVE_METER_CONTRACT,
                    expected_text,
                    cutoff_text,
                ),
            ).fetchone()[0]
        )
        incomplete_seals = int(
            connection.execute(
                "SELECT COUNT(*) FROM bucket_seals WHERE meter_contract_id=? "
                "AND currency_row_count<>21",
                (CONTINUOUS_NARRATIVE_METER_CONTRACT,),
            ).fetchone()[0]
        )
        late_arrival_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM seal_integrity_events "
                "WHERE meter_contract_id=?",
                (CONTINUOUS_NARRATIVE_METER_CONTRACT,),
            ).fetchone()[0]
        )
        seal_gap_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM seal_gap_events "
                "WHERE meter_contract_id=?",
                (CONTINUOUS_NARRATIVE_METER_CONTRACT,),
            ).fetchone()[0]
        )
    except (OSError, sqlite3.Error, TypeError, ValueError):
        database_integrity = "error"
    finally:
        if connection is not None:
            connection.close()

    persistence = state.get("persistence") or {}
    contract_ok = bool(
        state.get("schema_version") == "continuous_narrative_meter_latest_v12"
        and state.get("meter_contract_id")
        == CONTINUOUS_NARRATIVE_METER_CONTRACT
        and int(state.get("seal_grace_seconds") or -1)
        == CONTINUOUS_NARRATIVE_SEAL_GRACE_SECONDS
        and int(state.get("bucket_minutes") or -1)
        == CONTINUOUS_NARRATIVE_BUCKET_MINUTES
        and contract_row
        and contract_row[0] == CONTINUOUS_NARRATIVE_METER_CONTRACT
        and int(contract_row[1]) == CONTINUOUS_NARRATIVE_BUCKET_MINUTES
        and int(contract_row[2]) == CONTINUOUS_NARRATIVE_SEAL_GRACE_SECONDS
        and tuple(contract_row[3:]) == (1, 0)
    )
    inert_ok = bool(
        state.get("research_only") is True
        and state.get("execution_eligible") is False
        and state.get("can_place_orders") is False
        and int(state.get("orders_placed") or 0) == 0
        and partial.get("state_kind") == "unsealed_provisional_live_view"
        and partial.get("complete") is False
        and partial.get("persisted") is False
        and partial.get("proof_eligible") is False
        and partial.get("research_only") is True
        and partial.get("execution_eligible") is False
        and partial.get("can_place_orders") is False
    )
    clock_ok = bool(
        generated_epoch is not None
        and sealed_epoch is not None
        and partial_epoch is not None
        and expected_sealed_clock is not None
        and sealed_epoch == expected_sealed_clock.timestamp()
        and partial_epoch > sealed_epoch
        and state.get("clock_utc") == state.get("sealed_clock_utc")
        and latest_seal == state.get("sealed_clock_utc")
        and premature_seal_count == 0
    )
    counts_ok = bool(
        seal_count >= 1
        and incomplete_seals == 0
        and late_arrival_count >= 0
        and int(persistence.get("total_late_arrival_incidents") or 0)
        == late_arrival_count
        and int(persistence.get("total_seal_gap_incidents") or 0)
        == seal_gap_count
        and state.get("integrity_status")
        == (
            "late_arrival_and_seal_gap_incident"
            if late_arrival_count and seal_gap_count
            else "late_arrival_incident"
            if late_arrival_count
            else "seal_gap_incident"
            if seal_gap_count
            else "ok"
        )
    )
    freshness_ok = bool(age is not None and -5.0 <= age <= maximum_age_sec)
    return {
        "ok": bool(
            contract_ok
            and inert_ok
            and clock_ok
            and counts_ok
            and freshness_ok
            and database_integrity == "ok"
        ),
        "contract_ok": contract_ok,
        "inert_ok": inert_ok,
        "clock_ok": clock_ok,
        "counts_ok": counts_ok,
        "freshness_ok": freshness_ok,
        "age_sec": age,
        "database_integrity": database_integrity,
        "sealed_clock_utc": state.get("sealed_clock_utc"),
        "expected_sealed_clock_utc": (
            expected_sealed_clock.isoformat() if expected_sealed_clock else None
        ),
        "partial_bucket_end_utc": partial.get("bucket_end_utc"),
        "bucket_seal_count": seal_count,
        "incomplete_seal_count": incomplete_seals,
        "late_arrival_incident_count": late_arrival_count,
        "seal_gap_incident_count": seal_gap_count,
        "premature_seal_count_as_of_cutoff": premature_seal_count,
    }


def run(
    *, output: Path = DEFAULT_OUTPUT, history: Path = DEFAULT_HISTORY,
    report: Path = DEFAULT_REPORT,
) -> dict[str, Any]:
    previous = read_json(output)
    quotes_state = read_json(STATE / "practice_007_market_quotes_v1.json")
    feature_state = read_json(STATE / "live_model_feature_snapshot_v1.json")
    source = read_json(STATE / "source_governance_v1.json")
    source_counts = source.get("counts") or {}
    raw_source_events = int(source_counts.get("events") or 0)
    quarantined_source_events = int(source_counts.get("quarantined_events") or 0)
    causal_source_events = int(
        source_counts.get("causal_events")
        if source_counts.get("causal_events") is not None
        else raw_source_events
    )
    lifecycle = read_json(STATE / "evidence_lifecycle_v1.json")
    allocator = read_json(STATE / "allocator_proof_v1.json")
    accounting = read_json(STATE / "governed_practice_accounting_v1.json")
    verifier = read_json(STATE / "independent_evidence_verifier_v1.json")
    executor = read_json(STATE / "practice_007_top_executor_heartbeat_v1.json")
    quote_worker = read_json(STATE / "practice_007_quote_stream_heartbeat_v1.json")
    clock = read_json(STATE / "clock_integrity_v1.json")
    storage = read_json(STATE / "storage_headroom_v1.json")
    shadow_archive = read_json(STATE / "shadow_archive_integrity_v1.json")
    shadow_compactor = read_json(STATE / "shadow_outcome_compactor_v1.json")
    log_archive = read_json(STATE / "verified_log_archiver_v1.json")
    proof = read_json(STATE / "proof_shadow_predictors_v1.json")
    news_watchlist = read_json(STATE / "news_technical_watchlist_v1.json")
    news_challenger = read_json(
        STATE / "practice_006_news_challenger_heartbeat_v1.json"
    )
    improvement_control = read_json(STATE / "improvement_control_engine_v1.json")
    direct_source_response = read_json(STATE / "direct_source_response_v1.json")
    move_first_news_audit = read_json(CURRENT_MOVE_FIRST_NEWS_AUDIT)
    major_move_gap_census = read_json(CURRENT_MAJOR_MOVE_GAP_CENSUS)
    live_move_news_snapshot = read_json(CURRENT_LIVE_MOVE_NEWS_SNAPSHOT)
    live_move_news_outcomes = read_json(CURRENT_LIVE_MOVE_NEWS_OUTCOMES)
    continuous_narrative_meter = read_json(
        CURRENT_CONTINUOUS_NARRATIVE_METER
    )
    news_outcome_improvement_audit = read_json(
        CURRENT_NEWS_OUTCOME_IMPROVEMENT_AUDIT
    )
    live_move_persistent_context = read_json(
        CURRENT_LIVE_MOVE_PERSISTENT_CONTEXT
    )
    official_release_fast_lane = read_json(CURRENT_OFFICIAL_RELEASE_FAST_LANE)
    official_release_fast_lane_heartbeat = read_json(
        CURRENT_OFFICIAL_RELEASE_FAST_LANE_HEARTBEAT
    )
    official_release_fast_mapping = read_json(
        CURRENT_OFFICIAL_RELEASE_FAST_MAPPING
    )
    official_release_fast_mapping_heartbeat = read_json(
        CURRENT_OFFICIAL_RELEASE_FAST_MAPPING_HEARTBEAT
    )
    official_release_fast_response = read_json(
        CURRENT_OFFICIAL_RELEASE_FAST_RESPONSE_WATCH
    )
    official_release_fast_response_heartbeat = read_json(
        CURRENT_OFFICIAL_RELEASE_FAST_RESPONSE_HEARTBEAT
    )
    executable_opportunity = read_json(STATE / "executable_opportunity_prospective_v1.json")
    gdelt_attention = read_json(STATE / "gdelt_attention_magnitude_prospective_v1.json")
    treasury_yields = read_json(STATE / "us_treasury_yield_prospective_v1.json")
    alfred_vintages = read_json(STATE / "alfred_vintage_prospective_v1.json")
    signal_availability = read_json(
        STATE / "practice_007_signal_feed_availability_v1.json"
    )
    source_gaps = read_json(ROOT / "config" / "forex_source_gap_register_v1.json")
    source_config = read_json(ROOT / "config" / "news_sources_v1.json")
    episode_ontology = read_json(ROOT / "config" / "market_episode_ontology_v1.json")
    # Heartbeats are live atomic files.  Take the audit cutoff only after all
    # input snapshots have been captured so a worker update during this read
    # pass is not mislabeled as a future timestamp.
    generated = datetime.now(timezone.utc)
    market = forex_market_state(generated)
    numeric_currency_integrity=structured_numeric_currency_integrity(
        LOCAL_NEWS_DATABASE,
        source_state_path=CURRENT_NEWS_COLLECTOR_STATE,
    )
    detail_quality_integrity = official_detail_quality_integrity(
        CURRENT_CONTEXT_ARTICLES
    )
    source_provenance_integrity = current_source_provenance_integrity(
        CURRENT_CONTEXT_ARTICLES
    )
    policy_state_integrity = persistent_policy_state_integrity(
        CURRENT_PERSISTENT_POLICY_STATE
    )
    official_source_depth_status = official_source_depth_integrity(
        read_json(CURRENT_OFFICIAL_CURRENCY_SOURCE_DEPTH),
        cutoff_epoch=generated.timestamp(),
    )
    quote_audit = synchronized_quote_audit(
        quotes_state.get("quotes") or {}, cutoff_epoch=generated.timestamp()
    )
    snapshot_epoch = parse_epoch(quotes_state.get("generated_utc"))
    quote_audit["snapshot_generated_utc"] = quotes_state.get("generated_utc")
    quote_audit["snapshot_age_sec"] = (
        None
        if snapshot_epoch is None
        else max(0.0, generated.timestamp() - snapshot_epoch)
    )
    quote_coverage = quotes_state.get("coverage") or {}
    feature_contract = feature_state.get("contract") or {}
    intrahour = feature_contract.get("intrahour_forecast") or {}
    lifecycle_counts = (lifecycle.get("lifecycle") or {}).get("states") or {}
    account = (accounting.get("accounting") or {}).get("account_operational_continuity") or {}
    executor_details = executor.get("details") or {}
    configured_source_ids = [
        str(row.get("source_id") or "")
        for row in source_config.get("sources") or []
        if isinstance(row, dict) and row.get("source_id")
    ]
    configured_source_count = len(configured_source_ids) or EXPECTED_CONFIGURED_NEWS_SOURCES
    source_config_ids_unique = len(configured_source_ids) == len(set(configured_source_ids))
    expected = {
        "instruments": 68,
        "completed_candle_timeframes": ["M1", "M30", "H1", "H4"],
        "forecast_horizons_sec": [60,120,180,300,600,900,1800,3600],
        "configured_news_sources": configured_source_count,
        "proof_families": 4,
    }
    actual = {
        "quoted_instruments": int(quotes_state.get("quote_count") or 0),
        "current_quoted_instruments": int(
            quote_coverage.get("current_quote_count")
            if quote_coverage.get("current_quote_count") is not None
            else quotes_state.get("quote_count") or 0
        ),
        "retained_last_known_instruments": int(
            quote_coverage.get("retained_last_known_count") or 0
        ),
        "live_feature_instruments": int(feature_state.get("instrument_count") or 0),
        "completed_candle_timeframes": intrahour.get("context_timeframes") or [],
        "forecast_horizons_sec": intrahour.get("forecast_horizons_sec") or [],
        "configured_news_sources": int(
            source.get("configured_news_source_count")
            if source.get("configured_news_source_count") is not None
            else source.get("configured_source_count") or 0
        ),
        "runtime_observed_news_sources": int(source.get("runtime_observed_source_count") or 0),
        "operational_news_sources": int(source.get("operational_source_count") or 0),
        "proof_families": len(proof.get("families") or []),
    }
    feature_coverage = live_feature_coverage_diagnostic(
        feature_state, expected["instruments"]
    )
    live_move_forward_integrity = live_move_forward_proof_integrity(
        live_move_news_snapshot,
        live_move_news_outcomes,
        cutoff_epoch=generated.timestamp(),
    )
    news_outcome_improvement_status = news_outcome_improvement_integrity(
        news_outcome_improvement_audit,
        database_path=NEWS_OUTCOME_IMPROVEMENT_DATABASE,
        cutoff_epoch=generated.timestamp(),
    )
    live_move_persistent_context_status = live_move_persistent_context_integrity(
        live_move_persistent_context,
        cutoff_epoch=generated.timestamp(),
    )
    official_release_fast_lane_status = official_release_fast_lane_integrity(
        official_release_fast_lane,
        official_release_fast_lane_heartbeat,
        database_path=OFFICIAL_RELEASE_FAST_LANE_DATABASE,
        cutoff_epoch=generated.timestamp(),
    )
    source_governance_fast_lane_adapter_status = (
        source_governance_fast_lane_adapter_integrity(
            source,
            database_path=SOURCE_GOVERNANCE_DATABASE,
        )
    )
    official_release_fast_mapping_status = official_release_fast_mapping_integrity(
        official_release_fast_mapping,
        official_release_fast_mapping_heartbeat,
        database_path=OFFICIAL_RELEASE_FAST_MAPPING_DATABASE,
        cutoff_epoch=generated.timestamp(),
    )
    official_release_fast_response_status = official_release_fast_response_integrity(
        official_release_fast_response,
        official_release_fast_response_heartbeat,
        database_path=OFFICIAL_RELEASE_FAST_RESPONSE_DATABASE,
        cutoff_epoch=generated.timestamp(),
    )
    continuous_narrative_meter_status = continuous_narrative_meter_integrity(
        continuous_narrative_meter,
        database_path=CONTINUOUS_NARRATIVE_METER_DATABASE,
        cutoff_epoch=generated.timestamp(),
    )
    exclusions = []
    if actual["live_feature_instruments"] == 0 and market == "weekend_closed":
        exclusions.append({"surface":"live_feature_instruments","reason":"weekend_market_closed_expected"})
    elif actual["live_feature_instruments"] != expected["instruments"]:
        exclusions.append(
            {
                "surface": "live_feature_instruments",
                "reason": (
                    "freshness_guard_excluded_instruments"
                    if feature_coverage["status"] == "fail_closed_quote_exclusions"
                    else "unexpected_live_feature_coverage"
                ),
                "instrument_count": len(feature_coverage["exclusions"]),
                "instruments": [
                    row["instrument"] for row in feature_coverage["exclusions"]
                ],
            }
        )
    if market == "weekend_closed" and (
        quote_audit["cross_pair_event_time_dispersion_sec"] or 0.0
    ) > 5.0:
        exclusions.append(
            {
                "surface": "cross_pair_synchronization",
                "reason": "weekend_close_quotes_retained_for_diagnostics_not_live_features",
            }
        )
    if actual["retained_last_known_instruments"]:
        exclusions.append(
            {
                "surface": "current_quote_coverage",
                "reason": "earlier_closing_instruments_retained_for_research_only",
                "instrument_count": actual["retained_last_known_instruments"],
                "instruments": quote_coverage.get("retained_last_known_instruments") or [],
            }
        )
    checks = {
        "all_68_quotes_retained": actual["quoted_instruments"] == 68 and quote_audit["valid_instruments"] == 68,
        "candle_contract_matches": actual["completed_candle_timeframes"] == expected["completed_candle_timeframes"],
        "horizon_contract_matches": actual["forecast_horizons_sec"] == expected["forecast_horizons_sec"],
        "source_configuration_complete": (
            source_config_ids_unique
            and actual["configured_news_sources"] == expected["configured_news_sources"]
        ),
        "source_quarantine_reconciled": (
            causal_source_events
            == raw_source_events - quarantined_source_events
        ),
        "structured_numeric_sources_are_single_currency": bool(
            numeric_currency_integrity.get("ok")
        ),
        "official_enriched_details_are_content_not_error_shells": bool(
            detail_quality_integrity.get("ok")
        ),
        "direct_sources_have_verified_provenance": bool(
            source_provenance_integrity.get("ok")
        ),
        "persistent_policy_state_uses_completed_documents": bool(
            policy_state_integrity.get("ok")
        ),
        "official_source_depth_complete_current_and_zero_weight": bool(
            official_source_depth_status.get("ok")
        ),
        "source_governance_uses_current_news_collector_contract": (
            source_governance_collector_bound(source)
        ),
        "source_governance_fast_lane_adapter_prospective_and_inert": bool(
            source_governance_fast_lane_adapter_status.get("ok")
        ),
        "move_first_news_mapping_audit_current": move_first_news_audit_is_current(
            move_first_news_audit,
            major_move_gap_census,
            cutoff_epoch=generated.timestamp(),
        ),
        "live_move_news_forward_proof_contract_current": bool(
            live_move_forward_integrity.get("ok")
        ),
        "continuous_narrative_meter_sealed_current_and_inert": bool(
            continuous_narrative_meter_status.get("ok")
        ),
        "canonical_news_outcome_record_valid_and_inert": bool(
            news_outcome_improvement_status.get("ok")
        ),
        "live_move_persistent_context_current_and_inert": bool(
            live_move_persistent_context_status.get("ok")
        ),
        "official_release_fast_lane_current_and_inert": bool(
            official_release_fast_lane_status.get("ok")
        ),
        "official_release_fast_mapping_current_and_inert": bool(
            official_release_fast_mapping_status.get("ok")
        ),
        "official_release_fast_response_watch_current_and_inert": bool(
            official_release_fast_response_status.get("ok")
        ),
        "four_frozen_proof_families_running": actual["proof_families"] == 4,
        "independent_verifier_matches": independent_verifier_operationally_safe(
            verifier,
            lifecycle_counts,
            cutoff_epoch=generated.timestamp(),
        ),
        "no_confirmed_candidates": int(lifecycle_counts.get("confirmed_candidate", 0)) == 0,
        "allocator_not_executing": not bool((allocator.get("cohort") or {}).get("practice_execution")),
        "practice_account_position_scope_safe": (
            bool(account.get("account_state_current"))
            and bool(account.get("flatness_known"))
            and int(account.get("pending_order_count")) == 0
            and int(account.get("open_trade_count") or 0) == 0
        ),
        "new_entry_fail_closed": not bool(executor_details.get("new_entry_authorized")),
        "real_money_disabled": not bool(executor_details.get("real_money_routing")),
        "executor_healthy": executor.get("status") == "running" and int(executor_details.get("instrument_count") or 0) == 68,
        "quote_crosscheck_healthy": quote_worker.get("status") == "running",
        "signal_availability_market_state_consistent": (
            availability_market_state_consistent(signal_availability, market)
        ),
        "cross_pair_snapshot_routable": cross_pair_snapshot_routable(
            quote_audit,
            quotes_state,
            cutoff_epoch=generated.timestamp(),
            market_state=market,
        ),
        "clock_explicitly_classified": clock.get("status") in {"ok", "mitigated"},
        "episode_ontology_predefined": episode_ontology.get("contract") == "predefined_without_reference_to_strategy_profit",
        "storage_headroom_safe": storage.get("status") == "ok",
        "verified_log_archiver_healthy": log_archive.get("status") == "ok",
        "shadow_archive_current_manifests_healthy": (
            shadow_archive.get("status") in {
                "ok", "ok_with_legacy_unmanifested", "degraded_historical_detail_gap"
            }
            and not (shadow_archive.get("failures") or [])
            and int(shadow_archive.get("readable_parquet_parts") or 0)
                == int(shadow_archive.get("parquet_parts") or -1)
            and int(shadow_archive.get("validated_manifest_parts") or 0)
                == int(shadow_archive.get("manifest_parts") or -1)
        ),
        "shadow_archive_historical_detail_gap_governed": (
            int(
                (shadow_archive.get("detail_reconciliation") or {}).get(
                    "historical_missing_detail_rows",
                    max(
                        0,
                        int(
                            (shadow_archive.get("detail_reconciliation") or {}).get(
                                "detail_gap_vs_rollup"
                            )
                            or 0
                        ),
                    ),
                )
                or 0
            ) == 0
            or historical_archive_gap_digest_preserved(shadow_archive)
        ),
        "shadow_archive_historical_gap_digest_preserved": (
            historical_archive_gap_digest_preserved(shadow_archive)
        ),
        "shadow_archive_delete_scope_safe": (
            shadow_compactor.get("archive_delete_contract")
            == "validated_parquet_same_observed_date_row_id_range_and_cutoff"
        ),
        "news_technical_watchlist_shadow_only": (
            news_watchlist.get("status") in {"ok", "blocked_clock_integrity"}
            and bool(news_watchlist.get("research_only"))
            and not bool(news_watchlist.get("execution_eligible"))
            and not bool(news_watchlist.get("can_place_orders"))
            and not bool(news_watchlist.get("can_promote"))
        ),
        "news_challenger_isolated_and_bounded": (
            not news_challenger
            or (
                news_challenger.get("status") in {
                    "running", "watchlist_stale", "daily_fill_cap",
                    "session_loss_limit"
                }
                and bool(
                    (news_challenger.get("policy") or {}).get(
                        "research_only_challenger"
                    )
                )
                and not bool(
                    (news_challenger.get("policy") or {}).get(
                        "real_money_route"
                    )
                )
                and int(
                    (news_challenger.get("policy") or {}).get("fixed_units")
                    or 0
                ) == 100
                and int(
                    (news_challenger.get("policy") or {}).get(
                        "max_total_open_positions"
                    )
                    or 0
                ) == 8
                and str(news_challenger.get("account_suffix") or "") == "-006"
                and int(
                    (news_challenger.get("account") or {}).get(
                        "open_trade_count"
                    )
                    or 0
                ) <= 8
                and int(
                    (news_challenger.get("account") or {}).get(
                        "owned_open_trade_count"
                    )
                    or 0
                )
                == int(
                    (news_challenger.get("account") or {}).get(
                        "open_trade_count"
                    )
                    or 0
                )
                and bool(
                    (news_challenger.get("policy") or {}).get(
                        "allow_uncorroborated_secondary"
                    )
                )
                and bool(
                    (news_challenger.get("policy") or {}).get(
                        "core_governed_executor_unchanged"
                    )
                )
            )
        ),
        "news_watchlist_uses_current_classification_contract": (
            news_consumer_classification_bound(news_watchlist)
        ),
        "improvement_control_fail_closed": (
            improvement_control.get("status") == "ok"
            and bool(improvement_control.get("research_only"))
            and not bool(improvement_control.get("execution_eligible"))
            and not bool(improvement_control.get("can_place_orders"))
            and not bool(improvement_control.get("can_promote"))
        ),
        "direct_source_response_shadow_only": (
            direct_source_response.get("status") == "ok"
            and bool(direct_source_response.get("research_only"))
            and not bool(direct_source_response.get("execution_eligible"))
            and not bool(direct_source_response.get("can_place_orders"))
            and not bool(direct_source_response.get("can_promote"))
        ),
        "executable_opportunity_proof_fail_closed": (
            executable_opportunity_proof_is_fail_closed(
                executable_opportunity,
                market_state=market,
            )
        ),
        "gdelt_attention_magnitude_proof_fail_closed": (
            gdelt_attention.get("status") in (
                {"ok", "market_or_quote_stale"}
                if market == "weekend_closed" else {"ok"}
            )
            and bool(gdelt_attention.get("research_only"))
            and not bool(gdelt_attention.get("execution_eligible"))
            and not bool(gdelt_attention.get("can_place_orders"))
            and not bool(gdelt_attention.get("can_promote"))
            and gdelt_attention.get("direction_policy") == "abstain"
            and gdelt_attention.get("supported_execution_decision") == "no_trade"
        ),
        "us_treasury_yield_source_fail_closed": (
            treasury_yields.get("status") == "ok"
            and bool(treasury_yields.get("research_only"))
            and not bool(treasury_yields.get("execution_eligible"))
            and not bool(treasury_yields.get("can_place_orders"))
            and not bool(treasury_yields.get("can_promote"))
            and treasury_yields.get("direction_policy") == "abstain"
            and treasury_yields.get("database_integrity") == "ok"
            and treasury_yields.get("supported_execution_decision") == "no_trade"
            and int((treasury_yields.get("totals") or {}).get("prospective_eligible_rows") or 0) >= 0
        ),
        "alfred_vintage_source_fail_closed": (
            alfred_vintages.get("status") in {"blocked_missing_fred_api_key", "ok", "partial_error"}
            and bool(alfred_vintages.get("research_only"))
            and not bool(alfred_vintages.get("execution_eligible"))
            and not bool(alfred_vintages.get("can_place_orders"))
            and not bool(alfred_vintages.get("can_promote"))
            and alfred_vintages.get("supported_execution_decision") == "no_trade"
            and (
                alfred_vintages.get("status") == "blocked_missing_fred_api_key"
                or alfred_vintages.get("database_integrity") == "ok"
            )
        ),
        "prospective_observation_clocks_trusted": all(
            bool((state.get("observation_clock") or {}).get("trusted_for_prospective_evidence"))
            for state in (
                news_watchlist, direct_source_response,
                executable_opportunity, gdelt_attention,
            )
        ),
        "prospective_collector_cohorts_code_bound": all(
            collector_cohort_bound(state)
            for state in (
                executable_opportunity, gdelt_attention,
                treasury_yields, alfred_vintages,
            )
        ),
    }
    failures = sorted(name for name, passed in checks.items() if not passed)
    drift = {
        "previous_generated_utc": previous.get("generated_utc"),
        "governed_hypothesis_delta": int((lifecycle.get("lifecycle") or {}).get("hypothesis_count") or 0)-int((previous.get("measurements") or {}).get("governed_hypotheses") or 0),
        "futility_retirement_delta": int(lifecycle_counts.get("futility_rejected") or 0)-int((previous.get("measurements") or {}).get("futility_retired") or 0),
        "source_event_delta": causal_source_events-int((previous.get("measurements") or {}).get("source_events") or 0),
        "proof_forecast_totals": proof.get("totals") or {},
        "automatic_retraining_triggered": False,
    }
    payload = {
        "schema_version": 1, "generated_utc": generated.isoformat(),
        "status": "ok" if not failures else "degraded", "research_only": True,
        "can_place_orders": False, "can_promote": False, "real_money_routing": False,
        "supported_decision": "no_trade", "market_state": market,
        "expected_coverage": expected, "actual_coverage": actual,
        "coverage_exclusions": exclusions,
        "live_feature_coverage": feature_coverage,
        "cross_pair_snapshot": quote_audit,
        "official_release_fast_lane": official_release_fast_lane_status,
        "source_governance_fast_lane_adapter": (
            source_governance_fast_lane_adapter_status
        ),
        "official_release_fast_mapping": official_release_fast_mapping_status,
        "official_release_fast_response_watch": official_release_fast_response_status,
        "structured_numeric_currency_integrity": numeric_currency_integrity,
        "official_detail_quality_integrity": detail_quality_integrity,
        "current_source_provenance_integrity": source_provenance_integrity,
        "persistent_policy_state_integrity": policy_state_integrity,
        "official_source_depth_integrity": official_source_depth_status,
        "live_move_forward_proof_integrity": live_move_forward_integrity,
        "continuous_narrative_meter": continuous_narrative_meter_status,
        "live_move_persistent_context": live_move_persistent_context_status,
        "checks": checks, "failures": failures, "drift": drift,
        "measurements": {
            "governed_hypotheses": int((lifecycle.get("lifecycle") or {}).get("hypothesis_count") or 0),
            "futility_retired": int(lifecycle_counts.get("futility_rejected") or 0),
            "confirmed": int(lifecycle_counts.get("confirmed_candidate") or 0),
            "source_events": causal_source_events,
            "raw_source_events": raw_source_events,
            "quarantined_source_events": quarantined_source_events,
            "source_contracts": int(source_counts.get("contracts") or 0),
            "allocator_decisions": int((allocator.get("evidence") or {}).get("decision_count") or 0),
            "balance": account.get("balance"), "nav": account.get("nav"),
            "cumulative_pl": account.get("cumulative_pl"),
            "free_disk_gib": (storage.get("disk") or {}).get("free_gib"),
            "free_disk_percent": (storage.get("disk") or {}).get("free_percent"),
            "verified_log_reclaimed_bytes": (
                (log_archive.get("lifetime") or {}).get("reclaimed_bytes")
                or log_archive.get("reclaimed_bytes")
            ),
            "shadow_archive_raw_rows": int(shadow_archive.get("total_rows") or 0),
            "shadow_archive_unique_rows": int(
                shadow_archive.get("total_unique_rows")
                or shadow_archive.get("total_rows") or 0
            ),
            "shadow_archive_rows": int(
                shadow_archive.get("total_unique_rows")
                or shadow_archive.get("total_rows") or 0
            ),
            "shadow_archive_duplicate_rows": int(
                shadow_archive.get("duplicate_rows") or 0
            ),
            "shadow_archive_manifest_validated_parts": int(
                shadow_archive.get("validated_manifest_parts") or 0
            ),
            "shadow_archive_legacy_unmanifested_parts": int(
                shadow_archive.get("legacy_unmanifested_parts") or 0
            ),
            "shadow_archive_detail_gap_vs_rollup": (
                (shadow_archive.get("detail_reconciliation") or {}).get("detail_gap_vs_rollup")
            ),
            "shadow_archive_gap_dates_have_matching_frozen_snapshot": (
                (shadow_archive.get("detail_reconciliation") or {}).get(
                    "gap_dates_have_matching_frozen_snapshot"
                )
            ),
            "prospective_treasury_yield_rows": int(
                (treasury_yields.get("totals") or {}).get("prospective_eligible_rows") or 0
            ),
            "prospective_alfred_vintage_rows": int(
                (alfred_vintages.get("totals") or {}).get("prospective_rows") or 0
            ),
            "news_challenger_eligible_count": int(
                news_challenger.get("eligible_count") or 0
            ),
            "news_challenger_fills_today_utc": int(
                news_challenger.get("fills_today_utc") or 0
            ),
            "news_challenger_technical_exits_since_start": int(
                news_challenger.get("technical_exits_since_start") or 0
            ),
        },
        "source_gap_states": source_gaps.get("sources") or [],
        "logic_switches": {
            "frozen_cohorts_immutable": True,
            "adaptive_retraining": False,
            "retired_hypotheses_routable": False,
            "legacy_qualification_routable": False,
            "core_governed_entries_require_signed_one_time_canary": True,
            "news_challenger_is_separate_fast_rotation_practice_experiment": True,
            "independent_verifier_required": True,
            "sentiment_combined_score_forbidden": True,
            "live_vacuum_forbidden": True,
            "verified_rotated_log_compression": True,
            "daily_treasury_direction_policy": "abstain",
            "collector_code_changes_require_new_cohort": True,
            "archive_deletion_scoped_to_manifested_date": True,
            "historical_gap_can_be_silently_reconstructed": False,
            "weekend_feed_gaps_are_operational_failures": False,
        },
    }
    lines = [
        "# Forex project integrity and coverage", "", f"Generated: `{payload['generated_utc']}`", "",
        f"Status: **{payload['status']}**; supported decision: **no_trade**; market: **{market}**.", "",
        f"- Quotes: **{actual['quoted_instruments']} / 68 last-known**; current stream snapshot: **{actual['current_quoted_instruments']}**; retained stale: **{actual['retained_last_known_instruments']}**",
        f"- Sources: **{actual['operational_news_sources']} operational / {actual['runtime_observed_news_sources']} observed / {actual['configured_news_sources']} registered of {expected['configured_news_sources']} configured**",
        f"- Governed hypotheses: **{payload['measurements']['governed_hypotheses']:,}**; retired: **{payload['measurements']['futility_retired']:,}**; confirmed: **{payload['measurements']['confirmed']:,}**",
        f"- Practice 007 balance/NAV/P&L: **{account.get('balance')} / {account.get('nav')} / {account.get('cumulative_pl')}**", "",
        f"- Free disk: **{(storage.get('disk') or {}).get('free_gib')} GiB ({(storage.get('disk') or {}).get('free_percent')}%)**",
        f"- Verified rotated-log reclamation: **{int((log_archive.get('lifetime') or {}).get('reclaimed_bytes') or log_archive.get('reclaimed_bytes') or 0) / 1024**3:.2f} GiB**", "",
        "| Check | Result |", "|---|---|",
    ]
    lines.extend(f"| {name} | {'PASS' if passed else 'FAIL'} |" for name,passed in checks.items())
    lines.extend(["", "New sources remain research-only until source integrity, point-in-time replay, placebos, incremental-value tests, governed evidence, and untouched confirmation pass.", ""])
    atomic_json(output, payload)
    atomic_text(report, "\n".join(lines))
    history.parent.mkdir(parents=True, exist_ok=True)
    with history.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.time()
    while True:
        run(output=args.output, history=args.history, report=args.report)
        if args.interval_sec <= 0.0 or (
            args.duration_sec > 0.0 and time.time() - started >= args.duration_sec
        ):
            break
        time.sleep(max(60.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "availability_market_state_consistent", "collector_cohort_bound",
    "executable_opportunity_proof_is_fail_closed", "forex_market_state",
    "synchronized_quote_audit", "run",
]
