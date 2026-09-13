#!/usr/bin/env python3
"""Read-only coverage, drift, synchronization, and operational truth audit."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from oanda_integrity_publication import compact_integrity_payload
from oanda_project_runtime_health import read_supervisor_observation, scope_integrity_checks
from oanda_news_classification_contract import NEWS_CLASSIFICATION_VERSION
from oanda_news_collector_contract import NEWS_COLLECTOR_CONTRACT_ID
from oanda_continuous_narrative_meter_v12 import (
    BUCKET_MINUTES as CONTINUOUS_NARRATIVE_BUCKET_MINUTES,
    METER_CONTRACT_ID as CONTINUOUS_NARRATIVE_METER_CONTRACT,
    SEAL_GRACE_SECONDS as CONTINUOUS_NARRATIVE_SEAL_GRACE_SECONDS,
)
from oanda_event_technical_preflight import (
    CONTRACT_ID as EVENT_TECHNICAL_PREFLIGHT_CONTRACT_ID,
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
from oanda_source_governance_news_fast_lane import (
    ACTIVATED_UTC as NEWS_GOVERNANCE_FAST_LANE_ACTIVATED_UTC,
    COHORT_ID as NEWS_GOVERNANCE_FAST_LANE_COHORT_ID,
    CONTRACT_ID as NEWS_GOVERNANCE_FAST_LANE_CONTRACT_ID,
    LEGACY_CONTRACT_ID as NEWS_GOVERNANCE_FAST_LANE_LEGACY_CONTRACT_ID,
    PRIOR_CONTRACT_ID as NEWS_GOVERNANCE_FAST_LANE_PRIOR_CONTRACT_ID,
    PRIOR_RETIREMENT_REASON as NEWS_GOVERNANCE_FAST_LANE_PRIOR_RETIREMENT_REASON,
)


ROOT = Path(__file__).resolve().parent
EXPECTED_CONFIGURED_NEWS_SOURCES = 103
MAXIMUM_AUDIT_SNAPSHOT_PUBLICATION_AGE_SEC = 180.0
SOURCE_GOVERNANCE_FULL_INTEGRITY_ATTESTATION_CONTRACT_ID = (
    "source_governance_full_quick_check_attestation_v1_20260901"
)
SOURCE_GOVERNANCE_FULL_INTEGRITY_MAX_AGE_SEC = 21600.0
COMPONENT_FULL_INTEGRITY_ATTESTATION_CONTRACT_ID = (
    "project_integrity_component_full_quick_check_attestation_v1_20260902"
)
COMPONENT_FULL_INTEGRITY_MAX_AGE_SEC = 21600.0
PROJECT_INTEGRITY_HISTORY_ROTATE_BYTES = 64 * 1024**2
EXPECTED_QUOTE_TRADEABILITY_CONTRACT = (
    "oanda_client_price_status_boolean_v1"
)
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
DEFAULT_OUTPUT = STATE / "project_integrity_audit_v1.json"
DEFAULT_HISTORY = DATA / "logs" / "project_integrity_audit_v1.jsonl"
DEFAULT_REPORT = DATA / "reports" / "project_integrity" / "PROJECT_INTEGRITY_CURRENT.md"
DEFAULT_PUBLICATION_GUARD = STATE / "project_integrity_audit_guard_v1.sqlite"
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
CURRENT_MOVE_FIRST_LIVE_CASE_CAPTURE = (
    DATA
    / "reports"
    / "move_first_live_case_capture_v4"
    / "MOVE_FIRST_LIVE_CASE_CAPTURE_CURRENT.json"
)
CURRENT_MOVE_FIRST_LIVE_ARM_ALIGNMENT = (
    DATA
    / "reports"
    / "move_first_live_arm_alignment_v1"
    / "MOVE_FIRST_LIVE_ARM_ALIGNMENT_CURRENT.json"
)
CURRENT_MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT = (
    DATA
    / "reports"
    / "move_first_operational_mapping_alignment_v1"
    / "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json"
)
CURRENT_MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_V2 = (
    DATA
    / "reports"
    / "move_first_operational_mapping_alignment_v2"
    / "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json"
)
CURRENT_MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_V3 = (
    DATA
    / "reports"
    / "move_first_operational_mapping_alignment_v3"
    / "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json"
)
CURRENT_MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_V4 = (
    DATA
    / "reports"
    / "move_first_operational_mapping_alignment_v4"
    / "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json"
)
EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_CONTRACT_ID = (
    "move_first_operational_mapping_alignment_v1_receipt_backed_"
    "prospective_20260901T140000Z"
)
EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_COHORT_ID = (
    "move_first_operational_mapping_alignment_v1_prospective_20260901T140000Z"
)
EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_V2_CONTRACT_ID = (
    "move_first_operational_mapping_alignment_v2_receipt_backed_"
    "syndication_dedup_prospective_20260901T150000Z"
)
EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_V2_COHORT_ID = (
    "move_first_operational_mapping_alignment_v2_prospective_20260901T150000Z"
)
EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_V2_STORY_DEDUPLICATION_RULE = (
    "publisher_suffix_stripped_normalized_headline_v2"
)
EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_V3_CONTRACT_ID = (
    "move_first_operational_mapping_alignment_v3_receipt_backed_"
    "narrative_family_age_decay_prospective_20260901T170000Z"
)
EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_V3_COHORT_ID = (
    "move_first_operational_mapping_alignment_v3_prospective_20260901T170000Z"
)
EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_V3_STORY_DEDUPLICATION_RULE = (
    "conservative_time_bounded_narrative_family_v3"
)
EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_V3_BROAD_HALF_LIFE_MINUTES = 30.0
EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_V4_CONTRACT_ID = (
    "move_first_operational_mapping_alignment_v4_receipt_backed_"
    "subsecond_causal_narrative_family_prospective_20260902T121500Z"
)
EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_V4_COHORT_ID = (
    "move_first_operational_mapping_alignment_v4_prospective_"
    "20260902T121500Z"
)
EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_V4_STORY_DEDUPLICATION_RULE = (
    "conservative_time_bounded_narrative_family_v3"
)
EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_V4_BROAD_HALF_LIFE_MINUTES = 30.0
EXPECTED_MOVE_FIRST_CAUSAL_GAP_TAXONOMY_CONTRACT_ID = (
    "move_first_causal_gap_taxonomy_v4_narrative_family_concentration_exact_retained_clock_20260901"
)
EXPECTED_MOVE_FIRST_NARRATIVE_FAMILY_CONTRACT_ID = (
    "retained_story_narrative_family_v1_conservative_time_bounded_headline_component_20260901"
)
EXPECTED_MOVE_FIRST_FACTOR_SUPPORT_DIAGNOSTIC_CONTRACT_ID = (
    "move_first_factor_support_v1_positive_aligned_primary_and_unambiguous_20260901"
)
CURRENT_MAJOR_MOVE_GAP_CENSUS = (
    DATA
    / "reports"
    / "major_move_gap_census"
    / "MAJOR_MOVE_GAP_CENSUS_CURRENT.json"
)
CURRENT_MAJOR_MOVE_GAP_CENSUS_HEARTBEAT = (
    STATE / "major_move_gap_census_heartbeat_v1.json"
)
CURRENT_EXECUTABLE_MOVE_CENSUS = (
    STATE / "executable_move_census_latest_v3_20260902h.json"
)
CURRENT_EXECUTABLE_MOVE_CENSUS_VERIFIER = (
    STATE / "executable_move_census_verifier_latest_v3_20260902h.json"
)
EXPECTED_EXECUTABLE_MOVE_CENSUS_COHORT = (
    "all68_executable_move_census_v3_20260902h"
)
CURRENT_LIVE_MOVE_NEWS_SNAPSHOT = STATE / "live_move_news_snapshot_v7r3.json"
CURRENT_LIVE_MOVE_NEWS_OUTCOMES = STATE / "live_move_news_outcomes_v4r3.json"
CURRENT_LIVE_MOVE_NEWS_CASES = STATE / "live_move_news_cases_v7r3.sqlite"
CURRENT_CONTINUOUS_NARRATIVE_METER = (
    STATE / "continuous_narrative_meter_v12.json"
)
CURRENT_EVENT_TECHNICAL_PREFLIGHT = STATE / "event_technical_preflight_v1.json"
CURRENT_SCHEDULED_EVENT_QUOTE_CAPTURE = (
    DATA
    / "local_news_sentiment"
    / "scheduled_event_quote_capture_latest_v1.json"
)
SCHEDULED_EVENT_QUOTE_CAPTURE_DATABASE = (
    DATA
    / "local_news_sentiment"
    / "scheduled_event_quote_capture_v1.sqlite"
)
EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_CONTRACT = (
    "scheduled_event_quote_capture_v1_all68_prospective_20260902"
)
EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_COHORT = (
    "scheduled_event_quote_capture_v1_20260902a"
)
EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_UNIVERSE_SHA256 = (
    "b6abe559dcaa9faa0a1e48217f97a12dc9043eb9595d2b946bf80f93035be142"
)
EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_HORIZONS_MIN = [0, 1, 5, 15, 30, 60]
CURRENT_SCHEDULED_EVENT_QUOTE_CAPTURE_V2 = (
    DATA
    / "local_news_sentiment"
    / "scheduled_event_quote_capture_latest_v2.json"
)
SCHEDULED_EVENT_QUOTE_CAPTURE_V2_DATABASE = (
    DATA
    / "local_news_sentiment"
    / "scheduled_event_quote_capture_v2.sqlite"
)
EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_V2_CONTRACT = (
    "scheduled_event_quote_capture_v2_oanda_rest_current_snapshot_20260902"
)
EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_V2_COHORT = (
    "scheduled_event_quote_capture_v2_20260902a"
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
CURRENT_SOURCE_CONDITIONED_CURRENCY_RANK_V6 = (
    STATE / "source_conditioned_currency_rank_v6.json"
)
SOURCE_CONDITIONED_CURRENCY_RANK_V6_DATABASE = (
    STATE / "source_conditioned_currency_rank_v6.sqlite"
)
SOURCE_FACTOR_RESPONSE_MAP_V7_DATABASE = (
    DATA
    / "local_news_sentiment"
    / "causal_source_factor_response_map_v7.sqlite"
)
EXPECTED_SOURCE_CONDITIONED_CURRENCY_RANK_V6_CONTRACT = (
    "source_conditioned_currency_rank_v6_v7_input_explicit_no_trade_20260901"
)
EXPECTED_SOURCE_FACTOR_RESPONSE_MAP_V7_CONTRACT = (
    "causal_source_factor_response_map_v7_v152_subject_bound_release_policy_targets_20260901"
)
CURRENT_SOURCE_CONDITIONED_CURRENCY_RANK_V7 = (
    STATE / "source_conditioned_currency_rank_v7.json"
)
SOURCE_CONDITIONED_CURRENCY_RANK_V7_DATABASE = (
    STATE / "source_conditioned_currency_rank_v7.sqlite"
)
SOURCE_FACTOR_RESPONSE_MAP_V8_DATABASE = (
    DATA
    / "local_news_sentiment"
    / "causal_source_factor_response_map_v8.sqlite"
)
EXPECTED_SOURCE_CONDITIONED_CURRENCY_RANK_V7_CONTRACT = (
    "source_conditioned_currency_rank_v7_v8_input_explicit_no_trade_20260901"
)
EXPECTED_SOURCE_FACTOR_RESPONSE_MAP_V8_CONTRACT = (
    "causal_source_factor_response_map_v8_"
    "v152_boj_market_structure_survey_overlay_20260901"
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
CURRENT_NEWS_GOVERNANCE_FAST_LANE = (
    STATE / "source_governance_news_fast_lane_v3.json"
)
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


def _open_readonly_evidence_database(path: Path) -> sqlite3.Connection:
    """Missing audit inputs stay missing; inspection cannot create a ledger."""
    connection = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=5.0)
    connection.execute("PRAGMA query_only=ON")
    return connection


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True), encoding="utf-8"
        )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        temporary.write_text(value, encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _claim_audit_publication(guard: Path) -> dict[str, Any]:
    """Claim a monotonic generation so stale overlapping scans cannot publish."""
    guard.parent.mkdir(parents=True, exist_ok=True)
    owner_id = uuid.uuid4().hex
    claimed_utc = utc_now()
    connection = sqlite3.connect(guard, timeout=30.0, isolation_level=None)
    try:
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_publication_guard(
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                generation INTEGER NOT NULL,
                owner_id TEXT NOT NULL,
                claimed_utc TEXT NOT NULL,
                status TEXT NOT NULL
            )
            """
        )
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT generation FROM audit_publication_guard WHERE singleton=1"
        ).fetchone()
        generation = int(row[0]) + 1 if row else 1
        connection.execute(
            """
            INSERT INTO audit_publication_guard(
                singleton,generation,owner_id,claimed_utc,status
            ) VALUES(1,?,?,?,'running')
            ON CONFLICT(singleton) DO UPDATE SET
                generation=excluded.generation,
                owner_id=excluded.owner_id,
                claimed_utc=excluded.claimed_utc,
                status='running'
            """,
            (generation, owner_id, claimed_utc),
        )
        connection.execute("COMMIT")
        return {
            "generation": generation,
            "owner_id": owner_id,
            "claimed_utc": claimed_utc,
        }
    finally:
        connection.close()


def _publish_owned_audit(
    *,
    guard: Path,
    ownership: dict[str, Any],
    output: Path,
    report: Path,
    history: Path,
    payload: dict[str, Any],
    report_text: str,
) -> bool:
    """Publish only if no newer audit pass has claimed ownership.

    Immutable episode artifacts are complete and hash-verified before publishing
    references. Each current file replacement is atomic; the existing current,
    Markdown and JSONL writes are not a multi-file filesystem transaction.
    """
    connection = sqlite3.connect(guard, timeout=30.0, isolation_level=None)
    try:
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT generation,owner_id FROM audit_publication_guard "
            "WHERE singleton=1"
        ).fetchone()
        owns = bool(
            row
            and int(row[0]) == int(ownership["generation"])
            and str(row[1]) == str(ownership["owner_id"])
        )
        if not owns:
            connection.execute("ROLLBACK")
            return False
        publication_payload = compact_integrity_payload(
            payload, snapshot_dir=output.parent
        )
        atomic_json(output, publication_payload)
        atomic_text(report, report_text)
        history.parent.mkdir(parents=True, exist_ok=True)
        rotate_jsonl_history_if_needed(history)
        with history.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(publication_payload, sort_keys=True, separators=(",", ":")) + "\n"
            )
        connection.execute(
            "UPDATE audit_publication_guard SET status='completed' "
            "WHERE singleton=1 AND generation=? AND owner_id=?",
            (int(ownership["generation"]), str(ownership["owner_id"])),
        )
        connection.execute("COMMIT")
        return True
    except BaseException:
        try:
            connection.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        connection.close()


def rotate_jsonl_history_if_needed(
    history: Path,
    *,
    maximum_bytes: int | None = None,
    now: datetime | None = None,
) -> Path | None:
    """Atomically seal an oversized JSONL history for verified archiving.

    The verified log archiver only consumes inactive ``.part_*.jsonl`` files
    and deletes their raw form after gzip round-trip, byte-count, and SHA-256
    verification.  Renaming here therefore bounds the active audit log while
    preserving every historical byte in the existing recoverable evidence
    pipeline.  Publication ownership serializes this operation.
    """

    limit = max(
        1,
        int(
            PROJECT_INTEGRITY_HISTORY_ROTATE_BYTES
            if maximum_bytes is None
            else maximum_bytes
        ),
    )
    try:
        if not history.is_file() or history.stat().st_size < limit:
            return None
    except OSError:
        return None
    observed = now or datetime.now(timezone.utc)
    stamp = observed.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    base = history.with_name(
        f"{history.stem}.part_{stamp}_{os.getpid():06d}{history.suffix}"
    )
    rotated = base
    sequence = 0
    while rotated.exists():
        sequence += 1
        rotated = history.with_name(
            f"{history.stem}.part_{stamp}_{os.getpid():06d}_"
            f"{sequence:04d}{history.suffix}"
        )
    history.replace(rotated)
    return rotated


def read_json(
    path: Path,
    *,
    attempts: int = 3,
    retry_delay_sec: float = 0.02,
) -> dict[str, Any]:
    """Read one current JSON snapshot across bounded Windows replace races."""

    bounded_attempts = max(1, int(attempts))
    for attempt in range(bounded_attempts):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            if attempt + 1 < bounded_attempts:
                time.sleep(max(0.0, float(retry_delay_sec)))
                continue
            return {}
        return value if isinstance(value, dict) else {}
    return {}


def parse_epoch(value: Any) -> float | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def audit_snapshot_publication_freshness(
    snapshot_utc: Any,
    finished_utc: Any,
    *,
    maximum_age_sec: float = MAXIMUM_AUDIT_SNAPSHOT_PUBLICATION_AGE_SEC,
) -> dict[str, Any]:
    """Classify whether an expensive audit is still current when published."""

    snapshot_epoch = parse_epoch(snapshot_utc)
    finished_epoch = parse_epoch(finished_utc)
    age_sec = (
        None
        if snapshot_epoch is None or finished_epoch is None
        else finished_epoch - snapshot_epoch
    )
    ok = bool(
        age_sec is not None
        and 0.0 <= age_sec <= maximum_age_sec
    )
    return {
        "ok": ok,
        "snapshot_utc": snapshot_utc,
        "finished_utc": finished_utc,
        "age_sec": age_sec,
        "maximum_age_sec": maximum_age_sec,
    }


def prior_source_governance_full_integrity_attestation(
    previous: dict[str, Any],
) -> dict[str, Any]:
    """Return the last explicit full SQLite attestation, or migrate one pass.

    The pre-attestation audit performed a real full ``PRAGMA quick_check`` on
    every cycle. Its last successful adapter result is therefore valid source
    evidence; this narrow bridge preserves that completed scan without forcing
    another multi-gigabyte scan immediately after deployment. Subsequent
    reports carry the explicit contract and database metadata.
    """

    adapter = previous.get("source_governance_fast_lane_adapter") or {}
    explicit = adapter.get("full_database_integrity_attestation")
    if isinstance(explicit, dict):
        return dict(explicit)
    checks = previous.get("checks") or {}
    checked_utc = previous.get("audit_finished_utc")
    if not (
        isinstance(adapter, dict)
        and adapter.get("database_integrity") == "ok"
        and checks.get(
            "source_governance_fast_lane_adapter_prospective_and_inert"
        )
        is True
        and parse_epoch(checked_utc) is not None
    ):
        return {}
    return {
        "contract_id": SOURCE_GOVERNANCE_FULL_INTEGRITY_ATTESTATION_CONTRACT_ID,
        "database_path": str(SOURCE_GOVERNANCE_DATABASE.resolve()),
        "checked_utc": str(checked_utc),
        "result": "ok",
        "legacy_completed_scan_migrated": True,
    }


def prior_component_full_integrity_attestation(
    previous: dict[str, Any],
    *,
    component_key: str,
    check_key: str,
    database_path: Path,
    table_name: str,
) -> dict[str, Any]:
    """Return a recent explicit full scan or migrate one completed scan.

    Active append-only research databases can grow while this audit runs. A
    full quick-check on every five-minute cycle eventually makes live inputs
    stale by publication. Retain the completed full scan as a bounded prefix
    attestation, then execute current schema/count queries on every cycle.
    """

    component = previous.get(component_key) or {}
    explicit = component.get("full_database_integrity_attestation")
    if isinstance(explicit, dict):
        return dict(explicit)
    checks = previous.get("checks") or {}
    checked_utc = previous.get("audit_finished_utc")
    if not (
        isinstance(component, dict)
        and component.get("database_integrity") == "ok"
        and checks.get(check_key) is True
        and parse_epoch(checked_utc) is not None
    ):
        return {}
    return {
        "contract_id": COMPONENT_FULL_INTEGRITY_ATTESTATION_CONTRACT_ID,
        "database_path": str(database_path.resolve()),
        "table_name": table_name,
        "checked_utc": str(checked_utc),
        "result": "ok",
        "legacy_completed_scan_migrated": True,
    }


def resolve_component_full_integrity(
    connection: sqlite3.Connection,
    *,
    supplied_attestation: dict[str, Any] | None,
    database_path: Path,
    table_name: str,
    cutoff_epoch: float,
    maximum_age_sec: float = COMPONENT_FULL_INTEGRITY_MAX_AGE_SEC,
) -> tuple[str, dict[str, Any], float | None, str]:
    """Resolve a bounded full scan while checking current rows separately."""

    supplied = (
        dict(supplied_attestation)
        if isinstance(supplied_attestation, dict)
        else {}
    )
    supplied_epoch = parse_epoch(supplied.get("checked_utc"))
    supplied_age = (
        None if supplied_epoch is None else max(0.0, cutoff_epoch - supplied_epoch)
    )
    supplied_valid = bool(
        supplied.get("contract_id")
        == COMPONENT_FULL_INTEGRITY_ATTESTATION_CONTRACT_ID
        and supplied.get("database_path") == str(database_path.resolve())
        and supplied.get("table_name") == table_name
        and supplied.get("result") == "ok"
        and supplied_age is not None
        and supplied_age <= maximum_age_sec
    )
    if supplied_valid:
        return "ok", supplied, supplied_age, "reused_bounded_full_scan"

    result = str(connection.execute("PRAGMA quick_check").fetchone()[0])
    checked_epoch = time.time()
    row_count = int(
        connection.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
    )
    attestation = {
        "contract_id": COMPONENT_FULL_INTEGRITY_ATTESTATION_CONTRACT_ID,
        "database_path": str(database_path.resolve()),
        "table_name": table_name,
        "checked_utc": datetime.fromtimestamp(
            checked_epoch, timezone.utc
        ).isoformat(),
        "result": result,
        "attested_row_count": row_count,
        "legacy_completed_scan_migrated": False,
    }
    age = max(0.0, cutoff_epoch - checked_epoch)
    return result, attestation, age, "new_full_scan"


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
    full_database_integrity_attestation: dict[str, Any] | None = None,
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
    full_attestation: dict[str, Any] = {}
    full_integrity_age_sec: float | None = None
    integrity_mode = "unavailable"
    try:
        connection = sqlite3.connect(
            f"file:{database_path.resolve().as_posix()}?mode=ro",
            uri=True,
            timeout=2.0,
        )
        try:
            (
                database_integrity,
                full_attestation,
                full_integrity_age_sec,
                integrity_mode,
            ) = resolve_component_full_integrity(
                connection,
                supplied_attestation=full_database_integrity_attestation,
                database_path=database_path,
                table_name="diagnoses",
                cutoff_epoch=cutoff_epoch,
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
        "full_database_integrity_attestation": full_attestation,
        "full_database_integrity_age_sec": full_integrity_age_sec,
        "integrity_mode": integrity_mode,
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
    full_database_integrity_attestation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Verify the official release observation lane is fresh and inert."""

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
    observation_count = -1
    prospective_count = -1
    current_observation_count = -1
    current_prospective_count = -1
    count_basis = "unavailable"
    full_attestation: dict[str, Any] = {}
    full_integrity_age_sec: float | None = None
    integrity_mode = "unavailable"
    append_only_triggers_present = False
    connection: sqlite3.Connection | None = None
    try:
        connection = _open_readonly_evidence_database(database_path)
        connection.execute("BEGIN")
        (
            database_integrity,
            full_attestation,
            full_integrity_age_sec,
            integrity_mode,
        ) = resolve_component_full_integrity(
            connection,
            supplied_attestation=full_database_integrity_attestation,
            database_path=database_path,
            table_name="official_release_observation",
            cutoff_epoch=cutoff_epoch,
        )
        trigger_names = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        }
        append_only_triggers_present = {
            "trg_fast_lane_observation_no_update",
            "trg_fast_lane_observation_no_delete",
        } <= trigger_names
        columns = {
            str(row[1])
            for row in connection.execute(
                "PRAGMA table_info(official_release_observation)"
            )
        }
        current_row = connection.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(prospective_observation), 0)
            FROM official_release_observation
            """
        ).fetchone()
        current_observation_count = int(current_row[0])
        current_prospective_count = int(current_row[1])
        if "first_seen_utc" not in columns or snapshot_epoch is None:
            raise ValueError("fast_lane_snapshot_missing_stable_count_cutoff")
        row = connection.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(prospective_observation), 0)
            FROM official_release_observation
            WHERE julianday(first_seen_utc)<=julianday(?)
            """,
            (snapshot_generated_utc,),
        ).fetchone()
        observation_count = int(row[0])
        prospective_count = int(row[1])
        count_basis = "database_rows_first_seen_at_or_before_snapshot_generated_utc"
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
        and snapshot_age is not None
        and -5.0 <= snapshot_age <= maximum_heartbeat_age_sec
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
            and append_only_triggers_present
        ),
        "contract_ok": contract_ok,
        "inert_ok": inert_ok,
        "coverage_ok": coverage_ok,
        "counts_ok": counts_ok,
        "freshness_ok": freshness_ok,
        "heartbeat_age_sec": heartbeat_age,
        "snapshot_age_sec": snapshot_age,
        "snapshot_generated_utc": snapshot_generated_utc,
        "count_basis": count_basis,
        "database_integrity": database_integrity,
        "full_database_integrity_attestation": full_attestation,
        "full_database_integrity_age_sec": full_integrity_age_sec,
        "integrity_mode": integrity_mode,
        "append_only_triggers_present": append_only_triggers_present,
        "observations": observation_count,
        "prospective_observations": prospective_count,
        "current_database_observations": current_observation_count,
        "current_database_prospective_observations": current_prospective_count,
    }


def official_release_fast_mapping_integrity(
    snapshot: dict[str, Any],
    heartbeat: dict[str, Any],
    *,
    database_path: Path,
    cutoff_epoch: float,
    maximum_heartbeat_age_sec: float = 180.0,
    full_database_integrity_attestation: dict[str, Any] | None = None,
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
    full_attestation: dict[str, Any] = {}
    full_integrity_age_sec: float | None = None
    integrity_mode = "unavailable"
    append_only_triggers_present = False
    connection: sqlite3.Connection | None = None
    try:
        connection = _open_readonly_evidence_database(database_path)
        connection.execute("BEGIN")
        (
            database_integrity,
            full_attestation,
            full_integrity_age_sec,
            integrity_mode,
        ) = resolve_component_full_integrity(
            connection,
            supplied_attestation=full_database_integrity_attestation,
            database_path=database_path,
            table_name="official_release_mapping",
            cutoff_epoch=cutoff_epoch,
        )
        trigger_names = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        }
        append_only_triggers_present = {
            "trg_fast_mapping_no_update",
            "trg_fast_mapping_no_delete",
        } <= trigger_names
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
            and append_only_triggers_present
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
        "full_database_integrity_attestation": full_attestation,
        "full_database_integrity_age_sec": full_integrity_age_sec,
        "integrity_mode": integrity_mode,
        "append_only_triggers_present": append_only_triggers_present,
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
        connection = _open_readonly_evidence_database(database_path)
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
    maximum_upstream_generation_lag_sec: float = 28800.0,
) -> bool:
    """Require a fresh, fail-closed audit within its bounded census cadence.

    Both workers intentionally run a heavy six-hour rebuild.  Their completion
    phases can drift, so requiring the audit timestamp to be newer than every
    census publication creates a predictable false incident between healthy
    runs.  Bound both artifacts and their generation distance instead.  This
    remains a retrospective, nonexecuting diagnostic and does not relax any
    predictor, lifecycle, authorization, or broker gate.
    """

    generated = parse_epoch(audit.get("generated_utc"))
    upstream_generated = parse_epoch(upstream_census.get("generated_utc"))
    generation_lag = (
        None
        if generated is None or upstream_generated is None
        else abs(generated - upstream_generated)
    )
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
        and 0.0 <= cutoff_epoch - generated <= maximum_age_sec
        and 0.0 <= cutoff_epoch - upstream_generated <= maximum_age_sec
        and generation_lag is not None
        and generation_lag <= maximum_upstream_generation_lag_sec
    )


def major_move_gap_census_progress_status(
    report: dict[str, Any],
    heartbeat: dict[str, Any],
    *,
    cutoff_epoch: float,
    maximum_report_age_sec: float = 28800.0,
    maximum_heartbeat_age_sec: float = 120.0,
) -> dict[str, Any]:
    """Separate worker liveness from completed evidence freshness.

    A fresh heartbeat proves only that the long rebuild process is alive.  It
    never makes an old completed census current and therefore cannot relax the
    downstream move-first evidence check.
    """

    report_generated = parse_epoch(report.get("generated_utc"))
    report_age_sec = (
        None
        if report_generated is None
        else cutoff_epoch - report_generated
    )
    evidence_fresh = bool(
        report_age_sec is not None
        and 0.0 <= report_age_sec <= maximum_report_age_sec
    )
    heartbeat_updated = parse_epoch(heartbeat.get("updated_at"))
    heartbeat_age_sec = (
        None
        if heartbeat_updated is None
        else cutoff_epoch - heartbeat_updated
    )
    worker_live = bool(
        heartbeat.get("worker") == "oanda_major_move_gap_census"
        and heartbeat.get("status") == "running"
        and heartbeat_age_is_current(
            heartbeat_age_sec,
            maximum_age_sec=maximum_heartbeat_age_sec,
        )
    )
    phase = str(heartbeat.get("phase") or "unobserved")
    rebuild_in_progress = bool(
        worker_live
        and phase not in {"cycle_complete", "sleeping", "stopped"}
    )
    if evidence_fresh and rebuild_in_progress:
        status = "current_rebuild_in_progress"
    elif evidence_fresh:
        status = "current_completed_evidence"
    elif rebuild_in_progress:
        status = "stale_rebuild_in_progress"
    elif worker_live:
        status = "stale_worker_idle"
    else:
        status = "stale_worker_unobserved"
    return {
        "status": status,
        "evidence_fresh": evidence_fresh,
        "worker_live": worker_live,
        "rebuild_in_progress": rebuild_in_progress,
        "report_generated_utc": report.get("generated_utc"),
        "report_publication_utc": report.get("publication_utc"),
        "report_age_sec": report_age_sec,
        "maximum_report_age_sec": maximum_report_age_sec,
        "heartbeat_updated_at": heartbeat.get("updated_at"),
        "heartbeat_age_sec": heartbeat_age_sec,
        "maximum_heartbeat_age_sec": maximum_heartbeat_age_sec,
        "phase": phase,
        "phase_age_sec": heartbeat.get("phase_age_sec"),
        "progress_sequence": heartbeat.get("progress_sequence"),
        "progress_age_sec": heartbeat.get("progress_age_sec"),
        "details": heartbeat.get("details") or {},
        "fresh_heartbeat_does_not_refresh_completed_evidence": True,
    }


def move_first_live_case_capture_is_current(
    capture: dict[str, Any],
    *,
    cutoff_epoch: float,
    maximum_age_sec: float = 240.0,
) -> bool:
    """Require a fresh, direct, append-only and nonexecuting live capture."""

    generated = parse_epoch(capture.get("generated_utc"))
    return bool(
        capture.get("schema_version") == "move_first_live_case_capture_report_v1"
        and capture.get("cohort_id")
        == "move_first_live_case_capture_v4_prospective_20260901T070500Z"
        and capture.get("source_distance") == "direct_append_only_live_mover_database"
        and capture.get("append_only") is True
        and capture.get("research_only") is True
        and capture.get("execution_eligible") is False
        and capture.get("can_place_orders") is False
        and capture.get("execution_decision") == "no_trade"
        and capture.get("historical_rows_imported") == "0"
        and capture.get("sqlite_integrity") == "ok"
        and capture.get("factor_graph_cycle") is False
        and isinstance(capture.get("case_count"), int)
        and int(capture.get("case_count") or 0)
        == int(capture.get("factor_membership_count") or 0)
        and generated is not None
        and 0.0 <= cutoff_epoch - generated <= maximum_age_sec
    )


def move_first_live_arm_alignment_is_current(
    alignment: dict[str, Any],
    capture: dict[str, Any],
    *,
    cutoff_epoch: float,
    maximum_age_sec: float = 240.0,
    maximum_source_lag_sec: float = 120.0,
) -> bool:
    """Require a current, causality-labelled, nonexecuting V4 arm audit."""

    generated = parse_epoch(alignment.get("generated_utc"))
    capture_generated = parse_epoch(capture.get("generated_utc"))
    metrics = alignment.get("arm_metrics") or {}
    support_metrics = alignment.get("support_qualified_arm_metrics") or {}
    if not isinstance(metrics, dict) or not isinstance(support_metrics, dict):
        return False
    pre_move = [
        value
        for value in metrics.values()
        if isinstance(value, dict)
        and value.get("clock_classification")
        == "available_at_or_before_move_start"
        and value.get("eligible_as_pre_move_directional_diagnostic") is True
    ]
    label_derived = [
        value
        for value in metrics.values()
        if isinstance(value, dict)
        and value.get("clock_classification")
        == "label_derived_after_move_detection"
        and value.get("eligible_as_pre_move_directional_diagnostic") is False
    ]
    source_case_count = int(alignment.get("source_case_count") or 0)
    capture_case_count = int(capture.get("case_count") or 0)
    resolved_episode_count = int(
        alignment.get("resolved_factor_episode_count") or 0
    )
    taxonomy = alignment.get("causal_gap_taxonomy")
    episode_rows = alignment.get("episode_rows")
    factor_support = alignment.get("factor_support_audit")
    factor_support_valid = False
    if isinstance(factor_support, dict) and isinstance(episode_rows, list):
        source_status_counts = factor_support.get("source_case_status_counts")
        selected_status_counts = factor_support.get(
            "selected_episode_status_counts"
        )

        def valid_support_counts(value: Any, expected_total: int) -> bool:
            return bool(
                isinstance(value, dict)
                and all(
                    isinstance(key, str)
                    and bool(key)
                    and isinstance(count, int)
                    and not isinstance(count, bool)
                    and count >= 0
                    for key, count in value.items()
                )
                and sum(value.values()) == expected_total
            )

        episode_support_rows_valid = all(
            isinstance(row, dict)
            and isinstance(row.get("factor_support"), dict)
            and row["factor_support"].get("contract_id")
            == EXPECTED_MOVE_FIRST_FACTOR_SUPPORT_DIAGNOSTIC_CONTRACT_ID
            and row["factor_support"].get("membership_rewritten") is False
            and row.get("factor_support_qualified")
            is (row["factor_support"].get("status") == "supported")
            for row in episode_rows
        )
        qualified_source = int(
            factor_support.get("support_qualified_source_case_count") or 0
        )
        qualified_selected = int(
            factor_support.get("support_qualified_selected_episode_count") or 0
        )
        unsupported_source = int(
            factor_support.get("unsupported_source_case_count") or 0
        )
        unsupported_selected = int(
            factor_support.get("unsupported_selected_episode_count") or 0
        )
        factor_support_valid = bool(
            factor_support.get("contract_id")
            == EXPECTED_MOVE_FIRST_FACTOR_SUPPORT_DIAGNOSTIC_CONTRACT_ID
            and valid_support_counts(source_status_counts, source_case_count)
            and valid_support_counts(
                selected_status_counts, resolved_episode_count
            )
            and qualified_source + unsupported_source == source_case_count
            and qualified_selected + unsupported_selected
            == resolved_episode_count
            and int((source_status_counts or {}).get("supported") or 0)
            == qualified_source
            and int((selected_status_counts or {}).get("supported") or 0)
            == qualified_selected
            and factor_support.get("existing_membership_bytes_preserved")
            is True
            and factor_support.get("changes_existing_arm_metrics") is False
            and factor_support.get("promotion_eligible") is False
            and factor_support.get("execution_eligible") is False
            and episode_support_rows_valid
        )

    def rebuild_arm_metric(
        rows: list[dict[str, Any]],
        arm_name: str,
        clock_classification: str,
        interpretation: str,
    ) -> dict[str, Any] | None:
        compact: list[dict[str, Any]] = []
        for row in rows:
            arms = row.get("arms")
            arm = arms.get(arm_name) if isinstance(arms, dict) else None
            direction = arm.get("direction") if isinstance(arm, dict) else None
            actual = row.get("actual_direction")
            net_room = row.get("executable_net_room_pips")
            episode = row.get("resolved_factor_episode_id")
            if (
                not isinstance(direction, int)
                or isinstance(direction, bool)
                or direction not in {-1, 0, 1}
                or not isinstance(actual, int)
                or isinstance(actual, bool)
                or actual not in {-1, 1}
                or not isinstance(net_room, (int, float))
                or isinstance(net_room, bool)
                or float(net_room) != float(net_room)
                or float(net_room) in {float("inf"), float("-inf")}
                or float(net_room) < 0.0
                or not isinstance(episode, str)
                or not episode
            ):
                return None
            compact.append(
                {
                    "episode": episode,
                    "direction": direction,
                    "actual": actual,
                    "net_room_pips": float(net_room),
                }
            )
        signaled = [row for row in compact if row["direction"] != 0]
        correct = [
            row for row in signaled if row["direction"] == row["actual"]
        ]
        wrong = [
            row for row in signaled if row["direction"] != row["actual"]
        ]
        aligned_room = sum(row["net_room_pips"] for row in correct)
        opposed_room = sum(row["net_room_pips"] for row in wrong)
        signature_payload = [
            [row["episode"], row["direction"]] for row in compact
        ]
        canonical = json.dumps(
            signature_payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        signature = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        return {
            "clock_classification": clock_classification,
            "eligible_as_pre_move_directional_diagnostic": (
                clock_classification == "available_at_or_before_move_start"
            ),
            "effective_episode_count": len(compact),
            "signaled_episode_count": len(signaled),
            "abstained_episode_count": len(compact) - len(signaled),
            "correct_direction_count": len(correct),
            "wrong_direction_count": len(wrong),
            "directional_coverage_pct": (
                round(100.0 * len(signaled) / len(compact), 9)
                if compact
                else None
            ),
            "directional_accuracy_pct": (
                round(100.0 * len(correct) / len(signaled), 9)
                if signaled
                else None
            ),
            "aligned_observed_net_room_pips": round(aligned_room, 9),
            "opposed_observed_net_room_pips": round(opposed_room, 9),
            "signed_alignment_room_pips": round(
                aligned_room - opposed_room, 9
            ),
            "decision_signature_sha256": signature,
            "interpretation": interpretation,
        }

    def rebuild_equivalence_groups(
        rebuilt: dict[str, dict[str, Any]],
        *,
        pre_move_only: bool,
    ) -> list[dict[str, Any]]:
        grouped: dict[str, list[str]] = {}
        for arm_name, metric in rebuilt.items():
            if (
                pre_move_only
                and metric.get("clock_classification")
                != "available_at_or_before_move_start"
            ):
                continue
            signature = str(metric["decision_signature_sha256"])
            grouped.setdefault(signature, []).append(arm_name)
        output = [
            {
                "decision_signature_sha256": signature,
                "arms": sorted(arms),
                "arm_count": len(arms),
            }
            for signature, arms in sorted(grouped.items())
        ]
        output.sort(key=lambda item: (-item["arm_count"], item["arms"]))
        return output

    arm_metric_rows_valid = bool(
        isinstance(episode_rows, list)
        and set(metrics) == set(support_metrics)
        and all(
            isinstance(row, dict)
            and isinstance(row.get("arms"), dict)
            and set(row["arms"]) == set(metrics)
            for row in episode_rows
        )
    )
    rebuilt_metrics: dict[str, dict[str, Any]] = {}
    rebuilt_support_metrics: dict[str, dict[str, Any]] = {}
    support_rows = (
        [row for row in episode_rows if row.get("factor_support_qualified") is True]
        if isinstance(episode_rows, list)
        else []
    )
    if arm_metric_rows_valid:
        for arm_name, metric in metrics.items():
            if not isinstance(metric, dict):
                arm_metric_rows_valid = False
                break
            clock_classification = str(metric.get("clock_classification") or "")
            if clock_classification not in {
                "available_at_or_before_move_start",
                "label_derived_after_move_detection",
            }:
                arm_metric_rows_valid = False
                break
            rebuilt_legacy = rebuild_arm_metric(
                episode_rows,
                arm_name,
                clock_classification,
                "move_conditioned_alignment_diagnostic_not_trade_pnl",
            )
            rebuilt_supported = rebuild_arm_metric(
                support_rows,
                arm_name,
                clock_classification,
                (
                    "factor_support_qualified_move_conditioned_alignment_"
                    "diagnostic_not_trade_pnl"
                ),
            )
            if rebuilt_legacy is None or rebuilt_supported is None:
                arm_metric_rows_valid = False
                break
            rebuilt_metrics[arm_name] = rebuilt_legacy
            rebuilt_support_metrics[arm_name] = rebuilt_supported

    support_count = alignment.get("support_qualified_factor_episode_count")
    arm_metrics_reconciled = bool(
        arm_metric_rows_valid
        and metrics == rebuilt_metrics
        and support_metrics == rebuilt_support_metrics
        and isinstance(support_count, int)
        and not isinstance(support_count, bool)
        and support_count == len(support_rows) == qualified_selected
    )
    rebuilt_groups = rebuild_equivalence_groups(
        rebuilt_metrics, pre_move_only=False
    )
    rebuilt_pre_move_groups = rebuild_equivalence_groups(
        rebuilt_metrics, pre_move_only=True
    )
    rebuilt_support_groups = rebuild_equivalence_groups(
        rebuilt_support_metrics, pre_move_only=False
    )
    rebuilt_support_pre_move_groups = rebuild_equivalence_groups(
        rebuilt_support_metrics, pre_move_only=True
    )
    arm_equivalence_reconciled = bool(
        alignment.get("decision_equivalence_groups") == rebuilt_groups
        and alignment.get("pre_move_decision_equivalence_groups")
        == rebuilt_pre_move_groups
        and alignment.get("support_qualified_decision_equivalence_groups")
        == rebuilt_support_groups
        and alignment.get(
            "support_qualified_pre_move_decision_equivalence_groups"
        )
        == rebuilt_support_pre_move_groups
        and alignment.get("unique_decision_vector_count")
        == len(rebuilt_groups)
        and alignment.get("pre_move_unique_decision_vector_count")
        == len(rebuilt_pre_move_groups)
        and alignment.get("support_qualified_unique_decision_vector_count")
        == len(rebuilt_support_groups)
        and alignment.get(
            "support_qualified_pre_move_unique_decision_vector_count"
        )
        == len(rebuilt_support_pre_move_groups)
    )
    taxonomy_valid = False
    if isinstance(taxonomy, dict) and isinstance(episode_rows, list):
        state_counts = taxonomy.get("state_counts")
        reason_counts = taxonomy.get(
            "strict_abstention_reason_episode_counts"
        )

        def nonnegative_integer(value: Any) -> bool:
            return (
                isinstance(value, int)
                and not isinstance(value, bool)
                and value >= 0
            )

        counts_valid = (
            isinstance(state_counts, dict)
            and bool(state_counts or resolved_episode_count == 0)
            and all(
                isinstance(key, str)
                and bool(key)
                and nonnegative_integer(value)
                for key, value in state_counts.items()
            )
            and isinstance(reason_counts, dict)
            and all(
                isinstance(key, str)
                and bool(key)
                and nonnegative_integer(value)
                for key, value in reason_counts.items()
            )
        )
        retained_total = taxonomy.get("retained_forward_research_story_count")
        retained_aligned = taxonomy.get(
            "retained_forward_aligned_story_count"
        )
        retained_opposed = taxonomy.get(
            "retained_forward_opposed_story_count"
        )
        rows_valid = all(
            isinstance(row, dict)
            and isinstance(row.get("causal_gap"), dict)
            and row["causal_gap"].get("contract_id")
            == EXPECTED_MOVE_FIRST_CAUSAL_GAP_TAXONOMY_CONTRACT_ID
            and isinstance(row["causal_gap"].get("state"), str)
            and bool(row["causal_gap"].get("state"))
            and row["causal_gap"].get("interpretation")
            == "move_conditioned_source_gap_diagnostic_not_prediction_or_trade_pnl"
            and nonnegative_integer(
                row["causal_gap"].get(
                    "strict_forward_independent_story_count"
                )
            )
            and isinstance(
                row["causal_gap"].get("strict_abstention_reasons"), dict
            )
            and all(
                isinstance(key, str)
                and bool(key)
                and nonnegative_integer(value)
                for key, value in row["causal_gap"][
                    "strict_abstention_reasons"
                ].items()
            )
            and nonnegative_integer(
                row["causal_gap"].get("retained_forward_research_story_count")
            )
            and nonnegative_integer(
                row["causal_gap"].get("retained_forward_aligned_story_count")
            )
            and nonnegative_integer(
                row["causal_gap"].get("retained_forward_opposed_story_count")
            )
            and row["causal_gap"]["retained_forward_research_story_count"]
            == row["causal_gap"]["retained_forward_aligned_story_count"]
            + row["causal_gap"]["retained_forward_opposed_story_count"]
            and isinstance(
                row["causal_gap"].get("retained_forward_research_stories"),
                list,
            )
            and len(row["causal_gap"]["retained_forward_research_stories"])
            == row["causal_gap"]["retained_forward_research_story_count"]
            and all(
                isinstance(story, dict)
                and isinstance(story.get("story_key"), str)
                and bool(story.get("story_key"))
                and story.get("story_key_type")
                in {"source_event_id", "source_time_headline_sha256"}
                and isinstance(story.get("story_cluster_id"), str)
                and isinstance(story.get("story_cluster_key"), str)
                and bool(story.get("story_cluster_key"))
                and story.get("story_cluster_key_type")
                in {"story_cluster_id", "story_key_fallback"}
                and story["story_cluster_key"]
                == (
                    f"story_cluster:{story['story_cluster_id']}"
                    if story["story_cluster_id"]
                    else story["story_key"]
                )
                and isinstance(story.get("source_event_id"), str)
                and isinstance(story.get("source_id"), str)
                and isinstance(story.get("event_type"), str)
                and isinstance(story.get("headline"), str)
                and isinstance(story.get("effective_from_utc"), str)
                and isinstance(story.get("aligned"), bool)
                and isinstance(story.get("narrative_family_key"), str)
                and bool(story.get("narrative_family_key"))
                and story.get("narrative_family_contract_id")
                == EXPECTED_MOVE_FIRST_NARRATIVE_FAMILY_CONTRACT_ID
                and story.get("narrative_family_method")
                in {
                    "time_bounded_headline_similarity_component",
                    "upstream_story_cluster_or_singleton",
                }
                and isinstance(
                    story.get("narrative_family_representative_story_key"),
                    str,
                )
                and bool(
                    story.get("narrative_family_representative_story_key")
                )
                and nonnegative_integer(
                    story.get("narrative_family_component_story_count")
                )
                and story["narrative_family_component_story_count"] > 0
                and nonnegative_integer(
                    story.get(
                        "narrative_family_component_upstream_cluster_count"
                    )
                )
                and story[
                    "narrative_family_component_upstream_cluster_count"
                ]
                > 0
                and nonnegative_integer(
                    story.get("narrative_family_similarity_edge_count")
                )
                for story in row["causal_gap"][
                    "retained_forward_research_stories"
                ]
            )
            and len(
                {
                    story["story_key"]
                    for story in row["causal_gap"][
                        "retained_forward_research_stories"
                    ]
                }
            )
            == len(row["causal_gap"]["retained_forward_research_stories"])
            and sum(
                int(story["aligned"] is True)
                for story in row["causal_gap"][
                    "retained_forward_research_stories"
                ]
            )
            == row["causal_gap"]["retained_forward_aligned_story_count"]
            for row in episode_rows
        )
        rebuilt_state_counts: dict[str, int] = {}
        rebuilt_reason_counts: dict[str, int] = {}
        rebuilt_story_episode_ids: dict[str, set[str]] = {}
        rebuilt_story_aligned_links: dict[str, int] = {}
        rebuilt_story_opposed_links: dict[str, int] = {}
        rebuilt_story_metadata: dict[str, dict[str, Any]] = {}
        rebuilt_source_ids: set[str] = set()
        rebuilt_episodes_with_story = 0
        rebuilt_episodes_with_strict_independent_story = 0
        rebuilt_cluster_episode_ids: dict[str, set[str]] = {}
        rebuilt_cluster_source_event_ids: dict[str, set[str]] = {}
        rebuilt_cluster_aligned_links: dict[str, int] = {}
        rebuilt_cluster_opposed_links: dict[str, int] = {}
        rebuilt_cluster_metadata: dict[str, dict[str, Any]] = {}
        rebuilt_episodes_with_multiple_clusters = 0
        rebuilt_family_episode_ids: dict[str, set[str]] = {}
        rebuilt_family_source_event_ids: dict[str, set[str]] = {}
        rebuilt_family_source_ids: dict[str, set[str]] = {}
        rebuilt_family_cluster_keys: dict[str, set[str]] = {}
        rebuilt_family_aligned_links: dict[str, int] = {}
        rebuilt_family_opposed_links: dict[str, int] = {}
        rebuilt_family_metadata: dict[str, dict[str, Any]] = {}
        rebuilt_story_family_assignments: dict[str, tuple[Any, ...]] = {}
        rebuilt_family_assignments_consistent = True
        rebuilt_episodes_with_multiple_families = 0
        if rows_valid:
            for row in episode_rows:
                gap = row["causal_gap"]
                state = gap["state"]
                rebuilt_state_counts[state] = rebuilt_state_counts.get(state, 0) + 1
                for reason in gap["strict_abstention_reasons"]:
                    rebuilt_reason_counts[reason] = (
                        rebuilt_reason_counts.get(reason, 0) + 1
                    )
                stories = gap["retained_forward_research_stories"]
                if stories:
                    rebuilt_episodes_with_story += 1
                if gap["strict_forward_independent_story_count"] > 0:
                    rebuilt_episodes_with_strict_independent_story += 1
                episode_id = str(row.get("resolved_factor_episode_id") or "")
                row_cluster_keys: set[str] = set()
                row_family_keys: set[str] = set()
                for story in stories:
                    story_key = story["story_key"]
                    rebuilt_story_episode_ids.setdefault(story_key, set()).add(
                        episode_id
                    )
                    rebuilt_story_aligned_links[story_key] = (
                        rebuilt_story_aligned_links.get(story_key, 0)
                        + int(story["aligned"] is True)
                    )
                    rebuilt_story_opposed_links[story_key] = (
                        rebuilt_story_opposed_links.get(story_key, 0)
                        + int(story["aligned"] is not True)
                    )
                    if story["source_id"]:
                        rebuilt_source_ids.add(story["source_id"])
                    rebuilt_story_metadata.setdefault(
                        story_key,
                        {
                            "story_key": story_key,
                            "story_key_type": story["story_key_type"],
                            "source_event_id": story["source_event_id"],
                            "source_id": story["source_id"],
                            "headline": story["headline"],
                            "effective_from_utc": story[
                                "effective_from_utc"
                            ],
                        },
                    )
                    cluster_key = story["story_cluster_key"]
                    row_cluster_keys.add(cluster_key)
                    rebuilt_cluster_episode_ids.setdefault(
                        cluster_key, set()
                    ).add(episode_id)
                    if story["source_event_id"]:
                        rebuilt_cluster_source_event_ids.setdefault(
                            cluster_key, set()
                        ).add(story["source_event_id"])
                    rebuilt_cluster_aligned_links[cluster_key] = (
                        rebuilt_cluster_aligned_links.get(cluster_key, 0)
                        + int(story["aligned"] is True)
                    )
                    rebuilt_cluster_opposed_links[cluster_key] = (
                        rebuilt_cluster_opposed_links.get(cluster_key, 0)
                        + int(story["aligned"] is not True)
                    )
                    cluster_metadata = {
                        "story_cluster_key": cluster_key,
                        "story_cluster_key_type": story[
                            "story_cluster_key_type"
                        ],
                        "story_cluster_id": story["story_cluster_id"],
                        "representative_source_event_id": story[
                            "source_event_id"
                        ],
                        "representative_source_id": story["source_id"],
                        "representative_headline": story["headline"],
                        "first_effective_from_utc": story[
                            "effective_from_utc"
                        ],
                    }
                    previous_cluster_metadata = rebuilt_cluster_metadata.get(
                        cluster_key
                    )
                    if (
                        previous_cluster_metadata is None
                        or cluster_metadata["first_effective_from_utc"]
                        < previous_cluster_metadata["first_effective_from_utc"]
                    ):
                        rebuilt_cluster_metadata[cluster_key] = (
                            cluster_metadata
                        )
                    family_key = story["narrative_family_key"]
                    row_family_keys.add(family_key)
                    assignment_signature = (
                        family_key,
                        story["narrative_family_contract_id"],
                        story["narrative_family_method"],
                        story[
                            "narrative_family_representative_story_key"
                        ],
                        story["narrative_family_component_story_count"],
                        story[
                            "narrative_family_component_upstream_cluster_count"
                        ],
                        story["narrative_family_similarity_edge_count"],
                    )
                    prior_assignment = rebuilt_story_family_assignments.setdefault(
                        story_key, assignment_signature
                    )
                    if prior_assignment != assignment_signature:
                        rebuilt_family_assignments_consistent = False
                    rebuilt_family_episode_ids.setdefault(
                        family_key, set()
                    ).add(episode_id)
                    if story["source_event_id"]:
                        rebuilt_family_source_event_ids.setdefault(
                            family_key, set()
                        ).add(story["source_event_id"])
                    if story["source_id"]:
                        rebuilt_family_source_ids.setdefault(
                            family_key, set()
                        ).add(story["source_id"])
                    rebuilt_family_cluster_keys.setdefault(
                        family_key, set()
                    ).add(cluster_key)
                    rebuilt_family_aligned_links[family_key] = (
                        rebuilt_family_aligned_links.get(family_key, 0)
                        + int(story["aligned"] is True)
                    )
                    rebuilt_family_opposed_links[family_key] = (
                        rebuilt_family_opposed_links.get(family_key, 0)
                        + int(story["aligned"] is not True)
                    )
                    family_metadata = {
                        "narrative_family_key": family_key,
                        "narrative_family_contract_id": story[
                            "narrative_family_contract_id"
                        ],
                        "narrative_family_method": story[
                            "narrative_family_method"
                        ],
                        "representative_story_key": story[
                            "narrative_family_representative_story_key"
                        ],
                        "representative_source_event_id": story[
                            "source_event_id"
                        ],
                        "representative_source_id": story["source_id"],
                        "representative_headline": story["headline"],
                        "first_effective_from_utc": story[
                            "effective_from_utc"
                        ],
                        "similarity_edge_count": story[
                            "narrative_family_similarity_edge_count"
                        ],
                    }
                    previous_family_metadata = rebuilt_family_metadata.get(
                        family_key
                    )
                    if (
                        previous_family_metadata is None
                        or (
                            family_metadata["first_effective_from_utc"],
                            family_metadata["representative_story_key"],
                        )
                        < (
                            previous_family_metadata[
                                "first_effective_from_utc"
                            ],
                            previous_family_metadata[
                                "representative_story_key"
                            ],
                        )
                    ):
                        rebuilt_family_metadata[family_key] = family_metadata
                if len(row_cluster_keys) > 1:
                    rebuilt_episodes_with_multiple_clusters += 1
                if len(row_family_keys) > 1:
                    rebuilt_episodes_with_multiple_families += 1
        rebuilt_concentration: list[dict[str, Any]] = []
        for story_key, episode_ids in rebuilt_story_episode_ids.items():
            rebuilt_concentration.append(
                {
                    **rebuilt_story_metadata[story_key],
                    "factor_episode_count": len(episode_ids),
                    "aligned_factor_episode_link_count": (
                        rebuilt_story_aligned_links.get(story_key, 0)
                    ),
                    "opposed_factor_episode_link_count": (
                        rebuilt_story_opposed_links.get(story_key, 0)
                    ),
                }
            )
        rebuilt_concentration.sort(
            key=lambda item: (
                -item["factor_episode_count"],
                -item["aligned_factor_episode_link_count"],
                item["story_key"],
            )
        )
        rebuilt_dominant_count = (
            rebuilt_concentration[0]["factor_episode_count"]
            if rebuilt_concentration
            else 0
        )
        rebuilt_dominant_pct = (
            round(100.0 * rebuilt_dominant_count / resolved_episode_count, 9)
            if resolved_episode_count
            else None
        )
        rebuilt_cluster_concentration: list[dict[str, Any]] = []
        for cluster_key, episode_ids in rebuilt_cluster_episode_ids.items():
            rebuilt_cluster_concentration.append(
                {
                    **rebuilt_cluster_metadata[cluster_key],
                    "factor_episode_count": len(episode_ids),
                    "source_event_count": len(
                        rebuilt_cluster_source_event_ids.get(
                            cluster_key, set()
                        )
                    ),
                    "aligned_factor_episode_link_count": (
                        rebuilt_cluster_aligned_links.get(cluster_key, 0)
                    ),
                    "opposed_factor_episode_link_count": (
                        rebuilt_cluster_opposed_links.get(cluster_key, 0)
                    ),
                }
            )
        rebuilt_cluster_concentration.sort(
            key=lambda item: (
                -item["factor_episode_count"],
                -item["source_event_count"],
                -item["aligned_factor_episode_link_count"],
                item["story_cluster_key"],
            )
        )
        rebuilt_dominant_cluster_count = (
            rebuilt_cluster_concentration[0]["factor_episode_count"]
            if rebuilt_cluster_concentration
            else 0
        )
        rebuilt_dominant_cluster_pct = (
            round(
                100.0
                * rebuilt_dominant_cluster_count
                / resolved_episode_count,
                9,
            )
            if resolved_episode_count
            else None
        )
        rebuilt_family_concentration: list[dict[str, Any]] = []
        for family_key, episode_ids in rebuilt_family_episode_ids.items():
            rebuilt_family_concentration.append(
                {
                    **rebuilt_family_metadata[family_key],
                    "factor_episode_count": len(episode_ids),
                    "source_event_count": len(
                        rebuilt_family_source_event_ids.get(
                            family_key, set()
                        )
                    ),
                    "source_count": len(
                        rebuilt_family_source_ids.get(family_key, set())
                    ),
                    "upstream_story_cluster_count": len(
                        rebuilt_family_cluster_keys.get(family_key, set())
                    ),
                    "aligned_factor_episode_link_count": (
                        rebuilt_family_aligned_links.get(family_key, 0)
                    ),
                    "opposed_factor_episode_link_count": (
                        rebuilt_family_opposed_links.get(family_key, 0)
                    ),
                }
            )
        rebuilt_family_concentration.sort(
            key=lambda item: (
                -item["factor_episode_count"],
                -item["source_event_count"],
                -item["upstream_story_cluster_count"],
                -item["aligned_factor_episode_link_count"],
                item["narrative_family_key"],
            )
        )
        rebuilt_dominant_family_count = (
            rebuilt_family_concentration[0]["factor_episode_count"]
            if rebuilt_family_concentration
            else 0
        )
        rebuilt_dominant_family_pct = (
            round(
                100.0
                * rebuilt_dominant_family_count
                / resolved_episode_count,
                9,
            )
            if resolved_episode_count
            else None
        )
        taxonomy_valid = bool(
            counts_valid
            and nonnegative_integer(taxonomy.get("episode_count"))
            and taxonomy.get("episode_count") == resolved_episode_count
            and len(episode_rows) == resolved_episode_count
            and sum(state_counts.values()) == resolved_episode_count
            and state_counts == dict(sorted(rebuilt_state_counts.items()))
            and reason_counts == dict(sorted(rebuilt_reason_counts.items()))
            and nonnegative_integer(retained_total)
            and nonnegative_integer(retained_aligned)
            and nonnegative_integer(retained_opposed)
            and retained_total == retained_aligned + retained_opposed
            and retained_total
            == sum(
                row["causal_gap"]["retained_forward_research_story_count"]
                for row in episode_rows
            )
            and retained_aligned
            == sum(
                row["causal_gap"]["retained_forward_aligned_story_count"]
                for row in episode_rows
            )
            and retained_opposed
            == sum(
                row["causal_gap"]["retained_forward_opposed_story_count"]
                for row in episode_rows
            )
            and taxonomy.get(
                "factor_episodes_with_retained_forward_story_count"
            )
            == rebuilt_episodes_with_story
            and taxonomy.get(
                "factor_episodes_with_strict_independent_story_count"
            )
            == rebuilt_episodes_with_strict_independent_story
            and taxonomy.get("unique_retained_forward_story_count")
            == len(rebuilt_story_episode_ids)
            and taxonomy.get("unique_retained_forward_source_count")
            == len(rebuilt_source_ids)
            and taxonomy.get("unique_retained_forward_story_cluster_count")
            == len(rebuilt_cluster_episode_ids)
            and taxonomy.get(
                "factor_episodes_with_multiple_retained_story_clusters_count"
            )
            == rebuilt_episodes_with_multiple_clusters
            and taxonomy.get("narrative_family_contract_id")
            == EXPECTED_MOVE_FIRST_NARRATIVE_FAMILY_CONTRACT_ID
            and taxonomy.get(
                "unique_retained_forward_narrative_family_count"
            )
            == len(rebuilt_family_episode_ids)
            and taxonomy.get(
                "factor_episodes_with_multiple_retained_narrative_families_count"
            )
            == rebuilt_episodes_with_multiple_families
            and rebuilt_family_assignments_consistent
            and taxonomy.get("dominant_retained_story_factor_episode_count")
            == rebuilt_dominant_count
            and taxonomy.get("dominant_retained_story_factor_episode_pct")
            == rebuilt_dominant_pct
            and taxonomy.get("retained_story_concentration_top20")
            == rebuilt_concentration[:20]
            and taxonomy.get(
                "dominant_retained_story_cluster_factor_episode_count"
            )
            == rebuilt_dominant_cluster_count
            and taxonomy.get(
                "dominant_retained_story_cluster_factor_episode_pct"
            )
            == rebuilt_dominant_cluster_pct
            and taxonomy.get("retained_story_cluster_concentration_top20")
            == rebuilt_cluster_concentration[:20]
            and taxonomy.get(
                "dominant_retained_narrative_family_factor_episode_count"
            )
            == rebuilt_dominant_family_count
            and taxonomy.get(
                "dominant_retained_narrative_family_factor_episode_pct"
            )
            == rebuilt_dominant_family_pct
            and taxonomy.get(
                "retained_narrative_family_concentration_top20"
            )
            == rebuilt_family_concentration[:20]
            and rows_valid
            and taxonomy.get("interpretation")
            == "retained_pre_move_source_gap_and_syndication_concentration_diagnostic_on_move_conditioned_cases"
        )
    source_lag = (
        None
        if generated is None or capture_generated is None
        else capture_generated - generated
    )
    return bool(
        alignment.get("schema_version")
        == "move_first_live_arm_alignment_report_v1"
        and alignment.get("audit_id")
        == "move_first_live_arm_alignment_v1_20260901"
        and alignment.get("source_cohort_id")
        == "move_first_live_case_capture_v4_prospective_20260901T070500Z"
        and alignment.get(
            "selection_conditioned_on_realized_executable_move"
        )
        is True
        and alignment.get("predictive_backtest_eligible") is False
        and alignment.get("promotion_eligible") is False
        and alignment.get("research_only") is True
        and alignment.get("execution_eligible") is False
        and alignment.get("can_authorize") is False
        and alignment.get("can_place_orders") is False
        and alignment.get("execution_decision") == "no_trade"
        and alignment.get("causal_gap_taxonomy_contract_id")
        == EXPECTED_MOVE_FIRST_CAUSAL_GAP_TAXONOMY_CONTRACT_ID
        and taxonomy_valid
        and factor_support_valid
        and arm_metrics_reconciled
        and arm_equivalence_reconciled
        and int(alignment.get("arm_count") or 0) == len(metrics) == 12
        and int(alignment.get("pre_move_arm_count") or 0)
        == len(pre_move)
        == 10
        and int(alignment.get("label_derived_control_arm_count") or 0)
        == len(label_derived)
        == 2
        and 0
        <= int(alignment.get("pre_move_unique_decision_vector_count") or 0)
        <= len(pre_move)
        and 0 <= source_case_count <= capture_case_count
        and 0
        <= resolved_episode_count
        <= int(capture.get("resolved_factor_episode_count") or 0)
        and generated is not None
        and 0.0 <= cutoff_epoch - generated <= maximum_age_sec
        and source_lag is not None
        and source_lag <= maximum_source_lag_sec
    )


def move_first_operational_mapping_alignment_is_current(
    alignment: dict[str, Any],
    capture: dict[str, Any],
    *,
    cutoff_epoch: float,
    maximum_age_sec: float = 240.0,
    maximum_source_lag_sec: float = 180.0,
    expected_contract_id: str = EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_CONTRACT_ID,
    expected_cohort_id: str = EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_COHORT_ID,
    expected_story_deduplication_rule: str | None = None,
    expected_story_duplicate_count_field: str = (
        "operational_syndicated_duplicate_count"
    ),
    expected_broad_age_decay_half_life_minutes: float | None = None,
    expected_operational_clock_rule: str | None = None,
) -> bool:
    """Require fresh receipt-backed clocks on an explicitly inert sidecar."""

    generated = parse_epoch(alignment.get("generated_utc"))
    capture_generated = parse_epoch(capture.get("generated_utc"))
    cohort_start = parse_epoch(alignment.get("cohort_start_utc"))
    ledger_cutoff = parse_epoch(alignment.get("ledger_cutoff_utc"))
    factor_cutoff = parse_epoch(alignment.get("factor_merge_cutoff_utc"))
    rows = alignment.get("episode_rows")
    metrics = alignment.get("arm_metrics")
    if not isinstance(rows, list) or not isinstance(metrics, dict):
        return False
    expected_arms = {
        "legacy_strict",
        "legacy_broad",
        "receipt_backed_strict",
        "receipt_backed_broad",
        "technical_continuation_control",
    }
    episode_count = int(alignment.get("resolved_factor_episode_count") or 0)
    source_case_count = int(alignment.get("source_case_count") or 0)
    if set(metrics) != expected_arms or len(rows) != episode_count:
        return False

    metrics_valid = all(
        isinstance(metric, dict)
        and int(metric.get("episode_count") or 0) == episode_count
        and int(metric.get("signaled_episode_count") or 0)
        == int(metric.get("aligned_episode_count") or 0)
        + int(metric.get("opposed_episode_count") or 0)
        and episode_count
        == int(metric.get("aligned_episode_count") or 0)
        + int(metric.get("opposed_episode_count") or 0)
        + int(metric.get("abstained_episode_count") or 0)
        for metric in metrics.values()
    )
    allowed_statuses = {
        "admitted",
        "mapping_available_after_move_start",
        "no_current_mapping_receipt",
        "outside_operational_lookback",
    }
    allowed_contracts = {
        # This historical alignment contract remains bound to V2. V3 mapping
        # visibility requires a new consumer observation contract/cohort.
        NEWS_GOVERNANCE_FAST_LANE_LEGACY_CONTRACT_ID: "general_news_fast_lane",
        OFFICIAL_FAST_LANE_GOVERNANCE_ADAPTER_CONTRACT_ID: (
            "official_release_fast_lane"
        ),
    }
    rows_valid = True
    for row in rows:
        if not isinstance(row, dict):
            rows_valid = False
            break
        move_start = parse_epoch(row.get("move_start_utc"))
        receipts = row.get("operational_event_receipts")
        statuses = row.get("legacy_recent_story_mapping_status_counts")
        directions = row.get("directions")
        alignments = row.get("alignments")
        if (
            move_start is None
            or not isinstance(receipts, list)
            or not isinstance(statuses, dict)
            or not isinstance(directions, dict)
            or not isinstance(alignments, dict)
            or set(directions) != expected_arms
            or set(alignments) != expected_arms
            or any(
                not isinstance(value, int)
                or isinstance(value, bool)
                or value not in {-1, 0, 1}
                for value in directions.values()
            )
            or any(value not in {"aligned", "opposed", "abstained"} for value in alignments.values())
            or any(key not in allowed_statuses for key in statuses)
            or any(
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
                for value in statuses.values()
            )
            or int(row.get("operational_pre_move_event_count") or 0)
            != len(receipts)
            or not 0
            <= int(row.get("operational_independent_story_count") or 0)
            <= len(receipts)
            or row.get("source_case_bytes_rewritten") is not False
            or row.get("selection_conditioned_on_realized_executable_move")
            is not True
            or row.get("predictive_backtest_eligible") is not False
            or row.get("promotion_eligible") is not False
            or row.get("research_only") is not True
            or row.get("execution_eligible") is not False
            or row.get("can_authorize") is not False
            or row.get("can_place_orders") is not False
            or row.get("execution_decision") != "no_trade"
            or (
                expected_story_deduplication_rule is not None
                and (
                    row.get("story_deduplication_rule")
                    != expected_story_deduplication_rule
                    or int(row.get(expected_story_duplicate_count_field) or 0)
                    + int(row.get("operational_independent_story_count") or 0)
                    != len(receipts)
                )
            )
            or (
                expected_broad_age_decay_half_life_minutes is not None
                and float(row.get("broad_age_decay_half_life_minutes") or 0.0)
                != expected_broad_age_decay_half_life_minutes
            )
        ):
            rows_valid = False
            break
        receipt_ids: set[str] = set()
        for receipt in receipts:
            if not isinstance(receipt, dict):
                rows_valid = False
                break
            available = parse_epoch(receipt.get("mapping_available_utc"))
            operational = parse_epoch(
                receipt.get("operational_effective_from_utc")
            )
            contract = str(receipt.get("mapping_contract_id") or "")
            receipt_id = str(receipt.get("mapping_receipt_id") or "")
            if (
                not receipt_id
                or receipt_id in receipt_ids
                or available is None
                or operational is None
                or available > move_start
                or operational > move_start
                or contract not in allowed_contracts
                or receipt.get("mapping_kind") != allowed_contracts[contract]
            ):
                rows_valid = False
                break
            receipt_ids.add(receipt_id)
        if not rows_valid:
            break

    source_lag = (
        None
        if generated is None or capture_generated is None
        else capture_generated - generated
    )
    return bool(
        alignment.get("schema_version")
        == "move_first_operational_mapping_alignment_report_v1"
        and alignment.get("contract_id")
        == expected_contract_id
        and alignment.get("cohort_id")
        == expected_cohort_id
        and (
            expected_operational_clock_rule is None
            or alignment.get("operational_clock_rule")
            == expected_operational_clock_rule
        )
        and (
            expected_story_deduplication_rule is None
            or (
                alignment.get("story_deduplication_rule")
                == expected_story_deduplication_rule
                and int(
                    alignment.get(expected_story_duplicate_count_field) or 0
                )
                == sum(
                    int(row.get(expected_story_duplicate_count_field) or 0)
                    for row in rows
                )
            )
        )
        and (
            expected_broad_age_decay_half_life_minutes is None
            or (
                float(
                    alignment.get("broad_age_decay_half_life_minutes") or 0.0
                )
                == expected_broad_age_decay_half_life_minutes
                and int(
                    alignment.get("operational_story_family_duplicate_count")
                    or 0
                )
                == sum(
                    int(row.get("operational_story_family_duplicate_count") or 0)
                    for row in rows
                )
            )
        )
        and alignment.get("source_case_bytes_rewritten") is False
        and int(alignment.get("historical_rows_imported") or 0) == 0
        and alignment.get("selection_conditioned_on_realized_executable_move")
        is True
        and alignment.get("predictive_backtest_eligible") is False
        and alignment.get("promotion_eligible") is False
        and alignment.get("research_only") is True
        and alignment.get("execution_eligible") is False
        and alignment.get("can_authorize") is False
        and alignment.get("can_place_orders") is False
        and alignment.get("execution_decision") == "no_trade"
        and metrics_valid
        and rows_valid
        and 0 <= episode_count <= source_case_count
        and source_case_count <= int(capture.get("case_count") or 0)
        and generated is not None
        and 0.0 <= cutoff_epoch - generated <= maximum_age_sec
        and ledger_cutoff is not None
        and (
            ledger_cutoff <= generated
            or (
                cohort_start is not None
                and generated < cohort_start
                and ledger_cutoff == cohort_start
                and source_case_count == 0
                and episode_count == 0
                and int(alignment.get("receipt_backed_event_count") or 0) == 0
                and int(
                    alignment.get("receipt_backed_independent_story_count")
                    or 0
                ) == 0
            )
        )
        and factor_cutoff is not None
        and factor_cutoff <= generated
        and source_lag is not None
        and source_lag <= maximum_source_lag_sec
    )


def move_first_operational_mapping_alignment_is_preserved_baseline(
    alignment: dict[str, Any],
    capture: dict[str, Any],
    *,
    expected_contract_id: str = EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_CONTRACT_ID,
    expected_cohort_id: str = EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_COHORT_ID,
    expected_story_deduplication_rule: str | None = None,
    expected_story_duplicate_count_field: str = (
        "operational_syndicated_duplicate_count"
    ),
    expected_broad_age_decay_half_life_minutes: float | None = None,
) -> bool:
    """Validate a frozen inert baseline without requiring live freshness.

    A superseded collector's final report remains evidence, but its timestamp
    must become stale after the runtime is retired.  Requiring it to remain
    current would either create a false incident or force an obsolete process
    to keep running.  Reuse the full causal/inertness validator at the report's
    own publication clock while allowing the live capture ledger to advance.
    """

    generated = parse_epoch(alignment.get("generated_utc"))
    if generated is None:
        return False
    return move_first_operational_mapping_alignment_is_current(
        alignment,
        capture,
        cutoff_epoch=generated,
        maximum_age_sec=1.0,
        maximum_source_lag_sec=float("inf"),
        expected_contract_id=expected_contract_id,
        expected_cohort_id=expected_cohort_id,
        expected_story_deduplication_rule=expected_story_deduplication_rule,
        expected_story_duplicate_count_field=expected_story_duplicate_count_field,
        expected_broad_age_decay_half_life_minutes=(
            expected_broad_age_decay_half_life_minutes
        ),
    )


def independent_verifier_operationally_safe(
    verifier: dict[str, Any],
    lifecycle_counts: dict[str, Any],
    *,
    cutoff_epoch: float,
    current_lifecycle_fingerprint: str | None = None,
    current_lifecycle_integrity: dict[str, Any] | None = None,
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
        completed_epoch = parse_epoch(
            verifier.get("completed_verification_utc")
            or verifier.get("generated_utc")
        )
        rebuilt = verifier.get("rebuilt_lifecycle") or {}
        rebuilt_states = rebuilt.get("states") or {}
        fingerprints = verifier.get("input_fingerprints") or {}
        fingerprint_matches = bool(
            current_lifecycle_fingerprint is None
            or fingerprints.get("lifecycle_state")
            == current_lifecycle_fingerprint
        )
        semantic_integrity_matches = False
        if current_lifecycle_integrity:
            current_states = current_lifecycle_integrity.get("current_states") or {}
            expected_count = sum(int(value or 0) for value in lifecycle_counts.values())
            current_integrity_self_consistent = bool(
                current_lifecycle_integrity.get("contract")
                == "evidence_lifecycle_publication_integrity_v1"
                and current_lifecycle_integrity.get("current_state_sha256")
                and int(current_lifecycle_integrity.get("current_state_count") or 0)
                == expected_count
                and int(current_lifecycle_integrity.get("hypothesis_count") or 0)
                == expected_count
                and all(
                    int(current_states.get(state) or 0) == int(count or 0)
                    for state, count in lifecycle_counts.items()
                )
            )
            verifier_integrity = None
            for check in verifier.get("checks") or []:
                if (
                    check.get("name") == "lifecycle_publication_integrity"
                    and check.get("passed") is True
                ):
                    verifier_integrity = (check.get("evidence") or {}).get("published")
                    break
            semantic_integrity_matches = bool(
                current_integrity_self_consistent
                and isinstance(verifier_integrity, dict)
                and verifier_integrity == current_lifecycle_integrity
            )
        return bool(
            completed_epoch is not None
            and 0.0
            <= cutoff_epoch - completed_epoch
            <= maximum_previous_match_age_sec
            and (fingerprint_matches or semantic_integrity_matches)
            and int(rebuilt.get("hypothesis_count") or 0)
            == sum(int(value or 0) for value in lifecycle_counts.values())
            and all(
                int(rebuilt_states.get(state) or 0) == int(count or 0)
                for state, count in lifecycle_counts.items()
            )
            and verifier.get("can_place_orders") is False
            and verifier.get("can_promote") is False
            and verifier.get("real_money_routing") is False
            and verifier.get("research_only") is True
            and verifier.get("independent_implementation") is True
            and not (verifier.get("production_calculator_imports") or [])
            and not (verifier.get("failed_checks") or [])
        )
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


def lifecycle_genealogy_sync_is_current(lifecycle: dict[str, Any]) -> bool:
    """Require the evidence publisher's targeted genealogy handoff.

    Exact database equality remains independently checked by the verifier. This
    smaller contract check prevents an older lifecycle publisher that omits the
    post-ingest handoff from being treated as the current operational path.
    """

    sync = lifecycle.get("genealogy_sync")
    if not isinstance(sync, dict):
        return False
    try:
        lifecycle_count = int(sync.get("lifecycle_hypotheses"))
        genealogy_count = int(sync.get("genealogy_governed_cells"))
    except (TypeError, ValueError):
        return False
    return bool(
        sync.get("ok") is True
        and sync.get("status") == "synchronized"
        and sync.get("research_only") is True
        and sync.get("can_place_orders") is False
        and sync.get("can_promote") is False
        and lifecycle_count == genealogy_count
        and lifecycle_count
        == int((lifecycle.get("lifecycle") or {}).get("hypothesis_count") or -1)
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
    """Require current observations to retain source lineage and honest trust."""
    state = read_json(path)
    articles = state.get("articles")
    if not isinstance(articles, list):
        return {
            "status": "missing_or_invalid",
            "article_count": 0,
            "direct_article_count": 0,
            "lineage_bound_article_count": 0,
            "missing_lineage_count": 0,
            "invalid_rows": [],
            "invalid_row_count": 0,
            "ok": False,
        }
    direct_count = 0
    lineage_bound_count = 0
    missing_lineage_count = 0
    invalid: list[dict[str, str]] = []
    for raw in articles:
        if not isinstance(raw, dict):
            continue
        source_contract_id = str(raw.get("source_contract_id") or "").strip()
        source_cohort_id = str(raw.get("source_cohort_id") or "").strip()
        if source_contract_id and source_cohort_id:
            lineage_bound_count += 1
        else:
            missing_lineage_count += 1
            invalid.append(
                {
                    "event_id": str(raw.get("event_id") or ""),
                    "source_id": str(raw.get("source_id") or ""),
                    "headline": str(raw.get("headline") or ""),
                    "reason": "missing_source_contract_or_cohort",
                }
            )
        if raw.get("source_direct") is not True:
            continue
        direct_count += 1
        if raw.get("source_verified") is not True:
            invalid.append(
                {
                    "event_id": str(raw.get("event_id") or ""),
                    "source_id": str(raw.get("source_id") or ""),
                    "headline": str(raw.get("headline") or ""),
                    "reason": "unverified_source_labeled_direct",
                }
            )
    if not invalid:
        status = "ok"
    elif missing_lineage_count:
        status = "missing_source_lineage"
    else:
        status = "unverified_source_labeled_direct"
    return {
        "status": status,
        "generated_utc": state.get("generated_utc"),
        "article_count": len(articles),
        "direct_article_count": direct_count,
        "lineage_bound_article_count": lineage_bound_count,
        "missing_lineage_count": missing_lineage_count,
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


def integrity_runtime_contract() -> dict[str, str]:
    """Expose the contract identity loaded by this long-lived audit process."""

    return {"classification_version": CURRENT_NEWS_CLASSIFICATION_VERSION}


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


def executable_move_census_current_integrity(
    state: dict[str, Any],
    verifier: dict[str, Any],
    *,
    cutoff_epoch: float,
    maximum_age_sec: float = 180.0,
) -> dict[str, Any]:
    """Require the current all-68 census and independent verifier to agree."""

    state_epoch = parse_epoch(state.get("generated_utc"))
    verifier_epoch = parse_epoch(verifier.get("generated_utc"))
    verifier_finished_epoch = parse_epoch(verifier.get("audit_finished_utc"))
    verifier_latest_epoch = parse_epoch(verifier.get("latest_generated_utc"))
    state_age = None if state_epoch is None else cutoff_epoch - state_epoch
    verifier_age = (
        None
        if verifier_finished_epoch is None
        else cutoff_epoch - verifier_finished_epoch
    )
    verifier_duration = (
        None
        if verifier_epoch is None or verifier_finished_epoch is None
        else verifier_finished_epoch - verifier_epoch
    )
    state_frames = int(state.get("frame_count") or 0)
    verified_database_frames = int(
        (verifier.get("counts") or {}).get("frames") or 0
    )
    verified_latest_frames = int(
        verifier.get("latest_frame_count")
        if verifier.get("latest_frame_count") is not None
        else verified_database_frames
    )
    schedule_census = state.get("schedule_census") or {}
    preactivation_state_ok = bool(
        state_frames == 0
        and state.get("status") == "collecting_pre_activation"
        and not state.get("latest_frame_utc")
    )
    active_state_ok = bool(
        state_frames > 0
        and "status" not in state
        and parse_epoch(state.get("latest_frame_utc")) is not None
        and int(schedule_census.get("observed_frames") or 0)
        == int(schedule_census.get("expected_open_frames") or 0)
        and int(schedule_census.get("observed_frames") or 0) <= state_frames
        and int(schedule_census.get("missing_open_frames") or 0) == 0
    )
    state_mode_ok = bool(preactivation_state_ok or active_state_ok)
    contract_ok = bool(
        state.get("schema_version") == "executable_move_census_latest_v1"
        and state.get("cohort_id")
        == EXPECTED_EXECUTABLE_MOVE_CENSUS_COHORT
        and state_mode_ok
        and int(state.get("instrument_count") or 0) == 68
        and int(state.get("side_count") or 0) == 136
        and verifier.get("schema_version")
        == "executable_move_census_verifier_v1"
        and verifier.get("cohort_id")
        == EXPECTED_EXECUTABLE_MOVE_CENSUS_COHORT
    )
    inert_ok = bool(
        state.get("research_only") is True
        and state.get("can_trade") is False
        and state.get("can_authorize") is False
        and state.get("can_promote") is False
        and verifier.get("research_only") is True
        and verifier.get("can_trade") is False
        and verifier.get("can_authorize") is False
        and verifier.get("can_promote") is False
        and verifier.get("supported_execution_decision") == "no_trade"
    )
    freshness_ok = bool(
        state_age is not None
        and verifier_age is not None
        and verifier_duration is not None
        and -5.0 <= state_age <= maximum_age_sec
        and -5.0 <= verifier_age <= maximum_age_sec
        and -5.0 <= verifier_duration <= 900.0
        and verifier_latest_epoch is not None
        and verifier_epoch is not None
        and verifier_latest_epoch <= verifier_epoch + 5.0
        and state_epoch is not None
        and state_epoch >= verifier_latest_epoch - 5.0
    )
    counts_ok = bool(
        state_frames >= verified_latest_frames
        and verified_database_frames >= verified_latest_frames
        and int((verifier.get("counts") or {}).get("frame_quotes") or 0)
        == 68 * verified_database_frames
    )
    semantic = verifier.get("semantic_validity") or {}
    semantic_total = int(semantic.get("valid_quote_rows") or 0) + int(
        semantic.get("invalid_quote_rows") or 0
    )
    semantic_ok = bool(
        int(semantic.get("source_identity_mismatch_rows") or 0) == 0
        and int(semantic.get("zero_valid_open_market_frames") or 0) == 0
        and semantic_total == 68 * verified_database_frames
        and (
            verified_database_frames == 0
            or int(semantic.get("valid_quote_rows") or 0) > 0
        )
    )
    verifier_ok = bool(
        verifier.get("verified") is True
        and int(verifier.get("failure_count") or 0) == 0
        and not (verifier.get("failures") or [])
    )
    return {
        "ok": bool(
            contract_ok
            and inert_ok
            and freshness_ok
            and counts_ok
            and semantic_ok
            and verifier_ok
        ),
        "contract_ok": contract_ok,
        "state_mode_ok": state_mode_ok,
        "preactivation_state_ok": preactivation_state_ok,
        "active_state_ok": active_state_ok,
        "inert_ok": inert_ok,
        "freshness_ok": freshness_ok,
        "counts_ok": counts_ok,
        "semantic_ok": semantic_ok,
        "semantic_validity": semantic,
        "verifier_ok": verifier_ok,
        "state_age_sec": state_age,
        "verifier_age_sec": verifier_age,
        "verifier_duration_sec": verifier_duration,
        "state_frames": state_frames,
        "verified_frames": verified_latest_frames,
        "verified_database_frames": verified_database_frames,
        "verified_latest_frames": verified_latest_frames,
        "database_append_suffix_frames": (
            verified_database_frames - verified_latest_frames
        ),
        "verification_lag_frames": state_frames - verified_latest_frames,
        "cohort_id": state.get("cohort_id"),
    }


# Compatibility name for focused tests and historical tooling.  The function
# validates whichever prospective cohort is bound by the current constants.
executable_move_census_v2e_integrity = executable_move_census_current_integrity


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
    state: dict[str, Any],
    *,
    database_path: Path,
    full_database_integrity_attestation: dict[str, Any] | None = None,
    cutoff_epoch: float | None = None,
    maximum_full_integrity_age_sec: float = (
        SOURCE_GOVERNANCE_FULL_INTEGRITY_MAX_AGE_SEC
    ),
) -> dict[str, Any]:
    """Verify the current adapter and a bounded full-database attestation.

    Current adapter rows and every referenced source-event row are checked on
    every project audit. A successful full scan of the multi-gigabyte shared
    registry remains mandatory, but is explicitly attested for a bounded six
    hours rather than making every operational snapshot stale at publication.
    """

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
        "adapter_table_integrity": "missing",
        "full_database_integrity": "missing",
        "full_database_integrity_attestation": {},
        "full_database_integrity_age_sec": None,
        "full_database_check_performed": False,
        "database_integrity_scope": (
            "bounded_full_quick_check_attestation_plus_current_adapter_table_"
            "and_cross_table_receipt_validation"
        ),
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
        cutoff = float(cutoff_epoch if cutoff_epoch is not None else time.time())
        resolved_database = str(database_path.resolve())
        connection = sqlite3.connect(
            f"file:{database_path.resolve().as_posix()}?mode=ro",
            uri=True,
            timeout=5.0,
        )
        connection.execute("PRAGMA query_only=ON")
        current_metadata = {
            "database_path": resolved_database,
            "page_size": int(connection.execute("PRAGMA page_size").fetchone()[0]),
            "page_count": int(connection.execute("PRAGMA page_count").fetchone()[0]),
            "schema_version": int(
                connection.execute("PRAGMA schema_version").fetchone()[0]
            ),
            "user_version": int(
                connection.execute("PRAGMA user_version").fetchone()[0]
            ),
        }
        diagnostics["adapter_table_integrity"] = str(
            connection.execute(
                "PRAGMA quick_check('official_fast_lane_governance_imports')"
            ).fetchone()[0]
        )
        supplied = (
            dict(full_database_integrity_attestation)
            if isinstance(full_database_integrity_attestation, dict)
            else {}
        )
        supplied_checked_epoch = parse_epoch(supplied.get("checked_utc"))
        supplied_age_sec = (
            None
            if supplied_checked_epoch is None
            else max(0.0, cutoff - supplied_checked_epoch)
        )
        legacy_migration = supplied.get("legacy_completed_scan_migrated") is True
        metadata_matches = bool(
            supplied.get("database_path") == resolved_database
            and (
                legacy_migration
                or (
                    int(supplied.get("page_size", -1))
                    == current_metadata["page_size"]
                    and int(supplied.get("schema_version", -1))
                    == current_metadata["schema_version"]
                    and int(supplied.get("user_version", -1))
                    == current_metadata["user_version"]
                    and current_metadata["page_count"]
                    >= int(supplied.get("page_count", -1))
                )
            )
        )
        supplied_is_current = bool(
            supplied.get("contract_id")
            == SOURCE_GOVERNANCE_FULL_INTEGRITY_ATTESTATION_CONTRACT_ID
            and supplied.get("result") == "ok"
            and supplied_age_sec is not None
            and supplied_age_sec <= maximum_full_integrity_age_sec
            and metadata_matches
        )
        if supplied_is_current:
            attestation = {
                **supplied,
                **current_metadata,
                # A legacy completed scan is accepted exactly once.  This
                # publication enriches it with current database metadata; all
                # later reuse must satisfy the normal page/schema binding.
                "legacy_completed_scan_migrated": False,
                "migration_source": (
                    "pre_attestation_full_quick_check"
                    if legacy_migration
                    else str(supplied.get("migration_source") or "")
                ),
            }
        else:
            diagnostics["full_database_check_performed"] = True
            full_result = str(
                connection.execute("PRAGMA quick_check").fetchone()[0]
            )
            checked_utc = datetime.now(timezone.utc).isoformat()
            attestation = {
                "contract_id": (
                    SOURCE_GOVERNANCE_FULL_INTEGRITY_ATTESTATION_CONTRACT_ID
                ),
                **current_metadata,
                "checked_utc": checked_utc,
                "result": full_result,
                "legacy_completed_scan_migrated": False,
            }
        diagnostics["full_database_integrity_attestation"] = attestation
        diagnostics["full_database_integrity"] = str(
            attestation.get("result") or "missing"
        )
        attested_epoch = parse_epoch(attestation.get("checked_utc"))
        diagnostics["full_database_integrity_age_sec"] = (
            None
            if attested_epoch is None
            else max(0.0, cutoff - attested_epoch)
        )
        diagnostics["database_integrity"] = (
            "ok"
            if diagnostics["adapter_table_integrity"] == "ok"
            and diagnostics["full_database_integrity"] == "ok"
            and diagnostics["full_database_integrity_age_sec"] is not None
            and diagnostics["full_database_integrity_age_sec"]
            <= maximum_full_integrity_age_sec
            else "error"
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
        and diagnostics.get("adapter_table_integrity") == "ok"
        and diagnostics.get("full_database_integrity") == "ok"
        and diagnostics.get("full_database_integrity_age_sec") is not None
        and diagnostics.get("full_database_integrity_age_sec")
        <= maximum_full_integrity_age_sec
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


def news_source_governance_fast_lane_integrity(
    state: dict[str, Any], *, database_path: Path,
    cutoff_epoch: float | None = None, maximum_age_sec: float = 150.0,
) -> dict[str, Any]:
    """Verify V3 committed input/visibility attestations without granting entry proof."""
    cutoff = cutoff_epoch if cutoff_epoch is not None else time.time()
    generated = parse_epoch(state.get("generated_utc"))
    activated = NEWS_GOVERNANCE_FAST_LANE_ACTIVATED_UTC.isoformat()
    waiting = cutoff < NEWS_GOVERNANCE_FAST_LANE_ACTIVATED_UTC.timestamp()
    age = None if generated is None else cutoff-generated
    sequence = state.get("committed_batch_seq")
    cursor = state.get("next_scan_cursor_rowid")
    cutoff_valid = bool(waiting or (isinstance(sequence,int) and not isinstance(sequence,bool)
        and sequence>0 and isinstance(cursor,int) and not isinstance(cursor,bool) and cursor>=0))
    state_contract_ok = bool(
        state.get("schema_version")==3
        and state.get("contract_id")==NEWS_GOVERNANCE_FAST_LANE_CONTRACT_ID
        and state.get("cohort_id")==NEWS_GOVERNANCE_FAST_LANE_COHORT_ID
        and state.get("activated_utc")==activated
        and state.get("status") == ("waiting_for_activation" if waiting else "ok")
        and state.get("research_only") is True and state.get("execution_eligible") is False
        and state.get("can_place_orders") is False and state.get("can_promote") is False
        and state.get("can_authorize") is False and state.get("real_money_routing") is False
        and state.get("supported_decision")=="no_trade"
        and state.get("scan_clock")=="committed_article_rowid_with_identity_anchor"
        and state.get("revision_handling")=="deferred_to_full_source_governance_reconciliation"
        and state.get("operational_view")=="source_events_fast_mapped_v3"
        and state.get("availability_basis")=="post_commit_independent_mapping_read"
        and state.get("consumer_first_observation_required") is True
        and (waiting or (state.get("transaction_committed") is True and
            state.get("sqlite_integrity")=="deferred_to_shared_governance_integrity"))
        and age is not None and 0<=age<=maximum_age_sec and cutoff_valid)
    diagnostics: dict[str,Any] = {
        "ok":False,"status":str(state.get("status") or "missing"),
        "state_contract_ok":state_contract_ok,"state_age_sec":age,
        "state_snapshot_cutoff_valid":cutoff_valid,"state_snapshot_batch_seq":sequence,
        "state_snapshot_cutoff_utc":state.get("scan_started_utc"),
        "database_integrity":"not_checked" if waiting else "missing",
        "schema_ok":waiting,"append_only_triggers_present":waiting,
        "operational_view_present":waiting,"state_count_reconciled":waiting,
        "consumer_observation_required":True,"prospective_entry_proof_granted":False,
    }
    counts=("receipt_count","database_current_receipt_count","receipts_after_state_cutoff",
        "retained_invalid_prior_receipt_count","retained_legacy_receipt_count",
        "unknown_contract_receipt_count","preactivation_receipts","optimistic_clock_receipts",
        "input_clock_violations","contract_or_inert_violations",
        "orphan_or_mismatched_source_events","operational_clock_violations",
        "checkpoint_violations","visibility_violations","pending_batch_count")
    diagnostics.update({key:0 if waiting else -1 for key in counts})
    if waiting:
        diagnostics["ok"]=state_contract_ok
        return diagnostics
    connection=None
    try:
        connection=sqlite3.connect(f"file:{database_path.resolve().as_posix()}?mode=ro",uri=True,timeout=5.0)
        connection.execute("BEGIN")
        connection.execute("SELECT 1 FROM source_events LIMIT 1").fetchone()
        diagnostics["database_integrity"]="covered_by_shared_source_governance_integrity"
        required={
            "news_fast_lane_batches_v3":{"batch_seq","contract_id","cohort_id","input_identity","previous_rowid","input_rowid","input_anchor_event_id","scan_started_utc","statistics_json"},
            "news_fast_lane_mappings_v3":{"source_event_id","batch_seq","source_id","provider_event_id","raw_payload_sha256","input_rowid","input_first_seen_utc","input_last_seen_utc","research_only","execution_eligible","can_authorize"},
            "news_fast_lane_visibility_v3":{"batch_seq","mapping_visible_utc","availability_basis","consumer_first_observation_required"},
        }
        diagnostics["schema_ok"]=all(columns <= {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")} for table,columns in required.items())
        objects={str(row[0]):str(row[1]) for row in connection.execute("SELECT name,type FROM sqlite_master")}
        diagnostics["append_only_triggers_present"]=all(objects.get(f"{table}_no_{action}")=="trigger" for table in required for action in ("update","delete"))
        diagnostics["operational_view_present"]=objects.get("source_events_fast_mapped_v3")=="view"
        if not diagnostics["schema_ok"]:
            return diagnostics
        def count(sql: str, parameters: tuple = ()) -> int:
            return int(connection.execute(sql,parameters).fetchone()[0])
        diagnostics["database_current_receipt_count"]=count("SELECT COUNT(*) FROM news_fast_lane_mappings_v3")
        diagnostics["receipt_count"]=count("SELECT COUNT(*) FROM news_fast_lane_mappings_v3 WHERE batch_seq<=?",(sequence or 0,))
        diagnostics["receipts_after_state_cutoff"]=diagnostics["database_current_receipt_count"]-diagnostics["receipt_count"]
        diagnostics["retained_invalid_prior_receipt_count"]=count("SELECT COUNT(*) FROM news_fast_lane_import_receipts WHERE adapter_contract_id=?",(NEWS_GOVERNANCE_FAST_LANE_PRIOR_CONTRACT_ID,))
        diagnostics["retained_legacy_receipt_count"]=count("SELECT COUNT(*) FROM news_fast_lane_import_receipts WHERE adapter_contract_id=?",("news_source_governance_fast_lane_v2_first_seen_prospective_20260901",))
        diagnostics["unknown_contract_receipt_count"]=count("SELECT COUNT(*) FROM news_fast_lane_batches_v3 WHERE contract_id<>? OR cohort_id<>?",(NEWS_GOVERNANCE_FAST_LANE_CONTRACT_ID,NEWS_GOVERNANCE_FAST_LANE_COHORT_ID))
        diagnostics["preactivation_receipts"]=count("SELECT COUNT(*) FROM news_fast_lane_mappings_v3 WHERE julianday(input_first_seen_utc)<julianday(?)",(activated,))
        diagnostics["input_clock_violations"]=count("SELECT COUNT(*) FROM news_fast_lane_mappings_v3 WHERE julianday(input_first_seen_utc) IS NULL OR julianday(input_last_seen_utc) IS NULL OR julianday(input_last_seen_utc)<julianday(input_first_seen_utc)")
        diagnostics["optimistic_clock_receipts"]=count("SELECT COUNT(*) FROM news_fast_lane_mappings_v3 AS mapping JOIN news_fast_lane_visibility_v3 USING(batch_seq) WHERE julianday(mapping_visible_utc)<julianday(input_first_seen_utc)")
        diagnostics["contract_or_inert_violations"]=count("SELECT COUNT(*) FROM news_fast_lane_mappings_v3 WHERE research_only<>1 OR execution_eligible<>0 OR can_authorize<>0")
        diagnostics["orphan_or_mismatched_source_events"]=count("SELECT COUNT(*) FROM news_fast_lane_mappings_v3 AS mapping LEFT JOIN source_events AS event USING(source_event_id) LEFT JOIN news_fast_lane_batches_v3 AS batch USING(batch_seq) WHERE event.source_event_id IS NULL OR batch.batch_seq IS NULL OR event.source_id<>mapping.source_id OR event.provider_event_id<>mapping.provider_event_id OR event.raw_payload_sha256<>mapping.raw_payload_sha256 OR mapping.input_rowid<=batch.previous_rowid OR mapping.input_rowid>batch.input_rowid")
        diagnostics["checkpoint_violations"]=count("SELECT COUNT(*) FROM (SELECT *,LAG(input_rowid,1,0) OVER (ORDER BY batch_seq) AS prior_cursor,LAG(input_identity,1,input_identity) OVER (ORDER BY batch_seq) AS prior_identity FROM news_fast_lane_batches_v3) WHERE previous_rowid<>prior_cursor OR input_rowid<previous_rowid OR input_identity<>prior_identity OR (input_rowid>0 AND COALESCE(input_anchor_event_id,'')='')")
        diagnostics["visibility_violations"]=count("SELECT COUNT(*) FROM news_fast_lane_visibility_v3 AS visibility LEFT JOIN news_fast_lane_batches_v3 AS batch USING(batch_seq) WHERE batch.batch_seq IS NULL OR julianday(mapping_visible_utc) IS NULL OR julianday(mapping_visible_utc)<julianday(scan_started_utc) OR availability_basis<>'post_commit_independent_mapping_read' OR consumer_first_observation_required<>1")
        diagnostics["pending_batch_count"]=count("SELECT COUNT(*) FROM news_fast_lane_batches_v3 AS batch LEFT JOIN news_fast_lane_visibility_v3 AS visibility USING(batch_seq) WHERE visibility.batch_seq IS NULL AND batch.batch_seq<=?",(sequence or 0,))
        if diagnostics["operational_view_present"]:
            diagnostics["operational_clock_violations"]=count("SELECT COUNT(*) FROM source_events_fast_mapped_v3 WHERE operational_effective_from_utc IS NOT NULL OR consumer_first_observation_required<>1 OR availability_basis<>'post_commit_independent_mapping_read'")
        stored=connection.execute("SELECT input_rowid,input_identity,input_anchor_event_id,mapping_visible_utc FROM news_fast_lane_batches_v3 JOIN news_fast_lane_visibility_v3 USING(batch_seq) WHERE batch_seq=?",(sequence or 0,)).fetchone()
        diagnostics["state_count_reconciled"]=bool(stored and stored[0]==cursor and stored[1]==state.get("input_identity") and stored[2]==state.get("input_anchor_event_id") and stored[3]==state.get("mapping_visible_utc")
            and diagnostics["receipt_count"]==int(state.get("total_receipt_count") or 0)
            and diagnostics["retained_invalid_prior_receipt_count"]==int(state.get("retained_invalid_prior_receipt_count") or 0)
            and diagnostics["retained_legacy_receipt_count"]==int(state.get("retained_legacy_receipt_count") or 0))
    except (OSError,sqlite3.Error,TypeError,ValueError) as error:
        diagnostics["database_error"]=type(error).__name__
    finally:
        if connection is not None:
            connection.close()
    violations=("unknown_contract_receipt_count","preactivation_receipts","optimistic_clock_receipts","input_clock_violations",
        "contract_or_inert_violations","orphan_or_mismatched_source_events","operational_clock_violations",
        "checkpoint_violations","visibility_violations","pending_batch_count")
    diagnostics["ok"]=bool(state_contract_ok and diagnostics["schema_ok"] and diagnostics["append_only_triggers_present"]
        and diagnostics["operational_view_present"] and diagnostics["state_count_reconciled"]
        and all(diagnostics[key]==0 for key in violations))
    return diagnostics


def synchronized_quote_audit(quotes: dict[str, Any], *, cutoff_epoch: float) -> dict[str, Any]:
    rows: list[tuple[str, float, float, float, int]] = []
    future: list[str] = []
    invalid: list[str] = []
    tradeable: list[str] = []
    non_tradeable: list[str] = []
    tradeability_unknown: list[str] = []
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
        raw_tradeable = raw.get("tradeable")
        if raw_tradeable is True:
            tradeable.append(str(instrument))
            tradeability_code = 1
        elif raw_tradeable is False:
            non_tradeable.append(str(instrument))
            tradeability_code = 0
        else:
            tradeability_unknown.append(str(instrument))
            tradeability_code = -1
        rows.append((str(instrument), epoch, bid, ask, tradeability_code))
    rows.sort(key=lambda row: row[0])
    canonical = json.dumps(rows, separators=(",", ":"), ensure_ascii=True)
    epochs = [row[1] for row in rows]
    return {
        "valid_instruments": len(rows),
        "invalid_instruments": sorted(invalid),
        "future_instruments": sorted(future),
        "tradeable_instruments": sorted(tradeable),
        "non_tradeable_instruments": sorted(non_tradeable),
        "tradeability_unknown_instruments": sorted(tradeability_unknown),
        "tradeable_instrument_count": len(tradeable),
        "non_tradeable_instrument_count": len(non_tradeable),
        "tradeability_unknown_count": len(tradeability_unknown),
        "maximum_quote_age_sec": max((cutoff_epoch-e for e in epochs), default=None),
        "cross_pair_event_time_dispersion_sec": max(epochs)-min(epochs) if epochs else None,
        "snapshot_sha256": "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        "ingestion_order_invariant": True,
    }


def quote_tradeability_contract_current(
    quote_audit: dict[str, Any],
    quote_state: dict[str, Any],
) -> bool:
    """Reconcile OANDA price status without treating a halt as feed silence."""

    coverage = quote_state.get("coverage") or {}
    current = int(coverage.get("current_quote_count") or 0)
    current_tradeable = int(
        coverage.get("current_tradeable_quote_count") or 0
    )
    current_non_tradeable = int(
        coverage.get("current_non_tradeable_quote_count") or 0
    )
    current_unknown = int(
        coverage.get("current_tradeability_unknown_count") or 0
    )
    if (
        int(quote_state.get("schema_version") or 0) < 3
        or coverage.get("tradeability_contract")
        != EXPECTED_QUOTE_TRADEABILITY_CONTRACT
        or current_tradeable + current_non_tradeable + current_unknown
        != current
        or current_unknown != 0
        or int(quote_audit.get("tradeability_unknown_count") or 0) != 0
    ):
        return False

    retained = int(coverage.get("retained_last_known_count") or 0)
    if retained == 0 and current == int(quote_state.get("quote_count") or 0):
        expected_non_tradeable = sorted(
            str(value)
            for value in (
                coverage.get("current_non_tradeable_instruments") or []
            )
        )
        return bool(
            current_tradeable
            == int(quote_audit.get("tradeable_instrument_count") or 0)
            and current_non_tradeable
            == int(quote_audit.get("non_tradeable_instrument_count") or 0)
            and expected_non_tradeable
            == sorted(quote_audit.get("non_tradeable_instruments") or [])
        )
    return True


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
        and quote_tradeability_contract_current(quote_audit, quote_state)
        and snapshot_age is not None
        and snapshot_age <= maximum_snapshot_age_sec
    )


def capture_current_quote_snapshot(
    path: Path = STATE / "practice_007_market_quotes_v1.json",
) -> tuple[dict[str, Any], datetime]:
    """Read the atomic quote cache and bind its audit cutoff to that read.

    The full project audit loads several large evidence artifacts before it
    evaluates quote routability.  Reading the quote cache at the beginning of
    that pass made a healthy snapshot look stale by the time the audit cutoff
    was assigned.  Keep the 15-second transport gate strict while measuring
    it from a cutoff captured immediately after this lightweight read.
    """

    state = read_json(path)
    return state, datetime.now(timezone.utc)


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
    full_database_integrity_attestation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Require the V12 meter to expose only sealed immutable proof rows.

    Seal completeness is evaluated at the meter snapshot's own knowledge time.
    Comparing an older snapshot with a newer database/seal boundary creates a
    false one-bucket lag whenever the producer and this audit interleave.
    """

    generated_epoch = parse_epoch(state.get("generated_utc"))
    knowledge_cutoff_epoch = (
        generated_epoch if generated_epoch is not None else cutoff_epoch
    )
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
    full_attestation: dict[str, Any] = {}
    full_integrity_age_sec: float | None = None
    integrity_mode = "unavailable"
    connection: sqlite3.Connection | None = None
    try:
        connection = _open_readonly_evidence_database(database_path)
        connection.execute("BEGIN")
        (
            database_integrity,
            full_attestation,
            full_integrity_age_sec,
            integrity_mode,
        ) = resolve_component_full_integrity(
            connection,
            supplied_attestation=full_database_integrity_attestation,
            database_path=database_path,
            table_name="bucket_seals",
            cutoff_epoch=cutoff_epoch,
        )
        contract_row = connection.execute(
            "SELECT meter_contract_id,bucket_minutes,seal_grace_seconds,research_only,"
            "execution_eligible FROM meter_contract_registry"
        ).fetchone()
        bucket_minutes = int(contract_row[1]) if contract_row else 0
        grace_seconds = int(contract_row[2]) if contract_row else -1
        expected_sealed_clock = latest_sealable_clock(
            knowledge_cutoff_epoch,
            bucket_minutes=bucket_minutes,
            grace_seconds=grace_seconds,
        )
        expected_text = (
            expected_sealed_clock.isoformat() if expected_sealed_clock else ""
        )
        cutoff_text = datetime.fromtimestamp(
            knowledge_cutoff_epoch, timezone.utc
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
                "WHERE meter_contract_id=? "
                "AND julianday(detected_utc)<=julianday(?)",
                (CONTINUOUS_NARRATIVE_METER_CONTRACT, cutoff_text),
            ).fetchone()[0]
        )
        seal_gap_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM seal_gap_events "
                "WHERE meter_contract_id=? "
                "AND julianday(detected_utc)<=julianday(?)",
                (CONTINUOUS_NARRATIVE_METER_CONTRACT, cutoff_text),
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
        "full_database_integrity_attestation": full_attestation,
        "full_database_integrity_age_sec": full_integrity_age_sec,
        "integrity_mode": integrity_mode,
        "count_basis": (
            "database_seals_and_incidents_at_or_before_snapshot_generated_utc"
        ),
        "snapshot_generated_utc": state.get("generated_utc"),
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


def event_technical_preflight_source_integrity(
    state: dict[str, Any],
    *,
    cutoff_epoch: float,
    maximum_age_sec: float = 180.0,
) -> dict[str, Any]:
    """Require explicit, internally reconciled policy-release transport truth."""

    generated_epoch = parse_epoch(state.get("generated_utc"))
    age = None if generated_epoch is None else cutoff_epoch - generated_epoch
    rows = [row for row in state.get("events") or [] if isinstance(row, dict)]
    policy_rows = [row for row in rows if row.get("policy_event") is True]
    valid_states = {
        "direct_release_ready",
        "fallback_only_direct_release_unavailable",
        "direct_release_unavailable_no_operational_fallback",
        "no_registered_direct_release_source",
    }
    direct_ready = sum(
        1
        for row in policy_rows
        if row.get("policy_release_transport_state") == "direct_release_ready"
    )
    fallback_only = sum(
        1
        for row in policy_rows
        if row.get("policy_release_transport_state")
        == "fallback_only_direct_release_unavailable"
    )
    blocked = sum(
        1
        for row in policy_rows
        if row.get("policy_release_transport_state")
        in {
            "direct_release_unavailable_no_operational_fallback",
            "no_registered_direct_release_source",
        }
    )
    row_semantics_ok = all(
        row.get("policy_release_transport_state") in valid_states
        and (
            bool(row.get("policy_direct_release_transport_available"))
            == (row.get("policy_release_transport_state") == "direct_release_ready")
        )
        and (
            row.get("policy_release_transport_state")
            != "fallback_only_direct_release_unavailable"
            or (
                row.get("policy_fallback_transport_available") is True
                and row.get("policy_direct_release_transport_available") is False
            )
        )
        for row in policy_rows
    )
    counts_ok = bool(
        int(state.get("policy_direct_release_ready_rows") or 0) == direct_ready
        and int(state.get("policy_fallback_only_rows") or 0) == fallback_only
        and int(state.get("policy_release_transport_blocked_rows") or 0) == blocked
        and direct_ready + fallback_only + blocked == len(policy_rows)
    )
    contract_ok = bool(
        state.get("contract_id") == EVENT_TECHNICAL_PREFLIGHT_CONTRACT_ID
        and state.get("schema_version") == "event_technical_preflight_v1"
    )
    inert_ok = bool(
        state.get("research_only") is True
        and state.get("execution_eligible") is False
        and (state.get("policy") or {}).get("can_place_orders") is False
        and (state.get("policy") or {}).get("can_promote") is False
        and (state.get("policy") or {}).get(
            "calendar_readiness_is_not_release_transport_readiness"
        )
        is True
        and (state.get("policy") or {}).get(
            "publisher_search_fallback_is_not_direct_release_transport"
        )
        is True
        and all(row.get("execution_eligible") is False for row in rows)
    )
    freshness_ok = bool(age is not None and -5.0 <= age <= maximum_age_sec)
    return {
        "ok": bool(
            contract_ok
            and inert_ok
            and freshness_ok
            and row_semantics_ok
            and counts_ok
            and policy_rows
        ),
        "contract_ok": contract_ok,
        "inert_ok": inert_ok,
        "freshness_ok": freshness_ok,
        "row_semantics_ok": row_semantics_ok,
        "counts_ok": counts_ok,
        "age_sec": age,
        "event_rows": len(rows),
        "policy_rows": len(policy_rows),
        "direct_release_ready_rows": direct_ready,
        "fallback_only_rows": fallback_only,
        "blocked_rows": blocked,
    }


def scheduled_event_quote_capture_integrity(
    state: dict[str, Any],
    *,
    database_path: Path,
    cutoff_epoch: float,
    maximum_age_sec: float = 30.0,
) -> dict[str, Any]:
    """Independently verify the prospective scheduled-clock quote ledger.

    The scheduled clock is deliberately separate from source first-seen time.
    This verifier therefore checks only immutable timing/quote evidence and
    requires explicit direction abstention and execution inertness.
    """

    generated_epoch = parse_epoch(state.get("generated_utc"))
    age = None if generated_epoch is None else cutoff_epoch - generated_epoch
    freshness_ok = bool(age is not None and -5.0 <= age <= maximum_age_sec)
    contract_ok = bool(
        state.get("schema_version") == "scheduled_event_quote_capture_v1"
        and state.get("contract_id")
        == EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_CONTRACT
        and state.get("cohort_id")
        == EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_COHORT
        and state.get("horizons_min")
        == EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_HORIZONS_MIN
        and int(state.get("expected_instrument_count") or 0) == 68
        and state.get("expected_instrument_universe_sha256")
        == EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_UNIVERSE_SHA256
        and state.get("clock_semantics")
        == "scheduled_release_time_not_source_first_seen_time"
    )
    inert_ok = bool(
        state.get("research_only") is True
        and state.get("execution_eligible") is False
        and state.get("can_place_orders") is False
        and state.get("can_authorize") is False
        and state.get("can_promote") is False
        and state.get("direction_policy") == "abstain"
        and state.get("supported_execution_decision") == "no_trade"
    )
    state_counts = state.get("counts") or {}
    expected_count_keys = {
        "registered_event_clocks",
        "terminal_capture_attempts",
        "exact_all68_captures",
        "invalid_terminal_captures",
        "proof_quote_rows",
    }
    state_counts_typed = bool(
        expected_count_keys.issubset(state_counts)
        and all(
            isinstance(state_counts.get(key), int)
            and not isinstance(state_counts.get(key), bool)
            and int(state_counts.get(key) or 0) >= 0
            for key in expected_count_keys
        )
    )

    database_integrity = "missing"
    database_counts: dict[str, int] = {}
    row_contracts_ok = False
    payload_hashes_ok = False
    capture_payloads_ok = False
    append_only_triggers_ok = False
    database_error = ""
    try:
        uri = f"file:{database_path.resolve().as_posix()}?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=30.0) as connection:
            database_integrity = str(
                connection.execute("PRAGMA quick_check").fetchone()[0]
            )
            clock_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduled_event_clock"
                ).fetchone()[0]
            )
            capture_row = connection.execute(
                """
                SELECT COUNT(*),
                       SUM(CASE WHEN timing_quality='prospective_exact_live_quote'
                                THEN 1 ELSE 0 END),
                       SUM(CASE WHEN timing_quality!='prospective_exact_live_quote'
                                THEN 1 ELSE 0 END),
                       SUM(proof_quote_count)
                FROM scheduled_event_quote_capture
                """
            ).fetchone()
            database_counts = {
                "registered_event_clocks": clock_count,
                "terminal_capture_attempts": int(capture_row[0] or 0),
                "exact_all68_captures": int(capture_row[1] or 0),
                "invalid_terminal_captures": int(capture_row[2] or 0),
                "proof_quote_rows": int(capture_row[3] or 0),
            }
            invalid_clock_contracts = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM scheduled_event_clock
                    WHERE research_only!=1 OR execution_eligible!=0
                       OR can_place_orders!=0 OR can_authorize!=0
                       OR can_promote!=0 OR contract_id!=? OR cohort_id!=?
                    """,
                    (
                        EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_CONTRACT,
                        EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_COHORT,
                    ),
                ).fetchone()[0]
            )
            invalid_capture_contracts = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM scheduled_event_quote_capture
                    WHERE research_only!=1 OR execution_eligible!=0
                       OR can_place_orders!=0 OR can_authorize!=0
                       OR can_promote!=0 OR contract_id!=? OR cohort_id!=?
                       OR horizon_min NOT IN (0,1,5,15,30,60)
                       OR (timing_quality='prospective_exact_live_quote'
                           AND proof_quote_count!=68)
                       OR (timing_quality!='prospective_exact_live_quote'
                           AND proof_quote_count!=0)
                    """,
                    (
                        EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_CONTRACT,
                        EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_COHORT,
                    ),
                ).fetchone()[0]
            )
            row_contracts_ok = not invalid_clock_contracts and not invalid_capture_contracts
            trigger_names = {
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type='trigger' AND name IN (
                      'trg_scheduled_event_clock_no_update',
                      'trg_scheduled_event_clock_no_delete',
                      'trg_scheduled_event_quote_capture_no_update',
                      'trg_scheduled_event_quote_capture_no_delete'
                    )
                    """
                )
            }
            append_only_triggers_ok = len(trigger_names) == 4
            payload_hashes_ok = True
            for payload_json, payload_sha256 in connection.execute(
                "SELECT event_payload_json,event_payload_sha256 "
                "FROM scheduled_event_clock"
            ):
                if hashlib.sha256(str(payload_json).encode("utf-8")).hexdigest() != str(
                    payload_sha256
                ):
                    payload_hashes_ok = False
                    break
            capture_payloads_ok = True
            for (
                payload_json,
                payload_sha256,
                timing_quality,
                proof_quote_count,
                horizon_min,
            ) in connection.execute(
                """
                SELECT payload_json,payload_sha256,timing_quality,
                       proof_quote_count,horizon_min
                FROM scheduled_event_quote_capture
                """
            ):
                if hashlib.sha256(str(payload_json).encode("utf-8")).hexdigest() != str(
                    payload_sha256
                ):
                    payload_hashes_ok = False
                    capture_payloads_ok = False
                    break
                try:
                    payload = json.loads(str(payload_json))
                except (TypeError, ValueError):
                    capture_payloads_ok = False
                    break
                exact = str(timing_quality) == "prospective_exact_live_quote"
                if not (
                    payload.get("contract_id")
                    == EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_CONTRACT
                    and payload.get("cohort_id")
                    == EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_COHORT
                    and payload.get("research_only") is True
                    and payload.get("execution_eligible") is False
                    and payload.get("can_place_orders") is False
                    and payload.get("can_authorize") is False
                    and payload.get("can_promote") is False
                    and payload.get("direction_policy") == "abstain"
                    and payload.get("clock_semantics")
                    == "scheduled_release_time_not_source_first_seen_time"
                    and int(payload.get("horizon_min") or 0) == int(horizon_min)
                    and int(payload.get("proof_quote_count") or 0)
                    == int(proof_quote_count)
                    and (
                        (exact and len(payload.get("quotes") or {}) == 68)
                        or (not exact and not (payload.get("quotes") or {}))
                    )
                ):
                    capture_payloads_ok = False
                    break
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        database_error = f"{type(exc).__name__}:{exc}"

    counts_ok = bool(
        state_counts_typed
        and database_counts
        and all(
            int(state_counts.get(key) or 0) == int(database_counts.get(key) or 0)
            for key in expected_count_keys
        )
        and int(database_counts.get("registered_event_clocks") or 0) > 0
        and int(database_counts.get("proof_quote_rows") or 0)
        == 68 * int(database_counts.get("exact_all68_captures") or 0)
    )
    database_ok = bool(
        database_integrity == "ok"
        and state.get("database_integrity") == "ok"
        and state.get("status") == "ok"
    )
    return {
        "ok": bool(
            contract_ok
            and inert_ok
            and freshness_ok
            and database_ok
            and counts_ok
            and row_contracts_ok
            and payload_hashes_ok
            and capture_payloads_ok
            and append_only_triggers_ok
        ),
        "contract_ok": contract_ok,
        "inert_ok": inert_ok,
        "freshness_ok": freshness_ok,
        "age_sec": age,
        "database_integrity": database_integrity,
        "database_error": database_error,
        "counts_ok": counts_ok,
        "state_counts": {
            key: state_counts.get(key) for key in sorted(expected_count_keys)
        },
        "database_counts": database_counts,
        "row_contracts_ok": row_contracts_ok,
        "payload_hashes_ok": payload_hashes_ok,
        "capture_payloads_ok": capture_payloads_ok,
        "append_only_triggers_ok": append_only_triggers_ok,
    }


def scheduled_event_quote_capture_v2_integrity(
    state: dict[str, Any],
    *,
    database_path: Path,
    cutoff_epoch: float,
    maximum_age_sec: float = 30.0,
) -> dict[str, Any]:
    """Independently verify the read-only current-pricing event cohort."""

    generated_epoch = parse_epoch(state.get("generated_utc"))
    age = None if generated_epoch is None else cutoff_epoch - generated_epoch
    freshness_ok = bool(age is not None and -5.0 <= age <= maximum_age_sec)
    contract_ok = bool(
        state.get("schema_version") == "scheduled_event_quote_capture_v2"
        and state.get("contract_id")
        == EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_V2_CONTRACT
        and state.get("cohort_id")
        == EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_V2_COHORT
        and state.get("horizons_min")
        == EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_HORIZONS_MIN
        and int(state.get("expected_instrument_count") or 0) == 68
        and state.get("expected_instrument_universe_sha256")
        == EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_UNIVERSE_SHA256
        and state.get("quote_acquisition")
        == "one_read_only_oanda_practice_pricing_request_all68"
        and state.get("currentness_clock")
        == "pricing_response_observed_utc"
        and state.get("broker_price_time_role")
        == "preserved_diagnostic_not_currentness_gate"
    )
    inert_ok = bool(
        state.get("research_only") is True
        and state.get("execution_eligible") is False
        and state.get("can_place_orders") is False
        and state.get("can_authorize") is False
        and state.get("can_promote") is False
        and state.get("direction_policy") == "abstain"
        and state.get("supported_execution_decision") == "no_trade"
    )
    count_keys = {
        "registered_event_clocks",
        "terminal_capture_attempts",
        "valid_current_snapshots",
        "invalid_terminal_captures",
        "observed_universe_rows",
        "tradeable_proof_rows",
        "nontradeable_observed_rows",
        "direct_event_tradeable_rows",
    }
    state_counts = state.get("counts") or {}
    state_counts_typed = bool(
        count_keys.issubset(state_counts)
        and all(
            isinstance(state_counts.get(key), int)
            and not isinstance(state_counts.get(key), bool)
            and int(state_counts.get(key) or 0) >= 0
            for key in count_keys
        )
    )
    database_integrity = "missing"
    database_counts: dict[str, int] = {}
    row_contracts_ok = False
    payload_hashes_ok = False
    capture_payloads_ok = False
    append_only_triggers_ok = False
    database_error = ""
    try:
        uri = f"file:{database_path.resolve().as_posix()}?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=30.0) as connection:
            database_integrity = str(
                connection.execute("PRAGMA quick_check").fetchone()[0]
            )
            clocks = int(connection.execute(
                "SELECT COUNT(*) FROM scheduled_event_clock_v2"
            ).fetchone()[0])
            row = connection.execute(
                """
                SELECT COUNT(*),
                       COALESCE(SUM(timing_quality=
                         'prospective_current_oanda_pricing_snapshot'),0),
                       COALESCE(SUM(timing_quality!=
                         'prospective_current_oanda_pricing_snapshot'),0),
                       COALESCE(SUM(universe_quote_count),0),
                       COALESCE(SUM(tradeable_proof_count),0),
                       COALESCE(SUM(nontradeable_observed_count),0),
                       COALESCE(SUM(direct_event_tradeable_count),0)
                FROM scheduled_event_quote_capture_v2
                """
            ).fetchone()
            database_counts = {
                "registered_event_clocks": clocks,
                "terminal_capture_attempts": int(row[0] or 0),
                "valid_current_snapshots": int(row[1] or 0),
                "invalid_terminal_captures": int(row[2] or 0),
                "observed_universe_rows": int(row[3] or 0),
                "tradeable_proof_rows": int(row[4] or 0),
                "nontradeable_observed_rows": int(row[5] or 0),
                "direct_event_tradeable_rows": int(row[6] or 0),
            }
            invalid_clocks = int(connection.execute(
                """
                SELECT COUNT(*) FROM scheduled_event_clock_v2
                WHERE research_only!=1 OR execution_eligible!=0
                   OR can_place_orders!=0 OR can_authorize!=0
                   OR can_promote!=0 OR contract_id!=? OR cohort_id!=?
                """,
                (
                    EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_V2_CONTRACT,
                    EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_V2_COHORT,
                ),
            ).fetchone()[0])
            invalid_captures = int(connection.execute(
                """
                SELECT COUNT(*) FROM scheduled_event_quote_capture_v2
                WHERE research_only!=1 OR execution_eligible!=0
                   OR can_place_orders!=0 OR can_authorize!=0
                   OR can_promote!=0 OR contract_id!=? OR cohort_id!=?
                   OR horizon_min NOT IN (0,1,5,15,30,60)
                   OR (timing_quality=
                         'prospective_current_oanda_pricing_snapshot'
                       AND (universe_quote_count!=68
                         OR tradeable_proof_count+nontradeable_observed_count!=68))
                   OR (timing_quality!=
                         'prospective_current_oanda_pricing_snapshot'
                       AND tradeable_proof_count!=0)
                """,
                (
                    EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_V2_CONTRACT,
                    EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_V2_COHORT,
                ),
            ).fetchone()[0])
            row_contracts_ok = not invalid_clocks and not invalid_captures
            trigger_names = {
                str(item[0]) for item in connection.execute(
                    """
                    SELECT name FROM sqlite_master WHERE type='trigger' AND name IN (
                      'trg_scheduled_event_clock_v2_no_update',
                      'trg_scheduled_event_clock_v2_no_delete',
                      'trg_scheduled_event_quote_capture_v2_no_update',
                      'trg_scheduled_event_quote_capture_v2_no_delete'
                    )
                    """
                )
            }
            append_only_triggers_ok = len(trigger_names) == 4
            payload_hashes_ok = True
            for payload_json, payload_sha256 in connection.execute(
                "SELECT event_payload_json,event_payload_sha256 "
                "FROM scheduled_event_clock_v2"
            ):
                if hashlib.sha256(str(payload_json).encode("utf-8")).hexdigest() != str(
                    payload_sha256
                ):
                    payload_hashes_ok = False
                    break
            capture_payloads_ok = True
            for db_row in connection.execute(
                """
                SELECT payload_json,payload_sha256,response_sha256,
                       timing_quality,universe_quote_count,
                       tradeable_proof_count,nontradeable_observed_count,
                       direct_event_tradeable_count,horizon_min
                FROM scheduled_event_quote_capture_v2
                """
            ):
                payload_json, payload_sha256 = str(db_row[0]), str(db_row[1])
                if hashlib.sha256(payload_json.encode("utf-8")).hexdigest() != payload_sha256:
                    payload_hashes_ok = False
                    capture_payloads_ok = False
                    break
                try:
                    payload = json.loads(payload_json)
                except (TypeError, ValueError):
                    capture_payloads_ok = False
                    break
                valid = str(db_row[3]) == "prospective_current_oanda_pricing_snapshot"
                quotes = payload.get("quotes") or {}
                tradeable = payload.get("tradeable_quotes") or {}
                nontradeable = payload.get("nontradeable_quotes") or {}
                direct = payload.get("direct_event_tradeable_quotes") or {}
                if not (
                    payload.get("contract_id")
                    == EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_V2_CONTRACT
                    and payload.get("cohort_id")
                    == EXPECTED_SCHEDULED_EVENT_QUOTE_CAPTURE_V2_COHORT
                    and payload.get("research_only") is True
                    and payload.get("execution_eligible") is False
                    and payload.get("can_place_orders") is False
                    and payload.get("can_authorize") is False
                    and payload.get("can_promote") is False
                    and payload.get("direction_policy") == "abstain"
                    and payload.get("broker_tick_age_is_diagnostic_only") is True
                    and payload.get("response_sha256") == str(db_row[2])
                    and int(payload.get("horizon_min") or 0) == int(db_row[8])
                    and int(payload.get("universe_quote_count") or 0) == int(db_row[4])
                    and int(payload.get("tradeable_proof_count") or 0) == int(db_row[5])
                    and int(payload.get("nontradeable_observed_count") or 0) == int(db_row[6])
                    and int(payload.get("direct_event_tradeable_count") or 0) == int(db_row[7])
                    and len(quotes) == int(db_row[4])
                    and len(tradeable) == int(db_row[5])
                    and len(nontradeable) == int(db_row[6])
                    and len(direct) == int(db_row[7])
                    and ((valid and len(quotes) == 68) or (not valid and not tradeable))
                ):
                    capture_payloads_ok = False
                    break
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        database_error = f"{type(exc).__name__}:{exc}"

    counts_ok = bool(
        state_counts_typed
        and database_counts
        and all(
            int(state_counts.get(key) or 0) == int(database_counts.get(key) or 0)
            for key in count_keys
        )
        and int(database_counts.get("registered_event_clocks") or 0) > 0
        and int(database_counts.get("observed_universe_rows") or 0)
        == 68 * int(database_counts.get("valid_current_snapshots") or 0)
    )
    database_ok = bool(
        database_integrity == "ok"
        and state.get("database_integrity") == "ok"
        and state.get("status") == "ok"
    )
    return {
        "ok": bool(
            contract_ok and inert_ok and freshness_ok and database_ok
            and counts_ok and row_contracts_ok and payload_hashes_ok
            and capture_payloads_ok and append_only_triggers_ok
        ),
        "contract_ok": contract_ok,
        "inert_ok": inert_ok,
        "freshness_ok": freshness_ok,
        "age_sec": age,
        "database_integrity": database_integrity,
        "database_error": database_error,
        "counts_ok": counts_ok,
        "state_counts": {
            key: state_counts.get(key) for key in sorted(count_keys)
        },
        "database_counts": database_counts,
        "row_contracts_ok": row_contracts_ok,
        "payload_hashes_ok": payload_hashes_ok,
        "capture_payloads_ok": capture_payloads_ok,
        "append_only_triggers_ok": append_only_triggers_ok,
    }


def source_conditioned_rank_v6_inventory_integrity(
    state: dict[str, Any],
    *,
    source_database: Path,
    cutoff_epoch: float | None = None,
    maximum_age_sec: float = 30.0,
    expected_rank_contract: str = (
        EXPECTED_SOURCE_CONDITIONED_CURRENCY_RANK_V6_CONTRACT
    ),
    expected_source_contract: str = EXPECTED_SOURCE_FACTOR_RESPONSE_MAP_V7_CONTRACT,
) -> dict[str, Any]:
    """Independently reconcile source production, abstention, and eligibility."""

    generated_epoch = parse_epoch(state.get("generated_utc"))
    generated_utc = str(state.get("generated_utc") or "")
    age = (
        None
        if cutoff_epoch is None or generated_epoch is None
        else cutoff_epoch - generated_epoch
    )
    freshness_ok = bool(
        cutoff_epoch is None
        or (age is not None and -5.0 <= age <= maximum_age_sec)
    )
    empty = {
        "ok": False,
        "contract_ok": False,
        "inert_ok": False,
        "counts_ok": False,
        "freshness_ok": freshness_ok,
        "age_sec": age,
        "database_integrity": "missing",
        "count_basis": "issued_utc_at_or_before_rank_snapshot_generated_utc",
        "snapshot_generated_utc": generated_utc or None,
        "total_rows": 0,
        "rank_eligible_rows": 0,
        "abstain_rows": 0,
        "distinct_event_count": 0,
        "distinct_factor_observation_count": 0,
        "forecast_state_counts": {},
        "abstain_reason_counts": {},
        "currency_counts": {},
        "horizon_counts": {},
        "contract_counts": {},
        "status": "missing_or_invalid_state",
    }
    if not generated_utc or not source_database.exists():
        return empty
    connection = sqlite3.connect(
        f"file:{source_database.as_posix()}?mode=ro", uri=True, timeout=15.0
    )
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=15000")
        connection.execute("BEGIN")
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if "source_factor_forecast" not in tables:
            return {**empty, "status": "forecast_table_missing"}
        where = "julianday(issued_utc) <= julianday(?)"
        parameters = (generated_utc,)
        summary = connection.execute(
            f"""
            SELECT COUNT(*) total_rows,
                   SUM(CASE WHEN forecast_state='forecast'
                                  AND prospective_proof_eligible=1
                                  AND probability_strengthening IS NOT NULL
                                  AND predicted_currency_factor_bps IS NOT NULL
                                  AND predicted_absolute_factor_bps IS NOT NULL
                            THEN 1 ELSE 0 END) rank_eligible_rows,
                   SUM(CASE WHEN forecast_state='abstain'
                            THEN 1 ELSE 0 END) abstain_rows,
                   COUNT(DISTINCT canonical_event_id) distinct_event_count,
                   COUNT(DISTINCT factor_observation_id)
                       distinct_factor_observation_count
              FROM source_factor_forecast
             WHERE {where}
            """,
            parameters,
        ).fetchone()

        def grouped(column: str, *, extra_where: str = "") -> dict[str, int]:
            return {
                str(row[0]): int(row[1])
                for row in connection.execute(
                    f"""
                    SELECT {column},COUNT(*)
                      FROM source_factor_forecast
                     WHERE {where}{extra_where}
                     GROUP BY {column}
                     ORDER BY COUNT(*) DESC,{column}
                    """,
                    parameters,
                )
                if str(row[0] or "")
            }

        total = int(summary["total_rows"] or 0)
        eligible = int(summary["rank_eligible_rows"] or 0)
        abstain = int(summary["abstain_rows"] or 0)
        states = grouped("forecast_state")
        reasons = grouped(
            "abstain_reason", extra_where=" AND forecast_state='abstain'"
        )
        currencies = grouped("currency")
        horizons = grouped("horizon_min")
        contracts = grouped("contract_id")
        database_integrity = str(
            connection.execute("PRAGMA quick_check(1)").fetchone()[0]
        )
    except (sqlite3.Error, KeyError, TypeError, ValueError) as error:
        return {**empty, "status": f"database_error:{type(error).__name__}"}
    finally:
        connection.close()

    expected_status = (
        "rank_eligible_rows_available"
        if eligible
        else "all_rows_abstain"
        if total and abstain == total
        else "no_rank_eligible_rows"
        if total
        else "no_rows_at_cutoff"
    )
    inventory = state.get("source_forecast_inventory") or {}
    expected_inventory = {
        "total_rows": total,
        "rank_eligible_rows": eligible,
        "abstain_rows": abstain,
        "excluded_from_rank_rows": total - eligible,
        "distinct_event_count": int(summary["distinct_event_count"] or 0),
        "distinct_factor_observation_count": int(
            summary["distinct_factor_observation_count"] or 0
        ),
        "forecast_state_counts": states,
        "abstain_reason_counts": reasons,
        "currency_counts": currencies,
        "horizon_counts": horizons,
        "contract_counts": contracts,
        "status": expected_status,
        "database_integrity": database_integrity,
    }
    counts_ok = all(
        inventory.get(key) == value
        for key, value in expected_inventory.items()
    ) and int(state.get("source_forecast_rows") or 0) == eligible
    contract_ok = bool(
        state.get("contract_id")
        == expected_rank_contract
        and state.get("required_source_contract_id")
        == expected_source_contract
        and contracts in ({}, {expected_source_contract: total})
        and state.get("source_forecast_rows_semantics")
        == "rank_eligible_non_abstaining_rows_loaded_by_adapter"
        and inventory.get("count_basis")
        == "issued_utc_at_or_before_adapter_generated_utc"
    )
    policy = state.get("policy") or {}
    inert_ok = bool(
        state.get("source_input_status") == "ready"
        and state.get("supported_execution_decision") == "no_trade"
        and state.get("research_only")
        and not state.get("execution_eligible")
        and not state.get("can_place_orders")
        and not state.get("can_authorize")
        and not state.get("can_promote")
        and policy.get("abstain_inventory_is_diagnostic_only") is True
        and policy.get("abstain_rows_can_trigger_rank_decisions") is False
    )
    return {
        **empty,
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
        "database_integrity": database_integrity,
        "total_rows": total,
        "rank_eligible_rows": eligible,
        "abstain_rows": abstain,
        "distinct_event_count": expected_inventory["distinct_event_count"],
        "distinct_factor_observation_count": expected_inventory[
            "distinct_factor_observation_count"
        ],
        "forecast_state_counts": states,
        "abstain_reason_counts": reasons,
        "currency_counts": currencies,
        "horizon_counts": horizons,
        "contract_counts": contracts,
        "status": expected_status,
    }


def source_conditioned_rank_v7_inventory_integrity(
    state: dict[str, Any],
    *,
    source_database: Path,
    cutoff_epoch: float | None = None,
    maximum_age_sec: float = 30.0,
) -> dict[str, Any]:
    """Independently reconcile the current V8-to-rank-V7 contract."""

    return source_conditioned_rank_v6_inventory_integrity(
        state,
        source_database=source_database,
        cutoff_epoch=cutoff_epoch,
        maximum_age_sec=maximum_age_sec,
        expected_rank_contract=(
            EXPECTED_SOURCE_CONDITIONED_CURRENCY_RANK_V7_CONTRACT
        ),
        expected_source_contract=EXPECTED_SOURCE_FACTOR_RESPONSE_MAP_V8_CONTRACT,
    )


def retired_source_rank_v7_diagnostics(
    state: dict[str, Any], *, source_database: Path,
) -> dict[str, Any]:
    """Reconcile sealed inventory at its own publication, without live authority."""
    result = source_conditioned_rank_v7_inventory_integrity(
        state, source_database=source_database,
        cutoff_epoch=parse_epoch(state.get("generated_utc")),
    )
    result.update(
        disposition="retired_diagnostic_preserved",
        live_freshness_assessed=False,
        prospective_performance_verified=False,
        operationally_ready=False,
    )
    return result


def run(
    *, output: Path = DEFAULT_OUTPUT, history: Path = DEFAULT_HISTORY,
    report: Path = DEFAULT_REPORT, publication_guard: Path | None = None,
) -> dict[str, Any]:
    timing_started = time.monotonic()
    guard = publication_guard or output.with_name(
        f"{output.stem}_guard_v1.sqlite"
    )
    ownership = _claim_audit_publication(guard)
    previous = read_json(output)
    feature_state = read_json(STATE / "live_model_feature_snapshot_v1.json")
    source = read_json(STATE / "source_governance_v1.json")
    news_governance_fast_lane = read_json(CURRENT_NEWS_GOVERNANCE_FAST_LANE)
    source_counts = source.get("counts") or {}
    raw_source_events = int(source_counts.get("events") or 0)
    quarantined_source_events = int(source_counts.get("quarantined_events") or 0)
    causal_source_events = int(
        source_counts.get("causal_events")
        if source_counts.get("causal_events") is not None
        else raw_source_events
    )
    lifecycle_state_path = STATE / "evidence_lifecycle_v1.json"
    lifecycle = read_json(lifecycle_state_path)
    lifecycle_state_fingerprint = "sha256:" + hashlib.sha256(
        lifecycle_state_path.read_bytes()
    ).hexdigest()
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
    improvement_control = read_json(STATE / "improvement_control_engine_v1.json")
    direct_source_response = read_json(STATE / "direct_source_response_v1.json")
    move_first_news_audit = read_json(CURRENT_MOVE_FIRST_NEWS_AUDIT)
    move_first_live_case_capture = read_json(CURRENT_MOVE_FIRST_LIVE_CASE_CAPTURE)
    move_first_live_arm_alignment = read_json(
        CURRENT_MOVE_FIRST_LIVE_ARM_ALIGNMENT
    )
    move_first_operational_mapping_alignment = read_json(
        CURRENT_MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT
    )
    move_first_operational_mapping_alignment_v2 = read_json(
        CURRENT_MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_V2
    )
    move_first_operational_mapping_alignment_v3 = read_json(
        CURRENT_MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_V3
    )
    move_first_operational_mapping_alignment_v4 = read_json(
        CURRENT_MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_V4
    )
    major_move_gap_census = read_json(CURRENT_MAJOR_MOVE_GAP_CENSUS)
    major_move_gap_census_heartbeat = read_json(
        CURRENT_MAJOR_MOVE_GAP_CENSUS_HEARTBEAT
    )
    executable_move_census_verifier_current = read_json(
        CURRENT_EXECUTABLE_MOVE_CENSUS_VERIFIER
    )
    # Read the independently published verifier first, then the append-only
    # live projection it covers.  This order prevents a newer verifier from
    # being compared against an older projection read just before publication.
    executable_move_census_current = read_json(
        CURRENT_EXECUTABLE_MOVE_CENSUS
    )
    live_move_news_snapshot = read_json(CURRENT_LIVE_MOVE_NEWS_SNAPSHOT)
    live_move_news_outcomes = read_json(CURRENT_LIVE_MOVE_NEWS_OUTCOMES)
    continuous_narrative_meter = read_json(
        CURRENT_CONTINUOUS_NARRATIVE_METER
    )
    event_technical_preflight = read_json(CURRENT_EVENT_TECHNICAL_PREFLIGHT)
    news_outcome_improvement_audit = read_json(
        CURRENT_NEWS_OUTCOME_IMPROVEMENT_AUDIT
    )
    live_move_persistent_context = read_json(
        CURRENT_LIVE_MOVE_PERSISTENT_CONTEXT
    )
    source_conditioned_rank_v7 = read_json(
        CURRENT_SOURCE_CONDITIONED_CURRENCY_RANK_V7
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
    # slower input snapshots have been captured so a worker update during this
    # read pass is not mislabeled as a future timestamp.  Quotes are refreshed
    # here and bound to the same cutoff; otherwise the long input pass itself
    # creates a false cross-pair freshness failure.
    quotes_state, generated = capture_current_quote_snapshot()
    timing_after_input_snapshot = time.monotonic()
    market = forex_market_state(generated)
    component_durations_sec: dict[str, float] = {}

    def timed_component(name: str, callback: Any) -> Any:
        started = time.monotonic()
        try:
            return callback()
        finally:
            component_durations_sec[name] = round(
                time.monotonic() - started, 6
            )

    major_move_gap_census_rebuild = timed_component(
        "major_move_gap_census_rebuild",
        lambda: major_move_gap_census_progress_status(
            major_move_gap_census,
            major_move_gap_census_heartbeat,
            cutoff_epoch=generated.timestamp(),
        ),
    )
    numeric_currency_integrity = timed_component(
        "structured_numeric_currency_integrity",
        lambda: structured_numeric_currency_integrity(
            LOCAL_NEWS_DATABASE,
            source_state_path=CURRENT_NEWS_COLLECTOR_STATE,
        ),
    )
    detail_quality_integrity = timed_component(
        "official_detail_quality_integrity",
        lambda: official_detail_quality_integrity(CURRENT_CONTEXT_ARTICLES),
    )
    source_provenance_integrity = timed_component(
        "current_source_provenance_integrity",
        lambda: current_source_provenance_integrity(CURRENT_CONTEXT_ARTICLES),
    )
    policy_state_integrity = timed_component(
        "persistent_policy_state_integrity",
        lambda: persistent_policy_state_integrity(
            CURRENT_PERSISTENT_POLICY_STATE
        ),
    )
    official_source_depth_status = timed_component(
        "official_source_depth_integrity",
        lambda: official_source_depth_integrity(
            read_json(CURRENT_OFFICIAL_CURRENCY_SOURCE_DEPTH),
            cutoff_epoch=generated.timestamp(),
        ),
    )
    quote_audit = timed_component(
        "synchronized_quote_audit",
        lambda: synchronized_quote_audit(
            quotes_state.get("quotes") or {},
            cutoff_epoch=generated.timestamp(),
        ),
    )
    executable_move_census_current_status = timed_component(
        "executable_move_census_current_integrity",
        lambda: executable_move_census_current_integrity(
            executable_move_census_current,
            executable_move_census_verifier_current,
            cutoff_epoch=generated.timestamp(),
        ),
    )
    timing_after_core_integrity = time.monotonic()
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
    live_move_forward_integrity = timed_component(
        "live_move_forward_proof_integrity",
        lambda: live_move_forward_proof_integrity(
            live_move_news_snapshot,
            live_move_news_outcomes,
            cutoff_epoch=generated.timestamp(),
        ),
    )
    news_outcome_improvement_status = timed_component(
        "news_outcome_improvement_integrity",
        lambda: news_outcome_improvement_integrity(
            news_outcome_improvement_audit,
            database_path=NEWS_OUTCOME_IMPROVEMENT_DATABASE,
            cutoff_epoch=generated.timestamp(),
            full_database_integrity_attestation=(
                prior_component_full_integrity_attestation(
                    previous,
                    component_key="news_outcome_improvement_integrity",
                    check_key="canonical_news_outcome_record_valid_and_inert",
                    database_path=NEWS_OUTCOME_IMPROVEMENT_DATABASE,
                    table_name="diagnoses",
                )
            ),
        ),
    )
    live_move_persistent_context_status = timed_component(
        "live_move_persistent_context_integrity",
        lambda: live_move_persistent_context_integrity(
            live_move_persistent_context,
            cutoff_epoch=generated.timestamp(),
        ),
    )
    official_release_fast_lane_status = timed_component(
        "official_release_fast_lane_integrity",
        lambda: official_release_fast_lane_integrity(
            official_release_fast_lane,
            official_release_fast_lane_heartbeat,
            database_path=OFFICIAL_RELEASE_FAST_LANE_DATABASE,
            cutoff_epoch=generated.timestamp(),
            full_database_integrity_attestation=(
                prior_component_full_integrity_attestation(
                    previous,
                    component_key="official_release_fast_lane",
                    check_key="official_release_fast_lane_current_and_inert",
                    database_path=OFFICIAL_RELEASE_FAST_LANE_DATABASE,
                    table_name="official_release_observation",
                )
            ),
        ),
    )
    source_governance_fast_lane_adapter_status = timed_component(
        "source_governance_fast_lane_adapter_integrity",
        lambda: source_governance_fast_lane_adapter_integrity(
            source,
            database_path=SOURCE_GOVERNANCE_DATABASE,
            full_database_integrity_attestation=(
                prior_source_governance_full_integrity_attestation(previous)
            ),
            cutoff_epoch=generated.timestamp(),
        ),
    )
    news_governance_fast_lane_status = timed_component(
        "news_source_governance_fast_lane_integrity",
        lambda: news_source_governance_fast_lane_integrity(
            news_governance_fast_lane,
            database_path=SOURCE_GOVERNANCE_DATABASE,
            cutoff_epoch=generated.timestamp(),
        ),
    )
    official_release_fast_mapping_status = timed_component(
        "official_release_fast_mapping_integrity",
        lambda: official_release_fast_mapping_integrity(
            official_release_fast_mapping,
            official_release_fast_mapping_heartbeat,
            database_path=OFFICIAL_RELEASE_FAST_MAPPING_DATABASE,
            cutoff_epoch=generated.timestamp(),
            full_database_integrity_attestation=(
                prior_component_full_integrity_attestation(
                    previous,
                    component_key="official_release_fast_mapping",
                    check_key="official_release_fast_mapping_current_and_inert",
                    database_path=OFFICIAL_RELEASE_FAST_MAPPING_DATABASE,
                    table_name="official_release_mapping",
                )
            ),
        ),
    )
    official_release_fast_response_status = timed_component(
        "official_release_fast_response_integrity",
        lambda: official_release_fast_response_integrity(
            official_release_fast_response,
            official_release_fast_response_heartbeat,
            database_path=OFFICIAL_RELEASE_FAST_RESPONSE_DATABASE,
            cutoff_epoch=generated.timestamp(),
        ),
    )
    continuous_narrative_meter_status = timed_component(
        "continuous_narrative_meter_integrity",
        lambda: continuous_narrative_meter_integrity(
            continuous_narrative_meter,
            database_path=CONTINUOUS_NARRATIVE_METER_DATABASE,
            cutoff_epoch=generated.timestamp(),
            full_database_integrity_attestation=(
                prior_component_full_integrity_attestation(
                    previous,
                    component_key="continuous_narrative_meter",
                    check_key=(
                        "continuous_narrative_meter_sealed_current_and_inert"
                    ),
                    database_path=CONTINUOUS_NARRATIVE_METER_DATABASE,
                    table_name="bucket_seals",
                )
            ),
        ),
    )
    event_technical_preflight_status = timed_component(
        "event_technical_preflight_source_integrity",
        lambda: event_technical_preflight_source_integrity(
            event_technical_preflight,
            cutoff_epoch=generated.timestamp(),
        ),
    )
    # These workers publish every two seconds while this audit performs slower
    # scans. Bind both reads to an adjacent cutoff so audit runtime cannot make
    # a healthy live heartbeat look stale.
    scheduled_event_quote_capture = read_json(
        CURRENT_SCHEDULED_EVENT_QUOTE_CAPTURE
    )
    scheduled_event_quote_capture_v2 = read_json(
        CURRENT_SCHEDULED_EVENT_QUOTE_CAPTURE_V2
    )
    scheduled_capture_cutoff_epoch = time.time()
    scheduled_event_quote_capture_status = timed_component(
        "scheduled_event_quote_capture_integrity",
        lambda: scheduled_event_quote_capture_integrity(
            scheduled_event_quote_capture,
            database_path=SCHEDULED_EVENT_QUOTE_CAPTURE_DATABASE,
            cutoff_epoch=scheduled_capture_cutoff_epoch,
        ),
    )
    scheduled_event_quote_capture_v2_status = timed_component(
        "scheduled_event_quote_capture_v2_integrity",
        lambda: scheduled_event_quote_capture_v2_integrity(
            scheduled_event_quote_capture_v2,
            database_path=SCHEDULED_EVENT_QUOTE_CAPTURE_V2_DATABASE,
            cutoff_epoch=scheduled_capture_cutoff_epoch,
        ),
    )
    source_conditioned_rank_v7_status = timed_component(
        "retired_source_rank_v7_diagnostics",
        lambda: retired_source_rank_v7_diagnostics(
            source_conditioned_rank_v7,
            source_database=SOURCE_FACTOR_RESPONSE_MAP_V8_DATABASE,
        ),
    )
    from oanda_source_publication_successor_integrity import check_successor_readiness
    source_publication_successor_status = timed_component(
        "source_publication_successor_readiness",
        lambda: check_successor_readiness(now_utc=generated),
    )
    timing_after_extended_integrity = time.monotonic()
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
        "news_source_governance_fast_lane_current_prospective_and_inert": bool(
            news_governance_fast_lane_status.get("ok")
        ),
        "move_first_news_mapping_audit_current": move_first_news_audit_is_current(
            move_first_news_audit,
            major_move_gap_census,
            cutoff_epoch=generated.timestamp(),
        ),
        "move_first_live_case_capture_current_and_inert": (
            move_first_live_case_capture_is_current(
                move_first_live_case_capture,
                cutoff_epoch=generated.timestamp(),
            )
        ),
        "move_first_live_arm_alignment_current_causal_and_inert": (
            move_first_live_arm_alignment_is_current(
                move_first_live_arm_alignment,
                move_first_live_case_capture,
                cutoff_epoch=generated.timestamp(),
            )
        ),
        "move_first_operational_mapping_alignment_v1_preserved_baseline_and_inert": (
            move_first_operational_mapping_alignment_is_preserved_baseline(
                move_first_operational_mapping_alignment,
                move_first_live_case_capture,
            )
        ),
        "move_first_operational_mapping_alignment_v4_current_subsecond_causal_and_inert": (
            move_first_operational_mapping_alignment_is_current(
                move_first_operational_mapping_alignment_v4,
                move_first_live_case_capture,
                cutoff_epoch=generated.timestamp(),
                expected_contract_id=(
                    EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_V4_CONTRACT_ID
                ),
                expected_cohort_id=(
                    EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_V4_COHORT_ID
                ),
                expected_story_deduplication_rule=(
                    EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_V4_STORY_DEDUPLICATION_RULE
                ),
                expected_story_duplicate_count_field=(
                    "operational_story_family_duplicate_count"
                ),
                expected_broad_age_decay_half_life_minutes=(
                    EXPECTED_OPERATIONAL_MAPPING_ALIGNMENT_V4_BROAD_HALF_LIFE_MINUTES
                ),
                expected_operational_clock_rule=(
                    "max_source_effective_detail_available_and_mapping_receipt_"
                    "available_utc_subsecond_precision"
                ),
            )
        ),
        "live_move_news_forward_proof_contract_current": bool(
            live_move_forward_integrity.get("ok")
        ),
        "continuous_narrative_meter_sealed_current_and_inert": bool(
            continuous_narrative_meter_status.get("ok")
        ),
        "event_technical_preflight_source_transport_current_and_inert": bool(
            event_technical_preflight_status.get("ok")
        ),
        "scheduled_event_quote_capture_current_append_only_and_inert": bool(
            scheduled_event_quote_capture_status.get("ok")
        ),
        "scheduled_event_quote_capture_v2_current_append_only_and_inert": bool(
            scheduled_event_quote_capture_v2_status.get("ok")
        ),
        "retired_source_rank_v7_inventory_preserved_and_inert": bool(
            source_conditioned_rank_v7_status.get("ok")
        ),
        "source_publication_successors_disposition_valid_and_inert": bool(
            source_publication_successor_status.get("ok")
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
            current_lifecycle_fingerprint=lifecycle_state_fingerprint,
            current_lifecycle_integrity=lifecycle.get("integrity") or {},
        ),
        "lifecycle_genealogy_post_ingest_sync_current": (
            lifecycle_genealogy_sync_is_current(lifecycle)
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
        "quote_tradeability_status_preserved": (
            quote_tradeability_contract_current(quote_audit, quotes_state)
        ),
        "signal_availability_market_state_consistent": (
            availability_market_state_consistent(signal_availability, market)
        ),
        "cross_pair_snapshot_routable": cross_pair_snapshot_routable(
            quote_audit,
            quotes_state,
            cutoff_epoch=generated.timestamp(),
            market_state=market,
        ),
        "executable_move_census_current_independently_verified": bool(
            executable_move_census_current_status.get("ok")
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
    runtime_health = timed_component(
        "current_supervisor_runtime_observation",
        lambda: read_supervisor_observation(DATA / "logs"),
    )
    audit_finished = datetime.now(timezone.utc)
    publication_freshness = audit_snapshot_publication_freshness(
        generated.isoformat(),
        audit_finished.isoformat(),
    )
    checks["audit_snapshot_fresh_at_publication"] = bool(
        publication_freshness["ok"]
    )
    runtime_scope = scope_integrity_checks(checks, runtime_health)
    timing_finished = time.monotonic()
    phase_durations_sec = {
        "input_snapshot": round(
            timing_after_input_snapshot - timing_started, 6
        ),
        "core_integrity": round(
            timing_after_core_integrity - timing_after_input_snapshot, 6
        ),
        "extended_integrity": round(
            timing_after_extended_integrity - timing_after_core_integrity, 6
        ),
        "checks_and_assembly": round(
            timing_finished - timing_after_extended_integrity, 6
        ),
        "total": round(timing_finished - timing_started, 6),
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
        "schema_version": 1,
        "classification_version": integrity_runtime_contract()[
            "classification_version"
        ],
        "generated_utc": generated.isoformat(),
        "audit_started_utc": ownership["claimed_utc"],
        "audit_finished_utc": audit_finished.isoformat(),
        "publication_latency_sec": publication_freshness["age_sec"],
        "snapshot_fresh_at_publication": publication_freshness["ok"],
        "maximum_publication_latency_sec": publication_freshness[
            "maximum_age_sec"
        ],
        "phase_durations_sec": phase_durations_sec,
        "component_durations_sec": component_durations_sec,
        "audit_owner_id": ownership["owner_id"],
        "publication_generation": ownership["generation"],
        "publication_status": "published",
        "status": "ok" if not failures else "degraded", "research_only": True,
        "can_place_orders": False, "can_promote": False, "real_money_routing": False,
        "supported_decision": "no_trade", "market_state": market,
        "expected_coverage": expected, "actual_coverage": actual,
        "coverage_exclusions": exclusions,
        "live_feature_coverage": feature_coverage,
        "cross_pair_snapshot": quote_audit,
        "executable_move_census_current": executable_move_census_current_status,
        "official_release_fast_lane": official_release_fast_lane_status,
        "source_governance_fast_lane_adapter": (
            source_governance_fast_lane_adapter_status
        ),
        "news_source_governance_fast_lane": news_governance_fast_lane_status,
        "official_release_fast_mapping": official_release_fast_mapping_status,
        "official_release_fast_response_watch": official_release_fast_response_status,
        "structured_numeric_currency_integrity": numeric_currency_integrity,
        "official_detail_quality_integrity": detail_quality_integrity,
        "current_source_provenance_integrity": source_provenance_integrity,
        "persistent_policy_state_integrity": policy_state_integrity,
        "official_source_depth_integrity": official_source_depth_status,
        "live_move_forward_proof_integrity": live_move_forward_integrity,
        "news_outcome_improvement_integrity": (
            news_outcome_improvement_status
        ),
        "major_move_gap_census_rebuild": major_move_gap_census_rebuild,
        "move_first_live_case_capture": move_first_live_case_capture,
        "move_first_live_arm_alignment": move_first_live_arm_alignment,
        "move_first_operational_mapping_alignment": (
            move_first_operational_mapping_alignment
        ),
        "move_first_operational_mapping_alignment_v2": (
            move_first_operational_mapping_alignment_v2
        ),
        "move_first_operational_mapping_alignment_v3": (
            move_first_operational_mapping_alignment_v3
        ),
        "move_first_operational_mapping_alignment_v4": (
            move_first_operational_mapping_alignment_v4
        ),
        "continuous_narrative_meter": continuous_narrative_meter_status,
        "event_technical_preflight": event_technical_preflight_status,
        "scheduled_event_quote_capture": scheduled_event_quote_capture_status,
        "scheduled_event_quote_capture_v2": (
            scheduled_event_quote_capture_v2_status
        ),
        "source_conditioned_currency_rank_v7": source_conditioned_rank_v7_status,
        "source_publication_successors": source_publication_successor_status,
        "live_move_persistent_context": live_move_persistent_context_status,
        "checks": checks, "failures": failures, "drift": drift,
        "runtime_health": runtime_health,
        **runtime_scope,
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
            "source_rank_v7_raw_forecast_rows": int(
                source_conditioned_rank_v7_status.get("total_rows") or 0
            ),
            "source_rank_v7_eligible_forecast_rows": int(
                source_conditioned_rank_v7_status.get("rank_eligible_rows") or 0
            ),
            "source_rank_v7_abstain_rows": int(
                source_conditioned_rank_v7_status.get("abstain_rows") or 0
            ),
            "scheduled_event_clocks": int(
                (
                    scheduled_event_quote_capture_status.get("database_counts")
                    or {}
                ).get("registered_event_clocks")
                or 0
            ),
            "scheduled_event_terminal_captures": int(
                (
                    scheduled_event_quote_capture_status.get("database_counts")
                    or {}
                ).get("terminal_capture_attempts")
                or 0
            ),
            "scheduled_event_v2_clocks": int(
                (
                    scheduled_event_quote_capture_v2_status.get(
                        "database_counts"
                    ) or {}
                ).get("registered_event_clocks")
                or 0
            ),
            "scheduled_event_v2_valid_current_snapshots": int(
                (
                    scheduled_event_quote_capture_v2_status.get(
                        "database_counts"
                    ) or {}
                ).get("valid_current_snapshots")
                or 0
            ),
        },
        "source_gap_states": source_gaps.get("sources") or [],
        "logic_switches": {
            "frozen_cohorts_immutable": True,
            "adaptive_retraining": False,
            "retired_hypotheses_routable": False,
            "legacy_qualification_routable": False,
            "core_governed_entries_require_signed_one_time_canary": True,
            "practice_006_retired_and_not_supervised": True,
            "independent_verifier_required": True,
            "sentiment_combined_score_forbidden": True,
            "live_vacuum_forbidden": True,
            "verified_rotated_log_compression": True,
            "daily_treasury_direction_policy": "abstain",
            "collector_code_changes_require_new_cohort": True,
            "archive_deletion_scoped_to_manifested_date": True,
            "historical_gap_can_be_silently_reconstructed": False,
            "weekend_feed_gaps_are_operational_failures": False,
            "scheduled_event_clock_assigns_direction": False,
        },
    }
    lines = [
        "# Forex project integrity and coverage", "", f"Generated: `{payload['generated_utc']}`", "",
        f"Retained artifact audit: **{payload['status']}**; supported decision: **no_trade**; market: **{market}**.", "",
        f"Current supervision: **{runtime_health['status']}**; reported running workers **{runtime_health.get('running_worker_count', 'unknown')} / {runtime_health.get('expected_worker_count', 13)}**. This reports process/output observations, not forecast success.",
        f"Retained failures: **{len(runtime_scope['inactive_component_failures'])} inactive-component** and **{len(runtime_scope['active_shared_or_unknown_failures'])} active/shared/unknown**. Inactive checks remain failed; historical evidence is not repaired by stopping its producer.",
        f"Live executor running: **{runtime_scope['live_runtime_assertions']['executor_healthy']['currently_running']}**; old four-family proof worker running: **{runtime_scope['live_runtime_assertions']['four_frozen_proof_families_running']['currently_running']}**. Their retained artifact claims below are historical.", "",
        f"- Quotes: **{actual['quoted_instruments']} / 68 last-known**; current stream snapshot: **{actual['current_quoted_instruments']}**; retained stale: **{actual['retained_last_known_instruments']}**",
        f"- Sources: **{actual['operational_news_sources']} operational / {actual['runtime_observed_news_sources']} observed / {actual['configured_news_sources']} registered of {expected['configured_news_sources']} configured**",
        f"- Governed hypotheses: **{payload['measurements']['governed_hypotheses']:,}**; retired: **{payload['measurements']['futility_retired']:,}**; confirmed: **{payload['measurements']['confirmed']:,}**",
        f"- Practice 007 balance/NAV/P&L: **{account.get('balance')} / {account.get('nav')} / {account.get('cumulative_pl')}**", "",
        f"- Source-rank V7 raw / eligible / abstain rows: **{source_conditioned_rank_v7_status.get('total_rows')} / {source_conditioned_rank_v7_status.get('rank_eligible_rows')} / {source_conditioned_rank_v7_status.get('abstain_rows')}**", "",
        f"- Scheduled event clocks / terminal captures: **{payload['measurements']['scheduled_event_clocks']} / {payload['measurements']['scheduled_event_terminal_captures']}**", "",
        f"- Audit snapshot age at publication: **{publication_freshness['age_sec']:.1f}s** (limit **{publication_freshness['maximum_age_sec']:.0f}s**)",
        f"- Free disk: **{(storage.get('disk') or {}).get('free_gib')} GiB ({(storage.get('disk') or {}).get('free_percent')}%)**",
        f"- Verified rotated-log reclamation: **{int((log_archive.get('lifetime') or {}).get('reclaimed_bytes') or log_archive.get('reclaimed_bytes') or 0) / 1024**3:.2f} GiB**", "",
        "| Retained artifact check | Result | Current component scope |", "|---|---|---|",
    ]
    lines.extend(f"| {name} | {'PASS' if passed else 'FAIL'} | {runtime_scope['check_scopes'][name]['scope']} |" for name,passed in checks.items())
    lines.extend(["", "New sources remain research-only until source integrity, point-in-time replay, placebos, incremental-value tests, governed evidence, and untouched confirmation pass.", ""])
    published = _publish_owned_audit(
        guard=guard,
        ownership=ownership,
        output=output,
        report=report,
        history=history,
        payload=payload,
        report_text="\n".join(lines),
    )
    if not published:
        payload["publication_status"] = "superseded"
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--publication-guard", type=Path)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.time()
    while True:
        run(
            output=args.output,
            history=args.history,
            report=args.report,
            publication_guard=args.publication_guard,
        )
        if args.interval_sec <= 0.0 or (
            args.duration_sec > 0.0 and time.time() - started >= args.duration_sec
        ):
            break
        time.sleep(max(60.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "_claim_audit_publication", "_publish_owned_audit",
    "audit_snapshot_publication_freshness",
    "availability_market_state_consistent", "collector_cohort_bound",
    "executable_opportunity_proof_is_fail_closed", "forex_market_state",
    "synchronized_quote_audit", "run",
]
