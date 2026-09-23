#!/usr/bin/env python3
"""Frozen prospective official-event paired evaluator (research only).

Stage one seals an outcome-blind decision and its complete Cartesian schedule
before the first one-minute outcome can exist.  It copies the exact upstream
source, semantic mapping and executable entry-capture bytes into an append-only
ledger.  A causal completed-close technical component is copied at the same
boundary; if no T0-safe snapshot exists, technical arms abstain forever.

Stage two reads only the already-frozen Horizon Capture V1 rows and appends
executable bid/ask outcomes.  Nothing in this module can trade, authorize or
promote a hypothesis.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Callable, Mapping, Sequence

import oanda_official_event_quote_horizon_capture_v1 as horizon_v1
import oanda_official_release_fast_lane as fast_lane


ROOT = Path(__file__).resolve().parent
LOCAL_NEWS = ROOT / "data" / "oanda_training_manager" / "local_news_sentiment"
STATE_ROOT = ROOT / "data" / "oanda_training_manager" / "state"
CONFIG_PATH = ROOT / "config" / "official_event_paired_evaluator_v1.json"
AUTHORITY_MAP_PATH = ROOT / "config" / "official_central_bank_source_map_v1.json"
NEWS_SOURCE_CONFIG_PATH = ROOT / "config" / "news_sources_v1.json"
INPUT_RELEASE_DATABASE = LOCAL_NEWS / "official_release_fast_lane_v4.sqlite"
INPUT_MAPPING_DATABASE = LOCAL_NEWS / "official_release_fast_mapping_v3.sqlite"
INPUT_HORIZON_DATABASE = LOCAL_NEWS / "official_event_quote_horizon_capture_v1.sqlite"
FEATURE_SNAPSHOT_PATH = STATE_ROOT / "live_model_feature_snapshot_v1.json"
OUTPUT_DATABASE = (
    LOCAL_NEWS / "official_event_paired_evaluator_v1_20260830b.sqlite"
)
STATE_PATH = (
    LOCAL_NEWS / "official_event_paired_evaluator_latest_v1_20260830b.json"
)
HEARTBEAT_PATH = (
    LOCAL_NEWS / "official_event_paired_evaluator_heartbeat_v1_20260830b.json"
)

SCHEMA_VERSION = "official_event_paired_evaluator_v1"
CONTRACT_ID = "official_event_paired_evaluator_v1_append_only_20260830"
COHORT_ID = "official_event_paired_evaluator_v1_20260830b"
ACTIVATED_UTC = dt.datetime(2026, 8, 30, 19, 0, tzinfo=dt.timezone.utc)
REQUIRED_MAPPING_CONTRACT_ID = (
    "official_release_fast_mapping_v3_semantic_vs_publish_gate_20260824"
)
REQUIRED_MAPPING_COHORT_ID = "official_release_fast_mapping_v3_20260824"
REQUIRED_CLASSIFICATION_VERSION = (
    "local_fx_news_rules_20260828_v151_pair_breakout_recap_boundary"
)
REQUIRED_ENTRY_CONTRACT_ID = fast_lane.QUOTE_CAPTURE_CONTRACT_ID
REQUIRED_ENTRY_COHORT_ID = fast_lane.QUOTE_CAPTURE_COHORT_ID
REQUIRED_RAW_COLLECTOR_CONTRACT_ID = fast_lane.CONTRACT_ID
REQUIRED_RAW_COLLECTOR_COHORT_ID = fast_lane.COLLECTOR_COHORT_ID
REQUIRED_HORIZON_CONTRACT_ID = horizon_v1.CONTRACT_ID
REQUIRED_HORIZON_COHORT_ID = horizon_v1.COHORT_ID
REQUIRED_AUTHORITY_MAP_CONTRACT_ID = (
    "official_central_bank_source_map_v9_us_policy_communication_clocks_20260827"
)
REQUIRED_AUTHORITY_MAP_SHA256 = (
    "09d4f80708cbed509564e2f7bc5245a24de44df3728ed0583b1d9cbb6f299a19"
)
REQUIRED_NEWS_SOURCE_CONFIG_SHA256 = (
    "6058d3a81d250add66e1d22b0f2541990f3bc90c52575c4a7a97c3361ad0a45b"
)
REQUIRED_FAST_LANE_SOURCE_SHA256 = (
    "55f3a1260c2c3644fa2c4de0e70c4fe081e21fff366caeeaae8fe80bfb1d752a"
)
REQUIRED_HORIZON_CAPTURE_SOURCE_SHA256 = (
    "a0920b5edb530876c42e1e2892f624624db4221262c6a67fdcb42f9a12c146c9"
)
EXPECTED_INSTRUMENTS = tuple(fast_lane.EXPECTED_QUOTE_INSTRUMENTS)
EXPECTED_INSTRUMENT_COUNT = fast_lane.EXPECTED_QUOTE_COUNT
EXPECTED_UNIVERSE_SHA256 = fast_lane.EXPECTED_QUOTE_UNIVERSE_SHA256
ARMS = (
    "official_source_only",
    "price_only",
    "official_plus_technical_confirmation",
    "official_flipped_control",
    "no_trade",
)
HORIZONS_MIN = (1, 5, 15, 30, 60)
SLIPPAGE_STRESS_PIPS = (0.0, 0.25, 0.5)
SEAL_DEADLINE_OFFSET_SEC = 55.0
FIRST_HORIZON_COMMIT_MARGIN_SEC = 5.0
EPISODE_BUCKET_MIN = 15
TECHNICAL_RULE_ID = "completed_m1_ema_5_20_at_t0_v1"
TECHNICAL_FAST_SPAN = 5
TECHNICAL_SLOW_SPAN = 20
TECHNICAL_MINIMUM_CLOSES = 20
TECHNICAL_MAXIMUM_SNAPSHOT_AGE_SEC = 1800.0
TECHNICAL_MAXIMUM_LAST_BAR_AGE_SEC = 120.0
ENTRY_MAXIMUM_CAPTURE_LATENCY_SEC = 15.0
ENTRY_MAXIMUM_QUOTE_AGE_SEC = 30.0
ENTRY_MAXIMUM_FUTURE_SKEW_SEC = 2.0
ENTRY_MAXIMUM_EVENT_OFFSET_SEC = 15.0
HORIZON_MAXIMUM_ATTEMPT_DELAY_SEC = 20.0
HORIZON_MAXIMUM_SNAPSHOT_AGE_SEC = 15.0
HORIZON_MAXIMUM_QUOTE_AGE_SEC = 30.0
HORIZON_MAXIMUM_FUTURE_SKEW_SEC = 2.0
HORIZON_MAXIMUM_TARGET_OFFSET_SEC = 20.0
ATOMIC_JSON_REPLACE_ATTEMPTS = 8
ATOMIC_JSON_REPLACE_INITIAL_DELAY_SEC = 0.01
ATOMIC_JSON_REPLACE_MAX_DELAY_SEC = 0.5
FROZEN_CONFIG_FILE_SHA256 = (
    "d553877ba5140e31c5eb42233761bf5923cae4b44e601325fc776dab6b2e50e1"
)
FROZEN_PRODUCER_SOURCE_SHA256 = "ea3374e4bab29dfc2f5703853927b14d119b40c2394bc7050fab8045f42073d7"
POLICY = {
    "prospective_only": True,
    "historical_backfill_allowed": False,
    "decision_and_schedule_must_be_sealed_before_first_horizon": True,
    "outcome_database_may_not_be_read_before_decision_commit": True,
    "technical_may_confirm_or_veto_official_but_never_reverse": True,
    "one_pair_shared_by_all_arms": True,
    "entry_economics_role": "event_clock_counterfactual_research_measurement_not_proof_of_order_submission_before_semantic_decision",
    "research_only": True,
    "execution_eligible": False,
    "can_place_orders": False,
    "can_authorize": False,
    "can_promote": False,
    "supported_execution_decision": "no_trade",
}


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_utc(value: dt.datetime | None = None) -> str:
    current = value or utc_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=dt.timezone.utc)
    return current.astimezone(dt.timezone.utc).isoformat()


def parse_time(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def finite_number(value: Any) -> float | None:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return None
    return output if math.isfinite(output) else None


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_text(value: str) -> str:
    return sha256_bytes(value.encode("utf-8"))


def read_bytes(path: Path) -> bytes:
    try:
        return path.read_bytes()
    except OSError:
        return b""


def read_json_bytes(path: Path) -> tuple[bytes, dict[str, Any]]:
    raw = read_bytes(path)
    if not raw:
        return b"", {}
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return raw, {}
    return raw, payload if isinstance(payload, dict) else {}


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        for attempt in range(ATOMIC_JSON_REPLACE_ATTEMPTS):
            try:
                os.replace(temporary, path)
                return
            except PermissionError:
                if attempt == ATOMIC_JSON_REPLACE_ATTEMPTS - 1:
                    raise
                time.sleep(
                    min(
                        ATOMIC_JSON_REPLACE_MAX_DELAY_SEC,
                        ATOMIC_JSON_REPLACE_INITIAL_DELAY_SEC * (2**attempt),
                    )
                )
    finally:
        temporary.unlink(missing_ok=True)


def script_sha256() -> str:
    return sha256_bytes(Path(__file__).read_bytes())


def normalized_producer_source_sha256() -> str:
    raw = Path(__file__).read_bytes()
    normalized = re.sub(
        rb'FROZEN_PRODUCER_SOURCE_SHA256 = "[A-Za-z0-9_]+"',
        b'FROZEN_PRODUCER_SOURCE_SHA256 = "<FROZEN>"',
        raw,
        count=1,
    )
    return sha256_bytes(normalized)


def validate_producer_source() -> str:
    observed = normalized_producer_source_sha256()
    if FROZEN_PRODUCER_SOURCE_SHA256 != "TO_BE_FROZEN" and observed != FROZEN_PRODUCER_SOURCE_SHA256:
        raise ValueError("frozen paired-evaluator producer source changed")
    return observed


def validate_dependency_sources() -> tuple[str, str]:
    fast_hash = sha256_bytes(Path(fast_lane.__file__).read_bytes())
    horizon_hash = sha256_bytes(Path(horizon_v1.__file__).read_bytes())
    if fast_hash != REQUIRED_FAST_LANE_SOURCE_SHA256:
        raise ValueError("frozen entry-capture dependency source changed")
    if horizon_hash != REQUIRED_HORIZON_CAPTURE_SOURCE_SHA256:
        raise ValueError("frozen horizon-capture dependency source changed")
    observed_limits = {
        "entry_capture_latency": float(fast_lane.QUOTE_CAPTURE_MAX_LATENCY_SECONDS),
        "entry_quote_age": float(fast_lane.QUOTE_CAPTURE_MAX_QUOTE_AGE_SECONDS),
        "entry_future_skew": float(fast_lane.QUOTE_CAPTURE_MAX_FUTURE_SKEW_SECONDS),
        "entry_event_offset": float(fast_lane.QUOTE_CAPTURE_MAX_ENTRY_OFFSET_SECONDS),
        "horizon_attempt_delay": float(horizon_v1.MAXIMUM_ATTEMPT_DELAY_SEC),
        "horizon_snapshot_age": float(horizon_v1.MAXIMUM_SNAPSHOT_AGE_SEC),
        "horizon_quote_age": float(horizon_v1.MAXIMUM_QUOTE_AGE_SEC),
        "horizon_future_skew": float(horizon_v1.MAXIMUM_FUTURE_SKEW_SEC),
        "horizon_target_offset": float(horizon_v1.MAXIMUM_TARGET_OFFSET_SEC),
    }
    frozen_limits = {
        "entry_capture_latency": ENTRY_MAXIMUM_CAPTURE_LATENCY_SEC,
        "entry_quote_age": ENTRY_MAXIMUM_QUOTE_AGE_SEC,
        "entry_future_skew": ENTRY_MAXIMUM_FUTURE_SKEW_SEC,
        "entry_event_offset": ENTRY_MAXIMUM_EVENT_OFFSET_SEC,
        "horizon_attempt_delay": HORIZON_MAXIMUM_ATTEMPT_DELAY_SEC,
        "horizon_snapshot_age": HORIZON_MAXIMUM_SNAPSHOT_AGE_SEC,
        "horizon_quote_age": HORIZON_MAXIMUM_QUOTE_AGE_SEC,
        "horizon_future_skew": HORIZON_MAXIMUM_FUTURE_SKEW_SEC,
        "horizon_target_offset": HORIZON_MAXIMUM_TARGET_OFFSET_SEC,
    }
    if observed_limits != frozen_limits:
        raise ValueError("frozen dependency timing limits changed")
    return fast_hash, horizon_hash


def _configured_source_lineage(source: Mapping[str, Any]) -> tuple[str, str]:
    contract = str(source.get("source_contract_id") or "").strip()
    cohort = str(source.get("source_cohort_id") or "").strip()
    if contract:
        return contract, cohort
    canonical = {
        str(key): value
        for key, value in source.items()
        if key not in {
            "source_contract_id", "source_cohort_id", "source_contract_derived",
            "source_lineage_version", "source_config_sha256",
        }
    }
    digest = sha256_bytes(
        json.dumps(
            canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("utf-8")
    )
    source_id = re.sub(
        r"[^a-z0-9_.-]+", "_", str(source.get("source_id") or "").casefold()
    ).strip("_") or "unknown"
    derived = f"derived_source_config_lineage_v1:{source_id}:{digest[:24]}"
    return derived, derived


def validate_news_source_config(
    path: Path = NEWS_SOURCE_CONFIG_PATH,
) -> tuple[bytes, dict[str, tuple[str, str]]]:
    raw, payload = read_json_bytes(path)
    if sha256_bytes(raw) != REQUIRED_NEWS_SOURCE_CONFIG_SHA256:
        raise ValueError("frozen news source config bytes changed")
    lineages: dict[str, tuple[str, str]] = {}
    for row in payload.get("sources") or []:
        if not isinstance(row, Mapping):
            continue
        source_id = str(row.get("source_id") or "").strip()
        if source_id:
            lineages[source_id] = _configured_source_lineage(row)
    if not lineages:
        raise ValueError("frozen news source config has no source lineages")
    return raw, lineages


def validate_authority_map(
    path: Path = AUTHORITY_MAP_PATH,
) -> tuple[bytes, dict[str, Any]]:
    raw, payload = read_json_bytes(path)
    if sha256_bytes(raw) != REQUIRED_AUTHORITY_MAP_SHA256:
        raise ValueError("frozen source-authority map bytes changed")
    if payload.get("contract_id") != REQUIRED_AUTHORITY_MAP_CONTRACT_ID:
        raise ValueError("frozen source-authority map contract changed")
    if int(payload.get("expected_currency_count") or 0) != 21:
        raise ValueError("frozen source-authority currency count changed")
    return raw, payload


def authority_currency_for_source(
    authority_map: Mapping[str, Any], source_id: str
) -> str:
    matches: set[str] = set()
    for row in authority_map.get("currencies") or []:
        if not isinstance(row, Mapping):
            continue
        governed = {
            str(item or "").strip()
            for field in (
                "release_source_ids",
                "statistical_release_source_ids",
                "communication_source_ids",
            )
            for item in (row.get(field) or [])
            if str(item or "").strip()
        }
        if source_id in governed:
            matches.add(str(row.get("currency") or "").upper().strip())
    return next(iter(matches)) if len(matches) == 1 else ""


def validate_frozen_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    raw, payload = read_json_bytes(path)
    if not payload:
        raise ValueError("paired evaluator config missing or malformed")
    if sha256_bytes(raw) != FROZEN_CONFIG_FILE_SHA256:
        raise ValueError("frozen paired-evaluator config bytes changed")
    expected = {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "required_entry_capture_contract_id": REQUIRED_ENTRY_CONTRACT_ID,
        "required_entry_capture_cohort_id": REQUIRED_ENTRY_COHORT_ID,
        "required_raw_release_collector_contract_id": REQUIRED_RAW_COLLECTOR_CONTRACT_ID,
        "required_raw_release_collector_cohort_id": REQUIRED_RAW_COLLECTOR_COHORT_ID,
        "required_mapping_contract_id": REQUIRED_MAPPING_CONTRACT_ID,
        "required_mapping_cohort_id": REQUIRED_MAPPING_COHORT_ID,
        "required_classification_version": REQUIRED_CLASSIFICATION_VERSION,
        "required_horizon_capture_contract_id": REQUIRED_HORIZON_CONTRACT_ID,
        "required_horizon_capture_cohort_id": REQUIRED_HORIZON_COHORT_ID,
        "required_source_authority_map_contract_id": REQUIRED_AUTHORITY_MAP_CONTRACT_ID,
        "required_source_authority_map_sha256": REQUIRED_AUTHORITY_MAP_SHA256,
        "required_news_source_config_sha256": REQUIRED_NEWS_SOURCE_CONFIG_SHA256,
        "required_fast_lane_source_sha256": REQUIRED_FAST_LANE_SOURCE_SHA256,
        "required_horizon_capture_source_sha256": REQUIRED_HORIZON_CAPTURE_SOURCE_SHA256,
        "expected_instrument_count": EXPECTED_INSTRUMENT_COUNT,
        "expected_instrument_universe_sha256": EXPECTED_UNIVERSE_SHA256,
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise ValueError(f"frozen paired-evaluator config mismatch:{key}")
    if parse_time(payload.get("activated_utc")) != ACTIVATED_UTC:
        raise ValueError("frozen paired-evaluator activation changed")
    decision = payload.get("decision") if isinstance(payload.get("decision"), Mapping) else {}
    for key, value in {
        "first_horizon_min": 1,
        "seal_deadline_offset_sec": SEAL_DEADLINE_OFFSET_SEC,
        "pair_selection": "lowest_entry_spread_among_pairs_containing_issuer_then_lexicographic",
        "clock_policy": "fresh_read_start_and_fresh_precommit_clock_per_event",
        "first_horizon_commit_margin_sec": FIRST_HORIZON_COMMIT_MARGIN_SEC,
        "issuer_binding": "single_identical_currency_from_direct_verified_raw_source_authority_and_mapping_source_currencies; mentioned currencies ignored",
        "mapping_selection": "earliest_mapping_copy_observed_by_seal_deadline",
        "market_episode_bucket_min": EPISODE_BUCKET_MIN,
    }.items():
        if decision.get(key) != value:
            raise ValueError(f"frozen paired-evaluator decision mismatch:{key}")
    technical = payload.get("technical_rule") if isinstance(payload.get("technical_rule"), Mapping) else {}
    for key, value in {
        "rule_id": TECHNICAL_RULE_ID,
        "feature_snapshot_schema_version": 1,
        "required_completed_bars_contract": True,
        "required_timeframe": "M1",
        "minimum_completed_closes": TECHNICAL_MINIMUM_CLOSES,
        "fast_ema_span": TECHNICAL_FAST_SPAN,
        "slow_ema_span": TECHNICAL_SLOW_SPAN,
        "maximum_snapshot_age_sec": TECHNICAL_MAXIMUM_SNAPSHOT_AGE_SEC,
        "maximum_last_completed_bar_age_sec": TECHNICAL_MAXIMUM_LAST_BAR_AGE_SEC,
        "snapshot_must_not_postdate_event_t0": True,
        "last_bar_must_complete_by_event_t0": True,
        "missing_or_noncausal_policy": "terminal_abstain_never_attach_later",
    }.items():
        if technical.get(key) != value:
            raise ValueError(f"frozen paired-evaluator technical mismatch:{key}")
    if tuple(payload.get("arms") or ()) != ARMS:
        raise ValueError("frozen paired-evaluator arms changed")
    if tuple(int(value) for value in payload.get("horizons_min") or ()) != HORIZONS_MIN:
        raise ValueError("frozen paired-evaluator horizons changed")
    if tuple(float(value) for value in payload.get("round_trip_slippage_pips") or ()) != SLIPPAGE_STRESS_PIPS:
        raise ValueError("frozen paired-evaluator slippage stresses changed")
    policy = payload.get("policy") if isinstance(payload.get("policy"), Mapping) else {}
    for key, value in POLICY.items():
        if policy.get(key) != value:
            raise ValueError(f"unsafe paired-evaluator policy:{key}")
    entry_quality = (
        payload.get("entry_capture_quality")
        if isinstance(payload.get("entry_capture_quality"), Mapping)
        else {}
    )
    for key, value in {
        "maximum_capture_latency_sec": ENTRY_MAXIMUM_CAPTURE_LATENCY_SEC,
        "maximum_quote_age_sec": ENTRY_MAXIMUM_QUOTE_AGE_SEC,
        "maximum_future_skew_sec": ENTRY_MAXIMUM_FUTURE_SKEW_SEC,
        "maximum_event_offset_sec": ENTRY_MAXIMUM_EVENT_OFFSET_SEC,
    }.items():
        if entry_quality.get(key) != value:
            raise ValueError(f"frozen entry-capture quality mismatch:{key}")
    horizon_quality = (
        payload.get("horizon_capture_quality")
        if isinstance(payload.get("horizon_capture_quality"), Mapping)
        else {}
    )
    for key, value in {
        "maximum_attempt_delay_sec": HORIZON_MAXIMUM_ATTEMPT_DELAY_SEC,
        "maximum_snapshot_age_sec": HORIZON_MAXIMUM_SNAPSHOT_AGE_SEC,
        "maximum_quote_age_sec": HORIZON_MAXIMUM_QUOTE_AGE_SEC,
        "maximum_future_skew_sec": HORIZON_MAXIMUM_FUTURE_SKEW_SEC,
        "maximum_target_offset_sec": HORIZON_MAXIMUM_TARGET_OFFSET_SEC,
    }.items():
        if horizon_quality.get(key) != value:
            raise ValueError(f"frozen horizon-capture quality mismatch:{key}")
    return payload


def open_database(path: Path = OUTPUT_DATABASE) -> sqlite3.Connection:
    validate_frozen_config()
    producer_identity = validate_producer_source()
    fast_lane_identity, horizon_identity = validate_dependency_sources()
    authority_bytes, _ = validate_authority_map()
    news_source_bytes, _ = validate_news_source_config()
    config_hash = sha256_bytes(CONFIG_PATH.read_bytes())
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS paired_event_cohort_manifest (
              manifest_id TEXT PRIMARY KEY,
              contract_id TEXT NOT NULL UNIQUE,
              cohort_id TEXT NOT NULL UNIQUE,
              activated_utc TEXT NOT NULL,
              config_file_sha256 TEXT NOT NULL,
              producer_source_sha256 TEXT NOT NULL,
              producer_file_sha256 TEXT NOT NULL,
              authority_map_sha256 TEXT NOT NULL,
              news_source_config_sha256 TEXT NOT NULL,
              fast_lane_source_sha256 TEXT NOT NULL,
              horizon_capture_source_sha256 TEXT NOT NULL,
              required_classification_version TEXT NOT NULL,
              entry_max_capture_latency_sec REAL NOT NULL,
              entry_max_quote_age_sec REAL NOT NULL,
              entry_max_future_skew_sec REAL NOT NULL,
              entry_max_event_offset_sec REAL NOT NULL,
              horizon_max_attempt_delay_sec REAL NOT NULL,
              horizon_max_snapshot_age_sec REAL NOT NULL,
              horizon_max_quote_age_sec REAL NOT NULL,
              horizon_max_future_skew_sec REAL NOT NULL,
              horizon_max_target_offset_sec REAL NOT NULL,
              research_only INTEGER NOT NULL CHECK(research_only=1),
              execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
              can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
              can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
              can_promote INTEGER NOT NULL CHECK(can_promote=0)
            );
            CREATE TABLE IF NOT EXISTS paired_event_decision (
              decision_id TEXT PRIMARY KEY,
              input_capture_id TEXT NOT NULL UNIQUE,
              observation_id TEXT NOT NULL,
              event_first_known_utc TEXT NOT NULL,
              decision_read_started_utc TEXT NOT NULL,
              decision_precommit_utc TEXT NOT NULL,
              issued_utc TEXT NOT NULL,
              decision_latency_sec REAL NOT NULL,
              first_horizon_target_utc TEXT NOT NULL,
              decision_state TEXT NOT NULL,
              invalid_reason TEXT NOT NULL,
              source_id TEXT NOT NULL,
              source_contract_id TEXT NOT NULL,
              source_cohort_id TEXT NOT NULL,
              issuer_currency TEXT NOT NULL,
              selected_instrument TEXT NOT NULL,
              official_currency_score REAL,
              official_pair_side TEXT NOT NULL,
              technical_pair_side TEXT NOT NULL,
              technical_state TEXT NOT NULL,
              technical_reason TEXT NOT NULL,
              entry_bid REAL,
              entry_ask REAL,
              entry_pip REAL,
              entry_spread_pips REAL,
              entry_quote_utc TEXT NOT NULL,
              market_episode_id TEXT NOT NULL,
              outcome_schedule_json TEXT NOT NULL,
              outcome_schedule_sha256 TEXT NOT NULL,
              input_capture_bytes BLOB NOT NULL,
              input_capture_sha256 TEXT NOT NULL,
              input_capture_row_bytes BLOB NOT NULL,
              input_capture_row_sha256 TEXT NOT NULL,
              entry_component_bytes BLOB NOT NULL,
              entry_component_sha256 TEXT NOT NULL,
              source_payload_bytes BLOB NOT NULL,
              source_payload_sha256 TEXT NOT NULL,
              source_row_bytes BLOB NOT NULL,
              source_row_sha256 TEXT NOT NULL,
              authority_map_contract_id TEXT NOT NULL,
              authority_map_bytes BLOB NOT NULL,
              authority_map_sha256 TEXT NOT NULL,
              news_source_config_sha256 TEXT NOT NULL,
              mapping_id TEXT NOT NULL,
              mapping_payload_bytes BLOB NOT NULL,
              mapping_payload_sha256 TEXT NOT NULL,
              mapping_row_bytes BLOB NOT NULL,
              mapping_row_sha256 TEXT NOT NULL,
              technical_material_bytes BLOB NOT NULL,
              technical_material_sha256 TEXT NOT NULL,
              technical_snapshot_file_sha256 TEXT NOT NULL,
              config_file_sha256 TEXT NOT NULL,
              producer_source_sha256 TEXT NOT NULL,
              producer_file_sha256 TEXT NOT NULL,
              fast_lane_source_sha256 TEXT NOT NULL,
              horizon_capture_source_sha256 TEXT NOT NULL,
              entry_economics_role TEXT NOT NULL,
              research_only INTEGER NOT NULL CHECK(research_only=1),
              execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
              can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
              can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
              can_promote INTEGER NOT NULL CHECK(can_promote=0),
              contract_id TEXT NOT NULL,
              cohort_id TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS paired_event_arm (
              arm_id TEXT PRIMARY KEY,
              decision_id TEXT NOT NULL,
              arm_name TEXT NOT NULL CHECK(arm_name IN (
                'official_source_only','price_only',
                'official_plus_technical_confirmation',
                'official_flipped_control','no_trade')),
              action_state TEXT NOT NULL CHECK(action_state IN ('trade','abstain','invalid')),
              side TEXT NOT NULL CHECK(side IN ('buy','sell','abstain','invalid')),
              reason TEXT NOT NULL,
              signed_currency_factor_id TEXT NOT NULL,
              market_episode_id TEXT NOT NULL,
              research_only INTEGER NOT NULL CHECK(research_only=1),
              execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
              can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
              can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
              can_promote INTEGER NOT NULL CHECK(can_promote=0),
              contract_id TEXT NOT NULL,
              cohort_id TEXT NOT NULL,
              UNIQUE(decision_id,arm_name),
              FOREIGN KEY(decision_id) REFERENCES paired_event_decision(decision_id)
            );
            CREATE TABLE IF NOT EXISTS paired_event_horizon_input (
              horizon_input_id TEXT PRIMARY KEY,
              decision_id TEXT NOT NULL,
              input_horizon_capture_id TEXT NOT NULL,
              horizon_min INTEGER NOT NULL CHECK(horizon_min IN (1,5,15,30,60)),
              input_state TEXT NOT NULL CHECK(input_state IN ('valid','invalid')),
              invalid_reason TEXT NOT NULL,
              horizon_payload_bytes BLOB NOT NULL,
              horizon_payload_sha256 TEXT NOT NULL,
              horizon_row_bytes BLOB NOT NULL,
              horizon_row_sha256 TEXT NOT NULL,
              exit_component_bytes BLOB NOT NULL,
              exit_component_sha256 TEXT NOT NULL,
              exit_component_row_bytes BLOB NOT NULL,
              exit_component_row_sha256 TEXT NOT NULL,
              exit_bid REAL,
              exit_ask REAL,
              exit_pip REAL,
              exit_quote_utc TEXT NOT NULL,
              research_only INTEGER NOT NULL CHECK(research_only=1),
              execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
              can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
              can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
              can_promote INTEGER NOT NULL CHECK(can_promote=0),
              contract_id TEXT NOT NULL,
              cohort_id TEXT NOT NULL,
              UNIQUE(decision_id,horizon_min),
              FOREIGN KEY(decision_id) REFERENCES paired_event_decision(decision_id)
            );
            CREATE TABLE IF NOT EXISTS paired_event_outcome (
              outcome_id TEXT PRIMARY KEY,
              decision_id TEXT NOT NULL,
              arm_id TEXT NOT NULL,
              horizon_input_id TEXT NOT NULL,
              arm_name TEXT NOT NULL,
              horizon_min INTEGER NOT NULL CHECK(horizon_min IN (1,5,15,30,60)),
              slippage_stress_pips REAL NOT NULL CHECK(slippage_stress_pips IN (0.0,0.25,0.5)),
              outcome_state TEXT NOT NULL CHECK(outcome_state IN ('trade','abstain','invalid')),
              outcome_reason TEXT NOT NULL,
              gross_executable_pips REAL,
              spread_cost_pips REAL,
              net_after_cost_pips REAL,
              research_only INTEGER NOT NULL CHECK(research_only=1),
              execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
              can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
              can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
              can_promote INTEGER NOT NULL CHECK(can_promote=0),
              contract_id TEXT NOT NULL,
              cohort_id TEXT NOT NULL,
              UNIQUE(decision_id,arm_name,horizon_min,slippage_stress_pips),
              FOREIGN KEY(decision_id) REFERENCES paired_event_decision(decision_id),
              FOREIGN KEY(arm_id) REFERENCES paired_event_arm(arm_id),
              FOREIGN KEY(horizon_input_id) REFERENCES paired_event_horizon_input(horizon_input_id)
            );
            CREATE INDEX IF NOT EXISTS paired_event_decision_event
              ON paired_event_decision(event_first_known_utc,issuer_currency);
            CREATE INDEX IF NOT EXISTS paired_event_arm_factor_episode
              ON paired_event_arm(signed_currency_factor_id,market_episode_id);
            CREATE INDEX IF NOT EXISTS paired_event_outcome_cell
              ON paired_event_outcome(arm_name,horizon_min,slippage_stress_pips,outcome_state);
            """
        )
        for table in (
            "paired_event_cohort_manifest",
            "paired_event_decision",
            "paired_event_arm",
            "paired_event_horizon_input",
            "paired_event_outcome",
        ):
            connection.executescript(
                f"""
                CREATE TRIGGER IF NOT EXISTS {table}_no_update
                  BEFORE UPDATE ON {table}
                  BEGIN SELECT RAISE(ABORT,'{table} is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS {table}_no_delete
                  BEFORE DELETE ON {table}
                  BEGIN SELECT RAISE(ABORT,'{table} is append-only'); END;
                """
            )
        expected_manifest = {
            "manifest_id": "official_event_paired_evaluator_v1_manifest",
            "contract_id": CONTRACT_ID,
            "cohort_id": COHORT_ID,
            "activated_utc": iso_utc(ACTIVATED_UTC),
            "config_file_sha256": config_hash,
            "producer_source_sha256": producer_identity,
            "producer_file_sha256": script_sha256(),
            "authority_map_sha256": sha256_bytes(authority_bytes),
            "news_source_config_sha256": sha256_bytes(news_source_bytes),
            "fast_lane_source_sha256": fast_lane_identity,
            "horizon_capture_source_sha256": horizon_identity,
            "required_classification_version": REQUIRED_CLASSIFICATION_VERSION,
            "entry_max_capture_latency_sec": ENTRY_MAXIMUM_CAPTURE_LATENCY_SEC,
            "entry_max_quote_age_sec": ENTRY_MAXIMUM_QUOTE_AGE_SEC,
            "entry_max_future_skew_sec": ENTRY_MAXIMUM_FUTURE_SKEW_SEC,
            "entry_max_event_offset_sec": ENTRY_MAXIMUM_EVENT_OFFSET_SEC,
            "horizon_max_attempt_delay_sec": HORIZON_MAXIMUM_ATTEMPT_DELAY_SEC,
            "horizon_max_snapshot_age_sec": HORIZON_MAXIMUM_SNAPSHOT_AGE_SEC,
            "horizon_max_quote_age_sec": HORIZON_MAXIMUM_QUOTE_AGE_SEC,
            "horizon_max_future_skew_sec": HORIZON_MAXIMUM_FUTURE_SKEW_SEC,
            "horizon_max_target_offset_sec": HORIZON_MAXIMUM_TARGET_OFFSET_SEC,
            "research_only": 1,
            "execution_eligible": 0,
            "can_place_orders": 0,
            "can_authorize": 0,
            "can_promote": 0,
        }
        existing_manifest = connection.execute(
            "SELECT * FROM paired_event_cohort_manifest"
        ).fetchall()
        if not existing_manifest:
            columns = tuple(expected_manifest)
            connection.execute(
                f"INSERT INTO paired_event_cohort_manifest "
                f"({','.join(columns)}) VALUES "
                f"({','.join('?' for _ in columns)})",
                tuple(expected_manifest[column] for column in columns),
            )
        else:
            if len(existing_manifest) != 1 or dict(existing_manifest[0]) != expected_manifest:
                raise ValueError("paired-evaluator cohort manifest identity mismatch")
        connection.commit()
        return connection
    except BaseException:
        connection.close()
        raise


def _ema(values: Sequence[float], span: int) -> float:
    alpha = 2.0 / (float(span) + 1.0)
    output = float(values[0])
    for value in values[1:]:
        output = alpha * float(value) + (1.0 - alpha) * output
    return output


def technical_at_t0(
    snapshot: Mapping[str, Any],
    *,
    snapshot_file_bytes: bytes,
    instrument: str,
    event_t0: dt.datetime,
) -> tuple[str, str, bytes, str]:
    """Return a terminal technical side or abstention from a causal T0 view."""

    base_material: dict[str, Any] = {
        "rule_id": TECHNICAL_RULE_ID,
        "instrument": instrument,
        "event_t0_utc": iso_utc(event_t0),
        "snapshot_schema_version": snapshot.get("schema_version"),
        "snapshot_id": snapshot.get("snapshot_id"),
        "snapshot_generated_utc": snapshot.get("generated_utc"),
    }
    reason = ""
    generated = parse_time(snapshot.get("generated_utc"))
    contract = snapshot.get("contract") if isinstance(snapshot.get("contract"), Mapping) else {}
    intrahour = contract.get("intrahour_forecast") if isinstance(contract.get("intrahour_forecast"), Mapping) else {}
    instruments = snapshot.get("instruments") if isinstance(snapshot.get("instruments"), Mapping) else {}
    row = instruments.get(instrument) if isinstance(instruments.get(instrument), Mapping) else {}
    structural = row.get("structural_series") if isinstance(row.get("structural_series"), Mapping) else {}
    m1 = structural.get("M1") if isinstance(structural.get("M1"), Mapping) else {}
    closes_raw = m1.get("close") if isinstance(m1.get("close"), list) else []
    times_raw = m1.get("bar_start_times_utc") if isinstance(m1.get("bar_start_times_utc"), list) else []
    closes = [finite_number(value) for value in closes_raw]
    if not snapshot_file_bytes or not snapshot:
        reason = "technical_snapshot_missing"
    elif snapshot.get("schema_version") != 1:
        reason = "technical_snapshot_schema_mismatch"
    elif generated is None:
        reason = "technical_snapshot_clock_missing"
    elif generated > event_t0:
        reason = "technical_snapshot_postdates_event_t0"
    elif (event_t0 - generated).total_seconds() > TECHNICAL_MAXIMUM_SNAPSHOT_AGE_SEC:
        reason = "technical_snapshot_stale_at_event_t0"
    elif contract.get("feature_values_observable_at_generation") is not True:
        reason = "technical_snapshot_not_observable_at_generation"
    elif contract.get("historical_outcomes_included") is not False:
        reason = "technical_snapshot_contains_outcomes"
    elif intrahour.get("completed_bars_only") is not True:
        reason = "technical_snapshot_not_completed_bars_only"
    elif not row:
        reason = "technical_instrument_missing"
    elif len(closes) < TECHNICAL_MINIMUM_CLOSES or len(times_raw) != len(closes_raw):
        reason = "technical_completed_close_history_insufficient"
    elif any(value is None or value <= 0.0 for value in closes[-TECHNICAL_MINIMUM_CLOSES:]):
        reason = "technical_completed_closes_invalid"
    else:
        used_times = [parse_time(value) for value in times_raw[-TECHNICAL_MINIMUM_CLOSES:]]
        if any(value is None for value in used_times):
            reason = "technical_completed_bar_clock_missing"
        else:
            valid_times = [value for value in used_times if value is not None]
            if any(right <= left for left, right in zip(valid_times, valid_times[1:])):
                reason = "technical_completed_bar_clocks_not_strictly_increasing"
            elif generated is None or any(
                value + dt.timedelta(minutes=1) > generated for value in valid_times
            ):
                reason = "technical_bar_not_complete_at_snapshot_generation"
            elif any(
                value + dt.timedelta(minutes=1) > event_t0 for value in valid_times
            ):
                reason = "technical_bar_not_complete_at_event_t0"
            elif (
                event_t0 - (valid_times[-1] + dt.timedelta(minutes=1))
            ).total_seconds() > TECHNICAL_MAXIMUM_LAST_BAR_AGE_SEC:
                reason = "technical_last_completed_bar_stale_at_event_t0"

    base_material["snapshot_contract"] = contract
    base_material["instrument_component"] = row
    if reason:
        base_material.update({"technical_state": "abstain", "technical_reason": reason})
        material = canonical_json(base_material).encode("utf-8")
        return "", reason, material, sha256_bytes(snapshot_file_bytes) if snapshot_file_bytes else ""

    valid_closes = [float(value) for value in closes[-TECHNICAL_MINIMUM_CLOSES:] if value is not None]
    fast = _ema(valid_closes, TECHNICAL_FAST_SPAN)
    slow = _ema(valid_closes, TECHNICAL_SLOW_SPAN)
    side = "buy" if fast > slow else "sell" if fast < slow else ""
    reason = "" if side else "technical_ema_tie"
    base_material.update(
        {
            "technical_state": "trade" if side else "abstain",
            "technical_reason": reason,
            "technical_side": side,
            "fast_ema": fast,
            "slow_ema": slow,
            "closes_used": valid_closes,
        }
    )
    material = canonical_json(base_material).encode("utf-8")
    return side, reason, material, sha256_bytes(snapshot_file_bytes)


def _single_currency(value: Any) -> str:
    if not isinstance(value, (list, tuple)):
        return ""
    currencies = sorted({str(item or "").upper().strip() for item in value if str(item or "").strip()})
    return currencies[0] if len(currencies) == 1 else ""


def _pair_side_for_issuer(instrument: str, issuer: str, score: float) -> str:
    base, quote = instrument.split("_", 1)
    issuer_strengthens = score > 0.0
    if issuer == base:
        return "buy" if issuer_strengthens else "sell"
    if issuer == quote:
        return "sell" if issuer_strengthens else "buy"
    return ""


def _reverse_side(side: str) -> str:
    return "sell" if side == "buy" else "buy" if side == "sell" else ""


def select_lowest_spread_pair(
    quotes: Mapping[str, Any], issuer_currency: str
) -> tuple[str, dict[str, Any], str]:
    candidates: list[tuple[float, str, dict[str, Any]]] = []
    for instrument in EXPECTED_INSTRUMENTS:
        if issuer_currency not in instrument.split("_"):
            continue
        row = quotes.get(instrument)
        if not isinstance(row, Mapping):
            continue
        bid = finite_number(row.get("bid"))
        ask = finite_number(row.get("ask"))
        pip = finite_number(row.get("pip"))
        if bid is None or ask is None or pip is None or bid <= 0 or ask <= bid or pip <= 0:
            continue
        component = dict(row)
        spread = (ask - bid) / pip
        candidates.append((round(spread, 12), instrument, component))
    if not candidates:
        return "", {}, "issuer_pair_entry_quote_missing"
    _, instrument, row = sorted(candidates, key=lambda value: (value[0], value[1]))[0]
    return instrument, row, ""


def _episode_id(issuer: str, event: dt.datetime) -> str:
    seconds = EPISODE_BUCKET_MIN * 60
    bucket_epoch = int(event.timestamp()) // seconds * seconds
    material = f"{issuer}|{bucket_epoch}|{EPISODE_BUCKET_MIN}m"
    return "official_event_episode_" + sha256_text(material)[:32]


def _factor_id(instrument: str, side: str, issuer: str) -> str:
    """Deduplicate by the signed issuer factor, never by the chosen pair."""

    if side not in {"buy", "sell"} or "_" not in instrument or not issuer:
        return ""
    base, quote = instrument.split("_", 1)
    if issuer == base:
        sign = "+" if side == "buy" else "-"
    elif issuer == quote:
        sign = "-" if side == "buy" else "+"
    else:
        return ""
    material = f"{issuer}|{sign}"
    return "signed_currency_factor_" + sha256_text(material)[:32]


def _arm_rows(
    *,
    decision_id: str,
    decision_state: str,
    decision_reason: str,
    instrument: str,
    issuer_currency: str,
    market_episode_id: str,
    official_side: str,
    technical_side: str,
    technical_reason: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name in ARMS:
        action = "abstain"
        side = "abstain"
        reason = ""
        if name == "no_trade":
            reason = "predeclared_no_trade_control"
        elif decision_state != "sealed":
            action, side, reason = "invalid", "invalid", decision_reason
        elif name == "official_source_only":
            if official_side:
                action, side = "trade", official_side
            else:
                reason = "official_issuer_direction_unavailable"
        elif name == "price_only":
            if technical_side:
                action, side = "trade", technical_side
            else:
                reason = technical_reason or "technical_direction_unavailable"
        elif name == "official_plus_technical_confirmation":
            if not official_side:
                reason = "official_issuer_direction_unavailable"
            elif not technical_side:
                reason = technical_reason or "technical_direction_unavailable"
            elif technical_side == official_side:
                action, side = "trade", official_side
            else:
                reason = "technical_veto_direction_conflict"
        elif name == "official_flipped_control":
            if official_side:
                action, side = "trade", _reverse_side(official_side)
            else:
                reason = "official_issuer_direction_unavailable"
        arm_id = "paired_event_arm_" + sha256_text(
            f"{decision_id}|{name}|{CONTRACT_ID}|{COHORT_ID}"
        )[:32]
        rows.append(
            {
                "arm_id": arm_id,
                "decision_id": decision_id,
                "arm_name": name,
                "action_state": action,
                "side": side,
                "reason": reason,
                "signed_currency_factor_id": _factor_id(
                    instrument, side, issuer_currency
                ),
                "market_episode_id": market_episode_id,
            }
        )
    return rows


def build_decision(
    *,
    capture_row: Mapping[str, Any],
    source_row: Mapping[str, Any] | None,
    mapping_row: Mapping[str, Any] | None,
    feature_snapshot_bytes: bytes,
    feature_snapshot: Mapping[str, Any],
    issued_utc: dt.datetime,
    decision_read_started_utc: dt.datetime | None = None,
    authority_map_bytes: bytes | None = None,
    authority_map: Mapping[str, Any] | None = None,
    news_source_config_bytes: bytes | None = None,
    news_source_lineages: Mapping[str, tuple[str, str]] | None = None,
    config_bytes: bytes | None = None,
    producer_sha256: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Build one immutable outcome-blind decision and all five arms."""

    issued = issued_utc.astimezone(dt.timezone.utc) if issued_utc.tzinfo else issued_utc.replace(tzinfo=dt.timezone.utc)
    read_started = decision_read_started_utc or issued
    read_started = (
        read_started.astimezone(dt.timezone.utc)
        if read_started.tzinfo
        else read_started.replace(tzinfo=dt.timezone.utc)
    )
    capture_bytes = str(capture_row.get("capture_payload_json") or "").encode("utf-8")
    source_bytes = str((source_row or {}).get("raw_payload_json") or "").encode("utf-8")
    mapping_bytes = str((mapping_row or {}).get("mapping_payload_json") or "").encode("utf-8")
    capture_row_bytes = canonical_json(dict(capture_row)).encode("utf-8")
    source_row_bytes = canonical_json(dict(source_row or {})).encode("utf-8")
    mapping_row_bytes = canonical_json(dict(mapping_row or {})).encode("utf-8")
    authority_bytes = (
        authority_map_bytes
        if authority_map_bytes is not None
        else read_bytes(AUTHORITY_MAP_PATH)
    )
    if authority_map is None:
        try:
            decoded_authority = json.loads(authority_bytes.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            decoded_authority = {}
        authority = decoded_authority if isinstance(decoded_authority, Mapping) else {}
    else:
        authority = authority_map
    if news_source_config_bytes is None or news_source_lineages is None:
        frozen_news_source_bytes, frozen_source_lineages = validate_news_source_config()
        news_config_bytes = frozen_news_source_bytes
        source_lineages = frozen_source_lineages
    else:
        news_config_bytes = news_source_config_bytes
        source_lineages = news_source_lineages
    fast_dependency_hash, horizon_dependency_hash = validate_dependency_sources()
    try:
        capture = json.loads(capture_bytes.decode("utf-8")) if capture_bytes else {}
    except ValueError:
        capture = {}
    try:
        source = json.loads(source_bytes.decode("utf-8")) if source_bytes else {}
    except ValueError:
        source = {}
    try:
        mapping = json.loads(mapping_bytes.decode("utf-8")) if mapping_bytes else {}
    except ValueError:
        mapping = {}

    event = parse_time(capture_row.get("event_first_known_utc") or capture.get("event_first_known_utc"))
    if event is None:
        event = ACTIVATED_UTC
    first_target = event + dt.timedelta(minutes=1)
    invalid_reasons: list[str] = []
    if event < ACTIVATED_UTC:
        invalid_reasons.append("event_before_cohort_activation")
    if issued < event:
        invalid_reasons.append("decision_issued_before_event_t0")
    if read_started < event or read_started > issued:
        invalid_reasons.append("decision_read_clock_invalid")
    if issued > first_target or (issued - event).total_seconds() > SEAL_DEADLINE_OFFSET_SEC:
        invalid_reasons.append("decision_not_sealed_before_first_horizon")
    if str(capture_row.get("capture_contract_id") or "") != REQUIRED_ENTRY_CONTRACT_ID:
        invalid_reasons.append("entry_capture_contract_mismatch")
    if str(capture_row.get("capture_cohort_id") or "") != REQUIRED_ENTRY_COHORT_ID:
        invalid_reasons.append("entry_capture_cohort_mismatch")
    if int(capture_row.get("research_only") or 0) != 1 or any(
        int(capture_row.get(key) or 0) != 0
        for key in ("execution_eligible", "can_authorize", "can_promote")
    ):
        invalid_reasons.append("entry_capture_safety_boundary_invalid")
    if (
        not capture
        or capture.get("timing_quality") != "prospective_exact_live_quote"
        or capture.get("capture_contract_id") != REQUIRED_ENTRY_CONTRACT_ID
        or capture.get("capture_cohort_id") != REQUIRED_ENTRY_COHORT_ID
        or capture.get("input_prospective_observation") is not True
        or capture.get("capture_activation_eligible") is not True
        or int(capture.get("proof_quote_count") or 0) != EXPECTED_INSTRUMENT_COUNT
        or capture.get("instrument_universe_sha256") != EXPECTED_UNIVERSE_SHA256
    ):
        invalid_reasons.append("entry_capture_payload_not_proof_valid")
    expected_capture_id = sha256_text(
        f"{capture_row.get('observation_id') or ''}|{REQUIRED_ENTRY_CONTRACT_ID}|{REQUIRED_ENTRY_COHORT_ID}"
    )
    if str(capture_row.get("capture_id") or "") != expected_capture_id:
        invalid_reasons.append("entry_capture_id_derivation_mismatch")
    if (
        str(capture.get("observation_id") or "") != str(capture_row.get("observation_id") or "")
        or parse_time(capture.get("event_first_known_utc")) != parse_time(capture_row.get("event_first_known_utc"))
        or parse_time(capture.get("captured_utc")) != parse_time(capture_row.get("captured_utc"))
        or int(capture_row.get("proof_quote_count") or 0) != EXPECTED_INSTRUMENT_COUNT
    ):
        invalid_reasons.append("entry_capture_row_payload_lineage_mismatch")
    capture_quotes = capture.get("quotes") if isinstance(capture.get("quotes"), Mapping) else {}
    if set(capture_quotes) != set(EXPECTED_INSTRUMENTS):
        invalid_reasons.append("entry_capture_quote_universe_mismatch")
    captured_at = parse_time(capture.get("captured_utc"))
    recorded_capture_latency = finite_number(capture.get("capture_latency_seconds"))
    derived_capture_latency = (
        (captured_at - event).total_seconds()
        if captured_at is not None
        else None
    )
    if (
        derived_capture_latency is None
        or derived_capture_latency < 0.0
        or derived_capture_latency > ENTRY_MAXIMUM_CAPTURE_LATENCY_SEC
        or recorded_capture_latency is None
        or abs(recorded_capture_latency - derived_capture_latency) > 1e-6
    ):
        invalid_reasons.append("entry_capture_latency_invalid")
    invalid_entry_components = 0
    for expected_instrument in EXPECTED_INSTRUMENTS:
        component = capture_quotes.get(expected_instrument)
        if not isinstance(component, Mapping):
            invalid_entry_components += 1
            continue
        bid = finite_number(component.get("bid"))
        ask = finite_number(component.get("ask"))
        pip = finite_number(component.get("pip"))
        quote_clock = parse_time(
            component.get("quote_time_utc") or component.get("time")
        )
        age_seconds = finite_number(component.get("age_seconds"))
        event_offset_seconds = finite_number(component.get("event_offset_seconds"))
        derived_age = (
            (captured_at - quote_clock).total_seconds()
            if captured_at is not None and quote_clock is not None
            else None
        )
        derived_event_offset = (
            (quote_clock - event).total_seconds()
            if quote_clock is not None
            else None
        )
        if (
            bid is None
            or ask is None
            or pip is None
            or bid <= 0.0
            or ask <= bid
            or pip <= 0.0
            or quote_clock is None
            or captured_at is None
            or derived_age is None
            or derived_age < -ENTRY_MAXIMUM_FUTURE_SKEW_SEC
            or derived_age > ENTRY_MAXIMUM_QUOTE_AGE_SEC
            or derived_event_offset is None
            or derived_event_offset > ENTRY_MAXIMUM_EVENT_OFFSET_SEC
            or age_seconds is None
            or abs(age_seconds - derived_age) > 1e-6
            or event_offset_seconds is None
            or abs(event_offset_seconds - derived_event_offset) > 1e-6
            or not str(component.get("source") or "").strip()
        ):
            invalid_entry_components += 1
    if invalid_entry_components:
        invalid_reasons.append(
            f"entry_capture_component_validation_failed:{invalid_entry_components}"
        )
    if not source_row or not source:
        invalid_reasons.append("raw_source_payload_missing")
    else:
        if (
            int(source_row.get("prospective_observation") or 0) != 1
            or int(source_row.get("listing_bootstrap") or 0) != 0
            or int(source_row.get("identity_preexisting") or 0) != 0
            or int(source_row.get("publisher_time_eligible") or 0) != 1
            or int(source_row.get("observation_clock_trusted") or 0) != 1
        ):
            invalid_reasons.append("raw_source_row_not_prospective_causal")
        if int(source_row.get("research_only") or 0) != 1 or any(
            int(source_row.get(key) or 0) != 0
            for key in ("execution_eligible", "can_authorize")
        ):
            invalid_reasons.append("raw_source_safety_boundary_invalid")
        if (
            str(source_row.get("collector_contract_id") or "")
            != REQUIRED_RAW_COLLECTOR_CONTRACT_ID
            or str(source_row.get("collector_cohort_id") or "")
            != REQUIRED_RAW_COLLECTOR_COHORT_ID
        ):
            invalid_reasons.append("raw_source_collector_contract_or_cohort_mismatch")
        if source.get("source_verified") is not True or source.get("source_direct") is not True:
            invalid_reasons.append("raw_source_not_direct_verified_authority")
        expected_material = sha256_bytes(source_bytes)
        expected_observation = sha256_text(
            f"{source_row.get('source_id') or ''}|{source_row.get('item_key') or ''}|{expected_material}"
        )
        if str(source_row.get("material_sha256") or "") != expected_material:
            invalid_reasons.append("raw_source_material_hash_mismatch")
        if str(source_row.get("observation_id") or "") != expected_observation:
            invalid_reasons.append("raw_source_observation_id_derivation_mismatch")
        if parse_time(source_row.get("first_seen_utc")) != event:
            invalid_reasons.append("raw_source_event_clock_mismatch")
        if (
            str(source.get("source_contract_id") or "")
            and str(source.get("source_contract_id") or "")
            != str(source_row.get("source_contract_id") or "")
        ):
            invalid_reasons.append("raw_source_contract_lineage_mismatch")
    if not mapping_row or not mapping:
        invalid_reasons.append("mapping_copy_missing_by_seal_deadline")
    else:
        mapped = parse_time(mapping_row.get("mapped_utc"))
        mapping_first_seen = parse_time(mapping_row.get("first_seen_utc"))
        if mapping_first_seen != event:
            invalid_reasons.append("mapping_event_clock_mismatch")
        if (
            mapped is None
            or mapped < event
            or mapped > read_started
            or mapped > issued
            or mapped > first_target
        ):
            invalid_reasons.append("mapping_not_available_before_seal")
        if str(mapping_row.get("mapper_contract_id") or "") != REQUIRED_MAPPING_CONTRACT_ID:
            invalid_reasons.append("mapping_contract_mismatch")
        if str(mapping_row.get("mapper_cohort_id") or "") != REQUIRED_MAPPING_COHORT_ID:
            invalid_reasons.append("mapping_cohort_mismatch")
        if str(mapping_row.get("classification_version") or "") != REQUIRED_CLASSIFICATION_VERSION:
            invalid_reasons.append("mapping_classification_version_mismatch")
        expected_mapping_id = sha256_text(
            f"{mapping_row.get('observation_id') or ''}|"
            f"{REQUIRED_CLASSIFICATION_VERSION}|{REQUIRED_MAPPING_CONTRACT_ID}"
        )
        if str(mapping_row.get("mapping_id") or "") != expected_mapping_id:
            invalid_reasons.append("mapping_id_derivation_mismatch")
        if int(mapping_row.get("input_prospective_observation") or 0) != 1:
            invalid_reasons.append("mapping_not_prospective")
        if int(mapping_row.get("input_listing_bootstrap") or 0) != 0:
            invalid_reasons.append("mapping_listing_bootstrap")
        if int(mapping_row.get("input_publisher_time_eligible") or 0) != 1:
            invalid_reasons.append("mapping_publisher_time_ineligible")
        if int(mapping_row.get("research_only") or 0) != 1 or any(
            int(mapping_row.get(key) or 0) != 0
            for key in ("execution_eligible", "can_authorize", "can_promote")
        ):
            invalid_reasons.append("mapping_safety_boundary_invalid")

    observation_id = str(capture_row.get("observation_id") or "")
    if (
        not observation_id
        or str((source_row or {}).get("observation_id") or "") != observation_id
        or str((mapping_row or {}).get("observation_id") or "") != observation_id
    ):
        invalid_reasons.append("observation_lineage_mismatch")
    source_id = str((source_row or {}).get("source_id") or source.get("source_id") or "")
    source_contract_id = str((source_row or {}).get("source_contract_id") or "")
    source_cohort_id = str((source_row or {}).get("source_cohort_id") or "")
    expected_source_lineage = source_lineages.get(source_id)
    if (
        not source_contract_id
        or not source_cohort_id
        or expected_source_lineage is None
        or (source_contract_id, source_cohort_id) != tuple(expected_source_lineage)
    ):
        invalid_reasons.append("raw_source_contract_or_cohort_mismatch")
    if source_id and str((mapping_row or {}).get("source_id") or "") != source_id:
        invalid_reasons.append("source_lineage_mismatch")
    if str((mapping_row or {}).get("source_contract_id") or "") != str(
        (source_row or {}).get("source_contract_id") or ""
    ):
        invalid_reasons.append("mapping_source_contract_lineage_mismatch")
    if source_id and str(source.get("source_id") or "") != source_id:
        invalid_reasons.append("source_payload_identity_mismatch")
    if mapping and str(mapping.get("fast_lane_observation_id") or "") != observation_id:
        invalid_reasons.append("mapping_payload_observation_lineage_mismatch")
    if mapping and str(mapping.get("source_id") or "") != source_id:
        invalid_reasons.append("mapping_payload_source_lineage_mismatch")
    if mapping and str(mapping.get("classification_version") or "") != REQUIRED_CLASSIFICATION_VERSION:
        invalid_reasons.append("mapping_payload_classification_version_mismatch")

    source_issuer = _single_currency(source.get("source_currencies"))
    mapping_issuer = _single_currency(mapping.get("source_currencies"))
    authority_issuer = authority_currency_for_source(authority, source_id)
    issuer = (
        source_issuer
        if source_issuer
        and source_issuer == mapping_issuer
        and source_issuer == authority_issuer
        else ""
    )
    allowed_currencies = {currency for instrument in EXPECTED_INSTRUMENTS for currency in instrument.split("_")}
    if not issuer or issuer not in allowed_currencies:
        invalid_reasons.append("single_issuer_currency_binding_failed")
    if (
        not authority_bytes
        or sha256_bytes(authority_bytes) != REQUIRED_AUTHORITY_MAP_SHA256
        or authority.get("contract_id") != REQUIRED_AUTHORITY_MAP_CONTRACT_ID
    ):
        invalid_reasons.append("source_authority_map_contract_or_hash_mismatch")

    quotes = capture_quotes
    instrument, entry, pair_reason = select_lowest_spread_pair(quotes, issuer) if issuer else ("", {}, "issuer_pair_unavailable")
    if pair_reason:
        invalid_reasons.append(pair_reason)
    entry_bytes = canonical_json(entry).encode("utf-8") if entry else b""
    entry_bid = finite_number(entry.get("bid")) if entry else None
    entry_ask = finite_number(entry.get("ask")) if entry else None
    entry_pip = finite_number(entry.get("pip")) if entry else None
    entry_spread = (
        (entry_ask - entry_bid) / entry_pip
        if entry_bid is not None and entry_ask is not None and entry_pip is not None and entry_pip > 0
        else None
    )
    entry_quote_utc = str(entry.get("quote_time_utc") or entry.get("time") or "") if entry else ""
    captured_clock = parse_time(capture.get("captured_utc"))
    entry_clock = parse_time(entry_quote_utc)
    if entry:
        if entry_clock is None or captured_clock is None:
            invalid_reasons.append("entry_quote_clock_missing")
        elif entry_clock > captured_clock or entry_clock > event + dt.timedelta(seconds=15):
            invalid_reasons.append("entry_quote_clock_outside_t0_capture")

    score = None
    currency_scores = mapping.get("currency_scores") if isinstance(mapping.get("currency_scores"), Mapping) else {}
    score = finite_number(currency_scores.get(issuer)) if issuer else None
    semantic_direction_available = int(
        (mapping_row or {}).get("semantic_direction_available") or 0
    )
    if score is not None and score != 0.0 and semantic_direction_available != 1:
        invalid_reasons.append("mapping_score_without_semantic_direction_flag")
    official_side = (
        _pair_side_for_issuer(instrument, issuer, score)
        if (
            instrument
            and issuer
            and score is not None
            and score != 0.0
            and semantic_direction_available == 1
        )
        else ""
    )
    technical_side, technical_reason, technical_bytes, technical_snapshot_hash = technical_at_t0(
        feature_snapshot,
        snapshot_file_bytes=feature_snapshot_bytes,
        instrument=instrument,
        event_t0=event,
    ) if instrument else ("", "technical_pair_unavailable", b"", sha256_bytes(feature_snapshot_bytes) if feature_snapshot_bytes else "")

    decision_state = "invalid" if invalid_reasons else "sealed"
    invalid_reason = ";".join(dict.fromkeys(invalid_reasons))
    input_capture_id = str(capture_row.get("capture_id") or "")
    decision_id = "official_event_paired_decision_" + sha256_text(
        f"{input_capture_id}|{CONTRACT_ID}|{COHORT_ID}"
    )[:32]
    episode_id = _episode_id(issuer, event) if issuer else ""
    schedule = [
        {"arm_name": arm, "horizon_min": horizon, "slippage_stress_pips": slip}
        for arm in ARMS
        for horizon in HORIZONS_MIN
        for slip in SLIPPAGE_STRESS_PIPS
    ]
    schedule_json = canonical_json(schedule)
    config_material = config_bytes if config_bytes is not None else read_bytes(CONFIG_PATH)
    decision = {
        "decision_id": decision_id,
        "input_capture_id": input_capture_id,
        "observation_id": observation_id,
        "event_first_known_utc": iso_utc(event),
        "decision_read_started_utc": iso_utc(read_started),
        "decision_precommit_utc": iso_utc(issued),
        "issued_utc": iso_utc(issued),
        "decision_latency_sec": round((issued - event).total_seconds(), 6),
        "first_horizon_target_utc": iso_utc(first_target),
        "decision_state": decision_state,
        "invalid_reason": invalid_reason,
        "source_id": source_id,
        "source_contract_id": source_contract_id,
        "source_cohort_id": source_cohort_id,
        "issuer_currency": issuer,
        "selected_instrument": instrument,
        "official_currency_score": score,
        "official_pair_side": official_side,
        "technical_pair_side": technical_side,
        "technical_state": "trade" if technical_side else "abstain",
        "technical_reason": technical_reason,
        "entry_bid": entry_bid,
        "entry_ask": entry_ask,
        "entry_pip": entry_pip,
        "entry_spread_pips": entry_spread,
        "entry_quote_utc": entry_quote_utc,
        "market_episode_id": episode_id,
        "outcome_schedule_json": schedule_json,
        "outcome_schedule_sha256": sha256_text(schedule_json),
        "input_capture_bytes": capture_bytes,
        "input_capture_sha256": sha256_bytes(capture_bytes),
        "input_capture_row_bytes": capture_row_bytes,
        "input_capture_row_sha256": sha256_bytes(capture_row_bytes),
        "entry_component_bytes": entry_bytes,
        "entry_component_sha256": sha256_bytes(entry_bytes),
        "source_payload_bytes": source_bytes,
        "source_payload_sha256": sha256_bytes(source_bytes),
        "source_row_bytes": source_row_bytes,
        "source_row_sha256": sha256_bytes(source_row_bytes),
        "authority_map_contract_id": str(authority.get("contract_id") or ""),
        "authority_map_bytes": authority_bytes,
        "authority_map_sha256": sha256_bytes(authority_bytes),
        "news_source_config_sha256": sha256_bytes(news_config_bytes),
        "mapping_id": str((mapping_row or {}).get("mapping_id") or ""),
        "mapping_payload_bytes": mapping_bytes,
        "mapping_payload_sha256": sha256_bytes(mapping_bytes),
        "mapping_row_bytes": mapping_row_bytes,
        "mapping_row_sha256": sha256_bytes(mapping_row_bytes),
        "technical_material_bytes": technical_bytes,
        "technical_material_sha256": sha256_bytes(technical_bytes),
        "technical_snapshot_file_sha256": technical_snapshot_hash,
        "config_file_sha256": sha256_bytes(config_material),
        "producer_source_sha256": (
            producer_sha256 or normalized_producer_source_sha256()
        ),
        "producer_file_sha256": script_sha256(),
        "fast_lane_source_sha256": fast_dependency_hash,
        "horizon_capture_source_sha256": horizon_dependency_hash,
        "entry_economics_role": (
            "event_clock_counterfactual_research_measurement_not_proof_of_"
            "order_submission_before_semantic_decision"
        ),
    }
    arms = _arm_rows(
        decision_id=decision_id,
        decision_state=decision_state,
        decision_reason=invalid_reason,
        instrument=instrument,
        issuer_currency=issuer,
        market_episode_id=episode_id,
        official_side=official_side,
        technical_side=technical_side,
        technical_reason=technical_reason,
    )
    return decision, arms


def insert_decision(
    connection: sqlite3.Connection,
    decision: Mapping[str, Any],
    arms: Sequence[Mapping[str, Any]],
    *,
    transaction_already_open: bool = False,
) -> bool:
    manifests = connection.execute(
        "SELECT * FROM paired_event_cohort_manifest"
    ).fetchall()
    if len(manifests) != 1:
        raise ValueError("paired-evaluator cohort manifest missing or ambiguous")
    manifest = dict(manifests[0])
    identity_checks = {
        "config_file_sha256": decision.get("config_file_sha256"),
        "producer_source_sha256": decision.get("producer_source_sha256"),
        "producer_file_sha256": decision.get("producer_file_sha256"),
        "authority_map_sha256": decision.get("authority_map_sha256"),
        "news_source_config_sha256": decision.get("news_source_config_sha256"),
        "fast_lane_source_sha256": decision.get("fast_lane_source_sha256"),
        "horizon_capture_source_sha256": decision.get(
            "horizon_capture_source_sha256"
        ),
    }
    for field, observed in identity_checks.items():
        if str(observed or "") != str(manifest.get(field) or ""):
            raise ValueError(f"decision cohort identity mismatch:{field}")
    if (
        str(decision.get("authority_map_contract_id") or "")
        != REQUIRED_AUTHORITY_MAP_CONTRACT_ID
        or str(manifest.get("contract_id") or "") != CONTRACT_ID
        or str(manifest.get("cohort_id") or "") != COHORT_ID
        or str(manifest.get("required_classification_version") or "")
        != REQUIRED_CLASSIFICATION_VERSION
    ):
        raise ValueError("decision cohort contract identity mismatch")
    if len(arms) != len(ARMS) or {str(row.get("arm_name") or "") for row in arms} != set(ARMS):
        raise ValueError("decision arm Cartesian set incomplete")
    if not transaction_already_open:
        connection.execute("BEGIN IMMEDIATE")
    try:
        if connection.execute(
            "SELECT 1 FROM paired_event_decision WHERE input_capture_id=?",
            (decision["input_capture_id"],),
        ).fetchone():
            connection.rollback()
            return False
        columns = tuple(decision.keys())
        connection.execute(
            f"INSERT INTO paired_event_decision ({','.join(columns)},research_only,execution_eligible,can_place_orders,can_authorize,can_promote,contract_id,cohort_id) "
            f"VALUES ({','.join('?' for _ in columns)},1,0,0,0,0,?,?)",
            tuple(decision[column] for column in columns) + (CONTRACT_ID, COHORT_ID),
        )
        for arm in arms:
            connection.execute(
                """
                INSERT INTO paired_event_arm (
                  arm_id,decision_id,arm_name,action_state,side,reason,
                  signed_currency_factor_id,market_episode_id,research_only,
                  execution_eligible,can_place_orders,can_authorize,can_promote,
                  contract_id,cohort_id
                ) VALUES (?,?,?,?,?,?,?,?,1,0,0,0,0,?,?)
                """,
                (
                    arm["arm_id"], arm["decision_id"], arm["arm_name"],
                    arm["action_state"], arm["side"], arm["reason"],
                    arm["signed_currency_factor_id"], arm["market_episode_id"],
                    CONTRACT_ID, COHORT_ID,
                ),
            )
        connection.commit()
        return True
    except BaseException:
        connection.rollback()
        raise


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


RELEASE_REQUIRED_SCHEMA: dict[str, set[str]] = {
    "official_release_quote_capture": {
        "capture_id", "observation_id", "event_first_known_utc",
        "captured_utc", "capture_payload_json", "proof_quote_count",
        "research_only", "execution_eligible", "can_authorize", "can_promote",
        "capture_contract_id", "capture_cohort_id",
    },
    "official_release_observation": {
        "observation_id", "source_id", "source_contract_id", "source_cohort_id",
        "item_key", "material_sha256", "first_seen_utc",
        "prospective_observation", "listing_bootstrap", "identity_preexisting",
        "publisher_time_eligible", "observation_clock_trusted",
        "raw_payload_json", "research_only", "execution_eligible", "can_authorize",
    },
}
MAPPING_REQUIRED_SCHEMA: dict[str, set[str]] = {
    "official_release_mapping": {
        "mapping_id", "observation_id", "source_id", "source_contract_id",
        "first_seen_utc", "input_prospective_observation",
        "input_listing_bootstrap", "input_publisher_time_eligible",
        "classification_version", "semantic_direction_available",
        "mapping_payload_json", "mapped_utc", "research_only",
        "execution_eligible", "can_authorize", "can_promote",
        "mapper_contract_id", "mapper_cohort_id",
    },
}
HORIZON_REQUIRED_SCHEMA: dict[str, set[str]] = {
    "official_event_horizon_capture": {
        "capture_id", "input_capture_id", "observation_id",
        "event_first_known_utc", "horizon_min", "target_utc",
        "capture_read_started_utc", "attempted_utc", "attempt_delay_sec",
        "timing_quality", "proof_quote_count", "input_capture_payload_sha256",
        "connection_generation", "quote_snapshot_generated_utc",
        "quote_snapshot_sha256", "component_root_sha256",
        "payload_json", "payload_sha256", "research_only", "execution_eligible",
        "can_place_orders", "can_authorize", "can_promote", "contract_id",
        "cohort_id",
    },
    "official_event_horizon_quote": {
        "quote_id", "capture_id", "horizon_min", "instrument", "bid", "ask", "pip",
        "quote_utc", "quote_age_sec", "target_offset_sec", "spread_pips",
        "source", "connection_generation", "payload_json", "payload_sha256", "research_only",
        "execution_eligible", "can_place_orders", "can_authorize", "can_promote",
        "contract_id", "cohort_id",
    },
}


def inspect_sqlite_input(
    path: Path, required_schema: Mapping[str, set[str]]
) -> dict[str, Any]:
    """Return an explicit read-only integrity/schema state for an upstream DB."""

    result: dict[str, Any] = {
        "path": str(path),
        "state": "unhealthy",
        "exists": path.exists(),
        "quick_check": "not_run",
        "errors": [],
    }
    if not path.exists():
        result["errors"].append("database_missing")
        return result
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(
            f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=2.0
        )
        connection.row_factory = sqlite3.Row
        quick = connection.execute("PRAGMA quick_check(1)").fetchone()
        quick_text = str(quick[0] if quick else "missing_result")
        result["quick_check"] = quick_text
        if quick_text.lower() != "ok":
            result["errors"].append(f"quick_check:{quick_text}")
        for table, required_columns in required_schema.items():
            if not _table_exists(connection, table):
                result["errors"].append(f"missing_table:{table}")
                continue
            observed = {
                str(row[1])
                for row in connection.execute(f'PRAGMA table_info("{table}")')
            }
            missing = sorted(required_columns - observed)
            if missing:
                result["errors"].append(
                    f"missing_columns:{table}:{','.join(missing)}"
                )
    except sqlite3.Error as exc:
        result["errors"].append(f"sqlite_error:{type(exc).__name__}:{exc}")
    finally:
        if connection is not None:
            connection.close()
    if not result["errors"]:
        result["state"] = "healthy"
    return result


def seal_due_decisions(
    output: sqlite3.Connection,
    *,
    release_database: Path = INPUT_RELEASE_DATABASE,
    mapping_database: Path = INPUT_MAPPING_DATABASE,
    feature_snapshot_path: Path = FEATURE_SNAPSHOT_PATH,
    authority_map_path: Path = AUTHORITY_MAP_PATH,
    now: dt.datetime | None = None,
    clock: Callable[[], dt.datetime] | None = None,
) -> dict[str, Any]:
    """Seal decisions and commit them without opening the outcome database."""

    fixed_now = now

    def fresh_clock() -> dt.datetime:
        value = clock() if clock is not None else (fixed_now or utc_now())
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return value.astimezone(dt.timezone.utc)

    release_health = inspect_sqlite_input(release_database, RELEASE_REQUIRED_SCHEMA)
    mapping_health = inspect_sqlite_input(mapping_database, MAPPING_REQUIRED_SCHEMA)
    counts: dict[str, Any] = {
        "eligible_captures": 0,
        "pending_mapping": 0,
        "sealed": 0,
        "invalid": 0,
        "duplicates": 0,
        "errors": [],
        "input_health": {
            "release": release_health,
            "mapping": mapping_health,
        },
        "health_state": (
            "healthy"
            if release_health["state"] == mapping_health["state"] == "healthy"
            else "degraded"
        ),
    }
    if counts["health_state"] != "healthy":
        return counts
    release = sqlite3.connect(f"file:{release_database.resolve().as_posix()}?mode=ro", uri=True)
    mapping = sqlite3.connect(f"file:{mapping_database.resolve().as_posix()}?mode=ro", uri=True)
    release.row_factory = sqlite3.Row
    mapping.row_factory = sqlite3.Row
    feature_bytes, feature_payload = read_json_bytes(feature_snapshot_path)
    authority_bytes, authority_payload = validate_authority_map(authority_map_path)
    news_source_bytes, news_source_lineages = validate_news_source_config()
    config_bytes = read_bytes(CONFIG_PATH)
    producer_hash = normalized_producer_source_sha256()
    try:
        existing = {
            str(row[0])
            for row in output.execute("SELECT input_capture_id FROM paired_event_decision")
        }
        rows = release.execute(
            """
            SELECT * FROM official_release_quote_capture
            WHERE event_first_known_utc>=?
            ORDER BY event_first_known_utc,capture_id
            """,
            (iso_utc(ACTIVATED_UTC),),
        ).fetchall()
        for capture_row_raw in rows:
            capture_row = dict(capture_row_raw)
            capture_id = str(capture_row.get("capture_id") or "")
            if capture_id in existing:
                counts["duplicates"] += 1
                continue
            counts["eligible_captures"] += 1
            event = parse_time(capture_row.get("event_first_known_utc"))
            if event is None:
                event = ACTIVATED_UTC
            deadline = event + dt.timedelta(seconds=SEAL_DEADLINE_OFFSET_SEC)
            read_started = fresh_clock()
            source_raw = release.execute(
                "SELECT * FROM official_release_observation WHERE observation_id=?",
                (capture_row.get("observation_id"),),
            ).fetchone()
            mapping_candidates = mapping.execute(
                """
                SELECT * FROM official_release_mapping
                WHERE observation_id=? AND mapped_utc<=?
                  AND classification_version=?
                  AND mapper_contract_id=? AND mapper_cohort_id=?
                ORDER BY mapped_utc,mapping_id
                """,
                (
                    capture_row.get("observation_id"),
                    iso_utc(read_started),
                    REQUIRED_CLASSIFICATION_VERSION,
                    REQUIRED_MAPPING_CONTRACT_ID,
                    REQUIRED_MAPPING_COHORT_ID,
                ),
            ).fetchall()
            mapping_raw = mapping_candidates[0] if mapping_candidates else None
            capture_payload = {}
            try:
                capture_payload = json.loads(str(capture_row.get("capture_payload_json") or "{}"))
            except ValueError:
                pass
            capture_is_terminal_invalid = capture_payload.get("timing_quality") != "prospective_exact_live_quote"
            try:
                output.execute("BEGIN IMMEDIATE")
            except sqlite3.Error as exc:
                counts["health_state"] = "degraded"
                counts["errors"].append(
                    f"output_write_lock:{type(exc).__name__}:{exc}"
                )
                continue
            # The write lock is held before this timestamp is sampled.  Thus a
            # lock wait cannot masquerade as an on-time precommit decision.
            precommit = fresh_clock()
            if mapping_raw is None and precommit < deadline and not capture_is_terminal_invalid:
                output.rollback()
                counts["pending_mapping"] += 1
                continue
            try:
                decision, arms = build_decision(
                    capture_row=capture_row,
                    source_row=None if source_raw is None else dict(source_raw),
                    mapping_row=None if mapping_raw is None else dict(mapping_raw),
                    feature_snapshot_bytes=feature_bytes,
                    feature_snapshot=feature_payload,
                    issued_utc=precommit,
                    decision_read_started_utc=read_started,
                    authority_map_bytes=authority_bytes,
                    authority_map=authority_payload,
                    news_source_config_bytes=news_source_bytes,
                    news_source_lineages=news_source_lineages,
                    config_bytes=config_bytes,
                    producer_sha256=producer_hash,
                )
                inserted = insert_decision(
                    output, decision, arms, transaction_already_open=True
                )
            except BaseException:
                if output.in_transaction:
                    output.rollback()
                raise
            if inserted:
                counts["sealed" if decision["decision_state"] == "sealed" else "invalid"] += 1
                existing.add(capture_id)
            else:
                counts["duplicates"] += 1
        return counts
    finally:
        release.close()
        mapping.close()


def _horizon_input(
    decision: Mapping[str, Any],
    capture_row: Mapping[str, Any],
    quote_row: Mapping[str, Any] | None,
) -> dict[str, Any]:
    horizon_min = int(capture_row.get("horizon_min") or 0)
    if horizon_min not in HORIZONS_MIN:
        raise ValueError(f"unsupported upstream horizon:{horizon_min}")
    horizon_bytes = str(capture_row.get("payload_json") or "").encode("utf-8")
    quote_bytes = str((quote_row or {}).get("payload_json") or "").encode("utf-8")
    horizon_row_bytes = canonical_json(dict(capture_row)).encode("utf-8")
    quote_row_bytes = canonical_json(dict(quote_row or {})).encode("utf-8")
    reasons: list[str] = []
    try:
        payload = json.loads(horizon_bytes.decode("utf-8")) if horizon_bytes else {}
    except ValueError:
        payload = {}
    try:
        quote = json.loads(quote_bytes.decode("utf-8")) if quote_bytes else {}
    except ValueError:
        quote = {}
    if str(capture_row.get("contract_id") or "") != REQUIRED_HORIZON_CONTRACT_ID or str(capture_row.get("cohort_id") or "") != REQUIRED_HORIZON_COHORT_ID:
        reasons.append("horizon_contract_or_cohort_mismatch")
    if int(capture_row.get("research_only") or 0) != 1 or any(
        int(capture_row.get(key) or 0) != 0
        for key in ("execution_eligible", "can_place_orders", "can_authorize", "can_promote")
    ):
        reasons.append("horizon_safety_boundary_invalid")
    if sha256_bytes(horizon_bytes) != str(capture_row.get("payload_sha256") or ""):
        reasons.append("horizon_payload_hash_mismatch")
    if str(capture_row.get("input_capture_payload_sha256") or "") != str(decision.get("input_capture_sha256") or ""):
        reasons.append("entry_capture_hash_drift")
    if str(capture_row.get("input_capture_id") or "") != str(decision.get("input_capture_id") or ""):
        reasons.append("horizon_entry_lineage_mismatch")
    if (
        str(capture_row.get("observation_id") or "")
        != str(decision.get("observation_id") or "")
        or parse_time(capture_row.get("event_first_known_utc"))
        != parse_time(decision.get("event_first_known_utc"))
    ):
        reasons.append("horizon_row_decision_lineage_mismatch")
    if capture_row.get("timing_quality") != "prospective_exact_live_quote" or int(capture_row.get("proof_quote_count") or 0) != EXPECTED_INSTRUMENT_COUNT:
        reasons.append("horizon_capture_not_proof_valid")
    decision_event = parse_time(decision.get("event_first_known_utc"))
    expected_target = (
        decision_event + dt.timedelta(minutes=horizon_min)
        if decision_event is not None and horizon_min in HORIZONS_MIN
        else None
    )
    capture_read_started = parse_time(capture_row.get("capture_read_started_utc"))
    attempted = parse_time(capture_row.get("attempted_utc"))
    recorded_attempt_delay = finite_number(capture_row.get("attempt_delay_sec"))
    derived_attempt_delay = (
        (attempted - expected_target).total_seconds()
        if attempted is not None and expected_target is not None
        else None
    )
    snapshot_generated = parse_time(capture_row.get("quote_snapshot_generated_utc"))
    snapshot_age = (
        (attempted - snapshot_generated).total_seconds()
        if attempted is not None and snapshot_generated is not None
        else None
    )
    if (
        expected_target is None
        or capture_read_started is None
        or attempted is None
        or capture_read_started < expected_target
        or capture_read_started > attempted
        or derived_attempt_delay is None
        or derived_attempt_delay < 0.0
        or derived_attempt_delay > HORIZON_MAXIMUM_ATTEMPT_DELAY_SEC
        or recorded_attempt_delay is None
        or abs(recorded_attempt_delay - derived_attempt_delay) > 1e-6
    ):
        reasons.append("horizon_capture_attempt_clock_invalid")
    if (
        snapshot_age is None
        or snapshot_age < -HORIZON_MAXIMUM_FUTURE_SKEW_SEC
        or snapshot_age > HORIZON_MAXIMUM_SNAPSHOT_AGE_SEC
    ):
        reasons.append("horizon_quote_snapshot_clock_invalid")
    if (
        str(payload.get("capture_id") or "") != str(capture_row.get("capture_id") or "")
        or str(payload.get("input_capture_id") or "") != str(decision.get("input_capture_id") or "")
        or str(payload.get("observation_id") or "") != str(decision.get("observation_id") or "")
        or parse_time(payload.get("event_first_known_utc")) != decision_event
        or int(payload.get("horizon_min") or 0) != horizon_min
        or payload.get("timing_quality") != "prospective_exact_live_quote"
        or int(payload.get("proof_quote_count") or 0) != EXPECTED_INSTRUMENT_COUNT
        or int(payload.get("expected_instrument_count") or 0) != EXPECTED_INSTRUMENT_COUNT
        or payload.get("expected_instrument_universe_sha256") != EXPECTED_UNIVERSE_SHA256
        or payload.get("entry_capture_contract_id") != REQUIRED_ENTRY_CONTRACT_ID
        or payload.get("entry_capture_cohort_id") != REQUIRED_ENTRY_COHORT_ID
        or payload.get("contract_id") != REQUIRED_HORIZON_CONTRACT_ID
        or payload.get("cohort_id") != REQUIRED_HORIZON_COHORT_ID
        or parse_time(payload.get("capture_read_started_utc")) != capture_read_started
        or parse_time(payload.get("attempted_utc")) != attempted
        or finite_number(payload.get("attempt_delay_sec")) != recorded_attempt_delay
        or parse_time(payload.get("quote_snapshot_generated_utc")) != snapshot_generated
        or str(payload.get("quote_snapshot_sha256") or "")
        != str(capture_row.get("quote_snapshot_sha256") or "")
        or str(payload.get("component_root_sha256") or "")
        != str(capture_row.get("component_root_sha256") or "")
        or int(payload.get("connection_generation") or 0)
        != int(capture_row.get("connection_generation") or 0)
        or payload.get("research_only") is not True
        or payload.get("execution_eligible") is not False
        or payload.get("can_place_orders") is not False
        or payload.get("can_authorize") is not False
        or payload.get("can_promote") is not False
    ):
        reasons.append("horizon_row_payload_lineage_mismatch")
    expected_capture_id = "official_event_horizon_" + sha256_text(
        f"{decision.get('input_capture_id') or ''}|{horizon_min}|{REQUIRED_HORIZON_CONTRACT_ID}|{REQUIRED_HORIZON_COHORT_ID}"
    )[:32]
    if str(capture_row.get("capture_id") or "") != expected_capture_id:
        reasons.append("horizon_capture_id_derivation_mismatch")
    if (
        expected_target is None
        or parse_time(capture_row.get("target_utc")) != expected_target
        or parse_time(payload.get("target_utc")) != expected_target
    ):
        reasons.append("horizon_target_clock_mismatch")
    payload_quotes = payload.get("quotes") if isinstance(payload.get("quotes"), Mapping) else {}
    quote_snapshot_material = (
        payload.get("quote_snapshot_material")
        if isinstance(payload.get("quote_snapshot_material"), Mapping)
        else {}
    )
    expected_snapshot_hash = sha256_text(canonical_json(quote_snapshot_material))
    if expected_snapshot_hash != str(capture_row.get("quote_snapshot_sha256") or ""):
        reasons.append("horizon_quote_snapshot_hash_mismatch")
    component_hashes = [
        sha256_text(canonical_json(payload_quotes[instrument]))
        for instrument in sorted(payload_quotes)
    ]
    expected_component_root = sha256_text("".join(component_hashes))
    if expected_component_root != str(capture_row.get("component_root_sha256") or ""):
        reasons.append("horizon_component_root_mismatch")
    if set(payload_quotes) != set(EXPECTED_INSTRUMENTS):
        reasons.append("horizon_quote_universe_mismatch")
    invalid_horizon_components = 0
    for expected_instrument in EXPECTED_INSTRUMENTS:
        component = payload_quotes.get(expected_instrument)
        if not isinstance(component, Mapping):
            invalid_horizon_components += 1
            continue
        component_bid = finite_number(component.get("bid"))
        component_ask = finite_number(component.get("ask"))
        component_pip = finite_number(component.get("pip"))
        component_clock = parse_time(component.get("quote_utc"))
        target_offset = finite_number(component.get("target_offset_sec"))
        quote_age = finite_number(component.get("quote_age_sec"))
        derived_offset = (
            (component_clock - expected_target).total_seconds()
            if component_clock is not None and expected_target is not None
            else None
        )
        derived_quote_age = (
            (attempted - component_clock).total_seconds()
            if attempted is not None and component_clock is not None
            else None
        )
        if (
            str(component.get("instrument") or "") != expected_instrument
            or component_bid is None
            or component_ask is None
            or component_pip is None
            or component_bid <= 0.0
            or component_ask <= component_bid
            or component_pip <= 0.0
            or component_clock is None
            or derived_offset is None
            or abs(derived_offset) > HORIZON_MAXIMUM_TARGET_OFFSET_SEC
            or target_offset is None
            or abs(target_offset - derived_offset) > 1e-6
            or derived_quote_age is None
            or derived_quote_age < -HORIZON_MAXIMUM_FUTURE_SKEW_SEC
            or derived_quote_age > HORIZON_MAXIMUM_QUOTE_AGE_SEC
            or quote_age is None
            or abs(quote_age - derived_quote_age) > 1e-6
            or not str(component.get("source") or "").strip()
        ):
            invalid_horizon_components += 1
    if invalid_horizon_components:
        reasons.append(
            f"horizon_component_validation_failed:{invalid_horizon_components}"
        )
    instrument = str(decision.get("selected_instrument") or "")
    if not quote_row or not quote:
        reasons.append("selected_exit_component_missing")
    else:
        if str(quote_row.get("capture_id") or "") != str(capture_row.get("capture_id") or ""):
            reasons.append("selected_exit_capture_lineage_mismatch")
        if str(quote_row.get("instrument") or "") != instrument:
            reasons.append("selected_exit_instrument_mismatch")
        if int(quote_row.get("horizon_min") or 0) != horizon_min:
            reasons.append("selected_exit_horizon_mismatch")
        if (
            str(quote_row.get("contract_id") or "") != REQUIRED_HORIZON_CONTRACT_ID
            or str(quote_row.get("cohort_id") or "") != REQUIRED_HORIZON_COHORT_ID
            or int(quote_row.get("research_only") or 0) != 1
            or any(
                int(quote_row.get(key) or 0) != 0
                for key in (
                    "execution_eligible", "can_place_orders", "can_authorize",
                    "can_promote",
                )
            )
        ):
            reasons.append("selected_exit_row_safety_or_lineage_invalid")
        if sha256_bytes(quote_bytes) != str(quote_row.get("payload_sha256") or ""):
            reasons.append("selected_exit_component_hash_mismatch")
        expected_quote_id = "official_event_horizon_quote_" + sha256_text(
            f"{capture_row.get('capture_id') or ''}|{instrument}|"
            f"{sha256_bytes(quote_bytes)}"
        )[:32]
        if str(quote_row.get("quote_id") or "") != expected_quote_id:
            reasons.append("selected_exit_quote_id_derivation_mismatch")
        expected_component = payload_quotes.get(instrument)
        if not isinstance(expected_component, Mapping) or canonical_json(expected_component).encode("utf-8") != quote_bytes:
            reasons.append("horizon_header_component_mismatch")
        entry_pip = finite_number(decision.get("entry_pip"))
        exit_pip = finite_number(quote.get("pip"))
        if (
            entry_pip is None
            or exit_pip is None
            or entry_pip <= 0.0
            or exit_pip <= 0.0
            or exit_pip != entry_pip
        ):
            reasons.append("selected_exit_pip_mismatch")
        exit_bid = finite_number(quote.get("bid"))
        exit_ask = finite_number(quote.get("ask"))
        exit_clock = parse_time(quote.get("quote_utc"))
        exit_quote_age = finite_number(quote.get("quote_age_sec"))
        exit_target_offset = finite_number(quote.get("target_offset_sec"))
        exit_spread = finite_number(quote.get("spread_pips"))
        if (
            finite_number(quote_row.get("bid")) != exit_bid
            or finite_number(quote_row.get("ask")) != exit_ask
            or finite_number(quote_row.get("pip")) != exit_pip
            or parse_time(quote_row.get("quote_utc")) != exit_clock
            or finite_number(quote_row.get("quote_age_sec")) != exit_quote_age
            or finite_number(quote_row.get("target_offset_sec")) != exit_target_offset
            or finite_number(quote_row.get("spread_pips")) != exit_spread
            or str(quote_row.get("source") or "") != str(quote.get("source") or "")
            or int(quote_row.get("connection_generation") or 0)
            != int(quote.get("connection_generation") or 0)
        ):
            reasons.append("selected_exit_row_payload_value_mismatch")
        if (
            exit_bid is None
            or exit_ask is None
            or exit_bid <= 0.0
            or exit_ask <= exit_bid
        ):
            reasons.append("selected_exit_bid_ask_invalid")
        if (
            expected_target is None
            or exit_clock is None
            or abs((exit_clock - expected_target).total_seconds()) > HORIZON_MAXIMUM_TARGET_OFFSET_SEC
        ):
            reasons.append("selected_exit_quote_clock_invalid")
    horizon_input_id = "paired_event_horizon_input_" + sha256_text(
        f"{decision['decision_id']}|{horizon_min}|{CONTRACT_ID}|{COHORT_ID}"
    )[:32]
    return {
        "horizon_input_id": horizon_input_id,
        "decision_id": decision["decision_id"],
        "input_horizon_capture_id": str(capture_row.get("capture_id") or ""),
        "horizon_min": horizon_min,
        "input_state": "invalid" if reasons else "valid",
        "invalid_reason": ";".join(dict.fromkeys(reasons)),
        "horizon_payload_bytes": horizon_bytes,
        "horizon_payload_sha256": sha256_bytes(horizon_bytes),
        "horizon_row_bytes": horizon_row_bytes,
        "horizon_row_sha256": sha256_bytes(horizon_row_bytes),
        "exit_component_bytes": quote_bytes,
        "exit_component_sha256": sha256_bytes(quote_bytes),
        "exit_component_row_bytes": quote_row_bytes,
        "exit_component_row_sha256": sha256_bytes(quote_row_bytes),
        "exit_bid": finite_number(quote.get("bid")) if quote else None,
        "exit_ask": finite_number(quote.get("ask")) if quote else None,
        "exit_pip": finite_number(quote.get("pip")) if quote else None,
        "exit_quote_utc": str(quote.get("quote_utc") or "") if quote else "",
    }


def _outcome_rows(
    decision: Mapping[str, Any],
    arms: Sequence[Mapping[str, Any]],
    horizon_input: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    entry_bid = finite_number(decision.get("entry_bid"))
    entry_ask = finite_number(decision.get("entry_ask"))
    pip = finite_number(decision.get("entry_pip"))
    exit_bid = finite_number(horizon_input.get("exit_bid"))
    exit_ask = finite_number(horizon_input.get("exit_ask"))
    for arm in arms:
        for slip in SLIPPAGE_STRESS_PIPS:
            state = str(arm.get("action_state") or "invalid")
            reason = str(arm.get("reason") or "")
            gross = spread_cost = net = None
            if arm.get("arm_name") == "no_trade" or state == "abstain":
                state, gross, spread_cost, net = "abstain", 0.0, 0.0, 0.0
            elif state == "invalid":
                reason = reason or "decision_arm_invalid"
            elif horizon_input.get("input_state") != "valid":
                state, reason = "invalid", str(horizon_input.get("invalid_reason") or "horizon_input_invalid")
            elif None in {entry_bid, entry_ask, pip, exit_bid, exit_ask} or pip is None or pip <= 0:
                state, reason = "invalid", "executable_quote_economics_invalid"
            else:
                assert entry_bid is not None and entry_ask is not None and exit_bid is not None and exit_ask is not None and pip is not None
                entry_mid = (entry_bid + entry_ask) / 2.0
                exit_mid = (exit_bid + exit_ask) / 2.0
                if arm.get("side") == "buy":
                    gross = (exit_bid - entry_ask) / pip
                    signed_mid = (exit_mid - entry_mid) / pip
                elif arm.get("side") == "sell":
                    gross = (entry_bid - exit_ask) / pip
                    signed_mid = (entry_mid - exit_mid) / pip
                else:
                    state, reason = "invalid", "trade_arm_side_invalid"
                    signed_mid = 0.0
                if state == "trade":
                    spread_cost = max(0.0, signed_mid - gross)
                    net = gross - float(slip)
            outcome_id = "paired_event_outcome_" + sha256_text(
                f"{decision['decision_id']}|{arm['arm_name']}|{horizon_input['horizon_min']}|{slip:.2f}|{CONTRACT_ID}|{COHORT_ID}"
            )[:32]
            rows.append(
                {
                    "outcome_id": outcome_id,
                    "decision_id": decision["decision_id"],
                    "arm_id": arm["arm_id"],
                    "horizon_input_id": horizon_input["horizon_input_id"],
                    "arm_name": arm["arm_name"],
                    "horizon_min": horizon_input["horizon_min"],
                    "slippage_stress_pips": float(slip),
                    "outcome_state": state,
                    "outcome_reason": reason,
                    "gross_executable_pips": gross,
                    "spread_cost_pips": spread_cost,
                    "net_after_cost_pips": net,
                }
            )
    return rows


def insert_horizon_and_outcomes(
    connection: sqlite3.Connection,
    horizon_input: Mapping[str, Any],
    outcomes: Sequence[Mapping[str, Any]],
) -> bool:
    connection.execute("BEGIN IMMEDIATE")
    try:
        if connection.execute(
            "SELECT 1 FROM paired_event_horizon_input WHERE decision_id=? AND horizon_min=?",
            (horizon_input["decision_id"], horizon_input["horizon_min"]),
        ).fetchone():
            connection.rollback()
            return False
        columns = tuple(horizon_input.keys())
        connection.execute(
            f"INSERT INTO paired_event_horizon_input ({','.join(columns)},research_only,execution_eligible,can_place_orders,can_authorize,can_promote,contract_id,cohort_id) "
            f"VALUES ({','.join('?' for _ in columns)},1,0,0,0,0,?,?)",
            tuple(horizon_input[column] for column in columns) + (CONTRACT_ID, COHORT_ID),
        )
        for outcome in outcomes:
            columns = tuple(outcome.keys())
            connection.execute(
                f"INSERT INTO paired_event_outcome ({','.join(columns)},research_only,execution_eligible,can_place_orders,can_authorize,can_promote,contract_id,cohort_id) "
                f"VALUES ({','.join('?' for _ in columns)},1,0,0,0,0,?,?)",
                tuple(outcome[column] for column in columns) + (CONTRACT_ID, COHORT_ID),
            )
        connection.commit()
        return True
    except BaseException:
        connection.rollback()
        raise


def mature_available_outcomes(
    output: sqlite3.Connection,
    *,
    horizon_database: Path = INPUT_HORIZON_DATABASE,
) -> dict[str, Any]:
    """Append exact Horizon V1 outcomes after decisions are durably committed."""

    health = inspect_sqlite_input(horizon_database, HORIZON_REQUIRED_SCHEMA)
    counts: dict[str, Any] = {
        "available_horizons": 0,
        "inserted_horizons": 0,
        "inserted_outcomes": 0,
        "duplicates": 0,
        "unsupported_horizons": 0,
        "input_health": {"horizon": health},
        "health_state": "healthy" if health["state"] == "healthy" else "degraded",
    }
    if health["state"] != "healthy":
        return counts
    upstream = sqlite3.connect(f"file:{horizon_database.resolve().as_posix()}?mode=ro", uri=True)
    upstream.row_factory = sqlite3.Row
    try:
        decisions = output.execute("SELECT * FROM paired_event_decision ORDER BY event_first_known_utc,decision_id").fetchall()
        for decision_raw in decisions:
            decision = dict(decision_raw)
            arms = [dict(row) for row in output.execute("SELECT * FROM paired_event_arm WHERE decision_id=? ORDER BY arm_name", (decision["decision_id"],)).fetchall()]
            captures = upstream.execute(
                "SELECT * FROM official_event_horizon_capture WHERE input_capture_id=? ORDER BY horizon_min",
                (decision["input_capture_id"],),
            ).fetchall()
            for capture_raw in captures:
                capture = dict(capture_raw)
                counts["available_horizons"] += 1
                horizon_min = int(capture.get("horizon_min") or 0)
                if horizon_min not in HORIZONS_MIN:
                    counts["unsupported_horizons"] += 1
                    counts["health_state"] = "degraded"
                    continue
                if output.execute("SELECT 1 FROM paired_event_horizon_input WHERE decision_id=? AND horizon_min=?", (decision["decision_id"], horizon_min)).fetchone():
                    counts["duplicates"] += 1
                    continue
                quote_raw = upstream.execute(
                    "SELECT * FROM official_event_horizon_quote WHERE capture_id=? AND instrument=?",
                    (capture.get("capture_id"), decision.get("selected_instrument")),
                ).fetchone()
                horizon_input = _horizon_input(decision, capture, None if quote_raw is None else dict(quote_raw))
                outcomes = _outcome_rows(decision, arms, horizon_input)
                if insert_horizon_and_outcomes(output, horizon_input, outcomes):
                    counts["inserted_horizons"] += 1
                    counts["inserted_outcomes"] += len(outcomes)
                else:
                    counts["duplicates"] += 1
        return counts
    finally:
        upstream.close()


def database_counts(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        "decisions": int(connection.execute("SELECT COUNT(*) FROM paired_event_decision").fetchone()[0]),
        "sealed_decisions": int(connection.execute("SELECT COUNT(*) FROM paired_event_decision WHERE decision_state='sealed'").fetchone()[0]),
        "invalid_decisions": int(connection.execute("SELECT COUNT(*) FROM paired_event_decision WHERE decision_state='invalid'").fetchone()[0]),
        "arms": int(connection.execute("SELECT COUNT(*) FROM paired_event_arm").fetchone()[0]),
        "horizon_inputs": int(connection.execute("SELECT COUNT(*) FROM paired_event_horizon_input").fetchone()[0]),
        "outcomes": int(connection.execute("SELECT COUNT(*) FROM paired_event_outcome").fetchone()[0]),
    }


def run_cycle(
    *,
    output_database: Path = OUTPUT_DATABASE,
    release_database: Path = INPUT_RELEASE_DATABASE,
    mapping_database: Path = INPUT_MAPPING_DATABASE,
    horizon_database: Path = INPUT_HORIZON_DATABASE,
    feature_snapshot_path: Path = FEATURE_SNAPSHOT_PATH,
    authority_map_path: Path = AUTHORITY_MAP_PATH,
    state_path: Path = STATE_PATH,
    heartbeat_path: Path = HEARTBEAT_PATH,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    config = validate_frozen_config()
    validate_authority_map(authority_map_path)
    current = now or utc_now()
    connection = open_database(output_database)
    try:
        # This commit boundary is deliberate. The Horizon database has not yet
        # been opened, so no outcome can influence pair, side or abstention.
        decisions = seal_due_decisions(
            connection,
            release_database=release_database,
            mapping_database=mapping_database,
            feature_snapshot_path=feature_snapshot_path,
            authority_map_path=authority_map_path,
            # Preserve a caller-supplied deterministic test clock, but leave
            # production as None so each event samples a genuinely fresh clock.
            now=now,
        )
        outcomes = mature_available_outcomes(connection, horizon_database=horizon_database)
        counts = database_counts(connection)
    finally:
        connection.close()
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": iso_utc(current),
        "status": (
            "ok"
            if decisions.get("health_state") == "healthy"
            and outcomes.get("health_state") == "healthy"
            else "degraded"
        ),
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "activated_utc": config["activated_utc"],
        "decision_cycle": decisions,
        "outcome_cycle": outcomes,
        "input_health": {
            **dict(decisions.get("input_health") or {}),
            **dict(outcomes.get("input_health") or {}),
        },
        "ledger": counts,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "supported_execution_decision": "no_trade",
    }
    write_json_atomic(state_path, payload)
    write_json_atomic(heartbeat_path, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-database", type=Path, default=OUTPUT_DATABASE)
    parser.add_argument("--release-database", type=Path, default=INPUT_RELEASE_DATABASE)
    parser.add_argument("--mapping-database", type=Path, default=INPUT_MAPPING_DATABASE)
    parser.add_argument("--horizon-database", type=Path, default=INPUT_HORIZON_DATABASE)
    parser.add_argument("--feature-snapshot", type=Path, default=FEATURE_SNAPSHOT_PATH)
    parser.add_argument("--authority-map", type=Path, default=AUTHORITY_MAP_PATH)
    parser.add_argument("--state", type=Path, default=STATE_PATH)
    parser.add_argument("--heartbeat", type=Path, default=HEARTBEAT_PATH)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        payload = run_cycle(
            output_database=args.output_database,
            release_database=args.release_database,
            mapping_database=args.mapping_database,
            horizon_database=args.horizon_database,
            feature_snapshot_path=args.feature_snapshot,
            authority_map_path=args.authority_map,
            state_path=args.state,
            heartbeat_path=args.heartbeat,
        )
        if args.interval_sec <= 0.0:
            print(json.dumps(payload, indent=2, sort_keys=True))
            return 0 if payload.get("status") == "ok" else 2
        if args.duration_sec > 0.0 and time.monotonic() - started >= args.duration_sec:
            return 0
        time.sleep(max(0.1, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
