#!/usr/bin/env python3
"""Independently verify Official Event Paired Evaluator V1.

This module intentionally does not import the producer.  It reconstructs the
prospective decision schedule, issuer binding, pair choice, causal technical
rule, control arms and executable economics directly from the frozen upstream
ledgers and copied bytes.  It is read-only and cannot trade, authorize or
promote anything.
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
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parent
LOCAL_NEWS = ROOT / "data" / "oanda_training_manager" / "local_news_sentiment"
CONFIG_PATH = ROOT / "config" / "official_event_paired_evaluator_v1.json"
OUTPUT_DATABASE = (
    LOCAL_NEWS / "official_event_paired_evaluator_v1_20260830b.sqlite"
)
RELEASE_DATABASE = LOCAL_NEWS / "official_release_fast_lane_v4.sqlite"
MAPPING_DATABASE = LOCAL_NEWS / "official_release_fast_mapping_v3.sqlite"
HORIZON_DATABASE = LOCAL_NEWS / "official_event_quote_horizon_capture_v1.sqlite"
PRODUCER_SOURCE_PATH = ROOT / "oanda_official_event_paired_evaluator_v1.py"
AUTHORITY_MAP_PATH = ROOT / "config" / "official_central_bank_source_map_v1.json"
NEWS_SOURCE_CONFIG_PATH = ROOT / "config" / "news_sources_v1.json"
FAST_LANE_SOURCE_PATH = ROOT / "oanda_official_release_fast_lane.py"
HORIZON_SOURCE_PATH = ROOT / "oanda_official_event_quote_horizon_capture_v1.py"
STATE_PATH = (
    LOCAL_NEWS
    / "official_event_paired_evaluator_verifier_latest_v1_20260830b.json"
)
HEARTBEAT_PATH = (
    LOCAL_NEWS
    / "official_event_paired_evaluator_verifier_heartbeat_v1_20260830b.json"
)

SCHEMA_VERSION = "official_event_paired_evaluator_v1_verifier"
CONTRACT_ID = "official_event_paired_evaluator_v1_append_only_20260830"
COHORT_ID = "official_event_paired_evaluator_v1_20260830b"
FROZEN_CONFIG_SHA256 = (
    "d553877ba5140e31c5eb42233761bf5923cae4b44e601325fc776dab6b2e50e1"
)
FROZEN_PRODUCER_SOURCE_SHA256 = (
    "ea3374e4bab29dfc2f5703853927b14d119b40c2394bc7050fab8045f42073d7"
)
FROZEN_PRODUCER_FILE_SHA256 = (
    "eeccb9679d29bedafa9696c8923dbab3cad45b2616c773a0b9895e816ccd08ed"
)
ACTIVATED_UTC = dt.datetime(2026, 8, 30, 19, 0, tzinfo=dt.timezone.utc)
ENTRY_CONTRACT_ID = (
    "official_release_raw_quote_capture_v1_append_boundary_all68_20260829"
)
ENTRY_COHORT_ID = "official_release_raw_quote_capture_v1_20260829a"
RAW_COLLECTOR_CONTRACT_ID = (
    "official_release_fast_lane_v4_selection_v2_authoritative_communications_20260828T150000Z"
)
RAW_COLLECTOR_COHORT_ID = (
    "official_release_fast_lane_v4_communications_20260828T150000Z"
)
MAPPING_CONTRACT_ID = (
    "official_release_fast_mapping_v3_semantic_vs_publish_gate_20260824"
)
MAPPING_COHORT_ID = "official_release_fast_mapping_v3_20260824"
CLASSIFICATION_VERSION = (
    "local_fx_news_rules_20260828_v151_pair_breakout_recap_boundary"
)
HORIZON_CONTRACT_ID = (
    "official_event_quote_horizon_capture_v1_all68_append_only_20260830"
)
HORIZON_COHORT_ID = "official_event_quote_horizon_capture_v1_20260830a"
AUTHORITY_MAP_CONTRACT_ID = (
    "official_central_bank_source_map_v9_us_policy_communication_clocks_20260827"
)
AUTHORITY_MAP_SHA256 = (
    "09d4f80708cbed509564e2f7bc5245a24de44df3728ed0583b1d9cbb6f299a19"
)
NEWS_SOURCE_CONFIG_SHA256 = (
    "6058d3a81d250add66e1d22b0f2541990f3bc90c52575c4a7a97c3361ad0a45b"
)
FAST_LANE_SOURCE_SHA256 = (
    "55f3a1260c2c3644fa2c4de0e70c4fe081e21fff366caeeaae8fe80bfb1d752a"
)
HORIZON_SOURCE_SHA256 = (
    "a0920b5edb530876c42e1e2892f624624db4221262c6a67fdcb42f9a12c146c9"
)
ENTRY_ECONOMICS_ROLE = (
    "event_clock_counterfactual_research_measurement_not_proof_of_"
    "order_submission_before_semantic_decision"
)
EXPECTED_INSTRUMENTS = (
    "AUD_CAD", "AUD_CHF", "AUD_HKD", "AUD_JPY", "AUD_NZD", "AUD_SGD",
    "AUD_USD", "CAD_CHF", "CAD_HKD", "CAD_JPY", "CAD_SGD", "CHF_HKD",
    "CHF_JPY", "CHF_ZAR", "EUR_AUD", "EUR_CAD", "EUR_CHF", "EUR_CZK",
    "EUR_DKK", "EUR_GBP", "EUR_HKD", "EUR_HUF", "EUR_JPY", "EUR_NOK",
    "EUR_NZD", "EUR_PLN", "EUR_SEK", "EUR_SGD", "EUR_TRY", "EUR_USD",
    "EUR_ZAR", "GBP_AUD", "GBP_CAD", "GBP_CHF", "GBP_HKD", "GBP_JPY",
    "GBP_NZD", "GBP_PLN", "GBP_SGD", "GBP_USD", "GBP_ZAR", "HKD_JPY",
    "NZD_CAD", "NZD_CHF", "NZD_HKD", "NZD_JPY", "NZD_SGD", "NZD_USD",
    "SGD_CHF", "SGD_JPY", "TRY_JPY", "USD_CAD", "USD_CHF", "USD_CNH",
    "USD_CZK", "USD_DKK", "USD_HKD", "USD_HUF", "USD_JPY", "USD_MXN",
    "USD_NOK", "USD_PLN", "USD_SEK", "USD_SGD", "USD_THB", "USD_TRY",
    "USD_ZAR", "ZAR_JPY",
)
EXPECTED_INSTRUMENT_COUNT = 68
EXPECTED_UNIVERSE_SHA256 = (
    "b6abe559dcaa9faa0a1e48217f97a12dc9043eb9595d2b946bf80f93035be142"
)
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
HORIZON_TERMINAL_DELAY_SEC = 20.0
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
POLICY = {
    "prospective_only": True,
    "historical_backfill_allowed": False,
    "decision_and_schedule_must_be_sealed_before_first_horizon": True,
    "outcome_database_may_not_be_read_before_decision_commit": True,
    "technical_may_confirm_or_veto_official_but_never_reverse": True,
    "one_pair_shared_by_all_arms": True,
    "entry_economics_role": ENTRY_ECONOMICS_ROLE,
    "research_only": True,
    "execution_eligible": False,
    "can_place_orders": False,
    "can_authorize": False,
    "can_promote": False,
    "supported_execution_decision": "no_trade",
}
SAFETY_COLUMNS = {
    "research_only": 1,
    "execution_eligible": 0,
    "can_place_orders": 0,
    "can_authorize": 0,
    "can_promote": 0,
    "contract_id": CONTRACT_ID,
    "cohort_id": COHORT_ID,
}
TABLES = (
    "paired_event_cohort_manifest",
    "paired_event_decision",
    "paired_event_arm",
    "paired_event_horizon_input",
    "paired_event_outcome",
)
MANIFEST_COLUMNS = (
    "manifest_id",
    "contract_id",
    "cohort_id",
    "activated_utc",
    "config_file_sha256",
    "producer_source_sha256",
    "producer_file_sha256",
    "authority_map_sha256",
    "news_source_config_sha256",
    "fast_lane_source_sha256",
    "horizon_capture_source_sha256",
    "required_classification_version",
    "entry_max_capture_latency_sec",
    "entry_max_quote_age_sec",
    "entry_max_future_skew_sec",
    "entry_max_event_offset_sec",
    "horizon_max_attempt_delay_sec",
    "horizon_max_snapshot_age_sec",
    "horizon_max_quote_age_sec",
    "horizon_max_future_skew_sec",
    "horizon_max_target_offset_sec",
    "research_only",
    "execution_eligible",
    "can_place_orders",
    "can_authorize",
    "can_promote",
)
DECISION_IDENTITY_COLUMNS = {
    "decision_read_started_utc",
    "decision_precommit_utc",
    "source_contract_id",
    "source_cohort_id",
    "authority_map_contract_id",
    "authority_map_bytes",
    "authority_map_sha256",
    "news_source_config_sha256",
    "config_file_sha256",
    "producer_source_sha256",
    "producer_file_sha256",
    "fast_lane_source_sha256",
    "horizon_capture_source_sha256",
}
HORIZON_IDENTITY_COLUMNS = {
    "horizon_row_bytes",
    "horizon_row_sha256",
    "exit_component_row_bytes",
    "exit_component_row_sha256",
}


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    )


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_text(value: str) -> str:
    return sha256_bytes(value.encode("utf-8"))


def normalized_producer_source_sha256(raw: bytes) -> str:
    """Rebuild the producer's self-hash without importing producer code."""

    normalized = re.sub(
        rb'FROZEN_PRODUCER_SOURCE_SHA256 = "[A-Za-z0-9_]+"',
        b'FROZEN_PRODUCER_SOURCE_SHA256 = "<FROZEN>"',
        raw,
        count=1,
    )
    return sha256_bytes(normalized)


def _read_authority_map(
    path: Path, failures: list[str]
) -> tuple[bytes, dict[str, Any]]:
    try:
        raw = path.read_bytes()
        parsed = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        failures.append(f"authority_map:unreadable:{type(exc).__name__}")
        return b"", {}
    payload = parsed if isinstance(parsed, dict) else {}
    record(
        failures,
        sha256_bytes(raw) == AUTHORITY_MAP_SHA256,
        "authority_map:file_sha256:mismatch",
    )
    record(
        failures,
        payload.get("contract_id") == AUTHORITY_MAP_CONTRACT_ID,
        "authority_map:contract_id:mismatch",
    )
    record(
        failures,
        int(payload.get("expected_currency_count") or 0) == 21,
        "authority_map:currency_count:mismatch",
    )
    return raw, payload


def _authority_currency_for_source(
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


def _configured_source_lineage(source: Mapping[str, Any]) -> tuple[str, str]:
    contract = str(source.get("source_contract_id") or "").strip()
    cohort = str(source.get("source_cohort_id") or "").strip()
    if contract:
        return contract, cohort
    material = {
        str(key): value
        for key, value in source.items()
        if key
        not in {
            "source_contract_id",
            "source_cohort_id",
            "source_contract_derived",
            "source_lineage_version",
            "source_config_sha256",
        }
    }
    digest = sha256_bytes(
        json.dumps(
            material,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    )
    source_id = re.sub(
        r"[^a-z0-9_.-]+",
        "_",
        str(source.get("source_id") or "").casefold(),
    ).strip("_") or "unknown"
    derived = f"derived_source_config_lineage_v1:{source_id}:{digest[:24]}"
    return derived, derived


def _read_news_source_config(
    path: Path, failures: list[str]
) -> tuple[bytes, dict[str, tuple[str, str]]]:
    try:
        raw = path.read_bytes()
        parsed = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        failures.append(f"news_source_config:unreadable:{type(exc).__name__}")
        return b"", {}
    payload = parsed if isinstance(parsed, dict) else {}
    record(
        failures,
        sha256_bytes(raw) == NEWS_SOURCE_CONFIG_SHA256,
        "news_source_config:file_sha256:mismatch",
    )
    lineages: dict[str, tuple[str, str]] = {}
    for row in payload.get("sources") or []:
        if isinstance(row, Mapping):
            source_id = str(row.get("source_id") or "").strip()
            if source_id:
                lineages[source_id] = _configured_source_lineage(row)
    record(failures, bool(lineages), "news_source_config:lineages:empty")
    return raw, lineages


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


def same_number(left: Any, right: Any, tolerance: float = 1e-8) -> bool:
    if left is None or right is None:
        return left is None and right is None
    a = finite_number(left)
    b = finite_number(right)
    return (
        a is not None
        and b is not None
        and math.isclose(a, b, rel_tol=0.0, abs_tol=tolerance)
    )


def decode_json_bytes(value: Any) -> tuple[bytes, dict[str, Any] | None]:
    raw = bytes(value or b"")
    if not raw:
        return raw, None
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return raw, None
    return raw, parsed if isinstance(parsed, dict) else None


def open_read_only(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def record(failures: list[str], condition: bool, message: str) -> None:
    if not condition:
        failures.append(message)


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
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


def _verify_config(path: Path, failures: list[str]) -> bytes:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        failures.append(f"config:unreadable:{type(exc).__name__}")
        return b""
    expected = {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "required_entry_capture_contract_id": ENTRY_CONTRACT_ID,
        "required_entry_capture_cohort_id": ENTRY_COHORT_ID,
        "required_raw_release_collector_contract_id": RAW_COLLECTOR_CONTRACT_ID,
        "required_raw_release_collector_cohort_id": RAW_COLLECTOR_COHORT_ID,
        "required_mapping_contract_id": MAPPING_CONTRACT_ID,
        "required_mapping_cohort_id": MAPPING_COHORT_ID,
        "required_classification_version": CLASSIFICATION_VERSION,
        "required_horizon_capture_contract_id": HORIZON_CONTRACT_ID,
        "required_horizon_capture_cohort_id": HORIZON_COHORT_ID,
        "required_source_authority_map_contract_id": AUTHORITY_MAP_CONTRACT_ID,
        "required_source_authority_map_sha256": AUTHORITY_MAP_SHA256,
        "required_news_source_config_sha256": NEWS_SOURCE_CONFIG_SHA256,
        "required_fast_lane_source_sha256": FAST_LANE_SOURCE_SHA256,
        "required_horizon_capture_source_sha256": HORIZON_SOURCE_SHA256,
        "expected_instrument_count": EXPECTED_INSTRUMENT_COUNT,
        "expected_instrument_universe_sha256": EXPECTED_UNIVERSE_SHA256,
    }
    record(
        failures,
        sha256_bytes(raw) == FROZEN_CONFIG_SHA256,
        "config:file_sha256:mismatch",
    )
    for key, value in expected.items():
        record(failures, payload.get(key) == value, f"config:{key}:mismatch")
    record(
        failures,
        parse_time(payload.get("activated_utc")) == ACTIVATED_UTC,
        "config:activated_utc:mismatch",
    )
    decision = payload.get("decision")
    decision = decision if isinstance(decision, Mapping) else {}
    for key, value in {
        "first_horizon_min": 1,
        "seal_deadline_offset_sec": SEAL_DEADLINE_OFFSET_SEC,
        "pair_selection": (
            "lowest_entry_spread_among_pairs_containing_issuer_then_lexicographic"
        ),
        "clock_policy": "fresh_read_start_and_fresh_precommit_clock_per_event",
        "first_horizon_commit_margin_sec": FIRST_HORIZON_COMMIT_MARGIN_SEC,
        "issuer_binding": (
            "single_identical_currency_from_direct_verified_raw_source_"
            "authority_and_mapping_source_currencies; mentioned currencies ignored"
        ),
        "mapping_selection": "earliest_mapping_copy_observed_by_seal_deadline",
        "market_episode_bucket_min": EPISODE_BUCKET_MIN,
    }.items():
        record(failures, decision.get(key) == value, f"config:decision:{key}:mismatch")
    technical = payload.get("technical_rule")
    technical = technical if isinstance(technical, Mapping) else {}
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
        record(
            failures,
            technical.get(key) == value,
            f"config:technical:{key}:mismatch",
        )
    record(failures, tuple(payload.get("arms") or ()) == ARMS, "config:arms:mismatch")
    try:
        horizons = tuple(int(value) for value in payload.get("horizons_min") or ())
        slippage = tuple(
            float(value) for value in payload.get("round_trip_slippage_pips") or ()
        )
    except (TypeError, ValueError):
        horizons, slippage = (), ()
    record(failures, horizons == HORIZONS_MIN, "config:horizons:mismatch")
    record(
        failures,
        slippage == SLIPPAGE_STRESS_PIPS,
        "config:slippage:mismatch",
    )
    policy = payload.get("policy")
    policy = policy if isinstance(policy, Mapping) else {}
    for key, value in POLICY.items():
        record(failures, policy.get(key) == value, f"config:policy:{key}:unsafe")
    entry_quality = payload.get("entry_capture_quality")
    entry_quality = entry_quality if isinstance(entry_quality, Mapping) else {}
    for key, value in {
        "maximum_capture_latency_sec": ENTRY_MAXIMUM_CAPTURE_LATENCY_SEC,
        "maximum_quote_age_sec": ENTRY_MAXIMUM_QUOTE_AGE_SEC,
        "maximum_future_skew_sec": ENTRY_MAXIMUM_FUTURE_SKEW_SEC,
        "maximum_event_offset_sec": ENTRY_MAXIMUM_EVENT_OFFSET_SEC,
    }.items():
        record(
            failures,
            same_number(entry_quality.get(key), value),
            f"config:entry_capture_quality:{key}:mismatch",
        )
    horizon_quality = payload.get("horizon_capture_quality")
    horizon_quality = (
        horizon_quality if isinstance(horizon_quality, Mapping) else {}
    )
    for key, value in {
        "maximum_attempt_delay_sec": HORIZON_MAXIMUM_ATTEMPT_DELAY_SEC,
        "maximum_snapshot_age_sec": HORIZON_MAXIMUM_SNAPSHOT_AGE_SEC,
        "maximum_quote_age_sec": HORIZON_MAXIMUM_QUOTE_AGE_SEC,
        "maximum_future_skew_sec": HORIZON_MAXIMUM_FUTURE_SKEW_SEC,
        "maximum_target_offset_sec": HORIZON_MAXIMUM_TARGET_OFFSET_SEC,
    }.items():
        record(
            failures,
            same_number(horizon_quality.get(key), value),
            f"config:horizon_capture_quality:{key}:mismatch",
        )
    universe_material = json.dumps(list(EXPECTED_INSTRUMENTS), separators=(",", ":"))
    record(
        failures,
        len(EXPECTED_INSTRUMENTS) == EXPECTED_INSTRUMENT_COUNT
        and sha256_text(universe_material) == EXPECTED_UNIVERSE_SHA256,
        "verifier:frozen_universe_invalid",
    )
    return raw


def _verify_schema(connection: sqlite3.Connection, failures: list[str]) -> None:
    for table in TABLES:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        record(failures, exists is not None, f"schema:{table}:missing")
        for suffix in ("no_update", "no_delete"):
            trigger = f"{table}_{suffix}"
            row = connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",
                (trigger,),
            ).fetchone()
            sql = str(row[0] if row else "").upper()
            expected_action = "UPDATE" if suffix == "no_update" else "DELETE"
            record(
                failures,
                bool(row) and "RAISE" in sql and expected_action in sql,
                f"schema:trigger:{trigger}:missing_or_weak",
            )
    foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
    record(failures, not foreign_keys, "schema:foreign_key_violation")
    manifest_columns = tuple(
        str(row[1])
        for row in connection.execute(
            "PRAGMA table_info(paired_event_cohort_manifest)"
        ).fetchall()
    )
    record(
        failures,
        manifest_columns == MANIFEST_COLUMNS,
        "schema:paired_event_cohort_manifest:columns_mismatch",
    )
    decision_columns = {
        str(row[1])
        for row in connection.execute(
            "PRAGMA table_info(paired_event_decision)"
        ).fetchall()
    }
    record(
        failures,
        DECISION_IDENTITY_COLUMNS <= decision_columns,
        "schema:paired_event_decision:identity_columns_missing",
    )
    horizon_columns = {
        str(row[1])
        for row in connection.execute(
            "PRAGMA table_info(paired_event_horizon_input)"
        ).fetchall()
    }
    record(
        failures,
        HORIZON_IDENTITY_COLUMNS <= horizon_columns,
        "schema:paired_event_horizon_input:identity_columns_missing",
    )


def _verify_manifest(
    connection: sqlite3.Connection,
    *,
    config_sha: str,
    producer_source_sha: str,
    producer_file_sha: str,
    authority_sha: str,
    news_source_sha: str,
    fast_lane_sha: str,
    horizon_source_sha: str,
    failures: list[str],
) -> None:
    rows = connection.execute(
        "SELECT * FROM paired_event_cohort_manifest"
    ).fetchall()
    record(failures, len(rows) == 1, "manifest:row_count")
    if len(rows) != 1:
        return
    row = rows[0]
    expected = {
        "manifest_id": "official_event_paired_evaluator_v1_manifest",
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "activated_utc": ACTIVATED_UTC.isoformat(),
        "config_file_sha256": config_sha,
        "producer_source_sha256": producer_source_sha,
        "producer_file_sha256": producer_file_sha,
        "authority_map_sha256": authority_sha,
        "news_source_config_sha256": news_source_sha,
        "fast_lane_source_sha256": fast_lane_sha,
        "horizon_capture_source_sha256": horizon_source_sha,
        "required_classification_version": CLASSIFICATION_VERSION,
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
    for key, value in expected.items():
        record(failures, row[key] == value, f"manifest:{key}:mismatch")


def _single_currency(value: Any) -> str:
    if not isinstance(value, (list, tuple)):
        return ""
    currencies = sorted(
        {
            str(item or "").upper().strip()
            for item in value
            if str(item or "").strip()
        }
    )
    return currencies[0] if len(currencies) == 1 else ""


def _pair_side(instrument: str, issuer: str, score: float | None) -> str:
    if score is None or score == 0.0 or "_" not in instrument:
        return ""
    base, quote = instrument.split("_", 1)
    if issuer == base:
        return "buy" if score > 0.0 else "sell"
    if issuer == quote:
        return "sell" if score > 0.0 else "buy"
    return ""


def _reverse(side: str) -> str:
    return "sell" if side == "buy" else "buy" if side == "sell" else ""


def _select_pair(
    quotes: Mapping[str, Any], issuer: str
) -> tuple[str, dict[str, Any]]:
    candidates: list[tuple[float, str, dict[str, Any]]] = []
    for instrument in EXPECTED_INSTRUMENTS:
        if issuer not in instrument.split("_"):
            continue
        raw = quotes.get(instrument)
        if not isinstance(raw, Mapping):
            continue
        bid = finite_number(raw.get("bid"))
        ask = finite_number(raw.get("ask"))
        pip = finite_number(raw.get("pip"))
        if (
            bid is None
            or ask is None
            or pip is None
            or bid <= 0.0
            or ask <= bid
            or pip <= 0.0
        ):
            continue
        candidates.append((round((ask - bid) / pip, 12), instrument, dict(raw)))
    if not candidates:
        return "", {}
    _, instrument, row = sorted(candidates, key=lambda item: (item[0], item[1]))[0]
    return instrument, row


def _episode_id(issuer: str, event: dt.datetime) -> str:
    bucket_seconds = EPISODE_BUCKET_MIN * 60
    bucket = int(event.timestamp()) // bucket_seconds * bucket_seconds
    return "official_event_episode_" + sha256_text(
        f"{issuer}|{bucket}|{EPISODE_BUCKET_MIN}m"
    )[:32]


def _factor_id(instrument: str, side: str, issuer: str) -> str:
    if side not in {"buy", "sell"} or "_" not in instrument or not issuer:
        return ""
    base, quote = instrument.split("_", 1)
    if issuer == base:
        sign = "+" if side == "buy" else "-"
    elif issuer == quote:
        sign = "-" if side == "buy" else "+"
    else:
        return ""
    return "signed_currency_factor_" + sha256_text(f"{issuer}|{sign}")[:32]


def _ema(values: Sequence[float], span: int) -> float:
    alpha = 2.0 / (float(span) + 1.0)
    output = float(values[0])
    for value in values[1:]:
        output = alpha * float(value) + (1.0 - alpha) * output
    return output


def _verify_technical(
    decision: sqlite3.Row,
    event: dt.datetime,
    instrument: str,
    failures: list[str],
) -> tuple[str, str]:
    label = f"decision:{decision['decision_id']}:technical"
    raw, material = decode_json_bytes(decision["technical_material_bytes"])
    record(
        failures,
        sha256_bytes(raw) == str(decision["technical_material_sha256"]),
        f"{label}:material_sha_mismatch",
    )
    if not instrument:
        record(failures, raw == b"", f"{label}:unexpected_material_without_pair")
        return "", "technical_pair_unavailable"
    record(failures, material is not None, f"{label}:material_invalid")
    if material is None:
        return "", "technical_material_invalid"
    record(
        failures,
        canonical_json(material).encode("utf-8") == raw,
        f"{label}:material_not_canonical",
    )
    record(failures, material.get("rule_id") == TECHNICAL_RULE_ID, f"{label}:rule")
    record(failures, material.get("instrument") == instrument, f"{label}:instrument")
    record(
        failures,
        parse_time(material.get("event_t0_utc")) == event,
        f"{label}:event_clock",
    )
    digest = str(decision["technical_snapshot_file_sha256"] or "")
    record(
        failures,
        (not digest) or (len(digest) == 64 and all(c in "0123456789abcdef" for c in digest)),
        f"{label}:snapshot_file_sha_invalid",
    )

    generated = parse_time(material.get("snapshot_generated_utc"))
    contract = material.get("snapshot_contract")
    contract = contract if isinstance(contract, Mapping) else {}
    intrahour = contract.get("intrahour_forecast")
    intrahour = intrahour if isinstance(intrahour, Mapping) else {}
    component = material.get("instrument_component")
    component = component if isinstance(component, Mapping) else {}
    structural = component.get("structural_series")
    structural = structural if isinstance(structural, Mapping) else {}
    m1 = structural.get("M1")
    m1 = m1 if isinstance(m1, Mapping) else {}
    closes_raw = m1.get("close") if isinstance(m1.get("close"), list) else []
    times = (
        m1.get("bar_start_times_utc")
        if isinstance(m1.get("bar_start_times_utc"), list)
        else []
    )
    closes = [finite_number(value) for value in closes_raw]
    reason = ""
    if not digest:
        reason = "technical_snapshot_missing"
    elif material.get("snapshot_schema_version") != 1:
        reason = "technical_snapshot_schema_mismatch"
    elif generated is None:
        reason = "technical_snapshot_clock_missing"
    elif generated > event:
        reason = "technical_snapshot_postdates_event_t0"
    elif (event - generated).total_seconds() > TECHNICAL_MAXIMUM_SNAPSHOT_AGE_SEC:
        reason = "technical_snapshot_stale_at_event_t0"
    elif contract.get("feature_values_observable_at_generation") is not True:
        reason = "technical_snapshot_not_observable_at_generation"
    elif contract.get("historical_outcomes_included") is not False:
        reason = "technical_snapshot_contains_outcomes"
    elif intrahour.get("completed_bars_only") is not True:
        reason = "technical_snapshot_not_completed_bars_only"
    elif not component:
        reason = "technical_instrument_missing"
    elif len(closes) < TECHNICAL_MINIMUM_CLOSES or len(times) != len(closes_raw):
        reason = "technical_completed_close_history_insufficient"
    elif any(
        value is None or value <= 0.0
        for value in closes[-TECHNICAL_MINIMUM_CLOSES:]
    ):
        reason = "technical_completed_closes_invalid"
    else:
        used_times = [
            parse_time(value) for value in times[-TECHNICAL_MINIMUM_CLOSES:]
        ]
        if any(value is None for value in used_times):
            reason = "technical_completed_bar_clock_missing"
        else:
            valid_times = [value for value in used_times if value is not None]
            if any(
                right <= left
                for left, right in zip(valid_times, valid_times[1:])
            ):
                reason = "technical_completed_bar_clocks_not_strictly_increasing"
            elif generated is None or any(
                value + dt.timedelta(minutes=1) > generated
                for value in valid_times
            ):
                reason = "technical_bar_not_complete_at_snapshot_generation"
            elif any(
                value + dt.timedelta(minutes=1) > event for value in valid_times
            ):
                reason = "technical_bar_not_complete_at_event_t0"
            elif (
                event - (valid_times[-1] + dt.timedelta(minutes=1))
            ).total_seconds() > TECHNICAL_MAXIMUM_LAST_BAR_AGE_SEC:
                reason = "technical_last_completed_bar_stale_at_event_t0"

    side = ""
    if not reason:
        values = [float(v) for v in closes[-TECHNICAL_MINIMUM_CLOSES:] if v is not None]
        fast = _ema(values, TECHNICAL_FAST_SPAN)
        slow = _ema(values, TECHNICAL_SLOW_SPAN)
        side = "buy" if fast > slow else "sell" if fast < slow else ""
        reason = "" if side else "technical_ema_tie"
        record(
            failures,
            material.get("closes_used") == values,
            f"{label}:closes_used_mismatch",
        )
        record(
            failures,
            same_number(material.get("fast_ema"), fast, 1e-12),
            f"{label}:fast_ema_mismatch",
        )
        record(
            failures,
            same_number(material.get("slow_ema"), slow, 1e-12),
            f"{label}:slow_ema_mismatch",
        )
    expected_state = "trade" if side else "abstain"
    record(
        failures,
        material.get("technical_state") == expected_state,
        f"{label}:material_state_mismatch",
    )
    record(
        failures,
        str(material.get("technical_reason") or "") == reason,
        f"{label}:material_reason_mismatch",
    )
    record(
        failures,
        str(material.get("technical_side") or "") == side,
        f"{label}:material_side_mismatch",
    )
    return side, reason


def _safety(row: sqlite3.Row, label: str, failures: list[str]) -> None:
    for key, expected in SAFETY_COLUMNS.items():
        record(failures, row[key] == expected, f"{label}:safety:{key}")


def _expected_arms(
    decision: sqlite3.Row,
    official_side: str,
    technical_side: str,
    technical_reason: str,
) -> dict[str, dict[str, str]]:
    expected: dict[str, dict[str, str]] = {}
    state = str(decision["decision_state"])
    invalid_reason = str(decision["invalid_reason"])
    for name in ARMS:
        action, side, reason = "abstain", "abstain", ""
        if name == "no_trade":
            reason = "predeclared_no_trade_control"
        elif state != "sealed":
            action, side, reason = "invalid", "invalid", invalid_reason
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
                action, side = "trade", _reverse(official_side)
            else:
                reason = "official_issuer_direction_unavailable"
        expected[name] = {"action_state": action, "side": side, "reason": reason}
    return expected


def _verify_decision(
    output: sqlite3.Connection,
    release: sqlite3.Connection,
    mapping_db: sqlite3.Connection,
    decision: sqlite3.Row,
    config_bytes: bytes,
    producer_source_sha: str,
    producer_file_sha: str,
    authority_bytes: bytes,
    authority_payload: Mapping[str, Any],
    news_source_bytes: bytes,
    source_lineages: Mapping[str, tuple[str, str]],
    fast_lane_sha: str,
    horizon_source_sha: str,
    failures: list[str],
) -> None:
    label = f"decision:{decision['decision_id']}"
    _safety(decision, label, failures)
    capture = release.execute(
        "SELECT * FROM official_release_quote_capture WHERE capture_id=?",
        (decision["input_capture_id"],),
    ).fetchone()
    record(failures, capture is not None, f"{label}:input_capture_missing")
    if capture is None:
        return
    source = release.execute(
        "SELECT * FROM official_release_observation WHERE observation_id=?",
        (capture["observation_id"],),
    ).fetchone()
    record(failures, source is not None, f"{label}:source_row_missing")

    capture_payload_raw, capture_payload = decode_json_bytes(
        decision["input_capture_bytes"]
    )
    source_payload_raw, source_payload = decode_json_bytes(
        decision["source_payload_bytes"]
    )
    mapping_payload_raw, mapping_payload = decode_json_bytes(
        decision["mapping_payload_bytes"]
    )
    copied_authority_raw, copied_authority = decode_json_bytes(
        decision["authority_map_bytes"]
    )
    for name, raw, claimed in (
        ("input_capture", capture_payload_raw, decision["input_capture_sha256"]),
        ("source_payload", source_payload_raw, decision["source_payload_sha256"]),
        ("mapping_payload", mapping_payload_raw, decision["mapping_payload_sha256"]),
        ("entry_component", bytes(decision["entry_component_bytes"]), decision["entry_component_sha256"]),
        ("input_capture_row", bytes(decision["input_capture_row_bytes"]), decision["input_capture_row_sha256"]),
        ("source_row", bytes(decision["source_row_bytes"]), decision["source_row_sha256"]),
        ("mapping_row", bytes(decision["mapping_row_bytes"]), decision["mapping_row_sha256"]),
        ("authority_map", copied_authority_raw, decision["authority_map_sha256"]),
    ):
        record(failures, sha256_bytes(raw) == str(claimed), f"{label}:{name}:sha")
    record(
        failures,
        capture_payload_raw == str(capture["capture_payload_json"]).encode("utf-8"),
        f"{label}:input_capture_payload_drift",
    )
    record(
        failures,
        bytes(decision["input_capture_row_bytes"])
        == canonical_json(dict(capture)).encode("utf-8"),
        f"{label}:input_capture_row_drift",
    )
    if source is not None:
        record(
            failures,
            source_payload_raw == str(source["raw_payload_json"]).encode("utf-8"),
            f"{label}:source_payload_drift",
        )
        record(
            failures,
            bytes(decision["source_row_bytes"])
            == canonical_json(dict(source)).encode("utf-8"),
            f"{label}:source_row_drift",
        )

    event = parse_time(capture["event_first_known_utc"])
    issued = parse_time(decision["issued_utc"])
    read_started = parse_time(decision["decision_read_started_utc"])
    precommit = parse_time(decision["decision_precommit_utc"])
    first_target = parse_time(decision["first_horizon_target_utc"])
    record(
        failures,
        None not in {event, read_started, precommit, issued},
        f"{label}:clock_invalid",
    )
    if event is None or issued is None or read_started is None or precommit is None:
        return
    latency = (issued - event).total_seconds()
    record(
        failures,
        parse_time(decision["event_first_known_utc"]) == event,
        f"{label}:event_clock_mismatch",
    )
    record(
        failures,
        first_target == event + dt.timedelta(minutes=1),
        f"{label}:first_horizon_clock_mismatch",
    )
    record(
        failures,
        same_number(decision["decision_latency_sec"], latency, 1e-6),
        f"{label}:decision_latency_mismatch",
    )
    record(
        failures,
        precommit == issued,
        f"{label}:precommit_issued_clock_mismatch",
    )
    expected_id = "official_event_paired_decision_" + sha256_text(
        f"{decision['input_capture_id']}|{CONTRACT_ID}|{COHORT_ID}"
    )[:32]
    record(failures, decision["decision_id"] == expected_id, f"{label}:identity")
    record(
        failures,
        decision["observation_id"] == capture["observation_id"],
        f"{label}:observation_lineage",
    )
    record(
        failures,
        str(decision["config_file_sha256"]) == sha256_bytes(config_bytes),
        f"{label}:config_sha_mismatch",
    )
    record(
        failures,
        str(decision["producer_source_sha256"]) == producer_source_sha,
        f"{label}:producer_sha_mismatch",
    )
    record(
        failures,
        str(decision["producer_file_sha256"]) == producer_file_sha,
        f"{label}:producer_file_sha_mismatch",
    )
    record(
        failures,
        str(decision["fast_lane_source_sha256"]) == fast_lane_sha,
        f"{label}:fast_lane_source_sha_mismatch",
    )
    record(
        failures,
        str(decision["horizon_capture_source_sha256"]) == horizon_source_sha,
        f"{label}:horizon_source_sha_mismatch",
    )
    record(
        failures,
        str(decision["news_source_config_sha256"])
        == sha256_bytes(news_source_bytes),
        f"{label}:news_source_config_sha_mismatch",
    )
    record(
        failures,
        copied_authority_raw == authority_bytes,
        f"{label}:authority_map_bytes_drift",
    )
    record(
        failures,
        copied_authority == dict(authority_payload),
        f"{label}:authority_map_payload_drift",
    )
    record(
        failures,
        decision["authority_map_contract_id"] == AUTHORITY_MAP_CONTRACT_ID,
        f"{label}:authority_map_contract_mismatch",
    )
    record(
        failures,
        decision["entry_economics_role"] == ENTRY_ECONOMICS_ROLE,
        f"{label}:entry_economics_mislabeled",
    )

    expected_mapping = mapping_db.execute(
        """
        SELECT * FROM official_release_mapping
        WHERE observation_id=? AND mapped_utc<=?
          AND classification_version=?
          AND mapper_contract_id=? AND mapper_cohort_id=?
        ORDER BY mapped_utc,mapping_id LIMIT 1
        """,
        (
            capture["observation_id"],
            decision["decision_read_started_utc"],
            CLASSIFICATION_VERSION,
            MAPPING_CONTRACT_ID,
            MAPPING_COHORT_ID,
        ),
    ).fetchone()
    if expected_mapping is None:
        record(failures, decision["mapping_id"] == "", f"{label}:unexpected_mapping")
        record(failures, mapping_payload_raw == b"", f"{label}:unexpected_mapping_payload")
        record(
            failures,
            bytes(decision["mapping_row_bytes"]) == canonical_json({}).encode("utf-8"),
            f"{label}:unexpected_mapping_row",
        )
    else:
        record(
            failures,
            decision["mapping_id"] == expected_mapping["mapping_id"],
            f"{label}:earliest_mapping_not_selected",
        )
        record(
            failures,
            mapping_payload_raw
            == str(expected_mapping["mapping_payload_json"]).encode("utf-8"),
            f"{label}:mapping_payload_drift",
        )
        record(
            failures,
            bytes(decision["mapping_row_bytes"])
            == canonical_json(dict(expected_mapping)).encode("utf-8"),
            f"{label}:mapping_row_drift",
        )

    capture_payload = capture_payload or {}
    source_payload = source_payload or {}
    mapping_payload = mapping_payload or {}
    captured_at = parse_time(capture_payload.get("captured_utc"))
    derived_capture_latency = (
        (captured_at - event).total_seconds() if captured_at is not None else None
    )
    recorded_capture_latency = finite_number(
        capture_payload.get("capture_latency_seconds")
    )
    captured_quotes = capture_payload.get("quotes")
    captured_quotes = (
        captured_quotes if isinstance(captured_quotes, Mapping) else {}
    )
    invalid_entry_components = 0
    for expected_instrument in EXPECTED_INSTRUMENTS:
        component = captured_quotes.get(expected_instrument)
        if not isinstance(component, Mapping):
            invalid_entry_components += 1
            continue
        component_bid = finite_number(component.get("bid"))
        component_ask = finite_number(component.get("ask"))
        component_pip = finite_number(component.get("pip"))
        quote_clock = parse_time(
            component.get("quote_time_utc") or component.get("time")
        )
        derived_age = (
            (captured_at - quote_clock).total_seconds()
            if captured_at is not None and quote_clock is not None
            else None
        )
        event_offset = (
            (quote_clock - event).total_seconds()
            if quote_clock is not None
            else None
        )
        if (
            component_bid is None
            or component_ask is None
            or component_pip is None
            or component_bid <= 0.0
            or component_ask <= component_bid
            or component_pip <= 0.0
            or derived_age is None
            or derived_age < -ENTRY_MAXIMUM_FUTURE_SKEW_SEC
            or derived_age > ENTRY_MAXIMUM_QUOTE_AGE_SEC
            or event_offset is None
            or event_offset > ENTRY_MAXIMUM_EVENT_OFFSET_SEC
            or not same_number(component.get("age_seconds"), derived_age, 1e-6)
            or not same_number(
                component.get("event_offset_seconds"), event_offset, 1e-6
            )
            or not str(component.get("source") or "").strip()
        ):
            invalid_entry_components += 1
    source_id = str((source or {})["source_id"] if source is not None else "")
    source_issuer = _single_currency(source_payload.get("source_currencies"))
    mapping_issuer = _single_currency(mapping_payload.get("source_currencies"))
    authority_issuer = _authority_currency_for_source(
        authority_payload, source_id
    )
    issuer = (
        source_issuer
        if source_issuer
        and source_issuer == mapping_issuer
        and source_issuer == authority_issuer
        else ""
    )
    allowed = {currency for pair in EXPECTED_INSTRUMENTS for currency in pair.split("_")}
    if issuer not in allowed:
        issuer = ""
    record(failures, decision["issuer_currency"] == issuer, f"{label}:issuer_leakage")
    if source is not None:
        record(
            failures,
            decision["source_id"] == source["source_id"] == source_payload.get("source_id"),
            f"{label}:source_identity",
        )
        record(
            failures,
            decision["source_contract_id"] == source["source_contract_id"],
            f"{label}:source_contract_id",
        )
        record(
            failures,
            decision["source_cohort_id"] == source["source_cohort_id"],
            f"{label}:source_cohort_id",
        )
    if expected_mapping is not None:
        record(
            failures,
            mapping_payload.get("fast_lane_observation_id") == decision["observation_id"],
            f"{label}:mapping_payload_observation",
        )
        record(
            failures,
            mapping_payload.get("source_id") == decision["source_id"],
            f"{label}:mapping_payload_source",
        )

    quotes = capture_payload.get("quotes")
    quotes = quotes if isinstance(quotes, Mapping) else {}
    selected, entry = _select_pair(quotes, issuer) if issuer else ("", {})
    record(
        failures,
        decision["selected_instrument"] == selected,
        f"{label}:outcome_blind_pair_selection",
    )
    entry_raw = canonical_json(entry).encode("utf-8") if entry else b""
    record(
        failures,
        bytes(decision["entry_component_bytes"]) == entry_raw,
        f"{label}:entry_component_mismatch",
    )
    bid = finite_number(entry.get("bid")) if entry else None
    ask = finite_number(entry.get("ask")) if entry else None
    pip = finite_number(entry.get("pip")) if entry else None
    spread = (
        (ask - bid) / pip
        if bid is not None and ask is not None and pip is not None and pip > 0.0
        else None
    )
    for field, value in (
        ("entry_bid", bid),
        ("entry_ask", ask),
        ("entry_pip", pip),
        ("entry_spread_pips", spread),
    ):
        record(failures, same_number(decision[field], value), f"{label}:{field}")
    expected_quote_clock = str(entry.get("quote_time_utc") or entry.get("time") or "")
    record(
        failures,
        decision["entry_quote_utc"] == expected_quote_clock,
        f"{label}:entry_quote_clock",
    )

    # Rebuild the producer-independent validity verdict in its frozen order.
    invalid_reasons: list[str] = []
    capture_dict = dict(capture)
    source_dict = dict(source) if source is not None else {}
    mapping_dict = dict(expected_mapping) if expected_mapping is not None else {}
    if event < ACTIVATED_UTC:
        invalid_reasons.append("event_before_cohort_activation")
    if issued < event:
        invalid_reasons.append("decision_issued_before_event_t0")
    if read_started < event or read_started > issued:
        invalid_reasons.append("decision_read_clock_invalid")
    if issued > event + dt.timedelta(minutes=1) or latency > SEAL_DEADLINE_OFFSET_SEC:
        invalid_reasons.append("decision_not_sealed_before_first_horizon")
    if str(capture_dict.get("capture_contract_id") or "") != ENTRY_CONTRACT_ID:
        invalid_reasons.append("entry_capture_contract_mismatch")
    if str(capture_dict.get("capture_cohort_id") or "") != ENTRY_COHORT_ID:
        invalid_reasons.append("entry_capture_cohort_mismatch")
    if int(capture_dict.get("research_only") or 0) != 1 or any(
        int(capture_dict.get(key) or 0) != 0
        for key in ("execution_eligible", "can_authorize", "can_promote")
    ):
        invalid_reasons.append("entry_capture_safety_boundary_invalid")
    if (
        not capture_payload
        or capture_payload.get("timing_quality") != "prospective_exact_live_quote"
        or capture_payload.get("capture_contract_id") != ENTRY_CONTRACT_ID
        or capture_payload.get("capture_cohort_id") != ENTRY_COHORT_ID
        or capture_payload.get("input_prospective_observation") is not True
        or capture_payload.get("capture_activation_eligible") is not True
        or int(capture_payload.get("proof_quote_count") or 0)
        != EXPECTED_INSTRUMENT_COUNT
        or capture_payload.get("instrument_universe_sha256")
        != EXPECTED_UNIVERSE_SHA256
    ):
        invalid_reasons.append("entry_capture_payload_not_proof_valid")
    expected_capture_id = sha256_text(
        f"{capture_dict.get('observation_id') or ''}|"
        f"{ENTRY_CONTRACT_ID}|{ENTRY_COHORT_ID}"
    )
    if str(capture_dict.get("capture_id") or "") != expected_capture_id:
        invalid_reasons.append("entry_capture_id_derivation_mismatch")
    if (
        str(capture_payload.get("observation_id") or "")
        != str(capture_dict.get("observation_id") or "")
        or parse_time(capture_payload.get("event_first_known_utc"))
        != parse_time(capture_dict.get("event_first_known_utc"))
        or parse_time(capture_payload.get("captured_utc"))
        != parse_time(capture_dict.get("captured_utc"))
        or int(capture_dict.get("proof_quote_count") or 0)
        != EXPECTED_INSTRUMENT_COUNT
    ):
        invalid_reasons.append("entry_capture_row_payload_lineage_mismatch")
    if set(captured_quotes) != set(EXPECTED_INSTRUMENTS):
        invalid_reasons.append("entry_capture_quote_universe_mismatch")
    if (
        derived_capture_latency is None
        or derived_capture_latency < 0.0
        or derived_capture_latency > ENTRY_MAXIMUM_CAPTURE_LATENCY_SEC
        or recorded_capture_latency is None
        or abs(recorded_capture_latency - derived_capture_latency) > 1e-6
    ):
        invalid_reasons.append("entry_capture_latency_invalid")
    if invalid_entry_components:
        invalid_reasons.append(
            f"entry_capture_component_validation_failed:{invalid_entry_components}"
        )
    if source is None or not source_payload:
        invalid_reasons.append("raw_source_payload_missing")
    else:
        if (
            int(source_dict.get("prospective_observation") or 0) != 1
            or int(source_dict.get("listing_bootstrap") or 0) != 0
            or int(source_dict.get("identity_preexisting") or 0) != 0
            or int(source_dict.get("publisher_time_eligible") or 0) != 1
            or int(source_dict.get("observation_clock_trusted") or 0) != 1
        ):
            invalid_reasons.append("raw_source_row_not_prospective_causal")
        if int(source_dict.get("research_only") or 0) != 1 or any(
            int(source_dict.get(key) or 0) != 0
            for key in ("execution_eligible", "can_authorize")
        ):
            invalid_reasons.append("raw_source_safety_boundary_invalid")
        if (
            str(source_dict.get("collector_contract_id") or "")
            != RAW_COLLECTOR_CONTRACT_ID
            or str(source_dict.get("collector_cohort_id") or "")
            != RAW_COLLECTOR_COHORT_ID
        ):
            invalid_reasons.append(
                "raw_source_collector_contract_or_cohort_mismatch"
            )
        if (
            source_payload.get("source_verified") is not True
            or source_payload.get("source_direct") is not True
        ):
            invalid_reasons.append("raw_source_not_direct_verified_authority")
        expected_material = sha256_bytes(source_payload_raw)
        expected_observation = sha256_text(
            f"{source_dict.get('source_id') or ''}|"
            f"{source_dict.get('item_key') or ''}|{expected_material}"
        )
        if str(source_dict.get("material_sha256") or "") != expected_material:
            invalid_reasons.append("raw_source_material_hash_mismatch")
        if str(source_dict.get("observation_id") or "") != expected_observation:
            invalid_reasons.append("raw_source_observation_id_derivation_mismatch")
        if parse_time(source_dict.get("first_seen_utc")) != event:
            invalid_reasons.append("raw_source_event_clock_mismatch")
        if (
            str(source_payload.get("source_contract_id") or "")
            and str(source_payload.get("source_contract_id") or "")
            != str(source_dict.get("source_contract_id") or "")
        ):
            invalid_reasons.append("raw_source_contract_lineage_mismatch")
    if expected_mapping is None or not mapping_payload:
        invalid_reasons.append("mapping_copy_missing_by_seal_deadline")
    else:
        mapped = parse_time(mapping_dict.get("mapped_utc"))
        if parse_time(mapping_dict.get("first_seen_utc")) != event:
            invalid_reasons.append("mapping_event_clock_mismatch")
        if (
            mapped is None
            or mapped < event
            or mapped > read_started
            or mapped > issued
            or mapped > event + dt.timedelta(minutes=1)
        ):
            invalid_reasons.append("mapping_not_available_before_seal")
        if str(mapping_dict.get("mapper_contract_id") or "") != MAPPING_CONTRACT_ID:
            invalid_reasons.append("mapping_contract_mismatch")
        if str(mapping_dict.get("mapper_cohort_id") or "") != MAPPING_COHORT_ID:
            invalid_reasons.append("mapping_cohort_mismatch")
        if (
            str(mapping_dict.get("classification_version") or "")
            != CLASSIFICATION_VERSION
        ):
            invalid_reasons.append("mapping_classification_version_mismatch")
        expected_mapping_id = sha256_text(
            f"{mapping_dict.get('observation_id') or ''}|"
            f"{CLASSIFICATION_VERSION}|{MAPPING_CONTRACT_ID}"
        )
        if str(mapping_dict.get("mapping_id") or "") != expected_mapping_id:
            invalid_reasons.append("mapping_id_derivation_mismatch")
        if int(mapping_dict.get("input_prospective_observation") or 0) != 1:
            invalid_reasons.append("mapping_not_prospective")
        if int(mapping_dict.get("input_listing_bootstrap") or 0) != 0:
            invalid_reasons.append("mapping_listing_bootstrap")
        if int(mapping_dict.get("input_publisher_time_eligible") or 0) != 1:
            invalid_reasons.append("mapping_publisher_time_ineligible")
        if int(mapping_dict.get("research_only") or 0) != 1 or any(
            int(mapping_dict.get(key) or 0) != 0
            for key in ("execution_eligible", "can_authorize", "can_promote")
        ):
            invalid_reasons.append("mapping_safety_boundary_invalid")
    observation_id = str(capture_dict.get("observation_id") or "")
    if (
        not observation_id
        or str(source_dict.get("observation_id") or "") != observation_id
        or str(mapping_dict.get("observation_id") or "") != observation_id
    ):
        invalid_reasons.append("observation_lineage_mismatch")
    source_id = str(
        source_dict.get("source_id") or source_payload.get("source_id") or ""
    )
    source_contract_id = str(source_dict.get("source_contract_id") or "")
    source_cohort_id = str(source_dict.get("source_cohort_id") or "")
    if (
        not source_contract_id
        or not source_cohort_id
        or source_lineages.get(source_id)
        != (source_contract_id, source_cohort_id)
    ):
        invalid_reasons.append("raw_source_contract_or_cohort_mismatch")
    if source_id and str(mapping_dict.get("source_id") or "") != source_id:
        invalid_reasons.append("source_lineage_mismatch")
    if str(mapping_dict.get("source_contract_id") or "") != source_contract_id:
        invalid_reasons.append("mapping_source_contract_lineage_mismatch")
    if source_id and str(source_payload.get("source_id") or "") != source_id:
        invalid_reasons.append("source_payload_identity_mismatch")
    if mapping_payload and str(mapping_payload.get("fast_lane_observation_id") or "") != observation_id:
        invalid_reasons.append("mapping_payload_observation_lineage_mismatch")
    if mapping_payload and str(mapping_payload.get("source_id") or "") != source_id:
        invalid_reasons.append("mapping_payload_source_lineage_mismatch")
    if (
        mapping_payload
        and str(mapping_payload.get("classification_version") or "")
        != CLASSIFICATION_VERSION
    ):
        invalid_reasons.append("mapping_payload_classification_version_mismatch")
    if not issuer:
        invalid_reasons.append("single_issuer_currency_binding_failed")
    if (
        not copied_authority_raw
        or sha256_bytes(copied_authority_raw) != AUTHORITY_MAP_SHA256
        or copied_authority.get("contract_id") != AUTHORITY_MAP_CONTRACT_ID
    ):
        invalid_reasons.append("source_authority_map_contract_or_hash_mismatch")
    if not selected:
        invalid_reasons.append(
            "issuer_pair_entry_quote_missing" if issuer else "issuer_pair_unavailable"
        )
    entry_clock = parse_time(expected_quote_clock)
    if entry:
        if entry_clock is None or captured_at is None:
            invalid_reasons.append("entry_quote_clock_missing")
        elif entry_clock > captured_at or entry_clock > event + dt.timedelta(seconds=15):
            invalid_reasons.append("entry_quote_clock_outside_t0_capture")
    scores = mapping_payload.get("currency_scores")
    scores = scores if isinstance(scores, Mapping) else {}
    score = finite_number(scores.get(issuer)) if issuer else None
    semantic_direction_available = int(
        mapping_dict.get("semantic_direction_available") or 0
    )
    if score is not None and score != 0.0 and semantic_direction_available != 1:
        invalid_reasons.append("mapping_score_without_semantic_direction_flag")
    expected_invalid_reason = ";".join(dict.fromkeys(invalid_reasons))
    expected_decision_state = "invalid" if invalid_reasons else "sealed"
    record(
        failures,
        decision["decision_state"] == expected_decision_state,
        f"{label}:decision_state_reconstruction",
    )
    record(
        failures,
        decision["invalid_reason"] == expected_invalid_reason,
        f"{label}:invalid_reason_reconstruction",
    )

    official_side = (
        _pair_side(selected, issuer, score)
        if semantic_direction_available == 1
        else ""
    )
    record(
        failures,
        same_number(decision["official_currency_score"], score),
        f"{label}:official_score",
    )
    record(
        failures,
        decision["official_pair_side"] == official_side,
        f"{label}:official_side",
    )
    technical_side, technical_reason = _verify_technical(
        decision, event, selected, failures
    )
    record(
        failures,
        decision["technical_pair_side"] == technical_side,
        f"{label}:technical_side",
    )
    record(
        failures,
        decision["technical_reason"] == technical_reason,
        f"{label}:technical_reason",
    )
    record(
        failures,
        decision["technical_state"] == ("trade" if technical_side else "abstain"),
        f"{label}:technical_state",
    )
    episode = _episode_id(issuer, event) if issuer else ""
    record(failures, decision["market_episode_id"] == episode, f"{label}:episode")

    schedule = [
        {"arm_name": arm, "horizon_min": horizon, "slippage_stress_pips": slip}
        for arm in ARMS
        for horizon in HORIZONS_MIN
        for slip in SLIPPAGE_STRESS_PIPS
    ]
    schedule_json = canonical_json(schedule)
    record(
        failures,
        decision["outcome_schedule_json"] == schedule_json,
        f"{label}:schedule_grid",
    )
    record(
        failures,
        decision["outcome_schedule_sha256"] == sha256_text(schedule_json),
        f"{label}:schedule_sha",
    )

    arms = output.execute(
        "SELECT * FROM paired_event_arm WHERE decision_id=? ORDER BY arm_name",
        (decision["decision_id"],),
    ).fetchall()
    record(failures, len(arms) == len(ARMS), f"{label}:arm_count")
    by_name = {str(row["arm_name"]): row for row in arms}
    record(failures, set(by_name) == set(ARMS), f"{label}:arm_grid")
    expected_arms = _expected_arms(
        decision, official_side, technical_side, technical_reason
    )
    for name in ARMS:
        arm = by_name.get(name)
        if arm is None:
            continue
        arm_label = f"{label}:arm:{name}"
        _safety(arm, arm_label, failures)
        expected_arm_id = "paired_event_arm_" + sha256_text(
            f"{decision['decision_id']}|{name}|{CONTRACT_ID}|{COHORT_ID}"
        )[:32]
        record(failures, arm["arm_id"] == expected_arm_id, f"{arm_label}:identity")
        for field in ("action_state", "side", "reason"):
            record(
                failures,
                arm[field] == expected_arms[name][field],
                f"{arm_label}:{field}",
            )
        expected_factor = _factor_id(selected, str(arm["side"]), issuer)
        record(
            failures,
            arm["signed_currency_factor_id"] == expected_factor,
            f"{arm_label}:issuer_factor_dedupe",
        )
        record(
            failures,
            arm["market_episode_id"] == episode,
            f"{arm_label}:episode",
        )


def _expected_horizon_input(
    decision: sqlite3.Row,
    capture: sqlite3.Row,
    quote: sqlite3.Row | None,
) -> tuple[str, str, bytes, bytes, bytes, bytes, dict[str, Any]]:
    capture_dict = dict(capture)
    quote_dict = dict(quote) if quote is not None else {}
    header_bytes = str(capture["payload_json"] or "").encode("utf-8")
    quote_bytes = (
        str(quote["payload_json"] or "").encode("utf-8") if quote is not None else b""
    )
    capture_row_bytes = canonical_json(capture_dict).encode("utf-8")
    quote_row_bytes = canonical_json(quote_dict).encode("utf-8")
    try:
        payload = json.loads(header_bytes.decode("utf-8")) if header_bytes else {}
    except ValueError:
        payload = {}
    try:
        quote_payload = json.loads(quote_bytes.decode("utf-8")) if quote_bytes else {}
    except ValueError:
        quote_payload = {}
    reasons: list[str] = []
    if capture["contract_id"] != HORIZON_CONTRACT_ID or capture["cohort_id"] != HORIZON_COHORT_ID:
        reasons.append("horizon_contract_or_cohort_mismatch")
    if int(capture["research_only"] or 0) != 1 or any(
        int(capture[key] or 0) != 0
        for key in ("execution_eligible", "can_place_orders", "can_authorize", "can_promote")
    ):
        reasons.append("horizon_safety_boundary_invalid")
    if sha256_bytes(header_bytes) != str(capture["payload_sha256"] or ""):
        reasons.append("horizon_payload_hash_mismatch")
    if capture["input_capture_payload_sha256"] != decision["input_capture_sha256"]:
        reasons.append("entry_capture_hash_drift")
    if capture["input_capture_id"] != decision["input_capture_id"]:
        reasons.append("horizon_entry_lineage_mismatch")
    if (
        capture["observation_id"] != decision["observation_id"]
        or parse_time(capture["event_first_known_utc"])
        != parse_time(decision["event_first_known_utc"])
    ):
        reasons.append("horizon_row_decision_lineage_mismatch")
    if capture["timing_quality"] != "prospective_exact_live_quote" or int(capture["proof_quote_count"] or 0) != EXPECTED_INSTRUMENT_COUNT:
        reasons.append("horizon_capture_not_proof_valid")
    horizon_min = int(capture["horizon_min"] or 0)
    decision_event = parse_time(decision["event_first_known_utc"])
    expected_target = (
        decision_event + dt.timedelta(minutes=horizon_min)
        if decision_event is not None and horizon_min in HORIZONS_MIN
        else None
    )
    read_started = parse_time(capture["capture_read_started_utc"])
    attempted = parse_time(capture["attempted_utc"])
    recorded_delay = finite_number(capture["attempt_delay_sec"])
    derived_delay = (
        (attempted - expected_target).total_seconds()
        if attempted is not None and expected_target is not None
        else None
    )
    snapshot_generated = parse_time(capture["quote_snapshot_generated_utc"])
    snapshot_age = (
        (attempted - snapshot_generated).total_seconds()
        if attempted is not None and snapshot_generated is not None
        else None
    )
    if (
        expected_target is None
        or read_started is None
        or attempted is None
        or read_started < expected_target
        or read_started > attempted
        or derived_delay is None
        or not 0.0 <= derived_delay <= HORIZON_MAXIMUM_ATTEMPT_DELAY_SEC
        or not same_number(recorded_delay, derived_delay, 1e-6)
    ):
        reasons.append("horizon_capture_attempt_clock_invalid")
    if (
        snapshot_age is None
        or snapshot_age < -HORIZON_MAXIMUM_FUTURE_SKEW_SEC
        or snapshot_age > HORIZON_MAXIMUM_SNAPSHOT_AGE_SEC
    ):
        reasons.append("horizon_quote_snapshot_clock_invalid")
    if (
        str(payload.get("capture_id") or "") != str(capture["capture_id"])
        or str(payload.get("input_capture_id") or "") != str(decision["input_capture_id"])
        or str(payload.get("observation_id") or "") != str(decision["observation_id"])
        or parse_time(payload.get("event_first_known_utc")) != decision_event
        or int(payload.get("horizon_min") or 0) != horizon_min
        or payload.get("timing_quality") != "prospective_exact_live_quote"
        or int(payload.get("proof_quote_count") or 0) != EXPECTED_INSTRUMENT_COUNT
        or int(payload.get("expected_instrument_count") or 0) != EXPECTED_INSTRUMENT_COUNT
        or payload.get("expected_instrument_universe_sha256") != EXPECTED_UNIVERSE_SHA256
        or payload.get("entry_capture_contract_id") != ENTRY_CONTRACT_ID
        or payload.get("entry_capture_cohort_id") != ENTRY_COHORT_ID
        or payload.get("contract_id") != HORIZON_CONTRACT_ID
        or payload.get("cohort_id") != HORIZON_COHORT_ID
        or parse_time(payload.get("capture_read_started_utc")) != read_started
        or parse_time(payload.get("attempted_utc")) != attempted
        or not same_number(payload.get("attempt_delay_sec"), recorded_delay)
        or parse_time(payload.get("quote_snapshot_generated_utc")) != snapshot_generated
        or str(payload.get("quote_snapshot_sha256") or "")
        != str(capture["quote_snapshot_sha256"] or "")
        or str(payload.get("component_root_sha256") or "")
        != str(capture["component_root_sha256"] or "")
        or int(payload.get("connection_generation") or 0)
        != int(capture["connection_generation"] or 0)
        or payload.get("research_only") is not True
        or payload.get("execution_eligible") is not False
        or payload.get("can_place_orders") is not False
        or payload.get("can_authorize") is not False
        or payload.get("can_promote") is not False
    ):
        reasons.append("horizon_row_payload_lineage_mismatch")
    expected_capture_id = "official_event_horizon_" + sha256_text(
        f"{decision['input_capture_id']}|{horizon_min}|"
        f"{HORIZON_CONTRACT_ID}|{HORIZON_COHORT_ID}"
    )[:32]
    if capture["capture_id"] != expected_capture_id:
        reasons.append("horizon_capture_id_derivation_mismatch")
    if (
        expected_target is None
        or parse_time(capture["target_utc"]) != expected_target
        or parse_time(payload.get("target_utc")) != expected_target
    ):
        reasons.append("horizon_target_clock_mismatch")
    payload_quotes = payload.get("quotes")
    payload_quotes = payload_quotes if isinstance(payload_quotes, Mapping) else {}
    snapshot_material = payload.get("quote_snapshot_material")
    snapshot_material = snapshot_material if isinstance(snapshot_material, Mapping) else {}
    if sha256_text(canonical_json(snapshot_material)) != str(capture["quote_snapshot_sha256"] or ""):
        reasons.append("horizon_quote_snapshot_hash_mismatch")
    component_hashes = [
        sha256_text(canonical_json(payload_quotes[instrument]))
        for instrument in sorted(payload_quotes)
    ]
    if sha256_text("".join(component_hashes)) != str(capture["component_root_sha256"] or ""):
        reasons.append("horizon_component_root_mismatch")
    if set(payload_quotes) != set(EXPECTED_INSTRUMENTS):
        reasons.append("horizon_quote_universe_mismatch")
    invalid_components = 0
    for expected_instrument in EXPECTED_INSTRUMENTS:
        component = payload_quotes.get(expected_instrument)
        if not isinstance(component, Mapping):
            invalid_components += 1
            continue
        bid = finite_number(component.get("bid"))
        ask = finite_number(component.get("ask"))
        pip = finite_number(component.get("pip"))
        clock = parse_time(component.get("quote_utc"))
        derived_offset = (
            (clock - expected_target).total_seconds()
            if clock is not None and expected_target is not None
            else None
        )
        derived_age = (
            (attempted - clock).total_seconds()
            if attempted is not None and clock is not None
            else None
        )
        if (
            str(component.get("instrument") or "") != expected_instrument
            or bid is None or ask is None or pip is None
            or bid <= 0.0 or ask <= bid or pip <= 0.0
            or derived_offset is None
            or abs(derived_offset) > HORIZON_MAXIMUM_TARGET_OFFSET_SEC
            or not same_number(component.get("target_offset_sec"), derived_offset, 1e-6)
            or derived_age is None
            or derived_age < -HORIZON_MAXIMUM_FUTURE_SKEW_SEC
            or derived_age > HORIZON_MAXIMUM_QUOTE_AGE_SEC
            or not same_number(component.get("quote_age_sec"), derived_age, 1e-6)
            or not str(component.get("source") or "").strip()
        ):
            invalid_components += 1
    if invalid_components:
        reasons.append(f"horizon_component_validation_failed:{invalid_components}")
    instrument = str(decision["selected_instrument"] or "")
    if quote is None or not quote_payload:
        reasons.append("selected_exit_component_missing")
    else:
        if quote["capture_id"] != capture["capture_id"]:
            reasons.append("selected_exit_capture_lineage_mismatch")
        if quote["instrument"] != instrument:
            reasons.append("selected_exit_instrument_mismatch")
        if int(quote["horizon_min"] or 0) != horizon_min:
            reasons.append("selected_exit_horizon_mismatch")
        if (
            quote["contract_id"] != HORIZON_CONTRACT_ID
            or quote["cohort_id"] != HORIZON_COHORT_ID
            or int(quote["research_only"] or 0) != 1
            or any(int(quote[key] or 0) != 0 for key in (
                "execution_eligible", "can_place_orders", "can_authorize", "can_promote"
            ))
        ):
            reasons.append("selected_exit_row_safety_or_lineage_invalid")
        if sha256_bytes(quote_bytes) != quote["payload_sha256"]:
            reasons.append("selected_exit_component_hash_mismatch")
        expected_quote_id = "official_event_horizon_quote_" + sha256_text(
            f"{capture['capture_id']}|{instrument}|{sha256_bytes(quote_bytes)}"
        )[:32]
        if quote["quote_id"] != expected_quote_id:
            reasons.append("selected_exit_quote_id_derivation_mismatch")
        expected_component = payload_quotes.get(instrument)
        if not isinstance(expected_component, Mapping) or canonical_json(expected_component).encode("utf-8") != quote_bytes:
            reasons.append("horizon_header_component_mismatch")
        entry_pip = finite_number(decision["entry_pip"])
        exit_pip = finite_number(quote_payload.get("pip"))
        if (
            entry_pip is None
            or exit_pip is None
            or entry_pip <= 0.0
            or exit_pip <= 0.0
            or exit_pip != entry_pip
        ):
            reasons.append("selected_exit_pip_mismatch")
        for row_field, payload_field in (
            ("bid", "bid"), ("ask", "ask"), ("pip", "pip"),
            ("quote_age_sec", "quote_age_sec"),
            ("target_offset_sec", "target_offset_sec"),
            ("spread_pips", "spread_pips"),
        ):
            if not same_number(quote[row_field], quote_payload.get(payload_field)):
                reasons.append("selected_exit_row_payload_value_mismatch")
                break
        if (
            parse_time(quote["quote_utc"]) != parse_time(quote_payload.get("quote_utc"))
            or str(quote["source"] or "") != str(quote_payload.get("source") or "")
            or int(quote["connection_generation"] or 0)
            != int(quote_payload.get("connection_generation") or 0)
        ):
            reasons.append("selected_exit_row_payload_value_mismatch")
        exit_bid = finite_number(quote_payload.get("bid"))
        exit_ask = finite_number(quote_payload.get("ask"))
        exit_clock = parse_time(quote_payload.get("quote_utc"))
        if exit_bid is None or exit_ask is None or exit_bid <= 0.0 or exit_ask <= exit_bid:
            reasons.append("selected_exit_bid_ask_invalid")
        if (
            expected_target is None
            or exit_clock is None
            or abs((exit_clock - expected_target).total_seconds())
            > HORIZON_MAXIMUM_TARGET_OFFSET_SEC
        ):
            reasons.append("selected_exit_quote_clock_invalid")
    return (
        "invalid" if reasons else "valid",
        ";".join(dict.fromkeys(reasons)),
        header_bytes,
        quote_bytes,
        capture_row_bytes,
        quote_row_bytes,
        quote_payload,
    )


def _verify_horizon_and_outcomes(
    output: sqlite3.Connection,
    horizon_db: sqlite3.Connection,
    decision: sqlite3.Row,
    horizon_input: sqlite3.Row,
    failures: list[str],
) -> None:
    label = f"decision:{decision['decision_id']}:horizon:{horizon_input['horizon_min']}"
    _safety(horizon_input, label, failures)
    capture = horizon_db.execute(
        "SELECT * FROM official_event_horizon_capture WHERE capture_id=?",
        (horizon_input["input_horizon_capture_id"],),
    ).fetchone()
    record(failures, capture is not None, f"{label}:upstream_capture_missing")
    if capture is None:
        return
    quote = horizon_db.execute(
        "SELECT * FROM official_event_horizon_quote WHERE capture_id=? AND instrument=?",
        (capture["capture_id"], decision["selected_instrument"]),
    ).fetchone()
    (
        state,
        reason,
        header_bytes,
        quote_bytes,
        capture_row_bytes,
        quote_row_bytes,
        quote_payload,
    ) = _expected_horizon_input(decision, capture, quote)
    expected_id = "paired_event_horizon_input_" + sha256_text(
        f"{decision['decision_id']}|{capture['horizon_min']}|{CONTRACT_ID}|{COHORT_ID}"
    )[:32]
    record(failures, horizon_input["horizon_input_id"] == expected_id, f"{label}:identity")
    record(failures, horizon_input["decision_id"] == decision["decision_id"], f"{label}:decision_link")
    record(failures, horizon_input["horizon_min"] == capture["horizon_min"], f"{label}:horizon")
    record(failures, horizon_input["input_state"] == state, f"{label}:state")
    record(failures, horizon_input["invalid_reason"] == reason, f"{label}:reason")
    record(failures, bytes(horizon_input["horizon_payload_bytes"]) == header_bytes, f"{label}:header_bytes")
    record(failures, horizon_input["horizon_payload_sha256"] == sha256_bytes(header_bytes), f"{label}:header_sha")
    record(failures, bytes(horizon_input["horizon_row_bytes"]) == capture_row_bytes, f"{label}:horizon_row_bytes")
    record(failures, horizon_input["horizon_row_sha256"] == sha256_bytes(capture_row_bytes), f"{label}:horizon_row_sha")
    record(failures, bytes(horizon_input["exit_component_bytes"]) == quote_bytes, f"{label}:component_bytes")
    record(failures, horizon_input["exit_component_sha256"] == sha256_bytes(quote_bytes), f"{label}:component_sha")
    record(failures, bytes(horizon_input["exit_component_row_bytes"]) == quote_row_bytes, f"{label}:component_row_bytes")
    record(failures, horizon_input["exit_component_row_sha256"] == sha256_bytes(quote_row_bytes), f"{label}:component_row_sha")
    for field, key in (("exit_bid", "bid"), ("exit_ask", "ask"), ("exit_pip", "pip")):
        record(failures, same_number(horizon_input[field], finite_number(quote_payload.get(key)) if quote_payload else None), f"{label}:{field}")
    record(failures, horizon_input["exit_quote_utc"] == str(quote_payload.get("quote_utc") or ""), f"{label}:exit_clock")

    arms = {
        str(row["arm_name"]): row
        for row in output.execute(
            "SELECT * FROM paired_event_arm WHERE decision_id=?",
            (decision["decision_id"],),
        ).fetchall()
    }
    outcomes = output.execute(
        "SELECT * FROM paired_event_outcome WHERE horizon_input_id=?",
        (horizon_input["horizon_input_id"],),
    ).fetchall()
    record(
        failures,
        len(outcomes) == len(ARMS) * len(SLIPPAGE_STRESS_PIPS),
        f"{label}:outcome_count",
    )
    by_key = {
        (str(row["arm_name"]), float(row["slippage_stress_pips"])): row
        for row in outcomes
    }
    expected_grid = {
        (arm, slip) for arm in ARMS for slip in SLIPPAGE_STRESS_PIPS
    }
    record(failures, set(by_key) == expected_grid, f"{label}:outcome_grid")
    entry_bid = finite_number(decision["entry_bid"])
    entry_ask = finite_number(decision["entry_ask"])
    pip = finite_number(decision["entry_pip"])
    exit_bid = finite_number(horizon_input["exit_bid"])
    exit_ask = finite_number(horizon_input["exit_ask"])
    for key in sorted(expected_grid):
        arm_name, slip = key
        row = by_key.get(key)
        arm = arms.get(arm_name)
        if row is None or arm is None:
            continue
        out_label = f"{label}:outcome:{arm_name}:{slip:.2f}"
        _safety(row, out_label, failures)
        expected_id = "paired_event_outcome_" + sha256_text(
            f"{decision['decision_id']}|{arm_name}|{horizon_input['horizon_min']}|{slip:.2f}|{CONTRACT_ID}|{COHORT_ID}"
        )[:32]
        record(failures, row["outcome_id"] == expected_id, f"{out_label}:identity")
        record(failures, row["decision_id"] == decision["decision_id"], f"{out_label}:decision_link")
        record(failures, row["arm_id"] == arm["arm_id"], f"{out_label}:arm_link")
        expected_state = str(arm["action_state"])
        expected_reason = str(arm["reason"])
        gross = spread_cost = net = None
        if arm_name == "no_trade" or expected_state == "abstain":
            expected_state, gross, spread_cost, net = "abstain", 0.0, 0.0, 0.0
        elif expected_state == "invalid":
            expected_reason = expected_reason or "decision_arm_invalid"
        elif state != "valid":
            expected_state, expected_reason = "invalid", reason or "horizon_input_invalid"
        elif None in {entry_bid, entry_ask, pip, exit_bid, exit_ask} or pip is None or pip <= 0.0:
            expected_state, expected_reason = "invalid", "executable_quote_economics_invalid"
        else:
            assert entry_bid is not None and entry_ask is not None
            assert exit_bid is not None and exit_ask is not None and pip is not None
            entry_mid = (entry_bid + entry_ask) / 2.0
            exit_mid = (exit_bid + exit_ask) / 2.0
            if arm["side"] == "buy":
                gross = (exit_bid - entry_ask) / pip
                signed_mid = (exit_mid - entry_mid) / pip
            elif arm["side"] == "sell":
                gross = (entry_bid - exit_ask) / pip
                signed_mid = (entry_mid - exit_mid) / pip
            else:
                expected_state, expected_reason = "invalid", "trade_arm_side_invalid"
                signed_mid = 0.0
            if expected_state == "trade":
                spread_cost = max(0.0, signed_mid - gross)
                net = gross - slip
        record(failures, row["outcome_state"] == expected_state, f"{out_label}:state")
        record(failures, row["outcome_reason"] == expected_reason, f"{out_label}:reason")
        record(failures, same_number(row["gross_executable_pips"], gross), f"{out_label}:gross")
        record(failures, same_number(row["spread_cost_pips"], spread_cost), f"{out_label}:spread_descriptive_only")
        record(failures, same_number(row["net_after_cost_pips"], net), f"{out_label}:slippage_once")


def verify_paired_evaluator_database(
    *,
    output_database: Path = OUTPUT_DATABASE,
    release_database: Path = RELEASE_DATABASE,
    mapping_database: Path = MAPPING_DATABASE,
    horizon_database: Path = HORIZON_DATABASE,
    config_path: Path = CONFIG_PATH,
    producer_source_path: Path = PRODUCER_SOURCE_PATH,
    authority_map_path: Path = AUTHORITY_MAP_PATH,
    news_source_config_path: Path = NEWS_SOURCE_CONFIG_PATH,
    fast_lane_source_path: Path = FAST_LANE_SOURCE_PATH,
    horizon_source_path: Path = HORIZON_SOURCE_PATH,
    observed_utc: dt.datetime | None = None,
) -> dict[str, Any]:
    """Verify the paired experiment without mutating or importing its producer."""

    failures: list[str] = []
    config_bytes = _verify_config(config_path, failures)
    try:
        producer_raw = producer_source_path.read_bytes()
        producer_file_sha = sha256_bytes(producer_raw)
        producer_source_sha = normalized_producer_source_sha256(producer_raw)
    except OSError as exc:
        failures.append(f"producer_source:unreadable:{type(exc).__name__}")
        producer_file_sha = ""
        producer_source_sha = ""
    record(
        failures,
        producer_file_sha == FROZEN_PRODUCER_FILE_SHA256,
        "producer_source:file_sha256:mismatch",
    )
    record(
        failures,
        producer_source_sha == FROZEN_PRODUCER_SOURCE_SHA256,
        "producer_source:normalized_sha256:mismatch",
    )
    authority_bytes, authority_payload = _read_authority_map(
        authority_map_path, failures
    )
    news_source_bytes, source_lineages = _read_news_source_config(
        news_source_config_path, failures
    )
    dependency_hashes: dict[str, str] = {}
    for name, path, expected in (
        ("fast_lane", fast_lane_source_path, FAST_LANE_SOURCE_SHA256),
        ("horizon", horizon_source_path, HORIZON_SOURCE_SHA256),
    ):
        try:
            observed = sha256_bytes(path.read_bytes())
        except OSError as exc:
            failures.append(f"dependency:{name}:unreadable:{type(exc).__name__}")
            observed = ""
        dependency_hashes[name] = observed
        record(
            failures,
            observed == expected,
            f"dependency:{name}:file_sha256:mismatch",
        )
    now = observed_utc or dt.datetime.now(dt.timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.timezone.utc)
    now = now.astimezone(dt.timezone.utc)
    counts = {
        "eligible_raw_captures": 0,
        "decisions": 0,
        "sealed_decisions": 0,
        "invalid_decisions": 0,
        "arms": 0,
        "horizon_inputs": 0,
        "outcomes": 0,
        "due_missing_decisions": 0,
        "due_missing_upstream_horizons": 0,
        "available_missing_paired_horizons": 0,
        "effective_factor_episode_groups": 0,
    }
    connections: list[sqlite3.Connection] = []
    try:
        output = open_read_only(output_database)
        release = open_read_only(release_database)
        mapping_db = open_read_only(mapping_database)
        horizon_db = open_read_only(horizon_database)
        connections.extend((output, release, mapping_db, horizon_db))
        for name, connection in (
            ("output", output),
            ("release", release),
            ("mapping", mapping_db),
            ("horizon", horizon_db),
        ):
            integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
            record(failures, integrity == "ok", f"database:{name}:integrity:{integrity}")
        _verify_schema(output, failures)
        _verify_manifest(
            output,
            config_sha=sha256_bytes(config_bytes),
            producer_source_sha=producer_source_sha,
            producer_file_sha=producer_file_sha,
            authority_sha=sha256_bytes(authority_bytes),
            news_source_sha=sha256_bytes(news_source_bytes),
            fast_lane_sha=dependency_hashes["fast_lane"],
            horizon_source_sha=dependency_hashes["horizon"],
            failures=failures,
        )

        eligible = release.execute(
            """
            SELECT capture_id,event_first_known_utc
            FROM official_release_quote_capture
            WHERE event_first_known_utc>=?
            ORDER BY event_first_known_utc,capture_id
            """,
            (ACTIVATED_UTC.isoformat(),),
        ).fetchall()
        counts["eligible_raw_captures"] = len(eligible)
        decisions = output.execute(
            "SELECT * FROM paired_event_decision ORDER BY event_first_known_utc,decision_id"
        ).fetchall()
        counts["decisions"] = len(decisions)
        counts["sealed_decisions"] = sum(
            str(row["decision_state"]) == "sealed" for row in decisions
        )
        counts["invalid_decisions"] = sum(
            str(row["decision_state"]) == "invalid" for row in decisions
        )
        decision_by_capture = {str(row["input_capture_id"]): row for row in decisions}
        for row in eligible:
            event = parse_time(row["event_first_known_utc"])
            if event is not None and now > event + dt.timedelta(seconds=SEAL_DEADLINE_OFFSET_SEC):
                if str(row["capture_id"]) not in decision_by_capture:
                    counts["due_missing_decisions"] += 1
                    failures.append(f"schedule:due_decision_missing:{row['capture_id']}")
        eligible_ids = {str(row["capture_id"]) for row in eligible}
        for decision in decisions:
            record(
                failures,
                decision["input_capture_id"] in eligible_ids,
                f"decision:{decision['decision_id']}:preactivation_or_orphan",
            )
            _verify_decision(
                output,
                release,
                mapping_db,
                decision,
                config_bytes,
                producer_source_sha,
                producer_file_sha,
                authority_bytes,
                authority_payload,
                news_source_bytes,
                source_lineages,
                dependency_hashes["fast_lane"],
                dependency_hashes["horizon"],
                failures,
            )

            upstream_horizons = horizon_db.execute(
                "SELECT * FROM official_event_horizon_capture WHERE input_capture_id=? ORDER BY horizon_min",
                (decision["input_capture_id"],),
            ).fetchall()
            paired_horizons = output.execute(
                "SELECT * FROM paired_event_horizon_input WHERE decision_id=? ORDER BY horizon_min",
                (decision["decision_id"],),
            ).fetchall()
            paired_by_horizon = {int(row["horizon_min"]): row for row in paired_horizons}
            upstream_by_horizon = {int(row["horizon_min"]): row for row in upstream_horizons}
            for horizon, upstream in upstream_by_horizon.items():
                if horizon not in paired_by_horizon:
                    counts["available_missing_paired_horizons"] += 1
                    failures.append(
                        f"decision:{decision['decision_id']}:available_horizon_omitted:{horizon}"
                    )
            for horizon, paired in paired_by_horizon.items():
                record(
                    failures,
                    horizon in upstream_by_horizon,
                    f"decision:{decision['decision_id']}:paired_horizon_without_upstream:{horizon}",
                )
                if horizon in upstream_by_horizon:
                    _verify_horizon_and_outcomes(
                        output, horizon_db, decision, paired, failures
                    )
            event = parse_time(decision["event_first_known_utc"])
            if event is not None:
                for horizon in HORIZONS_MIN:
                    deadline = event + dt.timedelta(minutes=horizon, seconds=HORIZON_TERMINAL_DELAY_SEC)
                    if now > deadline and horizon not in upstream_by_horizon:
                        counts["due_missing_upstream_horizons"] += 1
                        failures.append(
                            f"decision:{decision['decision_id']}:upstream_due_horizon_missing:{horizon}"
                        )

        counts["arms"] = int(output.execute("SELECT COUNT(*) FROM paired_event_arm").fetchone()[0])
        counts["horizon_inputs"] = int(output.execute("SELECT COUNT(*) FROM paired_event_horizon_input").fetchone()[0])
        counts["outcomes"] = int(output.execute("SELECT COUNT(*) FROM paired_event_outcome").fetchone()[0])
        counts["effective_factor_episode_groups"] = int(
            output.execute(
                """
                SELECT COUNT(*) FROM (
                  SELECT signed_currency_factor_id,market_episode_id
                  FROM paired_event_arm
                  WHERE signed_currency_factor_id<>''
                  GROUP BY signed_currency_factor_id,market_episode_id
                )
                """
            ).fetchone()[0]
        )
        duplicate_outcomes = output.execute(
            """
            SELECT decision_id,arm_name,horizon_min,slippage_stress_pips,COUNT(*)
            FROM paired_event_outcome
            GROUP BY decision_id,arm_name,horizon_min,slippage_stress_pips
            HAVING COUNT(*)<>1
            """
        ).fetchall()
        record(failures, not duplicate_outcomes, "schema:duplicate_outcome_grid")
    except (OSError, sqlite3.Error, TypeError, ValueError, KeyError) as exc:
        failures.append(f"verification_exception:{type(exc).__name__}:{exc}")
    finally:
        for connection in reversed(connections):
            connection.close()

    unique = list(dict.fromkeys(failures))
    return {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "verified": not unique,
        "status": "verified" if not unique else "failed",
        "observed_utc": now.isoformat(),
        "output_database": str(output_database),
        "release_database": str(release_database),
        "mapping_database": str(mapping_database),
        "horizon_database": str(horizon_database),
        "config_path": str(config_path),
        "producer_source_path": str(producer_source_path),
        "authority_map_path": str(authority_map_path),
        "news_source_config_path": str(news_source_config_path),
        "fast_lane_source_path": str(fast_lane_source_path),
        "horizon_source_path": str(horizon_source_path),
        "counts": counts,
        "failures": unique,
        "entry_economics_role": ENTRY_ECONOMICS_ROLE,
        "practice_latency_proof_state": "not_measured_by_this_counterfactual_cohort",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "supported_execution_decision": "no_trade",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-database", type=Path, default=OUTPUT_DATABASE)
    parser.add_argument("--release-database", type=Path, default=RELEASE_DATABASE)
    parser.add_argument("--mapping-database", type=Path, default=MAPPING_DATABASE)
    parser.add_argument("--horizon-database", type=Path, default=HORIZON_DATABASE)
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    parser.add_argument("--producer-source", type=Path, default=PRODUCER_SOURCE_PATH)
    parser.add_argument("--authority-map", type=Path, default=AUTHORITY_MAP_PATH)
    parser.add_argument("--news-source-config", type=Path, default=NEWS_SOURCE_CONFIG_PATH)
    parser.add_argument("--fast-lane-source", type=Path, default=FAST_LANE_SOURCE_PATH)
    parser.add_argument("--horizon-source", type=Path, default=HORIZON_SOURCE_PATH)
    parser.add_argument("--state", type=Path, default=STATE_PATH)
    parser.add_argument("--heartbeat", type=Path, default=HEARTBEAT_PATH)
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    latest: dict[str, Any] = {}
    while True:
        latest = verify_paired_evaluator_database(
            output_database=args.output_database,
            release_database=args.release_database,
            mapping_database=args.mapping_database,
            horizon_database=args.horizon_database,
            config_path=args.config,
            producer_source_path=args.producer_source,
            authority_map_path=args.authority_map,
            news_source_config_path=args.news_source_config,
            fast_lane_source_path=args.fast_lane_source,
            horizon_source_path=args.horizon_source,
        )
        write_json_atomic(args.state, latest)
        write_json_atomic(args.heartbeat, latest)
        print(json.dumps(latest, indent=2, sort_keys=True), flush=True)
        if args.duration_sec <= 0.0 or time.monotonic() - started >= args.duration_sec:
            break
        time.sleep(max(1.0, args.interval_sec))
    return 0 if latest.get("verified") else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["verify_paired_evaluator_database"]
