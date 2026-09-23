#!/usr/bin/env python3
"""Freeze verified source cases without converting hindsight into forecasts.

The input is a small, manually verified original-authority case register.  This
builder validates source clocks, causal availability, missing expectation
inputs, and immutable links to the movement-first episode queue.  It never
assigns a trading direction or enables execution.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sqlite3
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parent
DATABASE = ROOT / "data/oanda_training_manager/state/spike_blurb_factor_reconstruction_v1.sqlite"
CONFIG = ROOT / "config/spike_blurb_verified_source_cases_v3.json"
REPORT_ROOT = ROOT / "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/verified_source_cases_v3"
CONTRACT_ID = "spike_blurb_verified_source_cases_v3_20260820"
UPSTREAM_CONTRACT_ID = "spike_blurb_unmatched_episode_queue_v2_20260820"
FREEZE_UTC = "2026-08-20T17:00:00+00:00"
SCHEMA_VERSION = 1

IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9_]{2,127}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
ALLOWED_CLOCK_PRECISIONS = {"exact_second", "exact_minute", "date_only"}
ALLOWED_AUTHORITY_TIERS = {"primary", "secondary"}

TOP_KEYS = {
    "schema_version",
    "contract_id",
    "upstream_contract_id",
    "purpose",
    "research_only",
    "execution_eligible",
    "forecast_proof_eligible",
    "causal_driver_assignment_policy",
    "cases",
}
CASE_KEYS = {
    "case_id",
    "case_version",
    "market_episode_buckets",
    "event_currency",
    "event_family",
    "event_clock_utc",
    "event_clock_precision",
    "response_watch_eligible",
    "response_watch_eligible_from_utc",
    "factual_content_pre_entry_available",
    "direction_policy",
    "consensus_state",
    "rate_repricing_state",
    "causal_driver_assignment_state",
    "case_note",
    "documents",
    "factors",
}
DOCUMENT_KEYS = {
    "document_id",
    "source_role",
    "authority",
    "country",
    "authority_tier",
    "url",
    "document_type",
    "publication_clock_precision",
    "published_at_utc",
    "published_date",
    "event_clock_utc",
    "content_causal_at_event",
    "historical_first_seen_state",
    "verified_retrieved_utc",
}
FACTOR_KEYS = {
    "factor_id",
    "factor_version",
    "factor_family",
    "metric",
    "value_number",
    "value_text",
    "unit",
    "prior_value_number",
    "revision_delta_number",
    "direction_interpretation",
}


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def stable_id(prefix: str, *parts: object) -> str:
    return f"{prefix}_{sha256_bytes('|'.join(map(str, parts)).encode())[:24]}"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def exact_bool(value: Any, field: str) -> bool:
    if type(value) is not bool:
        raise ValueError(f"{field}_must_be_boolean")
    return value


def exact_int(value: Any, field: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{field}_must_be_integer_ge_{minimum}")
    return value


def optional_number(value: Any, field: str) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise ValueError(f"{field}_must_be_finite_number_or_null")
    return value


def require_identifier(value: Any, field: str) -> str:
    text = str(value)
    if not IDENTIFIER.fullmatch(text):
        raise ValueError(f"{field}_invalid_identifier")
    return text


def parse_utc(value: Any, field: str, *, nullable: bool = False) -> datetime | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field}_must_be_utc_string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field}_invalid_utc") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise ValueError(f"{field}_must_be_utc")
    return parsed.astimezone(timezone.utc)


def require_date(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field}_must_be_date_string")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field}_invalid_date") from exc
    return parsed.isoformat()


def require_https(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field}_must_be_https_url")
    parsed = urlparse(value)
    if parsed.scheme != "https" or not parsed.netloc:
        raise ValueError(f"{field}_must_be_https_url")
    return value


def require_exact_keys(value: dict[str, Any], expected: set[str], field: str) -> None:
    if set(value) != expected:
        missing = sorted(expected - set(value))
        extra = sorted(set(value) - expected)
        raise ValueError(f"{field}_key_mismatch:missing={missing}:extra={extra}")


def validate_document(document: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise ValueError("document_must_be_object")
    require_exact_keys(document, DOCUMENT_KEYS, "document")
    require_identifier(document["document_id"], "document_id")
    if not all(isinstance(document[field], str) and document[field] for field in (
        "source_role", "authority", "country", "document_type", "historical_first_seen_state"
    )):
        raise ValueError("document_text_field_invalid")
    tier = document["authority_tier"]
    if tier not in ALLOWED_AUTHORITY_TIERS:
        raise ValueError("authority_tier_invalid")
    require_https(document["url"], "document_url")
    precision = document["publication_clock_precision"]
    if precision not in ALLOWED_CLOCK_PRECISIONS:
        raise ValueError("publication_clock_precision_invalid")
    published = parse_utc(document["published_at_utc"], "published_at_utc", nullable=True)
    published_date = require_date(document["published_date"], "published_date")
    if precision == "date_only" and published is not None:
        raise ValueError("date_only_document_cannot_have_exact_publication_clock")
    if precision != "date_only" and published is None:
        raise ValueError("exact_clock_document_requires_publication_clock")
    if published is not None and published.date().isoformat() != published_date:
        raise ValueError("published_date_clock_mismatch")
    event_clock = parse_utc(document["event_clock_utc"], "document_event_clock_utc", nullable=True)
    content_causal = exact_bool(document["content_causal_at_event"], "content_causal_at_event")
    if tier == "secondary" and content_causal:
        raise ValueError("secondary_document_cannot_be_causal_content")
    parse_utc(document["verified_retrieved_utc"], "verified_retrieved_utc")
    return {"published": published, "event_clock": event_clock, "content_causal": content_causal}


def validate_factor(factor: dict[str, Any]) -> None:
    if not isinstance(factor, dict):
        raise ValueError("factor_must_be_object")
    require_exact_keys(factor, FACTOR_KEYS, "factor")
    require_identifier(factor["factor_id"], "factor_id")
    exact_int(factor["factor_version"], "factor_version", 1)
    for field in ("factor_family", "metric", "unit"):
        if not isinstance(factor[field], str) or not factor[field]:
            raise ValueError(f"{field}_invalid")
    number = optional_number(factor["value_number"], "value_number")
    text = factor["value_text"]
    if text is not None and (not isinstance(text, str) or not text):
        raise ValueError("value_text_invalid")
    if (number is None) == (text is None):
        raise ValueError("factor_requires_exactly_one_value_representation")
    optional_number(factor["prior_value_number"], "prior_value_number")
    optional_number(factor["revision_delta_number"], "revision_delta_number")
    if factor["direction_interpretation"] != "not_assigned":
        raise ValueError("direction_interpretation_must_remain_unassigned")


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(config, dict):
        raise ValueError("config_must_be_object")
    require_exact_keys(config, TOP_KEYS, "config")
    if config["schema_version"] != SCHEMA_VERSION:
        raise ValueError("schema_version_mismatch")
    if config["contract_id"] != CONTRACT_ID or config["upstream_contract_id"] != UPSTREAM_CONTRACT_ID:
        raise ValueError("contract_identity_mismatch")
    if not isinstance(config["purpose"], str) or not config["purpose"]:
        raise ValueError("purpose_missing")
    if not isinstance(config["causal_driver_assignment_policy"], str):
        raise ValueError("causal_driver_assignment_policy_invalid")
    if exact_bool(config["research_only"], "research_only") is not True:
        raise ValueError("research_only_must_be_true")
    if exact_bool(config["execution_eligible"], "execution_eligible") is not False:
        raise ValueError("execution_eligible_must_be_false")
    if exact_bool(config["forecast_proof_eligible"], "forecast_proof_eligible") is not False:
        raise ValueError("forecast_proof_eligible_must_be_false")
    if not isinstance(config["cases"], list) or not config["cases"]:
        raise ValueError("cases_missing")

    case_ids: set[str] = set()
    document_ids: set[str] = set()
    for case in config["cases"]:
        if not isinstance(case, dict):
            raise ValueError("case_must_be_object")
        require_exact_keys(case, CASE_KEYS, "case")
        case_id = require_identifier(case["case_id"], "case_id")
        if case_id in case_ids:
            raise ValueError("duplicate_case_id")
        case_ids.add(case_id)
        exact_int(case["case_version"], "case_version", 1)
        buckets = case["market_episode_buckets"]
        if not isinstance(buckets, list) or not buckets or len(buckets) != len(set(buckets)):
            raise ValueError("market_episode_buckets_invalid")
        for bucket in buckets:
            parse_utc(bucket, "market_episode_bucket")
        for field in (
            "event_currency", "event_family", "event_clock_precision", "direction_policy",
            "consensus_state", "rate_repricing_state", "causal_driver_assignment_state", "case_note"
        ):
            if not isinstance(case[field], str) or not case[field]:
                raise ValueError(f"{field}_invalid")
        if case["event_clock_precision"] not in ALLOWED_CLOCK_PRECISIONS:
            raise ValueError("event_clock_precision_invalid")
        event_clock = parse_utc(case["event_clock_utc"], "event_clock_utc", nullable=True)
        if case["event_clock_precision"] == "date_only" and event_clock is not None:
            raise ValueError("date_only_case_cannot_have_event_clock")
        if case["event_clock_precision"] != "date_only" and event_clock is None:
            raise ValueError("exact_case_requires_event_clock")
        watch = exact_bool(case["response_watch_eligible"], "response_watch_eligible")
        watch_clock = parse_utc(
            case["response_watch_eligible_from_utc"], "response_watch_eligible_from_utc", nullable=True
        )
        if watch != (watch_clock is not None):
            raise ValueError("watch_eligibility_clock_mismatch")
        factual = exact_bool(
            case["factual_content_pre_entry_available"], "factual_content_pre_entry_available"
        )
        if not case["direction_policy"].startswith("abstain"):
            raise ValueError("direction_policy_must_abstain")
        if case["causal_driver_assignment_state"] != "candidate_not_proven":
            raise ValueError("causal_driver_assignment_must_remain_candidate")
        documents = case["documents"]
        if not isinstance(documents, list) or not documents:
            raise ValueError("documents_missing")
        document_states: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for document in documents:
            state = validate_document(document)
            if document["document_id"] in document_ids:
                raise ValueError("duplicate_document_id")
            document_ids.add(document["document_id"])
            document_states.append((document, state))
        if watch:
            if event_clock is None or watch_clock is None:
                raise ValueError("watch_requires_exact_case_clock")
            clock_support = any(
                document["authority_tier"] == "primary"
                and state["event_clock"] is not None
                and state["event_clock"] <= watch_clock
                for document, state in document_states
            )
            if not clock_support:
                raise ValueError("watch_clock_lacks_primary_support")
        causal_primary = [
            state["published"]
            for document, state in document_states
            if document["authority_tier"] == "primary"
            and state["content_causal"]
            and state["published"] is not None
        ]
        if factual and not causal_primary:
            raise ValueError("factual_pre_entry_flag_lacks_exact_primary_content")
        if not factual and causal_primary:
            raise ValueError("exact_primary_causal_content_contradicts_false_factual_flag")
        if "release" in case["event_family"] and "missing" in case["consensus_state"]:
            if not case["direction_policy"].startswith("abstain"):
                raise ValueError("missing_consensus_requires_abstention")
        factors = case["factors"]
        if not isinstance(factors, list) or not factors:
            raise ValueError("factors_missing")
        factor_ids: set[str] = set()
        for factor in factors:
            validate_factor(factor)
            if factor["factor_id"] in factor_ids:
                raise ValueError("duplicate_factor_id_within_case")
            factor_ids.add(factor["factor_id"])
    return config


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS verified_external_source_contracts (
          contract_id TEXT PRIMARY KEY,
          upstream_contract_id TEXT NOT NULL,
          schema_version INTEGER NOT NULL,
          contract_json TEXT NOT NULL,
          builder_sha256 TEXT NOT NULL,
          config_sha256 TEXT NOT NULL,
          frozen_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS verified_external_source_cases (
          case_id TEXT PRIMARY KEY,
          contract_id TEXT NOT NULL,
          case_version INTEGER NOT NULL,
          event_currency TEXT NOT NULL,
          event_family TEXT NOT NULL,
          event_clock_utc TEXT,
          event_clock_precision TEXT NOT NULL,
          market_episode_buckets_json TEXT NOT NULL,
          response_watch_eligible INTEGER NOT NULL,
          response_watch_eligible_from_utc TEXT,
          factual_content_pre_entry_available INTEGER NOT NULL,
          direction_policy TEXT NOT NULL,
          consensus_state TEXT NOT NULL,
          rate_repricing_state TEXT NOT NULL,
          causal_driver_assignment_state TEXT NOT NULL,
          case_note TEXT NOT NULL,
          case_json TEXT NOT NULL,
          row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL,
          execution_eligible INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          FOREIGN KEY(contract_id) REFERENCES verified_external_source_contracts(contract_id)
        );
        CREATE TABLE IF NOT EXISTS verified_external_source_documents (
          document_id TEXT PRIMARY KEY,
          contract_id TEXT NOT NULL,
          case_id TEXT NOT NULL,
          authority TEXT NOT NULL,
          authority_tier TEXT NOT NULL,
          source_role TEXT NOT NULL,
          source_url TEXT NOT NULL,
          document_type TEXT NOT NULL,
          publication_clock_precision TEXT NOT NULL,
          published_at_utc TEXT,
          published_date TEXT NOT NULL,
          event_clock_utc TEXT,
          content_causal_at_event INTEGER NOT NULL,
          document_json TEXT NOT NULL,
          row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL,
          execution_eligible INTEGER NOT NULL,
          FOREIGN KEY(contract_id) REFERENCES verified_external_source_contracts(contract_id),
          FOREIGN KEY(case_id) REFERENCES verified_external_source_cases(case_id)
        );
        CREATE TABLE IF NOT EXISTS verified_external_source_factors (
          case_id TEXT NOT NULL,
          factor_id TEXT NOT NULL,
          contract_id TEXT NOT NULL,
          factor_version INTEGER NOT NULL,
          factor_family TEXT NOT NULL,
          metric TEXT NOT NULL,
          value_number REAL,
          value_text TEXT,
          unit TEXT NOT NULL,
          prior_value_number REAL,
          revision_delta_number REAL,
          direction_interpretation TEXT NOT NULL,
          factor_json TEXT NOT NULL,
          row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL,
          execution_eligible INTEGER NOT NULL,
          PRIMARY KEY(case_id, factor_id),
          FOREIGN KEY(contract_id) REFERENCES verified_external_source_contracts(contract_id),
          FOREIGN KEY(case_id) REFERENCES verified_external_source_cases(case_id)
        );
        CREATE TABLE IF NOT EXISTS verified_external_source_episode_links (
          link_id TEXT PRIMARY KEY,
          contract_id TEXT NOT NULL,
          case_id TEXT NOT NULL,
          episode_id TEXT NOT NULL,
          market_episode_15m TEXT NOT NULL,
          hindsight_factor_key TEXT NOT NULL,
          representative_instrument TEXT NOT NULL,
          representative_start_utc TEXT NOT NULL,
          representative_end_utc TEXT NOT NULL,
          representative_movement_bps REAL NOT NULL,
          representative_net_pips REAL NOT NULL,
          representative_spread_pips REAL NOT NULL,
          instrument_count INTEGER NOT NULL,
          response_watch_active_before_episode INTEGER NOT NULL,
          watch_to_episode_seconds REAL,
          source_case_role TEXT NOT NULL,
          causal_driver_assigned INTEGER NOT NULL,
          outcome_selected INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          research_only INTEGER NOT NULL,
          execution_eligible INTEGER NOT NULL,
          row_sha256 TEXT NOT NULL,
          UNIQUE(contract_id, case_id, episode_id),
          FOREIGN KEY(contract_id) REFERENCES verified_external_source_contracts(contract_id),
          FOREIGN KEY(case_id) REFERENCES verified_external_source_cases(case_id)
        );
        CREATE INDEX IF NOT EXISTS verified_source_links_episode
          ON verified_external_source_episode_links(contract_id, market_episode_15m, episode_id);
        CREATE TRIGGER IF NOT EXISTS verified_external_source_contracts_no_update
          BEFORE UPDATE ON verified_external_source_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_external_source_contracts_no_delete
          BEFORE DELETE ON verified_external_source_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_external_source_cases_no_update
          BEFORE UPDATE ON verified_external_source_cases BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_external_source_cases_no_delete
          BEFORE DELETE ON verified_external_source_cases BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_external_source_documents_no_update
          BEFORE UPDATE ON verified_external_source_documents BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_external_source_documents_no_delete
          BEFORE DELETE ON verified_external_source_documents BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_external_source_factors_no_update
          BEFORE UPDATE ON verified_external_source_factors BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_external_source_factors_no_delete
          BEFORE DELETE ON verified_external_source_factors BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_external_source_episode_links_no_update
          BEFORE UPDATE ON verified_external_source_episode_links BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_external_source_episode_links_no_delete
          BEFORE DELETE ON verified_external_source_episode_links BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def immutable_row(
    connection: sqlite3.Connection,
    table: str,
    columns: list[str],
    values: Iterable[Any],
    lookup_where: str,
    lookup_values: tuple[Any, ...],
    expected_hash: str,
) -> None:
    values_tuple = tuple(values)
    placeholders = ",".join("?" for _ in columns)
    connection.execute(
        f"INSERT OR IGNORE INTO {table} ({','.join(columns)}) VALUES ({placeholders})", values_tuple
    )
    row = connection.execute(
        f"SELECT row_sha256 FROM {table} WHERE {lookup_where}", lookup_values
    ).fetchone()
    if row is None or row[0] != expected_hash:
        raise RuntimeError(f"immutable_row_conflict:{table}:{lookup_values}")


def load_episodes(connection: sqlite3.Connection, buckets: list[str]) -> list[sqlite3.Row]:
    placeholders = ",".join("?" for _ in buckets)
    rows = list(connection.execute(
        f"""
        SELECT episode_id,market_episode_15m,hindsight_factor_key,
               representative_instrument,representative_start_utc,representative_end_utc,
               representative_movement_bps,representative_net_pips,representative_spread_pips,
               instrument_count
        FROM unmatched_research_episodes
        WHERE contract_id=? AND market_episode_15m IN ({placeholders})
        ORDER BY market_episode_15m,priority_rank
        """,
        (UPSTREAM_CONTRACT_ID, *buckets),
    ))
    found = {str(row["market_episode_15m"]) for row in rows}
    if found != set(buckets):
        raise RuntimeError(f"upstream_episode_bucket_missing:{sorted(set(buckets)-found)}")
    return rows


def build_verified_cases(
    database: Path = DATABASE,
    config_path: Path = CONFIG,
    report_root: Path = REPORT_ROOT,
) -> dict[str, Any]:
    raw_config = json.loads(config_path.read_text(encoding="utf-8"))
    config = validate_config(raw_config)
    builder_hash = sha256_file(Path(__file__).resolve())
    config_hash = sha256_file(config_path)
    if not SHA256.fullmatch(builder_hash) or not SHA256.fullmatch(config_hash):
        raise RuntimeError("artifact_hash_invalid")

    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    ensure_schema(connection)
    connection.commit()
    contract_payload = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "upstream_contract_id": UPSTREAM_CONTRACT_ID,
        "builder_sha256": builder_hash,
        "config_sha256": config_hash,
        "freeze_utc": FREEZE_UTC,
        "case_count": len(config["cases"]),
        "research_only": True,
        "execution_eligible": False,
        "forecast_proof_eligible": False,
        "direction_assignment": "none",
        "causal_driver_assignment": "none",
    }
    contract_json = canonical_json(contract_payload)
    existing = connection.execute(
        "SELECT contract_json,builder_sha256,config_sha256 FROM verified_external_source_contracts WHERE contract_id=?",
        (CONTRACT_ID,),
    ).fetchone()
    if existing is not None and tuple(existing) != (contract_json, builder_hash, config_hash):
        raise RuntimeError("immutable_contract_conflict")

    connection.execute("BEGIN IMMEDIATE")
    if existing is None:
        connection.execute(
            "INSERT INTO verified_external_source_contracts VALUES (?,?,?,?,?,?,?)",
            (CONTRACT_ID, UPSTREAM_CONTRACT_ID, SCHEMA_VERSION, contract_json, builder_hash, config_hash, FREEZE_UTC),
        )

    case_summaries: list[dict[str, Any]] = []
    all_row_hashes: list[str] = []
    for case in config["cases"]:
        episodes = load_episodes(connection, list(case["market_episode_buckets"]))
        starts = [parse_utc(row["representative_start_utc"], "representative_start_utc") for row in episodes]
        assert all(start is not None for start in starts)
        earliest_start = min(start for start in starts if start is not None)
        watch_clock = parse_utc(
            case["response_watch_eligible_from_utc"], "response_watch_eligible_from_utc", nullable=True
        )
        if case["response_watch_eligible"] and (watch_clock is None or watch_clock > earliest_start):
            raise RuntimeError(f"watch_not_available_before_episode:{case['case_id']}")
        exact_primary_causal = []
        for document in case["documents"]:
            published = parse_utc(document["published_at_utc"], "published_at_utc", nullable=True)
            if (
                document["authority_tier"] == "primary"
                and document["content_causal_at_event"]
                and published is not None
                and published <= earliest_start
            ):
                exact_primary_causal.append(document["document_id"])
        if case["factual_content_pre_entry_available"] != bool(exact_primary_causal):
            raise RuntimeError(f"pre_entry_content_availability_mismatch:{case['case_id']}")

        case_json = canonical_json(case)
        case_hash = sha256_bytes(case_json.encode())
        immutable_row(
            connection,
            "verified_external_source_cases",
            [
                "case_id", "contract_id", "case_version", "event_currency", "event_family",
                "event_clock_utc", "event_clock_precision", "market_episode_buckets_json",
                "response_watch_eligible", "response_watch_eligible_from_utc",
                "factual_content_pre_entry_available", "direction_policy", "consensus_state",
                "rate_repricing_state", "causal_driver_assignment_state", "case_note", "case_json",
                "row_sha256", "research_only", "execution_eligible", "forecast_proof_eligible"
            ],
            [
                case["case_id"], CONTRACT_ID, case["case_version"], case["event_currency"],
                case["event_family"], case["event_clock_utc"], case["event_clock_precision"],
                canonical_json(case["market_episode_buckets"]), int(case["response_watch_eligible"]),
                case["response_watch_eligible_from_utc"], int(case["factual_content_pre_entry_available"]),
                case["direction_policy"], case["consensus_state"], case["rate_repricing_state"],
                case["causal_driver_assignment_state"], case["case_note"], case_json, case_hash,
                1, 0, 0,
            ],
            "case_id=?",
            (case["case_id"],),
            case_hash,
        )
        all_row_hashes.append(case_hash)

        for document in case["documents"]:
            document_json = canonical_json(document)
            document_hash = sha256_bytes(document_json.encode())
            immutable_row(
                connection,
                "verified_external_source_documents",
                [
                    "document_id", "contract_id", "case_id", "authority", "authority_tier", "source_role",
                    "source_url", "document_type", "publication_clock_precision", "published_at_utc",
                    "published_date", "event_clock_utc", "content_causal_at_event", "document_json",
                    "row_sha256", "research_only", "execution_eligible"
                ],
                [
                    document["document_id"], CONTRACT_ID, case["case_id"], document["authority"],
                    document["authority_tier"], document["source_role"], document["url"],
                    document["document_type"], document["publication_clock_precision"],
                    document["published_at_utc"], document["published_date"], document["event_clock_utc"],
                    int(document["content_causal_at_event"]), document_json, document_hash, 1, 0,
                ],
                "document_id=?",
                (document["document_id"],),
                document_hash,
            )
            all_row_hashes.append(document_hash)

        for factor in case["factors"]:
            factor_json = canonical_json(factor)
            factor_hash = sha256_bytes(factor_json.encode())
            immutable_row(
                connection,
                "verified_external_source_factors",
                [
                    "case_id", "factor_id", "contract_id", "factor_version", "factor_family", "metric",
                    "value_number", "value_text", "unit", "prior_value_number", "revision_delta_number",
                    "direction_interpretation", "factor_json", "row_sha256", "research_only", "execution_eligible"
                ],
                [
                    case["case_id"], factor["factor_id"], CONTRACT_ID, factor["factor_version"],
                    factor["factor_family"], factor["metric"], factor["value_number"], factor["value_text"],
                    factor["unit"], factor["prior_value_number"], factor["revision_delta_number"],
                    factor["direction_interpretation"], factor_json, factor_hash, 1, 0,
                ],
                "case_id=? AND factor_id=?",
                (case["case_id"], factor["factor_id"]),
                factor_hash,
            )
            all_row_hashes.append(factor_hash)

        delays: list[float] = []
        for episode in episodes:
            start = parse_utc(episode["representative_start_utc"], "representative_start_utc")
            assert start is not None
            delay = (start - watch_clock).total_seconds() if watch_clock is not None else None
            watch_active = int(bool(case["response_watch_eligible"] and delay is not None and delay >= 0))
            link_id = stable_id("verified_source_link", CONTRACT_ID, case["case_id"], episode["episode_id"])
            link_payload = {
                "link_id": link_id,
                "contract_id": CONTRACT_ID,
                "case_id": case["case_id"],
                "episode_id": episode["episode_id"],
                "market_episode_15m": episode["market_episode_15m"],
                "hindsight_factor_key": episode["hindsight_factor_key"],
                "representative_instrument": episode["representative_instrument"],
                "representative_start_utc": episode["representative_start_utc"],
                "representative_end_utc": episode["representative_end_utc"],
                "representative_movement_bps": float(episode["representative_movement_bps"]),
                "representative_net_pips": float(episode["representative_net_pips"]),
                "representative_spread_pips": float(episode["representative_spread_pips"]),
                "instrument_count": int(episode["instrument_count"]),
                "response_watch_active_before_episode": watch_active,
                "watch_to_episode_seconds": delay,
                "source_case_role": "research_pointer_not_direction_label",
                "causal_driver_assigned": 0,
                "outcome_selected": 0,
                "forecast_proof_eligible": 0,
                "research_only": 1,
                "execution_eligible": 0,
            }
            link_hash = sha256_bytes(canonical_json(link_payload).encode())
            immutable_row(
                connection,
                "verified_external_source_episode_links",
                list(link_payload) + ["row_sha256"],
                list(link_payload.values()) + [link_hash],
                "link_id=?",
                (link_id,),
                link_hash,
            )
            all_row_hashes.append(link_hash)
            if delay is not None:
                delays.append(delay)

        case_summaries.append({
            "case_id": case["case_id"],
            "event_currency": case["event_currency"],
            "event_family": case["event_family"],
            "linked_episode_count": len(episodes),
            "linked_factor_keys": sorted({str(row["hindsight_factor_key"]) for row in episodes}),
            "response_watch_eligible": case["response_watch_eligible"],
            "factual_content_pre_entry_available": case["factual_content_pre_entry_available"],
            "direction_policy": case["direction_policy"],
            "consensus_state": case["consensus_state"],
            "rate_repricing_state": case["rate_repricing_state"],
            "minimum_watch_to_episode_seconds": min(delays) if delays else None,
            "maximum_watch_to_episode_seconds": max(delays) if delays else None,
            "document_count": len(case["documents"]),
            "factor_count": len(case["factors"]),
            "causal_driver_assigned": False,
        })

    connection.commit()
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    counts = {
        "case_count": int(connection.execute(
            "SELECT count(*) FROM verified_external_source_cases WHERE contract_id=?", (CONTRACT_ID,)
        ).fetchone()[0]),
        "document_count": int(connection.execute(
            "SELECT count(*) FROM verified_external_source_documents WHERE contract_id=?", (CONTRACT_ID,)
        ).fetchone()[0]),
        "factor_count": int(connection.execute(
            "SELECT count(*) FROM verified_external_source_factors WHERE contract_id=?", (CONTRACT_ID,)
        ).fetchone()[0]),
        "episode_link_count": int(connection.execute(
            "SELECT count(*) FROM verified_external_source_episode_links WHERE contract_id=?", (CONTRACT_ID,)
        ).fetchone()[0]),
    }
    execution_sum = int(connection.execute(
        """
        SELECT
          (SELECT coalesce(sum(execution_eligible),0) FROM verified_external_source_cases WHERE contract_id=?) +
          (SELECT coalesce(sum(execution_eligible),0) FROM verified_external_source_documents WHERE contract_id=?) +
          (SELECT coalesce(sum(execution_eligible),0) FROM verified_external_source_factors WHERE contract_id=?) +
          (SELECT coalesce(sum(execution_eligible),0) FROM verified_external_source_episode_links WHERE contract_id=?)
        """, (CONTRACT_ID, CONTRACT_ID, CONTRACT_ID, CONTRACT_ID)
    ).fetchone()[0])
    connection.close()
    if counts != {"case_count": 3, "document_count": 4, "factor_count": 14, "episode_link_count": 5}:
        raise RuntimeError(f"unexpected_materialized_counts:{counts}")
    if execution_sum != 0:
        raise RuntimeError("execution_surface_must_be_zero")

    snapshot_payload = {
        "contract": contract_payload,
        "row_sha256": sorted(all_row_hashes),
        "counts": counts,
    }
    result = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "upstream_contract_id": UPSTREAM_CONTRACT_ID,
        "generated_utc": utc_now(),
        **counts,
        "exact_watch_case_count": sum(int(row["response_watch_eligible"]) for row in case_summaries),
        "pre_entry_factual_case_count": sum(
            int(row["factual_content_pre_entry_available"]) for row in case_summaries
        ),
        "direction_eligible_case_count": 0,
        "causal_driver_assignment_count": 0,
        "forecast_proof_eligible_count": 0,
        "execution_eligible_count": execution_sum,
        "case_summaries": case_summaries,
        "consensus_states": dict(Counter(row["consensus_state"] for row in case_summaries)),
        "rate_repricing_states": dict(Counter(row["rate_repricing_state"] for row in case_summaries)),
        "snapshot_sha256": sha256_bytes(canonical_json(snapshot_payload).encode()),
        "sqlite_integrity": integrity,
        "research_only": True,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    report_root.mkdir(parents=True, exist_ok=True)
    (report_root / "VERIFIED_SOURCE_CASES_V1.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    lines = [
        "# Verified external source cases V1",
        "",
        f"- Frozen source cases: **{counts['case_count']}**.",
        f"- Original/secondary documents: **{counts['document_count']}**.",
        f"- Versioned measurable factors: **{counts['factor_count']}**.",
        f"- Links to deduplicated movement episodes: **{counts['episode_link_count']}**.",
        f"- Exact event-watch cases: **{result['exact_watch_case_count']}**.",
        f"- Direction-eligible cases: **0**; causal drivers assigned: **0**.",
        "- A source case is a research pointer, not a hindsight direction label.",
        "- Execution decision: **no_trade**.",
        "",
        "## Cases",
        "",
        "| Case | Currency | Family | Links | Watch | Factual pre-entry | Direction policy |",
        "|---|---|---|---:|---|---|---|",
    ]
    for row in case_summaries:
        lines.append(
            f"| {row['case_id']} | {row['event_currency']} | {row['event_family']} | "
            f"{row['linked_episode_count']} | {str(row['response_watch_eligible']).lower()} | "
            f"{str(row['factual_content_pre_entry_available']).lower()} | {row['direction_policy']} |"
        )
    lines.extend([
        "",
        "## Knowledge-time conclusions",
        "",
        "- BOJ: exact 03:56 UTC policy release plus exact 06:30 UTC press-event clock; transcript text is ex-post.",
        "- BLS: exact 12:30 UTC numeric release; causal consensus and contemporaneous rate repricing are absent.",
        "- China tariff: primary fact is date-only; the exact 10:35:30 UTC secondary witness is after movement onset.",
        "",
    ])
    (report_root / "VERIFIED_SOURCE_CASES_V1.md").write_text("\n".join(lines), encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--report-root", type=Path, default=REPORT_ROOT)
    args = parser.parse_args()
    result = build_verified_cases(args.database, args.config, args.report_root)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
