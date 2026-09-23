#!/usr/bin/env python3
"""Capture all-68 executable quotes at the earliest durable official-event clock.

This prospective research cohort watches both immutable official-source
ledgers: the fast lane and the main local-news ledger.  Every post-activation
alias is retained with its source first-seen clock and the time this worker
actually observed the durable row.  One canonical publisher event can produce
only one terminal quote capture, at ``max(source_first_seen, row_observed)``.

The V1 fast-lane-only cohort remains immutable.  This worker reads no semantic
mapping and has no execution, lifecycle, authorization, or promotion surface.
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
import urllib.parse

from oanda_news_collector_contract import (
    NEWS_COLLECTOR_COHORT_ID,
    NEWS_COLLECTOR_CONTRACT_ID,
)
from oanda_official_release_fast_lane_contract import (
    OFFICIAL_RELEASE_FAST_LANE_COHORT_ID,
    OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID,
)
from oanda_quote_transport import load_quote_snapshot
import oanda_official_event_pair_quote_capture_v1 as quote_v1


ROOT = Path(__file__).resolve().parent
LOCAL_NEWS = ROOT / "data" / "oanda_training_manager" / "local_news_sentiment"
STATE = ROOT / "data" / "oanda_training_manager" / "state"
CONFIG_PATH = ROOT / "config" / "official_event_pair_quote_capture_v2.json"
SOURCE_MAP_PATH = ROOT / "config" / "official_central_bank_source_map_v1.json"
FAST_DATABASE = LOCAL_NEWS / "official_release_fast_lane_v4.sqlite"
MAIN_DATABASE = LOCAL_NEWS / "local_news_sentiment_v1.sqlite"
QUOTE_PATH = STATE / "practice_007_market_quotes_v1.json"
OUTPUT_DATABASE = LOCAL_NEWS / "official_event_pair_quote_capture_v2.sqlite"
SNAPSHOT_PATH = LOCAL_NEWS / "official_event_pair_quote_capture_latest_v2.json"
HEARTBEAT_PATH = LOCAL_NEWS / "official_event_pair_quote_capture_heartbeat_v2.json"

SCHEMA_VERSION = "official_event_pair_quote_capture_v2"
CONTRACT_ID = "official_event_pair_quote_capture_v2_dual_ledger_earliest_durable_20260904"
COHORT_ID = "official_event_pair_quote_capture_v2_20260904a"
ACTIVATED_UTC = dt.datetime(2026, 9, 4, 15, 30, tzinfo=dt.timezone.utc)
REQUIRED_V1_PRODUCER_SHA256 = (
    "40ceed485e6143a6388324f68a29d5786e30f28c3c2f2b79a28cbe49dabae1d0"
)
REQUIRED_SOURCE_MAP_SHA256 = (
    "09d4f80708cbed509564e2f7bc5245a24de44df3728ed0583b1d9cbb6f299a19"
)
REQUIRED_SOURCE_MAP_CONTRACT_ID = (
    "official_central_bank_source_map_v9_us_policy_communication_clocks_20260827"
)
REQUIRED_FAST_COLLECTOR_CONTRACT_ID = OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID
REQUIRED_FAST_COLLECTOR_COHORT_ID = OFFICIAL_RELEASE_FAST_LANE_COHORT_ID
REQUIRED_MAIN_COLLECTOR_CONTRACT_ID = NEWS_COLLECTOR_CONTRACT_ID
REQUIRED_MAIN_COLLECTOR_COHORT_ID = NEWS_COLLECTOR_COHORT_ID
EXPECTED_INSTRUMENTS = tuple(quote_v1.EXPECTED_INSTRUMENTS)
EXPECTED_INSTRUMENT_COUNT = quote_v1.EXPECTED_INSTRUMENT_COUNT
EXPECTED_UNIVERSE_SHA256 = quote_v1.EXPECTED_UNIVERSE_SHA256
POLICY = {
    "prospective_only": True,
    "historical_backfill_allowed": False,
    "preactivation_identity_import_allowed": False,
    "source_first_seen_and_row_observed_retained": True,
    "actionable_clock_is_max_source_first_seen_and_row_observed": True,
    "one_terminal_capture_per_canonical_event": True,
    "later_alias_cannot_backdate_or_recapture": True,
    "semantic_mapping_read_before_capture": False,
    "all_68_surface_retained_for_audit": True,
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


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_text(value: str) -> str:
    return sha256_bytes(value.encode("utf-8"))


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _file_sha256(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def source_ids_from_frozen_map(path: Path = SOURCE_MAP_PATH) -> tuple[str, ...]:
    if _file_sha256(path) != REQUIRED_SOURCE_MAP_SHA256:
        raise ValueError("official source map hash mismatch; start a new cohort")
    payload = read_json(path)
    if payload.get("contract_id") != REQUIRED_SOURCE_MAP_CONTRACT_ID:
        raise ValueError("official source map contract mismatch")
    source_ids: list[str] = []
    for row in payload.get("currencies") or []:
        if not isinstance(row, Mapping):
            continue
        for key in (
            "release_source_ids",
            "communication_source_ids",
            "statistical_release_source_ids",
        ):
            for source_id in row.get(key) or []:
                normalized = str(source_id or "").strip()
                if normalized and normalized not in source_ids:
                    source_ids.append(normalized)
    if len(source_ids) != 42:
        raise ValueError(f"official source universe mismatch:{len(source_ids)}")
    return tuple(sorted(source_ids))


def validate_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    payload = read_json(path)
    expected = {
        "schema_version": 2,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "activated_utc": iso_utc(ACTIVATED_UTC),
        "required_v1_producer_sha256": REQUIRED_V1_PRODUCER_SHA256,
        "required_source_map_sha256": REQUIRED_SOURCE_MAP_SHA256,
        "required_source_map_contract_id": REQUIRED_SOURCE_MAP_CONTRACT_ID,
        "required_fast_collector_contract_id": REQUIRED_FAST_COLLECTOR_CONTRACT_ID,
        "required_fast_collector_cohort_id": REQUIRED_FAST_COLLECTOR_COHORT_ID,
        "required_main_collector_contract_id": REQUIRED_MAIN_COLLECTOR_CONTRACT_ID,
        "required_main_collector_cohort_id": REQUIRED_MAIN_COLLECTOR_COHORT_ID,
        "expected_source_count": 42,
        "expected_instrument_count": EXPECTED_INSTRUMENT_COUNT,
        "expected_instrument_universe_sha256": EXPECTED_UNIVERSE_SHA256,
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise ValueError(f"dual-ledger capture config mismatch:{key}")
    if _file_sha256(ROOT / "oanda_official_event_pair_quote_capture_v1.py") != REQUIRED_V1_PRODUCER_SHA256:
        raise ValueError("pinned V1 quote builder hash mismatch; start a new cohort")
    if list(payload.get("source_ids") or []) != list(source_ids_from_frozen_map()):
        raise ValueError("dual-ledger capture source universe mismatch")
    configured_policy = payload.get("policy")
    configured_policy = configured_policy if isinstance(configured_policy, Mapping) else {}
    for key, value in POLICY.items():
        if configured_policy.get(key) != value:
            raise ValueError(f"dual-ledger capture config mismatch:policy.{key}")
    return payload


def canonical_url(value: Any) -> str:
    text = str(value or "").strip()
    if not text.lower().startswith(("http://", "https://")):
        return ""
    try:
        parsed = urllib.parse.urlsplit(text)
    except ValueError:
        return ""
    host = (parsed.hostname or "").lower().strip(".")
    if not host:
        return ""
    port = f":{parsed.port}" if parsed.port and parsed.port not in (80, 443) else ""
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if path != "/":
        path = path.rstrip("/")
    return urllib.parse.urlunsplit(("https", host + port, path, parsed.query, ""))


def _payload(raw_payload_json: str) -> dict[str, Any]:
    try:
        value = json.loads(str(raw_payload_json or "{}"))
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def canonical_event_identity(row: Mapping[str, Any]) -> tuple[str, str]:
    payload = _payload(str(row.get("raw_payload_json") or ""))
    external_id = str(payload.get("external_id") or "").strip()
    url = ""
    for value in (
        external_id,
        row.get("source_url"),
        payload.get("url"),
        payload.get("source_url"),
        payload.get("publisher_url"),
        payload.get("detail_source_url"),
    ):
        url = canonical_url(value)
        if url:
            break
    exact_publisher_clock = bool(row.get("publisher_time_eligible")) or not bool(
        payload.get("published_time_inferred")
    )
    published = parse_time(payload.get("published_utc"))
    if url:
        key = f"url:{url}"
        if exact_publisher_clock and published is not None:
            key += f"|published:{iso_utc(published)}"
    elif external_id:
        key = f"external:{row.get('source_id') or ''}:{external_id.lower()}"
    else:
        headline = re.sub(
            r"[^a-z0-9]+",
            " ",
            str(payload.get("headline") or payload.get("title") or row.get("headline") or "").lower(),
        ).strip()
        if not headline:
            return "", ""
        published_key = "" if published is None else iso_utc(published)
        key = f"headline:{row.get('source_id') or ''}:{headline}|published:{published_key}"
    return key, sha256_text(f"official_event_v2|{key}")


def _alias_id(ledger_name: str, upstream_observation_id: str) -> str:
    return sha256_text(
        f"{ledger_name}|{upstream_observation_id}|{CONTRACT_ID}|{COHORT_ID}"
    )


def _main_rows(database: Path, source_ids: Sequence[str], relation: str) -> list[dict[str, Any]]:
    if not database.exists():
        return []
    placeholders = ",".join("?" for _ in source_ids)
    connection = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True, timeout=10.0)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            f"""SELECT event_id,source_id,source_quality,source_verified,
                       published_utc,first_seen_utc,headline,source_url,payload_json
                FROM articles
                WHERE source_id IN ({placeholders}) AND first_seen_utc {relation} ?
                ORDER BY first_seen_utc,event_id""",
            (*source_ids, iso_utc(ACTIVATED_UTC)),
        ).fetchall()
    finally:
        connection.close()
    output = []
    for source in rows:
        payload = _payload(str(source["payload_json"] or ""))
        reasons: list[str] = []
        if int(source["source_verified"] or 0) != 1 or float(source["source_quality"] or 0) < 0.9:
            reasons.append("main_source_not_verified_high_quality")
        if payload.get("source_direct") is not True:
            reasons.append("main_source_not_direct")
        if payload.get("observation_clock_trusted") is not True:
            reasons.append("main_observation_clock_not_trusted")
        if payload.get("source_listing_bootstrap") is True:
            reasons.append("main_listing_bootstrap")
        if payload.get("collector_contract_id") != REQUIRED_MAIN_COLLECTOR_CONTRACT_ID:
            reasons.append("main_collector_contract_mismatch")
        if payload.get("collector_cohort_id") != REQUIRED_MAIN_COLLECTOR_COHORT_ID:
            reasons.append("main_collector_cohort_mismatch")
        output.append(
            {
                "ledger_name": "main_news",
                "upstream_observation_id": str(source["event_id"]),
                "source_id": str(source["source_id"]),
                "source_contract_id": str(payload.get("source_contract_id") or ""),
                "source_cohort_id": str(payload.get("source_cohort_id") or ""),
                "source_first_seen_utc": str(source["first_seen_utc"]),
                "source_url": str(source["source_url"] or ""),
                "headline": str(source["headline"] or ""),
                "publisher_time_eligible": not bool(payload.get("published_time_inferred")),
                "raw_payload_json": str(source["payload_json"] or ""),
                "ledger_collector_contract_id": str(payload.get("collector_contract_id") or ""),
                "ledger_collector_cohort_id": str(payload.get("collector_cohort_id") or ""),
                "base_ineligibility_reasons": reasons,
            }
        )
    return output


def _fast_rows(database: Path, source_ids: Sequence[str], relation: str) -> list[dict[str, Any]]:
    if not database.exists():
        return []
    placeholders = ",".join("?" for _ in source_ids)
    connection = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True, timeout=10.0)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            f"""SELECT * FROM official_release_observation
                WHERE source_id IN ({placeholders}) AND first_seen_utc {relation} ?
                ORDER BY first_seen_utc,observation_id""",
            (*source_ids, iso_utc(ACTIVATED_UTC)),
        ).fetchall()
    finally:
        connection.close()
    output = []
    for source in rows:
        payload = _payload(str(source["raw_payload_json"] or ""))
        reasons: list[str] = []
        if int(source["prospective_observation"] or 0) != 1:
            reasons.append("fast_not_prospective")
        if int(source["listing_bootstrap"] or 0) != 0:
            reasons.append("fast_listing_bootstrap")
        if int(source["identity_preexisting"] or 0) != 0:
            reasons.append("fast_identity_preexisting")
        if int(source["publisher_time_eligible"] or 0) != 1:
            reasons.append("fast_publisher_time_ineligible")
        if int(source["observation_clock_trusted"] or 0) != 1:
            reasons.append("fast_observation_clock_not_trusted")
        if source["collector_contract_id"] != REQUIRED_FAST_COLLECTOR_CONTRACT_ID:
            reasons.append("fast_collector_contract_mismatch")
        if source["collector_cohort_id"] != REQUIRED_FAST_COLLECTOR_COHORT_ID:
            reasons.append("fast_collector_cohort_mismatch")
        output.append(
            {
                "ledger_name": "fast_lane",
                "upstream_observation_id": str(source["observation_id"]),
                "source_id": str(source["source_id"]),
                "source_contract_id": str(source["source_contract_id"] or ""),
                "source_cohort_id": str(source["source_cohort_id"] or ""),
                "source_first_seen_utc": str(source["first_seen_utc"]),
                "source_url": str(payload.get("url") or ""),
                "headline": str(payload.get("title") or payload.get("headline") or ""),
                "publisher_time_eligible": bool(source["publisher_time_eligible"]),
                "raw_payload_json": str(source["raw_payload_json"] or ""),
                "ledger_collector_contract_id": str(source["collector_contract_id"] or ""),
                "ledger_collector_cohort_id": str(source["collector_cohort_id"] or ""),
                "base_ineligibility_reasons": reasons,
            }
        )
    return output


def load_postactivation_candidates(
    fast_database: Path,
    main_database: Path,
    source_ids: Sequence[str],
    existing_alias_ids: set[str],
) -> list[dict[str, Any]]:
    rows = [
        *_fast_rows(fast_database, source_ids, ">="),
        *_main_rows(main_database, source_ids, ">="),
    ]
    output = []
    for row in rows:
        alias_id = _alias_id(row["ledger_name"], row["upstream_observation_id"])
        if alias_id in existing_alias_ids:
            continue
        identity_key, canonical_event_id = canonical_event_identity(row)
        output.append(
            {
                **row,
                "alias_id": alias_id,
                "identity_key": identity_key,
                "canonical_event_id": canonical_event_id,
            }
        )
    return sorted(
        output,
        key=lambda row: (
            str(row.get("source_first_seen_utc") or ""),
            str(row.get("ledger_name") or ""),
            str(row.get("upstream_observation_id") or ""),
        ),
    )


def preactivation_identities(
    fast_database: Path,
    main_database: Path,
    source_ids: Sequence[str],
) -> set[str]:
    rows = [
        *_fast_rows(fast_database, source_ids, "<"),
        *_main_rows(main_database, source_ids, "<"),
    ]
    return {
        canonical_event_id
        for row in rows
        if (canonical_event_id := canonical_event_identity(row)[1])
    }


def build_alias(
    candidate: Mapping[str, Any],
    row_observed_utc: dt.datetime,
    preexisting: set[str],
) -> dict[str, Any]:
    observed = row_observed_utc.astimezone(dt.timezone.utc)
    source_first_seen = parse_time(candidate.get("source_first_seen_utc"))
    reasons = list(candidate.get("base_ineligibility_reasons") or [])
    if source_first_seen is None:
        source_first_seen = observed
        reasons.append("source_first_seen_invalid")
    if source_first_seen < ACTIVATED_UTC:
        reasons.append("source_first_seen_before_activation")
    canonical_event_id = str(candidate.get("canonical_event_id") or "")
    if not canonical_event_id:
        reasons.append("canonical_publisher_identity_missing")
    identity_preexisting = canonical_event_id in preexisting if canonical_event_id else False
    if identity_preexisting:
        reasons.append("canonical_identity_preexisting_before_activation")
    actionable = max(source_first_seen, observed)
    return {
        "alias_id": str(candidate["alias_id"]),
        "ledger_name": str(candidate["ledger_name"]),
        "upstream_observation_id": str(candidate["upstream_observation_id"]),
        "canonical_event_id": canonical_event_id,
        "canonical_identity_key": str(candidate.get("identity_key") or ""),
        "source_id": str(candidate.get("source_id") or ""),
        "source_contract_id": str(candidate.get("source_contract_id") or ""),
        "source_cohort_id": str(candidate.get("source_cohort_id") or ""),
        "source_first_seen_utc": iso_utc(source_first_seen),
        "row_observed_utc": iso_utc(observed),
        "actionable_event_utc": iso_utc(actionable),
        "identity_preexisting_before_activation": identity_preexisting,
        "eligibility_state": (
            "eligible_new_prospective_alias" if not reasons else "rejected_alias"
        ),
        "ineligibility_reasons": sorted(set(reasons)),
        "raw_payload_sha256": sha256_text(str(candidate.get("raw_payload_json") or "")),
        "raw_payload_json": str(candidate.get("raw_payload_json") or ""),
        "ledger_collector_contract_id": str(candidate.get("ledger_collector_contract_id") or ""),
        "ledger_collector_cohort_id": str(candidate.get("ledger_collector_cohort_id") or ""),
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "activated_utc": iso_utc(ACTIVATED_UTC),
        "research_only": True,
        "execution_eligible": False,
        "can_authorize": False,
        "can_promote": False,
    }


def open_output_database(path: Path = OUTPUT_DATABASE) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA busy_timeout=30000")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS official_event_alias (
          alias_id TEXT PRIMARY KEY,
          ledger_name TEXT NOT NULL,
          upstream_observation_id TEXT NOT NULL,
          canonical_event_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          source_first_seen_utc TEXT NOT NULL,
          row_observed_utc TEXT NOT NULL,
          actionable_event_utc TEXT NOT NULL,
          identity_preexisting_before_activation INTEGER NOT NULL CHECK(identity_preexisting_before_activation IN (0,1)),
          eligibility_state TEXT NOT NULL,
          raw_payload_sha256 TEXT NOT NULL,
          alias_payload_json TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
          can_promote INTEGER NOT NULL CHECK(can_promote=0),
          contract_id TEXT NOT NULL,
          cohort_id TEXT NOT NULL,
          activated_utc TEXT NOT NULL,
          UNIQUE(ledger_name,upstream_observation_id)
        );
        CREATE TABLE IF NOT EXISTS official_event_pair_quote_capture (
          capture_id TEXT PRIMARY KEY,
          canonical_event_id TEXT NOT NULL UNIQUE,
          winner_alias_id TEXT NOT NULL UNIQUE,
          source_id TEXT NOT NULL,
          source_first_seen_utc TEXT NOT NULL,
          row_observed_utc TEXT NOT NULL,
          actionable_event_utc TEXT NOT NULL,
          captured_utc TEXT NOT NULL,
          detection_latency_seconds REAL NOT NULL,
          timing_quality TEXT NOT NULL,
          eligible_quote_count INTEGER NOT NULL,
          exact_all_68_available INTEGER NOT NULL CHECK(exact_all_68_available IN (0,1)),
          raw_observation_payload_sha256 TEXT NOT NULL,
          quote_snapshot_payload_sha256 TEXT NOT NULL,
          capture_payload_json TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
          can_promote INTEGER NOT NULL CHECK(can_promote=0),
          contract_id TEXT NOT NULL,
          cohort_id TEXT NOT NULL,
          activated_utc TEXT NOT NULL,
          FOREIGN KEY(winner_alias_id) REFERENCES official_event_alias(alias_id)
        );
        CREATE INDEX IF NOT EXISTS idx_official_event_alias_canonical
          ON official_event_alias(canonical_event_id,actionable_event_utc);
        CREATE TRIGGER IF NOT EXISTS official_event_alias_no_update
        BEFORE UPDATE ON official_event_alias BEGIN SELECT RAISE(ABORT,'append_only:official_event_alias'); END;
        CREATE TRIGGER IF NOT EXISTS official_event_alias_no_delete
        BEFORE DELETE ON official_event_alias BEGIN SELECT RAISE(ABORT,'append_only:official_event_alias'); END;
        CREATE TRIGGER IF NOT EXISTS official_event_pair_quote_capture_v2_no_update
        BEFORE UPDATE ON official_event_pair_quote_capture BEGIN SELECT RAISE(ABORT,'append_only:official_event_pair_quote_capture_v2'); END;
        CREATE TRIGGER IF NOT EXISTS official_event_pair_quote_capture_v2_no_delete
        BEFORE DELETE ON official_event_pair_quote_capture BEGIN SELECT RAISE(ABORT,'append_only:official_event_pair_quote_capture_v2'); END;
        """
    )
    connection.commit()
    return connection


def insert_alias(connection: sqlite3.Connection, alias: Mapping[str, Any]) -> bool:
    cursor = connection.execute(
        """INSERT OR IGNORE INTO official_event_alias VALUES(
             ?,?,?,?,?,?,?,?,?,?,?,?,1,0,0,0,?,?,?)""",
        (
            alias["alias_id"], alias["ledger_name"], alias["upstream_observation_id"],
            alias["canonical_event_id"], alias["source_id"], alias["source_first_seen_utc"],
            alias["row_observed_utc"], alias["actionable_event_utc"],
            int(bool(alias["identity_preexisting_before_activation"])),
            alias["eligibility_state"], alias["raw_payload_sha256"], canonical_json(alias),
            CONTRACT_ID, COHORT_ID, iso_utc(ACTIVATED_UTC),
        ),
    )
    return bool(cursor.rowcount)


def build_capture(
    alias: Mapping[str, Any],
    quote_payload: Mapping[str, Any],
    captured_utc: dt.datetime,
) -> dict[str, Any]:
    observation = {
        "observation_id": alias["canonical_event_id"],
        "source_id": alias["source_id"],
        "source_contract_id": alias.get("source_contract_id") or "",
        "first_seen_utc": alias["actionable_event_utc"],
        "raw_payload_json": alias.get("raw_payload_json") or "",
        "collector_contract_id": alias.get("ledger_collector_contract_id") or "",
        "collector_cohort_id": alias.get("ledger_collector_cohort_id") or "",
    }
    base = quote_v1.build_capture(observation, quote_payload, captured_utc)
    capture_id = sha256_text(
        f"{alias['canonical_event_id']}|{CONTRACT_ID}|{COHORT_ID}"
    )
    return {
        **base,
        "schema_version": SCHEMA_VERSION,
        "capture_id": capture_id,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "activated_utc": iso_utc(ACTIVATED_UTC),
        "observation_id": alias["canonical_event_id"],
        "canonical_event_id": alias["canonical_event_id"],
        "canonical_identity_key": alias["canonical_identity_key"],
        "winner_alias_id": alias["alias_id"],
        "winner_ledger_name": alias["ledger_name"],
        "source_first_seen_utc": alias["source_first_seen_utc"],
        "row_observed_utc": alias["row_observed_utc"],
        "actionable_event_utc": alias["actionable_event_utc"],
        "event_first_known_utc": alias["actionable_event_utc"],
        "raw_observation_payload_sha256": alias["raw_payload_sha256"],
        "semantic_mapping_read_before_capture": False,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
    }


def insert_capture(connection: sqlite3.Connection, capture: Mapping[str, Any]) -> bool:
    cursor = connection.execute(
        """INSERT OR IGNORE INTO official_event_pair_quote_capture VALUES(
             ?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?,1,0,0,0,?,?,?)""",
        (
            capture["capture_id"], capture["canonical_event_id"], capture["winner_alias_id"],
            capture["source_id"], capture["source_first_seen_utc"], capture["row_observed_utc"],
            capture["actionable_event_utc"], capture["captured_utc"],
            capture["detection_latency_seconds"], capture["timing_quality"],
            capture["eligible_quote_count"], int(bool(capture["exact_all_68_available"])),
            capture["raw_observation_payload_sha256"], capture["quote_snapshot_payload_sha256"],
            canonical_json(capture), CONTRACT_ID, COHORT_ID, iso_utc(ACTIVATED_UTC),
        ),
    )
    return bool(cursor.rowcount)


def diagnostics(connection: sqlite3.Connection) -> dict[str, Any]:
    alias = connection.execute(
        """SELECT COUNT(*),COALESCE(SUM(eligibility_state='eligible_new_prospective_alias'),0),
                  COALESCE(SUM(identity_preexisting_before_activation),0),COALESCE(MAX(row_observed_utc),'')
           FROM official_event_alias"""
    ).fetchone()
    captures = connection.execute(
        """SELECT COUNT(*),COALESCE(SUM(timing_quality='prospective_per_pair_executable_quotes'),0),
                  COALESCE(SUM(exact_all_68_available),0),COALESCE(SUM(eligible_quote_count),0),
                  COALESCE(MAX(captured_utc),'') FROM official_event_pair_quote_capture"""
    ).fetchone()
    return {
        "alias_count": int(alias[0]),
        "eligible_alias_count": int(alias[1]),
        "preactivation_identity_rejection_count": int(alias[2]),
        "latest_alias_observed_utc": str(alias[3]),
        "capture_count": int(captures[0]),
        "per_pair_ready_count": int(captures[1]),
        "exact_all_68_count": int(captures[2]),
        "eligible_pair_quote_total": int(captures[3]),
        "latest_captured_utc": str(captures[4]),
    }


def run_cycle(
    *,
    fast_database: Path = FAST_DATABASE,
    main_database: Path = MAIN_DATABASE,
    quote_path: Path = QUOTE_PATH,
    output_database: Path = OUTPUT_DATABASE,
    snapshot_path: Path = SNAPSHOT_PATH,
    heartbeat_path: Path = HEARTBEAT_PATH,
    observed_utc: dt.datetime | None = None,
) -> dict[str, Any]:
    validate_config()
    cycle_started = (observed_utc or utc_now()).astimezone(dt.timezone.utc)
    connection = open_output_database(output_database)
    inserted_aliases = 0
    inserted_captures = 0
    pending_count = 0
    error = ""
    try:
        existing_alias_ids = {
            str(row[0]) for row in connection.execute("SELECT alias_id FROM official_event_alias")
        }
        source_ids = source_ids_from_frozen_map()
        candidates = load_postactivation_candidates(
            fast_database, main_database, source_ids, existing_alias_ids
        )
        pending_count = len(candidates)
        if candidates:
            preexisting = preactivation_identities(
                fast_database, main_database, source_ids
            )
            aliases = [build_alias(row, cycle_started, preexisting) for row in candidates]
            for alias in aliases:
                inserted_aliases += int(insert_alias(connection, alias))
            connection.commit()
            captured_event_ids = {
                str(row[0])
                for row in connection.execute(
                    "SELECT canonical_event_id FROM official_event_pair_quote_capture"
                )
            }
            eligible = [
                row
                for row in aliases
                if row["eligibility_state"] == "eligible_new_prospective_alias"
                and row["canonical_event_id"] not in captured_event_ids
            ]
            winners: dict[str, dict[str, Any]] = {}
            for alias in eligible:
                current = winners.get(alias["canonical_event_id"])
                rank = (alias["actionable_event_utc"], alias["ledger_name"], alias["alias_id"])
                if current is None or rank < (
                    current["actionable_event_utc"], current["ledger_name"], current["alias_id"]
                ):
                    winners[alias["canonical_event_id"]] = alias
            if winners:
                quote_payload = load_quote_snapshot(quote_path)
                for alias in sorted(winners.values(), key=lambda row: row["actionable_event_utc"]):
                    capture = build_capture(alias, quote_payload, cycle_started)
                    inserted_captures += int(insert_capture(connection, capture))
                connection.commit()
        counts = diagnostics(connection)
        integrity = str(connection.execute("PRAGMA quick_check").fetchone()[0])
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        connection.rollback()
        error = f"{type(exc).__name__}: {exc}"
        counts = diagnostics(connection)
        integrity = "error"
    finally:
        connection.close()
    completed = cycle_started if observed_utc is not None else utc_now()
    snapshot = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "activated_utc": iso_utc(ACTIVATED_UTC),
        "generated_utc": iso_utc(completed),
        "status": "ok" if not error and integrity == "ok" else "error",
        "error": error,
        "pending_aliases_seen": pending_count,
        "inserted_aliases": inserted_aliases,
        "inserted_captures": inserted_captures,
        "counts": counts,
        "sqlite_integrity": integrity,
        "source_count": 42,
        "policy": dict(POLICY),
    }
    write_json_atomic(snapshot_path, snapshot)
    write_json_atomic(
        heartbeat_path,
        {
            "schema_version": 1,
            "worker": "oanda_official_event_pair_quote_capture_v2",
            "status": "running" if snapshot["status"] == "ok" else "error",
            "updated_at": snapshot["generated_utc"],
            "phase": "polling_dual_raw_official_ledgers",
            "details": {
                "contract_id": CONTRACT_ID,
                "cohort_id": COHORT_ID,
                **counts,
                "last_error": error,
                "research_only": True,
                "execution_eligible": False,
            },
        },
    )
    if error:
        raise RuntimeError(error)
    return snapshot


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fast-database", type=Path, default=FAST_DATABASE)
    parser.add_argument("--main-database", type=Path, default=MAIN_DATABASE)
    parser.add_argument("--quote-path", type=Path, default=QUOTE_PATH)
    parser.add_argument("--output-database", type=Path, default=OUTPUT_DATABASE)
    parser.add_argument("--snapshot", type=Path, default=SNAPSHOT_PATH)
    parser.add_argument("--heartbeat", type=Path, default=HEARTBEAT_PATH)
    parser.add_argument("--interval-sec", type=float, default=1.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    while True:
        result = run_cycle(
            fast_database=args.fast_database,
            main_database=args.main_database,
            quote_path=args.quote_path,
            output_database=args.output_database,
            snapshot_path=args.snapshot,
            heartbeat_path=args.heartbeat,
        )
        if not args.quiet:
            print(canonical_json(result), flush=True)
        if args.once or (
            args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec
        ):
            return 0
        time.sleep(max(0.25, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ACTIVATED_UTC",
    "COHORT_ID",
    "CONTRACT_ID",
    "POLICY",
    "build_alias",
    "build_capture",
    "canonical_event_identity",
    "diagnostics",
    "load_postactivation_candidates",
    "open_output_database",
    "preactivation_identities",
    "run_cycle",
    "source_ids_from_frozen_map",
    "validate_config",
]
