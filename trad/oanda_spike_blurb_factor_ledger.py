#!/usr/bin/env python3
"""Build pure-source factor observations and link them to movement episodes.

Official source items are deduplicated before factors are created.  Historical
numeric backfills are useful for explanation but never become decision-time
features.  Point-in-time official observations use the system first-seen clock.
No generated row is eligible for promotion, authorization, or execution.
"""

from __future__ import annotations

import argparse
import bisect
import datetime as dt
import hashlib
import json
import math
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from oanda_spike_blurb_factor_reconstruction import (
    CONFIG,
    DIRECT_RESPONSE_DB,
    MACRO_DB,
    REPORT_ROOT,
    SOURCE_DB,
    atomic_text,
    canonical_json,
    file_sha256,
    load_contract,
    parse_epoch,
    sha256_bytes,
    utc_now,
)
from oanda_spike_blurb_movement_inventory import DATABASE, BUILD_CONTRACT_ID, stable_id


FACTOR_CONTRACT_ID = "spike_blurb_pure_source_factor_v1_20260819"
OUTPUT_JSON = REPORT_ROOT / "SPIKE_BLURB_PURE_SOURCE_FACTOR_LEDGER_V1.json"
OUTPUT_MD = REPORT_ROOT / "SPIKE_BLURB_PURE_SOURCE_FACTOR_LEDGER_V1.md"
PRE_ENTRY_LOOKBACK_SEC = 6 * 60 * 60

OFFICIAL_POPULATIONS = {
    "official_policy_publisher",
    "official_macro_publisher",
    "official_rates_curve",
}

SOURCE_CURRENCY_PREFIXES = (
    ("boj", "JPY"), ("japan", "JPY"), ("ecb", "EUR"), ("eurostat", "EUR"),
    ("rba", "AUD"), ("australia", "AUD"), ("rbnz", "NZD"), ("new_zealand", "NZD"),
    ("fed", "USD"), ("fomc", "USD"), ("us_", "USD"), ("bls", "USD"),
    ("census", "USD"), ("boc", "CAD"), ("canada", "CAD"), ("boe", "GBP"),
    ("ons_", "GBP"), ("snb", "CHF"), ("swiss", "CHF"), ("riksbank", "SEK"),
    ("sweden", "SEK"), ("norges", "NOK"), ("norway", "NOK"),
    ("nationalbanken", "DKK"), ("denmark", "DKK"), ("nbp", "PLN"),
    ("poland", "PLN"), ("cnb", "CZK"), ("czech", "CZK"), ("mnb", "HUF"),
    ("hungary", "HUF"), ("tcmb", "TRY"), ("turkey", "TRY"),
    ("banxico", "MXN"), ("mexico", "MXN"), ("sarb", "ZAR"),
    ("south_africa", "ZAR"), ("hkma", "HKD"), ("hong_kong", "HKD"),
    ("mas", "SGD"), ("singapore", "SGD"), ("bot", "THB"),
    ("thailand", "THB"), ("pboc", "CNH"), ("china", "CNH"),
)


def _json(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value or ""))
    except (json.JSONDecodeError, TypeError, ValueError):
        return default


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def source_currency(
    *, source_id: str, base_currency: str | None, payload: Mapping[str, Any], expected: set[str]
) -> str | None:
    base = str(base_currency or "").upper()
    if base in expected:
        return base
    source_lower = str(source_id).lower()
    for prefix, currency in SOURCE_CURRENCY_PREFIXES:
        if source_lower.startswith(prefix) and currency in expected:
            return currency
    raw = payload.get("raw_payload") if isinstance(payload.get("raw_payload"), dict) else {}
    candidates: list[str] = []
    for owner in (payload, raw):
        values = owner.get("direct_currencies") or owner.get("currencies") or []
        if isinstance(values, list):
            candidates.extend(str(value).upper() for value in values)
    unique = sorted({value for value in candidates if value in expected})
    return unique[0] if len(unique) == 1 else None


def classify_factor_type(event_series: str, event_name: str, event_type: str) -> str:
    text = " ".join([event_series, event_name, event_type]).lower()
    if any(word in text for word in ("policy rate", "cash rate", "interest rate", "ocr", "lpr")):
        return "policy_rate_change"
    if any(word in text for word in ("monetary policy", "policy decision", "minutes", "outlook")):
        return "statement_delta"
    if any(word in text for word in ("intervention", "foreign exchange operation")):
        return "intervention_action"
    if any(word in text for word in ("cpi", "hicp", "inflation", "producer price", "ppi")):
        return "inflation_level_change"
    if any(word in text for word in ("employment", "unemployment", "payroll", "jolts", "job", "wage")):
        return "labor_level_change"
    if any(word in text for word in ("gdp", "growth", "retail", "housing", "production", "business")):
        return "growth_level_change"
    if any(word in text for word in ("trade", "tariff", "fiscal", "budget")):
        return "trade_or_fiscal_change"
    if any(word in text for word in ("yield", "curve", "treasury", "funding", "liquidity")):
        return "yield_or_rate_repricing"
    if any(word in text for word in ("conflict", "war", "political", "election")):
        return "political_or_conflict_shock"
    return "semantic_factor_unquantified"


def relevance_state(factor_type: str, headline: str, event_type: str) -> str:
    text = f"{headline} {event_type}".lower()
    if factor_type in {"policy_rate_change", "intervention_action"}:
        return "direct_action"
    if any(word in text for word in ("decision", "statement", "minutes", "outlook", "inflation", "employment", "gdp")):
        return "direct_release_or_statement"
    if any(word in text for word in ("speech", "remarks", "research", "survey", "conference")):
        return "official_context"
    return "official_unresolved_relevance"


def _source_priority(population: str, factor_type: str, numeric: bool) -> str:
    if factor_type in {"policy_rate_change", "intervention_action"}:
        return "official_action_or_release"
    if numeric:
        return "official_numeric_series_vintage"
    if population == "official_policy_publisher":
        return "official_statement_or_document"
    if population == "official_rates_curve":
        return "official_numeric_series_vintage"
    return "official_action_or_release"


def _factor_score(actual: float, prior: float) -> float:
    change = actual - prior
    scale = max(abs(prior), 1e-9)
    return max(-10.0, min(10.0, change / scale))


def load_numeric_factors(path: Path, expected: set[str]) -> tuple[list[dict[str, Any]], set[str]]:
    database = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    database.execute("PRAGMA query_only=ON")
    query = """
      SELECT row_id,revision_id,release_key,source_event_id,recorded_utc,causal_known_utc,
             scheduled_utc,source_reported_update_utc,event_series_id,event_name,
             currencies_json,unit,actual_value,previous_value,revised_previous_value,
             known_before_recorded_timestamp,source_id,source_name,source_url,
             source_verified,source_direct,payload_sha256,payload_json
      FROM macro_release_revisions
      WHERE actual_value IS NOT NULL AND previous_value IS NOT NULL
        AND source_verified=1 AND source_direct=1
      ORDER BY release_key,recorded_utc,row_id
    """
    by_release: dict[str, tuple[Any, ...]] = {}
    for row in database.execute(query):
        by_release.setdefault(str(row[2]), row)
    database.close()
    output: list[dict[str, Any]] = []
    source_ids: set[str] = set()
    for row in by_release.values():
        (
            row_id, revision_id, release_key, source_event_id, recorded, causal_known,
            scheduled, source_update, event_series, event_name, currencies_json, unit,
            actual, previous, revised_previous, known_before, source_id, source_name,
            source_url, verified, direct, payload_sha, payload_json,
        ) = row
        currencies = [str(value).upper() for value in _json(currencies_json, [])]
        currency = next((value for value in currencies if value in expected), None)
        if currency is None:
            payload = _json(payload_json, {})
            currency = source_currency(
                source_id=str(source_id), base_currency=None, payload=payload, expected=expected
            )
        if currency is None:
            continue
        actual_value = float(actual)
        prior_value = float(revised_previous if revised_previous is not None else previous)
        valid_causal = bool(known_before) and parse_epoch(causal_known) is not None
        known_utc = str(causal_known) if valid_causal else str(recorded)
        event_utc = str(source_update or scheduled or causal_known or recorded)
        factor_type = classify_factor_type(str(event_series), str(event_name), "official_numeric_release")
        source_evidence_id = stable_id("pure_source", FACTOR_CONTRACT_ID, "macro", release_key)
        provenance = {
            "release_key": release_key,
            "revision_id": revision_id,
            "source_event_id": source_event_id,
            "payload_sha256": payload_sha,
            "source_id": source_id,
            "event_utc": event_utc,
            "known_utc": known_utc,
            "actual": actual_value,
            "prior": prior_value,
            "revised_prior": revised_previous,
        }
        factor_id = stable_id("factor", FACTOR_CONTRACT_ID, release_key, currency, factor_type)
        output.append(
            {
                "source_evidence": {
                    "source_evidence_id": source_evidence_id,
                    "factor_contract_id": FACTOR_CONTRACT_ID,
                    "source_event_id": str(source_event_id or ""),
                    "provider_event_id": str(release_key),
                    "source_id": str(source_id),
                    "source_population": "official_macro_publisher",
                    "source_priority_class": _source_priority("official_macro_publisher", factor_type, True),
                    "published_utc": event_utc,
                    "first_seen_utc": str(recorded),
                    "known_utc": known_utc,
                    "event_utc": event_utc,
                    "story_cluster_id": str(release_key),
                    "underlying_event_id": str(release_key),
                    "raw_payload_sha256": str(payload_sha or ""),
                    "quality_state": "verified_direct_numeric_release",
                    "headline": str(event_name),
                    "event_type": "official_numeric_release",
                    "currency": currency,
                    "causal_state": "point_in_time_captured" if valid_causal else "historical_backfill_only",
                    "provenance_sha256": sha256_bytes(canonical_json(provenance).encode("utf-8")),
                    "research_only": 1,
                    "execution_eligible": 0,
                },
                "factor": {
                    "factor_id": factor_id,
                    "factor_contract_id": FACTOR_CONTRACT_ID,
                    "source_evidence_id": source_evidence_id,
                    "factor_type": factor_type,
                    "currency": currency,
                    "known_utc": known_utc,
                    "published_utc": event_utc,
                    "first_seen_utc": str(recorded),
                    "raw_value": actual_value,
                    "prior_value": float(previous),
                    "revised_prior_value": _finite(revised_previous),
                    "unit": str(unit or ""),
                    "signed_factor_score": _factor_score(actual_value, prior_value),
                    "numeric_measurement_state": "actual_minus_latest_known_prior",
                    "currency_direction_state": "unresolved_without_expectation_or_market_response",
                    "causal_state": "point_in_time_captured" if valid_causal else "historical_backfill_only",
                    "relevance_state": relevance_state(factor_type, str(event_name), "official_numeric_release"),
                    "provenance_sha256": sha256_bytes(canonical_json(provenance).encode("utf-8")),
                    "research_only": 1,
                    "execution_eligible": 0,
                    "forecast_proof_eligible": 0,
                },
            }
        )
        if source_event_id:
            source_ids.add(str(source_event_id))
    return output, source_ids


def load_official_source_factors(
    path: Path, expected: set[str], suppress_source_event_ids: set[str]
) -> list[dict[str, Any]]:
    database = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=120)
    database.execute("PRAGMA query_only=ON")
    query = """
      WITH eligible AS (
        SELECT *,
          CASE
            WHEN story_cluster_id IS NOT NULL AND story_cluster_id<>'' THEN story_cluster_id
            ELSE source_id||'|'||COALESCE(published_at_utc,'')||'|'||
                 COALESCE(CAST(raw_value AS TEXT),'')||'|'||COALESCE(CAST(normalized_value AS TEXT),'')
          END AS dedup_key
        FROM source_events
        WHERE source_population IN ('official_policy_publisher','official_macro_publisher','official_rates_curve')
          AND NOT EXISTS (
            SELECT 1 FROM source_event_quarantines q WHERE q.source_event_id=source_events.source_event_id
          )
      ), ranked AS (
        SELECT *,ROW_NUMBER() OVER(
          PARTITION BY dedup_key
          ORDER BY COALESCE(first_seen_at_utc,effective_from_utc,published_at_utc),event_version,source_event_id
        ) AS row_number
        FROM eligible
      )
      SELECT source_event_id,provider_event_id,source_id,source_family,source_population,
             base_currency,event_type,story_cluster_id,event_id,published_at_utc,
             first_seen_at_utc,effective_from_utc,raw_value,normalized_value,quality_state,
             parser_version,raw_payload_sha256,source_contract_id,source_cohort_id,payload_json
      FROM ranked WHERE row_number=1
      ORDER BY COALESCE(first_seen_at_utc,effective_from_utc,published_at_utc),source_event_id
    """
    rows = list(database.execute(query))
    database.close()
    output: list[dict[str, Any]] = []
    for row in rows:
        (
            source_event_id, provider_event_id, source_id, source_family, population,
            base_currency, event_type, story_cluster_id, event_id, published, first_seen,
            effective, raw_value, normalized_value, quality_state, parser_version,
            raw_payload_sha, source_contract_id, source_cohort_id, payload_json,
        ) = row
        if str(source_event_id) in suppress_source_event_ids:
            continue
        payload = _json(payload_json, {})
        currency = source_currency(
            source_id=str(source_id), base_currency=base_currency, payload=payload, expected=expected
        )
        if currency is None:
            continue
        raw_payload = payload.get("raw_payload") if isinstance(payload.get("raw_payload"), dict) else {}
        headline = str(payload.get("headline") or raw_payload.get("headline") or provider_event_id or "")
        factor_type = classify_factor_type(str(source_id), headline, str(event_type))
        known_utc = str(first_seen or effective or published or "")
        published_utc = str(published or known_utc)
        event_utc = published_utc
        if parse_epoch(known_utc) is None or parse_epoch(event_utc) is None:
            continue
        causal = "point_in_time_observed"
        if str(quality_state) in {"bootstrap_or_revision_nonproof", "historical_backfill_only"}:
            causal = "historical_backfill_only"
        currency_scores = raw_payload.get("currency_scores") if isinstance(raw_payload.get("currency_scores"), dict) else {}
        score = _finite(currency_scores.get(currency))
        if score is None:
            score = _finite(normalized_value)
        if score is None:
            score = _finite(raw_value)
        score = float(score or 0.0)
        numeric_state = (
            "official_rate_curve_level_or_shape"
            if str(population) == "official_rates_curve"
            else "semantic_model_score_only"
        )
        factor_id = stable_id(
            "factor", FACTOR_CONTRACT_ID, "source", source_event_id, currency, factor_type
        )
        source_evidence_id = stable_id("pure_source", FACTOR_CONTRACT_ID, source_event_id)
        provenance = {
            "source_event_id": source_event_id,
            "provider_event_id": provider_event_id,
            "source_id": source_id,
            "story_cluster_id": story_cluster_id,
            "published_utc": published_utc,
            "first_seen_utc": known_utc,
            "raw_payload_sha256": raw_payload_sha,
            "parser_version": parser_version,
            "source_contract_id": source_contract_id,
            "source_cohort_id": source_cohort_id,
        }
        priority = _source_priority(str(population), factor_type, str(population) == "official_rates_curve")
        output.append(
            {
                "source_evidence": {
                    "source_evidence_id": source_evidence_id,
                    "factor_contract_id": FACTOR_CONTRACT_ID,
                    "source_event_id": str(source_event_id),
                    "provider_event_id": str(provider_event_id or ""),
                    "source_id": str(source_id),
                    "source_population": str(population),
                    "source_priority_class": priority,
                    "published_utc": published_utc,
                    "first_seen_utc": known_utc,
                    "known_utc": known_utc,
                    "event_utc": event_utc,
                    "story_cluster_id": str(story_cluster_id or source_event_id),
                    "underlying_event_id": str(event_id or story_cluster_id or source_event_id),
                    "raw_payload_sha256": str(raw_payload_sha or ""),
                    "quality_state": str(quality_state or "unknown"),
                    "headline": headline,
                    "event_type": str(event_type or "unknown"),
                    "currency": currency,
                    "causal_state": causal,
                    "provenance_sha256": sha256_bytes(canonical_json(provenance).encode("utf-8")),
                    "research_only": 1,
                    "execution_eligible": 0,
                },
                "factor": {
                    "factor_id": factor_id,
                    "factor_contract_id": FACTOR_CONTRACT_ID,
                    "source_evidence_id": source_evidence_id,
                    "factor_type": factor_type,
                    "currency": currency,
                    "known_utc": known_utc,
                    "published_utc": published_utc,
                    "first_seen_utc": known_utc,
                    "raw_value": _finite(raw_value),
                    "prior_value": None,
                    "revised_prior_value": None,
                    "unit": "source_score" if numeric_state == "semantic_model_score_only" else "source_native",
                    "signed_factor_score": score,
                    "numeric_measurement_state": numeric_state,
                    "currency_direction_state": (
                        "semantic_hypothesis_only"
                        if numeric_state == "semantic_model_score_only"
                        else "unresolved_rate_level_context"
                    ),
                    "causal_state": causal,
                    "relevance_state": relevance_state(factor_type, headline, str(event_type)),
                    "provenance_sha256": sha256_bytes(canonical_json(provenance).encode("utf-8")),
                    "research_only": 1,
                    "execution_eligible": 0,
                    "forecast_proof_eligible": 0,
                },
            }
        )
    return output


def ensure_schema(database: sqlite3.Connection) -> None:
    database.executescript(
        """
        CREATE TABLE IF NOT EXISTS factor_contracts (
          factor_contract_id TEXT PRIMARY KEY,
          movement_build_contract_id TEXT NOT NULL,
          parent_contract_id TEXT NOT NULL,
          created_utc TEXT NOT NULL,
          builder_sha256 TEXT NOT NULL,
          source_database_path TEXT NOT NULL,
          source_database_bytes INTEGER NOT NULL,
          macro_database_path TEXT NOT NULL,
          macro_database_bytes INTEGER NOT NULL,
          contract_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS source_attributions (
          source_evidence_id TEXT PRIMARY KEY,
          factor_contract_id TEXT NOT NULL,
          source_event_id TEXT NOT NULL,
          provider_event_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          source_population TEXT NOT NULL,
          source_priority_class TEXT NOT NULL,
          published_utc TEXT NOT NULL,
          first_seen_utc TEXT NOT NULL,
          known_utc TEXT NOT NULL,
          event_utc TEXT NOT NULL,
          story_cluster_id TEXT NOT NULL,
          underlying_event_id TEXT NOT NULL,
          raw_payload_sha256 TEXT NOT NULL,
          quality_state TEXT NOT NULL,
          headline TEXT NOT NULL,
          event_type TEXT NOT NULL,
          currency TEXT NOT NULL,
          causal_state TEXT NOT NULL,
          provenance_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          FOREIGN KEY(factor_contract_id) REFERENCES factor_contracts(factor_contract_id)
        );
        CREATE INDEX IF NOT EXISTS source_attributions_currency_time
          ON source_attributions(currency,event_utc,known_utc);
        CREATE TABLE IF NOT EXISTS factor_observations (
          factor_id TEXT PRIMARY KEY,
          factor_contract_id TEXT NOT NULL,
          source_evidence_id TEXT NOT NULL,
          factor_type TEXT NOT NULL,
          currency TEXT NOT NULL,
          known_utc TEXT NOT NULL,
          published_utc TEXT NOT NULL,
          first_seen_utc TEXT NOT NULL,
          raw_value REAL,
          prior_value REAL,
          revised_prior_value REAL,
          unit TEXT NOT NULL,
          signed_factor_score REAL NOT NULL,
          numeric_measurement_state TEXT NOT NULL,
          currency_direction_state TEXT NOT NULL,
          causal_state TEXT NOT NULL,
          relevance_state TEXT NOT NULL,
          provenance_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0),
          FOREIGN KEY(factor_contract_id) REFERENCES factor_contracts(factor_contract_id),
          FOREIGN KEY(source_evidence_id) REFERENCES source_attributions(source_evidence_id)
        );
        CREATE INDEX IF NOT EXISTS factor_observations_currency_time
          ON factor_observations(currency,published_utc,known_utc);
        CREATE TABLE IF NOT EXISTS movement_factor_links (
          link_id TEXT PRIMARY KEY,
          factor_contract_id TEXT NOT NULL,
          candidate_id TEXT NOT NULL,
          factor_id TEXT NOT NULL,
          source_evidence_id TEXT NOT NULL,
          relation TEXT NOT NULL,
          event_to_entry_sec INTEGER NOT NULL,
          known_to_entry_sec INTEGER NOT NULL,
          decision_time_eligible INTEGER NOT NULL,
          direction_alignment_state TEXT NOT NULL,
          outcome_selected INTEGER NOT NULL CHECK(outcome_selected=1),
          forecast_proof_eligible INTEGER NOT NULL CHECK(forecast_proof_eligible=0),
          research_only INTEGER NOT NULL CHECK(research_only=1),
          UNIQUE(factor_contract_id,candidate_id,factor_id),
          FOREIGN KEY(factor_contract_id) REFERENCES factor_contracts(factor_contract_id),
          FOREIGN KEY(candidate_id) REFERENCES movement_candidates(candidate_id),
          FOREIGN KEY(factor_id) REFERENCES factor_observations(factor_id),
          FOREIGN KEY(source_evidence_id) REFERENCES source_attributions(source_evidence_id)
        );
        CREATE INDEX IF NOT EXISTS movement_factor_links_candidate ON movement_factor_links(candidate_id);
        CREATE INDEX IF NOT EXISTS movement_factor_links_factor ON movement_factor_links(factor_id);
        CREATE TRIGGER IF NOT EXISTS factor_contracts_no_update
          BEFORE UPDATE ON factor_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS factor_contracts_no_delete
          BEFORE DELETE ON factor_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS source_attributions_no_update
          BEFORE UPDATE ON source_attributions BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS source_attributions_no_delete
          BEFORE DELETE ON source_attributions BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS factor_observations_no_update
          BEFORE UPDATE ON factor_observations BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS factor_observations_no_delete
          BEFORE DELETE ON factor_observations BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS movement_factor_links_no_update
          BEFORE UPDATE ON movement_factor_links BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS movement_factor_links_no_delete
          BEFORE DELETE ON movement_factor_links BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


SOURCE_COLUMNS = [
    "source_evidence_id", "factor_contract_id", "source_event_id", "provider_event_id",
    "source_id", "source_population", "source_priority_class", "published_utc", "first_seen_utc",
    "known_utc", "event_utc", "story_cluster_id", "underlying_event_id", "raw_payload_sha256",
    "quality_state", "headline", "event_type", "currency", "causal_state", "provenance_sha256",
    "research_only", "execution_eligible",
]
FACTOR_COLUMNS = [
    "factor_id", "factor_contract_id", "source_evidence_id", "factor_type", "currency",
    "known_utc", "published_utc", "first_seen_utc", "raw_value", "prior_value",
    "revised_prior_value", "unit", "signed_factor_score", "numeric_measurement_state",
    "currency_direction_state", "causal_state", "relevance_state", "provenance_sha256",
    "research_only", "execution_eligible", "forecast_proof_eligible",
]
LINK_COLUMNS = [
    "link_id", "factor_contract_id", "candidate_id", "factor_id", "source_evidence_id",
    "relation", "event_to_entry_sec", "known_to_entry_sec", "decision_time_eligible",
    "direction_alignment_state", "outcome_selected", "forecast_proof_eligible", "research_only",
]


def insert_rows(database: sqlite3.Connection, table: str, columns: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        return
    database.executemany(
        f"INSERT INTO {table}({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
        [tuple(row.get(column) for column in columns) for row in rows],
    )


def load_candidates(database: sqlite3.Connection) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    query = """
      SELECT candidate_id,instrument,entry_epoch,exit_epoch,selected_side,base_currency,quote_currency
      FROM movement_candidates WHERE build_contract_id=? ORDER BY entry_epoch,candidate_id
    """
    for candidate_id, instrument, entry, exit_epoch, side, base, quote in database.execute(
        query, (BUILD_CONTRACT_ID,)
    ):
        row = {
            "candidate_id": str(candidate_id),
            "instrument": str(instrument),
            "entry_epoch": int(entry),
            "exit_epoch": int(exit_epoch),
            "selected_side": str(side),
            "base_currency": str(base),
            "quote_currency": str(quote),
        }
        grouped[str(base)].append(row)
        grouped[str(quote)].append(row)
    return grouped


def link_factors(
    factors: Sequence[Mapping[str, Any]], candidates: Mapping[str, list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for item in factors:
        factor = item["factor"]
        source = item["source_evidence"]
        currency = str(factor["currency"])
        event_epoch = parse_epoch(source["event_utc"])
        known_epoch = parse_epoch(factor["known_utc"])
        if event_epoch is None or known_epoch is None:
            continue
        for candidate in candidates.get(currency, []):
            entry = int(candidate["entry_epoch"])
            exit_epoch = int(candidate["exit_epoch"])
            if event_epoch < entry - PRE_ENTRY_LOOKBACK_SEC or event_epoch > exit_epoch:
                continue
            causal = factor["causal_state"] in {"point_in_time_captured", "point_in_time_observed"}
            if causal and known_epoch <= entry and entry - known_epoch <= PRE_ENTRY_LOOKBACK_SEC:
                relation = "pre_entry_causal"
                decision_eligible = 1
            elif event_epoch <= entry < known_epoch:
                relation = "published_pre_entry_observed_late"
                decision_eligible = 0
            elif causal and entry < known_epoch <= exit_epoch:
                relation = "during_move_causal"
                decision_eligible = 0
            else:
                relation = "ex_post_factor_overlap"
                decision_eligible = 0
            output.append(
                {
                    "link_id": stable_id(
                        "movement_factor_link", FACTOR_CONTRACT_ID, candidate["candidate_id"], factor["factor_id"]
                    ),
                    "factor_contract_id": FACTOR_CONTRACT_ID,
                    "candidate_id": candidate["candidate_id"],
                    "factor_id": factor["factor_id"],
                    "source_evidence_id": factor["source_evidence_id"],
                    "relation": relation,
                    "event_to_entry_sec": entry - event_epoch,
                    "known_to_entry_sec": entry - known_epoch,
                    "decision_time_eligible": decision_eligible,
                    "direction_alignment_state": "unresolved_until_factor_response_calibration",
                    "outcome_selected": 1,
                    "forecast_proof_eligible": 0,
                    "research_only": 1,
                }
            )
    return output


def build_factor_ledger(
    *,
    config_path: Path = CONFIG,
    database_path: Path = DATABASE,
    source_db: Path = SOURCE_DB,
    macro_db: Path = MACRO_DB,
) -> dict[str, Any]:
    contract = load_contract(config_path)
    expected = set(contract["expected_currencies"])
    numeric, suppress = load_numeric_factors(macro_db, expected)
    semantic = load_official_source_factors(source_db, expected, suppress)
    combined_by_factor: dict[str, dict[str, Any]] = {}
    for item in [*numeric, *semantic]:
        combined_by_factor.setdefault(str(item["factor"]["factor_id"]), item)
    factors = list(combined_by_factor.values())
    database = sqlite3.connect(database_path, timeout=120)
    database.execute("PRAGMA journal_mode=WAL")
    database.execute("PRAGMA synchronous=FULL")
    database.execute("PRAGMA foreign_keys=ON")
    ensure_schema(database)
    existing = database.execute(
        "SELECT builder_sha256 FROM factor_contracts WHERE factor_contract_id=?",
        (FACTOR_CONTRACT_ID,),
    ).fetchone()
    builder_sha = file_sha256(Path(__file__).resolve())
    if existing:
        database.close()
        if str(existing[0]) != builder_sha:
            raise RuntimeError("immutable_factor_contract_collision")
        return factor_snapshot(database_path, reused=True)
    candidates = load_candidates(database)
    links = link_factors(factors, candidates)
    factor_contract = {
        "factor_contract_id": FACTOR_CONTRACT_ID,
        "movement_build_contract_id": BUILD_CONTRACT_ID,
        "parent_contract_id": contract["contract_id"],
        "created_utc": utc_now(),
        "builder_sha256": builder_sha,
        "source_database_path": str(source_db.resolve()),
        "source_database_bytes": source_db.stat().st_size,
        "macro_database_path": str(macro_db.resolve()),
        "macro_database_bytes": macro_db.stat().st_size,
        "contract_json": canonical_json(
            {
                "factor_contract_id": FACTOR_CONTRACT_ID,
                "pure_source_priority": contract["pure_source_priority"],
                "factor_ontology": contract["factor_ontology"],
                "official_populations": sorted(OFFICIAL_POPULATIONS),
                "pre_entry_lookback_sec": PRE_ENTRY_LOOKBACK_SEC,
                "numeric_backfill_policy": "historical_attribution_only_unless_causal_known_clock_is_valid",
                "semantic_policy": "one earliest system-known row per story/source-value identity",
                "direction_policy": "unresolved_until_response_calibration",
                "execution": "none",
            }
        ),
    }
    source_rows = [item["source_evidence"] for item in factors]
    factor_rows = [item["factor"] for item in factors]
    with database:
        insert_rows(database, "factor_contracts", list(factor_contract), [factor_contract])
        insert_rows(database, "source_attributions", SOURCE_COLUMNS, source_rows)
        insert_rows(database, "factor_observations", FACTOR_COLUMNS, factor_rows)
        insert_rows(database, "movement_factor_links", LINK_COLUMNS, links)
    integrity = str(database.execute("PRAGMA integrity_check").fetchone()[0])
    database.close()
    return factor_snapshot(database_path, reused=False, integrity=integrity)


def factor_snapshot(database_path: Path, *, reused: bool, integrity: str | None = None) -> dict[str, Any]:
    database = sqlite3.connect(f"file:{database_path.resolve().as_posix()}?mode=ro", uri=True)
    database.execute("PRAGMA query_only=ON")
    factor_count = int(database.execute(
        "SELECT COUNT(*) FROM factor_observations WHERE factor_contract_id=?", (FACTOR_CONTRACT_ID,)
    ).fetchone()[0])
    source_count = int(database.execute(
        "SELECT COUNT(*) FROM source_attributions WHERE factor_contract_id=?", (FACTOR_CONTRACT_ID,)
    ).fetchone()[0])
    link_count = int(database.execute(
        "SELECT COUNT(*) FROM movement_factor_links WHERE factor_contract_id=?", (FACTOR_CONTRACT_ID,)
    ).fetchone()[0])
    causal = dict(database.execute(
        "SELECT causal_state,COUNT(*) FROM factor_observations WHERE factor_contract_id=? GROUP BY causal_state",
        (FACTOR_CONTRACT_ID,),
    ).fetchall())
    measurement = dict(database.execute(
        "SELECT numeric_measurement_state,COUNT(*) FROM factor_observations WHERE factor_contract_id=? GROUP BY numeric_measurement_state",
        (FACTOR_CONTRACT_ID,),
    ).fetchall())
    currencies = dict(database.execute(
        "SELECT currency,COUNT(*) FROM factor_observations WHERE factor_contract_id=? GROUP BY currency ORDER BY currency",
        (FACTOR_CONTRACT_ID,),
    ).fetchall())
    relations = dict(database.execute(
        "SELECT relation,COUNT(*) FROM movement_factor_links WHERE factor_contract_id=? GROUP BY relation",
        (FACTOR_CONTRACT_ID,),
    ).fetchall())
    linked_candidates = int(database.execute(
        "SELECT COUNT(DISTINCT candidate_id) FROM movement_factor_links WHERE factor_contract_id=?",
        (FACTOR_CONTRACT_ID,),
    ).fetchone()[0])
    decision_linked = int(database.execute(
        "SELECT COUNT(DISTINCT candidate_id) FROM movement_factor_links WHERE factor_contract_id=? AND decision_time_eligible=1",
        (FACTOR_CONTRACT_ID,),
    ).fetchone()[0])
    movement_count = int(database.execute(
        "SELECT COUNT(*) FROM movement_candidates WHERE build_contract_id=?", (BUILD_CONTRACT_ID,)
    ).fetchone()[0])
    if integrity is None:
        integrity = str(database.execute("PRAGMA integrity_check").fetchone()[0])
    database.close()
    snapshot: dict[str, Any] = {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "factor_contract_id": FACTOR_CONTRACT_ID,
        "movement_build_contract_id": BUILD_CONTRACT_ID,
        "database_path": str(database_path.resolve()),
        "database_bytes": database_path.stat().st_size,
        "sqlite_integrity": integrity,
        "reused_existing_immutable_cohort": reused,
        "source_evidence_count": source_count,
        "factor_observation_count": factor_count,
        "factor_causal_state_counts": causal,
        "factor_measurement_state_counts": measurement,
        "currency_counts": currencies,
        "currency_coverage_count": len(currencies),
        "movement_factor_link_count": link_count,
        "movement_factor_relation_counts": relations,
        "movement_candidate_count": movement_count,
        "linked_movement_candidate_count": linked_candidates,
        "decision_time_linked_movement_candidate_count": decision_linked,
        "unlinked_movement_candidate_count": movement_count - linked_candidates,
        "research_only": True,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    snapshot["snapshot_sha256"] = sha256_bytes(canonical_json(snapshot).encode("utf-8"))
    return snapshot


def render_report(snapshot: Mapping[str, Any]) -> str:
    lines = [
        "# Spike/Blurb Pure-Source Factor Ledger V1",
        "",
        f"- Generated: `{snapshot['generated_utc']}`",
        f"- Contract: `{snapshot['factor_contract_id']}`",
        f"- Snapshot SHA-256: `{snapshot['snapshot_sha256']}`",
        "- Safety: **research-only / execution-ineligible / no-trade**",
        "",
        "## Factor evidence",
        "",
        f"- Deduplicated pure-source evidence: **{snapshot['source_evidence_count']:,}**",
        f"- Factor observations: **{snapshot['factor_observation_count']:,}**",
        f"- Currency coverage: **{snapshot['currency_coverage_count']}/21**",
        "",
        "### Causal state",
        "",
    ]
    for state, count in sorted(snapshot["factor_causal_state_counts"].items()):
        lines.append(f"- `{state}`: **{int(count):,}**")
    lines.extend(["", "### Measurement state", ""])
    for state, count in sorted(snapshot["factor_measurement_state_counts"].items()):
        lines.append(f"- `{state}`: **{int(count):,}**")
    lines.extend(
        [
            "",
            "## Movement attribution coverage",
            "",
            f"- Movement candidates: **{snapshot['movement_candidate_count']:,}**",
            f"- Candidates with at least one pure-source factor overlap: **{snapshot['linked_movement_candidate_count']:,}**",
            f"- Candidates with a factor causally known before entry: **{snapshot['decision_time_linked_movement_candidate_count']:,}**",
            f"- Unlinked candidates: **{snapshot['unlinked_movement_candidate_count']:,}**",
            "",
        ]
    )
    for relation, count in sorted(snapshot["movement_factor_relation_counts"].items()):
        lines.append(f"- `{relation}`: **{int(count):,}** links")
    lines.extend(
        [
            "",
            "A source-factor overlap explains where to investigate; it is not a forecast win. Historical numeric backfills and news first seen after entry are barred from decision-time evidence.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--source-db", type=Path, default=SOURCE_DB)
    parser.add_argument("--macro-db", type=Path, default=MACRO_DB)
    parser.add_argument("--output-json", type=Path, default=OUTPUT_JSON)
    parser.add_argument("--output-md", type=Path, default=OUTPUT_MD)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    snapshot = build_factor_ledger(
        config_path=args.config,
        database_path=args.database,
        source_db=args.source_db,
        macro_db=args.macro_db,
    )
    atomic_text(args.output_json, json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    atomic_text(args.output_md, render_report(snapshot))
    print(json.dumps(snapshot, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
