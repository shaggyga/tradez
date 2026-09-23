#!/usr/bin/env python3
"""Causal source-factor response map (research-only V1).

This module deliberately sits beside, rather than inside, the sealed V12
narrative meter and the V3 directional response watcher.  It retains every
verified/direct V3 *input* prospective official observation, including rows
whose semantic currency score is zero, collapses parallel transports into one
economic event, and relates versioned source-native factors to later currency
responses.

The response label is a cross-pair currency factor plus both executable
"currency strengthens" and "currency weakens" paths.  No pair/side is selected
after observing the outcome.  Forecasts are ordered and prequential: only
outcomes whose fixed horizon had matured before the factor became known may be
used.  Sparse cells abstain.  There is no broker, lifecycle, authorization,
promotion, execution, or canonical-watchlist surface in this file.
"""

from __future__ import annotations

import argparse
import bisect
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import statistics
import time
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlsplit, urlunsplit

from src.forex_system.features.currency_state_engine import (
    PairObservation,
    solve_weighted_currency_state,
)


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
LOCAL_NEWS = DATA / "local_news_sentiment"
CANDLE_ROOT = DATA / "candles"
REPORT_ROOT = DATA / "reports" / "causal_source_factor_response"
QUOTE_PATH = DATA / "state" / "practice_007_market_quotes_v1.json"
TECHNICAL_PATH = DATA / "state" / "practice_007_signal_snapshot_research_v1.json"

INPUT_MAPPING_DATABASE = LOCAL_NEWS / "official_release_fast_mapping_v3.sqlite"
INPUT_RAW_DATABASE = LOCAL_NEWS / "official_release_fast_lane_v4.sqlite"
OUTPUT_DATABASE = LOCAL_NEWS / "causal_source_factor_response_map_v1.sqlite"
SNAPSHOT_PATH = LOCAL_NEWS / "causal_source_factor_response_map_latest_v1.json"
REPORT_PATH = REPORT_ROOT / "CAUSAL_SOURCE_FACTOR_RESPONSE_MAP_V1.md"

SCHEMA_VERSION = "causal_source_factor_response_map_v1"
CONTRACT_ID = "causal_source_factor_response_map_v1_point_in_time_episode_v2_20260827"
COHORT_ID = "causal_source_factor_response_map_v1_prospective_20260827T170000Z"
# The boundary is intentionally later than implementation and focused-test
# work.  Nothing observed while the cohort was still being authored can be
# relabelled as prospective proof.
ACTIVATED_UTC = datetime(2026, 8, 27, 17, 0, tzinfo=timezone.utc)
REQUIRED_MAPPER_CONTRACT = (
    "official_release_fast_mapping_v3_semantic_vs_publish_gate_20260824"
)
REQUIRED_PRE_MAP_QUOTE_CONTRACT = (
    "official_release_fast_mapper_pre_semantic_quote_v1_20260828"
)
REQUIRED_CLASSIFICATION_VERSION = (
    "local_fx_news_rules_20260827_v147_greenback_recap_boundary"
)
HORIZONS_MIN = (1, 5, 15, 30, 60, 120)
MIN_EFFECTIVE_N = 8
MAX_CANDLE_DELAY_SECONDS = 180
MAX_LIVE_CAPTURE_LATENCY_SECONDS = 15.0
MAX_ENTRY_OFFSET_SECONDS = 15.0
MAX_LIVE_QUOTE_AGE_SECONDS = 30.0
RESPONSE_MATURITY_GRACE_SECONDS = 180.0
NULL_RESPONSE_BPS = 0.5
# Official policy decisions can emit several separately titled documents
# (decision, projections, implementation note, press-conference transcript)
# from one shock.  In the absence of a stable upstream episode identifier we
# conservatively cluster those documents in a 12-hour UTC window.  Other
# release families use a much narrower one-hour window.  The family remains in
# the key, so an inflation release and a policy decision at the same clock do
# not become one episode.
POLICY_DECISION_EPISODE_WINDOW_MINUTES = 12 * 60
GENERAL_RELEASE_EPISODE_WINDOW_MINUTES = 60
CURRENCIES = (
    "AUD", "CAD", "CHF", "CNH", "CZK", "DKK", "EUR", "GBP", "HKD",
    "HUF", "JPY", "MXN", "NOK", "NZD", "PLN", "SEK", "SGD", "THB",
    "TRY", "USD", "ZAR",
)
SOLVER_POLICY = {
    "minimum_observations": 6,
    "minimum_component_currencies": 4,
    "spread_floor_bps": 0.05,
    "maximum_weight": 10.0,
    "measurement_uncertainty_floor_bps": 0.05,
    "return_clip_minimum_bps": 5.0,
    "return_clip_median_multiple": 8.0,
}

POLICY = {
    "research_only": True,
    "execution_eligible": False,
    "can_place_orders": False,
    "can_authorize": False,
    "can_promote": False,
    "broker_access": False,
    "canonical_watchlist_mutation": False,
    "selected_pair_or_side": False,
    "supported_decision": "research_abstain_or_factor_forecast_only",
}

_INPUT_INTEGRITY_CACHE: dict[str, tuple[tuple[Any, ...], str]] = {}
_INPUT_OBSERVATION_CACHE: dict[
    tuple[str, str, str, str],
    tuple[tuple[tuple[Any, ...], tuple[Any, ...]], tuple[dict[str, Any], ...]],
] = {}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def digest(*parts: Any) -> str:
    raw = "\x1f".join(str(part) for part in parts).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def finite_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def slug(value: Any, *, fallback: str = "unknown") -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text[:160] or fallback


def normalize_headline(value: Any) -> str:
    return " ".join(str(value or "").casefold().split())[:500]


def normalize_url(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    parsed = urlsplit(text)
    path = re.sub(r"/+", "/", parsed.path or "/").rstrip("/") or "/"
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), path, "", ""))


def authority_key(payload: Mapping[str, Any]) -> str:
    url = normalize_url(payload.get("source_url") or payload.get("publisher_url"))
    host = urlsplit(url).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return slug(host or payload.get("source_name") or payload.get("source_id"))


def pip_size(instrument: str) -> float:
    return 0.01 if instrument.upper().endswith("_JPY") else 0.0001


def canonical_event_id(payload: Mapping[str, Any], currency: str) -> str:
    """Transport-independent event identity.

    URL/headline/publisher clock are preferred to mapper ``event_id`` because
    separate official transports can mint different classifier event IDs for
    the same release.  The original transport IDs remain in the transport
    table.
    """

    published = (
        parse_time(payload.get("source_native_published_utc"))
        or parse_time(payload.get("published_utc"))
        or parse_time(payload.get("scheduled_utc"))
    )
    published_key = iso(published.replace(second=0, microsecond=0)) if published else ""
    headline = normalize_headline(payload.get("headline") or payload.get("event_name"))
    reference = slug(payload.get("reference_period") or payload.get("reference_date"), fallback="none")
    topic = slug(payload.get("topic_signature") or payload.get("event_series_id"), fallback="none")
    # A shared lineage/content identifier is useful when title/clock metadata is
    # absent.  When that metadata exists, title+publisher minute is deliberately
    # primary: parallel official transports can use different hostnames and
    # mint different lineage/content hashes and URLs for the same release.
    if headline and published_key:
        # Topic/category are intentionally excluded here. A later parser or a
        # parallel transport can classify the same release differently; those
        # dimensions belong in factor rows, not in economic-event identity.
        identity = (currency.upper(), headline, published_key, reference)
    elif payload.get("event_lineage_id"):
        identity = (currency.upper(), "lineage", str(payload["event_lineage_id"]))
    elif payload.get("material_content_sha256"):
        identity = (currency.upper(), "material", str(payload["material_content_sha256"]))
    else:
        url = normalize_url(payload.get("source_url") or payload.get("publisher_url"))
        identity = (currency.upper(), headline, published_key, reference, topic, url)
    return "source_event_" + digest(*identity)[:32]


def _upstream_episode_identifier(payload: Mapping[str, Any]) -> str:
    """Return only an explicitly episode-level upstream identifier.

    Document/material/lineage IDs are deliberately excluded: a decision,
    projections PDF and transcript normally have different document IDs even
    though they express the same market shock.
    """

    for key in (
        "independent_episode_key",
        "market_episode_id",
        "economic_episode_id",
        "source_event_episode_id",
        "release_episode_id",
        "decision_cycle_id",
        "policy_episode_id",
        "scheduled_event_id",
    ):
        value = payload.get(key)
        if value not in (None, "", [], {}):
            if isinstance(value, (Mapping, list, tuple)):
                return json.dumps(value, sort_keys=True, separators=(",", ":"))
            return str(value).strip()
    return ""


def _episode_family(payload: Mapping[str, Any]) -> str:
    explicit = (
        payload.get("economic_event_family")
        or payload.get("market_event_family")
        or payload.get("event_family")
        or payload.get("release_family")
    )
    explicit_family = slug(explicit, fallback="")

    category = slug(payload.get("category"), fallback="unknown")
    document_type = slug(payload.get("policy_document_type"), fallback="")
    headline = normalize_headline(payload.get("headline") or payload.get("event_name"))
    policy_text = " ".join((document_type.replace("_", " "), headline))

    # Minutes/accounts and speeches are separate market releases.  They must
    # not be merged into the earlier rate-decision episode merely because they
    # discuss that decision.
    if explicit_family in {
        "central_bank_minutes",
        "policy_minutes",
        "minutes",
        "accounts",
    } or document_type in {"minutes", "accounts", "minutes_or_accounts"} or any(
        token in policy_text for token in ("minutes of", "account of the monetary policy")
    ):
        return "central_bank_minutes"
    if explicit_family in {
        "central_bank_speech",
        "policy_speech",
        "speech",
        "remarks",
        "testimony",
        "interview",
    } or document_type in {"speech", "remarks", "testimony", "interview"} or any(
        token in policy_text
        for token in ("speech by", "remarks by", "testimony by", "interview with")
    ):
        return "central_bank_speech"

    decision_document_types = {
        "decision",
        "rate_decision",
        "policy_decision",
        "statement",
        "policy_statement",
        "projections",
        "implementation_note",
        "press_conference",
        "press_conference_transcript",
        "monetary_policy_report",
    }
    decision_event_families = decision_document_types | {
        "central_bank_decision",
        "central_bank_policy_decision",
        "monetary_policy_decision",
        "policy_projections",
        "economic_projections",
        "policy_implementation",
    }
    decision_markers = (
        "interest rate decision",
        "monetary policy decision",
        "monetary policy statement",
        "policy rate decision",
        "summary of economic projections",
        "economic projections",
        "implementation note",
        "press conference transcript",
        "monetary policy report",
    )
    if (
        explicit_family in decision_event_families
        or
        document_type in decision_document_types
        or any(marker in policy_text for marker in decision_markers)
        or payload.get("official_policy_release") is True
    ):
        return "central_bank_policy_decision"

    if explicit_family:
        return explicit_family

    series = slug(payload.get("event_series_id"), fallback="")
    event_name = slug(payload.get("event_name"), fallback="")
    if series:
        return f"{category}:{series}"
    if event_name:
        return f"{category}:{event_name}"
    return category


def _episode_authority(payload: Mapping[str, Any], currency: str, family: str) -> str:
    if family.startswith("central_bank_"):
        # Currency is the durable monetary-authority identity.  It also avoids
        # splitting one bank across press/statistics/document hostnames.
        return f"monetary_authority_{currency.upper()}"
    for key in (
        "issuing_authority",
        "authority_name",
        "institution_name",
        "provider",
        "source_name",
        "source_id",
    ):
        if payload.get(key):
            return slug(payload[key])
    return authority_key(payload)


def _scheduled_episode_clock(payload: Mapping[str, Any]) -> datetime | None:
    for key in (
        "scheduled_utc",
        "scheduled_release_utc",
        "event_scheduled_utc",
        "decision_scheduled_utc",
        "release_scheduled_utc",
    ):
        parsed = parse_time(payload.get(key))
        if parsed is not None:
            return parsed
    return None


def _episode_clock(
    payload: Mapping[str, Any], first_known_utc: datetime | None
) -> datetime | None:
    scheduled = _scheduled_episode_clock(payload)
    if scheduled is not None:
        return scheduled
    for key in ("source_native_published_utc", "published_utc"):
        parsed = parse_time(payload.get(key))
        if parsed is not None:
            return parsed
    return first_known_utc


def _clock_bucket(clock: datetime, window_minutes: int) -> str:
    seconds = max(60, int(window_minutes) * 60)
    epoch = int(clock.timestamp())
    start = epoch - epoch % seconds
    return iso(datetime.fromtimestamp(start, timezone.utc))


def market_episode_id(
    payload: Mapping[str, Any],
    currency: str = "",
    *,
    first_known_utc: datetime | None = None,
) -> str:
    """Underlying economic/market episode, not merely a release document.

    A genuine upstream episode ID wins and remains currency-independent.  If
    none exists, the conservative fallback is authority/currency + event
    family + a fixed scheduled/published-time window.  Document lineage,
    content hashes, URLs and headlines never define effective N.
    """

    stable = _upstream_episode_identifier(payload)
    if stable:
        identity = ("upstream_economic_episode", stable)
    else:
        family = _episode_family(payload)
        authority = _episode_authority(payload, currency, family)
        clock = _episode_clock(payload, first_known_utc)
        if clock is None:
            # A clockless document cannot safely be pooled with another
            # document.  This branch is deliberately non-inflating only when
            # upstream provides an episode ID in a future contract.
            identity = (
                "clockless_unpooled",
                authority,
                currency.upper(),
                family,
                normalize_headline(payload.get("headline") or payload.get("event_name")),
                str(payload.get("event_lineage_id") or payload.get("material_content_sha256") or ""),
            )
        else:
            scheduled = _scheduled_episode_clock(payload)
            if scheduled is not None:
                # Exact scheduled clocks are stronger than a fallback window.
                # Separate scheduled decisions remain separate even if they
                # occur within the same 12-hour period.
                identity = (
                    "authority_currency_family_exact_scheduled_minute",
                    authority,
                    currency.upper(),
                    family,
                    iso(scheduled.replace(second=0, microsecond=0)),
                )
            else:
                window = (
                    POLICY_DECISION_EPISODE_WINDOW_MINUTES
                    if family == "central_bank_policy_decision"
                    else GENERAL_RELEASE_EPISODE_WINDOW_MINUTES
                )
                identity = (
                    "authority_currency_family_publication_window",
                    authority,
                    currency.upper(),
                    family,
                    _clock_bucket(clock, window),
                    window,
                )
    return "market_episode_" + digest(*identity)[:32]


def source_currencies(payload: Mapping[str, Any]) -> list[str]:
    values = payload.get("source_currencies")
    if not isinstance(values, list):
        values = []
    result = sorted(
        {
            str(value or "").upper()
            for value in values
            if re.fullmatch(r"[A-Z]{3}", str(value or "").upper())
        }
    )
    return result


def _causal_numeric_clock(payload: Mapping[str, Any], first_seen: datetime) -> datetime:
    candidates = [first_seen]
    for key in (
        "numeric_causal_known_utc",
        "detail_attachment_available_utc",
        "detail_available_utc",
    ):
        parsed = parse_time(payload.get(key))
        if parsed is not None:
            candidates.append(parsed)
    return max(candidates)


def _sign_label(value: float, comparison: float = 0.0) -> str:
    if value > comparison:
        return "up"
    if value < comparison:
        return "down"
    return "flat"


def structured_factors(
    payload: Mapping[str, Any], first_seen: datetime
) -> list[dict[str, Any]]:
    """Extract stable, structured factors without assigning FX direction."""

    factors: dict[str, dict[str, Any]] = {}

    def add(factor_type: str, value: Any, known: datetime = first_seen) -> None:
        normalized = slug(value)
        if normalized == "unknown":
            return
        key = f"{slug(factor_type)}:{normalized}"
        prior = factors.get(key)
        row = {
            "factor_key": key,
            "factor_type": slug(factor_type),
            "factor_value": normalized,
            "factor_known_utc": iso(max(first_seen, known)),
        }
        if prior is None or str(row["factor_known_utc"]) < str(prior["factor_known_utc"]):
            factors[key] = row

    add("authority", authority_key(payload))
    add("category", payload.get("category"))
    add("source_role", payload.get("source_role"))
    add("series", payload.get("event_series_id"))
    add("action", payload.get("topic_action"))
    add("document", payload.get("policy_document_type"))
    for mechanism in payload.get("transmission_mechanisms") or []:
        add("mechanism", mechanism)
    for tag in (payload.get("topic_tags") or [])[:12]:
        add("topic", tag)

    numeric_clock = _causal_numeric_clock(payload, first_seen)
    actual = finite_number(payload.get("actual_value"))
    if actual is None:
        actual = finite_number(payload.get("actual"))
    previous = finite_number(payload.get("previous_value"))
    if previous is None:
        previous = finite_number(payload.get("previous"))
    if actual is not None:
        add("actual_sign", _sign_label(actual), numeric_clock)
    if actual is not None and previous is not None:
        add("actual_vs_previous", _sign_label(actual, previous), numeric_clock)

    consensus_state = slug(payload.get("consensus_capture_state"), fallback="")
    consensus = finite_number(payload.get("consensus_value"))
    if consensus is None:
        consensus = finite_number(payload.get("consensus"))
    consensus_observed = (
        parse_time(payload.get("consensus_observed_at_utc"))
        or parse_time(payload.get("consensus_observed_utc"))
        or parse_time(payload.get("consensus_retrieved_utc"))
    )
    release_clock = (
        parse_time(payload.get("scheduled_utc"))
        or parse_time(payload.get("scheduled_release_utc"))
        or parse_time(payload.get("published_utc"))
    )
    consensus_provenance = str(
        payload.get("consensus_source_provenance")
        or payload.get("consensus_source_id")
        or payload.get("consensus_provider")
        or payload.get("consensus_source_url")
        or payload.get("consensus_snapshot_id")
        or ""
    ).strip()
    # A state label cannot self-certify causality. The archived observation
    # clock must precede the release clock and carry explicit provenance.
    causal_consensus = bool(
        consensus_state == "causal_pre_release_snapshot"
        and consensus_observed is not None
        and release_clock is not None
        and consensus_observed < release_clock
        and consensus_provenance
    )
    if (
        actual is not None
        and consensus is not None
        and causal_consensus
    ):
        add("surprise", _sign_label(actual, consensus), numeric_clock)

    revised = finite_number(payload.get("revised_previous_value"))
    if revised is None:
        revised = finite_number(payload.get("revised_previous"))
    if revised is not None and previous is not None:
        add("revision", _sign_label(revised, previous), numeric_clock)

    components = payload.get("source_native_components")
    if isinstance(components, Mapping):
        component_items: Iterable[tuple[Any, Any]] = components.items()
    elif isinstance(components, list):
        component_items = (
            (row.get("name") or row.get("component") or index, row)
            for index, row in enumerate(components)
            if isinstance(row, Mapping)
        )
    else:
        component_items = []
    for raw_name, raw_value in component_items:
        name = slug(raw_name)
        if isinstance(raw_value, Mapping):
            component_actual = finite_number(
                raw_value.get("actual_value", raw_value.get("actual"))
            )
            component_previous = finite_number(
                raw_value.get("previous_value", raw_value.get("previous"))
            )
            movement = raw_value.get("movement") or raw_value.get("direction")
            if component_actual is not None and component_previous is not None:
                movement = _sign_label(component_actual, component_previous)
        else:
            movement = raw_value
        if movement not in (None, ""):
            add(f"component_{name}", movement, numeric_clock)

    # Every retained event has at least an authority/category/role source fact,
    # even when semantic currency_scores are empty.
    return sorted(factors.values(), key=lambda row: row["factor_key"])


def _input_fingerprint(path: Path) -> tuple[Any, ...]:
    values: list[Any] = []
    try:
        stat = path.stat()
        values.extend((str(path.resolve()), stat.st_size, stat.st_mtime_ns))
    except OSError:
        values.extend((str(path.resolve()), None, None))

    # SQLite may create/touch an empty ``-wal`` merely because a read-only
    # connection opened a WAL-mode database.  That is not a logical input
    # change and previously invalidated the cache every five seconds.  A
    # non-empty WAL contains committed/uncheckpointed pages, so its size and
    # nanosecond timestamp remain part of the exact change detector.
    wal = Path(str(path) + "-wal")
    try:
        wal_stat = wal.stat()
        if wal_stat.st_size > 0:
            values.extend((str(wal.resolve()), wal_stat.st_size, wal_stat.st_mtime_ns))
        else:
            values.extend((str(wal.resolve()), 0, None))
    except OSError:
        values.extend((str(wal.resolve()), 0, None))
    return tuple(values)


def _open_readonly(path: Path) -> sqlite3.Connection:
    uri = f"file:{path.resolve().as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=30.0)
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


def _quick_check(connection: sqlite3.Connection) -> str:
    return str(connection.execute("PRAGMA quick_check").fetchone()[0])


def _verify_input_integrity(
    connection: sqlite3.Connection, path: Path
) -> bool:
    fingerprint = _input_fingerprint(path)
    cache_key = str(path.resolve())
    cached = _INPUT_INTEGRITY_CACHE.get(cache_key)
    if cached is not None and cached[0] == fingerprint and cached[1] == "ok":
        return False
    integrity = _quick_check(connection)
    if integrity != "ok":
        raise ValueError(f"input database integrity failed:{path}:{integrity}")
    _INPUT_INTEGRITY_CACHE[cache_key] = (fingerprint, integrity)
    return True


def load_current_observations(
    mapping_database: Path = INPUT_MAPPING_DATABASE,
    raw_database: Path = INPUT_RAW_DATABASE,
    *,
    required_classifier: str = REQUIRED_CLASSIFICATION_VERSION,
    required_mapper_contract: str = REQUIRED_MAPPER_CONTRACT,
) -> list[dict[str, Any]]:
    if not mapping_database.exists():
        raise FileNotFoundError(mapping_database)
    if not raw_database.exists():
        raise FileNotFoundError(raw_database)
    cache_key = (
        str(mapping_database.resolve()),
        str(raw_database.resolve()),
        str(required_classifier),
        str(required_mapper_contract),
    )
    input_fingerprints = (
        _input_fingerprint(mapping_database),
        _input_fingerprint(raw_database),
    )
    cached = _INPUT_OBSERVATION_CACHE.get(cache_key)
    if cached is not None and cached[0] == input_fingerprints:
        # Callers treat observations as immutable inputs and canonicalization
        # constructs new event dictionaries.  Return a new list so list-level
        # operations cannot mutate the cache while avoiding a costly deep copy
        # of unchanged versioned payloads.
        return list(cached[1])
    mapping_connection = _open_readonly(mapping_database)
    mapping_connection.row_factory = sqlite3.Row
    raw_connection = _open_readonly(raw_database)
    raw_connection.row_factory = sqlite3.Row
    try:
        _verify_input_integrity(mapping_connection, mapping_database)
        _verify_input_integrity(raw_connection, raw_database)
        rows = mapping_connection.execute(
            """
            SELECT mapping_id,observation_id,source_id,source_contract_id,
                   first_seen_utc,input_prospective_observation,
                   input_listing_bootstrap,classification_version,
                   mapping_payload_json,mapped_utc,mapper_contract_id,
                   mapper_cohort_id
            FROM official_release_mapping
            WHERE input_prospective_observation=1
              AND input_listing_bootstrap=0
              AND classification_version=?
              AND mapper_contract_id=?
            ORDER BY first_seen_utc,mapping_id
            """,
            (required_classifier, required_mapper_contract),
        ).fetchall()
        raw_ids = [str(row["observation_id"]) for row in rows]
        raw_by_id: dict[str, sqlite3.Row] = {}
        for start in range(0, len(raw_ids), 500):
            chunk = raw_ids[start : start + 500]
            placeholders = ",".join("?" for _ in chunk)
            if not chunk:
                continue
            found = raw_connection.execute(
                f"""
                SELECT observation_id,material_sha256,first_seen_utc,
                       prospective_observation,listing_bootstrap,
                       observation_clock_trusted,observation_clock_source,
                       collector_contract_id,collector_cohort_id,raw_payload_json
                FROM official_release_observation
                WHERE observation_id IN ({placeholders})
                """,
                tuple(chunk),
            ).fetchall()
            raw_by_id.update({str(row["observation_id"]): row for row in found})
    finally:
        mapping_connection.close()
        raw_connection.close()

    output: list[dict[str, Any]] = []
    seen_mapping_ids: set[str] = set()
    for row in rows:
        mapping_id = str(row["mapping_id"])
        if mapping_id in seen_mapping_ids:
            continue
        seen_mapping_ids.add(mapping_id)
        raw = raw_by_id.get(str(row["observation_id"]))
        if raw is None:
            raise ValueError(f"raw observation missing:{row['observation_id']}")
        if not int(raw["prospective_observation"] or 0):
            continue
        if int(raw["listing_bootstrap"] or 0):
            continue
        if not int(raw["observation_clock_trusted"] or 0):
            continue
        raw_first = parse_time(raw["first_seen_utc"])
        mapped_first = parse_time(row["first_seen_utc"])
        mapped_at = parse_time(row["mapped_utc"])
        if raw_first is None or mapped_first is None or mapped_at is None:
            continue
        if raw_first != mapped_first:
            raise ValueError(f"first-known clock mismatch:{row['observation_id']}")
        try:
            payload = json.loads(str(row["mapping_payload_json"]))
            raw_payload = json.loads(str(raw["raw_payload_json"]))
        except json.JSONDecodeError as exc:
            raise ValueError("invalid input payload JSON") from exc
        if not isinstance(payload, dict) or not isinstance(raw_payload, dict):
            raise ValueError("input payload must be an object")
        if payload.get("source_direct") is not True or payload.get("source_verified") is not True:
            continue
        currencies = source_currencies(payload)
        if not currencies:
            continue
        output.append(
            {
                "mapping_id": mapping_id,
                "observation_id": str(row["observation_id"]),
                "source_id": str(row["source_id"]),
                "source_contract_id": str(row["source_contract_id"]),
                "first_seen_utc": iso(raw_first),
                "mapped_utc": iso(mapped_at),
                "classification_version": str(row["classification_version"]),
                "mapper_contract_id": str(row["mapper_contract_id"]),
                "mapper_cohort_id": str(row["mapper_cohort_id"]),
                "material_sha256": str(raw["material_sha256"]),
                "observation_clock_source": str(raw["observation_clock_source"]),
                "collector_contract_id": str(raw["collector_contract_id"]),
                "collector_cohort_id": str(raw["collector_cohort_id"]),
                "mapping_payload": payload,
                "raw_payload_hash": digest(
                    json.dumps(raw_payload, sort_keys=True, separators=(",", ":"))
                ),
                "currencies": currencies,
            }
        )
    ending_fingerprints = (
        _input_fingerprint(mapping_database),
        _input_fingerprint(raw_database),
    )
    if ending_fingerprints == input_fingerprints:
        _INPUT_OBSERVATION_CACHE[cache_key] = (
            ending_fingerprints,
            tuple(output),
        )
    else:
        # A writer committed during the two-database read.  Preserve this
        # cycle's point-in-time result, but never cache it; the next cycle will
        # reload and observe the new immutable rows.
        _INPUT_OBSERVATION_CACHE.pop(cache_key, None)
    return output


def canonicalize_observations(
    observations: Sequence[Mapping[str, Any]],
    *,
    activation_utc: datetime = ACTIVATED_UTC,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for observation in observations:
        payload = observation.get("mapping_payload")
        if not isinstance(payload, Mapping):
            continue
        for currency in observation.get("currencies") or source_currencies(payload):
            event_id = canonical_event_id(payload, str(currency))
            grouped.setdefault((event_id, str(currency).upper()), []).append(observation)

    events: list[dict[str, Any]] = []
    for (event_id, currency), transports in grouped.items():
        transports = sorted(
            transports,
            key=lambda row: (str(row.get("first_seen_utc") or ""), str(row.get("mapping_id") or "")),
        )
        primary = transports[0]
        first_seen = parse_time(primary.get("first_seen_utc"))
        payload = primary.get("mapping_payload")
        if first_seen is None or not isinstance(payload, Mapping):
            continue
        factor_rows: dict[str, dict[str, Any]] = {}
        for transport in transports:
            transport_payload = transport.get("mapping_payload")
            transport_seen = parse_time(transport.get("first_seen_utc"))
            if not isinstance(transport_payload, Mapping) or transport_seen is None:
                continue
            for factor in structured_factors(transport_payload, transport_seen):
                key = str(factor["factor_key"])
                prior = factor_rows.get(key)
                if prior is None or str(factor["factor_known_utc"]) < str(prior["factor_known_utc"]):
                    factor_rows[key] = factor
        evidence_class = (
            "prospective_v1" if first_seen >= activation_utc else "preactivation_diagnostic"
        )
        # Keep structured release facts for later analog construction, without
        # inventing an orientation.  The full versioned classifier payload is
        # still retained per transport upstream; this compact event copy makes
        # the stable policy/release dimensions directly inspectable.
        source_fact_keys = (
            "source_id",
            "source_name",
            "source_kind",
            "source_role",
            "category",
            "event_name",
            "event_series_id",
            "event_family",
            "economic_event_family",
            "market_event_family",
            "release_family",
            "independent_episode_key",
            "market_episode_id",
            "economic_episode_id",
            "source_event_episode_id",
            "release_episode_id",
            "decision_cycle_id",
            "policy_episode_id",
            "scheduled_event_id",
            "scheduled_utc",
            "scheduled_release_utc",
            "reference_date",
            "reference_period",
            "topic_action",
            "policy_document_type",
            "official_policy_release",
            "policy_assertion_status",
            "policy_vote_split",
            "policy_dissent",
            "monetary_impulse",
            "transmission_mechanisms",
            "release_components",
            "source_native_components",
            "structured_component_change",
            "actual_value",
            "previous_value",
            "revised_previous_value",
            "consensus_value",
            "consensus_capture_state",
            "numeric_causal_known_utc",
            "detail_available_utc",
        )
        source_facts = {
            key: payload.get(key)
            for key in source_fact_keys
            if payload.get(key) not in (None, "", [], {})
        }
        # Forward-compatible explicit action/release fields are preserved when
        # an upstream mapper adds them under a stable name.
        for key, value in payload.items():
            lowered = str(key).lower()
            if (
                ("explicit_policy_action" in lowered or "release_factor" in lowered)
                and value not in (None, "", [], {})
            ):
                source_facts[str(key)] = value
        events.append(
            {
                "canonical_event_id": event_id,
                "market_episode_id": market_episode_id(
                    payload, currency, first_known_utc=first_seen
                ),
                "currency": currency,
                "first_known_utc": iso(first_seen),
                "evidence_class": evidence_class,
                "prospective_proof_eligible": evidence_class == "prospective_v1",
                "headline": str(payload.get("headline") or payload.get("event_name") or ""),
                "source_url": normalize_url(payload.get("source_url") or payload.get("publisher_url")),
                "authority": authority_key(payload),
                "category": slug(payload.get("category")),
                "event_series_id": slug(payload.get("event_series_id"), fallback=""),
                "topic_signature": slug(payload.get("topic_signature"), fallback=""),
                "source_role": slug(payload.get("source_role")),
                "transport_count": len(transports),
                "source_facts": source_facts,
                "transport_observations": [dict(row) for row in transports],
                "factors": sorted(factor_rows.values(), key=lambda row: row["factor_key"]),
                "research_only": True,
                "execution_eligible": False,
            }
        )
    return sorted(events, key=lambda row: (row["first_known_utc"], row["canonical_event_id"]))


@dataclass(frozen=True)
class CandlePoint:
    timestamp: datetime
    bid_open: float
    ask_open: float
    bid_close: float
    ask_close: float
    bid_high: float | None = None
    bid_low: float | None = None
    ask_high: float | None = None
    ask_low: float | None = None

    @property
    def mid_open(self) -> float:
        return (self.bid_open + self.ask_open) / 2.0

    @property
    def mid_close(self) -> float:
        return (self.bid_close + self.ask_close) / 2.0

    @property
    def close_timestamp(self) -> datetime:
        # OANDA M1 CSV timestamps identify bar *open*. Completed bid/ask close
        # values become known at the following minute boundary.
        return self.timestamp + timedelta(minutes=1)


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def compact_technical_snapshot(
    payload: Mapping[str, Any], currency: str
) -> dict[str, Any]:
    rows = payload.get("top_signals")
    if not isinstance(rows, list):
        rows = []
    relevant: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        instrument = str(row.get("instrument") or "").upper()
        if currency not in instrument.split("_"):
            continue
        horizon_rows = []
        for horizon in row.get("horizon_breakdown") or []:
            if not isinstance(horizon, Mapping):
                continue
            seconds = int(finite_number(horizon.get("horizon_sec")) or 0)
            if seconds not in {value * 60 for value in HORIZONS_MIN}:
                continue
            horizon_rows.append(
                {
                    "horizon_sec": seconds,
                    "direction": str(
                        horizon.get("direction_state")
                        or horizon.get("direction")
                        or "neutral"
                    ),
                    "confidence": finite_number(horizon.get("signal_confidence")),
                    "projected_net_pips": finite_number(horizon.get("projected_net_pips")),
                    "signal_eligible": bool(horizon.get("signal_eligible", False)),
                    "blocked_by": list(horizon.get("signal_blocked_by") or []),
                }
            )
        relevant.append(
            {
                "instrument": instrument,
                "direction": str(row.get("direction_state") or row.get("direction") or "neutral"),
                "confidence": finite_number(row.get("signal_confidence")),
                "signal_eligible": bool(row.get("signal_eligible", False)),
                "horizons": horizon_rows,
            }
        )
    return {
        "snapshot_utc": str(payload.get("updated_at") or ""),
        "currency": currency,
        "pair_rows": relevant,
        "research_only": True,
        "execution_eligible": False,
    }


def build_live_entry_snapshot(
    event: Mapping[str, Any],
    quote_payload: Mapping[str, Any],
    technical_payload: Mapping[str, Any],
    observed_utc: datetime,
) -> dict[str, Any]:
    first_known = parse_time(event.get("first_known_utc"))
    if first_known is None:
        raise ValueError("invalid event first-known clock")
    observed = observed_utc.astimezone(timezone.utc)
    latency = (observed - first_known).total_seconds()
    timing_quality = "prospective_exact_live_quote"
    reason = ""
    accepted: dict[str, dict[str, Any]] = {}
    if latency < 0.0 or latency > MAX_LIVE_CAPTURE_LATENCY_SECONDS:
        timing_quality = "prospective_clock_missed"
        reason = f"capture_latency_seconds:{latency:.3f}"
    else:
        quotes = quote_payload.get("quotes")
        if not isinstance(quotes, Mapping):
            quotes = {}
        for instrument, raw in sorted(quotes.items()):
            if not isinstance(raw, Mapping):
                continue
            bid = finite_number(raw.get("bid"))
            ask = finite_number(raw.get("ask"))
            pip = finite_number(raw.get("pip"))
            quote_time = parse_time(raw.get("time"))
            if (
                bid is None
                or ask is None
                or ask <= bid
                or pip is None
                or pip <= 0.0
                or quote_time is None
            ):
                continue
            age = (observed - quote_time).total_seconds()
            event_offset = (quote_time - first_known).total_seconds()
            if age < -2.0 or age > MAX_LIVE_QUOTE_AGE_SECONDS:
                continue
            if event_offset < -2.0:
                continue
            if event_offset > MAX_ENTRY_OFFSET_SECONDS:
                continue
            if quote_time >= first_known + timedelta(minutes=min(HORIZONS_MIN)):
                continue
            accepted[str(instrument).upper()] = {
                "bid": bid,
                "ask": ask,
                "pip": pip,
                "quote_time_utc": iso(quote_time),
                "age_seconds": round(age, 6),
                "event_offset_seconds": round(event_offset, 6),
            }
        if len(accepted) < 60:
            timing_quality = "prospective_quote_coverage_invalid"
            reason = f"fresh_quote_count:{len(accepted)}<60"
            accepted = {}
    snapshot_id = "source_entry_" + digest(
        event["canonical_event_id"], CONTRACT_ID
    )[:32]
    return {
        "entry_snapshot_id": snapshot_id,
        "canonical_event_id": event["canonical_event_id"],
        "currency": event["currency"],
        "event_first_known_utc": iso(first_known),
        "captured_utc": iso(observed),
        "capture_latency_seconds": round(latency, 6),
        "timing_quality": timing_quality,
        "invalid_reason": reason,
        "quote_count": len(accepted),
        "quotes": accepted,
        "technical_snapshot": compact_technical_snapshot(
            technical_payload, str(event["currency"])
        ),
        "research_only": True,
        "execution_eligible": False,
    }


def build_live_entry_snapshot_from_pre_map(
    event: Mapping[str, Any],
    pre_map_snapshot: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Revalidate a pre-semantic quote bundle under the frozen V1 gates.

    The mapper's verdict is never trusted directly.  The event clock, capture
    clock and every executable quote are passed back through
    ``build_live_entry_snapshot``.  This lets a valid t0+10 bundle survive a
    t0+20 semantic mapping while a genuinely late bundle remains invalid.
    """

    if str(pre_map_snapshot.get("capture_contract_id") or "") != (
        REQUIRED_PRE_MAP_QUOTE_CONTRACT
    ):
        return None
    event_clock = parse_time(event.get("first_known_utc"))
    attached_clock = parse_time(pre_map_snapshot.get("event_first_known_utc"))
    captured = parse_time(pre_map_snapshot.get("captured_utc"))
    if event_clock is None or attached_clock is None or captured is None:
        return None
    if event_clock != attached_clock:
        return None
    raw_quotes = pre_map_snapshot.get("quotes")
    quotes: dict[str, dict[str, Any]] = {}
    if isinstance(raw_quotes, Mapping):
        for instrument, raw in raw_quotes.items():
            if not isinstance(raw, Mapping):
                continue
            quotes[str(instrument).upper()] = {
                "bid": raw.get("bid"),
                "ask": raw.get("ask"),
                "pip": raw.get("pip"),
                "time": raw.get("quote_time_utc") or raw.get("time"),
            }
    rebuilt = build_live_entry_snapshot(
        event,
        {"quotes": quotes},
        {},
        captured,
    )
    rebuilt.update(
        {
            "capture_origin": "fast_mapper_pre_semantic_quote",
            "capture_contract_id": REQUIRED_PRE_MAP_QUOTE_CONTRACT,
            "upstream_observation_id": str(
                pre_map_snapshot.get("observation_id") or ""
            ),
            # Technical state was not captured by the pre-map quote contract.
            # Never attach a later semantic-time technical snapshot to an
            # earlier proof entry.
            "technical_snapshot": {
                "snapshot_utc": "",
                "pair_rows": [],
                "research_only": True,
                "execution_eligible": False,
                "availability": "not_captured_at_pre_map_clock",
            },
        }
    )
    return rebuilt


def pre_map_entry_snapshots(
    event: Mapping[str, Any],
) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    for transport in event.get("transport_observations") or []:
        if not isinstance(transport, Mapping):
            continue
        payload = transport.get("mapping_payload")
        if not isinstance(payload, Mapping):
            continue
        raw = payload.get("fast_lane_pre_map_quote_snapshot")
        if not isinstance(raw, Mapping):
            continue
        rebuilt = build_live_entry_snapshot_from_pre_map(event, raw)
        if rebuilt is not None:
            snapshots.append(rebuilt)
    return snapshots


_CANDLE_TAIL_BLOCK_BYTES = 64 * 1024


def _candle_point_from_csv_row(raw: Mapping[str, Any]) -> CandlePoint | None:
    stamp = parse_time(raw.get("datetime") or raw.get("time"))
    if stamp is None:
        return None
    try:
        return CandlePoint(
            timestamp=stamp,
            bid_open=float(raw["bid_open"]),
            ask_open=float(raw["ask_open"]),
            bid_close=float(raw["bid_close"]),
            ask_close=float(raw["ask_close"]),
            bid_high=finite_number(raw.get("bid_high")),
            bid_low=finite_number(raw.get("bid_low")),
            ask_high=finite_number(raw.get("ask_high")),
            ask_low=finite_number(raw.get("ask_low")),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _load_candle_file_window(
    path: Path,
    start_utc: datetime,
    end_utc: datetime,
) -> list[CandlePoint]:
    """Read only the recent append-only CSV window and return chronological rows.

    Candle files are chronological append-only M1 ledgers.  The former
    ``csv.DictReader`` path started at byte zero on every maturity cycle, even
    though a live response needs at most the two-hour tail.  Walking complete
    lines backwards from EOF preserves exactly the same inclusive time window
    while avoiding hundreds of megabytes of unrelated historical reads.

    If a replay requests an old window this intentionally continues walking
    backwards until it reaches that window; there is no approximation,
    carried-forward value, or change to the causal response contract.
    """

    values: list[CandlePoint] = []
    with path.open("rb") as handle:
        header_line = handle.readline()
        data_start = handle.tell()
        if not header_line:
            return values
        try:
            fieldnames = next(
                csv.reader(
                    [header_line.decode("utf-8").rstrip("\r\n")]
                )
            )
        except (UnicodeDecodeError, csv.Error, StopIteration):
            return values
        if not fieldnames:
            return values

        handle.seek(0, os.SEEK_END)
        position = handle.tell()
        remainder = b""
        reached_window_start = False

        def inspect_line(encoded: bytes) -> bool:
            """Return true once an ordered row older than the window is hit."""

            line = encoded.rstrip(b"\r")
            if not line:
                return False
            try:
                cells = next(csv.reader([line.decode("utf-8")]))
            except (UnicodeDecodeError, csv.Error, StopIteration):
                return False
            raw = dict(zip(fieldnames, cells))
            point = _candle_point_from_csv_row(raw)
            if point is None:
                return False
            if point.timestamp > end_utc:
                return False
            if point.timestamp < start_utc:
                return True
            values.append(point)
            return False

        while position > data_start and not reached_window_start:
            read_size = min(_CANDLE_TAIL_BLOCK_BYTES, position - data_start)
            position -= read_size
            handle.seek(position)
            combined = handle.read(read_size) + remainder
            lines = combined.split(b"\n")
            remainder = lines[0]
            for encoded in reversed(lines[1:]):
                if inspect_line(encoded):
                    reached_window_start = True
                    break

        if not reached_window_start and remainder:
            inspect_line(remainder)

    values.reverse()
    return values


def load_candle_panel(
    candle_root: Path,
    start_utc: datetime,
    end_utc: datetime,
) -> dict[str, list[CandlePoint]]:
    panel: dict[str, list[CandlePoint]] = {}
    for path in sorted(candle_root.glob("*_M1.csv")):
        instrument = path.name.removesuffix("_M1.csv")
        values = _load_candle_file_window(path, start_utc, end_utc)
        if values:
            panel[instrument] = values
    return panel


def point_at_or_after(
    rows: Sequence[CandlePoint], at: datetime, max_delay_seconds: int = MAX_CANDLE_DELAY_SECONDS
) -> CandlePoint | None:
    times = [row.timestamp for row in rows]
    index = bisect.bisect_left(times, at)
    if index >= len(rows):
        return None
    point = rows[index]
    if (point.timestamp - at).total_seconds() > max_delay_seconds:
        return None
    return point


def close_point_at_or_after(
    rows: Sequence[CandlePoint],
    at: datetime,
    max_delay_seconds: int = MAX_CANDLE_DELAY_SECONDS,
) -> CandlePoint | None:
    close_times = [row.close_timestamp for row in rows]
    index = bisect.bisect_left(close_times, at)
    if index >= len(rows):
        return None
    point = rows[index]
    if (point.close_timestamp - at).total_seconds() > max_delay_seconds:
        return None
    return point


def _entry_values(
    instrument: str,
    rows: Sequence[CandlePoint],
    start: datetime,
    entry_snapshot: Mapping[str, Any] | None,
) -> tuple[datetime, float, float, str] | None:
    if entry_snapshot is not None:
        if str(entry_snapshot.get("timing_quality")) != "prospective_exact_live_quote":
            return None
        quotes = entry_snapshot.get("quotes")
        raw = quotes.get(instrument) if isinstance(quotes, Mapping) else None
        if not isinstance(raw, Mapping):
            return None
        bid = finite_number(raw.get("bid"))
        ask = finite_number(raw.get("ask"))
        stamp = parse_time(raw.get("quote_time_utc"))
        if bid is None or ask is None or ask <= bid or stamp is None:
            return None
        offset = (stamp - start).total_seconds()
        if offset < -2.0 or offset > MAX_ENTRY_OFFSET_SECONDS:
            return None
        if stamp >= start + timedelta(minutes=min(HORIZONS_MIN)):
            return None
        return stamp, bid, ask, "prospective_exact_live_quote"
    point = point_at_or_after(rows, start)
    if point is None:
        return None
    return point.timestamp, point.bid_open, point.ask_open, "preactivation_m1_approximation"


def _solve_currency_factor(
    panel: Mapping[str, Sequence[CandlePoint]],
    start: datetime,
    target: datetime,
    entry_snapshot: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, tuple[datetime, float, float, CandlePoint]]]:
    observations: list[PairObservation] = []
    endpoints: dict[str, tuple[datetime, float, float, CandlePoint]] = {}
    for instrument, rows in sorted(panel.items()):
        try:
            base, quote = instrument.upper().split("_", 1)
        except ValueError:
            continue
        entry = _entry_values(instrument, rows, start, entry_snapshot)
        exit_point = close_point_at_or_after(rows, target)
        if entry is None or exit_point is None:
            continue
        entry_time, entry_bid, entry_ask, _ = entry
        entry_mid = (entry_bid + entry_ask) / 2.0
        exit_mid = exit_point.mid_close
        if entry_mid <= 0.0 or exit_mid <= 0.0:
            continue
        spread_bps = (entry_ask - entry_bid) / entry_mid * 10_000.0
        weight = min(
            float(SOLVER_POLICY["maximum_weight"]),
            1.0 / max(spread_bps, float(SOLVER_POLICY["spread_floor_bps"])),
        )
        observations.append(
            PairObservation(
                instrument=instrument,
                base_currency=base,
                quote_currency=quote,
                return_bps=math.log(exit_mid / entry_mid) * 10_000.0,
                spread_bps=spread_bps,
                weight=max(weight, 1e-9),
                start_epoch=int(entry_time.timestamp()),
                end_epoch=int(exit_point.close_timestamp.timestamp()),
                actual_observation_duration_sec=max(
                    0, int((exit_point.close_timestamp - entry_time).total_seconds())
                ),
                endpoint_age_sec=max(
                    0.0, (exit_point.close_timestamp - target).total_seconds()
                ),
                start_alignment_sec=max(0.0, (entry_time - start).total_seconds()),
                bid=exit_point.bid_close,
                ask=exit_point.ask_close,
                pip=pip_size(instrument),
            )
        )
        endpoints[instrument] = (entry_time, entry_bid, entry_ask, exit_point)
    result = solve_weighted_currency_state(
        observations, currencies=CURRENCIES, policy=SOLVER_POLICY
    )
    return result, endpoints


def _path_profile(
    rows: Sequence[CandlePoint],
    *,
    entry_time: datetime,
    exit_time: datetime,
    entry_bid: float,
    entry_ask: float,
    currency_is_base: bool,
    pip: float,
) -> dict[str, Any]:
    times = [row.timestamp for row in rows]
    first = bisect.bisect_left(times, entry_time)
    # ``exit_time`` is a completed-bar clock. Include bars whose open precedes
    # that close boundary and exclude the next bar opening at the boundary.
    last = bisect.bisect_left(times, exit_time)
    long_values: list[tuple[datetime, float, float, float]] = []
    short_values: list[tuple[datetime, float, float, float]] = []
    for point in rows[first:last]:
        long_terminal = (point.bid_close - entry_ask) / pip
        long_best = ((point.bid_high if point.bid_high is not None else point.bid_close) - entry_ask) / pip
        long_worst = ((point.bid_low if point.bid_low is not None else point.bid_close) - entry_ask) / pip
        short_terminal = (entry_bid - point.ask_close) / pip
        short_best = (entry_bid - (point.ask_low if point.ask_low is not None else point.ask_close)) / pip
        short_worst = (entry_bid - (point.ask_high if point.ask_high is not None else point.ask_close)) / pip
        long_values.append((point.close_timestamp, long_terminal, long_best, long_worst))
        short_values.append((point.close_timestamp, short_terminal, short_best, short_worst))

    def summarize(
        values: Sequence[tuple[datetime, float, float, float]]
    ) -> dict[str, Any]:
        terminal = [value for _, value, _, _ in values]
        favorable = [value for _, _, value, _ in values]
        adverse = [value for _, _, _, value in values]
        first_clear = next((stamp for stamp, _, best, _ in values if best > 0.0), None)
        return {
            "terminal_executable_pips": round(terminal[-1], 6) if terminal else None,
            "mfe_pips": round(max(favorable), 6) if favorable else None,
            "mae_pips": round(min(adverse), 6) if adverse else None,
            "first_cost_clear_utc": iso(first_clear) if first_clear else None,
            "first_cost_clear_seconds": (
                round((first_clear - entry_time).total_seconds(), 6)
                if first_clear
                else None
            ),
        }

    return {
        "currency_strengthening": summarize(long_values if currency_is_base else short_values),
        "currency_weakening": summarize(short_values if currency_is_base else long_values),
        "extrema_sampling": "executable_m1_bid_ask_high_low",
    }


def _response_shape(final_bps: float, early_bps: float | None, horizon_min: int) -> str:
    if abs(final_bps) < NULL_RESPONSE_BPS:
        return "null"
    if horizon_min <= 1 or early_bps is None:
        return "immediate"
    if abs(early_bps) >= NULL_RESPONSE_BPS:
        if (early_bps > 0.0) != (final_bps > 0.0):
            return "reversal"
        return "immediate"
    return "delayed"


def compute_event_response(
    event: Mapping[str, Any],
    panel: Mapping[str, Sequence[CandlePoint]],
    horizon_min: int,
    *,
    entry_snapshot: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    if int(horizon_min) not in HORIZONS_MIN:
        raise ValueError(f"unsupported horizon:{horizon_min}")
    start = parse_time(event.get("first_known_utc"))
    if start is None:
        return None
    if bool(event.get("prospective_proof_eligible")) and (
        entry_snapshot is None
        or str(entry_snapshot.get("timing_quality")) != "prospective_exact_live_quote"
    ):
        return None
    target = start + timedelta(minutes=int(horizon_min))
    currency = str(event.get("currency") or "").upper()
    solver, endpoints = _solve_currency_factor(panel, start, target, entry_snapshot)
    strengths = solver.get("strengths_bps")
    if solver.get("status") not in {"ok", "degraded_disconnected"} or not isinstance(strengths, Mapping):
        return None
    if currency not in strengths:
        return None
    final_bps = float(strengths[currency])
    early_target = start + timedelta(minutes=min(1, int(horizon_min)))
    early_solver, _ = _solve_currency_factor(panel, start, early_target, entry_snapshot)
    early_strengths = early_solver.get("strengths_bps")
    early_bps = (
        float(early_strengths[currency])
        if isinstance(early_strengths, Mapping) and currency in early_strengths
        else None
    )
    ordered = sorted(
        ((str(code), float(value)) for code, value in strengths.items()),
        key=lambda item: item[1],
        reverse=True,
    )
    rank = next(index + 1 for index, item in enumerate(ordered) if item[0] == currency)
    paths: list[dict[str, Any]] = []
    for instrument, endpoint in sorted(endpoints.items()):
        base, quote = instrument.split("_", 1)
        if currency not in {base, quote}:
            continue
        rows = panel[instrument]
        entry_time, entry_bid, entry_ask, exit_point = endpoint
        pip = pip_size(instrument)
        profiles = _path_profile(
            rows,
            entry_time=entry_time,
            exit_time=exit_point.close_timestamp,
            entry_bid=entry_bid,
            entry_ask=entry_ask,
            currency_is_base=currency == base,
            pip=pip,
        )
        paths.append(
            {
                "instrument": instrument,
                "currency_leg": "base" if currency == base else "quote",
                "entry_utc": iso(entry_time),
                "exit_utc": iso(exit_point.close_timestamp),
                "entry_clock_offset_seconds": round(
                    (entry_time - start).total_seconds(), 6
                ),
                "exit_target_delay_seconds": round(
                    (exit_point.close_timestamp - target).total_seconds(), 6
                ),
                "entry_spread_pips": round((entry_ask - entry_bid) / pip, 6),
                "pair_midpoint_move_pips": round(
                    (exit_point.mid_close - (entry_bid + entry_ask) / 2.0) / pip, 6
                ),
                **profiles,
            }
        )
    if not paths:
        return None
    entry_method = (
        "prospective_exact_live_quote"
        if entry_snapshot is not None
        else "preactivation_m1_approximation"
    )
    timing_quality = (
        "prospective_exact_entry_completed_m1_exit"
        if entry_snapshot is not None
        else "diagnostic_approximate_not_proof"
    )
    solver_diagnostics = {
        key: solver.get(key)
        for key in (
            "status",
            "active_currencies",
            "unavailable_currencies",
            "components",
            "observation_count",
            "duplicate_observation_count",
            "weighted_observation_equivalent",
            "matrix_rank",
            "condition_number",
            "return_clip_bps",
            "median_abs_pair_return_bps",
            "residual_rmse_bps",
            "weighted_residual_sigma_bps",
            "pair_counts",
        )
    }
    response_id = "source_response_" + digest(
        event.get("canonical_event_id"), currency, horizon_min, CONTRACT_ID
    )[:32]
    return {
        "response_id": response_id,
        "canonical_event_id": str(event.get("canonical_event_id") or ""),
        "currency": currency,
        "horizon_min": int(horizon_min),
        "event_clock_utc": iso(start),
        "maturity_utc": iso(target),
        "maturity_state": "valid_canonical_ls_factor_and_bid_ask_paths",
        "currency_factor_bps": round(final_bps, 6),
        "absolute_currency_factor_bps": round(abs(final_bps), 6),
        "currency_rank": rank,
        "factor_currency_count": len(strengths),
        "usable_pair_count": int(solver.get("observation_count") or 0),
        "currency_pair_path_count": len(paths),
        "entry_method": entry_method,
        "response_timing_quality": timing_quality,
        "early_currency_factor_bps": None if early_bps is None else round(early_bps, 6),
        "response_shape": _response_shape(final_bps, early_bps, int(horizon_min)),
        "solver_status": str(solver.get("status") or "unknown"),
        "solver_condition_number": finite_number(solver.get("condition_number")),
        "solver_weighted_observation_equivalent": finite_number(
            solver.get("weighted_observation_equivalent")
        ),
        "solver_diagnostics": solver_diagnostics,
        "maximum_exit_target_delay_seconds": max(
            float(row["exit_target_delay_seconds"]) for row in paths
        ),
        "both_executable_paths": paths,
        "selected_side": None,
        "research_only": True,
        "execution_eligible": False,
    }


def open_output_database(path: Path = OUTPUT_DATABASE) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA busy_timeout=30000")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS source_event_observation (
            canonical_event_id TEXT PRIMARY KEY,
            currency TEXT NOT NULL,
            first_known_utc TEXT NOT NULL,
            activated_utc TEXT NOT NULL,
            evidence_class TEXT NOT NULL,
            prospective_proof_eligible INTEGER NOT NULL CHECK(prospective_proof_eligible IN (0,1)),
            headline TEXT NOT NULL,
            source_url TEXT NOT NULL,
            authority TEXT NOT NULL,
            category TEXT NOT NULL,
            event_series_id TEXT NOT NULL,
            topic_signature TEXT NOT NULL,
            source_role TEXT NOT NULL,
            event_payload_json TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
            can_promote INTEGER NOT NULL CHECK(can_promote=0),
            contract_id TEXT NOT NULL,
            cohort_id TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS source_event_transport (
            transport_id TEXT PRIMARY KEY,
            canonical_event_id TEXT NOT NULL,
            mapping_id TEXT NOT NULL,
            observation_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            source_contract_id TEXT NOT NULL,
            first_seen_utc TEXT NOT NULL,
            mapped_utc TEXT NOT NULL,
            observation_clock_source TEXT NOT NULL,
            material_sha256 TEXT NOT NULL,
            classification_version TEXT NOT NULL,
            mapper_contract_id TEXT NOT NULL,
            transport_payload_json TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            contract_id TEXT NOT NULL,
            cohort_id TEXT NOT NULL,
            FOREIGN KEY(canonical_event_id) REFERENCES source_event_observation(canonical_event_id)
        );
        CREATE TABLE IF NOT EXISTS source_event_episode (
            canonical_event_id TEXT PRIMARY KEY,
            market_episode_id TEXT NOT NULL,
            episode_contract TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            contract_id TEXT NOT NULL,
            cohort_id TEXT NOT NULL,
            FOREIGN KEY(canonical_event_id) REFERENCES source_event_observation(canonical_event_id)
        );
        CREATE TABLE IF NOT EXISTS source_factor_observation (
            factor_observation_id TEXT PRIMARY KEY,
            canonical_event_id TEXT NOT NULL,
            currency TEXT NOT NULL,
            factor_key TEXT NOT NULL,
            factor_type TEXT NOT NULL,
            factor_value TEXT NOT NULL,
            factor_known_utc TEXT NOT NULL,
            evidence_class TEXT NOT NULL,
            prospective_proof_eligible INTEGER NOT NULL CHECK(prospective_proof_eligible IN (0,1)),
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            contract_id TEXT NOT NULL,
            cohort_id TEXT NOT NULL,
            UNIQUE(canonical_event_id,currency,factor_key),
            FOREIGN KEY(canonical_event_id) REFERENCES source_event_observation(canonical_event_id)
        );
        CREATE TABLE IF NOT EXISTS source_event_entry_snapshot (
            entry_snapshot_id TEXT PRIMARY KEY,
            canonical_event_id TEXT NOT NULL UNIQUE,
            currency TEXT NOT NULL,
            event_first_known_utc TEXT NOT NULL,
            captured_utc TEXT NOT NULL,
            capture_latency_seconds REAL NOT NULL,
            timing_quality TEXT NOT NULL,
            invalid_reason TEXT NOT NULL,
            quote_count INTEGER NOT NULL,
            quotes_json TEXT NOT NULL,
            technical_snapshot_json TEXT NOT NULL,
            snapshot_payload_json TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
            can_promote INTEGER NOT NULL CHECK(can_promote=0),
            contract_id TEXT NOT NULL,
            cohort_id TEXT NOT NULL,
            FOREIGN KEY(canonical_event_id) REFERENCES source_event_observation(canonical_event_id)
        );
        CREATE TABLE IF NOT EXISTS source_event_response (
            response_id TEXT PRIMARY KEY,
            canonical_event_id TEXT NOT NULL,
            currency TEXT NOT NULL,
            horizon_min INTEGER NOT NULL CHECK(horizon_min IN (1,5,15,30,60,120)),
            event_clock_utc TEXT NOT NULL,
            maturity_utc TEXT NOT NULL,
            maturity_state TEXT NOT NULL,
            currency_factor_bps REAL NOT NULL,
            absolute_currency_factor_bps REAL NOT NULL,
            currency_rank INTEGER NOT NULL,
            factor_currency_count INTEGER NOT NULL,
            usable_pair_count INTEGER NOT NULL,
            currency_pair_path_count INTEGER NOT NULL,
            entry_method TEXT NOT NULL,
            response_timing_quality TEXT NOT NULL,
            early_currency_factor_bps REAL,
            response_shape TEXT NOT NULL,
            solver_status TEXT NOT NULL,
            solver_condition_number REAL,
            solver_weighted_observation_equivalent REAL,
            solver_diagnostics_json TEXT NOT NULL,
            both_executable_paths_json TEXT NOT NULL,
            selected_side TEXT CHECK(selected_side IS NULL),
            response_payload_json TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            contract_id TEXT NOT NULL,
            cohort_id TEXT NOT NULL,
            UNIQUE(canonical_event_id,horizon_min),
            FOREIGN KEY(canonical_event_id) REFERENCES source_event_observation(canonical_event_id)
        );
        CREATE TABLE IF NOT EXISTS source_response_terminal_gap (
            gap_id TEXT PRIMARY KEY,
            canonical_event_id TEXT NOT NULL,
            currency TEXT NOT NULL,
            horizon_min INTEGER NOT NULL CHECK(horizon_min IN (1,5,15,30,60,120)),
            target_utc TEXT NOT NULL,
            recorded_utc TEXT NOT NULL,
            reason TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            contract_id TEXT NOT NULL,
            cohort_id TEXT NOT NULL,
            UNIQUE(canonical_event_id,horizon_min),
            FOREIGN KEY(canonical_event_id) REFERENCES source_event_observation(canonical_event_id)
        );
        CREATE TABLE IF NOT EXISTS source_factor_forecast (
            forecast_id TEXT PRIMARY KEY,
            factor_observation_id TEXT NOT NULL,
            canonical_event_id TEXT NOT NULL,
            currency TEXT NOT NULL,
            factor_key TEXT NOT NULL,
            horizon_min INTEGER NOT NULL CHECK(horizon_min IN (1,5,15,30,60,120)),
            issued_utc TEXT NOT NULL,
            training_cutoff_utc TEXT NOT NULL,
            forecast_state TEXT NOT NULL,
            abstain_reason TEXT NOT NULL,
            backoff_level TEXT NOT NULL,
            backoff_key TEXT NOT NULL,
            raw_n INTEGER NOT NULL,
            effective_event_n INTEGER NOT NULL,
            probability_strengthening REAL,
            predicted_currency_factor_bps REAL,
            predicted_absolute_factor_bps REAL,
            training_latest_maturity_utc TEXT,
            forecast_payload_json TEXT NOT NULL,
            evidence_class TEXT NOT NULL,
            prospective_proof_eligible INTEGER NOT NULL CHECK(prospective_proof_eligible IN (0,1)),
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
            can_promote INTEGER NOT NULL CHECK(can_promote=0),
            contract_id TEXT NOT NULL,
            cohort_id TEXT NOT NULL,
            UNIQUE(factor_observation_id,horizon_min),
            FOREIGN KEY(factor_observation_id) REFERENCES source_factor_observation(factor_observation_id)
        );
        CREATE INDEX IF NOT EXISTS idx_source_factor_lookup
            ON source_factor_observation(currency,factor_type,factor_key,factor_known_utc);
        CREATE INDEX IF NOT EXISTS idx_source_response_maturity
            ON source_event_response(horizon_min,maturity_utc);
        """
    )
    for table in (
        "source_event_observation",
        "source_event_transport",
        "source_event_episode",
        "source_factor_observation",
        "source_event_entry_snapshot",
        "source_event_response",
        "source_response_terminal_gap",
        "source_factor_forecast",
    ):
        connection.executescript(
            f"""
            CREATE TRIGGER IF NOT EXISTS {table}_append_only_update
            BEFORE UPDATE ON {table} BEGIN
              SELECT RAISE(ABORT,'append_only:{table}');
            END;
            CREATE TRIGGER IF NOT EXISTS {table}_append_only_delete
            BEFORE DELETE ON {table} BEGIN
              SELECT RAISE(ABORT,'append_only:{table}');
            END;
            """
        )
    connection.commit()
    return connection


def insert_events(connection: sqlite3.Connection, events: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts = {"events": 0, "episodes": 0, "transports": 0, "factors": 0}
    for event in events:
        before = connection.total_changes
        event_payload = {key: value for key, value in dict(event).items() if key != "transport_observations"}
        connection.execute(
            """
            INSERT OR IGNORE INTO source_event_observation (
                canonical_event_id,currency,first_known_utc,activated_utc,
                evidence_class,prospective_proof_eligible,headline,source_url,
                authority,category,event_series_id,topic_signature,source_role,
                event_payload_json,research_only,execution_eligible,can_authorize,
                can_promote,contract_id,cohort_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0,0,0,?,?)
            """,
            (
                event["canonical_event_id"], event["currency"], event["first_known_utc"],
                iso(ACTIVATED_UTC), event["evidence_class"],
                int(bool(event["prospective_proof_eligible"])), event["headline"],
                event["source_url"], event["authority"], event["category"],
                event["event_series_id"], event["topic_signature"], event["source_role"],
                json.dumps(event_payload, sort_keys=True, separators=(",", ":"), default=str),
                CONTRACT_ID, COHORT_ID,
            ),
        )
        counts["events"] += connection.total_changes - before
        before = connection.total_changes
        connection.execute(
            """
            INSERT OR IGNORE INTO source_event_episode (
                canonical_event_id,market_episode_id,episode_contract,
                research_only,contract_id,cohort_id
            ) VALUES (?,?,?,1,?,?)
            """,
            (
                event["canonical_event_id"],
                event.get("market_episode_id")
                or "market_episode_" + digest(event["canonical_event_id"])[:32],
                "upstream_or_authority_currency_family_clock_episode_v2",
                CONTRACT_ID,
                COHORT_ID,
            ),
        )
        counts["episodes"] += connection.total_changes - before
        for transport in event.get("transport_observations") or []:
            transport_id = "source_transport_" + digest(
                transport.get("mapping_id"), event["canonical_event_id"], CONTRACT_ID
            )[:32]
            payload = {
                "mapping_id": transport.get("mapping_id"),
                "observation_id": transport.get("observation_id"),
                "raw_payload_hash": transport.get("raw_payload_hash"),
                "collector_contract_id": transport.get("collector_contract_id"),
                "collector_cohort_id": transport.get("collector_cohort_id"),
            }
            before = connection.total_changes
            connection.execute(
                """
                INSERT OR IGNORE INTO source_event_transport (
                    transport_id,canonical_event_id,mapping_id,observation_id,
                    source_id,source_contract_id,first_seen_utc,mapped_utc,
                    observation_clock_source,material_sha256,
                    classification_version,mapper_contract_id,
                    transport_payload_json,research_only,contract_id,cohort_id
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?)
                """,
                (
                    transport_id, event["canonical_event_id"], transport.get("mapping_id"),
                    transport.get("observation_id"), transport.get("source_id"),
                    transport.get("source_contract_id"), transport.get("first_seen_utc"),
                    transport.get("mapped_utc"), transport.get("observation_clock_source"),
                    transport.get("material_sha256"), transport.get("classification_version"),
                    transport.get("mapper_contract_id"),
                    json.dumps(payload, sort_keys=True, separators=(",", ":")),
                    CONTRACT_ID, COHORT_ID,
                ),
            )
            counts["transports"] += connection.total_changes - before
        for factor in event.get("factors") or []:
            factor_id = "source_factor_" + digest(
                event["canonical_event_id"], event["currency"], factor["factor_key"], CONTRACT_ID
            )[:32]
            factor_clock = parse_time(factor.get("factor_known_utc"))
            event_clock = parse_time(event.get("first_known_utc"))
            # V1 has an exact live entry bundle at the event clock. A numeric
            # or attachment-derived factor learned later is retained with its
            # true clock, but cannot borrow the earlier event response as proof.
            # It remains diagnostic until a dedicated factor-clock live-entry
            # cohort is opened.
            response_clock_compatible = bool(
                factor_clock is not None
                and event_clock is not None
                and abs((factor_clock - event_clock).total_seconds()) <= 1.0
            )
            factor_proof_eligible = bool(event["prospective_proof_eligible"]) and response_clock_compatible
            before = connection.total_changes
            connection.execute(
                """
                INSERT OR IGNORE INTO source_factor_observation (
                    factor_observation_id,canonical_event_id,currency,factor_key,
                    factor_type,factor_value,factor_known_utc,evidence_class,
                    prospective_proof_eligible,research_only,execution_eligible,
                    contract_id,cohort_id
                ) VALUES (?,?,?,?,?,?,?,?,?,1,0,?,?)
                """,
                (
                    factor_id, event["canonical_event_id"], event["currency"],
                    factor["factor_key"], factor["factor_type"], factor["factor_value"],
                    factor["factor_known_utc"], event["evidence_class"],
                    int(factor_proof_eligible), CONTRACT_ID, COHORT_ID,
                ),
            )
            counts["factors"] += connection.total_changes - before
    connection.commit()
    return counts


def insert_live_entry_snapshots(
    connection: sqlite3.Connection,
    events: Sequence[Mapping[str, Any]],
    quote_payload: Mapping[str, Any],
    technical_payload: Mapping[str, Any],
    observed_utc: datetime,
) -> int:
    existing = {
        str(row[0])
        for row in connection.execute(
            "SELECT canonical_event_id FROM source_event_entry_snapshot"
        ).fetchall()
    }
    inserted = 0
    for event in events:
        if not bool(event.get("prospective_proof_eligible")):
            continue
        if str(event["canonical_event_id"]) in existing:
            continue
        candidates = pre_map_entry_snapshots(event)
        current = build_live_entry_snapshot(
            event, quote_payload, technical_payload, observed_utc
        )
        current["capture_origin"] = "response_map_semantic_cycle"
        candidates.append(current)
        exact = [
            row
            for row in candidates
            if str(row.get("timing_quality"))
            == "prospective_exact_live_quote"
        ]
        pool = exact or candidates
        snapshot = min(
            pool,
            key=lambda row: (
                float(row.get("capture_latency_seconds") or float("inf")),
                str(row.get("captured_utc") or ""),
            ),
        )
        before = connection.total_changes
        connection.execute(
            """
            INSERT OR IGNORE INTO source_event_entry_snapshot (
                entry_snapshot_id,canonical_event_id,currency,event_first_known_utc,
                captured_utc,capture_latency_seconds,timing_quality,invalid_reason,
                quote_count,quotes_json,technical_snapshot_json,snapshot_payload_json,
                research_only,execution_eligible,can_authorize,can_promote,
                contract_id,cohort_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,0,0,0,?,?)
            """,
            (
                snapshot["entry_snapshot_id"], snapshot["canonical_event_id"],
                snapshot["currency"], snapshot["event_first_known_utc"],
                snapshot["captured_utc"], snapshot["capture_latency_seconds"],
                snapshot["timing_quality"], snapshot["invalid_reason"],
                snapshot["quote_count"],
                json.dumps(snapshot["quotes"], sort_keys=True, separators=(",", ":")),
                json.dumps(snapshot["technical_snapshot"], sort_keys=True, separators=(",", ":")),
                json.dumps(snapshot, sort_keys=True, separators=(",", ":"), default=str),
                CONTRACT_ID, COHORT_ID,
            ),
        )
        inserted += connection.total_changes - before
    connection.commit()
    return inserted


def load_entry_snapshots(connection: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT canonical_event_id,captured_utc,timing_quality,quotes_json,
               technical_snapshot_json,capture_latency_seconds
        FROM source_event_entry_snapshot
        """
    ).fetchall()
    return {
        str(row[0]): {
            "captured_utc": row[1],
            "timing_quality": row[2],
            "quotes": json.loads(str(row[3])),
            "technical_snapshot": json.loads(str(row[4])),
            "capture_latency_seconds": float(row[5]),
        }
        for row in rows
    }


def insert_responses(connection: sqlite3.Connection, responses: Iterable[Mapping[str, Any]]) -> int:
    inserted = 0
    for response in responses:
        before = connection.total_changes
        connection.execute(
            """
            INSERT OR IGNORE INTO source_event_response (
                response_id,canonical_event_id,currency,horizon_min,
                event_clock_utc,maturity_utc,maturity_state,currency_factor_bps,
                absolute_currency_factor_bps,currency_rank,factor_currency_count,
                usable_pair_count,currency_pair_path_count,entry_method,
                response_timing_quality,early_currency_factor_bps,response_shape,
                solver_status,solver_condition_number,
                solver_weighted_observation_equivalent,solver_diagnostics_json,
                both_executable_paths_json,selected_side,response_payload_json,
                research_only,execution_eligible,
                contract_id,cohort_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,?,1,0,?,?)
            """,
            (
                response["response_id"], response["canonical_event_id"], response["currency"],
                response["horizon_min"], response["event_clock_utc"], response["maturity_utc"],
                response["maturity_state"], response["currency_factor_bps"],
                response["absolute_currency_factor_bps"], response["currency_rank"],
                response["factor_currency_count"], response["usable_pair_count"],
                response["currency_pair_path_count"], response["entry_method"],
                response["response_timing_quality"], response["early_currency_factor_bps"],
                response["response_shape"], response["solver_status"],
                response["solver_condition_number"],
                response["solver_weighted_observation_equivalent"],
                json.dumps(response["solver_diagnostics"], sort_keys=True, separators=(",", ":")),
                json.dumps(response["both_executable_paths"], sort_keys=True, separators=(",", ":")),
                json.dumps(dict(response), sort_keys=True, separators=(",", ":"), default=str),
                CONTRACT_ID, COHORT_ID,
            ),
        )
        inserted += connection.total_changes - before
    connection.commit()
    return inserted


def insert_terminal_gaps(
    connection: sqlite3.Connection,
    gaps: Iterable[Mapping[str, Any]],
) -> int:
    inserted = 0
    for gap in gaps:
        before = connection.total_changes
        gap_id = "source_gap_" + digest(
            gap["canonical_event_id"], gap["horizon_min"], CONTRACT_ID
        )[:32]
        connection.execute(
            """
            INSERT OR IGNORE INTO source_response_terminal_gap (
                gap_id,canonical_event_id,currency,horizon_min,target_utc,
                recorded_utc,reason,research_only,execution_eligible,
                contract_id,cohort_id
            ) VALUES (?,?,?,?,?,?,?,1,0,?,?)
            """,
            (
                gap_id, gap["canonical_event_id"], gap["currency"],
                gap["horizon_min"], gap["target_utc"], gap["recorded_utc"],
                gap["reason"], CONTRACT_ID, COHORT_ID,
            ),
        )
        inserted += connection.total_changes - before
    connection.commit()
    return inserted


def _eligible_training_rows(
    connection: sqlite3.Connection,
    factor: Mapping[str, Any],
    horizon_min: int,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    base_query = """
        SELECT sf.canonical_event_id,ep.market_episode_id,sf.currency,
               sf.factor_key,sf.factor_type,e.authority,r.currency_factor_bps,
               r.absolute_currency_factor_bps,r.maturity_utc
        FROM source_factor_observation sf
        JOIN source_event_observation e ON e.canonical_event_id=sf.canonical_event_id
        JOIN source_event_episode ep ON ep.canonical_event_id=sf.canonical_event_id
        JOIN source_event_response r ON r.canonical_event_id=sf.canonical_event_id
        WHERE r.horizon_min=? AND r.maturity_state='valid_canonical_ls_factor_and_bid_ask_paths'
          AND r.maturity_utc<=? AND sf.canonical_event_id<>?
          AND sf.factor_known_utc=r.event_clock_utc
          AND ep.market_episode_id<>?
    """
    return [
        {
            "canonical_event_id": row[0], "market_episode_id": row[1],
            "currency": row[2], "factor_key": row[3],
            "factor_type": row[4], "authority": row[5], "factor_bps": float(row[6]),
            "absolute_bps": float(row[7]), "maturity_utc": row[8],
        }
        for row in connection.execute(
            base_query,
            (
                int(horizon_min), iso(cutoff), factor["canonical_event_id"],
                factor.get("market_episode_id")
                or "market_episode_" + digest(factor["canonical_event_id"])[:32],
            ),
        ).fetchall()
    ]


def _ordered_support(
    rows: Sequence[Mapping[str, Any]],
    factor: Mapping[str, Any],
    minimum_n: int,
) -> tuple[str, str, list[dict[str, Any]]]:
    levels = (
        (
            "currency_authority_factor",
            f"{factor['currency']}|{factor['authority']}|{factor['factor_key']}",
            lambda row: row["currency"] == factor["currency"]
            and row["authority"] == factor["authority"]
            and row["factor_key"] == factor["factor_key"],
        ),
        (
            "currency_factor",
            f"{factor['currency']}|{factor['factor_key']}",
            lambda row: row["currency"] == factor["currency"]
            and row["factor_key"] == factor["factor_key"],
        ),
        (
            "factor",
            str(factor["factor_key"]),
            lambda row: row["factor_key"] == factor["factor_key"],
        ),
        (
            "currency_factor_type",
            f"{factor['currency']}|{factor['factor_type']}",
            lambda row: row["currency"] == factor["currency"]
            and row["factor_type"] == factor["factor_type"],
        ),
        (
            "factor_type",
            str(factor["factor_type"]),
            lambda row: row["factor_type"] == factor["factor_type"],
        ),
    )
    for level, key, predicate in levels:
        matched_by_episode: dict[str, dict[str, Any]] = {}
        for row in rows:
            if predicate(row):
                matched_by_episode.setdefault(row["market_episode_id"], row)
        matched = sorted(matched_by_episode.values(), key=lambda row: row["maturity_utc"])
        if len(matched) >= minimum_n:
            return level, key, matched
    # Preserve the most specific support count in the abstention record.
    matched = [row for row in rows if row["currency"] == factor["currency"] and row["factor_key"] == factor["factor_key"]]
    by_episode = {row["market_episode_id"]: row for row in matched}
    return "none", "", sorted(by_episode.values(), key=lambda row: row["maturity_utc"])


def build_prequential_forecast(
    connection: sqlite3.Connection,
    factor: Mapping[str, Any],
    horizon_min: int,
    *,
    minimum_n: int = MIN_EFFECTIVE_N,
) -> dict[str, Any]:
    cutoff = parse_time(factor.get("factor_known_utc"))
    if cutoff is None:
        raise ValueError("factor_known_utc is invalid")
    all_rows = _eligible_training_rows(connection, factor, horizon_min, cutoff)
    level, key, rows = _ordered_support(all_rows, factor, minimum_n)
    effective_n = len({row["market_episode_id"] for row in rows})
    forecast_state = "forecast" if level != "none" and effective_n >= minimum_n else "abstain"
    values = [float(row["factor_bps"]) for row in rows]
    absolute = [float(row["absolute_bps"]) for row in rows]
    probability = (
        (sum(value > 0.0 for value in values) + 0.5) / (len(values) + 1.0)
        if forecast_state == "forecast"
        else None
    )
    forecast_id = "source_forecast_" + digest(
        factor["factor_observation_id"], horizon_min, CONTRACT_ID
    )[:32]
    return {
        "forecast_id": forecast_id,
        "factor_observation_id": factor["factor_observation_id"],
        "canonical_event_id": factor["canonical_event_id"],
        "currency": factor["currency"],
        "authority": factor["authority"],
        "factor_key": factor["factor_key"],
        "factor_type": factor["factor_type"],
        "horizon_min": int(horizon_min),
        "issued_utc": iso(cutoff),
        "training_cutoff_utc": iso(cutoff),
        "forecast_state": forecast_state,
        "abstain_reason": "" if forecast_state == "forecast" else f"low_effective_n:{effective_n}<{minimum_n}",
        "backoff_level": level,
        "backoff_key": key,
        "raw_n": len(rows),
        "effective_event_n": effective_n,
        "probability_strengthening": probability,
        "predicted_currency_factor_bps": statistics.mean(values) if forecast_state == "forecast" else None,
        "predicted_absolute_factor_bps": statistics.mean(absolute) if forecast_state == "forecast" else None,
        "training_latest_maturity_utc": max((row["maturity_utc"] for row in rows), default=None),
        "training_event_ids": sorted({row["canonical_event_id"] for row in rows}),
        "training_market_episode_ids": sorted(
            {row["market_episode_id"] for row in rows}
        ),
        "evidence_class": factor["evidence_class"],
        "prospective_proof_eligible": bool(factor["prospective_proof_eligible"]),
        "selected_pair_or_side": None,
        "research_only": True,
        "execution_eligible": False,
    }


def insert_forecasts(connection: sqlite3.Connection, *, minimum_n: int = MIN_EFFECTIVE_N) -> int:
    factor_rows = connection.execute(
        """
        SELECT sf.factor_observation_id,sf.canonical_event_id,sf.currency,
               sf.factor_key,sf.factor_type,sf.factor_known_utc,sf.evidence_class,
               sf.prospective_proof_eligible,e.authority,
               COALESCE(es.timing_quality,''),ep.market_episode_id
        FROM source_factor_observation sf
        JOIN source_event_observation e ON e.canonical_event_id=sf.canonical_event_id
        JOIN source_event_episode ep ON ep.canonical_event_id=sf.canonical_event_id
        LEFT JOIN source_event_entry_snapshot es
          ON es.canonical_event_id=sf.canonical_event_id
        ORDER BY sf.factor_known_utc,sf.factor_observation_id
        """
    ).fetchall()
    inserted = 0
    for row in factor_rows:
        factor = {
            "factor_observation_id": row[0], "canonical_event_id": row[1],
            "currency": row[2], "factor_key": row[3], "factor_type": row[4],
            "factor_known_utc": row[5], "evidence_class": row[6],
            "prospective_proof_eligible": bool(row[7])
            and str(row[9]) == "prospective_exact_live_quote",
            "authority": row[8],
            "market_episode_id": row[10],
        }
        for horizon in HORIZONS_MIN:
            forecast = build_prequential_forecast(
                connection, factor, horizon, minimum_n=minimum_n
            )
            before = connection.total_changes
            connection.execute(
                """
                INSERT OR IGNORE INTO source_factor_forecast (
                    forecast_id,factor_observation_id,canonical_event_id,currency,
                    factor_key,horizon_min,issued_utc,training_cutoff_utc,
                    forecast_state,abstain_reason,backoff_level,backoff_key,raw_n,
                    effective_event_n,probability_strengthening,
                    predicted_currency_factor_bps,predicted_absolute_factor_bps,
                    training_latest_maturity_utc,forecast_payload_json,
                    evidence_class,prospective_proof_eligible,research_only,
                    execution_eligible,can_authorize,can_promote,contract_id,cohort_id
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0,0,0,?,?)
                """,
                (
                    forecast["forecast_id"], forecast["factor_observation_id"],
                    forecast["canonical_event_id"], forecast["currency"],
                    forecast["factor_key"], forecast["horizon_min"],
                    forecast["issued_utc"], forecast["training_cutoff_utc"],
                    forecast["forecast_state"], forecast["abstain_reason"],
                    forecast["backoff_level"], forecast["backoff_key"],
                    forecast["raw_n"], forecast["effective_event_n"],
                    forecast["probability_strengthening"],
                    forecast["predicted_currency_factor_bps"],
                    forecast["predicted_absolute_factor_bps"],
                    forecast["training_latest_maturity_utc"],
                    json.dumps(forecast, sort_keys=True, separators=(",", ":"), default=str),
                    forecast["evidence_class"],
                    int(bool(forecast["prospective_proof_eligible"])),
                    CONTRACT_ID, COHORT_ID,
                ),
            )
            inserted += connection.total_changes - before
    connection.commit()
    return inserted


def census(connection: sqlite3.Connection) -> dict[str, Any]:
    event = connection.execute(
        """SELECT COUNT(*),SUM(prospective_proof_eligible),COUNT(DISTINCT currency),
                  SUM(CASE WHEN event_payload_json LIKE '%\"transport_count\":2%' THEN 1 ELSE 0 END)
           FROM source_event_observation"""
    ).fetchone()
    transport = connection.execute("SELECT COUNT(*) FROM source_event_transport").fetchone()
    episode = connection.execute(
        "SELECT COUNT(DISTINCT market_episode_id) FROM source_event_episode"
    ).fetchone()
    factor = connection.execute(
        "SELECT COUNT(*),COUNT(DISTINCT factor_key) FROM source_factor_observation"
    ).fetchone()
    entry = connection.execute(
        """SELECT COUNT(*),
                  SUM(CASE WHEN timing_quality='prospective_exact_live_quote' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN timing_quality<>'prospective_exact_live_quote' THEN 1 ELSE 0 END)
           FROM source_event_entry_snapshot"""
    ).fetchone()
    response = connection.execute(
        "SELECT COUNT(*),COUNT(DISTINCT canonical_event_id),COUNT(DISTINCT horizon_min) FROM source_event_response"
    ).fetchone()
    gaps = connection.execute(
        "SELECT COUNT(*) FROM source_response_terminal_gap"
    ).fetchone()
    forecast = connection.execute(
        """SELECT COUNT(*),SUM(CASE WHEN forecast_state='forecast' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN forecast_state='abstain' THEN 1 ELSE 0 END),
                  SUM(prospective_proof_eligible)
           FROM source_factor_forecast"""
    ).fetchone()
    return {
        "canonical_events": int(event[0] or 0),
        "prospective_proof_events": int(event[1] or 0),
        "preactivation_diagnostic_events": int((event[0] or 0) - (event[1] or 0)),
        "currencies": int(event[2] or 0),
        "multi_transport_events": int(event[3] or 0),
        "transport_rows": int(transport[0] or 0),
        "underlying_market_episodes": int(episode[0] or 0),
        "factor_observations": int(factor[0] or 0),
        "distinct_factors": int(factor[1] or 0),
        "prospective_entry_snapshots": int(entry[0] or 0),
        "valid_exact_entry_snapshots": int(entry[1] or 0),
        "invalid_or_missed_entry_snapshots": int(entry[2] or 0),
        "responses": int(response[0] or 0),
        "response_events": int(response[1] or 0),
        "response_horizons": int(response[2] or 0),
        "terminal_response_gaps": int(gaps[0] or 0),
        "forecasts": int(forecast[0] or 0),
        "non_abstaining_forecasts": int(forecast[1] or 0),
        "low_support_abstentions": int(forecast[2] or 0),
        "prospective_proof_forecasts": int(forecast[3] or 0),
    }


def response_summary(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT horizon_min,COUNT(*),AVG(currency_factor_bps),
               AVG(absolute_currency_factor_bps),
               AVG(CASE WHEN currency_factor_bps>0 THEN 1.0 ELSE 0.0 END),
               AVG(usable_pair_count)
        FROM source_event_response
        GROUP BY horizon_min ORDER BY horizon_min
        """
    ).fetchall()
    return [
        {
            "horizon_min": int(row[0]),
            "event_n": int(row[1]),
            "average_currency_factor_bps": round(float(row[2]), 6),
            "average_absolute_factor_bps": round(float(row[3]), 6),
            "strengthening_rate": round(float(row[4]), 6),
            "average_usable_pair_count": round(float(row[5]), 2),
        }
        for row in rows
    ]


def render_report(snapshot: Mapping[str, Any]) -> str:
    counts = snapshot["counts"]
    lines = [
        "# Causal Source-Factor Response Map V1",
        "",
        f"Generated: `{snapshot['generated_utc']}`",
        "",
        "This is an isolated research ledger. It does not alter V3/V12 and cannot route, authorize, or promote a trade.",
        "",
        "## Cohort boundary",
        "",
        f"- Contract: `{CONTRACT_ID}`",
        f"- Cohort: `{COHORT_ID}`",
        f"- Prospective activation: `{iso(ACTIVATED_UTC)}`",
        f"- Frozen mapper/classifier: `{REQUIRED_MAPPER_CONTRACT}` / `{REQUIRED_CLASSIFICATION_VERSION}`",
        f"- Existing pre-activation inputs are diagnostics only: **{counts['preactivation_diagnostic_events']}** canonical events.",
        f"- Fresh proof-eligible events: **{counts['prospective_proof_events']}**.",
        "",
        "## Evidence census",
        "",
        f"- Input mapping rows read: **{snapshot['input_mapping_rows']}**.",
        f"- Canonical events / transports: **{counts['canonical_events']} / {counts['transport_rows']}**.",
        f"- Currency/factor observations: **{counts['factor_observations']}**, spanning **{counts['distinct_factors']}** factor keys.",
        f"- Fixed-horizon responses locally available: **{counts['responses']}**.",
        f"- Prequential forecasts: **{counts['forecasts']}**; low-support abstentions **{counts['low_support_abstentions']}**; non-abstaining **{counts['non_abstaining_forecasts']}**.",
        "",
        "## Response coverage",
        "",
        "| Horizon | Event N | Avg signed factor | Avg absolute factor | Strengthening rate | Avg usable pairs |",
        "|---:|---:|---:|---:|---:|---:|",
    ]
    for row in snapshot["response_summary"]:
        lines.append(
            f"| {row['horizon_min']}m | {row['event_n']} | {row['average_currency_factor_bps']:+.3f} bps | "
            f"{row['average_absolute_factor_bps']:.3f} bps | {row['strengthening_rate']:.1%} | "
            f"{row['average_usable_pair_count']:.1f} |"
        )
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- Neutral official events are retained without requiring `currency_scores`.",
            "- Parallel transports and classifier generations cannot multiply event evidence.",
            "- Source clocks come from the immutable raw first-seen ledger; later numeric/detail factors use their later causal-known clock.",
            "- Each response retains both currency-strengthening and currency-weakening executable paths; `selected_side` is always null.",
            "- Forecast training is maturity-cutoff ordered and deduplicated by canonical event.",
            f"- The frozen minimum support is **{MIN_EFFECTIVE_N} independent prior events**; otherwise the map abstains.",
            "- Pre-activation records are never counted as prospective proof.",
            "",
            f"SQLite integrity: `{snapshot['sqlite_integrity']}`",
            "",
        ]
    )
    return "\n".join(lines)


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def run_cycle(
    *,
    mapping_database: Path = INPUT_MAPPING_DATABASE,
    raw_database: Path = INPUT_RAW_DATABASE,
    candle_root: Path = CANDLE_ROOT,
    output_database: Path = OUTPUT_DATABASE,
    snapshot_path: Path = SNAPSHOT_PATH,
    report_path: Path = REPORT_PATH,
    quote_path: Path = QUOTE_PATH,
    technical_path: Path = TECHNICAL_PATH,
    observed_utc: datetime | None = None,
) -> dict[str, Any]:
    observed = (observed_utc or utc_now()).astimezone(timezone.utc)
    observations = load_current_observations(mapping_database, raw_database)
    events = canonicalize_observations(observations)
    connection = open_output_database(output_database)
    try:
        inserted = insert_events(connection, events)
        quote_payload = read_json(quote_path)
        technical_payload = read_json(technical_path)
        inserted_entries = insert_live_entry_snapshots(
            connection, events, quote_payload, technical_payload, observed
        )
        entry_snapshots = load_entry_snapshots(connection)
        existing_responses = {
            (str(row[0]), int(row[1]))
            for row in connection.execute(
                "SELECT canonical_event_id,horizon_min FROM source_event_response"
            ).fetchall()
        }
        terminal_gaps = {
            (str(row[0]), int(row[1]))
            for row in connection.execute(
                "SELECT canonical_event_id,horizon_min FROM source_response_terminal_gap"
            ).fetchall()
        }
        due: list[tuple[Mapping[str, Any], int]] = []
        gap_rows: list[dict[str, Any]] = []
        for event in events:
            event_time = parse_time(event["first_known_utc"])
            if event_time is None:
                continue
            for horizon in HORIZONS_MIN:
                key = (event["canonical_event_id"], horizon)
                if key in existing_responses or key in terminal_gaps:
                    continue
                target = event_time + timedelta(minutes=horizon)
                if target + timedelta(seconds=RESPONSE_MATURITY_GRACE_SECONDS) > observed:
                    continue
                if bool(event.get("prospective_proof_eligible")):
                    snapshot = entry_snapshots.get(str(event["canonical_event_id"]))
                    if (
                        snapshot is None
                        or str(snapshot.get("timing_quality"))
                        != "prospective_exact_live_quote"
                    ):
                        quality = (
                            "missing_entry_snapshot"
                            if snapshot is None
                            else str(snapshot.get("timing_quality") or "invalid_entry_snapshot")
                        )
                        gap_rows.append(
                            {
                                "canonical_event_id": event["canonical_event_id"],
                                "currency": event["currency"],
                                "horizon_min": horizon,
                                "target_utc": iso(target),
                                "recorded_utc": iso(observed),
                                "reason": f"invalid_exact_entry_snapshot:{quality}",
                            }
                        )
                        continue
                due.append((event, horizon))
        response_rows: list[dict[str, Any]] = []
        panel_loaded = False
        if due:
            starts = [
                parse_time(event["first_known_utc"])
                for event, _ in due
                if parse_time(event["first_known_utc"]) is not None
            ]
            targets = [
                parse_time(event["first_known_utc"]) + timedelta(minutes=horizon)
                for event, horizon in due
                if parse_time(event["first_known_utc"]) is not None
            ]
            panel = load_candle_panel(
                candle_root,
                min(starts) - timedelta(minutes=1),
                max(targets) + timedelta(minutes=1),
            )
            panel_loaded = True
            for event, horizon in due:
                response = compute_event_response(
                    event,
                    panel,
                    horizon,
                    entry_snapshot=entry_snapshots.get(
                        str(event["canonical_event_id"])
                    ),
                )
                if response is not None:
                    response_rows.append(response)
                else:
                    event_time = parse_time(event["first_known_utc"])
                    gap_rows.append(
                        {
                            "canonical_event_id": event["canonical_event_id"],
                            "currency": event["currency"],
                            "horizon_min": horizon,
                            "target_utc": iso(event_time + timedelta(minutes=horizon)),
                            "recorded_utc": iso(observed),
                            "reason": "local_m1_or_canonical_ls_response_unavailable_after_grace",
                        }
                    )
        inserted_responses = insert_responses(connection, response_rows)
        inserted_gaps = insert_terminal_gaps(connection, gap_rows)
        inserted_forecasts = insert_forecasts(connection)
        counts = census(connection)
        summary = response_summary(connection)
        integrity = str(connection.execute("PRAGMA quick_check").fetchone()[0])
    finally:
        connection.close()
    snapshot = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "generated_utc": iso(observed),
        "activated_utc": iso(ACTIVATED_UTC),
        "required_mapper_contract_id": REQUIRED_MAPPER_CONTRACT,
        "required_classification_version": REQUIRED_CLASSIFICATION_VERSION,
        "horizons_min": list(HORIZONS_MIN),
        "minimum_effective_n": MIN_EFFECTIVE_N,
        "input_mapping_rows": len(observations),
        "input_canonical_events": len(events),
        "inserted": {
            **inserted,
            "entry_snapshots": inserted_entries,
            "responses": inserted_responses,
            "terminal_gaps": inserted_gaps,
            "forecasts": inserted_forecasts,
        },
        "pending_maturities_processed": len(due),
        "candle_panel_loaded": panel_loaded,
        "counts": counts,
        "response_summary": summary,
        "sqlite_integrity": integrity,
        "policy": POLICY,
    }
    atomic_write(snapshot_path, json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    atomic_write(report_path, render_report(snapshot))
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mapping-database", type=Path, default=INPUT_MAPPING_DATABASE)
    parser.add_argument("--raw-database", type=Path, default=INPUT_RAW_DATABASE)
    parser.add_argument("--candle-root", type=Path, default=CANDLE_ROOT)
    parser.add_argument("--output-database", type=Path, default=OUTPUT_DATABASE)
    parser.add_argument("--snapshot", type=Path, default=SNAPSHOT_PATH)
    parser.add_argument("--report", type=Path, default=REPORT_PATH)
    parser.add_argument("--quote-path", type=Path, default=QUOTE_PATH)
    parser.add_argument("--technical-path", type=Path, default=TECHNICAL_PATH)
    parser.add_argument("--interval-sec", type=float, default=5.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        snapshot = run_cycle(
            mapping_database=args.mapping_database,
            raw_database=args.raw_database,
            candle_root=args.candle_root,
            output_database=args.output_database,
            snapshot_path=args.snapshot,
            report_path=args.report,
            quote_path=args.quote_path,
            technical_path=args.technical_path,
        )
        compact = {
            "generated_utc": snapshot["generated_utc"],
            "input_mapping_rows": snapshot["input_mapping_rows"],
            "canonical_events": snapshot["counts"]["canonical_events"],
            "prospective_proof_events": snapshot["counts"]["prospective_proof_events"],
            "responses": snapshot["counts"]["responses"],
            "forecasts": snapshot["counts"]["forecasts"],
            "inserted": snapshot["inserted"],
            "pending_maturities_processed": snapshot["pending_maturities_processed"],
            "candle_panel_loaded": snapshot["candle_panel_loaded"],
            "sqlite_integrity": snapshot["sqlite_integrity"],
            "policy": snapshot["policy"],
        }
        print(json.dumps(compact, sort_keys=True, separators=(",", ":")), flush=True)
        if args.duration_sec <= 0.0:
            break
        if time.monotonic() - started >= args.duration_sec:
            break
        time.sleep(max(1.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
