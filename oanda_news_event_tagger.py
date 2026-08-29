#!/usr/bin/env python3
"""Canonical timestamp-aligned news tags for the full OANDA FX universe.

The project previously kept three separate forms of event context:

* curated historical events used by ``build_trading_logic_sheet.py``;
* structured live watches produced by the GPT news accounts; and
* price-only movement ledgers.

This module normalizes those sources into one event contract, expands every
event across all relevant OANDA pairs, and links price movements to only the
events that were known in the applicable time window. News remains evidence,
not an execution signal.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import io
import json
import math
import os
import re
import sqlite3
import time
import urllib.parse
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo


UTC = dt.timezone.utc
ROOT = Path(__file__).resolve().parent
DEFAULT_SEED_PATH = ROOT / "config" / "news_event_seed_v1.json"
DEFAULT_OUTPUT_ROOT = ROOT / "data" / "oanda_training_manager" / "news_event_tags"
DEFAULT_SIGNIFICANT_MOVES = (
    ROOT / "data" / "significant_moves" / "final" / "significant_moves_final.parquet"
)
DEFAULT_LEGACY_EVENT_LINKS = (
    ROOT / "data" / "significant_moves" / "event_links" / "move_event_links.csv"
)
DEFAULT_MARKET_MOVEMENT_ROOT = ROOT / "data" / "market_movement_ledger"
DEFAULT_QUOTE_STATE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "state"
    / "practice_007_market_quotes_v1.json"
)

PIPELINE_VERSION = "all_pair_news_event_tags_v3"
PAIR_CONTEXT_VERSION = "all_pair_news_context_v3"

# These source identities are retained in append-only engineering ledgers but
# are never admitted into the governed semantic event catalog.  The HUF V1
# prototypes were observed before the fully rule-byte-verified V3 contract;
# preserving their raw rows must not let either become proof evidence.
ENGINEERING_ONLY_SOURCE_IDS = {
    "hungary_ksh_release_calendar_exact_v1",
    "hungary_ksh_headline_cpi_release_clock_exact_v2",
}
HUF_KSH_VERIFIED_SOURCE_ID = "hungary_ksh_headline_cpi_release_clock_exact_v3"
HUF_KSH_VERIFIED_CONTRACT_ID = (
    "hungary_ksh_headline_cpi_release_clock_exact_v3_verified_policy_bytes_20260817"
)
HUF_KSH_POLICY_SHA256 = (
    "617f513efcccfd07fa159b0fdf9fc9e0e3243095e20deb50d88e88495fbd4030"
)
HUF_KSH_POLICY_ARCHIVE = (
    "ksh_dissemination_policy_2024_617f513efcccfd07.pdf"
)
HUF_KSH_CALENDAR_URL = "https://www.ksh.hu/prices?lang=en"
HUF_KSH_SOURCE_NAME = (
    "Hungarian Central Statistical Office headline CPI exact public-release clock"
)
HUF_KSH_RETRIEVAL_VIA = (
    "direct_official_ksh_headline_cpi_topic_calendar_plus_verified_archived_"
    "content_addressed_official_fixed_release_time_policy"
)

CORE_CURRENCIES = {
    "AUD",
    "CAD",
    "CHF",
    "EUR",
    "GBP",
    "JPY",
    "NZD",
    "USD",
}
ENERGY_SENSITIVE_CURRENCIES = {
    "AUD",
    "CAD",
    "MXN",
    "NOK",
    "NZD",
    "USD",
    "ZAR",
}
RISK_SENSITIVE_CURRENCIES = {
    "AUD",
    "CAD",
    "MXN",
    "NOK",
    "NZD",
    "SEK",
    "ZAR",
}
HAVEN_CURRENCIES = {"CHF", "JPY", "USD"}

EVENT_FIELDS = [
    "event_id",
    "event_utc",
    "event_time_basis",
    "scheduled_utc",
    "timing_precision",
    "clock_semantics",
    "independent_domestic_event",
    "linked_policy_factor",
    "first_known_utc",
    "updated_utc",
    "headline",
    "summary",
    "category",
    "source_name",
    "source_id",
    "source_contract_id",
    "source_cohort_id",
    "material_content_sha256",
    "release_time_rule_observed_sha256",
    "release_time_rule_archive_name",
    "release_rule_bytes_verified",
    "source_url",
    "source_verified",
    "corroboration_count",
    "severity",
    "movement_potential",
    "scope",
    "currencies",
    "pair_hints",
    "directional_bias_json",
    "pre_window_minutes",
    "post_window_minutes",
    "source_type",
    "context_only",
    "research_only",
    "directional_research_only",
    "execution_eligible",
    "can_place_orders",
    "raw_json",
]

PAIR_EVENT_FIELDS = [
    "event_id",
    "instrument",
    "base_currency",
    "quote_currency",
    "event_utc",
    "event_time_basis",
    "scheduled_utc",
    "timing_precision",
    "clock_semantics",
    "independent_domestic_event",
    "linked_policy_factor",
    "published_utc",
    "first_known_utc",
    "active_from_utc",
    "active_until_utc",
    "category",
    "headline",
    "source_url",
    "source_id",
    "source_contract_id",
    "source_cohort_id",
    "material_content_sha256",
    "source_verified",
    "severity",
    "pair_relevance",
    "relevance_reason",
    "expected_pair_direction",
    "base_bias",
    "quote_bias",
    "source_type",
    "intervention_status",
    "reports_prior_market_move",
    "context_only",
    "topic_tags",
    "currency_exposure_groups",
    "currency_basket_ids",
    "currency_basket_max_legs",
    "currency_basket_leg_risk_fraction",
    "research_only",
    "directional_research_only",
    "execution_eligible",
    "can_place_orders",
]

MOVE_TAG_FIELDS = [
    "movement_source",
    "movement_key",
    "move_id",
    "instrument",
    "start_utc",
    "end_utc",
    "news_match_status",
    "news_tag_count",
    "primary_event_id",
    "primary_event_utc",
    "primary_headline",
    "primary_category",
    "primary_source_url",
    "primary_source_verified",
    "primary_pair_relevance",
    "primary_match_confidence",
    "primary_temporal_relation",
    "primary_causal_relation",
    "primary_event_to_move_lead_minutes",
    "primary_availability_to_move_lead_minutes",
    "primary_predictive_eligible",
    "primary_expected_pair_direction",
    "event_ids_json",
    "event_categories_json",
    "event_links_json",
]

LEGACY_LINK_FIELDS = [
    "link_id",
    "move_id",
    "event_id",
    "event_timestamp",
    "macro_event_category",
    "central_bank_event",
    "rate_decision",
    "inflation_release",
    "employment_release",
    "gdp_release",
    "fiscal_event",
    "political_event",
    "geopolitical_event",
    "commodity_shock",
    "risk_sentiment_shock",
    "intervention",
    "policy_speech",
    "surprise_direction",
    "event_to_move_lead_minutes",
    "event_confidence",
    "source",
    "notes",
]


def utc_now() -> dt.datetime:
    return dt.datetime.now(tz=UTC)


def iso_utc(value: Any = None) -> str:
    parsed = parse_utc(value) if value is not None else utc_now()
    return parsed.astimezone(UTC).isoformat() if parsed is not None else ""


def parse_utc(value: Any) -> dt.datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = dt.datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def normalize_instrument(value: Any) -> str:
    text = re.sub(r"[^A-Za-z]", "", str(value or "")).upper()
    return f"{text[:3]}_{text[3:]}" if len(text) == 6 else ""


def split_instrument(value: Any) -> tuple[str, str]:
    instrument = normalize_instrument(value)
    if not instrument:
        return "", ""
    return tuple(instrument.split("_", 1))  # type: ignore[return-value]


def _split_values(value: Any, *, instruments: bool = False) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        values = re.split(r"[,;|\s]+", value)
    elif isinstance(value, (list, tuple, set)):
        values = list(value)
    else:
        values = [value]
    output: list[str] = []
    for raw in values:
        text = normalize_instrument(raw) if instruments else str(raw or "").upper().strip()
        valid = bool(text) if instruments else bool(re.fullmatch(r"[A-Z]{3}", text))
        if valid and text not in output:
            output.append(text)
    return output


def _split_topic_tags(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            decoded = re.split(r"[,;|\s]+", value)
        values = decoded if isinstance(decoded, list) else [decoded]
    elif isinstance(value, (list, tuple, set)):
        values = list(value)
    else:
        values = [value]
    return sorted(
        {
            tag
            for raw in values
            if (tag := str(raw or "").strip().lower())
            and re.fullmatch(r"#[a-z0-9_:-]+", tag)
        }
    )


def _json_value(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if value in (None, ""):
        return default
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default
    return parsed


def _canonical_url(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        parsed = urllib.parse.urlsplit(text)
    except ValueError:
        return text
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query = [
        (key, val)
        for key, val in query
        if not key.lower().startswith("utm_")
        and key.lower() not in {"source", "ref", "referrer"}
    ]
    return urllib.parse.urlunsplit(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path.rstrip("/"),
            urllib.parse.urlencode(query),
            "",
        )
    )


def _stable_id(prefix: str, *values: Any) -> str:
    payload = "|".join(str(value or "").strip().lower() for value in values)
    return f"{prefix}_{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:20]}"


def _normalize_bias(value: Any) -> str:
    number = safe_float(value, float("nan"))
    if math.isfinite(number):
        if number > 0:
            return "BULLISH"
        if number < 0:
            return "BEARISH"
        return "MIXED"
    text = str(value or "").upper().strip()
    if text in {"UP", "LONG", "STRONG", "POSITIVE"}:
        return "BULLISH"
    if text in {"DOWN", "SHORT", "WEAK", "NEGATIVE"}:
        return "BEARISH"
    return text if text in {"BULLISH", "BEARISH", "MIXED", "UNKNOWN"} else "UNKNOWN"


def _bias_score(value: Any) -> int:
    bias = _normalize_bias(value)
    return 1 if bias == "BULLISH" else -1 if bias == "BEARISH" else 0


def _extract_pair_biases(raw_bias: Mapping[str, Any]) -> tuple[dict[str, str], dict[str, str]]:
    currency_bias: dict[str, str] = {}
    pair_bias: dict[str, str] = {}
    for raw_key, value in raw_bias.items():
        key = str(raw_key or "").upper().strip()
        instrument = normalize_instrument(key)
        if instrument:
            pair_bias[instrument] = _normalize_bias(value)
        elif re.fullmatch(r"[A-Z]{3}", key):
            currency_bias[key] = _normalize_bias(value)
    return currency_bias, pair_bias


def _category_scope_expansion(
    category: str,
    headline: str,
    summary: str,
    currencies: Sequence[str],
) -> tuple[list[str], str]:
    text = f"{category} {headline} {summary}".lower()
    expanded = set(currencies)
    scope = "currency"
    if any(token in text for token in ("oil", "energy supply", "hormuz", "crude")):
        expanded.update(ENERGY_SENSITIVE_CURRENCIES)
    if any(
        token in text
        for token in (
            "geopolitical",
            "war",
            "risk sentiment",
            "carry unwind",
            "global volatility",
            "reciprocal tariff",
        )
    ):
        expanded.update(RISK_SENSITIVE_CURRENCIES)
        expanded.update(HAVEN_CURRENCIES)
        scope = "all_pairs"
    return sorted(expanded), scope


def _exact_ledger_flag(value: Any, expected: bool) -> bool:
    """Accept only the collector's canonical bool/0/1 CSV representations."""

    if isinstance(value, bool):
        return value is expected
    if isinstance(value, int) and value in {0, 1}:
        return bool(value) is expected
    if isinstance(value, str) and value.strip() in {"0", "1"}:
        return (value.strip() == "1") is expected
    return False


def _empty_serialized_mapping(value: Any) -> bool:
    if isinstance(value, Mapping):
        return not value
    if value in (None, ""):
        return True
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        return isinstance(decoded, Mapping) and not decoded
    return False


def normalize_event(raw: Mapping[str, Any], *, source_type: str) -> dict[str, Any] | None:
    source_id = str(raw.get("source_id") or "").strip()
    if source_id in ENGINEERING_ONLY_SOURCE_IDS:
        return None
    if source_id == HUF_KSH_VERIFIED_SOURCE_ID and not (
        source_type == "live_news_watch"
        and str(raw.get("source_type") or "").strip() == "local_no_gpt_news"
        and
        str(raw.get("source_contract_id") or "").strip()
        == HUF_KSH_VERIFIED_CONTRACT_ID
        and str(raw.get("source_cohort_id") or "").strip()
        == HUF_KSH_VERIFIED_CONTRACT_ID
        and str(raw.get("release_time_rule_observed_sha256") or "").strip().lower()
        == HUF_KSH_POLICY_SHA256
        and _exact_ledger_flag(raw.get("release_rule_bytes_verified"), True)
        and str(raw.get("release_time_rule_archive_name") or "").strip()
        == HUF_KSH_POLICY_ARCHIVE
        and _canonical_url(raw.get("source_url")) == HUF_KSH_CALENDAR_URL
        and _canonical_url(raw.get("publisher_url")) == HUF_KSH_CALENDAR_URL
        and str(raw.get("source_name") or "").strip() == HUF_KSH_SOURCE_NAME
        and str(raw.get("source_kind") or "").strip()
        == "ksh_release_calendar_verified_rule_exact_v3"
        and _exact_ledger_flag(raw.get("source_verified"), True)
        and _exact_ledger_flag(raw.get("source_direct"), True)
        and safe_float(raw.get("source_quality"), -1.0) == 1.0
        and str(raw.get("retrieval_via") or "").strip()
        == HUF_KSH_RETRIEVAL_VIA
        and str(raw.get("source_role") or "").strip()
        == "primary_statistical_calendar"
        and str(raw.get("event_series_id") or "").strip()
        == "hungary_headline_cpi_yoy"
        and str(raw.get("event_name") or "").strip()
        == "Hungary headline consumer price inflation"
        and str(raw.get("event_country") or "").strip() == "Hungary"
        and str(raw.get("timing_precision") or "").strip() == "minute"
        and str(raw.get("event_time_basis") or "").strip()
        == "scheduled_release"
        and str(raw.get("clock_semantics") or "").strip()
        == "domestic_official_statistical_release"
        and _exact_ledger_flag(raw.get("independent_domestic_event"), True)
        and _exact_ledger_flag(raw.get("linked_policy_factor"), False)
        and _split_values(raw.get("source_currencies")) == ["HUF"]
        and _split_values(raw.get("direct_currencies")) == ["HUF"]
        and _split_values(raw.get("currencies")) == ["HUF"]
        and re.fullmatch(
            r"[0-9a-f]{64}",
            str(raw.get("material_content_sha256") or "").strip().lower(),
        )
        is not None
        and str(raw.get("category") or "").strip() == "inflation_context"
        and _exact_ledger_flag(raw.get("directional_research_only"), True)
        and _exact_ledger_flag(raw.get("research_only"), True)
        and _exact_ledger_flag(raw.get("context_only"), True)
        and _exact_ledger_flag(raw.get("relevant"), False)
        and _exact_ledger_flag(raw.get("directional_publish_eligible"), False)
        and _exact_ledger_flag(raw.get("execution_eligible"), False)
        and _exact_ledger_flag(raw.get("can_place_orders"), False)
        and _empty_serialized_mapping(raw.get("currency_scores"))
        and _empty_serialized_mapping(raw.get("directional_bias"))
        and _empty_serialized_mapping(raw.get("pair_bias"))
        and raw.get("direction") in (None, "")
        and raw.get("actual") in (None, "")
        and raw.get("actual_value") in (None, "")
        and raw.get("consensus") in (None, "")
        and raw.get("consensus_value") in (None, "")
    ):
        return None
    scheduled_time = parse_utc(
        raw.get("scheduled_utc")
        or raw.get("release_utc")
        or raw.get("scheduled_release_utc")
    )
    if source_id == HUF_KSH_VERIFIED_SOURCE_ID:
        if scheduled_time is None:
            return None
        local_release = scheduled_time.astimezone(ZoneInfo("Europe/Budapest"))
        if (
            local_release.hour,
            local_release.minute,
            local_release.second,
            local_release.microsecond,
        ) != (8, 30, 0, 0):
            return None
    reported_time = parse_utc(
        raw.get("event_utc")
        or raw.get("event_timestamp")
        or raw.get("reported_update_utc")
    )
    published_time = parse_utc(raw.get("published_utc"))
    captured_time = parse_utc(
        raw.get("captured_utc")
        or raw.get("first_seen_utc")
        or raw.get("time_utc")
    )
    event_time = scheduled_time or reported_time or published_time or captured_time
    if event_time is None:
        return None
    event_time_basis = (
        "scheduled_release"
        if scheduled_time is not None
        else "reported_event"
        if reported_time is not None
        else "published"
        if published_time is not None
        else "first_observed"
    )
    first_known = parse_utc(
        raw.get("first_known_utc")
        or raw.get("first_seen_utc")
        or raw.get("published_utc")
        or event_time
    ) or event_time
    updated = parse_utc(
        raw.get("updated_utc")
        or raw.get("reported_update_utc")
        or raw.get("last_seen_utc")
        or first_known
    ) or first_known
    headline = re.sub(
        r"\s+",
        " ",
        str(raw.get("headline") or raw.get("event_name") or raw.get("event") or "").strip(),
    )
    summary = re.sub(
        r"\s+",
        " ",
        str(
            raw.get("summary")
            or raw.get("sentiment_reason")
            or raw.get("why_market_moving")
            or raw.get("macro_reason_verified")
            or ""
        ).strip(),
    )
    if not headline:
        headline = summary[:240] or f"Event at {iso_utc(event_time)}"
    category = str(
        raw.get("category")
        or raw.get("macro_category")
        or raw.get("macro_event_category")
        or "uncategorized"
    ).strip()
    source_url = _canonical_url(raw.get("source_url") or raw.get("macro_source_url"))
    source_name = str(raw.get("source_name") or raw.get("source") or "").strip()
    currencies = _split_values(raw.get("currencies"))
    direct_currencies_known = "direct_currencies" in raw
    direct_currencies = _split_values(raw.get("direct_currencies"))
    pair_hints = _split_values(raw.get("pair_hints"), instruments=True)
    raw_bias = _json_value(
        raw.get("directional_bias")
        or raw.get("directional_bias_json"),
        {},
    )
    if not isinstance(raw_bias, dict):
        raw_bias = {}
    currency_bias, pair_bias = _extract_pair_biases(raw_bias)
    currency_exposure_groups = sorted(
        f"{currency}:{'LONG' if _normalize_bias(bias) == 'BULLISH' else 'SHORT'}"
        for currency, bias in currency_bias.items()
        if _normalize_bias(bias) in {"BULLISH", "BEARISH"}
    )
    for instrument in pair_hints:
        base, quote = split_instrument(instrument)
        currencies.extend([value for value in (base, quote) if value and value not in currencies])
    currencies, inferred_scope = _category_scope_expansion(
        category,
        headline,
        summary,
        currencies,
    )
    scope = str(raw.get("scope") or inferred_scope or "currency").lower().strip()
    if scope not in {"currency", "explicit_pairs", "all_pairs"}:
        scope = "currency"
    event_id = str(raw.get("event_id") or raw.get("watch_id") or "").strip()
    if not event_id:
        event_id = _stable_id(
            "news",
            source_url,
            iso_utc(event_time)[:13],
            headline,
        )
    pre_window = max(0.0, safe_float(raw.get("pre_window_minutes"), 0.0))
    expires = parse_utc(raw.get("expires_utc"))
    default_post = 360.0 if source_type == "live_news_watch" else 720.0
    post_window = max(1.0, safe_float(raw.get("post_window_minutes"), default_post))
    if expires is not None:
        post_window = max(post_window, (expires - event_time).total_seconds() / 60.0)
    event = {
        "event_id": event_id,
        "event_utc": iso_utc(event_time),
        "event_time_basis": event_time_basis,
        "scheduled_utc": iso_utc(scheduled_time) if scheduled_time else "",
        "timing_precision": str(raw.get("timing_precision") or "").strip(),
        "clock_semantics": str(raw.get("clock_semantics") or "").strip(),
        "independent_domestic_event": truthy(
            raw.get("independent_domestic_event")
        ),
        "linked_policy_factor": truthy(raw.get("linked_policy_factor")),
        "published_utc": iso_utc(published_time) if published_time else "",
        "first_known_utc": iso_utc(first_known),
        "updated_utc": iso_utc(updated),
        "headline": headline[:1000],
        "summary": summary[:4000],
        "category": category[:160],
        "source_name": source_name[:200],
        "source_id": str(raw.get("source_id") or "").strip(),
        "source_contract_id": str(raw.get("source_contract_id") or "").strip(),
        "source_cohort_id": str(raw.get("source_cohort_id") or "").strip(),
        "material_content_sha256": str(
            raw.get("material_content_sha256") or ""
        ).strip(),
        "release_time_rule_observed_sha256": str(
            raw.get("release_time_rule_observed_sha256") or ""
        ).strip().lower(),
        "release_time_rule_archive_name": str(
            raw.get("release_time_rule_archive_name") or ""
        ).strip(),
        "release_rule_bytes_verified": truthy(
            raw.get("release_rule_bytes_verified")
        ),
        "source_url": source_url[:2000],
        "source_verified": truthy(raw.get("source_verified"))
        or source_type in {"curated_seed", "monitor_context"},
        "corroboration_count": max(0, int(safe_float(raw.get("corroboration_count"), 0))),
        "severity": max(0.0, min(100.0, safe_float(raw.get("severity"), 70.0))),
        "movement_potential": str(raw.get("movement_potential") or "HIGH").upper().strip(),
        "scope": scope,
        "direct_currencies": sorted(set(direct_currencies)),
        "direct_currencies_known": direct_currencies_known,
        "currencies": sorted(set(currencies)),
        "pair_hints": sorted(set(pair_hints)),
        "currency_bias": currency_bias,
        "pair_bias": pair_bias,
        "currency_exposure_groups": currency_exposure_groups,
        "pre_window_minutes": pre_window,
        "post_window_minutes": post_window,
        "source_type": source_type,
        "intervention_status": str(raw.get("intervention_status") or ""),
        "reports_prior_market_move": truthy(raw.get("reports_prior_market_move")),
        "context_only": truthy(raw.get("context_only")),
        "research_only": True,
        "directional_research_only": truthy(
            raw.get("directional_research_only")
        ),
        "execution_eligible": False,
        "can_place_orders": False,
        "topic_tags": _split_topic_tags(raw.get("topic_tags")),
        "raw": dict(raw),
    }
    return event


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return default


def load_seed_events(path: Path = DEFAULT_SEED_PATH) -> list[dict[str, Any]]:
    payload = _load_json(path, {})
    raw_events = payload.get("events") if isinstance(payload, dict) else []
    return [
        event
        for raw in (raw_events or [])
        if isinstance(raw, dict)
        and (event := normalize_event(raw, source_type="curated_seed")) is not None
    ]


def load_news_watch_events(root: Path = ROOT / "data" / "forex_gpt_manager") -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not root.exists():
        return events
    for path in sorted(root.rglob("news_watch_ledger.csv")):
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                rows = csv.DictReader(handle)
                for row in rows:
                    if str(row.get("status") or "").lower() not in {"new", "refreshed", "active"}:
                        continue
                    event = normalize_event(row, source_type="live_news_watch")
                    if event is not None:
                        event["source_ledger"] = str(path)
                        events.append(event)
        except (OSError, csv.Error):
            continue
    return events


def _currencies_from_text(values: Iterable[Any]) -> list[str]:
    found: set[str] = set()
    for value in values:
        found.update(re.findall(r"\b[A-Z]{3}\b", str(value or "")))
    return sorted(found)


def load_monitor_context_events(
    reports_root: Path = ROOT / "data" / "oanda_training_manager" / "reports",
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not reports_root.exists():
        return events
    for path in sorted(reports_root.rglob("NEWS_CONTEXT.jsonl")):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            try:
                payload = json.loads(line)
            except (ValueError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            sources = payload.get("sources") or []
            first_source = sources[0] if sources and isinstance(sources[0], dict) else {}
            implications = payload.get("fx_implications") or []
            raw = {
                "event_utc": payload.get("captured_utc"),
                "first_known_utc": payload.get("captured_utc"),
                "headline": str(payload.get("event") or "Timestamped news regime context").replace(
                    "_", " "
                ),
                "summary": "; ".join(str(value) for value in implications),
                "category": "news_regime_context",
                "currencies": _currencies_from_text(implications),
                "source_name": first_source.get("source"),
                "source_url": first_source.get("url"),
                "source_verified": bool(first_source.get("url")),
                "severity": 70,
                "post_window_minutes": 720,
                "scope": "all_pairs"
                if re.search(
                    r"oil|middle east|war|risk sentiment",
                    " ".join(str(value) for value in implications),
                    flags=re.I,
                )
                else "currency",
                "raw_context": payload,
            }
            event = normalize_event(raw, source_type="monitor_context")
            if event is not None:
                event["source_ledger"] = str(path)
                events.append(event)
    return events


def _event_completeness(event: Mapping[str, Any]) -> tuple[int, str]:
    score = 0
    score += 8 if event.get("source_verified") else 0
    score += 4 if event.get("source_url") else 0
    score += 2 * len(event.get("currencies") or [])
    score += len(event.get("pair_hints") or [])
    score += 2 if event.get("summary") else 0
    return score, str(event.get("updated_utc") or "")


def collect_events(
    *,
    seed_path: Path = DEFAULT_SEED_PATH,
    news_root: Path = ROOT / "data" / "forex_gpt_manager",
    reports_root: Path = ROOT / "data" / "oanda_training_manager" / "reports",
) -> list[dict[str, Any]]:
    candidates = (
        load_seed_events(seed_path)
        + load_news_watch_events(news_root)
        + load_monitor_context_events(reports_root)
    )
    deduped: dict[str, dict[str, Any]] = {}
    identity_index: dict[tuple[str, str], str] = {}
    for event in candidates:
        identity = (
            _canonical_url(event.get("source_url")),
            str(event.get("event_utc") or "")[:13],
        )
        event_id = str(event["event_id"])
        existing_id = identity_index.get(identity) if identity[0] else None
        key = existing_id or event_id
        existing = deduped.get(key)
        if existing is None or _event_completeness(event) > _event_completeness(existing):
            if existing is not None:
                event = {**existing, **event}
                event["event_id"] = key
            deduped[key] = event
        if identity[0]:
            identity_index[identity] = key
    return sorted(
        deduped.values(),
        key=lambda event: (str(event.get("event_utc") or ""), str(event.get("event_id") or "")),
    )


def discover_instruments(
    supplied: Iterable[Any] | None = None,
    *,
    quote_state: Path = DEFAULT_QUOTE_STATE,
) -> list[str]:
    instruments = {
        instrument
        for raw in (supplied or [])
        if (instrument := normalize_instrument(raw))
    }
    if instruments:
        return sorted(instruments)
    payload = _load_json(quote_state, {})
    quotes = payload.get("quotes") if isinstance(payload, dict) else {}
    if isinstance(quotes, dict):
        instruments.update(
            instrument
            for raw in quotes
            if (instrument := normalize_instrument(raw))
        )
    # Live quotes are an availability subset: a closed or temporarily stale
    # instrument must not disappear from the canonical news/pair universe.
    # Merge the persisted M1 universe even when current quotes are present.
    candles = ROOT / "data" / "oanda_training_manager" / "candles_m1_parquet"
    if candles.exists():
        instruments.update(
            instrument
            for path in candles.glob("*_M1.parquet")
            if (instrument := normalize_instrument(path.stem.removesuffix("_M1")))
        )
    return sorted(instruments)


def _event_pair_relevance(
    event: Mapping[str, Any],
    instrument: str,
) -> tuple[float, str] | None:
    base, quote = split_instrument(instrument)
    if not base:
        return None
    hints = set(event.get("pair_hints") or [])
    currencies = set(event.get("currencies") or [])
    direct_currencies = set(event.get("direct_currencies") or [])
    direct_currencies_known = bool(event.get("direct_currencies_known"))
    scope = str(event.get("scope") or "currency")
    if instrument in hints:
        return 1.0, "explicit_pair"
    # The semantic classifier expands global risk stories across haven and
    # commodity currencies.  Those inferred transmission legs must not look
    # more pair-specific than an event that directly names one currency.
    # New local-news ledgers preserve the pre-expansion currency mentions;
    # retain the legacy fallback for older/foreign ledgers without that field.
    relevance_currencies = (
        direct_currencies if direct_currencies_known else currencies
    )
    overlap = relevance_currencies.intersection({base, quote})
    if len(overlap) == 2:
        return 0.95, "both_currencies"
    if len(overlap) == 1:
        return 0.78, "one_currency"
    if scope == "all_pairs":
        return 0.42, "global_event"
    return None


def _expected_pair_direction(
    event: Mapping[str, Any],
    instrument: str,
) -> tuple[str, str, str]:
    base, quote = split_instrument(instrument)
    pair_bias = event.get("pair_bias") or {}
    explicit = _normalize_bias(pair_bias.get(instrument)) if isinstance(pair_bias, dict) else "UNKNOWN"
    if explicit == "BULLISH":
        return "LONG", "UNKNOWN", "UNKNOWN"
    if explicit == "BEARISH":
        return "SHORT", "UNKNOWN", "UNKNOWN"
    currency_bias = event.get("currency_bias") or {}
    base_bias = _normalize_bias(currency_bias.get(base)) if isinstance(currency_bias, dict) else "UNKNOWN"
    quote_bias = _normalize_bias(currency_bias.get(quote)) if isinstance(currency_bias, dict) else "UNKNOWN"
    delta = _bias_score(base_bias) - _bias_score(quote_bias)
    direction = "LONG" if delta > 0 else "SHORT" if delta < 0 else "UNKNOWN"
    return direction, base_bias, quote_bias


def expand_events(
    events: Sequence[Mapping[str, Any]],
    instruments: Sequence[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in events:
        event_time = parse_utc(event.get("event_utc"))
        first_known = parse_utc(event.get("first_known_utc"))
        if event_time is None or first_known is None:
            continue
        nominal_active_from = event_time - dt.timedelta(
            minutes=max(
                0.0,
                safe_float(event.get("pre_window_minutes"), 0.0),
            )
        )
        # ``active_from_utc`` is consumed by point-in-time replay as well as
        # by the current-context view.  A publisher timestamp can precede the
        # collector's first observation, so never expose an unscheduled event
        # before the system could actually have known it.  Scheduled events
        # still retain their pre-release window once the calendar observation
        # itself is known.
        active_from = max(nominal_active_from, first_known)
        active_until = event_time + dt.timedelta(
            minutes=max(1.0, safe_float(event.get("post_window_minutes"), 360.0))
        )
        for instrument in instruments:
            relevance = _event_pair_relevance(event, instrument)
            if relevance is None:
                continue
            score, reason = relevance
            direction, base_bias, quote_bias = _expected_pair_direction(event, instrument)
            base, quote = split_instrument(instrument)
            rows.append(
                {
                    "event_id": event.get("event_id"),
                    "instrument": instrument,
                    "base_currency": base,
                    "quote_currency": quote,
                    "event_utc": event.get("event_utc"),
                    "event_time_basis": event.get("event_time_basis"),
                    "scheduled_utc": event.get("scheduled_utc"),
                    "timing_precision": event.get("timing_precision"),
                    "clock_semantics": event.get("clock_semantics"),
                    "independent_domestic_event": bool(
                        event.get("independent_domestic_event")
                    ),
                    "linked_policy_factor": bool(
                        event.get("linked_policy_factor")
                    ),
                    "published_utc": event.get("published_utc"),
                    "first_known_utc": event.get("first_known_utc"),
                    "active_from_utc": iso_utc(active_from),
                    "active_until_utc": iso_utc(active_until),
                    "category": event.get("category"),
                    "headline": event.get("headline"),
                    "source_url": event.get("source_url"),
                    "source_id": event.get("source_id"),
                    "source_contract_id": event.get("source_contract_id"),
                    "source_cohort_id": event.get("source_cohort_id"),
                    "material_content_sha256": event.get(
                        "material_content_sha256"
                    ),
                    "source_verified": bool(event.get("source_verified")),
                    "severity": safe_float(event.get("severity"), 0.0),
                    "pair_relevance": score,
                    "relevance_reason": reason,
                    "expected_pair_direction": direction,
                    "base_bias": base_bias,
                    "quote_bias": quote_bias,
                    "source_type": event.get("source_type"),
                    "intervention_status": event.get("intervention_status"),
                    "reports_prior_market_move": bool(
                        event.get("reports_prior_market_move")
                    ),
                    "context_only": bool(event.get("context_only")),
                    "topic_tags": event.get("topic_tags") or [],
                    "currency_exposure_groups": event.get(
                        "currency_exposure_groups"
                    )
                    or [],
                    "currency_basket_ids": [
                        _stable_id(
                            "currency_basket",
                            event.get("event_id"),
                            group,
                        )
                        for group in event.get("currency_exposure_groups") or []
                    ],
                    "currency_basket_max_legs": 3,
                    "currency_basket_leg_risk_fraction": round(1.0 / 3.0, 6),
                    "research_only": True,
                    "directional_research_only": bool(
                        event.get("directional_research_only")
                    ),
                    "execution_eligible": False,
                    "can_place_orders": False,
                }
            )
    return sorted(
        rows,
        key=lambda row: (
            str(row["instrument"]),
            str(row["event_utc"]),
            str(row["event_id"]),
        ),
    )


def build_current_pair_context(
    pair_events: Sequence[Mapping[str, Any]],
    instruments: Sequence[str],
    *,
    as_of: Any = None,
    max_events_per_pair: int = 4,
) -> dict[str, dict[str, Any]]:
    now = parse_utc(as_of) or utc_now()
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in pair_events:
        first_known = parse_utc(raw.get("first_known_utc"))
        active_until = parse_utc(raw.get("active_until_utc"))
        active_from = parse_utc(raw.get("active_from_utc"))
        event_time = parse_utc(raw.get("event_utc"))
        if (
            first_known is None
            or active_from is None
            or active_until is None
            or event_time is None
        ):
            continue
        if first_known > now or active_from > now or active_until < now:
            continue
        row = dict(raw)
        row["age_minutes"] = max(0.0, (now - event_time).total_seconds() / 60.0)
        grouped[str(raw.get("instrument") or "")].append(row)
    output: dict[str, dict[str, Any]] = {}
    for instrument in instruments:
        active = sorted(
            grouped.get(instrument, []),
            key=lambda row: (
                not bool(row.get("context_only")),
                bool(row.get("source_verified")),
                safe_float(row.get("pair_relevance"), 0.0)
                * safe_float(row.get("severity"), 0.0),
                str(row.get("event_utc") or ""),
            ),
            reverse=True,
        )[: max(1, int(max_events_per_pair))]
        directions = {
            str(row.get("expected_pair_direction") or "UNKNOWN")
            for row in active
            if not bool(row.get("context_only"))
            if str(row.get("expected_pair_direction") or "UNKNOWN") != "UNKNOWN"
        }
        directional_state = (
            next(iter(directions))
            if len(directions) == 1
            else "CONFLICT"
            if len(directions) > 1
            else "UNKNOWN"
        )
        output[instrument] = {
            "instrument": instrument,
            "as_of_utc": iso_utc(now),
            "active_event_count": len(active),
            "directional_state": directional_state,
            "events": [
                {
                    "event_id": row.get("event_id"),
                    "event_utc": row.get("event_utc"),
                    "event_time_basis": row.get("event_time_basis"),
                    "scheduled_utc": row.get("scheduled_utc"),
                    "timing_precision": row.get("timing_precision"),
                    "clock_semantics": row.get("clock_semantics"),
                    "independent_domestic_event": row.get(
                        "independent_domestic_event"
                    ),
                    "linked_policy_factor": row.get("linked_policy_factor"),
                    "published_utc": row.get("published_utc"),
                    "first_known_utc": row.get("first_known_utc"),
                    "age_minutes": round(safe_float(row.get("age_minutes"), 0.0), 3),
                    "headline": row.get("headline"),
                    "category": row.get("category"),
                    "severity": row.get("severity"),
                    "pair_relevance": row.get("pair_relevance"),
                    "relevance_reason": row.get("relevance_reason"),
                    "expected_pair_direction": row.get("expected_pair_direction"),
                    "source_url": row.get("source_url"),
                    "source_id": row.get("source_id"),
                    "source_contract_id": row.get("source_contract_id"),
                    "source_cohort_id": row.get("source_cohort_id"),
                    "material_content_sha256": row.get(
                        "material_content_sha256"
                    ),
                    "source_verified": row.get("source_verified"),
                    "intervention_status": row.get("intervention_status"),
                    "reports_prior_market_move": row.get(
                        "reports_prior_market_move"
                    ),
                    "context_only": row.get("context_only"),
                    "can_place_orders": False,
                    "topic_tags": row.get("topic_tags") or [],
                    "currency_exposure_groups": row.get(
                        "currency_exposure_groups"
                    )
                    or [],
                    "currency_basket_ids": row.get("currency_basket_ids") or [],
                    "currency_basket_max_legs": row.get(
                        "currency_basket_max_legs"
                    ),
                    "currency_basket_leg_risk_fraction": row.get(
                        "currency_basket_leg_risk_fraction"
                    ),
                    "research_only": True,
                    "directional_research_only": row.get(
                        "directional_research_only"
                    ),
                    "execution_eligible": False,
                }
                for row in active
            ],
        }
    return output


def classify_causal_relation(
    pair_event: Mapping[str, Any],
    start_utc: Any,
    end_utc: Any,
) -> str:
    start = parse_utc(start_utc)
    end = parse_utc(end_utc) or start
    event_time = parse_utc(pair_event.get("event_utc"))
    first_known = parse_utc(pair_event.get("first_known_utc"))
    if start is None or end is None or event_time is None or first_known is None:
        return "UNKNOWN"
    if first_known > end:
        return "POST_HOC_EXPLANATION"
    if first_known > start:
        return "FIRST_WAVE_CONFIRMATION"
    event_age = max(0.0, (start - event_time).total_seconds() / 60.0)
    availability_age = max(0.0, (start - first_known).total_seconds() / 60.0)
    post_window = max(
        1.0,
        safe_float(pair_event.get("post_window_minutes"), 360.0),
    )
    stale_after = max(60.0, min(180.0, post_window / 2.0))
    if event_age > stale_after and availability_age > stale_after:
        return "STALE_CONTEXT"
    if truthy(pair_event.get("reports_prior_market_move")):
        return "CONTINUATION_SIGNAL"
    return "PRE_MOVE"


def _temporal_match(
    pair_event: Mapping[str, Any],
    start_utc: Any,
    end_utc: Any,
    *,
    include_post_hoc: bool = False,
    post_hoc_window_minutes: float = 120.0,
) -> dict[str, Any] | None:
    start = parse_utc(start_utc)
    end = parse_utc(end_utc) or start
    event_time = parse_utc(pair_event.get("event_utc"))
    first_known = parse_utc(pair_event.get("first_known_utc"))
    active_from = parse_utc(pair_event.get("active_from_utc"))
    active_until = parse_utc(pair_event.get("active_until_utc"))
    if None in {start, end, event_time, first_known, active_from, active_until}:
        return None
    assert start is not None and end is not None
    assert event_time is not None and first_known is not None
    assert active_from is not None and active_until is not None
    if first_known > end:
        if (
            not include_post_hoc
            or first_known
            > end + dt.timedelta(minutes=max(1.0, post_hoc_window_minutes))
        ):
            return None
        post_hoc = True
    else:
        post_hoc = False
        if active_from > end or active_until < start:
            return None
    lead = (start - event_time).total_seconds() / 60.0
    availability_lead = (start - first_known).total_seconds() / 60.0
    if event_time > end:
        relation = "pre_event"
        temporal_weight = 0.35
    elif event_time >= start:
        relation = "event_inside_move"
        temporal_weight = 1.0
    else:
        relation = "post_event"
        half_life = max(
            30.0,
            (active_until - event_time).total_seconds() / 120.0,
        )
        temporal_weight = max(0.2, math.exp(-max(0.0, lead) / half_life))
    if post_hoc:
        temporal_weight = min(temporal_weight, 0.15)
    source_weight = 1.0 if truthy(pair_event.get("source_verified")) else 0.65
    confidence = (
        safe_float(pair_event.get("pair_relevance"), 0.0)
        * temporal_weight
        * source_weight
    )
    return {
        **dict(pair_event),
        "temporal_relation": relation,
        "causal_relation": classify_causal_relation(pair_event, start, end),
        "event_to_move_lead_minutes": lead,
        "availability_to_move_lead_minutes": availability_lead,
        "predictive_eligible": (
            first_known <= start and not truthy(pair_event.get("context_only"))
        ),
        "confirmation_eligible": first_known <= end,
        "match_confidence": max(0.0, min(1.0, confidence)),
    }


def match_events_for_movement(
    pair_events: Sequence[Mapping[str, Any]],
    *,
    instrument: Any,
    start_utc: Any,
    end_utc: Any,
    max_matches: int = 8,
    include_post_hoc: bool = False,
    post_hoc_window_minutes: float = 120.0,
) -> list[dict[str, Any]]:
    normalized = normalize_instrument(instrument)
    matches = [
        match
        for row in pair_events
        if str(row.get("instrument") or "") == normalized
        and (
            match := _temporal_match(
                row,
                start_utc,
                end_utc,
                include_post_hoc=include_post_hoc,
                post_hoc_window_minutes=post_hoc_window_minutes,
            )
        )
        is not None
    ]
    return sorted(
        matches,
        key=lambda row: (
            safe_float(row.get("match_confidence"), 0.0),
            safe_float(row.get("severity"), 0.0),
            -abs(safe_float(row.get("event_to_move_lead_minutes"), 0.0)),
        ),
        reverse=True,
    )[: max(1, int(max_matches))]


def index_pair_events(
    pair_events: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in pair_events:
        instrument = normalize_instrument(row.get("instrument"))
        if instrument:
            grouped[instrument].append(dict(row))
    return dict(grouped)


def summarize_movement_tags(
    movement: Mapping[str, Any],
    matches: Sequence[Mapping[str, Any]],
    *,
    movement_source: str,
) -> dict[str, Any]:
    primary = dict(matches[0]) if matches else {}
    return {
        "movement_source": movement_source,
        "movement_key": str(movement.get("movement_key") or movement.get("move_id") or ""),
        "move_id": str(movement.get("move_id") or ""),
        "instrument": normalize_instrument(movement.get("instrument")),
        "start_utc": str(movement.get("start_utc") or movement.get("start_timestamp") or ""),
        "end_utc": str(movement.get("end_utc") or movement.get("end_timestamp") or ""),
        "news_match_status": "matched" if matches else "no_verified_time_pair_match",
        "news_tag_count": len(matches),
        "primary_event_id": primary.get("event_id", ""),
        "primary_event_utc": primary.get("event_utc", ""),
        "primary_headline": primary.get("headline", ""),
        "primary_category": primary.get("category", ""),
        "primary_source_url": primary.get("source_url", ""),
        "primary_source_verified": primary.get("source_verified", False),
        "primary_pair_relevance": primary.get("pair_relevance", ""),
        "primary_match_confidence": primary.get("match_confidence", ""),
        "primary_temporal_relation": primary.get("temporal_relation", ""),
        "primary_causal_relation": primary.get("causal_relation", ""),
        "primary_event_to_move_lead_minutes": primary.get(
            "event_to_move_lead_minutes", ""
        ),
        "primary_availability_to_move_lead_minutes": primary.get(
            "availability_to_move_lead_minutes", ""
        ),
        "primary_predictive_eligible": primary.get("predictive_eligible", False),
        "primary_expected_pair_direction": primary.get(
            "expected_pair_direction", ""
        ),
        "event_ids_json": json.dumps(
            [row.get("event_id") for row in matches],
            separators=(",", ":"),
        ),
        "event_categories_json": json.dumps(
            sorted({str(row.get("category") or "") for row in matches if row.get("category")}),
            separators=(",", ":"),
        ),
        "event_links_json": json.dumps(
            [
                {
                    "event_id": row.get("event_id"),
                    "event_utc": row.get("event_utc"),
                    "headline": row.get("headline"),
                    "category": row.get("category"),
                    "source_url": row.get("source_url"),
                    "source_verified": row.get("source_verified"),
                    "pair_relevance": row.get("pair_relevance"),
                    "match_confidence": row.get("match_confidence"),
                    "temporal_relation": row.get("temporal_relation"),
                    "causal_relation": row.get("causal_relation"),
                    "event_to_move_lead_minutes": row.get(
                        "event_to_move_lead_minutes"
                    ),
                    "availability_to_move_lead_minutes": row.get(
                        "availability_to_move_lead_minutes"
                    ),
                    "predictive_eligible": row.get("predictive_eligible"),
                    "confirmation_eligible": row.get("confirmation_eligible"),
                    "expected_pair_direction": row.get(
                        "expected_pair_direction"
                    ),
                }
                for row in matches
            ],
            separators=(",", ":"),
            default=str,
        ),
    }


def _event_csv_row(event: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **{field: event.get(field, "") for field in EVENT_FIELDS},
        "currencies": ",".join(event.get("currencies") or []),
        "pair_hints": ",".join(event.get("pair_hints") or []),
        "directional_bias_json": json.dumps(
            {
                **(event.get("currency_bias") or {}),
                **(event.get("pair_bias") or {}),
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        "raw_json": json.dumps(event.get("raw") or {}, default=str, separators=(",", ":"))[
            :12000
        ],
    }


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(text, encoding="utf-8")
    os.replace(temp, path)


def _atomic_write_json(path: Path, payload: Any) -> None:
    _atomic_write_text(
        path,
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
    )


def _atomic_write_csv(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
    fields: Sequence[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})
    os.replace(temp, path)


def _write_catalog_sqlite(
    path: Path,
    events: Sequence[Mapping[str, Any]],
    pair_events: Sequence[Mapping[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=NORMAL")
        # A full catalog replacement can exceed SQLite's default 1,000-page
        # auto-checkpoint threshold. Long-lived readers must not turn the
        # writer's commit into a multi-minute blocking checkpoint.
        connection.execute("PRAGMA wal_autocheckpoint=0")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                event_utc TEXT NOT NULL,
                first_known_utc TEXT NOT NULL,
                category TEXT,
                headline TEXT,
                source_url TEXT,
                source_verified INTEGER NOT NULL,
                severity REAL NOT NULL,
                event_json TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS pair_event_tags (
                event_id TEXT NOT NULL,
                instrument TEXT NOT NULL,
                event_utc TEXT NOT NULL,
                active_until_utc TEXT NOT NULL,
                pair_relevance REAL NOT NULL,
                expected_pair_direction TEXT,
                tag_json TEXT NOT NULL,
                PRIMARY KEY (event_id, instrument)
            )
            """
        )
        connection.execute("DELETE FROM events")
        connection.execute("DELETE FROM pair_event_tags")
        connection.executemany(
            """
            INSERT INTO events (
                event_id, event_utc, first_known_utc, category, headline,
                source_url, source_verified, severity, event_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    event.get("event_id"),
                    event.get("event_utc"),
                    event.get("first_known_utc"),
                    event.get("category"),
                    event.get("headline"),
                    event.get("source_url"),
                    int(bool(event.get("source_verified"))),
                    safe_float(event.get("severity"), 0.0),
                    json.dumps(event, default=str, separators=(",", ":")),
                )
                for event in events
            ],
        )
        connection.executemany(
            """
            INSERT INTO pair_event_tags (
                event_id, instrument, event_utc, active_until_utc,
                pair_relevance, expected_pair_direction, tag_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.get("event_id"),
                    row.get("instrument"),
                    row.get("event_utc"),
                    row.get("active_until_utc"),
                    safe_float(row.get("pair_relevance"), 0.0),
                    row.get("expected_pair_direction"),
                    json.dumps(row, default=str, separators=(",", ":")),
                )
                for row in pair_events
            ],
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_pair_event_time "
            "ON pair_event_tags(instrument, event_utc)"
        )
        connection.commit()
        # Do not checkpoint in the live refresh path. Even PASSIVE checkpoints
        # can wait behind a long-lived reader on Windows after the catalog
        # transaction is safely committed. Periodic database maintenance may
        # checkpoint later; JSON/CSV/context publication must not be held up.
    finally:
        connection.close()


def _acquire_sync_lock(output_root: Path, stale_seconds: int = 600) -> Path | None:
    path = output_root / ".sync.lock"
    output_root.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        try:
            if time.time() - path.stat().st_mtime > stale_seconds:
                path.unlink()
                descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            else:
                return None
        except OSError:
            return None
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(
            json.dumps({"pid": os.getpid(), "created_utc": iso_utc()}, sort_keys=True)
        )
    return path


def synchronize_catalog(
    *,
    instruments: Iterable[Any] | None = None,
    seed_path: Path = DEFAULT_SEED_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    as_of: Any = None,
    write_sqlite: bool = True,
) -> dict[str, Any]:
    lock = _acquire_sync_lock(output_root)
    if lock is None:
        return {
            "status": "skipped_locked",
            "output_root": str(output_root),
        }
    try:
        universe = discover_instruments(instruments)
        events = collect_events(seed_path=seed_path)
        pair_events = expand_events(events, universe)
        pair_context = build_current_pair_context(pair_events, universe, as_of=as_of)
        event_rows = [_event_csv_row(event) for event in events]

        _atomic_write_json(output_root / "events_latest.json", events)
        # The manifest is the publication commit marker.  Bind the exact
        # event bytes it follows so an independent consumer cannot accept a
        # count-matching but different mutable payload.
        events_artifact = output_root / "events_latest.json"
        events_sha256 = hashlib.sha256(events_artifact.read_bytes()).hexdigest()
        _atomic_write_csv(output_root / "events_latest.csv", event_rows, EVENT_FIELDS)
        _atomic_write_json(output_root / "pair_event_tags_latest.json", pair_events)
        _atomic_write_csv(
            output_root / "pair_event_tags_latest.csv",
            pair_events,
            PAIR_EVENT_FIELDS,
        )
        context_payload = {
            "schema_version": PAIR_CONTEXT_VERSION,
            "generated_utc": iso_utc(),
            "as_of_utc": iso_utc(as_of),
            "instrument_count": len(universe),
            "instruments": universe,
            "pairs": pair_context,
        }
        _atomic_write_json(
            output_root / "latest_pair_news_context.json",
            context_payload,
        )
        sqlite_status = "skipped_live_refresh"
        if write_sqlite:
            _write_catalog_sqlite(
                output_root / "news_event_tags.sqlite",
                events,
                pair_events,
            )
            sqlite_status = "updated"
        active_pair_count = sum(
            1 for value in pair_context.values() if value.get("active_event_count")
        )
        manifest = {
            "manifest_schema_version": 2,
            "pipeline_version": PIPELINE_VERSION,
            "generated_utc": iso_utc(),
            "seed_path": str(seed_path),
            "event_count": len(events),
            "events_artifact_name": events_artifact.name,
            "events_sha256": events_sha256,
            "verified_event_count": sum(
                1 for event in events if event.get("source_verified")
            ),
            "instrument_count": len(universe),
            "pair_event_tag_count": len(pair_events),
            "active_pair_count": active_pair_count,
            "sqlite_refresh_status": sqlite_status,
            "source_type_counts": dict(
                sorted(
                    {
                        source_type: sum(
                            1
                            for event in events
                            if event.get("source_type") == source_type
                        )
                        for source_type in {
                            str(event.get("source_type") or "") for event in events
                        }
                    }.items()
                )
            ),
            "outputs": {
                "events_json": str(output_root / "events_latest.json"),
                "events_csv": str(output_root / "events_latest.csv"),
                "pair_event_tags_json": str(
                    output_root / "pair_event_tags_latest.json"
                ),
                "pair_event_tags_csv": str(
                    output_root / "pair_event_tags_latest.csv"
                ),
                "pair_context": str(
                    output_root / "latest_pair_news_context.json"
                ),
                "sqlite": str(output_root / "news_event_tags.sqlite"),
            },
            "interpretation": (
                "Timestamped news tags are evidence only. A tag never authorizes "
                "an order and unmatched movements remain explicitly unmatched."
            ),
        }
        _atomic_write_json(output_root / "manifest.json", manifest)
        return {"status": "ok", **manifest}
    finally:
        try:
            lock.unlink()
        except OSError:
            pass


def ensure_catalog_current(
    *,
    instruments: Iterable[Any] | None = None,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    max_age_seconds: int = 300,
) -> dict[str, Any]:
    context_path = output_root / "latest_pair_news_context.json"
    stale = True
    try:
        stale = time.time() - context_path.stat().st_mtime > max(1, max_age_seconds)
    except OSError:
        stale = True
    if stale:
        synchronize_catalog(instruments=instruments, output_root=output_root)
    payload = _load_json(context_path, {})
    if not isinstance(payload, dict):
        return {}
    supplied = discover_instruments(instruments)
    pairs = payload.get("pairs") if isinstance(payload.get("pairs"), dict) else {}
    if supplied and not set(supplied).issubset(set(pairs)):
        synchronize_catalog(instruments=supplied, output_root=output_root)
        payload = _load_json(context_path, {})
    return payload if isinstance(payload, dict) else {}


def load_pair_events(
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> list[dict[str, Any]]:
    payload = _load_json(output_root / "pair_event_tags_latest.json", [])
    return [dict(row) for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []


def append_live_movement_links(
    movement: Mapping[str, Any],
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    pair_events: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    rows = list(pair_events) if pair_events is not None else load_pair_events(output_root)
    matches = match_events_for_movement(
        rows,
        instrument=movement.get("instrument"),
        start_utc=movement.get("start_utc") or movement.get("start_timestamp"),
        end_utc=movement.get("end_utc") or movement.get("end_timestamp"),
    )
    summary = summarize_movement_tags(
        movement,
        matches,
        movement_source=str(movement.get("movement_source") or "live_movement_ledger"),
    )
    path = output_root / "live_movement_news_tags.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(summary, default=str, sort_keys=True) + "\n")
    database = output_root / "news_event_tags.sqlite"
    connection = sqlite3.connect(database, timeout=30)
    try:
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS movement_event_links (
                link_id TEXT PRIMARY KEY,
                movement_key TEXT,
                move_id TEXT,
                instrument TEXT NOT NULL,
                start_utc TEXT,
                end_utc TEXT,
                event_id TEXT,
                match_confidence REAL,
                temporal_relation TEXT,
                event_to_move_lead_minutes REAL,
                source TEXT,
                link_json TEXT NOT NULL
            )
            """
        )
        movement_key = str(
            movement.get("movement_key") or movement.get("move_id") or ""
        )
        if matches:
            for match in matches:
                link_id = _stable_id(
                    "link",
                    movement_key,
                    match.get("event_id"),
                    movement.get("start_utc") or movement.get("start_timestamp"),
                )
                connection.execute(
                    """
                    INSERT OR REPLACE INTO movement_event_links (
                        link_id, movement_key, move_id, instrument, start_utc,
                        end_utc, event_id, match_confidence, temporal_relation,
                        event_to_move_lead_minutes, source, link_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        link_id,
                        movement_key,
                        movement.get("move_id"),
                        normalize_instrument(movement.get("instrument")),
                        movement.get("start_utc")
                        or movement.get("start_timestamp"),
                        movement.get("end_utc")
                        or movement.get("end_timestamp"),
                        match.get("event_id"),
                        safe_float(match.get("match_confidence"), 0.0),
                        match.get("temporal_relation"),
                        safe_float(
                            match.get("event_to_move_lead_minutes"), 0.0
                        ),
                        match.get("source_url"),
                        json.dumps(match, default=str, separators=(",", ":")),
                    ),
                )
        else:
            link_id = _stable_id(
                "link",
                movement_key,
                "unmatched",
                movement.get("start_utc") or movement.get("start_timestamp"),
            )
            connection.execute(
                """
                INSERT OR REPLACE INTO movement_event_links (
                    link_id, movement_key, move_id, instrument, start_utc,
                    end_utc, event_id, match_confidence, temporal_relation,
                    event_to_move_lead_minutes, source, link_json
                ) VALUES (?, ?, ?, ?, ?, ?, '', 0, 'unmatched', NULL, '', ?)
                """,
                (
                    link_id,
                    movement_key,
                    movement.get("move_id"),
                    normalize_instrument(movement.get("instrument")),
                    movement.get("start_utc")
                    or movement.get("start_timestamp"),
                    movement.get("end_utc")
                    or movement.get("end_timestamp"),
                    json.dumps(summary, default=str, separators=(",", ":")),
                ),
            )
        connection.commit()
    finally:
        connection.close()
    return {"summary": summary, "matches": matches}


def _atomic_write_dataframe(frame: Any, csv_path: Path, parquet_path: Path) -> None:
    import pandas as pd

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_temp = csv_path.with_name(f".{csv_path.name}.{os.getpid()}.tmp")
    parquet_temp = parquet_path.with_name(
        f".{parquet_path.stem}.{os.getpid()}.tmp.parquet"
    )
    frame.to_csv(csv_temp, index=False)
    parquet_frame = frame.copy()
    numeric_fields = {
        "news_tag_count",
        "primary_pair_relevance",
        "primary_match_confidence",
        "primary_event_to_move_lead_minutes",
        "primary_availability_to_move_lead_minutes",
        "severity",
        "pair_relevance",
        "event_to_move_lead_minutes",
        "availability_to_move_lead_minutes",
        "match_confidence",
        "currency_basket_max_legs",
        "currency_basket_leg_risk_fraction",
    }
    boolean_fields = {
        "primary_source_verified",
        "primary_predictive_eligible",
        "source_verified",
        "reports_prior_market_move",
        "context_only",
        "predictive_eligible",
        "confirmation_eligible",
        "research_only",
        "execution_eligible",
    }
    for column in numeric_fields.intersection(parquet_frame.columns):
        parquet_frame[column] = pd.to_numeric(
            parquet_frame[column],
            errors="coerce",
        )
    for column in boolean_fields.intersection(parquet_frame.columns):
        parquet_frame[column] = (
            parquet_frame[column]
            .map(
                lambda value: (
                    None
                    if value in {"", None}
                    else truthy(value)
                )
            )
            .astype("boolean")
        )
    parquet_frame.to_parquet(parquet_temp, index=False)
    os.replace(csv_temp, csv_path)
    os.replace(parquet_temp, parquet_path)


def _legacy_link_row(
    movement: Mapping[str, Any],
    match: Mapping[str, Any],
) -> dict[str, Any]:
    category = str(match.get("category") or "").lower()
    move_id = str(movement.get("move_id") or movement.get("movement_key") or "")
    event_id = str(match.get("event_id") or "")
    return {
        "link_id": _stable_id("link", move_id, event_id),
        "move_id": move_id,
        "event_id": event_id,
        "event_timestamp": match.get("event_utc"),
        "macro_event_category": match.get("category"),
        "central_bank_event": int(
            "central_bank" in category or "fomc" in category
        ),
        "rate_decision": int("rate" in category or "fomc" in category),
        "inflation_release": int("inflation" in category or "cpi" in category),
        "employment_release": int(
            "employment" in category or "jobs" in category
        ),
        "gdp_release": int("gdp" in category or "growth" in category),
        "fiscal_event": int("fiscal" in category or "tariff" in category),
        "political_event": int("political" in category or "election" in category),
        "geopolitical_event": int(
            "geopolitical" in category or "war" in category
        ),
        "commodity_shock": int(
            "commodity" in category or "oil" in category or "energy" in category
        ),
        "risk_sentiment_shock": int("risk" in category),
        "intervention": int("intervention" in category),
        "policy_speech": int("speech" in category or "guidance" in category),
        "surprise_direction": match.get("expected_pair_direction"),
        "event_to_move_lead_minutes": match.get(
            "event_to_move_lead_minutes"
        ),
        "event_confidence": match.get("match_confidence"),
        "source": match.get("source_url"),
        "notes": (
            f"{match.get('temporal_relation')}; relevance="
            f"{safe_float(match.get('pair_relevance'), 0.0):.3f}; "
            f"{match.get('headline')}"
        )[:2000],
    }


def backfill_significant_moves(
    *,
    source_path: Path = DEFAULT_SIGNIFICANT_MOVES,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    legacy_links_path: Path = DEFAULT_LEGACY_EVENT_LINKS,
    pair_events: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    import pandas as pd

    rows = list(pair_events) if pair_events is not None else load_pair_events(output_root)
    rows_by_instrument = index_pair_events(rows)
    frame = pd.read_parquet(source_path)
    summaries: list[dict[str, Any]] = []
    links: list[dict[str, Any]] = []
    legacy_links: list[dict[str, Any]] = []
    for movement in frame.to_dict(orient="records"):
        matches = match_events_for_movement(
            rows_by_instrument.get(
                normalize_instrument(movement.get("instrument")),
                [],
            ),
            instrument=movement.get("instrument"),
            start_utc=movement.get("start_timestamp"),
            end_utc=movement.get("end_timestamp"),
            include_post_hoc=True,
        )
        summary = summarize_movement_tags(
            movement,
            matches,
            movement_source=str(source_path),
        )
        summaries.append(summary)
        for match in matches:
            link = {
                "link_id": _stable_id(
                    "link",
                    movement.get("move_id"),
                    match.get("event_id"),
                ),
                "move_id": movement.get("move_id"),
                "instrument": movement.get("instrument"),
                "start_utc": movement.get("start_timestamp"),
                "end_utc": movement.get("end_timestamp"),
                **match,
            }
            links.append(link)
            legacy_links.append(_legacy_link_row(movement, match))
    summary_frame = pd.DataFrame(summaries, columns=MOVE_TAG_FIELDS)
    link_columns = list(
        dict.fromkeys(
            [
                "link_id",
                "move_id",
                "instrument",
                "start_utc",
                "end_utc",
                *PAIR_EVENT_FIELDS,
                "temporal_relation",
                "causal_relation",
                "event_to_move_lead_minutes",
                "availability_to_move_lead_minutes",
                "predictive_eligible",
                "confirmation_eligible",
                "match_confidence",
            ]
        )
    )
    links_frame = pd.DataFrame(links, columns=link_columns)
    _atomic_write_dataframe(
        summary_frame,
        output_root / "significant_move_news_tags.csv",
        output_root / "significant_move_news_tags.parquet",
    )
    _atomic_write_dataframe(
        links_frame,
        output_root / "significant_move_news_links.csv",
        output_root / "significant_move_news_links.parquet",
    )
    _atomic_write_csv(
        legacy_links_path,
        legacy_links,
        LEGACY_LINK_FIELDS,
    )
    return {
        "source": str(source_path),
        "movement_count": len(summaries),
        "matched_movement_count": sum(
            1 for row in summaries if row["news_match_status"] == "matched"
        ),
        "unmatched_movement_count": sum(
            1
            for row in summaries
            if row["news_match_status"] != "matched"
        ),
        "link_count": len(links),
        "instrument_count": int(frame["instrument"].nunique()),
        "summary_csv": str(output_root / "significant_move_news_tags.csv"),
        "summary_parquet": str(
            output_root / "significant_move_news_tags.parquet"
        ),
        "links_csv": str(output_root / "significant_move_news_links.csv"),
        "legacy_links_csv": str(legacy_links_path),
    }


def read_csv_tail(path: Path, max_rows: int) -> list[dict[str, Any]]:
    """Read a bounded CSV tail without scanning a large actively appended file."""
    limit = max(1, int(max_rows))
    block_size = 1024 * 1024
    with path.open("rb") as handle:
        header = handle.readline()
        data_start = handle.tell()
        handle.seek(0, os.SEEK_END)
        position = handle.tell()
        chunks: list[bytes] = []
        newline_count = 0
        while position > data_start and newline_count <= limit:
            size = min(block_size, position - data_start)
            position -= size
            handle.seek(position)
            chunk = handle.read(size)
            chunks.append(chunk)
            newline_count += chunk.count(b"\n")
    payload = b"".join(reversed(chunks))
    if position > data_start and b"\n" in payload:
        payload = payload.split(b"\n", 1)[1]
    lines = payload.splitlines()[-limit:]
    text = header.decode("utf-8-sig", errors="replace").rstrip("\r\n")
    if lines:
        text += "\n" + b"\n".join(lines).decode("utf-8", errors="replace")
    return [
        dict(row)
        for row in csv.DictReader(io.StringIO(text, newline=""))
    ]


def backfill_market_movement_ledgers(
    *,
    movement_root: Path = DEFAULT_MARKET_MOVEMENT_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    pair_events: Sequence[Mapping[str, Any]] | None = None,
    max_rows_per_file: int = 25000,
) -> dict[str, Any]:
    rows = list(pair_events) if pair_events is not None else load_pair_events(output_root)
    rows_by_instrument = index_pair_events(rows)
    summaries: list[dict[str, Any]] = []
    seen_movement_keys: set[str] = set()
    source_files = sorted(movement_root.rglob("market_movements.csv")) if movement_root.exists() else []
    source_bytes = 0
    selected_rows = 0
    for path in source_files:
        try:
            source_bytes += path.stat().st_size
            movements = read_csv_tail(path, max_rows_per_file)
        except (OSError, csv.Error):
            continue
        selected_rows += len(movements)
        for movement in movements:
            movement_key = str(movement.get("movement_key") or "").strip()
            if not movement_key:
                movement_key = _stable_id(
                    "movement",
                    movement.get("instrument"),
                    movement.get("start_utc"),
                    movement.get("end_utc"),
                    movement.get("direction"),
                    movement.get("window_minutes"),
                )
                movement["movement_key"] = movement_key
            if movement_key in seen_movement_keys:
                continue
            seen_movement_keys.add(movement_key)
            instrument = normalize_instrument(movement.get("instrument"))
            matches = match_events_for_movement(
                rows_by_instrument.get(instrument, []),
                instrument=instrument,
                start_utc=movement.get("start_utc"),
                end_utc=movement.get("end_utc"),
                include_post_hoc=True,
            )
            summaries.append(
                summarize_movement_tags(
                    movement,
                    matches,
                    movement_source=str(path),
                )
            )
    _atomic_write_csv(
        output_root / "market_movement_news_tags.csv",
        summaries,
        MOVE_TAG_FIELDS,
    )
    return {
        "selection_mode": "bounded_recent_tail",
        "max_rows_per_file": max_rows_per_file,
        "source_file_count": len(source_files),
        "source_bytes": source_bytes,
        "selected_rows_before_deduplication": selected_rows,
        "movement_count": len(summaries),
        "matched_movement_count": sum(
            1 for row in summaries if row["news_match_status"] == "matched"
        ),
        "output_csv": str(output_root / "market_movement_news_tags.csv"),
    }


def build_report(
    sync_result: Mapping[str, Any],
    significant_result: Mapping[str, Any] | None,
    movement_result: Mapping[str, Any] | None,
    *,
    output_root: Path,
) -> Path:
    lines = [
        "# All-Pair News Event Tagging",
        "",
        f"- Generated UTC: `{sync_result.get('generated_utc', iso_utc())}`",
        f"- Canonical events: **{sync_result.get('event_count', 0):,}**",
        f"- Verified events: **{sync_result.get('verified_event_count', 0):,}**",
        f"- OANDA instruments: **{sync_result.get('instrument_count', 0):,}**",
        f"- Event/pair tags: **{sync_result.get('pair_event_tag_count', 0):,}**",
        f"- Pairs with active context: **{sync_result.get('active_pair_count', 0):,}**",
        "",
        "## Historical Coverage",
        "",
    ]
    if significant_result:
        lines.extend(
            [
                f"- Significant movements: **{significant_result.get('movement_count', 0):,}**",
                f"- Time/pair matched: **{significant_result.get('matched_movement_count', 0):,}**",
                f"- Explicitly unmatched: **{significant_result.get('unmatched_movement_count', 0):,}**",
                f"- Event links: **{significant_result.get('link_count', 0):,}**",
                f"- Pair coverage: **{significant_result.get('instrument_count', 0):,}**",
            ]
        )
    else:
        lines.append("- Significant-move backfill was not requested.")
    lines.extend(["", "## Live Movement Ledgers", ""])
    if movement_result:
        lines.extend(
            [
                f"- Source ledgers: **{movement_result.get('source_file_count', 0):,}**",
                f"- Selection mode: **{movement_result.get('selection_mode', 'unknown')}**",
                f"- Maximum source rows per file: **{movement_result.get('max_rows_per_file', 0):,}**",
                f"- Source bytes: **{movement_result.get('source_bytes', 0):,}**",
                f"- Selected rows before stable-key deduplication: **{movement_result.get('selected_rows_before_deduplication', 0):,}**",
                f"- Movement rows: **{movement_result.get('movement_count', 0):,}**",
                f"- Time/pair matched: **{movement_result.get('matched_movement_count', 0):,}**",
            ]
        )
    else:
        lines.append("- Live movement-ledger backfill was not requested.")
    lines.extend(
        [
            "",
            "## Contract",
            "",
            "- Every pair receives a context row, including zero-event rows.",
            "- Every backfilled movement receives a matched or explicitly unmatched result.",
            "- The significant-move catalog is exhaustive; the actively appended 1.6 GB movement ledger uses a bounded recent tail and live appends continue from deployment.",
            "- Pair relevance is based on explicit pair hints, affected currencies, or global-event scope.",
            "- `first_known_utc` prevents a source published after a move from being treated as causal evidence.",
            "- News tags are watch/evidence metadata. They never authorize an order.",
            "",
        ]
    )
    path = output_root / "ALL_PAIR_NEWS_EVENT_TAGGING_LATEST.md"
    _atomic_write_text(path, "\n".join(lines))
    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=Path, default=DEFAULT_SEED_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument(
        "--significant-moves",
        type=Path,
        default=DEFAULT_SIGNIFICANT_MOVES,
    )
    parser.add_argument(
        "--movement-root",
        type=Path,
        default=DEFAULT_MARKET_MOVEMENT_ROOT,
    )
    parser.add_argument("--skip-significant-backfill", action="store_true")
    parser.add_argument("--skip-movement-backfill", action="store_true")
    parser.add_argument(
        "--skip-sqlite",
        action="store_true",
        help="Publish live JSON/CSV/context without refreshing the maintenance SQLite mirror.",
    )
    parser.add_argument("--movement-tail-rows", type=int, default=25000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    sync_result = synchronize_catalog(
        seed_path=args.seed,
        output_root=args.output_root,
        write_sqlite=not args.skip_sqlite,
    )
    if sync_result.get("status") != "ok":
        print(json.dumps(sync_result, indent=2, sort_keys=True))
        return 2
    pair_events = load_pair_events(args.output_root)
    significant_result = None
    if not args.skip_significant_backfill and args.significant_moves.exists():
        significant_result = backfill_significant_moves(
            source_path=args.significant_moves,
            output_root=args.output_root,
            pair_events=pair_events,
        )
    movement_result = None
    if not args.skip_movement_backfill:
        movement_result = backfill_market_movement_ledgers(
            movement_root=args.movement_root,
            output_root=args.output_root,
            pair_events=pair_events,
            max_rows_per_file=args.movement_tail_rows,
        )
    report = build_report(
        sync_result,
        significant_result,
        movement_result,
        output_root=args.output_root,
    )
    result = {
        "sync": sync_result,
        "significant_moves": significant_result,
        "market_movements": movement_result,
        "report": str(report),
    }
    _atomic_write_json(args.output_root / "run_latest.json", result)
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
