#!/usr/bin/env python3
"""No-GPT FX news ingestion and causal local sentiment features.

The service consumes RSS/Atom feeds and the public GDELT DOC endpoint, stores
the first time each item became available locally, and emits research-only
currency and pair scores. It never places orders and never grants account
eligibility to its outputs.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import email.utils
import hashlib
import html
import http.cookiejar
import io
import json
import math
import os
import re
import ssl
import sqlite3
from oanda_news_source_observation_ledger_v1 import (
    CONTRACT as SOURCE_OBSERVATION_LEDGER_CONTRACT,
    initialize as initialize_source_observation_ledger,
    ledger_transaction as source_observation_transaction,
    record_observation as record_source_observation,
    select_observation as select_source_observation,
    bind_active_version as bind_active_source_version,
    source_version_floor, source_version_provenance_valid,
    preserve_active_version as preserve_active_source_version,
    record_projection as record_source_projection,
)
from oanda_news_classification_observation_v1 import (
    CONTRACT as CLASSIFICATION_OBSERVATION_CONTRACT,
    initialize as initialize_classification_observations,
    classification_floor, classification_provenance_valid,
    publication_as_of as classification_publication_as_of,
    record_and_bind as record_and_bind_classification,
)
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from collections import Counter, defaultdict
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import oanda_news_event_tagger as event_tagger
from oanda_news_causal_aggregation_guard_v2 import guard_topic as guard_causal_news_topic
from oanda_news_causal_aggregation_guard_v2 import build_current_news_snapshot
from oanda_news_causal_aggregation_guard_v2 import MAX_CURRENT_SNAPSHOT_BYTES
from oanda_news_collector_contract import (
    NEWS_COLLECTOR_COHORT_ID,
    NEWS_COLLECTOR_CONTRACT_ID,
)
from oanda_news_classification_contract import (
    CONFLICT_DURATION_RECAP_GUARD_ACTIVATED_UTC_V1,
    CONFLICT_DURATION_RECAP_GUARD_COHORT_ID_V1,
    CONFLICT_DURATION_RECAP_GUARD_CONTRACT_ID_V1,
    DEESCALATION_PROPOSAL_GUARD_ACTIVATED_UTC_V1,
    DEESCALATION_PROPOSAL_GUARD_COHORT_ID_V1,
    DEESCALATION_PROPOSAL_GUARD_CONTRACT_ID_V1,
    ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_ACTIVATED_UTC_V2,
    ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_COHORT_ID_V2,
    ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_CONTRACT_ID_V2,
    ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_CURRENCIES_V2,
    JAPAN_EXTERNAL_POLICY_PRESSURE_ACTIVATED_UTC_V1,
    JAPAN_EXTERNAL_POLICY_PRESSURE_COHORT_ID_V1,
    JAPAN_EXTERNAL_POLICY_PRESSURE_CONTRACT_ID_V1,
    NEWS_CLASSIFICATION_VERSION,
    OFFICIAL_DEFENSE_NONMARKET_HEALTH_GUARD_ACTIVATED_UTC_V1,
    OFFICIAL_DEFENSE_NONMARKET_HEALTH_GUARD_COHORT_ID_V1,
    OFFICIAL_DEFENSE_NONMARKET_HEALTH_GUARD_CONTRACT_ID_V1,
    OFFICIAL_SEARCH_POLICY_RATE_ACTIVATED_UTC_V1,
    OFFICIAL_SEARCH_POLICY_RATE_COHORT_ID_V1,
    OFFICIAL_SEARCH_POLICY_RATE_CONTRACT_ID_V1,
    OFFICIAL_SEARCH_POLICY_RATE_SOURCE_CURRENCIES_V1,
    SECONDARY_MARKET_STATE_GUARD_ACTIVATED_UTC_V1,
    SECONDARY_MARKET_STATE_GUARD_COHORT_ID_V1,
    SECONDARY_MARKET_STATE_GUARD_CONTRACT_ID_V1,
)

try:
    import certifi
except ImportError:  # pragma: no cover - standard trust store remains available
    certifi = None

try:
    import truststore
except ImportError:  # pragma: no cover - standard/certifi trust remains available
    truststore = None

try:
    import requests
except ImportError:  # pragma: no cover - urllib remains the default transport
    requests = None

try:
    import pypdf
    from pypdf.errors import PdfReadError
except ImportError:  # pragma: no cover - collector reports the optional gap
    pypdf = None

    class PdfReadError(Exception):
        pass


UTC = dt.timezone.utc
ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "news_sources_v1.json"
DEFAULT_OUTPUT_ROOT = (
    ROOT / "data" / "oanda_training_manager" / "local_news_sentiment"
)
DEFAULT_LEDGER = (
    ROOT
    / "data"
    / "forex_gpt_manager"
    / "local_no_gpt_news"
    / "news_watch_ledger.csv"
)
DEFAULT_EVENT_ROOT = ROOT / "data" / "oanda_training_manager" / "news_event_tags"
DEFAULT_BROKER_CLOCK_HEARTBEAT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "state"
    / "practice_007_top_executor_heartbeat_v1.json"
)
DEFAULT_CLOCK_INTEGRITY_STATE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "state"
    / "clock_integrity_v1.json"
)
OBSERVATION_TIME_CONTRACT_ID = (
    "observation_time_v4_fail_closed_consistent_integrity_sources_20260817"
)
COLLECTOR_CONTRACT_ID = NEWS_COLLECTOR_CONTRACT_ID
COLLECTOR_COHORT_ID = NEWS_COLLECTOR_COHORT_ID
# Scheduled event windows are consumed by the signal matrix.  A 15-minute
# materialization interval could make a 60-minute pre-window appear as much as
# 15 minutes late even though the event itself was already known.  Four
# minutes keeps the derived pair context aligned with the collector cadence
# without turning the relatively heavy catalog rebuild into a per-cycle job.
EVENT_CATALOG_REFRESH_SECONDS = 240
EVENT_CATALOG_LOCK_STALE_SECONDS = 600
SCHEMA_VERSION = "local_fx_news_sentiment_v3"
# Source-specific numeric contracts below are prospective-only. Version 100
# migrates retained rows after the latest official numeric-parser additions.
CLASSIFICATION_VERSION = NEWS_CLASSIFICATION_VERSION
DERIVED_SOURCE_LINEAGE_VERSION = "derived_source_config_lineage_v1"
OFFICIAL_PDF_ATTACHMENT_PARSER_CONTRACT_ID = (
    "official_same_host_pdf_attachment_v1_pypdf_bounded_20260827"
)
GOOGLE_NEWS_OFFICIAL_PUBLISHER_RESOLUTION_CONTRACT_ID = (
    "google_news_official_publisher_resolution_v1_batchexecute_trusted_host_20260901"
)
GOOGLE_NEWS_BATCH_EXECUTE_URL = (
    "https://news.google.com/_/DotsSplashUi/data/batchexecute"
)
SCHEDULED_OFFICIAL_PDF_PROBE_CONTRACT_ID = (
    "scheduled_official_pdf_probe_v1_exact_event_clock_bounded_20260827"
)
RECLASSIFICATION_BATCH_SIZE = 5000
SECONDARY_MACRO_HEADLINE_PARSER_ACTIVATED_UTC = "2026-08-15T21:12:00+00:00"
SECONDARY_JAPAN_MACHINERY_PARSER_ACTIVATED_UTC = (
    "2026-08-19T01:10:00+00:00"
)
SECONDARY_JAPAN_PMI_PARSER_ACTIVATED_UTC = (
    "2026-09-01T01:30:00+00:00"
)
ISSUER_BOUND_POLICY_COMMUNICATION_CONTRACT_ID = (
    "issuer_bound_policy_communication_v1_direct_verified_20260829"
)
ISSUER_BOUND_POLICY_COMMUNICATION_COHORT_ID = (
    "issuer_bound_policy_communication_v1_20260829a"
)
ISSUER_BOUND_POLICY_COMMUNICATION_ACTIVATED_UTC = (
    "2026-08-29T12:20:00+00:00"
)
TLS_CONTEXT = (
    truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    if truststore is not None
    else ssl.create_default_context(
        cafile=certifi.where() if certifi is not None else None
    )
)
_PROCESS_DATABASE_CONNECTIONS: dict[str, sqlite3.Connection] = {}
_WAL_CHECKPOINT_LAST_ATTEMPT: dict[str, float] = {}


def source_config_lineage(source: Mapping[str, Any]) -> dict[str, Any]:
    """Bind an otherwise unversioned source to its exact configured contract.

    Explicit source contracts remain authoritative.  The derived fallback is
    prospective metadata only: it does not change source_id, event identity,
    polling cadence, or any retained article.
    """

    result = dict(source)
    if clean_text(result.get("source_contract_id")):
        return result
    canonical = {
        key: value
        for key, value in result.items()
        if key not in {
            "source_contract_id", "source_cohort_id",
            "source_contract_derived", "source_lineage_version",
            "source_config_sha256",
        }
    }
    encoded = json.dumps(
        canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    source_id = re.sub(
        r"[^a-z0-9_.-]+", "_", clean_text(result.get("source_id")).casefold()
    ).strip("_") or "unknown"
    contract = f"{DERIVED_SOURCE_LINEAGE_VERSION}:{source_id}:{digest[:24]}"
    result.update({
        "source_contract_id": contract,
        "source_cohort_id": contract,
        "source_contract_derived": True,
        "source_lineage_version": DERIVED_SOURCE_LINEAGE_VERSION,
        "source_config_sha256": digest,
    })
    return result


def apply_source_config_lineage(config: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(config)
    result["sources"] = [
        source_config_lineage(source) if isinstance(source, Mapping) else source
        for source in config.get("sources") or []
    ]
    return result


def migrate_derived_source_state(
    source: Mapping[str, Any], state: Mapping[str, Any], *, now: dt.datetime
) -> dict[str, Any]:
    """Adopt fallback lineage without resetting validators or poll history."""

    result = dict(state)
    if source.get("source_contract_derived") is not True:
        return result
    configured = clean_text(source.get("source_contract_id"))
    prior = clean_text(result.get("source_contract_id"))
    if configured and configured != prior:
        result["source_contract_id"] = configured
        result["source_cohort_id"] = clean_text(source.get("source_cohort_id"))
        result["source_contract_derived"] = True
        result["source_lineage_version"] = clean_text(
            source.get("source_lineage_version")
        )
        result["source_config_sha256"] = clean_text(
            source.get("source_config_sha256")
        )
        result["source_lineage_adopted_utc"] = iso_utc(now)
        if prior:
            result["previous_source_contract_id"] = prior
    return result

CORE_CURRENCIES = (
    "AUD",
    "CAD",
    "CHF",
    "EUR",
    "GBP",
    "JPY",
    "NZD",
    "USD",
)
ALL_CURRENCIES = (
    "AUD",
    "CAD",
    "CHF",
    "CNH",
    "CZK",
    "DKK",
    "EUR",
    "GBP",
    "HKD",
    "HUF",
    "JPY",
    "MXN",
    "NOK",
    "NZD",
    "PLN",
    "SEK",
    "SGD",
    "THB",
    "TRY",
    "USD",
    "ZAR",
)
HAVENS = {"CHF", "JPY", "USD"}
RISK_CURRENCIES = {"AUD", "CAD", "MXN", "NOK", "NZD", "ZAR"}
OIL_EXPORTERS = {"CAD", "MXN", "NOK"}
FORCE_HTTPS_HOSTS = {
    "bankofcanada.ca",
    "bankofengland.co.uk",
    "boj.or.jp",
    "cnb.cz",
    "ecb.europa.eu",
    "federalreserve.gov",
    "hkma.gov.hk",
    "mof.go.jp",
    "norges-bank.no",
    "rba.gov.au",
    "riksbank.se",
    "snb.ch",
    "tcmb.gov.tr",
}


def estimated_news_reaction_horizon_minutes(category: Any) -> int:
    """Category prior for the first tradable reaction, not event relevance TTL."""
    normalized = str(category or "market_news").lower().strip()
    if normalized in {"risk_off_geopolitical_or_financial", "trade_policy"}:
        return 360
    if normalized in {"monetary_policy", "fx_intervention", "commodity_shock"}:
        return 180
    return 60


def news_horizon_label(minutes: Any) -> str:
    value = max(1, int(safe_float(minutes, 60.0)))
    if value == 1440:
        return "D1"
    if value >= 60 and value % 60 == 0:
        return f"H{value // 60}"
    return f"M{value}"

CURRENCY_ALIASES: dict[str, tuple[str, ...]] = {
    "AUD": (
        r"\baud\b",
        r"\baustralian dollar\b",
        r"\baussie\b",
        r"\breserve bank of australia\b",
        r"\brba\b",
        r"\baustralia(?:n)?\b",
    ),
    "CAD": (
        r"\bcad\b",
        r"\bcanadian dollar\b",
        r"\bloonie\b",
        r"\bbank of canada\b",
        r"\bboc\b",
        r"\bcanada(?:['’]s)?\b",
        r"\bcanadian(?:['’]s)?\b",
    ),
    "CHF": (
        r"\bchf\b",
        r"\bswiss franc\b",
        r"\bswiss national bank\b",
        r"\bsnb\b",
        r"\bswitzerland\b",
        r"\bswiss\b",
    ),
    "CNH": (
        r"\bcnh\b",
        r"\boffshore yuan\b",
        r"\bchinese yuan\b",
        r"\brenminbi\b",
        r"\bpbo[cC]\b",
        r"\bpeople's bank of china\b",
        r"\bchina\b",
        r"\bchinese\b",
    ),
    "CZK": (
        r"\bczk\b",
        r"\bczech koruna\b",
        r"\bczech national bank\b",
        r"\bczech republic\b",
        r"\bczechia\b",
        r"\bczech\b",
    ),
    "DKK": (
        r"\bdkk\b",
        r"\bdanish krone\b",
        r"\bdanmarks nationalbank\b",
        r"\bdenmark\b",
        r"\bdanish\b",
    ),
    "EUR": (
        r"\beur\b",
        r"\beuro\b",
        r"\beurozone\b",
        r"\beuro area\b",
        r"\beuropean central bank\b",
        r"\becb\b",
        r"\blagarde\b",
    ),
    "GBP": (
        r"\bgbp\b",
        r"\bbritish pound\b",
        r"\bpound sterling\b",
        r"\bsterling\b",
        r"\bbank of england\b",
        r"\bboe\b",
        r"\buk economy\b",
        r"\bbritish economy\b",
        r"\bunited kingdom\b",
        r"\buk\b",
        r"\bbritain\b",
        r"\bbritish\b",
        r"\bbritons?\b",
    ),
    "HKD": (
        r"\bhkd\b",
        r"\bhong kong dollar\b",
        r"\bhkma\b",
        r"\bhong kong\b",
    ),
    "HUF": (
        r"\bhuf\b",
        r"\bhungarian forint\b",
        r"\bmagyar nemzeti bank\b",
        r"\bhungary\b",
        r"\bhungarian\b",
    ),
    "JPY": (
        r"\bjpy\b",
        r"\bjapanese yen\b",
        r"\byen\b",
        r"\bbank of japan\b",
        r"\bboj\b",
        r"\bueda\b",
        r"\bjapan(?:ese)?\b",
        r"日本銀行",
        r"日銀",
        r"日本円",
        r"円(?:安|高|相場|買い|売り)?",
    ),
    "MXN": (
        r"\bmxn\b",
        r"\bmexican peso\b",
        r"\bbanxico\b",
        r"\bmexico\b",
        r"\bmexican\b",
    ),
    "NOK": (
        r"\bnok\b",
        r"\bnorwegian krone\b",
        r"\bnorges bank\b",
        r"\bnorway\b",
        r"\bnorwegian\b",
    ),
    "NZD": (
        r"\bnzd\b",
        r"\bnew zealand dollar\b",
        r"\bkiwi dollar\b",
        r"\breserve bank of new zealand\b",
        r"\brbnz\b",
        r"\bnew zealand\b",
    ),
    "PLN": (
        r"\bpln\b",
        r"\bpolish zloty\b",
        r"\bnational bank of poland\b",
        r"\bpoland\b",
        r"\bpolish\b",
    ),
    "SEK": (
        r"\bsek\b",
        r"\bswedish krona\b",
        r"\briksbank\b",
        r"\bsweden\b",
        r"\bswedish\b",
    ),
    "SGD": (
        r"\bsgd\b",
        r"\bsingapore dollar\b",
        r"\bmonetary authority of singapore\b",
        r"\bsingapore\b",
    ),
    "THB": (
        r"\bthb\b",
        r"\bthai baht\b",
        r"\bbank of thailand\b",
        r"\bthailand\b",
        r"\bthai\b",
    ),
    "TRY": (
        r"\btry\b",
        r"\bturkish lira\b",
        r"\bcentral bank of turkey\b",
        r"\bturkey\b",
        r"\bt[üu]rkiye\b",
        r"\bturkish\b",
    ),
    "USD": (
        r"\busd\b",
        r"\bu\.s\. dollar\b",
        r"\bus dollar\b",
        r"\bgreenback\b",
        r"\bfederal reserve\b",
        r"\bthe fed\b",
        r"\bfed['’]s\b",
        r"\bpowell\b",
        r"\bu\.s\. economy\b",
        r"\bus economy\b",
        # A bare ``US`` is too ambiguous to be a currency alias.  In a PMI
        # release headline, however, the country/series construction is
        # specific enough to retain the observation as USD macro context.
        r"\bu\.?s\.?\s+(?:ism\s+)?(?:services|manufacturing|composite)\s+pmi\b",
        r"\bunited states\s+(?:ism\s+)?(?:services|manufacturing|composite)\s+pmi\b",
        r"\binstitute for supply management\b",
    ),
    "ZAR": (
        r"\bzar\b",
        r"\bsouth african rand\b",
        r"\bsarb\b",
        r"\bsouth africa\b",
        r"\bsouth african\b",
    ),
}
POLICY_INSTITUTION_PATTERNS: dict[str, tuple[str, ...]] = {
    "AUD": (r"\breserve bank of australia\b", r"\brba\b"),
    "CAD": (r"\bbank of canada\b", r"\bboc\b"),
    "CHF": (r"\bswiss national bank\b", r"\bsnb\b"),
    "CNH": (r"\bpeople'?s bank of china\b", r"\bpbo[cC]\b"),
    "CZK": (r"\bczech national bank\b", r"\bcnb\b"),
    "DKK": (r"\bdanmarks nationalbank\b",),
    "EUR": (r"\beuropean central bank\b", r"\becb\b"),
    "GBP": (r"\bbank of england\b", r"\bboe\b"),
    "HKD": (r"\bhong kong monetary authority\b", r"\bhkma\b"),
    "HUF": (r"\bmagyar nemzeti bank\b", r"\bmnb\b"),
    "JPY": (r"\bbank of japan\b", r"\bboj\b"),
    "MXN": (r"\bbanco de m[eé]xico\b", r"\bbanxico\b"),
    "NOK": (r"\bnorges bank\b",),
    "NZD": (r"\breserve bank of new zealand\b", r"\brbnz\b"),
    "PLN": (r"\bnarodowy bank polski\b", r"\bnational bank of poland\b", r"\bnbp\b"),
    "SEK": (r"\bsveriges riksbank\b", r"\briksbank\b"),
    "SGD": (r"\bmonetary authority of singapore\b", r"\bmas\b"),
    "THB": (r"\bbank of thailand\b", r"\bbot\b"),
    "TRY": (
        r"\bcentral bank of the republic of t[üu]rkiye\b",
        r"\bcentral bank of turkey\b",
        r"\b(?:tcmb|cbrt)\b",
    ),
    "USD": (
        r"\bfederal reserve\b",
        r"\bthe fed\b",
        r"\bfed['’]s\b",
        r"\bfomc\b",
    ),
    "ZAR": (r"\bsouth african reserve bank\b", r"\bsarb\b"),
}

MARKET_TERMS = (
    "interest rate",
    "policy rate",
    "rate policy",
    "monetary policy",
    "central bank",
    "inflation",
    "consumer price",
    "producer price",
    "employment",
    "labour",
    "unemployment",
    "payroll",
    "wage",
    "gdp",
    "growth",
    "recession",
    "contraction",
    "currency",
    "exchange rate",
    "forex",
    "foreign exchange",
    "intervention",
    "tariff",
    "tariffs",
    "import tax",
    "import taxes",
    "customs duty",
    "customs duties",
    "sanction",
    "war",
    "attack",
    "conflict",
    "ceasefire",
    "oil",
    "crude",
    "commodity",
    "financial stability",
    "banking crisis",
    "yield",
    "bond market",
    "risk-off",
    "risk on",
    "manufacturing pmi",
    "services pmi",
    "composite pmi",
    "purchasing managers index",
    "金融政策",
    "利上げ",
    "利下げ",
    "円安",
    "円高",
    "為替介入",
    "物価",
    "金利",
)

HAWKISH_TERMS: dict[str, float] = {
    "rate hike": 0.9,
    "rate hikes": 0.9,
    "faster rate hikes": 0.9,
    "quicker rate hikes": 0.9,
    "further rate hikes": 0.8,
    "accelerate rate hikes": 0.9,
    "faster hike pace": 0.8,
    "hasten rate hike pace": 0.8,
    "increase the ocr": 1.0,
    "interest rate rise": 0.8,
    "interest rates rise": 0.8,
    "rates rise": 0.6,
    "raises interest rate": 1.0,
    "raised interest rate": 1.0,
    "raises policy rate": 1.0,
    "raised policy rate": 1.0,
    "raise the cash rate": 1.0,
    "increase the cash rate": 1.0,
    "raised the cash rate": 1.0,
    "increased the cash rate": 1.0,
    "raise the policy rate": 1.0,
    "increase the policy rate": 1.0,
    "including increasing the cash rate": 0.55,
    "higher for longer": 0.75,
    "hawkish": 0.65,
    "tightening": 0.55,
    "inflation accelerates": 0.55,
    "inflation rises": 0.45,
    "inflation above forecast": 0.6,
    "stronger than expected": 0.45,
    "beats forecast": 0.45,
    "employment rises": 0.4,
    "payrolls rise": 0.4,
    "wage growth": 0.3,
    "利上げ": 0.9,
    "利上げ加速": 0.9,
}
DOVISH_TERMS: dict[str, float] = {
    "rate cut": -0.9,
    "cuts interest rate": -1.0,
    "cut interest rate": -1.0,
    "lowers policy rate": -1.0,
    "lowered policy rate": -1.0,
    "lower the cash rate": -1.0,
    "decrease the cash rate": -1.0,
    "lowered the cash rate": -1.0,
    "decreased the cash rate": -1.0,
    "lower the policy rate": -1.0,
    "decrease the policy rate": -1.0,
    "dovish": -0.65,
    "monetary easing": -0.55,
    "quantitative easing": -0.5,
    "recession": -0.6,
    "economic contraction": -0.55,
    "weaker than expected": -0.45,
    "misses forecast": -0.45,
    "unemployment rises": -0.4,
    "inflation slows": -0.45,
    "job losses": -0.45,
    "growth slows": -0.35,
    "利下げ": -0.9,
}
RISK_OFF_TERMS: dict[str, float] = {
    "war": 0.7,
    "military attack": 0.8,
    "missile attack": 0.9,
    "air strike": 0.8,
    "sanctions": 0.55,
    "banking crisis": 0.8,
    "market crash": 0.9,
    "stock selloff": 0.65,
    "risk-off": 0.7,
    "geopolitical tensions": 0.5,
    "trade war": 0.6,
    "tariff escalation": 0.55,
}
RISK_ON_TERMS: dict[str, float] = {
    "ceasefire": 0.65,
    "peace deal": 0.75,
    "deal to reopen the strait of hormuz": 0.75,
    # A credible report that a Hormuz agreement is possible is a risk-regime
    # setup, not yet a completed agreement. Keep its weight below a concrete
    # reopening deal while mapping the haven/risk transmission exposed by the
    # August 6 CHF case.
    "strait of hormuz deal": 0.4,
    "tariff rollback": 0.55,
    "trade agreement": 0.5,
    "risk-on": 0.6,
}

# Status-quo language can be important context without being a newly observed
# catalyst.  It must not re-arm a broad geopolitical basket merely because the
# sentence repeats words such as ``closed``, ``sanctions``, or ``military``.
ONGOING_CONFLICT_STATUS_PATTERNS = (
    r"\bremains?\s+(?:closed|blocked|suspended|under blockade)\b",
    r"\bstalemate\s+(?:continues|extends|persists)\b",
    r"\bno\s+change\s+in\b.{0,48}\b(?:blockade|closure|sanctions|operations)\b",
    r"\buntil\b.{0,96}\b(?:blockade|sanctions|frozen assets|operations)\b",
    r"\b(?:due to|amid|because of)\b.{0,64}\bblockade\b",
    r"\bblockade\b.{0,64}\b(?:causes?|disrupts?|risks?|threatens?)\b",
)
SUPPLY_ROUTE_RELIEF_PATTERNS = (
    r"\bnaval escorts?\b.{0,48}\b(?:early )?(?:results?|success)\b",
    r"\b(?:barrels?|cargo|ships?|tankers?|traffic)\b.{0,64}"
    r"\b(?:break|breaks|breaking|pass|passes|passing|move|moves|moving)\b"
    r".{0,32}\b(?:through|past)\b.{0,32}\bblockade\b",
    r"\b(?:shipping|traffic|oil flows?)\b.{0,40}"
    r"\b(?:resume|resumes|resumed|restore|restores|restored)\b",
    r"\bblockade\b.{0,40}\b(?:breached|broken|eased)\b",
)
NEW_CONFLICT_ACTION_PATTERNS = (
    r"\b(?:attacks?|attacked|strikes?|struck|hits?|hit|fires?|fired|"
    r"launches?|launched|seizes?|seized|closes?|closed|blocks?|blocked|"
    r"imposes?|imposed|expands?|expanded)\b",
    r"\b(?:missile|projectile|drone|airstrike|explosion)\b",
)
OFFICIAL_DEFENSE_NONMARKET_HEALTH_PATTERNS = (
    r"\bclinical\s+guidance\b.{0,96}\b(?:health|human\s+performance)\b",
    r"\bhealth\s+and\s+human\s+performance\s+optimi[sz]ation\b",
)
OFFICIAL_NON_MARKET_PROGRAM_PATTERNS = (
    r"\b(?:culture|cultural|arts?|heritage)\s+(?:festival|programme|program|event)\b",
    r"\brenewable\s+energy\s+(?:cooperation|partnership|dialogue|forum)\b",
    r"\b(?:commemorative|ceremonial|courtesy)\s+(?:visit|meeting|event)\b",
    # Defense-agency health/personnel guidance can contain standing readiness
    # boilerplate ("lethal force", "battlefield", "peace through strength")
    # without reporting a new conflict action.  Preserve the official item as
    # immutable context, but do not manufacture a global risk-off observation.
) + OFFICIAL_DEFENSE_NONMARKET_HEALTH_PATTERNS
NON_EVENT_WAR_PATTERNS = (
    # A procurement headline names a department and a company award; it does
    # not report a new conflict event merely because the agency contains "War".
    r"\bdepartment of war awards?\b",
    # Macro coverage comparing household income with conflict-driven
    # inflation is context about economic effects, not a fresh escalation.
    r"\b(?:wages?|pay|income|earnings)\b.{0,32}"
    r"\b(?:outpace|outstrip|beat)\b.{0,48}\b(?:war|conflict)\s+inflation\b",
    # Metaphorical campaigns and incidental references are not reports of a
    # fresh geopolitical escalation.
    r"\bwar on (?:involution|drugs|poverty|waste|talent|prices?|cancer)\b",
    r"\btug of war\b",
    r"\bwar of words\b",
    r"\bdespite\b.{0,56}\b(?:war|conflict)\b",
    r"\bsince\b.{0,32}\b(?:war|conflict)\b",
)
FAILED_DEESCALATION_PATTERNS = (
    r"\b(?:ends?|ended|breaks?|broke|broken|violates?|violated|abandons?|"
    r"abandoned|rejects?|rejected)\b.{0,40}\b(?:ceasefire|truce|peace deal)\b",
    r"\b(?:ceasefire|truce|peace deal)\b.{0,40}\b(?:ends?|ended|collapses?|"
    r"collapsed|fails?|failed|breaks?|broken|violated|unravels?|"
    r"expires?|expired|expiry|"
    r"(?:is|was|declared)\s+over)\b",
    # Negotiations that explicitly make no progress are not a risk-on peace
    # event merely because the headline contains the phrase "peace deal".
    r"\b(?:no|little|without)\s+progress\b.{0,64}"
    r"\b(?:ceasefire|truce|peace deal|peace talks?)\b",
    r"\b(?:ceasefire|truce|peace deal|peace talks?)\b.{0,64}"
    r"\b(?:no|little|without)\s+progress\b",
    r"\b(?:deal|agreement)\b.{0,64}"
    r"\b(?:not feasible|stalls?|stalled|collapses?|collapsed|fails?|failed|"
    r"ruled out|rejected)\b",
    r"\b(?:not feasible|stalls?|stalled|collapses?|collapsed|fails?|failed|"
    r"ruled out|rejected)\b.{0,64}\b(?:deal|agreement)\b",
)
DEESCALATION_PROPOSAL_PATTERNS = (
    r"\b(?:calls?\s+for|urges?|seeks?|proposes?|requests?|demands?|"
    r"asks?\s+for|wants?)\b.{0,64}\b(?:ceasefire|truce|peace\s+deal|"
    r"peace\s+talks?)\b",
    r"\b(?:ceasefire|truce|peace\s+deal|peace\s+talks?)\b.{0,64}"
    r"\b(?:calls?|urges?|seeks?|proposes?|requests?|demands?|asks?|wants?)\b",
)
DEESCALATION_ACTUALIZATION_PATTERNS = (
    r"\b(?:agrees?|agreed|reaches?|reached|signs?|signed|announces?|"
    r"announced|implements?|implemented)\b.{0,64}\b(?:ceasefire|truce|"
    r"peace\s+deal|peace\s+agreement)\b",
    r"\b(?:ceasefire|truce|peace\s+deal|peace\s+agreement)\b.{0,64}"
    r"\b(?:agreed|reached|signed|announced|implemented|takes?\s+effect|"
    r"in\s+effect|begins?|began|starts?|started)\b",
)
DIRECT_CONFLICT_ESCALATION_PATTERNS = (
    r"\b(?:war|conflict|hostilities)\b.{0,40}\b(?:escalates?|escalated|"
    r"resumes?|resumed|renews?|renewed|widens?|widened|erupts?|erupted)\b",
    r"\b(?:escalates?|escalated|resumes?|resumed|renews?|renewed|widens?|"
    r"widened|erupts?|erupted)\b.{0,40}\b(?:war|conflict|hostilities)\b",
    r"\b(?:military|missile|air|drone|naval)\s+(?:attack|strike|assault)s?\b",
    r"\b(?:joins?|joined|enters?|entered)\b.{0,32}\b(?:war|conflict|hostilities)\b",
    r"\b(?:war|conflict|hostilities)\b.{0,32}\b(?:joins?|joined|enters?|entered)\b",
    r"\bcomes?\s+under\s+(?:military|missile|air|drone|naval)?\s*attack\b",
    r"\bgeopolitical tensions\b.{0,24}\b(?:rise|rises|rose|escalate|escalates|escalated)\b",
    r"\b(?:rise|rises|rose|escalate|escalates|escalated)\b.{0,24}\bgeopolitical tensions\b",
    r"\b(?:invasion|blockade|bombardment)\b",
    # Maritime attacks at the Hormuz chokepoint are concrete escalation
    # events even when a compact headline omits the weapon or attacker.
    r"\b(?:ship|vessel|tanker|cargo vessel)\b.{0,40}"
    r"\b(?:struck|hit|attacked)\b.{0,48}"
    r"\b(?:hormuz|gulf of oman|oman(?:'s)? coast)\b",
    r"\b(?:hormuz|gulf of oman|oman(?:'s)? coast)\b.{0,48}"
    r"\b(?:ship|vessel|tanker|cargo vessel)\b.{0,40}"
    r"\b(?:struck|hit|attacked)\b",
)
CANCELLED_CONFLICT_ACTION_PATTERNS = (
    r"\b(?:halt(?:s|ed)?|cancel(?:s|led|ed)?|call(?:s|ed)? off|"
    r"hold(?:s|ing)? off|held off|delay(?:s|ed)?|pause(?:s|d)?)\b"
    r".{0,48}\b(?:planned\s+)?(?:military|missile|air|drone|naval)?\s*"
    r"(?:attack|strike|assault|bombing)s?\b",
    r"\b(?:military|missile|air|drone|naval)?\s*"
    r"(?:attack|strike|assault|bombing)s?\b.{0,32}"
    r"\b(?:halted|cancelled|canceled|called off|held off|delayed|paused)\b",
)
OFFICIAL_POLICY_RELEASE_PATTERNS = (
    r"\bissues?\s+(?:an?\s+)?fomc statement\b",
    r"\bfomc statement\b",
    r"\bmonetary policy (?:decision|statement|assessment)\b",
    r"\b(?:interest|policy) rate decision\b",
    r"\bbank rate (?:maintained|increased|raised|reduced|lowered|held)\b",
    r"\bofficial cash rate (?:maintained|increased|raised|reduced|lowered|held)\b",
    r"\b(?:repo|reference|base) rate (?:maintained|increased|raised|reduced|lowered|held)\b",
    r"\b(?:bank|policy|official cash|repo|reference|base) rate "
    r"(?:is |was )?(?:unchanged|maintained|held|kept|left)\b",
    r"\bmonetary policy committee(?:'s)? decision\b",
    r"\bsummary of opinions\b",
    r"\bminutes? of (?:the )?monetary policy\b",
    r"\b(?:accounts?|record) of (?:the )?monetary policy\b",
    r"\bmonetary policy report\b",
    r"\bpolicy meeting summary\b",
    r"\bközlemény\s+a\s+monetáris\s+tanács\b",
    r"\b(?:monetary council|monetary policy (?:board|committee)|"
    r"policy (?:board|committee)|governing council|committee|board|council)\b"
    r".{0,180}\b(?:cut|reduce[sd]?|lower(?:ed|s)?|raise[sd]?|increase[sd]?|"
    r"hike[sd]?|leave|left|keep|kept|maintain(?:ed|s)?)\b.{0,56}"
    r"\b(?:central bank )?(?:base|bank|policy|interest|official cash|cash|"
    r"repo|reference) rate\b",
    r"金融政策",
    r"主な意見",
)
PRIMARY_SPENDING_RELEASE_POSITIVE_PATTERNS = (
    r"\b(?:household|consumer|retail)\s+spending\b.{0,32}"
    r"\b(?:up|rose|rises|increased|increases|grew|grows|rebounded|rebounds)\b",
    r"\b(?:up|rose|rises|increased|increases|grew|grows|rebounded|rebounds)\b"
    r".{0,32}\b(?:household|consumer|retail)\s+spending\b",
)
PRIMARY_SPENDING_RELEASE_NEGATIVE_PATTERNS = (
    r"\b(?:household|consumer|retail)\s+spending\b.{0,32}"
    r"\b(?:down|fell|falls|decreased|decreases|declined|declines|contracted|contracts)\b",
    r"\b(?:down|fell|falls|decreased|decreases|declined|declines|contracted|contracts)\b"
    r".{0,32}\b(?:household|consumer|retail)\s+spending\b",
)
ACTIVITY_RELEASE_POSITIVE_PATTERNS = (
    r"\b(?:gdp|economic growth|growth)\b.{0,32}"
    r"\b(?:beats?|outperform(?:s|ed)?|exceeds?|tops?)\b",
    r"\b(?:retail sales?|consumer spending)\b.{0,48}"
    r"\b(?:unexpectedly\s+)?(?:rise|rises|rose|increase|increases|increased|"
    r"gain|gains|gained|rebound|rebounds|rebounded)\b",
    r"\b(?:unexpectedly\s+)?(?:rise|rises|rose|increase|increases|increased|"
    r"gain|gains|gained|rebound|rebounds|rebounded)\b.{0,48}"
    r"\b(?:retail sales?|consumer spending)\b",
)
ACTIVITY_RELEASE_NEGATIVE_PATTERNS = (
    r"\b(?:gdp|economic growth|growth)\b.{0,32}"
    r"\b(?:miss(?:es|ed)?|disappoint(?:s|ed)?|undershoot(?:s|ed)?)\b",
    r"\b(?:retail sales?|consumer spending)\b.{0,48}"
    r"\b(?:unexpectedly\s+)?(?:fall|falls|fell|decrease|decreases|decreased|"
    r"decline|declines|declined|drop|drops|dropped|contract|contracts|contracted)\b",
    r"\b(?:unexpectedly\s+)?(?:fall|falls|fell|decrease|decreases|decreased|"
    r"decline|declines|declined|drop|drops|dropped|contract|contracts|contracted)\b"
    r".{0,48}\b(?:retail sales?|consumer spending)\b",
    r"\bunemployment\s+rate\b.{0,48}"
    r"\b(?:climb|climbs|climbed|rise|rises|rose|increase|increases|increased)\b",
)
POSITIVE_WORDS = {
    "accelerates",
    "advance",
    "beats",
    "expands",
    "gain",
    "growth",
    "improves",
    "optimism",
    "rebound",
    "resilient",
    "rises",
    "strong",
    "surges",
}
NEGATIVE_WORDS = {
    "collapse",
    "conflict",
    "contraction",
    "crisis",
    "decline",
    "falls",
    "fear",
    "misses",
    "recession",
    "risk",
    "slows",
    "tensions",
    "weak",
    "war",
}

POLICY_NEUTRAL_PATTERNS = (
    r"\b(?:expected|forecast|seen|likely)\s+to\s+(?:hold|keep|leave)\b.{0,32}\b(?:rate|rates|policy rate)\b",
    r"\b(?:hold|holds|held|keep|keeps|kept|leave|leaves|left)\b.{0,24}\b(?:rate|rates|policy rate)\b.{0,16}\b(?:unchanged|steady)\b",
    r"\b(?:rate|rates|policy rate)\b.{0,24}\b(?:unchanged|steady|on hold)\b",
    r"\b(?:bank|policy|official cash|repo|reference|base) rate "
    r"(?:is |was )?(?:unchanged|maintained|held|kept|left)\b",
    r"\bnot\b.{0,24}\b(?:edging|moving|leaning)\b.{0,28}"
    r"\b(?:towards?\s+)?(?:a\s+)?(?:rate hike|higher rates)\b",
    r"\b(?:rules? out|no (?:case|need) for)\b.{0,28}\b(?:rate hike|higher rates)\b",
    # A headline can mention a hike while explicitly denying that it will
    # support the currency.  The policy noun is not a bullish currency claim
    # in that construction (for example, "BOJ rate hikes will not save the
    # yen").  Retain the item as context instead of manufacturing a hawkish
    # directional score from the words "rate hike" alone.
    r"\b(?:rate hikes?|tightening|higher rates)\b.{0,48}"
    r"\b(?:(?:will|would|may|might|can|could)\s+not|won't|wouldn't|"
    r"can't|couldn't)\s+(?:save|support|strengthen|rescue|help)\b",
)
POLICY_SPECULATION_PATTERN = re.compile(
    r"\b(?:"
    r"could|may|might|would|possible|potential|potentially|"
    r"weighs?|weighing|mulls?|mulling|considers?|considering|"
    r"speculation|odds|chance|chances|preview|what to expect|"
    r"meeting approaches|decision today|to release (?:its|a) decision|"
    r"prices?\s+in|priced\s+in|pricing\s+in|"
    r"ahead of|awaits?|awaiting|jitters?|dilemma|"
    r"rate (?:hike|cut) meeting|rate (?:hike|cut) bets?|"
    r"rate (?:hike|cut) expectations?|rate (?:hike|cut) fears?|unlikely|less likely"
    r"|poll|forecast(?:s|ed|ing)?|markets?\s+(?:tip|tips|expect|expects|price|prices)"
    r")\b",
    flags=re.I,
)
POLICY_DATA_DEPENDENT_GUIDANCE_PATTERN = re.compile(
    r"\b(?:data|inflation|cpi|jobs?|employment|payrolls?|report|figures?)\b"
    r".{0,64}\b(?:will|would|could|may|might)\s+(?:help\s+)?"
    r"(?:determin(?:e|es|ed|ing)|inform|guide|shape)\b.{0,72}"
    r"\b(?:stance|decision|view|case|support|policy|rate[- ]hikes?|"
    r"rate[- ]cuts?|tightening|easing)\b|"
    r"\b(?:stance|decision|view|case|support|policy)\b.{0,72}"
    r"\b(?:will|would|could|may|might)\s+(?:depend|hinge)\b.{0,64}"
    r"\b(?:data|inflation|cpi|jobs?|employment|payrolls?|report|figures?)\b|"
    r"\b(?:rate[- ]hikes?|rate[- ]cuts?|tightening|easing|policy action)\b"
    r".{0,56}\b(?:depend(?:s|ed|ing)?|hinge(?:s|d|ing)?)\s+on\b"
    r".{0,56}\b(?:data|inflation|cpi|jobs?|employment|payrolls?|report|figures?)\b",
    flags=re.I,
)
POLICY_CONDITIONAL_ACTION_PATTERN = re.compile(
    r"(?:\b(?:rate hikes?|tightening|higher rates?)\b.{0,64}\bif\b|"
    r"\bif\b.{0,64}\b(?:rate hikes?|tightening|higher rates?)\b)",
    flags=re.I,
)
SECONDARY_HOUSEHOLD_COST_CONTEXT_PATTERNS = (
    # Household anecdotes about bills and living costs can mention inflation
    # slowing without reporting a new CPI surprise. Keep these articles for
    # context, but do not turn their sentiment wording into a currency impulse.
    r"\b(?:famil(?:y|ies)|households?|consumers?|workers?)\b.{0,80}"
    r"\b(?:bills?|costs?|cash|budgets?|spending|cost of living|living costs?)\b",
    r"\b(?:smashed|struggling|bleeding|feeling|hit|hurt)\b.{0,80}"
    r"\b(?:bills?|costs?|cash|budgets?|inflation|cost of living)\b",
)
LOCAL_PARALLEL_CURRENCY_MARKET_PATTERN = re.compile(
    r"\b(?:cuba(?:n)?\b.{0,80}\b(?:informal|parallel)\s+(?:currency|fx)\s+market|"
    r"(?:informal|parallel)\s+(?:currency|fx)\s+market\b.{0,100}\b(?:cuba|mlc)\b)",
    flags=re.I,
)
RELEASE_GRADE_CATEGORIES = frozenset(
    {
        "monetary_policy",
        "inflation_release",
        "labor_release",
        "growth_release",
        "manufacturing_release",
        "business_activity_release",
        "trade_balance_release",
        "fx_intervention",
    }
)
CORROBORATION_REQUIRED_CATEGORIES = RELEASE_GRADE_CATEGORIES.union(
    {
        "commodity_shock",
        "risk_off_geopolitical_or_financial",
        "risk_on_deescalation",
        "trade_policy",
    }
)

# Broad topic tags are useful for exposure mapping, but they are not enough to
# prove that two publishers reported the same event.  Without a claim-level
# overlap check, unrelated stories such as an attack report and a general war
# strategy article can share ``middle_east|united_states|escalation`` and
# incorrectly corroborate each other.
SEMANTIC_CLUSTER_STOPWORDS = frozenset(
    {
        "a",
        "after",
        "amid",
        "an",
        "and",
        "as",
        "at",
        "by",
        "for",
        "from",
        "in",
        "into",
        "is",
        "it",
        "latest",
        # Generic market-recap wording is not claim identity.  Without these
        # exclusions, unrelated headlines such as "Canadian dollar rebounds
        # from two-week low" and "Gold falls to two-week low" satisfy the
        # three-token overlap rule and are incorrectly displayed as one story.
        "high",
        "low",
        "one",
        "two",
        "three",
        "week",
        "news",
        "of",
        "on",
        "report",
        "reports",
        "says",
        "the",
        "to",
        "with",
    }
)

LEDGER_FIELDS = (
    "watch_id",
    "status",
    "event_utc",
    "scheduled_utc",
    "timing_precision",
    "event_time_basis",
    "clock_semantics",
    "independent_domestic_event",
    "linked_policy_factor",
    "published_utc",
    "first_known_utc",
    "first_seen_utc",
    "updated_utc",
    "last_seen_utc",
    "headline",
    "summary",
    "category",
    "direct_currencies",
    "currencies",
    "pair_hints",
    "directional_bias",
    "severity",
    "movement_potential",
    "scope",
    "source_name",
    "source_id",
    "source_kind",
    "source_quality",
    "source_direct",
    "retrieval_via",
    "source_role",
    "publisher_url",
    "source_currencies",
    "event_series_id",
    "event_name",
    "event_country",
    "source_contract_id",
    "source_cohort_id",
    "material_content_sha256",
    "release_time_rule_observed_sha256",
    "release_time_rule_archive_name",
    "release_rule_bytes_verified",
    "source_url",
    "source_verified",
    "relevant",
    "directional_publish_eligible",
    "currency_scores",
    "pair_bias",
    "direction",
    "actual",
    "actual_value",
    "consensus",
    "consensus_value",
    "corroboration_count",
    "pre_window_minutes",
    "post_window_minutes",
    "source_type",
    "local_sentiment_score",
    "directional_confidence",
    "topic_tags",
    "intervention_status",
    "reports_prior_market_move",
    "context_only",
    "research_only",
    "directional_research_only",
    "execution_eligible",
    "can_place_orders",
)


def utc_now() -> dt.datetime:
    return dt.datetime.now(tz=UTC)


def iso_utc(value: dt.datetime | None = None) -> str:
    current = value or utc_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    return current.astimezone(UTC).isoformat()


def parse_datetime(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except ValueError:
        pass
    for format_string in (
        "%Y%m%dT%H%M%S",
        "%Y%m%dT%H%M",
        "%d %b %Y",
        "%d %B %Y",
    ):
        try:
            return dt.datetime.strptime(text, format_string).replace(tzinfo=UTC)
        except ValueError:
            pass
    try:
        parsed = email.utils.parsedate_to_datetime(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except (TypeError, ValueError, OverflowError):
        return None


def causal_known_datetime(article: Mapping[str, Any]) -> dt.datetime | None:
    """Earliest defensible use time for an article.

    A feed can be observed slightly before its source-reported publication
    timestamp because of clock skew or pre-release RSS population.  Retain the
    raw arrival for latency diagnostics, but never make a forward feature or
    historical call available before either boundary.
    """

    explicit = parse_datetime(article.get("causal_known_utc"))
    version_floor = source_version_floor(article)
    if version_floor is not None:
        explicit = max(explicit, version_floor) if explicit is not None else version_floor
    derived_floor = classification_floor(article)
    if derived_floor is not None:
        explicit = max(explicit, derived_floor) if explicit is not None else derived_floor
    first_seen = parse_datetime(article.get("first_seen_utc"))
    published = parse_datetime(article.get("published_utc"))
    # Enriched figures are a distinct causal observation from a pre-existing
    # release landing page or headline.  The storage payload deliberately
    # omits the derived ``causal_known_utc`` field, so retain this immutable
    # detail boundary when reconstructing rows for live views and replay.
    detail_available = parse_datetime(article.get("detail_available_utc"))
    inferred_primary_release_guard = bool(
        str(article.get("source_role") or "").lower()
        == "primary_statistical_release"
        and bool(article.get("published_time_inferred"))
        and first_seen is not None
    )
    if explicit is not None:
        # Recompute the safe floor for retained rows written before the
        # publication-hold field existed.
        causal = max(
            candidate
            for candidate in (explicit, first_seen, published, detail_available)
            if candidate is not None
        )
        if inferred_primary_release_guard:
            causal = max(causal, first_seen + dt.timedelta(seconds=60))
        return causal
    # Legacy/synthetic rows did not retain whether a later publisher timestamp
    # was already present at observation time.  Preserve their established
    # first-known boundary instead of inventing a revision-time delay.
    causal = first_seen or published
    if detail_available is not None:
        causal = max(
            candidate
            for candidate in (causal, detail_available)
            if candidate is not None
        )
    if inferred_primary_release_guard and causal is not None:
        causal = max(causal, first_seen + dt.timedelta(seconds=60))
    return causal


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def clamp(value: float, lower: float = -1.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def clean_text(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def canonical_url(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        parsed = urllib.parse.urlsplit(text)
        filtered = [
            (key, val)
            for key, val in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
            if key.lower()
            not in {
                "utm_source",
                "utm_medium",
                "utm_campaign",
                "utm_term",
                "utm_content",
                "gclid",
                "fbclid",
            }
        ]
        scheme = parsed.scheme.lower()
        hostname = (parsed.hostname or "").lower()
        if scheme == "http" and any(
            hostname == domain or hostname.endswith("." + domain)
            for domain in FORCE_HTTPS_HOSTS
        ):
            scheme = "https"
        return urllib.parse.urlunsplit(
            (
                scheme,
                parsed.netloc.lower(),
                parsed.path.rstrip("/"),
                urllib.parse.urlencode(filtered),
                "",
            )
        )
    except ValueError:
        return text


def stable_id(*values: Any) -> str:
    payload = "\x1f".join(str(value or "") for value in values)
    return "local_news_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def normalized_headline(value: Any) -> str:
    text = headline_content(value).lower()
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def headline_content(value: Any, *, publisher_name: Any = "") -> str:
    text = clean_text(value)
    # GDELT/Google-style titles often append a publisher after `` - `` or
    # `` | ``, but those separators also occur inside economically meaningful
    # headlines (for example, ``US - Japan pact to hit yen speculators``).
    # Preserve a short suffix whenever it contains an explicit currency or
    # market term.  The older unconditional truncation silently discarded the
    # JPY clause and created false-negative currency mappings.
    for separator in (" | ", " - "):
        parts = text.split(separator)
        if len(parts) <= 1:
            continue
        suffix = parts[-1].strip()
        suffix_lower = suffix.lower()
        normalized_suffix = re.sub(r"[^a-z0-9]+", " ", suffix_lower).strip()
        normalized_publisher = re.sub(
            r"[^a-z0-9]+",
            " ",
            clean_text(publisher_name).lower(),
        ).strip()
        suffix_is_publisher = bool(
            normalized_publisher and normalized_suffix == normalized_publisher
        )
        looks_like_domain = bool(
            re.search(
                r"(?:^|\s)[a-z0-9][a-z0-9.-]*\.(?:com|org|net|co|io|news)"
                r"(?:\s|$)",
                suffix_lower,
            )
        )
        has_currency_semantics = any(
            re.search(pattern, suffix_lower, flags=re.I)
            for patterns in CURRENCY_ALIASES.values()
            for pattern in patterns
        )
        has_market_semantics = any(
            re.search(
                r"(?<!\w)" + re.escape(term).replace(r"\ ", r"\s+") + r"(?!\w)",
                suffix_lower,
                flags=re.I,
            )
            for term in MARKET_TERMS
        )
        if suffix_is_publisher or looks_like_domain or (
            len(suffix.split()) <= 8
            and not has_currency_semantics
            and not has_market_semantics
        ):
            text = separator.join(parts[:-1])
    return text.strip()


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        temporary.write_text(text, encoding="utf-8")
        for attempt in range(8):
            try:
                os.replace(temporary, path)
                break
            except PermissionError:
                if attempt == 7:
                    raise
                time.sleep(0.05 * (attempt + 1))
    finally:
        temporary.unlink(missing_ok=True)


def atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        temporary.write_bytes(payload)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def retain_verified_official_pdf(path: Path, payload: bytes, expected_sha256: str) -> None:
    """Verify retained bytes on cache hits and after the existing atomic write.

    A mismatched existing archive is preserved and refused. The returned path
    is evidence for the supplied fetched body, never an unchecked cache alias.
    """
    if (type(payload) is not bytes or not 0 < len(payload) <= 64 * 1024 * 1024
            or not isinstance(expected_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None
            or hashlib.sha256(payload).hexdigest() != expected_sha256):
        raise ValueError("official PDF archive input binding invalid")
    path = Path(path)
    if path.name != expected_sha256 + ".pdf":
        raise ValueError("official PDF archive path binding invalid")
    if not path.exists():
        atomic_write_bytes(path, payload)
    with path.open("rb") as retained:
        current = retained.read(len(payload) + 1)
    if len(current) != len(payload) or hashlib.sha256(current).hexdigest() != expected_sha256:
        raise ValueError("official PDF archive hash mismatch")


def atomic_write_json(path: Path, payload: Any) -> None:
    atomic_write_text(
        path,
        json.dumps(payload, indent=2, sort_keys=True, default=str, allow_nan=False)
        + "\n",
    )


class CollectorCycleProgress:
    """Thread-safe source-cycle progress for liveness and stall detection."""

    def __init__(self, cycle_started_utc: dt.datetime) -> None:
        now = time.monotonic()
        self.cycle_started_utc = cycle_started_utc
        self.cycle_started_monotonic = now
        self.phase_started_monotonic = now
        self.last_progress_monotonic = now
        self.last_progress_utc = cycle_started_utc
        self.phase = "starting_cycle"
        self.progress_sequence = 0
        self.details: dict[str, Any] = {}
        self._lock = threading.Lock()

    def update(
        self, phase: str, details: Mapping[str, Any] | None = None
    ) -> None:
        now = time.monotonic()
        observed = utc_now()
        with self._lock:
            if phase != self.phase:
                self.phase_started_monotonic = now
            self.phase = str(phase)
            self.last_progress_monotonic = now
            self.last_progress_utc = observed
            self.progress_sequence += 1
            if isinstance(details, Mapping):
                self.details.update(dict(details))

    def snapshot(
        self,
        *,
        heartbeat_utc: dt.datetime | None = None,
        status: str = "running_cycle",
        cycle_in_progress: bool = True,
    ) -> dict[str, Any]:
        now_monotonic = time.monotonic()
        observed = heartbeat_utc or utc_now()
        with self._lock:
            return {
                "schema_version": SCHEMA_VERSION,
                "classification_version": CLASSIFICATION_VERSION,
                "collector_contract_id": COLLECTOR_CONTRACT_ID,
                "collector_cohort_id": COLLECTOR_COHORT_ID,
                "issuer_bound_policy_communication_contract_id": (
                    ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_CONTRACT_ID_V2
                ),
                "issuer_bound_policy_communication_cohort_id": (
                    ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_COHORT_ID_V2
                ),
                "issuer_bound_policy_communication_activated_utc": (
                    ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_ACTIVATED_UTC_V2
                ),
                "prior_issuer_bound_policy_communication_contract_id": (
                    ISSUER_BOUND_POLICY_COMMUNICATION_CONTRACT_ID
                ),
                "prior_issuer_bound_policy_communication_cohort_id": (
                    ISSUER_BOUND_POLICY_COMMUNICATION_COHORT_ID
                ),
                "generated_utc": iso_utc(observed),
                "heartbeat_utc": iso_utc(observed),
                "cycle_started_utc": iso_utc(self.cycle_started_utc),
                "status": status,
                "cycle_in_progress": cycle_in_progress,
                "phase": self.phase,
                "phase_age_sec": round(
                    max(0.0, now_monotonic - self.phase_started_monotonic), 3
                ),
                "progress_sequence": self.progress_sequence,
                "progress_age_sec": round(
                    max(0.0, now_monotonic - self.last_progress_monotonic), 3
                ),
                "last_progress_utc": iso_utc(self.last_progress_utc),
                "details": dict(self.details),
                "policy": {
                    "research_only": True,
                    "execution_eligible": False,
                    "openai_calls": 0,
                },
            }


def publish_collector_cycle_heartbeat(
    *, output_root: Path, progress: CollectorCycleProgress,
    status: str = "running_cycle", cycle_in_progress: bool = True,
) -> dict[str, Any]:
    """Publish liveness separately; never rewrite completed evidence."""

    payload = progress.snapshot(
        status=status, cycle_in_progress=cycle_in_progress
    )
    atomic_write_json(output_root / "collector_heartbeat_v1.json", payload)
    return payload


def run_collector_cycle_heartbeat(
    stop_event: threading.Event,
    *,
    output_root: Path,
    progress: CollectorCycleProgress,
    interval_sec: float,
) -> None:
    """Publish promptly and then periodically until the enclosing cycle ends."""

    while not stop_event.is_set():
        try:
            publish_collector_cycle_heartbeat(
                output_root=output_root,
                progress=progress,
            )
        except Exception:
            # A heartbeat is diagnostic.  It must never abort ingestion; the
            # supervisor will safely fail stale if publication itself breaks.
            pass
        if stop_event.wait(max(5.0, interval_sec)):
            break


def emit_collector_progress(
    progress_callback: Any,
    phase: str,
    **details: Any,
) -> None:
    if progress_callback is not None:
        progress_callback(phase, details)


def load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return default


def normalized_observation_time(
    local_now: dt.datetime,
    *,
    heartbeat_path: Path = DEFAULT_BROKER_CLOCK_HEARTBEAT,
    clock_integrity_path: Path = DEFAULT_CLOCK_INTEGRITY_STATE,
    maximum_state_age_sec: float = 90.0,
    maximum_future_state_skew_sec: float = 2.0,
    maximum_source_disagreement_sec: float = 2.0,
) -> tuple[dt.datetime, dict[str, Any]]:
    """Return a causally trusted collection time from a fresh clock audit.

    This only normalizes first-known and audit times.  It does not alter quote
    values, signal horizons, execution, or the Windows system clock.  The
    long-running executor heartbeat is retained as an API-compatible argument
    but is deliberately not trusted: its cached clock offset can outlive a
    Windows Time repair.  Only the independently refreshed integrity state may
    attest the host clock or supply a bounded correction.
    """

    local = (
        local_now.replace(tzinfo=UTC)
        if local_now.tzinfo is None
        else local_now.astimezone(UTC)
    )
    # ``heartbeat_path`` intentionally remains unused so callers do not need a
    # flag-day signature change.  It must never regain authority without a new
    # observation-time contract.
    _ = heartbeat_path
    external_state = load_json(clock_integrity_path, {})
    external_state = external_state if isinstance(external_state, dict) else {}
    integrity_generated = parse_datetime(external_state.get("generated_utc"))
    integrity_age_sec = (
        (local - integrity_generated).total_seconds()
        if integrity_generated is not None
        else math.inf
    )
    integrity_state_fresh = bool(
        integrity_generated is not None
        and integrity_age_sec >= -float(maximum_future_state_skew_sec)
        and integrity_age_sec <= float(maximum_state_age_sec)
        and str(external_state.get("status") or "") in {"ok", "mitigated"}
    )
    timestamp_normalization_trusted = (
        external_state.get("timestamp_normalization_trusted") is True
    )
    raw_lead = external_state.get("broker_clock_lead_sec")
    lead = (
        float(raw_lead)
        if isinstance(raw_lead, (int, float))
        and not isinstance(raw_lead, bool)
        and math.isfinite(float(raw_lead))
        else math.nan
    )
    raw_samples = external_state.get("broker_clock_sample_count")
    samples = (
        int(raw_samples)
        if isinstance(raw_samples, int) and not isinstance(raw_samples, bool)
        else 0
    )
    broker_alignment_attested = bool(
        external_state.get("source_fresh") is True
        and samples >= 32
        and math.isfinite(lead)
        and abs(lead) <= 2.0
    )
    external = (
        external_state.get("external_https_clock")
        if isinstance(external_state, dict) else {}
    )
    external = external if isinstance(external, dict) else {}
    raw_external_offset = external.get("offset_sec")
    external_offset = (
        float(raw_external_offset)
        if isinstance(raw_external_offset, (int, float))
        and not isinstance(raw_external_offset, bool)
        and math.isfinite(float(raw_external_offset))
        else math.nan
    )
    raw_round_trip_ms = external.get("round_trip_ms")
    round_trip_ms = (
        float(raw_round_trip_ms)
        if isinstance(raw_round_trip_ms, (int, float))
        and not isinstance(raw_round_trip_ms, bool)
        and math.isfinite(float(raw_round_trip_ms))
        else math.inf
    )
    raw_precision_sec = external.get("precision_sec")
    precision_sec = (
        float(raw_precision_sec)
        if isinstance(raw_precision_sec, (int, float))
        and not isinstance(raw_precision_sec, bool)
        and math.isfinite(float(raw_precision_sec))
        else math.inf
    )
    broker_source_available = bool(
        external_state.get("source_fresh") is True
        and samples >= 32
        and math.isfinite(lead)
        and abs(lead) <= 300.0
    )
    external_source_available = bool(
        external.get("status") == "ok"
        and math.isfinite(external_offset)
        and abs(external_offset) <= 300.0
        and round_trip_ms <= 2_000.0
        and precision_sec <= 1.0
    )
    source_disagreement_sec = (
        abs(lead - external_offset)
        if broker_source_available and external_source_available
        else None
    )
    clock_sources_consistent = bool(
        source_disagreement_sec is None
        or source_disagreement_sec <= float(maximum_source_disagreement_sec)
    )
    external_alignment_attested = bool(
        external_source_available
        and abs(external_offset) <= 2.0
        and clock_sources_consistent
    )
    host_clock_synchronized = bool(
        integrity_state_fresh
        and timestamp_normalization_trusted
        and external_state.get("host_clock_synchronized") is True
        and clock_sources_consistent
        and (broker_alignment_attested or external_alignment_attested)
    )
    external_usable = bool(
        integrity_state_fresh
        and timestamp_normalization_trusted
        and external_source_available
        and clock_sources_consistent
        and 2.0 < abs(external_offset) <= 300.0
    )
    broker_usable = bool(
        integrity_state_fresh
        and timestamp_normalization_trusted
        and broker_source_available
        and clock_sources_consistent
        and 2.0 < abs(lead) <= 300.0
    )
    if host_clock_synchronized:
        source = "clock_integrity_synchronized_host"
        status = "aligned"
        applied = 0.0
        trusted = True
    elif external_usable:
        source = "clock_integrity_oanda_https_date"
        status = "external_offset_applied"
        applied = external_offset
        trusted = True
    elif broker_usable:
        source = "clock_integrity_fresh_quote_stream"
        status = "broker_offset_applied"
        applied = lead
        trusted = True
    else:
        source = "unavailable"
        status = "clock_integrity_untrusted"
        applied = 0.0
        trusted = False
    corrected = local + dt.timedelta(seconds=applied)
    return corrected, {
        "contract_id": OBSERVATION_TIME_CONTRACT_ID,
        "source": source,
        "local_raw_utc": iso_utc(local),
        "normalized_utc": iso_utc(corrected),
        "status": status,
        "broker_clock_lead_sec": round(lead, 3) if math.isfinite(lead) else None,
        "sample_count": samples,
        "heartbeat_age_sec": None,
        "applied_offset_sec": round(applied, 3),
        "external_clock_offset_sec": (
            round(external_offset, 3) if math.isfinite(external_offset) else None
        ),
        "external_clock_age_sec": (
            round(integrity_age_sec, 3)
            if math.isfinite(integrity_age_sec)
            else None
        ),
        "external_clock_usable": external_usable,
        "broker_clock_usable": broker_usable,
        "integrity_generated_utc": (
            iso_utc(integrity_generated)
            if integrity_generated is not None else None
        ),
        "integrity_age_sec": (
            round(integrity_age_sec, 3)
            if math.isfinite(integrity_age_sec) else None
        ),
        "integrity_state_fresh": integrity_state_fresh,
        "clock_sources_consistent": clock_sources_consistent,
        "clock_source_disagreement_sec": (
            round(source_disagreement_sec, 3)
            if source_disagreement_sec is not None else None
        ),
        "timestamp_normalization_trusted": timestamp_normalization_trusted,
        "host_clock_synchronized": host_clock_synchronized,
        "cached_executor_offset_permitted": False,
        "trusted_for_prospective_evidence": trusted,
        "normalized": bool(applied),
    }


def prospective_clock_attestation(value: Any) -> bool:
    """Return true only for this exact, fail-closed observation-time contract.

    A caller-supplied mapping must not become prospective merely by setting a
    truthy flag.  The immutable contract identity is part of the attestation.
    Keeping this predicate shared also prevents consumers from drifting to a
    weaker Boolean-only interpretation.
    """

    return bool(
        isinstance(value, Mapping)
        and value.get("trusted_for_prospective_evidence") is True
        and clean_text(value.get("contract_id")) == OBSERVATION_TIME_CONTRACT_ID
    )


def prospective_collector_provenance(value: Any) -> bool:
    """Validate immutable first-seen provenance for a V39 article/topic row."""

    return bool(
        isinstance(value, Mapping)
        and value.get("observation_clock_trusted") is True
        and classification_provenance_valid(value, {
            "collector_contract_id": COLLECTOR_CONTRACT_ID,
            "collector_cohort_id": COLLECTOR_COHORT_ID,
            "observation_time_contract_id": OBSERVATION_TIME_CONTRACT_ID,
        })
        and source_version_provenance_valid(value, {
            "collector_contract_id": COLLECTOR_CONTRACT_ID,
            "collector_cohort_id": COLLECTOR_COHORT_ID,
            "observation_time_contract_id": OBSERVATION_TIME_CONTRACT_ID,
        })
        and clean_text(value.get("observation_time_contract_id"))
        == OBSERVATION_TIME_CONTRACT_ID
        and clean_text(value.get("collector_contract_id"))
        == COLLECTOR_CONTRACT_ID
        and clean_text(value.get("collector_cohort_id")) == COLLECTOR_COHORT_ID
    )


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def child_text(node: ET.Element, names: Iterable[str]) -> str:
    wanted = {name.lower() for name in names}
    for child in node.iter():
        if child is node or local_name(child.tag) not in wanted:
            continue
        value = clean_text("".join(child.itertext()))
        if value:
            return value
    return ""


def entry_link(node: ET.Element) -> str:
    for child in node.iter():
        if local_name(child.tag) != "link":
            continue
        href = str(child.attrib.get("href") or "").strip()
        relation = str(child.attrib.get("rel") or "alternate").lower()
        if href and relation in {"", "alternate"}:
            return href
        value = clean_text("".join(child.itertext()))
        if value:
            return value
    return ""


def trusted_host(url: Any, domains: Sequence[Any]) -> bool:
    host = urllib.parse.urlsplit(str(url or "")).netloc.lower().split(":", 1)[0]
    return bool(
        host
        and any(
            host == str(domain).lower().strip()
            or host.endswith("." + str(domain).lower().strip())
            for domain in domains
            if str(domain).strip()
        )
    )


def resolve_google_news_official_publisher_url(
    listing_url: Any,
    *,
    timeout_sec: float,
    maximum_bytes: int = 1_000_000,
) -> str:
    """Resolve one Google News wrapper without granting publisher trust.

    Google News RSS items currently expose a signed article identifier in the
    wrapper page rather than returning an HTTP redirect. This bounded helper
    asks Google's article-resolution endpoint for the original publisher URL.
    The caller must still validate the result against its configured official
    domains before fetching or classifying any content.
    """

    url = canonical_url(listing_url)
    parts = urllib.parse.urlsplit(url)
    if parts.scheme.lower() != "https" or parts.hostname != "news.google.com":
        raise ValueError(
            "Google News official resolution requires an HTTPS news.google.com URL"
        )
    match = re.fullmatch(r"/(?:rss/)?articles/([A-Za-z0-9_-]{20,500})", parts.path)
    if match is None:
        raise ValueError("Google News official resolution URL shape is unsupported")
    article_id = match.group(1)
    page_limit = max(100_000, min(1_000_000, int(maximum_bytes)))
    headers = {
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Encoding": "identity",
        "User-Agent": (
            "ForexResearchNewsCollector/1.0 "
            "(causal research; low-rate official publisher resolution)"
        ),
    }
    page_request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(
        page_request,
        timeout=timeout_sec,
        context=TLS_CONTEXT,
    ) as response:
        page = response.read(page_limit + 1)
    if len(page) > page_limit:
        raise ValueError("Google News official resolution page exceeds limit")
    page_text = page.decode("utf-8", errors="replace")
    tag_match = re.search(
        rf'''<div\b[^>]*\bdata-n-a-id=["']{re.escape(article_id)}["'][^>]*>''',
        page_text,
        flags=re.I | re.S,
    )
    if tag_match is None:
        raise ValueError("Google News official resolution attributes were not found")
    attributes = {
        name.lower(): html.unescape(value)
        for name, _quote, value in re.findall(
            r'''(data-n-a-(?:id|sg|ts))\s*=\s*(["'])(.*?)\2''',
            tag_match.group(0),
            flags=re.I | re.S,
        )
    }
    if attributes.get("data-n-a-id") != article_id:
        raise ValueError("Google News official resolution article ID mismatch")
    timestamp = attributes.get("data-n-a-ts") or ""
    signature = attributes.get("data-n-a-sg") or ""
    if not re.fullmatch(r"\d{1,20}", timestamp) or not re.fullmatch(
        r"[A-Za-z0-9_.~-]{10,500}", signature
    ):
        raise ValueError("Google News official resolution signature is invalid")
    request_payload = [
        "garturlreq",
        [
            [
                "X",
                "X",
                ["X", "X"],
                None,
                None,
                1,
                1,
                "US:en",
                None,
                1,
                None,
                None,
                None,
                None,
                None,
                0,
                1,
            ],
            "X",
            "X",
            1,
            [1, 1, 1],
            1,
            1,
            None,
            0,
            0,
            None,
            0,
        ],
        article_id,
        int(timestamp),
        signature,
    ]
    rpc = [
        "Fbv4je",
        json.dumps(request_payload, separators=(",", ":")),
    ]
    body = urllib.parse.urlencode(
        {"f.req": json.dumps([[rpc]], separators=(",", ":"))}
    ).encode("utf-8")
    batch_request = urllib.request.Request(
        GOOGLE_NEWS_BATCH_EXECUTE_URL,
        data=body,
        headers={
            **headers,
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        },
    )
    with urllib.request.urlopen(
        batch_request,
        timeout=timeout_sec,
        context=TLS_CONTEXT,
    ) as response:
        batch = response.read(500_001)
    if len(batch) > 500_000:
        raise ValueError("Google News official resolution response exceeds limit")
    response_text = batch.decode("utf-8", errors="replace")
    json_line = next(
        (
            line.strip()
            for line in response_text.splitlines()
            if line.lstrip().startswith("[[")
        ),
        "",
    )
    if not json_line:
        raise ValueError("Google News official resolution response is missing JSON")
    try:
        outer = json.loads(json_line)
        nested = json.loads(outer[0][2])
        resolved = canonical_url(nested[1])
    except (IndexError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("Google News official resolution response is invalid") from exc
    resolved_parts = urllib.parse.urlsplit(resolved)
    if (
        resolved_parts.scheme.lower() not in {"http", "https"}
        or not resolved_parts.hostname
    ):
        raise ValueError(
            "Google News official resolution returned an invalid publisher URL"
        )
    return resolved


def entry_source_url(node: ET.Element) -> str:
    for child in node.iter():
        if local_name(child.tag) == "source":
            return canonical_url(child.attrib.get("url") or child.attrib.get("href"))
    return ""


def parse_rss(payload: bytes, source: Mapping[str, Any]) -> list[dict[str, Any]]:
    root = ET.fromstring(payload)
    if local_name(root.tag) not in {"rss", "feed", "rdf"}:
        raise ValueError("expected RSS, Atom or RDF feed root")
    entries = [
        node
        for node in root.iter()
        if local_name(node.tag) in {"item", "entry"}
    ]
    output: list[dict[str, Any]] = []
    author_patterns = [
        re.compile(str(pattern), flags=re.I)
        for pattern in source.get("author_patterns") or ()
    ]
    exclude_author_patterns = [
        re.compile(str(pattern), flags=re.I)
        for pattern in source.get("exclude_author_patterns") or ()
    ]
    for node in entries:
        title = child_text(node, ("title",))
        if not title:
            continue
        author = child_text(node, ("author", "creator"))
        if author_patterns and not any(
            pattern.search(author) for pattern in author_patterns
        ):
            continue
        if any(pattern.search(author) for pattern in exclude_author_patterns):
            continue
        published = child_text(
            node,
            ("pubdate", "published", "updated", "date", "created"),
        )
        publisher_url = entry_source_url(node)
        publisher_verified = trusted_host(
            publisher_url,
            source.get("publisher_trusted_domains") or (),
        )
        if source.get("require_trusted_publisher") and not publisher_verified:
            continue
        date_only_publication = bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", published))
        parsed_published = (
            None
            if date_only_publication
            and source.get("date_only_publication_is_inferred")
            else parse_datetime(published)
        )
        summary = child_text(
            node,
            ("description", "summary", "content", "encoded"),
        )
        if trusted_host(source.get("url"), ("news.google.com",)):
            # Google News descriptions repeat the title and publisher label;
            # they are not article abstracts and can create false currency
            # mentions such as "Investing.com South Africa".
            summary = ""
        external_id = child_text(node, ("guid", "id"))
        if source.get("recurring_release_feed") and not external_id:
            # Some authoritative singleton feeds (notably BLS "Latest
            # Numbers") omit guid/id while reusing one URL and title forever.
            # Give the publisher container a stable series identity so the
            # material-content hash below can create a new immutable version
            # whenever the embedded figures change.  Without this fallback a
            # fresh release overwrites an old row and inherits its stale
            # first-seen clock.
            external_id = "recurring_container:" + hashlib.sha256(
                "|".join(
                    (
                        clean_text(source.get("source_id")),
                        canonical_url(
                            urllib.parse.urljoin(
                                str(source.get("url") or ""),
                                entry_link(node),
                            )
                        ),
                        clean_text(title),
                    )
                ).encode("utf-8")
            ).hexdigest()
        row = {
                "source_id": source.get("source_id"),
                "source_name": (
                    child_text(node, ("source",)) or author or source.get("name")
                ),
                "source_kind": str(source.get("kind") or "rss"),
                "source_quality": source.get("quality", 0.8),
                "source_verified": bool(source.get("verified")) or publisher_verified,
                "source_direct": configured_source_is_direct(source),
                "retrieval_via": str(source.get("retrieval_via") or "direct"),
                "source_role": source_role(source),
                "source_contract_id": clean_text(source.get("source_contract_id")),
                "source_cohort_id": clean_text(source.get("source_cohort_id")),
                "numeric_parser_activated_utc": clean_text(
                    source.get("numeric_parser_activated_utc")
                ),
                "numeric_extraction_contract_id": clean_text(
                    source.get("numeric_extraction_contract_id")
                ),
                "source_currencies": list(source.get("currencies") or []),
                "title": title,
                "summary": summary,
                "url": canonical_url(
                    urllib.parse.urljoin(
                        str(source.get("url") or ""),
                        entry_link(node),
                    )
                ),
                "publisher_url": publisher_url,
                "published_utc": iso_utc(parsed_published)
                if parsed_published
                else "",
                "external_id": external_id,
            }
        if source.get("recurring_release_feed") and external_id:
            # Recurring official releases commonly reuse one landing URL and
            # one headline every month.  Preserve the series lineage while
            # versioning each observation by its source-reported timestamp;
            # otherwise URL-level duplicate cleanup collapses a new release
            # into an older month and silently loses the fresh blurb.
            row.update(
                {
                    "structured_event": True,
                    "event_name": title,
                    "event_series_id": external_id,
                    "source_reported_update_utc": (
                        iso_utc(parsed_published) if parsed_published else ""
                    ),
                }
            )
            if source.get("mutable_content_versioned"):
                # Some official "latest numbers" feeds reuse both the item
                # URL and its nominal publication timestamp while replacing
                # the embedded values.  The content hash is therefore the
                # only publisher-observable material-version boundary.  Use
                # first_seen as the causal time for each version instead of
                # attributing new values to the stale container timestamp.
                material = f"{clean_text(title)}\n{clean_text(summary)}"
                row.update(
                    {
                        "material_content_sha256": hashlib.sha256(
                            material.encode("utf-8")
                        ).hexdigest(),
                        "content_versioned_at_collection": True,
                        "mutable_content_source": True,
                        "publisher_container_timestamp_utc": (
                            iso_utc(parsed_published) if parsed_published else ""
                        ),
                        "published_utc": "",
                        "published_time_inferred": True,
                    }
                )
            value_matches = list(
                re.finditer(
                    r"\b([A-Za-z]+\s+\d{4})(?:\s*\(r\))?\s*:\s*"
                    r"([+-]?\d+(?:\.\d+)?)\s*[^%]{0,8}%\s*Change\b",
                    summary,
                    flags=re.I,
                )
            )
            if value_matches:
                row["reference_period"] = clean_text(value_matches[0].group(1))
                row["actual"] = clean_text(value_matches[0].group(2))
                row["actual_value"] = optional_float(value_matches[0].group(2))
                row["unit"] = "percent change"
            if len(value_matches) >= 2:
                row["previous"] = clean_text(value_matches[1].group(2))
                row["previous_value"] = optional_float(value_matches[1].group(2))
        output.append(row)
    return output


def parse_bls_timeseries(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Normalize the newest official BLS series observation without look-ahead.

    The public API exposes values promptly but does not expose the release
    timestamp.  Therefore the observation remains timestamped at first local
    availability by ``classify_article`` and never backdated to the reference
    month.
    """

    parsed = json.loads(payload.decode("utf-8-sig"))
    status = clean_text(parsed.get("status")) if isinstance(parsed, Mapping) else ""
    if status != "REQUEST_SUCCEEDED":
        messages = parsed.get("message") if isinstance(parsed, Mapping) else []
        if isinstance(messages, list):
            message = "; ".join(clean_text(item) for item in messages if clean_text(item))
        else:
            message = clean_text(messages)
        raise ValueError(
            f"BLS API status {status or 'MISSING'}"
            + (f": {message}" if message else "")
        )
    results = parsed.get("Results") if isinstance(parsed, Mapping) else {}
    series_rows = results.get("series") if isinstance(results, Mapping) else []
    if not isinstance(series_rows, list) or not series_rows:
        return []
    series = series_rows[0] if isinstance(series_rows[0], Mapping) else {}
    observations = series.get("data") if isinstance(series, Mapping) else []
    if not isinstance(observations, list):
        return []
    observations = [
        row
        for row in observations
        if isinstance(row, Mapping)
        and re.fullmatch(r"M(?:0[1-9]|1[0-2])", clean_text(row.get("period")))
        and optional_float(row.get("value")) is not None
    ]
    observations.sort(
        key=lambda row: (
            int(safe_float(row.get("year"), 0)),
            int(clean_text(row.get("period"))[1:]),
        ),
        reverse=True,
    )
    if not observations:
        return []
    latest = observations[0]
    previous = observations[1] if len(observations) >= 2 else None
    scale = safe_float(source.get("value_scale"), 1.0)
    transform = clean_text(source.get("value_transform")).lower()
    if transform == "percent_change_1":
        prior_previous = observations[2] if len(observations) >= 3 else None
        latest_level = safe_float(latest.get("value"), 0.0)
        previous_level = (
            safe_float(previous.get("value"), 0.0)
            if previous is not None
            else 0.0
        )
        prior_previous_level = (
            safe_float(prior_previous.get("value"), 0.0)
            if prior_previous is not None
            else 0.0
        )
        actual_value = (
            ((latest_level / previous_level) - 1.0) * 100.0
            if previous_level
            else None
        )
        previous_value = (
            ((previous_level / prior_previous_level) - 1.0) * 100.0
            if prior_previous_level
            else None
        )
    else:
        actual_value = safe_float(latest.get("value"), 0.0) * scale
        previous_value = (
            safe_float(previous.get("value"), 0.0) * scale
            if previous is not None
            else None
        )
    if actual_value is None:
        return []
    series_id = clean_text(series.get("seriesID") or source.get("series_id"))
    event_name = clean_text(source.get("event_name")) or series_id
    reference_period = clean_text(
        f"{latest.get('periodName') or latest.get('period')} {latest.get('year')}"
    )
    native_release = parse_datetime(
        (source.get("release_utc_by_reference") or {}).get(reference_period)
    )
    unit = clean_text(source.get("unit")) or "index"
    precision = max(0, int(safe_float(source.get("value_precision"), 3)))
    actual_text = f"{actual_value:.{precision}f}"
    previous_text = (
        f"{previous_value:.{precision}f}"
        if previous_value is not None
        else ""
    )
    title = f"United States {event_name}: actual {actual_text} {unit}"
    if previous_text:
        title += f", previous {previous_text} {unit}"
    return [
        {
            "source_id": source.get("source_id"),
            "source_name": source.get("name"),
            "source_kind": "bls_timeseries",
            "source_quality": source.get("quality", 1.0),
            "source_verified": bool(source.get("verified", True)),
            "source_direct": configured_source_is_direct(source),
            "retrieval_via": str(source.get("retrieval_via") or "direct_api"),
            "source_role": source_role(source),
            "source_contract_id": clean_text(source.get("source_contract_id")),
            "source_cohort_id": clean_text(source.get("source_cohort_id")),
            "source_currencies": list(source.get("currencies") or ["USD"]),
            "title": title,
            "summary": (
                f"Official BLS public time-series observation for {event_name}; "
                f"reference period {reference_period}."
            ),
            "url": canonical_url(source.get("url")),
            "published_utc": iso_utc(native_release) if native_release else "",
            "published_time_inferred": native_release is None,
            "timing_precision": (
                "official_exact_schedule" if native_release else "first_seen_only"
            ),
            "external_id": (
                f"{series_id}:{latest.get('year')}:{latest.get('period')}"
            ),
            "event_series_id": series_id,
            "event_name": event_name,
            "event_country": "United States",
            "reference_period": reference_period,
            "unit": unit,
            "actual": actual_text,
            "actual_value": actual_value,
            "previous": previous_text,
            "previous_value": previous_value,
            "structured_event": True,
        }
    ]


def parse_bls_timeseries_batch(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Expand one official BLS batch response into versioned series events."""

    parsed = json.loads(payload.decode("utf-8-sig"))
    status = clean_text(parsed.get("status")) if isinstance(parsed, Mapping) else ""
    if status != "REQUEST_SUCCEEDED":
        messages = parsed.get("message") if isinstance(parsed, Mapping) else []
        if isinstance(messages, list):
            message = "; ".join(clean_text(item) for item in messages if clean_text(item))
        else:
            message = clean_text(messages)
        raise ValueError(
            f"BLS API status {status or 'MISSING'}"
            + (f": {message}" if message else "")
        )
    results = parsed.get("Results") if isinstance(parsed, Mapping) else {}
    response_series = results.get("series") if isinstance(results, Mapping) else []
    if not isinstance(response_series, list):
        return []
    response_by_id = {
        clean_text(row.get("seriesID")): row
        for row in response_series
        if isinstance(row, Mapping) and clean_text(row.get("seriesID"))
    }
    configured_series = source.get("series")
    if not isinstance(configured_series, list):
        configured_series = []
    output: list[dict[str, Any]] = []
    for configured in configured_series:
        if not isinstance(configured, Mapping):
            continue
        series_id = clean_text(configured.get("series_id"))
        response_row = response_by_id.get(series_id)
        if response_row is None:
            continue
        child_source = dict(source)
        child_source.update(configured)
        child_source["kind"] = "bls_timeseries"
        # Series identity already lives in event_series_id/external_id. Keep
        # the physical configured BLS source ID so the USD source contract is
        # available during retained-corpus reclassification.
        child_source["source_id"] = clean_text(source.get("source_id"))
        child_source["name"] = (
            clean_text(configured.get("name"))
            or f"{clean_text(source.get('name'))} {series_id}"
        )
        child_payload = json.dumps(
            {
                "status": "REQUEST_SUCCEEDED",
                "Results": {"series": [response_row]},
            }
        ).encode("utf-8")
        output.extend(parse_bls_timeseries(child_payload, child_source))
    return output


def provider_parse_retry_after(
    source_kind: str,
    error: Exception | str,
    now: dt.datetime,
) -> str:
    """Translate provider-in-body quota failures into an explicit backoff.

    BLS returns HTTP 200 when the registration key's daily allowance is
    exhausted. Repeating the same request cannot restore coverage and only
    consumes collector time. Keep the source degraded, but wait until five
    minutes after the next New York calendar day before trying again.
    """
    message = str(error).lower()
    if source_kind != "bls_timeseries_batch" or "daily threshold" not in message:
        return ""
    local = now.astimezone(ZoneInfo("America/New_York"))
    next_date = local.date() + dt.timedelta(days=1)
    retry_local = dt.datetime.combine(
        next_date,
        dt.time(hour=0, minute=5),
        tzinfo=ZoneInfo("America/New_York"),
    )
    return iso_utc(retry_local.astimezone(UTC))


def provider_http_retry_after(
    source: Mapping[str, Any],
    status: int,
    retry_after_seconds: float,
    now: dt.datetime,
) -> str:
    """Return an explicit provider-aware HTTP backoff deadline.

    Respect Retry-After when supplied. GDELT commonly returns 429 without the
    header; an hourly retry loop only prolongs a shared-IP rate limit and adds
    no causal value, so use the source's bounded low-priority cooldown.
    """

    if retry_after_seconds > 0:
        return iso_utc(now + dt.timedelta(seconds=retry_after_seconds))
    if str(source.get("kind") or "").lower() == "gdelt" and status == 429:
        cooldown = max(
            3600.0,
            safe_float(source.get("rate_limit_retry_sec"), 21600.0),
        )
        return iso_utc(now + dt.timedelta(seconds=cooldown))
    return ""


def parse_bea_release_blurb(payload: bytes) -> str:
    """Extract the three headline observations from a BEA news-release page."""

    page = payload.decode("utf-8", errors="replace")
    page = re.sub(r"<script\b[\s\S]*?</script>", " ", page, flags=re.I)
    page = re.sub(r"<style\b[\s\S]*?</style>", " ", page, flags=re.I)
    text = clean_text(page)
    deficit = re.search(
        r"(?:announced today that\s+)?(the\s+)?goods and services deficit was\s+"
        r"\$[0-9.,]+\s+billion\s+in\s+[A-Za-z]+,\s+"
        r"(?:up|down)\s+\$[0-9.,]+\s+billion\s+from\s+"
        r"\$[0-9.,]+\s+billion\s+in\s+[A-Za-z]+,?\s+revised\.",
        text,
        flags=re.I,
    )
    exports = re.search(
        r"(?:[A-Za-z]+\s+)?exports were\s+\$[0-9.,]+\s+billion,\s+"
        r"\$[0-9.,]+\s+billion\s+(?:more|less)\s+than\s+"
        r"[A-Za-z]+\s+exports\.",
        text,
        flags=re.I,
    )
    imports = re.search(
        r"(?:[A-Za-z]+\s+)?imports were\s+\$[0-9.,]+\s+billion,\s+"
        r"\$[0-9.,]+\s+billion\s+(?:more|less)\s+than\s+"
        r"[A-Za-z]+\s+imports\.",
        text,
        flags=re.I,
    )
    if deficit is None or exports is None or imports is None:
        return ""
    blurb = " ".join(
        clean_text(match.group(0)) for match in (deficit, exports, imports)
    )
    blurb = re.sub(r"^announced today that\s+", "", blurb, flags=re.I)
    return blurb[0].upper() + blurb[1:] if blurb else ""


class _OfficialDocumentTextParser(HTMLParser):
    """Extract human-visible document text without navigation or scripts."""

    _SKIP = {"script", "style", "noscript", "svg", "nav", "header", "footer"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._skip_depth = 0
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in self._SKIP:
            self._skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in self._SKIP and self._skip_depth:
            self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._skip_depth and data.strip():
            self._parts.append(data)

    def text(self) -> str:
        return re.sub(r"\s+", " ", " ".join(self._parts)).strip()


class _OfficialDocumentLinkParser(HTMLParser):
    """Collect anchor targets and labels from one trusted official page."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._href = ""
        self._label_parts: list[str] = []
        self.links: list[tuple[str, str]] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag.lower() != "a" or self._href:
            return
        values = {str(key).lower(): value for key, value in attrs}
        self._href = html.unescape(str(values.get("href") or "")).strip()
        self._label_parts = []

    def handle_data(self, data: str) -> None:
        if self._href and data.strip():
            self._label_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or not self._href:
            return
        self.links.append(
            (
                self._href,
                re.sub(r"\s+", " ", " ".join(self._label_parts)).strip(),
            )
        )
        self._href = ""
        self._label_parts = []


def same_authority_pdf_attachment_links(
    payload: bytes,
    *,
    page_url: str,
    trusted_domains: Sequence[Any],
    maximum: int = 1,
    label_patterns: Sequence[Any] = (),
) -> list[str]:
    """Return bounded PDF links that remain on the landing page's exact host.

    ``trusted_host`` deliberately accepts subdomains for ordinary official
    redirects. Attachments use the narrower exact-host rule: an official HTML
    page cannot turn a third-party or sibling host into trusted document
    evidence merely by linking to it.
    """

    canonical_page = canonical_url(page_url)
    page_parts = urllib.parse.urlsplit(canonical_page)
    page_host = str(page_parts.hostname or "").casefold()
    if (
        page_parts.scheme.lower() not in {"http", "https"}
        or not page_host
        or not trusted_host(canonical_page, trusted_domains)
    ):
        return []
    maximum = max(1, min(3, int(maximum)))
    patterns = [
        re.compile(str(pattern), flags=re.I)
        for pattern in label_patterns
        if str(pattern).strip()
    ]
    parser = _OfficialDocumentLinkParser()
    parser.feed(payload.decode("utf-8", errors="replace"))
    candidates: list[tuple[int, str]] = []
    for href, label in parser.links:
        resolved = canonical_url(urllib.parse.urljoin(canonical_page, href))
        parts = urllib.parse.urlsplit(resolved)
        if (
            parts.scheme.lower() not in {"http", "https"}
            or str(parts.hostname or "").casefold() != page_host
            or not parts.path.lower().endswith(".pdf")
            or not trusted_host(resolved, trusted_domains)
        ):
            continue
        haystack = f"{label} {parts.path}"
        if patterns and not any(pattern.search(haystack) for pattern in patterns):
            continue
        priority = 0 if re.search(r"\bfull\s+text\b", label, re.I) else 1
        candidates.append((priority, resolved))
    result: list[str] = []
    for _priority, value in sorted(candidates, key=lambda item: (item[0], item[1])):
        if value not in result:
            result.append(value)
        if len(result) >= maximum:
            break
    return result


def _attachment_parent_state_key(url: str) -> str:
    """Compact source-state marker for a completed attachment discovery."""

    digest = hashlib.sha256(canonical_url(url).encode("utf-8")).hexdigest()[:32]
    return f"__official_pdf_attachment_parent_v1__:{digest}"


def extract_official_document_text(
    payload: bytes,
    *,
    content_type: str,
    url: str,
    maximum_characters: int = 60_000,
    maximum_pages: int = 80,
) -> tuple[str, str]:
    """Return bounded text plus its parser kind from an official HTML/PDF."""

    maximum_characters = max(1_000, min(200_000, int(maximum_characters)))
    maximum_pages = max(1, min(300, int(maximum_pages)))
    media_type = str(content_type or "").split(";", 1)[0].strip().lower()
    is_pdf = media_type == "application/pdf" or urllib.parse.urlsplit(url).path.lower().endswith(".pdf")
    if is_pdf:
        if pypdf is None:
            raise ValueError("pypdf is required for official PDF extraction")
        reader = pypdf.PdfReader(io.BytesIO(payload), strict=False)
        text = " ".join(
            str(page.extract_text() or "") for page in reader.pages[:maximum_pages]
        )
        parser_kind = "official_pdf_text"
    elif media_type in {"text/html", "application/xhtml+xml", "", "text/plain"}:
        if media_type == "text/plain":
            text = payload.decode("utf-8", errors="replace")
        else:
            parser = _OfficialDocumentTextParser()
            parser.feed(payload.decode("utf-8", errors="replace"))
            text = parser.text()
        parser_kind = "official_html_text"
    else:
        raise ValueError(f"unsupported official detail content type: {media_type}")
    text = re.sub(r"\s+", " ", html.unescape(text)).strip()
    if len(text) < 80:
        raise ValueError("official detail text is empty or too short")
    return text[:maximum_characters], parser_kind


def parse_stats_nz_release_detail(payload: bytes) -> str:
    """Extract release prose from Stats NZ's embedded page-view JSON."""

    page = payload.decode("utf-8", errors="replace")
    match = re.search(
        r'''<div\b[^>]*\bid=["']pageViewData["'][^>]*\bdata-value=["']([^"']+)["']''',
        page,
        flags=re.I | re.S,
    )
    if match is None:
        raise ValueError("Stats NZ pageViewData was not found")
    try:
        parsed = json.loads(html.unescape(match.group(1)))
    except json.JSONDecodeError as exc:
        raise ValueError("Stats NZ pageViewData is invalid JSON") from exc
    fragments: list[str] = []
    if isinstance(parsed, Mapping):
        for key in ("MetaDescription", "FeaturedText"):
            value = parsed.get(key)
            if isinstance(value, str) and value.strip():
                fragments.append(value)
        for block in parsed.get("PageBlocks") or ():
            if not isinstance(block, Mapping):
                continue
            value = block.get("Content")
            if isinstance(value, str) and value.strip():
                fragments.append(value)
    parser = _OfficialDocumentTextParser()
    parser.feed(" ".join(fragments))
    text = re.sub(r"\s+", " ", html.unescape(parser.text())).strip()
    if len(text) < 80:
        raise ValueError("Stats NZ release detail is empty or too short")
    return text


def ons_release_bulletin_links(
    listing_html: str,
    *,
    base_url: str,
    release_title: str,
    maximum: int = 8,
) -> list[str]:
    """Return deterministic ONS bulletin links for one release package.

    ONS umbrella releases can contain separate payroll, employment, vacancy,
    and earnings bulletins.  Selecting the first link silently turns a labour
    package into whichever child happens to appear first.  Labour releases are
    therefore collected as one bounded package; other releases retain the
    historical single-bulletin behavior.
    """

    links: list[str] = []
    for match in re.finditer(
        r'''href=["']([^"']*/bulletins/[^"'#?]+(?:[?#][^"']*)?)["']''',
        listing_html,
        flags=re.I,
    ):
        value = canonical_url(
            urllib.parse.urljoin(base_url, html.unescape(match.group(1)))
        )
        if value and value not in links:
            links.append(value)
    if not links:
        return []
    if not re.search(r"\b(?:uk\s+)?labou?r\s+market\b", release_title, re.I):
        return links[:1]
    priority_slugs = (
        "labourmarketoverview",
        "earningsandemploymentfrompayasyouearn",
        "employmentintheuk",
        "vacanciesandjobsintheuk",
        "averageweeklyearningsingreatbritain",
    )
    ranked = sorted(
        links,
        key=lambda value: (
            next(
                (
                    index
                    for index, slug in enumerate(priority_slugs)
                    if slug in value.lower().replace("-", "")
                ),
                len(priority_slugs),
            ),
            value,
        ),
    )
    return ranked[: max(1, int(maximum))]


ABS_OFFICIAL_PAGE_RELEASE_CLOCK_CONTRACT_ID = (
    "abs_official_page_release_date_time_v1_20260827"
)


def parse_abs_official_page_release_clock(value: Any) -> dt.datetime | None:
    """Parse the exact clock printed on an official ABS release page.

    ABS listing pages do not carry a machine-readable publication timestamp,
    but their first-party detail pages print an explicit ``Release date and
    time`` value. Accept only that labelled field and the two timezone labels
    ABS uses. A general date elsewhere in the document must never be mistaken
    for the release clock.
    """

    match = re.search(
        r"\bRelease\s+date\s+and\s+time\s+"
        r"(?P<day>\d{1,2})/(?P<month>\d{1,2})/(?P<year>20\d{2})\s+"
        r"(?P<hour>\d{1,2})[:.](?P<minute>\d{2})\s*"
        r"(?P<meridiem>am|pm)\s+(?P<zone>AEST|AEDT)\b",
        clean_text(value),
        flags=re.I,
    )
    if match is None:
        return None
    hour = int(match.group("hour"))
    if not 1 <= hour <= 12:
        return None
    if hour == 12:
        hour = 0
    if match.group("meridiem").lower() == "pm":
        hour += 12
    zone_offset_hours = 10 if match.group("zone").upper() == "AEST" else 11
    try:
        local_release = dt.datetime(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
            hour,
            int(match.group("minute")),
            tzinfo=dt.timezone(dt.timedelta(hours=zone_offset_hours)),
        )
    except ValueError:
        return None
    return local_release.astimezone(UTC)


def mutable_detail_versioning_active(
    source: Mapping[str, Any],
    *,
    now: dt.datetime,
) -> bool:
    """Return whether an explicitly governed mutable-page contract is active."""

    if source.get("detail_mutable_placeholder_versioning") is not True:
        return False
    contract_id = clean_text(source.get("detail_mutable_placeholder_contract_id"))
    activation = parse_datetime(source.get("detail_mutable_placeholder_activated_utc"))
    patterns = source.get("detail_placeholder_text_patterns")
    return bool(
        contract_id
        and activation is not None
        and activation <= now
        and isinstance(patterns, list)
        and patterns
    )


def mutable_detail_refetch_pending(
    source: Mapping[str, Any],
    source_state: Mapping[str, Any],
    url: str,
    *,
    now: dt.datetime,
) -> bool:
    """Bound re-fetches to an observed placeholder or one migration baseline.

    This is deliberately opt-in. Most official documents are immutable and
    retain the original one-fetch rule. A few central banks pre-publish the
    final decision URL as a placeholder and replace that same page at release
    time; those pages need a short, governed content-version window.
    """

    if not url or not mutable_detail_versioning_active(source, now=now):
        return False
    first_seen_by_url = source_state.get("detail_first_seen_utc_by_url")
    if not isinstance(first_seen_by_url, Mapping) or url not in first_seen_by_url:
        return False
    first_seen = parse_datetime(first_seen_by_url.get(url))
    if first_seen is None:
        return False
    age_minutes = (now - first_seen).total_seconds() / 60.0
    maximum_age = max(
        1.0,
        safe_float(
            source.get("detail_mutable_refetch_max_age_minutes"),
            240.0,
        ),
    )
    if not (-1.0 <= age_minutes <= maximum_age):
        return False
    hashes = source_state.get("detail_content_sha256_by_url")
    placeholders = source_state.get("detail_content_is_placeholder_by_url")
    baseline_missing = not isinstance(hashes, Mapping) or not clean_text(
        hashes.get(url)
    )
    was_placeholder = bool(
        isinstance(placeholders, Mapping) and placeholders.get(url) is True
    )
    return baseline_missing or was_placeholder


def mutable_detail_placeholder_matches(
    source: Mapping[str, Any],
    *,
    title: Any,
    document_text: Any,
) -> bool:
    """Match only source-configured placeholder language."""

    text = f"{clean_text(title)}\n{clean_text(document_text)}"
    for configured in source.get("detail_placeholder_text_patterns") or ():
        try:
            if re.search(str(configured), text, flags=re.I):
                return True
        except re.error:
            # A malformed optional pattern cannot broaden collection or make an
            # ordinary immutable page mutable. Source-contract tests reject it.
            continue
    return False


def enrich_recent_official_release_details(
    articles: list[dict[str, Any]],
    source: Mapping[str, Any],
    source_state: Mapping[str, Any],
    *,
    timeout_sec: float,
    maximum_bytes: int,
    now: dt.datetime,
) -> tuple[int, dict[str, str], str]:
    """Fetch a recent primary release page when its RSS item has no blurb.

    Detail enrichment is opt-in and restricted to the configured first-party
    domains.  A per-URL first-availability map prevents later polls from
    rewriting when the richer contents became locally knowable.
    """

    enrichment_kind = str(source.get("detail_enrichment") or "")
    if enrichment_kind not in {
        "bea_release_blurb",
        "official_document_text",
        "ons_release_bulletin",
        "stats_nz_release_detail",
    }:
        return 0, {}, ""
    trusted_domains = source.get("trusted_domains") or ()
    follow_pdf_attachments = bool(
        source.get("detail_follow_same_authority_pdf_attachments") is True
        and enrichment_kind == "official_document_text"
    )
    maximum_age_minutes = max(
        1.0,
        safe_float(source.get("detail_max_age_minutes"), 20.0),
    )
    prior_times = source_state.get("detail_first_seen_utc_by_url")
    first_seen_by_url = (
        {str(key): str(value) for key, value in prior_times.items()}
        if isinstance(prior_times, Mapping)
        else {}
    )
    prior_content_hashes_raw = source_state.get("detail_content_sha256_by_url")
    prior_content_hashes = (
        {str(key): clean_text(value) for key, value in prior_content_hashes_raw.items()}
        if isinstance(prior_content_hashes_raw, Mapping)
        else {}
    )
    prior_placeholders_raw = source_state.get(
        "detail_content_is_placeholder_by_url"
    )
    prior_placeholders = (
        {str(key): value is True for key, value in prior_placeholders_raw.items()}
        if isinstance(prior_placeholders_raw, Mapping)
        else {}
    )
    prior_version_counts_raw = source_state.get("detail_content_version_count_by_url")
    prior_version_counts = (
        {
            str(key): max(0, int(safe_float(value, 0)))
            for key, value in prior_version_counts_raw.items()
        }
        if isinstance(prior_version_counts_raw, Mapping)
        else {}
    )
    mutable_versioning = mutable_detail_versioning_active(source, now=now)
    enriched = 0
    attempted_details = 0
    error = ""
    detail_limit = min(
        maximum_bytes,
        max(100_000, int(safe_float(source.get("detail_maximum_bytes"), 8_000_000))),
    )
    context_patterns = [
        re.compile(str(pattern), flags=re.I)
        for pattern in source.get("detail_context_url_patterns") or ()
    ]
    # Historical official pages may be archived for current-view research by
    # any of the opt-in detail parsers, including the ONS two-hop bulletin
    # resolver.  The archived body is always marked bootstrap/research-only;
    # this broadens document retrieval, never prospective eligibility.
    allow_context_archive = bool(
        source.get("detail_context_archive_only") and context_patterns
    )
    for article in articles:
        published = parse_datetime(article.get("published_utc"))
        age_minutes = (
            (now - published).total_seconds() / 60.0
            if published is not None
            else 0.0
            if article.get("source_listing_new_item")
            else math.inf
        )
        url = canonical_url(article.get("url"))
        attachment_parent_state_key = _attachment_parent_state_key(url) if url else ""
        # A source contract may gain the attachment parser after its landing
        # page was already enriched. Permit exactly one bounded revisit without
        # moving the landing page's original detail-availability timestamp.
        attachment_upgrade_pending = bool(
            follow_pdf_attachments
            and url in first_seen_by_url
            and attachment_parent_state_key not in first_seen_by_url
        )
        mutable_refetch_pending = mutable_detail_refetch_pending(
            source,
            source_state,
            url,
            now=now,
        )
        google_official_redirect = bool(
            str(source.get("retrieval_via") or "")
            == "google_news_official_site_search"
            and trusted_host(url, ("news.google.com",))
        )
        context_archive_only = bool(
            allow_context_archive
            and url
            and any(pattern.search(url) for pattern in context_patterns)
            and (
                bool(article.get("source_listing_bootstrap"))
                or not (-1.0 <= age_minutes <= maximum_age_minutes)
            )
        )
        # A newly introduced detail parser may first encounter a still-current
        # listing URL that the headline-only collector already knew. An
        # explicit source opt-in may fetch that body now, but availability is
        # stamped at this retrieval and the listing remains bootstrap.
        enrich_recent_existing_item = bool(
            source.get("detail_enrich_recent_existing_items") is True
            and article.get("source_listing_bootstrap")
            and -1.0 <= age_minutes <= maximum_age_minutes
        )
        if (
            url in first_seen_by_url
            and not attachment_upgrade_pending
            and not mutable_refetch_pending
        ):
            if context_archive_only:
                # Re-emit the quarantine metadata on later listing polls even
                # though the official body itself must not be fetched again.
                # This makes archive provenance survive parser/classifier
                # refreshes without moving the original availability clock.
                article["source_listing_bootstrap"] = True
                article["detail_context_archive_only"] = True
                article["detail_enrichment_research_only"] = True
                article["detail_available_utc"] = first_seen_by_url[url]
            continue
        if (
            not url
            or (
                not trusted_host(url, trusted_domains)
                and not google_official_redirect
            )
            or (
                bool(article.get("source_listing_bootstrap"))
                and not context_archive_only
                and not enrich_recent_existing_item
                and not attachment_upgrade_pending
                and not mutable_refetch_pending
            )
            or (
                not (-1.0 <= age_minutes <= maximum_age_minutes)
                and not context_archive_only
                and not attachment_upgrade_pending
                and not mutable_refetch_pending
            )
        ):
            continue
        attempted_details += 1
        if attempted_details > max(
            1,
            int(
                safe_float(
                    source.get("detail_context_max_items_per_cycle")
                    if context_archive_only
                    else source.get("detail_max_items_per_cycle"),
                    4,
                )
            ),
        ):
            break
        attachment_records: list[dict[str, Any]] = []
        attachment_discovery_state = "not_enabled"
        try:
            detail_request_url = url
            publisher_resolution_method = ""
            publisher_resolution_known_utc = ""
            if google_official_redirect:
                resolution_contract = clean_text(
                    source.get("google_news_publisher_resolution_contract_id")
                )
                if (
                    resolution_contract
                    != GOOGLE_NEWS_OFFICIAL_PUBLISHER_RESOLUTION_CONTRACT_ID
                ):
                    raise ValueError(
                        "Google News official publisher resolution contract is invalid"
                    )
                detail_request_url = resolve_google_news_official_publisher_url(
                    url,
                    timeout_sec=timeout_sec,
                    maximum_bytes=detail_limit,
                )
                if not trusted_host(detail_request_url, trusted_domains):
                    raise ValueError(
                        "Google News resolved outside configured official domains"
                    )
                publisher_resolution_method = (
                    GOOGLE_NEWS_OFFICIAL_PUBLISHER_RESOLUTION_CONTRACT_ID
                )
                publisher_resolution_known_utc = iso_utc(now)
            request = urllib.request.Request(
                detail_request_url,
                headers={
                    "Accept": (
                        "application/pdf,text/html,application/xhtml+xml,text/plain"
                    ),
                    "Accept-Encoding": "identity",
                    "User-Agent": (
                        "ForexResearchNewsCollector/1.0 "
                        "(causal research; low-rate primary detail fetch)"
                    ),
                },
            )
            with urllib.request.urlopen(
                request,
                timeout=timeout_sec,
                context=TLS_CONTEXT,
            ) as response:
                final_url = canonical_url(response.geturl())
                content_type = str(response.headers.get("Content-Type") or "")
                if not trusted_host(final_url, trusted_domains):
                    raise ValueError("official detail redirected outside trusted domain")
                detail = response.read(detail_limit + 1)
            if len(detail) > detail_limit:
                raise ValueError("official detail payload exceeds limit")
            if enrichment_kind == "ons_release_bulletin":
                listing_html = detail.decode("utf-8", errors="replace")
                bulletin_urls = ons_release_bulletin_links(
                    listing_html,
                    base_url=final_url,
                    release_title=clean_text(article.get("title")),
                    maximum=int(
                        safe_float(source.get("detail_bundle_max_items"), 8)
                    ),
                )
                if not bulletin_urls:
                    raise ValueError("official ONS bulletin link not found")
                bulletin_payloads: list[bytes] = []
                bulletin_texts: list[str] = []
                fetched_urls: list[str] = []
                per_document_characters = max(
                    10_000,
                    int(
                        safe_float(
                            source.get("detail_maximum_characters"), 60_000
                        )
                    )
                    // max(1, len(bulletin_urls)),
                )
                for bulletin_url in bulletin_urls:
                    if not trusted_host(bulletin_url, trusted_domains):
                        raise ValueError("official ONS bulletin left trusted domain")
                    bulletin_request = urllib.request.Request(
                        bulletin_url,
                        headers={
                            "Accept": "text/html,application/xhtml+xml,text/plain",
                            "Accept-Encoding": "identity",
                            "User-Agent": (
                                "ForexResearchNewsCollector/1.0 "
                                "(causal research; low-rate primary detail fetch)"
                            ),
                        },
                    )
                    with urllib.request.urlopen(
                        bulletin_request,
                        timeout=timeout_sec,
                        context=TLS_CONTEXT,
                    ) as bulletin_response:
                        resolved_url = canonical_url(bulletin_response.geturl())
                        content_type = str(
                            bulletin_response.headers.get("Content-Type") or ""
                        )
                        if not trusted_host(resolved_url, trusted_domains):
                            raise ValueError(
                                "official ONS bulletin redirected outside trusted domain"
                            )
                        payload = bulletin_response.read(detail_limit + 1)
                    if len(payload) > detail_limit:
                        raise ValueError("official ONS bulletin exceeds limit")
                    text, _document_parser = extract_official_document_text(
                        payload,
                        content_type=content_type,
                        url=resolved_url,
                        maximum_characters=per_document_characters,
                        maximum_pages=int(
                            safe_float(source.get("detail_maximum_pages"), 80)
                        ),
                    )
                    bulletin_payloads.append(payload)
                    bulletin_texts.append(f"[{resolved_url}] {text}")
                    fetched_urls.append(resolved_url)
                detail = b"\n\n".join(bulletin_payloads)
                document_text = "\n\n".join(bulletin_texts)
                final_url = fetched_urls[0]
                parser_kind = (
                    "ons_release_bundle"
                    if len(fetched_urls) > 1
                    else "ons_release_bulletin"
                )
                article["detail_bundle_urls"] = fetched_urls
                article["detail_bundle_count"] = len(fetched_urls)
            elif enrichment_kind == "bea_release_blurb":
                document_text = parse_bea_release_blurb(detail)
                parser_kind = "bea_release_blurb"
                if not document_text:
                    raise ValueError("official detail blurb not found")
            elif enrichment_kind == "stats_nz_release_detail":
                document_text = parse_stats_nz_release_detail(detail)
                parser_kind = "stats_nz_release_detail"
            else:
                document_text, parser_kind = extract_official_document_text(
                    detail,
                    content_type=content_type,
                    url=final_url,
                    maximum_characters=int(
                        safe_float(source.get("detail_maximum_characters"), 60_000)
                    ),
                    maximum_pages=int(
                        safe_float(source.get("detail_maximum_pages"), 80)
                    ),
                )
            if follow_pdf_attachments and parser_kind == "official_html_text":
                attachment_urls = same_authority_pdf_attachment_links(
                    detail,
                    page_url=final_url,
                    trusted_domains=trusted_domains,
                    maximum=int(
                        safe_float(
                            source.get("detail_attachment_max_items_per_page"),
                            1,
                        )
                    ),
                    label_patterns=(
                        source.get("detail_attachment_link_text_patterns") or ()
                    ),
                )
                attachment_discovery_state = (
                    "same_host_pdf_selected" if attachment_urls
                    else "no_matching_same_host_pdf"
                )
                attachment_total_limit = min(
                    maximum_bytes,
                    max(
                        100_000,
                        int(
                            safe_float(
                                source.get("detail_attachment_maximum_bytes"),
                                2_000_000,
                            )
                        ),
                    ),
                )
                attachment_character_limit = max(
                    1_000,
                    min(
                        120_000,
                        int(
                            safe_float(
                                source.get(
                                    "detail_attachment_maximum_characters"
                                ),
                                60_000,
                            )
                        ),
                    ),
                )
                total_attachment_bytes = 0
                total_attachment_characters = 0
                landing_host = str(
                    urllib.parse.urlsplit(final_url).hostname or ""
                ).casefold()
                attachment_texts: list[str] = []
                for attachment_url in attachment_urls:
                    remaining_bytes = (
                        attachment_total_limit - total_attachment_bytes
                    )
                    remaining_characters = (
                        attachment_character_limit - total_attachment_characters
                    )
                    if remaining_bytes <= 0 or remaining_characters < 1_000:
                        break
                    attachment_request = urllib.request.Request(
                        attachment_url,
                        headers={
                            "Accept": "application/pdf",
                            "Accept-Encoding": "identity",
                            "User-Agent": (
                                "ForexResearchNewsCollector/1.0 "
                                "(causal research; low-rate primary attachment fetch)"
                            ),
                        },
                    )
                    with urllib.request.urlopen(
                        attachment_request,
                        timeout=timeout_sec,
                        context=TLS_CONTEXT,
                    ) as attachment_response:
                        resolved_attachment_url = canonical_url(
                            attachment_response.geturl()
                        )
                        attachment_content_type = str(
                            attachment_response.headers.get("Content-Type") or ""
                        )
                        resolved_attachment_host = str(
                            urllib.parse.urlsplit(
                                resolved_attachment_url
                            ).hostname
                            or ""
                        ).casefold()
                        if (
                            not landing_host
                            or resolved_attachment_host != landing_host
                            or not trusted_host(
                                resolved_attachment_url, trusted_domains
                            )
                        ):
                            raise ValueError(
                                "official PDF attachment redirected outside "
                                "landing-page host"
                            )
                        attachment_payload = attachment_response.read(
                            remaining_bytes + 1
                        )
                    if len(attachment_payload) > remaining_bytes:
                        raise ValueError(
                            "official PDF attachments exceed aggregate byte limit"
                        )
                    if not attachment_payload.lstrip().startswith(b"%PDF-"):
                        raise ValueError(
                            "official PDF attachment returned a non-PDF payload"
                        )
                    attachment_text, attachment_parser_kind = (
                        extract_official_document_text(
                            attachment_payload,
                            content_type=attachment_content_type,
                            url=resolved_attachment_url,
                            maximum_characters=remaining_characters,
                            maximum_pages=int(
                                safe_float(
                                    source.get("detail_attachment_maximum_pages"),
                                    source.get("detail_maximum_pages", 80),
                                )
                            ),
                        )
                    )
                    attachment_quality_issue = official_document_quality_issue(
                        attachment_text
                    )
                    if attachment_quality_issue:
                        raise ValueError(
                            "official PDF attachment quality rejected:"
                            f"{attachment_quality_issue}"
                        )
                    attachment_sha256 = hashlib.sha256(
                        attachment_payload
                    ).hexdigest()
                    # Stage the clock in the article's attachment record.
                    # A later attachment or archive failure must not advance
                    # returned availability state for this rejected article.
                    attachment_available = first_seen_by_url.get(
                        resolved_attachment_url, iso_utc(now)
                    )
                    attachment_archive_path = ""
                    if bool(source.get("archive_official_pdfs", True)):
                        safe_source_id = re.sub(
                            r"[^a-z0-9_.-]+",
                            "_",
                            str(
                                source.get("source_id") or "official"
                            ).lower(),
                        ).strip("._") or "official"
                        archive_path = (
                            DEFAULT_OUTPUT_ROOT
                            / "official_documents"
                            / safe_source_id
                            / f"{attachment_sha256}.pdf"
                        )
                        retain_verified_official_pdf(
                            archive_path, attachment_payload, attachment_sha256
                        )
                        attachment_archive_path = str(archive_path)
                    attachment_records.append(
                        {
                            "url": resolved_attachment_url,
                            "content_sha256": attachment_sha256,
                            "content_bytes": len(attachment_payload),
                            "text_characters": len(attachment_text),
                            "available_utc": attachment_available,
                            "parser_kind": attachment_parser_kind,
                            "parser_contract_id": (
                                OFFICIAL_PDF_ATTACHMENT_PARSER_CONTRACT_ID
                            ),
                            "archive_path": attachment_archive_path,
                            "host_trust": "exact_landing_page_host",
                            "research_only": True,
                            "execution_eligible": False,
                        }
                    )
                    attachment_texts.append(
                        f"[Official PDF attachment: "
                        f"{resolved_attachment_url}] {attachment_text}"
                    )
                    total_attachment_bytes += len(attachment_payload)
                    total_attachment_characters += len(attachment_text)
                if attachment_urls and not attachment_records:
                    raise ValueError(
                        "official PDF attachment selection produced no valid document"
                    )
                if attachment_records:
                    maximum_document_characters = max(
                        1_000,
                        min(
                            200_000,
                            int(
                                safe_float(
                                    source.get("detail_maximum_characters"),
                                    60_000,
                                )
                            ),
                        ),
                    )
                    document_text = clean_text(
                        " ".join([document_text, *attachment_texts])
                    )[:maximum_document_characters]
            quality_issue = official_document_quality_issue(document_text)
            if quality_issue:
                raise ValueError(f"official detail quality rejected:{quality_issue}")
            verified_detail_archive_path = ""
            if parser_kind == "official_pdf_text" and bool(source.get("archive_official_pdfs", True)):
                safe_source_id = re.sub(
                    r"[^a-z0-9_.-]+", "_", str(source.get("source_id") or "official").lower()
                ).strip("._") or "official"
                detail_sha256 = hashlib.sha256(detail).hexdigest()
                archive_path = DEFAULT_OUTPUT_ROOT / "official_documents" / safe_source_id / f"{detail_sha256}.pdf"
                retain_verified_official_pdf(archive_path, detail, detail_sha256)
                verified_detail_archive_path = str(archive_path)
        except (OSError, ValueError, PdfReadError, urllib.error.HTTPError) as exc:
            error = str(exc)[:500]
            continue
        # All selected attachments, document quality, and optional PDF
        # archives have passed. Preserve prior clocks and successful insertion
        # order, committing no attachment clocks from a refused article.
        for attachment_record in attachment_records:
            first_seen_by_url.setdefault(
                attachment_record["url"], attachment_record["available_utc"]
            )
        raw_detail_sha256 = hashlib.sha256(detail).hexdigest()
        material_content_sha256 = hashlib.sha256(
            clean_text(document_text).encode("utf-8")
        ).hexdigest()
        prior_material_sha256 = clean_text(prior_content_hashes.get(url))
        baseline_observed_late = bool(
            mutable_versioning
            and url in first_seen_by_url
            and not prior_material_sha256
        )
        material_changed = bool(
            mutable_versioning
            and prior_material_sha256
            and prior_material_sha256 != material_content_sha256
        )
        current_placeholder = bool(
            mutable_versioning
            and mutable_detail_placeholder_matches(
                source,
                title=article.get("title"),
                document_text=document_text,
            )
        )
        content_version_count = (
            max(1, prior_version_counts.get(url, 1)) + 1
            if material_changed
            else max(1, prior_version_counts.get(url, 1))
        )
        detail_available = (
            iso_utc(now)
            if material_changed or baseline_observed_late
            else first_seen_by_url.setdefault(url, iso_utc(now))
        )
        article["summary"] = document_text
        article["detail_enriched"] = True
        article["detail_enrichment_kind"] = parser_kind
        article["detail_available_utc"] = detail_available
        article["detail_content_sha256"] = raw_detail_sha256
        article["detail_content_bytes"] = len(detail)
        article["detail_text_characters"] = len(document_text)
        article["detail_source_url"] = final_url
        if mutable_versioning:
            article.update(
                {
                    "material_content_sha256": material_content_sha256,
                    "content_versioned_at_collection": True,
                    "mutable_content_source": True,
                    "publisher_container_timestamp_utc": clean_text(
                        article.get("published_utc")
                    ),
                    "detail_content_version_contract_id": clean_text(
                        source.get("detail_mutable_placeholder_contract_id")
                    ),
                    "detail_content_version_number": content_version_count,
                    "detail_placeholder_observed": current_placeholder,
                    "_detail_state_content_sha256": material_content_sha256,
                    "_detail_state_is_placeholder": current_placeholder,
                    "_detail_state_version_count": content_version_count,
                }
            )
        if material_changed or baseline_observed_late:
            # A stable publisher URL now exposes materially different content.
            # Give the new body its own immutable event identity while keeping
            # the earlier placeholder row untouched. A migration baseline is
            # explicitly quarantined and can never be credited as prospective
            # proof for a transition that happened before this contract.
            article.update(
                {
                    "structured_event": True,
                    "event_series_id": clean_text(article.get("external_id")) or url,
                    "immutable_source_version_boundary": True,
                    "detail_content_transition": material_changed,
                    "detail_placeholder_transition": bool(
                        material_changed
                        and prior_placeholders.get(url) is True
                        and not current_placeholder
                    ),
                    "supersedes_material_content_sha256": prior_material_sha256,
                    "content_version_observed_utc": iso_utc(now),
                }
            )
        if baseline_observed_late:
            article["source_listing_bootstrap"] = True
            article["detail_existing_item_observed_late"] = True
            article["detail_version_baseline_observed_late"] = True
        if publisher_resolution_method:
            article["detail_listing_url"] = url
            article["detail_publisher_resolution_contract_id"] = (
                publisher_resolution_method
            )
            article["detail_publisher_resolution_known_utc"] = (
                publisher_resolution_known_utc
            )
            article["detail_publisher_resolution_research_only"] = True
        if clean_text(source.get("source_id")) == "abs_latest_releases":
            official_release = parse_abs_official_page_release_clock(document_text)
            if official_release is not None:
                # This corrects the publisher clock, not the knowledge clock.
                # ``detail_available`` and collector ``first_seen`` remain the
                # causal boundary, so a page recovered after an outage is
                # retained as late diagnostic evidence rather than converted
                # into a historical forward signal.
                article["published_utc"] = iso_utc(official_release)
                article["published_time_inferred"] = False
                article["source_native_published_utc"] = iso_utc(
                    official_release
                )
                article["publication_clock_basis"] = (
                    "official_abs_page_release_date_and_time"
                )
                article["publication_clock_contract_id"] = (
                    ABS_OFFICIAL_PAGE_RELEASE_CLOCK_CONTRACT_ID
                )
                article["publication_clock_known_utc"] = detail_available
        if follow_pdf_attachments:
            first_seen_by_url.setdefault(
                attachment_parent_state_key,
                iso_utc(now),
            )
            article["detail_attachment_discovery_state"] = (
                attachment_discovery_state
            )
            article["detail_attachment_enriched"] = bool(attachment_records)
            article["detail_attachment_count"] = len(attachment_records)
            article["detail_attachments"] = attachment_records
            article["detail_attachment_urls"] = [
                row["url"] for row in attachment_records
            ]
            article["detail_attachment_content_sha256"] = (
                attachment_records[0]["content_sha256"]
                if attachment_records else ""
            )
            article["detail_attachment_content_bytes"] = sum(
                int(row["content_bytes"]) for row in attachment_records
            )
            article["detail_attachment_text_characters"] = sum(
                int(row["text_characters"]) for row in attachment_records
            )
            article["detail_attachment_available_utc"] = (
                min(row["available_utc"] for row in attachment_records)
                if attachment_records else ""
            )
            article["detail_attachment_parser_contract_id"] = (
                OFFICIAL_PDF_ATTACHMENT_PARSER_CONTRACT_ID
            )
            article["detail_attachment_research_only"] = True
        if enrich_recent_existing_item:
            article["detail_existing_item_observed_late"] = True
        if context_archive_only:
            # A point-in-time archive is useful research context, but the
            # contents were not first observed at their publication time.
            # Preserve the causal quarantine even on later listing polls.
            article["source_listing_bootstrap"] = True
            article["detail_context_archive_only"] = True
        if verified_detail_archive_path:
            article["detail_archive_path"] = verified_detail_archive_path
        article["detail_enrichment_research_only"] = bool(
            enrichment_kind
            in {
                "official_document_text",
                "ons_release_bulletin",
                "stats_nz_release_detail",
            }
        )
        enriched += 1
    # Keep bounded state while preserving insertion order for recent URLs.
    return enriched, dict(list(first_seen_by_url.items())[-100:]), error


def official_document_quality_issue(document_text: Any) -> str:
    """Reject access/maintenance shells before they become policy evidence."""
    text = clean_text(document_text)
    lowered = text.lower()
    if len(text) < 20:
        return "empty_or_too_short"
    # Error shells are normally short. A long genuine publication can discuss
    # an unavailable service without itself being a publisher challenge page.
    if len(text) <= 2_000:
        if any(
            marker in lowered
            for marker in (
                "service is currently unavailable",
                "temporarily unavailable",
                "scheduled maintenance",
                "maintenance sorry",
                "try accessing the page using a different device",
            )
        ):
            return "maintenance_or_unavailable_page"
        if any(
            marker in lowered
            for marker in (
                "access denied",
                "request unsuccessful",
                "enable javascript and cookies",
                "cloudflare ray id",
                "the requested url was rejected",
                "verify you are human",
                "captcha",
            )
        ):
            return "access_challenge_page"
        if any(
            marker in lowered
            for marker in (
                "404 not found",
                "page not found",
                "we couldn't find the page",
                "we could not find the page",
            )
        ):
            return "not_found_page"
    return ""


def nested_value(payload: Any, path: str) -> Any:
    current = payload
    for part in str(path or "").split("."):
        if not part:
            continue
        if not isinstance(current, Mapping):
            return None
        current = current.get(part)
    return current


def configured_source_is_direct(source: Mapping[str, Any]) -> bool:
    """Use explicit provenance, defaulting only verified first-party rows direct."""
    if "direct" in source:
        return source.get("direct") is True
    return source.get("verified") is True


def source_role(source: Mapping[str, Any]) -> str:
    configured = clean_text(source.get("source_role"))
    if configured:
        return configured
    if bool(source.get("verified")) and configured_source_is_direct(source):
        return "primary_policy_release"
    if configured_source_is_direct(source):
        return "direct_publisher"
    return "news_aggregator"


def credential_environment_names(source: Mapping[str, Any]) -> list[str]:
    configured = source.get("credential_env")
    if isinstance(configured, str):
        configured = [configured]
    return [
        clean_text(value)
        for value in (configured or ())
        if clean_text(value)
    ]


def source_credential(source: Mapping[str, Any]) -> str:
    for name in credential_environment_names(source):
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return ""


def source_runtime_status(source: Mapping[str, Any]) -> str:
    """Return a non-secret operational state for configured source routing."""

    if source.get("enabled", True) is False:
        return "disabled"
    scheduled_pdf_runtime_attested = bool(
        clean_text(source.get("kind")).lower() == "scheduled_official_pdf"
        and clean_text(source.get("required_runtime_contract_id"))
        == SCHEDULED_OFFICIAL_PDF_PROBE_CONTRACT_ID
    )
    if (
        source.get("runtime_supported", True) is False
        and not scheduled_pdf_runtime_attested
    ):
        return "unsupported"
    if source.get("externally_managed", False) is True:
        return "external_adapter"
    if credential_environment_names(source) and not source_credential(source):
        return "credential_missing"
    return "enabled"


def external_adapter_runtime_state(
    source: Mapping[str, Any],
    prior: Mapping[str, Any],
) -> dict[str, Any]:
    """Project a governed external collector into news-source health."""
    updated = dict(prior)
    configured = clean_text(source.get("prospective_state"))
    if not configured:
        updated.update(
            {
                "operational_status": "external_adapter",
                "last_error": "external_adapter_state_not_configured",
                "consecutive_errors": 1,
            }
        )
        return updated
    state_path = Path(configured)
    if not state_path.is_absolute():
        state_path = ROOT / state_path
    try:
        payload = load_json(state_path, {})
        if not isinstance(payload, Mapping):
            raise ValueError("external adapter state is not an object")
        observed = clean_text(payload.get("generated_utc"))
        status = clean_text(payload.get("status")).lower()
        error = clean_text(payload.get("error"))
        errors = payload.get("errors")
        if isinstance(errors, Mapping) and errors:
            error = clean_text(json.dumps(dict(errors), sort_keys=True))
        healthy = status == "ok" and not error
        evidence = payload.get("evidence")
        parsed_items = (
            int(safe_float((evidence or {}).get("immutable_observations"), 0.0))
            if isinstance(evidence, Mapping)
            else int(safe_float(payload.get("currency_count"), 0.0))
        )
        updated.update(
            {
                "operational_status": "external_adapter",
                "last_attempt_utc": observed,
                "last_success_utc": observed if healthy else updated.get("last_success_utc"),
                "last_status": 200 if healthy else 0,
                "last_error": "" if healthy else (error or f"external_adapter_status:{status or 'unknown'}"),
                "consecutive_errors": 0 if healthy else 1,
                "parsed_items": parsed_items,
                "external_adapter": clean_text(source.get("runtime_adapter")),
                "external_source_contract_id": clean_text(payload.get("source_contract_id")),
                "external_source_cohort_id": clean_text(payload.get("source_cohort_id")),
            }
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        updated.update(
            {
                "operational_status": "external_adapter",
                "last_error": f"external_adapter_state_error:{clean_text(exc)}",
                "consecutive_errors": 1,
            }
        )
    return updated


def summarize_source_health(
    sources: Sequence[Mapping[str, Any]],
    source_states: Mapping[str, Mapping[str, Any]],
) -> dict[str, int]:
    """Separate configured availability from observed fetch health.

    ``operational_sources`` historically meant "enabled and callable", which
    can conceal a source that is currently failing under retry backoff.  Keep
    that compatibility count, but expose the observed state independently so
    monitoring does not mistake an enabled source for a healthy one.
    """

    counts = {
        "configured": 0,
        "enabled": 0,
        "healthy": 0,
        "degraded": 0,
        "uninitialized": 0,
        "credential_missing": 0,
        "external_adapter": 0,
        "disabled": 0,
        "unsupported": 0,
    }
    for source in sources:
        if not isinstance(source, Mapping) or not source.get("source_id"):
            continue
        counts["configured"] += 1
        runtime_status = source_runtime_status(source)
        if runtime_status != "enabled":
            counts[runtime_status] = counts.get(runtime_status, 0) + 1
            continue
        counts["enabled"] += 1
        state = source_states.get(str(source["source_id"])) or {}
        if not state.get("last_attempt_utc"):
            counts["uninitialized"] += 1
        elif int(state.get("consecutive_errors") or 0) > 0:
            counts["degraded"] += 1
        else:
            counts["healthy"] += 1
    return counts


def redact_source_secret(value: Any, source: Mapping[str, Any]) -> str:
    text = str(value or "")
    credential = source_credential(source)
    if credential:
        text = text.replace(credential, "[REDACTED]")
        text = text.replace(urllib.parse.quote_plus(credential), "[REDACTED]")
    return text


def optional_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        result = float(value)
        return result if math.isfinite(result) else None
    text = clean_text(value).replace(",", "")
    if not text or text.lower() in {"na", "n/a", "none", "null", "-"}:
        return None
    match = re.search(r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)", text)
    if match is None:
        return None
    try:
        result = float(match.group(0))
    except ValueError:
        return None
    return result if math.isfinite(result) else None


def decompose_source_native_component_changes(
    components: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Retain source-native macro channels without inventing a direction.

    A fall in activity/sentiment and a rise in inflation expectations can
    imply opposite rate/currency channels. This records the observable change
    from the prior release (and a consensus surprise only when supplied). It
    deliberately does not flatten those channels into a trade score.
    """

    if not isinstance(components, Mapping):
        return {}
    activity_names = {
        "consumer_sentiment",
        "current_economic_conditions",
        "consumer_expectations",
        "growth",
        "output",
        "employment",
    }
    inflation_names = {
        "year_ahead_inflation_expectations",
        "long_run_inflation_expectations",
        "inflation",
        "core_inflation",
    }
    policy_rate_names = {"policy_rate"}
    observations: list[dict[str, Any]] = []
    rate_channel_signs: set[int] = set()
    for component_name, raw_values in components.items():
        if not isinstance(raw_values, Mapping):
            continue
        actual = optional_float(raw_values.get("actual"))
        previous = optional_float(raw_values.get("previous"))
        consensus = optional_float(raw_values.get("consensus"))
        dimension = (
            "activity_growth"
            if component_name in activity_names
            else "inflation_expectations"
            if component_name in inflation_names
            else "policy_rate"
            if component_name in policy_rate_names
            else "other"
        )
        delta_previous = (
            round(actual - previous, 12)
            if actual is not None and previous is not None
            else None
        )
        surprise_consensus = (
            round(actual - consensus, 12)
            if actual is not None and consensus is not None
            else None
        )
        directional_input = (
            surprise_consensus
            if surprise_consensus is not None
            else delta_previous
        )
        rate_channel_sign = 0
        if dimension in {
            "activity_growth",
            "inflation_expectations",
            "policy_rate",
        }:
            if directional_input is not None and directional_input > 0:
                rate_channel_sign = 1
            elif directional_input is not None and directional_input < 0:
                rate_channel_sign = -1
        if rate_channel_sign:
            rate_channel_signs.add(rate_channel_sign)
        observations.append(
            {
                "component": str(component_name),
                "dimension": dimension,
                "actual": actual,
                "previous": previous,
                "consensus": consensus,
                "delta_previous": delta_previous,
                "surprise_consensus": surprise_consensus,
                "rate_channel_sign": rate_channel_sign,
                "directional_basis": (
                    "actual_minus_consensus"
                    if surprise_consensus is not None
                    else "actual_minus_previous_context_only"
                    if delta_previous is not None
                    else "insufficient_values"
                ),
            }
        )
    return {
        "components": observations,
        "cross_channel_conflict": len(rate_channel_signs) > 1,
        "consensus_component_count": sum(
            row["surprise_consensus"] is not None for row in observations
        ),
        "previous_change_component_count": sum(
            row["delta_previous"] is not None for row in observations
        ),
        "directional_use": "research_only_pending_consensus_and_rate_repricing",
    }


def mapped_value(
    row: Mapping[str, Any],
    fields: Mapping[str, Any],
    logical_name: str,
    default_path: str,
) -> Any:
    configured = fields.get(logical_name, default_path)
    if isinstance(configured, Sequence) and not isinstance(configured, (str, bytes)):
        for path in configured:
            value = nested_value(row, str(path))
            if value not in (None, ""):
                return value
        return None
    return nested_value(row, str(configured))


def parse_json_records(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    parsed = json.loads(payload.decode("utf-8-sig"))
    records_path = source.get("records_path", "records")
    rows = parsed if str(records_path or "").strip() in {"", "."} else nested_value(
        parsed,
        str(records_path),
    )
    if not isinstance(rows, list):
        raise ValueError("configured JSON records path must resolve to a list")
    fields = source.get("fields") if isinstance(source.get("fields"), Mapping) else {}
    include = [
        re.compile(str(pattern), flags=re.I)
        for pattern in source.get("include_title_patterns") or ()
    ]
    exclude = [
        re.compile(str(pattern), flags=re.I)
        for pattern in source.get("exclude_title_patterns") or ()
    ]
    maximum = max(1, int(safe_float(source.get("max_items"), len(rows) or 1)))
    output: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        title = clean_text(mapped_value(row, fields, "title", "title"))
        if (
            not title
            or (include and not any(pattern.search(title) for pattern in include))
            or any(pattern.search(title) for pattern in exclude)
        ):
            continue
        published = parse_datetime(mapped_value(row, fields, "published", "date"))
        output.append(
            {
                "source_id": source.get("source_id"),
                "source_name": source.get("name"),
                "source_kind": "json_records",
                "source_quality": source.get("quality", 0.9),
                "source_verified": bool(source.get("verified")),
                "source_direct": configured_source_is_direct(source),
                "retrieval_via": str(source.get("retrieval_via") or "direct"),
                "source_role": source_role(source),
                "source_contract_id": clean_text(source.get("source_contract_id")),
                "source_cohort_id": clean_text(source.get("source_cohort_id")),
                "numeric_parser_activated_utc": clean_text(
                    source.get("numeric_parser_activated_utc")
                ),
                "numeric_extraction_contract_id": clean_text(
                    source.get("numeric_extraction_contract_id")
                ),
                "source_currencies": list(source.get("currencies") or []),
                "title": title,
                "summary": clean_text(
                    mapped_value(row, fields, "summary", "summary")
                ),
                "url": canonical_url(
                    urllib.parse.urljoin(
                        str(source.get("url") or ""),
                        str(mapped_value(row, fields, "url", "link") or ""),
                    )
                ),
                "published_utc": iso_utc(published) if published else "",
                "external_id": clean_text(
                    mapped_value(row, fields, "id", "id")
                ),
                "language": clean_text(source.get("language")),
            }
        )
        if len(output) >= maximum:
            break
    return output


COUNTRY_CURRENCY = {
    "australia": "AUD",
    "canada": "CAD",
    "china": "CNH",
    "czech republic": "CZK",
    "denmark": "DKK",
    "euro area": "EUR",
    "hong kong": "HKD",
    "hungary": "HUF",
    "japan": "JPY",
    "mexico": "MXN",
    "new zealand": "NZD",
    "norway": "NOK",
    "poland": "PLN",
    "singapore": "SGD",
    "south africa": "ZAR",
    "sweden": "SEK",
    "switzerland": "CHF",
    "thailand": "THB",
    "turkey": "TRY",
    "united kingdom": "GBP",
    "united states": "USD",
}


def parse_economic_calendar(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Normalize a Trading Economics-compatible calendar response.

    Values are captured for causal research only. No generic positive/negative
    interpretation is applied because the desirable surprise sign is
    series-specific (for example GDP versus unemployment).
    """

    parsed = json.loads(payload.decode("utf-8-sig"))
    records_path = source.get("records_path", "")
    rows = parsed if str(records_path or "").strip() in {"", "."} else nested_value(
        parsed,
        str(records_path),
    )
    if not isinstance(rows, list):
        return []
    output: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        country = clean_text(row.get("Country"))
        event = clean_text(row.get("Event") or row.get("Category"))
        if not event:
            continue
        scheduled = parse_datetime(row.get("Date"))
        vendor_update = parse_datetime(row.get("LastUpdate"))
        actual = row.get("Actual")
        consensus = row.get("Forecast")
        previous = row.get("Previous")
        revised_previous = row.get("Revised")
        actual_value = optional_float(row.get("ActualValue"))
        consensus_value = optional_float(row.get("ForecastValue"))
        previous_value = optional_float(row.get("PreviousValue"))
        revised_previous_value = optional_float(row.get("RevisedValue"))
        currency = clean_text(row.get("Currency")).upper()
        if currency not in ALL_CURRENCIES:
            currency = COUNTRY_CURRENCY.get(country.lower(), "")
        values = []
        if actual not in (None, ""):
            values.append(f"actual {clean_text(actual)}")
        if consensus not in (None, ""):
            values.append(f"consensus {clean_text(consensus)}")
        if previous not in (None, ""):
            values.append(f"previous {clean_text(previous)}")
        title = f"{country} {event}".strip()
        if values:
            title += ": " + ", ".join(values)
        output.append(
            {
                "source_id": source.get("source_id"),
                "source_name": source.get("name"),
                "source_kind": "economic_calendar",
                "source_quality": source.get("quality", 0.95),
                "source_verified": bool(source.get("verified")),
                "source_direct": bool(source.get("direct", False)),
                "retrieval_via": str(source.get("retrieval_via") or "licensed_api"),
                "source_role": source_role(source),
                "source_currencies": [currency] if currency else [],
                "title": title,
                "summary": clean_text(
                    f"reference {row.get('Reference') or row.get('ReferenceDate') or ''}; "
                    f"unit {row.get('Unit') or ''}; importance {row.get('Importance') or ''}"
                ),
                "url": canonical_url(row.get("SourceURL")),
                "publisher_url": canonical_url(row.get("SourceURL")),
                "published_utc": iso_utc(vendor_update) if vendor_update else "",
                "scheduled_utc": iso_utc(scheduled) if scheduled else "",
                "source_reported_update_utc": iso_utc(vendor_update)
                if vendor_update
                else "",
                "external_id": clean_text(row.get("CalendarId") or row.get("CalendarID")),
                "event_series_id": clean_text(row.get("Symbol") or row.get("Ticker")),
                "event_name": event,
                "event_country": country,
                "reference_period": clean_text(row.get("Reference")),
                "reference_date": clean_text(row.get("ReferenceDate")),
                "timing_precision": clean_text(row.get("DateSpan")),
                "importance": row.get("Importance"),
                "unit": clean_text(row.get("Unit")),
                "actual": actual,
                "actual_value": actual_value,
                "consensus": consensus,
                "consensus_value": consensus_value,
                "previous": previous,
                "previous_value": previous_value,
                "revised_previous": revised_previous,
                "revised_previous_value": revised_previous_value,
                "structured_event": True,
            }
        )
    return output


def parse_stats_nz_calendar(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Normalize Stats NZ's first-party monthly release-calendar endpoint."""

    parsed = json.loads(payload.decode("utf-8-sig"))
    items = parsed.get("items") if isinstance(parsed, Mapping) else {}
    rows = items.get("upcoming") if isinstance(items, Mapping) else []
    if not isinstance(rows, list):
        return []
    timezone_name = clean_text(source.get("source_timezone")) or "Pacific/Auckland"
    local_timezone = ZoneInfo(timezone_name)
    output: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        event_name = clean_text(row.get("DisplayName"))
        local_value = clean_text(row.get("PublicationDate"))
        if not event_name or not local_value:
            continue
        try:
            scheduled_local = dt.datetime.fromisoformat(local_value)
        except ValueError:
            continue
        if scheduled_local.tzinfo is None:
            scheduled_local = scheduled_local.replace(tzinfo=local_timezone)
        scheduled = scheduled_local.astimezone(UTC)
        event_id = clean_text(row.get("ID"))
        publisher_url = canonical_url(
            source.get("publisher_url") or source.get("url")
        )
        output.append(
            {
                "source_id": source.get("source_id"),
                "source_name": source.get("name"),
                "source_kind": "stats_nz_calendar",
                "source_quality": source.get("quality", 0.99),
                "source_verified": bool(source.get("verified", True)),
                "source_direct": configured_source_is_direct(source),
                "retrieval_via": str(source.get("retrieval_via") or "direct"),
                "source_role": source_role(source),
                "source_currencies": list(source.get("currencies") or ["NZD"]),
                "title": f"New Zealand {event_name}",
                "summary": (
                    "Official Stats NZ release calendar entry; "
                    f"scheduled {local_value} {timezone_name}."
                ),
                "url": publisher_url,
                "publisher_url": publisher_url,
                "published_utc": "",
                "scheduled_utc": iso_utc(scheduled),
                "source_reported_update_utc": "",
                "external_id": f"stats_nz_calendar:{event_id}:{local_value}",
                "event_series_id": normalized_headline(event_name),
                "event_name": event_name,
                "event_country": "New Zealand",
                "reference_date": clean_text(row.get("DateString")),
                "timing_precision": "minute",
                "structured_event": True,
            }
        )
    return output


def parse_census_release_calendar(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Normalize the Census Bureau economic-indicator release calendar.

    The official list-view page exposes a stable local-time sort key on each
    table row.  Calendar entries are planning context only: they identify when
    a USD release can occur, but they do not infer a direction before the
    release or invent an expectation value.
    """

    page = payload.decode(str(source.get("encoding") or "utf-8"), errors="replace")
    timezone_name = clean_text(source.get("source_timezone")) or "America/New_York"
    local_timezone = ZoneInfo(timezone_name)
    now = utc_now()
    earliest = now - dt.timedelta(
        days=max(0.0, safe_float(source.get("calendar_lookback_days"), 1.0))
    )
    latest = now + dt.timedelta(
        days=max(1.0, safe_float(source.get("calendar_lookahead_days"), 90.0))
    )
    include = [
        re.compile(str(pattern), flags=re.I)
        for pattern in source.get("event_title_patterns") or ()
    ]
    maximum = max(1, int(safe_float(source.get("max_items"), 200)))
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for block in re.findall(r"<tr\b[^>]*>(.*?)</tr>", page, flags=re.I | re.S):
        schedule_match = re.search(
            r"sorttable_customkey\s*=\s*['\"](\d{12})['\"]",
            block,
            flags=re.I,
        )
        link_match = re.search(
            r"<a\b[^>]*href\s*=\s*['\"]([^'\"]+)['\"][^>]*>(.*?)</a>",
            block,
            flags=re.I | re.S,
        )
        if schedule_match is None or link_match is None:
            continue
        event_name = clean_text(link_match.group(2))
        if not event_name or (include and not any(p.search(event_name) for p in include)):
            continue
        try:
            scheduled_local = dt.datetime.strptime(
                schedule_match.group(1), "%Y%m%d%H%M"
            ).replace(tzinfo=local_timezone)
        except ValueError:
            continue
        scheduled = scheduled_local.astimezone(UTC)
        if scheduled < earliest or scheduled > latest:
            continue
        publisher_url = canonical_url(
            urllib.parse.urljoin(str(source.get("url") or ""), link_match.group(1))
        )
        if not trusted_host(publisher_url, source.get("trusted_domains") or ()):
            continue
        event_series_id = re.sub(r"[^a-z0-9]+", " ", event_name.lower()).strip()
        external_id = (
            f"census_calendar:{schedule_match.group(1)}:{event_series_id}"
        )
        if external_id in seen:
            continue
        seen.add(external_id)
        cells = [
            clean_text(value)
            for value in re.findall(r"<td\b[^>]*>(.*?)</td>", block, flags=re.I | re.S)
        ]
        reference_period = cells[3] if len(cells) > 3 else ""
        output.append(
            {
                "source_id": source.get("source_id"),
                "source_name": source.get("name"),
                "source_kind": "census_release_calendar",
                "source_quality": source.get("quality", 0.99),
                "source_verified": bool(source.get("verified", True)),
                "source_direct": configured_source_is_direct(source),
                "retrieval_via": str(source.get("retrieval_via") or "direct"),
                "source_role": source_role(source),
                "source_currencies": list(source.get("currencies") or ["USD"]),
                # A hyphen is commonly interpreted as a publisher suffix by
                # headline normalization. Preserve every part of the official
                # release name with punctuation that cannot be stripped.
                "title": (
                    "United States "
                    + re.sub(r"\s+-\s+", ": ", event_name)
                ),
                "summary": (
                    "Official U.S. Census Bureau economic-indicator calendar entry; "
                    f"scheduled {scheduled_local.isoformat()}."
                ),
                "url": publisher_url,
                "publisher_url": publisher_url,
                "published_utc": "",
                "scheduled_utc": iso_utc(scheduled),
                "source_reported_update_utc": "",
                "external_id": external_id,
                "event_series_id": event_series_id,
                "event_name": event_name,
                "event_country": "United States",
                "reference_period": reference_period,
                "timing_precision": "minute",
                "structured_event": True,
            }
        )
        if len(output) >= maximum:
            break
    return sorted(output, key=lambda row: str(row.get("scheduled_utc") or ""))


def build_recurring_release_calendar(
    source: Mapping[str, Any],
    *,
    now: dt.datetime,
) -> list[dict[str, Any]]:
    """Materialize a bounded official recurring-release schedule.

    This adapter is for publishers that document stable working-day rules but
    whose calendar page is not reliably machine-fetchable. It creates timing
    context only: actual, consensus, surprise, and direction remain unknown.
    Configured holiday exclusions keep working-day arithmetic explicit.
    """

    current = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
    current = current.astimezone(UTC)
    rules = source.get("rules") or []
    excluded = {
        clean_text(value)
        for value in source.get("excluded_working_dates") or []
        if clean_text(value)
    }
    lookback = max(0, int(safe_float(source.get("calendar_lookback_months"), 1)))
    lookahead = max(0, int(safe_float(source.get("calendar_lookahead_months"), 2)))

    def month_start(offset: int) -> dt.date:
        absolute = current.year * 12 + current.month - 1 + offset
        year, month_zero = divmod(absolute, 12)
        return dt.date(year, month_zero + 1, 1)

    def nth_working_day(first: dt.date, ordinal: int) -> dt.date | None:
        cursor = first
        observed = 0
        while cursor.month == first.month:
            if cursor.weekday() < 5 and cursor.isoformat() not in excluded:
                observed += 1
                if observed == ordinal:
                    return cursor
            cursor += dt.timedelta(days=1)
        return None

    publisher_url = canonical_url(source.get("publisher_url") or source.get("url"))
    output: list[dict[str, Any]] = []

    def append_event(
        *,
        event_name: str,
        series_id: str,
        scheduled: dt.datetime,
        reference_period: str = "",
        timing_precision: str = "minute",
        schedule_window_end: dt.datetime | None = None,
    ) -> None:
        output.append(
            {
                "source_id": source.get("source_id"),
                "source_name": source.get("name"),
                "source_kind": "recurring_release_calendar",
                "source_quality": source.get("quality", 0.98),
                "source_verified": bool(source.get("verified", True)),
                "source_direct": configured_source_is_direct(source),
                "retrieval_via": str(
                    source.get("retrieval_via")
                    or "official_published_recurrence_rule"
                ),
                "source_role": source_role(source),
                "source_currencies": list(source.get("currencies") or []),
                "source_contract_id": clean_text(source.get("source_contract_id")),
                "source_cohort_id": clean_text(source.get("source_cohort_id")),
                "title": event_name,
                "summary": (
                    "Official published release-calendar entry; scheduled "
                    f"{iso_utc(scheduled)}. Actual and consensus unknown."
                ),
                "url": publisher_url,
                "publisher_url": publisher_url,
                "published_utc": "",
                "scheduled_utc": iso_utc(scheduled),
                "source_reported_update_utc": "",
                "external_id": (
                    f"recurring_calendar:{series_id}:{scheduled.isoformat()}"
                ),
                "event_series_id": series_id,
                "event_name": event_name,
                "event_country": clean_text(source.get("event_country")),
                "reference_period": reference_period,
                "timing_precision": timing_precision,
                "schedule_window_end_utc": (
                    iso_utc(schedule_window_end)
                    if schedule_window_end is not None
                    else ""
                ),
                "actual": None,
                "actual_value": None,
                "consensus": None,
                "consensus_value": None,
                "structured_event": True,
                "directional_research_only": bool(
                    source.get("directional_research_only", True)
                ),
            }
        )

    # Some official schedules publish exact dates rather than a stable
    # nth-working-day rule. Keep those dates explicit instead of guessing a
    # recurrence from past releases.
    first_allowed = month_start(-lookback)
    last_month = month_start(lookahead)
    last_allowed = (
        dt.date(last_month.year + (1 if last_month.month == 12 else 0),
                1 if last_month.month == 12 else last_month.month + 1,
                1)
        - dt.timedelta(days=1)
    )
    for raw_event in source.get("explicit_schedule") or []:
        if not isinstance(raw_event, Mapping):
            continue
        event_name = clean_text(raw_event.get("event_name"))
        series_id = clean_text(raw_event.get("event_series_id"))
        date_text = clean_text(raw_event.get("local_date"))
        local_time_text = clean_text(raw_event.get("local_time"))
        utc_time_text = clean_text(raw_event.get("utc_time"))
        time_text = local_time_text or utc_time_text
        if not event_name or not series_id or not date_text or not time_text:
            continue
        try:
            scheduled_date = dt.date.fromisoformat(date_text)
            if scheduled_date < first_allowed or scheduled_date > last_allowed:
                continue
            hour, minute = (int(value) for value in time_text.split(":", 1))
            configured_zone = clean_text(
                raw_event.get("source_timezone") or source.get("source_timezone")
            )
            event_timezone = (
                ZoneInfo(configured_zone)
                if local_time_text and configured_zone
                else UTC
            )
            scheduled = dt.datetime.combine(
                scheduled_date,
                dt.time(hour=hour, minute=minute, tzinfo=event_timezone),
            ).astimezone(UTC)
            window_end_text = clean_text(
                raw_event.get("release_window_end_local")
                or raw_event.get("release_window_end_utc")
            )
            schedule_window_end = None
            if window_end_text:
                end_hour, end_minute = (
                    int(value) for value in window_end_text.split(":", 1)
                )
                end_timezone = (
                    event_timezone
                    if raw_event.get("release_window_end_local")
                    else UTC
                )
                schedule_window_end = dt.datetime.combine(
                    scheduled_date,
                    dt.time(
                        hour=end_hour,
                        minute=end_minute,
                        tzinfo=end_timezone,
                    ),
                ).astimezone(UTC)
        except (TypeError, ValueError):
            continue
        append_event(
            event_name=event_name,
            series_id=series_id,
            scheduled=scheduled,
            reference_period=clean_text(raw_event.get("reference_period")),
            timing_precision=(
                clean_text(raw_event.get("timing_precision")) or "minute"
            ),
            schedule_window_end=schedule_window_end,
        )
    for offset in range(-lookback, lookahead + 1):
        first = month_start(offset)
        for raw_rule in rules:
            if not isinstance(raw_rule, Mapping):
                continue
            event_name = clean_text(raw_rule.get("event_name"))
            series_id = clean_text(raw_rule.get("event_series_id"))
            ordinal = int(safe_float(raw_rule.get("working_day"), 0))
            local_time_text = clean_text(raw_rule.get("local_time"))
            time_text = local_time_text or clean_text(raw_rule.get("utc_time"))
            if not event_name or not series_id or ordinal <= 0:
                continue
            try:
                hour, minute = (int(value) for value in time_text.split(":", 1))
                scheduled_date = nth_working_day(first, ordinal)
                if scheduled_date is None:
                    continue
                configured_zone = clean_text(
                    raw_rule.get("source_timezone")
                    or source.get("source_timezone")
                )
                event_timezone = (
                    ZoneInfo(configured_zone)
                    if local_time_text and configured_zone
                    else UTC
                )
                scheduled_local = dt.datetime.combine(
                    scheduled_date,
                    dt.time(hour=hour, minute=minute, tzinfo=event_timezone),
                )
                scheduled = scheduled_local.astimezone(UTC)
            except (TypeError, ValueError):
                continue
            append_event(
                event_name=event_name,
                series_id=series_id,
                scheduled=scheduled,
            )
    deduplicated = {
        str(row.get("external_id") or ""): row
        for row in output
        if str(row.get("external_id") or "")
    }
    return sorted(
        deduplicated.values(), key=lambda row: str(row.get("scheduled_utc") or "")
    )


def _explicit_schedule_event_utc(
    source: Mapping[str, Any], event: Mapping[str, Any]
) -> dt.datetime:
    """Parse one explicitly configured publisher clock without inference."""

    date_text = clean_text(event.get("local_date"))
    local_time_text = clean_text(event.get("local_time"))
    utc_time_text = clean_text(event.get("utc_time"))
    if (
        not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", date_text)
        or bool(local_time_text) == bool(utc_time_text)
    ):
        raise ValueError("scheduled official PDF event needs one exact local/UTC clock")
    time_text = local_time_text or utc_time_text
    if not re.fullmatch(r"\d{2}:\d{2}", time_text):
        raise ValueError("scheduled official PDF event time is not exact to a minute")
    try:
        scheduled_date = dt.date.fromisoformat(date_text)
        hour, minute = (int(value) for value in time_text.split(":", 1))
        timezone_name = clean_text(
            event.get("source_timezone") or source.get("source_timezone")
        )
        event_timezone = (
            ZoneInfo(timezone_name) if local_time_text and timezone_name else UTC
        )
        return dt.datetime.combine(
            scheduled_date,
            dt.time(hour=hour, minute=minute, tzinfo=event_timezone),
        ).astimezone(UTC)
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("scheduled official PDF event clock is invalid") from exc


def scheduled_official_pdf_event(
    source: Mapping[str, Any], *, now: dt.datetime
) -> dict[str, Any] | None:
    """Select at most one exact event inside a tightly bounded probe window."""

    if clean_text(source.get("kind")).lower() != "scheduled_official_pdf":
        raise ValueError("source is not a scheduled official PDF probe")
    current = now.replace(tzinfo=UTC) if now.tzinfo is None else now.astimezone(UTC)
    before_seconds = min(
        300.0, max(0.0, safe_float(source.get("probe_window_before_sec"), 60.0))
    )
    after_seconds = min(
        7_200.0,
        max(1.0, safe_float(source.get("probe_window_after_sec"), 1_800.0)),
    )
    candidates: list[dict[str, Any]] = []
    configured_events = source.get("explicit_schedule")
    if not isinstance(configured_events, list) or not configured_events:
        raise ValueError("scheduled official PDF probe has no explicit schedule")
    for raw_event in configured_events:
        if not isinstance(raw_event, Mapping):
            raise ValueError("scheduled official PDF event is not an object")
        event_name = clean_text(raw_event.get("event_name"))
        series_id = clean_text(raw_event.get("event_series_id"))
        reference_period = clean_text(raw_event.get("reference_period"))
        if (
            not event_name
            or not series_id
            or not re.fullmatch(
                r"20\d{2}-(?:0[1-9]|1[0-2])(?:-(?:0[1-9]|[12]\d|3[01]))?",
                reference_period,
            )
        ):
            raise ValueError("scheduled official PDF event identity is incomplete")
        if len(reference_period) == 10:
            try:
                dt.date.fromisoformat(reference_period)
            except ValueError as exc:
                raise ValueError(
                    "scheduled official PDF reference date is invalid"
                ) from exc
        scheduled = _explicit_schedule_event_utc(source, raw_event)
        if (
            scheduled - dt.timedelta(seconds=before_seconds)
            <= current
            <= scheduled + dt.timedelta(seconds=after_seconds)
        ):
            event = dict(raw_event)
            event["scheduled_utc"] = iso_utc(scheduled)
            event["event_id"] = (
                f"scheduled_official_pdf:{series_id}:{scheduled.isoformat()}"
            )
            candidates.append(event)
    if len(candidates) > 1:
        raise ValueError("scheduled official PDF probe windows overlap")
    return candidates[0] if candidates else None


def scheduled_official_pdf_url(
    source: Mapping[str, Any], event: Mapping[str, Any]
) -> str:
    """Resolve only controlled date tokens and require one exact HTTPS host."""

    scheduled = parse_datetime(event.get("scheduled_utc"))
    reference_period = clean_text(event.get("reference_period"))
    if scheduled is None or not re.fullmatch(
        r"20\d{2}-(?:0[1-9]|1[0-2])(?:-(?:0[1-9]|[12]\d|3[01]))?",
        reference_period,
    ):
        raise ValueError("scheduled official PDF URL lacks an exact event period")
    if len(reference_period) == 10:
        try:
            dt.date.fromisoformat(reference_period)
        except ValueError as exc:
            raise ValueError("scheduled official PDF reference date is invalid") from exc
    reference_year, reference_month = (
        int(value) for value in reference_period[:7].split("-", 1)
    )
    template = clean_text(
        event.get("document_url")
        or source.get("document_url")
        or source.get("document_url_template")
    )
    if not template:
        raise ValueError("scheduled official PDF URL is not configured")
    replacements = {
        "{reference_year}": f"{reference_year:04d}",
        "{reference_month}": f"{reference_month:02d}",
        "{reference_month_name}": dt.date(
            reference_year, reference_month, 1
        ).strftime("%B"),
        "{event_year}": f"{scheduled.year:04d}",
        "{event_month}": f"{scheduled.month:02d}",
        "{event_day}": f"{scheduled.day:02d}",
    }
    resolved = template
    for token, value in replacements.items():
        resolved = resolved.replace(token, value)
    if "{" in resolved or "}" in resolved:
        raise ValueError("scheduled official PDF URL has an unsupported template token")
    resolved = canonical_url(resolved)
    parts = urllib.parse.urlsplit(resolved)
    exact_host = clean_text(source.get("exact_document_host")).casefold()
    if (
        parts.scheme.lower() != "https"
        or not exact_host
        or clean_text(parts.hostname).casefold() != exact_host
        or not trusted_host(resolved, source.get("trusted_domains") or ())
        or not parts.path.lower().endswith(".pdf")
    ):
        raise ValueError("scheduled official PDF URL violates exact-host policy")
    return resolved


def parse_stats_sa_ppi_pdf_text(
    document_text: Any,
    *,
    source: Mapping[str, Any],
    event: Mapping[str, Any],
) -> dict[str, Any]:
    """Extract the narrow Stats SA headline PPI facts and embedded clock."""

    text = clean_text(document_text)
    if not re.search(r"\bSTATISTICAL RELEASE\s+P0142\.1\b", text, flags=re.I):
        raise ValueError("Stats SA PPI publication identity is missing")
    reference_period = clean_text(event.get("reference_period"))
    scheduled = parse_datetime(event.get("scheduled_utc"))
    if scheduled is None or not re.fullmatch(
        r"20\d{2}-(?:0[1-9]|1[0-2])", reference_period
    ):
        raise ValueError("Stats SA PPI event metadata is invalid")
    reference_year, reference_month = (
        int(value) for value in reference_period.split("-", 1)
    )
    reference_month_name = dt.date(reference_year, reference_month, 1).strftime("%B")
    if not re.search(
        rf"\bProducer Price Index\s+{re.escape(reference_month_name)}\s+"
        rf"{reference_year}\b",
        text,
        flags=re.I,
    ):
        raise ValueError("Stats SA PPI PDF reference period does not match schedule")

    embargo_matches = list(
        re.finditer(
            r"\bEmbargoed until:\s*(?P<day>\d{1,2})\s+"
            r"(?P<month>[A-Za-z]+)\s+(?P<year>20\d{2})\s+"
            r"(?P<hour>\d{1,2}):(?P<minute>\d{2})\b",
            text,
            flags=re.I,
        )
    )
    if len(embargo_matches) != 1:
        raise ValueError("Stats SA PPI embedded release clock is missing or ambiguous")
    embargo = embargo_matches[0]
    try:
        embargo_month = dt.datetime.strptime(
            embargo.group("month"), "%B"
        ).month
        event_timezone = ZoneInfo(
            clean_text(
                event.get("source_timezone") or source.get("source_timezone")
            )
        )
        embedded_release = dt.datetime(
            int(embargo.group("year")),
            embargo_month,
            int(embargo.group("day")),
            int(embargo.group("hour")),
            int(embargo.group("minute")),
            tzinfo=event_timezone,
        ).astimezone(UTC)
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Stats SA PPI embedded release clock is invalid") from exc
    if embedded_release != scheduled:
        raise ValueError("Stats SA PPI embedded release clock does not match schedule")

    decimal = r"\d+(?:[,.]\d+)?"
    annual_matches = list(
        re.finditer(
            r"\bAnnual producer price inflation\s*\(final manufacturing\)\s+"
            rf"was\s+(?P<actual>{decimal})%\s+in\s+"
            rf"(?P<month>{re.escape(reference_month_name)})\s+{reference_year},\s+"
            rf"compared with\s+(?P<previous>{decimal})%\s+in\s+"
            r"(?P<previous_month>[A-Za-z]+)\s+(?P<previous_year>20\d{2})\.",
            text,
            flags=re.I,
        )
    )
    monthly_matches = list(
        re.finditer(
            r"\bThe producer price index\s*\(PPI\)\s+"
            r"(?P<verb>increased|decreased)\s+by\s+"
            rf"(?P<value>{decimal})%\s+month-on-month\s+in\s+"
            rf"{re.escape(reference_month_name)}\s+{reference_year}\.",
            text,
            flags=re.I,
        )
    )
    if len(annual_matches) != 1 or len(monthly_matches) != 1:
        raise ValueError("Stats SA PPI headline facts are missing or ambiguous")
    annual = annual_matches[0]
    monthly = monthly_matches[0]
    previous_period = dt.date(reference_year, reference_month, 1) - dt.timedelta(days=1)
    if (
        annual.group("previous_month").casefold()
        != previous_period.strftime("%B").casefold()
        or int(annual.group("previous_year")) != previous_period.year
    ):
        raise ValueError("Stats SA PPI prior comparison period is inconsistent")

    def decimal_value(value: str) -> float:
        parsed = float(value.replace(",", "."))
        if not math.isfinite(parsed):
            raise ValueError("Stats SA PPI numeric value is not finite")
        return parsed

    annual_value = decimal_value(annual.group("actual"))
    previous_annual_value = decimal_value(annual.group("previous"))
    monthly_value = decimal_value(monthly.group("value"))
    monthly_value = (
        -abs(monthly_value)
        if monthly.group("verb").casefold() == "decreased"
        else abs(monthly_value)
    )
    if (
        abs(annual_value) > 100.0
        or abs(previous_annual_value) > 100.0
        or abs(monthly_value) > 50.0
    ):
        raise ValueError("Stats SA PPI headline value is outside parser bounds")
    return {
        "actual": f"{annual_value:.12g}%",
        "actual_value": annual_value,
        "previous": f"{previous_annual_value:.12g}%",
        "previous_value": previous_annual_value,
        "unit": "year_percent_change",
        "source_native_components": {
            "headline_final_manufacturing_ppi_yoy": {
                "actual": annual_value,
                "previous": previous_annual_value,
                "unit": "year_percent_change",
            },
            "headline_final_manufacturing_ppi_mom": {
                "actual": monthly_value,
                "unit": "month_percent_change",
            },
        },
        "release_components": [
            {
                "component_id": "headline_final_manufacturing_ppi_yoy",
                "actual_value": annual_value,
                "previous_value": previous_annual_value,
                "unit": "year_percent_change",
            },
            {
                "component_id": "headline_final_manufacturing_ppi_mom",
                "actual_value": monthly_value,
                "unit": "month_percent_change",
            },
        ],
        "embedded_release_utc": iso_utc(embedded_release),
    }


def parse_dol_weekly_claims_pdf_text(
    document_text: Any,
    *,
    source: Mapping[str, Any],
    event: Mapping[str, Any],
) -> dict[str, Any]:
    """Extract a bounded set of source-native DOL weekly-claims facts.

    The fixed ``/ui/data.pdf`` endpoint is overwritten each week.  The parser
    therefore requires the document's embedded embargo date to equal the
    configured event clock and rejects partial or ambiguous opening prose.
    These values never imply a USD direction without a causally captured
    expectation and independent rate-repricing evidence.
    """

    text = clean_text(document_text)
    if not re.search(
        r"\bUNEMPLOYMENT INSURANCE WEEKLY CLAIMS\b", text, flags=re.I
    ):
        raise ValueError("DOL weekly-claims publication identity is missing")
    scheduled = parse_datetime(event.get("scheduled_utc"))
    if scheduled is None:
        raise ValueError("DOL weekly-claims event clock is missing")
    clock_matches = list(
        re.finditer(
            r"\bEMBARGOED UNTIL\s+8:30\s+A\.M\.\s*\(Eastern\)\s+"
            r"[A-Za-z]+,?\s+(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s+"
            r"(?P<year>20\d{2})\b",
            text,
            flags=re.I,
        )
    )
    if len(clock_matches) != 1:
        raise ValueError("DOL weekly-claims embedded release clock is missing or ambiguous")
    clock = clock_matches[0]
    try:
        event_timezone = ZoneInfo(
            clean_text(event.get("source_timezone") or source.get("source_timezone"))
        )
        embedded_release = dt.datetime(
            int(clock.group("year")),
            dt.datetime.strptime(clock.group("month"), "%B").month,
            int(clock.group("day")),
            8,
            30,
            tzinfo=event_timezone,
        ).astimezone(UTC)
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("DOL weekly-claims embedded release clock is invalid") from exc
    if embedded_release != scheduled:
        raise ValueError("DOL weekly-claims embedded release clock does not match schedule")

    integer = r"\d{1,3}(?:,\d{3})*"
    decimal = r"\d+(?:\.\d+)?"
    initial_matches = list(
        re.finditer(
            r"\bIn the week ending (?P<period>[A-Za-z]+\s+\d{1,2}),\s+"
            r"the advance figure for seasonally adjusted initial claims was\s+"
            rf"(?P<actual>{integer}),\s+"
            r"(?:an?\s+(?P<change_direction>increase|decrease) of\s+"
            rf"(?P<change>{integer})|(?P<change_unchanged>unchanged))\s+from\s+"
            r"(?:the previous week's revised level\.\s+"
            r"The previous week's level was revised\s+"
            r"(?P<revision_direction>up|down)(?:\s+by)?\s+"
            rf"(?P<revision>{integer})\s+from\s+"
            rf"(?P<unrevised>{integer})\s+to\s+(?P<revised>{integer})\."
            r"|the previous week's unrevised level of\s+"
            rf"(?P<unrevised_only>{integer})\.)",
            text,
            flags=re.I,
        )
    )
    continued_matches = list(
        re.finditer(
            r"\bThe advance number for seasonally adjusted insured unemployment "
            r"during the week ending (?P<period>[A-Za-z]+\s+\d{1,2}) was\s+"
            rf"(?P<actual>{integer}),\s+"
            r"(?:an?\s+(?P<change_direction>increase|decrease) of\s+"
            rf"(?P<change>{integer})|(?P<change_unchanged>unchanged))\s+from\s+"
            r"(?:the previous week's revised level\.\s+"
            r"The previous week's level was revised\s+"
            r"(?P<revision_direction>up|down)(?:\s+by)?\s+"
            rf"(?P<revision>{integer})\s+from\s+"
            rf"(?P<unrevised>{integer})\s+to\s+(?P<revised>{integer})\."
            r"|the previous week's unrevised level of\s+"
            rf"(?P<unrevised_only>{integer})\.)",
            text,
            flags=re.I,
        )
    )
    rate_matches = list(
        re.finditer(
            r"\bThe advance seasonally adjusted insured unemployment rate was\s+"
            rf"(?P<actual>{decimal})\s+percent for the week ending\s+"
            r"(?P<period>[A-Za-z]+\s+\d{1,2}),\s+"
            r"(?:"
            r"(?P<unchanged>unchanged)\s+from"
            r"|an?\s+(?P<rate_change_direction>increase|decrease)\s+of\s+"
            rf"(?P<rate_change>{decimal})\s+percentage\s+points?\s+from"
            r")\s+"
            r"the previous week's unrevised rate(?:\s+of\s+"
            rf"(?P<previous>{decimal})\s+percent)?\.",
            text,
            flags=re.I,
        )
    )
    if len(initial_matches) != 1 or len(continued_matches) != 1 or len(rate_matches) != 1:
        raise ValueError("DOL weekly-claims headline facts are missing or ambiguous")

    def whole(value: str) -> int:
        parsed = int(value.replace(",", ""))
        if parsed < 0 or parsed > 20_000_000:
            raise ValueError("DOL weekly-claims integer is outside parser bounds")
        return parsed

    def signed(value: str, direction: str) -> int:
        parsed = whole(value)
        return -parsed if direction.casefold() in {"decrease", "down"} else parsed

    def claims_component(match: re.Match[str]) -> dict[str, Any]:
        actual = whole(match.group("actual"))
        if match.group("unrevised_only") is not None:
            unrevised = whole(match.group("unrevised_only"))
            revised = unrevised
            revision = 0
        else:
            revised = whole(match.group("revised"))
            unrevised = whole(match.group("unrevised"))
            revision = signed(
                match.group("revision"), match.group("revision_direction")
            )
            if revised - unrevised != revision:
                raise ValueError("DOL weekly-claims revision arithmetic is inconsistent")
        change = (
            0
            if match.group("change_unchanged")
            else signed(match.group("change"), match.group("change_direction"))
        )
        if actual - revised != change:
            raise ValueError("DOL weekly-claims weekly-change arithmetic is inconsistent")
        return {
            "reference_period": match.group("period"),
            "actual": actual,
            "previous_revised": revised,
            "previous_unrevised": unrevised,
            "revision": revision,
            "weekly_change": change,
            "unit": "claims",
        }

    initial = claims_component(initial_matches[0])
    continued = claims_component(continued_matches[0])
    rate_match = rate_matches[0]
    insured_rate = float(rate_match.group("actual"))
    previous_rate_text = rate_match.group("previous")
    rate_change_text = rate_match.group("rate_change")
    rate_change_direction = clean_text(rate_match.group("rate_change_direction"))
    if rate_match.group("unchanged"):
        rate_change = 0.0
        derived_previous_rate = insured_rate
    elif rate_change_text is not None and rate_change_direction:
        magnitude = float(rate_change_text)
        rate_change = -magnitude if rate_change_direction.casefold() == "decrease" else magnitude
        derived_previous_rate = insured_rate - rate_change
    else:
        raise ValueError("DOL insured-unemployment rate change is unavailable")
    previous_rate = (
        float(previous_rate_text)
        if previous_rate_text is not None
        else derived_previous_rate
    )
    if not math.isfinite(insured_rate) or insured_rate < 0.0 or insured_rate > 100.0:
        raise ValueError("DOL insured-unemployment rate is outside parser bounds")
    if abs((insured_rate - previous_rate) - rate_change) > 1e-9:
        raise ValueError("DOL insured-unemployment rate arithmetic is inconsistent")

    reference_period = clean_text(event.get("reference_period"))
    try:
        extracted_month_day = dt.datetime.strptime(
            f"{initial['reference_period']} 2000", "%B %d %Y"
        )
        scheduled_local = scheduled.astimezone(event_timezone)
        extracted_date = dt.date(
            scheduled_local.year,
            extracted_month_day.month,
            extracted_month_day.day,
        )
        if extracted_date > scheduled_local.date():
            extracted_date = extracted_date.replace(year=extracted_date.year - 1)
        extracted_reference_period = extracted_date.isoformat()
    except (TypeError, ValueError) as exc:
        raise ValueError("DOL weekly-claims reference period is invalid") from exc
    if reference_period and reference_period != extracted_reference_period:
        raise ValueError("DOL weekly-claims reference period does not match schedule")
    return {
        "actual": str(initial["actual"]),
        "actual_value": float(initial["actual"]),
        "previous": str(initial["previous_revised"]),
        "previous_value": float(initial["previous_revised"]),
        "revised_previous": str(initial["previous_revised"]),
        "revised_previous_value": float(initial["previous_revised"]),
        "unrevised_previous": str(initial["previous_unrevised"]),
        "unrevised_previous_value": float(initial["previous_unrevised"]),
        "revision_raw": float(initial["revision"]),
        "unit": "claims",
        "source_native_components": {
            "initial_claims_sa": initial,
            "insured_unemployment_sa": continued,
            "insured_unemployment_rate_sa": {
                "reference_period": rate_match.group("period"),
                "actual": insured_rate,
                "previous_unrevised": previous_rate,
                "weekly_change": rate_change,
                "unit": "percent",
            },
        },
        "release_components": [
            {
                "component_id": "initial_claims_sa",
                "actual_value": float(initial["actual"]),
                "previous_value": float(initial["previous_revised"]),
                "unrevised_previous_value": float(initial["previous_unrevised"]),
                "revision_raw": float(initial["revision"]),
                "unit": "claims",
            },
            {
                "component_id": "insured_unemployment_sa",
                "actual_value": float(continued["actual"]),
                "previous_value": float(continued["previous_revised"]),
                "unrevised_previous_value": float(continued["previous_unrevised"]),
                "revision_raw": float(continued["revision"]),
                "unit": "claims",
            },
            {
                "component_id": "insured_unemployment_rate_sa",
                "actual_value": insured_rate,
                "previous_value": previous_rate,
                "unit": "percent",
            },
        ],
        "reference_period": extracted_reference_period,
        "embedded_release_utc": iso_utc(embedded_release),
    }


def fetch_scheduled_official_pdf(
    source: Mapping[str, Any],
    source_state: Mapping[str, Any],
    *,
    timeout_sec: float,
    maximum_bytes: int,
    now: dt.datetime,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fetch, hash, archive and parse one exact scheduled official PDF."""

    observed = now.replace(tzinfo=UTC) if now.tzinfo is None else now.astimezone(UTC)
    updated = dict(source_state)
    configured_contract = clean_text(source.get("source_contract_id"))
    configured_cohort = clean_text(source.get("source_cohort_id"))
    prior_contract = clean_text(source_state.get("source_contract_id"))
    updated.update(
        {
            "last_attempt_utc": iso_utc(observed),
            "source_contract_id": configured_contract,
            "source_cohort_id": configured_cohort,
            "scheduled_pdf_probe_contract_id": (
                SCHEDULED_OFFICIAL_PDF_PROBE_CONTRACT_ID
            ),
        }
    )

    def fail(message: Any, *, status: int = 0) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        updated.update(
            {
                "last_status": int(status),
                "last_error": redact_source_secret(clean_text(message), source)[:500],
                "consecutive_errors": int(updated.get("consecutive_errors") or 0)
                + 1,
                "scheduled_pdf_probe_state": "error",
                "parsed_items": 0,
            }
        )
        return [], updated

    try:
        event = scheduled_official_pdf_event(source, now=observed)
    except ValueError as exc:
        return fail(exc)
    if event is None:
        updated.update(
            {
                "scheduled_pdf_probe_state": "outside_exact_event_window",
                "parsed_items": 0,
            }
        )
        return [], updated
    event_id = clean_text(event.get("event_id"))
    event_clock = parse_datetime(event.get("scheduled_utc"))
    if event_clock is None:
        return fail("scheduled official PDF event clock is missing")
    if observed < event_clock:
        # The wake-up window may begin just before the announced minute, but
        # an official embargo clock is a hard knowledge hold. Do not request,
        # parse, archive, or emit numeric contents before that clock even if a
        # publisher happens to expose the predictable URL prematurely.
        updated.update(
            {
                "scheduled_pdf_probe_state": "pre_release_clock_hold_no_fetch",
                "active_event_id": event_id,
                "active_scheduled_utc": iso_utc(event_clock),
                "last_error": "",
                "consecutive_errors": 0,
                "parsed_items": 0,
            }
        )
        return [], updated
    observed_hashes_raw = source_state.get("observed_pdf_sha256_by_event")
    observed_hashes = (
        {
            str(key): [
                clean_text(value)
                for value in values
                if re.fullmatch(r"[0-9a-f]{64}", clean_text(value))
            ][-4:]
            for key, values in observed_hashes_raw.items()
            if isinstance(values, list)
        }
        if isinstance(observed_hashes_raw, Mapping)
        else {}
    )
    if (
        source.get("stop_after_first_valid_capture", True) is True
        and observed_hashes.get(event_id)
        and prior_contract == configured_contract
    ):
        updated.update(
            {
                "scheduled_pdf_probe_state": "event_already_captured",
                "last_error": "",
                "consecutive_errors": 0,
                "parsed_items": 0,
            }
        )
        return [], updated
    try:
        document_url = scheduled_official_pdf_url(source, event)
    except ValueError as exc:
        return fail(exc)
    byte_limit = min(
        int(maximum_bytes),
        max(
            100_000,
            min(
                8_000_000,
                int(safe_float(source.get("direct_pdf_maximum_bytes"), 2_000_000)),
            ),
        ),
    )
    request = urllib.request.Request(
        document_url,
        headers={
            "Accept": "application/pdf",
            "Accept-Encoding": "identity",
            "User-Agent": (
                "ForexResearchNewsCollector/1.0 "
                "(causal research; bounded exact-clock official PDF probe)"
            ),
        },
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=max(2.0, float(timeout_sec)),
            context=TLS_CONTEXT,
        ) as response:
            final_url = canonical_url(response.geturl())
            status = int(getattr(response, "status", 200))
            content_type = clean_text(response.headers.get("Content-Type"))
            etag = clean_text(response.headers.get("ETag"))
            last_modified = clean_text(response.headers.get("Last-Modified"))
            payload = response.read(byte_limit + 1)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            updated.update(
                {
                    "last_status": 404,
                    "last_error": "",
                    "consecutive_errors": 0,
                    "scheduled_pdf_probe_state": "not_yet_published",
                    "active_event_id": event_id,
                    "active_scheduled_utc": clean_text(event.get("scheduled_utc")),
                    "parsed_items": 0,
                }
            )
            return [], updated
        return fail(f"HTTP {exc.code}", status=int(exc.code))
    except OSError as exc:
        return fail(exc)
    if final_url != document_url:
        return fail("scheduled official PDF redirected away from exact URL", status=status)
    if len(payload) > byte_limit:
        return fail("scheduled official PDF exceeds configured byte limit", status=status)
    if content_type.split(";", 1)[0].strip().lower() != "application/pdf":
        return fail("scheduled official PDF returned a non-PDF content type", status=status)
    if not payload.startswith(b"%PDF-"):
        return fail("scheduled official PDF returned a non-PDF payload", status=status)
    content_sha256 = hashlib.sha256(payload).hexdigest()
    if content_sha256 in observed_hashes.get(event_id, ()) and prior_contract == configured_contract:
        updated.update(
            {
                "last_success_utc": iso_utc(observed),
                "last_status": status,
                "last_error": "",
                "consecutive_errors": 0,
                "scheduled_pdf_probe_state": "unchanged_valid_capture",
                "response_bytes": len(payload),
                "parsed_items": 0,
            }
        )
        return [], updated
    try:
        document_text, parser_kind = extract_official_document_text(
            payload,
            content_type=content_type,
            url=document_url,
            maximum_characters=int(
                safe_float(source.get("direct_pdf_maximum_characters"), 60_000)
            ),
            maximum_pages=int(
                safe_float(source.get("direct_pdf_maximum_pages"), 20)
            ),
        )
        quality_issue = official_document_quality_issue(document_text)
        if quality_issue:
            raise ValueError(f"scheduled official PDF quality rejected:{quality_issue}")
        numeric_parser = clean_text(source.get("document_numeric_parser"))
        if numeric_parser == "stats_sa_ppi_headline_v1":
            numeric = parse_stats_sa_ppi_pdf_text(
                document_text,
                source=source,
                event=event,
            )
        elif numeric_parser == "dol_weekly_claims_headline_v1":
            numeric = parse_dol_weekly_claims_pdf_text(
                document_text,
                source=source,
                event=event,
            )
        else:
            raise ValueError("scheduled official PDF numeric parser is not allowlisted")
    except (ValueError, PdfReadError) as exc:
        return fail(f"parse_error: {exc}", status=status)

    safe_source_id = re.sub(
        r"[^a-z0-9_.-]+",
        "_",
        clean_text(source.get("source_id")).casefold(),
    ).strip("._") or "official"
    archive_path = (
        DEFAULT_OUTPUT_ROOT
        / "official_documents"
        / safe_source_id
        / f"{content_sha256}.pdf"
    )
    try:
        if archive_path.exists():
            if hashlib.sha256(archive_path.read_bytes()).hexdigest() != content_sha256:
                raise ValueError("scheduled official PDF archive hash mismatch")
        else:
            atomic_write_bytes(archive_path, payload)
        if hashlib.sha256(archive_path.read_bytes()).hexdigest() != content_sha256:
            raise ValueError("scheduled official PDF archive verification failed")
    except (OSError, ValueError) as exc:
        return fail(exc, status=status)

    scheduled = event_clock
    activation = parse_datetime(source.get("numeric_parser_activated_utc"))
    if scheduled is None or activation is None:
        return fail("scheduled official PDF activation clock is missing", status=status)
    engineering_bootstrap = bool(
        event.get("engineering_bootstrap_recovery") is True
        or scheduled < activation
    )
    numeric_known = max(observed, scheduled, activation)
    first_seen_raw = source_state.get("document_first_seen_utc_by_event")
    first_seen_by_event = (
        {str(key): clean_text(value) for key, value in first_seen_raw.items()}
        if isinstance(first_seen_raw, Mapping)
        else {}
    )
    first_seen_by_event.setdefault(event_id, iso_utc(observed))
    prior_event_hashes = list(observed_hashes.get(event_id) or ())
    prior_event_hashes.append(content_sha256)
    observed_hashes[event_id] = list(dict.fromkeys(prior_event_hashes))[-4:]
    event_name = clean_text(event.get("event_name"))
    reference_period = clean_text(event.get("reference_period"))
    if numeric_parser == "stats_sa_ppi_headline_v1":
        annual_value = float(numeric["actual_value"])
        previous_value = float(numeric["previous_value"])
        monthly_value = float(
            numeric["source_native_components"]
            ["headline_final_manufacturing_ppi_mom"]["actual"]
        )
        release_summary = (
            "Official Stats SA PPI numeric release. "
            f"Headline final manufacturing year percent change {annual_value:.12g}; "
            f"previous year percent change {previous_value:.12g}; "
            f"month percent change {monthly_value:.12g}. "
            "Consensus was not captured."
        )
    elif numeric_parser == "dol_weekly_claims_headline_v1":
        initial = numeric["source_native_components"]["initial_claims_sa"]
        continued = numeric["source_native_components"]["insured_unemployment_sa"]
        insured_rate = numeric["source_native_components"][
            "insured_unemployment_rate_sa"
        ]
        release_summary = (
            "Official U.S. Department of Labor weekly unemployment-insurance "
            f"claims release. Initial claims {int(initial['actual'])}; prior revised "
            f"{int(initial['previous_revised'])}; prior unrevised "
            f"{int(initial['previous_unrevised'])}. Insured unemployment "
            f"{int(continued['actual'])}; prior revised "
            f"{int(continued['previous_revised'])}. Insured unemployment rate "
            f"{float(insured_rate['actual']):.12g} percent. Consensus was not captured."
        )
    else:  # guarded by the allowlist above
        raise ValueError("scheduled official PDF numeric parser is not allowlisted")
    try:
        server_last_modified = (
            email.utils.parsedate_to_datetime(last_modified)
            if last_modified
            else None
        )
    except (TypeError, ValueError):
        # Publisher transport metadata is diagnostic only. A malformed header
        # must not erase an otherwise valid, content-addressed observation.
        server_last_modified = None
    if server_last_modified is not None and server_last_modified.tzinfo is None:
        server_last_modified = server_last_modified.replace(tzinfo=UTC)
    article = {
        "source_id": source.get("source_id"),
        "source_name": source.get("name"),
        "source_kind": "scheduled_official_pdf",
        "source_quality": source.get("quality", 1.0),
        "source_verified": bool(source.get("verified", True)),
        "source_direct": configured_source_is_direct(source),
        "retrieval_via": "exact_scheduled_first_party_pdf",
        "source_role": source_role(source),
        "source_currencies": list(source.get("currencies") or []),
        "source_contract_id": configured_contract,
        "source_cohort_id": configured_cohort,
        "title": event_name,
        "summary": release_summary,
        "url": document_url,
        "publisher_url": clean_text(source.get("publisher_url")),
        "published_utc": iso_utc(scheduled),
        "source_native_published_utc": iso_utc(scheduled),
        "scheduled_utc": iso_utc(scheduled),
        "source_reported_update_utc": "",
        "external_id": event_id,
        "event_series_id": clean_text(event.get("event_series_id")),
        "event_name": event_name,
        "event_country": clean_text(source.get("event_country")),
        "reference_period": reference_period,
        "timing_precision": "minute",
        "event_time_basis": "official_document_embargo_clock",
        "clock_semantics": "domestic_official_statistical_release",
        "publication_clock_basis": "official_document_embargo_until_exact_minute",
        "publication_clock_contract_id": SCHEDULED_OFFICIAL_PDF_PROBE_CONTRACT_ID,
        "publication_clock_known_utc": iso_utc(observed),
        "published_time_inferred": False,
        "structured_event": True,
        **numeric,
        "consensus": None,
        "consensus_value": None,
        "consensus_capture_state": "not_captured",
        "surprise": None,
        "direction": None,
        "numeric_causal_known_utc": iso_utc(numeric_known),
        "numeric_parser_activated_utc": iso_utc(activation),
        "numeric_extraction_contract_id": clean_text(
            source.get("numeric_extraction_contract_id")
        ),
        "numeric_direction_policy": (
            "abstain_until_causal_consensus_and_rate_repricing"
            if numeric_parser == "dol_weekly_claims_headline_v1"
            else "abstain_without_causal_consensus"
        ),
        "detail_enriched": True,
        "detail_enrichment_kind": parser_kind,
        "detail_available_utc": iso_utc(observed),
        "detail_content_sha256": content_sha256,
        "detail_content_bytes": len(payload),
        "detail_text_characters": len(document_text),
        "detail_source_url": document_url,
        "detail_archive_path": str(archive_path),
        "detail_enrichment_research_only": True,
        "material_content_sha256": content_sha256,
        "document_revision_number": len(prior_event_hashes),
        "is_material_revision": len(prior_event_hashes) > 1,
        "supersedes_material_content_sha256": (
            prior_event_hashes[-2] if len(prior_event_hashes) > 1 else ""
        ),
        "revised_at_utc": iso_utc(observed) if len(prior_event_hashes) > 1 else "",
        "revision_id": (
            hashlib.sha256(
                f"{event_id}|{content_sha256}|{len(prior_event_hashes)}".encode("utf-8")
            ).hexdigest()
            if len(prior_event_hashes) > 1
            else ""
        ),
        "document_server_last_modified_utc": (
            iso_utc(server_last_modified) if server_last_modified is not None else ""
        ),
        "document_server_last_modified_semantics": (
            "transport_metadata_not_first_seen_or_causal_clock"
        ),
        "document_etag": etag,
        "scheduled_pdf_probe_contract_id": SCHEDULED_OFFICIAL_PDF_PROBE_CONTRACT_ID,
        "evidence_classification": (
            "engineering_bootstrap_recovery_not_proof"
            if engineering_bootstrap
            else "prospective_research_observation_unvalidated"
        ),
        "engineering_bootstrap_recovery": engineering_bootstrap,
        "engineering_gap_observed_pdf_live_by_utc": (
            clean_text(source.get("engineering_gap_observed_pdf_live_by_utc"))
            if engineering_bootstrap
            else ""
        ),
        "source_listing_bootstrap": engineering_bootstrap,
        "source_listing_new_item": not bool(source_state.get("document_first_seen_utc_by_event", {}).get(event_id))
        if isinstance(source_state.get("document_first_seen_utc_by_event"), Mapping)
        else True,
        "publication_clock_diagnostic_only": engineering_bootstrap,
        "research_only": True,
        "directional_research_only": True,
        "proof_eligible": False,
        "promotion_eligible": False,
        "execution_eligible": False,
        "can_authorize": False,
        "can_place_orders": False,
    }
    updated.update(
        {
            "last_success_utc": iso_utc(observed),
            "last_status": status,
            "last_error": "",
            "consecutive_errors": 0,
            "response_bytes": len(payload),
            "parsed_items": 1,
            "scheduled_pdf_probe_state": "valid_capture_archived",
            "active_event_id": event_id,
            "active_scheduled_utc": iso_utc(scheduled),
            "last_document_url": document_url,
            "last_document_sha256": content_sha256,
            "last_document_archive_path": str(archive_path),
            "etag": etag,
            "last_modified": last_modified,
            "document_first_seen_utc_by_event": dict(
                list(first_seen_by_event.items())[-24:]
            ),
            "observed_pdf_sha256_by_event": dict(
                list(observed_hashes.items())[-24:]
            ),
        }
    )
    return [article], updated


class _OfficialReleaseTableParser(HTMLParser):
    """Extract visible table cells without relying on CSS or page scripts."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        if tag.lower() == "tr":
            self._row = []
        elif tag.lower() in {"td", "th"} and self._row is not None:
            self._cell = []

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        normalized = tag.lower()
        if normalized in {"td", "th"} and self._cell is not None:
            assert self._row is not None
            self._row.append(clean_text(" ".join(self._cell)))
            self._cell = None
        elif normalized == "tr" and self._row is not None:
            if self._row:
                self.rows.append(self._row)
            self._row = None
            self._cell = None


def _exact_calendar_article(
    source: Mapping[str, Any],
    *,
    event_name: str,
    event_series_id: str,
    scheduled: dt.datetime,
    reference_period: str,
    source_row: Sequence[str],
    material_commitment: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a direction-free exact-clock record from an official schedule."""

    scheduled_utc = scheduled.astimezone(UTC)
    publisher_url = canonical_url(source.get("publisher_url") or source.get("url"))
    # Preserve byte-for-byte material identities for already-frozen exact-clock
    # cohorts.  New cohorts may opt into a richer semantic commitment.
    material: Any = (
        {
            "source_row": list(source_row),
            "semantic_commitment": dict(material_commitment),
        }
        if material_commitment is not None
        else list(source_row)
    )
    row_hash = hashlib.sha256(
        json.dumps(
            material,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    return {
        "source_id": source.get("source_id"),
        "source_name": source.get("name"),
        "source_kind": source.get("kind"),
        "source_quality": source.get("quality", 1.0),
        "source_verified": bool(source.get("verified", True)),
        "source_direct": configured_source_is_direct(source),
        "retrieval_via": clean_text(source.get("retrieval_via")),
        "source_role": source_role(source),
        "source_currencies": list(source.get("currencies") or []),
        "source_contract_id": clean_text(source.get("source_contract_id")),
        "source_cohort_id": clean_text(source.get("source_cohort_id")),
        "title": event_name,
        "summary": (
            "Official pre-announced exact release clock; scheduled "
            f"{iso_utc(scheduled_utc)}. Actual, consensus and direction unknown."
        ),
        "url": publisher_url,
        "publisher_url": publisher_url,
        "published_utc": "",
        "scheduled_utc": iso_utc(scheduled_utc),
        "source_reported_update_utc": "",
        "external_id": (
            f"official_exact_clock:{event_series_id}:{scheduled_utc.isoformat()}"
        ),
        "event_series_id": event_series_id,
        "event_name": event_name,
        "event_country": clean_text(source.get("event_country")),
        "reference_period": reference_period,
        "timing_precision": "minute",
        "event_time_basis": "scheduled_release",
        "clock_semantics": "domestic_official_statistical_release",
        "independent_domestic_event": True,
        "linked_policy_factor": False,
        "directional_research_only": bool(
            source.get("directional_research_only", True)
        ),
        "research_only": True,
        "can_place_orders": False,
        "actual": None,
        "actual_value": None,
        "consensus": None,
        "consensus_value": None,
        "direction": None,
        "execution_eligible": False,
        "structured_event": True,
        "material_content_sha256": row_hash,
    }


def parse_denmark_statistics_release_calendar(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse Statistics Denmark's official exact-time release table.

    The official calendar publishes a date, minute, release type, title,
    subject, reference period, and confirmation flag. Only explicitly mapped
    ``News`` rows are retained; StatBank table rows cannot multiply one macro
    release into many apparent events.
    """

    parser = _OfficialReleaseTableParser()
    parser.feed(payload.decode(str(source.get("encoding") or "utf-8"), errors="replace"))
    mappings = [
        dict(row)
        for row in source.get("event_mappings") or []
        if isinstance(row, Mapping)
    ]
    timezone_name = clean_text(source.get("source_timezone")) or "Europe/Copenhagen"
    event_timezone = ZoneInfo(timezone_name)
    output: list[dict[str, Any]] = []
    for row in parser.rows:
        if len(row) < 7:
            continue
        date_text, time_text, release_type, title, subject, period, confirmed = row[:7]
        if release_type.casefold() != "news":
            continue
        if bool(source.get("require_confirmed", True)) and confirmed.casefold() != "yes":
            continue
        mapping = next(
            (
                item
                for item in mappings
                if re.search(
                    clean_text(item.get("title_pattern")),
                    title,
                    flags=re.I,
                )
            ),
            None,
        )
        if mapping is None:
            continue
        normalized_date = re.sub(r"[^0-9]", "-", date_text).strip("-")
        try:
            scheduled_date = dt.datetime.strptime(normalized_date, "%d-%m-%Y").date()
            hour, minute = (int(value) for value in time_text.split(":", 1))
            scheduled = dt.datetime.combine(
                scheduled_date,
                dt.time(hour=hour, minute=minute, tzinfo=event_timezone),
            )
        except (TypeError, ValueError):
            continue
        output.append(
            _exact_calendar_article(
                source,
                event_name=clean_text(mapping.get("event_name")) or title,
                event_series_id=clean_text(mapping.get("event_series_id")),
                scheduled=scheduled,
                reference_period=period,
                source_row=[date_text, time_text, release_type, title, subject, period, confirmed],
            )
        )
    deduplicated = {
        str(item.get("external_id")): item
        for item in output
        if item.get("external_id")
    }
    return sorted(
        deduplicated.values(),
        key=lambda item: (str(item.get("scheduled_utc") or ""), str(item.get("event_series_id") or "")),
    )


KSH_V3_SOURCE_ID = "hungary_ksh_headline_cpi_release_clock_exact_v3"
KSH_V3_CONTRACT_ID = (
    "hungary_ksh_headline_cpi_release_clock_exact_v3_"
    "verified_policy_bytes_20260817"
)
KSH_V3_CALENDAR_URL = "https://www.ksh.hu/prices?lang=en"
KSH_V3_POLICY_URL = (
    "https://www.ksh.hu/docs/bemutatkozas/eng/"
    "dissemination-and-communication-policy-2024.pdf"
)
KSH_V3_POLICY_SHA256 = (
    "617f513efcccfd07fa159b0fdf9fc9e0e3243095e20deb50d88e88495fbd4030"
)
KSH_V3_POLICY_ARCHIVE = (
    "ksh_dissemination_policy_2024_617f513efcccfd07.pdf"
)
KSH_V3_POLICY_SECTION = (
    "VI. Release and access, page 11: first releases including leading "
    "indicators and related data categories are published strictly at "
    "8:30 a.m."
)
KSH_V3_PUBLIC_KNOWLEDGE_POLICY = (
    "public_release_at_0830_local; "
    "embargoed_or_pre_release_access_is_not_public_causal_knowledge"
)
KSH_V3_MAPPING = {
    "title_pattern": r"^Consumer prices,\s+[A-Za-z]+\s+[0-9]{4}$",
    "event_series_id": "hungary_headline_cpi_yoy",
    "event_name": "Hungary headline consumer price inflation",
}
KSH_V3_SOURCE_NAME = (
    "Hungarian Central Statistical Office headline CPI exact public-release clock"
)
KSH_V3_RETRIEVAL_VIA = (
    "direct_official_ksh_headline_cpi_topic_calendar_plus_verified_archived_"
    "content_addressed_official_fixed_release_time_policy"
)


def parse_ksh_release_calendar_exact(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse exact KSH first-release clocks from the official topic table.

    KSH's topic pages publish the next release *date*.  KSH's official
    dissemination policy separately fixes first releases at 08:30 local time.
    The source contract binds both official pages; this parser combines them
    without assigning a surprise, direction, or trading action.
    """

    parser = _OfficialReleaseTableParser()
    if clean_text(source.get("encoding")):
        raise ValueError("KSH V3 encoding override is invalid")
    parser.feed(payload.decode("utf-8", errors="replace"))
    mappings = [
        dict(row)
        for row in source.get("event_mappings") or []
        if isinstance(row, Mapping)
    ]
    if clean_text(source.get("kind")) != "ksh_release_calendar_verified_rule_exact_v3":
        raise ValueError("KSH V3 source kind is invalid")
    if (
        clean_text(source.get("name")) != KSH_V3_SOURCE_NAME
        or list(source.get("currencies") or []) != ["HUF"]
        or source.get("verified") is not True
        or source.get("direct") is not True
        or safe_float(source.get("quality"), -1.0) != 1.0
        or clean_text(source.get("retrieval_via")) != KSH_V3_RETRIEVAL_VIA
        or clean_text(source.get("source_role"))
        != "primary_statistical_calendar"
        or clean_text(source.get("event_country")) != "Hungary"
        or source.get("directional_research_only") is not True
        or list(source.get("trusted_domains") or [])
        != ["ksh.hu", "www.ksh.hu"]
    ):
        raise ValueError("KSH V3 official-source semantics are invalid")
    timezone_name = clean_text(source.get("source_timezone"))
    if timezone_name != "Europe/Budapest":
        raise ValueError("KSH V3 source timezone is invalid")
    try:
        event_timezone = ZoneInfo(timezone_name)
    except Exception as exc:
        raise ValueError("KSH exact calendar timezone is invalid") from exc
    release_time = clean_text(source.get("official_release_time_local"))
    if release_time != "08:30":
        raise ValueError("KSH V3 release time is invalid")
    try:
        hour, minute = (int(value) for value in release_time.split(":", 1))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise ValueError("KSH exact calendar release time is invalid") from exc
    rule_url = canonical_url(source.get("release_time_rule_url"))
    if rule_url != KSH_V3_POLICY_URL:
        raise ValueError("KSH V3 release-time rule URL is invalid")
    trusted_domains = source.get("trusted_domains") or ()
    if not trusted_host(rule_url, trusted_domains):
        raise ValueError("KSH release-time rule URL is outside trusted domains")
    configured_calendar_url = canonical_url(source.get("url"))
    publisher_calendar_url = canonical_url(source.get("publisher_url"))
    if (
        configured_calendar_url != KSH_V3_CALENDAR_URL
        or publisher_calendar_url != KSH_V3_CALENDAR_URL
    ):
        raise ValueError("KSH V3 calendar URL is invalid")
    calendar_url = publisher_calendar_url
    if not trusted_host(calendar_url, trusted_domains):
        raise ValueError("KSH exact calendar URL is outside trusted domains")
    policy_sha256 = clean_text(source.get("release_time_rule_sha256")).lower()
    if policy_sha256 != KSH_V3_POLICY_SHA256:
        raise ValueError("KSH release-time policy content hash is invalid")
    observed_policy_sha256 = clean_text(
        source.get("release_time_rule_observed_sha256")
    ).lower()
    if (
        source.get("release_rule_bytes_verified") is not True
        or observed_policy_sha256 != policy_sha256
    ):
        raise ValueError("KSH release-time policy bytes were not verified")
    rule_archive_name = clean_text(source.get("release_time_rule_archive_name"))
    if rule_archive_name != KSH_V3_POLICY_ARCHIVE:
        raise ValueError("KSH V3 release-time policy archive identity is invalid")
    policy_section = clean_text(source.get("release_time_rule_section"))
    if policy_section != KSH_V3_POLICY_SECTION:
        raise ValueError("KSH V3 release-time policy section is invalid")
    contract_id = clean_text(source.get("source_contract_id"))
    cohort_id = clean_text(source.get("source_cohort_id"))
    if (
        clean_text(source.get("source_id")) != KSH_V3_SOURCE_ID
        or contract_id != KSH_V3_CONTRACT_ID
        or cohort_id != KSH_V3_CONTRACT_ID
    ):
        raise ValueError("KSH source contract/cohort identity is invalid")
    public_knowledge_time_policy = clean_text(
        source.get("public_knowledge_time_policy")
    )
    if public_knowledge_time_policy != KSH_V3_PUBLIC_KNOWLEDGE_POLICY:
        raise ValueError("KSH public-knowledge-time policy is invalid")
    reference_period_rule = clean_text(source.get("reference_period_rule"))
    if reference_period_rule != "next_calendar_month_after_latest_release_title":
        raise ValueError("KSH reference-period rule is invalid")
    if len(mappings) != 1:
        raise ValueError("KSH exact CPI clock requires one frozen event mapping")
    mapping = mappings[0]
    if mapping != KSH_V3_MAPPING:
        raise ValueError("KSH exact CPI event mapping does not match frozen contract")
    title_pattern = clean_text(mapping.get("title_pattern"))
    event_series_id = clean_text(mapping.get("event_series_id"))
    event_name = clean_text(mapping.get("event_name"))
    if not title_pattern or not event_series_id or not event_name:
        raise ValueError("KSH exact CPI event mapping is incomplete")
    try:
        compiled_title_pattern = re.compile(title_pattern, flags=re.I)
    except re.error as exc:
        raise ValueError("KSH exact CPI title pattern is invalid") from exc

    output: list[dict[str, Any]] = []
    matched_rows = 0
    for row in parser.rows:
        if len(row) < 3:
            continue
        title, latest_release, next_release = row[:3]
        if compiled_title_pattern.search(title) is None:
            continue
        matched_rows += 1
        try:
            latest_release_date = dt.datetime.strptime(
                latest_release, "%d/%m/%Y"
            ).date()
        except ValueError as exc:
            raise ValueError("KSH mapped CPI row has malformed latest-release date") from exc
        try:
            scheduled_date = dt.datetime.strptime(next_release, "%d/%m/%Y").date()
        except ValueError as exc:
            raise ValueError("KSH mapped CPI row has malformed next-release date") from exc
        scheduled = dt.datetime.combine(
            scheduled_date,
            dt.time(hour=hour, minute=minute, tzinfo=event_timezone),
        )
        reference_period = ""
        period_match = re.search(
            r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})\b",
            title,
            flags=re.I,
        )
        if period_match is None:
            raise ValueError("KSH mapped CPI row has no reference month")
        current = dt.datetime.strptime(
            f"{period_match.group(1)} {period_match.group(2)}", "%B %Y"
        )
        next_month = (
            current.replace(year=current.year + 1, month=1)
            if current.month == 12
            else current.replace(month=current.month + 1)
        )
        if (latest_release_date.year, latest_release_date.month) != (
            next_month.year,
            next_month.month,
        ):
            raise ValueError(
                "KSH latest-release date is inconsistent with headline month"
            )
        next_release_month = (
            latest_release_date.replace(
                year=latest_release_date.year + 1,
                month=1,
                day=1,
            )
            if latest_release_date.month == 12
            else latest_release_date.replace(
                month=latest_release_date.month + 1,
                day=1,
            )
        )
        if not (
            latest_release_date < scheduled_date
            and (scheduled_date.year, scheduled_date.month)
            == (next_release_month.year, next_release_month.month)
        ):
            raise ValueError("KSH next-release date sequence is invalid")
        reference_period = next_month.strftime("%YM%m")
        semantic_commitment = {
            "source_contract_id": contract_id,
            "source_cohort_id": cohort_id,
            "source_timezone": timezone_name,
            "calendar_url": calendar_url,
            "official_release_time_local": release_time,
            "public_knowledge_time_policy": public_knowledge_time_policy,
            "release_time_rule_url": rule_url,
            "release_time_rule_sha256": policy_sha256,
            "release_time_rule_observed_sha256": observed_policy_sha256,
            "release_time_rule_archive_name": rule_archive_name,
            "release_rule_bytes_verified": True,
            "release_time_rule_section": policy_section,
            "event_series_id": event_series_id,
            "event_name": event_name,
            "title_pattern": title_pattern,
            "reference_period_rule": reference_period_rule,
        }
        article = _exact_calendar_article(
            source,
            event_name=event_name,
            event_series_id=event_series_id,
            scheduled=scheduled,
            reference_period=reference_period,
            source_row=[
                title,
                latest_release,
                next_release,
                release_time,
                rule_url,
            ],
            material_commitment=semantic_commitment,
        )
        article["release_time_rule_url"] = rule_url
        article["release_time_rule_sha256"] = policy_sha256
        article["release_time_rule_observed_sha256"] = observed_policy_sha256
        article["release_time_rule_archive_name"] = rule_archive_name
        article["release_rule_bytes_verified"] = True
        article["release_time_rule_section"] = policy_section
        article["reference_period_rule"] = reference_period_rule
        article["material_commitment"] = semantic_commitment
        article["release_time_basis"] = "official_ksh_first_release_fixed_local_time"
        output.append(article)
    if matched_rows == 0:
        raise ValueError("KSH mapped headline-CPI release row is missing")
    if matched_rows != 1 or len(output) != 1:
        raise ValueError("KSH mapped headline-CPI release row is ambiguous")
    deduplicated = {
        str(item.get("external_id")): item
        for item in output
        if item.get("external_id")
    }
    return sorted(
        deduplicated.values(),
        key=lambda item: (
            str(item.get("scheduled_utc") or ""),
            str(item.get("event_series_id") or ""),
        ),
    )


def _denmark_calendar_page_date_bounds(
    payload: bytes,
    source: Mapping[str, Any],
) -> tuple[dt.date | None, dt.date | None]:
    parser = _OfficialReleaseTableParser()
    parser.feed(payload.decode(str(source.get("encoding") or "utf-8"), errors="replace"))
    dates: list[dt.date] = []
    for row in parser.rows:
        if len(row) < 7:
            continue
        normalized_date = re.sub(r"[^0-9]", "-", row[0]).strip("-")
        try:
            dates.append(dt.datetime.strptime(normalized_date, "%d-%m-%Y").date())
        except ValueError:
            continue
    return (min(dates), max(dates)) if dates else (None, None)


def fetch_denmark_statistics_release_calendar_pages(
    first_payload: bytes,
    source: Mapping[str, Any],
    *,
    headers: Mapping[str, str],
    timeout_sec: float,
    maximum_bytes: int,
    now: dt.datetime,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fetch a bounded forward calendar horizon across official HTML pages.

    Statistics Denmark exposes thousands of historic/future rows in 100-row
    pages.  Page 1 covers only a few days.  Walk forward deterministically
    until at least the configured prospective horizon is visible, subject to
    a strict page and byte cap.  Any failure to reach the horizon fails the
    source rather than presenting page 1 as complete coverage.
    """

    horizon_days = max(
        45,
        min(180, int(safe_float(source.get("prospective_horizon_days"), 45))),
    )
    maximum_pages = max(
        2,
        min(50, int(safe_float(source.get("pagination_max_pages"), 12))),
    )
    start_page = max(1, int(safe_float(source.get("pagination_start_page"), 1)))
    total_limit = max(
        len(first_payload),
        min(
            maximum_bytes,
            int(
                safe_float(
                    source.get("pagination_maximum_bytes_total"),
                    maximum_bytes,
                )
            ),
        ),
    )
    template = clean_text(source.get("pagination_url_template"))
    if "{page}" not in template:
        raise ValueError("Denmark calendar pagination_url_template lacks {page}")
    target_date = now.astimezone(UTC).date() + dt.timedelta(days=horizon_days)
    page_payloads = [first_payload]
    total_bytes = len(first_payload)
    _, last_date = _denmark_calendar_page_date_bounds(first_payload, source)
    stop_reason = "horizon_covered" if last_date and last_date >= target_date else ""
    request_headers = {
        str(key): str(value)
        for key, value in headers.items()
        if str(key).lower() not in {"if-none-match", "if-modified-since"}
    }
    request_headers["Accept"] = "text/html,application/xhtml+xml"
    for page_number in range(start_page + 1, start_page + maximum_pages):
        if stop_reason:
            break
        page_url = template.format(page=page_number)
        if not trusted_host(page_url, source.get("trusted_domains") or ()):
            raise ValueError("Denmark calendar pagination left trusted domain")
        request = urllib.request.Request(page_url, headers=request_headers)
        with urllib.request.urlopen(
            request,
            timeout=timeout_sec,
            context=TLS_CONTEXT,
        ) as response:
            if int(response.status) != 200:
                raise ValueError(
                    f"Denmark calendar pagination HTTP {int(response.status)}"
                )
            remaining = total_limit - total_bytes
            if remaining <= 0:
                raise ValueError("Denmark calendar pagination byte cap reached")
            payload = response.read(remaining + 1)
        if len(payload) > remaining:
            raise ValueError("Denmark calendar pagination byte cap reached")
        page_first, page_last = _denmark_calendar_page_date_bounds(payload, source)
        if page_first is None or page_last is None:
            raise ValueError("Denmark calendar pagination returned no dated rows")
        if last_date is not None and page_last <= last_date:
            raise ValueError("Denmark calendar pagination stopped advancing")
        page_payloads.append(payload)
        total_bytes += len(payload)
        last_date = page_last
        if last_date >= target_date:
            stop_reason = "horizon_covered"
    if not stop_reason:
        raise ValueError(
            "Denmark calendar pagination cap did not reach configured horizon"
        )
    output: list[dict[str, Any]] = []
    for payload in page_payloads:
        output.extend(parse_denmark_statistics_release_calendar(payload, source))
    deduplicated = {
        str(item.get("external_id")): item
        for item in output
        if item.get("external_id")
    }
    return (
        sorted(
            deduplicated.values(),
            key=lambda item: (
                str(item.get("scheduled_utc") or ""),
                str(item.get("event_series_id") or ""),
            ),
        ),
        {
            "pagination_pages_fetched": len(page_payloads),
            "pagination_total_bytes": total_bytes,
            "pagination_horizon_target_date": target_date.isoformat(),
            "pagination_horizon_end_date": last_date.isoformat() if last_date else "",
            "pagination_horizon_days": horizon_days,
            "pagination_stop_reason": stop_reason,
        },
    )


def _xlsx_rows(
    payload: bytes,
    *,
    worksheet_name: str = "",
) -> list[list[str]]:
    """Read a workbook-selected OOXML worksheet using only the stdlib."""

    namespace = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    relationship_namespace = {
        "r": "http://schemas.openxmlformats.org/package/2006/relationships"
    }
    document_relationship = (
        "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
    )
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as workbook:
            try:
                shared_root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
            except KeyError:
                shared = []
            else:
                shared = [
                    "".join(
                        node.text or ""
                        for node in item.iterfind(".//m:t", namespace)
                    )
                    for item in shared_root.findall("m:si", namespace)
                ]
            workbook_root = ET.fromstring(workbook.read("xl/workbook.xml"))
            relationships_root = ET.fromstring(
                workbook.read("xl/_rels/workbook.xml.rels")
            )
            relationships = {
                str(row.get("Id") or ""): str(row.get("Target") or "")
                for row in relationships_root.findall("r:Relationship", relationship_namespace)
                if str(row.get("Type") or "").endswith("/worksheet")
                and str(row.get("TargetMode") or "").casefold() != "external"
            }
            sheet_rows = workbook_root.findall("m:sheets/m:sheet", namespace)
            selected_sheet = next(
                (
                    row
                    for row in sheet_rows
                    if worksheet_name
                    and clean_text(row.get("name")).casefold()
                    == worksheet_name.casefold()
                ),
                sheet_rows[0] if sheet_rows and not worksheet_name else None,
            )
            if selected_sheet is None:
                raise ValueError("official xlsx worksheet name not found")
            relationship_id = str(selected_sheet.get(document_relationship) or "")
            target = relationships.get(relationship_id, "").replace("\\", "/")
            if not target:
                raise ValueError("official xlsx worksheet relationship not found")
            if target.startswith("/"):
                sheet_path = target.lstrip("/")
            else:
                sheet_path = urllib.parse.urljoin("xl/", target)
            sheet_path = urllib.parse.unquote(sheet_path)
            if not sheet_path.startswith("xl/") or ".." in Path(sheet_path).parts:
                raise ValueError("official xlsx worksheet path is unsafe")
            sheet = ET.fromstring(workbook.read(sheet_path))
    except (KeyError, zipfile.BadZipFile, ET.ParseError) as exc:
        raise ValueError("invalid official xlsx release calendar") from exc

    def column_number(reference: str) -> int:
        match = re.match(r"([A-Z]+)", reference.upper())
        if match is None:
            return 0
        value = 0
        for character in match.group(1):
            value = value * 26 + ord(character) - ord("A") + 1
        return value

    rows: list[list[str]] = []
    for row in sheet.findall(".//m:sheetData/m:row", namespace):
        values: dict[int, str] = {}
        for cell in row.findall("m:c", namespace):
            value = cell.findtext("m:v", default="", namespaces=namespace)
            if cell.get("t") == "s" and value:
                try:
                    value = shared[int(value)]
                except (IndexError, ValueError) as exc:
                    raise ValueError("invalid official xlsx shared-string index") from exc
            elif cell.get("t") == "inlineStr":
                value = "".join(
                    node.text or "" for node in cell.iterfind(".//m:t", namespace)
                )
            values[column_number(str(cell.get("r") or ""))] = clean_text(value)
        if values:
            rows.append([values.get(index, "") for index in range(1, 8)])
    return rows


def parse_hong_kong_censtatd_release_calendar_xlsx(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse C&SD's annual schedule at its documented 16:30 HKT clock."""

    mappings = {
        clean_text(row.get("series_title")).casefold(): dict(row)
        for row in source.get("event_mappings") or []
        if isinstance(row, Mapping) and clean_text(row.get("series_title"))
    }
    timezone_name = clean_text(source.get("source_timezone")) or "Asia/Hong_Kong"
    event_timezone = ZoneInfo(timezone_name)
    release_time = clean_text(source.get("official_release_time_local")) or "16:30"
    try:
        hour, minute = (int(value) for value in release_time.split(":", 1))
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid official Hong Kong release time") from exc
    output: list[dict[str, Any]] = []
    for row in _xlsx_rows(
        payload,
        worksheet_name=clean_text(source.get("worksheet_name")),
    ):
        if len(row) < 5:
            continue
        serial_text, _category, _subject, series_title, title = row[:5]
        mapping = mappings.get(series_title.casefold())
        if mapping is None:
            continue
        try:
            serial = int(float(serial_text))
            release_date = dt.date(1899, 12, 30) + dt.timedelta(days=serial)
            scheduled = dt.datetime.combine(
                release_date,
                dt.time(hour=hour, minute=minute, tzinfo=event_timezone),
            )
        except (TypeError, ValueError, OverflowError):
            continue
        output.append(
            _exact_calendar_article(
                source,
                event_name=clean_text(mapping.get("event_name")) or series_title,
                event_series_id=clean_text(mapping.get("event_series_id")),
                scheduled=scheduled,
                reference_period=title,
                source_row=row,
            )
        )
    deduplicated = {
        str(item.get("external_id")): item
        for item in output
        if item.get("external_id")
    }
    return sorted(
        deduplicated.values(),
        key=lambda item: (str(item.get("scheduled_utc") or ""), str(item.get("event_series_id") or "")),
    )


def parse_alpha_vantage_news(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    parsed = json.loads(payload.decode("utf-8-sig"))
    if isinstance(parsed, Mapping):
        provider_error = clean_text(
            parsed.get("Error Message")
            or parsed.get("Note")
            or parsed.get("Information")
        )
        if provider_error and not isinstance(parsed.get("feed"), list):
            # Alpha Vantage returns HTTP 200 for quota and credential errors.
            # Treat those payloads as failures so monitoring cannot mistake an
            # empty quota response for a healthy zero-news interval.
            raise ValueError(f"alpha_vantage_provider_error: {provider_error[:240]}")
    rows = parsed.get("feed") if isinstance(parsed, Mapping) else None
    if not isinstance(rows, list):
        return []
    output: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        title = clean_text(row.get("title"))
        if not title:
            continue
        ticker_sentiment = row.get("ticker_sentiment")
        currencies: set[str] = set()
        if isinstance(ticker_sentiment, list):
            for item in ticker_sentiment:
                if not isinstance(item, Mapping):
                    continue
                ticker = clean_text(item.get("ticker")).upper()
                match = re.fullmatch(r"FOREX:([A-Z]{3})", ticker)
                if match and match.group(1) in ALL_CURRENCIES:
                    currencies.add(match.group(1))
        vendor_currency_numerators: dict[str, float] = defaultdict(float)
        vendor_currency_denominators: dict[str, float] = defaultdict(float)
        if isinstance(ticker_sentiment, list):
            for item in ticker_sentiment:
                if not isinstance(item, Mapping):
                    continue
                ticker = clean_text(item.get("ticker")).upper()
                match = re.fullmatch(r"FOREX:([A-Z]{3})", ticker)
                score = optional_float(item.get("ticker_sentiment_score"))
                if not match or match.group(1) not in ALL_CURRENCIES or score is None:
                    continue
                relevance = optional_float(item.get("relevance_score"))
                weight = max(0.01, relevance if relevance is not None else 1.0)
                currency = match.group(1)
                vendor_currency_numerators[currency] += weight * clamp(score)
                vendor_currency_denominators[currency] += weight
        vendor_currency_sentiment = {
            currency: round(
                vendor_currency_numerators[currency]
                / vendor_currency_denominators[currency],
                6,
            )
            for currency in sorted(vendor_currency_numerators)
            if vendor_currency_denominators[currency] > 0
        }
        published = parse_datetime(row.get("time_published"))
        output.append(
            {
                "source_id": source.get("source_id"),
                "source_name": clean_text(row.get("source")) or source.get("name"),
                "source_kind": "alpha_vantage_news",
                "source_quality": source.get("quality", 0.7),
                "source_verified": False,
                "source_direct": False,
                "retrieval_via": "credentialed_aggregator_api",
                "source_role": source_role(source),
                # Alpha's queried/related ticker is vendor metadata, not
                # evidence that the article text is about that currency.  A
                # FOREX:TRY query can return unrelated equity transcripts with
                # a tiny TRY relevance tag.  Keep those tags available to the
                # vendor ablation channel without injecting them into the
                # deterministic local entity/direction mapping.
                "source_currencies": [],
                "vendor_currencies": sorted(currencies),
                "title": title,
                "summary": clean_text(row.get("summary")),
                "url": canonical_url(row.get("url")),
                "published_utc": iso_utc(published) if published else "",
                "external_id": clean_text(row.get("url")),
                "vendor_sentiment_score": optional_float(
                    row.get("overall_sentiment_score")
                ),
                "vendor_sentiment_label": clean_text(
                    row.get("overall_sentiment_label")
                ),
                "vendor_topics": row.get("topics")
                if isinstance(row.get("topics"), list)
                else [],
                "vendor_ticker_sentiment": ticker_sentiment
                if isinstance(ticker_sentiment, list)
                else [],
                # Vendor scores are retained as a separate research channel.
                # They never become local currency direction in this parser.
                "vendor_currency_sentiment": vendor_currency_sentiment,
                "vendor_sentiment_research_only": True,
                "source_contract_id": clean_text(source.get("source_contract_id")),
                "source_cohort_id": clean_text(source.get("source_cohort_id")),
            }
        )
    return output


def parse_finnhub_news(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    parsed = json.loads(payload.decode("utf-8-sig"))
    if not isinstance(parsed, list):
        return []
    output: list[dict[str, Any]] = []
    for row in parsed:
        if not isinstance(row, Mapping):
            continue
        title = clean_text(row.get("headline"))
        if not title:
            continue
        timestamp = optional_float(row.get("datetime"))
        published = None
        if timestamp is not None:
            try:
                published = dt.datetime.fromtimestamp(timestamp, tz=UTC)
            except (OverflowError, OSError, ValueError):
                published = None
        output.append(
            {
                "source_id": source.get("source_id"),
                "source_name": clean_text(row.get("source")) or source.get("name"),
                "source_kind": "finnhub_news",
                "source_quality": source.get("quality", 0.7),
                "source_verified": False,
                "source_direct": False,
                "retrieval_via": "credentialed_aggregator_api",
                "source_role": source_role(source),
                "source_currencies": list(source.get("currencies") or []),
                "title": title,
                "summary": clean_text(row.get("summary")),
                "url": canonical_url(row.get("url")),
                "published_utc": iso_utc(published) if published else "",
                "external_id": clean_text(row.get("id")),
                "vendor_category": clean_text(row.get("category")),
                "vendor_related": clean_text(row.get("related")),
                # Keep credentialed aggregator observations bound to the exact
                # configured source contract just like every other parser.
                # The source remains unverified, indirect and research-only;
                # this is lineage metadata, not an execution or trust upgrade.
                "source_contract_id": clean_text(source.get("source_contract_id")),
                "source_cohort_id": clean_text(source.get("source_cohort_id")),
            }
        )
    return output


class _LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.current_href = ""
        self.current_text: list[str] = []
        self.links: list[tuple[str, str]] = []
        self.embedded_links: list[tuple[str, str]] = []
        self.page_view_data = ""

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        values = {key.lower(): value or "" for key, value in attrs}
        # Some official statistical sites render their listing entirely from
        # JSON embedded in a data attribute.  Preserve that first-party
        # payload so the same low-rate HTML source can still be monitored
        # without a browser or an unofficial aggregator.
        if values.get("id") == "pageViewData" and values.get("data-value"):
            self.page_view_data = values["data-value"]
        # A small number of first-party publishers render article cards as
        # custom elements instead of anchors.  Danmarks Nationalbank, for
        # example, exposes the canonical URL and title in a JSON ``link``
        # attribute.  Treat that publisher-provided structure exactly like an
        # anchor while retaining the same URL allow-list checks downstream.
        embedded_link = values.get("link") or values.get("data-link")
        if embedded_link:
            try:
                decoded_link = json.loads(html.unescape(embedded_link))
            except (TypeError, ValueError, json.JSONDecodeError):
                decoded_link = {}
            if isinstance(decoded_link, Mapping):
                embedded_url = clean_text(
                    decoded_link.get("url") or decoded_link.get("href")
                )
                embedded_title = clean_text(
                    decoded_link.get("text") or decoded_link.get("title")
                )
                if embedded_url and embedded_title:
                    self.embedded_links.append((embedded_url, embedded_title))
        if tag.lower() != "a":
            return
        self.current_href = values.get("href", "")
        self.current_text = []

    def handle_data(self, data: str) -> None:
        if self.current_href:
            self.current_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or not self.current_href:
            return
        self.links.append((self.current_href, clean_text(" ".join(self.current_text))))
        self.current_href = ""
        self.current_text = []


def parse_html_links(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    parser = _LinkParser()
    parser.feed(payload.decode(str(source.get("encoding") or "utf-8"), errors="replace"))
    base_url = str(source.get("url") or "")
    include = [
        re.compile(str(pattern), flags=re.I)
        for pattern in source.get("link_patterns") or ()
    ]
    exclude = [
        re.compile(str(pattern), flags=re.I)
        for pattern in source.get("exclude_title_patterns") or ()
    ]
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    maximum = max(1, int(safe_float(source.get("max_items"), 100)))

    candidates = list(parser.links) + list(parser.embedded_links)
    if parser.page_view_data:
        try:
            page_data = json.loads(parser.page_view_data)
        except (TypeError, ValueError, json.JSONDecodeError):
            page_data = {}
        pages = (
            page_data.get("PaginatedBlockPages")
            if isinstance(page_data, Mapping)
            else []
        )
        for page in pages or []:
            if not isinstance(page, Mapping):
                continue
            candidates.append(
                (
                    clean_text(page.get("PageLink") or page.get("Link")),
                    clean_text(page.get("Title")),
                )
            )

    for href, title in candidates:
        url = canonical_url(urllib.parse.urljoin(base_url, href))
        if (
            not title
            or url in seen
            or (include and not any(pattern.search(url) for pattern in include))
            or any(pattern.search(title) for pattern in exclude)
            or not trusted_host(url, source.get("trusted_domains") or ())
        ):
            continue
        seen.add(url)
        diagnostic_release = parse_datetime(
            (source.get("diagnostic_release_utc_by_url") or {}).get(url)
        )
        output.append(
            {
                "source_id": source.get("source_id"),
                "source_name": source.get("name"),
                "source_kind": "html_links",
                "source_quality": source.get("quality", 0.9),
                "source_verified": bool(source.get("verified")),
                "source_direct": configured_source_is_direct(source),
                "retrieval_via": str(source.get("retrieval_via") or "direct"),
                "source_role": source_role(source),
                "source_contract_id": clean_text(source.get("source_contract_id")),
                "source_cohort_id": clean_text(source.get("source_cohort_id")),
                "numeric_parser_activated_utc": clean_text(
                    source.get("numeric_parser_activated_utc")
                ),
                "numeric_extraction_contract_id": clean_text(
                    source.get("numeric_extraction_contract_id")
                ),
                "source_currencies": list(source.get("currencies") or []),
                "title": title,
                "summary": "",
                "url": url,
                # Listing pages frequently omit machine-readable timestamps.
                # first_seen remains the causal timestamp in that case. A
                # source-contract-bound diagnostic clock can repair event
                # alignment after an outage, but is explicitly non-forward.
                "published_utc": (
                    iso_utc(diagnostic_release) if diagnostic_release else ""
                ),
                "published_time_inferred": diagnostic_release is None,
                "source_native_published_utc": (
                    iso_utc(diagnostic_release) if diagnostic_release else ""
                ),
                "publication_clock_basis": (
                    "source_contract_diagnostic_official_release_clock"
                    if diagnostic_release else ""
                ),
                "publication_clock_contract_id": (
                    clean_text(source.get("release_clock_contract_id"))
                    if diagnostic_release else ""
                ),
                "publication_clock_known_utc": (
                    clean_text(source.get("release_clock_contract_activated_utc"))
                    if diagnostic_release else ""
                ),
                "publication_clock_diagnostic_only": bool(diagnostic_release),
                # The pre-repair retrieval-clock row remains immutable. This
                # exact-clock diagnostic is a separately versioned source
                # observation and must not be folded into the old stable URL.
                "immutable_source_version_boundary": bool(diagnostic_release),
                "external_id": "",
            }
        )
        if len(output) >= maximum:
            break
    return output


def parse_singstat_table(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse one frozen SingStat series as a prospective singleton release.

    The API exposes the latest revised table, not a historical publication
    clock.  Collector first-seen is therefore the default causal boundary.
    A frozen period-specific clock may be used only when its provenance is
    recorded in the source contract; adding it changes the immutable identity.
    """
    parsed = json.loads(payload.decode("utf-8-sig"))
    data = parsed.get("Data") if isinstance(parsed, Mapping) else None
    if not isinstance(data, Mapping):
        raise ValueError("SingStat payload has no Data object")
    wanted = clean_text(source.get("row_text"))
    rows = [row for row in data.get("row") or [] if isinstance(row, Mapping)]
    selected = next(
        (row for row in rows if clean_text(row.get("rowText")) == wanted),
        rows[0] if rows and not wanted else None,
    )
    if not isinstance(selected, Mapping):
        raise ValueError("SingStat configured row was not found")
    observations = [
        (clean_text(row.get("key")), optional_float(row.get("value")))
        for row in selected.get("columns") or []
        if isinstance(row, Mapping)
    ]
    observations = [(period, value) for period, value in observations if period and value is not None]
    if not observations:
        raise ValueError("SingStat row has no numeric observations")
    period, actual = observations[-1]
    previous = observations[-2][1] if len(observations) >= 2 else None
    resource_id = clean_text(source.get("resource_id") or data.get("id"))
    series_id = clean_text(source.get("event_series_id"))
    native_release = parse_datetime(
        (source.get("release_utc_by_reference") or {}).get(period)
    )
    release_identity = iso_utc(native_release) if native_release else "first_seen"
    external_id = (
        f"singstat:{resource_id}:{series_id}:{period}:{release_identity}"
    )
    return [
        {
            "source_id": source.get("source_id"),
            "source_name": source.get("name"),
            "source_kind": "singstat_table",
            "source_quality": source.get("quality", 1.0),
            "source_verified": bool(source.get("verified")),
            "source_direct": configured_source_is_direct(source),
            "retrieval_via": str(source.get("retrieval_via") or "official_source_native_json_api"),
            "source_role": source_role(source),
            "source_contract_id": clean_text(source.get("source_contract_id")),
            "source_cohort_id": clean_text(source.get("source_cohort_id")),
            "numeric_parser_activated_utc": clean_text(source.get("numeric_parser_activated_utc")),
            "numeric_extraction_contract_id": clean_text(source.get("numeric_extraction_contract_id")),
            "source_currencies": list(source.get("currencies") or ["SGD"]),
            "title": clean_text(data.get("title")) or clean_text(source.get("event_name")),
            "summary": f"Official SingStat {clean_text(selected.get('rowText'))}: {actual:.12g} {clean_text(selected.get('uoM'))} for {period}.",
            "url": canonical_url(source.get("url")),
            "published_utc": iso_utc(native_release) if native_release else "",
            "published_time_inferred": native_release is None,
            "external_id": external_id,
            "structured_event": True,
            "event_series_id": series_id,
            "event_name": clean_text(source.get("event_name")),
            "event_country": clean_text(source.get("event_country")) or "Singapore",
            "reference_period": period,
            "timing_precision": (
                clean_text(source.get("release_clock_precision"))
                or "verified_exact_release_clock"
                if native_release
                else "collector_first_seen"
            ),
            "unit": clean_text(source.get("unit")),
            "actual": f"{actual:.12g}",
            "actual_value": actual,
            "previous": "" if previous is None else f"{previous:.12g}",
            "previous_value": previous,
            "source_native_update_date": clean_text(data.get("dataLastUpdated")),
            "source_native_components": {
                "configured_row": {"actual": actual, "previous": previous}
            },
            "numeric_direction_policy": "abstain_and_learn_response",
            "directional_research_only": True,
        }
    ]


def _official_numeric_table_article(
    source: Mapping[str, Any],
    *,
    source_kind: str,
    provider_prefix: str,
    period: str,
    actual: float,
    previous: float | None,
    title: str,
    summary: str,
    native_update: str = "",
    components: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one direction-neutral, first-seen-bounded numeric release row."""

    series_id = clean_text(source.get("event_series_id"))
    table_id = clean_text(source.get("table_id") or source.get("resource_id"))
    native_release = parse_datetime(
        (source.get("release_utc_by_reference") or {}).get(period)
    )
    release_identity = iso_utc(native_release) if native_release else "first_seen"
    return {
        "source_id": source.get("source_id"),
        "source_name": source.get("name"),
        "source_kind": source_kind,
        "source_quality": source.get("quality", 1.0),
        "source_verified": bool(source.get("verified")),
        "source_direct": configured_source_is_direct(source),
        "retrieval_via": str(
            source.get("retrieval_via") or "official_source_native_api"
        ),
        "source_role": source_role(source),
        "source_contract_id": clean_text(source.get("source_contract_id")),
        "source_cohort_id": clean_text(source.get("source_cohort_id")),
        "numeric_parser_activated_utc": clean_text(
            source.get("numeric_parser_activated_utc")
        ),
        "numeric_extraction_contract_id": clean_text(
            source.get("numeric_extraction_contract_id")
        ),
        "source_currencies": list(source.get("currencies") or []),
        "title": clean_text(title) or clean_text(source.get("event_name")),
        "summary": clean_text(summary),
        "url": canonical_url(source.get("url")),
        # Table APIs expose the latest revised snapshot.  Use an earlier clock
        # only when the frozen, verified source contract contains the exact
        # official release timestamp for this reference period.  Including
        # that clock in the identity creates a new immutable observation when
        # a previously first-seen-only record is later repaired.
        "published_utc": iso_utc(native_release) if native_release else "",
        "published_time_inferred": native_release is None,
        "external_id": (
            f"{provider_prefix}:{table_id}:{series_id}:{period}:{release_identity}"
        ),
        "structured_event": True,
        "event_series_id": series_id,
        "event_name": clean_text(source.get("event_name")),
        "event_country": clean_text(source.get("event_country")),
        "reference_period": period,
        "timing_precision": (
            "official_exact_schedule" if native_release else "collector_first_seen"
        ),
        "unit": clean_text(source.get("unit")),
        "actual": f"{actual:.12g}",
        "actual_value": actual,
        "previous": "" if previous is None else f"{previous:.12g}",
        "previous_value": previous,
        "consensus": "",
        "consensus_value": None,
        "source_native_update_date": clean_text(native_update),
        "source_native_components": dict(components or {}),
        "numeric_direction_policy": "abstain_and_learn_response",
        "directional_research_only": True,
    }


def parse_ssb_jsonstat2_cpi_yoy(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse the frozen total-CPI 12-month rate from SSB JSON-stat2."""

    parsed = json.loads(payload.decode("utf-8-sig"))
    if not isinstance(parsed, Mapping) or parsed.get("class") != "dataset":
        raise ValueError("SSB payload is not a JSON-stat2 dataset")
    dimensions = parsed.get("dimension")
    dimension_ids = [clean_text(value) for value in parsed.get("id") or []]
    sizes = [int(value) for value in parsed.get("size") or []]
    if not isinstance(dimensions, Mapping) or len(dimension_ids) != len(sizes):
        raise ValueError("SSB JSON-stat2 dimensions are malformed")
    time_id = clean_text(source.get("time_dimension_id") or "Tid")
    series_id = clean_text(source.get("series_dimension_id") or "ContentsCode")
    expected_series = clean_text(source.get("series_code"))
    if time_id not in dimension_ids or series_id not in dimension_ids:
        raise ValueError("SSB configured dimensions are absent")
    for dimension_id, size in zip(dimension_ids, sizes):
        if dimension_id != time_id and size != 1:
            raise ValueError("SSB query did not reduce non-time dimensions")
    series_dimension = dimensions.get(series_id) or {}
    series_index = ((series_dimension.get("category") or {}).get("index") or {})
    if expected_series and expected_series not in series_index:
        raise ValueError("SSB configured CPI series is absent")
    time_dimension = dimensions.get(time_id) or {}
    time_index = ((time_dimension.get("category") or {}).get("index") or {})
    if not isinstance(time_index, Mapping):
        raise ValueError("SSB time index is absent")
    ordered_periods = [
        clean_text(item[0])
        for item in sorted(time_index.items(), key=lambda item: int(item[1]))
    ]
    values = [optional_float(value) for value in parsed.get("value") or []]
    if len(ordered_periods) < 2 or len(values) != len(ordered_periods):
        raise ValueError("SSB CPI query must contain at least two periods")
    observations = [
        (period, value)
        for period, value in zip(ordered_periods, values)
        if period and value is not None
    ]
    if len(observations) < 2:
        raise ValueError("SSB CPI query has fewer than two numeric observations")
    period, actual = observations[-1]
    previous = observations[-2][1]
    return [
        _official_numeric_table_article(
            source,
            source_kind="ssb_jsonstat2_cpi_yoy",
            provider_prefix="ssb",
            period=period,
            actual=actual,
            previous=previous,
            title=clean_text(parsed.get("label")),
            summary=(
                f"Official Statistics Norway total CPI 12-month rate: "
                f"{actual:.12g}% for {period}."
            ),
            native_update=clean_text(parsed.get("updated")),
            components={
                expected_series or "configured_series": {
                    "actual": actual,
                    "previous": previous,
                }
            },
        )
    ]


def parse_denmark_statbank_cpi_yoy(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse total annual CPI change from the official StatBank CSV API."""

    rows = list(
        csv.DictReader(
            io.StringIO(payload.decode("utf-8-sig", errors="strict")),
            delimiter=";",
        )
    )
    period_column = clean_text(source.get("period_column") or "TID")
    value_column = clean_text(source.get("value_column") or "INDHOLD")
    observations: list[tuple[str, float]] = []
    for row in rows:
        period_field = clean_text(row.get(period_column))
        match = re.search(r"\b(\d{4}M\d{2})\b", period_field)
        value = optional_float(
            clean_text(row.get(value_column)).replace(".", "").replace(",", ".")
        )
        if match and value is not None:
            observations.append((match.group(1), value))
    observations.sort(key=lambda item: item[0])
    if len(observations) < 2:
        raise ValueError("StatBank CPI query has fewer than two numeric periods")
    period, actual = observations[-1]
    previous = observations[-2][1]
    return [
        _official_numeric_table_article(
            source,
            source_kind="denmark_statbank_cpi_yoy",
            provider_prefix="statbank_dk",
            period=period,
            actual=actual,
            previous=previous,
            title=clean_text(source.get("event_name")),
            summary=(
                f"Official Statistics Denmark total CPI annual change: "
                f"{actual:.12g}% for {period}."
            ),
            components={
                "configured_total_cpi_yoy": {
                    "actual": actual,
                    "previous": previous,
                }
            },
        )
    ]


def parse_hong_kong_censtatd_cpi_yoy(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse Composite CPI annual change from the official C&SD API."""

    parsed = json.loads(payload.decode("utf-8-sig"))
    header = parsed.get("header") if isinstance(parsed, Mapping) else None
    status = header.get("status") if isinstance(header, Mapping) else None
    status_code = status.get("code") if isinstance(status, Mapping) else None
    if not isinstance(status, Mapping) or int(
        status_code if status_code is not None else -1
    ) != 0:
        raise ValueError("Hong Kong C&SD API did not return success")
    wanted_series = clean_text(source.get("series_code"))
    wanted_description = clean_text(source.get("series_description")).lower()
    observations: list[tuple[str, float]] = []
    for row in parsed.get("dataSet") or []:
        if not isinstance(row, Mapping):
            continue
        if wanted_series and clean_text(row.get("sv")) != wanted_series:
            continue
        if wanted_description and clean_text(row.get("svDesc")).lower() != wanted_description:
            continue
        period = clean_text(row.get("period"))
        value = optional_float(row.get("figure"))
        if re.fullmatch(r"\d{6}", period) and value is not None:
            observations.append((period, value))
    observations.sort(key=lambda item: item[0])
    if len(observations) < 2:
        raise ValueError("Hong Kong Composite CPI series has fewer than two periods")
    period, actual = observations[-1]
    previous = observations[-2][1]
    return [
        _official_numeric_table_article(
            source,
            source_kind="hong_kong_censtatd_cpi_yoy",
            provider_prefix="hk_censtatd",
            period=period,
            actual=actual,
            previous=previous,
            title=clean_text(header.get("title")),
            summary=(
                f"Official Hong Kong Composite CPI year-on-year change: "
                f"{actual:.12g}% for {period}."
            ),
            components={
                wanted_series or "configured_composite_cpi_yoy": {
                    "actual": actual,
                    "previous": previous,
                }
            },
        )
    ]


def parse_bot_sdds_cpi_index(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse Thailand's headline CPI row from the official BOT SDDS table.

    The SDDS page is a current official snapshot rather than a release-time
    API.  Consequently the observation is bounded by collector first-seen
    time and remains direction-neutral.  Cell positions are frozen by this
    contract and verified against the configured row label.
    """

    document = payload.decode(
        str(source.get("encoding") or "utf-8"), errors="replace"
    )
    row_label = clean_text(source.get("row_label") or "Consumer price index")
    row_html = next(
        (
            fragment
            for fragment in re.findall(
                r"<tr\b[^>]*>.*?</tr>", document, flags=re.I | re.S
            )
            if re.search(re.escape(row_label), fragment, flags=re.I)
        ),
        None,
    )
    if row_html is None:
        raise ValueError(f"BOT SDDS row not found: {row_label}")
    cells = [
        clean_text(
            html.unescape(re.sub(r"<[^>]+>", " ", fragment, flags=re.S))
        )
        for fragment in re.findall(
            r"<td\b[^>]*>(.*?)</td>", row_html, flags=re.I | re.S
        )
    ]
    if len(cells) < 6 or cells[0].lower() != row_label.lower():
        raise ValueError("BOT SDDS CPI row does not match the frozen cell contract")
    period_match = re.fullmatch(r"([A-Z][a-z]{2})/(\d{2})", cells[2])
    if period_match is None:
        raise ValueError(f"BOT SDDS CPI period is malformed: {cells[2]}")
    try:
        period_date = dt.datetime.strptime(
            f"{period_match.group(1)} {period_match.group(2)}", "%b %y"
        )
    except ValueError as exc:
        raise ValueError(f"BOT SDDS CPI period is invalid: {cells[2]}") from exc
    actual = optional_float(cells[3])
    previous = optional_float(cells[5])
    if actual is None or previous is None:
        raise ValueError("BOT SDDS CPI values are missing or non-numeric")
    period = period_date.strftime("%YM%m")
    return [
        _official_numeric_table_article(
            source,
            source_kind="bot_sdds_cpi_index",
            provider_prefix="bot_sdds",
            period=period,
            actual=actual,
            previous=previous,
            title=clean_text(source.get("event_name")),
            summary=(
                f"Official Bank of Thailand SDDS headline CPI index: "
                f"{actual:.12g} for {period}."
            ),
            components={
                "headline_cpi_index": {
                    "actual": actual,
                    "previous": previous,
                    "source_period_label": cells[2],
                    "status": cells[4],
                    "previous_status": cells[6] if len(cells) > 6 else "",
                }
            },
        )
    ]


def parse_banxico_inflation_snapshot(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse Banco de Mexico's current SIE headline-inflation snapshot."""

    document = payload.decode(
        str(source.get("encoding") or "iso-8859-1"), errors="replace"
    )

    def selected_value(select_id: str) -> str:
        select_match = re.search(
            rf"<select\b[^>]*\bid=[\"']{re.escape(select_id)}[\"'][^>]*>"
            rf"(.*?)</select>",
            document,
            flags=re.I | re.S,
        )
        if select_match is None:
            raise ValueError(f"Banxico inflation selector missing: {select_id}")
        option = re.search(
            r"<option\b(?=[^>]*\bselected\b)[^>]*\bvalue=[\"']?([^\"' >]+)",
            select_match.group(1),
            flags=re.I | re.S,
        )
        if option is None:
            raise ValueError(f"Banxico selected option missing: {select_id}")
        return clean_text(option.group(1))

    month = int(selected_value("selectMeses"))
    year = int(selected_value("selectAnios"))
    if not 1 <= month <= 12 or not 1969 <= year <= 2200:
        raise ValueError("Banxico inflation reference period is invalid")

    def value_by_id(cell_id: str) -> float:
        match = re.search(
            rf"<td\b[^>]*\bid=[\"']{re.escape(cell_id)}[\"'][^>]*>"
            rf"\s*([-+]?\d+(?:\.\d+)?)\s*</td>",
            document,
            flags=re.I | re.S,
        )
        value = optional_float(match.group(1)) if match else None
        if value is None:
            raise ValueError(f"Banxico inflation value missing: {cell_id}")
        return value

    monthly = value_by_id("tdSP30577")
    accumulated = value_by_id("tdSP30579")
    annual = value_by_id("tdSP30578")
    period = f"{year:04d}M{month:02d}"
    return [
        _official_numeric_table_article(
            source,
            source_kind="banxico_inflation_snapshot",
            provider_prefix="banxico_sie",
            period=period,
            actual=annual,
            previous=None,
            title=clean_text(source.get("event_name")),
            summary=(
                f"Official Banco de Mexico SIE headline annual CPI inflation: "
                f"{annual:.12g}% for {period}."
            ),
            components={
                "headline_cpi_monthly": {"actual": monthly},
                "headline_cpi_year_to_date": {"actual": accumulated},
                "headline_cpi_annual": {"actual": annual},
            },
        )
    ]


def parse_sarb_homepage_rates(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse one frozen SARB homepage-indicator series from its public API."""

    parsed = json.loads(payload.decode("utf-8-sig"))
    if not isinstance(parsed, list):
        raise ValueError("SARB homepage-rates payload is not a list")
    series_code = clean_text(source.get("series_code"))
    matches = [
        row
        for row in parsed
        if isinstance(row, Mapping)
        and clean_text(row.get("TimeseriesCode")) == series_code
    ]
    if len(matches) != 1:
        raise ValueError(
            f"SARB homepage-rates series {series_code!r} matched {len(matches)} rows"
        )
    selected = matches[0]
    period = clean_text(selected.get("Date"))
    try:
        dt.date.fromisoformat(period)
    except ValueError as exc:
        raise ValueError(f"SARB indicator period is malformed: {period}") from exc
    actual = optional_float(selected.get("Value"))
    if actual is None:
        raise ValueError("SARB indicator value is missing")
    return [
        _official_numeric_table_article(
            source,
            source_kind="sarb_homepage_rates",
            provider_prefix="sarb_webapi",
            period=period,
            actual=actual,
            previous=None,
            title=clean_text(source.get("event_name")),
            summary=(
                f"Official South African Reserve Bank {clean_text(selected.get('Name'))}: "
                f"{actual:.12g} for {period}."
            ),
            components={
                series_code: {
                    "actual": actual,
                    "name": clean_text(selected.get("Name")),
                    "section": clean_text(selected.get("SectionName")),
                    "movement": selected.get("UpDown"),
                }
            },
        )
    ]


def parse_cnb_homepage_inflation(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse the current CZSO/ARAD inflation snapshot published by CNB."""

    document = payload.decode(
        str(source.get("encoding") or "utf-8"), errors="replace"
    )
    block = re.search(
        r"<h2>\s*Inflation\s*</h2>(.*?)More about inflation",
        document,
        flags=re.I | re.S,
    )
    if block is None:
        raise ValueError("CNB homepage inflation block is missing")
    value_match = re.search(
        r"inflationBox-graph[^>]*\bdata-value=[\"']([-+]?\d+(?:\.\d+)?)[\"']",
        block.group(1),
        flags=re.I | re.S,
    )
    period_match = re.search(
        r"<p>\s*([A-Z][a-z]+)\s+(\d{4})\s*</p>",
        block.group(1),
        flags=re.I | re.S,
    )
    actual = optional_float(value_match.group(1)) if value_match else None
    if actual is None or period_match is None:
        raise ValueError("CNB homepage inflation value or period is malformed")
    try:
        period_date = dt.datetime.strptime(
            f"{period_match.group(1)} {period_match.group(2)}", "%B %Y"
        )
    except ValueError as exc:
        raise ValueError("CNB homepage inflation period is invalid") from exc
    period = period_date.strftime("%YM%m")
    return [
        _official_numeric_table_article(
            source,
            source_kind="cnb_homepage_inflation",
            provider_prefix="cnb_arad",
            period=period,
            actual=actual,
            previous=None,
            title=clean_text(source.get("event_name")),
            summary=(
                f"Official Czech National Bank/CZSO annual inflation snapshot: "
                f"{actual:.12g}% for {period}."
            ),
            components={"headline_cpi_yoy": {"actual": actual}},
        )
    ]


def parse_ksh_prices_snapshot(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse Hungary's current headline CPI snapshot from the official KSH page.

    The page is a continuously updated statistical topic page, not a release
    timestamp API.  Its value is therefore bounded by collector first-seen
    time and remains direction-neutral for prospective research.
    """

    document = payload.decode(
        str(source.get("encoding") or "utf-8"), errors="replace"
    )
    block = re.search(
        r'<div\b[^>]*class=["\'][^"\']*key-feature-value[^"\']*["\'][^>]*>'
        r'(.*?)</section>',
        document,
        flags=re.I | re.S,
    )
    if block is None:
        # KSH has changed the wrapper class before.  Keep a narrow fallback
        # anchored to the exact published indicator name and reference period.
        block = re.search(
            r'(<div\b[^>]*class=["\'][^"\']*value[^"\']*["\'][^>]*>.*?'
            r'Change in consumer prices.*?Last data for period:.*?</div>)',
            document,
            flags=re.I | re.S,
        )
    if block is None:
        raise ValueError("KSH consumer-price snapshot block is missing")
    value_match = re.search(
        r'<div\b[^>]*class=["\'][^"\']*value[^"\']*["\'][^>]*>\s*'
        r'([-+]?\d+(?:\.\d+)?)\s*<span>\s*%\s*</span>',
        block.group(1),
        flags=re.I | re.S,
    )
    label_match = re.search(
        r'<h3\b[^>]*class=["\'][^"\']*title[^"\']*["\'][^>]*>\s*'
        r'Change in consumer prices\s*</h3>',
        block.group(1),
        flags=re.I | re.S,
    )
    period_match = re.search(
        r'(?:Last data for period:\s*|class=["\'][^"\']*ref-time(?:-sm)?[^"\']*["\'][^>]*>\s*)'
        r'([A-Z][a-z]+)\s+(\d{4})',
        block.group(1),
        flags=re.I | re.S,
    )
    actual = optional_float(value_match.group(1)) if value_match else None
    if actual is None or label_match is None or period_match is None:
        raise ValueError("KSH consumer-price snapshot value, label, or period is malformed")
    try:
        period_date = dt.datetime.strptime(
            f"{period_match.group(1)} {period_match.group(2)}", "%B %Y"
        )
    except ValueError as exc:
        raise ValueError("KSH consumer-price snapshot period is invalid") from exc
    period = period_date.strftime("%YM%m")
    return [
        _official_numeric_table_article(
            source,
            source_kind="ksh_prices_snapshot",
            provider_prefix="ksh_prices",
            period=period,
            actual=actual,
            previous=None,
            title=clean_text(source.get("event_name")),
            summary=(
                f"Official Hungarian Central Statistical Office annual headline "
                f"consumer-price change: {actual:.12g}% for {period}."
            ),
            components={"headline_cpi_yoy": {"actual": actual}},
        )
    ]


def _ksh_ppi_history_values(
    payload: bytes,
    *,
    current_period: dt.datetime,
    encoding: str = "cp1252",
) -> tuple[float, float]:
    """Return current/prior headline PPI y/y changes from KSH STADAT.

    The official table contains several stacked index panels.  Select only the
    explicitly labelled ``Corresponding period of the previous year`` panel
    and its exact ``Total industry B+C+D+E`` column.  This avoids accidentally
    treating a level or month-on-month panel as the annual change.
    """

    try:
        rows = list(
            csv.reader(
                io.StringIO(payload.decode(encoding, errors="strict")),
                delimiter=";",
            )
        )
    except (LookupError, UnicodeError, csv.Error) as exc:
        raise ValueError("KSH PPI history CSV is malformed") from exc
    if len(rows) < 4:
        raise ValueError("KSH PPI history CSV is empty")
    header = rows[1]
    if not header or clean_text(header[-1]) != "Total industry B+C+D+E":
        raise ValueError("KSH PPI total-industry column is missing")
    panel_index = next(
        (
            index
            for index, row in enumerate(rows)
            if row
            and clean_text(row[0]).casefold().startswith(
                "corresponding period of the previous year"
            )
        ),
        None,
    )
    if panel_index is None:
        raise ValueError("KSH PPI annual-change panel is missing")
    values: dict[tuple[int, int], float] = {}
    table_year: int | None = None
    for row in rows[panel_index + 1 :]:
        if not row:
            continue
        first = clean_text(row[0])
        if first and not re.fullmatch(r"20\d{2}", first):
            break
        if first:
            table_year = int(first)
        month = clean_text(row[1] if len(row) > 1 else "")
        if table_year is None or not month:
            continue
        try:
            month_number = dt.datetime.strptime(month, "%B").month
        except ValueError:
            continue
        raw_value = clean_text(row[-1]).replace(",", ".")
        value = optional_float(raw_value)
        if value is not None:
            values[(table_year, month_number)] = round(value - 100.0, 12)
    previous_period = current_period.replace(day=1) - dt.timedelta(days=1)
    current_value = values.get((current_period.year, current_period.month))
    previous_value = values.get((previous_period.year, previous_period.month))
    if current_value is None or previous_value is None:
        raise ValueError("KSH PPI current/prior annual changes are missing")
    return current_value, previous_value


def parse_ksh_ppi_release_snapshot(
    payload: bytes,
    history_payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse the current KSH PPI release from stable first-party surfaces.

    KSH's topic page exposes the current release summary, component changes,
    and exact publication date.  Its STADAT CSV supplies an independently
    checkable current/prior annual series.  The source remains direction-neutral
    because neither surface supplies a causally captured market consensus.
    """

    encoding = clean_text(source.get("encoding")) or "iso-8859-2"
    try:
        parser = _OfficialDocumentTextParser()
        parser.feed(payload.decode(encoding, errors="strict"))
        text = clean_text(parser.text())
    except (LookupError, UnicodeError) as exc:
        raise ValueError("KSH PPI topic page encoding is invalid") from exc
    period_match = re.search(
        r"\bIndustrial producer prices,\s*(?P<month>[A-Z][a-z]+)\s+"
        r"(?P<year>20\d{2})\b",
        text,
    )
    headline_match = re.search(
        r"\bIndustrial producer prices were\s+(?:an average\s+)?"
        r"(?P<value>\d+(?:\.\d+)?)%\s+(?P<direction>higher|lower)\b",
        text,
        flags=re.I,
    )
    monthly_match = re.search(
        r"\bCompared to (?:the )?previous month,.*?"
        r"industrial producer prices as a whole\s+"
        r"(?:became|were|increased|decreased)\s+"
        r"(?P<value>\d+(?:\.\d+)?)%\s+(?P<direction>higher|lower)\b",
        text,
        flags=re.I,
    )
    annual_components = re.search(
        r"\bDomestic output prices\s+"
        r"(?P<domestic_verb>rose|increased|were up|went up|fell|decreased|"
        r"lessened|were cut|diminished)\s+(?:by\s+)?"
        r"(?P<domestic>\d+(?:\.\d+)?)%.*?"
        r"non-domestic(?: output prices| ones)?\s+"
        r"(?:(?P<nondomestic_verb>rose|increased|were up|went up|fell|"
        r"decreased|lessened|were cut|diminished)\s+)?(?:by\s+)?"
        r"(?P<nondomestic>\d+(?:\.\d+)?)%\s+compared to",
        text,
        flags=re.I,
    )
    monthly_components = re.search(
        r"\bCompared to (?:the )?previous month,\s+domestic output prices\s+"
        r"(?P<domestic_verb>went up|rose|increased|were up|fell|decreased|"
        r"lessened|were cut|diminished)\s+(?:by\s+)?"
        r"(?P<domestic>\d+(?:\.\d+)?)%.*?"
        r"non-domestic output prices\s+"
        r"(?:(?P<nondomestic_verb>went up|rose|increased|were up|fell|"
        r"decreased|lessened|were cut|diminished)\s+)?(?:by\s+)?"
        r"(?P<nondomestic>\d+(?:\.\d+)?)%",
        text,
        flags=re.I,
    )
    if not all(
        (period_match, headline_match, monthly_match, annual_components, monthly_components)
    ):
        raise ValueError("KSH PPI release summary is incomplete")
    try:
        reference_date = dt.datetime.strptime(
            f"{period_match.group('month')} {period_match.group('year')}",
            "%B %Y",
        )
    except ValueError as exc:
        raise ValueError("KSH PPI reference period is invalid") from exc
    release_match = re.search(
        rf"\bIndustrial producer prices,\s*{re.escape(period_match.group('month'))}"
        rf"\s+{period_match.group('year')}\s+"
        r"(?P<released>\d{2}/\d{2}/20\d{2})\s+"
        r"(?P<next>\d{2}/\d{2}/20\d{2})\b",
        text,
        flags=re.I,
    )
    if release_match is None:
        raise ValueError("KSH PPI exact publication date is missing")
    try:
        release_date = dt.datetime.strptime(
            release_match.group("released"), "%d/%m/%Y"
        ).date()
        release_clock = dt.datetime.combine(
            release_date,
            dt.time(8, 30, tzinfo=ZoneInfo("Europe/Budapest")),
        ).astimezone(UTC)
    except ValueError as exc:
        raise ValueError("KSH PPI exact publication date is invalid") from exc

    def signed(value: str, direction: str) -> float:
        parsed = optional_float(value)
        if parsed is None:
            raise ValueError("KSH PPI component value is invalid")
        if clean_text(direction).casefold() in {
            "lower", "fell", "decreased", "lessened", "were cut", "diminished"
        }:
            return -abs(parsed)
        return abs(parsed)

    annual_value = signed(
        headline_match.group("value"), headline_match.group("direction")
    )
    monthly_value = signed(
        monthly_match.group("value"), monthly_match.group("direction")
    )
    history_current, previous_value = _ksh_ppi_history_values(
        history_payload,
        current_period=reference_date,
        encoding=clean_text(source.get("history_encoding")) or "cp1252",
    )
    if abs(history_current - annual_value) > 0.05:
        raise ValueError("KSH PPI release and STADAT headline values disagree")
    annual_domestic = signed(
        annual_components.group("domestic"), annual_components.group("domestic_verb")
    )
    annual_nondomestic = signed(
        annual_components.group("nondomestic"),
        annual_components.group("nondomestic_verb")
        or annual_components.group("domestic_verb"),
    )
    monthly_domestic = signed(
        monthly_components.group("domestic"),
        monthly_components.group("domestic_verb"),
    )
    monthly_nondomestic = signed(
        monthly_components.group("nondomestic"),
        monthly_components.group("nondomestic_verb")
        or monthly_components.group("domestic_verb"),
    )
    components = {
        "headline_ppi_yoy": {
            "actual": annual_value,
            "previous": previous_value,
            "unit": "year_percent_change",
        },
        "headline_ppi_mom": {
            "actual": monthly_value,
            "unit": "month_percent_change",
        },
        "domestic_output_ppi_yoy": {
            "actual": annual_domestic,
            "unit": "year_percent_change",
        },
        "non_domestic_output_ppi_yoy": {
            "actual": annual_nondomestic,
            "unit": "year_percent_change",
        },
        "domestic_output_ppi_mom": {
            "actual": monthly_domestic,
            "unit": "month_percent_change",
        },
        "non_domestic_output_ppi_mom": {
            "actual": monthly_nondomestic,
            "unit": "month_percent_change",
        },
    }
    period = reference_date.strftime("%YM%m")
    source_with_clock = dict(source)
    source_with_clock["release_utc_by_reference"] = {
        period: iso_utc(release_clock)
    }
    article = _official_numeric_table_article(
        source_with_clock,
        source_kind="ksh_ppi_release_snapshot",
        provider_prefix="ksh_ppi",
        period=period,
        actual=annual_value,
        previous=previous_value,
        title=clean_text(source.get("event_name")),
        summary=(
            f"Official Hungarian Central Statistical Office industrial producer "
            f"prices: {annual_value:.12g}% y/y and {monthly_value:.12g}% m/m for "
            f"{period}; domestic {annual_domestic:.12g}% y/y and "
            f"{monthly_domestic:.12g}% m/m; non-domestic "
            f"{annual_nondomestic:.12g}% y/y and {monthly_nondomestic:.12g}% m/m."
        ),
        native_update=release_date.isoformat(),
        components=components,
    )
    article.update(
        {
            "scheduled_utc": iso_utc(release_clock),
            "source_reported_update_utc": iso_utc(release_clock),
            "revision_state": "not_reported_by_source",
            "release_components": [
                {"component_id": key, **value} for key, value in components.items()
            ],
            "numeric_direction_policy": (
                "abstain_until_causal_consensus_and_rate_repricing"
            ),
            "history_table_url": canonical_url(source.get("history_table_url")),
        }
    )
    return [article]


class SourceContentPending(ValueError):
    """A valid provider page explicitly lacks the registered final measure."""

    def __init__(self, reason: str, details: Mapping[str, Any]):
        super().__init__(reason)
        self.reason = reason
        self.details = dict(details)


def parse_scb_cpi_snapshot(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse Sweden's latest CPI and target CPIF from Statistics Sweden."""

    text = clean_text(
        payload.decode(str(source.get("encoding") or "utf-8"), errors="replace")
    )
    release = re.search(
        r"The inflation rate according to the CPI in ([A-Z][a-z]+ \d{4}) was "
        r"([-+]?\d+(?:\.\d+)?) percent, (?:up|down) from "
        r"([-+]?\d+(?:\.\d+)?) percent in [A-Z][a-z]+\.\s*"
        r"The monthly change for the CPI from [A-Z][a-z]+ to [A-Z][a-z]+ was "
        r"([-+]?\d+(?:\.\d+)?) percent\.\s*"
        r"The inflation rate according to the CPIF .*? was "
        r"([-+]?\d+(?:\.\d+)?) percent in [A-Z][a-z]+, (?:up|down) from "
        r"([-+]?\d+(?:\.\d+)?) percent in [A-Z][a-z]+\.",
        text,
        flags=re.I,
    )
    if release is None:
        # The product page alternates between final CPI/CPIF releases and a
        # preliminary CPI-only flash. Never substitute that CPI for target CPIF
        # or combine August preliminary CPI with July final key figures.
        flash = re.search(
            r"Flash CPI:\s*Inflation rate [-+]?\d+(?:\.\d+)? percent in "
            r"(?P<period>[A-Z][a-z]+ \d{4}).{0,160}?"
            r"The preliminary CPI inflation rate for (?P=period) was "
            r"[-+]?\d+(?:\.\d+)? percent.{0,500}?"
            r"The regular publication for (?P<month>[A-Z][a-z]+) takes place on "
            r"(?P<release>[A-Z][a-z]+ \d{1,2})\.", text, flags=re.I,
        )
        if flash:
            period_date = dt.datetime.strptime(flash.group("period"), "%B %Y")
            if flash.group("month").casefold() != period_date.strftime("%B").casefold():
                raise ValueError("SCB flash and regular release periods disagree")
            release_date = dt.datetime.strptime(
                flash.group("release") + " " + str(period_date.year), "%B %d %Y"
            )
            if release_date.month < period_date.month:
                release_date = release_date.replace(year=release_date.year + 1)
            if release_date <= period_date:
                raise ValueError("SCB regular release date precedes reference period")
            raise SourceContentPending("awaiting_regular_cpif_release", {
                "reference_period": period_date.strftime("%YM%m"),
                "regular_release_date": release_date.date().isoformat(),
                "source_stage": "preliminary_cpi_only", "numeric_rows_emitted": 0,
            })
        raise ValueError("SCB current CPI/CPIF release summary is missing or malformed")
    try:
        period_date = dt.datetime.strptime(release.group(1), "%B %Y")
    except ValueError as exc:
        raise ValueError("SCB current CPI/CPIF period is invalid") from exc
    cpi_actual = float(release.group(2))
    cpi_previous = float(release.group(3))
    cpi_monthly = float(release.group(4))
    cpif_actual = float(release.group(5))
    cpif_previous = float(release.group(6))
    period = period_date.strftime("%YM%m")
    return [
        _official_numeric_table_article(
            source,
            source_kind="scb_cpi_snapshot",
            provider_prefix="scb_cpi",
            period=period,
            # CPIF is the Riksbank's target variable.  CPI remains preserved
            # as a separate native component rather than silently discarded.
            actual=cpif_actual,
            previous=cpif_previous,
            title=clean_text(source.get("event_name")),
            summary=(
                f"Official Statistics Sweden CPIF annual inflation: "
                f"{cpif_actual:.12g}% for {period}; headline CPI "
                f"{cpi_actual:.12g}%."
            ),
            components={
                "cpif_yoy": {"actual": cpif_actual, "previous": cpif_previous},
                "headline_cpi_yoy": {
                    "actual": cpi_actual,
                    "previous": cpi_previous,
                },
                "headline_cpi_monthly": {"actual": cpi_monthly},
            },
        )
    ]


def parse_tuik_press_indicators(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse the frozen CPI indicator from TurkStat's public portal API."""

    parsed = json.loads(payload.decode("utf-8-sig"))
    if not isinstance(parsed, Mapping) or parsed.get("isError") is not False:
        raise ValueError("TurkStat indicator API did not return success")
    indicator_id = int(safe_float(source.get("indicator_id"), 1.0))
    matches = [
        row
        for row in parsed.get("data") or []
        if isinstance(row, Mapping) and int(safe_float(row.get("id"), -1)) == indicator_id
    ]
    if len(matches) != 1:
        raise ValueError(
            f"TurkStat CPI indicator {indicator_id} matched {len(matches)} rows"
        )
    selected = matches[0]
    period_match = re.fullmatch(r"(\d{4})/(\d{1,2})", clean_text(selected.get("date")))
    actual = optional_float(selected.get("value"))
    if period_match is None or actual is None:
        raise ValueError("TurkStat CPI indicator period or value is malformed")
    period = f"{int(period_match.group(1)):04d}M{int(period_match.group(2)):02d}"
    annual_series: Mapping[str, Any] | None = None
    for graphic in selected.get("graphics") or []:
        if not isinstance(graphic, Mapping):
            continue
        for series in graphic.get("series") or []:
            if (
                isinstance(series, Mapping)
                and clean_text(series.get("title")).lower()
                == "consumer price index - annual"
            ):
                annual_series = series
                break
        if annual_series is not None:
            break
    values = [
        value
        for value in (optional_float(item) for item in (annual_series or {}).get("data") or [])
        if value is not None
    ]
    if len(values) < 2 or not math.isclose(values[-1], actual, abs_tol=1e-9):
        raise ValueError("TurkStat CPI history is missing or disagrees with headline value")
    previous = values[-2]
    press_url = clean_text(selected.get("pressUrl"))
    if not re.fullmatch(r"/en/press/\d+", press_url):
        raise ValueError("TurkStat CPI release link is malformed")
    return [
        _official_numeric_table_article(
            source,
            source_kind="tuik_press_indicators",
            provider_prefix="tuik_portal",
            period=period,
            actual=actual,
            previous=previous,
            title=clean_text(source.get("event_name")),
            summary=(
                f"Official TurkStat annual consumer-price inflation: "
                f"{actual:.12g}% for {period}."
            ),
            components={
                "headline_cpi_yoy": {
                    "actual": actual,
                    "previous": previous,
                    "press_url": urllib.parse.urljoin(source.get("url") or "", press_url),
                    "history_count": len(values),
                }
            },
        )
    ]


def parse_rbnz_ocr_snapshot(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse the current OCR from the Reserve Bank of New Zealand page.

    This is an official mutable snapshot, not a historical release-time API.
    The common numeric article contract therefore leaves ``published_utc``
    blank and bounds knowledge by the collector's first-seen timestamp.
    """

    document = payload.decode(
        str(source.get("encoding") or "utf-8"), errors="replace"
    )
    document = re.sub(
        r"<script\b[^>]*>.*?</script>", " ", document, flags=re.I | re.S
    )
    document = re.sub(
        r"<style\b[^>]*>.*?</style>", " ", document, flags=re.I | re.S
    )
    text = clean_text(html.unescape(re.sub(r"<[^>]+>", " ", document)))
    release = re.search(
        r"Official Cash Rate\s+([-+]?\d+(?:\.\d+)?)\s*%\s*"
        r"Updated:\s*(\d{1,2}:\d{2}\s*[ap]m),\s*"
        r"(\d{1,2}\s+[A-Z][a-z]{2}\s+\d{4})\s*"
        r"Next update:\s*(\d{1,2}:\d{2}\s*[ap]m),\s*"
        r"(\d{1,2}\s+[A-Z][a-z]{2}\s+\d{4})",
        text,
        flags=re.I,
    )
    if release is None:
        raise ValueError("RBNZ OCR value or update clock is missing or malformed")
    actual = optional_float(release.group(1))
    if actual is None:
        raise ValueError("RBNZ OCR value is not numeric")
    try:
        update_date = dt.datetime.strptime(release.group(3), "%d %b %Y").date()
        next_date = dt.datetime.strptime(release.group(5), "%d %b %Y").date()
        update_time = dt.datetime.strptime(
            release.group(2).replace(" ", "").upper(), "%I:%M%p"
        ).time()
        update_local = dt.datetime.combine(
            update_date,
            update_time,
            tzinfo=ZoneInfo(
                clean_text(source.get("event_timezone")) or "Pacific/Auckland"
            ),
        )
    except ValueError as exc:
        raise ValueError("RBNZ OCR update date is invalid") from exc
    period = update_date.isoformat()
    source_with_clock = {
        **source,
        "release_utc_by_reference": {
            period: iso_utc(update_local.astimezone(UTC)),
        },
    }
    return [
        _official_numeric_table_article(
            source_with_clock,
            source_kind="rbnz_ocr_snapshot",
            provider_prefix="rbnz_ocr",
            period=period,
            actual=actual,
            previous=None,
            title=clean_text(source.get("event_name")),
            summary=(
                f"Official Reserve Bank of New Zealand cash rate: "
                f"{actual:.12g}% as updated {release.group(2)}, "
                f"{release.group(3)}."
            ),
            native_update=f"{release.group(2)}, {release.group(3)}",
            components={
                "official_cash_rate": {"actual": actual},
                "next_update": {
                    "local_time": clean_text(release.group(4)),
                    "local_date": next_date.isoformat(),
                    "timezone": clean_text(
                        source.get("event_timezone") or "Pacific/Auckland"
                    ),
                },
            },
        )
    ]


def parse_umich_current_release(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse the official live Surveys of Consumers release page.

    The University operates a lagging data/archive host and a separate live
    public-release host.  This parser targets only the latter and preserves the
    activity and inflation components separately; their currency channels may
    conflict and therefore must not be collapsed into a forced direction.
    """

    text = clean_text(
        payload.decode(str(source.get("encoding") or "utf-8"), errors="replace")
    )
    release = re.search(
        r"\b(Preliminary|Final) Results for ([A-Z][a-z]+ \d{4})\b",
        text,
    )
    sentiment = re.search(
        r"Index of Consumer Sentiment\s+(-?\d+(?:\.\d+)?)\s+"
        r"(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+"
        r"[-+]?\d+(?:\.\d+)?%\s+[-+]?\d+(?:\.\d+)?%",
        text,
        flags=re.I,
    )
    if not release or not sentiment:
        return []
    release_state = release.group(1).lower()
    reference_period = release.group(2)
    native_release = parse_datetime(
        (source.get("release_utc_by_identity") or {}).get(
            f"{release_state}|{reference_period}"
        )
    )

    def three_component(label: str) -> dict[str, float] | None:
        match = re.search(
            rf"{label}\s+(-?\d+(?:\.\d+)?)\s+"
            rf"(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)",
            text,
            flags=re.I,
        )
        if not match:
            return None
        return {
            "actual": float(match.group(1)),
            "previous": float(match.group(2)),
            "year_ago": float(match.group(3)),
        }

    components: dict[str, Any] = {
        "consumer_sentiment": {
            "actual": float(sentiment.group(1)),
            "previous": float(sentiment.group(2)),
            "year_ago": float(sentiment.group(3)),
        },
        "current_economic_conditions": three_component(
            "Current Economic Conditions"
        ),
        "consumer_expectations": three_component(
            "Index of Consumer Expectations"
        ),
    }
    year_ahead = re.search(
        r"Year-ahead inflation expectations\s+.*?from\s+"
        r"(\d+(?:\.\d+)?)%\s+.*?to\s+(\d+(?:\.\d+)?)%",
        text,
        flags=re.I,
    )
    if year_ahead:
        components["year_ahead_inflation_expectations"] = {
            "actual": float(year_ahead.group(2)),
            "previous": float(year_ahead.group(1)),
        }
    long_run = re.search(
        r"Long-run inflation expectations\s+.*?at\s+(\d+(?:\.\d+)?)%",
        text,
        flags=re.I,
    )
    if long_run:
        components["long_run_inflation_expectations"] = {
            "actual": float(long_run.group(1))
        }
    blurb_match = re.search(
        r"Surveys of Consumers Director Joanne Hsu\s+(.*?)\s+Copyright",
        text,
        flags=re.I,
    )
    blurb = clean_text(blurb_match.group(1)) if blurb_match else ""
    title = f"{release.group(1)} Results for {reference_period}"
    actual = components["consumer_sentiment"]["actual"]
    previous = components["consumer_sentiment"]["previous"]
    external_id = hashlib.sha256(
        json.dumps(
            {
                "title": title,
                "components": components,
                # A later exact official-calendar clock is a material identity
                # repair. Including it creates a new immutable observation
                # rather than rewriting the earlier first-seen snapshot.
                "source_native_published_utc": (
                    iso_utc(native_release) if native_release else ""
                ),
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    return [{
        "source_id": source.get("source_id"),
        "source_name": source.get("name"),
        "source_kind": "umich_current_release",
        "source_quality": source.get("quality", 0.98),
        "source_verified": bool(source.get("verified")),
        "source_direct": configured_source_is_direct(source),
        "retrieval_via": str(source.get("retrieval_via") or "direct"),
        "source_role": source_role(source),
        "source_contract_id": clean_text(source.get("source_contract_id")),
        "source_cohort_id": clean_text(source.get("source_cohort_id")),
        "numeric_parser_activated_utc": clean_text(
            source.get("numeric_parser_activated_utc")
        ),
        "numeric_extraction_contract_id": clean_text(
            source.get("numeric_extraction_contract_id")
        ),
        "source_currencies": list(source.get("currencies") or []),
        "title": title,
        "summary": blurb,
        "url": canonical_url(source.get("url")),
        # The mutable page has no embedded machine-readable timestamp. Use an
        # exact frozen official-calendar clock when configured; otherwise
        # retain first_seen as the only causal clock and mark it inferred.
        "published_utc": iso_utc(native_release) if native_release else "",
        "published_time_inferred": native_release is None,
        "external_id": external_id,
        # This page is a mutable release container, not an ordinary article.
        # Version its monthly/preliminary/final observations by their
        # source-native values instead of allowing the stable-URL de-duplicator
        # to collapse successive releases into one record.
        "structured_event": True,
        "event_series_id": f"umich_consumer_sentiment_{release_state}",
        "event_name": f"United States University of Michigan Consumer Sentiment {release.group(1)}",
        "event_country": "United States",
        "reference_period": reference_period,
        "timing_precision": (
            "official_exact_schedule" if native_release else "first_seen_only"
        ),
        "unit": "index_points",
        "actual": str(actual),
        "actual_value": actual,
        "previous": str(previous),
        "previous_value": previous,
        "consensus": "",
        "consensus_value": None,
        "source_native_components": components,
        "directional_research_only": True,
    }]


def parse_bls_current_release(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse a mutable official BLS current-release page prospectively.

    BLS RSS exposes a mutable "latest numbers" container and the public API
    can be quota limited at release time.  A product-specific ``nr0`` page is
    the authoritative initial release, including its embargo timestamp.  The
    page is still a mutable singleton, so the first collected snapshot is
    annotated as bootstrap by :func:`annotate_singleton_release_history`.

    This parser deliberately preserves headline and underlying PPI as two
    source-native observations.  Neither receives a forced USD direction:
    that requires a causally captured pre-release consensus or subsequent
    price confirmation.
    """

    parser = _OfficialDocumentTextParser()
    parser.feed(
        payload.decode(str(source.get("encoding") or "utf-8"), errors="replace")
    )
    text = parser.text()
    release = re.search(
        r"PRODUCER PRICE INDEXES\s*[-–]\s*([A-Z][A-Z]+)\s+(\d{4})",
        text,
        flags=re.I,
    )
    embargo = re.search(
        r"(?:EMBARGOED UNTIL\s*)?8:30\s*a\.m\.\s*\(ET\)\s*"
        r"(?:Monday|Tuesday|Wednesday|Thursday|Friday),\s*"
        r"([A-Z][a-z]+\s+\d{1,2},\s+\d{4})",
        text,
        flags=re.I,
    )
    if not release or not embargo:
        return []

    def signed_change(fragment: str) -> float | None:
        lowered = fragment.lower()
        if "unchanged" in lowered:
            return 0.0
        number = re.search(r"(-?\d+(?:\.\d+)?)\s*percent", lowered)
        if not number:
            return None
        value = abs(float(number.group(1)))
        if re.search(r"\b(?:fell|declined|decreased|dropped|edged down|moved down|inching down)\b", lowered):
            return -value
        if re.search(r"\b(?:rose|advanced|increased|gained|edged up|moved up|inching up)\b", lowered):
            return value
        return float(number.group(1))

    month_name = release.group(1).title()
    year = int(release.group(2))
    try:
        reference_date = dt.datetime.strptime(
            f"{month_name} {year}", "%B %Y"
        ).date()
        release_local = dt.datetime.strptime(
            embargo.group(1), "%B %d, %Y"
        ).replace(hour=8, minute=30, tzinfo=ZoneInfo("America/New_York"))
    except ValueError:
        return []
    reference_period = reference_date.strftime("%Y-%m")
    published_utc = release_local.astimezone(UTC).isoformat()

    headline = re.search(
        r"The Producer Price Index for final demand\s+"
        r"((?:was unchanged)|(?:(?:rose|advanced|increased|gained|fell|declined|"
        r"decreased|dropped|edged up|edged down|moved up|moved down)\s+"
        r"-?\d+(?:\.\d+)?\s*percent))\s+in\s+"
        rf"{re.escape(month_name)}\b(.*?)Prices for final demand less foods, energy, and trade services",
        text,
        flags=re.I,
    )
    underlying = re.search(
        r"Prices for final demand less foods, energy, and trade services\s+"
        r"((?:were unchanged)|(?:(?:rose|advanced|increased|gained|fell|declined|"
        r"decreased|dropped|edged up|edged down|moved up|moved down)\s+"
        r"-?\d+(?:\.\d+)?\s*percent))\s+in\s+"
        rf"{re.escape(month_name)}\b(.*?)(?:Product Detail|Final demand services|Final demand goods)",
        text,
        flags=re.I,
    )
    if not headline or not underlying:
        return []

    def prior_change(section: str) -> float | None:
        match = re.search(
            r"(?:prices|index|final demand|this measure|the measure)\s+"
            r"((?:was unchanged)|(?:were unchanged)|(?:(?:rose|advanced|increased|"
            r"gained|fell|declined|decreased|dropped|edged up|edged down|moved up|"
            r"moved down)\s+-?\d+(?:\.\d+)?\s*percent))\s+in\s+"
            r"[A-Z][a-z]+",
            section,
            flags=re.I,
        )
        if match is None:
            match = re.search(
                r"\bafter\s+((?:inching up|inching down|rising|falling|"
                r"moving up|moving down)\s+-?\d+(?:\.\d+)?\s*percent)\s+"
                r"in\s+[A-Z][a-z]+",
                section,
                flags=re.I,
            )
        return signed_change(match.group(1)) if match else None

    def year_change(section: str) -> float | None:
        matches = re.findall(
            r"(?:advanced|increased|rose|gained|fell|declined|decreased|dropped)\s+"
            r"(-?\d+(?:\.\d+)?)\s*percent[^.]{0,100}(?:12 months|year)",
            section,
            flags=re.I,
        )
        if not matches:
            matches = re.findall(
                r"(?:12 months|year)[^.]{0,100}?"
                r"(?:advanced|increased|rose|gained|fell|declined|decreased|dropped)\s+"
                r"(-?\d+(?:\.\d+)?)\s*percent",
                section,
                flags=re.I,
            )
        return float(matches[-1]) if matches else None

    definitions = (
        (
            "WPSFD4",
            "Producer Price Index Final Demand",
            signed_change(headline.group(1)),
            prior_change(headline.group(2)),
            year_change(headline.group(2)),
        ),
        (
            "WPSFD49116",
            "Producer Price Index Final Demand Less Foods Energy and Trade Services",
            signed_change(underlying.group(1)),
            prior_change(underlying.group(2)),
            year_change(underlying.group(2)),
        ),
    )
    if any(actual is None for _, _, actual, _, _ in definitions):
        return []

    output: list[dict[str, Any]] = []
    for series_id, event_name, actual, previous, year_over_year in definitions:
        components = {
            "month_over_month": {"actual": actual, "previous": previous},
            "year_over_year": {"actual": year_over_year},
        }
        external_id = hashlib.sha256(
            json.dumps(
                {
                    "series_id": series_id,
                    "reference_period": reference_period,
                    "components": components,
                },
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        output.append({
            # The physical publisher/source remains the configured BLS release
            # page. event_series_id and external_id already separate headline
            # and underlying components; a synthetic child source_id prevents
            # the classifier from recovering the configured USD binding.
            "source_id": source.get("source_id"),
            "source_name": source.get("name"),
            "source_kind": "bls_current_release",
            "source_quality": source.get("quality", 1.0),
            "source_verified": bool(source.get("verified")),
            "source_direct": configured_source_is_direct(source),
            "retrieval_via": str(source.get("retrieval_via") or "direct"),
            "source_role": source_role(source),
            "source_contract_id": clean_text(source.get("source_contract_id")),
            "source_cohort_id": clean_text(source.get("source_cohort_id")),
            "numeric_parser_activated_utc": clean_text(
                source.get("numeric_parser_activated_utc")
            ),
            "numeric_extraction_contract_id": clean_text(
                source.get("numeric_extraction_contract_id")
            ),
            "source_currencies": list(source.get("currencies") or []),
            "title": f"{event_name} - {month_name} {year}",
            "summary": clean_text(headline.group(0) if series_id == "WPSFD4" else underlying.group(0)),
            "url": canonical_url(source.get("url")),
            "published_utc": published_utc,
            "published_time_inferred": False,
            "external_id": external_id,
            "structured_event": True,
            "event_series_id": series_id,
            "event_name": event_name,
            "event_country": "United States",
            "reference_period": reference_period,
            "timing_precision": "official_embargo_timestamp",
            "unit": "percent_change",
            "actual": str(actual),
            "actual_value": actual,
            "previous": "" if previous is None else str(previous),
            "previous_value": previous,
            "consensus": "",
            "consensus_value": None,
            "source_native_components": components,
            "directional_research_only": True,
        })
    return output


def parse_bls_employment_current_release(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse the official BLS Employment Situation singleton prospectively.

    The generic BLS latest-numbers feed is a mutable multi-release container
    and is not a dependable release-time contract.  The Employment Situation
    ``nr0`` page carries the official embargo clock and the source-native
    payroll, unemployment, earnings, and revision facts.  This adapter keeps
    those factors separate and direction-neutral: causal consensus and
    contemporaneous rate repricing are still required before interpreting a
    value as USD-positive or USD-negative.
    """

    parser = _OfficialDocumentTextParser()
    parser.feed(
        payload.decode(str(source.get("encoding") or "utf-8"), errors="replace")
    )
    text = parser.text()
    release = re.search(
        r"THE EMPLOYMENT SITUATION\s*[-–]\s*([A-Z][A-Z]+)\s+(\d{4})",
        text,
        flags=re.I,
    )
    embargo = re.search(
        r"(?:EMBARGOED UNTIL\s*)?8:30\s*a\.m\.\s*\(ET\)\s*"
        r"(?:Monday|Tuesday|Wednesday|Thursday|Friday),\s*"
        r"([A-Z][a-z]+\s+\d{1,2},\s+\d{4})",
        text,
        flags=re.I,
    )
    if not release or not embargo:
        return []
    month_name = release.group(1).title()
    year = int(release.group(2))
    try:
        reference_date = dt.datetime.strptime(
            f"{month_name} {year}", "%B %Y"
        ).date()
        release_local = dt.datetime.strptime(
            embargo.group(1), "%B %d, %Y"
        ).replace(hour=8, minute=30, tzinfo=ZoneInfo("America/New_York"))
    except ValueError:
        return []
    reference_period = reference_date.strftime("%Y-%m")
    published_utc = release_local.astimezone(UTC).isoformat()

    def number(fragment: str) -> float | None:
        match = re.search(r"[+-]?\d[\d,]*(?:\.\d+)?", fragment)
        if not match:
            return None
        return float(match.group(0).replace(",", ""))

    payroll_match = re.search(
        r"Total nonfarm payroll employment\s+"
        r"(increased|rose|grew|decreased|declined|fell)\s+by\s+"
        r"([+-]?\d[\d,]*)\s+in\s+" + re.escape(month_name),
        text,
        flags=re.I,
    )
    payroll_parenthetical = re.search(
        r"(?:Both\s+)?(?:total\s+)?nonfarm payroll employment\s*"
        r"\(\s*([+-]?\d[\d,]*)\s*\)",
        text,
        flags=re.I,
    )
    payroll_actual: float | None = None
    payroll_summary = ""
    if payroll_match:
        payroll_actual = abs(number(payroll_match.group(2)) or 0.0)
        if payroll_match.group(1).lower() in {"decreased", "declined", "fell"}:
            payroll_actual = -payroll_actual
        payroll_summary = clean_text(payroll_match.group(0))
    elif payroll_parenthetical:
        payroll_actual = number(payroll_parenthetical.group(1))
        payroll_summary = clean_text(payroll_parenthetical.group(0))
    if payroll_actual is None:
        return []

    prior_month_name = (
        reference_date.replace(day=1) - dt.timedelta(days=1)
    ).strftime("%B")
    prior_payroll_match = re.search(
        r"(?:the\s+)?change\s+for\s+" + re.escape(prior_month_name)
        + r"\s+was\s+revised\b.*?\bto\s+([+-]?\d[\d,]*)",
        text,
        flags=re.I,
    )
    prior_payroll = (
        number(prior_payroll_match.group(1)) if prior_payroll_match else None
    )
    combined_revision_match = re.search(
        r"With these revisions,\s+employment\b.*?\bcombined\s+is\s+"
        r"(\d[\d,]*)\s+(higher|lower)\s+than\s+previously\s+reported",
        text,
        flags=re.I,
    )
    combined_revision: float | None = None
    if combined_revision_match:
        combined_revision = abs(number(combined_revision_match.group(1)) or 0.0)
        if combined_revision_match.group(2).lower() == "lower":
            combined_revision = -combined_revision

    unemployment_match = re.search(
        r"unemployment rate\s+"
        r"(was unchanged at|remained at|edged up to|rose to|increased to|"
        r"edged down to|fell to|declined to)\s+"
        r"(\d+(?:\.\d+)?)\s+percent",
        text,
        flags=re.I,
    )
    unemployment_actual = (
        number(unemployment_match.group(2)) if unemployment_match else None
    )
    unemployment_previous = (
        unemployment_actual
        if unemployment_match
        and unemployment_match.group(1).lower() in {"was unchanged at", "remained at"}
        else None
    )

    earnings_match = re.search(
        r"average hourly earnings for all employees\b.*?"
        r"(rose|increased|gained|fell|declined|decreased)\s+by\s+"
        r"\d+\s+cents?,\s+or\s+(\d+(?:\.\d+)?)\s+percent",
        text,
        flags=re.I,
    )
    earnings_actual: float | None = None
    if earnings_match:
        earnings_actual = abs(number(earnings_match.group(2)) or 0.0)
        if earnings_match.group(1).lower() in {"fell", "declined", "decreased"}:
            earnings_actual = -earnings_actual
    earnings_yoy_match = re.search(
        r"Over the year,\s+average hourly earnings\s+have\s+"
        r"(?:increased|risen|decreased|declined)\s+by\s+"
        r"([+-]?\d+(?:\.\d+)?)\s+percent",
        text,
        flags=re.I,
    )
    earnings_yoy = (
        number(earnings_yoy_match.group(1)) if earnings_yoy_match else None
    )

    definitions: list[tuple[str, str, float, float | None, str, str, dict[str, Any]]] = [
        (
            "CES0000000001_NET_CHANGE",
            "Total Nonfarm Payroll Employment Monthly Change",
            payroll_actual,
            prior_payroll,
            "jobs",
            payroll_summary,
            {
                "monthly_change_jobs": {
                    "actual": payroll_actual,
                    "prior_month_revised": prior_payroll,
                },
                "prior_two_month_revision_jobs": {"actual": combined_revision},
            },
        )
    ]
    if unemployment_actual is not None:
        definitions.append(
            (
                "LNS14000000_LEVEL",
                "Unemployment Rate",
                unemployment_actual,
                unemployment_previous,
                "percent",
                clean_text(unemployment_match.group(0)) if unemployment_match else "",
                {
                    "unemployment_rate": {
                        "actual": unemployment_actual,
                        "previous": unemployment_previous,
                    }
                },
            )
        )
    if earnings_actual is not None:
        definitions.append(
            (
                "CES0500000003_PCT_CHANGE",
                "Average Hourly Earnings Monthly Change",
                earnings_actual,
                None,
                "percent_change",
                clean_text(earnings_match.group(0)) if earnings_match else "",
                {
                    "month_over_month": {"actual": earnings_actual},
                    "year_over_year": {"actual": earnings_yoy},
                },
            )
        )
    if combined_revision is not None:
        definitions.append(
            (
                "CES_PRIOR_TWO_MONTH_REVISION",
                "Prior Two-Month Payroll Revision",
                combined_revision,
                None,
                "jobs",
                clean_text(combined_revision_match.group(0))
                if combined_revision_match else "",
                {"prior_two_month_revision_jobs": {"actual": combined_revision}},
            )
        )

    output: list[dict[str, Any]] = []
    for series_id, event_name, actual, previous, unit, summary, components in definitions:
        external_id = hashlib.sha256(
            json.dumps(
                {
                    "series_id": series_id,
                    "reference_period": reference_period,
                    "actual": actual,
                    "previous": previous,
                    "components": components,
                },
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        output.append(
            {
                "source_id": source.get("source_id"),
                "source_name": source.get("name"),
                "source_kind": "bls_employment_current_release",
                "source_quality": source.get("quality", 1.0),
                "source_verified": bool(source.get("verified")),
                "source_direct": configured_source_is_direct(source),
                "retrieval_via": str(source.get("retrieval_via") or "direct"),
                "source_role": source_role(source),
                "source_contract_id": clean_text(source.get("source_contract_id")),
                "source_cohort_id": clean_text(source.get("source_cohort_id")),
                "numeric_parser_activated_utc": clean_text(
                    source.get("numeric_parser_activated_utc")
                ),
                "numeric_extraction_contract_id": clean_text(
                    source.get("numeric_extraction_contract_id")
                ),
                "source_currencies": list(source.get("currencies") or []),
                "title": f"{event_name} - {month_name} {year}",
                "summary": summary,
                "url": canonical_url(source.get("url")),
                "published_utc": published_utc,
                "published_time_inferred": False,
                "external_id": external_id,
                "structured_event": True,
                "event_series_id": series_id,
                "event_name": event_name,
                "event_country": "United States",
                "reference_period": reference_period,
                "timing_precision": "official_embargo_timestamp",
                "unit": unit,
                "actual": str(actual),
                "actual_value": actual,
                "previous": "" if previous is None else str(previous),
                "previous_value": previous,
                "consensus": "",
                "consensus_value": None,
                "source_native_components": components,
                "directional_research_only": True,
            }
        )
    return output


def parse_statcan_major_indicators(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse Canada's current Labour Force Survey from the official JSON API.

    The Daily Atom feed is useful for discovery, but it can remain unchanged
    across the 08:30 release boundary.  Statistics Canada's ``ind-econ`` web
    service is the first-party machine-readable surface used by The Daily and
    exposes the current national employment level, monthly change and
    unemployment rate.  This adapter is a redundant prospective path.  It
    deliberately preserves source-native changes without interpreting CAD
    direction because a causal pre-release consensus and contemporaneous rate
    repricing are not available here.
    """

    parsed = json.loads(payload.decode("utf-8-sig"))
    indicators = (parsed.get("results") or {}).get("indicators") or []
    national: dict[str, Mapping[str, Any]] = {}
    for row in indicators:
        if not isinstance(row, Mapping):
            continue
        try:
            geo_code = int(row.get("geo_code"))
        except (TypeError, ValueError):
            continue
        daily_title = clean_text((row.get("daily_title") or {}).get("en"))
        title = clean_text((row.get("title") or {}).get("en"))
        if geo_code != 0 or daily_title.lower() != "labour force survey":
            continue
        if title.lower() in {"employment level", "unemployment rate"}:
            national[title.lower()] = row
    employment = national.get("employment level")
    unemployment = national.get("unemployment rate")
    if employment is None or unemployment is None:
        return []

    release_date_text = clean_text(employment.get("release_date"))
    reference_period = clean_text((employment.get("refper") or {}).get("en"))
    daily_path = clean_text((employment.get("daily_url") or {}).get("en"))
    if not release_date_text or not reference_period or not daily_path:
        return []
    try:
        release_date = dt.date.fromisoformat(release_date_text)
        release_local = dt.datetime.combine(
            release_date,
            dt.time(hour=8, minute=30),
            tzinfo=ZoneInfo(str(source.get("source_timezone") or "America/Toronto")),
        )
    except (ValueError, ZoneInfoNotFoundError):
        return []

    def numeric_text(value: Any) -> float | None:
        match = re.search(r"[+-]?\d[\d,]*(?:\.\d+)?", clean_text(value))
        if match is None:
            return None
        return optional_float(match.group(0).replace(",", ""))

    employment_level = numeric_text((employment.get("value") or {}).get("en"))
    employment_change = numeric_text(
        ((employment.get("growth_rate") or {}).get("growth") or {}).get("en")
    )
    unemployment_rate = numeric_text((unemployment.get("value") or {}).get("en"))
    unemployment_change = numeric_text(
        ((unemployment.get("growth_rate") or {}).get("growth") or {}).get("en")
    )
    if employment_level is None or employment_change is None or unemployment_rate is None:
        return []
    unemployment_previous = (
        unemployment_rate - unemployment_change
        if unemployment_change is not None
        else None
    )
    published_utc = release_local.astimezone(UTC).isoformat()
    page_url = urllib.parse.urljoin(
        "https://www150.statcan.gc.ca/n1/", daily_path.lstrip("/")
    )
    components: dict[str, dict[str, Any]] = {
        "employment_level": {
            "actual_value": employment_level,
            "unit": "persons",
        },
        "employment_monthly_change": {
            "actual_value": employment_change,
            "unit": "month_percent_change",
        },
        "unemployment_rate": {
            "actual_value": unemployment_rate,
            "previous_value": unemployment_previous,
            "unit": "percent",
        },
    }
    if unemployment_change is not None:
        components["unemployment_rate_monthly_change"] = {
            "actual_value": unemployment_change,
            "unit": "percentage_points",
        }
    identity = {
        "reference_period": reference_period,
        "release_date": release_date_text,
        "employment_level": employment_level,
        "employment_change": employment_change,
        "unemployment_rate": unemployment_rate,
        "unemployment_change": unemployment_change,
    }
    external_id = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    summary = (
        f"Canada employment level {employment_level:.0f}; monthly change "
        f"{employment_change:.12g}%; unemployment rate "
        f"{unemployment_rate:.12g}%"
    )
    if unemployment_change is not None:
        summary += f"; monthly change {unemployment_change:.12g} percentage points"
    return [
        {
            "source_id": source.get("source_id"),
            "source_name": source.get("name"),
            "source_kind": "statcan_major_indicators",
            "source_quality": source.get("quality", 1.0),
            "source_verified": bool(source.get("verified")),
            "source_direct": configured_source_is_direct(source),
            "retrieval_via": str(source.get("retrieval_via") or "direct"),
            "source_role": source_role(source),
            "source_contract_id": clean_text(source.get("source_contract_id")),
            "source_cohort_id": clean_text(source.get("source_cohort_id")),
            "numeric_parser_activated_utc": clean_text(
                source.get("numeric_parser_activated_utc")
            ),
            "numeric_extraction_contract_id": clean_text(
                source.get("numeric_extraction_contract_id")
            ),
            "source_currencies": ["CAD"],
            "title": f"Labour Force Survey - {reference_period}",
            "summary": summary,
            "url": page_url,
            "published_utc": published_utc,
            "published_time_inferred": False,
            "external_id": external_id,
            "structured_event": True,
            "event_series_id": "statcan_labour_force_national_bundle",
            "event_name": "Statistics Canada Labour Force Survey",
            "event_country": "Canada",
            "reference_period": reference_period,
            "timing_precision": "official_0830_eastern_service_clock",
            "unit": "month_percent_change",
            "actual": f"{employment_change:.12g}",
            "actual_value": employment_change,
            "previous": "",
            "previous_value": None,
            "consensus": "",
            "consensus_value": None,
            "source_native_components": components,
            "directional_research_only": True,
            "numeric_direction_policy": (
                "abstain_until_causal_consensus_and_rate_repricing"
            ),
        }
    ]


def parse_japan_cpi_csv(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse the latest populated national CPI YoY row from the official CSV.

    The source-native file includes a long history plus future blank rows.
    Only a row with an exact official release clock in the frozen source
    contract is emitted. Absolute CPI changes remain direction-neutral until
    consensus, rate repricing, or post-release technical confirmation exists.
    """

    try:
        text = payload.decode("cp932")
    except UnicodeDecodeError:
        text = payload.decode("utf-8-sig", errors="replace")
    rows = list(csv.reader(io.StringIO(text)))
    observations: list[tuple[str, float, float | None]] = []
    for row in rows:
        if not row or not re.fullmatch(r"20\d{4}", clean_text(row[0])):
            continue
        all_items = optional_float(row[1] if len(row) > 1 else None)
        if all_items is None:
            continue
        core = optional_float(row[2] if len(row) > 2 else None)
        period = f"{row[0][:4]}-{row[0][4:]}"
        observations.append((period, all_items, core))
    if not observations:
        return []
    period, actual, core = observations[-1]
    release_map = source.get("release_utc_by_reference") or {}
    published = parse_datetime(release_map.get(period))
    if published is None:
        return []
    previous = observations[-2][1] if len(observations) >= 2 else None
    components: dict[str, Any] = {
        "all_items_yoy": {"actual": actual, "previous": previous},
    }
    if core is not None:
        components["all_items_less_fresh_food_yoy"] = {"actual": core}
    external_id = hashlib.sha256(
        json.dumps(
            {
                "period": period,
                "actual": actual,
                "previous": previous,
                "core": core,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    return [
        {
            "source_id": source.get("source_id"),
            "source_name": source.get("name"),
            "source_kind": "japan_cpi_csv",
            "source_quality": source.get("quality", 1.0),
            "source_verified": bool(source.get("verified")),
            "source_direct": configured_source_is_direct(source),
            "retrieval_via": str(
                source.get("retrieval_via") or "official_source_native_csv"
            ),
            "source_role": source_role(source),
            "source_contract_id": clean_text(source.get("source_contract_id")),
            "source_cohort_id": clean_text(source.get("source_cohort_id")),
            "numeric_parser_activated_utc": clean_text(
                source.get("numeric_parser_activated_utc")
            ),
            "numeric_extraction_contract_id": clean_text(
                source.get("numeric_extraction_contract_id")
            ),
            "source_currencies": list(source.get("currencies") or ["JPY"]),
            "title": f"Japan national Consumer Price Index - {period}",
            "summary": (
                f"Official national CPI annual change: {actual:.12g}%."
            ),
            "url": canonical_url(source.get("url")),
            "published_utc": iso_utc(published),
            "published_time_inferred": False,
            "external_id": external_id,
            "structured_event": True,
            "event_series_id": "japan_cpi_national_yoy",
            "event_name": "Japan national Consumer Price Index annual change",
            "event_country": "Japan",
            "reference_period": period,
            "timing_precision": "official_exact_schedule",
            "unit": "year_percent_change",
            "actual": f"{actual:.12g}",
            "actual_value": actual,
            "previous": "" if previous is None else f"{previous:.12g}",
            "previous_value": previous,
            "consensus": "",
            "consensus_value": None,
            "source_native_components": components,
            "numeric_direction_policy": "abstain_and_learn_response",
            "directional_research_only": True,
        }
    ]


def parse_japan_cpi_current_summary(
    payload: bytes,
    source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Parse the official mutable Japan CPI result summary conservatively.

    The first snapshot under a new source contract is quarantined by the
    singleton-history gate.  Later release identities can be prospective, but
    only when the exact release clock is frozen in configuration.
    """

    try:
        page = payload.decode("cp932")
    except UnicodeDecodeError:
        page = payload.decode("utf-8-sig", errors="replace")
    parser = _OfficialDocumentTextParser()
    parser.feed(page)
    text = re.sub(r"\s+", " ", html.unescape(parser.text())).strip()
    heading = re.search(
        r"Consumer Price Index.*?(?P<reference_year>20\d{2})年(?:（[^）]+）)?"
        r"(?P<reference_month>\d{1,2})月分.*?"
        r"(?P<release_year>20\d{2})年(?P<release_month>\d{1,2})月"
        r"(?P<release_day>\d{1,2})日公表",
        text,
        flags=re.I,
    )
    if heading is None:
        # The Japanese page title does not include the English label.
        heading = re.search(
            r"(?P<reference_year>20\d{2})年(?:（[^）]+）)?"
            r"(?P<reference_month>\d{1,2})月分.*?"
            r"(?P<release_year>20\d{2})年(?P<release_month>\d{1,2})月"
            r"(?P<release_day>\d{1,2})日公表",
            text,
        )
    if heading is None:
        return []
    actual_match = re.search(
        r"\(1\).*?総合指数.*?前年同月比は\s*"
        r"(?P<value>\d+(?:\.\d+)?)\s*[%％]の(?P<direction>上昇|下落)",
        text,
        flags=re.S,
    )
    if actual_match is None:
        return []
    actual = float(actual_match.group("value"))
    if actual_match.group("direction") == "下落":
        actual = -actual
    reference = (
        f"{int(heading.group('reference_year')):04d}-"
        f"{int(heading.group('reference_month')):02d}"
    )
    release_map = source.get("release_utc_by_reference") or {}
    published = parse_datetime(release_map.get(reference))
    if published is None:
        return []
    external_id = hashlib.sha256(
        json.dumps(
            {"reference_period": reference, "actual": actual, "published": iso_utc(published)},
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    return [
        {
            "source_id": source.get("source_id"),
            "source_name": source.get("name"),
            "source_kind": "japan_cpi_current_summary",
            "source_quality": source.get("quality", 1.0),
            "source_verified": bool(source.get("verified")),
            "source_direct": configured_source_is_direct(source),
            "retrieval_via": str(source.get("retrieval_via") or "official_result_summary"),
            "source_role": source_role(source),
            "source_contract_id": clean_text(source.get("source_contract_id")),
            "source_cohort_id": clean_text(source.get("source_cohort_id")),
            "numeric_parser_activated_utc": clean_text(source.get("numeric_parser_activated_utc")),
            "numeric_extraction_contract_id": clean_text(source.get("numeric_extraction_contract_id")),
            "source_currencies": list(source.get("currencies") or ["JPY"]),
            "title": f"Japan national Consumer Price Index - {reference}",
            "summary": f"Official national CPI annual change: {actual:.12g}%.",
            "url": canonical_url(source.get("url")),
            "published_utc": iso_utc(published),
            "published_time_inferred": False,
            "external_id": external_id,
            "structured_event": True,
            "event_series_id": "japan_cpi_national_yoy",
            "event_name": "Japan national Consumer Price Index annual change",
            "event_country": "Japan",
            "reference_period": reference,
            "timing_precision": "official_exact_schedule",
            "unit": "year_percent_change",
            "actual": f"{actual:.12g}",
            "actual_value": actual,
            "previous": "",
            "previous_value": None,
            "consensus": "",
            "consensus_value": None,
            "numeric_direction_policy": "abstain_and_learn_response",
            "directional_research_only": True,
        }
    ]


def annotate_singleton_release_history(
    articles: list[dict[str, Any]],
    source_state: Mapping[str, Any],
    *,
    maximum_ids: int = 100,
) -> list[str]:
    """Bootstrap the first singleton snapshot and retain release identities."""

    stored = source_state.get("known_release_ids")
    seeded = isinstance(stored, list)
    known = {clean_text(value) for value in (stored or []) if clean_text(value)}
    current = [clean_text(row.get("external_id")) for row in articles]
    current = [value for value in current if value]
    if not seeded:
        for article in articles:
            article["source_listing_bootstrap"] = True
    merged = list(dict.fromkeys([*current, *known]))
    return merged[: max(1, int(maximum_ids))]


def annotate_html_listing_history(
    articles: list[dict[str, Any]],
    source_state: Mapping[str, Any],
    *,
    listing_bootstrap: bool,
    maximum_urls: int = 2000,
) -> list[str]:
    """Mark HTML-listing rows using persistent causal URL history.

    ``last_success_utc`` alone is not sufficient proof that the database has
    already observed a listing's links.  A preserved collector state paired
    with a rebuilt database used to make every undated back-catalog link look
    prospectively new.  URL history gives the listing its own causal seed.

    ``known_item_urls`` answers whether a link is newly observed on this poll.
    It cannot also answer whether the link belonged to the original listing
    bootstrap: after one successful poll both baseline and genuinely new URLs
    are merely "known".  ``bootstrap_item_urls`` is therefore a separate,
    persistent quarantine.  Legacy states that predate that field migrate
    conservatively by treating their already-known URLs as bootstrap history;
    an old observation must not become fresh evidence because code changed.
    """

    stored = source_state.get("known_item_urls")
    # An older broken parser could persist an empty list after a nominal HTTP
    # success.  That is not evidence that any back-catalog URL was observed.
    history_seeded = isinstance(stored, list) and bool(stored)
    known_urls = {
        canonical_url(value)
        for value in (stored or [])
        if canonical_url(value)
    }
    current_urls = [
        canonical_url(article.get("url") or article.get("source_url"))
        for article in articles
    ]
    current_urls = [value for value in current_urls if value]

    stored_bootstrap = source_state.get("bootstrap_item_urls")
    if isinstance(stored_bootstrap, list):
        bootstrap_urls = {
            canonical_url(value)
            for value in stored_bootstrap
            if canonical_url(value)
        }
    else:
        # Fail closed while migrating collector states written before the
        # persistent bootstrap quarantine existed.  These URLs were already
        # observed before this collector cohort and are not prospective.
        bootstrap_urls = set(known_urls)
    if listing_bootstrap or not history_seeded:
        bootstrap_urls.update(current_urls)

    for article in articles:
        url = canonical_url(article.get("url") or article.get("source_url"))
        if url and url in bootstrap_urls:
            article["source_listing_bootstrap"] = True
            article["source_listing_new_item"] = False
        else:
            article["source_listing_new_item"] = bool(url and url not in known_urls)
    ordered = list(dict.fromkeys([*current_urls, *sorted(known_urls)]))
    return ordered[: max(1, int(maximum_urls))]


def retained_html_listing_bootstrap_urls(
    articles: Sequence[Mapping[str, Any]],
    source_state: Mapping[str, Any],
) -> list[str]:
    """Return the append-only bootstrap URL quarantine for persisted state."""

    stored_bootstrap = source_state.get("bootstrap_item_urls")
    if isinstance(stored_bootstrap, list):
        prior = [
            canonical_url(value)
            for value in stored_bootstrap
            if canonical_url(value)
        ]
    else:
        # Conservative one-time migration from the pre-quarantine state
        # contract.  Do not retroactively call an already-seen URL new.
        prior_known = source_state.get("known_item_urls")
        prior = [
            canonical_url(value)
            for value in (prior_known if isinstance(prior_known, list) else [])
            if canonical_url(value)
        ]
    current = [
        canonical_url(article.get("url") or article.get("source_url"))
        for article in articles
        if article.get("source_listing_bootstrap") is True
        and canonical_url(article.get("url") or article.get("source_url"))
    ]
    # This list contains only the finite initial/reseeded catalog, not every
    # future item.  Retaining it without the rolling known-URL cap is what
    # makes the causal quarantine permanent across later polls and restarts.
    return list(dict.fromkeys([*prior, *current]))


def parse_gdelt(payload: bytes, source: Mapping[str, Any]) -> list[dict[str, Any]]:
    parsed = json.loads(payload.decode("utf-8-sig"))
    output: list[dict[str, Any]] = []
    for row in parsed.get("articles") or []:
        if not isinstance(row, dict):
            continue
        title = clean_text(row.get("title"))
        if not title:
            continue
        seen = str(row.get("seendate") or "")
        if re.fullmatch(r"\d{14}", seen):
            seen = (
                f"{seen[:4]}-{seen[4:6]}-{seen[6:8]}T"
                f"{seen[8:10]}:{seen[10:12]}:{seen[12:14]}Z"
            )
        domain = str(row.get("domain") or "").lower().strip()
        output.append(
            {
                "source_id": source.get("source_id"),
                "source_name": domain or source.get("name"),
                "source_kind": "gdelt",
                "source_quality": source.get("quality", 0.65),
                "source_verified": False,
                "source_direct": False,
                "retrieval_via": "gdelt",
                "source_role": source_role(source),
                "source_currencies": [],
                "title": title,
                "summary": clean_text(row.get("summary")),
                "url": canonical_url(row.get("url") or row.get("url_mobile")),
                "published_utc": iso_utc(parse_datetime(seen))
                if parse_datetime(seen)
                else "",
                "external_id": "",
                "domain": domain,
                "language": row.get("language"),
                "source_country": row.get("sourcecountry"),
                # Broad discovery observations still need immutable lineage.
                # This does not upgrade GDELT to a verified/direct source or
                # make its content eligible for execution.
                "source_contract_id": clean_text(source.get("source_contract_id")),
                "source_cohort_id": clean_text(source.get("source_cohort_id")),
            }
        )
    return output


def source_request_url(
    source: Mapping[str, Any],
    *,
    now: dt.datetime | None = None,
) -> str:
    kind = str(source.get("kind") or "").lower()
    base_url = str(source.get("url") or "")
    reference = now or utc_now()
    reference = (
        reference.replace(tzinfo=UTC)
        if reference.tzinfo is None
        else reference.astimezone(UTC)
    )
    # Some official publishers partition release indexes by year.  Resolve
    # only explicit, controlled placeholders instead of applying unrestricted
    # string formatting to publisher URLs.
    base_url = base_url.replace("{year}", f"{reference.year:04d}")
    base_url = base_url.replace("{month}", f"{reference.month:02d}")
    parameters: dict[str, Any] = {}
    if kind == "gdelt":
        parameters.update(
            {
                "query": str(source.get("query") or ""),
                "mode": "artlist",
                "maxrecords": "250",
                "format": "json",
                "timespan": str(source.get("timespan") or "1h"),
                "sort": "datedesc",
            }
        )
    configured = source.get("query_params")
    if isinstance(configured, Mapping):
        parameters.update(
            {
                str(key): value
                for key, value in configured.items()
                if value not in (None, "")
            }
        )
    rotating = source.get("rotating_query_param")
    if isinstance(rotating, Mapping):
        parameter_name = clean_text(rotating.get("name"))
        values = [
            clean_text(value)
            for value in (rotating.get("values") or [])
            if clean_text(value)
        ]
        period_seconds = max(
            60,
            int(safe_float(rotating.get("period_sec"), 7200.0)),
        )
        if parameter_name and values:
            # Select from UTC time, not process state, so restarts within the
            # same interval repeat the same request instead of adaptively
            # changing source coverage or consuming an extra quota slot.
            rotation_index = int(reference.timestamp() // period_seconds) % len(values)
            parameters[parameter_name] = values[rotation_index]
    relative_start_days = source.get("relative_start_days")
    if relative_start_days not in (None, ""):
        try:
            lookback_days = max(0, int(relative_start_days))
        except (TypeError, ValueError):
            lookback_days = 0
        parameters["start_date"] = (
            reference.date() - dt.timedelta(days=lookback_days)
        ).isoformat()
    credential = source_credential(source)
    credential_parameter = clean_text(source.get("credential_query_param"))
    if credential and credential_parameter:
        parameters[credential_parameter] = credential
    if not parameters:
        return base_url
    parsed = urllib.parse.urlsplit(base_url)
    existing = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    existing.update(parameters)
    return urllib.parse.urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urllib.parse.urlencode(existing, doseq=True),
            parsed.fragment,
        )
    )


def verify_content_addressed_release_rule(
    source: Mapping[str, Any],
    *,
    timeout_sec: float,
) -> tuple[str, int, str]:
    """Verify and retain the exact official bytes behind a fixed clock rule."""

    rule_url = canonical_url(source.get("release_time_rule_url"))
    trusted_domains = source.get("trusted_domains") or ()
    if not rule_url or not trusted_host(rule_url, trusted_domains):
        raise ValueError("release-time rule URL is missing or outside trusted domains")
    expected_sha256 = clean_text(source.get("release_time_rule_sha256")).lower()
    if re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None:
        raise ValueError("release-time rule SHA-256 is invalid")
    archive_name = clean_text(source.get("release_time_rule_archive_name"))
    if re.fullmatch(r"[A-Za-z0-9._-]+", archive_name) is None:
        raise ValueError("release-time rule archive name is invalid")
    archive_path = DEFAULT_OUTPUT_ROOT / "source_rule_artifacts" / archive_name
    if archive_path.exists():
        payload = archive_path.read_bytes()
        actual_sha256 = hashlib.sha256(payload).hexdigest()
        if actual_sha256 != expected_sha256:
            raise ValueError("archived release-time rule SHA-256 mismatch")
        return actual_sha256, len(payload), "local_content_addressed_archive"

    maximum_bytes = max(
        100_000,
        min(
            10_000_000,
            int(safe_float(source.get("release_time_rule_maximum_bytes"), 5_000_000)),
        ),
    )
    marker = b"\n__FOREX_RULE_HTTP_STATUS__:"
    command = [
        clean_text(source.get("curl_executable")) or "curl.exe",
        "--silent",
        "--show-error",
        "--location",
        "--max-time",
        str(max(1, int(math.ceil(timeout_sec)))),
        "--max-filesize",
        str(maximum_bytes),
        "--retry",
        "2",
        "--retry-all-errors",
        "--retry-delay",
        "1",
        "--write-out",
        marker.decode("ascii") + "%{http_code}",
        rule_url,
    ]
    completed = subprocess.run(
        command,
        capture_output=True,
        timeout=max(2.0, (timeout_sec + 2.0) * 3.0),
        check=False,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    payload, separator, status_text = completed.stdout.rpartition(marker)
    if not separator:
        raise OSError("release-time rule fetch returned no HTTP status")
    try:
        status = int(status_text.strip())
    except ValueError as exc:
        raise OSError("release-time rule fetch returned invalid HTTP status") from exc
    if completed.returncode != 0 or status != 200:
        diagnostic = clean_text(
            completed.stderr[:300].decode("utf-8", errors="replace")
        )
        raise OSError(
            f"release-time rule fetch failed: HTTP {status}, "
            f"curl {completed.returncode}: {diagnostic}"
        )
    if len(payload) > maximum_bytes:
        raise ValueError("release-time rule payload exceeds configured limit")
    actual_sha256 = hashlib.sha256(payload).hexdigest()
    if actual_sha256 != expected_sha256:
        raise ValueError("release-time rule live SHA-256 mismatch")
    atomic_write_bytes(archive_path, payload)
    if hashlib.sha256(archive_path.read_bytes()).hexdigest() != expected_sha256:
        raise ValueError("release-time rule archive verification failed")
    return actual_sha256, len(payload), "live_fetch_then_content_addressed_archive"


def fetch_source(
    source: Mapping[str, Any],
    source_state: dict[str, Any],
    *,
    timeout_sec: float,
    maximum_bytes: int,
    now: dt.datetime,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    source = dict(source)
    if str(source.get("retrieval_via") or "") == "google_news_official_site_search":
        # Where an authority has no reliable public feed, Google is discovery
        # transport only. Follow the item to the configured official domain,
        # extract/archive there, and keep the whole addition shadow-only.
        source.setdefault("detail_enrichment", "official_document_text")
        source.setdefault("detail_max_age_minutes", 1440)
        source.setdefault("directional_research_only", True)
        source.setdefault(
            "google_news_publisher_resolution_contract_id",
            GOOGLE_NEWS_OFFICIAL_PUBLISHER_RESOLUTION_CONTRACT_ID,
        )
    source_kind = str(source.get("kind") or "").lower()
    if source_kind == "recurring_release_calendar":
        articles = build_recurring_release_calendar(source, now=now)
        updated = dict(source_state)
        configured_contract = clean_text(source.get("source_contract_id"))
        configured_cohort = clean_text(source.get("source_cohort_id"))
        prior_contract = clean_text(source_state.get("source_contract_id"))
        updated.update(
            {
                "last_attempt_utc": iso_utc(now),
                "last_success_utc": iso_utc(now),
                "last_status": 200,
                "last_error": "",
                "consecutive_errors": 0,
                "response_bytes": 0,
                "parsed_items": len(articles),
                "schedule_materialization": "local_official_rule",
                "source_contract_id": configured_contract,
                "source_cohort_id": configured_cohort,
            }
        )
        if configured_contract and configured_contract != prior_contract:
            updated["source_lineage_adopted_utc"] = iso_utc(now)
            if prior_contract:
                updated["previous_source_contract_id"] = prior_contract
        return articles, updated
    if source_kind == "scheduled_official_pdf":
        return fetch_scheduled_official_pdf(
            source,
            source_state,
            timeout_sec=timeout_sec,
            maximum_bytes=maximum_bytes,
            now=now,
        )
    configured_source_contract = clean_text(source.get("source_contract_id"))
    observed_source_contract = clean_text(source_state.get("source_contract_id"))
    listing_bootstrap = bool(
        source_kind == "html_links"
        and (
            not source_state.get("last_success_utc")
            or (
                configured_source_contract
                and configured_source_contract != observed_source_contract
            )
        )
    )
    request_url = source_request_url(source, now=now)
    headers = {
        "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, application/json",
        "Accept-Encoding": "identity",
        "User-Agent": "ForexResearchNewsCollector/1.0 (causal research; low-rate polling)",
    }
    configured_headers = source.get("headers")
    if isinstance(configured_headers, Mapping):
        headers.update(
            {
                str(key): str(value)
                for key, value in configured_headers.items()
                if value not in (None, "")
            }
        )
    suppressed_headers = source.get("suppress_default_headers")
    if isinstance(suppressed_headers, list):
        for header_name in suppressed_headers:
            normalized_name = clean_text(header_name).lower()
            matching_key = next(
                (key for key in headers if key.lower() == normalized_name),
                None,
            )
            if matching_key is not None:
                headers.pop(matching_key, None)
    credential = source_credential(source)
    credential_header = clean_text(source.get("credential_header"))
    if credential and credential_header:
        prefix = str(source.get("credential_header_prefix") or "")
        headers[credential_header] = f"{prefix}{credential}"
    configured_source_contract = clean_text(source.get("source_contract_id"))
    observed_source_contract = clean_text(source_state.get("source_contract_id"))
    source_contract_changed = bool(
        configured_source_contract
        and configured_source_contract != observed_source_contract
    )
    # An ETag validated under a broken parser only proves that the bytes are
    # unchanged; it cannot prove that the repaired parser has ever seen them.
    # Force one complete body whenever the source contract changes.
    detail_context_target = max(
        0,
        int(safe_float(source.get("detail_context_target_count"), 0)),
    )
    context_archive_pending = bool(
        source.get("detail_context_archive_only")
        and detail_context_target
        and len(source_state.get("detail_first_seen_utc_by_url") or {})
        < detail_context_target
    )
    mutable_detail_pending = bool(
        source.get("detail_mutable_placeholder_versioning") is True
        and any(
            mutable_detail_refetch_pending(
                source,
                source_state,
                canonical_url(url),
                now=now,
            )
            for url in (
                source_state.get("detail_first_seen_utc_by_url") or {}
            )
        )
    )
    # A transport success is not a successful ingestion. Unparsed or pending
    # bodies must be fetched again, even when the server retains its ETag.
    body_retry_required = bool(
        source_state.get("source_body_retry_required")
        or source_state.get("last_parse_status") in {"error", "pending_source_content"}
    )
    conditional_get = bool(
        not body_retry_required
        and source.get("conditional_get", True)
        and not source_contract_changed
        and not context_archive_pending
        and not mutable_detail_pending
    )
    if not conditional_get:
        # Configured headers must obey the same forced-body rule as retained
        # state validators, including arbitrary HTTP header capitalization.
        headers = {key: value for key, value in headers.items()
                   if str(key).lower() not in {"if-none-match", "if-modified-since"}}
    if conditional_get and source_state.get("etag"):
        headers["If-None-Match"] = str(source_state["etag"])
    if conditional_get and source_state.get("last_modified"):
        headers["If-Modified-Since"] = str(source_state["last_modified"])
    request_data: bytes | None = None
    if source_kind == "bls_timeseries_batch":
        configured_series = source.get("series")
        if not isinstance(configured_series, list):
            configured_series = []
        series_ids = [
            clean_text(row.get("series_id"))
            for row in configured_series
            if isinstance(row, Mapping) and clean_text(row.get("series_id"))
        ]
        request_body: dict[str, Any] = {
            "seriesid": series_ids,
            "startyear": str(now.year - 1),
            "endyear": str(now.year),
        }
        if credential:
            request_body["registrationkey"] = credential
        request_data = json.dumps(request_body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        request_url,
        data=request_data,
        headers=headers,
        method="POST" if request_data is not None else None,
    )
    updated = dict(source_state)
    if source.get("source_contract_derived") is not True:
        # An explicit source contract supersedes the fallback hash-derived
        # lineage. Do not leave stale derived-lineage labels in runtime state;
        # they make an explicit cohort appear mixed even though the configured
        # contract and parser have already changed.
        for stale_lineage_field in (
            "source_contract_derived",
            "source_lineage_version",
            "source_config_sha256",
        ):
            updated.pop(stale_lineage_field, None)
    updated["last_attempt_utc"] = iso_utc(now)
    updated["source_contract_id"] = clean_text(source.get("source_contract_id"))
    updated["source_cohort_id"] = clean_text(source.get("source_cohort_id"))
    if source_contract_changed:
        updated["source_lineage_adopted_utc"] = iso_utc(now)
        if observed_source_contract:
            updated["previous_source_contract_id"] = observed_source_contract
        if not source.get("detail_enrichment"):
            # Detail-page failures belong to the former source contract.  A
            # feed-body-only successor must not keep advertising or retrying a
            # forbidden detail transport that is no longer configured.
            updated.pop("detail_enriched_items", None)
            updated.pop("detail_first_seen_utc_by_url", None)
            updated.pop("last_detail_error", None)
    final_request_started = False

    def not_modified_result():
        updated.update(last_status=304, response_bytes=0,
                       http_transport=clean_text(source.get("http_transport")).lower() or "urllib")
        sent_validator = any(
            str(key).lower() in {"if-none-match", "if-modified-since"} and value
            for key, value in headers.items()
        )
        if not final_request_started or not conditional_get or not sent_validator:
            updated.update(
                last_error="not_modified_without_usable_conditional_body",
                consecutive_errors=int(source_state.get("consecutive_errors") or 0) + 1,
                source_body_retry_required=True,
            )
        else:
            updated.update(last_success_utc=iso_utc(now), last_error="", consecutive_errors=0)
        return [], updated

    try:
        opener: urllib.request.OpenerDirector | None = None
        bootstrap_url = clean_text(source.get("bootstrap_url"))
        if bootstrap_url:
            bootstrap_url = bootstrap_url.replace("{year}", f"{now.year:04d}")
            bootstrap_url = bootstrap_url.replace("{month}", f"{now.month:02d}")
            if not trusted_host(bootstrap_url, source.get("trusted_domains") or ()):
                raise ValueError("source bootstrap URL is outside trusted domains")
            cookie_jar = http.cookiejar.CookieJar()
            opener = urllib.request.build_opener(
                urllib.request.HTTPSHandler(context=TLS_CONTEXT),
                urllib.request.HTTPCookieProcessor(cookie_jar),
            )
            bootstrap_headers = {
                "Accept": "text/html,application/xhtml+xml,*/*",
                "Accept-Encoding": "identity",
                "User-Agent": headers["User-Agent"],
            }
            configured_bootstrap_headers = source.get("bootstrap_headers")
            if isinstance(configured_bootstrap_headers, Mapping):
                bootstrap_headers.update(
                    {
                        str(key): str(value)
                        for key, value in configured_bootstrap_headers.items()
                        if value not in (None, "")
                    }
                )
            bootstrap_request = urllib.request.Request(
                bootstrap_url,
                headers=bootstrap_headers,
            )
            with opener.open(bootstrap_request, timeout=timeout_sec) as bootstrap_response:
                bootstrap_limit = max(
                    100_000,
                    min(
                        maximum_bytes,
                        int(safe_float(source.get("bootstrap_maximum_bytes"), 1_000_000)),
                    ),
                )
                bootstrap_payload = bootstrap_response.read(bootstrap_limit + 1)
                if len(bootstrap_payload) > bootstrap_limit:
                    raise ValueError("source bootstrap payload exceeds limit")
        transport = clean_text(source.get("http_transport")).lower() or "urllib"
        final_request_started = True
        if transport == "requests":
            if requests is None:
                raise ValueError("requests transport is configured but unavailable")
            if opener is not None:
                raise ValueError("requests transport does not support bootstrap_url")
            with requests.request(
                "POST" if request_data is not None else "GET",
                request_url,
                data=request_data,
                headers=headers,
                timeout=timeout_sec,
                stream=True,
            ) as response:
                status = int(response.status_code)
                if status == 304:
                    return not_modified_result()
                if status >= 400:
                    retry_after = safe_float(response.headers.get("Retry-After"), 0.0)
                    excerpt = response.raw.read(500, decode_content=True)
                    updated.update(
                        {
                            "last_status": status,
                            "last_error": redact_source_secret(
                                clean_text(excerpt.decode("utf-8", errors="replace")),
                                source,
                            ),
                            "consecutive_errors": int(updated.get("consecutive_errors") or 0)
                            + 1,
                            "retry_after_utc": provider_http_retry_after(
                                source, status, retry_after, now
                            ),
                        }
                    )
                    return [], updated
                chunks: list[bytes] = []
                payload_size = 0
                for chunk in response.iter_content(chunk_size=65_536):
                    if not chunk:
                        continue
                    payload_size += len(chunk)
                    if payload_size > maximum_bytes:
                        raise ValueError(f"source payload exceeds {maximum_bytes} bytes")
                    chunks.append(chunk)
                payload = b"".join(chunks)
                updated.update(
                    {
                        "last_success_utc": iso_utc(now),
                        "last_status": status,
                        "last_error": "",
                        "etag": response.headers.get("ETag") or updated.get("etag", ""),
                        "last_modified": response.headers.get("Last-Modified")
                        or updated.get("last_modified", ""),
                        "consecutive_errors": 0,
                        "response_bytes": len(payload),
                        "http_transport": "requests",
                    }
                )
        elif transport == "curl":
            if opener is not None:
                raise ValueError("curl transport does not support bootstrap_url")
            if credential:
                # Header values are command-line arguments for this narrow
                # Windows/SChannel compatibility path. Never expose a secret
                # there; credentialed sources must use urllib or requests.
                raise ValueError("curl transport does not support credentials")
            marker = b"\n__FOREX_HTTP_STATUS__:"
            curl_retry_count = min(
                3,
                max(0, int(safe_float(source.get("curl_retry_count"), 1.0))),
            )
            command = [
                clean_text(source.get("curl_executable")) or "curl.exe",
                "--silent",
                "--show-error",
                "--location",
                "--max-time",
                str(max(1, int(math.ceil(timeout_sec)))),
                "--max-filesize",
                str(maximum_bytes),
                "--retry",
                str(curl_retry_count),
                "--retry-all-errors",
                "--retry-delay",
                "1",
                "--write-out",
                marker.decode("ascii") + "%{http_code}",
            ]
            curl_ip_version = clean_text(source.get("curl_ip_version"))
            if curl_ip_version in {"4", "6"}:
                command.append(f"--ipv{curl_ip_version}")
            # Windows/SChannel can abort an otherwise valid official HTTPS
            # connection when the certificate revocation endpoint is briefly
            # unavailable. This opt-in keeps certificate and hostname
            # validation enabled; only revocation-network failures become
            # best-effort. Never use --insecure for causal source transport.
            if source.get("curl_ssl_revoke_best_effort") is True:
                command.append("--ssl-revoke-best-effort")
            # Some gateways alternate transient 5xx pages with a valid retry.
            # Without --fail curl concatenates each error body onto stdout,
            # corrupting the final successful JSON/XML payload.  Keep this
            # opt-in because it changes the retained error-body diagnostic.
            if source.get("curl_fail_http_errors"):
                command.append("--fail")
            for key, value in headers.items():
                command.extend(["--header", f"{key}: {value}"])
            if request_data is not None:
                command.extend(["--request", "POST", "--data-binary", "@-"])
            else:
                command.extend(["--request", "GET"])
            command.append(request_url)
            completed = subprocess.run(
                command,
                input=request_data,
                capture_output=True,
                timeout=max(
                    2.0,
                    (timeout_sec + 2.0) * (curl_retry_count + 1),
                ),
                check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            body, separator, status_text = completed.stdout.rpartition(marker)
            if not separator:
                error_text = completed.stderr.decode("utf-8", errors="replace")
                raise OSError(
                    f"curl transport failed without HTTP status: {error_text[:300]}"
                )
            try:
                status = int(status_text.strip())
            except ValueError as exc:
                raise OSError("curl transport returned invalid HTTP status") from exc
            if len(body) > maximum_bytes:
                raise ValueError(f"source payload exceeds {maximum_bytes} bytes")
            if completed.returncode != 0 and status < 400:
                error_text = completed.stderr.decode("utf-8", errors="replace")
                raise OSError(
                    f"curl transport exit {completed.returncode}: {error_text[:300]}"
                )
            if status == 304:
                return not_modified_result()
            if status >= 400:
                error_body = clean_text(
                    body[:500].decode("utf-8", errors="replace")
                )
                error_stderr = clean_text(
                    completed.stderr[:500].decode("utf-8", errors="replace")
                )
                updated.update(
                    {
                        "last_status": status,
                        "last_error": redact_source_secret(
                            error_body or error_stderr or f"HTTP {status}",
                            source,
                        ),
                        "consecutive_errors": int(updated.get("consecutive_errors") or 0)
                        + 1,
                        "http_transport": "curl",
                    }
                )
                return [], updated
            payload = body
            updated.update(
                {
                    "last_success_utc": iso_utc(now),
                    "last_status": status,
                    "last_error": "",
                    "consecutive_errors": 0,
                    "response_bytes": len(payload),
                    "http_transport": "curl",
                }
            )
        elif transport == "urllib":
            response_context = (
                opener.open(request, timeout=timeout_sec)
                if opener is not None
                else urllib.request.urlopen(
                    request,
                    timeout=timeout_sec,
                    context=TLS_CONTEXT,
                )
            )
            with response_context as response:
                payload = response.read(maximum_bytes + 1)
                if len(payload) > maximum_bytes:
                    raise ValueError(f"source payload exceeds {maximum_bytes} bytes")
                updated.update(
                    {
                        "last_success_utc": iso_utc(now),
                        "last_status": int(response.status),
                        "last_error": "",
                        "etag": response.headers.get("ETag") or updated.get("etag", ""),
                        "last_modified": response.headers.get("Last-Modified")
                        or updated.get("last_modified", ""),
                        "consecutive_errors": 0,
                        "response_bytes": len(payload),
                        "http_transport": "urllib",
                    }
                )
        else:
            raise ValueError(f"unsupported http_transport: {transport}")
    except urllib.error.HTTPError as exc:
        if exc.code == 304:
            return not_modified_result()
        retry_after = safe_float(exc.headers.get("Retry-After"), 0.0)
        updated.update(
            {
                "last_status": int(exc.code),
                "last_error": redact_source_secret(
                    clean_text(exc.read(500).decode("utf-8", errors="replace")),
                    source,
                ),
                "consecutive_errors": int(updated.get("consecutive_errors") or 0) + 1,
                "retry_after_utc": provider_http_retry_after(
                    source, int(exc.code), retry_after, now
                ),
            }
        )
        return [], updated
    except (OSError, ValueError, ET.ParseError, json.JSONDecodeError) as exc:
        updated.update(
            {
                "last_status": 0,
                "last_error": redact_source_secret(str(exc), source)[:500],
                "consecutive_errors": int(updated.get("consecutive_errors") or 0) + 1,
            }
        )
        return [], updated
    try:
        kind = source_kind
        if kind == "gdelt":
            articles = parse_gdelt(payload, source)
        elif kind == "economic_calendar":
            articles = parse_economic_calendar(payload, source)
        elif kind == "stats_nz_calendar":
            articles = parse_stats_nz_calendar(payload, source)
        elif kind == "census_release_calendar":
            articles = parse_census_release_calendar(payload, source)
        elif kind == "bls_timeseries":
            articles = parse_bls_timeseries(payload, source)
        elif kind == "bls_timeseries_batch":
            articles = parse_bls_timeseries_batch(payload, source)
        elif kind == "alpha_vantage_news":
            articles = parse_alpha_vantage_news(payload, source)
        elif kind == "finnhub_news":
            articles = parse_finnhub_news(payload, source)
        elif kind == "json_records":
            articles = parse_json_records(payload, source)
        elif kind == "html_links":
            articles = parse_html_links(payload, source)
        elif kind == "singstat_table":
            articles = parse_singstat_table(payload, source)
        elif kind == "ssb_jsonstat2_cpi_yoy":
            articles = parse_ssb_jsonstat2_cpi_yoy(payload, source)
        elif kind == "denmark_statbank_cpi_yoy":
            articles = parse_denmark_statbank_cpi_yoy(payload, source)
        elif kind == "hong_kong_censtatd_cpi_yoy":
            articles = parse_hong_kong_censtatd_cpi_yoy(payload, source)
        elif kind == "denmark_statistics_release_calendar":
            articles, pagination_state = (
                fetch_denmark_statistics_release_calendar_pages(
                    payload,
                    source,
                    headers=headers,
                    timeout_sec=timeout_sec,
                    maximum_bytes=maximum_bytes,
                    now=now,
                )
            )
            updated.update(pagination_state)
        elif kind == "hong_kong_censtatd_release_calendar_xlsx":
            articles = parse_hong_kong_censtatd_release_calendar_xlsx(payload, source)
        elif kind in {
            "ksh_release_calendar_exact",
            "ksh_release_calendar_verified_rule_exact_v3",
        }:
            rule_sha256, rule_bytes, rule_basis = verify_content_addressed_release_rule(
                source,
                timeout_sec=timeout_sec,
            )
            source["release_time_rule_observed_sha256"] = rule_sha256
            source["release_rule_bytes_verified"] = True
            updated.update(
                {
                    "release_time_rule_observed_sha256": rule_sha256,
                    "release_time_rule_observed_bytes": rule_bytes,
                    "release_time_rule_verification_basis": rule_basis,
                    "release_time_rule_verified_utc": iso_utc(now),
                }
            )
            articles = parse_ksh_release_calendar_exact(payload, source)
        elif kind == "bot_sdds_cpi_index":
            articles = parse_bot_sdds_cpi_index(payload, source)
        elif kind == "banxico_inflation_snapshot":
            articles = parse_banxico_inflation_snapshot(payload, source)
        elif kind == "sarb_homepage_rates":
            articles = parse_sarb_homepage_rates(payload, source)
        elif kind == "cnb_homepage_inflation":
            articles = parse_cnb_homepage_inflation(payload, source)
        elif kind == "ksh_prices_snapshot":
            articles = parse_ksh_prices_snapshot(payload, source)
        elif kind == "ksh_ppi_release_snapshot":
            history_url = canonical_url(source.get("history_table_url"))
            if not history_url or not trusted_host(
                history_url, source.get("trusted_domains") or ()
            ):
                raise ValueError("KSH PPI history table URL is missing or untrusted")
            history_headers = {
                str(key): str(value)
                for key, value in headers.items()
                if str(key).lower() not in {"if-none-match", "if-modified-since"}
            }
            history_headers["Accept"] = "text/csv,application/octet-stream"
            try:
                history_request = urllib.request.Request(
                    history_url, headers=history_headers
                )
                with urllib.request.urlopen(
                    history_request,
                    timeout=timeout_sec,
                    context=TLS_CONTEXT,
                ) as history_response:
                    history_payload = history_response.read(maximum_bytes + 1)
                    history_status = int(history_response.status)
            except (OSError, urllib.error.HTTPError) as exc:
                raise ValueError(f"KSH PPI history fetch failed: {exc}") from exc
            if len(history_payload) > maximum_bytes:
                raise ValueError("KSH PPI history payload exceeds configured limit")
            updated["history_table_status"] = history_status
            updated["history_table_bytes"] = len(history_payload)
            updated["history_table_sha256"] = hashlib.sha256(
                history_payload
            ).hexdigest()
            articles = parse_ksh_ppi_release_snapshot(
                payload, history_payload, source
            )
        elif kind == "scb_cpi_snapshot":
            articles = parse_scb_cpi_snapshot(payload, source)
        elif kind == "tuik_press_indicators":
            articles = parse_tuik_press_indicators(payload, source)
        elif kind == "rbnz_ocr_snapshot":
            articles = parse_rbnz_ocr_snapshot(payload, source)
        elif kind == "umich_current_release":
            articles = parse_umich_current_release(payload, source)
        elif kind == "bls_current_release":
            articles = parse_bls_current_release(payload, source)
        elif kind == "bls_employment_current_release":
            articles = parse_bls_employment_current_release(payload, source)
        elif kind == "statcan_major_indicators":
            articles = parse_statcan_major_indicators(payload, source)
        elif kind == "japan_cpi_csv":
            articles = parse_japan_cpi_csv(payload, source)
        elif kind == "japan_cpi_current_summary":
            articles = parse_japan_cpi_current_summary(payload, source)
        elif kind in {"", "rss", "atom"}:
            articles = parse_rss(payload, source)
        else:
            raise ValueError(f"unsupported source kind: {kind}")
    except SourceContentPending as exc:
        # Preserve the last successfully parsed listing/bootstrap boundary.
        if "last_success_utc" in source_state:
            updated["last_success_utc"] = source_state["last_success_utc"]
        else:
            updated.pop("last_success_utc", None)
        updated["source_body_retry_required"] = True
        updated.update(
            last_parse_status="pending_source_content", last_error="", consecutive_errors=0,
            parsed_items=0, source_content_state={"status": exc.reason, **exc.details,
                                               "observed_utc": iso_utc(now)},
        )
        return [], updated
    except (ValueError, ET.ParseError, json.JSONDecodeError, UnicodeError) as exc:
        updated["last_parse_status"] = "error"
        updated["last_error"] = f"parse_error: {exc}"[:500]
        updated["consecutive_errors"] = int(source_state.get("consecutive_errors") or 0) + 1
        updated["source_body_retry_required"] = True
        if "last_success_utc" in source_state:
            updated["last_success_utc"] = source_state["last_success_utc"]
        else:
            updated.pop("last_success_utc", None)
        provider_retry = provider_parse_retry_after(kind, exc, now)
        if provider_retry:
            updated["retry_after_utc"] = provider_retry
        return [], updated
    updated.pop("source_body_retry_required", None)
    updated["last_parse_status"] = "parsed"
    updated["last_parse_success_utc"] = iso_utc(now)
    updated.pop("source_content_state", None)
    rss_listing_bootstrap = bool(
        kind == "rss"
        and source.get("bootstrap_existing_items")
        and (
            not source_state.get("last_success_utc")
            or source_contract_changed
        )
    )
    if rss_listing_bootstrap:
        # A newly configured search/RSS source usually returns a back catalog.
        # Those items were not known at their publication times and must not
        # be stamped as fresh forward evidence merely because this collector
        # observed the feed for the first time.  Retain them as provenance and
        # negative-control context; later unseen feed items remain prospective.
        for article in articles:
            article["source_listing_bootstrap"] = True
        updated["bootstrap_item_count"] = len(articles)
        updated["bootstrap_completed_utc"] = iso_utc(now)
    for article in articles:
        article["directional_research_only"] = bool(
            source.get("directional_research_only")
        )
    if kind == "html_links":
        updated["known_item_urls"] = annotate_html_listing_history(
            articles,
            source_state,
            listing_bootstrap=listing_bootstrap,
        )
        updated["bootstrap_item_urls"] = retained_html_listing_bootstrap_urls(
            articles,
            source_state,
        )
    if kind in {
        "umich_current_release",
        "bls_current_release",
        "bls_employment_current_release",
        "statcan_major_indicators",
        "singstat_table",
        "ssb_jsonstat2_cpi_yoy",
        "denmark_statbank_cpi_yoy",
        "hong_kong_censtatd_cpi_yoy",
        "bot_sdds_cpi_index",
        "banxico_inflation_snapshot",
        "sarb_homepage_rates",
        "cnb_homepage_inflation",
        "ksh_prices_snapshot",
        "ksh_ppi_release_snapshot",
        "scb_cpi_snapshot",
        "tuik_press_indicators",
        "rbnz_ocr_snapshot",
        "japan_cpi_current_summary",
    }:
        updated["known_release_ids"] = annotate_singleton_release_history(
            articles,
            source_state,
        )
    if source.get("detail_enrichment"):
        detail_count, detail_times, detail_error = (
            enrich_recent_official_release_details(
                articles,
                source,
                source_state,
                timeout_sec=timeout_sec,
                maximum_bytes=maximum_bytes,
                now=now,
            )
        )
        updated["detail_enriched_items"] = detail_count
        updated["detail_first_seen_utc_by_url"] = detail_times
        updated["last_detail_error"] = detail_error
        content_hashes = dict(
            source_state.get("detail_content_sha256_by_url") or {}
        )
        placeholder_states = dict(
            source_state.get("detail_content_is_placeholder_by_url") or {}
        )
        version_counts = dict(
            source_state.get("detail_content_version_count_by_url") or {}
        )
        for article in articles:
            detail_url = canonical_url(article.get("url"))
            content_hash = clean_text(
                article.pop("_detail_state_content_sha256", "")
            )
            placeholder_state = article.pop(
                "_detail_state_is_placeholder",
                None,
            )
            version_count = article.pop("_detail_state_version_count", None)
            if not detail_url or not content_hash:
                continue
            content_hashes[detail_url] = content_hash
            placeholder_states[detail_url] = placeholder_state is True
            version_counts[detail_url] = max(1, int(safe_float(version_count, 1)))
        retained_detail_urls = set(detail_times)
        updated["detail_content_sha256_by_url"] = {
            str(key): clean_text(value)
            for key, value in content_hashes.items()
            if str(key) in retained_detail_urls and clean_text(value)
        }
        updated["detail_content_is_placeholder_by_url"] = {
            str(key): value is True
            for key, value in placeholder_states.items()
            if str(key) in retained_detail_urls
        }
        updated["detail_content_version_count_by_url"] = {
            str(key): max(1, int(safe_float(value, 1)))
            for key, value in version_counts.items()
            if str(key) in retained_detail_urls
        }
    updated["parsed_items"] = len(articles)
    return articles, updated


def burst_poll_active(source: Mapping[str, Any], now: dt.datetime) -> bool:
    burst_windows = source.get("burst_poll_windows")
    if not isinstance(burst_windows, list) or not burst_windows:
        return False
    timezone_name = clean_text(source.get("poll_timezone")) or "UTC"
    try:
        local_now = now.astimezone(ZoneInfo(timezone_name))
    except (KeyError, ValueError):
        local_now = now.astimezone(UTC)
    if local_now.weekday() >= 5:
        return False
    minute_of_day = local_now.hour * 60 + local_now.minute
    for window in burst_windows:
        if not isinstance(window, Mapping):
            continue
        local_date = clean_text(window.get("local_date"))
        if local_date and local_date != local_now.date().isoformat():
            continue
        start_text = clean_text(window.get("start"))
        end_text = clean_text(window.get("end"))
        if not re.fullmatch(r"\d{2}:\d{2}", start_text) or not re.fullmatch(
            r"\d{2}:\d{2}", end_text
        ):
            continue
        start_hour, start_minute = (int(part) for part in start_text.split(":"))
        end_hour, end_minute = (int(part) for part in end_text.split(":"))
        if (
            start_hour * 60 + start_minute
            <= minute_of_day
            <= end_hour * 60 + end_minute
        ):
            return True
    return False


def collection_order(
    sources: Sequence[Mapping[str, Any]],
    now: dt.datetime,
) -> list[Mapping[str, Any]]:
    """Put explicitly scheduled release-window sources first, stably.

    The collector remains single-threaded and deterministic, but a known 08:30
    official release should not wait behind two dozen unrelated endpoints.
    Outside declared burst windows the configured source order is unchanged.
    """

    indexed = list(enumerate(sources))
    indexed.sort(key=lambda item: (0 if burst_poll_active(item[1], now) else 1, item[0]))
    return [source for _, source in indexed]


def due_for_poll(
    source: Mapping[str, Any],
    source_state: Mapping[str, Any],
    policy: Mapping[str, Any],
    now: dt.datetime,
) -> bool:
    retry_after = parse_datetime(source_state.get("retry_after_utc"))
    if retry_after and retry_after > now:
        return False
    configured_contract = clean_text(source.get("source_contract_id"))
    observed_contract = clean_text(source_state.get("source_contract_id"))
    if configured_contract and configured_contract != observed_contract:
        # A parser/source-contract repair must receive one immediate attempt;
        # inheriting the obsolete adapter's normal or error cadence can leave
        # a repaired high-impact release path falsely degraded for hours.
        return True
    observed_collector_contract = clean_text(
        source_state.get("collector_contract_id")
    )
    if (
        observed_collector_contract
        and observed_collector_contract != COLLECTOR_CONTRACT_ID
    ):
        # Source configuration can be reloaded by an older long-running
        # process before that process has loaded the accompanying parser code.
        # Bind successful poll state to the collector code cohort so the new
        # process immediately retries instead of accepting that mixed cohort.
        return True
    kind = str(source.get("kind") or "rss").lower()
    configured_transport = clean_text(source.get("http_transport")).lower() or "urllib"
    observed_transport = clean_text(source_state.get("http_transport")).lower()
    if configured_transport != (observed_transport or "urllib"):
        # A repaired transport receives one immediate attempt instead of
        # inheriting the retired adapter's exponential error backoff.
        return True
    if kind == "recurring_release_calendar" and (
        not source_state.get("last_success_utc")
        or source_state.get("last_error")
        or int(safe_float(source_state.get("parsed_items"), 0.0)) <= 0
    ):
        # A source may retain a prior HTTP failure after being migrated from a
        # blocked web page, or a false zero-row success from an older adapter,
        # after migration to deterministic official-rule materialization. Give
        # the local adapter an immediate chance to replace that stale state;
        # normal poll cadence resumes after its first nonempty success.
        return True
    last_attempt = parse_datetime(source_state.get("last_attempt_utc"))
    if last_attempt is None:
        return True
    default_interval = safe_float(
        policy.get("gdelt_poll_interval_sec")
        if kind == "gdelt"
        else policy.get("rss_poll_interval_sec"),
        600.0 if kind == "gdelt" else 180.0,
    )
    interval = safe_float(source.get("poll_interval_sec"), default_interval)
    if burst_poll_active(source, now):
        interval = safe_float(source.get("burst_poll_interval_sec"), interval)
    errors = int(source_state.get("consecutive_errors") or 0)
    if errors:
        if kind == "gdelt":
            # GDELT is a supplementary discovery index and may answer 429
            # without Retry-After.  Scale from its comparatively long poll
            # interval so the generic 30-second floor cannot become a no-op.
            interval = min(3600.0, interval * (2 ** min(errors, 2)))
        elif kind == "bls_timeseries_batch":
            interval = min(3600.0, interval * (2 ** min(errors, 5)))
        else:
            interval = max(interval, min(3600.0, 30.0 * (2 ** min(errors, 7))))
    return (now - last_attempt).total_seconds() >= interval


def phrase_expression(phrase: str) -> str:
    """Match policy phrases across spaces/hyphens and Japanese text safely."""

    if any(ord(character) > 127 for character in phrase):
        return re.escape(phrase)
    return (
        r"(?<!\w)"
        + re.escape(phrase).replace(r"\ ", r"[\s-]+")
        + r"(?!\w)"
    )


def phrase_total(text: str, terms: Mapping[str, float]) -> float:
    total = 0.0
    for phrase, weight in terms.items():
        expression = phrase_expression(phrase)
        match = re.search(expression, text, flags=re.I)
        if match is None:
            continue
        preceding = text[max(0, match.start() - 48) : match.start()]
        if re.search(
            r"\b(?:"
            r"not|no|unlikely to|rules out|ruled out|"
            r"far from|a long way from|some way off|not ready for|"
            r"little chance of|low odds of"
            r")(?:\s+(?:a|an|the))?\s+$",
            preceding,
            flags=re.I,
        ):
            continue
        following = text[match.end() : match.end() + 48]
        if re.search(
            r"^\s*(?:now\s+)?"
            r"(?:(?:looks?|seems?|appears?)\s+)?"
            r"(?:very\s+)?(?:unlikely|less likely|not likely|ruled out|"
            r"off the table|doubtful)\b",
            following,
            flags=re.I,
        ):
            continue
        total += weight
    return total


def phrase_present(text: str, phrase: str) -> bool:
    expression = phrase_expression(phrase)
    return re.search(expression, text, flags=re.I) is not None


def policy_assertion_status(
    title: str,
    *,
    source_verified: bool,
) -> tuple[str, float]:
    """Separate an observed policy action from previews and hypotheticals."""

    date_only_title = bool(
        re.fullmatch(
            r"\s*\d{1,2}\s+(?:January|February|March|April|May|June|July|"
            r"August|September|October|November|December)\s+20\d{2}\s*",
            clean_text(title),
            flags=re.I,
        )
    )
    if any(re.search(pattern, title, flags=re.I) for pattern in POLICY_NEUTRAL_PATTERNS):
        return "neutral_or_expected_hold", 0.0
    if POLICY_DATA_DEPENDENT_GUIDANCE_PATTERN.search(title):
        # A speaker saying that a future release will determine their stance
        # does not reveal the sign of that future release or policy decision.
        # Preserve it as conditional context, with no currency impulse even
        # when the quote is carried by an official transport.
        return (
            "official_conditional" if source_verified else "unverified_speculation",
            0.0,
        )
    if (
        (
            not date_only_title
            and (
                POLICY_SPECULATION_PATTERN.search(title)
                or POLICY_CONDITIONAL_ACTION_PATTERN.search(title)
            )
        )
        or re.search(
            r"\b(?:economic|data|inflation|policy)\s+calendar\b|"
            r"\b(?:main|key)\s+focus\b.{0,48}\b(?:today|this week)\b",
            title,
            flags=re.I,
        )
    ):
        # An official conditional remark is useful context. A third-party
        # headline preview is not a confirmed policy impulse.
        return (
            "official_conditional" if source_verified else "unverified_speculation",
            0.35 if source_verified else 0.0,
        )
    return "asserted", 1.0


def extract_currencies(
    text: str,
    source_currencies: Sequence[str] = (),
) -> list[str]:
    found = {str(value).upper() for value in source_currencies if value}
    lowered = text.lower()
    for currency, patterns in CURRENCY_ALIASES.items():
        if any(re.search(pattern, lowered, flags=re.I) for pattern in patterns):
            found.add(currency)
    # Preserve both legs of explicit six-letter or separated FX pair symbols.
    # Word-boundary alias rules intentionally avoid matching a code embedded in
    # another token, so a headline-leading ``EURUSD`` otherwise contributes no
    # EUR leg and can inherit only the later policy currency from its recap.
    code_pattern = "|".join(sorted(CURRENCY_ALIASES))
    for match in re.finditer(
        rf"\b({code_pattern})\s*[/_-]?\s*({code_pattern})\b",
        lowered,
        flags=re.I,
    ):
        base, quote = (part.upper() for part in match.groups())
        if base != quote:
            found.update((base, quote))
    # A bare ``dollar`` is intentionally not a USD alias: many countries use
    # dollars and discovery headlines include local-dollar stories.  It is
    # unambiguous enough when the same text explicitly describes U.S. economic
    # or Federal Reserve context.
    if (
        "USD" not in found
        and re.search(r"\bdollar\b", lowered, flags=re.I)
        and re.search(
            r"\b(?:u\.?s\.?|united states)\s+economic\b|"
            r"\b(?:federal reserve|fomc|the fed)\b",
            lowered,
            flags=re.I,
        )
    ):
        found.add("USD")
    if "USD" not in found and re.search(
        r"(?:\bdollars?\b\s*/\s*|/\s*(?:u\.?s\.?\s+|us\s+)?dollars?\b)",
        lowered,
        flags=re.I,
    ):
        # A bare ``dollar`` is ambiguous in prose, but not on one side of an
        # explicit currency-pair slash (for example, ``Dollar/Yen``).
        found.add("USD")
    # Aggregator headlines frequently shorten the institution to bare "Fed".
    # Require nearby policy language so ordinary uses of the word "fed" do not
    # become USD observations.  U.S. Treasury securities are themselves a
    # direct rates transmission channel and are unambiguous USD context.
    if "USD" not in found and (
        (
            re.search(r"\bfed\b", lowered, flags=re.I)
            and re.search(
                r"\b(?:rates?|cuts?|hikes?|policy|fomc|chair|governor|inflation)\b",
                lowered,
                flags=re.I,
            )
        )
        or re.search(
            r"\b(?:u\.?s\.?|united states)\s+treasur(?:y|ies)\b",
            lowered,
            flags=re.I,
        )
    ):
        found.add("USD")
    return sorted(found)


def external_policy_subject_currencies(headline: str) -> list[str]:
    """Return the policy target, not the nationality of a commentator.

    Secondary headlines often lead with a U.S. official and then describe a
    policy action that belongs to another authority (for example, Treasury
    Secretary Bessent calling on Japan to raise rates and support the yen).
    Treating every named authority as a directional leg created a synthetic
    USD factor from a JPY thesis.  This deliberately narrow rule requires both
    a directive/expectation verb and an explicit policy or currency action in
    the target clause.  Ambiguous or multi-currency targets remain unbound.
    """

    value = clean_text(headline)
    match = re.search(
        r"\b(?:calls?\s+on|urges?|expects?)\b(?P<target>.{0,220})$",
        value,
        flags=re.I,
    )
    if match is None:
        return []
    target = clean_text(match.group("target"))
    if not re.search(
        r"\b(?:interest\s+rates?|policy\s+rates?|rate\s+hikes?|"
        r"rais(?:e|es|ed|ing)\s+rates?|cut(?:s|ting)?\s+rates?|"
        r"central\s+bank|boj|boost\s+(?:the\s+)?yen|stronger\s+yen|"
        r"support\s+(?:the\s+)?yen)\b",
        target,
        flags=re.I,
    ):
        return []
    currencies = extract_currencies(target)
    return currencies if len(currencies) == 1 else []


def secondary_release_subject_currencies(headline: str) -> list[str]:
    """Bind a release-shaped secondary headline to one named economy.

    A long article body may discuss comparison countries, policy implications,
    commodities and prior stories.  Those are context, not additional release
    subjects.  Binding is intentionally limited to one currency named in a
    headline that clearly identifies a statistical release family; multi-
    economy roundups abstain rather than choosing a subject.
    """

    value = clean_text(headline)
    if not re.search(
        r"\b(?:pmi|purchasing\s+managers(?:'|’)?\s+index|cpi|ppi|"
        r"consumer\s+prices?|producer\s+prices?|inflation|"
        r"unemployment|employment|payrolls?|retail\s+sales?|"
        r"industrial\s+production|gdp|gross\s+domestic\s+product|"
        r"trade\s+balance|current\s+account|machine(?:ry)?\s+orders?)\b",
        value,
        flags=re.I,
    ):
        return []
    if not re.search(
        r"\b(?:index|release|report|data|figures?|actual|expected|"
        r"hit(?:s)?|post(?:s|ed)?|rise(?:s|n)?|rose|fall(?:s|en)?|fell|"
        r"expand(?:s|ed|ing)?|contract(?:s|ed|ing)?|"
        r"accelerat(?:e|es|ed|ing)|slow(?:s|ed|ing)?|"
        r"surge(?:s|d)?|drop(?:s|ped)?|\d+(?:\.\d+)?\s*%?)\b",
        value,
        flags=re.I,
    ):
        return []
    currencies = extract_currencies(value)
    return currencies if len(currencies) == 1 else []


def extract_policy_currencies(text: str) -> list[str]:
    return sorted(
        currency
        for currency, patterns in POLICY_INSTITUTION_PATTERNS.items()
        if any(re.search(pattern, text, flags=re.I) for pattern in patterns)
    )


TOPIC_ENTITY_PATTERNS: dict[str, tuple[str, ...]] = {
    "middle_east": (
        r"\biran(?:ian)?\b",
        r"\bisrael(?:i)?\b",
        r"\bmiddle east\b",
        r"\bstrait of hormuz\b",
    ),
    "russia_ukraine": (r"\brussia(?:n)?\b", r"\bukrain(?:e|ian)\b"),
    "china": (r"\bchina\b", r"\bchinese\b", r"\bpbo[cC]\b"),
    "united_states": (r"\bu\.?s\.?\b", r"\bunited states\b", r"\bfederal reserve\b"),
    "euro_area": (r"\beuro area\b", r"\beurozone\b", r"\beuropean central bank\b"),
    "oil": (r"\boil\b", r"\bcrude\b", r"\bpetroleum\b"),
    "tariffs": (r"\btariff", r"\btrade war\b"),
}


INTERVENTION_UNCERTAINTY_PATTERN = re.compile(
    r"\b(?:rumou?r(?:s|ed)?|suspect(?:ed|s)?|speculat(?:ion|ive)|"
    r"possible|possibly|may have|might have|appears? to have|"
    r"looks? like|reportedly|unconfirmed|stoking)\b",
    flags=re.I,
)
INTERVENTION_NEGATED_ACTION_PATTERN = re.compile(
    r"\b(?:did|does|do|has|have|had|may|might|could)\s+not\s+"
    r"(?:have\s+)?interven(?:e|es|ed|ing)\b|"
    r"\bno\s+(?:fx\s+|foreign[- ]exchange\s+|currency\s+)?intervention\b",
    flags=re.I,
)
INTERVENTION_OFFICIAL_ACTION_PATTERN = re.compile(
    r"\b(?:confirmed?|conduct(?:ed|s)?|carr(?:y|ies|ied) out|"
    r"interven(?:e|es|ed|ing)|bought|sold|spend|spends|spent)\b",
    flags=re.I,
)
INTERVENTION_COORDINATED_ACTION_PATTERN = re.compile(
    r"\b(?:joint|coordinated)\b.{0,32}\b"
    r"(?:currency|foreign[- ]exchange|forex)\b.{0,24}\b"
    r"(?:action|operation)s?\b|"
    r"\b(?:currency|foreign[- ]exchange|forex)\s+actions?\b"
    r".{0,48}\bcounter(?:s|ed|ing)?\b.{0,24}\b"
    r"(?:disorderly|excessive)\b.{0,24}\bmovements?\b",
    flags=re.I,
)
INTERVENTION_WARNING_PATTERN = re.compile(
    r"\b(?:ready to|prepared to|will not rule out|appropriate action|"
    r"excessive moves?|one-sided moves?|closely watching)\b",
    flags=re.I,
)
INTERVENTION_MOVE_PATTERN = re.compile(
    r"\b(?:gain(?:s|ed|ing)?|advanc(?:e|es|ed|ing)|climb(?:s|ed|ing)?|"
    r"ris(?:e|es|en|ing)|"
    r"firm(?:s|ed|ing)?|support(?:s|ed|ing)?|appreciat(?:e|es|ed|ing)|"
    r"surg(?:e|es|ed|ing)|jump(?:s|ed|ing)?|rall(?:y|ies|ied|ying)|"
    r"strengthen(?:s|ed|ing)?|stronger|soar(?:s|ed|ing)?|rocket(?:s|ed|ing)?|"
    r"fall(?:s|en)?|fell|slid(?:e|es)?|weaken(?:s|ed)?|weaker|"
    r"pressur(?:e|es|ed|ing)|crack(?:s|ed|ing)?|"
    r"depreciat(?:e|es|ed|ing)|declin(?:e|es|ed|ing)|"
    r"drop(?:s|ped|ping)?|tumbl(?:e|es|ed|ing)|plung(?:e|es|ed|ing))\b",
    flags=re.I,
)
RETROSPECTIVE_EXPLAINER_PATTERN = re.compile(
    r"\b(?:here(?:'|’)?s\s+what\s+to\s+know|what\s+you\s+need\s+to\s+know|"
    r"what\s+to\s+know|explainer|explained|recap|timeline)\b",
    flags=re.I,
)
RETROSPECTIVE_INTERVENTION_CAUSAL_PATTERN = re.compile(
    r"\b(?:"
    r"(?:rate[- ]hike\s+signal|remarks?|signal)\b.{0,72}\bled\b.{0,72}\bintervention|"
    r"\bwas\s+(?:the\s+)?(?:deciding|key|main)\s+factor\b.{0,96}\bintervention|"
    r"\bfirst\s+(?:coordinated\s+)?intervention\s+in\s+\d+\s+years\b"
    r")",
    flags=re.I,
)
REPORTED_MARKET_MOVE_HEADLINE_PATTERN = re.compile(
    r"^(?:(?:morning\s+bid|market\s+wrap|asia\s+morning\s+bid):\s*)?"
    r"(?:the\s+)?(?:u\.?s\.?\s+|global\s+|most\s+)?"
    r"(?:(?:asian|asia-pacific|european|japanese|chinese|gulf|indian|"
    r"australian|canadian|british|new\s+zealand|hong\s+kong|singaporean|"
    r"south\s+african|mexican|norwegian|swedish|swiss|thai|turkish|"
    r"polish|czech|hungarian|danish)\s+)?"
    r"(?:markets?|wall\s+street|equity\s+markets?|futures?|stocks?|shares?|equities|commodit(?:y|ies)|dow|s&p|nasdaq|"
    r"(?:azeri\s+light\s+)?oil(?:\s+prices?)?(?:\s+and\s+(?:yields?|bonds?|stocks?|shares?))?|"
    r"crude|brent|wti|gold(?:\s*,\s*silver)?|silver(?:\s*,\s*gold)?|copper|"
    r"bonds?|yields?|treasur(?:y|ies)|currenc(?:y|ies)|"
    r"dollars?|greenback|yen|euro(?![- ]area)|pounds?|sterling|francs?|pesos?|rupees?|"
    r"yuan|renminbi|kron(?:e|a)|rand|lira)\b.{0,96}\b"
    r"(?:gain(?:s|ed|ing)?|advanc(?:e|es|ed|ing)|climb(?:s|ed|ing)?|"
    r"ris(?:e|es|en|ing)|mix(?:es|ed|ing)|dip(?:s|ped|ping)?|"
    r"firm(?:s|ed|ing)?|surg(?:e|es|ed|ing)|jump(?:s|ed|ing)?|"
    r"rall(?:y|ies|ied|ying)|strengthen(?:s|ed|ing)?|stronger|"
    r"march(?:es|ed|ing)?|"
    r"head(?:s|ed|ing)?\s+(?:higher|lower|toward(?:s)?|for)|"
    r"recover(?:s|ed|ing)?|rebound(?:s|ed|ing)?|retreat(?:s|ed|ing)?|"
    r"eas(?:e|es|ed|ing)|stead(?:y|ies|ied|ying)|"
    r"(?:end|close)(?:s|d|ed|ing)?\s+(?:higher|lower)|"
    r"trad(?:e|es|ed|ing)\s+(?:flat|higher|lower)|"
    r"tread(?:s|ing)?\s+water|cling(?:s|ing)?\s+to\s+(?:gains?|losses?)|"
    r"soar(?:s|ed|ing)?|sink(?:s|ing)?|sank|sunk|"
    r"up|higher|fall(?:s|en)?|fell|down|lower|"
    r"claw(?:s|ed|ing)?\s+back|slip(?:s|ped|ping)?|"
    r"slid(?:e|es)?|weaken(?:s|ed|ing)?|weaker|crack(?:s|ed|ing)?|declin(?:e|es|ed|ing)|"
    r"drop(?:s|ped|ping)?|tumbl(?:e|es|ed|ing)|"
    r"plung(?:e|es|ed|ing)|add(?:s|ed|ing)?)\b",
    flags=re.I,
)
SECONDARY_REPORTED_COMMODITY_STATE_PATTERN = re.compile(
    r"\b(?:oil|crude|brent|wti|natural\s+gas|gold|silver|copper)\b"
    r".{0,64}\b(?:head(?:s|ed|ing)?\s+(?:toward(?:s)?|for)|"
    r"on\s+track\s+for)\b.{0,40}\b(?:weekly|monthly)\s+"
    r"(?:rise|gain|fall|drop)\b",
    flags=re.I,
)
LIVE_MULTI_ASSET_RECAP_HEADLINE_PATTERN = re.compile(
    r"^(?:live\b|market\s+live\b).{0,320}\b"
    r"(?:check\s+(?:the\s+)?latest\s+move|"
    r"why\s+(?:are\s+)?stocks?\s+(?:up|down)\s+today)\b",
    flags=re.I,
)
REPORTED_CURRENCY_RELATIVE_PERFORMANCE_HEADLINE_PATTERN = re.compile(
    r"\b(?:AUD|CAD|CHF|CNH|CZK|DKK|EUR|GBP|HKD|HUF|JPY|MXN|NOK|NZD|"
    r"PLN|SEK|SGD|THB|TRY|USD|ZAR)"
    r"(?:\s*(?:,|and|&)\s*(?:AUD|CAD|CHF|CNH|CZK|DKK|EUR|GBP|HKD|"
    r"HUF|JPY|MXN|NOK|NZD|PLN|SEK|SGD|THB|TRY|USD|ZAR)){0,3}\s+"
    r"(?:outperform|underperform)(?:s|ed|ing)?\b",
    flags=re.I,
)
REPORTED_FX_PAIR_EXTREME_HEADLINE_PATTERN = re.compile(
    r"^(?:aud|cad|chf|cnh|czk|dkk|eur|gbp|hkd|huf|jpy|mxn|nok|nzd|pln|"
    r"sek|sgd|thb|try|usd|zar)[/_-]?"
    r"(?:aud|cad|chf|cnh|czk|dkk|eur|gbp|hkd|huf|jpy|mxn|nok|nzd|pln|"
    r"sek|sgd|thb|try|usd|zar)\b.{0,64}\b"
    r"(?:trad(?:e|es|ed|ing)|mov(?:e|es|ed|ing)|hit(?:s|ting)?|reach(?:es|ed|ing)?|"
    r"set(?:s|ting)?|ris(?:e|es|en|ing)|fall(?:s|en|ing)?)\s+"
    r"(?:to|at|near)\s+(?:a\s+)?(?:new\s+)?"
    r"(?:(?:daily|weekly|monthly|yearly|session|\d+[- ](?:day|week|month|year))[- ]+)?"
    r"(?:highs?|lows?)\b",
    flags=re.I,
)
REPORTED_FX_PAIR_PRICE_STATE_HEADLINE_PATTERN = re.compile(
    r"^(?:aud|cad|chf|cnh|czk|dkk|eur|gbp|hkd|huf|jpy|mxn|nok|nzd|pln|"
    r"sek|sgd|thb|try|usd|zar)[/_-]?"
    r"(?:aud|cad|chf|cnh|czk|dkk|eur|gbp|hkd|huf|jpy|mxn|nok|nzd|pln|"
    r"sek|sgd|thb|try|usd|zar)\b.{0,64}\b"
    r"(?:stead(?:y|ies|ied|ying)|hover(?:s|ed|ing)?|hold(?:s|ing)?|"
    r"trad(?:e|es|ed|ing))\b.{0,32}\b(?:near|around|at)\s+\d",
    flags=re.I,
)
REPORTED_FX_PAIR_TECHNICAL_BREAKOUT_HEADLINE_PATTERN = re.compile(
    r"^(?:aud|cad|chf|cnh|czk|dkk|eur|gbp|hkd|huf|jpy|mxn|nok|nzd|pln|"
    r"sek|sgd|thb|try|usd|zar)[/_-]?"
    r"(?:aud|cad|chf|cnh|czk|dkk|eur|gbp|hkd|huf|jpy|mxn|nok|nzd|pln|"
    r"sek|sgd|thb|try|usd|zar)\b.{0,48}\b"
    r"(?:gain(?:s|ed|ing)?|advanc(?:e|es|ed|ing)|climb(?:s|ed|ing)?|"
    r"ris(?:e|es|en|ing)|surge(?:s|d|ing)?|jump(?:s|ed|ing)?|"
    r"rall(?:y|ies|ied|ying)|fall(?:s|en|ing)?|fell|"
    r"slip(?:s|ped|ping)?|slid(?:e|es)?|declin(?:e|es|ed|ing)|"
    r"drop(?:s|ped|ping)?|plung(?:e|es|ed|ing)|"
    r"break(?:s|ing)?|broke)\b.{0,36}\b"
    r"(?:above|below|over|under|through|past|moving\s+average|"
    r"\d+[- ]day\s+ma|support|resistance|technical\s+levels?|"
    r"makes?\s+a\s+break\s+for\s+it)\b",
    flags=re.I,
)
REPORTED_INLINE_COMMODITY_MOVE_PATTERN = re.compile(
    r"(?:—|–|;|:)\s*.{0,32}\b"
    r"(?:oil(?:\s+prices?)?|crude(?:\s+oil)?|brent|wti|gold|silver)\b"
    r".{0,48}\b(?:gain(?:s|ed|ing)?|advanc(?:e|es|ed|ing)|"
    r"climb(?:s|ed|ing)?|ris(?:e|es|en|ing)|surge(?:s|d|ing)?|"
    r"jump(?:s|ed|ing)?|rall(?:y|ies|ied|ying)|soar(?:s|ed|ing)?|"
    r"fall(?:s|en|ing)?|fell|slip(?:s|ped|ping)?|slid(?:e|es)?|"
    r"declin(?:e|es|ed|ing)|drop(?:s|ped|ping)?|"
    r"tumbl(?:e|es|ed|ing)|plung(?:e|es|ed|ing)|"
    r"extend(?:s|ed|ing)?\s+(?:a\s+|the\s+)?(?:gain|rise|rally|decline|fall|drop))\b",
    flags=re.I,
)
REPORTED_COMMODITY_SUPPLY_PRICE_RECAP_PATTERN = re.compile(
    r"\b(?:oil|crude(?:\s+oil)?|brent|wti)\s+"
    r"(?:supply|supplies|shipments?|exports?|flows?|offers?)\b.{0,96}\b"
    r"prices?\s+(?:rise(?:s|n)?|rose|surge(?:s|d)?|"
    r"jump(?:s|ed)?|rall(?:y|ies|ied)|soar(?:s|ed)?|"
    r"fall(?:s|en)?|fell|drop(?:s|ped)?|slip(?:s|ped)?|"
    r"declin(?:e|es|ed)|tumble(?:s|d)|plunge(?:s|d))\b",
    flags=re.I,
)
REPORTED_LOCAL_FUEL_MARKET_MOVE_PATTERN = re.compile(
    r"^(?:[a-z][a-z.-]*\s+){0,4}fuel\s+prices?\b.{0,96}\b"
    r"(?:stead(?:y|ies|ied|ying)|ris(?:e|es|en|ing)|rose|"
    r"fall(?:s|en|ing)?|fell|climb(?:s|ed|ing)?|"
    r"drop(?:s|ped|ping)?|slip(?:s|ped|ping)?)\b",
    flags=re.I,
)
NON_CATALYST_CONTEXT_PATTERN = re.compile(
    r"\b(?:not\s+(?:new|fresh)\s+information|nothing\s+new|"
    r"already\s+(?:fully\s+)?priced\s+in|(?:largely|mostly)\s+priced\s+in|"
    r"reaction\s+(?:should|is\s+likely\s+to)\s+be\s+muted|"
    r"no\s+material\s+(?:change|market\s+impact)|"
    r"(?:benefit(?:s|ed|ting)?|reap(?:s|ed|ing)?\b.{0,32}\b(?:profits?|"
    r"billions?|gains?))\b.{0,80}\b(?:amid|from)\b.{0,80}"
    r"\b(?:war|conflict|supply\s+disruption))\b",
    flags=re.I,
)
SECONDARY_MARKET_POLICY_EXPECTATION_PATTERN = re.compile(
    r"\b(?:traders?|markets?|investors?)\b.{0,40}"
    r"\bbrac(?:e|es|ed|ing)\b.{0,80}"
    r"\b(?:hawkish|dovish|rate[- ]hikes?|rate[- ]cuts?|"
    r"central\s+bank|ecb|fed(?:eral\s+reserve)?|bank\s+of\s+england|"
    r"bank\s+of\s+japan|reserve\s+bank)\b|"
    r"\b(?:how\s+have\s+)?(?:interest|policy)\s+rate\s+"
    r"(?:expectations?|pricing)\b.{0,72}"
    r"\b(?:change(?:d|s|ing)?|shift(?:ed|s|ing)?|move(?:d|s|ing)?|"
    r"repric(?:e|es|ed|ing))\b|"
    r"\b(?:change(?:d|s|ing)?|shift(?:ed|s|ing)?|move(?:d|s|ing)?|"
    r"repric(?:e|es|ed|ing))\b.{0,72}"
    r"\b(?:interest|policy)\s+rate\s+(?:expectations?|pricing)\b",
    flags=re.I,
)
GEOPOLITICAL_HYPOTHETICAL_PATTERN = re.compile(
    r"\b(?:could|may|might|should|would|possibly|potentially|"
    r"is\s+going\s+to\s+have\s+to|calls?\s+for|urges?)\b.{0,80}"
    r"\b(?:attack|strike|invade|escalat(?:e|es|ed|ion)|war)\b|"
    r"\b(?:attack|strike|invade|escalat(?:e|es|ed|ion)|war)\b.{0,80}"
    r"\b(?:could|may|might|should|would|possibly|potentially)\b|"
    r"\b(?:weighs?|mulls?|considers?)\b.{0,80}"
    r"\b(?:attack|strike|target|invade|escalat(?:e|es|ed|ion)|war)\b|"
    r"\b(?:options?|ways?)\s+to\b.{0,48}"
    r"\b(?:attack|strike|target|invade|escalat(?:e|es|ed|ing|ion)|war)\b",
    flags=re.I,
)
CEASEFIRE_UNDERMINED_PATTERN = re.compile(
    r"\b(?:impact(?:s|ed|ing)?|threaten(?:s|ed|ing)?|undermin(?:e|es|ed|ing)|"
    r"derail(?:s|ed|ing)?|hurt(?:s|ing)?|weaken(?:s|ed|ing)?|"
    r"cast(?:s|ing)?\s+doubt\s+(?:on|over))\b.{0,64}\bceasefire\b|"
    r"\bceasefire\b.{0,64}\b(?:at\s+risk|in\s+doubt|under\s+threat|"
    r"threaten(?:ed|ing)?|undermin(?:ed|ing)?|derail(?:ed|ing)?)\b",
    flags=re.I,
)


def currency_alias_pattern(currency: str, *, monetary_only: bool = False) -> str:
    aliases = CURRENCY_ALIASES.get(str(currency).upper(), ())
    if monetary_only:
        money_tokens = (
            "dollar",
            "greenback",
            "yen",
            "franc",
            "pound",
            "sterling",
            "euro",
            "yuan",
            "renminbi",
            "krone",
            "krona",
            "peso",
            "lira",
            "rand",
            "zloty",
            "baht",
            "forint",
            "koruna",
        )
        code_pattern = rf"\b{str(currency).lower()}\b"
        aliases = tuple(
            pattern
            for pattern in aliases
            if pattern.lower() == code_pattern
            or any(token in pattern.lower() for token in money_tokens)
        )
        if str(currency).upper() == "EUR":
            # "Euro-area inflation strengthens" describes regional data, not
            # an observed move in the euro currency itself.
            aliases = (
                r"\beur\b",
                r"\beuro\b(?![- ]?(?:area|zone))",
            )
    return "(?:" + "|".join(aliases) + ")" if aliases else rf"\b{re.escape(currency)}\b"


def has_intervention_signal(text: str, currencies: Sequence[str]) -> bool:
    if not currencies:
        return False
    return bool(
        re.search(r"(?:為替介入|円買い介入|円売り介入)", text)
        or
        re.search(
            r"\b(?:fx|forex|foreign[- ]exchange|currency)\s+intervention\b",
            text,
            re.I,
        )
        or re.search(
            r"\b(?:joint|coordinated)\b.{0,32}\bintervention\b",
            text,
            re.I,
        )
        or INTERVENTION_COORDINATED_ACTION_PATTERN.search(text)
        or (
            "JPY" in currencies
            and re.search(
                r"\b(?:joint|coordinated)\b.{0,36}\baction\b.{0,48}"
                r"\b(?:yen|japanese yen)\b",
                text,
                re.I,
            )
        )
        or re.search(r"\brate checks?\b", text, re.I)
        # A bare ``intervention`` anywhere in a long document is not an FX
        # intervention claim.  Research PDFs commonly discuss driver,
        # government, or statistical intervention and may name many countries
        # elsewhere in tables.  Require the intervention word to share a
        # bounded clause with a monetary currency alias.  Explicit ``FX``,
        # ``foreign-exchange``, currency-buying/selling and Japanese-language
        # forms above remain authoritative regardless of this fallback.
        or any(
            re.search(
                rf"(?:{currency_alias_pattern(currency, monetary_only=True)})"
                r"[^.;:]{0,80}\binterven(?:tion|e|es|ed|ing)\b|"
                r"\binterven(?:tion|e|es|ed|ing)\b[^.;:]{0,80}"
                rf"(?:{currency_alias_pattern(currency, monetary_only=True)})",
                text,
                re.I,
            )
            for currency in currencies
        )
        or any(
            re.search(
                rf"{currency_alias_pattern(currency)}[- ](?:buying|selling)\b",
                text,
                re.I,
            )
            for currency in currencies
        )
        or any(
            re.search(
                rf"\b(?:spend|spends|spent)\b.{{0,80}}\b"
                rf"(?:support|defend|boost)(?:s|ed|ing)?\b.{{0,32}}"
                rf"{currency_alias_pattern(currency)}",
                text,
                re.I,
            )
            for currency in currencies
        )
    )


def intervention_assertion_status(
    text: str,
    *,
    source_verified: bool,
) -> tuple[str, float]:
    if source_verified and re.search(r"(?:円買い介入|円売り介入|為替介入を実施)", text):
        return "official_confirmed", 1.0
    if INTERVENTION_NEGATED_ACTION_PATTERN.search(text):
        # A report that an authority did *not* intervene is attribution
        # context.  It must not inherit a positive intervention weight merely
        # because "intervened" and a prior currency move share a headline.
        return "negated_or_denied", 0.0
    if re.search(r"\brate checks?\b", text, re.I):
        return "rate_check", 0.35
    if re.search(r"\brumou?r(?:s|ed)?\b", text, re.I):
        return "rumor", 0.35
    if INTERVENTION_UNCERTAINTY_PATTERN.search(text):
        return "suspected", 0.55
    reported_action = bool(
        INTERVENTION_OFFICIAL_ACTION_PATTERN.search(text)
        or INTERVENTION_COORDINATED_ACTION_PATTERN.search(text)
    )
    if source_verified and reported_action:
        return "official_confirmed", 1.0
    if source_verified and INTERVENTION_WARNING_PATTERN.search(text):
        return "official_warning", 0.30
    if reported_action:
        return "reported", 0.75
    return "context", 0.25


def intervention_currency_scores(
    text: str,
    currencies: Sequence[str],
    *,
    assertion_weight: float,
) -> dict[str, float]:
    output: dict[str, float] = {}
    if "JPY" in currencies:
        japanese_buying = bool(re.search(r"円買い(?:・ドル売り)?介入", text))
        japanese_selling = bool(re.search(r"円売り(?:・ドル買い)?介入", text))
        if japanese_buying != japanese_selling:
            output["JPY"] = (0.70 if japanese_buying else -0.70) * assertion_weight
    positive = re.compile(
        r"\b(?:buy(?:s|ing)?|support(?:s|ed|ing)?|defend(?:s|ed|ing)?|"
        r"prop(?:s|ped)? up|boost(?:s|ed|ing)?|gain(?:s|ed)?|"
        r"surg(?:e|es|ed|ing)|jump(?:s|ed|ing)?|rall(?:y|ies|ied|ying)|"
        r"strengthen(?:s|ed|ing)?|soar(?:s|ed)?|rocket(?:s|ed)?)\b",
        re.I,
    )
    positive_movement = re.compile(
        r"\b(?:gain(?:s|ed)?|surg(?:e|es|ed|ing)|jump(?:s|ed|ing)?|"
        r"rall(?:y|ies|ied|ying)|strengthen(?:s|ed|ing)?|"
        r"soar(?:s|ed)?|rocket(?:s|ed)?)\b",
        re.I,
    )
    negative = re.compile(
        r"\b(?:sell(?:s|ing)?|weaken(?:s|ed|ing)?|fall(?:s|en)?|fell|"
        r"slid(?:e|es)?|drop(?:s|ped)?|tumbl(?:e|es|ed)|plung(?:e|es|ed))\b",
        re.I,
    )
    positive_action = re.compile(
        r"\b(?:buy(?:s|ing)?|support(?:s|ed|ing)?|defend(?:s|ed|ing)?|"
        r"prop(?:s|ped)? up|boost(?:s|ed|ing)?)\b",
        re.I,
    )
    negative_action = re.compile(
        r"\b(?:sell(?:s|ing)?|weaken(?:s|ed|ing)?)\b",
        re.I,
    )
    for currency in currencies:
        alias = currency_alias_pattern(currency, monetary_only=True)
        support_override = bool(
            re.search(
                rf"\b(?:stem|halt|stop|curb|reverse|counter)\w*\b"
                rf"[^,;:.]{{0,28}}{alias}(?:'s)?[^,;:.]{{0,12}}"
                r"\b(?:fall|decline|slide|weakness|depreciation)\b",
                text,
                re.I,
            )
        )
        if support_override:
            output[currency] = 0.70 * assertion_weight
            continue
        explicit_buying = bool(re.search(rf"{alias}[- ]buying\b", text, re.I))
        explicit_selling = bool(re.search(rf"{alias}[- ]selling\b", text, re.I))
        if explicit_buying != explicit_selling:
            output[currency] = (
                0.70 if explicit_buying else -0.70
            ) * assertion_weight
            continue
        direction = 0
        alias_matches = list(re.finditer(alias, text, re.I))
        for match in alias_matches:
            prefix = re.split(
                r"[,;:.]",
                text[max(0, match.start() - 48) : match.start()],
            )[-1]
            positive_actions = list(positive_action.finditer(prefix))
            negative_actions = list(negative_action.finditer(prefix))
            candidates = [
                (len(prefix) - candidate.end(), sign)
                for candidate, sign in (
                    (positive_actions[-1] if positive_actions else None, 1),
                    (negative_actions[-1] if negative_actions else None, -1),
                )
                if candidate is not None
            ]
            if candidates:
                direction = min(candidates)[1]
                break
        if direction:
            output[currency] = 0.70 * direction * assertion_weight
            continue
        for match in alias_matches:
            # Prefer the first movement verb in the currency's own clause.
            suffix = re.split(
                r"[,;:.]",
                text[match.end() : match.end() + 64],
                maxsplit=1,
            )[0]
            positive_match = positive_movement.search(suffix)
            negative_match = negative.search(suffix)
            candidates = [
                (candidate.start(), sign)
                for candidate, sign in (
                    (positive_match, 1),
                    (negative_match, -1),
                )
                if candidate is not None
            ]
            if candidates:
                direction = min(candidates)[1]
                break
        if direction == 0:
            # Currency can follow its verb ("buy yen"), but never cross a
            # punctuation boundary from a different subject ("stocks surge, yen").
            for match in alias_matches:
                prefix = re.split(r"[,;:.]", text[max(0, match.start() - 48) : match.start()])[-1]
                positive_matches = list(positive.finditer(prefix))
                negative_matches = list(negative.finditer(prefix))
                candidates = [
                    (len(prefix) - candidate.end(), sign)
                    for candidate, sign in (
                        (
                            positive_matches[-1]
                            if positive_matches
                            else None,
                            1,
                        ),
                        (
                            negative_matches[-1]
                            if negative_matches
                            else None,
                            -1,
                        ),
                    )
                    if candidate is not None
                ]
                if candidates:
                    direction = min(candidates)[1]
                    break
        if direction:
            output[currency] = 0.70 * direction * assertion_weight
    if "USD" not in currencies:
        dollar_direction = 0
        for match in re.finditer(r"\b(?:u\.s\. |us )?dollars?\b", text, re.I):
            prefix = re.split(
                r"[,;:.]",
                text[max(0, match.start() - 40) : match.start()],
            )[-1]
            buys = list(re.finditer(r"\bbuy(?:s|ing)?\b", prefix, re.I))
            sells = list(re.finditer(r"\bsell(?:s|ing)?\b", prefix, re.I))
            candidates = [
                (len(prefix) - candidate.end(), sign)
                for candidate, sign in (
                    (buys[-1] if buys else None, 1),
                    (sells[-1] if sells else None, -1),
                )
                if candidate is not None
            ]
            if candidates:
                dollar_direction = min(candidates)[1]
                break
        if dollar_direction:
            output["USD"] = 0.70 * dollar_direction * assertion_weight
    return output


def reported_currency_move_scores(
    text: str,
    currencies: Sequence[str],
) -> dict[str, float]:
    """Extract an already-observed currency move without pair-side inversion."""
    output: dict[str, float] = {}
    positive = r"(?:extend(?:s|ed|ing)?\s+(?:its\s+)?(?:rally|gains?|advance)|gain(?:s|ed|ing)?|advanc(?:e|es|ed|ing)|climb(?:s|ed|ing)?|firm(?:s|ed|ing)?|support(?:s|ed|ing)?|appreciat(?:e|es|ed|ing)|surg(?:e|es|ed|ing)|jump(?:s|ed|ing)?|rall(?:y|ies|ied|ying)|strengthen(?:s|ed|ing)?|stronger|soar(?:s|ed|ing)?|rocket(?:s|ed|ing)?)"
    negative = r"(?:extend(?:s|ed|ing)?\s+(?:its\s+)?(?:slide|losses?|decline|drop)|fall(?:s|en)?|fell|slip(?:s|ped|ping)?|slid(?:e|es)?|weaken(?:s|ed|ing)?|weaker|pressur(?:e|es|ed|ing)|crack(?:s|ed|ing)?|depreciat(?:e|es|ed|ing)|declin(?:e|es|ed|ing)|drop(?:s|ped|ping)?|tumbl(?:e|es|ed|ing)|plung(?:e|es|ed|ing))"
    # A headline such as "Yen rally fades" reports the reversal of the rally,
    # not a current yen gain. Resolve these clauses before the generic first
    # movement verb can assign the initial move's sign.
    for currency in currencies:
        alias = currency_alias_pattern(currency, monetary_only=True)
        if re.search(
            rf"{alias}[^,;:.]{{0,20}}\b(?:rally|gains?|advance)\b"
            r"[^,;:.]{0,20}\b(?:fade(?:s|d)?|give(?:s)? up|gave up|"
            r"pare(?:s|d)?|erase(?:s|d)?|lose(?:s)? steam)\b",
            text,
            re.I,
        ) or re.search(
            rf"{alias}[^,;:.]{{0,20}}\b(?:give(?:s)? up|gave up|pare(?:s|d)?|erase(?:s|d)?)\b"
            r"[^,;:.]{0,16}\bgains?\b",
            text,
            re.I,
        ):
            output[currency] = -0.70
        elif re.search(
            rf"{alias}[^,;:.]{{0,20}}\b(?:losses?|slide|decline|selloff)\b"
            r"[^,;:.]{0,20}\b(?:fade(?:s|d)?|pare(?:s|d)?|recover(?:s|ed)?)\b",
            text,
            re.I,
        ) or re.search(
            rf"{alias}[^,;:.]{{0,20}}\b(?:pare(?:s|d)?|recover(?:s|ed)?)\b"
            r"[^,;:.]{0,16}\blosses?\b",
            text,
            re.I,
        ):
            output[currency] = 0.70
    # A move in BASE/QUOTE is a move in the pair rate, not in the currency
    # token closest to the verb.  Falling USD/JPY means USD weaker and JPY
    # stronger; process explicit symbols before single-currency clauses.
    currency_set = {str(value).upper() for value in currencies}
    # Market-commentary publishers frequently omit the slash from a six-letter
    # pair ticker and describe a newly observed extreme (for example,
    # ``EURUSD moves to new lows``).  Resolve that explicit pair-rate outcome
    # before policy language deeper in the recap can masquerade as a fresh
    # directional catalyst.
    for pair_match in re.finditer(
        r"\b([A-Z]{3})\s*[/_-]?\s*([A-Z]{3})\b"
        r"[^,;:.]{0,48}\bmov(?:e|es|ed|ing)\s+"
        r"(?:sharply\s+)?(?:to\s+)?(?:a\s+)?(?:new\s+)?"
        r"(highs?|lows?|higher|lower)\b",
        text,
        re.I,
    ):
        base = pair_match.group(1).upper()
        quote = pair_match.group(2).upper()
        state = pair_match.group(3).lower()
        if base not in currency_set or quote not in currency_set or base == quote:
            continue
        sign = 1.0 if state.startswith("high") else -1.0
        output[base] = 0.70 * sign
        output[quote] = -0.70 * sign
    # A pair-led technical recap can report the already-observed rate move
    # without saying "new highs/lows" (for example, "USDJPY jumps above its
    # 100 day MA"). Resolve the ticker as one rate before the two currency
    # aliases can each inherit the same nearby movement verb.
    for pair_match in re.finditer(
        rf"^\s*([A-Z]{{3}})\s*[/_-]?\s*([A-Z]{{3}})\b"
        rf"[^,;:.]{{0,48}}\b({positive}|{negative})\b"
        r"[^,;:.]{0,36}\b(?:above|below|over|under|through|past|"
        r"moving\s+average|\d+[- ]day\s+ma|support|resistance|"
        r"technical\s+levels?|makes?\s+a\s+break\s+for\s+it)\b",
        text,
        re.I,
    ):
        base = pair_match.group(1).upper()
        quote = pair_match.group(2).upper()
        movement = pair_match.group(3)
        if base not in currency_set or quote not in currency_set or base == quote:
            continue
        sign = 1.0 if re.fullmatch(positive, movement, re.I) else -1.0
        output[base] = 0.70 * sign
        output[quote] = -0.70 * sign
    for base in sorted(currency_set):
        base_alias = currency_alias_pattern(base, monetary_only=True)
        if base == "USD":
            base_alias = rf"(?:{base_alias}|\b(?:u\.?s\.?\s+|us\s+)?dollars?\b)"
        for quote in sorted(currency_set - {base}):
            quote_alias = currency_alias_pattern(quote, monetary_only=True)
            if quote == "USD":
                quote_alias = rf"(?:{quote_alias}|\b(?:u\.?s\.?\s+|us\s+)?dollars?\b)"
            named_pair = re.search(
                rf"{base_alias}\s*/\s*{quote_alias}\s+"
                rf"(?:(?:is|has)\s+)?({positive}|{negative})\b",
                text,
                flags=re.I,
            )
            if named_pair:
                movement = named_pair.group(1)
                sign = 1.0 if re.fullmatch(positive, movement, re.I) else -1.0
                output[base] = 0.70 * sign
                output[quote] = -0.70 * sign
    for pair_match in re.finditer(
        rf"\b([A-Z]{{3}})\s*[/_-]\s*([A-Z]{{3}})\b"
        rf"[^,;:.]{{0,28}}\b({positive}|{negative})\b",
        text,
        re.I,
    ):
        base = pair_match.group(1).upper()
        quote = pair_match.group(2).upper()
        movement = pair_match.group(3)
        if base not in currency_set or quote not in currency_set or base == quote:
            continue
        sign = 1.0 if re.fullmatch(positive, movement, re.I) else -1.0
        # Do not attach a later named-currency clause to the whole pair.  In
        # ``EUR/USD breakout puts dollar pressure ...`` the negative verb
        # describes USD, so treating it as a falling EUR/USD rate reverses
        # both retrospective legs.  The same ambiguity occurs with
        # ``USD/JPY ... yen pressure``.  Resolve an explicit subject between
        # the pair token and the movement word before applying pair-rate
        # direction.
        intervening = text[pair_match.end(2) : pair_match.start(3)]
        named_subject = ""
        for candidate in (base, quote):
            aliases = currency_alias_pattern(candidate, monetary_only=True)
            if re.search(rf"(?:{aliases})\s*$", intervening, flags=re.I):
                named_subject = candidate
                break
            if candidate == "USD" and re.search(
                r"(?:u\.?s\.?\s+|us\s+)?dollars?\s*$",
                intervening,
                flags=re.I,
            ):
                named_subject = candidate
                break
        if named_subject:
            other = quote if named_subject == base else base
            output[named_subject] = 0.70 * sign
            output[other] = -0.70 * sign
            continue
        output[base] = 0.70 * sign
        output[quote] = -0.70 * sign
    # Headlines commonly shorten "U.S. dollar" to just "dollar".  The
    # generic token is deliberately excluded from CURRENCY_ALIASES because it
    # is ambiguous in isolation, but it is unambiguous when paired with a
    # named currency via "against".  Resolve that pair before the generic
    # nearest-verb logic can incorrectly attach the dollar's move to the quote
    # currency (for example, "Dollar falls against yen").
    dollar_alias = r"(?:u\.?s\.?\s+|us\s+)?dollars?"
    for quote in currencies:
        if quote == "USD":
            continue
        quote_alias = currency_alias_pattern(quote, monetary_only=True)
        if re.search(
            rf"\b{dollar_alias}\b[^,;:.]{{0,28}}\b{positive}\b"
            rf"[^,;:.]{{0,20}}\bagainst\s+{quote_alias}",
            text,
            re.I,
        ):
            output["USD"] = 0.70
            output[quote] = -0.70
        elif re.search(
            rf"\b{dollar_alias}\b[^,;:.]{{0,28}}\b{negative}\b"
            rf"[^,;:.]{{0,20}}\bagainst\s+{quote_alias}",
            text,
            re.I,
        ):
            output["USD"] = -0.70
            output[quote] = 0.70
    for base in currencies:
        base_alias = currency_alias_pattern(base, monetary_only=True)
        for quote in currencies:
            if quote == base:
                continue
            quote_alias = currency_alias_pattern(quote, monetary_only=True)
            if re.search(
                rf"{base_alias}[^,;:.]{{0,28}}\b{positive}\b[^,;:.]{{0,20}}\bagainst\s+{quote_alias}",
                text,
                re.I,
            ):
                output[base] = 0.70
                output[quote] = -0.70
            elif re.search(
                rf"{base_alias}[^,;:.]{{0,28}}\b{negative}\b[^,;:.]{{0,20}}\bagainst\s+{quote_alias}",
                text,
                re.I,
            ):
                output[base] = -0.70
                output[quote] = 0.70
    for currency in currencies:
        if currency in output:
            continue
        alias = currency_alias_pattern(currency, monetary_only=True)
        for match in re.finditer(alias, text, re.I):
            suffix = re.split(
                r"[,;:.]",
                text[match.end() : match.end() + 48],
                maxsplit=1,
            )[0]
            positive_match = re.match(
                rf"\s+(?:(?:is|has)\s+)?(?:(?:sharply|strongly|further|again)\s+)?{positive}\b",
                suffix,
                re.I,
            )
            negative_match = re.match(
                rf"\s+(?:(?:is|has)\s+)?(?:(?:sharply|steeply|further|again)\s+)?{negative}\b",
                suffix,
                re.I,
            )
            if positive_match:
                output[currency] = 0.70
                break
            if negative_match:
                output[currency] = -0.70
                break
    return output


def clause_local_oil_direction(text: str) -> tuple[bool, bool]:
    """Return oil up/down only when the move belongs to the oil clause.

    Multi-asset ticker headlines commonly contain clauses such as
    ``Dow rises ... oil falls & gold surges``.  A broad look-ahead from the
    word ``oil`` can incorrectly attach the later gold verb to oil.  Bound
    each commodity mention at normal clause separators and use the first
    direction verb in that local clause.
    """
    modifier = r"(?:sharply|slightly|further|again|modestly|strongly)\s+"
    positive = re.compile(
        rf"^\s*(?:prices?\s+)?(?:{modifier})?"
        r"(?:rise(?:s|n)?|rose|surge(?:s|d)?|jump(?:s|ed)?|"
        r"rall(?:y|ies|ied)|soar(?:s|ed)?)\b",
        flags=re.I,
    )
    negative = re.compile(
        rf"^\s*(?:prices?\s+)?(?:{modifier})?"
        r"(?:fall(?:s|en)?|fell|drop(?:s|ped)?|slid(?:e|es)?|"
        r"tumble(?:s|d)?|ease(?:s|d)?)\b",
        flags=re.I,
    )
    directions: list[int] = []
    for match in re.finditer(r"\b(?:oil|crude(?:\s+oil)?|brent|wti)\b", text, re.I):
        clause = re.split(r"(?:[,&;|]|\s[-–—]\s)", text[match.end() :], maxsplit=1)[0]
        clause = clause[:64]
        up_match = positive.search(clause)
        down_match = negative.search(clause)
        candidates = [
            (candidate.start(), direction)
            for candidate, direction in ((up_match, 1), (down_match, -1))
            if candidate is not None
        ]
        if candidates:
            directions.append(min(candidates)[1])
    return (any(value > 0 for value in directions), any(value < 0 for value in directions))


def verbal_currency_stability_research_scores(
    text: str,
    currencies: Sequence[str],
) -> dict[str, float]:
    """Map official verbal currency support into research-only direction.

    Saying that a named currency should be stable or backing stabilization is
    weaker than an intervention order. It remains useful for reaction and
    source-latency research, but must not create an active pair score here.
    """

    if not re.search(
        r"\b(?:treasury secretary|finance minister|central bank governor|"
        r"boj (?:chief|governor)|ecb president|bessent)\b",
        text,
        flags=re.I,
    ):
        return {}
    output: dict[str, float] = {}
    for currency in currencies:
        alias = currency_alias_pattern(currency, monetary_only=True)
        supports_stability = bool(
            re.search(
                rf"\b(?:backs?|supports?|endorses?)\b.{{0,72}}{alias}"
                rf"[- ]?stabili[sz](?:ation|e|ing)\b",
                text,
                flags=re.I,
            )
            or re.search(
                rf"\bstable\s+{alias}\b.{{0,40}}\bimportant\b",
                text,
                flags=re.I,
            )
            or re.search(
                rf"{alias}[- ]?stabili[sz]ation\s+efforts?\b",
                text,
                flags=re.I,
            )
            or re.search(
                rf"\b(?:calls?\s+on|urges?)\b.{{0,80}}\b(?:boost|strengthen|"
                rf"support|stabili[sz]e)\s+(?:the\s+)?{alias}\b",
                text,
                flags=re.I,
            )
        )
        if supports_stability:
            output[str(currency)] = 0.35
    return output


def aggregate_intervention_status(
    rows: Sequence[Mapping[str, Any]],
    *,
    distinct_source_count: int,
) -> str:
    statuses = {
        str(row.get("intervention_status") or "")
        for row in rows
        if str(row.get("intervention_status") or "")
    }
    if "official_confirmed" in statuses:
        return "official_confirmed"
    if "official_warning" in statuses:
        return "official_warning"
    if distinct_source_count >= 2 and statuses.intersection(
        {"reported", "suspected", "rumor", "rate_check"}
    ):
        return "corroborated"
    for status in ("reported", "suspected", "rate_check", "rumor", "context"):
        if status in statuses:
            return status
    return ""


def explicit_policy_decision_action(text: str) -> str:
    """Return the issuing committee's explicit current rate action, if any.

    Policy releases routinely recap decisions by foreign central banks and
    discuss possible future actions. An unordered document-wide phrase count
    can therefore reverse the issuer's actual decision. These deliberately
    narrow patterns require a decision-making subject and a rate-action clause.
    Surprise and tradability remain separate evidence questions.
    """

    value = clean_text(text).lower()
    if not value:
        return ""
    # MNB publishes its domestic-language decision before or alongside the
    # English translation. Preserve that primary transport instead of waiting
    # for a secondary English headline. Hungarian places the rate object
    # before the action verb ("alapkamatot ... mérsékelte").
    if re.search(
        r"\bmonetáris\s+tanács\b.{0,220}\balapkamatot\b.{0,100}"
        r"\b(?:mérsékelte|csökkentette)\b",
        value,
        flags=re.I,
    ):
        return "cut"
    if re.search(
        r"\bmonetáris\s+tanács\b.{0,220}\balapkamatot\b.{0,100}"
        r"\b(?:emelte|megemelte)\b",
        value,
        flags=re.I,
    ):
        return "hike"
    if re.search(
        r"\bmonetáris\s+tanács\b.{0,220}\balapkamat(?:ot)?\b.{0,100}"
        r"\b(?:változatlanul\s+(?:hagyta|tartotta)|nem\s+változtatta)\b",
        value,
        flags=re.I,
    ):
        return "hold"
    subject = (
        r"(?:the\s+)?(?:monetary\s+council|monetary\s+policy\s+"
        r"(?:board|committee)|policy\s+(?:board|committee)|governing\s+"
        r"council|rate[- ]setting\s+committee|committee|board|council)"
    )
    rate = (
        r"(?:the\s+)?(?:central\s+bank\s+)?(?:base|bank|policy|interest|"
        r"official\s+cash|cash|repo|reference)\s+rate"
    )
    prefixes = (
        rf"\b{subject}\b.{{0,180}}",
        r"\bat\s+(?:its|today'?s)\s+meeting\b.{0,180}",
    )
    actions = (
        ("cut", r"(?:decid(?:e|ed)\s+to\s+)?(?:cut|reduce[sd]?|lower(?:ed|s)?)"),
        ("hike", r"(?:decid(?:e|ed)\s+to\s+)?(?:raise[sd]?|increase[sd]?|hike[sd]?)"),
        (
            "hold",
            r"(?:decid(?:e|ed)\s+to\s+)?(?:leave|left|keep|kept|maintain(?:ed|s)?)",
        ),
    )
    for action, verb in actions:
        if any(
            re.search(prefix + rf"\b{verb}\b.{{0,56}}\b{rate}\b", value, flags=re.I)
            for prefix in prefixes
        ):
            return action
    return ""


def policy_action_label(text: str, monetary_impulse: float) -> str:
    explicit = explicit_policy_decision_action(text)
    if explicit:
        return explicit
    if any(re.search(pattern, text, flags=re.I) for pattern in POLICY_NEUTRAL_PATTERNS):
        return "hold"
    if re.search(
        r"\b(?:increase[sd]?|raise[sd]?|hike[sd]?)\b.{0,28}"
        r"\b(?:bank|policy|interest|official cash|repo|reference|base)?\s*rate",
        text,
        flags=re.I,
    ):
        return "hike"
    if re.search(
        r"\b(?:cut[st]?|reduce[sd]?|lower(?:ed|s)?)\b.{0,28}"
        r"\b(?:bank|policy|interest|official cash|repo|reference|base)?\s*rate",
        text,
        flags=re.I,
    ):
        return "cut"
    if monetary_impulse > 0.05:
        return "hawkish_guidance"
    if monetary_impulse < -0.05:
        return "dovish_guidance"
    return "neutral_guidance"


def extract_policy_vote(text: str) -> tuple[str, str]:
    match = None
    # Numeric ranges are ubiquitous in official PDFs (axis labels, age bands,
    # price segments, dates).  Accept a split only when the same local clause
    # explicitly identifies voting.  This preserves forms such as ``voted
    # 6-3``, ``vote was 8 to 1`` and ``7-2 majority`` without turning chart
    # text such as ``-40 -20 0 20 40`` into a policy vote.
    for candidate in re.finditer(
        r"\b(\d{1,2})\s*(?:-|–|—|to)\s*(\d{1,2})\b",
        text,
        flags=re.I,
    ):
        context = text[
            max(0, candidate.start() - 80) : min(len(text), candidate.end() + 80)
        ]
        if re.search(
            r"\b(?:vote|votes|voted|voting|ballot|majority|dissent(?:ed|ing|s)?)\b",
            context,
            flags=re.I,
        ):
            match = candidate
            break
    vote_split = f"{match.group(1)}-{match.group(2)}" if match else ""
    dissent = ""
    if match:
        vote_context = text[
            max(0, match.start() - 160) : min(len(text), match.end() + 240)
        ]
        if re.search(r"\b(?:wanted|preferred|voted for)\b.{0,40}\b(?:hike|increase|raise)", vote_context):
            dissent = "hawkish"
        elif re.search(r"\b(?:wanted|preferred|voted for)\b.{0,40}\b(?:cut|reduce|lower)", vote_context):
            dissent = "dovish"
    return vote_split, dissent


def build_topic_metadata(
    *,
    text: str,
    entity_text: str | None = None,
    category: str,
    direct_currencies: Sequence[str],
    currency_scores: Mapping[str, Any],
    monetary_impulse: float,
    risk_off: float,
    risk_on: float,
    oil_up: bool,
    oil_down: bool,
    intervention_status: str = "",
) -> dict[str, Any]:
    entity_text = text if entity_text is None else entity_text
    entities = sorted(
        name
        for name, patterns in TOPIC_ENTITY_PATTERNS.items()
        if any(re.search(pattern, entity_text, flags=re.I) for pattern in patterns)
    )
    action = "neutral"
    if category == "monetary_policy":
        action = policy_action_label(text, monetary_impulse)
    elif category == "fx_intervention":
        signed = sum(
            safe_float(currency_scores.get(currency))
            for currency in direct_currencies
        )
        if not signed:
            signed = sum(safe_float(value) for value in currency_scores.values())
        direction = (
            "strengthening" if signed > 0 else "weakening" if signed < 0 else "mixed"
        )
        action = f"{intervention_status or 'context'}_{direction}"
    elif risk_off > 0:
        action = "escalation"
    elif risk_on > 0:
        action = "deescalation"
    elif oil_up:
        action = "oil_up"
    elif oil_down:
        action = "oil_down"
    elif currency_scores:
        signed = sum(safe_float(value) for value in currency_scores.values())
        action = "positive" if signed > 0 else "negative" if signed < 0 else "mixed"
    currency_key = "-".join(sorted(set(direct_currencies))) or "global"
    entity_key = "-".join(entities) or "general"
    signature = f"{category}|{currency_key}|{entity_key}|{action}"
    # Flatten while keeping a compact, stable user-facing hashtag vocabulary.
    flattened_tags = [f"#{category}"]
    flattened_tags.extend(f"#{entity}_{action}" for entity in entities)
    if category == "fx_intervention":
        for currency in sorted(set(direct_currencies)):
            value = safe_float(currency_scores.get(currency))
            direction = (
                "strengthening" if value > 0 else "weakening" if value < 0 else "mixed"
            )
            flattened_tags.append(
                f"#{currency.lower()}_{intervention_status or 'context'}_{direction}"
            )
    else:
        flattened_tags.extend(
            f"#{currency.lower()}_{action}"
            for currency in sorted(set(direct_currencies))
        )
    vote_split, policy_dissent = extract_policy_vote(text)
    return {
        "topic_signature": signature,
        "topic_tags": sorted(set(flattened_tags)),
        "topic_action": action,
        "topic_entities": entities,
        "policy_vote_split": vote_split,
        "policy_dissent": policy_dissent,
    }


def semantic_headline_tokens(value: Any) -> set[str]:
    """Return stable claim-bearing tokens for semantic topic corroboration."""

    aliases = {
        "vessel": "ship",
        "tanker": "ship",
        "ships": "ship",
        "vessels": "ship",
        "attacked": "attack",
        "attacks": "attack",
        "struck": "attack",
        "projectile": "attack",
        "projectiles": "attack",
        "closure": "closed",
        "closes": "closed",
        "closing": "closed",
    }
    tokens = {
        aliases.get(token, token)
        for token in re.findall(r"[a-z0-9]+", clean_text(value).lower())
    }
    return {
        token
        for token in tokens
        if len(token) >= 3 and token not in SEMANTIC_CLUSTER_STOPWORDS
    }


def headlines_support_same_claim(left: Any, right: Any) -> bool:
    """Require material headline overlap before broad tags corroborate a claim."""

    def blockade_effect_state(value: Any) -> int:
        text = clean_text(value).lower()
        restriction = bool(
            re.search(
                r"\b(?:oil\s+)?blockade\s+(?:is\s+)?working\b|"
                r"\bblockade\b.{0,48}\b(?:bites?|hits?|squeezes?|cuts?|"
                r"restricts?|blocks?)\b|"
                r"\boil\s+(?:offers?|shipments?|exports?|flows?)\b.{0,48}"
                r"\b(?:fall|falls|fell|drop|drops|dropped|squeezed?)\b",
                text,
            )
        )
        relief = bool(
            re.search(
                r"\b(?:transport(?:s|ed|ing)?|mov(?:e|es|ed|ing)|"
                r"ship(?:s|ped|ping)?)\b.{0,72}\bdespite\b.{0,40}\bblockade\b|"
                r"\b(?:break|breaks|broke|breaking)\s+through\b.{0,40}\bblockade\b|"
                r"\bpassage\b.{0,32}\b(?:open|opened|reopened)\b|"
                r"\bnaval\s+escorts?\b.{0,48}\b(?:work|works|worked|results?)\b",
                text,
            )
        )
        return 1 if restriction and not relief else -1 if relief and not restriction else 0

    left_blockade_state = blockade_effect_state(left)
    right_blockade_state = blockade_effect_state(right)
    if left_blockade_state and right_blockade_state:
        if left_blockade_state != right_blockade_state:
            return False
    left_tokens = semantic_headline_tokens(left)
    right_tokens = semantic_headline_tokens(right)
    if not left_tokens or not right_tokens:
        return False
    shared = left_tokens.intersection(right_tokens)
    overlap = len(shared) / max(1, min(len(left_tokens), len(right_tokens)))
    return len(shared) >= 3 and overlap >= 0.35


NON_STANCE_LIQUIDITY_IMPLEMENTATION_TITLE_PATTERN = re.compile(
    r"\bthe\s+road\s+to\s+ample\b.{0,24}"
    r"\btowards?\s+a\s+demand[- ]driven\s+liquidity\s+regime\b",
    flags=re.I,
)
NON_STANCE_MONETARY_POLICY_DISCLAIMER_PATTERN = re.compile(
    r"\bnone\s+of\s+these\s+issues\s+bears?\s+on\s+the\s+stance\s+of\s+"
    r"monetary\s+policy(?:\s+itself)?\b",
    flags=re.I,
)


def policy_non_stance_liquidity_implementation(title: str, text: str) -> bool:
    """Identify an implementation document that explicitly disclaims stance.

    This guard is intentionally narrow.  The known RBA speech is bound by its
    exact source-native title even before detail enrichment; future liquidity
    implementation documents require both liquidity/reserve-system language
    and the publisher's explicit non-stance disclaimer.
    """

    clean_title = clean_text(title)
    clean_body = clean_text(text)
    if NON_STANCE_LIQUIDITY_IMPLEMENTATION_TITLE_PATTERN.search(clean_title):
        return True
    implementation_subject = bool(
        re.search(
            r"\b(?:liquidity|reserve(?:s)?|settlement\s+balances?)\b.{0,96}"
            r"\b(?:regime|framework|implementation|operations?)\b|"
            r"\b(?:regime|framework|implementation|operations?)\b.{0,96}"
            r"\b(?:liquidity|reserve(?:s)?|settlement\s+balances?)\b",
            f"{clean_title}. {clean_body[:4_000]}",
            flags=re.I,
        )
    )
    return bool(
        implementation_subject
        and NON_STANCE_MONETARY_POLICY_DISCLAIMER_PATTERN.search(clean_body)
    )


def policy_document_type(title: str, text: str) -> str:
    """Classify the publisher's document type without body-reference leakage.

    Titles and publisher breadcrumbs carry the document identity.  The full
    body is only a fallback: speeches routinely cite minutes and monetary
    policy reports, and those citations must not relabel the speech itself.
    """

    if policy_non_stance_liquidity_implementation(title, text):
        return "liquidity_implementation_communication"

    def classify(value: str) -> str:
        if re.search(r"\bsummary of opinions\b|主な意見", value):
            return "summary_of_opinions"
        if re.search(
            r"\bminutes?\b|\baccounts? of (?:the )?monetary policy\b",
            value,
        ):
            return "minutes_or_accounts"
        if re.search(r"\bmonetary policy report\b|\boutlook report\b", value):
            return "policy_or_outlook_report"
        if re.search(
            r"\b(?:interest|policy|bank|official cash|repo) rate decision\b",
            value,
        ):
            return "rate_decision"
        if re.search(
            r"\b(?:speech(?:es)?|remarks?|testimony|press conference)\b|記者会見",
            value,
        ):
            return "policy_communication"
        return ""

    clean_title = clean_text(title).lower()
    document_type = classify(clean_title)
    if document_type:
        return document_type

    # Publisher page headers commonly contain breadcrumbs such as
    # ``Speeches | RBA Speech``.  Prefer that explicit container identity to
    # policy-report references later in the page.
    header = clean_text(text)[:1_200].lower()
    if re.search(r"\b(?:speech(?:es)?|rba speech)\b", header):
        return "policy_communication"
    document_type = classify(header)
    if document_type:
        return document_type
    return classify(clean_text(text).lower()) or "official_policy_document"


def issuer_bound_policy_communication(
    raw: Mapping[str, Any],
    *,
    title: str,
    text: str,
    source_currencies: Sequence[str],
    first_seen: dt.datetime,
    document_type: str,
) -> bool:
    """Bind a new direct central-bank speech to its configured issuer.

    Long official speeches routinely discuss foreign central banks, exchange
    rates and comparison economies.  Those names remain mentioned entities,
    but they are not independent source observations.  This cohort is
    deliberately prospective: replaying a pre-activation article cannot
    silently acquire the new scoping rule or proof standing.
    """

    activated = parse_datetime(ISSUER_BOUND_POLICY_COMMUNICATION_ACTIVATED_UTC)
    observed = first_seen
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=dt.timezone.utc)
    observed = observed.astimezone(dt.timezone.utc)
    role = source_role(raw)
    if (
        activated is None
        or observed < activated
        or raw.get("source_verified") is not True
        or raw.get("source_direct", raw.get("source_verified")) is not True
        or not source_currencies
        or document_type != "policy_communication"
        or role
        not in {
            "primary_policy_release",
            "primary_policy_communication",
            "primary_policy_and_event_communication",
        }
    ):
        return False
    return bool(
        re.search(
            r"\b(?:monetary policy|interest rates?|policy rates?|inflation|"
            r"employment|economic outlook|central banks?|federal reserve|"
            r"bank of england|bank of canada|reserve bank|riksbank|"
            r"norges bank|swiss national bank)\b",
            f"{clean_text(title)}. {clean_text(text)[:12_000]}",
            flags=re.I,
        )
    )


def configured_authority_policy_communication(
    raw: Mapping[str, Any],
    *,
    title: str,
    text: str,
    source_currencies: Sequence[str],
    document_type: str,
) -> bool:
    """Recognize a policy communication from its governed source container.

    Some dedicated central-bank speech feeds publish source-native titles that
    do not contain a document label. The source ID and issuer currency are
    therefore allowed to establish *document identity*, but never direction.
    Direction is still derived from the issuer-bound text, remains research
    only, and is admitted prospectively only after the V2 activation clock.

    Mixed press/publication feeds are intentionally absent from the source map
    and must continue to identify a speech or press conference explicitly.
    """

    source_id = clean_text(raw.get("source_id"))
    expected_currency = (
        ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_CURRENCIES_V2.get(source_id)
    )
    if (
        not expected_currency
        or raw.get("source_verified") is not True
        or raw.get("source_direct", raw.get("source_verified")) is not True
        or set(source_currencies) != {expected_currency}
        or document_type
        not in {
            "",
            "official_policy_document",
            "policy_communication",
        }
        or policy_non_stance_liquidity_implementation(title, text)
    ):
        return False
    role = source_role(raw)
    if role not in {
        "primary_policy_release",
        "primary_policy_communication",
        "primary_policy_and_event_communication",
        "primary_policy_commentary",
    }:
        return False
    return bool(
        re.search(
            r"\b(?:monetary policy|interest rates?|policy rates?|bank rate|"
            r"official cash rate|repo rate|inflation(?: target| risks?)?|"
            r"economic outlook|labou?r market|employment)\b",
            f"{clean_text(title)}. {clean_text(text)[:12_000]}",
            flags=re.I,
        )
        or (
            source_id == "japan_mof_press_conferences_ja"
            and re.search(
                r"(?:金融政策|政策金利|金利差|長期金利|物価|円安|円高|"
                r"為替(?:市場|介入)|協調介入|リフレ(?:政策|論)?)",
                f"{clean_text(title)}。{clean_text(text)[:12_000]}",
            )
        )
    )


def japan_external_policy_pressure_research(
    raw: Mapping[str, Any],
    *,
    text: str,
    source_currencies: Sequence[str],
) -> bool:
    """Detect issuer-bound external tightening pressure in Japan MOF Q&A.

    Reporter questions can quote a foreign official while the minister
    disputes whether the comment was a formal request. The information may
    still alter expectations for the yen, but it is not a BOJ decision or a
    confirmed intervention. Keep this deliberately narrow and research-only:
    exact direct source, issuer currency, trusted host, explicit pressure to
    end reflation, and an FX/rate transmission channel in the same document.
    """

    if (
        clean_text(raw.get("source_id"))
        != "japan_mof_press_conferences_ja"
        or raw.get("source_verified") is not True
        or raw.get("source_direct", raw.get("source_verified")) is not True
        or set(source_currencies) != {"JPY"}
        or not trusted_host(raw.get("url"), ("mof.go.jp",))
    ):
        return False
    bounded = clean_text(text)[:20_000]
    external_tightening_pressure = bool(
        re.search(r"\bstop\s+the\s+reflation\b", bounded, flags=re.I)
        or re.search(
            r"リフレ(?:政策|論)?(?:を|は)?(?:やめる|やめろ|止める|"
            r"停止|終了)(?:べき)?",
            bounded,
        )
    )
    rate_or_fx_channel = bool(
        re.search(
            r"(?:金利差|円の過小評価|円安|円高|為替(?:市場|介入)|"
            r"協調介入|日銀|日本銀行)",
            bounded,
        )
        or re.search(
            r"\b(?:rate differential|undervalued yen|weak yen|"
            r"foreign[- ]exchange intervention|coordinated intervention|"
            r"bank of japan)\b",
            bounded,
            flags=re.I,
        )
    )
    return external_tightening_pressure and rate_or_fx_channel


def boj_issuer_bound_policy_attachment(
    raw: Mapping[str, Any],
    *,
    title: str,
    text: str,
    source_currencies: Sequence[str],
) -> bool:
    """Identify a verified BOJ policy speech whose PDF is research context.

    BOJ speeches can cite foreign economies, currencies, oil exporters, and
    market-data vendors across dozens of attachment pages. Those mentions are
    useful entities, but they are not independent directional observations for
    CAD, MXN, NOK, USD, or EUR. Keep this detector deliberately narrower than
    ``official_policy_release`` so a speech cannot silently replace the latest
    stance-bearing decision in persistent policy state.
    """

    if (
        str(raw.get("source_id") or "") != "boj_updates"
        or raw.get("source_verified") is not True
        or raw.get("source_direct", raw.get("source_verified")) is not True
        or source_role(raw) != "primary_policy_release"
        or set(source_currencies) != {"JPY"}
        or raw.get("detail_attachment_enriched") is not True
        or raw.get("detail_attachment_research_only") is not True
        or not trusted_host(raw.get("url"), ("boj.or.jp",))
    ):
        return False
    if not re.search(
        r"\b(?:speech(?:es)?|remarks?|testimony|press conference)\b",
        clean_text(title),
        flags=re.I,
    ):
        return False
    return bool(
        re.search(
            r"\b(?:monetary policy|policy interest rate|policy rate)\b|金融政策",
            clean_text(text),
            flags=re.I,
        )
    )


def boj_non_market_research_document(
    raw: Mapping[str, Any],
    *,
    title: str,
    source_currencies: Sequence[str],
) -> bool:
    """Keep BOJ Review/methodology papers outside policy-event semantics.

    ``boj_updates`` intentionally discovers both policy communications and
    research publications.  A BOJ Review PDF is authoritative research, but
    its long methodology text (charts, comparison countries and phrases such
    as ``driver intervention``) is not a new FX or policy action.  Bind these
    rows to the issuing currency and retain them as non-market audit context.
    The boundary is deliberately source/host/path specific so genuine BOJ
    speeches, decisions and Ministry-of-Finance intervention releases are not
    weakened.
    """

    return bool(
        str(raw.get("source_id") or "") == "boj_updates"
        and raw.get("source_verified") is True
        and raw.get("source_direct", raw.get("source_verified")) is True
        and set(source_currencies) == {"JPY"}
        and trusted_host(raw.get("url"), ("boj.or.jp",))
        and (
            re.search(r"^\s*\(boj review\)\b", clean_text(title), flags=re.I)
            or "/research/wps_rev/" in canonical_url(raw.get("url")).lower()
        )
    )


def cbrt_non_market_administrative_document(
    raw: Mapping[str, Any],
    *,
    title: str,
    source_currencies: Sequence[str],
) -> bool:
    """Identify CBRT contest administration without weakening policy releases.

    The CBRT press-release feed includes student paper-contest announcements
    alongside Monetary Policy Committee decisions.  The contest page can use
    economic and policy vocabulary in its body, but it is not a market event.
    Keep the boundary tied to the verified CBRT source, issuer currency, host,
    and an explicit contest/competition phrase in the title.  Genuine rate
    decisions and policy communications therefore remain untouched.
    """

    return bool(
        str(raw.get("source_id") or "") == "tcmb_press"
        and raw.get("source_verified") is True
        and raw.get("source_direct", raw.get("source_verified")) is True
        and set(source_currencies) == {"TRY"}
        and trusted_host(raw.get("url"), ("tcmb.gov.tr",))
        and re.search(
            r"\b(?:paper|essay|research)\s+(?:contest|competition)\b|"
            r"\b(?:contest|competition)\s+for\s+(?:university\s+)?students\b",
            clean_text(title),
            flags=re.I,
        )
    )


def policy_direction_text(title: str, text: str, document_type: str) -> str:
    """Return the decision-bearing section used for policy direction.

    Central-bank minutes recap global markets, prior pricing, arguments on both
    sides and finally the board's own decision. Applying an unordered phrase
    score to the whole document lets an early market-background sentence
    override the actual conclusion. Retain the complete document elsewhere,
    but for minutes/accounts prefer the issuer's considerations/decision
    section when the publisher supplies a recognizable heading.
    """

    full_text = clean_text(text)
    if document_type != "minutes_or_accounts" or not full_text:
        return clean_text(f"{title}. {full_text}")
    lowered = full_text.lower()
    start_markers = (
        "considerations for monetary policy",
        "monetary policy considerations",
        "monetary policy discussion and decisions",
        "discussion of monetary policy",
    )
    start = max(lowered.rfind(marker) for marker in start_markers)
    if start < 0:
        return clean_text(f"{title}. {full_text}")
    end_markers = (
        "financial stability advice",
        "back to top",
        "sign up for",
    )
    ends = [
        lowered.find(marker, start + 1)
        for marker in end_markers
        if lowered.find(marker, start + 1) >= 0
    ]
    end = min(ends) if ends else len(full_text)
    section = clean_text(full_text[start:end])
    section_lower = section.lower()
    decision_start = section_lower.rfind("the decision")
    conclusion_limit = decision_start if decision_start >= 0 else len(section)
    conclusion_markers = (
        "in finalising its statement",
        "having considered these various arguments",
        "having considered the arguments",
    )
    conclusion_start = max(
        section_lower.rfind(marker, 0, conclusion_limit)
        for marker in conclusion_markers
    )
    if conclusion_start >= 0:
        # Earlier paragraphs deliberately present both the tighten and hold
        # cases. The concluding guidance plus the explicit decision is the
        # Board's own stance, while those arguments stay in the full archive.
        section = clean_text(section[conclusion_start:])
    return clean_text(f"{title}. {section or full_text}")


def semantic_claim_decomposition(
    text: str,
    currencies: Sequence[str],
) -> list[dict[str, Any]]:
    """Retain economically distinct claims instead of flattening a headline.

    The sign is the conventional first-order *rate/currency* implication, not
    linguistic sentiment.  It is deliberately diagnostic: conflicting claims
    block directional publication and must later be resolved with causal
    consensus surprise and rate-repricing evidence.
    """

    normalized = clean_text(text).lower()
    targets = sorted({str(value).upper() for value in currencies if value})
    rules: tuple[tuple[str, int, str, str], ...] = (
        (
            "labor_weakness",
            -1,
            r"\b(?:job\s+loss(?:es)?|lost\s+jobs?|payrolls?\s+(?:fell|declined|"
            r"dropped)|employment\s+(?:fell|declined|contracted)|unemployment\s+"
            r"(?:rose|increased)|layoffs?\s+(?:rose|surged|increased)|"
            r"weakening\s+labor(?:\s+market)?|labor(?:\s+market)?\s+"
            r"(?:weakens|weakening|softens|softening|cools|cooling|"
            r"deteriorates|deteriorating|loses?\s+momentum)|"
            r"labou?r\s+market\s+(?:shows?\s+(?:further\s+)?signs?\s+of\s+)?"
            r"cooling|vacanc(?:y|ies)\s+(?:fell|declined|dropped)|"
            r"weaker\s+hiring\s+conditions?)\b",
            "weaker labor normally lowers the expected policy path",
        ),
        (
            "labor_strength",
            1,
            r"\b(?:job\s+gain(?:s)?|added\s+jobs?|payrolls?\s+(?:rose|grew|"
            r"increased)|employment\s+(?:rose|grew|expanded)|unemployment\s+"
            r"(?:fell|declined|dropped))\b",
            "stronger labor normally raises the expected policy path",
        ),
        (
            "inflation_persistence",
            1,
            r"\b(?:(?:sticky|persistent|stubborn|hot|elevated|accelerating)\s+"
            r"inflation|inflation\s+(?:accelerated|rose|increased|remained\s+"
            r"high|remains\s+high|stayed\s+high|accelerates|rises|increases))\b",
            "persistent inflation normally raises the expected policy path",
        ),
        (
            "inflation_easing",
            -1,
            r"\b(?:(?:cooling|easing|slowing|softer)\s+inflation|inflation\s+"
            r"(?:cooled|eased|slowed|fell|declined))\b",
            "easing inflation normally lowers the expected policy path",
        ),
    )
    claims: list[dict[str, Any]] = []
    for dimension, sign, pattern, mechanism in rules:
        match = re.search(pattern, normalized, flags=re.I)
        if not match:
            continue
        claims.append(
            {
                "dimension": dimension,
                "rate_currency_sign": sign,
                "currencies": targets,
                "evidence_text": clean_text(match.group(0)),
                "mechanism": mechanism,
            }
        )
    return claims


STATCAN_NUMERIC_RELEASE_RULES: tuple[dict[str, str], ...] = (
    {
        "title_pattern": r"^labour force survey\b",
        "value_pattern": r"\bemployment\b.{0,80}?\b(?P<verb>rose|increased|grew|edged up|fell|decreased|declined|edged down)\b(?:\s+by)?\s+(?P<value>\d[\d,]*)\b",
        "event_series_id": "statcan_employment_change",
        "event_name": "Statistics Canada employment change",
        "unit": "persons",
        "activation_utc": "2026-08-15T21:30:00Z",
        "contract_id": "statcan_labour_force_actuals_v1_20260815",
    },
    {
        "title_pattern": r"^monthly survey of manufacturing\b",
        "value_pattern": r"\bmanufacturing sales\b.{0,220}?\b(?P<verb>edged up|rose|increased|grew|expanded|edged down|fell|decreased|declined|contracted)\b(?:\s+by)?\s+(?P<value>\d+(?:\.\d+)?)\s*%",
        "event_series_id": "statcan_manufacturing_sales_mom",
        "event_name": "Statistics Canada manufacturing sales monthly change",
    },
    {
        "title_pattern": r"^wholesale trade\b",
        "value_pattern": r"\bwholesale sales\b.{0,260}?\b(?P<verb>edged up|rose|increased|grew|expanded|edged down|fell|decreased|declined|contracted)\b(?:\s+by)?\s+(?P<value>\d+(?:\.\d+)?)\s*%",
        "event_series_id": "statcan_wholesale_sales_mom",
        "event_name": "Statistics Canada wholesale sales monthly change",
    },
    {
        "title_pattern": r"^retail trade\b",
        "value_pattern": r"\bretail sales\b.{0,220}?\b(?P<verb>edged up|rose|increased|grew|expanded|edged down|fell|decreased|declined|contracted)\b(?:\s+by)?\s+(?P<value>\d+(?:\.\d+)?)\s*%",
        "event_series_id": "statcan_retail_sales_mom",
        "event_name": "Statistics Canada retail sales monthly change",
    },
    {
        "title_pattern": r"^gross domestic product by industry\b",
        "value_pattern": r"\breal gross domestic product\b.{0,220}?\b(?P<verb>edged up|rose|increased|grew|expanded|edged down|fell|decreased|declined|contracted)\b(?:\s+by)?\s+(?P<value>\d+(?:\.\d+)?)\s*%",
        "event_series_id": "statcan_real_gdp_industry_mom",
        "event_name": "Statistics Canada real GDP by industry monthly change",
    },
)


def official_numeric_release_fields(
    raw: Mapping[str, Any],
    *,
    first_seen: dt.datetime,
) -> dict[str, Any]:
    """Extract allowlisted source-native changes without assigning direction.

    Historical rows remain non-prospective: parser activation is a separate
    knowledge boundary so a later deployment cannot be backdated to the
    article's original first-seen timestamp.
    """
    if not bool(raw.get("source_verified")) or not bool(
        raw.get("source_direct", raw.get("source_verified", False))
    ):
        return {}
    source_id = str(raw.get("source_id") or "")
    if source_id == "ons_published_releases" and re.search(
        r"\b(?:uk\s+)?labou?r\s+market\b",
        clean_text(raw.get("title")),
        re.I,
    ):
        title = clean_text(raw.get("title"))
        if not bool(raw.get("detail_enriched")) or clean_text(
            raw.get("detail_enrichment_kind")
        ) not in {"ons_release_bulletin", "ons_release_bundle"}:
            return {}
        summary = clean_text(raw.get("summary"))
        components: list[dict[str, Any]] = []

        def add_component(
            component_id: str,
            pattern: str,
            *,
            unit: str,
            sign_group: str | None = None,
        ) -> None:
            match = re.search(pattern, summary, flags=re.I)
            if match is None:
                return
            value = optional_float(match.group("value").replace(",", ""))
            if value is None:
                return
            if sign_group:
                verb = clean_text(match.group(sign_group)).lower()
                if verb in {"fell", "declined", "decreased", "dropped", "down"}:
                    value = -abs(value)
                elif verb in {"rose", "increased", "grew", "up"}:
                    value = abs(value)
            components.append(
                {
                    "component_id": component_id,
                    "actual_value": value,
                    "unit": unit,
                }
            )

        add_component(
            "payroll_employees_monthly_change",
            r"(?:payrolls?\s+(?:change\s+)?|payrolled employees\b.{0,100}?)"
            r"(?:(?P<verb>rose|increased|grew|fell|declined|decreased|dropped)"
            r"(?:\s+by)?\s+)?(?P<value>-?\d[\d,]*)\b",
            unit="persons",
            sign_group="verb",
        )
        add_component(
            "unemployment_rate",
            r"(?:ilo\s+)?unemployment rate\b.{0,48}?"
            r"(?P<value>\d+(?:\.\d+)?)\s*%",
            unit="percent",
        )
        add_component(
            "regular_earnings_growth",
            r"(?:regular earnings|earnings\s*\(ex(?:cluding)?\s+bonus\))"
            r"\b.{0,80}?(?P<value>\d+(?:\.\d+)?)\s*%",
            unit="year_percent_change",
        )
        add_component(
            "total_earnings_growth",
            r"total earnings\b.{0,80}?(?P<value>\d+(?:\.\d+)?)\s*%",
            unit="year_percent_change",
        )
        add_component(
            "vacancies_change",
            r"vacanc(?:y|ies)\b.{0,64}?"
            r"(?P<verb>rose|increased|grew|fell|declined|decreased|dropped)"
            r"(?:\s+by)?\s+(?P<value>\d[\d,]*)\b",
            unit="positions",
            sign_group="verb",
        )
        if not components:
            return {}
        published = parse_datetime(raw.get("published_utc"))
        detail_known = parse_datetime(raw.get("detail_available_utc"))
        activation = parse_datetime(raw.get("numeric_parser_activated_utc"))
        numeric_known = max(
            candidate
            for candidate in (first_seen, published, detail_known, activation)
            if candidate is not None
        )
        return {
            "structured_event": True,
            "source_currencies": ["GBP"],
            "event_series_id": "ons_uk_labour_release_package",
            "event_name": "ONS UK labour market release package",
            "event_country": "United Kingdom",
            "release_components": components,
            "numeric_causal_known_utc": iso_utc(numeric_known),
            "numeric_extraction_contract_id": (
                "ons_uk_labour_bundle_components_v1_20260818"
            ),
            "numeric_direction_policy": (
                "abstain_until_causal_consensus_and_rate_repricing"
            ),
        }
    if source_id == "china_nbs_latest_releases_direct_v1":
        if not bool(raw.get("detail_enriched")) or clean_text(
            raw.get("detail_enrichment_kind")
        ) not in {
            "official_document_text",
            "official_html_text",
            "official_pdf_text",
        }:
            return {}
        title = clean_text(raw.get("title"))
        title = re.sub(r"^\d+\.\s*", "", title)
        summary = clean_text(raw.get("summary"))
        # The English NBS page currently emits a typographic apostrophe while
        # older archives may contain either ASCII or mojibake. Normalize the
        # live form before applying the narrow source-specific regex.
        summary = summary.replace("\u2019", "'")
        published = parse_datetime(raw.get("published_utc"))
        detail_known = parse_datetime(raw.get("detail_available_utc"))
        activation = parse_datetime(raw.get("numeric_parser_activated_utc"))
        if published is None or detail_known is None:
            return {}
        numeric_known = max(
            candidate
            for candidate in (first_seen, published, detail_known, activation)
            if candidate is not None
        )
        common = {
            "structured_event": True,
            "scheduled_utc": iso_utc(published),
            "source_reported_update_utc": iso_utc(published),
            "event_country": "China",
            "importance": "high",
            "numeric_causal_known_utc": iso_utc(numeric_known),
            "numeric_extraction_contract_id": str(
                raw.get("numeric_extraction_contract_id")
                or "china_nbs_allowlisted_cpi_ppi_gdp_v1_20260816"
            ),
            "numeric_direction_policy": "abstain_and_learn_response",
        }
        if re.search(r"^Consumer Price Index in\b", title, flags=re.I):
            match = re.search(
                r"China(?:['’])?s?\s+Consumer Price Index\s*\(CPI\)\s+"
                r"(?P<verb>increased|decreased)\s+by\s+"
                r"(?P<value>\d+(?:\.\d+)?)%\s+year on year",
                summary,
                flags=re.I,
            )
            if match is None:
                return {}
            value = optional_float(match.group("value"))
            if value is None:
                return {}
            value = -abs(value) if match.group("verb").lower() == "decreased" else abs(value)
            period = re.search(r"^Consumer Price Index in\s+(.+)$", title, flags=re.I)
            return {
                **common,
                "event_series_id": "china_nbs_cpi_yoy",
                "event_name": "China Consumer Price Index annual change",
                "reference_period": clean_text(period.group(1)) if period else "",
                "unit": "year_percent_change",
                "actual": f"{value:.12g}",
                "actual_value": value,
            }
        if re.search(r"^Industrial Producer Price Indexes in\b", title, flags=re.I):
            match = re.search(
                r"producer price index for industrial products\s*\(PPI\)\s+"
                r"(?P<verb>increased|decreased)\s+by\s+"
                r"(?P<value>\d+(?:\.\d+)?)%\s+year on year",
                summary,
                flags=re.I,
            )
            if match is None:
                return {}
            value = optional_float(match.group("value"))
            if value is None:
                return {}
            value = -abs(value) if match.group("verb").lower() == "decreased" else abs(value)
            period = re.search(
                r"^Industrial Producer Price Indexes in\s+(.+)$", title, flags=re.I
            )
            return {
                **common,
                "event_series_id": "china_nbs_ppi_yoy",
                "event_name": "China industrial producer price annual change",
                "reference_period": clean_text(period.group(1)) if period else "",
                "unit": "year_percent_change",
                "actual": f"{value:.12g}",
                "actual_value": value,
            }
        if re.search(r"^Preliminary Accounting Results of GDP\b", title, flags=re.I):
            year_match = re.search(r"\b(20\d{2})\b", title)
            section = re.search(
                r"Quarter-on-Quarter Growth Rate of GDP(?P<body>.{0,1200}?)Note:",
                summary,
                flags=re.I,
            )
            if year_match is None or section is None:
                return {}
            row = re.search(
                rf"\b{re.escape(year_match.group(1))}\b\s*\|(?P<values>.{{1,200}})$",
                section.group("body"),
            )
            values = (
                re.findall(r"-?\d+(?:\.\d+)?", row.group("values"))
                if row is not None
                else []
            )
            if not values:
                return {}
            value = float(values[-1])
            return {
                **common,
                "event_series_id": "china_nbs_real_gdp_qoq",
                "event_name": "China real GDP quarterly change",
                "reference_period": clean_text(title),
                "unit": "quarter_percent_change",
                "actual": f"{value:.12g}",
                "actual_value": value,
            }
        return {}
    if source_id == "poland_gus_economic_releases_direct_v1":
        if not bool(raw.get("detail_enriched")) or clean_text(
            raw.get("detail_enrichment_kind")
        ) not in {"official_document_text", "official_html_text", "official_pdf_text"}:
            return {}
        title = clean_text(raw.get("title"))
        summary = clean_text(raw.get("summary"))
        published = parse_datetime(raw.get("published_utc"))
        detail_known = parse_datetime(raw.get("detail_available_utc"))
        activation = parse_datetime(raw.get("numeric_parser_activated_utc"))
        if published is None or detail_known is None:
            return {}
        numeric_known = max(
            candidate
            for candidate in (first_seen, published, detail_known, activation)
            if candidate is not None
        )
        common = {
            "structured_event": True,
            "scheduled_utc": iso_utc(published),
            "source_reported_update_utc": iso_utc(published),
            "event_country": "Poland",
            "importance": "high",
            "numeric_causal_known_utc": iso_utc(numeric_known),
            "numeric_extraction_contract_id": str(
                raw.get("numeric_extraction_contract_id")
                or "poland_gus_allowlisted_cpi_gdp_v1_20260816"
            ),
            "numeric_direction_policy": "abstain_and_learn_response",
        }
        if re.search(r"^Consumer price indices in\b", title, flags=re.I):
            match = re.search(
                r"Consumer prices\b.{0,120}?"
                r"(?P<verb>increased|decreased)\s+by\s+"
                r"(?P<value>\d+(?:\.\d+)?)%\s+compared with the corresponding month",
                summary,
                flags=re.I,
            )
            if match is None:
                return {}
            value = optional_float(match.group("value"))
            if value is None:
                return {}
            value = -abs(value) if match.group("verb").lower() == "decreased" else abs(value)
            period = re.search(r"^Consumer price indices in\s+(.+)$", title, flags=re.I)
            return {
                **common,
                "event_series_id": "poland_gus_cpi_yoy",
                "event_name": "Poland consumer price index annual change",
                "reference_period": clean_text(period.group(1)) if period else "",
                "unit": "year_percent_change",
                "actual": f"{value:.12g}",
                "actual_value": value,
            }
        if re.search(r"^Flash estimate of Gross Domestic Product\b", title, flags=re.I):
            match = re.search(
                r"seasonally adjusted GDP\b.{0,140}?"
                r"was\s+(?P<direction>higher|lower)\s+by\s+"
                r"(?P<value>\d+(?:\.\d+)?)%\s+than in the previous quarter",
                summary,
                flags=re.I,
            )
            if match is None:
                return {}
            value = optional_float(match.group("value"))
            if value is None:
                return {}
            value = -abs(value) if match.group("direction").lower() == "lower" else abs(value)
            return {
                **common,
                "event_series_id": "poland_gus_real_gdp_qoq",
                "event_name": "Poland seasonally adjusted real GDP quarterly change",
                "reference_period": title,
                "unit": "quarter_percent_change",
                "actual": f"{value:.12g}",
                "actual_value": value,
            }
        return {}
    if source_id == "hungary_ksh_industrial_ppi_first_release_direct_v1":
        if not bool(raw.get("detail_enriched")) or clean_text(
            raw.get("detail_enrichment_kind")
        ) not in {"official_document_text", "official_html_text"}:
            return {}
        summary = clean_text(raw.get("summary"))
        period_match = re.search(
            r"\bIndustrial producer prices,\s*(?P<month>[A-Z][a-z]+)\s+"
            r"(?P<year>20\d{2})\b",
            summary,
        )
        release_match = re.search(
            r"\bPublished\s+on:\s*(?P<day>\d{1,2})\s+"
            r"(?P<month>[A-Z][a-z]+)\s+(?P<year>20\d{2})\b",
            summary,
        )
        headline_match = re.search(
            r"\bIndustrial producer prices were\s+(?:an average\s+)?"
            r"(?P<value>\d+(?:\.\d+)?)%\s+"
            r"(?P<direction>higher|lower)\b",
            summary,
            flags=re.I,
        )
        monthly_match = re.search(
            r"\bCompared to (?:the )?previous month,.*?"
            r"industrial producer prices as a whole\s+"
            r"(?:became|were|increased|decreased)\s+"
            r"(?P<value>\d+(?:\.\d+)?)%\s+"
            r"(?P<direction>higher|lower)\b",
            summary,
            flags=re.I,
        )
        if not all((period_match, release_match, headline_match, monthly_match)):
            return {}
        try:
            reference_date = dt.datetime.strptime(
                f"{period_match.group('month')} {period_match.group('year')}",
                "%B %Y",
            )
            release_date = dt.datetime.strptime(
                f"{release_match.group('day')} {release_match.group('month')} "
                f"{release_match.group('year')}",
                "%d %B %Y",
            )
            release_clock = dt.datetime(
                release_date.year,
                release_date.month,
                release_date.day,
                8,
                30,
                tzinfo=ZoneInfo("Europe/Budapest"),
            ).astimezone(UTC)
        except (TypeError, ValueError) as exc:
            raise ValueError("KSH PPI period or publication date is invalid") from exc

        previous_date = reference_date.replace(day=1) - dt.timedelta(days=1)
        table_match = re.search(
            r"\bIndustrial price indices\b(?P<table>.*?)(?:\bNext:|$)",
            summary,
            flags=re.I,
        )
        table_rows: dict[tuple[int, int], tuple[float, float, float]] = {}
        table_year: int | None = None
        if table_match is not None:
            row_pattern = re.compile(
                r"(?:(?P<year>20\d{2})\s+)?"
                r"(?P<month>January|February|March|April|May|June|July|August|"
                r"September|October|November|December)\s+"
                r"(?P<domestic>\d+(?:\.\d+)?)\s+"
                r"(?P<nondomestic>\d+(?:\.\d+)?)\s+"
                r"(?P<total>\d+(?:\.\d+)?)\b",
                flags=re.I,
            )
            for row in row_pattern.finditer(table_match.group("table")):
                if row.group("year"):
                    table_year = int(row.group("year"))
                if table_year is None:
                    continue
                month_number = dt.datetime.strptime(
                    row.group("month"), "%B"
                ).month
                table_rows[(table_year, month_number)] = (
                    float(row.group("domestic")),
                    float(row.group("nondomestic")),
                    float(row.group("total")),
                )
        previous_row = table_rows.get((previous_date.year, previous_date.month))
        annual_components = re.search(
            r"\bDomestic output prices\s+"
            r"(?P<domestic_verb>rose|increased|were up|went up|fell|decreased|"
            r"lessened|were cut|diminished)\s+(?:by\s+)?"
            r"(?P<domestic>\d+(?:\.\d+)?)%.*?"
            r"non-domestic(?: output prices| ones)?\s+"
            r"(?:(?P<nondomestic_verb>rose|increased|were up|went up|fell|"
            r"decreased|lessened|were cut|diminished)\s+)?(?:by\s+)?"
            r"(?P<nondomestic>\d+(?:\.\d+)?)%\s+compared to",
            summary,
            flags=re.I,
        )
        monthly_components = re.search(
            r"\bCompared to (?:the )?previous month,\s+domestic output prices\s+"
            r"(?P<domestic_verb>went up|rose|increased|were up|fell|decreased|"
            r"lessened|were cut|diminished)\s+(?:by\s+)?"
            r"(?P<domestic>\d+(?:\.\d+)?)%.*?"
            r"non-domestic output prices\s+"
            r"(?:(?P<nondomestic_verb>went up|rose|increased|were up|fell|"
            r"decreased|lessened|were cut|diminished)\s+)?(?:by\s+)?"
            r"(?P<nondomestic>\d+(?:\.\d+)?)%",
            summary,
            flags=re.I,
        )
        if not all((previous_row, annual_components, monthly_components)):
            return {}

        def signed(value: str, direction: str) -> float:
            parsed = optional_float(value)
            if parsed is None:
                raise ValueError("KSH PPI numeric value is invalid")
            direction = clean_text(direction).casefold()
            if direction in {
                "lower", "fell", "decreased", "lessened", "were cut", "diminished"
            }:
                return -abs(parsed)
            return abs(parsed)

        annual_value = signed(
            headline_match.group("value"), headline_match.group("direction")
        )
        monthly_value = signed(
            monthly_match.group("value"), monthly_match.group("direction")
        )
        previous_value = round(float(previous_row[2]) - 100.0, 12)
        annual_domestic = signed(
            annual_components.group("domestic"),
            annual_components.group("domestic_verb"),
        )
        annual_nondomestic = signed(
            annual_components.group("nondomestic"),
            annual_components.group("nondomestic_verb")
            or annual_components.group("domestic_verb"),
        )
        monthly_domestic = signed(
            monthly_components.group("domestic"),
            monthly_components.group("domestic_verb"),
        )
        monthly_nondomestic = signed(
            monthly_components.group("nondomestic"),
            monthly_components.group("nondomestic_verb")
            or monthly_components.group("domestic_verb"),
        )
        if any(
            abs(value) > 100.0
            for value in (
                annual_value,
                monthly_value,
                previous_value,
                annual_domestic,
                annual_nondomestic,
                monthly_domestic,
                monthly_nondomestic,
            )
        ):
            return {}
        detail_known = parse_datetime(raw.get("detail_available_utc"))
        activation = parse_datetime(raw.get("numeric_parser_activated_utc"))
        numeric_known = max(
            candidate
            for candidate in (first_seen, release_clock, detail_known, activation)
            if candidate is not None
        )
        components = {
            "headline_ppi_yoy": {
                "actual_value": annual_value,
                "previous_value": previous_value,
                "unit": "year_percent_change",
            },
            "headline_ppi_mom": {
                "actual_value": monthly_value,
                "unit": "month_percent_change",
            },
            "domestic_output_ppi_yoy": {
                "actual_value": annual_domestic,
                "unit": "year_percent_change",
            },
            "non_domestic_output_ppi_yoy": {
                "actual_value": annual_nondomestic,
                "unit": "year_percent_change",
            },
            "domestic_output_ppi_mom": {
                "actual_value": monthly_domestic,
                "unit": "month_percent_change",
            },
            "non_domestic_output_ppi_mom": {
                "actual_value": monthly_nondomestic,
                "unit": "month_percent_change",
            },
        }
        return {
            "structured_event": True,
            "source_currencies": ["HUF"],
            "scheduled_utc": iso_utc(release_clock),
            "source_reported_update_utc": iso_utc(release_clock),
            "event_series_id": "hungary_ksh_industrial_ppi_yoy",
            "event_name": "Hungary industrial producer price annual change",
            "event_country": "Hungary",
            "reference_period": reference_date.strftime("%YM%m"),
            "unit": "year_percent_change",
            "actual": f"{annual_value:.12g}",
            "actual_value": annual_value,
            "previous": f"{previous_value:.12g}",
            "previous_value": previous_value,
            "revision_state": "not_reported_by_source",
            "source_native_components": components,
            "release_components": [
                {"component_id": key, **value} for key, value in components.items()
            ],
            "importance": "medium",
            "numeric_causal_known_utc": iso_utc(numeric_known),
            "numeric_extraction_contract_id": str(
                raw.get("numeric_extraction_contract_id")
                or "hungary_ksh_industrial_ppi_components_v1_20260901"
            ),
            "numeric_direction_policy": (
                "abstain_until_causal_consensus_and_rate_repricing"
            ),
        }
    if source_id == "swiss_fso_releases":
        title = clean_text(raw.get("title"))
        summary = clean_text(raw.get("summary"))
        # The FSO uses both phrases (and either decoded or mojibake umlauts)
        # for the same month-over-month comparison. Normalize before applying
        # the narrow allowlist rather than broadening the economic meaning.
        summary = re.sub(
            r"\b(?:gegen.ber|im\s+vergleich\s+zum)\s+vormonat\b",
            "gegenuber dem vormonat",
            summary,
            flags=re.I,
        )
        rules = (
            (
                r"\bkonsumentenpreise\b",
                r"\blandesindex der konsumentenpreise\b.{0,180}?"
                r"\b(?P<verb>sank|stieg)\b.{0,80}?"
                r"\bgegen(?:ü|u)ber dem vormonat\b.{0,30}?"
                r"\bum\s+(?P<value>\d+(?:[\.,]\d+)?)\s*%",
                "swiss_fso_cpi_mom",
                "Switzerland consumer price index monthly change",
            ),
            (
                r"\bproduzenten- und importpreisindex\b",
                r"\bgesamtindex der produzenten- und importpreise\b.{0,180}?"
                r"\b(?P<verb>sank|stieg)\b.{0,80}?"
                r"\bgegen(?:ü|u)ber dem vormonat\b.{0,30}?"
                r"\bum\s+(?P<value>\d+(?:[\.,]\d+)?)\s*%",
                "swiss_fso_producer_import_price_mom",
                "Switzerland producer and import price index monthly change",
            ),
        )
        for title_pattern, value_pattern, series_id, event_name in rules:
            if not re.search(title_pattern, title, flags=re.I):
                continue
            match = re.search(value_pattern, summary, flags=re.I)
            if match is None:
                return {}
            value = optional_float(match.group("value").replace(",", "."))
            if value is None:
                return {}
            value = -abs(value) if match.group("verb").lower() == "sank" else abs(value)
            published = parse_datetime(raw.get("published_utc"))
            published_time_inferred = bool(
                raw.get("published_time_inferred")
            ) or published is None
            # The FSO RSS currently supplies a date-only clock for these
            # releases. That date is useful display context, but it is not an
            # exact source release/update timestamp. Substituting each poll's
            # ``first_seen`` value here made ``structured_version`` change on
            # every collection cycle and stored the same CPI value as a fresh
            # release hundreds of times. Keep first_seen as the causal
            # availability boundary below while leaving the native release
            # clock empty until the publisher supplies an exact timestamp.
            native_release = None if published_time_inferred else published
            activation = parse_datetime(raw.get("numeric_parser_activated_utc"))
            numeric_known = max(
                candidate
                for candidate in (first_seen, native_release, activation)
                if candidate is not None
            )
            reference = re.search(
                r"\bim\s+([A-ZÄÖÜ][A-Za-zÄÖÜäöü]+\s+20\d{2})\b", summary
            )
            return {
                "structured_event": True,
                "scheduled_utc": (
                    iso_utc(native_release) if native_release is not None else ""
                ),
                "source_reported_update_utc": (
                    iso_utc(native_release) if native_release is not None else ""
                ),
                "event_series_id": series_id,
                "event_name": event_name,
                "event_country": "Switzerland",
                "reference_period": clean_text(reference.group(1)) if reference else "",
                "unit": "month_percent_change",
                "actual": f"{value:.12g}",
                "actual_value": value,
                "importance": "medium",
                "numeric_causal_known_utc": iso_utc(numeric_known),
                "numeric_extraction_contract_id": str(
                    raw.get("numeric_extraction_contract_id")
                    or "swiss_fso_allowlisted_period_change_v2_20260816"
                ),
                "numeric_direction_policy": "abstain_and_learn_response",
            }
        return {}
    if source_id == "abs_latest_releases":
        title = clean_text(raw.get("title"))
        activation = parse_datetime(raw.get("numeric_parser_activated_utc"))
        published = parse_datetime(raw.get("published_utc")) or first_seen
        published_time_inferred = bool(raw.get("published_time_inferred")) or not bool(
            clean_text(raw.get("published_utc"))
        )
        # A listing observation time is not a release schedule. Reusing the
        # current poll clock as ``scheduled_utc`` created a new material event
        # id on every ABS poll even when URL, reference period and values were
        # unchanged. Preserve the first-arrival causal clock below, but leave
        # native release/update clocks empty until ABS supplies one.
        native_release = None if published_time_inferred else published
        detail_known = (
            parse_datetime(raw.get("detail_available_utc"))
            if bool(raw.get("detail_enriched"))
            else None
        )
        numeric_known = max(
            candidate for candidate in (
                first_seen,
                published,
                activation,
                detail_known,
            ) if candidate is not None
        )
        # The ABS calendar exposes a generic statistical-page title alongside
        # the media headline. Derive the same allowlisted headline inputs from
        # the freshly fetched first-party body so story deduplication cannot
        # discard the only row carrying the numeric event.
        if (
            re.fullmatch(r"Wage Price Index, Australia", title, flags=re.I)
            and bool(raw.get("detail_enriched"))
            and clean_text(raw.get("detail_enrichment_kind"))
            in {"official_document_text", "official_html_text"}
        ):
            wage_body_match = re.search(
                r"Wage Price Index\s*\(WPI\)\s+rose\s+"
                r"(?P<quarter>\d+(?:\.\d+)?)\s+per cent\s+in\s+the\s+"
                r"(?P<period>[A-Za-z]+\s+quarter\s+20\d{2})\s+and\s+"
                r"(?P<annual>\d+(?:\.\d+)?)\s+per cent\s+annually",
                clean_text(raw.get("summary")),
                flags=re.I,
            )
            if wage_body_match is not None:
                title = (
                    "Media Release - Annual wage growth of "
                    f"{wage_body_match.group('annual')}% in "
                    f"{wage_body_match.group('period')}"
                )
        rules = (
            (
                r"^Media Release - CPI\s+(?P<verb>rose|fell)\s+"
                r"(?P<value>\d+(?:\.\d+)?)\s*%\s+in\s+the\s+year\s+to\s+"
                r"(?P<period>[A-Za-z]+\s+20\d{2})$",
                "abs_australia_cpi_yoy",
                "Australia Consumer Price Index annual change",
                "year_percent_change",
                {"fell", "contracted"},
            ),
            (
                r"^Media Release - Unemployment rate\s+"
                r"(?P<verb>remains at|rose to|fell to)\s+"
                r"(?P<value>\d+(?:\.\d+)?)\s*%\s+in\s+"
                r"(?P<period>[A-Za-z]+)$",
                "abs_australia_unemployment_rate",
                "Australia unemployment rate",
                "percent",
                set(),
            ),
            (
                r"^Media Release - Australian economy\s+"
                r"(?P<verb>grew|contracted)\s+"
                r"(?P<value>\d+(?:\.\d+)?)\s*%\s+in\s+the\s+"
                r"(?P<period>[A-Za-z]+\s+quarter)$",
                "abs_australia_real_gdp_qoq",
                "Australia real GDP quarterly change",
                "quarter_percent_change",
                {"contracted"},
            ),
            (
                r"^Media Release - Annual wage growth of\s+"
                r"(?P<value>\d+(?:\.\d+)?)\s*%\s+in\s+"
                r"(?P<period>[A-Za-z]+\s+quarter\s+20\d{2})$",
                "abs_australia_wage_price_index_yoy",
                "Australia Wage Price Index annual change",
                "year_percent_change",
                set(),
            ),
        )
        for pattern, series_id, event_name, unit, negative_verbs in rules:
            match = re.search(pattern, title, flags=re.I)
            if match is None:
                continue
            value = optional_float(match.group("value"))
            if value is None:
                return {}
            verb = clean_text(match.groupdict().get("verb")).lower()
            if verb in negative_verbs:
                value = -abs(value)
            else:
                value = abs(value)
            result = {
                "structured_event": True,
                "scheduled_utc": iso_utc(native_release) if native_release else "",
                "source_reported_update_utc": (
                    iso_utc(native_release) if native_release else ""
                ),
                "event_series_id": series_id,
                "event_name": event_name,
                "event_country": "Australia",
                "reference_period": clean_text(match.group("period")),
                "unit": unit,
                "actual": f"{value:.12g}",
                "actual_value": value,
                "importance": "high",
                "numeric_causal_known_utc": iso_utc(numeric_known),
                "numeric_extraction_contract_id": str(
                    raw.get("numeric_extraction_contract_id")
                    or "abs_allowlisted_headline_actuals_v2_20260819"
                ),
                "numeric_direction_policy": "abstain_and_learn_response",
            }
            if (
                series_id == "abs_australia_wage_price_index_yoy"
                and bool(raw.get("detail_enriched"))
                and clean_text(raw.get("detail_enrichment_kind"))
                in {"official_document_text", "official_html_text"}
            ):
                summary = clean_text(raw.get("summary"))
                detail_match = re.search(
                    r"Wage Price Index\s*\(WPI\)\s+rose\s+"
                    r"(?P<quarter>\d+(?:\.\d+)?)\s+per cent\s+in\s+the\s+"
                    r"(?P<period>[A-Za-z]+\s+quarter\s+20\d{2})\s+and\s+"
                    r"(?P<annual>\d+(?:\.\d+)?)\s+per cent\s+annually",
                    summary,
                    flags=re.I,
                )
                if detail_match is None:
                    return {}
                quarter_value = optional_float(detail_match.group("quarter"))
                annual_value = optional_float(detail_match.group("annual"))
                if (
                    quarter_value is None
                    or annual_value is None
                    or abs(annual_value - value) > 1e-9
                ):
                    return {}
                year_ago_match = re.search(
                    r"Annual wage growth of\s+\d+(?:\.\d+)?\s+per cent\s+"
                    r"is slightly down from\s+"
                    r"(?P<year_ago>\d+(?:\.\d+)?)\s+per cent\s+"
                    r"at the same time last year",
                    summary,
                    flags=re.I,
                )
                year_ago = (
                    optional_float(year_ago_match.group("year_ago"))
                    if year_ago_match is not None
                    else None
                )
                components = {
                    "wage_price_index_qoq": {
                        "actual": quarter_value,
                        "unit": "quarter_percent_change",
                    },
                    "wage_price_index_yoy": {
                        "actual": annual_value,
                        "year_ago": year_ago,
                        "unit": "year_percent_change",
                    },
                }
                result["source_native_components"] = components
                result["release_components"] = [
                    {
                        "component_id": component_id,
                        "actual_value": component["actual"],
                        "year_ago_value": component.get("year_ago"),
                        "unit": component["unit"],
                    }
                    for component_id, component in components.items()
                ]
                result["numeric_extraction_contract_id"] = (
                    "abs_wage_detail_components_v1_20260819"
                )
            return result
        return {}
    if source_id == "stats_nz_releases":
        if not bool(raw.get("detail_enriched")) or clean_text(
            raw.get("detail_enrichment_kind")
        ) != "stats_nz_release_detail":
            return {}
        title = clean_text(raw.get("title"))
        summary = clean_text(raw.get("summary"))
        published = parse_datetime(raw.get("published_utc"))
        detail_known = parse_datetime(raw.get("detail_available_utc"))
        activation = parse_datetime(raw.get("numeric_parser_activated_utc"))
        if published is None or detail_known is None:
            return {}
        numeric_known = max(
            candidate
            for candidate in (first_seen, published, detail_known, activation)
            if candidate is not None
        )
        common = {
            "structured_event": True,
            "scheduled_utc": iso_utc(published),
            "source_reported_update_utc": iso_utc(published),
            "event_country": "New Zealand",
            "numeric_causal_known_utc": iso_utc(numeric_known),
            "numeric_direction_policy": "abstain_and_learn_response",
        }
        if re.search(r"^Labour market statistics:", title, flags=re.I):
            match = re.search(
                r"\bthe unemployment rate was\s+"
                r"(?P<value>\d+(?:\.\d+)?)\s+percent\s+in\s+the\s+"
                r"(?P<period>[A-Za-z]+\s+20\d{2}\s+quarter)\b"
                r".{0,120}?\bcompared with\s+"
                r"(?P<previous>\d+(?:\.\d+)?)\s+percent",
                summary,
                flags=re.I,
            )
            if match is None:
                return {}
            value = optional_float(match.group("value"))
            previous = optional_float(match.group("previous"))
            if value is None or previous is None:
                return {}
            return {
                **common,
                "event_series_id": "stats_nz_unemployment_rate",
                "event_name": "New Zealand unemployment rate",
                "reference_period": clean_text(match.group("period")),
                "unit": "percent",
                "actual": f"{value:.12g}",
                "actual_value": value,
                "previous": f"{previous:.12g}",
                "previous_value": previous,
                "importance": "high",
                "numeric_extraction_contract_id": str(
                    raw.get("numeric_extraction_contract_id")
                    or "stats_nz_allowlisted_labour_v1_20260816"
                ),
            }
        if re.search(r"^Business price indexes:", title, flags=re.I):
            component_specs = (
                (
                    "output_ppi",
                    r"\b(?:the\s+)?output producers price index\s*\(PPI\)\s+"
                    r"(?P<verb>rose|fell)\s+(?P<value>\d+(?:\.\d+)?)\s+percent",
                ),
                (
                    "input_ppi",
                    r"\b(?:the\s+)?input PPI\s+"
                    r"(?P<verb>rose|fell)\s+(?P<value>\d+(?:\.\d+)?)\s+percent",
                ),
                (
                    "farm_expenses_price_index",
                    r"\b(?:the\s+)?farm expenses price index\s*\(FEPI\)\s+"
                    r"(?P<verb>rose|fell)\s+(?P<value>\d+(?:\.\d+)?)\s+percent",
                ),
                (
                    "capital_goods_price_index",
                    r"\b(?:the\s+)?capital goods price index\s*\(CGPI\)\s+"
                    r"(?P<verb>rose|fell)\s+(?P<value>\d+(?:\.\d+)?)\s+percent",
                ),
            )
            components: dict[str, dict[str, Any]] = {}
            for component_id, pattern in component_specs:
                match = re.search(pattern, summary, flags=re.I)
                if match is None:
                    continue
                value = optional_float(match.group("value"))
                if value is None:
                    continue
                if match.group("verb").lower() == "fell":
                    value = -abs(value)
                components[component_id] = {
                    "actual_value": value,
                    "unit": "quarter_percent_change",
                }
            output_value = optional_float(
                (components.get("output_ppi") or {}).get("actual_value")
            )
            if output_value is None:
                return {}
            period = re.search(
                r"^Business price indexes:\s*(?P<period>.+)$", title, flags=re.I
            )
            return {
                **common,
                "event_series_id": "stats_nz_output_ppi_qoq",
                "event_name": "New Zealand output producer price index quarterly change",
                "reference_period": clean_text(period.group("period")) if period else "",
                "unit": "quarter_percent_change",
                "actual": f"{output_value:.12g}",
                "actual_value": output_value,
                "source_native_components": components,
                "release_components": [
                    {"component_id": key, **value}
                    for key, value in components.items()
                ],
                "importance": "medium",
                "numeric_extraction_contract_id": str(
                    raw.get("numeric_extraction_contract_id")
                    or "stats_nz_business_price_components_v1_20260819"
                ),
            }
        return {}
    if source_id == "ons_published_releases":
        if not bool(raw.get("detail_enriched")) or clean_text(
            raw.get("detail_enrichment_kind")
        ) != "ons_release_bulletin":
            return {}
        title = clean_text(raw.get("title"))
        summary = clean_text(raw.get("summary"))
        published = parse_datetime(raw.get("published_utc"))
        detail_known = parse_datetime(raw.get("detail_available_utc"))
        activation = parse_datetime(raw.get("numeric_parser_activated_utc"))
        if published is None or detail_known is None:
            return {}
        numeric_known = max(
            candidate
            for candidate in (first_seen, published, detail_known, activation)
            if candidate is not None
        )
        common = {
            "structured_event": True,
            "scheduled_utc": iso_utc(published),
            "source_reported_update_utc": iso_utc(published),
            "event_country": "United Kingdom",
            "importance": "high",
            "numeric_causal_known_utc": iso_utc(numeric_known),
            "numeric_extraction_contract_id": str(
                raw.get("numeric_extraction_contract_id")
                or "ons_allowlisted_gdp_cpi_v1_20260816"
            ),
            "numeric_direction_policy": "abstain_and_learn_response",
        }
        gdp_rules = (
            (
                r"^GDP first quarterly estimate, UK\b",
                "ons_uk_real_gdp_qoq",
                "UK real GDP quarterly change",
                "quarter_percent_change",
            ),
            (
                r"^GDP monthly estimate, UK\b",
                "ons_uk_real_gdp_mom",
                "UK real GDP monthly change",
                "month_percent_change",
            ),
        )
        for title_pattern, series_id, event_name, unit in gdp_rules:
            if not re.search(title_pattern, title, flags=re.I):
                continue
            match = re.search(
                r"\breal gross domestic product(?:\s*\(GDP\))?\b.{0,120}?"
                r"\b(?P<verb>increased|decreased|grew|fell)\b(?:\s+by)?\s+"
                r"(?P<value>\d+(?:\.\d+)?)\s*%",
                summary,
                flags=re.I,
            )
            if match is None:
                return {}
            value = optional_float(match.group("value"))
            if value is None:
                return {}
            if match.group("verb").lower() in {"decreased", "fell"}:
                value = -abs(value)
            else:
                value = abs(value)
            reference = re.search(
                r"\b(?:Quarter\s+[1-4]\s+20\d{2}|[A-Za-z]+\s+20\d{2})\b",
                summary,
                flags=re.I,
            )
            return {
                **common,
                "event_series_id": series_id,
                "event_name": event_name,
                "reference_period": clean_text(reference.group(0)) if reference else "",
                "unit": unit,
                "actual": f"{value:.12g}",
                "actual_value": value,
            }
        if re.search(r"^Consumer price inflation, UK\b", title, flags=re.I):
            match = re.search(
                r"\bConsumer Prices Index\b(?:\s*\(CPI\))?.{0,100}?"
                r"\b(?P<verb>rose|fell)\b(?:\s+by)?\s+"
                r"(?P<value>\d+(?:\.\d+)?)\s*%\s+in\s+the\s+12\s+months\s+to\s+"
                r"(?P<period>[A-Za-z]+\s+20\d{2})\b",
                summary,
                flags=re.I,
            )
            if match is None:
                return {}
            value = optional_float(match.group("value"))
            if value is None:
                return {}
            value = -abs(value) if match.group("verb").lower() == "fell" else abs(value)
            result = {
                **common,
                "event_series_id": "ons_uk_cpi_yoy",
                "event_name": "UK Consumer Prices Index annual change",
                "reference_period": clean_text(match.group("period")),
                "unit": "year_percent_change",
                "actual": f"{value:.12g}",
                "actual_value": value,
            }
            previous = re.search(
                r"\b(?:up|down)\s+from\s+(?P<value>\d+(?:\.\d+)?)\s*%",
                summary,
                flags=re.I,
            )
            if previous is not None:
                previous_value = optional_float(previous.group("value"))
                if previous_value is not None:
                    result["previous"] = f"{previous_value:.12g}"
                    result["previous_value"] = previous_value
            return result
        return {}
    if source_id == "eurostat_economy_finance":
        title = clean_text(raw.get("title"))
        summary = clean_text(raw.get("summary"))
        published = parse_datetime(raw.get("published_utc")) or first_seen
        activation = parse_datetime(raw.get("numeric_parser_activated_utc"))
        numeric_known = max(
            candidate
            for candidate in (first_seen, published, activation)
            if candidate is not None
        )
        common = {
            "structured_event": True,
            "scheduled_utc": iso_utc(published),
            "source_reported_update_utc": iso_utc(published),
            "event_country": "Euro area",
            "importance": "high",
            "numeric_causal_known_utc": iso_utc(numeric_known),
            "numeric_extraction_contract_id": str(
                raw.get("numeric_extraction_contract_id")
                or "eurostat_allowlisted_gdp_inflation_v1_20260816"
            ),
            "numeric_direction_policy": "abstain_and_learn_response",
        }
        if re.search(r"^GDP\s+(?:up|down)\s+by\b", title, flags=re.I):
            match = re.search(
                r"\bGDP\s+(?P<verb>increased|decreased)\s+by\s+"
                r"(?P<value>\d+(?:[\.,]\d+)?)\s*%\s+in\s+the\s+euro\s+area\b"
                r".{0,160}?\bcompared\s+with\s+the\s+previous\s+quarter\b",
                summary,
                flags=re.I,
            )
            if match is None:
                return {}
            value = optional_float(match.group("value").replace(",", "."))
            if value is None:
                return {}
            value = -abs(value) if match.group("verb").lower() == "decreased" else abs(value)
            reference = re.search(
                r"\b(?:first|second|third|fourth) quarter of 20\d{2}\b",
                summary,
                flags=re.I,
            )
            return {
                **common,
                "event_series_id": "eurostat_euro_area_gdp_qoq",
                "event_name": "Euro area GDP quarterly change",
                "reference_period": clean_text(reference.group(0)) if reference else "",
                "unit": "quarter_percent_change",
                "actual": f"{value:.12g}",
                "actual_value": value,
            }
        if re.search(r"^Euro area annual inflation\b", title, flags=re.I):
            match = re.search(
                r"\bannual inflation\s+is\s+expected\s+to\s+be\s+"
                r"(?P<value>\d+(?:[\.,]\d+)?)\s*%\s+in\s+"
                r"(?P<period>[A-Za-z]+\s+20\d{2})\b"
                r".{0,80}?\b(?:up|down)\s+from\s+"
                r"(?P<previous>\d+(?:[\.,]\d+)?)\s*%",
                summary,
                flags=re.I,
            )
            if match is None:
                return {}
            value = optional_float(match.group("value").replace(",", "."))
            previous = optional_float(match.group("previous").replace(",", "."))
            if value is None or previous is None:
                return {}
            return {
                **common,
                "event_series_id": "eurostat_euro_area_hicp_flash_yoy",
                "event_name": "Euro area flash annual inflation",
                "reference_period": clean_text(match.group("period")),
                "unit": "year_percent_change",
                "actual": f"{value:.12g}",
                "actual_value": value,
                "previous": f"{previous:.12g}",
                "previous_value": previous,
            }
        return {}
    if source_id != "statcan_daily_releases":
        return {}
    published = parse_datetime(raw.get("published_utc"))
    if published is None or (first_seen - published).total_seconds() > 600:
        return {}
    title = clean_text(raw.get("title"))
    summary = clean_text(raw.get("summary"))
    for rule in STATCAN_NUMERIC_RELEASE_RULES:
        if not re.search(rule["title_pattern"], title, flags=re.I):
            continue
        match = re.search(rule["value_pattern"], summary, flags=re.I)
        if match is None:
            return {}
        value = optional_float(match.group("value").replace(",", ""))
        if value is None:
            return {}
        verb = clean_text(match.group("verb")).lower()
        if verb in {"edged down", "fell", "decreased", "declined", "contracted"}:
            value = -abs(value)
        else:
            value = abs(value)
        activation = parse_datetime(raw.get("numeric_parser_activated_utc"))
        rule_activation = parse_datetime(rule.get("activation_utc"))
        numeric_known = max(
            candidate
            for candidate in (first_seen, published, activation, rule_activation)
            if candidate is not None
        )
        reference_period = clean_text(title.split(",", 1)[1] if "," in title else "")
        fields: dict[str, Any] = {
            "structured_event": True,
            "scheduled_utc": iso_utc(published),
            "source_reported_update_utc": iso_utc(published),
            "event_series_id": rule["event_series_id"],
            "event_name": rule["event_name"],
            "event_country": "Canada",
            "reference_period": reference_period,
            "unit": rule.get("unit", "period_percent_change"),
            "actual": f"{value:.12g}",
            "actual_value": value,
            "importance": "medium",
            "numeric_causal_known_utc": iso_utc(numeric_known),
            "numeric_extraction_contract_id": rule.get(
                "contract_id", "statcan_allowlisted_period_change_v1_20260814"
            ),
            "numeric_direction_policy": "abstain_and_learn_response",
        }
        if rule["event_series_id"] == "statcan_employment_change":
            unemployment = re.search(
                r"\bunemployment rate\b.{0,100}?\bto\s+"
                r"(?P<value>\d+(?:\.\d+)?)\s*%",
                summary,
                flags=re.I,
            )
            if unemployment is not None:
                fields["source_native_components"] = {
                    "employment_change": {
                        "actual_value": value,
                        "unit": "persons",
                    },
                    "unemployment_rate": {
                        "actual_value": optional_float(unemployment.group("value")),
                        "unit": "percent",
                    },
                }
        return fields
    return {}


def secondary_macro_headline_numeric_fields(
    raw: Mapping[str, Any],
    *,
    first_seen: dt.datetime,
) -> dict[str, Any]:
    """Retain a narrow post-release actual/expected headline without leakage.

    A secondary headline observed after a release can reveal the reported
    actual and a publisher's expected-value reference, but it does not prove
    that this project captured that consensus before the release.  The fields
    are therefore research-only, and their knowledge time is no earlier than
    this parser's activation for historical rows.
    """

    if bool(raw.get("source_verified")):
        return {}
    title = headline_content(
        clean_text(raw.get("title")),
        publisher_name=raw.get("source_name"),
    )
    match = re.search(
        r"\bnew\s+zealand(?:['’]s)?\s+unemployment\s+rate\b.{0,40}?"
        r"\b(?:climb|climbs|climbed|rise|rises|rose|increase|increases|increased)\b"
        r".{0,24}?\bto\s+(?P<actual>\d+(?:\.\d+)?)\s*%"
        r".{0,48}?\b(?:vs\.?|versus)\s+(?P<consensus>\d+(?:\.\d+)?)\s*%"
        r"\s*(?:expected|forecast)",
        title,
        flags=re.I,
    )
    if match is not None:
        actual = optional_float(match.group("actual"))
        consensus = optional_float(match.group("consensus"))
        if actual is None or consensus is None:
            return {}
        published = parse_datetime(raw.get("published_utc"))
        activation = parse_datetime(SECONDARY_MACRO_HEADLINE_PARSER_ACTIVATED_UTC)
        numeric_known = max(
            candidate
            for candidate in (first_seen, published, activation)
            if candidate is not None
        )
        return {
            "structured_event": True,
            "source_currencies": ["NZD"],
            "event_series_id": "stats_nz_unemployment_rate",
            "event_name": "New Zealand unemployment rate",
            "event_country": "New Zealand",
            "unit": "percent",
            "actual": f"{actual:.12g}%",
            "actual_value": actual,
            "consensus": f"{consensus:.12g}%",
            "consensus_value": consensus,
            "numeric_causal_known_utc": iso_utc(numeric_known),
            "numeric_extraction_contract_id": (
                "secondary_nz_unemployment_actual_expected_headline_v1_20260815"
            ),
            "numeric_direction_policy": "research_only_post_release_reference",
            "consensus_capture_state": "post_release_reference_not_pre_release_capture",
            "directional_research_only": True,
        }

    if clean_text(raw.get("source_id")) == "finnhub_fx_market_news":
        match = re.search(
            r"^Japan\s+(?:(?P<month>[A-Za-z]+)\s+)?manufacturing\s+PMI\s+"
            r"(?:hits?|at|prints?)\s+(?P<actual>\d+(?:\.\d+)?)\b",
            title,
            flags=re.I,
        )
        if match is not None:
            actual = optional_float(match.group("actual"))
            if actual is None:
                return {}
            summary = clean_text(raw.get("summary"))
            previous_match = re.search(
                r"\bfrom\s+(?P<previous>\d+(?:\.\d+)?)\s+in\s+"
                r"(?P<previous_month>[A-Za-z]+)\b",
                summary,
                flags=re.I,
            )
            flash_match = re.search(
                r"\b(?:below|versus|vs\.?|from)\s+(?:a\s+|the\s+)?"
                r"flash\s+(?:reading|estimate)\s+(?:of\s+)?"
                r"(?P<flash>\d+(?:\.\d+)?)\b",
                summary,
                flags=re.I,
            )
            month = clean_text(match.group("month"))
            if not month:
                period_match = re.search(
                    r"\b(?:PMI|index)\b.{0,120}\b(?:in|for)\s+"
                    r"(?P<month>January|February|March|April|May|June|July|"
                    r"August|September|October|November|December)\b",
                    summary,
                    flags=re.I,
                )
                month = (
                    clean_text(period_match.group("month"))
                    if period_match is not None
                    else ""
                )
            published = parse_datetime(raw.get("published_utc"))
            activation = parse_datetime(
                SECONDARY_JAPAN_PMI_PARSER_ACTIVATED_UTC
            )
            numeric_known = max(
                candidate
                for candidate in (first_seen, published, activation)
                if candidate is not None
            )
            components: dict[str, Any] = {
                "final": {"actual_value": actual, "unit": "index_points"}
            }
            if flash_match is not None:
                components["flash_estimate"] = {
                    "actual_value": optional_float(flash_match.group("flash")),
                    "unit": "index_points",
                }
            fields: dict[str, Any] = {
                "structured_event": True,
                "source_currencies": ["JPY"],
                "event_series_id": "sp_global_japan_manufacturing_pmi_final",
                "event_name": "S&P Global Japan Manufacturing PMI final",
                "event_country": "Japan",
                "reference_period": month,
                "unit": "index_points",
                "actual": f"{actual:.12g}",
                "actual_value": actual,
                "numeric_causal_known_utc": iso_utc(numeric_known),
                "numeric_extraction_contract_id": (
                    "secondary_japan_manufacturing_pmi_final_v1_20260901"
                ),
                "numeric_direction_policy": (
                    "abstain_pending_authoritative_value_and_causal_consensus"
                ),
                "numeric_verification_state": (
                    "secondary_claim_not_authoritative_source"
                ),
                "release_stage": "final",
                "consensus_capture_state": (
                    "unavailable_no_pre_release_snapshot"
                ),
                "directional_research_only": True,
                "source_native_components": components,
            }
            if previous_match is not None:
                previous = optional_float(previous_match.group("previous"))
                fields.update(
                    {
                        "previous": f"{previous:.12g}",
                        "previous_value": previous,
                    }
                )
                components["previous_final"] = {
                    "actual_value": previous,
                    "unit": "index_points",
                    "reference_period": clean_text(
                        previous_match.group("previous_month")
                    ),
                }
            return fields

    if clean_text(raw.get("source_id")) != "finnhub_fx_market_news":
        return {}
    match = re.search(
        r"^Japan\s+(?P<month>[A-Za-z]+)\s+(?:Machine|Machinery)\s+Orders\s+"
        r"(?P<actual>[+-]?\d+(?:\.\d+)?)%\s+"
        r"(?P<period>y/y|m/m)\s*\(expected\s+"
        r"(?P<consensus>[+-]?\d+(?:\.\d+)?)%\)$",
        title,
        flags=re.I,
    )
    if match is None:
        return {}
    actual = optional_float(match.group("actual"))
    consensus = optional_float(match.group("consensus"))
    if actual is None or consensus is None:
        return {}
    period_token = match.group("period").lower()
    summary = clean_text(raw.get("summary"))
    period_label = "YoY" if period_token == "y/y" else "MoM"
    previous_match = re.search(
        rf"Machinery Orders\s*\({period_label}\).{{0,160}}?"
        r"\bprior\s+(?P<previous>[+-]?\d+(?:\.\d+)?)%",
        summary,
        flags=re.I,
    )
    previous = (
        optional_float(previous_match.group("previous"))
        if previous_match is not None
        else None
    )
    published = parse_datetime(raw.get("published_utc"))
    activation = parse_datetime(SECONDARY_JAPAN_MACHINERY_PARSER_ACTIVATED_UTC)
    numeric_known = max(
        candidate
        for candidate in (first_seen, published, activation)
        if candidate is not None
    )
    fields = {
        "structured_event": True,
        "source_currencies": ["JPY"],
        "event_series_id": f"japan_machinery_orders_{'yoy' if period_token == 'y/y' else 'mom'}",
        "event_name": f"Japan machinery orders {period_label} change",
        "event_country": "Japan",
        "reference_period": clean_text(match.group("month")),
        "unit": "year_percent_change" if period_token == "y/y" else "month_percent_change",
        "actual": f"{actual:.12g}%",
        "actual_value": actual,
        "consensus": f"{consensus:.12g}%",
        "consensus_value": consensus,
        "numeric_causal_known_utc": iso_utc(numeric_known),
        "numeric_extraction_contract_id": (
            "secondary_japan_machinery_orders_actual_expected_v1_20260819"
        ),
        "numeric_direction_policy": "research_only_post_release_reference",
        "consensus_capture_state": "post_release_reference_not_pre_release_capture",
        "directional_research_only": True,
    }
    if previous is not None:
        fields.update(
            {
                "previous": f"{previous:.12g}%",
                "previous_value": previous,
            }
        )
    return fields


def official_search_policy_rate_headline_fields(
    raw: Mapping[str, Any],
    *,
    first_seen: dt.datetime,
) -> dict[str, Any]:
    """Retain an explicit official-domain rate action without inventing FX alpha.

    These rows arrive through a Google News official-publisher search because
    a direct central-bank surface can be blocked or slow.  Publisher-domain
    verification makes the compact action/level useful research evidence, but
    the intermediary transport and absent pre-release consensus prevent it
    from standing in for the direct statement or assigning currency direction.
    The September RBNZ miss predates this contract and remains a frozen
    regression fixture rather than retroactive proof.
    """

    source_id = clean_text(raw.get("source_id"))
    expected_currency = OFFICIAL_SEARCH_POLICY_RATE_SOURCE_CURRENCIES_V1.get(
        source_id
    )
    source_currencies = {
        clean_text(value).upper() for value in raw.get("source_currencies") or []
    }
    if (
        not expected_currency
        or raw.get("source_verified") is not True
        or raw.get("source_direct", False) is True
        or clean_text(raw.get("retrieval_via"))
        != "google_news_official_site_search"
        or source_currencies != {expected_currency}
    ):
        return {}

    headline = headline_content(
        raw.get("title"), publisher_name=raw.get("source_name")
    )
    rate_label = (
        r"(?:ocr|official cash rate|policy rate|interest rate|bank rate|"
        r"key rate|repo rate|reference rate|base rate)"
    )
    action = (
        r"(?P<action>increas(?:e[sd]?|ing)|rais(?:e[sd]?|ing)|"
        r"hik(?:e[sd]?|ing)|reduc(?:e[sd]?|ing)|lower(?:s|ed|ing)?|"
        r"cut(?:s|ting)?)"
    )
    patterns = (
        rf"\b{rate_label}\b.{{0,32}}\b{action}\b",
        rf"\b{action}\b.{{0,32}}\b{rate_label}\b",
    )
    match = next(
        (
            candidate
            for pattern in patterns
            if (candidate := re.search(
                pattern
                + r"(?:\s+by)?\s+(?P<basis_points>\d+(?:\.\d+)?)\s*"
                r"(?:basis points?|bps?|bp)\b.{0,24}\b(?:to|at)\s+"
                r"(?P<level>\d+(?:\.\d+)?)\s*%",
                headline,
                flags=re.I,
            ))
        ),
        None,
    )
    if match is None:
        return {}

    action_text = match.group("action").lower()
    direction = -1 if re.match(r"(?:reduc|lower|cut)", action_text) else 1
    basis_points = float(match.group("basis_points"))
    level = float(match.group("level"))
    signed_basis_points = direction * basis_points
    previous_level = level - signed_basis_points / 100.0
    observed = first_seen
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=dt.timezone.utc)
    observed = observed.astimezone(dt.timezone.utc)
    activated = parse_datetime(OFFICIAL_SEARCH_POLICY_RATE_ACTIVATED_UTC_V1)
    activation_eligible = bool(activated is not None and observed >= activated)
    policy_action = "hike" if direction > 0 else "cut"
    diagnostic = {
        "official_search_policy_rate_headline_detected": True,
        "official_search_policy_rate_action": policy_action,
        "official_search_policy_rate_change_bp": round(signed_basis_points, 6),
        "official_search_policy_rate_level": round(level, 8),
        "official_search_policy_rate_previous_level": round(previous_level, 8),
        "official_search_policy_rate_contract_id": (
            OFFICIAL_SEARCH_POLICY_RATE_CONTRACT_ID_V1
        ),
        "official_search_policy_rate_cohort_id": (
            OFFICIAL_SEARCH_POLICY_RATE_COHORT_ID_V1
            if activation_eligible
            else ""
        ),
        "official_search_policy_rate_activated_utc": (
            OFFICIAL_SEARCH_POLICY_RATE_ACTIVATED_UTC_V1
        ),
        "official_search_policy_rate_activation_eligible": activation_eligible,
        "directional_research_only": True,
        "numeric_direction_policy": (
            "research_only_pending_pre_release_consensus_and_rate_repricing"
        ),
        "consensus_capture_state": "missing_pre_release_consensus",
    }
    if not activation_eligible:
        return diagnostic
    return {
        **diagnostic,
        "structured_event": True,
        "event_series_id": f"{expected_currency.lower()}_official_policy_rate",
        "event_name": f"{expected_currency} official policy rate decision",
        "unit": "percent",
        "actual": f"{level:.12g}%",
        "actual_value": level,
        "previous": f"{previous_level:.12g}%",
        "previous_value": previous_level,
        "release_components": ["policy_rate"],
        "source_native_components": {
            "policy_rate": {
                "actual": level,
                "previous": previous_level,
                "consensus": None,
            }
        },
        "numeric_causal_known_utc": iso_utc(observed),
        "numeric_parser_activated_utc": (
            OFFICIAL_SEARCH_POLICY_RATE_ACTIVATED_UTC_V1
        ),
        "numeric_extraction_contract_id": (
            OFFICIAL_SEARCH_POLICY_RATE_CONTRACT_ID_V1
        ),
        "numeric_verification_state": (
            "trusted_official_publisher_headline_via_search_not_direct_body"
        ),
        "release_stage": "initial_official_search_headline_observation",
    }


def classify_article(
    raw: Mapping[str, Any],
    *,
    first_seen: dt.datetime,
) -> dict[str, Any]:
    raw = dict(raw)
    detail_quality_issue = (
        official_document_quality_issue(raw.get("summary"))
        if bool(raw.get("detail_enriched"))
        else ""
    )
    rejected_detail_sha256 = ""
    if detail_quality_issue:
        rejected_detail_sha256 = clean_text(raw.get("detail_content_sha256"))
        raw["detail_enriched"] = False
        raw["detail_enrichment_research_only"] = True
        raw["detail_content_sha256"] = ""
        raw["detail_content_bytes"] = 0
        raw["detail_text_characters"] = 0
        raw["detail_source_url"] = ""
        raw["detail_archive_path"] = ""
        raw["detail_available_utc"] = ""
        # Never let a maintenance/challenge body influence term, currency,
        # sentiment, numeric-release, or persistent-policy classification.
        raw["summary"] = ""
    raw.update(official_numeric_release_fields(raw, first_seen=first_seen))
    raw.update(secondary_macro_headline_numeric_fields(raw, first_seen=first_seen))
    raw.update(
        official_search_policy_rate_headline_fields(raw, first_seen=first_seen)
    )
    title = clean_text(raw.get("title"))
    summary = clean_text(raw.get("summary"))
    if trusted_host(raw.get("url"), ("news.google.com",)):
        summary = ""
    clean_headline = headline_content(title, publisher_name=raw.get("source_name"))
    text = f"{clean_headline}. {summary}".lower()
    source_currencies = [
        str(value).upper() for value in raw.get("source_currencies") or []
    ]
    mentioned_currencies = extract_currencies(text, source_currencies)
    mentioned_currency_entities = list(mentioned_currencies)
    policy_subject_currencies = external_policy_subject_currencies(
        clean_headline
    )
    release_subject_currencies = secondary_release_subject_currencies(
        clean_headline
    )
    semantic_subject_currencies = (
        source_currencies
        if bool(raw.get("structured_event")) and source_currencies
        else policy_subject_currencies
        or release_subject_currencies
    )
    if semantic_subject_currencies:
        # Preserve every entity for audit, but restrict the directional event
        # legs to the issuing/target economy.  This also protects long article
        # bodies containing comparison countries and appended prior stories.
        mentioned_currencies = sorted(set(semantic_subject_currencies))
    policy_currencies = extract_policy_currencies(text)
    currencies = list(mentioned_currencies)
    official = bool(raw.get("source_verified"))
    official_non_market_research = boj_non_market_research_document(
        raw,
        title=title,
        source_currencies=source_currencies,
    )
    official_non_market_administrative = cbrt_non_market_administrative_document(
        raw,
        title=title,
        source_currencies=source_currencies,
    )
    if official_non_market_research or official_non_market_administrative:
        # Comparison countries and currencies in an official methodology PDF
        # (or economic terminology in a contest announcement) are entities in
        # the document, not direct event legs.
        mentioned_currencies = sorted(set(source_currencies))
        mentioned_currency_entities = list(mentioned_currencies)
        currencies = list(mentioned_currencies)
    structured_event = bool(raw.get("structured_event"))
    has_structured_numeric_release = bool(
        official
        and structured_event
        and (
            bool(raw.get("release_components"))
            or any(
                raw.get(key) is not None
                for key in (
                    "actual",
                    "actual_value",
                    "previous",
                    "previous_value",
                    "consensus",
                    "consensus_value",
                )
            )
        )
    )
    # Bound Treasury policy matching to the current release's title and lead.
    # Long Treasury pages append navigation/related releases; scanning the
    # entire page caused later sanctions and tax notices to inherit a prior
    # buyback announcement and a synthetic USD-negative score.
    official_policy_lead_text = f"{clean_headline}. {summary[:3000]}".lower()
    official_duration_liquidity_policy = bool(
        official
        and bool(raw.get("source_direct", raw.get("source_verified", False)))
        and str(raw.get("source_id") or "") == "us_treasury_press"
        and source_role(raw) == "primary_policy_release"
        and re.search(
            r"\b(?:increase(?:d|s|ing)?|double(?:d|s|ing)?|expand(?:ed|s|ing)?)\b"
            r".{0,120}\b(?:liquidity\s+support\s+)?buyback(?:s|\s+operations?)?\b",
            official_policy_lead_text,
            flags=re.I,
        )
        and re.search(
            r"\b(?:nominal\s+long[- ]end|long[- ]end|longer[- ]dated|"
            r"10[- ]year\s+to\s+20[- ]year|20[- ]year\s+to\s+30[- ]year)\b",
            official_policy_lead_text,
            flags=re.I,
        )
    )
    if has_structured_numeric_release and source_currencies:
        # Numeric observations belong to the configured issuing economy.  A
        # release page may mention comparison countries or market currencies,
        # but those words must not fan one actual value out to unrelated legs.
        currencies = sorted(set(source_currencies))
    headline_text = clean_headline.lower()
    secondary_market_roundup = bool(
        not official
        and (
            # Technical-analysis and levels articles describe an already
            # observed market state. Their appended news recap can explain a
            # move, but it is not the causal source and must not seed one.
            re.search(
                r"\b(?:fx|forex|currency|pair|usd|eur|gbp|jpy)?\s*"
                r"technical\s+analysis\b",
                headline_text,
                flags=re.I,
            )
            or (
                len(summary) >= 1000
                and re.search(
                    r"\b(?:what can you trade|markets? (?:and setups )?to watch|"
                    r"key takeaways for traders|(?:european|asian|u\.?s\.?|new york|"
                    r"london|daily|market) session wrap|(?:daily|market) wrap|"
                    r"(?:fx|forex)\s+news\s+wrap)\b",
                    f"{headline_text}. {summary.lower()}",
                    flags=re.I,
                )
            )
        )
    )
    # An unverified market roundup can mention old intervention episodes deep
    # in a long body. Only a headline-level claim may classify such an item as
    # a fresh intervention event; verified official material may use its body.
    intervention_signal = bool(
        not secondary_market_roundup
        and has_intervention_signal(
            headline_text
            if official_non_market_research
            else text
            if official
            else headline_text,
            currencies,
        )
    )
    intervention_status, intervention_weight = intervention_assertion_status(
        text,
        source_verified=bool(raw.get("source_verified")),
    )
    reported_move_scores = reported_currency_move_scores(
        text,
        currencies,
    )
    localized_parallel_currency_market = bool(
        LOCAL_PARALLEL_CURRENCY_MARKET_PATTERN.search(text)
    )
    reports_prior_market_move = bool(
        (reported_move_scores and INTERVENTION_MOVE_PATTERN.search(text))
        # An intervention explainer describes an already-known policy action,
        # even when its headline omits the price move that followed.  Treat it
        # as retrospective context so a late secondary recap cannot become a
        # fresh forward signal merely because another copy corroborates it.
        or (intervention_signal and RETROSPECTIVE_EXPLAINER_PATTERN.search(text))
        or (
            intervention_signal
            and RETROSPECTIVE_INTERVENTION_CAUSAL_PATTERN.search(text)
        )
        # Market-led headlines report the price reaction itself.  They may be
        # useful continuation context, but they are not the causal event that
        # produced that reaction (for example, "Stocks rally as oil drops").
        or REPORTED_MARKET_MOVE_HEADLINE_PATTERN.search(
            clean_headline.lower()
        )
        or (
            not official
            and SECONDARY_REPORTED_COMMODITY_STATE_PATTERN.search(
                clean_headline.lower()
            )
        )
        or LIVE_MULTI_ASSET_RECAP_HEADLINE_PATTERN.search(
            clean_headline.lower()
        )
        or REPORTED_CURRENCY_RELATIVE_PERFORMANCE_HEADLINE_PATTERN.search(
            clean_headline
        )
        # Pair-ticker headlines often state a newly observed price extreme
        # rather than using a generic rise/fall verb.  They are market-state
        # recaps as well (for example, "USDCAD trades to a new low").
        or REPORTED_FX_PAIR_EXTREME_HEADLINE_PATTERN.search(
            clean_headline.lower()
        )
        # Broker recaps often lead with a pair and its already-observed price
        # state (for example, "EUR/JPY steadies near 185.70 as ...").  That is
        # an outcome/market-state observation, not a new causal policy clock.
        or REPORTED_FX_PAIR_PRICE_STATE_HEADLINE_PATTERN.search(
            clean_headline.lower()
        )
        # Pair-led breakout/level headlines report an already observed rate
        # move even when they omit "new highs/lows" (for example, "USDJPY
        # jumps above its 100 day MA"). Preserve the two rate legs for replay
        # while excluding the recap from the current forward-news clock.
        or REPORTED_FX_PAIR_TECHNICAL_BREAKOUT_HEADLINE_PATTERN.search(
            clean_headline.lower()
        )
        # A causal or political clause can precede an explicit market-price
        # reaction (for example, "Trump warns ... — oil prices rise").  The
        # observed commodity move remains retrospective even though the
        # headline is not market-led, so keep it out of prospective scoring.
        or REPORTED_INLINE_COMMODITY_MOVE_PATTERN.search(
            clean_headline.lower()
        )
        or REPORTED_COMMODITY_SUPPLY_PRICE_RECAP_PATTERN.search(
            clean_headline.lower()
        )
        # Local pump-price roundups report an already observed retail/energy
        # price state.  A leading city/country token kept them outside the
        # broader market-led pattern (for example, "Kolkata fuel prices stay
        # steady as global oil rises").
        or REPORTED_LOCAL_FUEL_MARKET_MOVE_PATTERN.search(
            clean_headline.lower()
        )
    )
    secondary_conflict_duration_recap = bool(
        not official
        and re.search(
            r"\b(?:war|conflict)\s+enters\s+(?:its\s+)?"
            r"(?:an?\s+)?(?:first|second|third|fourth|fifth|sixth|seventh|"
            r"eighth|ninth|tenth|eleventh|twelfth|\d+(?:st|nd|rd|th)?)\s+"
            r"(?:month|year)\b",
            clean_headline,
            flags=re.I,
        )
    )
    if secondary_conflict_duration_recap:
        reports_prior_market_move = True
    non_catalyst_context = bool(
        not official and NON_CATALYST_CONTEXT_PATTERN.search(text)
    )
    secondary_analysis_context = bool(
        not official
        and re.search(
            r"(?:[-\u2013\u2014|\ufffd]\s*analysis\b|"
            r"\banalysis\s*[-\u2013\u2014|\ufffd])",
            clean_headline,
            flags=re.I,
        )
    )
    commodity_operational_metric_context = bool(
        not official
        and re.search(
            r"\b(?:oil|crude|natural\s+gas|gas)\b.{0,72}"
            r"\b(?:drilling\s+)?rig(?:s|\s+count)?\b|"
            r"\b(?:drilling\s+)?rig(?:s|\s+count)?\b.{0,72}"
            r"\b(?:oil|crude|natural\s+gas|gas)\b",
            clean_headline,
            flags=re.I,
        )
    )
    secondary_market_policy_expectation = bool(
        not official
        and SECONDARY_MARKET_POLICY_EXPECTATION_PATTERN.search(
            clean_headline.lower()
        )
    )
    source_identity = str(raw.get("source_id") or "").strip().lower()
    source_name_identity = str(raw.get("source_name") or "").strip().lower()
    official_discovery_path = bool(
        official
        or "official_search" in source_identity
        or "official search" in source_name_identity
    )
    secondary_inflation_expectations_context = bool(
        not official_discovery_path
        and re.search(
            r"\binflation\s+(?:expectations?|forecasts?|outlook)\b",
            text,
            flags=re.I,
        )
    )
    market_relevance = sum(1 for term in MARKET_TERMS if phrase_present(text, term))
    non_fx_equity_earnings_preview = bool(
        (
            (
                re.search(
                    r"^\s*forex\s+signals\b.*\bearnings\s+preview\b",
                    clean_headline,
                    flags=re.I,
                )
                and not mentioned_currencies
                and not policy_currencies
            )
            or (
                not official
                and re.search(
                    r"\b(?:stocks?|shares?|equities)\b",
                    clean_headline,
                    flags=re.I,
                )
                and re.search(
                    r"\b(?:worth\s+watching|stocks?\s+to\s+watch|"
                    r"investment\s+case|earnings\s+preview)\b",
                    clean_headline,
                    flags=re.I,
                )
            )
        )
    )
    non_fx_false_term_context = bool(
        (
            re.search(r"\b(?:football|premier\s+league|team\s+costs?)\b", text)
            and re.search(r"\binflation\b", text)
        )
        or (
            re.search(r"\bfake\s+currency\b", text)
            and re.search(
                r"\b(?:fraud|police|probe|arrest|counterfeit|crime)\b",
                text,
            )
        )
    )
    non_fx_banknote_design = bool(
        re.search(r"\b(?:euro|currency)\s+banknotes?\b", text)
        and re.search(
            r"\b(?:artwork|beethoven|birds?|design|look|motif|theme|vote)\b",
            text,
        )
        and not re.search(
            r"\b(?:issuance|issue|circulation|cash\s+access|counterfeit|"
            r"monetary\s+policy)\b",
            text,
        )
    )
    non_fx_ceremonial_remarks = bool(
        re.search(
            r"\b(?:renaming|naming|dedication|commemoration|memorial|award)\s+"
            r"ceremon(?:y|ies)\b|\bmemorial\s+service\b|\bpay\s+tribute\b",
            clean_headline,
            flags=re.I,
        )
    )
    official_defense_nonmarket_health_guard = bool(
        official
        and clean_text(raw.get("source_id")) == "us_dow_releases_direct_v1"
        and any(
            re.search(pattern, text, flags=re.I)
            for pattern in OFFICIAL_DEFENSE_NONMARKET_HEALTH_PATTERNS
        )
    )
    official_non_market_program = bool(
        official
        and any(
                re.search(pattern, text, flags=re.I)
            for pattern in OFFICIAL_NON_MARKET_PROGRAM_PATTERNS
        )
    )
    non_fx_cash_handling_supply = bool(
        # "Currency supply" in an ATM/cash-management company's earnings
        # headline describes banknote logistics, not an FX, energy, or
        # commodity supply shock.  Require both cash-handling and corporate
        # results language so official currency issuance remains observable.
        re.search(
            r"\b(?:atms?|cash[- ]handling|cash[- ]management|banknote|"
            r"currency[- ]supply)\b",
            clean_headline,
            flags=re.I,
        )
        and re.search(
            r"\b(?:q[1-4]|quarter(?:ly)?|earnings?|eps|profit|revenue|"
            r"volumes?|company|systems?)\b",
            clean_headline,
            flags=re.I,
        )
    )
    trade_policy_legal_challenge = bool(
        re.search(
            r"\b(?:tariffs?|trade\s+war|import\s+tax(?:es)?|"
            r"customs\s+dut(?:y|ies)|section\s+301)\b",
            text,
            flags=re.I,
        )
        and re.search(
            r"\b(?:sue|sues|sued|suing|lawsuit|"
            r"mov(?:e|es|ed|ing)\s+(?:to\s+)?court|legal\s+challenge|"
            r"court\s+challenge|challenge(?:s|d|ing)?)\b",
            text,
            flags=re.I,
        )
    )
    non_fx_corporate_commodity_results = bool(
        not official
        and re.search(
            r"\b(?:q[1-4]|quarter(?:ly)?|earnings?|eps|profit|revenue)\b",
            clean_headline,
            flags=re.I,
        )
        and re.search(
            r"\b(?:oil|crude|gold|silver)\s+(?:prices?\s+)?"
            r"(?:rise|rises|rose|surge|surges|jump|jumps|fall|falls|fell|"
            r"drop|drops|slide|slides|tumble|tumbles)\b",
            clean_headline,
            flags=re.I,
        )
    )
    issuer_scope_first_seen = first_seen
    if issuer_scope_first_seen.tzinfo is None:
        issuer_scope_first_seen = issuer_scope_first_seen.replace(
            tzinfo=dt.timezone.utc
        )
    issuer_scope_first_seen = issuer_scope_first_seen.astimezone(dt.timezone.utc)
    secondary_market_state_guard_activation = parse_datetime(
        SECONDARY_MARKET_STATE_GUARD_ACTIVATED_UTC_V1
    )
    secondary_market_state_guard_activation_eligible = bool(
        (secondary_analysis_context or commodity_operational_metric_context)
        and secondary_market_state_guard_activation is not None
        and issuer_scope_first_seen >= secondary_market_state_guard_activation
    )
    conflict_duration_recap_guard_activation = parse_datetime(
        CONFLICT_DURATION_RECAP_GUARD_ACTIVATED_UTC_V1
    )
    conflict_duration_recap_guard_activation_eligible = bool(
        secondary_conflict_duration_recap
        and conflict_duration_recap_guard_activation is not None
        and issuer_scope_first_seen >= conflict_duration_recap_guard_activation
    )
    candidate_policy_document_type = (
        policy_document_type(title, text)
        if official
        and source_currencies
        and bool(raw.get("source_direct", raw.get("source_verified", False)))
        else ""
    )
    legacy_issuer_bound_policy_communication = issuer_bound_policy_communication(
        raw,
        title=title,
        text=text,
        source_currencies=source_currencies,
        first_seen=first_seen,
        document_type=candidate_policy_document_type,
    )
    source_bound_policy_communication = configured_authority_policy_communication(
        raw,
        title=title,
        text=text,
        source_currencies=source_currencies,
        document_type=candidate_policy_document_type,
    )
    source_binding_activation = parse_datetime(
        ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_ACTIVATED_UTC_V2
    )
    source_binding_activation_eligible = bool(
        source_bound_policy_communication
        and source_binding_activation is not None
        and issuer_scope_first_seen >= source_binding_activation
    )
    if source_bound_policy_communication and source_binding_activation_eligible:
        issuer_bound_policy_communication_contract_id = (
            ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_CONTRACT_ID_V2
        )
        issuer_bound_policy_communication_cohort_id = (
            ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_COHORT_ID_V2
        )
        issuer_bound_policy_communication_activated_utc = (
            ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_ACTIVATED_UTC_V2
        )
        issuer_bound_policy_communication_binding_method = (
            "configured_authority_communication_source_v2"
        )
        issuer_bound_policy_communication_contract_activation_eligible = True
    elif legacy_issuer_bound_policy_communication:
        issuer_bound_policy_communication_contract_id = (
            ISSUER_BOUND_POLICY_COMMUNICATION_CONTRACT_ID
        )
        issuer_bound_policy_communication_cohort_id = (
            ISSUER_BOUND_POLICY_COMMUNICATION_COHORT_ID
        )
        issuer_bound_policy_communication_activated_utc = (
            ISSUER_BOUND_POLICY_COMMUNICATION_ACTIVATED_UTC
        )
        issuer_bound_policy_communication_binding_method = "document_identity_v1"
        issuer_bound_policy_communication_contract_activation_eligible = True
    elif source_bound_policy_communication:
        # Reclassifying an older source row may correct its diagnostic scope,
        # but it can never acquire prospective proof standing retroactively.
        issuer_bound_policy_communication_contract_id = (
            ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_CONTRACT_ID_V2
        )
        issuer_bound_policy_communication_cohort_id = (
            ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_COHORT_ID_V2
        )
        issuer_bound_policy_communication_activated_utc = (
            ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_ACTIVATED_UTC_V2
        )
        issuer_bound_policy_communication_binding_method = (
            "configured_authority_communication_source_v2_diagnostic_pre_activation"
        )
        issuer_bound_policy_communication_contract_activation_eligible = False
    else:
        issuer_bound_policy_communication_contract_id = ""
        issuer_bound_policy_communication_cohort_id = ""
        issuer_bound_policy_communication_activated_utc = (
            ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_ACTIVATED_UTC_V2
        )
        issuer_bound_policy_communication_binding_method = ""
        issuer_bound_policy_communication_contract_activation_eligible = False
    issuer_bound_policy_communication_observation = bool(
        legacy_issuer_bound_policy_communication
        or source_bound_policy_communication
    )
    official_policy_release = bool(
        official
        and source_currencies
        # A speech can quote a prior statement or rate decision.  Its document
        # identity takes precedence over those body references, so the speech
        # cannot replace the latest stance-bearing decision.
        and not issuer_bound_policy_communication_observation
        and any(
            re.search(pattern, text, flags=re.I)
            for pattern in OFFICIAL_POLICY_RELEASE_PATTERNS
        )
    )
    issuer_bound_policy_attachment = boj_issuer_bound_policy_attachment(
        raw,
        title=title,
        text=text,
        source_currencies=source_currencies,
    )
    issuer_bound_policy_context = bool(
        official_policy_release
        or issuer_bound_policy_attachment
        or issuer_bound_policy_communication_observation
    )
    japan_external_policy_pressure = japan_external_policy_pressure_research(
        raw,
        text=text,
        source_currencies=source_currencies,
    )
    japan_external_policy_pressure_activation = parse_datetime(
        JAPAN_EXTERNAL_POLICY_PRESSURE_ACTIVATED_UTC_V1
    )
    japan_external_policy_pressure_activation_eligible = bool(
        japan_external_policy_pressure
        and japan_external_policy_pressure_activation is not None
        and issuer_scope_first_seen
        >= japan_external_policy_pressure_activation
    )
    if issuer_bound_policy_context and source_currencies:
        # Foreign central banks and currencies commonly appear in the policy
        # backdrop. The issuer binding is authoritative for this source row;
        # those references remain mentioned entities, not independent direct
        # directional legs.
        mentioned_currencies = sorted(set(source_currencies))
        currencies = list(mentioned_currencies)
    document_type = (
        (
            "policy_communication"
            if source_bound_policy_communication
            else candidate_policy_document_type
            or policy_document_type(title, text)
        )
        if issuer_bound_policy_context
        else ""
    )
    non_stance_liquidity_implementation = bool(
        issuer_bound_policy_context
        and policy_non_stance_liquidity_implementation(title, text)
    )
    primary_spending_release_direction = 0
    activity_release_direction = 0
    primary_inflation_release = False
    if (
        official
        and str(raw.get("source_role") or "") == "primary_statistical_release"
        and source_currencies
    ):
        # Official listing titles can legitimately contain an internal
        # ``Media Release - ...`` separator; publisher-suffix stripping would
        # discard the actual release claim in that form.
        release_headline = title.lower()
        primary_inflation_release = bool(
            re.search(
                r"\b(?:inflation|consumer prices?|consumer price index|"
                r"konsumentenpreise?|verbraucherpreise?|teuerung)\b",
                text,
                flags=re.I,
            )
        )
        if any(
            re.search(pattern, release_headline, flags=re.I)
            for pattern in PRIMARY_SPENDING_RELEASE_POSITIVE_PATTERNS
        ):
            primary_spending_release_direction = 1
        elif any(
            re.search(pattern, release_headline, flags=re.I)
            for pattern in PRIMARY_SPENDING_RELEASE_NEGATIVE_PATTERNS
        ):
            primary_spending_release_direction = -1
    release_headline = clean_headline.lower()
    if any(
        re.search(pattern, release_headline, flags=re.I)
        for pattern in ACTIVITY_RELEASE_POSITIVE_PATTERNS
    ):
        activity_release_direction = 1
    elif any(
        re.search(pattern, release_headline, flags=re.I)
        for pattern in ACTIVITY_RELEASE_NEGATIVE_PATTERNS
    ):
        activity_release_direction = -1
    if issuer_bound_policy_context:
        relevant = True
    elif market_relevance == 0 and not any(currency in currencies for currency in CORE_CURRENCIES):
        relevant = False
    elif market_relevance == 0 and official:
        relevant = False
    else:
        relevant = True
    if intervention_signal:
        relevant = True
    if (
        non_fx_equity_earnings_preview
        or non_fx_corporate_commodity_results
        or non_fx_false_term_context
        or non_fx_banknote_design
        or non_fx_ceremonial_remarks
        or official_non_market_program
        or official_non_market_research
        or official_non_market_administrative
        or non_fx_cash_handling_supply
    ):
        # Some equity SEO roundups are prefixed with ``Forex Signals`` even
        # though the entire headline is an earnings calendar. Preserve the
        # database row for audit, but do not publish it as FX context merely
        # because ``forex`` counted as a market term.
        relevant = False

    direction_text = (
        policy_direction_text(title, text, document_type)
        if issuer_bound_policy_context
        else headline_text
        if release_subject_currencies
        else text
    )
    hawkish = phrase_total(direction_text, HAWKISH_TERMS)
    dovish = phrase_total(direction_text, DOVISH_TERMS)
    reduced_tightening_expectations = bool(
        re.search(
            r"\b(?:rate hikes?|tightening)\b.{0,48}"
            r"\b(?:in doubt|less likely|unlikely|reduced|lower|lowered|"
            r"easing|eased|cool(?:s|ed|ing)?|fade(?:s|d|ing)?|"
            r"drop(?:s|ped|ping)?|delay(?:s|ed|ing)?|weaker|"
            r"no move)\b",
            direction_text,
            flags=re.I,
        )
        or re.search(
            r"\b(?:reduce[sd]?|lower(?:s|ed)?|ease[sd]?|limit(?:s|ed)?|"
            r"weaken(?:s|ed)?|dampen(?:s|ed)?|soft(?:er|ens?|ened|ening)?|"
            r"cool(?:s|ed|ing)?|fade(?:s|d|ing)?|delay(?:s|ed|ing)?)\b.{0,48}"
            r"\b(?:rate[- ]hike|tightening)\s+(?:expectations?|pressure|"
            r"bets?|odds|case|risks?)\b",
            direction_text,
            flags=re.I,
        )
        or re.search(
            r"\bpare(?:s|d)?\b.{0,16}\b(?:bets?|odds|expectations?)\b"
            r".{0,48}\b(?:rate hikes?|tightening)\b",
            direction_text,
            flags=re.I,
        )
        or re.search(
            r"\b(?:does|do|did|will|would|may|might|can|could)\s+not\s+"
            r"(?:expect|forecast|anticipate|see)\b.{0,64}"
            r"\b(?:rate hikes?|tightening|higher rates?)\b",
            direction_text,
            flags=re.I,
        )
        or re.search(
            r"\b(?:rate[- ]hike|tightening)\s+(?:debate|case|expectations?|"
            r"bets?|odds|chances?|probability)\b.{0,72}"
            r"\b(?:cool(?:s|ed|ing)?|fade(?:s|d|ing)?|drop(?:s|ped|ping)?|"
            r"delay(?:s|ed|ing)?|no move|mudd(?:y|ies|ied|ying)|"
            r"cloud(?:s|ed|ing)?)\b",
            direction_text,
            flags=re.I,
        )
        or re.search(
            r"\b(?:probability|odds|chance|chances)\b.{0,64}"
            r"\b(?:rate[- ]hike|tightening)\b.{0,48}"
            r"\b(?:cool(?:s|ed|ing)?|fade(?:s|d|ing)?|drop(?:s|ped|ping)?|"
            r"delay(?:s|ed|ing)?|declin(?:e|es|ed|ing)|fall(?:s|en|ing)?|fell)\b",
            direction_text,
            flags=re.I,
        )
        or re.search(
            r"\b(?:weak|weaker|weakening|soft|softer|cooling)\b.{0,48}"
            r"\b(?:mudd(?:y|ies|ied|ying)|cloud(?:s|ed|ing)|undermin(?:e|es|ed|ing))\b"
            r".{0,48}\b(?:rate hikes?|tightening)\b",
            direction_text,
            flags=re.I,
        )
        or re.search(
            r"\b(?:less|not as)\s+aggressive\b.{0,24}"
            r"\b(?:rate hikes?|tightening)\b",
            direction_text,
            flags=re.I,
        )
    )
    if reduced_tightening_expectations:
        # The noun phrase "rate hike" is not hawkish when the predicate says
        # its probability or pressure is falling.
        hawkish = 0.0
        dovish = min(dovish, -0.65)
    rising_inflation_expectations = bool(
        re.search(
            r"\binflation\s+(?:expectations?|forecasts?|outlook)\b.{0,24}"
            r"\b(?:rise(?:s|n)?|rose|increas(?:e|es|ed|ing)|"
            r"tick(?:s|ed)?\s+up|pick(?:s|ed)?\s+up)\b",
            direction_text,
            flags=re.I,
        )
    )
    cooling_inflation_expectations = bool(
        not rising_inflation_expectations
        and (
        re.search(
            r"\binflation\s+(?:expectations?|forecasts?|outlook)\b.{0,32}"
            r"\b(?:cool(?:s|ed|ing)?(?:\s+down)?|fall(?:s|ing)?|fell|"
            r"drop(?:s|ped|ping)?|ease(?:s|d|ing)?|decline(?:s|d|ing)?)\b",
            direction_text,
            flags=re.I,
        )
        or re.search(
            r"\b(?:cool(?:s|ed|ing)?(?:\s+down)?|fall(?:s|ing)?|fell|"
            r"drop(?:s|ped|ping)?|ease(?:s|d|ing)?|decline(?:s|d|ing)?)\b"
            r".{0,32}\binflation\s+(?:expectations?|forecasts?|outlook)\b",
            direction_text,
            flags=re.I,
        )
        or re.search(
            r"\b(?:businesses?|economists?|markets?|consumers?)\s+"
            r"expect(?:s|ed|ing)?\s+inflation\s+to\s+"
            r"(?:cool|fall|drop|ease|decline|slow)\b",
            direction_text,
            flags=re.I,
        )
        or re.search(
            r"\binflation\s+(?:is\s+)?expected\s+to\s+"
            r"(?:cool|fall|drop|ease|decline|slow)\b",
            direction_text,
            flags=re.I,
        )
        )
    )
    if rising_inflation_expectations:
        # Use the first current-state expectation verb, not a later historical
        # clause such as "rises after recent falls".  The sign is retained as
        # a semantic hypothesis; unverified secondary surveys are gated below
        # pending causal policy-rate repricing.
        dovish = 0.0
        hawkish = max(hawkish, 0.5)
    if cooling_inflation_expectations:
        # Lower expected inflation reduces the associated central bank's
        # prospective tightening pressure.  This is an economic-meaning rule,
        # not a claim that every survey release will move its currency.  The
        # normal source/currency/assertion gates and prospective evidence
        # contract still apply.
        hawkish = 0.0
        dovish = min(dovish, -0.5)
    reduced_inflation_pressure = bool(
        re.search(
            r"\b(?:wage|pay)\s+growth\b.{0,32}\b(?:remains?|is|stays?)\s+"
            r"(?:moderate|contained|subdued)\b|"
            r"\b(?:wage|pay)\s+growth\b.{0,32}"
            r"\b(?:ease(?:s|d|ing)?|cool(?:s|ed|ing)?|slow(?:s|ed|ing)?)\b|"
            r"\bno\s+second[- ]round\s+inflation(?:ary)?\s+effects?\b|"
            r"\binflation(?:ary)?\s+pressures?\b.{0,32}"
            r"\b(?:ease(?:s|d|ing)?|cool(?:s|ed|ing)?|moderate(?:s|d|ing)?)\b",
            direction_text,
            flags=re.I,
        )
    )
    if reduced_inflation_pressure:
        hawkish = 0.0
        dovish = min(dovish, -0.4)
    if activity_release_direction < 0 and re.search(
        r"\brate[- ]hike (?:expectations?|bets?|odds|chances?)\b",
        release_headline,
        flags=re.I,
    ):
        # A weak activity release reducing the case for a hike must not become
        # hawkish merely because the headline contains the noun phrase
        # "rate-hike expectations".
        hawkish = 0.0
        dovish = min(dovish, -0.35)
    policy_data_dependent_guidance = bool(
        POLICY_DATA_DEPENDENT_GUIDANCE_PATTERN.search(title.lower())
    )
    assertion_status, assertion_weight = policy_assertion_status(
        title.lower(),
        source_verified=official,
    )
    monetary_impulse = clamp((hawkish + dovish) * assertion_weight)
    explicit_policy_action = (
        explicit_policy_decision_action(direction_text)
        if official_policy_release
        else ""
    )
    if explicit_policy_action == "cut":
        monetary_impulse = -1.0
    elif explicit_policy_action == "hike":
        monetary_impulse = 1.0
    elif explicit_policy_action == "hold":
        # A hold is a real event, but its direction depends on consensus and
        # guidance. Background wording must not manufacture a signed action.
        monetary_impulse = 0.0
    if non_stance_liquidity_implementation:
        # This RBA operational-liquidity speech explicitly says that the
        # issues discussed do not bear on the monetary-policy stance.  Its
        # citations and hypothetical rate language remain useful research
        # context, but cannot manufacture an AUD direction or supersede the
        # last stance-bearing policy decision.
        hawkish = 0.0
        dovish = 0.0
        monetary_impulse = 0.0
        explicit_policy_action = ""
    policy_stance_bearing_eligible = bool(
        official_policy_release and not non_stance_liquidity_implementation
    )
    if secondary_market_roundup:
        # Preserve broad multi-topic explainers as context. Flattening their
        # stale and conditional clauses into one currency impulse created a
        # high-confidence false intervention signal in the retail-sales case.
        monetary_impulse = 0.0
    secondary_household_cost_context = bool(
        not official
        and any(
            re.search(pattern, text, flags=re.I)
            for pattern in SECONDARY_HOUSEHOLD_COST_CONTEXT_PATTERNS
        )
    )
    if secondary_household_cost_context:
        monetary_impulse = 0.0
    secondary_mixed_macro_direction_conflict = bool(
        not official
        and activity_release_direction
        and monetary_impulse
        and activity_release_direction * monetary_impulse < 0
    )
    current_source_role = (
        "primary_policy_communication"
        if issuer_bound_policy_communication_observation
        else source_role(raw)
    )
    official_context_role = current_source_role in {
        "primary_statistical_release",
        "primary_fiscal_debt_release",
        "primary_policy_release",
        "primary_policy_communication",
        "primary_policy_and_event_communication",
        "primary_central_bank_statistical_release",
    }
    official_foreign_policy_role = bool(
        official
        and current_source_role == "primary_foreign_policy_and_hormuz_release"
    )
    if official_foreign_policy_role:
        # Foreign-ministry article pages can append navigation, prior stories,
        # speeches and old statements to an otherwise routine diplomatic item.
        # Those template fragments must not turn "health cooperation" into a
        # fresh war/risk-off event.  Keep the article lead, stopping at the
        # publisher's related/archive boilerplate; a real condemnation,
        # attack, ceasefire or Hormuz action remains visible in the title/lead.
        foreign_policy_lead = re.split(
            r"\b(?:previous\s+related\s+stories|recent\s+news|"
            r"recent\s+statements|recent\s+speeches|about\s+us)\b",
            summary,
            maxsplit=1,
            flags=re.I,
        )[0]
        risk_text = f"{headline_text}. {foreign_policy_lead.lower()}"
    else:
        risk_text = (
            headline_text
            if release_subject_currencies
            else headline_text
            if official and official_context_role
            else text
        )
    risk_off = clamp(phrase_total(risk_text, RISK_OFF_TERMS), 0.0, 1.0)
    non_event_war = bool(
        re.search(
        r"\b(?:price|pricing|bidding|talent)\s+war\b", risk_text
        )
        or any(
            re.search(pattern, risk_text, flags=re.I)
            for pattern in NON_EVENT_WAR_PATTERNS
        )
    )
    if non_event_war:
        # Corporate competition, procurement, and retrospective macro-impact
        # phrases are not fresh geopolitical risk events.
        risk_off = 0.0
    if secondary_market_roundup:
        risk_off = 0.0
    risk_on = clamp(phrase_total(risk_text, RISK_ON_TERMS), 0.0, 1.0)
    geopolitical_hypothetical = bool(
        not official and GEOPOLITICAL_HYPOTHETICAL_PATTERN.search(headline_text)
    )
    ceasefire_undermined = bool(CEASEFIRE_UNDERMINED_PATTERN.search(headline_text))
    if geopolitical_hypothetical:
        risk_off = 0.0
        risk_on = 0.0
    if ceasefire_undermined:
        risk_on = 0.0
    supply_route_relief = any(
        re.search(pattern, risk_text, flags=re.I)
        for pattern in SUPPLY_ROUTE_RELIEF_PATTERNS
    )
    if supply_route_relief:
        risk_off = 0.0
        risk_on = max(risk_on, 0.6)
        relevant = True
    fresh_deescalation = not ceasefire_undermined and (
        supply_route_relief
        or any(
            phrase_present(risk_text, phrase)
            for phrase in (
                "ceasefire",
                "peace deal",
                "deal to reopen the strait of hormuz",
                "strait of hormuz deal",
                "tariff rollback",
                "trade agreement",
            )
        )
    )
    if fresh_deescalation:
        # A concrete agreement or reopening report is itself the catalyst,
        # even when a compact headline contains no currency or market token.
        # Source corroboration and first-availability timing still determine
        # whether the clustered topic can publish a directional signal.
        relevant = True
    deescalation_proposal = any(
        re.search(pattern, risk_text, flags=re.I)
        for pattern in DEESCALATION_PROPOSAL_PATTERNS
    )
    deescalation_actualized = any(
        re.search(pattern, risk_text, flags=re.I)
        for pattern in DEESCALATION_ACTUALIZATION_PATTERNS
    )
    deescalation_proposal_only = bool(
        deescalation_proposal and not deescalation_actualized
    )
    deescalation_proposal_guard_activation = parse_datetime(
        DEESCALATION_PROPOSAL_GUARD_ACTIVATED_UTC_V1
    )
    deescalation_proposal_guard_activation_eligible = bool(
        deescalation_proposal_only
        and deescalation_proposal_guard_activation is not None
        and issuer_scope_first_seen >= deescalation_proposal_guard_activation
    )
    if deescalation_proposal_only:
        # A single actor asking for a ceasefire is negotiation intent, not a
        # completed de-escalation.  Preserve the observation for later event
        # research but do not manufacture a broad AUD/CAD/MXN/NOK/NZD/ZAR
        # risk-on basket (or its CHF/JPY/USD inverse).
        fresh_deescalation = False
        risk_on = 0.0
        relevant = True
    administrative_sanctions_cleanup = bool(
        re.search(
            r"\bsanctions?\s+(?:list\s+)?(?:removals?|delistings?)\b",
            risk_text,
            flags=re.I,
        )
        and re.search(
            r"\b(?:moderni[sz]ation|duplicate|defunct|deceased|data|list)\b",
            risk_text,
            flags=re.I,
        )
    )
    if administrative_sanctions_cleanup:
        # OFAC list hygiene is neither a new escalation nor necessarily a
        # geopolitical easing.  Keep it as official context without creating
        # a synthetic global haven/risk-currency impulse.
        risk_off = 0.0
        risk_on = 0.0
    sanctions_relief_or_waiver = bool(
        re.search(
            r"\bsanctions?\s+(?:relief|waivers?|easing|suspension|"
            r"lifting|lifted|removal|removed)\b|"
            r"\b(?:relax(?:es|ed|ing)?|ease(?:s|d|ing)?|lift(?:s|ed|ing)?|"
            r"waiv(?:e|es|ed|ing)|suspend(?:s|ed|ing)?)\b.{0,48}"
            r"\bsanctions?\b",
            risk_text,
            flags=re.I,
        )
    )
    if sanctions_relief_or_waiver:
        # Sanctions relief contains the same high-weight noun as an
        # imposition headline but reports the opposite policy action.  It is
        # useful official context, not a generic global haven/risk basket.
        # A country-specific easing hypothesis needs its own governed mapping
        # and cannot inherit the sanctions-escalation direction.
        risk_off = 0.0
        risk_on = 0.0
    official_sanctions_escalation = bool(
        official
        and clean_text(raw.get("source_id")) == "us_treasury_press"
        and not administrative_sanctions_cleanup
        and not sanctions_relief_or_waiver
        and (
            re.search(
                r"\b(?:operation economic outcast|"
                r"campaign against (?:the )?(?:iranian regime|iran)|"
                r"economic onslaught against iran)\b",
                risk_text[:5000],
                flags=re.I,
            )
            or re.search(
                r"\b(?:treasury|ofac)\b.{0,64}"
                r"\b(?:sanctions?|sanctioned|sanctioning|designates?|"
                r"designated)\b",
                headline_text,
                flags=re.I,
            )
        )
    )
    if official_sanctions_escalation:
        # Treasury's press-release feed mixes domestic finance documents with
        # OFAC/geopolitical actions.  An exact first-party sanctions campaign
        # must not be suppressed merely because the source has a broad policy
        # role.  This remains a governed risk-basket hypothesis, not automatic
        # execution authority.
        risk_off = max(risk_off, 0.75)
        relevant = True
    failed_deescalation = any(
        re.search(pattern, risk_text, flags=re.I)
        for pattern in FAILED_DEESCALATION_PATTERNS
    )
    mixed_threat_deal_context = bool(
        re.search(
            r"\b(?:threaten(?:s|ed|ing)?|warn(?:s|ed|ing)?)\b.{0,96}"
            r"\b(?:ceasefire|truce|peace\s+deal|hormuz\s+deal|deal)\b|"
            r"\b(?:ceasefire|truce|peace\s+deal|hormuz\s+deal|deal)\b.{0,96}"
            r"\b(?:threaten(?:s|ed|ing)?|warn(?:s|ed|ing)?)\b",
            risk_text,
            flags=re.I,
        )
    )
    if mixed_threat_deal_context:
        # One secondary headline can contain a tentative negotiation and a
        # simultaneous threat.  Flattening the word "deal" into risk-on
        # discards the opposing clause; keep the mixed narrative as context.
        fresh_deescalation = False
        risk_on = 0.0
    if failed_deescalation:
        # "Ceasefire ends" is an escalation, not a de-escalation merely
        # because the positive keyword is present.
        risk_on = 0.0
        risk_off = max(risk_off, 0.75)
    cancelled_conflict_action = any(
        re.search(pattern, risk_text, flags=re.I)
        for pattern in CANCELLED_CONFLICT_ACTION_PATTERNS
    )
    if cancelled_conflict_action and not failed_deescalation:
        # A cancelled, paused, or deliberately deferred strike is the opposite
        # of the attack phrase it contains.  Preserve it as a modest
        # de-escalation hypothesis; it is still research-only and requires
        # source corroboration and price-state confirmation downstream.
        risk_off = 0.0
        risk_on = max(risk_on, 0.4)
        relevant = True
    direct_conflict_escalation = (
        not cancelled_conflict_action
        and not supply_route_relief
        and not geopolitical_hypothetical
        and (failed_deescalation or any(
        re.search(pattern, risk_text, flags=re.I)
        for pattern in DIRECT_CONFLICT_ESCALATION_PATTERNS
        ))
    )
    ongoing_conflict_status = any(
        re.search(pattern, risk_text, flags=re.I)
        for pattern in ONGOING_CONFLICT_STATUS_PATTERNS
    )
    new_conflict_action = any(
        re.search(pattern, headline_text, flags=re.I)
        for pattern in NEW_CONFLICT_ACTION_PATTERNS
    )
    if ongoing_conflict_status and not new_conflict_action and not failed_deescalation:
        # Persistent closure, blockade, or stalemate wording is not a new
        # escalation. It can remain context, but cannot reset the catalyst
        # clock or seed another broad risk basket.
        direct_conflict_escalation = False
        risk_off = 0.0
    if direct_conflict_escalation and not non_event_war:
        risk_off = max(risk_off, 0.75)
        # Concrete attacks are market-relevant even when a compact headline
        # omits generic tokens such as "war", "oil", or a currency name.
        # Timeliness and corroboration are still enforced during clustering.
        relevant = True
    energy_supply_geopolitical_event = bool(
        (direct_conflict_escalation or fresh_deescalation)
        and re.search(
            r"\b(?:strait of hormuz|hormuz|oil tanker|petroleum tanker|"
            r"oil (?:site|facility|field|terminal|pipeline|refinery)|"
            r"(?:oil|crude) (?:blockade|offers?|supply|supplies|shipments?|exports?|flows?)|"
            r"energy (?:site|facility|terminal|pipeline))\b",
            risk_text,
            flags=re.I,
        )
    )
    fresh_risk_off = bool(
        not sanctions_relief_or_waiver
        and (
        official_sanctions_escalation
        or direct_conflict_escalation
        or any(
            phrase_present(risk_text, phrase)
            for phrase in (
                "banking crisis",
                "market crash",
                "stock selloff",
                "risk-off",
                "tariff escalation",
            )
        )
        or re.search(
            # Secondary headlines use several grammatical forms for the same
            # newly announced sanctions action ("announcement of", "launches
            # ... sanctions", "to detail ... sanctions push").  Keep the
            # wider wording research-only downstream, but do not silently
            # reduce it to generic market commentary merely because the verb
            # is more than 36 characters from ``sanctions``.
            r"\b(?:impos(?:e|es|ed|ing)|announc(?:e|es|ed|ing|ement)|"
            r"unveil(?:s|ed|ing)?|expand(?:s|ed|ing)?|"
            r"launch(?:es|ed|ing)?|detail(?:s|ed|ing)?)\b"
            r".{0,72}\bsanctions?\b",
            risk_text,
            flags=re.I,
        )
        )
    )
    if not official and risk_off > 0 and not fresh_risk_off:
        # A lone generic conflict word is not a new event. Market commentary
        # about yields, inflation, GDP, or oil during an existing war remains
        # context unless the headline reports an actual escalation action.
        risk_off = 0.0
    if issuer_bound_policy_context or has_structured_numeric_release:
        # A policy statement often discusses wars, tariffs, or oil as inputs.
        # That background is not a newly observed global-risk event.  The
        # same rule applies to a structured statistical release: a GDP/CPI
        # bulletin can discuss oil, wars, and comparison countries without
        # turning the one source-native numeric observation into independent
        # AUD/CAD/JPY/USD risk observations.
        risk_off = 0.0
        risk_on = 0.0
    if non_fx_ceremonial_remarks:
        # Official ceremonial pages can contain military vocabulary such as
        # "force", "defense", or historical battle references.  Those words
        # describe the ceremony, not a newly observed geopolitical shock.
        monetary_impulse = 0.0
        risk_off = 0.0
        risk_on = 0.0
        relevant = False
    if official_non_market_program:
        monetary_impulse = 0.0
        risk_off = 0.0
        risk_on = 0.0
        relevant = False
    if official_non_market_administrative:
        monetary_impulse = 0.0
        risk_off = 0.0
        risk_on = 0.0
        relevant = False
    if secondary_market_roundup:
        # Apply the boundary after every escalation/de-escalation transform.
        # A long technical article can append genuine conflict context after
        # the earlier preliminary reset; that background still is not a new
        # causal event observed by this market-state article.
        monetary_impulse = 0.0
        risk_off = 0.0
        risk_on = 0.0
    tokens = re.findall(r"[a-z][a-z'-]+", text)
    generic_sentiment = clamp(
        (
            sum(token in POSITIVE_WORDS for token in tokens)
            - sum(token in NEGATIVE_WORDS for token in tokens)
        )
        / max(3.0, math.sqrt(max(1, len(tokens)))),
    )

    scores: dict[str, float] = defaultdict(float)
    directional_currencies = (
        source_currencies
        or policy_subject_currencies
        or release_subject_currencies
        or policy_currencies
        or currencies
    )
    if monetary_impulse and not secondary_mixed_macro_direction_conflict:
        for currency in directional_currencies:
            scores[currency] += monetary_impulse
    if activity_release_direction and not secondary_mixed_macro_direction_conflict:
        for currency in directional_currencies:
            scores[currency] += 0.65 * activity_release_direction
    if risk_off:
        for currency in HAVENS:
            scores[currency] += 0.55 * risk_off
        for currency in RISK_CURRENCIES:
            if energy_supply_geopolitical_event and currency in OIL_EXPORTERS:
                # An energy-supply shock has opposing channels for commodity
                # exporters: generic de-risking can weaken them while higher
                # oil can strengthen them.  Without an observed authoritative
                # commodity reaction, a headline-only rule may not choose the
                # sign.  Price-reaction arms retain the event and resolve each
                # pair leg prospectively.
                continue
            scores[currency] -= 0.65 * risk_off
    if risk_on:
        for currency in HAVENS:
            scores[currency] -= 0.35 * risk_on
        for currency in RISK_CURRENCIES:
            if energy_supply_geopolitical_event and currency in OIL_EXPORTERS:
                continue
            scores[currency] += 0.55 * risk_on
    commodity_direction_text = (
        headline_text if release_subject_currencies else text
    )
    oil_up = any(
        phrase in commodity_direction_text
        for phrase in (
            "oil prices rise",
            "oil prices surge",
            "oil rises",
            "oil surges",
            "oil jumps",
            "crude rises",
            "crude surges",
            "crude oil rises",
            "crude oil surges",
            "supply disruption",
        )
    ) or bool(
        re.search(
            r"\b(?:oil|crude(?:\s+oil)?|brent|wti)\b.{0,96}\b"
            r"(?:prices?\s+)?(?:rise(?:s|n)?|rose|surge(?:s|d)?|"
            r"jump(?:s|ed)?|rall(?:y|ies|ied)|soar(?:s|ed)?)\b",
            commodity_direction_text,
            flags=re.I,
        )
    )
    oil_down = any(
        phrase in commodity_direction_text
        for phrase in (
            "oil prices fall",
            "oil prices drop",
            "oil falls",
            "oil drops",
            "oil slides",
            "oil tumbles",
            "oil eases",
            "crude falls",
            "crude drops",
            "crude oil falls",
            "crude oil drops",
            "supply glut",
        )
    )
    local_oil_up, local_oil_down = clause_local_oil_direction(
        commodity_direction_text
    )
    if local_oil_up or local_oil_down:
        # Prefer the direction in the commodity's own clause over broad
        # fallback matching that may cross into another asset's clause.
        oil_up, oil_down = local_oil_up, local_oil_down
    elif oil_up and oil_down:
        # Conflicting broad-only matches are not safe directional evidence.
        oil_up = False
        oil_down = False
    if issuer_bound_policy_context or has_structured_numeric_release:
        # Macro inputs discussed inside a central-bank decision are context
        # for that decision, not a separately observed commodity event.  A
        # structured statistical bulletin receives the same protection.
        oil_up = False
        oil_down = False
    if (
        non_fx_corporate_commodity_results
        or non_fx_ceremonial_remarks
        or non_fx_cash_handling_supply
        or secondary_market_roundup
    ):
        # The commodity clause explains a company's reported result; it is not
        # a newly observed macro shock.
        oil_up = False
        oil_down = False
    if oil_up or oil_down:
        oil_sign = 1.0 if oil_up else -1.0
        for currency in OIL_EXPORTERS:
            scores[currency] += 0.5 * oil_sign
        scores["JPY"] -= 0.2 * oil_sign
    if intervention_signal:
        for currency, value in intervention_currency_scores(
            text,
            currencies,
            assertion_weight=intervention_weight,
        ).items():
            scores[currency] += value
    if reports_prior_market_move:
        # A headline such as "Sterling slips as rate-hike bets grow" reports
        # an observed move.  Preserve that direction for retrospective
        # reaction mapping and let it override contradictory macro wording,
        # but keep it out of the forward pair signal below.
        for currency, value in reported_move_scores.items():
            scores[currency] = value

    unverified_market_numeric_claim = bool(
        not official
        and re.search(
            r"\b(?:oil|crude|brent|wti)\b.{0,64}(?:\$|usd\s*)"
            r"\d+(?:\.\d+)?\b",
            text,
            flags=re.I,
        )
    )
    if unverified_market_numeric_claim:
        # A secondary article's quoted market price is an outcome observation,
        # not an authoritative causal macro input. Retain it for retrospective
        # reconciliation while preventing a fresh commodity direction.
        for currency in set(OIL_EXPORTERS) | {"JPY"}:
            scores.pop(currency, None)
        oil_up = False
        oil_down = False
        reports_prior_market_move = True

    if (
        non_fx_equity_earnings_preview
        or non_fx_corporate_commodity_results
        or non_fx_false_term_context
        or non_fx_banknote_design
        or non_fx_ceremonial_remarks
        or official_non_market_program
        or official_non_market_research
        or official_non_market_administrative
        or non_fx_cash_handling_supply
    ):
        # An excluded non-FX row must not retain a latent directional score in
        # its immutable payload.  Downstream audits inspect raw rows as well as
        # the published feed, so `relevant = false` alone is not a sufficient
        # boundary.
        scores = {}
    if (
        secondary_analysis_context
        or commodity_operational_metric_context
        or secondary_conflict_duration_recap
    ):
        # An analysis label marks interpretation rather than a new causal
        # event, while rig counts measure upstream activity rather than the
        # direction of executable oil prices.  Both remain useful context;
        # neither may inherit an immediate FX basket from words such as
        # ``blockade``, ``oil`` or ``rise``.
        scores = {}
        risk_off = 0.0
        risk_on = 0.0
        oil_up = False
        oil_down = False
    if non_stance_liquidity_implementation:
        # Fail closed even if a future wording change happens to trigger an
        # intervention, reported-move, commodity, or activity phrase above.
        scores = {}

    scores = {
        currency: round(clamp(value), 6)
        for currency, value in scores.items()
        if abs(value) >= 0.05
    }
    structured_absolute_scores = dict(scores)
    causal_consensus_available = bool(
        optional_float(raw.get("consensus_value")) is not None
        and str(raw.get("consensus_capture_state") or "")
        in {
            "prospectively_captured_pre_release",
            "causal_pre_release_snapshot",
        }
    )
    if has_structured_numeric_release and not causal_consensus_available:
        # Absolute levels such as 3.5% wage growth cannot determine whether a
        # release is hawkish or dovish. Preserve the semantic hypothesis for
        # research, but do not publish a direction until surprise and rate
        # repricing are causally available.
        scores = {}
    official_fiscal_narrative_research_scores: dict[str, float] = {}
    if (
        official
        and source_role(raw) == "primary_fiscal_debt_release"
        and not official_policy_release
        and not has_structured_numeric_release
        and scores
    ):
        # A treasury/ministry narrative can contain selective economic claims
        # and contextual geopolitical language without reporting a new,
        # causally benchmarked release. Preserve the hypothesis for research,
        # but require structured facts or an explicit policy action before a
        # fiscal publication can become a directional currency event.
        official_fiscal_narrative_research_scores = dict(scores)
        scores = {}
    official_duration_liquidity_research_scores: dict[str, float] = {}
    if official_duration_liquidity_policy:
        # Larger long-duration Treasury buybacks are a predeclared
        # lower-yield/USD-negative hypothesis. It cannot publish a direction
        # by itself: the watchlist requires at least two aligned executable
        # pair reactions in the first ten minutes, and retains the independent
        # rate context for later prospective proof.
        official_duration_liquidity_research_scores = {"USD": -0.55}
        scores = {}
    japan_external_policy_pressure_research_scores = (
        {"JPY": 0.45} if japan_external_policy_pressure else {}
    )
    non_catalyst_research_scores: dict[str, float] = {}
    if non_catalyst_context and scores:
        # The publisher explicitly states that the information was known or
        # already priced. Keep the semantic direction for reaction research,
        # never as a new forward catalyst.
        non_catalyst_research_scores = dict(scores)
        scores = {}
    secondary_market_policy_expectation_scores: dict[str, float] = {}
    if secondary_market_policy_expectation and scores:
        # A secondary report that traders are pricing or bracing for a policy
        # stance is an expectations-state observation, not a new central-bank
        # action.  Syndicated paraphrases must not turn it into apparent
        # multi-source confirmation.  Retain the semantic hypothesis for
        # prospective rate/price-response research only.
        secondary_market_policy_expectation_scores = dict(scores)
        scores = {}
    secondary_inflation_expectations_scores: dict[str, float] = {}
    if secondary_inflation_expectations_context and scores:
        # A survey or secondary report about inflation expectations has a
        # plausible rate channel but not a causal currency sign on its own.
        # Keep the semantic hypothesis for governed response research and
        # withhold forward publication until contemporaneous policy repricing
        # or an authoritative source contract resolves it.
        secondary_inflation_expectations_scores = {
            currency: value
            for currency, value in scores.items()
            if not source_currencies or currency in set(source_currencies)
        }
        if not secondary_inflation_expectations_scores:
            secondary_inflation_expectations_scores = dict(scores)
        scores = {}
    if (
        (
            has_structured_numeric_release
            or issuer_bound_policy_context
            or official_non_market_research
            or official_non_market_administrative
        )
        and source_currencies
    ):
        # Preserve only the issuing economy after every semantic transform.
        # This second boundary is deliberate: risk, commodity, intervention,
        # reported-move and foreign-policy-context rules run after the initial
        # currency extraction and otherwise can silently re-expand one issuer
        # observation to unrelated currencies mentioned in its prose.
        native = set(source_currencies)
        scores = {
            currency: value
            for currency, value in scores.items()
            if currency in native
        }
        currencies = sorted(native)
    elif semantic_subject_currencies:
        # Subject binding is also applied after global risk/commodity and
        # reported-move transforms so no later rule can re-expand one release
        # or cross-authority policy thesis into unrelated currency factors.
        native = set(semantic_subject_currencies)
        scores = {
            currency: value
            for currency, value in scores.items()
            if currency in native
        }
        currencies = sorted(native)
    semantic_claims = semantic_claim_decomposition(
        direction_text,
        directional_currencies or currencies,
    )
    semantic_claim_signs = {
        int(claim["rate_currency_sign"])
        for claim in semantic_claims
        if claim.get("rate_currency_sign") in {-1, 1}
    }
    explicit_labor_headline = bool(
        re.search(
            r"\b(?:labou?r\s+market|payrolls?|employment|unemployment|"
            r"job\s+vacanc(?:y|ies))\b",
            headline_text,
            flags=re.I,
        )
    )
    if semantic_claim_signs == {-1} and explicit_labor_headline:
        # Do not let an embedded phrase such as "another rate hike" reverse a
        # release whose actual claim is cooling employment. The semantic side
        # remains research-only until causal surprise/rate evidence exists.
        scores = {}
    headline_policy_signs: set[int] = set()
    if re.search(
        r"\b(?:rate[- ]hikes?|fed\s+hikes?|hiking\s+(?:the\s+)?(?:policy\s+)?rate|"
        r"tighten(?:s|ed|ing)?|higher\s+rates?)\b",
        headline_text,
        flags=re.I,
    ):
        headline_policy_signs.add(1)
    if re.search(
        r"\b(?:rate[- ]cuts?|cutting\s+(?:the\s+)?(?:policy\s+)?rate|"
        r"policy\s+easing|eas(?:e|es|ed|ing)\s+(?:policy|rates?)|"
        r"lower\s+rates?)\b",
        headline_text,
        flags=re.I,
    ):
        headline_policy_signs.add(-1)
    opposing_policy_claim_conflict = bool(
        not official
        and len(headline_policy_signs) > 1
        and len(set(directional_currencies or currencies)) <= 1
    )
    semantic_claim_conflict = bool(
        len(semantic_claim_signs) > 1 or opposing_policy_claim_conflict
    )
    semantic_conflict_scores = (
        dict(scores or structured_absolute_scores)
        if semantic_claim_conflict
        else {}
    )
    semantic_direction_scores = (
        {
            currency: round(0.35 * next(iter(semantic_claim_signs)), 6)
            for currency in (source_currencies or directional_currencies)
        }
        if len(semantic_claim_signs) == 1
        and (has_structured_numeric_release or explicit_labor_headline)
        and not causal_consensus_available
        else {}
    )
    if semantic_claim_conflict:
        # A composite report such as weak payrolls plus sticky inflation has
        # competing currency channels.  Preserve every claim and the legacy
        # score for research, but require actual-versus-consensus surprise and
        # rate-market repricing before it can publish a direction.
        scores = {}
    if localized_parallel_currency_market:
        # Cuba's MLC/parallel-market EUR and USD quotes are local exchange-rate
        # evidence, not a move in the globally traded EUR or USD instruments.
        scores = {}
        relevant = False
    if non_fx_corporate_commodity_results:
        scores = {}
        relevant = False
    if non_fx_cash_handling_supply:
        scores = {}
        relevant = False
    currencies = (
        sorted(set(source_currencies))
        if has_structured_numeric_release and source_currencies
        else sorted(set(currencies) | set(scores))
    )
    context_only = bool(
        (not official_policy_release or non_stance_liquidity_implementation)
        and not localized_parallel_currency_market
        and not non_fx_equity_earnings_preview
        and not non_fx_corporate_commodity_results
        and not non_fx_false_term_context
        and not non_fx_banknote_design
        and not non_fx_ceremonial_remarks
        and not official_non_market_program
        and not official_non_market_research
        and not official_non_market_administrative
        and not non_fx_cash_handling_supply
        and
        not scores
        and (
            relevant
            or official
            or reports_prior_market_move
            or non_catalyst_context
            or bool(official_fiscal_narrative_research_scores)
            or bool(official_duration_liquidity_research_scores)
            or assertion_status in {"unverified_speculation", "official_conditional"}
        )
    )
    if context_only:
        # Retain market-related headlines for audit and later rule research,
        # but do not publish them into the active FX event catalog when the
        # deterministic classifier found no currency impulse.
        # A verified structured numeric release remains relevant observation
        # context even when direction correctly abstains without consensus.
        relevant = bool(
            has_structured_numeric_release
            or official_duration_liquidity_policy
        )
    release_headline_text = clean_headline.lower()
    secondary_inflation_release_claim = bool(
        # A secondary report can still describe a real statistical release.
        # Require release-shaped evidence in the headline itself so opinion,
        # preview, market-recap, and generic "inflation fears/focus" articles
        # do not enter the historical topic ledger as measured inflation data.
        re.search(
            r"\b(?:cpi|ppi|consumer\s+price\s+index|producer\s+price\s+index)\b",
            release_headline_text,
            flags=re.I,
        )
        or re.search(
            r"\b(?:inflation|consumer\s+prices?|producer\s+prices?)\b"
            r".{0,64}\b(?:rose|risen|rises|increased|increases|accelerated|"
            r"fell|fallen|falls|decreased|decreases|slowed|slows|eased|eases|"
            r"unchanged|steady)\b.{0,48}\b(?:january|february|march|april|may|"
            r"june|july|august|september|october|november|december|q[1-4]|"
            r"\d+(?:\.\d+)?\s*%)\b",
            release_headline_text,
            flags=re.I,
        )
        or re.search(
            r"\b(?:january|february|march|april|may|june|july|august|"
            r"september|october|november|december|q[1-4]|"
            r"\d+(?:\.\d+)?\s*%)\b.{0,48}"
            r"\b(?:inflation|consumer\s+prices?|producer\s+prices?)\b",
            release_headline_text,
            flags=re.I,
        )
    )
    secondary_labor_release_claim = bool(
        re.search(
            r"\b(?:labou?r\s+market|payrolls?|employment|unemployment|"
            r"average\s+weekly\s+earnings|job\s+vacanc(?:y|ies))\b",
            release_headline_text,
            flags=re.I,
        )
        and re.search(
            r"\b(?:release|report|data|figures?|rose|fell|declined|grew|"
            r"cool(?:s|ed|ing)?|weak(?:er|ens|ening)?|strong(?:er|ens)?)\b",
            release_headline_text,
            flags=re.I,
        )
    )
    secondary_business_activity_release_claim = bool(
        re.search(
            r"\b(?:business\s+activity|purchasing\s+managers(?:'|’)?\s+index|"
            r"manufacturing\s+pmi|services\s+pmi|composite\s+pmi|flash\s+pmi)\b",
            release_headline_text,
            flags=re.I,
        )
        and re.search(
            r"\b(?:pick(?:s|ed|ing)?\s+up|expand(?:s|ed|ing)?|"
            r"contract(?:s|ed|ing)?|rise(?:s|n)?|rose|fall(?:s|en)?|fell|"
            r"accelerat(?:e|es|ed|ing)|slow(?:s|ed|ing)?|"
            r"strengthen(?:s|ed|ing)?|weaken(?:s|ed|ing)?|"
            r"hit(?:s|ting)?|surge(?:s|d|ing)?|"
            r"index|release|report|data|figures?)\b",
            release_headline_text,
            flags=re.I,
        )
    )
    structured_identity_text = " ".join(
        (
            clean_text(raw.get("event_series_id")),
            clean_text(raw.get("event_name")),
            clean_headline,
        )
    ).lower()
    structured_native_category = ""
    if has_structured_numeric_release:
        if re.search(
            r"\b(?:cpi|hicp|consumer prices?|inflation|producer prices?|ppi)\b",
            structured_identity_text,
        ):
            structured_native_category = "inflation_release"
        elif re.search(
            r"\b(?:unemployment|employment|payrolls?|labou?r market|wages?)\b",
            structured_identity_text,
        ):
            structured_native_category = "labor_release"
        elif re.search(
            r"\b(?:trade balance|international trade|current account)\b",
            structured_identity_text,
        ):
            structured_native_category = "trade_balance_release"
        elif re.search(
            r"\b(?:policy rate|interest rate|bank rate|cash rate|repo rate)\b",
            structured_identity_text,
        ):
            structured_native_category = "monetary_policy"
        elif re.search(
            r"\b(?:pmi|purchasing managers(?:'|’)? index|"
            r"business activity|consumer sentiment|consumer confidence)\b",
            structured_identity_text,
        ):
            structured_native_category = "business_activity_release"
        elif re.search(
            r"\b(?:gdp|gross domestic product|retail sales?|industrial "
            r"production|manufacturing output|economic activity)\b",
            structured_identity_text,
        ):
            structured_native_category = "growth_release"

    if official_non_market_administrative:
        category = "official_non_market_administrative"
        scope = "currency"
    elif official_non_market_research:
        category = "market_news"
        scope = "currency"
    elif official_duration_liquidity_policy:
        # Treasury duration-supply operations transmit through the sovereign
        # curve before spot FX. Keep them distinct from generic fiscal prose.
        category = "sovereign_duration_liquidity_policy"
        scope = "currency"
    elif structured_native_category:
        # The explicit series identity is the authoritative release type.
        # Long bulletins often discuss labour, inflation, trade, and energy;
        # those contextual sections must not relabel a GDP/CPI observation.
        category = structured_native_category
        scope = "currency"
    elif secondary_market_roundup:
        category = "market_news"
        scope = "currency"
    elif risk_off > 0:
        category = "risk_off_geopolitical_or_financial"
        scope = "all_pairs"
    elif risk_on > 0:
        category = "risk_on_deescalation"
        scope = "all_pairs"
    elif intervention_signal:
        category = "fx_intervention"
        scope = "currency"
    elif secondary_labor_release_claim:
        # A labour report remains a labour report even when its body discusses
        # the Bank of England or the inflation trade-off. Those are downstream
        # interpretation channels, not the release's source identity.
        category = "labor_release"
        scope = "currency"
    elif secondary_business_activity_release_claim:
        # Preserve the identity of a PMI/business-activity release even when
        # secondary commentary discusses its implications for a central bank.
        # The policy interpretation is downstream context, not the event type.
        category = "business_activity_release"
        scope = "currency"
    elif (
        issuer_bound_policy_context
        or policy_currencies
        or (
            (monetary_impulse or reduced_tightening_expectations)
            and not primary_inflation_release
            and not secondary_inflation_release_claim
            and not activity_release_direction
            and not primary_spending_release_direction
            and "inflation" not in text
        )
        or "monetary policy" in text
        or "interest rate" in text
        or "policy rate" in text
        or "rate policy" in text
        or "bank rate" in text
        or "repo rate" in text
        or "official cash rate" in text
    ):
        category = "monetary_policy"
        scope = "currency"
    elif (
        primary_inflation_release
        or secondary_inflation_release_claim
    ):
        category = "inflation_release"
        scope = "currency"
    elif (
        "inflation" in text
        or "consumer price" in text
        or "producer price" in text
    ):
        category = "inflation_context"
        scope = "currency"
    elif structured_event and re.search(
        r"\b(?:retail sales?|advance monthly sales|adv_mrts)\b",
        " ".join(
            (
                clean_text(raw.get("event_series_id")),
                clean_text(raw.get("event_name")),
                title,
            )
        ).lower(),
        flags=re.I,
    ):
        category = "growth_release"
        scope = "currency"
    elif trade_policy_legal_challenge:
        # Legal challenges to tariffs can contain incidental labour terms
        # (for example, "forced-labour tariffs").  The causal subject is
        # trade policy, not an employment release.
        category = "trade_policy"
        scope = "all_pairs"
    elif any(
        term in text
        for term in (
            "employment",
            "labour",
            "unemployment",
            "payroll",
            "wage",
            "job openings",
            "jolts",
        )
    ):
        category = "labor_release"
        scope = "currency"
    elif any(
        term in text
        for term in (
            "purchasing managers index",
            "purchasing managers' index",
            "manufacturing pmi",
            "services pmi",
            "composite pmi",
            "flash pmi",
            "consumer sentiment",
            "consumer confidence",
            "consumer expectations",
        )
    ):
        category = "business_activity_release"
        scope = "currency"
    elif any(
        term in text
        for term in (
            "manufacturers' shipments",
            "manufacturers’ shipments",
            "factory orders",
            "durable goods",
            "machine orders",
            "machinery orders",
        )
    ):
        category = "manufacturing_release"
        scope = "currency"
    elif any(
        term in text
        for term in (
            "international trade in goods and services",
            "trade balance",
            "international trade balance",
        )
    ):
        category = "trade_balance_release"
        scope = "currency"
    elif oil_up or oil_down or "commodity" in text:
        category = "commodity_shock"
        scope = "all_pairs"
    elif primary_spending_release_direction or activity_release_direction or any(
        term in text for term in ("gdp", "growth", "recession", "contraction")
    ):
        category = "growth_release"
        scope = "currency"
    elif any(
        term in text
        for term in (
            "tariff",
            "trade war",
            "import tax",
            "customs duty",
            "customs duties",
        )
    ):
        category = "trade_policy"
        scope = "all_pairs"
    else:
        category = "market_news"
        scope = "currency"

    directional_corroboration_required = bool(
        category in CORROBORATION_REQUIRED_CATEGORIES
        or (
            not official
            and (
                monetary_impulse
                or secondary_inflation_release_claim
                or secondary_labor_release_claim
                or secondary_business_activity_release_claim
                or activity_release_direction
                or primary_spending_release_direction
                # The final category can be ``inflation_context`` when a
                # market recap mentions both oil and inflation.  The oil
                # transmission score is still a secondary commodity claim
                # and must not bypass the commodity corroboration boundary.
                or oil_up
                or oil_down
            )
        )
    )

    source_quality = clamp(safe_float(raw.get("source_quality"), 0.65), 0.0, 1.0)
    maximum_direction = max((abs(value) for value in scores.values()), default=0.0)
    confidence = clamp(
        0.2 * source_quality
        + 0.55 * maximum_direction
        + 0.05 * min(4, market_relevance),
        0.0,
        1.0,
    )
    severity = clamp(
        0.30
        + 0.25 * source_quality
        + 0.25 * max(maximum_direction, risk_off, risk_on)
        + 0.05 * min(4, market_relevance),
        0.0,
        1.0,
    )
    post_window = (
        720
        if category in {"risk_off_geopolitical_or_financial", "trade_policy"}
        else 360
        if category in {"monetary_policy", "fx_intervention", "commodity_shock"}
        else 180
    )
    directional_bias = {
        currency: "BULLISH" if score > 0 else "BEARISH"
        for currency, score in scores.items()
    }
    stability_research_scores = verbal_currency_stability_research_scores(
        direction_text,
        mentioned_currencies,
    )
    research_currency_scores = (
        non_catalyst_research_scores
        if non_catalyst_research_scores and not scores
        else secondary_market_policy_expectation_scores
        if secondary_market_policy_expectation_scores and not scores
        else secondary_inflation_expectations_scores
        if secondary_inflation_expectations_scores and not scores
        else official_duration_liquidity_research_scores
        if official_duration_liquidity_research_scores and not scores
        else japan_external_policy_pressure_research_scores
        if japan_external_policy_pressure_research_scores and not scores
        else official_fiscal_narrative_research_scores
        if official_fiscal_narrative_research_scores and not scores
        else semantic_conflict_scores
        if semantic_conflict_scores and not scores
        else semantic_direction_scores
        if semantic_direction_scores and not scores
        else
        {
            currency: round(0.25 * primary_spending_release_direction, 6)
            for currency in source_currencies
        }
        if primary_spending_release_direction and not scores
        else stability_research_scores
        if stability_research_scores and not scores
        else {}
    )
    published_raw = clean_text(raw.get("published_utc"))
    published = parse_datetime(published_raw)
    published_time_inferred = bool(raw.get("published_time_inferred")) or not bool(
        published_raw
    )
    event_time = published or first_seen
    detail_available = parse_datetime(raw.get("detail_available_utc"))
    numeric_available = parse_datetime(raw.get("numeric_causal_known_utc"))
    publication_clock_known = parse_datetime(
        raw.get("publication_clock_known_utc")
    )
    numeric_contract = clean_text(raw.get("numeric_extraction_contract_id"))
    numeric_activation = parse_datetime(raw.get("numeric_parser_activated_utc"))
    if (
        bool(raw.get("structured_event"))
        and bool(raw.get("source_verified"))
        and bool(raw.get("source_direct", raw.get("source_verified", False)))
        and optional_float(raw.get("actual_value")) is not None
        and numeric_contract
        and numeric_activation is not None
    ):
        # Current-release adapters become causal only at the latest of source
        # publication, local arrival, and parser activation. Historical page
        # snapshots therefore cannot be backdated by enabling a parser later.
        numeric_available = max(
            candidate
            for candidate in (
                numeric_available,
                numeric_activation,
                event_time,
                first_seen,
            )
            if candidate is not None
        )
    causal_known = max(
        candidate
        for candidate in (
            first_seen,
            event_time,
            detail_available,
            numeric_available,
            publication_clock_known,
        )
        if candidate is not None
    )
    inferred_primary_release_guard = bool(
        str(raw.get("source_role") or "").lower()
        == "primary_statistical_release"
        and published_time_inferred
    )
    if inferred_primary_release_guard:
        # HTML "latest releases" listings can expose a link just before its
        # scheduled publication minute without a machine-readable timestamp.
        # A one-minute research hold is conservative and prevents that listing
        # edge from becoming historical look-ahead.
        causal_known = max(
            causal_known,
            first_seen + dt.timedelta(seconds=60),
        )
    publication_hold_seconds = max(
        0.0,
        (causal_known - first_seen).total_seconds(),
    )
    source_listing_bootstrap = bool(raw.get("source_listing_bootstrap"))
    publication_clock_diagnostic_only = bool(
        raw.get("publication_clock_diagnostic_only")
    )
    reaction_horizon_minutes = estimated_news_reaction_horizon_minutes(category)
    availability_lag_minutes = (
        max(0.0, (first_seen - published).total_seconds() / 60.0)
        if published is not None
        else 0.0
    )
    forward_timeliness_limit_minutes = min(
        30.0,
        max(5.0, reaction_horizon_minutes * 0.25),
    )
    forward_signal_timely = bool(
        not reports_prior_market_move
        and not non_catalyst_context
        and not source_listing_bootstrap
        and not inferred_primary_release_guard
        and not publication_clock_diagnostic_only
        and not (has_structured_numeric_release and published_time_inferred)
        and availability_lag_minutes <= forward_timeliness_limit_minutes
    )
    topic_metadata = build_topic_metadata(
        text=text,
        entity_text=(
            clean_headline.lower()
            if (
                issuer_bound_policy_context
                or official_non_market_research
                or official_non_market_administrative
            )
            else text
        ),
        category=category,
        direct_currencies=mentioned_currencies,
        currency_scores=scores,
        monetary_impulse=monetary_impulse,
        risk_off=risk_off,
        risk_on=risk_on,
        oil_up=oil_up,
        oil_down=oil_down,
        intervention_status=intervention_status if intervention_signal else "",
    )
    if non_stance_liquidity_implementation:
        topic_metadata = {
            **topic_metadata,
            "topic_signature": (
                "monetary_policy|"
                f"{'-'.join(sorted(set(directional_currencies))) or 'global'}|"
                "general|neutral_non_stance_liquidity"
            ),
            "topic_action": "neutral_non_stance_liquidity",
            "topic_tags": sorted(
                {
                    "#monetary_policy",
                    "#non_stance_liquidity_implementation",
                    *(
                        f"#{currency.lower()}_neutral_non_stance_liquidity"
                        for currency in sorted(set(directional_currencies))
                    ),
                }
            ),
        }
    source_native_components = (
        raw.get("source_native_components")
        if isinstance(raw.get("source_native_components"), Mapping)
        else {}
    )
    structured_component_change = decompose_source_native_component_changes(
        source_native_components
    )
    actual_value = optional_float(raw.get("actual_value"))
    if actual_value is None:
        actual_value = optional_float(raw.get("actual"))
    consensus_value = optional_float(raw.get("consensus_value"))
    if consensus_value is None:
        consensus_value = optional_float(raw.get("consensus"))
    previous_value = optional_float(raw.get("previous_value"))
    if previous_value is None:
        previous_value = optional_float(raw.get("previous"))
    revised_previous_value = optional_float(raw.get("revised_previous_value"))
    if revised_previous_value is None:
        revised_previous_value = optional_float(raw.get("revised_previous"))
    surprise_raw = (
        round(actual_value - consensus_value, 12)
        if actual_value is not None and consensus_value is not None
        else None
    )
    # Trading Economics defines Previous as the post-revision value and Revised
    # as the formerly reported value. Keep the difference explicit and neutral.
    revision_raw = (
        round(previous_value - revised_previous_value, 12)
        if previous_value is not None and revised_previous_value is not None
        else None
    )
    transmission_mechanisms: list[str] = []
    if category in {
        "monetary_policy",
        "inflation_release",
        "labor_release",
        "growth_release",
        "manufacturing_release",
        "business_activity_release",
        "trade_balance_release",
    }:
        transmission_mechanisms.append("rate_expectations")
    if risk_off or risk_on:
        transmission_mechanisms.append("risk_regime")
    if category == "commodity_shock":
        transmission_mechanisms.append("terms_of_trade")
    if category == "fx_intervention":
        transmission_mechanisms.append("official_fx_flow")
    if category == "trade_policy":
        transmission_mechanisms.extend(["trade_balance", "risk_regime"])
    if category == "sovereign_duration_liquidity_policy":
        transmission_mechanisms.extend(
            ["sovereign_duration_supply", "yield_curve_repricing"]
        )
    transmission_mechanisms = list(dict.fromkeys(transmission_mechanisms))
    identity_url = canonical_url(raw.get("url"))
    source_id = str(raw.get("source_id") or "")
    google_news_item = source_id.startswith("google_news_")
    structured_event = bool(raw.get("structured_event"))
    if structured_event and raw.get("external_id"):
        identity = f"{source_id}:{raw.get('external_id')}"
    elif google_news_item and raw.get("external_id"):
        # The same Google News article can be returned by several configured
        # search queries.  The query id is discovery provenance, not an
        # independent publisher or event identity.
        identity = clean_text(raw.get("external_id"))
    elif (
        str(raw.get("source_kind") or "").lower() == "gdelt"
        or not bool(raw.get("source_verified"))
    ):
        identity = normalized_headline(title)
    else:
        identity = identity_url or raw.get("external_id") or normalized_headline(title)
    event_lineage_id = (
        stable_id("structured_lineage", identity)
        if structured_event
        else ""
    )
    material_content_sha256 = clean_text(raw.get("material_content_sha256"))
    structured_version = (
        clean_text(raw.get("source_reported_update_utc"))
        or "|".join(
            clean_text(raw.get(key))
            for key in (
                "scheduled_utc",
                "actual",
                "consensus",
                "previous",
                "revised_previous",
            )
        )
    )
    if material_content_sha256:
        structured_version = (
            f"{structured_version}|content:{material_content_sha256}"
            if structured_version
            else f"content:{material_content_sha256}"
        )
    identity_scope = "google_news" if google_news_item else source_id
    event_id = stable_id(
        identity_scope,
        identity,
        str(raw.get("source_kind") or ""),
        structured_version if structured_event else iso_utc(event_time)[:13],
    )
    return {
        "event_id": event_id,
        "classification_version": CLASSIFICATION_VERSION,
        "detail_enriched": bool(raw.get("detail_enriched")),
        "detail_quality_state": (
            "rejected" if detail_quality_issue else
            "valid" if bool(raw.get("detail_enriched")) else "not_enriched"
        ),
        "detail_quality_reason": detail_quality_issue,
        "rejected_detail_content_sha256": rejected_detail_sha256,
        "detail_enrichment_kind": clean_text(raw.get("detail_enrichment_kind")),
        "detail_enrichment_research_only": bool(
            raw.get("detail_enrichment_research_only")
            or raw.get("detail_attachment_research_only")
        ),
        # Keep the archive-only provenance explicit after classification.
        # Downstream audits can then distinguish a useful prior-document
        # baseline from text that was causally available at publication time.
        "detail_context_archive_only": bool(
            raw.get("detail_context_archive_only")
            or (
                source_listing_bootstrap
                and raw.get("detail_enrichment_research_only")
                and detail_available is not None
            )
        ),
        "detail_content_sha256": clean_text(raw.get("detail_content_sha256")),
        "detail_content_bytes": int(safe_float(raw.get("detail_content_bytes"), 0)),
        "detail_text_characters": int(
            safe_float(raw.get("detail_text_characters"), 0)
        ),
        "detail_source_url": canonical_url(raw.get("detail_source_url")),
        "detail_listing_url": canonical_url(raw.get("detail_listing_url")),
        "detail_publisher_resolution_contract_id": clean_text(
            raw.get("detail_publisher_resolution_contract_id")
        ),
        "detail_publisher_resolution_known_utc": clean_text(
            raw.get("detail_publisher_resolution_known_utc")
        ),
        "detail_publisher_resolution_research_only": bool(
            raw.get("detail_publisher_resolution_research_only")
        ),
        "detail_archive_path": clean_text(raw.get("detail_archive_path")),
        "detail_attachment_discovery_state": clean_text(
            raw.get("detail_attachment_discovery_state")
        ),
        "detail_attachment_enriched": bool(
            raw.get("detail_attachment_enriched")
        ),
        "detail_attachment_count": int(
            safe_float(raw.get("detail_attachment_count"), 0)
        ),
        "detail_attachment_urls": [
            canonical_url(value)
            for value in raw.get("detail_attachment_urls") or []
            if canonical_url(value)
        ],
        "detail_attachments": [
            dict(value)
            for value in raw.get("detail_attachments") or []
            if isinstance(value, Mapping)
        ],
        "detail_attachment_content_sha256": clean_text(
            raw.get("detail_attachment_content_sha256")
        ),
        "detail_attachment_content_bytes": int(
            safe_float(raw.get("detail_attachment_content_bytes"), 0)
        ),
        "detail_attachment_text_characters": int(
            safe_float(raw.get("detail_attachment_text_characters"), 0)
        ),
        "detail_attachment_available_utc": (
            iso_utc(attachment_available)
            if (
                attachment_available := parse_datetime(
                    raw.get("detail_attachment_available_utc")
                )
            ) is not None
            else ""
        ),
        "detail_attachment_parser_contract_id": clean_text(
            raw.get("detail_attachment_parser_contract_id")
        ),
        "detail_attachment_research_only": bool(
            raw.get("detail_attachment_research_only")
        ),
        "detail_available_utc": (
            iso_utc(detail_available) if detail_available is not None else ""
        ),
        "source_id": source_id,
        "source_name": str(raw.get("source_name") or ""),
        "source_kind": str(raw.get("source_kind") or ""),
        "source_quality": round(source_quality, 6),
        "source_verified": bool(raw.get("source_verified")),
        "source_direct": bool(
            raw.get("source_direct", raw.get("source_verified", False))
        ),
        "retrieval_via": str(raw.get("retrieval_via") or "direct"),
        "source_role": current_source_role,
        "source_contract_id": clean_text(raw.get("source_contract_id")),
        "source_cohort_id": clean_text(raw.get("source_cohort_id")),
        "collector_contract_id": clean_text(raw.get("collector_contract_id")),
        "collector_cohort_id": clean_text(raw.get("collector_cohort_id")),
        "observation_time_contract_id": clean_text(
            raw.get("observation_time_contract_id")
        ),
        "observation_clock_trusted": (
            raw.get("observation_clock_trusted") is True
        ),
        "observation_clock_source": clean_text(
            raw.get("observation_clock_source")
        ),
        "publisher_url": canonical_url(raw.get("publisher_url")),
        "external_id": clean_text(raw.get("external_id")),
        "event_lineage_id": event_lineage_id or event_id,
        "material_update_id": event_id,
        "material_content_sha256": material_content_sha256,
        "release_time_rule_observed_sha256": clean_text(
            raw.get("release_time_rule_observed_sha256")
        ),
        "release_time_rule_archive_name": clean_text(
            raw.get("release_time_rule_archive_name")
        ),
        "release_rule_bytes_verified": raw.get("release_rule_bytes_verified")
        is True,
        "content_versioned_at_collection": bool(
            raw.get("content_versioned_at_collection")
        ),
        "mutable_content_source": bool(raw.get("mutable_content_source")),
        "publisher_container_timestamp_utc": clean_text(
            raw.get("publisher_container_timestamp_utc")
        ),
        "causal_integrity_state": (
            "content_versioned_at_collection"
            if bool(raw.get("content_versioned_at_collection"))
            else "legacy_mutable_content_unversioned"
            if bool(raw.get("mutable_content_source"))
            else "standard"
        ),
        "historical_replay_eligible": not bool(
            raw.get("mutable_content_source")
            and not raw.get("content_versioned_at_collection")
        ),
        "published_utc": iso_utc(event_time),
        "published_time_inferred": published_time_inferred,
        "source_native_published_utc": clean_text(
            raw.get("source_native_published_utc")
        ),
        "publication_clock_basis": clean_text(
            raw.get("publication_clock_basis")
        ),
        "publication_clock_contract_id": clean_text(
            raw.get("publication_clock_contract_id")
        ),
        "publication_clock_known_utc": (
            iso_utc(publication_clock_known)
            if publication_clock_known is not None else ""
        ),
        "publication_clock_diagnostic_only": (
            publication_clock_diagnostic_only
        ),
        "immutable_source_version_boundary": bool(
            raw.get("immutable_source_version_boundary")
        ),
        "scheduled_utc": iso_utc(parse_datetime(raw.get("scheduled_utc")))
        if parse_datetime(raw.get("scheduled_utc"))
        else "",
        "source_reported_update_utc": iso_utc(
            parse_datetime(raw.get("source_reported_update_utc"))
        )
        if parse_datetime(raw.get("source_reported_update_utc"))
        else "",
        # Retain transport arrival separately, but never expose an item before
        # a future source-reported publication time.
        "causal_known_utc": iso_utc(causal_known),
        "numeric_causal_known_utc": (
            iso_utc(numeric_available) if numeric_available is not None else ""
        ),
        "numeric_extraction_contract_id": numeric_contract,
        "numeric_direction_policy": clean_text(raw.get("numeric_direction_policy")),
        "numeric_verification_state": clean_text(
            raw.get("numeric_verification_state")
        ),
        "official_search_policy_rate_headline_detected": bool(
            raw.get("official_search_policy_rate_headline_detected")
        ),
        "official_search_policy_rate_action": clean_text(
            raw.get("official_search_policy_rate_action")
        ),
        "official_search_policy_rate_change_bp": optional_float(
            raw.get("official_search_policy_rate_change_bp")
        ),
        "official_search_policy_rate_level": optional_float(
            raw.get("official_search_policy_rate_level")
        ),
        "official_search_policy_rate_previous_level": optional_float(
            raw.get("official_search_policy_rate_previous_level")
        ),
        "official_search_policy_rate_contract_id": clean_text(
            raw.get("official_search_policy_rate_contract_id")
        ),
        "official_search_policy_rate_cohort_id": clean_text(
            raw.get("official_search_policy_rate_cohort_id")
        ),
        "official_search_policy_rate_activated_utc": clean_text(
            raw.get("official_search_policy_rate_activated_utc")
        ),
        "official_search_policy_rate_activation_eligible": bool(
            raw.get("official_search_policy_rate_activation_eligible")
        ),
        "release_stage": clean_text(raw.get("release_stage")),
        "consensus_capture_state": clean_text(raw.get("consensus_capture_state")),
        "activity_release_direction": activity_release_direction,
        "secondary_mixed_macro_direction_conflict": (
            secondary_mixed_macro_direction_conflict
        ),
        "first_seen_utc": iso_utc(first_seen),
        "last_seen_utc": iso_utc(first_seen),
        "publication_hold_seconds": round(publication_hold_seconds, 6),
        "headline": title,
        "summary": summary,
        "source_url": identity_url,
        "domain": str(raw.get("domain") or urllib.parse.urlsplit(identity_url).netloc),
        "relevant": relevant,
        "context_only": context_only,
        "localized_parallel_currency_market": localized_parallel_currency_market,
        "exclusion_reason": (
            "localized_parallel_currency_market"
            if localized_parallel_currency_market
            else "non_fx_equity_earnings_preview"
            if non_fx_equity_earnings_preview
            else "non_fx_corporate_commodity_results"
            if non_fx_corporate_commodity_results
            else "non_fx_false_term_context"
            if non_fx_false_term_context
            else "non_fx_banknote_design"
            if non_fx_banknote_design
            else "non_fx_ceremonial_remarks"
            if non_fx_ceremonial_remarks
            else "official_non_market_program"
            if official_non_market_program
            else "official_non_market_research"
            if official_non_market_research
            else "official_non_market_administrative"
            if official_non_market_administrative
            else "non_fx_cash_handling_supply"
            if non_fx_cash_handling_supply
            else ""
        ),
        "context_reason": (
            "official_liquidity_implementation_explicitly_non_stance"
            if non_stance_liquidity_implementation and context_only
            else "already_priced_or_non_catalyst_context"
            if non_catalyst_context and context_only
            else "secondary_market_policy_expectation_context"
            if secondary_market_policy_expectation and context_only
            else "secondary_inflation_expectations_require_rate_repricing"
            if secondary_inflation_expectations_context and context_only
            else "official_duration_liquidity_policy_research_only"
            if official_duration_liquidity_research_scores and context_only
            else "japan_external_policy_pressure_research_only"
            if japan_external_policy_pressure_research_scores and context_only
            else "deescalation_proposal_without_agreement"
            if deescalation_proposal_only and context_only
            else "commodity_operational_metric_requires_price_repricing"
            if commodity_operational_metric_context and context_only
            else "secondary_analysis_not_fresh_catalyst"
            if secondary_analysis_context and context_only
            else "secondary_conflict_duration_recap_not_fresh_catalyst"
            if secondary_conflict_duration_recap and context_only
            else "official_fiscal_narrative_research_only"
            if official_fiscal_narrative_research_scores and context_only
            else "multi_claim_semantic_conflict"
            if semantic_claim_conflict and context_only
            else "official_neutral_policy_evidence"
            if official_policy_release and not scores
            else "reported_market_move_context"
            if reports_prior_market_move and context_only
            else "verbal_currency_stability_research_only"
            if stability_research_scores and context_only
            else "primary_release_absolute_direction_research_only"
            if research_currency_scores and context_only and official
            else "secondary_uncorroborated_semantic_direction"
            if research_currency_scores and context_only and not official
            else "official_structured_numeric_release_observation"
            if has_structured_numeric_release and context_only
            else "secondary_multi_topic_market_roundup_context"
            if secondary_market_roundup and context_only
            else "secondary_household_cost_context"
            if secondary_household_cost_context and context_only
            else "no_deterministic_fx_impulse"
            if context_only
            else ""
        ),
        "directional_evidence": bool(scores),
        "semantic_claims": semantic_claims,
        "semantic_claim_conflict": semantic_claim_conflict,
        "opposing_policy_claim_conflict": opposing_policy_claim_conflict,
        "secondary_conflict_duration_recap": secondary_conflict_duration_recap,
        "conflict_duration_recap_guard_contract_id": (
            CONFLICT_DURATION_RECAP_GUARD_CONTRACT_ID_V1
            if secondary_conflict_duration_recap
            else ""
        ),
        "conflict_duration_recap_guard_cohort_id": (
            CONFLICT_DURATION_RECAP_GUARD_COHORT_ID_V1
            if secondary_conflict_duration_recap
            else ""
        ),
        "conflict_duration_recap_guard_activated_utc": (
            CONFLICT_DURATION_RECAP_GUARD_ACTIVATED_UTC_V1
            if secondary_conflict_duration_recap
            else ""
        ),
        "conflict_duration_recap_guard_activation_eligible": bool(
            conflict_duration_recap_guard_activation_eligible
        ),
        "semantic_claim_contract": "economic_claim_decomposition_v1",
        "research_currency_scores": research_currency_scores,
        "research_directional_basis": (
            "multi_claim_components_no_flattened_direction"
            if semantic_claim_conflict
            else "secondary_policy_expectation_requires_official_or_rate_repricing_confirmation"
            if secondary_market_policy_expectation_scores
            else "secondary_inflation_expectations_require_rate_repricing_confirmation"
            if secondary_inflation_expectations_scores
            else "official_duration_liquidity_policy_requires_rate_and_price_confirmation"
            if official_duration_liquidity_research_scores
            else "japan_external_policy_pressure_requires_rate_and_price_confirmation"
            if japan_external_policy_pressure_research_scores
            else "verbal_currency_stability_support"
            if stability_research_scores
            else "absolute_primary_spending_release_without_consensus"
            if research_currency_scores
            else ""
        ),
        "directional_publish_eligible": bool(
            scores
            and forward_signal_timely
            and not issuer_bound_policy_communication_observation
            and not bool(raw.get("detail_enrichment_research_only"))
            and not bool(raw.get("detail_attachment_research_only"))
            and not bool(raw.get("directional_research_only"))
            and (
                bool(raw.get("source_verified"))
                or not directional_corroboration_required
            )
        ),
        "directional_corroboration_required": directional_corroboration_required,
        "directional_source_grade": (
            "verified_primary_or_publisher"
            if bool(raw.get("source_verified"))
            else "secondary_requires_corroboration"
            if category in CORROBORATION_REQUIRED_CATEGORIES
            else "contextual_directional"
        ),
        "directional_research_only": bool(
            raw.get("directional_research_only")
            or issuer_bound_policy_communication_observation
            or secondary_market_policy_expectation
            or secondary_inflation_expectations_context
        ),
        "official_policy_release": official_policy_release,
        "japan_external_policy_pressure_research": bool(
            japan_external_policy_pressure
        ),
        "japan_external_policy_pressure_contract_id": (
            JAPAN_EXTERNAL_POLICY_PRESSURE_CONTRACT_ID_V1
            if japan_external_policy_pressure
            else ""
        ),
        "japan_external_policy_pressure_cohort_id": (
            JAPAN_EXTERNAL_POLICY_PRESSURE_COHORT_ID_V1
            if japan_external_policy_pressure
            else ""
        ),
        "japan_external_policy_pressure_activated_utc": (
            JAPAN_EXTERNAL_POLICY_PRESSURE_ACTIVATED_UTC_V1
            if japan_external_policy_pressure
            else ""
        ),
        "japan_external_policy_pressure_activation_eligible": bool(
            japan_external_policy_pressure_activation_eligible
        ),
        "issuer_bound_policy_attachment": issuer_bound_policy_attachment,
        "issuer_bound_policy_attachment_contract_id": (
            "boj_official_policy_speech_pdf_currency_binding_v1_20260827"
            if issuer_bound_policy_attachment
            else ""
        ),
        "issuer_bound_policy_communication": (
            issuer_bound_policy_communication_observation
        ),
        "issuer_bound_policy_communication_contract_id": (
            issuer_bound_policy_communication_contract_id
        ),
        "issuer_bound_policy_communication_cohort_id": (
            issuer_bound_policy_communication_cohort_id
        ),
        "issuer_bound_policy_communication_activated_utc": (
            issuer_bound_policy_communication_activated_utc
        ),
        "issuer_bound_policy_communication_activation_eligible": bool(
            issuer_bound_policy_communication_contract_activation_eligible
        ),
        "issuer_bound_policy_communication_binding_method": (
            issuer_bound_policy_communication_binding_method
        ),
        "issuer_bound_policy_communication_source_identity": bool(
            source_bound_policy_communication
        ),
        "policy_document_type": document_type,
        "policy_stance_bearing_eligible": policy_stance_bearing_eligible,
        "official_non_market_research": official_non_market_research,
        "official_non_market_administrative": (
            official_non_market_administrative
        ),
        "official_defense_nonmarket_health_guard": (
            official_defense_nonmarket_health_guard
        ),
        "official_defense_nonmarket_health_guard_contract_id": (
            OFFICIAL_DEFENSE_NONMARKET_HEALTH_GUARD_CONTRACT_ID_V1
            if official_defense_nonmarket_health_guard
            else ""
        ),
        "official_defense_nonmarket_health_guard_cohort_id": (
            OFFICIAL_DEFENSE_NONMARKET_HEALTH_GUARD_COHORT_ID_V1
            if official_defense_nonmarket_health_guard
            else ""
        ),
        "official_defense_nonmarket_health_guard_activated_utc": (
            OFFICIAL_DEFENSE_NONMARKET_HEALTH_GUARD_ACTIVATED_UTC_V1
            if official_defense_nonmarket_health_guard
            else ""
        ),
        "official_defense_nonmarket_health_guard_activation_eligible": bool(
            official_defense_nonmarket_health_guard
            and first_seen
            >= dt.datetime.fromisoformat(
                OFFICIAL_DEFENSE_NONMARKET_HEALTH_GUARD_ACTIVATED_UTC_V1
            )
        ),
        "non_stance_liquidity_implementation": (
            non_stance_liquidity_implementation
        ),
        "event_temporality": (
            "retrospective_market_report"
            if reports_prior_market_move
            else "scheduled_or_current_release"
            if (
                official_policy_release
                or issuer_bound_policy_communication_observation
                or structured_event
            )
            else "current_report"
        ),
        "intervention_status": intervention_status if intervention_signal else "",
        "intervention_assertion_weight": (
            intervention_weight if intervention_signal else 0.0
        ),
        "reports_prior_market_move": reports_prior_market_move,
        "non_catalyst_context": non_catalyst_context,
        "availability_lag_minutes": round(availability_lag_minutes, 6),
        "forward_timeliness_limit_minutes": round(
            forward_timeliness_limit_minutes,
            6,
        ),
        "forward_signal_timely": forward_signal_timely,
        "source_listing_bootstrap": source_listing_bootstrap,
        "market_relevance_count": market_relevance,
        "structured_event": structured_event,
        "event_series_id": clean_text(raw.get("event_series_id")),
        "event_name": clean_text(raw.get("event_name")),
        "release_components": (
            list(raw.get("release_components") or ())
            if isinstance(raw.get("release_components"), Sequence)
            and not isinstance(raw.get("release_components"), (str, bytes))
            else []
        ),
        "event_country": clean_text(raw.get("event_country")),
        "reference_period": clean_text(raw.get("reference_period")),
        "reference_date": clean_text(raw.get("reference_date")),
        "timing_precision": clean_text(raw.get("timing_precision")),
        "event_time_basis": clean_text(raw.get("event_time_basis")),
        "clock_semantics": clean_text(raw.get("clock_semantics")),
        "independent_domestic_event": bool(
            raw.get("independent_domestic_event")
        ),
        "linked_policy_factor": bool(raw.get("linked_policy_factor")),
        # Exact-clock calendars are evidence/planning inputs only.  Preserve
        # the source assertion explicitly so no downstream classifier can
        # infer routeability from an exact timestamp.
        "execution_eligible": bool(raw.get("execution_eligible", False)),
        "schedule_window_end_utc": clean_text(
            raw.get("schedule_window_end_utc")
        ),
        "importance": raw.get("importance"),
        "unit": clean_text(raw.get("unit")),
        "actual": raw.get("actual"),
        "actual_value": actual_value,
        "consensus": raw.get("consensus"),
        "consensus_value": consensus_value,
        "previous": raw.get("previous"),
        "previous_value": previous_value,
        "revised_previous": raw.get("revised_previous"),
        "revised_previous_value": revised_previous_value,
        "surprise_raw": surprise_raw,
        "surprise_sign": (
            "POSITIVE" if surprise_raw is not None and surprise_raw > 0
            else "NEGATIVE" if surprise_raw is not None and surprise_raw < 0
            else "ZERO" if surprise_raw == 0
            else "UNKNOWN"
        ),
        "revision_raw": revision_raw,
        "directional_surprise_interpretation": (
            "pending_series_semantics" if surprise_raw is not None else "unavailable"
        ),
        "vendor_sentiment_score": optional_float(raw.get("vendor_sentiment_score")),
        "vendor_sentiment_label": clean_text(raw.get("vendor_sentiment_label")),
        "vendor_topics": raw.get("vendor_topics")
        if isinstance(raw.get("vendor_topics"), list)
        else [],
        "vendor_ticker_sentiment": raw.get("vendor_ticker_sentiment")
        if isinstance(raw.get("vendor_ticker_sentiment"), list)
        else [],
        "vendor_currency_sentiment": raw.get("vendor_currency_sentiment")
        if isinstance(raw.get("vendor_currency_sentiment"), Mapping)
        else {},
        "vendor_currencies": sorted(
            {
                str(value).upper()
                for value in raw.get("vendor_currencies") or []
                if str(value).upper() in ALL_CURRENCIES
            }
        ),
        "vendor_sentiment_research_only": bool(
            raw.get("vendor_sentiment_research_only")
        ),
        "source_native_update_date": clean_text(
            raw.get("source_native_update_date")
        ),
        "source_native_components": source_native_components,
        "structured_component_change": structured_component_change,
        "category": category,
        "scope": scope,
        "currencies": currencies,
        "source_native_currency_bound": bool(
            (
                has_structured_numeric_release
                or issuer_bound_policy_context
                or official_non_market_research
                or official_non_market_administrative
            )
            and source_currencies
        ),
        "source_currencies": sorted(set(source_currencies)),
        "policy_subject_currencies": sorted(set(policy_subject_currencies)),
        "release_subject_currencies": sorted(set(release_subject_currencies)),
        "mentioned_currency_entities": sorted(
            set(mentioned_currency_entities)
        ),
        "direct_currencies": sorted(set(mentioned_currencies)),
        "inferred_currencies": sorted(set(scores) - set(mentioned_currencies)),
        "currency_scores": scores,
        "directional_bias": directional_bias,
        "generic_sentiment_score": round(generic_sentiment, 6),
        "monetary_impulse": round(monetary_impulse, 6),
        "policy_assertion_status": assertion_status,
        "policy_assertion_weight": assertion_weight,
        "policy_data_dependent_guidance": policy_data_dependent_guidance,
        "risk_off_score": round(risk_off, 6),
        "risk_on_score": round(risk_on, 6),
        "secondary_analysis_context": secondary_analysis_context,
        "commodity_operational_metric_context": (
            commodity_operational_metric_context
        ),
        "secondary_market_state_guard_contract_id": (
            SECONDARY_MARKET_STATE_GUARD_CONTRACT_ID_V1
            if secondary_analysis_context or commodity_operational_metric_context
            else ""
        ),
        "secondary_market_state_guard_cohort_id": (
            SECONDARY_MARKET_STATE_GUARD_COHORT_ID_V1
            if secondary_analysis_context or commodity_operational_metric_context
            else ""
        ),
        "secondary_market_state_guard_activated_utc": (
            SECONDARY_MARKET_STATE_GUARD_ACTIVATED_UTC_V1
            if secondary_analysis_context or commodity_operational_metric_context
            else ""
        ),
        "secondary_market_state_guard_activation_eligible": bool(
            secondary_market_state_guard_activation_eligible
        ),
        "deescalation_proposal_only": deescalation_proposal_only,
        "deescalation_actualized": deescalation_actualized,
        "deescalation_proposal_guard_contract_id": (
            DEESCALATION_PROPOSAL_GUARD_CONTRACT_ID_V1
            if deescalation_proposal_only
            else ""
        ),
        "deescalation_proposal_guard_cohort_id": (
            DEESCALATION_PROPOSAL_GUARD_COHORT_ID_V1
            if deescalation_proposal_only
            else ""
        ),
        "deescalation_proposal_guard_activated_utc": (
            DEESCALATION_PROPOSAL_GUARD_ACTIVATED_UTC_V1
            if deescalation_proposal_only
            else ""
        ),
        "deescalation_proposal_guard_activation_eligible": bool(
            deescalation_proposal_guard_activation_eligible
        ),
        "official_sanctions_escalation": official_sanctions_escalation,
        "commodity_exporter_direction_state": (
            "ambiguous_pending_authoritative_repricing_or_price_reaction"
            if energy_supply_geopolitical_event and not (oil_up or oil_down)
            else "explicit_commodity_direction"
            if energy_supply_geopolitical_event
            else "not_applicable"
        ),
        "directional_confidence": round(confidence, 6),
        "directional_uncertainty": round(1.0 - confidence, 6),
        "transmission_mechanisms": transmission_mechanisms,
        "sentiment_dimensions": {
            "generic_tone": round(generic_sentiment, 6),
            "monetary_impulse": round(monetary_impulse, 6),
            "risk_off": round(risk_off, 6),
            "risk_on": round(risk_on, 6),
        },
        "sentiment_schema_version": "fx_news_sentiment_dimensions_v1",
        "severity": round(100.0 * severity, 3),
        "movement_potential": "HIGH" if severity >= 0.7 else "MEDIUM"
        if severity >= 0.45
        else "LOW",
        "estimated_reaction_horizon_minutes": reaction_horizon_minutes,
        "estimated_reaction_horizon_label": news_horizon_label(
            reaction_horizon_minutes
        ),
        "reaction_horizon_method": "category_prior_v1",
        "relevance_window_minutes": post_window,
        "post_window_minutes": post_window,
        **topic_metadata,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
    }


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA busy_timeout=30000")
    try:
        connection.execute("PRAGMA journal_mode=WAL")
    except sqlite3.OperationalError as exc:
        # Reasserting WAL can require an exclusive lock even when an existing
        # live database is already in WAL mode. Verify the current mode and
        # continue without disrupting readers; still fail for a non-WAL DB.
        if "locked" not in str(exc).lower():
            connection.close()
            raise
        current_mode = connection.execute("PRAGMA journal_mode").fetchone()
        if not current_mode or str(current_mode[0]).lower() != "wal":
            connection.close()
            raise
    connection.execute("PRAGMA synchronous=NORMAL")
    # Large rule migrations can write tens of MB. The default 1,000-page
    # auto-checkpoint may block the collector on Windows before artifacts are
    # published, so checkpoint separately with a hard timeout.
    connection.execute("PRAGMA wal_autocheckpoint=65536")
    connection.execute("PRAGMA journal_size_limit=67108864")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS articles (
            event_id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            source_quality REAL NOT NULL,
            source_verified INTEGER NOT NULL,
            published_utc TEXT NOT NULL,
            first_seen_utc TEXT NOT NULL,
            last_seen_utc TEXT NOT NULL,
            headline TEXT NOT NULL,
            summary TEXT NOT NULL,
            source_url TEXT NOT NULL,
            domain TEXT NOT NULL,
            relevant INTEGER NOT NULL,
            category TEXT NOT NULL,
            scope TEXT NOT NULL,
            currencies_json TEXT NOT NULL,
            currency_scores_json TEXT NOT NULL,
            directional_bias_json TEXT NOT NULL,
            generic_sentiment_score REAL NOT NULL,
            monetary_impulse REAL NOT NULL,
            risk_off_score REAL NOT NULL,
            risk_on_score REAL NOT NULL,
            directional_confidence REAL NOT NULL,
            severity REAL NOT NULL,
            movement_potential TEXT NOT NULL,
            post_window_minutes INTEGER NOT NULL,
            duplicate_count INTEGER NOT NULL DEFAULT 0,
            payload_json TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS topic_events (
            topic_id TEXT PRIMARY KEY,
            topic_signature TEXT NOT NULL,
            first_known_utc TEXT NOT NULL,
            last_known_utc TEXT NOT NULL,
            published_utc TEXT NOT NULL,
            category TEXT NOT NULL,
            direct_currencies_json TEXT NOT NULL,
            topic_tags_json TEXT NOT NULL,
            article_count INTEGER NOT NULL,
            distinct_source_count INTEGER NOT NULL,
            payload_json TEXT NOT NULL
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_utc)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_articles_relevant ON articles(relevant, first_seen_utc)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_articles_source_url "
        "ON articles(source_id, source_url, first_seen_utc)"
    )
    # Downstream causal-integrity repairs first resolve the immutable event id,
    # then fall back to the exact publisher/headline identity when an older
    # classifier or mutable-release migration changed that id.  Without this
    # index every pending decision performs a full article-table scan.  That
    # made the read-only signal/news monitor miss its freshness SLA even though
    # collection and signal logic were healthy.
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_articles_source_headline "
        "ON articles(source_name, headline, first_seen_utc)"
    )
    # Project integrity verifies every source-native currency-bound row for the
    # current classifier. Without this narrow partial expression index SQLite
    # scans the full article history even though only a few hundred rows are
    # relevant. The predicate is version-agnostic, so later classifier cohorts
    # reuse the same compact index.
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_articles_native_currency_contract "
        "ON articles(json_extract(payload_json,'$.classification_version')) "
        "WHERE json_extract(payload_json,'$.source_native_currency_bound')=1"
    )
    initialize_source_observation_ledger(connection)
    initialize_classification_observations(connection)
    return connection


def bounded_wal_checkpoint(
    path: Path,
    *,
    minimum_bytes: int = 128_000_000,
    timeout_sec: float = 25.0,
    minimum_interval_sec: float = 300.0,
) -> dict[str, Any]:
    wal_path = path.with_name(path.name + "-wal")
    try:
        before = wal_path.stat().st_size
    except OSError:
        before = 0
    if before < minimum_bytes:
        return {"status": "not_needed", "wal_bytes": before}
    checkpoint_key = str(path.resolve())
    now = time.monotonic()
    since_attempt = now - _WAL_CHECKPOINT_LAST_ATTEMPT.get(
        checkpoint_key,
        -math.inf,
    )
    if since_attempt < max(1.0, minimum_interval_sec):
        return {
            "status": "cooldown",
            "wal_bytes": before,
            "next_attempt_in_sec": round(
                max(0.0, minimum_interval_sec - since_attempt),
                3,
            ),
        }
    _WAL_CHECKPOINT_LAST_ATTEMPT[checkpoint_key] = now
    script = (
        "import json,os,sqlite3,sys;"
        "c=sqlite3.connect(sys.argv[1],timeout=1);"
        "c.execute('PRAGMA busy_timeout=0');"
        "p=c.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone();"
        "sys.stdout.write(json.dumps({'passive':p,'truncate':None}));"
        "sys.stdout.flush();os._exit(0)"
    )
    # A Windows venv executable is a launcher that starts the base interpreter.
    # Killing only that launcher on timeout leaves its checkpoint child alive,
    # so invoke the base interpreter directly when available.
    checkpoint_python = str(getattr(sys, "_base_executable", sys.executable))
    try:
        completed = subprocess.run(
            [checkpoint_python, "-c", script, str(path)],
            capture_output=True,
            text=True,
            timeout=max(1.0, timeout_sec),
            check=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except subprocess.TimeoutExpired:
        return {
            "status": "timeout",
            "wal_bytes": before,
            "timeout_sec": timeout_sec,
            "retry_after_sec": minimum_interval_sec,
        }
    try:
        after = wal_path.stat().st_size
    except OSError:
        after = 0
    checkpoint_result: dict[str, Any] = {}
    try:
        parsed_result = json.loads(completed.stdout)
        if isinstance(parsed_result, dict):
            checkpoint_result = parsed_result
    except (TypeError, ValueError):
        pass
    passive = checkpoint_result.get("passive") or []
    truncate = checkpoint_result.get("truncate")
    checkpoint_busy = bool(
        passive
        and safe_float(passive[0]) != 0
    )
    return {
        "status": (
            "busy"
            if completed.returncode == 0 and checkpoint_busy
            else "ok"
            if completed.returncode == 0
            else "error"
        ),
        "wal_bytes_before": before,
        "wal_bytes_after": after,
        "passive": passive,
        "truncate": truncate,
        "fully_checkpointed": bool(
            passive
            and int(safe_float(passive[0])) == 0
            and int(safe_float(passive[1]))
            == int(safe_float(passive[2]))
        ),
        "error": clean_text(completed.stderr)[:300],
    }


def process_database(path: Path) -> sqlite3.Connection:
    """Reuse the live connection so a Windows close checkpoint cannot stall a cycle."""

    key = str(path.resolve())
    connection = _PROCESS_DATABASE_CONNECTIONS.get(key)
    if connection is None:
        connection = open_database(path)
        _PROCESS_DATABASE_CONNECTIONS[key] = connection
    return connection


def close_process_database(path: Path, connection: sqlite3.Connection) -> None:
    """Close and evict a cached connection as one atomic lifecycle action."""

    key = str(path.resolve())
    if _PROCESS_DATABASE_CONNECTIONS.get(key) is connection:
        _PROCESS_DATABASE_CONNECTIONS.pop(key, None)
    connection.close()


TOPIC_HISTORY_VOLATILE_FIELDS = {
    # Poll freshness belongs to the raw article table. Rewriting the complete
    # semantic topic payload for these counters every minute creates WAL churn
    # without adding a new causal fact.
    "last_seen_utc",
    "duplicate_observation_count",
}


def topic_history_payload(topic: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in dict(topic).items()
        if key not in TOPIC_HISTORY_VOLATILE_FIELDS
    }


def verified_source_provenance_rank(
    row: Mapping[str, Any],
) -> tuple[int, int, int, int]:
    """Prefer a fully bound causal-source observation over legacy variants.

    Several versions of one official release clock can legitimately share a
    semantic topic and scheduled timestamp.  The context cluster must not let
    insertion order choose an older engineering observation when a later
    version binds the governing rule bytes, archive and immutable cohort.
    Downstream consumers still independently validate the exact contract and
    hashes; this rank only prevents display/topic deduplication from discarding
    the stronger provenance before those checks can run.
    """

    observed_rule_sha = clean_text(
        row.get("release_time_rule_observed_sha256")
    ).lower()
    archive_name = clean_text(row.get("release_time_rule_archive_name"))
    contract_id = clean_text(row.get("source_contract_id"))
    cohort_id = clean_text(row.get("source_cohort_id"))
    verified_rule = bool(
        row.get("release_rule_bytes_verified") is True
        and re.fullmatch(r"[0-9a-f]{64}", observed_rule_sha)
        and archive_name
    )
    return (
        int(prospective_collector_provenance(row)),
        int(verified_rule),
        int(bool(contract_id and cohort_id == contract_id)),
        int(bool(clean_text(row.get("material_content_sha256")))),
    )


def topic_history_rank(
    topic: Mapping[str, Any],
) -> tuple[int, int, float, int, tuple[int, int, int, int], str]:
    """Choose one stable, best-corroborated representative per semantic topic."""

    return (
        int(topic.get("topic_article_count") or 1),
        int(topic.get("distinct_source_count") or 1),
        safe_float(topic.get("source_quality"), 0.0),
        int(bool(topic.get("source_verified"))),
        verified_source_provenance_rank(topic),
        str(topic.get("event_id") or ""),
    )


def upsert_topic_events(
    connection: sqlite3.Connection,
    topics: Sequence[Mapping[str, Any]],
) -> tuple[int, int]:
    caller_owned_transaction = connection.in_transaction
    inserted = 0
    updated = 0
    # cluster_articles keeps individual article rows annotated with a shared
    # topic_id. Without this collapse, one collection cycle repeatedly toggles
    # the single topic_events row through every article variant, then does the
    # same work again on the next poll.
    representative_by_id: dict[str, Mapping[str, Any]] = {}
    for topic in topics:
        topic_id = str(topic.get("topic_id") or "")
        prior = representative_by_id.get(topic_id)
        if topic_id and (
            prior is None or topic_history_rank(topic) > topic_history_rank(prior)
        ):
            representative_by_id[topic_id] = topic
    for topic_id, topic in representative_by_id.items():
        exists = connection.execute(
            """
            SELECT first_known_utc, last_known_utc, article_count,
                   distinct_source_count, payload_json
            FROM topic_events WHERE topic_id = ?
            """,
            (topic_id,),
        ).fetchone()
        first_known = str(
            topic.get("causal_known_utc") or topic.get("first_seen_utc") or ""
        )
        last_known = str(topic.get("last_seen_utc") or first_known)
        article_count = int(topic.get("topic_article_count") or 1)
        source_count = int(topic.get("distinct_source_count") or 1)
        payload = topic_history_payload(topic)
        if prospective_collector_provenance(payload):
            payload["prospective_provenance_known_utc"] = first_known
        else:
            payload["prospective_provenance_known_utc"] = ""
        if exists is None:
            payload_json = json.dumps(payload, sort_keys=True)
            values = (
                str(topic.get("topic_signature") or ""),
                first_known,
                last_known,
                str(topic.get("published_utc") or ""),
                str(topic.get("category") or ""),
                json.dumps(topic.get("direct_currencies") or [], sort_keys=True),
                json.dumps(topic.get("topic_tags") or [], sort_keys=True),
                article_count,
                source_count,
                payload_json,
            )
            connection.execute(
                """
                INSERT INTO topic_events (
                    topic_id, topic_signature, first_known_utc, last_known_utc,
                    published_utc, category, direct_currencies_json,
                    topic_tags_json, article_count, distinct_source_count,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (topic_id, *values),
            )
            inserted += 1
            continue
        try:
            prior_payload = json.loads(str(exists[4]))
        except (TypeError, ValueError, json.JSONDecodeError):
            prior_payload = {}
        current_bound = prospective_collector_provenance(payload)
        prior_bound = prospective_collector_provenance(prior_payload)
        if current_bound:
            # The current representative owns the current semantic payload.
            # If a later article changes score/direction, its meaning was not
            # known at an older representative's clock even when both rows use
            # the same collector contract.
            payload["prospective_provenance_known_utc"] = first_known
        elif prior_bound:
            # An unattested refresh may update counters, but must never replace
            # a topic payload whose current prospective boundary was already
            # established under the exact collector/clock contract.
            payload = dict(prior_payload)
        payload_json = json.dumps(payload, sort_keys=True)
        values = (
            str(payload.get("topic_signature") or topic.get("topic_signature") or ""),
            first_known,
            last_known,
            str(payload.get("published_utc") or topic.get("published_utc") or ""),
            str(payload.get("category") or topic.get("category") or ""),
            json.dumps(
                payload.get("direct_currencies")
                or topic.get("direct_currencies") or [],
                sort_keys=True,
            ),
            json.dumps(
                payload.get("topic_tags") or topic.get("topic_tags") or [],
                sort_keys=True,
            ),
            article_count,
            source_count,
            payload_json,
        )
        prior_payload_json = json.dumps(
            topic_history_payload(prior_payload),
            sort_keys=True,
        )
        first_changed = bool(first_known and first_known < str(exists[0] or ""))
        material_change = bool(
            prior_payload_json != payload_json
            or article_count > int(exists[2] or 0)
            or source_count > int(exists[3] or 0)
            or first_changed
        )
        if not material_change:
            continue
        connection.execute(
            """
            UPDATE topic_events
            SET topic_signature = ?,
                first_known_utc = MIN(first_known_utc, ?),
                last_known_utc = MAX(last_known_utc, ?),
                published_utc = ?, category = ?,
                direct_currencies_json = ?, topic_tags_json = ?,
                article_count = MAX(article_count, ?),
                distinct_source_count = MAX(distinct_source_count, ?),
                payload_json = ?
            WHERE topic_id = ?
            """,
            (*values, topic_id),
        )
        updated += 1
    if not caller_owned_transaction:
        connection.commit()
    return inserted, updated


def reconcile_topic_history(
    connection: sqlite3.Connection,
    *,
    since: dt.datetime,
    current_topic_ids: Sequence[str],
) -> int:
    """Remove superseded topic identities while raw evidence is still retained."""

    caller_owned_transaction = connection.in_transaction
    ids = [str(value) for value in current_topic_ids if str(value)]
    if ids:
        placeholders = ",".join("?" for _ in ids)
        cursor = connection.execute(
            f"""
            DELETE FROM topic_events
            WHERE first_known_utc >= ?
              AND topic_id NOT IN ({placeholders})
            """,
            (iso_utc(since), *ids),
        )
    else:
        cursor = connection.execute(
            "DELETE FROM topic_events WHERE first_known_utc >= ?",
            (iso_utc(since),),
        )
    if not caller_owned_transaction:
        connection.commit()
    return max(0, int(cursor.rowcount))


ARTICLE_STORAGE_VOLATILE_FIELDS = {
    # Source polling state is maintained separately. Exact repeat observations
    # are not independent news evidence and should not rewrite causal rows.
    "last_seen_utc",
    "duplicate_observation_count",
    # These are recomputed from the current poll time. Treating them as
    # material content made unchanged listing pages rewrite every article on
    # every poll and falsely inflated duplicate/corroboration counts.
    "availability_lag_minutes",
    "causal_known_utc",
    "event_lineage_id",
    "material_update_id",
    "publication_hold_seconds",
}


def article_storage_payload(article: Mapping[str, Any]) -> dict[str, Any]:
    volatile_fields = set(ARTICLE_STORAGE_VOLATILE_FIELDS)
    if article.get("source_observation_ledger_contract") == SOURCE_OBSERVATION_LEDGER_CONTRACT or article.get("classification_observation_contract") == CLASSIFICATION_OBSERVATION_CONTRACT:
        # Existing readers that already honor the explicit causal clock also
        # retain the new version floor; it must not disappear during compaction.
        volatile_fields.discard("causal_known_utc")
    if bool(article.get("published_time_inferred")) and article.get("source_observation_ledger_contract") != SOURCE_OBSERVATION_LEDGER_CONTRACT and article.get("classification_observation_contract") != CLASSIFICATION_OBSERVATION_CONTRACT:
        # HTML listing pages without machine-readable dates use first_seen as
        # their causal time.  The rendered published timestamp therefore moves
        # with every poll and is not a content revision.
        volatile_fields.add("published_utc")
    return {
        key: value
        for key, value in dict(article).items()
        if key not in volatile_fields
    }


STRUCTURED_MATERIAL_FIELDS = (
    "event_series_id",
    "reference_period",
    "reference_date",
    "actual",
    "actual_value",
    "consensus",
    "consensus_value",
    "previous",
    "previous_value",
    "revised_previous",
    "revised_previous_value",
    "release_components",
    "source_native_components",
    "material_content_sha256",
)


def structured_material_identity(article: Mapping[str, Any]) -> str:
    """Return the stable economic identity of one structured observation.

    Observation and inferred publication clocks are deliberately excluded.
    A changed value, revision, component set, reference period, or content hash
    remains a distinct material version.
    """

    if not bool(article.get("structured_event")):
        return ""
    lineage = clean_text(article.get("event_lineage_id"))
    if not lineage:
        # Compact storage intentionally omits the derived lineage id. Rebuild
        # it from the same publisher identity inputs used by classification so
        # an existing compact row can still suppress a repeated inferred-clock
        # observation after a parser repair.
        source_id = clean_text(article.get("source_id"))
        external_id = clean_text(article.get("external_id"))
        identity_url = canonical_url(
            article.get("source_url") or article.get("url")
        )
        if external_id:
            identity = f"{source_id}:{external_id}"
        elif identity_url:
            identity = identity_url
        else:
            identity = normalized_headline(
                article.get("headline") or article.get("title")
            )
        if identity:
            lineage = stable_id("structured_lineage", identity)
    if not lineage:
        return ""
    material = {key: article.get(key) for key in STRUCTURED_MATERIAL_FIELDS}
    return stable_id(
        lineage,
        json.dumps(material, sort_keys=True, separators=(",", ":")),
    )


def activation_claim_predates_immutable_first_seen(
    article: Mapping[str, Any],
    immutable_first_seen: dt.datetime,
) -> bool:
    """Detect a prospective-contract claim created by a later repeat poll.

    A source can return the same stable URL indefinitely.  Classification is
    performed before the database lookup, so a repeat observed after a new
    contract activates can initially carry ``*_activation_eligible=True`` even
    though the row's immutable first-seen clock predates that contract.  The
    stored first-seen timestamp must govern every prospective admission claim.

    Only a positive claim whose explicit activation boundary is later than the
    immutable first-seen time requires replay.  A false flag can also mean the
    article is simply not an observation of that contract, so time alone must
    never turn a false flag true.
    """

    for key, value in article.items():
        suffix = "_activation_eligible"
        if not key.endswith(suffix) or not bool(value):
            continue
        activated = parse_datetime(
            article.get(f"{key[:-len(suffix)]}_activated_utc")
        )
        if activated is not None and immutable_first_seen < activated:
            return True
    return False


def replay_article_at_immutable_first_seen(
    article: Mapping[str, Any],
    immutable_first_seen: dt.datetime,
) -> dict[str, Any]:
    """Rebuild activation-sensitive semantics on the original causal clock."""

    if not activation_claim_predates_immutable_first_seen(
        article, immutable_first_seen
    ):
        return dict(article)
    raw = dict(article)
    raw["title"] = article.get("headline") or article.get("title") or ""
    raw["url"] = article.get("source_url") or article.get("url") or ""
    if not raw.get("source_currencies"):
        raw["source_currencies"] = list(article.get("currencies") or [])
    official_search_activation = parse_datetime(
        article.get("official_search_policy_rate_activated_utc")
    )
    if (
        bool(article.get("official_search_policy_rate_activation_eligible"))
        and official_search_activation is not None
        and immutable_first_seen < official_search_activation
    ):
        # These fields were synthesized only because the later repeat poll was
        # (incorrectly) considered post-activation.  Remove them before replay
        # so the pre-activation diagnostic cannot inherit structured-release
        # standing from its own previously classified payload.
        for key in (
            "structured_event",
            "event_series_id",
            "event_name",
            "unit",
            "actual",
            "actual_value",
            "previous",
            "previous_value",
            "release_components",
            "source_native_components",
            "structured_component_change",
            "numeric_causal_known_utc",
            "numeric_parser_activated_utc",
            "numeric_extraction_contract_id",
            "numeric_verification_state",
            "release_stage",
        ):
            raw.pop(key, None)
    return classify_article(raw, first_seen=immutable_first_seen)


def upsert_articles(
    connection: sqlite3.Connection,
    articles: Sequence[Mapping[str, Any]],
    now: dt.datetime,
    *,
    classification_clock_provider: Any = None,
    observation_receipt: Any = None,
) -> tuple[int, int]:
    """Atomically retain source observations and update coherent projections."""
    receipt = observation_receipt if isinstance(observation_receipt, dict) else {}
    receipt.update(source_observations_refused=0, classification_observations_refused=0)
    with source_observation_transaction(connection):
        initialize_source_observation_ledger(connection)
        initialize_classification_observations(connection)
        return _upsert_articles_with_source_observations(connection, articles, now, classification_clock_provider=classification_clock_provider, observation_receipt=receipt)


def _upsert_articles_with_source_observations(
    connection: sqlite3.Connection,
    articles: Sequence[Mapping[str, Any]],
    now: dt.datetime,
    *,
    classification_clock_provider: Any = None,
    observation_receipt: Any = None,
) -> tuple[int, int]:
    inserted = 0
    duplicates = 0
    for article in articles:
        incoming_observation = dict(article)
        storage_event_id = str(article["event_id"])
        existing = connection.execute(
            """
            SELECT first_seen_utc, published_utc, payload_json
            FROM articles WHERE event_id = ?
            """,
            (storage_event_id,),
        ).fetchone()
        # Some RSS/search providers revise an item's timestamp on later polls.
        # The classifier intentionally includes the timestamp bucket in a
        # fallback event id, so without this stable-link check the identical
        # article URL is inserted again as apparently new causal evidence.
        # Structured releases are excluded because their versioned event ids
        # intentionally preserve actual/revision updates.
        activation_sensitive_claim = any(
            key.endswith("_activation_eligible") and bool(value)
            for key, value in article.items()
        )
        if (
            existing is None
            and not bool(article.get("immutable_source_version_boundary"))
            and str(article.get("source_url") or "")
            and (
                not bool(article.get("structured_event"))
                or activation_sensitive_claim
                or bool(article.get("published_time_inferred"))
            )
        ):
            stable_candidates = connection.execute(
                """
                SELECT event_id, first_seen_utc, published_utc, payload_json
                FROM articles
                WHERE event_id IN (
                    SELECT event_id FROM articles
                    WHERE source_id = ? AND source_url = ? AND source_url <> ''
                    UNION
                    SELECT canonical_event_id FROM article_source_versions_v1 AS v
                    WHERE json_extract(v.source_identity_json,'$.source_id') = ?
                      AND json_extract(v.content_json,'$.source_url') = ?
                      AND EXISTS (
                        SELECT 1 FROM article_source_observations_v1 AS o
                        WHERE o.version_id = v.version_id
                          AND o.clock_status = 'valid_attested_observation'
                      )
                )
                ORDER BY first_seen_utc, event_id
                """,
                (article["source_id"], article["source_url"], article["source_id"], article["source_url"]),
            ).fetchall()
            stable_existing = stable_candidates[0] if stable_candidates else None
            inferred_structured_material_match = False
            if (
                bool(article.get("structured_event"))
                and bool(article.get("published_time_inferred"))
            ):
                current_material_identity = structured_material_identity(article)
                stable_existing = None
                for candidate in stable_candidates:
                    try:
                        candidate_payload = json.loads(str(candidate[3] or "{}"))
                    except (TypeError, ValueError, json.JSONDecodeError):
                        continue
                    if (
                        current_material_identity
                        and structured_material_identity(candidate_payload)
                        == current_material_identity
                    ):
                        stable_existing = candidate
                        inferred_structured_material_match = True
                        break
            stable_first_seen = (
                parse_datetime(stable_existing[1])
                if stable_existing is not None
                else None
            )
            retroactive_activation_claim = bool(
                stable_first_seen is not None
                and activation_claim_predates_immutable_first_seen(
                    article, stable_first_seen
                )
            )
            if stable_existing is not None and (
                not bool(article.get("structured_event"))
                or retroactive_activation_claim
                or inferred_structured_material_match
            ):
                storage_event_id = str(stable_existing[0])
                existing = (
                    stable_existing[1],
                    stable_existing[2],
                    stable_existing[3],
                )
        source_observation = record_source_observation(
            connection, incoming_observation, storage_event_id, now,
            expected_provenance={
                "collector_contract_id": COLLECTOR_CONTRACT_ID,
                "collector_cohort_id": COLLECTOR_COHORT_ID,
                "observation_time_contract_id": OBSERVATION_TIME_CONTRACT_ID,
            },
        )
        try:
            previous_for_selection = json.loads(str(existing[2])) if existing else {}
        except (TypeError, ValueError, json.JSONDecodeError):
            previous_for_selection = {}
        if not select_source_observation(source_observation, previous_for_selection, incoming_observation):
            if not source_observation["eligible"]:
                observation_receipt["source_observations_refused"] += 1
            duplicates += int(existing is not None)
            continue
        payload = dict(article)
        if storage_event_id != str(article["event_id"]):
            original_event_id = str(article["event_id"])
            payload["event_id"] = storage_event_id
            for identity_field in ("event_lineage_id", "material_update_id"):
                if str(payload.get(identity_field) or "") == original_event_id:
                    payload[identity_field] = storage_event_id
        if existing:
            immutable_first_seen = parse_datetime(existing[0])
            if immutable_first_seen is not None:
                article = replay_article_at_immutable_first_seen(
                    article, immutable_first_seen
                )
                payload = dict(article)
                if storage_event_id != str(article["event_id"]):
                    payload["event_id"] = storage_event_id
            payload["first_seen_utc"] = existing[0]
            try:
                previous_payload = json.loads(str(existing[2]))
            except (TypeError, ValueError, json.JSONDecodeError):
                previous_payload = {}
            # The table column is the immutable first-collected publication
            # time. Some RSS/search providers mutate an item's timestamp on a
            # later poll while retaining its stable URL. Keep the payload on
            # the same clock as the authoritative column so point-in-time
            # replay cannot silently move the article later (or earlier).
            payload["published_utc"] = str(existing[1] or "")
            # Re-polling an unchanged structured release must not move the
            # clock at which its numeric value became locally knowable.  This
            # used to drift whenever a collector contract changed because
            # the fresh classification carried the current poll time into
            # the replacement payload even though the table's first-seen
            # column remained immutable.
            for causal_field in ("numeric_causal_known_utc", "causal_known_utc"):
                previous_causal = parse_datetime(previous_payload.get(causal_field))
                current_causal = parse_datetime(payload.get(causal_field))
                if previous_causal is not None and current_causal is not None:
                    payload[causal_field] = iso_utc(
                        min(previous_causal, current_causal)
                    )
            # Clock provenance is immutable evidence metadata. Re-polling an
            # old article under a newer collector or clock contract must not
            # silently relabel the time semantics of its original first-seen
            # observation.
            provenance_defaults = {
                "collector_contract_id": "",
                "collector_cohort_id": "",
                "observation_time_contract_id": "",
                "observation_clock_trusted": False,
                "observation_clock_source": "legacy_unattested",
            }
            for provenance_field, legacy_default in provenance_defaults.items():
                # Absence is itself immutable evidence: a pre-V39 first-seen
                # row never carried a trusted clock attestation. Filling its
                # missing keys from a later poll would retroactively relabel
                # the historical observation as current prospective proof.
                payload[provenance_field] = previous_payload.get(
                    provenance_field, legacy_default
                )
            if bool(previous_payload.get("detail_enriched")) and not bool(payload.get("detail_enriched")):
                # The raw incoming observation is retained independently. Keep
                # all canonical fields on the prior enriched projection.
                duplicates += 1
                continue
            payload = bind_active_source_version(payload, source_observation, previous_payload)
            payload = record_and_bind_classification(connection, payload, previous_payload,
                classification_clock_provider, expected_provenance={
                "collector_contract_id": COLLECTOR_CONTRACT_ID,
                "collector_cohort_id": COLLECTOR_COHORT_ID,
                "observation_time_contract_id": OBSERVATION_TIME_CONTRACT_ID,
            }, operation="upsert_update")
            if payload is None:
                observation_receipt["classification_observations_refused"] += 1
                duplicates += 1
                continue
            if json.dumps(
                article_storage_payload(previous_payload),
                sort_keys=True,
            ) == json.dumps(
                article_storage_payload(payload),
                sort_keys=True,
            ):
                duplicates += 1
                continue
            connection.execute(
                """
                UPDATE articles
                SET source_id = ?, source_kind = ?, source_name = ?, source_quality = ?, source_verified = ?,
                    last_seen_utc = ?, headline = ?, summary = ?,
                    source_url = ?, domain = ?, relevant = ?, category = ?,
                    scope = ?, currencies_json = ?, currency_scores_json = ?,
                    directional_bias_json = ?, generic_sentiment_score = ?,
                    monetary_impulse = ?, risk_off_score = ?, risk_on_score = ?,
                    directional_confidence = ?, severity = ?,
                    movement_potential = ?, post_window_minutes = ?,
                    duplicate_count = duplicate_count + 1, payload_json = ?
                WHERE event_id = ?
                """,
                (
                    payload["source_id"],
                    payload["source_kind"],
                    payload["source_name"],
                    payload["source_quality"],
                    int(bool(payload["source_verified"])),
                    iso_utc(now),
                    payload["headline"],
                    payload["summary"],
                    payload["source_url"],
                    payload["domain"],
                    int(bool(payload["relevant"])),
                    payload["category"],
                    payload["scope"],
                    json.dumps(payload["currencies"], sort_keys=True),
                    json.dumps(payload["currency_scores"], sort_keys=True),
                    json.dumps(payload["directional_bias"], sort_keys=True),
                    payload["generic_sentiment_score"],
                    payload["monetary_impulse"],
                    payload["risk_off_score"],
                    payload["risk_on_score"],
                    payload["directional_confidence"],
                    payload["severity"],
                    payload["movement_potential"],
                    payload["post_window_minutes"],
                    json.dumps(article_storage_payload(payload), sort_keys=True),
                    storage_event_id,
                ),
            )
            record_source_projection(connection, storage_event_id, "upsert_update")
            duplicates += 1
            continue
        payload = bind_active_source_version(payload, source_observation)
        payload = record_and_bind_classification(connection, payload, {},
            classification_clock_provider, expected_provenance={
                "collector_contract_id": COLLECTOR_CONTRACT_ID,
                "collector_cohort_id": COLLECTOR_COHORT_ID,
                "observation_time_contract_id": OBSERVATION_TIME_CONTRACT_ID,
            }, operation="upsert_insert")
        if payload is None:
            observation_receipt["classification_observations_refused"] += 1
            continue
        connection.execute(
            """
            INSERT INTO articles (
                event_id, source_id, source_name, source_kind, source_quality,
                source_verified, published_utc, first_seen_utc, last_seen_utc,
                headline, summary, source_url, domain, relevant, category,
                scope, currencies_json, currency_scores_json,
                directional_bias_json, generic_sentiment_score,
                monetary_impulse, risk_off_score, risk_on_score,
                directional_confidence, severity, movement_potential,
                post_window_minutes, duplicate_count, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
            """,
            (
                storage_event_id,
                payload["source_id"],
                payload["source_name"],
                payload["source_kind"],
                payload["source_quality"],
                int(bool(payload["source_verified"])),
                payload["published_utc"],
                payload["first_seen_utc"],
                payload["last_seen_utc"],
                payload["headline"],
                payload["summary"],
                payload["source_url"],
                payload["domain"],
                int(bool(payload["relevant"])),
                payload["category"],
                payload["scope"],
                json.dumps(payload["currencies"], sort_keys=True),
                json.dumps(payload["currency_scores"], sort_keys=True),
                json.dumps(payload["directional_bias"], sort_keys=True),
                payload["generic_sentiment_score"],
                payload["monetary_impulse"],
                payload["risk_off_score"],
                payload["risk_on_score"],
                payload["directional_confidence"],
                payload["severity"],
                payload["movement_potential"],
                payload["post_window_minutes"],
                json.dumps(article_storage_payload(payload), sort_keys=True),
            ),
        )
        record_source_projection(connection, storage_event_id, "upsert_insert")
        inserted += 1
    return inserted, duplicates


def load_relevant_articles(
    connection: sqlite3.Connection,
    *,
    since: dt.datetime,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT payload_json, first_seen_utc, last_seen_utc, duplicate_count
        FROM articles
        WHERE relevant = 1 AND published_utc >= ?
        ORDER BY published_utc, event_id
        """,
        (iso_utc(since),),
    ).fetchall()
    output: list[dict[str, Any]] = []
    for payload_json, first_seen, last_seen, duplicate_count in rows:
        try:
            payload = json.loads(payload_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        payload["first_seen_utc"] = first_seen
        payload["causal_known_utc"] = iso_utc(
            causal_known_datetime(payload)
            or parse_datetime(first_seen)
            or since
        )
        payload["last_seen_utc"] = last_seen
        payload["duplicate_observation_count"] = int(duplicate_count or 0)
        payload["corroboration_count"] = int(
            payload.get("corroboration_count") or 0
        )
        output.append(payload)
    return collapse_exact_source_url_duplicates(output)


def load_canonical_articles_by_event_ids(
    connection: sqlite3.Connection,
    event_ids: Sequence[str],
) -> list[dict[str, Any]]:
    """Reload newly polled rows with their original database observation time.

    A repeated feed item is classified at the current poll time before
    ``upsert_articles`` recognizes it as a duplicate.  Incremental topic
    publication must not use that transient timestamp: doing so can make an
    old claim look newly known until the full retained-corpus rebuild finishes.
    Reloading the just-seen identities from the immutable article ledger keeps
    the low-latency topic path on the same causal clock as the final rebuild.
    """

    identities = sorted({str(value) for value in event_ids if str(value)})
    if not identities:
        return []
    placeholders = ",".join("?" for _ in identities)
    rows = connection.execute(
        f"""
        SELECT payload_json, first_seen_utc, last_seen_utc, duplicate_count
        FROM articles
        WHERE event_id IN ({placeholders})
        ORDER BY published_utc, event_id
        """,
        identities,
    ).fetchall()
    output: list[dict[str, Any]] = []
    for payload_json, first_seen, last_seen, duplicate_count in rows:
        try:
            payload = json.loads(payload_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        payload["first_seen_utc"] = str(first_seen or "")
        payload["causal_known_utc"] = iso_utc(
            causal_known_datetime(payload)
            or parse_datetime(first_seen)
        )
        payload["last_seen_utc"] = str(last_seen or first_seen or "")
        payload["duplicate_observation_count"] = int(duplicate_count or 0)
        output.append(payload)
    return collapse_exact_source_url_duplicates(output)


def cluster_incremental_topics_with_history(
    connection: sqlite3.Connection,
    articles: Sequence[Mapping[str, Any]],
    *,
    as_of: dt.datetime,
) -> list[dict[str, Any]]:
    """Cluster a low-latency source batch against matching retained stories.

    Per-source publication is deliberately faster than the full collection
    pass.  It must nevertheless consult prior articles sharing the semantic
    topic signature; otherwise a syndicated rewrite on a new URL can briefly
    masquerade as a fresh catalyst until end-of-cycle reconciliation.
    """

    current = [dict(article) for article in articles]
    current_ids = {
        str(article.get("event_id") or "") for article in current
        if str(article.get("event_id") or "")
    }
    signatures = sorted(
        {
            str(article.get("topic_signature") or "")
            for article in current
            if str(article.get("topic_signature") or "")
        }
    )
    historical_ids: set[str] = set()
    if signatures:
        placeholders = ",".join("?" for _ in signatures)
        since = as_of - dt.timedelta(hours=24)
        for (payload_json,) in connection.execute(
            f"""
            SELECT payload_json FROM topic_events
            WHERE topic_signature IN ({placeholders})
              AND published_utc >= ?
            """,
            (*signatures, iso_utc(since)),
        ).fetchall():
            try:
                payload = json.loads(str(payload_json or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            historical_ids.update(
                str(value)
                for value in payload.get("article_event_ids") or []
                if str(value)
            )
    canonical = load_canonical_articles_by_event_ids(
        connection, sorted(current_ids | historical_ids)
    )
    clustered = cluster_articles(canonical or current, as_of=as_of)
    return [
        topic
        for topic in clustered
        if current_ids
        & {
            str(value)
            for value in topic.get("article_event_ids") or []
            if str(value)
        }
    ]


def load_context_articles(
    connection: sqlite3.Connection,
    *,
    since: dt.datetime,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT payload_json, first_seen_utc, last_seen_utc, duplicate_count
        FROM articles
        WHERE relevant = 0 AND published_utc >= ?
        ORDER BY published_utc, event_id
        """,
        (iso_utc(since),),
    ).fetchall()
    output: list[dict[str, Any]] = []
    for payload_json, first_seen, last_seen, duplicate_count in rows:
        try:
            payload = json.loads(payload_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not bool(payload.get("context_only")):
            continue
        payload["first_seen_utc"] = first_seen
        payload["causal_known_utc"] = iso_utc(
            causal_known_datetime(payload)
            or parse_datetime(first_seen)
            or since
        )
        payload["last_seen_utc"] = last_seen
        payload["duplicate_observation_count"] = int(duplicate_count or 0)
        payload["corroboration_count"] = int(
            payload.get("corroboration_count") or 0
        )
        output.append(payload)
    return collapse_exact_source_url_duplicates(output)


def policy_article_is_schedule(article: Mapping[str, Any]) -> bool:
    """Return whether an official policy item is only a future event clock."""

    if clean_text(article.get("source_role")) == "primary_policy_calendar":
        return True
    source_id = clean_text(article.get("source_id")).lower()
    if "policy_decision_calendar" in source_id:
        return True
    text = " ".join(
        clean_text(article.get(field)).lower()
        for field in (
            "headline",
            "summary",
            "policy_document_type",
        )
    )
    schedule_phrases = (
        "decision calendar",
        "policy calendar",
        "decision dates",
        "meeting dates",
        "schedule for policy",
        "schedule of policy",
        "schedule for monetary policy",
        "schedule of monetary policy",
        "schedule for policy interest rate announcements",
    )
    return any(phrase in text for phrase in schedule_phrases)


def build_persistent_policy_state(
    articles: Sequence[Mapping[str, Any]],
    *,
    as_of: dt.datetime,
    sources: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Retain the latest first-party policy document until it is superseded.

    This is an auditable shadow context artifact. It cannot publish a trade,
    promote a model, veto execution, or authorize Practice 007.
    """

    documents: list[dict[str, Any]] = []
    excluded_policy_clock_count = 0
    excluded_non_stance_policy_count = 0
    for article in articles:
        if not bool(article.get("source_verified")):
            continue
        if not bool(article.get("official_policy_release")):
            continue
        # A future decision calendar or a release announcing future meeting
        # dates is an event clock, not a policy stance. Treating either as the
        # latest persistent document overwrites the last completed decision.
        if policy_article_is_schedule(article):
            excluded_policy_clock_count += 1
            continue
        # Fail closed: persistence is a statement about the issuer's policy
        # stance, not merely about publication by a central bank.  Classifier
        # v145 writes this explicit eligibility bit after inspecting the
        # source-native document identity and non-stance guards.  Legacy rows
        # without the bit cannot silently supersede a verified decision.
        if article.get("policy_stance_bearing_eligible") is not True:
            excluded_non_stance_policy_count += 1
            continue
        known = causal_known_datetime(article)
        if known is None or known > as_of:
            continue
        source_id = clean_text(article.get("source_id"))
        configured_source = (sources or {}).get(source_id) or {}
        authority_currencies = (
            configured_source.get("currencies")
            or article.get("source_currencies")
            or article.get("currencies")
            or []
        )
        currencies = [
            str(value).upper()
            for value in authority_currencies
            if str(value).upper() in ALL_CURRENCIES
        ]
        if not currencies:
            continue
        impulse = clamp(safe_float(article.get("monetary_impulse"), 0.0))
        state = "HAWKISH" if impulse > 0.05 else "DOVISH" if impulse < -0.05 else "NEUTRAL"
        for currency in currencies:
            documents.append(
                {
                    "currency": currency,
                    "state": state,
                    "monetary_impulse": round(impulse, 6),
                    "known_utc": iso_utc(known),
                    "published_utc": clean_text(article.get("published_utc")),
                    "source_id": source_id,
                    "event_id": clean_text(article.get("event_id")),
                    "headline": clean_text(article.get("headline")),
                    "summary": clean_text(article.get("summary"))[:2_000],
                    "source_url": canonical_url(article.get("source_url")),
                    "policy_document_type": clean_text(
                        article.get("policy_document_type")
                    ),
                    "policy_stance_bearing_eligible": True,
                    "detail_enriched": bool(article.get("detail_enriched")),
                    "detail_content_sha256": clean_text(
                        article.get("detail_content_sha256")
                    ),
                }
            )
    documents.sort(key=lambda row: (row["known_utc"], row["event_id"], row["currency"]))
    latest: dict[str, dict[str, Any]] = {}
    for document in documents:
        latest[document["currency"]] = {
            **document,
            "validity": "until_superseded_by_newer_first_party_policy_document",
        }
    return {
        "schema_version": "persistent_policy_state_v3_stance_bearing_only",
        "generated_utc": iso_utc(as_of),
        "research_only": True,
        "execution_eligible": False,
        "can_modify_practice_execution": False,
        "can_authorize": False,
        "currency_count": len(latest),
        "document_count": len(documents),
        "excluded_policy_clock_count": excluded_policy_clock_count,
        "excluded_non_stance_policy_count": excluded_non_stance_policy_count,
        "currencies": dict(sorted(latest.items())),
        "recent_documents": documents[-100:],
    }


def collapse_exact_source_url_duplicates(
    articles: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Collapse repeat observations without deleting immutable audit rows.

    Unstructured articles use their canonical URL. Structured releases use
    event lineage plus their economic material values, so repeated polls of an
    unclocked official landing page collapse while actual/revision changes
    remain distinct material observations.
    """

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for index, article in enumerate(articles):
        item = dict(article)
        source_url = canonical_url(item.get("source_url"))
        if structured_material_identity(item):
            key = (
                "structured_lineage",
                structured_material_identity(item),
            )
        elif source_url and not bool(item.get("structured_event")):
            key = ("url", source_url)
        else:
            key = (
                "event",
                str(item.get("event_id") or f"row:{index}"),
            )
        grouped[key].append(item)

    collapsed: list[dict[str, Any]] = []
    for rows in grouped.values():
        rows.sort(
            key=lambda row: (
                str(row.get("first_seen_utc") or "9999"),
                -safe_float(row.get("source_quality"), 0.0),
                str(row.get("event_id") or ""),
            )
        )
        # A source-contract repair may intentionally retain the legacy URL
        # row and add a separately versioned diagnostic observation. Prefer
        # that explicit version boundary in derived views without mutating or
        # deleting the historical row itself.
        preferred_rows = [
            row for row in rows
            if bool(row.get("immutable_source_version_boundary"))
        ] or rows
        result = dict(
            max(
                preferred_rows,
                key=lambda row: (
                    int(bool(row.get("detail_enriched"))),
                    len(clean_text(row.get("summary"))),
                    safe_float(row.get("source_quality"), 0.0),
                    str(row.get("causal_known_utc") or ""),
                ),
            )
            if any(bool(row.get("detail_enriched")) for row in preferred_rows)
            else preferred_rows[0]
        )
        if len(rows) > 1:
            result["first_seen_utc"] = min(
                str(row.get("first_seen_utc") or "9999") for row in rows
            )
            causal_candidates = [
                value
                for row in rows
                if (value := causal_known_datetime(row)) is not None
            ]
            causal_known = (
                causal_known_datetime(result)
                if bool(result.get("detail_enriched"))
                else min(
                    causal_candidates,
                    default=parse_datetime(result["first_seen_utc"]),
                )
            )
            result["causal_known_utc"] = (
                iso_utc(causal_known) if causal_known is not None else ""
            )
            result["last_seen_utc"] = max(
                str(row.get("last_seen_utc") or "") for row in rows
            )
            result["duplicate_observation_count"] = sum(
                int(row.get("duplicate_observation_count") or 0)
                for row in rows
            ) + len(rows) - 1
            result["cross_query_duplicate_count"] = len(rows) - 1
            result["discovery_source_ids"] = sorted(
                {
                    str(row.get("source_id") or "")
                    for row in rows
                    if str(row.get("source_id") or "")
                }
            )
        collapsed.append(result)
    collapsed.sort(
        key=lambda row: (
            str(row.get("published_utc") or ""),
            str(row.get("event_id") or ""),
        )
    )
    return collapsed


def enforce_directional_publication_invariants(
    article: Mapping[str, Any],
) -> dict[str, Any]:
    """Fail closed when a derived topic no longer carries a usable direction."""

    result = dict(article)
    scores = result.get("currency_scores")
    has_scores = bool(isinstance(scores, Mapping) and scores)
    if (
        not has_scores
        or bool(result.get("reports_prior_market_move"))
        or bool(result.get("non_catalyst_context"))
        or bool(result.get("context_only"))
    ):
        result["directional_publish_eligible"] = False
    if not has_scores:
        result["directional_evidence"] = False
        result["directional_bias"] = {}
    if bool(result.get("directional_publish_eligible")):
        # Publication is still a research-feed state, never an execution
        # authorization. Keep that boundary explicit on stale topic payloads.
        result["execution_eligible"] = False
        result["can_place_orders"] = False
    return result


def reclassify_stored_articles(
    connection: sqlite3.Connection,
    *,
    sources: Mapping[str, Mapping[str, Any]],
    since: dt.datetime,
    maximum_rows: int = RECLASSIFICATION_BATCH_SIZE,
    progress_callback: Any = None,
    classification_clock_provider: Any = None,
) -> int:
    """Apply the current deterministic rules to retained pre-versioned rows."""

    caller_owned_transaction = connection.in_transaction
    rows = connection.execute(
        """
        SELECT event_id, payload_json, first_seen_utc, last_seen_utc,
               published_utc
        FROM articles
        WHERE published_utc >= ?
          AND (
            COALESCE(
              CASE WHEN json_valid(payload_json)
                   THEN json_extract(payload_json,'$.classification_version') END,
              ''
            ) <> ?
            OR COALESCE(
              CASE WHEN json_valid(payload_json)
                   THEN json_extract(payload_json,'$.published_utc') END,
              ''
            ) <> COALESCE(published_utc,'')
            OR COALESCE(
              CASE WHEN json_valid(payload_json)
                   THEN json_extract(payload_json,'$.source_contract_id') END,
              ''
            ) = ''
            OR COALESCE(
              CASE WHEN json_valid(payload_json)
                   THEN json_extract(payload_json,'$.source_cohort_id') END,
              ''
            ) = ''
          )
        -- Reclassify verified official observations first, then newest-first
        -- within each trust tier. A rule upgrade can otherwise leave a
        -- source-native numeric release behind tens of thousands of broad
        -- discovery rows. The entire retained corpus is still rebuilt across
        -- bounded batches; only the order of migration changes.
        ORDER BY source_verified DESC, published_utc DESC, event_id
        LIMIT ?
        """,
        (iso_utc(since), CLASSIFICATION_VERSION, max(1, int(maximum_rows))),
    ).fetchall()
    emit_collector_progress(
        progress_callback,
        "postprocessing_evidence",
        postprocess_step="reclassifying_retained_articles",
        candidate_rows=len(rows),
        processed_rows=0,
        reclassified_rows=0,
    )
    changed = 0
    for processed, (
        event_id,
        payload_json,
        first_seen_text,
        last_seen_text,
        stored_published_text,
    ) in enumerate(rows, start=1):
        try:
            previous = json.loads(payload_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if (
            previous.get("classification_version") == CLASSIFICATION_VERSION
            and str(previous.get("published_utc") or "")
            == str(stored_published_text or "")
            and clean_text(previous.get("source_contract_id"))
            and clean_text(previous.get("source_cohort_id"))
        ):
            continue
        first_seen = parse_datetime(first_seen_text)
        if first_seen is None:
            continue
        source_config = sources.get(str(previous.get("source_id") or ""))
        if not isinstance(source_config, Mapping) or not source_config:
            # A retained observation cannot be bound to an invented contract.
            # Leave it unchanged until its actual configured source is present.
            continue
        source = source_config_lineage(source_config)
        diagnostic_release_urls = {
            canonical_url(value)
            for value in (source.get("diagnostic_release_utc_by_url") or {})
            if canonical_url(value)
        }
        current_source_contract = clean_text(source.get("source_contract_id"))
        previous_source_contract = clean_text(
            previous.get("source_contract_id")
        )
        if (
            canonical_url(previous.get("source_url")) in diagnostic_release_urls
            and current_source_contract
            and previous_source_contract != current_source_contract
        ):
            # These four ABS rows are the evidence that exposed the retrieval-
            # clock defect. Keep their V8 payloads byte-for-byte immutable;
            # V9 emits a distinct diagnostic observation carrying the exact
            # source-native release clock and a fail-closed forward gate.
            continue
        updated = classify_article(
            {
                "source_id": previous.get("source_id"),
                "source_name": previous.get("source_name"),
                "source_kind": previous.get("source_kind"),
                "source_quality": previous.get("source_quality"),
                "source_verified": previous.get("source_verified"),
                "source_direct": (
                    bool(source.get("direct"))
                    if "direct" in source
                    else bool(previous.get("source_direct", False))
                ),
                "retrieval_via": previous.get(
                    "retrieval_via",
                    source.get("retrieval_via", "direct"),
                ),
                "publisher_url": previous.get("publisher_url"),
                "numeric_parser_activated_utc": source.get(
                    "numeric_parser_activated_utc"
                ),
                "numeric_extraction_contract_id": source.get(
                    "numeric_extraction_contract_id"
                ),
                "source_currencies": list(source.get("currencies") or []),
                "title": previous.get("headline"),
                "summary": previous.get("summary"),
                "url": previous.get("source_url"),
                # The normalized table column is the immutable timestamp that
                # was collected with the article.  Mutable RSS/search payloads
                # must not move that clock during a later poll or a classifier
                # migration.
                "published_utc": stored_published_text,
                "published_time_inferred": bool(
                    previous.get("published_time_inferred")
                    or str(previous.get("source_kind") or "").lower()
                    == "html_links"
                ),
                **{
                    key: previous.get(key)
                    for key in (
                        "source_role",
                        "source_listing_bootstrap",
                        "source_listing_new_item",
                        "detail_enriched",
                        "detail_enrichment_kind",
                        "detail_available_utc",
                        "detail_enrichment_research_only",
                        "detail_content_sha256",
                        "detail_content_bytes",
                        "detail_text_characters",
                        "detail_source_url",
                        "detail_listing_url",
                        "detail_publisher_resolution_contract_id",
                        "detail_publisher_resolution_known_utc",
                        "detail_publisher_resolution_research_only",
                        "detail_archive_path",
                        "detail_attachment_discovery_state",
                        "detail_attachment_enriched",
                        "detail_attachment_count",
                        "detail_attachment_urls",
                        "detail_attachments",
                        "detail_attachment_content_sha256",
                        "detail_attachment_content_bytes",
                        "detail_attachment_text_characters",
                        "detail_attachment_available_utc",
                        "detail_attachment_parser_contract_id",
                        "detail_attachment_research_only",
                        "directional_research_only",
                        "scheduled_utc",
                        "source_reported_update_utc",
                        "structured_event",
                        "external_id",
                        "event_series_id",
                        "event_name",
                        "event_country",
                        "reference_period",
                        "reference_date",
                        "timing_precision",
                        "event_time_basis",
                        "clock_semantics",
                        "independent_domestic_event",
                        "linked_policy_factor",
                        "execution_eligible",
                        "importance",
                        "unit",
                        "actual",
                        "actual_value",
                        "numeric_causal_known_utc",
                        "numeric_direction_policy",
                        "source_native_components",
                        "source_native_update_date",
                        "consensus",
                        "consensus_value",
                        "consensus_capture_state",
                        "previous",
                        "previous_value",
                        "revised_previous",
                        "revised_previous_value",
                        "vendor_sentiment_score",
                        "vendor_sentiment_label",
                        "vendor_topics",
                        "vendor_ticker_sentiment",
                        "vendor_currency_sentiment",
                        "vendor_currencies",
                        "vendor_sentiment_research_only",
                        "source_contract_id",
                        "source_cohort_id",
                        "collector_contract_id",
                        "collector_cohort_id",
                        "material_content_sha256",
                        "release_time_rule_observed_sha256",
                        "release_time_rule_archive_name",
                        "release_rule_bytes_verified",
                        "content_versioned_at_collection",
                        "mutable_content_source",
                        "publisher_container_timestamp_utc",
                    )
                },
                # Current source configuration is authoritative for immutable
                # source/parser identities. Earlier parsers did not copy these
                # fields into HTML-link rows, so preserving an empty historical
                # payload here would silently detach the observation from the
                # contract that produced its numeric interpretation.
                "source_contract_id": clean_text(
                    source.get("source_contract_id")
                    or previous.get("source_contract_id")
                ),
                "source_cohort_id": clean_text(
                    source.get("source_cohort_id")
                    or previous.get("source_cohort_id")
                ),
                "mutable_content_source": bool(
                    source.get("mutable_content_versioned")
                ),
            },
            first_seen=first_seen,
        )
        # Preserve the stored identity and observation timestamps. Rule changes
        # must not make historical first-known evidence appear newly observed.
        updated["event_id"] = str(event_id)
        updated["first_seen_utc"] = str(first_seen_text)
        updated["last_seen_utc"] = str(last_seen_text)
        updated = preserve_active_source_version(updated, previous)
        with source_observation_transaction(connection):
            initialize_classification_observations(connection)
            updated = record_and_bind_classification(connection, updated, previous,
                classification_clock_provider, expected_provenance={
                "collector_contract_id": COLLECTOR_CONTRACT_ID,
                "collector_cohort_id": COLLECTOR_COHORT_ID,
                "observation_time_contract_id": OBSERVATION_TIME_CONTRACT_ID,
            }, operation="retained_reclassification",
                expected_stored_payload_json=payload_json)
            if updated is None:
                continue
            update_cursor = connection.execute(
                """
                UPDATE articles
                SET source_id = ?, source_kind = ?, source_name = ?, source_quality = ?, source_verified = ?,
                relevant = ?, category = ?, scope = ?, currencies_json = ?,
                    source_url = ?, domain = ?,
                    currency_scores_json = ?, directional_bias_json = ?,
                    generic_sentiment_score = ?, monetary_impulse = ?,
                    risk_off_score = ?, risk_on_score = ?,
                    directional_confidence = ?, severity = ?,
                    movement_potential = ?, post_window_minutes = ?,
                    payload_json = ?
                WHERE event_id = ? AND payload_json = ?
                """,
                (
                    updated["source_id"], updated["source_kind"], updated["source_name"],
                    updated["source_quality"], int(bool(updated["source_verified"])),
                    int(bool(updated["relevant"])),
                    updated["category"],
                    updated["scope"],
                    json.dumps(updated["currencies"], sort_keys=True),
                    updated["source_url"],
                    updated["domain"],
                    json.dumps(updated["currency_scores"], sort_keys=True),
                    json.dumps(updated["directional_bias"], sort_keys=True),
                    updated["generic_sentiment_score"],
                    updated["monetary_impulse"],
                    updated["risk_off_score"],
                    updated["risk_on_score"],
                    updated["directional_confidence"],
                    updated["severity"],
                    updated["movement_potential"],
                    updated["post_window_minutes"],
                    json.dumps(updated, sort_keys=True),
                    event_id, payload_json,
                ),
            )
            if update_cursor.rowcount != 1:
                raise ValueError("source_projection_changed_during_classification")
            record_source_projection(connection, str(event_id), "retained_reclassification")
        changed += 1
        if processed % 100 == 0:
            if not caller_owned_transaction:
                connection.commit()
            emit_collector_progress(
                progress_callback,
                "postprocessing_evidence",
                postprocess_step="reclassifying_retained_articles",
                candidate_rows=len(rows),
                processed_rows=processed,
                reclassified_rows=changed,
            )
    if not caller_owned_transaction:
        connection.commit()
    emit_collector_progress(
        progress_callback,
        "postprocessing_evidence",
        postprocess_step="reclassification_complete",
        candidate_rows=len(rows),
        processed_rows=len(rows),
        reclassified_rows=changed,
    )
    return changed


def refresh_recent_topic_contract(
    connection: sqlite3.Connection,
    *,
    sources: Mapping[str, Mapping[str, Any]],
    since: dt.datetime,
    as_of: dt.datetime,
    progress_callback: Any = None,
    classification_clock_provider: Any = None,
) -> dict[str, int]:
    """Publish recent rule upgrades before waiting on network collection.

    The raw article table and the derived topic table are separate causal
    contracts.  Reclassifying a retained article without immediately
    rebuilding its recent topic can leave a short interval where downstream
    consumers see the prior direction mapping.  Refresh recent rows first and
    republish their topics in the same transaction boundary; older retained
    history is still migrated later in the ordinary maintenance pass.
    """

    with source_observation_transaction(connection):
        reclassified = reclassify_stored_articles(
            connection,
            sources=sources,
            since=since,
            progress_callback=progress_callback,
            classification_clock_provider=classification_clock_provider,
        )
        stale_topic_count = 0
        for (payload_json,) in connection.execute(
            "SELECT payload_json FROM topic_events WHERE first_known_utc >= ?",
            (iso_utc(since),),
        ).fetchall():
            try:
                payload = json.loads(str(payload_json or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                stale_topic_count += 1
                continue
            if str(payload.get("classification_version") or "") != CLASSIFICATION_VERSION:
                stale_topic_count += 1
        if not reclassified and not stale_topic_count:
            return {
                "reclassified": 0,
                "stale_topics": 0,
                "topic_inserted": 0,
                "topic_updated": 0,
                "topic_removed": 0,
            }
        relevant = load_relevant_articles(connection, since=since)
        publication_floor = max([as_of] + [value for row in relevant if (value := classification_floor(row)) is not None])
        assessment_as_of = classification_publication_as_of(classification_clock_provider,
            publication_floor, expected_provenance={
                "collector_contract_id": COLLECTOR_CONTRACT_ID,
                "collector_cohort_id": COLLECTOR_COHORT_ID,
                "observation_time_contract_id": OBSERVATION_TIME_CONTRACT_ID,
            })
        topics = cluster_articles(relevant, as_of=assessment_as_of)
        inserted, updated = upsert_topic_events(connection, topics)
        removed = reconcile_topic_history(
            connection,
            since=since,
            current_topic_ids=[str(topic.get("topic_id") or "") for topic in topics],
        )
        return {
            "reclassified": reclassified,
            "stale_topics": stale_topic_count,
            "topic_inserted": inserted,
            "topic_updated": updated,
            "topic_removed": removed,
        }


def prune_database(
    connection: sqlite3.Connection,
    *,
    before: dt.datetime,
) -> int:
    # General discovery/news rows use the configured rolling retention window.
    # Verified official policy documents are small causal ground-truth records
    # whose last completed state remains valid until superseded. Preserve them
    # when an authority's meeting interval exceeds the general-news window.
    candidates = connection.execute(
        "SELECT event_id, payload_json FROM articles WHERE published_utc < ?",
        (iso_utc(before),),
    ).fetchall()
    delete_ids: list[tuple[str]] = []
    for event_id, payload_json in candidates:
        try:
            payload = json.loads(str(payload_json or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        if bool(payload.get("official_policy_release")) and bool(
            payload.get("source_verified")
        ):
            continue
        delete_ids.append((str(event_id),))
    if delete_ids:
        connection.executemany(
            "DELETE FROM articles WHERE event_id = ?",
            delete_ids,
        )
    connection.commit()
    return len(delete_ids)


def partition_articles_by_retention(
    articles: Sequence[Mapping[str, Any]],
    *,
    before: dt.datetime,
) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    """Keep unparseable timestamps visible, but reject known expired rows pre-insert."""

    eligible: list[Mapping[str, Any]] = []
    expired: list[Mapping[str, Any]] = []
    for article in articles:
        published = parse_datetime(article.get("published_utc"))
        persistent_policy_document = bool(
            article.get("official_policy_release")
            and article.get("source_verified")
        )
        if (
            published is not None
            and published < before
            and not persistent_policy_document
        ):
            expired.append(article)
        else:
            eligible.append(article)
    return eligible, expired


def retain_classified_discovery_article(article: Mapping[str, Any]) -> bool:
    """Drop GDELT query noise while retaining causal or audit-worthy context."""

    if str(article.get("source_kind") or "").lower() != "gdelt":
        return True
    if article.get("relevant"):
        return True
    # ``context_only`` is deliberately broad for ordinary subscribed feeds so
    # that neutral source material remains available for later research. GDELT
    # is discovery search, however, and a broad query can return local news,
    # games, or corporate stories merely because an incidental word matched.
    # Require an actual macro/market term or an identified FX currency before
    # retaining a non-directional discovery row.
    return bool(
        article.get("context_only")
        and (
            safe_float(article.get("market_relevance_count"), 0.0) > 0.0
            or article.get("currencies")
        )
    )


def ledger_row(article: Mapping[str, Any]) -> dict[str, Any]:
    scheduled_utc = clean_text(article.get("scheduled_utc"))
    return {
        "watch_id": article.get("topic_id") or article.get("event_id"),
        "status": "active",
        "event_utc": scheduled_utc or article.get("published_utc"),
        "scheduled_utc": scheduled_utc,
        "timing_precision": clean_text(article.get("timing_precision")),
        "event_time_basis": clean_text(article.get("event_time_basis")),
        "clock_semantics": clean_text(article.get("clock_semantics")),
        "independent_domestic_event": int(
            bool(article.get("independent_domestic_event"))
        ),
        "linked_policy_factor": int(bool(article.get("linked_policy_factor"))),
        "published_utc": article.get("published_utc"),
        "first_known_utc": article.get("causal_known_utc")
        or article.get("first_seen_utc"),
        "first_seen_utc": article.get("first_seen_utc"),
        "updated_utc": article.get("last_seen_utc"),
        "last_seen_utc": article.get("last_seen_utc"),
        "headline": article.get("headline"),
        "summary": article.get("summary"),
        "category": article.get("category"),
        "direct_currencies": ",".join(
            article.get("direct_currencies") or []
        ),
        "currencies": ",".join(article.get("currencies") or []),
        "pair_hints": "",
        "directional_bias": json.dumps(
            article.get("directional_bias") or {},
            sort_keys=True,
        ),
        "severity": article.get("severity"),
        "movement_potential": article.get("movement_potential"),
        "scope": article.get("scope"),
        "source_name": article.get("source_name"),
        "source_id": article.get("source_id"),
        "source_kind": article.get("source_kind"),
        "source_quality": article.get("source_quality"),
        "source_direct": int(bool(article.get("source_direct"))),
        "retrieval_via": article.get("retrieval_via"),
        "source_role": article.get("source_role"),
        "publisher_url": article.get("publisher_url"),
        "source_currencies": ",".join(article.get("source_currencies") or []),
        "event_series_id": article.get("event_series_id"),
        "event_name": article.get("event_name"),
        "event_country": article.get("event_country"),
        "source_contract_id": article.get("source_contract_id"),
        "source_cohort_id": article.get("source_cohort_id"),
        "material_content_sha256": article.get("material_content_sha256"),
        "release_time_rule_observed_sha256": article.get(
            "release_time_rule_observed_sha256"
        ),
        "release_time_rule_archive_name": article.get(
            "release_time_rule_archive_name"
        ),
        "release_rule_bytes_verified": int(
            article.get("release_rule_bytes_verified") is True
        ),
        "source_url": article.get("source_url"),
        "source_verified": int(bool(article.get("source_verified"))),
        "relevant": int(bool(article.get("relevant"))),
        "directional_publish_eligible": int(
            bool(article.get("directional_publish_eligible"))
        ),
        "currency_scores": json.dumps(
            article.get("currency_scores") or {}, sort_keys=True
        ),
        "pair_bias": json.dumps(article.get("pair_bias") or {}, sort_keys=True),
        "direction": article.get("direction") or "",
        "actual": article.get("actual"),
        "actual_value": article.get("actual_value"),
        "consensus": article.get("consensus"),
        "consensus_value": article.get("consensus_value"),
        "corroboration_count": int(article.get("corroboration_count") or 0),
        "pre_window_minutes": 60 if scheduled_utc else 0,
        "post_window_minutes": article.get("post_window_minutes"),
        "source_type": "local_no_gpt_news",
        "local_sentiment_score": article.get("generic_sentiment_score"),
        "directional_confidence": article.get("directional_confidence"),
        "topic_tags": json.dumps(article.get("topic_tags") or [], sort_keys=True),
        "intervention_status": article.get("intervention_status") or "",
        "reports_prior_market_move": int(
            bool(article.get("reports_prior_market_move"))
        ),
        "context_only": int(bool(article.get("context_only"))),
        "research_only": 1,
        "directional_research_only": int(
            bool(article.get("directional_research_only"))
        ),
        "execution_eligible": 0,
        "can_place_orders": 0,
    }


def write_ledger(
    path: Path,
    articles: Sequence[Mapping[str, Any]],
) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=LEDGER_FIELDS)
            writer.writeheader()
            for article in articles:
                writer.writerow(ledger_row(article))
        try:
            if path.exists() and temporary.read_bytes() == path.read_bytes():
                return False
        except OSError:
            pass
        for attempt in range(8):
            try:
                os.replace(temporary, path)
                return True
            except PermissionError:
                if attempt == 7:
                    raise
                time.sleep(0.05 * (attempt + 1))
    finally:
        temporary.unlink(missing_ok=True)
    return False


def split_instrument(instrument: str) -> tuple[str, str]:
    parts = str(instrument or "").upper().split("_", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else ("", "")


def article_source_identity(article: Mapping[str, Any]) -> str:
    for field in ("source_name", "domain", "source_id"):
        value = normalized_headline(article.get(field))
        if value:
            return value
    return "unknown"


def syndication_headline_key(article: Mapping[str, Any]) -> str:
    """Normalize presentation wrappers before testing story independence."""

    text = headline_content(
        article.get("headline"),
        publisher_name=article.get("source_name"),
    )
    # Redistributors commonly prepend a vertical/category label while keeping
    # the wire copy unchanged. That label and a publisher suffix must not turn
    # one story into apparent independent corroboration.
    text = re.sub(
        r"^(?:(?:business|world|financial|market|latest|international)\s+"
        r"news|news)\s*(?:\||:|-)\s*",
        "",
        text,
        flags=re.I,
    )
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def independent_source_representatives(
    articles: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Mapping[str, Any]], int]:
    """Return one representative per independently worded publisher group.

    Separate publishers carrying the exact same normalized headline are most
    often redistributing one wire/syndicated story.  They are useful coverage,
    but not independent corroboration.  Union publishers connected by an exact
    headline while still collapsing repeat articles from the same publisher.
    Paraphrased reports from separate publishers remain independent.
    """

    publisher_ids = {article_source_identity(article) for article in articles}
    parent = {identity: identity for identity in publisher_ids}

    def find(identity: str) -> str:
        while parent[identity] != identity:
            parent[identity] = parent[parent[identity]]
            identity = parent[identity]
        return identity

    def union(left: str, right: str) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root == right_root:
            return
        low, high = sorted((left_root, right_root))
        parent[high] = low

    publishers_by_headline: dict[str, set[str]] = defaultdict(set)
    for article in articles:
        headline_key = syndication_headline_key(article)
        if headline_key:
            publishers_by_headline[headline_key].add(
                article_source_identity(article)
            )
    for publishers in publishers_by_headline.values():
        ordered = sorted(publishers)
        for publisher in ordered[1:]:
            union(ordered[0], publisher)

    representatives: dict[str, Mapping[str, Any]] = {}
    for article in articles:
        group = find(article_source_identity(article))
        previous = representatives.get(group)
        if previous is None or (
            bool(article.get("detail_enriched")),
            len(clean_text(article.get("summary"))),
            bool(article.get("source_verified")),
            safe_float(article.get("source_quality"), 0.0),
        ) > (
            bool(previous.get("detail_enriched")),
            len(clean_text(previous.get("summary"))),
            bool(previous.get("source_verified")),
            safe_float(previous.get("source_quality"), 0.0),
        ):
            representatives[group] = article
    return representatives, len(publisher_ids)


CLUSTER_ENTITY_MATCH_CATEGORIES = frozenset(
    {
        "risk_off_geopolitical_or_financial",
        "risk_on_deescalation",
        "commodity_shock",
    }
)


def _cluster_candidate_index_keys(
    article: Mapping[str, Any],
) -> set[tuple[str, ...]]:
    """Return every identity capable of matching the current group tail.

    ``cluster_articles`` historically searched every prior group in reverse
    creation order.  The retained corpus is now large enough for that exact
    search to dominate each collection cycle.  These keys only narrow the
    candidate set; the original full match predicate is still applied before
    a group is accepted.
    """

    keys: set[tuple[str, ...]] = set()
    headline_key = str(article.get("_headline_key") or "")
    if headline_key:
        keys.add(("headline", headline_key))
    signature = str(article.get("_cluster_signature") or "")
    if signature:
        keys.add(("signature", signature))
    category = str(article.get("category") or "")
    if category in CLUSTER_ENTITY_MATCH_CATEGORIES:
        action = str(article.get("topic_action") or "")
        for entity in article.get("topic_entities") or ():
            value = str(entity or "")
            if value:
                keys.add(("entity", category, action, value))
    return keys


def _cluster_candidate_groups(
    candidates: Sequence[dict[str, Any]],
) -> list[list[dict[str, Any]]]:
    """Group candidates with indexed lookup and legacy-equivalent ordering."""

    groups: list[list[dict[str, Any]]] = []
    group_ids_by_key: dict[tuple[str, ...], set[int]] = defaultdict(set)

    def index_tail(group_id: int, article: Mapping[str, Any]) -> None:
        for key in _cluster_candidate_index_keys(article):
            group_ids_by_key[key].add(group_id)

    def unindex_tail(group_id: int, article: Mapping[str, Any]) -> None:
        for key in _cluster_candidate_index_keys(article):
            group_ids = group_ids_by_key.get(key)
            if group_ids is None:
                continue
            group_ids.discard(group_id)
            if not group_ids:
                group_ids_by_key.pop(key, None)

    for article in candidates:
        if article.get("topic_clustered"):
            groups.append([article])
            continue

        article_time = (
            parse_datetime(article.get("published_utc"))
            or parse_datetime(article.get("first_seen_utc"))
        )
        possible_group_ids: set[int] = set()
        for key in _cluster_candidate_index_keys(article):
            possible_group_ids.update(group_ids_by_key.get(key) or ())

        matched_group_id: int | None = None
        # Descending group id is exactly the former ``reversed(groups)``
        # precedence.  Candidate indexing changes cost, never selection.
        for group_id in sorted(possible_group_ids, reverse=True):
            prior = groups[group_id][-1]
            article_scheduled = clean_text(article.get("scheduled_utc"))
            prior_scheduled = clean_text(prior.get("scheduled_utc"))
            if (
                bool(article.get("structured_event"))
                or bool(prior.get("structured_event"))
            ) and article_scheduled != prior_scheduled:
                continue
            exact_match = bool(
                article.get("_headline_key")
                and article.get("_headline_key") == prior.get("_headline_key")
            )
            semantic_match = bool(
                article.get("_cluster_signature")
                and (
                    article.get("_cluster_signature")
                    == prior.get("_cluster_signature")
                    or (
                        str(article.get("category") or "")
                        in CLUSTER_ENTITY_MATCH_CATEGORIES
                        and str(article.get("category") or "")
                        == str(prior.get("category") or "")
                        and str(article.get("topic_action") or "")
                        == str(prior.get("topic_action") or "")
                        and bool(
                            set(article.get("topic_entities") or ())
                            & set(prior.get("topic_entities") or ())
                        )
                    )
                )
                and headlines_support_same_claim(
                    article.get("headline"),
                    prior.get("headline"),
                )
            )
            if not (exact_match or semantic_match):
                continue
            prior_time = (
                parse_datetime(prior.get("published_utc"))
                or parse_datetime(prior.get("first_seen_utc"))
            )
            if exact_match or (
                article_time is not None
                and prior_time is not None
                and abs((article_time - prior_time).total_seconds()) <= 18 * 3600
            ):
                matched_group_id = group_id
                break

        if matched_group_id is None:
            group_id = len(groups)
            groups.append([article])
            index_tail(group_id, article)
        else:
            prior = groups[matched_group_id][-1]
            unindex_tail(matched_group_id, prior)
            groups[matched_group_id].append(article)
            index_tail(matched_group_id, article)
    return groups


def cluster_articles(
    articles: Sequence[Mapping[str, Any]],
    *,
    as_of: dt.datetime | None = None,
) -> list[dict[str, Any]]:
    """Collapse paraphrases of one topic without counting polls as support."""

    guard_as_of = as_of if as_of is not None else utc_now()
    candidates: list[dict[str, Any]] = []
    for raw in articles:
        article = dict(raw)
        causal_known = causal_known_datetime(article)
        if as_of is not None and (causal_known is None or causal_known > as_of):
            continue
        if article.get("topic_clustered"):
            candidates.append(guard_causal_news_topic(
                enforce_directional_publication_invariants(article), None, as_of=guard_as_of
            ))
            continue
        headline_key = normalized_headline(article.get("headline"))
        signature = str(article.get("topic_signature") or "")
        if not signature:
            signature = f"headline|{headline_key}"
        article["_cluster_signature"] = signature
        article["_headline_key"] = headline_key
        candidates.append(article)

    candidates.sort(
        key=lambda row: (
            parse_datetime(row.get("published_utc"))
            or parse_datetime(row.get("first_seen_utc"))
            or dt.datetime.min.replace(tzinfo=UTC),
            str(row.get("event_id") or ""),
        )
    )
    groups = _cluster_candidate_groups(candidates)

    output: list[dict[str, Any]] = []
    for rows in groups:
        if len(rows) == 1 and rows[0].get("topic_clustered"):
            output.append(enforce_directional_publication_invariants(rows[0]))
            continue
        representative = max(
            rows,
            key=lambda article: (
                verified_source_provenance_rank(article),
                bool(article.get("detail_enriched")),
                bool(article.get("source_verified")),
                safe_float(article.get("source_quality"), 0.0),
                safe_float(article.get("directional_confidence"), 0.0),
            ),
        )
        result = dict(representative)
        source_representatives, publisher_source_count = (
            independent_source_representatives(rows)
        )
        distinct_sources = set(source_representatives)
        explicit_corroboration = max(
            (int(row.get("corroboration_count") or 0) for row in rows),
            default=0,
        )
        result["distinct_source_count"] = len(distinct_sources)
        result["publisher_source_count"] = publisher_source_count
        result["corroboration_count"] = max(
            explicit_corroboration,
            len(distinct_sources) - 1,
        )
        result["reports_prior_market_move"] = any(
            bool(row.get("reports_prior_market_move")) for row in rows
        )
        result["source_listing_bootstrap"] = any(
            bool(row.get("source_listing_bootstrap")) for row in rows
        )
        topic_first_known = min(
            (
                value
                for row in rows
                if (value := causal_known_datetime(row)) is not None
            ),
            default=None,
        )
        representative_detail_available = parse_datetime(
            result.get("detail_available_utc")
        )
        if representative_detail_available is not None:
            # The clustered artifact exposes the representative's enriched
            # figures/summary, so its usable time cannot precede the moment
            # that richer content was actually fetched, even when an earlier
            # calendar or headline row belongs to the same semantic topic.
            topic_first_known = max(
                candidate
                for candidate in (
                    topic_first_known,
                    representative_detail_available,
                )
                if candidate is not None
            )
        topic_first_published = min(
            (
                value
                for row in rows
                if (value := parse_datetime(row.get("published_utc"))) is not None
            ),
            default=None,
        )
        if topic_first_known is not None and topic_first_published is not None:
            topic_availability_lag = max(
                0.0,
                (topic_first_known - topic_first_published).total_seconds() / 60.0,
            )
        else:
            topic_availability_lag = min(
                (
                    safe_float(row.get("availability_lag_minutes"), math.inf)
                    for row in rows
                ),
                default=math.inf,
            )
        result["availability_lag_minutes"] = round(topic_availability_lag, 6)
        if not math.isfinite(result["availability_lag_minutes"]):
            result["availability_lag_minutes"] = 0.0
        result["forward_timeliness_limit_minutes"] = round(
            max(
                (
                    safe_float(row.get("forward_timeliness_limit_minutes"), 30.0)
                    for row in rows
                ),
                default=30.0,
            ),
            6,
        )
        result["forward_signal_timely"] = bool(
            not result["reports_prior_market_move"]
            and not result["source_listing_bootstrap"]
            and result["availability_lag_minutes"]
            <= result["forward_timeliness_limit_minutes"]
        )
        if str(result.get("category") or "") == "fx_intervention":
            result["intervention_status"] = aggregate_intervention_status(
                rows,
                distinct_source_count=len(distinct_sources),
            )
        result["syndicated_article_count"] = len(rows)
        result["duplicate_observation_count"] = sum(
            int(row.get("duplicate_observation_count") or 0)
            for row in rows
        )
        first_known = min(
            (
                value
                for row in rows
                if (value := causal_known_datetime(row)) is not None
            ),
            default=None,
        )
        if representative_detail_available is not None:
            first_known = max(
                candidate
                for candidate in (first_known, representative_detail_available)
                if candidate is not None
            )
        raw_first_seen = min(
            (
                value
                for row in rows
                if (value := parse_datetime(row.get("first_seen_utc"))) is not None
            ),
            default=first_known,
        )
        first_published = min(
            (
                value
                for row in rows
                if (value := parse_datetime(row.get("published_utc"))) is not None
            ),
            default=first_known,
        )
        last_seen = max(
            (
                value
                for row in rows
                if (value := parse_datetime(row.get("last_seen_utc"))) is not None
            ),
            default=first_known,
        )
        signature = str(
            result.get("topic_signature")
            or f"headline|{result.get('_headline_key') or normalized_headline(result.get('headline'))}"
        )
        scheduled_identity = clean_text(result.get("scheduled_utc"))
        identity_day = (
            scheduled_identity
            if bool(result.get("structured_event")) and scheduled_identity
            else iso_utc(first_published or first_known)[:10]
        )
        identity_claim = str(
            rows[0].get("_headline_key")
            or normalized_headline(rows[0].get("headline"))
        )
        if bool(result.get("structured_event")) and scheduled_identity:
            identity_claim = f"{identity_claim}|{scheduled_identity}"
        result["topic_id"] = stable_id(
            "topic",
            signature,
            identity_day,
            identity_claim,
        )
        result["topic_clustered"] = True
        result["topic_article_count"] = len(rows)
        result["article_event_ids"] = sorted(
            {
                str(row.get("event_id"))
                for row in rows
                if str(row.get("event_id") or "")
            }
        )
        result["story_cluster_id"] = stable_id(
            "story_cluster",
            signature,
            identity_day,
            identity_claim,
        )
        result["headline_variants"] = sorted(
            {
                clean_text(row.get("headline"))
                for row in rows
                if clean_text(row.get("headline"))
            }
        )[:20]
        result["first_seen_utc"] = (
            iso_utc(raw_first_seen) if raw_first_seen else ""
        )
        result["causal_known_utc"] = (
            iso_utc(first_known) if first_known else ""
        )
        result["published_utc"] = iso_utc(first_published) if first_published else ""
        result["last_seen_utc"] = iso_utc(last_seen) if last_seen else ""
        result["direct_currencies"] = sorted(
            {
                str(currency)
                for row in rows
                for currency in row.get("direct_currencies") or ()
            }
        )
        result["inferred_currencies"] = sorted(
            {
                str(currency)
                for row in rows
                for currency in row.get("inferred_currencies") or ()
            }
            - set(result["direct_currencies"])
        )
        result["topic_tags"] = sorted(
            {
                str(tag)
                for row in rows
                for tag in row.get("topic_tags") or ()
            }
        )
        scored_representatives = {
            source: row
            for source, row in source_representatives.items()
            if row.get("currency_scores")
        }
        directional_representatives = {
            source: row
            for source, row in scored_representatives.items()
            if not bool(row.get("detail_enrichment_research_only"))
            and not bool(row.get("directional_research_only"))
        }

        def aggregate_scores(
            representatives: Mapping[str, Mapping[str, Any]],
        ) -> dict[str, float]:
            numerators: dict[str, float] = defaultdict(float)
            denominators: dict[str, float] = defaultdict(float)
            for row in representatives.values():
                source_weight = max(
                    0.1, safe_float(row.get("source_quality"), 0.65)
                )
                for currency, value in (row.get("currency_scores") or {}).items():
                    numerators[str(currency)] += source_weight * safe_float(value)
                    denominators[str(currency)] += source_weight
            return {
                currency: round(
                    clamp(numerators[currency] / denominators[currency]), 6
                )
                for currency in sorted(numerators)
                if denominators[currency] > 0
            }

        research_only_scores = aggregate_scores(scored_representatives)
        result["currency_scores"] = aggregate_scores(directional_representatives)
        vendor_rows = [
            row
            for row in source_representatives.values()
            if optional_float(row.get("vendor_sentiment_score")) is not None
            or row.get("vendor_currency_sentiment")
        ]
        vendor_overall_values = [
            optional_float(row.get("vendor_sentiment_score"))
            for row in vendor_rows
            if optional_float(row.get("vendor_sentiment_score")) is not None
        ]
        vendor_currency_numerators: dict[str, float] = defaultdict(float)
        vendor_currency_denominators: dict[str, float] = defaultdict(float)
        for row in vendor_rows:
            source_weight = max(0.1, safe_float(row.get("source_quality"), 0.65))
            for currency, score in (row.get("vendor_currency_sentiment") or {}).items():
                vendor_currency_numerators[str(currency)] += source_weight * clamp(
                    safe_float(score)
                )
                vendor_currency_denominators[str(currency)] += source_weight
        result["vendor_sentiment_observation_count"] = len(vendor_rows)
        result["vendor_sentiment_source_ids"] = sorted(
            {
                str(row.get("source_id"))
                for row in vendor_rows
                if str(row.get("source_id") or "")
            }
        )
        result["vendor_sentiment_mean"] = (
            round(sum(vendor_overall_values) / len(vendor_overall_values), 6)
            if vendor_overall_values
            else None
        )
        result["vendor_sentiment_dispersion"] = (
            round(max(vendor_overall_values) - min(vendor_overall_values), 6)
            if vendor_overall_values
            else None
        )
        result["vendor_currency_sentiment"] = {
            currency: round(
                vendor_currency_numerators[currency]
                / vendor_currency_denominators[currency],
                6,
            )
            for currency in sorted(vendor_currency_numerators)
            if vendor_currency_denominators[currency] > 0
        }
        local_research_scores = research_only_scores or result["currency_scores"]
        result["vendor_local_direction_state"] = {
            currency: (
                "vendor_only"
                if currency not in local_research_scores
                else "neutral"
                if abs(safe_float(vendor_score)) < 0.05
                or abs(safe_float(local_research_scores.get(currency))) < 0.05
                else "aligned"
                if safe_float(vendor_score)
                * safe_float(local_research_scores.get(currency))
                > 0
                else "conflicted"
            )
            for currency, vendor_score in result["vendor_currency_sentiment"].items()
        }
        result["vendor_sentiment_research_only"] = True
        result["vendor_sentiment_execution_eligible"] = False
        result["directional_candidate_source_count"] = len(
            directional_representatives
        )
        result["directional_verified_source"] = any(
            bool(row.get("source_verified"))
            for row in directional_representatives.values()
        )
        result["directional_bias"] = {
            currency: "BULLISH" if value > 0 else "BEARISH"
            for currency, value in result["currency_scores"].items()
            if value != 0
        }
        if str(result.get("category") or "") == "fx_intervention":
            status = str(result.get("intervention_status") or "context")
            signed = sum(
                safe_float(result["currency_scores"].get(currency))
                for currency in result.get("direct_currencies") or ()
            )
            direction = (
                "strengthening"
                if signed > 0
                else "weakening"
                if signed < 0
                else "mixed"
            )
            result["topic_action"] = f"{status}_{direction}"
            result["topic_tags"] = sorted(
                set(result.get("topic_tags") or ())
                | {
                    f"#{str(currency).lower()}_{status}_{direction}"
                    for currency in result.get("direct_currencies") or ()
                }
            )
        result["source_verified"] = any(
            bool(row.get("source_verified")) for row in rows
        )
        result["source_direct"] = any(bool(row.get("source_direct")) for row in rows)
        release_grade_required = (
            str(result.get("category") or "")
            in CORROBORATION_REQUIRED_CATEGORIES
            or any(
                bool(row.get("directional_corroboration_required"))
                for row in rows
            )
        )
        corroborated_secondary = (
            not result["directional_verified_source"]
            and len(directional_representatives) >= 2
        )
        result["directional_publish_eligible"] = bool(
            result.get("currency_scores")
            and result.get("forward_signal_timely")
            and (
                result["directional_verified_source"]
                or corroborated_secondary
                or not release_grade_required
            )
        )
        result["directional_source_grade"] = (
            "verified_primary_or_publisher"
            if result["directional_verified_source"]
            else "corroborated_secondary"
            if corroborated_secondary
            else "secondary_requires_corroboration"
            if release_grade_required
            else "contextual_directional"
        )
        if research_only_scores and not result.get("currency_scores"):
            # Enriched documents and explicitly research-only sources retain
            # their semantics for shadow analysis, but topic aggregation must
            # never upgrade them into a publishable forward factor.
            result["research_currency_scores"] = research_only_scores
            result["directional_bias"] = {}
            result["directional_publish_eligible"] = False
            result["directional_evidence"] = False
            result["relevant"] = False
            result["context_only"] = True
            result["context_reason"] = (
                "detail_enrichment_or_directional_research_only_context"
            )
        if result.get("reports_prior_market_move"):
            # Reported price action is valuable historical reaction evidence,
            # but publishing it as a forward signal creates look-ahead leakage.
            if result.get("currency_scores"):
                result["research_currency_scores"] = dict(
                    result.get("currency_scores") or {}
                )
            result["currency_scores"] = {}
            result["directional_bias"] = {}
            result["directional_publish_eligible"] = False
            result["directional_evidence"] = False
            result["relevant"] = False
            result["context_only"] = True
            result["context_reason"] = "reported_market_move_context"
        elif (
            not result.get("forward_signal_timely")
            and result.get("currency_scores")
        ):
            # A direction discovered after a meaningful fraction of its
            # reaction horizon is retrospective evidence, even when the
            # headline does not literally describe the observed price move.
            result["research_currency_scores"] = dict(
                result.get("currency_scores") or {}
            )
            result["currency_scores"] = {}
            result["directional_bias"] = {}
            result["directional_publish_eligible"] = False
            result["directional_evidence"] = False
            result["relevant"] = False
            result["context_only"] = True
            result["context_reason"] = (
                "source_listing_bootstrap_context"
                if result.get("source_listing_bootstrap")
                else "source_late_for_reaction_horizon"
            )
        elif (
            release_grade_required
            and not result["directional_publish_eligible"]
            and result.get("currency_scores")
        ):
            # Preserve the classifier output for research and retrospective
            # scoring, but do not publish an uncorroborated secondary macro
            # headline as an active directional FX event.
            result["research_currency_scores"] = dict(
                result.get("currency_scores") or {}
            )
            result["currency_scores"] = {}
            result["directional_bias"] = {}
            result["directional_evidence"] = False
            result["relevant"] = False
            result["context_only"] = True
            result["context_reason"] = (
                "secondary_uncorroborated_macro_context"
                if str(result.get("category") or "")
                in RELEASE_GRADE_CATEGORIES
                else "secondary_uncorroborated_directional_context"
            )
        result["source_ids"] = sorted(
            {
                str(row.get("source_id"))
                for row in rows
                if str(row.get("source_id") or "")
            }
        )
        result["source_names"] = sorted(
            {
                str(row.get("source_name"))
                for row in rows
                if str(row.get("source_name") or "")
            }
        )
        result.pop("_cluster_signature", None)
        result.pop("_headline_key", None)
        output.append(guard_causal_news_topic(
            enforce_directional_publication_invariants(result), rows, as_of=guard_as_of
        ))
    output.sort(
        key=lambda article: (
            str(article.get("published_utc") or ""),
            str(article.get("event_id") or ""),
        )
    )
    return output


def cluster_context_articles(
    articles: Sequence[Mapping[str, Any]],
    *,
    as_of: dt.datetime,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Publish a compact context view while preserving every database row.

    The retained context table is intentionally lossless. The UI/audit artifact
    is not: it should show distinct claims instead of hundreds of syndicated
    paraphrases. Restrict semantic clustering to its displayed tail so runtime
    remains bounded, then merge any groups that resolve to the same stable
    topic ID.
    """

    window = [dict(row) for row in articles[-max(1, int(limit)):]]
    for article in window:
        article["_context_original_topic_signature"] = str(
            article.get("topic_signature") or ""
        )
        if not bool(article.get("structured_event")):
            # Entity extraction can vary between syndicated headlines (for
            # example ``US`` appears in one tariff rewrite but not another).
            # Context-only rows carry no tradable score, so same-category
            # claim overlap is the appropriate bounded display deduplicator.
            article["topic_signature"] = (
                f"context|{str(article.get('category') or 'market_news')}"
            )
    clustered = cluster_articles(window, as_of=as_of)
    for article in clustered:
        original_signature = article.pop(
            "_context_original_topic_signature", ""
        )
        if original_signature:
            article["topic_signature"] = original_signature
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for index, article in enumerate(clustered):
        key = str(
            article.get("topic_id")
            or article.get("event_id")
            or f"context:{index}"
        )
        grouped[key].append(article)

    output: list[dict[str, Any]] = []
    for topic_id, rows in grouped.items():
        representative = max(rows, key=topic_history_rank)
        result = dict(representative)
        article_event_ids = sorted(
            {
                str(value)
                for row in rows
                for value in (
                    row.get("article_event_ids")
                    or [row.get("event_id")]
                )
                if str(value or "")
            }
        )
        headline_variants = sorted(
            {
                str(value)
                for row in rows
                for value in (
                    row.get("headline_variants")
                    or [row.get("headline")]
                )
                if str(value or "")
            }
        )
        source_ids = sorted(
            {
                str(value)
                for row in rows
                for value in (row.get("source_ids") or [row.get("source_id")])
                if str(value or "")
            }
        )
        source_names = sorted(
            {
                str(value)
                for row in rows
                for value in (row.get("source_names") or [row.get("source_name")])
                if str(value or "")
            }
        )
        result.update(
            {
                "topic_id": topic_id,
                "topic_clustered": True,
                "topic_article_count": len(article_event_ids),
                "semantic_context_duplicate_count": max(
                    0, len(article_event_ids) - 1
                ),
                "article_event_ids": article_event_ids,
                "headline_variants": headline_variants,
                "source_ids": source_ids,
                "source_names": source_names,
                "distinct_source_count": len(source_names),
                "corroboration_count": max(0, len(source_names) - 1),
                "first_seen_utc": min(
                    str(row.get("first_seen_utc") or "9999") for row in rows
                ),
                "causal_known_utc": min(
                    str(
                        row.get("causal_known_utc")
                        or row.get("first_seen_utc")
                        or "9999"
                    )
                    for row in rows
                ),
                "last_seen_utc": max(
                    str(row.get("last_seen_utc") or "") for row in rows
                ),
                "published_utc": min(
                    str(row.get("published_utc") or "9999") for row in rows
                ),
                "context_only": True,
                "relevant": False,
                "execution_eligible": False,
            }
        )
        output.append(result)
    output.sort(
        key=lambda row: (
            str(row.get("published_utc") or ""),
            str(row.get("topic_id") or ""),
        )
    )
    return output


def collection_observation_time(local_now, *, clock_integrity_path=None):
    """Route every production observation to the explicit run clock, if set.

    Omitting the path retains the original call and its default clock contract.
    Passing a path selects an input; it does not inject or relax attestation.
    """
    if clock_integrity_path is None:
        return normalized_observation_time(local_now)
    return normalized_observation_time(local_now, clock_integrity_path=clock_integrity_path)


def refresh_pair_aggregation_clock(
    previous_cutoff: dt.datetime, *, now: dt.datetime | None = None,
    clock_integrity_path: Path | None = None
) -> tuple[dt.datetime, dict[str, Any]]:
    """Sample an attested clock after input reads, before fresh aggregation.

    Explicit clocks retain the existing deterministic offline test interface.
    Production callers must re-evaluate the actual inputs at this cutoff.
    """
    if now is not None:
        if now.tzinfo is None or now < previous_cutoff:
            raise ValueError("pair_aggregation_clock_invalid")
        return now, {"source": "explicit_offline_clock", "trusted": False}
    cutoff, attestation = collection_observation_time(utc_now(), clock_integrity_path=clock_integrity_path)
    if cutoff < previous_cutoff or not prospective_clock_attestation(attestation):
        raise ValueError("pair_aggregation_clock_untrusted_or_regressed")
    return cutoff, attestation


def build_pair_scores(
    articles: Sequence[Mapping[str, Any]],
    instruments: Sequence[str],
    *,
    as_of: dt.datetime,
) -> dict[str, Any]:
    active: list[dict[str, Any]] = []
    for article in cluster_articles(articles, as_of=as_of):
        first_seen = parse_datetime(article.get("first_seen_utc"))
        causal_known = causal_known_datetime(article)
        published = parse_datetime(article.get("published_utc"))
        if (
            first_seen is None
            or causal_known is None
            or published is None
            or causal_known > as_of
        ):
            continue
        publication_age_minutes = max(
            0.0, (as_of - published).total_seconds() / 60.0
        )
        # Forward causality starts only after both local arrival and the
        # source-reported publication boundary.
        causal_age_minutes = max(
            0.0, (as_of - causal_known).total_seconds() / 60.0
        )
        # Availability gates admission; later computation does not renew the
        # age or reaction phase of the underlying story/material evidence.
        evidence_known = parse_datetime(article.get("source_evidence_available_utc")) or causal_known
        evidence_age_minutes = max(0.0, (as_of - evidence_known).total_seconds() / 60.0)
        post_window = max(1.0, safe_float(article.get("post_window_minutes"), 180.0))
        if publication_age_minutes > post_window:
            continue
        decay = math.exp(
            -math.log(2.0)
            * evidence_age_minutes
            / max(30.0, post_window / 2.0)
        )
        weight = (
            decay
            * safe_float(article.get("source_quality"), 0.65)
            * max(0.10, safe_float(article.get("directional_confidence"), 0.0))
            * min(
                1.35,
                1.0
                + 0.10
                * math.log1p(max(0, int(article.get("corroboration_count") or 0))),
            )
        )
        reaction_horizon = max(
            1.0,
            safe_float(
                article.get("estimated_reaction_horizon_minutes"),
                estimated_news_reaction_horizon_minutes(article.get("category")),
            ),
        )
        active.append(
            {
                **article,
                "_age_minutes": publication_age_minutes,
                "_causal_age_minutes": causal_age_minutes,
                "_source_evidence_age_minutes": evidence_age_minutes,
                "_weight": weight,
                "_reaction_horizon_minutes": reaction_horizon,
                "_remaining_relevance_minutes": max(
                    0.0, post_window - publication_age_minutes
                ),
                "_reaction_phase": (
                    "initial_reaction"
                    if evidence_age_minutes <= reaction_horizon
                    else "continuation_context"
                ),
            }
        )

    pairs: dict[str, Any] = {}
    quality_counts: Counter[str] = Counter()
    for instrument in instruments:
        base, quote = split_instrument(instrument)
        numerator = 0.0
        denominator = 0.0
        contributors: list[dict[str, Any]] = []
        base_direct_topics: set[str] = set()
        quote_direct_topics: set[str] = set()
        base_inferred_topics: set[str] = set()
        quote_inferred_topics: set[str] = set()
        global_topics: set[str] = set()
        currency_exposure_groups: set[str] = set()
        for article in active:
            scores = article.get("currency_scores") or {}
            research_scores = article.get("research_currency_scores") or scores
            direct_currencies = set(article.get("direct_currencies") or ())
            inferred_currencies = set(article.get("inferred_currencies") or ())
            topic_id = str(article.get("topic_id") or article.get("event_id") or "")
            pair_related = bool(
                base in scores
                or quote in scores
                or base in direct_currencies
                or quote in direct_currencies
                or article.get("scope") == "all_pairs"
            )
            if not pair_related:
                continue
            base_score = safe_float(scores.get(base), 0.0)
            quote_score = safe_float(scores.get(quote), 0.0)
            research_pair_value = safe_float(
                research_scores.get(base), 0.0
            ) - safe_float(research_scores.get(quote), 0.0)
            forward_pair_value = base_score - quote_score
            forward_pair_eligible = bool(
                article.get("forward_signal_timely", True)
                and not article.get("reports_prior_market_move")
                and safe_float(article.get("_causal_age_minutes"), float("inf"))
                <= safe_float(article.get("_reaction_horizon_minutes"), 0.0)
                # Timeliness alone is not a direction.  Context-only topics
                # retain a latent research score, but an empty publishable
                # score must remain an explicit no-forward-direction reject.
                and abs(forward_pair_value) > 0.0
            )
            pair_value = forward_pair_value if forward_pair_eligible else 0.0
            weight = safe_float(article.get("_weight"), 0.0)
            direct_leg_count = int(base in direct_currencies) + int(
                quote in direct_currencies
            )
            article_exposure_groups = sorted(
                f"{currency}:{'LONG' if safe_float(scores.get(currency)) > 0 else 'SHORT'}"
                for currency in direct_currencies
                if abs(safe_float(scores.get(currency))) > 0
            )
            currency_exposure_groups.update(article_exposure_groups)
            evidence_weight = (
                1.0
                if direct_leg_count == 2
                else 0.70
                if direct_leg_count == 1
                else 0.40
                if article.get("scope") == "all_pairs"
                else 0.30
            )
            if pair_value != 0.0:
                if base in direct_currencies:
                    base_direct_topics.add(topic_id)
                if quote in direct_currencies:
                    quote_direct_topics.add(topic_id)
                if base in inferred_currencies or (
                    base in scores and base not in direct_currencies
                ):
                    base_inferred_topics.add(topic_id)
                if quote in inferred_currencies or (
                    quote in scores and quote not in direct_currencies
                ):
                    quote_inferred_topics.add(topic_id)
                if article.get("scope") == "all_pairs":
                    global_topics.add(topic_id)
                numerator += weight * evidence_weight * pair_value
                denominator += weight * evidence_weight
            contributors.append(
                {
                    "topic_id": topic_id,
                    "topic_tags": article.get("topic_tags") or [],
                    "article_count": int(article.get("topic_article_count") or 1),
                    "distinct_source_count": int(
                        article.get("distinct_source_count") or 1
                    ),
                    "headline": article.get("headline"),
                    "published_utc": article.get("published_utc"),
                    "first_seen_utc": article.get("first_seen_utc"),
                    "causal_known_utc": article.get("causal_known_utc"),
                    "age_minutes": round(safe_float(article.get("_age_minutes")), 3),
                    "causal_age_minutes": round(
                        safe_float(article.get("_causal_age_minutes")), 3
                    ),
                    "estimated_reaction_horizon_minutes": int(
                        safe_float(article.get("_reaction_horizon_minutes"), 60.0)
                    ),
                    "estimated_reaction_horizon_sec": int(
                        60.0
                        * safe_float(article.get("_reaction_horizon_minutes"), 60.0)
                    ),
                    "estimated_reaction_horizon_label": news_horizon_label(
                        article.get("_reaction_horizon_minutes")
                    ),
                    "reaction_horizon_method": str(
                        article.get("reaction_horizon_method") or "category_prior_v1"
                    ),
                    "relevance_window_minutes": int(
                        safe_float(article.get("post_window_minutes"), 180.0)
                    ),
                    "remaining_relevance_minutes": round(
                        safe_float(article.get("_remaining_relevance_minutes")), 3
                    ),
                    "reaction_phase": str(article.get("_reaction_phase") or ""),
                    "category": article.get("category"),
                    "pair_score": round(pair_value, 6),
                    "research_pair_score": round(research_pair_value, 6),
                    "forward_pair_eligible": forward_pair_eligible,
                    "weight": round(weight, 6),
                    "evidence_weight": evidence_weight,
                    "base_direct": base in direct_currencies,
                    "quote_direct": quote in direct_currencies,
                    "source_name": article.get("source_name"),
                    "source_ids": article.get("source_ids")
                    or [article.get("source_id")],
                    "source_url": article.get("source_url"),
                    "source_verified": bool(article.get("source_verified")),
                    "source_direct": bool(article.get("source_direct")),
                    "intervention_status": article.get("intervention_status"),
                    "reports_prior_market_move": bool(
                        article.get("reports_prior_market_move")
                    ),
                    "availability_lag_minutes": safe_float(
                        article.get("availability_lag_minutes")
                    ),
                    "forward_timeliness_limit_minutes": safe_float(
                        article.get("forward_timeliness_limit_minutes")
                    ),
                    "forward_signal_timely": bool(
                        article.get("forward_signal_timely")
                    ),
                    "currency_exposure_groups": article_exposure_groups,
                    "currency_basket_ids": [
                        stable_id("currency_basket", topic_id, group)
                        for group in article_exposure_groups
                    ],
                }
            )
        score = clamp(numerator / denominator) if denominator > 0 else 0.0
        direction = "LONG" if score >= 0.12 else "SHORT" if score <= -0.12 else "NEUTRAL"
        if base_direct_topics and quote_direct_topics:
            evidence_quality = "TWO_SIDED_DIRECT"
            confidence_cap = 0.90
        elif base_direct_topics or quote_direct_topics:
            evidence_quality = "ONE_SIDED_DIRECT"
            confidence_cap = 0.55
        elif global_topics:
            evidence_quality = "GLOBAL_TOPIC_PROXY"
            confidence_cap = 0.30
        elif base_inferred_topics or quote_inferred_topics:
            evidence_quality = "INFERRED_ONLY"
            confidence_cap = 0.20
        else:
            evidence_quality = "NO_CURRENT_EVIDENCE"
            confidence_cap = 0.0
        quality_counts[evidence_quality] += 1
        directional_contributors = sum(
            abs(safe_float(row.get("pair_score"))) > 0 for row in contributors
        )
        context_directional_contributors = sum(
            abs(safe_float(row.get("research_pair_score"))) > 0
            for row in contributors
        )
        raw_confidence = clamp(
            abs(score) * min(1.0, 0.45 + 0.20 * directional_contributors),
            0.0,
            1.0,
        )
        confidence = min(raw_confidence, confidence_cap)
        contributors.sort(
            key=lambda row: abs(safe_float(row.get("pair_score")) * safe_float(row.get("weight"))),
            reverse=True,
        )
        directional_horizon_rows = [
            row
            for row in contributors
            if abs(safe_float(row.get("pair_score"))) > 0.0
        ]
        dominant_horizon = (
            directional_horizon_rows[0]
            if directional_horizon_rows
            else contributors[0]
            if contributors
            else {}
        )
        pairs[instrument] = {
            "instrument": instrument,
            "as_of_utc": iso_utc(as_of),
            "direction": direction,
            "score": round(score, 6),
            "confidence": round(confidence, 6),
            "raw_confidence": round(raw_confidence, 6),
            "confidence_cap": confidence_cap,
            "evidence_quality": evidence_quality,
            "active_event_count": len(contributors),
            "directional_event_count": directional_contributors,
            "context_directional_event_count": context_directional_contributors,
            "estimated_reaction_horizon_sec": int(
                safe_float(dominant_horizon.get("estimated_reaction_horizon_sec"))
            ),
            "estimated_reaction_horizon_label": str(
                dominant_horizon.get("estimated_reaction_horizon_label") or ""
            ),
            "reaction_horizon_method": str(
                dominant_horizon.get("reaction_horizon_method") or ""
            ),
            "reaction_phase": str(dominant_horizon.get("reaction_phase") or ""),
            "remaining_relevance_minutes": safe_float(
                dominant_horizon.get("remaining_relevance_minutes")
            ),
            "leg_evidence": {
                "base": {
                    "currency": base,
                    "direct_topic_count": len(base_direct_topics),
                    "inferred_topic_count": len(base_inferred_topics),
                },
                "quote": {
                    "currency": quote,
                    "direct_topic_count": len(quote_direct_topics),
                    "inferred_topic_count": len(quote_inferred_topics),
                },
                "global_topic_count": len(global_topics),
            },
            "events": contributors[:8],
            "currency_exposure_groups": sorted(currency_exposure_groups),
            "currency_basket_policy": {
                "shared_total_risk_fraction": 1.0,
                "max_legs": 3,
                "suggested_leg_risk_fraction": round(1.0 / 3.0, 6),
                "research_only": True,
                "execution_eligible": False,
            },
            "research_only": True,
            "execution_eligible": False,
            "matrix_weight": 0.0,
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": iso_utc(),
        "as_of_utc": iso_utc(as_of),
        "policy": {
            "research_only": True,
            "execution_eligible": False,
            "matrix_weight": 0.0,
            "promotion_requirement": "chronological validation before any execution weight",
        },
        "active_article_count": len(active),
        "instrument_count": len(instruments),
        "coverage": {
            "pair_count": len(pairs),
            "currency_count": len(
                {
                    currency
                    for instrument in instruments
                    for currency in split_instrument(instrument)
                    if currency
                }
            ),
            "evidence_quality_counts": dict(sorted(quality_counts.items())),
            "directional_pair_count": sum(
                row.get("direction") != "NEUTRAL" for row in pairs.values()
            ),
            "direct_directional_pair_count": sum(
                row.get("direction") != "NEUTRAL"
                and row.get("evidence_quality")
                in {"ONE_SIDED_DIRECT", "TWO_SIDED_DIRECT"}
                for row in pairs.values()
            ),
            "global_proxy_directional_pair_count": sum(
                row.get("direction") != "NEUTRAL"
                and row.get("evidence_quality") == "GLOBAL_TOPIC_PROXY"
                for row in pairs.values()
            ),
            "all_pairs_emitted": len(pairs) == len(instruments),
        },
        "pairs": pairs,
    }


def _process_is_running(pid: Any) -> bool:
    try:
        process_id = int(pid)
    except (TypeError, ValueError):
        return False
    if process_id <= 0:
        return False
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def request_event_catalog_refresh(
    *,
    output_root: Path,
    ledger_path: Path = DEFAULT_LEDGER,
    max_age_seconds: int = EVENT_CATALOG_REFRESH_SECONDS,
) -> dict[str, Any]:
    """Start a non-blocking event-catalog refresh when its context is stale."""

    context_path = output_root / "latest_pair_news_context.json"
    manifest_path = output_root / "manifest.json"
    age_seconds = math.inf
    context_modified = 0.0
    try:
        context_modified = context_path.stat().st_mtime
        age_seconds = max(0.0, time.time() - context_modified)
    except OSError:
        pass
    ledger_newer_than_context = False
    try:
        ledger_newer_than_context = ledger_path.stat().st_mtime > context_modified
    except OSError:
        pass
    if (
        age_seconds <= max(1, int(max_age_seconds))
        and not ledger_newer_than_context
    ):
        manifest = load_json(manifest_path, {})
        return {
            "status": "fresh",
            "age_seconds": round(age_seconds, 3),
            "ledger_newer_than_context": False,
            "generated_utc": manifest.get("generated_utc")
            if isinstance(manifest, dict)
            else None,
            "event_count": manifest.get("event_count")
            if isinstance(manifest, dict)
            else None,
            "pair_event_tag_count": manifest.get("pair_event_tag_count")
            if isinstance(manifest, dict)
            else None,
        }

    lock_path = output_root / ".sync.lock"
    stale_lock_recovered = False
    if lock_path.exists():
        lock_payload = load_json(lock_path, {})
        lock_pid = (
            lock_payload.get("pid") if isinstance(lock_payload, dict) else None
        )
        try:
            lock_age_seconds = max(0.0, time.time() - lock_path.stat().st_mtime)
        except OSError:
            lock_age_seconds = 0.0
        if (
            lock_age_seconds > EVENT_CATALOG_LOCK_STALE_SECONDS
            and not _process_is_running(lock_pid)
        ):
            try:
                lock_path.unlink()
                stale_lock_recovered = True
            except OSError:
                pass
        if lock_path.exists():
            return {
                "status": "refresh_running",
                "age_seconds": round(age_seconds, 3)
                if math.isfinite(age_seconds)
                else None,
                "ledger_newer_than_context": ledger_newer_than_context,
                "pid": lock_pid,
                "lock_age_seconds": round(lock_age_seconds, 3),
            }

    command = [
        sys.executable,
        str(ROOT / "oanda_news_event_tagger.py"),
        "--output-root",
        str(output_root),
        "--skip-significant-backfill",
        "--skip-movement-backfill",
        "--skip-sqlite",
    ]
    environment = os.environ.copy()
    environment["FOREX_ALLOW_LIVE"] = "0"
    environment["FOREX_LIVE_EXECUTE"] = "0"
    process = subprocess.Popen(
        command,
        cwd=str(ROOT),
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    result = {
        "status": "refresh_started",
        "age_seconds": round(age_seconds, 3)
        if math.isfinite(age_seconds)
        else None,
        "ledger_newer_than_context": ledger_newer_than_context,
        "pid": process.pid,
    }
    if stale_lock_recovered:
        result["stale_lock_recovered"] = True
    return result


def run_cycle(
    *,
    config_path: Path = DEFAULT_CONFIG,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    ledger_path: Path = DEFAULT_LEDGER,
    event_root: Path = DEFAULT_EVENT_ROOT,
    state_path: Path | None = None,
    clock_integrity_path: Path | None = None,
    coverage_root: Path | None = None,
    refresh_event_catalog: bool = True,
    force: bool = False,
    now: dt.datetime | None = None,
    progress_callback: Any = None,
) -> dict[str, Any]:
    emit_collector_progress(progress_callback, "starting_cycle")
    raw_current = now or utc_now()
    if now is None:
        current, observation_clock = collection_observation_time(raw_current, clock_integrity_path=clock_integrity_path)
    else:
        current = (
            raw_current.replace(tzinfo=UTC)
            if raw_current.tzinfo is None
            else raw_current.astimezone(UTC)
        )
        observation_clock = {
            "source": "explicit_cycle_time",
            "local_raw_utc": iso_utc(current),
            "normalized_utc": iso_utc(current),
            "status": "injected",
            "broker_clock_lead_sec": None,
            "sample_count": 0,
            "heartbeat_age_sec": None,
            "applied_offset_sec": 0.0,
            "normalized": False,
        }
    config = load_json(config_path, {})
    if not isinstance(config, dict):
        raise ValueError(f"invalid news source config: {config_path}")
    config = apply_source_config_lineage(config)
    policy = config.get("policy") or {}
    state_file = state_path or output_root / "collector_state_v1.json"
    state = load_json(state_file, {})
    if not isinstance(state, dict):
        state = {}
    if now is None and not prospective_clock_attestation(observation_clock):
        blocked = {
            "schema_version": SCHEMA_VERSION,
            "collector_contract_id": COLLECTOR_CONTRACT_ID,
            "collector_cohort_id": COLLECTOR_COHORT_ID,
            "generated_utc": iso_utc(raw_current),
            "status": "blocked_clock_integrity",
            "observation_clock": observation_clock,
            "policy": {
                "research_only": True,
                "execution_eligible": False,
                "openai_calls": 0,
            },
            "attempted_sources": 0,
            "configured_sources": len(config.get("sources") or []),
            "operational_sources": 0,
            "fetched_items": 0,
            "inserted_items": 0,
            "duplicate_items": 0,
            "database_opened": False,
            "prospective_evidence_written": False,
        }
        state.update(
            {
                "schema_version": SCHEMA_VERSION,
                "updated_utc": iso_utc(raw_current),
                "status": "blocked_clock_integrity",
                "last_cycle": blocked,
            }
        )
        atomic_write_json(state_file, state)
        atomic_write_json(output_root / "collector_latest_v1.json", blocked)
        return blocked
    source_states = state.get("sources")
    if not isinstance(source_states, dict):
        source_states = {}
    active_source_ids = {
        str(source.get("source_id"))
        for source in config.get("sources") or []
        if isinstance(source, dict) and source.get("source_id")
    }
    retired_source_states = sorted(set(source_states) - active_source_ids)
    source_states = {
        source_id: value
        for source_id, value in source_states.items()
        if source_id in active_source_ids
    }

    timeout_sec = max(2.0, safe_float(policy.get("request_timeout_sec"), 25.0))
    maximum_bytes = max(100_000, int(safe_float(policy.get("maximum_feed_bytes"), 8_000_000)))
    fetched: list[dict[str, Any]] = []
    classified_all: list[dict[str, Any]] = []
    classified: list[dict[str, Any]] = []
    retention_expired: list[dict[str, Any]] = []
    irrelevant_discovery_skipped = 0
    inserted = 0
    duplicates = 0
    incremental_topic_history_inserted = 0
    incremental_topic_history_updated = 0
    retention_days = max(1, int(safe_float(policy.get("retention_days"), 45)))
    retention_cutoff = current - dt.timedelta(days=retention_days)
    configured_sources = {
        str(row["source_id"]): row
        for row in config.get("sources") or []
        if isinstance(row, dict) and row.get("source_id")
    }
    database_path = output_root / "local_news_sentiment_v1.sqlite"
    emit_collector_progress(progress_callback, "opening_database")
    connection = process_database(database_path)
    def classification_clock_provider():
        observed, clock_evidence = (
            collection_observation_time(utc_now(), clock_integrity_path=clock_integrity_path) if now is None
            else (current, observation_clock)
        )
        return observed, {
            "collector_contract_id": COLLECTOR_CONTRACT_ID,
            "collector_cohort_id": COLLECTOR_COHORT_ID,
            "observation_time_contract_id": OBSERVATION_TIME_CONTRACT_ID,
            "observation_clock_trusted": prospective_clock_attestation(clock_evidence),
            "observation_clock_source": clean_text(clock_evidence.get("source")),
        }
    priority_refresh = refresh_recent_topic_contract(
        connection,
        sources=configured_sources,
        since=max(retention_cutoff, current - dt.timedelta(hours=6)),
        as_of=current,
        progress_callback=progress_callback,
        classification_clock_provider=classification_clock_provider,
    )
    incremental_topic_history_inserted += priority_refresh["topic_inserted"]
    incremental_topic_history_updated += priority_refresh["topic_updated"]
    emit_collector_progress(
        progress_callback,
        "collecting_sources",
        configured_sources=len(configured_sources),
        completed_sources=0,
    )
    attempted = 0
    source_results: list[dict[str, Any]] = []
    operational_sources = 0
    for source in collection_order(
        [row for row in config.get("sources") or [] if isinstance(row, Mapping)],
        current,
    ):
        if not isinstance(source, dict) or not source.get("source_id"):
            continue
        source_id = str(source["source_id"])
        emit_collector_progress(
            progress_callback,
            "collecting_sources",
            source_id=source_id,
            completed_sources=len(source_results),
            attempted_sources=attempted,
        )
        prior = migrate_derived_source_state(
            source,
            source_states.get(source_id) or {},
            now=current,
        )
        if not prior.get("retry_after_utc"):
            inherited_retry = provider_parse_retry_after(
                str(source.get("kind") or "").lower(),
                str(prior.get("last_error") or ""),
                current,
            )
            if inherited_retry:
                # Apply the new provider contract to a quota error already
                # persisted by the previous worker; do not spend another
                # request merely to rediscover the same exhausted allowance.
                prior["retry_after_utc"] = inherited_retry
        runtime_status = source_runtime_status(source)
        if runtime_status != "enabled":
            if runtime_status == "external_adapter":
                prior = external_adapter_runtime_state(source, prior)
                if not prior.get("last_error"):
                    operational_sources += 1
            else:
                prior["operational_status"] = runtime_status
            source_states[source_id] = prior
            source_results.append(
                {
                    "source_id": source_id,
                    "status": runtime_status,
                    "last_status": prior.get("last_status"),
                }
            )
            continue
        operational_sources += 1
        prior["operational_status"] = "enabled"
        if not force and not due_for_poll(source, prior, policy, current):
            source_results.append(
                {
                    "source_id": source_id,
                    "status": "not_due",
                    "last_status": prior.get("last_status"),
                }
            )
            continue
        attempted += 1
        source_observed = current
        source_clock = observation_clock
        if now is None:
            source_observed, source_clock = collection_observation_time(utc_now(), clock_integrity_path=clock_integrity_path)
            if not prospective_clock_attestation(source_clock):
                source_results.append(
                    {
                        "source_id": source_id,
                        "status": "blocked_clock_integrity_before_fetch",
                        "items": 0,
                    }
                )
                continue
        rows, updated = fetch_source(
            source,
            prior,
            timeout_sec=timeout_sec,
            maximum_bytes=maximum_bytes,
            now=source_observed,
        )
        updated["collector_contract_id"] = COLLECTOR_CONTRACT_ID
        updated["classification_version"] = CLASSIFICATION_VERSION
        if now is None:
            source_observed, source_clock = collection_observation_time(utc_now(), clock_integrity_path=clock_integrity_path)
            if not prospective_clock_attestation(source_clock):
                # The response is deliberately not classified or committed.
                # Only a content hash/count enters the diagnostic state; it is
                # not prospective evidence and cannot later be replayed as if
                # its first-known timestamp had been trusted.
                quarantine_sha256 = hashlib.sha256(
                    json.dumps(
                        rows, sort_keys=True, default=str, separators=(",", ":")
                    ).encode("utf-8")
                ).hexdigest()
                # Do not advance polling validators or last-fetch cadence for
                # a response that was deliberately excluded from the causal
                # ledger.  Persisting its ETag/Last-Modified could make the
                # next trusted poll receive 304 and lose the event forever.
                source_states[source_id] = prior
                source_results.append(
                    {
                        "source_id": source_id,
                        "status": "quarantined_clock_integrity_after_fetch",
                        "items": len(rows),
                        "payload_sha256": quarantine_sha256,
                    }
                )
                continue
        source_states[source_id] = updated
        fetched.extend(rows)
        # Commit each source as soon as its request and parsing finish.  A
        # slow or blocked source later in the cycle must not hold already
        # collected official/news observations outside the causal database.
        # The per-source completion time is the first timestamp at which the
        # downstream research stack can actually observe a new row.
        for row in rows:
            row["collector_contract_id"] = COLLECTOR_CONTRACT_ID
            row["collector_cohort_id"] = COLLECTOR_COHORT_ID
            row["observation_time_contract_id"] = OBSERVATION_TIME_CONTRACT_ID
            row["observation_clock_trusted"] = prospective_clock_attestation(
                source_clock
            )
            row["observation_clock_source"] = clean_text(source_clock.get("source"))
        source_classified_all = [
            classify_article(row, first_seen=source_observed) for row in rows
        ]
        classified_all.extend(source_classified_all)
        source_discovery_filtered = [
            article
            for article in source_classified_all
            if retain_classified_discovery_article(article)
        ]
        irrelevant_discovery_skipped += (
            len(source_classified_all) - len(source_discovery_filtered)
        )
        source_classified, source_expired = partition_articles_by_retention(
            source_discovery_filtered,
            before=retention_cutoff,
        )
        classified.extend(source_classified)
        retention_expired.extend(source_expired)
        if source_classified:
            upsert_receipt = {}
            source_inserted, source_duplicates = upsert_articles(
                connection,
                source_classified,
                source_observed,
                classification_clock_provider=classification_clock_provider,
                observation_receipt=upsert_receipt,
            )
            connection.commit()
            inserted += source_inserted
            duplicates += source_duplicates
            if upsert_receipt["source_observations_refused"] or upsert_receipt["classification_observations_refused"]:
                # Keep the prior ETag/Last-Modified and polling cadence so the
                # next trusted poll can retry the retained but unpublished row.
                source_states[source_id] = prior
                source_results.append({
                    "source_id": source_id,
                    "status": "quarantined_source_or_classification_clock",
                    "items": len(rows),
                    "observation_receipt": dict(upsert_receipt),
                })
                continue
            # Reload from the immutable ledger before publishing incremental
            # topics.  Duplicate feed rows were observed earlier than this
            # poll and must retain that original causal timestamp.
            source_canonical = load_canonical_articles_by_event_ids(
                connection,
                [article.get("event_id") for article in source_classified],
            )
            source_relevant = [
                article for article in source_canonical if article.get("relevant")
            ]
            if source_relevant:
                source_topics = cluster_incremental_topics_with_history(
                    connection,
                    source_relevant,
                    as_of=classification_publication_as_of(classification_clock_provider,
                        max([source_observed] + [value for row in source_relevant if (value := classification_floor(row)) is not None]),
                        expected_provenance={
                "collector_contract_id": COLLECTOR_CONTRACT_ID,
                "collector_cohort_id": COLLECTOR_COHORT_ID,
                "observation_time_contract_id": OBSERVATION_TIME_CONTRACT_ID,
            }),
                )
                source_topic_inserted, source_topic_updated = upsert_topic_events(
                    connection,
                    source_topics,
                )
                incremental_topic_history_inserted += source_topic_inserted
                incremental_topic_history_updated += source_topic_updated
        source_results.append(
            {
                "source_id": source_id,
                "status": (
                    "pending_source_content" if updated.get("last_parse_status") == "pending_source_content"
                    else "ok" if not updated.get("last_error") else "error"
                ),
                "http_status": updated.get("last_status"),
                "items": len(rows),
                "error": updated.get("last_error") or "",
                "source_content_state": updated.get("source_content_state"),
            }
        )
        if str(source.get("kind") or "").lower() == "gdelt":
            time.sleep(0.25)

    emit_collector_progress(
        progress_callback,
        "postprocessing_evidence",
        completed_sources=len(source_results),
        attempted_sources=attempted,
        inserted_items=inserted,
        duplicate_items=duplicates,
    )
    completed = current
    completion_clock = observation_clock
    if now is None:
        completed, completion_clock = collection_observation_time(utc_now(), clock_integrity_path=clock_integrity_path)
        if not prospective_clock_attestation(completion_clock):
            connection.commit()
            close_process_database(database_path, connection)
            blocked = {
                "schema_version": SCHEMA_VERSION,
                "collector_contract_id": COLLECTOR_CONTRACT_ID,
                "collector_cohort_id": COLLECTOR_COHORT_ID,
                "generated_utc": iso_utc(completed),
                "status": "blocked_clock_integrity_at_completion",
                "observation_clock": completion_clock,
                "policy": {
                    "research_only": True,
                    "execution_eligible": False,
                    "openai_calls": 0,
                },
                "attempted_sources": attempted,
                "configured_sources": len(config.get("sources") or []),
                "fetched_items": len(fetched),
                "inserted_items": inserted,
                "duplicate_items": duplicates,
                "source_commits_before_block": inserted + duplicates,
                "derived_publications_written": False,
                "sources": source_results,
            }
            state.update(
                {
                    "schema_version": SCHEMA_VERSION,
                    "updated_utc": iso_utc(completed),
                    "status": "blocked_clock_integrity",
                    "sources": source_states,
                    "last_cycle": blocked,
                }
            )
            atomic_write_json(state_file, state)
            atomic_write_json(output_root / "collector_latest_v1.json", blocked)
            return blocked
    try:
        historical_reclassified = reclassify_stored_articles(
            connection,
            sources=configured_sources,
            since=retention_cutoff,
            progress_callback=progress_callback,
            classification_clock_provider=classification_clock_provider,
        )
        reclassified = priority_refresh["reclassified"] + historical_reclassified
        emit_collector_progress(
            progress_callback,
            "postprocessing_evidence",
            postprocess_step="pruning_retention",
            reclassified_rows=reclassified,
        )
        pruned = prune_database(
            connection,
            before=retention_cutoff,
        )
        emit_collector_progress(
            progress_callback,
            "postprocessing_evidence",
            postprocess_step="loading_retained_evidence",
            pruned_rows=pruned,
        )
        relevant = load_relevant_articles(
            connection,
            since=retention_cutoff,
        )
        context_only = load_context_articles(
            connection,
            since=retention_cutoff,
        )
    except Exception:
        connection.rollback()
        raise
    emit_collector_progress(
        progress_callback,
        "postprocessing_evidence",
        postprocess_step="clustering_retained_evidence",
        relevant_items=len(relevant),
        context_items=len(context_only),
    )
    completed = classification_publication_as_of(classification_clock_provider,
        max([completed] + [value for row in [*relevant, *context_only] if (value := classification_floor(row)) is not None]),
        expected_provenance={
                "collector_contract_id": COLLECTOR_CONTRACT_ID,
                "collector_cohort_id": COLLECTOR_COHORT_ID,
                "observation_time_contract_id": OBSERVATION_TIME_CONTRACT_ID,
            })
    published_relevant = cluster_articles(relevant, as_of=completed)
    published_context = cluster_context_articles(context_only, as_of=completed)
    persistent_policy_state = build_persistent_policy_state(
        [*relevant, *context_only],
        as_of=completed,
        sources=configured_sources,
    )
    topic_history_removed = reconcile_topic_history(
        connection,
        since=completed - dt.timedelta(days=retention_days),
        current_topic_ids=[
            str(topic.get("topic_id") or "") for topic in published_relevant
        ],
    )
    topic_history_inserted, topic_history_updated = upsert_topic_events(
        connection,
        published_relevant,
    )
    scheduled_context = [
        article
        for article in context_only
        if bool(article.get("structured_event"))
        and bool(clean_text(article.get("scheduled_utc")))
    ]
    ledger_changed = write_ledger(
        ledger_path,
        [*published_relevant, *scheduled_context],
    )

    emit_collector_progress(
        progress_callback,
        "publishing_derived_views",
        relevant_items=len(relevant),
        context_items=len(context_only),
    )
    instruments = event_tagger.discover_instruments()
    # Retained inputs have now been read and topic/ledger maintenance completed.
    # Recompute eligibility and decay at an actual fresh cutoff; no input arrival
    # clock or existing result is relabelled as a new observation.
    input_processing_cutoff = completed
    aggregation_options = {} if clock_integrity_path is None else {"clock_integrity_path": clock_integrity_path}
    completed, aggregation_clock = refresh_pair_aggregation_clock(completed, now=now, **aggregation_options)
    pair_scores = build_pair_scores(published_relevant, instruments, as_of=completed)
    pair_scores["aggregation_clock"] = aggregation_clock
    pair_scores["input_processing_cutoff_utc"] = iso_utc(input_processing_cutoff)
    atomic_write_json(output_root / "pair_sentiment_latest.json", pair_scores)
    joint_news = build_current_news_snapshot(published_relevant, as_of=completed)
    joint_news["source_bindings"] = {
        filename: hashlib.sha256((Path(__file__).resolve().parent / filename).read_bytes()).hexdigest()
        for filename in (
            "oanda_local_news_sentiment.py", "oanda_news_classification_contract.py",
            "oanda_news_causal_aggregation_guard_v1.py", "oanda_news_causal_aggregation_guard_v2.py",
            "oanda_news_source_observation_ledger_v1.py", "oanda_news_classification_observation_v1.py",
        )
    }
    joint_news["aggregation_clock"] = aggregation_clock
    joint_news["generated_utc"] = iso_utc(utc_now())
    if len(json.dumps(joint_news, indent=2, sort_keys=True, allow_nan=False).encode("utf-8")) + 1 > MAX_CURRENT_SNAPSHOT_BYTES:
        joint_news.update(status="unavailable", news_state="unavailable", topics=[],
                          topic_count=0, directional_topic_count=0, context_topic_count=0,
                          errors=["published_snapshot_byte_bound"])
    atomic_write_json(output_root / "joint_news_current_v1.json", joint_news)
    atomic_write_json(
        output_root / "persistent_policy_state_v1.json",
        persistent_policy_state,
    )
    import oanda_news_source_coverage as source_coverage

    coverage_report = source_coverage.build_coverage(
        config=config,
        state={"sources": source_states},
        instruments=instruments,
        as_of=completed,
    )
    atomic_write_json(output_root / "source_coverage_latest.json", coverage_report)
    # Keep the narrower monetary-authority contract synchronized with every
    # completed source cycle.  Broad news coverage cannot substitute for a
    # mapped, verified first-party central-bank/reserve-authority source.
    import oanda_official_central_bank_coverage as central_bank_coverage

    coverage_json_path = (central_bank_coverage.DEFAULT_JSON if coverage_root is None
                          else coverage_root / central_bank_coverage.DEFAULT_JSON.name)
    coverage_markdown_path = (central_bank_coverage.DEFAULT_MD if coverage_root is None
                              else coverage_root / central_bank_coverage.DEFAULT_MD.name)
    central_bank_report = central_bank_coverage.build_report(
        mapping=load_json(central_bank_coverage.DEFAULT_MAP, {}),
        source_config=config,
        collector_state={"sources": source_states},
        linked_config=load_json(central_bank_coverage.DEFAULT_LINKS, {}),
        instruments=instruments,
        as_of=completed,
    )
    atomic_write_json(coverage_json_path, central_bank_report)
    atomic_write_text(
        coverage_markdown_path,
        central_bank_coverage.render_markdown(central_bank_report),
    )
    atomic_write_json(
        output_root / "articles_latest.json",
        {
            "schema_version": SCHEMA_VERSION,
            "generated_utc": iso_utc(completed),
            "article_count": len(published_relevant),
            "raw_relevant_article_count": len(relevant),
            "syndicated_items_collapsed": len(relevant) - len(published_relevant),
            "articles": published_relevant[-500:],
        },
    )
    atomic_write_json(
        output_root / "topics_latest.json",
        {
            "schema_version": SCHEMA_VERSION,
            "generated_utc": iso_utc(completed),
            "topic_count": len(published_relevant),
            "raw_relevant_article_count": len(relevant),
            "semantic_or_syndicated_items_collapsed": (
                len(relevant) - len(published_relevant)
            ),
            "topics": published_relevant[-500:],
        },
    )
    atomic_write_json(
        output_root / "context_articles_latest.json",
        {
            "schema_version": SCHEMA_VERSION,
            "generated_utc": iso_utc(completed),
            "policy": {
                "research_only": True,
                "execution_eligible": False,
                "excluded_from_directional_scoring": True,
                "scheduled_structured_events_forwarded_to_event_catalog": True,
            },
            "article_count": len(published_context),
            "raw_context_article_count": len(context_only),
            "published_context_window_count": min(500, len(context_only)),
            "semantic_or_syndicated_items_collapsed": (
                min(500, len(context_only)) - len(published_context)
            ),
            "articles": published_context,
        },
    )

    catalog_result: dict[str, Any] = {"status": "skipped"}
    if refresh_event_catalog:
        emit_collector_progress(
            progress_callback,
            "refreshing_event_catalog",
            published_relevant_items=len(published_relevant),
        )
        catalog_result = request_event_catalog_refresh(
            output_root=event_root,
            ledger_path=ledger_path,
        )

    result = {
        "schema_version": SCHEMA_VERSION,
        "collector_contract_id": COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": COLLECTOR_COHORT_ID,
        "issuer_bound_policy_communication_contract_id": (
            ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_CONTRACT_ID_V2
        ),
        "issuer_bound_policy_communication_cohort_id": (
            ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_COHORT_ID_V2
        ),
        "issuer_bound_policy_communication_activated_utc": (
            ISSUER_BOUND_POLICY_COMMUNICATION_SOURCE_ACTIVATED_UTC_V2
        ),
        "prior_issuer_bound_policy_communication_contract_id": (
            ISSUER_BOUND_POLICY_COMMUNICATION_CONTRACT_ID
        ),
        "prior_issuer_bound_policy_communication_cohort_id": (
            ISSUER_BOUND_POLICY_COMMUNICATION_COHORT_ID
        ),
        "generated_utc": iso_utc(completed),
        "status": "ok",
        "observation_clock": observation_clock,
        "policy": {
            "research_only": True,
            "execution_eligible": False,
            "openai_calls": 0,
        },
        "attempted_sources": attempted,
        "configured_sources": len(config.get("sources") or []),
        "operational_sources": operational_sources,
        "source_health": summarize_source_health(
            config.get("sources") or [],
            source_states,
        ),
        "retired_source_states": retired_source_states,
        "fetched_items": len(fetched),
        "classified_items": len(classified_all),
        "irrelevant_discovery_skipped_items": irrelevant_discovery_skipped,
        "retention_eligible_items": len(classified),
        "retention_skipped_items": len(retention_expired),
        "inserted_items": inserted,
        "duplicate_items": duplicates,
        "reclassified_items": reclassified,
        "priority_recent_reclassified_items": priority_refresh["reclassified"],
        "priority_recent_stale_topic_items": priority_refresh["stale_topics"],
        "priority_recent_removed_topic_items": priority_refresh["topic_removed"],
        "pruned_items": pruned,
        "relevant_retained_items": len(relevant),
        "published_relevant_items": len(published_relevant),
        "syndicated_items_collapsed": len(relevant) - len(published_relevant),
        "topic_history_inserted": topic_history_inserted,
        "topic_history_updated": topic_history_updated,
        "incremental_topic_history_inserted": (
            incremental_topic_history_inserted
        ),
        "incremental_topic_history_updated": (
            incremental_topic_history_updated
        ),
        "topic_history_removed": topic_history_removed,
        "ledger_changed": ledger_changed,
        "context_retained_items": len(context_only),
        "scheduled_event_items_in_ledger": len(scheduled_context),
        "published_context_items": len(published_context),
        "persistent_policy_currency_count": persistent_policy_state.get(
            "currency_count"
        ),
        "persistent_policy_document_count": persistent_policy_state.get(
            "document_count"
        ),
        "context_items_collapsed_in_latest_window": (
            min(500, len(context_only)) - len(published_context)
        ),
        "active_articles": pair_scores.get("active_article_count"),
        "active_scored_pairs": sum(
            1
            for row in (pair_scores.get("pairs") or {}).values()
            if row.get("direction") != "NEUTRAL"
        ),
        "active_direct_scored_pairs": sum(
            1
            for row in (pair_scores.get("pairs") or {}).values()
            if row.get("direction") != "NEUTRAL"
            and row.get("evidence_quality")
            in {"ONE_SIDED_DIRECT", "TWO_SIDED_DIRECT"}
        ),
        "active_global_proxy_scored_pairs": sum(
            1
            for row in (pair_scores.get("pairs") or {}).values()
            if row.get("direction") != "NEUTRAL"
            and row.get("evidence_quality") == "GLOBAL_TOPIC_PROXY"
        ),
        "source_coverage": {
            "currency_count": coverage_report.get("currency_count"),
            "pair_count": coverage_report.get("pair_count"),
            "all_currencies_configured": coverage_report.get(
                "all_currencies_configured"
            ),
            "pair_coverage_tier_counts": coverage_report.get(
                "pair_coverage_tier_counts"
            ),
        },
        "official_central_bank_coverage": {
            "contract_complete": central_bank_report.get("contract_complete"),
            "minimum_operational_complete": central_bank_report.get(
                "minimum_operational_complete"
            ),
            "fully_healthy": central_bank_report.get("fully_healthy"),
            "currency_summary": central_bank_report.get("currency_summary"),
            "pair_summary": central_bank_report.get("pair_summary"),
            "global_blockers": central_bank_report.get("global_blockers"),
        },
        "sources": source_results,
        "event_catalog": catalog_result,
        "paths": {
            "database": str(database_path),
            "ledger": str(ledger_path),
            "pair_scores": str(output_root / "pair_sentiment_latest.json"),
            "persistent_policy_state": str(
                output_root / "persistent_policy_state_v1.json"
            ),
            "topics": str(output_root / "topics_latest.json"),
            "source_coverage": str(
                output_root / "source_coverage_latest.json"
            ),
            "official_central_bank_coverage": str(
                coverage_json_path
            ),
            "context_articles": str(output_root / "context_articles_latest.json"),
            "event_context": str(event_root / "latest_pair_news_context.json"),
        },
    }
    result["wal_checkpoint"] = bounded_wal_checkpoint(database_path)
    emit_collector_progress(progress_callback, "publishing_completed_snapshot")
    state.update(
        {
            "schema_version": SCHEMA_VERSION,
            "updated_utc": iso_utc(completed),
            "status": "running",
            "sources": source_states,
            "last_cycle": result,
        }
    )
    atomic_write_json(state_file, state)
    atomic_write_json(output_root / "collector_latest_v1.json", result)
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--event-root", type=Path, default=DEFAULT_EVENT_ROOT)
    parser.add_argument("--state", type=Path)
    parser.add_argument("--clock-integrity-state", type=Path, help="Explicit clock attestation for this collection run; original default when omitted")
    parser.add_argument("--coverage-root", type=Path, help="Monetary-authority JSON and Markdown output directory; original defaults when omitted")
    parser.add_argument("--interval-sec", type=float, default=60.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--no-refresh-event-catalog", action="store_true")
    return parser.parse_args()


def lower_research_process_priority() -> None:
    """Keep the broad weekend collector subordinate to live execution work."""

    if os.name != "nt":
        return
    try:
        import ctypes

        below_normal_priority_class = 0x00004000
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.GetCurrentProcess.restype = ctypes.c_void_p
        kernel32.SetPriorityClass.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
        kernel32.SetPriorityClass.restype = ctypes.c_int
        handle = kernel32.GetCurrentProcess()
        kernel32.SetPriorityClass(handle, below_normal_priority_class)
    except Exception:
        # Priority is an operational hint, never a reason to stop collection.
        return


def main() -> int:
    lower_research_process_priority()
    args = parse_args()
    started = time.monotonic()
    first = True
    while True:
        cycle_started_utc = utc_now()
        progress = CollectorCycleProgress(cycle_started_utc)
        heartbeat_stop = threading.Event()
        heartbeat_thread = threading.Thread(
            target=run_collector_cycle_heartbeat,
            kwargs={
                "stop_event": heartbeat_stop,
                "output_root": args.output_root,
                "progress": progress,
                "interval_sec": 30.0,
            },
            name="collector-cycle-heartbeat",
            daemon=True,
        )
        heartbeat_thread.start()
        try:
            result = run_cycle(
                config_path=args.config,
                output_root=args.output_root,
                ledger_path=args.ledger,
                event_root=args.event_root,
                state_path=args.state,
                clock_integrity_path=getattr(args, "clock_integrity_state", None),
                coverage_root=getattr(args, "coverage_root", None),
                refresh_event_catalog=not args.no_refresh_event_catalog,
                force=bool(args.force and first),
                progress_callback=progress.update,
            )
            heartbeat_stop.set()
            heartbeat_thread.join()
            result = dict(result)
            result["cycle_in_progress"] = False
            result["heartbeat_utc"] = iso_utc()
            atomic_write_json(args.output_root / "collector_latest_v1.json", result)
            progress.update("cycle_complete")
            publish_collector_cycle_heartbeat(
                output_root=args.output_root,
                progress=progress,
                status="cycle_complete",
                cycle_in_progress=False,
            )
            print(json.dumps(result, sort_keys=True), flush=True)
        except Exception as exc:
            heartbeat_stop.set()
            heartbeat_thread.join()
            error = {
                "schema_version": SCHEMA_VERSION,
                "generated_utc": iso_utc(),
                "status": "error",
                "error": str(exc)[:1000],
                "policy": {
                    "research_only": True,
                    "execution_eligible": False,
                    "openai_calls": 0,
                },
            }
            atomic_write_json(args.output_root / "collector_latest_v1.json", error)
            progress.update("cycle_error", {"error": str(exc)[:1000]})
            publish_collector_cycle_heartbeat(
                output_root=args.output_root,
                progress=progress,
                status="error",
                cycle_in_progress=False,
            )
            print(json.dumps(error, sort_keys=True), flush=True)
        first = False
        if args.once:
            break
        if args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec:
            break
        time.sleep(max(5.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
