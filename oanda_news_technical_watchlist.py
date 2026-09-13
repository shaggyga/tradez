#!/usr/bin/env python3
"""Prospective, research-only news/technical comparison watchlist.

This worker does not modify the existing news classifier, strategy lab,
lifecycle database, authorization, or executor.  It turns current news into one
currency-factor observation per market episode, keeps news-only and
technical-only forecasts separate, and records aligned/conflicted combinations
as distinct shadow arms.

Consensus, rate repricing, and pre-event price state are explicit evidence
fields.  Missing data never becomes a zero-valued confirmation.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import re
import sqlite3
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

from oanda_local_news_sentiment import (
    CLASSIFICATION_VERSION as NEWS_CLASSIFICATION_VERSION,
    COLLECTOR_COHORT_ID,
    COLLECTOR_CONTRACT_ID,
    OBSERVATION_TIME_CONTRACT_ID,
    normalized_observation_time,
    prospective_collector_provenance,
)


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
NEWS_DB = DATA / "local_news_sentiment" / "local_news_sentiment_v1.sqlite"
MACRO_DB = STATE / "macro_surprise_v1.sqlite"
SIGNALS = STATE / "practice_007_signal_snapshot_research_v1.json"
QUOTES = STATE / "practice_007_market_quotes_v1.json"
QUOTE_HISTORY = DATA / "market_sentiment_ticker" / "quote_history.json"
RATES = STATE / "rates_policy_repricing_v1.json"
DAILY_RATES = STATE / "official_daily_rate_context_v1.json"
OPPORTUNITY_DB = STATE / "executable_opportunity_prospective_v1.sqlite"
OPPORTUNITY_STATE = STATE / "executable_opportunity_prospective_v1.json"
OPPORTUNITY_MODEL_COHORT_ID = "executable_opportunity_ranking_v2_20260814"
POLICY_STATE = DATA / "local_news_sentiment" / "persistent_policy_state_v1.json"
DEFAULT_DB = STATE / "news_technical_watchlist_v1.sqlite"
DEFAULT_STATE = STATE / "news_technical_watchlist_v1.json"
DEFAULT_REPORT = DATA / "reports" / "news_technical_watchlist" / "NEWS_TECHNICAL_WATCHLIST_CURRENT.md"
RATE_REPRICING_CLOCK_BOUND_COHORT_ID = (
    "rates_policy_repricing_v2_clock_v4_20260817"
)
DAILY_RATE_CLOCK_BOUND_COHORT_ID = (
    "official_daily_rate_context_v2_clock_v4_20260817"
)
PAIR_SCORE_CONTRACT_ID = (
    "same_episode_signed_base_minus_quote_currency_score_v1_20260901"
)

UTC = dt.timezone.utc
# V41 begins when every news expression is defined by the signed base-minus-
# quote score for the same source episode.  V40 and all of its child cohorts
# remain immutable; equal same-episode scores on both pair legs now cancel.
# A classifier rollout can leave the retained topic table on the prior
# semantic contract for part of a collection pass.  The v4 cohort explicitly
# requires the current classifier version so stale topic payloads cannot seed
# forecasts during that transition.  V5 additionally starts after the
# producer's recent-topic-first collection contract was frozen.  V7 begins
# after scheduled release-window sources received deterministic first priority.
# V8 adds a separately frozen H30 opportunity-ranking branch. V9 requires the
# shared prospective ledger row to belong to that exact model cohort, so old
# v1 forecasts cannot contaminate a v2-ranked observation. V10 starts after
# the upstream collector's explicit provider-in-body quota backoff contract.
# V11 binds opportunity rows to the exact current collector cohort rather than
# accepting every collector implementation attached to model v2. V12 begins
# after the official Michigan survey calendar/report source and consumer-survey
# classification contract were added. V13 begins after numeric zero values were
# preserved through deterministic retained-corpus reclassification. V14 fixes
# the official Michigan report-listing regex; source parser coverage changes
# begin a new child cohort even though existing links remain bootstrap-only.
# V15 starts when that source's normal polling cadence changes from six hours
# to 15 minutes; sampling-frequency changes require a new source cohort. V16
# adds the distinct official live-release page and preserves its activity and
# inflation components without forcing a direction. V17 begins when a changed
# source contract receives an immediate retry instead of inheriting an obsolete
# parser's normal/error cadence. V18 treats an empty URL-history list from a
# broken parser as unseeded, keeping the first nonempty archive snapshot out of
# forward evidence. V19 invalidates stale HTTP validators on a changed source
# contract so a repaired parser receives one complete response body. V20
# versions the mutable live Michigan release as a structured event so later
# preliminary/final/monthly updates cannot collapse into its stable URL. V21
# prevents metaphorical "enters a price war" headlines from re-triggering the
# direct-conflict escalation matcher after the non-event veto fires. V22 adds
# a separate, explicitly uncorroborated research arm. It can score a timely
# secondary-source currency thesis without making that thesis publishable,
# confirmable, authorizable, or executable. V23 begins after the semantic
# contract stopped treating "rate hikes will not save/support the currency"
# as a bullish currency claim. V24 begins after the parser learned that
# limiting tightening risks is dovish and that simultaneous inflation/labor
# claims are conflicted rather than a single directional score. V25 begins
# after "pare bets on a Fed rate hike" stopped inheriting the bullish score
# from the embedded noun phrase. V26 begins after the direct ONS release feed
# learned to follow the official listing to its timestamped statistical
# bulletin; that prospective information-set change cannot share v25 evidence.
# V27 starts after mutable RSS/search timestamps were made immutable in the
# retained payload and falling inflation-expectation surveys received their
# explicit, economically signed semantic rule.
# V28 adds explicit shadow-only thesis lifecycle and source-contract
# provenance, and begins after the RBNZ official-publisher fallback moved from
# a 15-minute to a five-minute sampling contract.
# V29 begins after direct Cleveland and Richmond regional-Fed speech surfaces
# were added.  That changes the prospective information set, so no v28
# observation can share a child cohort with the new official-source coverage.
# V30 begins after the UKMTO official-publisher fallback was added for maritime
# incident notices.  The direct UKMTO site returns HTTP 403 on this host, so
# the fallback remains explicitly indirect and cannot claim direct-site SLA.
# V31 begins after RSS back-catalog bootstrap protection was added to that
# fallback.  Historical notices retained on its first snapshot are context,
# never newly observed prospective evidence.
# V32 begins after fresh official Statistics Canada Labour Force Survey actuals
# received a neutral structured-numeric contract.  The new information set is
# prospective only and cannot share a cohort with v31 observations.
# V33 records slow official rate/yield context separately from intraday policy
# repricing.  It cannot satisfy rate confirmation or assign direction.
# V34 adds separately typed SGD two-year and HKD one-month funding context;
# unlike tenors are never treated as a cross-currency rate differential. V35
# additionally binds slow-rate context to the underlying rate date: a fresh
# collector poll cannot make a stale month-end observation look current. V41
# expresses every news thesis as signed base-minus-quote currency scores. V42
# prevents crypto-primary secondary headlines from becoming direct currency
# factors merely because they mention a central bank, rate, or yield. The
# underlying story remains immutable context in the source ledger. V43 starts
# after the live report was found to use its cycle-start clock even though the
# quote file was read later. Every decision clock is now taken only after all
# bounded input snapshots have been read, and quote ages are recomputed against
# that post-read clock. V42 and all of its child cohorts stay immutable. V44
# admits only prospectively activated, issuer-bound Japan MOF external-policy
# pressure into the isolated official research arm. It cannot publish,
# authorize, or execute and must still receive rate/price confirmation. V45
# begins when unilateral ceasefire proposals stop creating broad risk-on
# factors. V46 begins when publisher-labelled analysis and commodity
# operational metrics stop inheriting immediate FX directions. Every prior
# cohort remains immutable. V47 begins after conditional policy actions and
# already-realized weekly market recaps stop becoming new research factors.
# V48 begins after opposing same-currency policy claims stop being flattened.
# V49 begins after secondary conflict-duration recaps stop being treated as a
# newly observed escalation. Every V48 row remains immutable.
PARENT_COHORT_ID = "news_technical_watchlist_v48_opposing_policy_claim_guard_20260904"
COHORT_ID = "news_technical_watchlist_v49_conflict_duration_recap_guard_20260904"
UNCORROBORATED_NEWS_PARENT_COHORTS = {
    5: "uncorroborated_news_response_h5_v23_opposing_policy_claim_guard_20260904",
    15: "uncorroborated_news_response_h15_v23_opposing_policy_claim_guard_20260904",
    30: "uncorroborated_news_response_h30_v23_opposing_policy_claim_guard_20260904",
}
UNCORROBORATED_NEWS_COHORTS = {
    5: "uncorroborated_news_response_h5_v24_conflict_duration_recap_guard_20260904",
    15: "uncorroborated_news_response_h15_v24_conflict_duration_recap_guard_20260904",
    30: "uncorroborated_news_response_h30_v24_conflict_duration_recap_guard_20260904",
}
UNCORROBORATED_NEWS_INVALID_SAMPLING_COHORTS = {
    5: "uncorroborated_news_response_h5_v15_energy_exporter_ambiguity_20260818",
    15: "uncorroborated_news_response_h15_v15_energy_exporter_ambiguity_20260818",
    30: "uncorroborated_news_response_h30_v15_energy_exporter_ambiguity_20260818",
}
SECONDARY_CRYPTO_PRIMARY_GATE_CONTRACT_ID = (
    "secondary_crypto_primary_currency_factor_gate_v1_20260901"
)
SECONDARY_CRYPTO_PRIMARY_PATTERN = re.compile(
    r"\b(?:xrp|bitcoin|btc|ethereum|eth|crypto(?:currency|currencies)?|"
    r"altcoin|stablecoin|dogecoin|solana|cardano|memecoin|token)\b",
    re.IGNORECASE,
)
RECONFIRMATION_COHORT_ID = (
    "news_technical_reconfirmation_h15_v40_conflict_duration_recap_guard_20260904"
)
RECONFIRMATION_PARENT_COHORT_ID = (
    "news_technical_reconfirmation_h15_v39_opposing_policy_claim_guard_20260904"
)
NEWS_MAGNITUDE_COHORTS = {
    "news_magnitude_ranked_h5": "news_magnitude_ranked_h5_v41_conflict_duration_recap_guard_20260904",
    "news_magnitude_ranked_h15": "news_magnitude_ranked_h15_v41_conflict_duration_recap_guard_20260904",
    "news_magnitude_ranked_h30": "news_magnitude_ranked_h30_v35_conflict_duration_recap_guard_20260904",
    "news_magnitude_direction_confirmed_h5": (
        "news_magnitude_direction_confirmed_h5_v41_conflict_duration_recap_guard_20260904"
    ),
    "news_magnitude_direction_confirmed_h15": (
        "news_magnitude_direction_confirmed_h15_v41_conflict_duration_recap_guard_20260904"
    ),
    "news_magnitude_direction_confirmed_h30": (
        "news_magnitude_direction_confirmed_h30_v35_conflict_duration_recap_guard_20260904"
    ),
}
NEWS_REMAINING_MOVE_COHORTS = {
    **{
        f"news_remaining_move_cost_clear_h{horizon}": (
            f"news_remaining_move_cost_clear_h{horizon}_v9_conflict_duration_recap_guard_20260904"
        )
        for horizon in (5, 15, 30)
    },
    **{
        f"news_remaining_move_negative_control_h{horizon}": (
            f"news_remaining_move_negative_control_h{horizon}_v9_conflict_duration_recap_guard_20260904"
        )
        for horizon in (5, 15, 30)
    },
}
NEWS_REMAINING_MOVE_PARENT_COHORTS = {
    arm: f"{arm}_v8_opposing_policy_claim_guard_20260904"
    for arm in NEWS_REMAINING_MOVE_COHORTS
}
OFFICIAL_RELEASE_FAST_CONFIRMATION_COHORT_ID = (
    "official_release_fast_multileg_h15_v9_conflict_duration_recap_guard_20260904"
)
OFFICIAL_SURPRISE_RATE_COHORTS = {
    "official_surprise_rate_confirmed_h15": (
        "official_surprise_rate_confirmed_h15_v9_conflict_duration_recap_guard_20260904"
    ),
    "official_surprise_rate_negative_control_h15": (
        "official_surprise_rate_negative_control_h15_v9_conflict_duration_recap_guard_20260904"
    ),
}
OFFICIAL_SURPRISE_RATE_PARENT_COHORTS = {
    "official_surprise_rate_confirmed_h15": (
        "official_surprise_rate_confirmed_h15_v8_opposing_policy_claim_guard_20260904"
    ),
    "official_surprise_rate_negative_control_h15": (
        "official_surprise_rate_negative_control_h15_v8_opposing_policy_claim_guard_20260904"
    ),
}
OFFICIAL_SURPRISE_RATE_MIN_ABS_Z = 0.5
OFFICIAL_SURPRISE_RATE_MIN_ABS_CHANGE_BPS = 1.0
OFFICIAL_RELEASE_FAST_CONFIRMATION_PARENT_COHORT_ID = (
    "official_release_fast_multileg_h15_v8_opposing_policy_claim_guard_20260904"
)
NEWS_MAGNITUDE_PARENT_COHORTS = {
    "news_magnitude_ranked_h5": "news_magnitude_ranked_h5_v40_opposing_policy_claim_guard_20260904",
    "news_magnitude_ranked_h15": "news_magnitude_ranked_h15_v40_opposing_policy_claim_guard_20260904",
    "news_magnitude_ranked_h30": "news_magnitude_ranked_h30_v34_opposing_policy_claim_guard_20260904",
    "news_magnitude_direction_confirmed_h5": (
        "news_magnitude_direction_confirmed_h5_v40_opposing_policy_claim_guard_20260904"
    ),
    "news_magnitude_direction_confirmed_h15": (
        "news_magnitude_direction_confirmed_h15_v40_opposing_policy_claim_guard_20260904"
    ),
    "news_magnitude_direction_confirmed_h30": (
        "news_magnitude_direction_confirmed_h30_v34_opposing_policy_claim_guard_20260904"
    ),
}
SCHEMA_VERSION = 1
MAX_QUOTE_AGE_SEC = 120.0
MAX_SIGNAL_AGE_SEC = 180.0
MAX_OPPORTUNITY_AGE_SEC = 600.0
MAX_PAIR_LEGS_PER_FACTOR = 3
MAX_UNCORROBORATED_PAIR_LEGS_PER_FACTOR = 1
MAX_ENTRY_SPREAD_PIPS = 5.0
REMAINING_MOVE_COST_SAFETY_MULTIPLE = 1.5
REMAINING_MOVE_MIN_CLEAR_PROBABILITY = 0.55
DEFAULT_HORIZON_MIN = 60
RECONFIRMATION_HORIZON_MIN = 15

MACRO_GROUPS = {
    "inflation_release": "inflation",
    "inflation_context": "inflation",
    "monetary_policy": "monetary_policy",
    "central_bank_rate_decision": "monetary_policy",
    "labor_release": "employment",
    "employment_release": "employment",
    "growth_release": "growth",
    "business_activity_release": "growth",
    "manufacturing_release": "growth",
    "trade_balance_release": "trade_balance",
}

TERM_STOPWORDS = frozenset(
    "a an and are as at be by for from has have in into is it its of on or that the this to was were will with today latest says after before amid near over under".split()
)
TERM_SPIKE_MIN_N = 10
TERM_SPIKE_LIMIT = 100


def utc_now() -> dt.datetime:
    return dt.datetime.now(tz=UTC)


def iso(value: dt.datetime | None = None) -> str:
    return (value or utc_now()).astimezone(UTC).isoformat()


def parse_utc(value: Any) -> dt.datetime | None:
    try:
        result = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=UTC)
    return result.astimezone(UTC)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False, default=str) + "\n",
        encoding="utf-8",
    )
    _replace_with_retry(temporary, path)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    _replace_with_retry(temporary, path)


def _replace_with_retry(temporary: Path, path: Path) -> None:
    for attempt in range(8):
        try:
            os.replace(temporary, path)
            return
        except PermissionError:
            if attempt == 7:
                temporary.unlink(missing_ok=True)
                raise
            time.sleep(min(0.4, 0.025 * (2**attempt)))


def normalized_direction(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"long", "buy", "bullish", "up"}:
        return "long"
    if text in {"short", "sell", "bearish", "down"}:
        return "short"
    return "neutral"


def canonical_episode_id(topic: Mapping[str, Any]) -> str:
    """Collapse repeated macro headlines into one currency/date/series episode."""

    payload = topic.get("payload") or {}
    story_cluster = str(payload.get("story_cluster_id") or "").strip()
    if story_cluster:
        digest = hashlib.sha256(
            f"story|{story_cluster}".encode("utf-8")
        ).hexdigest()[:24]
        return f"news_episode_{digest}"
    category = str(topic.get("category") or payload.get("category") or "unknown").lower()
    group = MACRO_GROUPS.get(category, category)
    currencies = sorted(str(x).upper() for x in topic.get("currencies") or [] if str(x))
    series = str(payload.get("event_series_id") or "").strip().lower()
    reference = str(
        payload.get("reference_period") or payload.get("reference_date") or ""
    ).strip().lower()
    known = parse_utc(topic.get("first_known_utc")) or utc_now()
    if group in set(MACRO_GROUPS.values()):
        identity = [group, ",".join(currencies), series or reference or known.date().isoformat()]
    else:
        signature = str(topic.get("topic_signature") or payload.get("topic_signature") or "")
        identity = [group, ",".join(currencies), signature or str(topic.get("topic_id") or "")]
    digest = hashlib.sha256("|".join(identity).encode("utf-8")).hexdigest()[:24]
    return f"news_episode_{digest}"


def term_features(*values: Any, category: str = "", action: str = "") -> list[str]:
    text = " ".join(str(value or "") for value in values).lower()
    words = [
        word for word in re.findall(r"[a-z][a-z0-9]+", text)
        if len(word) >= 3 and word not in TERM_STOPWORDS
    ]
    terms: set[str] = set(words)
    terms.update(f"{a}_{b}" for a, b in zip(words, words[1:]))
    if category:
        terms.add(f"category:{category}")
    if action:
        terms.add(f"action:{action}")
    return sorted(terms)[:80]


def term_spike_statistics(
    term_values: Mapping[str, Sequence[tuple[float, float]]],
    *,
    minimum_n: int = TERM_SPIKE_MIN_N,
    limit: int = TERM_SPIKE_LIMIT,
) -> dict[str, Any]:
    """Report lexical outcomes only after a minimally interpretable sample.

    Raw term evidence remains in the immutable watchlist rows.  Suppressing
    tiny repeated summaries prevents dozens of words from one or two news
    episodes from looking like distinct predictive findings.
    """

    rows: list[dict[str, Any]] = []
    candidate_count = 0
    for term, values in term_values.items():
        candidate_count += 1
        n = len(values)
        if n < minimum_n:
            continue
        rows.append(
            {
                "term": term,
                "n": n,
                "direction_accuracy": sum(gross > 0 for gross, _ in values) / n,
                "after_cost_win_rate": sum(net > 0 for _, net in values) / n,
                "mean_executable_pips": sum(net for _, net in values) / n,
                "large_move_rate": sum(abs(gross) >= 10 for gross, _ in values) / n,
            }
        )
    rows.sort(key=lambda row: (-row["n"], row["term"]))
    return {
        "rows": rows[:limit],
        "policy": {
            "minimum_matured_observations": minimum_n,
            "maximum_reported_terms": limit,
            "candidate_term_count": candidate_count,
            "supported_term_count": len(rows),
            "suppressed_low_support_count": candidate_count - len(rows),
            "raw_evidence_retained": True,
            "evidence_unit": "term_x_news_episode_x_currency_factor",
        },
    }


def collapse_term_factor_episode_values(
    rows: Iterable[tuple[Any, Any, Any, Any, Any]],
) -> dict[str, list[tuple[float, float]]]:
    """Collapse correlated pair expressions to one event/currency observation."""

    grouped: dict[str, dict[tuple[str, str], list[tuple[float, float]]]] = (
        defaultdict(lambda: defaultdict(list))
    )
    for episode_id, currency, terms_json, gross, net in rows:
        try:
            terms = json.loads(terms_json or "[]")
        except (TypeError, ValueError, json.JSONDecodeError):
            terms = []
        factor_key = (str(episode_id or ""), str(currency or ""))
        for term in set(str(value) for value in terms):
            grouped[term][factor_key].append((safe_float(gross), safe_float(net)))
    collapsed: dict[str, list[tuple[float, float]]] = {}
    for term, factor_episodes in grouped.items():
        collapsed[term] = [
            (
                sum(gross for gross, _ in values) / len(values),
                sum(net for _, net in values) / len(values),
            )
            for values in factor_episodes.values()
            if values
        ]
    return collapsed


def secondary_crypto_primary_context(payload: Mapping[str, Any]) -> bool:
    """Identify secondary crypto-first stories that are not FX-policy proof.

    These stories remain immutable narrative context. They cannot directly
    seed a currency-response cohort merely because a rate or central-bank term
    appears in a crypto outlook. Verified issuer-bound communications and
    structured releases are never excluded by this gate.
    """

    if str(payload.get("directional_source_grade") or "") != (
        "secondary_requires_corroboration"
    ):
        return False
    if bool(payload.get("structured_event")) or bool(
        payload.get("issuer_bound_policy_communication")
    ):
        return False
    return bool(
        SECONDARY_CRYPTO_PRIMARY_PATTERN.search(
            str(payload.get("headline") or "")
        )
    )


def load_currency_factors(
    news_db: Path,
    observed: dt.datetime,
    diagnostics: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if not news_db.exists():
        return []
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(
            f"file:{news_db.as_posix()}?mode=ro",
            uri=True,
            timeout=30.0,
        )
        connection.execute("PRAGMA busy_timeout=30000")
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT topic_id,topic_signature,first_known_utc,published_utc,category,
                   direct_currencies_json,article_count,distinct_source_count,payload_json
            FROM topic_events
            ORDER BY first_known_utc,topic_id
            """
        ).fetchall()
        if diagnostics is not None:
            diagnostics["news_database_read_state"] = "ok"
    except sqlite3.Error as exc:
        # Collector migrations may briefly hold an exclusive schema/write
        # lock. A research consumer must fail closed without crashing or
        # publishing stale factors; the next supervised cycle retries.
        if diagnostics is not None:
            diagnostics["news_database_read_state"] = "temporarily_unavailable"
            diagnostics["news_database_read_error_type"] = type(exc).__name__
        return []
    finally:
        if connection is not None:
            connection.close()
    episodes: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        try:
            payload = json.loads(row["payload_json"] or "{}")
            currencies = json.loads(row["direct_currencies_json"] or "[]")
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not prospective_collector_provenance(payload):
            # V38 and earlier topic rows have no immutable V4/V39 first-seen
            # clock provenance.  They remain available to historical audits,
            # but cannot seed this prospective child cohort.
            continue
        if str(payload.get("classification_version") or "") != NEWS_CLASSIFICATION_VERSION:
            # Semantic changes are part of the immutable source contract.
            # Fail closed while the collector reclassifies retained topics;
            # otherwise a newly restarted consumer can emit forecasts from an
            # obsolete direction mapping before the producer catches up.
            continue
        known = parse_utc(payload.get("prospective_provenance_known_utc"))
        horizon = max(1, int(safe_float(payload.get("estimated_reaction_horizon_minutes"), DEFAULT_HORIZON_MIN)))
        if known is None or known > observed or observed > known + dt.timedelta(minutes=horizon):
            continue
        if not bool(payload.get("forward_signal_timely")):
            continue
        if bool(payload.get("reports_prior_market_move")):
            continue
        publish_eligible = bool(payload.get("directional_publish_eligible"))
        uncorroborated_research = bool(
            not publish_eligible
            and str(payload.get("context_reason") or "").startswith(
                "secondary_uncorroborated_"
            )
            and payload.get("research_currency_scores")
        )
        if uncorroborated_research and secondary_crypto_primary_context(payload):
            if diagnostics is not None:
                diagnostics["secondary_crypto_primary_suppressed"] = int(
                    diagnostics.get("secondary_crypto_primary_suppressed", 0)
                ) + 1
                examples = diagnostics.setdefault(
                    "secondary_crypto_primary_examples", []
                )
                if len(examples) < 5:
                    examples.append(
                        {
                            "topic_id": str(row["topic_id"]),
                            "headline": str(payload.get("headline") or ""),
                            "source_ids": list(payload.get("source_ids") or []),
                            "reason": "secondary_crypto_primary_context_only",
                        }
                    )
            continue
        official_release_research = bool(
            not publish_eligible
            and payload.get("research_currency_scores")
            and str(payload.get("directional_source_grade") or "")
            == "verified_primary_or_publisher"
            and (
                payload.get("structured_event")
                or str(payload.get("research_directional_basis") or "")
                == "official_duration_liquidity_policy_requires_rate_and_price_confirmation"
                or (
                    str(payload.get("research_directional_basis") or "")
                    == "japan_external_policy_pressure_requires_rate_and_price_confirmation"
                    and bool(
                        payload.get(
                            "japan_external_policy_pressure_activation_eligible"
                        )
                    )
                )
            )
        )
        if not publish_eligible and not uncorroborated_research and not official_release_research:
            continue
        if publish_eligible and bool(payload.get("context_only")):
            continue
        scores = (
            payload.get("currency_scores")
            if publish_eligible
            else payload.get("research_currency_scores")
        ) or {}
        if uncorroborated_research:
            # Broad inferred factors are admissible only in this isolated
            # shadow arm. The production-grade factor path remains restricted
            # to currencies explicitly published by the topic contract.
            currencies = sorted(str(value).upper() for value in scores)
        topic = {
            "topic_id": str(row["topic_id"]),
            "topic_signature": str(row["topic_signature"]),
            "first_known_utc": iso(known),
            "published_utc": str(row["published_utc"]),
            "category": str(row["category"]),
            "currencies": currencies,
            "payload": payload,
        }
        episode_id = canonical_episode_id(topic)
        for currency in currencies:
            currency = str(currency).upper()
            score = safe_float(scores.get(currency), 0.0)
            if not score:
                continue
            key = (episode_id, currency)
            action = str(payload.get("topic_action") or "")
            candidate = episodes.setdefault(
                key,
                {
                    "episode_id": episode_id,
                    "currency": currency,
                    "score": score,
                    "confidence": safe_float(payload.get("directional_confidence"), 0.0),
                    "horizon_min": horizon,
                    "first_known_utc": str(row["first_known_utc"]),
                    "category": str(row["category"]),
                    "event_series_id": str(payload.get("event_series_id") or ""),
                    "structured_event": bool(payload.get("structured_event")),
                    "story_cluster_id": str(payload.get("story_cluster_id") or ""),
                    "release_components": list(payload.get("release_components") or []),
                    "scheduled_utc": str(payload.get("scheduled_utc") or ""),
                    "reference_period": str(payload.get("reference_period") or ""),
                    "topic_ids": [],
                    "headlines": [],
                    "source_ids": set(),
                    "source_contract_ids": set(),
                    "source_cohort_ids": set(),
                    "publisher_count": 0,
                    "article_count": 0,
                    "terms": set(),
                    "uncorroborated_research": uncorroborated_research,
                    "official_release_research": official_release_research,
                    "directional_source_grade": str(
                        payload.get("directional_source_grade") or ""
                    ),
                },
            )
            # Same episode is one factor.  Preserve the strongest predeclared
            # interpretation rather than summing repeated headlines.
            if abs(score) > abs(safe_float(candidate["score"])):
                candidate["score"] = score
                candidate["confidence"] = safe_float(payload.get("directional_confidence"), 0.0)
                candidate["horizon_min"] = horizon
            candidate["topic_ids"].append(str(row["topic_id"]))
            candidate["headlines"].append(str(payload.get("headline") or ""))
            candidate["source_ids"].update(str(x) for x in payload.get("source_ids") or [])
            candidate["source_contract_ids"].update(
                str(value)
                for value in (
                    [payload.get("source_contract_id")]
                    + list(payload.get("source_contract_ids") or [])
                )
                if str(value or "")
            )
            candidate["source_cohort_ids"].update(
                str(value)
                for value in (
                    [payload.get("source_cohort_id")]
                    + list(payload.get("source_cohort_ids") or [])
                )
                if str(value or "")
            )
            candidate["publisher_count"] = max(candidate["publisher_count"], int(row["distinct_source_count"] or 0))
            candidate["article_count"] += int(row["article_count"] or 0)
            candidate["terms"].update(
                term_features(
                    payload.get("headline"), payload.get("summary"),
                    category=str(row["category"]), action=action,
                )
            )
    result: list[dict[str, Any]] = []
    for row in episodes.values():
        row["topic_ids"] = sorted(set(row["topic_ids"]))
        row["headlines"] = sorted(set(x for x in row["headlines"] if x))
        row["source_ids"] = sorted(row["source_ids"])
        row["source_contract_ids"] = sorted(row["source_contract_ids"])
        row["source_cohort_ids"] = sorted(row["source_cohort_ids"])
        row["terms"] = sorted(row["terms"])
        row["episode_topic_count"] = len(row["topic_ids"])
        result.append(row)
    return sorted(result, key=lambda row: (row["first_known_utc"], row["episode_id"], row["currency"]))


def load_technical_signals(path: Path, observed: dt.datetime) -> tuple[dict[str, dict[str, Any]], str]:
    payload = load_json(path)
    updated = parse_utc(payload.get("updated_at"))
    if updated is None or (observed - updated).total_seconds() > MAX_SIGNAL_AGE_SEC:
        return {}, "stale_or_missing"
    signals: dict[str, dict[str, Any]] = {}
    for row in payload.get("top_signals") or []:
        if not isinstance(row, Mapping):
            continue
        instrument = str(row.get("instrument") or "")
        side = normalized_direction(row.get("direction_state") or row.get("direction"))
        if not instrument or side == "neutral":
            continue
        signals[instrument] = {
            "instrument": instrument,
            "direction": side,
            "confidence": safe_float(row.get("signal_confidence"), 0.5),
            "projected_net_pips": safe_float(row.get("projected_net_pips"), 0.0),
            "horizon_min": max(1, int(safe_float(row.get("preferred_horizon_sec") or row.get("execution_horizon_sec"), 3600.0) / 60.0)),
            "signal_eligible": bool(row.get("signal_eligible")),
            "source_id": str(row.get("signal_id") or row.get("candidate_id") or row.get("id") or ""),
            "payload": dict(row),
        }
    return signals, "fresh"


def load_quotes(path: Path, observed: dt.datetime) -> tuple[dict[str, dict[str, Any]], str]:
    payload = load_json(path)
    result: dict[str, dict[str, Any]] = {}
    for instrument, row in (payload.get("quotes") or {}).items():
        if not isinstance(row, Mapping):
            continue
        quote_time = parse_utc(row.get("time"))
        bid, ask = safe_float(row.get("bid")), safe_float(row.get("ask"))
        pip = safe_float(row.get("pip"), 0.01 if str(instrument).endswith("_JPY") else 0.0001)
        if quote_time is None or bid <= 0 or ask <= bid or pip <= 0:
            continue
        age = (observed - quote_time).total_seconds()
        result[str(instrument)] = {
            "bid": bid, "ask": ask, "mid": (bid + ask) / 2.0, "pip": pip,
            "time": iso(quote_time), "age_sec": age,
            "spread_pips": (ask - bid) / pip,
            "fresh": -5.0 <= age <= MAX_QUOTE_AGE_SEC,
        }
    state = "fresh" if result and any(row["fresh"] for row in result.values()) else "closed_or_stale"
    return result, state


def rebase_quotes_to_post_read_clock(
    quotes: Mapping[str, Mapping[str, Any]],
    observed: dt.datetime,
) -> tuple[dict[str, dict[str, Any]], str, dict[str, Any]]:
    """Recompute quote ages only after every bounded input snapshot is read.

    The quote payload is already an immutable in-memory copy at this point.
    Any quote timestamp later than the post-read decision clock fails closed;
    a cycle-start timestamp is never allowed to make later-read input appear
    available earlier than it was.
    """

    rebased: dict[str, dict[str, Any]] = {}
    negative_age_instruments: list[str] = []
    for instrument, source in quotes.items():
        row = dict(source)
        quote_time = parse_utc(row.get("time"))
        if quote_time is None:
            row["age_sec"] = None
            row["fresh"] = False
        else:
            age = (observed - quote_time).total_seconds()
            row["age_sec"] = age
            row["fresh"] = 0.0 <= age <= MAX_QUOTE_AGE_SEC
            if age < 0.0:
                negative_age_instruments.append(str(instrument))
        rebased[str(instrument)] = row
    state = (
        "fresh"
        if rebased and any(row.get("fresh") is True for row in rebased.values())
        else "closed_or_stale"
    )
    return rebased, state, {
        "contract_id": "watchlist_post_input_read_quote_age_v1_20260904",
        "quote_count": len(rebased),
        "negative_quote_age_count": len(negative_age_instruments),
        "negative_quote_age_instruments": negative_age_instruments,
        "all_negative_quote_ages_fail_closed": True,
    }


def load_opportunity_forecasts(
    path: Path,
    observed: dt.datetime,
    required_cohort_id: str,
) -> tuple[dict[tuple[str, int], dict[str, Any]], str]:
    """Load the latest causal H5/H15/H30 magnitude forecast per instrument.

    This source is research-only.  It supplies pair/horizon ranking while news
    retains responsibility for direction; its weak directional output is kept
    separately for an explicitly confirmed ablation arm.
    """

    if not path.exists() or not required_cohort_id:
        return {}, "missing"
    try:
        connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT forecast_id,cohort_id,issued_at_utc,instrument,horizon_sec,
                   predicted_clear_probability,predicted_up_probability,
                   predicted_direction,predicted_direction_confidence,
                   predicted_magnitude_pips,predicted_ev_pips,
                   entry_spread_pips,modeled_entry_cost_pips
            FROM forecasts
            WHERE cohort_id = ?
              AND issued_at_utc <= ? AND issued_at_utc >= ?
              AND horizon_sec IN (300,900,1800)
            ORDER BY issued_at_utc DESC,forecast_id
            """,
            (
                required_cohort_id,
                iso(observed),
                iso(observed - dt.timedelta(seconds=MAX_OPPORTUNITY_AGE_SEC)),
            ),
        ).fetchall()
        connection.close()
    except sqlite3.Error:
        return {}, "unavailable"
    result: dict[tuple[str, int], dict[str, Any]] = {}
    newest_age = math.inf
    for row in rows:
        key = (str(row["instrument"]), int(row["horizon_sec"]))
        if key in result:
            continue
        issued = parse_utc(row["issued_at_utc"])
        if issued is None:
            continue
        age = max(0.0, (observed - issued).total_seconds())
        newest_age = min(newest_age, age)
        direction_value = int(safe_float(row["predicted_direction"], 0.0))
        result[key] = {
            "forecast_id": str(row["forecast_id"]),
            "cohort_id": str(row["cohort_id"]),
            "issued_at_utc": iso(issued),
            "age_sec": round(age, 6),
            "instrument": key[0],
            "horizon_sec": key[1],
            "predicted_clear_probability": safe_float(
                row["predicted_clear_probability"]
            ),
            "predicted_up_probability": safe_float(row["predicted_up_probability"]),
            "predicted_direction": (
                "long" if direction_value > 0 else "short" if direction_value < 0 else "neutral"
            ),
            "predicted_direction_confidence": safe_float(
                row["predicted_direction_confidence"]
            ),
            "predicted_magnitude_pips": safe_float(row["predicted_magnitude_pips"]),
            "predicted_ev_pips": safe_float(row["predicted_ev_pips"]),
            "entry_spread_pips": safe_float(row["entry_spread_pips"]),
            "modeled_entry_cost_pips": safe_float(row["modeled_entry_cost_pips"]),
        }
    return result, "fresh" if result and newest_age <= MAX_OPPORTUNITY_AGE_SEC else "stale"


def opportunity_source_is_retired(contract: Mapping[str, Any]) -> bool:
    """Return true when the upstream ranker is intentionally drain-only.

    The opportunity ranker is a retired research producer.  Its immutable
    database remains available for outcome maturation and historical audits,
    but the live watchlist must not repeatedly scan that large database or
    present old forecasts as a current input after future production stops.
    """

    runtime_mode = str(contract.get("runtime_mode") or "").strip().lower()
    future_production = contract.get("future_forecast_production")
    return runtime_mode in {"mature_only", "drain_only", "retired"} or (
        future_production is False
    )


def lookup_macro_context(factor: Mapping[str, Any], database: Path, observed: dt.datetime) -> dict[str, Any]:
    result = {
        "state": "missing_pre_release_consensus",
        "causal_valid": False,
        "actual_value": None,
        "consensus_value": None,
        "standardized_surprise": None,
        "directional_interpretation": "unknown",
    }
    if not database.exists():
        return result
    series = str(factor.get("event_series_id") or "")
    scheduled = str(factor.get("scheduled_utc") or "")
    if not series and not scheduled:
        result["state"] = "unlinked_event_series"
        return result
    connection = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    query = """
        SELECT * FROM macro_release_revisions
        WHERE causal_known_utc <= ? AND known_before_recorded_timestamp = 1
          AND actual_value IS NOT NULL AND consensus_value IS NOT NULL
    """
    params: list[Any] = [iso(observed)]
    if series:
        query += " AND event_series_id = ?"
        params.append(series)
    elif scheduled:
        query += " AND scheduled_utc = ?"
        params.append(scheduled)
    query += " ORDER BY causal_known_utc DESC,row_id DESC LIMIT 1"
    row = connection.execute(query, params).fetchone()
    connection.close()
    if row is None:
        return result
    try:
        source_payload = json.loads(str(row["payload_json"] or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        source_payload = {}
    if not prospective_collector_provenance(source_payload):
        result["state"] = "unbound_macro_source_provenance"
        return result
    result.update(
        {
            "state": "causal_actual_consensus_available",
            "causal_valid": True,
            "actual_value": row["actual_value"],
            "consensus_value": row["consensus_value"],
            "standardized_surprise": row["standardized_surprise"],
            "directional_interpretation": str(row["directional_interpretation"] or "unknown"),
            "release_key": str(row["release_key"]),
        }
    )
    return result


def lookup_rate_context(currency: str, path: Path, observed: dt.datetime) -> dict[str, Any]:
    payload = load_json(path)
    row = (payload.get("currencies") or {}).get(currency)
    if not isinstance(row, Mapping):
        return {"state": "missing_rate_repricing", "causal_valid": False}
    known = parse_utc(row.get("observed_utc"))
    if known is None or known > observed or (observed - known).total_seconds() > 900:
        return {"state": "stale_rate_repricing", "causal_valid": False}
    provenance_bound = bool(
        row.get("observation_clock_trusted") is True
        and str(row.get("observation_time_contract_id") or "")
        == OBSERVATION_TIME_CONTRACT_ID
        and str(row.get("collector_cohort_id") or "")
        == RATE_REPRICING_CLOCK_BOUND_COHORT_ID
    )
    if not provenance_bound:
        return {
            "state": "unbound_rate_repricing_provenance",
            "causal_valid": False,
        }
    return {
        "state": "available",
        "causal_valid": True,
        "observed_utc": iso(known),
        "change_bps_15m": safe_float(row.get("change_bps_15m")),
        "change_bps_60m": safe_float(row.get("change_bps_60m")),
        "instrument": str(row.get("instrument") or ""),
        "source_id": str(row.get("source_id") or ""),
    }


def lookup_daily_rate_context(
    currency: str,
    path: Path,
    observed: dt.datetime,
) -> dict[str, Any]:
    """Return point-in-time slow rate context without confirming direction.

    Daily sovereign yields and once-daily OIS curves are useful regime fields,
    but they are not release-window policy repricing.  Bootstrap histories are
    retained as current-view context and remain ineligible for proof.
    """

    payload = load_json(path)
    row = (payload.get("currencies") or {}).get(currency)
    if not isinstance(row, Mapping):
        return {
            "state": "missing_daily_rate_context",
            "causal_valid": False,
            "intraday_rate_confirmation": False,
            "direction_policy": "abstain",
        }
    known = parse_utc(row.get("observed_utc"))
    if known is None or known > observed:
        return {
            "state": "not_known_at_decision_cutoff",
            "causal_valid": False,
            "intraday_rate_confirmation": False,
            "direction_policy": "abstain",
        }
    prospective = bool(row.get("prospective_eligible"))
    provenance_bound = bool(
        row.get("observation_clock_trusted") is True
        and str(row.get("observation_time_contract_id") or "")
        == OBSERVATION_TIME_CONTRACT_ID
        and str(row.get("collector_cohort_id") or "")
        == DAILY_RATE_CLOCK_BOUND_COHORT_ID
    )
    rate_date_text = str(row.get("rate_date") or "")
    try:
        rate_date = dt.date.fromisoformat(rate_date_text)
    except ValueError:
        rate_date = None
    rate_age_days = (
        (observed.date() - rate_date).days if rate_date is not None else None
    )
    underlying_fresh = bool(
        rate_age_days is not None and 0 <= rate_age_days <= 7
    )
    return {
        "state": (
            "stale_daily_rate_context"
            if not underlying_fresh
            else "unbound_daily_rate_provenance"
            if prospective and not provenance_bound
            else "prospective_daily_context"
            if prospective and provenance_bound
            else "bootstrap_current_view"
        ),
        "causal_valid": bool(
            prospective and provenance_bound and underlying_fresh
        ),
        "proof_eligible": bool(
            prospective and provenance_bound and underlying_fresh
        ),
        "source_provenance_bound": provenance_bound,
        "intraday_rate_confirmation": False,
        "direction_policy": "abstain",
        "observed_utc": iso(known),
        "rate_date": rate_date_text,
        "rate_age_calendar_days": rate_age_days,
        "underlying_observation_fresh": underlying_fresh,
        "rate_pct": safe_float(row.get("rate_pct")),
        "change_bps_1d": safe_float(row.get("change_bps_1d")),
        "source_id": str(row.get("source_id") or ""),
        "source_contract_id": str(row.get("source_contract_id") or ""),
        "cohort_id": str(row.get("cohort_id") or ""),
        "observation_kind": str(row.get("observation_kind") or ""),
        "comparison_group": str(row.get("comparison_group") or ""),
        "tenor_label": str(row.get("tenor_label") or ""),
        "rate_measure": str(row.get("rate_measure") or ""),
    }


def quote_history_rows(path: Path) -> dict[str, list[dict[str, Any]]]:
    payload = load_json(path)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in payload.get("rows") or []:
        if isinstance(row, Mapping):
            grouped[str(row.get("instrument") or "")].append(dict(row))
    for rows in grouped.values():
        rows.sort(key=lambda row: safe_float(row.get("minute_epoch")))
    return grouped


def priced_in_proxy(
    instrument: str,
    direction: str,
    event_time: dt.datetime | None,
    quote: Mapping[str, Any],
    history: Mapping[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    if event_time is None or instrument not in history:
        return {"state": "history_unavailable", "causal_valid": False}
    target = event_time.timestamp() - 15 * 60
    prior = [row for row in history[instrument] if safe_float(row.get("minute_epoch")) <= target]
    if not prior:
        return {"state": "history_unavailable", "causal_valid": False}
    row = prior[-1]
    prior_mid = (safe_float(row.get("close_bid")) + safe_float(row.get("close_ask"))) / 2.0
    pip = safe_float(quote.get("pip"))
    if prior_mid <= 0 or pip <= 0:
        return {"state": "history_unavailable", "causal_valid": False}
    sign = 1.0 if direction == "long" else -1.0
    signed_move = sign * (safe_float(quote.get("mid")) - prior_mid) / pip
    threshold = max(3.0, 2.0 * safe_float(quote.get("spread_pips")))
    state = "possibly_priced_in" if signed_move >= threshold else (
        "pre_reaction_conflict" if signed_move <= -threshold else "not_detected"
    )
    return {
        "state": state,
        "causal_valid": True,
        "lookback_min": 15,
        "signed_pre_event_move_pips": round(signed_move, 6),
        "threshold_pips": round(threshold, 6),
    }


def remaining_move_assessment(
    model: Mapping[str, Any],
    priced: Mapping[str, Any],
    *,
    expected_direction: str,
    factor_age_sec: float | None,
    factor_horizon_min: int,
    modeled_cost_pips: float,
) -> dict[str, Any]:
    """Conservatively estimate movement still available after observation.

    The opportunity model's gross magnitude is not credited in full when the
    news thesis is already old or when price moved in the thesis direction
    before the current decision.  This is a frozen shadow diagnostic only; it
    cannot authorize or promote a trade.
    """

    magnitude = max(0.0, safe_float(model.get("predicted_magnitude_pips")))
    clear_probability = min(
        1.0,
        max(0.0, safe_float(model.get("predicted_clear_probability"))),
    )
    cost = max(1e-9, safe_float(modeled_cost_pips))
    horizon_sec = max(60.0, float(max(1, factor_horizon_min) * 60))
    age_sec = max(0.0, safe_float(factor_age_sec))
    remaining_time_fraction = max(0.0, min(1.0, 1.0 - age_sec / horizon_sec))
    history_valid = priced.get("causal_valid") is True
    underway_signed = (
        safe_float(priced.get("signed_pre_event_move_pips"))
        if history_valid
        else 0.0
    )
    completed_in_thesis_direction = max(0.0, underway_signed)
    path_remaining = max(0.0, magnitude - completed_in_thesis_direction)
    age_decayed_magnitude = max(0.0, magnitude * remaining_time_fraction)
    conservative_remaining = min(path_remaining, age_decayed_magnitude)
    required_remaining = cost * REMAINING_MOVE_COST_SAFETY_MULTIPLE
    remaining_to_cost = conservative_remaining / cost
    reasons: list[str] = []
    if not history_valid:
        reasons.append("underway_history_unavailable")
    if str(priced.get("state") or "") == "possibly_priced_in":
        reasons.append("possibly_priced_in")
    if str(priced.get("state") or "") == "pre_reaction_conflict":
        reasons.append("pre_reaction_conflict")
    if remaining_time_fraction <= 0.0:
        reasons.append("declared_reaction_horizon_elapsed")
    model_direction = str(model.get("predicted_direction") or "neutral")
    if model_direction not in {"long", "short"}:
        reasons.append("model_direction_unavailable")
    elif model_direction != expected_direction:
        reasons.append("model_direction_conflicts_with_news")
    if clear_probability < REMAINING_MOVE_MIN_CLEAR_PROBABILITY:
        reasons.append("low_cost_clearance_probability")
    if conservative_remaining < required_remaining:
        reasons.append("insufficient_remaining_move_after_cost_margin")
    return {
        "assessment_contract_id": "news_remaining_move_cost_clearance_v1_20260824",
        "eligible": not reasons,
        "negative_control_reasons": reasons,
        "predicted_gross_magnitude_pips": round(magnitude, 6),
        "underway_signed_move_pips": round(underway_signed, 6),
        "completed_in_thesis_direction_pips": round(
            completed_in_thesis_direction, 6
        ),
        "path_remaining_magnitude_pips": round(path_remaining, 6),
        "age_decayed_magnitude_pips": round(age_decayed_magnitude, 6),
        "conservative_remaining_magnitude_pips": round(
            conservative_remaining, 6
        ),
        "remaining_time_fraction": round(remaining_time_fraction, 9),
        "modeled_cost_pips": round(cost, 6),
        "required_cost_clearance_multiple": REMAINING_MOVE_COST_SAFETY_MULTIPLE,
        "required_remaining_magnitude_pips": round(required_remaining, 6),
        "remaining_magnitude_to_cost": round(remaining_to_cost, 9),
        "predicted_clear_probability": round(clear_probability, 9),
        "minimum_clear_probability": REMAINING_MOVE_MIN_CLEAR_PROBABILITY,
        "research_only": True,
        "execution_eligible": False,
    }


def initial_reaction_state(
    instrument: str,
    direction: str,
    event_time: dt.datetime | None,
    quote: Mapping[str, Any],
    history: Mapping[str, list[dict[str, Any]]],
    *,
    window_min: int = 5,
) -> dict[str, Any]:
    """Measure the first executable-direction response after a news clock."""

    rows = history.get(instrument) or []
    if event_time is None or not rows:
        return {"state": "history_unavailable", "causal_valid": False}
    event_epoch = event_time.timestamp()
    prior = [row for row in rows if safe_float(row.get("minute_epoch")) < event_epoch]
    post = [
        row
        for row in rows
        if event_epoch <= safe_float(row.get("minute_epoch"))
        <= event_epoch + max(1, window_min) * 60
    ]
    if not prior or not post:
        return {"state": "awaiting_first_reaction", "causal_valid": False}
    before = prior[-1]
    after = post[-1]
    before_mid = (
        safe_float(before.get("close_bid")) + safe_float(before.get("close_ask"))
    ) / 2.0
    after_mid = (
        safe_float(after.get("close_bid")) + safe_float(after.get("close_ask"))
    ) / 2.0
    pip = safe_float(quote.get("pip"))
    if before_mid <= 0 or after_mid <= 0 or pip <= 0:
        return {"state": "history_unavailable", "causal_valid": False}
    sign = 1.0 if direction == "long" else -1.0
    signed_pips = sign * (after_mid - before_mid) / pip
    threshold = max(1.0, safe_float(quote.get("spread_pips")))
    state = (
        "aligned"
        if signed_pips >= threshold
        else "conflicted"
        if signed_pips <= -threshold
        else "insufficient"
    )
    return {
        "state": state,
        "causal_valid": True,
        "window_min": max(1, window_min),
        "signed_move_pips": round(signed_pips, 6),
        "clearance_threshold_pips": round(threshold, 6),
        "last_reaction_minute_epoch": int(safe_float(after.get("minute_epoch"))),
    }


def persistent_sma_confirmation(
    instrument: str,
    direction: str,
    history: Mapping[str, list[dict[str, Any]]],
    *,
    fast: int = 5,
    slow: int = 60,
    persistence_bars: int = 3,
) -> dict[str, Any]:
    """Require a persistent SMA state and reject immediate recross noise."""

    rows = history.get(instrument) or []
    mids = [
        (safe_float(row.get("close_bid")) + safe_float(row.get("close_ask"))) / 2.0
        for row in rows
        if safe_float(row.get("close_bid")) > 0
        and safe_float(row.get("close_ask")) > safe_float(row.get("close_bid"))
    ]
    required = slow + max(1, persistence_bars) - 1
    if len(mids) < required:
        return {"state": "history_unavailable", "causal_valid": False}
    signs: list[int] = []
    for end in range(len(mids) - persistence_bars + 1, len(mids) + 1):
        window = mids[:end]
        fast_value = sum(window[-fast:]) / fast
        slow_value = sum(window[-slow:]) / slow
        signs.append(1 if fast_value > slow_value else -1 if fast_value < slow_value else 0)
    expected = 1 if direction == "long" else -1
    aligned = bool(signs and all(value == expected for value in signs))
    conflicted = bool(signs and all(value == -expected for value in signs))
    state = "aligned_persistent" if aligned else "conflicted_persistent" if conflicted else "unstable_or_recrossed"
    return {
        "state": state,
        "causal_valid": True,
        "fast_bars": fast,
        "slow_bars": slow,
        "persistence_bars": persistence_bars,
        "recent_signs": signs,
    }


def pair_side(currency: str, score: float, instrument: str) -> str:
    parts = instrument.split("_")
    if len(parts) != 2 or currency not in parts or not score:
        return "neutral"
    currency_up = score > 0
    pair_up = currency_up if parts[0] == currency else not currency_up
    return "long" if pair_up else "short"


def episode_currency_score_matrix(
    factors: Iterable[Mapping[str, Any]],
) -> dict[str, dict[str, float]]:
    """Build one deterministic currency-score vector per causal episode.

    Repeated identical rows do not add weight. Conflicting distinct scores for
    one episode/currency are averaged, making unresolved disagreement smaller
    rather than allowing whichever row happened to be iterated last to win.
    """

    values: dict[str, dict[str, set[float]]] = {}
    for factor in factors:
        episode = str(factor.get("episode_id") or "").strip()
        currency = str(factor.get("currency") or "").strip().upper()
        if not episode or not currency:
            continue
        score = round(safe_float(factor.get("score")), 12)
        values.setdefault(episode, {}).setdefault(currency, set()).add(score)
    return {
        episode: {
            currency: sum(sorted(scores)) / len(scores)
            for currency, scores in sorted(by_currency.items())
            if scores
        }
        for episode, by_currency in sorted(values.items())
    }


def pair_news_score_differential(
    currency_scores: Mapping[str, float], instrument: str
) -> dict[str, Any]:
    """Express one episode as signed base score minus quote score.

    The deterministic owner prevents the same episode/pair from being emitted
    once from each leg. Equal scores cancel exactly and produce no direction.
    """

    parts = str(instrument).upper().split("_")
    if len(parts) != 2:
        return {
            "contract_id": PAIR_SCORE_CONTRACT_ID,
            "base_currency": "",
            "quote_currency": "",
            "base_score": 0.0,
            "quote_score": 0.0,
            "differential": 0.0,
            "direction": "neutral",
            "owner_currency": "",
        }
    base, quote = parts
    base_score = safe_float(currency_scores.get(base))
    quote_score = safe_float(currency_scores.get(quote))
    differential = base_score - quote_score
    if abs(differential) <= 1e-12:
        direction = "neutral"
    else:
        direction = "long" if differential > 0.0 else "short"
    scored_legs = [
        currency
        for currency, score in ((base, base_score), (quote, quote_score))
        if abs(score) > 1e-12
    ]
    owner = (
        sorted(scored_legs, key=lambda item: (-abs(
            base_score if item == base else quote_score
        ), item))[0]
        if scored_legs
        else ""
    )
    return {
        "contract_id": PAIR_SCORE_CONTRACT_ID,
        "base_currency": base,
        "quote_currency": quote,
        "base_score": base_score,
        "quote_score": quote_score,
        "differential": differential,
        "direction": direction,
        "owner_currency": owner,
    }


def signed_currency_factors(instrument: str, direction: str) -> list[str]:
    """Return the two signed currency exposures represented by a pair trade."""
    parts = str(instrument).upper().split("_")
    side = str(direction).lower()
    if len(parts) != 2 or side not in {"long", "short"}:
        return []
    base_side = side
    quote_side = "short" if side == "long" else "long"
    return [f"{parts[0]}:{base_side}", f"{parts[1]}:{quote_side}"]


def current_technical_factor_clusters(entries: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Cluster current technical rows that express any shared signed factor.

    The pair rows remain intact for diagnostics.  This additional view prevents
    correlated crosses (for example long USD/JPY and long EUR/JPY) from looking
    like independent confirmations when both primarily express short JPY.
    """
    rows = [dict(row) for row in entries if row.get("arm") == "technical_only"]
    factor_sets = [
        set(row.get("signed_currency_factors") or signed_currency_factors(
            str(row.get("instrument", "")), str(row.get("direction", ""))
        ))
        for row in rows
    ]
    parent = list(range(len(rows)))

    def root(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root, right_root = root(left), root(right)
        if left_root != right_root:
            parent[right_root] = left_root

    for left in range(len(rows)):
        for right in range(left + 1, len(rows)):
            if factor_sets[left] & factor_sets[right]:
                union(left, right)

    grouped: dict[int, list[int]] = defaultdict(list)
    for index in range(len(rows)):
        grouped[root(index)].append(index)
    clusters = []
    for indices in grouped.values():
        clusters.append(
            {
                "factor_keys": sorted(set().union(*(factor_sets[index] for index in indices))),
                "instruments": sorted(str(rows[index].get("instrument", "")) for index in indices),
                "entry_count": len(indices),
            }
        )
    clusters.sort(key=lambda row: (row["instruments"], row["factor_keys"]))
    return {
        "raw_entry_count": len(rows),
        "cluster_count": len(clusters),
        "clusters": clusters,
    }


def verification_state(macro: Mapping[str, Any], rates: Mapping[str, Any], priced: Mapping[str, Any]) -> str:
    if priced.get("state") == "pre_reaction_conflict":
        return "reaction_conflict"
    if macro.get("causal_valid") and rates.get("causal_valid"):
        return "causally_confirmed"
    missing = []
    if not macro.get("causal_valid"):
        missing.append("consensus")
    if not rates.get("causal_valid"):
        missing.append("rates")
    return "missing_" + "_and_".join(missing)


def macro_interpretation_direction(value: Any) -> str:
    """Return an explicit currency-strength direction or abstain."""

    text = str(value or "").strip().lower()
    if any(
        token in text
        for token in (
            "weakening",
            "weaker",
            "depreciat",
            "currency_negative",
            "currency-negative",
            "bearish",
            "dovish",
        )
    ):
        return "short"
    if any(
        token in text
        for token in (
            "strengthening",
            "stronger",
            "appreciat",
            "currency_positive",
            "currency-positive",
            "bullish",
            "hawkish",
        )
    ):
        return "long"
    return "neutral"


def official_surprise_rate_assessment(
    macro: Mapping[str, Any],
    rates: Mapping[str, Any],
    *,
    expected_currency_direction: str,
) -> dict[str, Any]:
    """Bind causal surprise direction to event-time rate repricing.

    This is a research-only confirmation contract. Missing, unstandardized, or
    directionally conflicted inputs are retained as negative controls and can
    never self-authorize a trade.
    """

    reasons: list[str] = []
    if macro.get("causal_valid") is not True:
        reasons.append("causal_pre_release_consensus_unavailable")
    standardized = macro.get("standardized_surprise")
    if not isinstance(standardized, (int, float)) or isinstance(
        standardized, bool
    ) or not math.isfinite(float(standardized)):
        surprise_z = None
        reasons.append("standardized_surprise_unavailable")
    else:
        surprise_z = float(standardized)
        if abs(surprise_z) < OFFICIAL_SURPRISE_RATE_MIN_ABS_Z:
            reasons.append("standardized_surprise_below_threshold")
    macro_direction = macro_interpretation_direction(
        macro.get("directional_interpretation")
    )
    if macro_direction == "neutral":
        reasons.append("series_direction_semantics_unavailable")
    elif macro_direction != expected_currency_direction:
        reasons.append("numeric_direction_conflicts_with_source_thesis")

    if rates.get("causal_valid") is not True:
        reasons.append("event_time_rate_repricing_unavailable")
    rate_change = rates.get("change_bps_15m")
    if not isinstance(rate_change, (int, float)) or isinstance(
        rate_change, bool
    ) or not math.isfinite(float(rate_change)):
        rate_change_bps = None
        rate_direction = "neutral"
        reasons.append("rate_repricing_value_unavailable")
    else:
        rate_change_bps = float(rate_change)
        rate_direction = (
            "long"
            if rate_change_bps > 0.0
            else "short"
            if rate_change_bps < 0.0
            else "neutral"
        )
        if abs(rate_change_bps) < OFFICIAL_SURPRISE_RATE_MIN_ABS_CHANGE_BPS:
            reasons.append("rate_repricing_below_threshold")
        if rate_direction != expected_currency_direction:
            reasons.append("rate_repricing_conflicts_with_source_thesis")
    return {
        "assessment_contract_id": (
            "official_surprise_rate_binding_v1_20260824"
        ),
        "eligible": not reasons,
        "negative_control_reasons": reasons,
        "expected_currency_direction": expected_currency_direction,
        "macro_direction": macro_direction,
        "standardized_surprise": surprise_z,
        "minimum_absolute_standardized_surprise": (
            OFFICIAL_SURPRISE_RATE_MIN_ABS_Z
        ),
        "rate_direction": rate_direction,
        "rate_change_bps_15m": rate_change_bps,
        "minimum_absolute_rate_change_bps_15m": (
            OFFICIAL_SURPRISE_RATE_MIN_ABS_CHANGE_BPS
        ),
        "research_only": True,
        "execution_eligible": False,
    }


def thesis_lifecycle_state(
    *,
    observed: dt.datetime,
    expires: dt.datetime | None,
    news_direction: str,
    technical_direction: str,
    priced: Mapping[str, Any],
) -> dict[str, str]:
    """Describe the current thesis evidence state without changing any gate."""

    if expires is not None and observed > expires:
        return {"state": "expired", "basis": "declared_reaction_horizon_elapsed"}
    if str(priced.get("state") or "") == "pre_reaction_conflict":
        return {"state": "contradicted", "basis": "predecision_price_conflict"}
    if technical_direction in {"long", "short"}:
        if technical_direction == news_direction:
            return {"state": "price_aligned", "basis": "technical_direction"}
        return {"state": "contradicted", "basis": "technical_direction"}
    if str(priced.get("state") or "") == "possibly_priced_in":
        return {"state": "price_aligned", "basis": "predecision_price_move"}
    return {"state": "active_unresolved", "basis": "no_price_confirmation"}


def entry_identifier(
    episode: str,
    instrument: str,
    arm: str,
    direction: str,
    horizon: int,
    cohort_id: str = COHORT_ID,
) -> str:
    raw = f"{cohort_id}|{episode}|{instrument}|{arm}|{direction}|{horizon}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def build_watchlist(
    factors: list[dict[str, Any]],
    technical: Mapping[str, dict[str, Any]],
    quotes: Mapping[str, dict[str, Any]],
    history: Mapping[str, list[dict[str, Any]]],
    observed: dt.datetime,
    macro_db: Path = MACRO_DB,
    rates_path: Path = RATES,
    policy_state: Mapping[str, Any] | None = None,
    opportunity: Mapping[tuple[str, int], Mapping[str, Any]] | None = None,
    daily_rates_path: Path = DAILY_RATES,
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    used_technical: set[str] = set()
    episode_scores = episode_currency_score_matrix(factors)
    processed_episode_currencies: set[tuple[str, str]] = set()
    for factor in factors:
        currency = str(factor["currency"])
        episode_id = str(factor["episode_id"])
        episode_currency_key = (episode_id, currency)
        if episode_currency_key in processed_episode_currencies:
            continue
        processed_episode_currencies.add(episode_currency_key)
        source_currency_score = safe_float(
            episode_scores.get(episode_id, {}).get(currency)
        )
        factor_first_known = parse_utc(factor.get("first_known_utc"))
        factor_horizon_min = max(1, int(safe_float(factor.get("horizon_min"), DEFAULT_HORIZON_MIN)))
        factor_age_sec = (
            max(0.0, (observed - factor_first_known).total_seconds())
            if factor_first_known is not None
            else None
        )
        factor_expires = (
            factor_first_known + dt.timedelta(minutes=factor_horizon_min)
            if factor_first_known is not None
            else None
        )
        factor_provenance = {
            "news_factor_first_known_utc": (
                iso(factor_first_known) if factor_first_known is not None else ""
            ),
            "news_factor_age_sec": factor_age_sec,
            "news_factor_expires_utc": (
                iso(factor_expires) if factor_expires is not None else ""
            ),
            "news_factor_article_count": int(
                safe_float(factor.get("article_count"), 0.0)
            ),
            "news_factor_publisher_count": int(
                safe_float(factor.get("publisher_count"), 0.0)
            ),
            "news_factor_episode_topic_count": int(
                safe_float(factor.get("episode_topic_count"), 0.0)
            ),
            "news_factor_source_ids": list(factor.get("source_ids") or []),
            "news_factor_source_contract_ids": list(
                factor.get("source_contract_ids") or []
            ),
            "news_factor_source_cohort_ids": list(
                factor.get("source_cohort_ids") or []
            ),
            # Repeated articles increase attention diagnostics but do not
            # change first-known time or extend the frozen reaction horizon.
            "news_repetition_refreshes_causal_age": False,
        }
        persistent_policy = (
            (policy_state or {}).get("currencies", {}).get(currency)
            if isinstance((policy_state or {}).get("currencies"), Mapping)
            else None
        )
        macro = lookup_macro_context(factor, macro_db, observed)
        rates = lookup_rate_context(currency, rates_path, observed)
        daily_rates = lookup_daily_rate_context(
            currency, daily_rates_path, observed
        )
        pair_candidates: list[tuple[float, str, str, dict[str, Any]]] = []
        pair_score_by_instrument: dict[str, dict[str, Any]] = {}
        for instrument, quote in quotes.items():
            pair_score = pair_news_score_differential(
                episode_scores.get(episode_id, {}), instrument
            )
            side = str(pair_score["direction"])
            if (
                side == "neutral"
                or str(pair_score["owner_currency"]) != currency
                or not quote.get("fresh")
            ):
                continue
            spread = safe_float(quote.get("spread_pips"), 999999.0)
            if spread > MAX_ENTRY_SPREAD_PIPS:
                continue
            pair_score_by_instrument[instrument] = pair_score
            pair_candidates.append((spread, instrument, side, quote))
        pair_limit = (
            MAX_UNCORROBORATED_PAIR_LEGS_PER_FACTOR
            if bool(factor.get("uncorroborated_research"))
            else MAX_PAIR_LEGS_PER_FACTOR
        )
        reaction_by_instrument = {
            instrument: initial_reaction_state(
                instrument,
                side,
                factor_first_known,
                quote,
                history,
            )
            for _, instrument, side, quote in pair_candidates
        }
        aligned_reaction_count = sum(
            1
            for reaction in reaction_by_instrument.values()
            if reaction.get("state") == "aligned"
        )
        for _, instrument, news_side, quote in sorted(pair_candidates)[:pair_limit]:
            tech = technical.get(instrument)
            if tech:
                used_technical.add(instrument)
            priced = priced_in_proxy(
                instrument, news_side, parse_utc(factor.get("first_known_utc")), quote, history
            )
            initial_reaction = reaction_by_instrument.get(instrument) or {
                "state": "history_unavailable",
                "causal_valid": False,
            }
            sma_confirmation = persistent_sma_confirmation(
                instrument,
                news_side,
                history,
            )
            verify = verification_state(macro, rates, priced)
            thesis_state = thesis_lifecycle_state(
                observed=observed,
                expires=factor_expires,
                news_direction=news_side,
                technical_direction=(
                    str(tech.get("direction") or "neutral") if tech else "neutral"
                ),
                priced=priced,
            )
            base = {
                **factor_provenance,
                "episode_id": episode_id,
                "currency": currency,
                "instrument": instrument,
                "horizon_min": int(factor["horizon_min"]),
                "news_direction": news_side,
                "news_score": safe_float(
                    pair_score_by_instrument[instrument]["differential"]
                ),
                "source_currency_score": source_currency_score,
                "episode_currency_scores": dict(
                    sorted(episode_scores.get(episode_id, {}).items())
                ),
                "pair_news_score_differential": dict(
                    pair_score_by_instrument[instrument]
                ),
                "pair_score_contract_id": PAIR_SCORE_CONTRACT_ID,
                "news_confidence": safe_float(factor["confidence"]),
                "technical_direction": tech.get("direction") if tech else "neutral",
                "technical_confidence": safe_float(tech.get("confidence")) if tech else None,
                "technical_expected_net_pips": safe_float(tech.get("projected_net_pips")) if tech else None,
                "technical_source_id": str(tech.get("source_id") or "") if tech else "",
                "macro_context": macro,
                "rate_context": rates,
                "daily_rate_context": daily_rates,
                "persistent_policy_context": persistent_policy
                or {"currency": currency, "state": "unavailable"},
                "priced_in_proxy": priced,
                "initial_reaction": initial_reaction,
                "initial_reaction_aligned_pair_count": aligned_reaction_count,
                "persistent_sma_confirmation": sma_confirmation,
                "verification_state": verify,
                "news_thesis_state": thesis_state["state"],
                "news_thesis_state_basis": thesis_state["basis"],
                "news_thesis_supersession_state": "not_evaluated",
                "topic_ids": factor["topic_ids"],
                "headlines": factor["headlines"],
                "terms": factor["terms"],
                "entry_quote": dict(quote),
                "research_only": True,
                "execution_eligible": False,
            }
            if bool(factor.get("uncorroborated_research")):
                if (
                    str(factor.get("category") or "")
                    in {
                        "risk_off_geopolitical_or_financial",
                        "risk_on_deescalation",
                        "commodity_shock",
                    }
                    and initial_reaction.get("state") != "aligned"
                ):
                    # Generic secondary risk templates are admitted only when
                    # the affected currency's executable pair response agrees.
                    # This preserves the AUD/NZD/CAD portion of a real shock
                    # while rejecting unsupported JPY/CHF propagation.
                    continue
                # Preserve three small-horizon hypotheses independently. They
                # are diagnostics of the source-to-currency mapping and have
                # no path into technical confirmation, magnitude ranking,
                # lifecycle promotion, authorization, or execution.
                for horizon in sorted(UNCORROBORATED_NEWS_COHORTS):
                    entries.append(
                        {
                            **base,
                            "arm": f"uncorroborated_news_response_h{horizon}",
                            "direction": news_side,
                            "horizon_min": horizon,
                            "cohort_id": UNCORROBORATED_NEWS_COHORTS[horizon],
                            "uncorroborated_research": True,
                            "execution_eligible": False,
                        }
                    )
                continue
            if bool(factor.get("official_release_research")):
                event_age_min = (
                    factor_age_sec / 60.0
                    if factor_age_sec is not None
                    else math.inf
                )
                currency_direction = (
                    "long" if source_currency_score > 0.0 else "short"
                )
                surprise_rate_assessment = official_surprise_rate_assessment(
                    macro,
                    rates,
                    expected_currency_direction=currency_direction,
                )
                surprise_rate_arm = (
                    "official_surprise_rate_confirmed_h15"
                    if surprise_rate_assessment["eligible"]
                    else "official_surprise_rate_negative_control_h15"
                )
                entries.append(
                    {
                        **base,
                        "arm": surprise_rate_arm,
                        "direction": news_side,
                        "horizon_min": 15,
                        "cohort_id": OFFICIAL_SURPRISE_RATE_COHORTS[
                            surprise_rate_arm
                        ],
                        "official_surprise_rate_assessment": (
                            surprise_rate_assessment
                        ),
                        "execution_eligible": False,
                    }
                )
                if (
                    event_age_min <= 10.0
                    and aligned_reaction_count >= 2
                    and initial_reaction.get("state") == "aligned"
                ):
                    entries.append(
                        {
                            **base,
                            "arm": "official_release_fast_multileg_h15",
                            "direction": news_side,
                            "horizon_min": 15,
                            "cohort_id": OFFICIAL_RELEASE_FAST_CONFIRMATION_COHORT_ID,
                            "direction_basis": "official_semantic_hypothesis_plus_multileg_first_reaction",
                            "execution_eligible": False,
                        }
                    )
                continue
            entries.append({**base, "arm": "news_only", "direction": news_side})
            if tech:
                if (
                    tech["direction"] == news_side
                    and sma_confirmation.get("state") == "aligned_persistent"
                ):
                    entries.append(
                        {
                            **base,
                            "arm": "news_technical_confirmed",
                            "direction": news_side,
                        }
                    )
                elif tech["direction"] != news_side or sma_confirmation.get("state") == "conflicted_persistent":
                    entries.append(
                        {
                            **base,
                            "arm": "news_technical_conflicted",
                            "direction": news_side,
                        }
                    )
        if bool(factor.get("uncorroborated_research")) or bool(
            factor.get("official_release_research")
        ):
            continue
        # Separate frozen short-horizon branches use the existing model only
        # for magnitude/cost ranking.  News remains the directional thesis.
        # A second ablation requires the weak price direction to agree, so its
        # incremental value can be measured rather than assumed.
        for horizon_sec, horizon_label in (
            (300, "h5"),
            (900, "h15"),
            (1800, "h30"),
        ):
            ranked: list[
                tuple[float, float, float, str, str, dict[str, Any], Mapping[str, Any]]
            ] = []
            for spread, instrument, news_side, quote in pair_candidates:
                model = (opportunity or {}).get((instrument, horizon_sec))
                if not model:
                    continue
                magnitude = safe_float(model.get("predicted_magnitude_pips"))
                cost = max(
                    spread,
                    safe_float(model.get("modeled_entry_cost_pips"), spread),
                    1e-9,
                )
                ranked.append(
                    (
                        -safe_float(model.get("predicted_clear_probability")),
                        -(magnitude / cost),
                        spread,
                        instrument,
                        news_side,
                        quote,
                        model,
                    )
                )
            selected = sorted(ranked)[:MAX_PAIR_LEGS_PER_FACTOR]
            for rank, (_, _, _, instrument, news_side, quote, model) in enumerate(
                selected,
                start=1,
            ):
                tech = technical.get(instrument)
                priced = priced_in_proxy(
                    instrument,
                    news_side,
                    parse_utc(factor.get("first_known_utc")),
                    quote,
                    history,
                )
                initial_reaction = reaction_by_instrument.get(instrument) or {
                    "state": "history_unavailable",
                    "causal_valid": False,
                }
                sma_confirmation = persistent_sma_confirmation(
                    instrument,
                    news_side,
                    history,
                )
                verify = verification_state(macro, rates, priced)
                thesis_state = thesis_lifecycle_state(
                    observed=observed,
                    expires=factor_expires,
                    news_direction=news_side,
                    technical_direction=(
                        str(tech.get("direction") or "neutral")
                        if tech
                        else "neutral"
                    ),
                    priced=priced,
                )
                model_fields = {
                    "opportunity_forecast_id": str(model.get("forecast_id") or ""),
                    "opportunity_cohort_id": str(model.get("cohort_id") or ""),
                    "opportunity_issued_at_utc": str(model.get("issued_at_utc") or ""),
                    "opportunity_age_sec": safe_float(model.get("age_sec")),
                    "opportunity_predicted_clear_probability": safe_float(
                        model.get("predicted_clear_probability")
                    ),
                    "opportunity_predicted_magnitude_pips": safe_float(
                        model.get("predicted_magnitude_pips")
                    ),
                    "opportunity_modeled_cost_pips": safe_float(
                        model.get("modeled_entry_cost_pips")
                    ),
                    "opportunity_predicted_direction": str(
                        model.get("predicted_direction") or "neutral"
                    ),
                    "opportunity_rank_within_currency_factor": rank,
                    "direction_source": "point_in_time_news",
                    "ranking_source": "cost_clearance_magnitude_model",
                }
                base = {
                    **factor_provenance,
                    "episode_id": episode_id,
                    "currency": currency,
                    "instrument": instrument,
                    "horizon_min": horizon_sec // 60,
                    "direction": news_side,
                    "news_direction": news_side,
                    "news_score": safe_float(
                        pair_score_by_instrument[instrument]["differential"]
                    ),
                    "source_currency_score": source_currency_score,
                    "episode_currency_scores": dict(
                        sorted(episode_scores.get(episode_id, {}).items())
                    ),
                    "pair_news_score_differential": dict(
                        pair_score_by_instrument[instrument]
                    ),
                    "pair_score_contract_id": PAIR_SCORE_CONTRACT_ID,
                    "news_confidence": safe_float(factor["confidence"]),
                    "technical_direction": tech.get("direction") if tech else "neutral",
                    "technical_confidence": safe_float(tech.get("confidence")) if tech else None,
                    "macro_context": macro,
                    "rate_context": rates,
                    "persistent_policy_context": persistent_policy
                    or {"currency": currency, "state": "unavailable"},
                    "priced_in_proxy": priced,
                    "initial_reaction": initial_reaction,
                    "persistent_sma_confirmation": sma_confirmation,
                    "verification_state": verify,
                    "news_thesis_state": thesis_state["state"],
                    "news_thesis_state_basis": thesis_state["basis"],
                    "news_thesis_supersession_state": "not_evaluated",
                    "topic_ids": factor["topic_ids"],
                    "headlines": factor["headlines"],
                    "terms": factor["terms"],
                    "entry_quote": dict(quote),
                    "research_only": True,
                    "execution_eligible": False,
                    **model_fields,
                }
                ranked_arm = f"news_magnitude_ranked_{horizon_label}"
                entries.append(
                    {
                        **base,
                        "arm": ranked_arm,
                        "cohort_id": NEWS_MAGNITUDE_COHORTS[ranked_arm],
                    }
                )
                confirmed_arm = (
                    f"news_magnitude_direction_confirmed_{horizon_label}"
                )
                if (
                    str(model.get("predicted_direction") or "neutral") == news_side
                    and initial_reaction.get("state") == "aligned"
                    and sma_confirmation.get("state") == "aligned_persistent"
                ):
                    entries.append(
                        {
                            **base,
                            "arm": confirmed_arm,
                            "cohort_id": NEWS_MAGNITUDE_COHORTS[confirmed_arm],
                        }
                    )

            # New immutable comparison: retain every currently liquid pair
            # expression for this currency factor, then separate genuinely
            # cost-clear remaining-move hypotheses from negative controls.
            # The predecessor top-three ranking above remains unchanged.
            comparison_candidates: list[
                tuple[
                    tuple[float, float, float, float, str],
                    float,
                    str,
                    str,
                    dict[str, Any],
                    Mapping[str, Any],
                    dict[str, Any],
                    dict[str, Any],
                ]
            ] = []
            for spread, instrument, news_side, quote in pair_candidates:
                model = (opportunity or {}).get((instrument, horizon_sec)) or {}
                cost = max(
                    spread,
                    safe_float(model.get("modeled_entry_cost_pips"), spread),
                    1e-9,
                )
                priced = priced_in_proxy(
                    instrument,
                    news_side,
                    factor_first_known,
                    quote,
                    history,
                )
                assessment = remaining_move_assessment(
                    model,
                    priced,
                    expected_direction=news_side,
                    factor_age_sec=factor_age_sec,
                    factor_horizon_min=horizon_sec // 60,
                    modeled_cost_pips=cost,
                )
                sort_key = (
                    0.0 if assessment["eligible"] else 1.0,
                    -safe_float(assessment["remaining_magnitude_to_cost"]),
                    -safe_float(model.get("predicted_clear_probability")),
                    spread,
                    instrument,
                )
                comparison_candidates.append(
                    (
                        sort_key,
                        spread,
                        instrument,
                        news_side,
                        quote,
                        model,
                        priced,
                        assessment,
                    )
                )
            comparison_candidates.sort(key=lambda row: row[0])
            expression_count = len(comparison_candidates)
            for expression_rank, (
                _,
                spread,
                instrument,
                news_side,
                quote,
                model,
                priced,
                assessment,
            ) in enumerate(comparison_candidates, start=1):
                tech = technical.get(instrument)
                initial_reaction = reaction_by_instrument.get(instrument) or {
                    "state": "history_unavailable",
                    "causal_valid": False,
                }
                sma_confirmation = persistent_sma_confirmation(
                    instrument,
                    news_side,
                    history,
                )
                verify = verification_state(macro, rates, priced)
                thesis_state = thesis_lifecycle_state(
                    observed=observed,
                    expires=factor_expires,
                    news_direction=news_side,
                    technical_direction=(
                        str(tech.get("direction") or "neutral")
                        if tech
                        else "neutral"
                    ),
                    priced=priced,
                )
                arm_kind = (
                    "cost_clear" if assessment["eligible"] else "negative_control"
                )
                arm = f"news_remaining_move_{arm_kind}_{horizon_label}"
                entries.append(
                    {
                        **factor_provenance,
                        "episode_id": episode_id,
                        "currency": currency,
                        "instrument": instrument,
                        "horizon_min": horizon_sec // 60,
                        "direction": news_side,
                        "news_direction": news_side,
                        "news_score": safe_float(
                            pair_score_by_instrument[instrument]["differential"]
                        ),
                        "source_currency_score": source_currency_score,
                        "episode_currency_scores": dict(
                            sorted(episode_scores.get(episode_id, {}).items())
                        ),
                        "pair_news_score_differential": dict(
                            pair_score_by_instrument[instrument]
                        ),
                        "pair_score_contract_id": PAIR_SCORE_CONTRACT_ID,
                        "news_confidence": safe_float(factor["confidence"]),
                        "technical_direction": (
                            tech.get("direction") if tech else "neutral"
                        ),
                        "technical_confidence": (
                            safe_float(tech.get("confidence")) if tech else None
                        ),
                        "macro_context": macro,
                        "rate_context": rates,
                        "daily_rate_context": daily_rates,
                        "persistent_policy_context": persistent_policy
                        or {"currency": currency, "state": "unavailable"},
                        "priced_in_proxy": priced,
                        "remaining_move_assessment": assessment,
                        "initial_reaction": initial_reaction,
                        "persistent_sma_confirmation": sma_confirmation,
                        "verification_state": verify,
                        "news_thesis_state": thesis_state["state"],
                        "news_thesis_state_basis": thesis_state["basis"],
                        "news_thesis_supersession_state": "not_evaluated",
                        "topic_ids": factor["topic_ids"],
                        "headlines": factor["headlines"],
                        "terms": factor["terms"],
                        "entry_quote": dict(quote),
                        "opportunity_forecast_id": str(
                            model.get("forecast_id") or ""
                        ),
                        "opportunity_cohort_id": str(model.get("cohort_id") or ""),
                        "opportunity_issued_at_utc": str(
                            model.get("issued_at_utc") or ""
                        ),
                        "opportunity_age_sec": safe_float(model.get("age_sec")),
                        "opportunity_predicted_clear_probability": safe_float(
                            model.get("predicted_clear_probability")
                        ),
                        "opportunity_predicted_magnitude_pips": safe_float(
                            model.get("predicted_magnitude_pips")
                        ),
                        "opportunity_modeled_cost_pips": max(
                            spread,
                            safe_float(
                                model.get("modeled_entry_cost_pips"), spread
                            ),
                        ),
                        "opportunity_predicted_direction": str(
                            model.get("predicted_direction") or "neutral"
                        ),
                        "pair_expression_rank": expression_rank,
                        "pair_expression_count": expression_count,
                        "pair_expression_universe": "all_current_liquid_pair_legs",
                        "direction_source": "point_in_time_news",
                        "ranking_source": (
                            "conservative_remaining_move_after_age_path_and_cost"
                        ),
                        "arm": arm,
                        "cohort_id": NEWS_REMAINING_MOVE_COHORTS[arm],
                        "research_only": True,
                        "execution_eligible": False,
                    }
                )
    for instrument, tech in technical.items():
        quote = quotes.get(instrument)
        if not quote or not quote.get("fresh") or safe_float(quote.get("spread_pips"), 999999.0) > MAX_ENTRY_SPREAD_PIPS:
            continue
        # One immutable entry per upstream forecast/instrument/side/horizon.
        # Polling an unchanged upstream signal again must not create a trial.
        episode = f"technical_{tech.get('source_id') or instrument}"
        entries.append(
            {
                "episode_id": episode,
                "currency": "",
                "instrument": instrument,
                "horizon_min": int(tech["horizon_min"]),
                "arm": "technical_only",
                "direction": tech["direction"],
                "signed_currency_factors": signed_currency_factors(instrument, tech["direction"]),
                "news_direction": "neutral",
                "news_score": 0.0,
                "news_confidence": None,
                "technical_direction": tech["direction"],
                "technical_confidence": tech["confidence"],
                "technical_expected_net_pips": tech["projected_net_pips"],
                "macro_context": {"state": "not_applicable", "causal_valid": False},
                "rate_context": {"state": "not_required_for_arm", "causal_valid": False},
                "persistent_policy_context": {
                    currency: (policy_state or {}).get("currencies", {}).get(currency)
                    for currency in str(instrument).split("_")
                    if isinstance((policy_state or {}).get("currencies"), Mapping)
                    and (policy_state or {}).get("currencies", {}).get(currency)
                },
                "priced_in_proxy": {"state": "not_applicable", "causal_valid": False},
                "verification_state": "technical_only",
                "topic_ids": [],
                "headlines": [],
                "terms": [],
                "entry_quote": dict(quote),
                "research_only": True,
                "execution_eligible": False,
            }
        )
    return entries


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS watchlist_entries (
            entry_id TEXT PRIMARY KEY,
            cohort_id TEXT NOT NULL,
            decided_utc TEXT NOT NULL,
            mature_after_utc TEXT NOT NULL,
            episode_id TEXT NOT NULL,
            currency TEXT NOT NULL,
            instrument TEXT NOT NULL,
            horizon_min INTEGER NOT NULL,
            arm TEXT NOT NULL,
            direction TEXT NOT NULL,
            news_score REAL NOT NULL,
            technical_confidence REAL,
            verification_state TEXT NOT NULL,
            entry_bid REAL NOT NULL,
            entry_ask REAL NOT NULL,
            entry_mid REAL NOT NULL,
            pip REAL NOT NULL,
            entry_spread_pips REAL NOT NULL,
            status TEXT NOT NULL,
            outcome_utc TEXT,
            gross_pips REAL,
            executable_pips REAL,
            direction_hit INTEGER,
            beat_cost INTEGER,
            terms_json TEXT NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS watchlist_entry_status
            ON watchlist_entries(status,mature_after_utc,arm);
        CREATE INDEX IF NOT EXISTS watchlist_cohort_status_arm
            ON watchlist_entries(cohort_id,status,arm);
        CREATE TABLE IF NOT EXISTS watchlist_cycles (
            observed_utc TEXT PRIMARY KEY,
            market_state TEXT NOT NULL,
            news_factor_count INTEGER NOT NULL,
            technical_signal_count INTEGER NOT NULL,
            entry_count INTEGER NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS news_technical_relation_state (
            cohort_id TEXT NOT NULL,
            news_episode_id TEXT NOT NULL,
            instrument TEXT NOT NULL,
            last_relation TEXT NOT NULL,
            last_technical_direction TEXT NOT NULL,
            last_technical_source_id TEXT NOT NULL,
            last_observed_utc TEXT NOT NULL,
            PRIMARY KEY (cohort_id,news_episode_id,instrument)
        );
        """
    )
    connection.commit()
    return connection


def build_reconfirmation_entries(
    connection: sqlite3.Connection,
    entries: Iterable[Mapping[str, Any]],
    observed: dt.datetime,
) -> list[dict[str, Any]]:
    """Create one prospective H15 row when a live news/technical relation re-aligns.

    The original frozen arms retain the first conjunction observed for a news
    episode.  This separate cohort measures a different hypothesis: after that
    episode has been observed in a neutral or conflicting technical state, does
    a later transition back into agreement carry short-lived executable value?
    Initialization is deliberately silent, and one event/pair can contribute
    at most one row because the immutable entry key is episode based.
    """
    result: list[dict[str, Any]] = []
    news_rows = [dict(row) for row in entries if row.get("arm") == "news_only"]
    for row in news_rows:
        news_episode = str(row.get("episode_id") or "")
        relation_episode_key = f"{news_episode}|currency:{str(row.get('currency') or '')}"
        instrument = str(row.get("instrument") or "")
        technical_direction = str(row.get("technical_direction") or "neutral")
        news_direction = str(row.get("news_direction") or row.get("direction") or "neutral")
        relation = (
            "confirmed"
            if technical_direction == news_direction
            and news_direction in {"long", "short"}
            and str(
                (row.get("persistent_sma_confirmation") or {}).get("state")
            )
            == "aligned_persistent"
            else "neutral" if technical_direction not in {"long", "short"}
            else "conflicted"
        )
        previous = connection.execute(
            """
            SELECT last_relation FROM news_technical_relation_state
            WHERE cohort_id=? AND news_episode_id=? AND instrument=?
            """,
            (RECONFIRMATION_COHORT_ID, relation_episode_key, instrument),
        ).fetchone()
        if previous is not None and str(previous[0]) != "confirmed" and relation == "confirmed":
            candidate = dict(row)
            candidate.update(
                {
                    "arm": "news_technical_reconfirmed_h15",
                    "horizon_min": RECONFIRMATION_HORIZON_MIN,
                    "cohort_id": RECONFIRMATION_COHORT_ID,
                    "reconfirmation_transition": f"{previous[0]}_to_confirmed",
                    "reconfirmation_observed_utc": iso(observed),
                    "research_only": True,
                    "execution_eligible": False,
                }
            )
            result.append(candidate)
        connection.execute(
            """
            INSERT INTO news_technical_relation_state VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(cohort_id,news_episode_id,instrument) DO UPDATE SET
                last_relation=excluded.last_relation,
                last_technical_direction=excluded.last_technical_direction,
                last_technical_source_id=excluded.last_technical_source_id,
                last_observed_utc=excluded.last_observed_utc
            """,
            (
                RECONFIRMATION_COHORT_ID,
                relation_episode_key,
                instrument,
                relation,
                technical_direction,
                str(row.get("technical_source_id") or ""),
                iso(observed),
            ),
        )
    connection.commit()
    return result


def persist_entries(connection: sqlite3.Connection, entries: Iterable[Mapping[str, Any]], observed: dt.datetime) -> int:
    inserted = 0
    for row in entries:
        quote = row["entry_quote"]
        horizon = int(row["horizon_min"])
        cohort_id = str(row.get("cohort_id") or COHORT_ID)
        if cohort_id in UNCORROBORATED_NEWS_COHORTS.values():
            # The evidence unit is event x currency x horizon. A later poll
            # may rank a different pair cheapest, but that is not a new news
            # observation and must not increase N or alter the frozen entry.
            already_sampled = connection.execute(
                """
                SELECT 1 FROM watchlist_entries
                WHERE cohort_id=? AND episode_id=? AND currency=?
                  AND horizon_min=?
                LIMIT 1
                """,
                (
                    cohort_id,
                    str(row["episode_id"]),
                    str(row["currency"]),
                    horizon,
                ),
            ).fetchone()
            if already_sampled is not None:
                continue
        identifier = entry_identifier(
            str(row["episode_id"]), str(row["instrument"]), str(row["arm"]),
            str(row["direction"]), horizon, cohort_id,
        )
        payload = dict(row)
        payload["entry_id"] = identifier
        payload["cohort_id"] = cohort_id
        payload["decided_utc"] = iso(observed)
        payload["mature_after_utc"] = iso(observed + dt.timedelta(minutes=horizon))
        connection.execute(
            """
            INSERT OR IGNORE INTO watchlist_entries VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',NULL,NULL,NULL,NULL,NULL,?,?
            )
            """,
            (
                identifier, cohort_id, payload["decided_utc"], payload["mature_after_utc"],
                row["episode_id"], row["currency"], row["instrument"], horizon,
                row["arm"], row["direction"], safe_float(row.get("news_score")),
                row.get("technical_confidence"), row["verification_state"],
                quote["bid"], quote["ask"], quote["mid"], quote["pip"], quote["spread_pips"],
                json.dumps(row.get("terms") or [], sort_keys=True),
                json.dumps(payload, sort_keys=True),
            ),
        )
        inserted += int(connection.execute("SELECT changes()").fetchone()[0] > 0)
    connection.commit()
    return inserted


def invalidate_uncorroborated_sampling_transition(
    connection: sqlite3.Connection,
) -> int:
    """Quarantine v1 rows whose pair could change across polling cycles."""

    placeholders = ",".join(
        "?" for _ in UNCORROBORATED_NEWS_INVALID_SAMPLING_COHORTS
    )
    cohort_ids = list(UNCORROBORATED_NEWS_INVALID_SAMPLING_COHORTS.values())
    cursor = connection.execute(
        f"""
        UPDATE watchlist_entries
        SET status='invalid_sampling_transition'
        WHERE cohort_id IN ({placeholders})
          AND status IN ('pending','matured')
        """,
        cohort_ids,
    )
    connection.commit()
    return max(0, int(cursor.rowcount))


def mature_entries(connection: sqlite3.Connection, quotes: Mapping[str, Mapping[str, Any]], observed: dt.datetime) -> int:
    matured = 0
    rows = connection.execute(
        """
        SELECT entry_id,mature_after_utc,instrument,direction,entry_bid,entry_ask,
               entry_mid,pip,payload_json FROM watchlist_entries
        WHERE status='pending' AND mature_after_utc <= ?
        ORDER BY mature_after_utc,entry_id
        """,
        (iso(observed),),
    ).fetchall()
    for row in rows:
        identifier, mature_after, instrument, side, entry_bid, entry_ask, entry_mid, pip, payload_json = row
        quote = quotes.get(str(instrument))
        quote_time = parse_utc(quote.get("time")) if quote else None
        maturity = parse_utc(mature_after)
        if not quote or not quote.get("fresh") or quote_time is None or maturity is None:
            continue
        # A stale weekend quote or a quote arriving far after the declared
        # horizon cannot manufacture a flat outcome or a spread-only loss.
        if abs((quote_time - maturity).total_seconds()) > 300:
            continue
        sign = 1.0 if side == "long" else -1.0
        gross = sign * (safe_float(quote["mid"]) - safe_float(entry_mid)) / safe_float(pip)
        executable = (
            (safe_float(quote["bid"]) - safe_float(entry_ask)) / safe_float(pip)
            if side == "long"
            else (safe_float(entry_bid) - safe_float(quote["ask"])) / safe_float(pip)
        )
        try:
            payload = json.loads(payload_json or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        payload["outcome"] = {
            "outcome_utc": iso(observed), "quote_time_utc": iso(quote_time),
            "gross_pips": round(gross, 6), "executable_pips": round(executable, 6),
            "direction_hit": gross > 0, "beat_cost": executable > 0,
        }
        connection.execute(
            """
            UPDATE watchlist_entries SET status='matured',outcome_utc=?,gross_pips=?,
                executable_pips=?,direction_hit=?,beat_cost=?,payload_json=?
            WHERE entry_id=? AND status='pending'
            """,
            (iso(observed), gross, executable, int(gross > 0), int(executable > 0), json.dumps(payload, sort_keys=True), identifier),
        )
        matured += int(connection.execute("SELECT changes()").fetchone()[0] > 0)
    connection.commit()
    return matured


def summarize_database(
    connection: sqlite3.Connection,
    cohort_ids: str | Iterable[str] | None = None,
) -> dict[str, Any]:
    if cohort_ids is None:
        ids = (
            COHORT_ID,
            RECONFIRMATION_COHORT_ID,
            OFFICIAL_RELEASE_FAST_CONFIRMATION_COHORT_ID,
            *UNCORROBORATED_NEWS_COHORTS.values(),
            *NEWS_MAGNITUDE_COHORTS.values(),
        )
    elif isinstance(cohort_ids, str):
        ids = (cohort_ids,)
    else:
        ids = tuple(str(value) for value in cohort_ids)
    ids = tuple(dict.fromkeys(ids))
    placeholders = ",".join("?" for _ in ids)
    status = {
        str(k): int(v)
        for k, v in connection.execute(
            "SELECT status,COUNT(*) FROM watchlist_entries "
            f"WHERE cohort_id IN ({placeholders}) GROUP BY status", ids
        )
    }
    arms: dict[str, Any] = {}
    arms_query = (
        "SELECT arm,COUNT(*),AVG(direction_hit),AVG(beat_cost),"
        "AVG(gross_pips),AVG(executable_pips) FROM watchlist_entries "
        "WHERE status='matured' "
        f"AND cohort_id IN ({placeholders}) GROUP BY arm ORDER BY arm"
    )
    for arm, n, hit, beat, gross, net in connection.execute(arms_query, ids):
        arms[str(arm)] = {
            "n": int(n), "direction_accuracy": hit, "after_cost_win_rate": beat,
            "mean_gross_pips": gross, "mean_executable_pips": net,
        }
    term_values = collapse_term_factor_episode_values(connection.execute(
        "SELECT episode_id,currency,terms_json,gross_pips,executable_pips "
        "FROM watchlist_entries "
        "WHERE status='matured' AND arm='news_only' "
        f"AND cohort_id IN ({placeholders})",
        ids,
    ))
    term_summary = term_spike_statistics(term_values)
    return {
        "cohort_ids": list(ids),
        "status_counts": status,
        "arms": arms,
        "term_spike_map": term_summary["rows"],
        "term_spike_map_policy": term_summary["policy"],
    }


def watchlist_lineage_census(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        str(cohort): int(count)
        for cohort, count in connection.execute(
            "SELECT cohort_id,count(*) FROM watchlist_entries GROUP BY cohort_id"
        )
    }


def matured_count_for_cohorts(
    connection: sqlite3.Connection, cohort_ids: Iterable[str]
) -> int:
    ids = tuple(dict.fromkeys(str(value) for value in cohort_ids))
    placeholders = ",".join("?" for _ in ids)
    return int(
        connection.execute(
            "SELECT count(*) FROM watchlist_entries WHERE status='matured' "
            f"AND cohort_id IN ({placeholders})",
            ids,
        ).fetchone()[0]
    )


def run(
    *, news_db: Path = NEWS_DB, macro_db: Path = MACRO_DB, signals_path: Path = SIGNALS,
    quotes_path: Path = QUOTES, history_path: Path = QUOTE_HISTORY, rates_path: Path = RATES,
    daily_rates_path: Path = DAILY_RATES,
    opportunity_path: Path = OPPORTUNITY_DB,
    opportunity_state_path: Path = OPPORTUNITY_STATE,
    policy_state_path: Path = POLICY_STATE,
    database_path: Path = DEFAULT_DB, state_path: Path = DEFAULT_STATE,
    report_path: Path = DEFAULT_REPORT, observed: dt.datetime | None = None,
) -> dict[str, Any]:
    live_clock = observed is None
    if live_clock:
        observed, observation_clock = normalized_observation_time(utc_now())
    else:
        observed = observed.astimezone(UTC)
        observation_clock = {
            "source": "provided_replay_time",
            "contract_id": "nonprospective_replay_time_v1",
            "trusted_for_prospective_evidence": False,
            "normalized": False,
        }
    input_read_started_utc = observed
    factor_load_diagnostics: dict[str, Any] = {
        "gate_contract_id": SECONDARY_CRYPTO_PRIMARY_GATE_CONTRACT_ID,
        "secondary_crypto_primary_suppressed": 0,
        "secondary_crypto_primary_examples": [],
    }
    factors = load_currency_factors(
        news_db, observed, diagnostics=factor_load_diagnostics
    )
    technical, technical_state = load_technical_signals(signals_path, observed)
    quotes, market_state = load_quotes(quotes_path, observed)
    history = quote_history_rows(history_path)
    opportunity_contract = load_json(opportunity_state_path)
    required_opportunity_cohort = str(opportunity_contract.get("cohort_id") or "")
    if opportunity_source_is_retired(opportunity_contract):
        required_opportunity_cohort = ""
        opportunity, opportunity_state = {}, "retired_drain_only"
    else:
        if (
            str(opportunity_contract.get("model_cohort_id") or "")
            != OPPORTUNITY_MODEL_COHORT_ID
            or not required_opportunity_cohort.startswith(
                f"{OPPORTUNITY_MODEL_COHORT_ID}.collector."
            )
        ):
            required_opportunity_cohort = ""
        opportunity, opportunity_state = load_opportunity_forecasts(
            opportunity_path,
            observed,
            required_opportunity_cohort,
        )
    persistent_policy_state = load_json(policy_state_path)
    if live_clock:
        post_read_observed, post_read_clock = normalized_observation_time(utc_now())
        input_read_clock_regressed = post_read_observed < input_read_started_utc
        observed = post_read_observed
        observation_clock = {
            **post_read_clock,
            "input_read_started_utc": iso(input_read_started_utc),
            "input_read_completed_utc": iso(observed),
            "input_read_elapsed_sec": round(
                (observed - input_read_started_utc).total_seconds(), 6
            ),
            "input_read_clock_regressed": input_read_clock_regressed,
            "decision_clock_policy": "post_bounded_input_snapshot_read",
        }
        quotes, market_state, quote_clock_diagnostics = (
            rebase_quotes_to_post_read_clock(quotes, observed)
        )
    else:
        quote_clock_diagnostics = {
            "contract_id": "nonprospective_replay_quote_age_v1",
            "quote_count": len(quotes),
            "negative_quote_age_count": sum(
                1 for row in quotes.values() if safe_float(row.get("age_sec")) < 0.0
            ),
            "all_negative_quote_ages_fail_closed": False,
        }
    clock_trusted = bool(
        observation_clock.get("trusted_for_prospective_evidence") is True
        and observation_clock.get("contract_id") == OBSERVATION_TIME_CONTRACT_ID
        and observation_clock.get("input_read_clock_regressed") is not True
        and quote_clock_diagnostics.get("negative_quote_age_count") == 0
    )
    entries = build_watchlist(
        factors,
        technical,
        quotes,
        history,
        observed,
        macro_db,
        rates_path,
        persistent_policy_state,
        opportunity,
        daily_rates_path,
    )
    technical_factor_clusters = current_technical_factor_clusters(entries)
    connection = open_database(database_path)
    invalidated_sampling_transition = (
        invalidate_uncorroborated_sampling_transition(connection)
    )
    reconfirmation_entries = (
        build_reconfirmation_entries(connection, entries, observed)
        if market_state == "fresh" and clock_trusted
        else []
    )
    entries.extend(reconfirmation_entries)
    current_cohort_ids = (
        COHORT_ID,
        RECONFIRMATION_COHORT_ID,
        OFFICIAL_RELEASE_FAST_CONFIRMATION_COHORT_ID,
        *OFFICIAL_SURPRISE_RATE_COHORTS.values(),
        *UNCORROBORATED_NEWS_COHORTS.values(),
        *NEWS_MAGNITUDE_COHORTS.values(),
        *NEWS_REMAINING_MOVE_COHORTS.values(),
    )
    current_matured_before = matured_count_for_cohorts(
        connection, current_cohort_ids
    )
    matured_all_history = (
        mature_entries(connection, quotes, observed) if clock_trusted else 0
    )
    current_matured_after = matured_count_for_cohorts(
        connection, current_cohort_ids
    )
    matured = current_matured_after - current_matured_before
    inserted = persist_entries(connection, entries, observed) if market_state == "fresh" and clock_trusted else 0
    summary = summarize_database(connection, current_cohort_ids)
    evidence_by_current_cohort = {
        cohort_id: summarize_database(connection, cohort_id)
        for cohort_id in current_cohort_ids
    }
    all_history_lineage_census = watchlist_lineage_census(connection)
    factor_states = Counter()
    for factor in factors:
        macro = lookup_macro_context(factor, macro_db, observed)
        rates = lookup_rate_context(str(factor["currency"]), rates_path, observed)
        daily_rates = lookup_daily_rate_context(
            str(factor["currency"]), daily_rates_path, observed
        )
        factor_states[f"macro:{macro['state']}"] += 1
        factor_states[f"rates:{rates['state']}"] += 1
        factor_states[f"daily_rates:{daily_rates['state']}"] += 1
    payload = {
        "schema_version": SCHEMA_VERSION,
        "cohort_id": COHORT_ID,
        "model_cohort_id": PARENT_COHORT_ID,
        "generated_utc": iso(observed),
        "status": "ok" if clock_trusted else "blocked_clock_integrity",
        # Top-level identity is deliberate: the Windows supervisor's runtime
        # contract gate reads one JSON property and does not traverse dotted
        # paths. Keep the duplicate diagnostic field for report consumers.
        "required_news_classification_version": NEWS_CLASSIFICATION_VERSION,
        "research_only": True,
        "can_place_orders": False,
        "can_promote": False,
        "execution_eligible": False,
        "market_state": market_state,
        "observation_clock": observation_clock,
        "observation_time_contract_id": OBSERVATION_TIME_CONTRACT_ID,
        "quote_age_clock": quote_clock_diagnostics,
        "technical_state": technical_state,
        "opportunity_state": opportunity_state,
        "opportunity_forecast_count": len(opportunity),
        "opportunity_required_collector_cohort_id": required_opportunity_cohort,
        "currency_factor_count": len(factors),
        "source_factor_admission_diagnostics": factor_load_diagnostics,
        "technical_signal_count": len(technical),
        "watchlist_entry_count": len(entries),
        "all_history_diagnostic_lineage_census": all_history_lineage_census,
        "evidence_by_current_cohort": evidence_by_current_cohort,
        "matured_current_cohorts_this_cycle": matured,
        "matured_all_history_this_cycle": matured_all_history,
        "technical_factor_clusters": technical_factor_clusters,
        "technical_independent_factor_cluster_count": technical_factor_clusters["cluster_count"],
        "diagnostics": {
            "technical_pair_rows_clustered_by_signed_currency_factor": True,
            "historical_arm_metrics_are_raw_pair_rows": True,
            "technical_only_sampling": "one_entry_per_upstream_forecast_instrument_side_horizon",
            "reconfirmation_cohort_id": RECONFIRMATION_COHORT_ID,
            "reconfirmation_entries_this_cycle": len(reconfirmation_entries),
            "reconfirmation_requires_observed_nonconfirmation_to_confirmation_transition": True,
            "news_magnitude_ranking_cohorts": dict(NEWS_MAGNITUDE_COHORTS),
            "news_remaining_move_comparison_cohorts": dict(
                NEWS_REMAINING_MOVE_COHORTS
            ),
            "news_remaining_move_contract": {
                "assessment_contract_id": (
                    "news_remaining_move_cost_clearance_v1_20260824"
                ),
                "pair_expression_scope": "all_current_liquid_pair_legs",
                "subtracts_completed_thesis_direction_move": True,
                "applies_declared_horizon_age_decay": True,
                "cost_safety_multiple": REMAINING_MOVE_COST_SAFETY_MULTIPLE,
                "minimum_clear_probability": (
                    REMAINING_MOVE_MIN_CLEAR_PROBABILITY
                ),
                "failed_or_missing_inputs_become_negative_controls": True,
            },
            "official_release_fast_confirmation_cohort_id": (
                OFFICIAL_RELEASE_FAST_CONFIRMATION_COHORT_ID
            ),
            "official_surprise_rate_cohorts": dict(
                OFFICIAL_SURPRISE_RATE_COHORTS
            ),
            "official_surprise_rate_contract": {
                "assessment_contract_id": (
                    "official_surprise_rate_binding_v1_20260824"
                ),
                "minimum_absolute_standardized_surprise": (
                    OFFICIAL_SURPRISE_RATE_MIN_ABS_Z
                ),
                "minimum_absolute_rate_change_bps_15m": (
                    OFFICIAL_SURPRISE_RATE_MIN_ABS_CHANGE_BPS
                ),
                "missing_or_conflicted_inputs_become_negative_controls": True,
                "inline_release_consensus_can_self_certify": False,
            },
            "uncorroborated_news_response_cohorts": dict(
                UNCORROBORATED_NEWS_COHORTS
            ),
            "secondary_crypto_primary_gate_contract_id": (
                SECONDARY_CRYPTO_PRIMARY_GATE_CONTRACT_ID
            ),
            "uncorroborated_v1_rows_invalidated": invalidated_sampling_transition,
            "required_news_classification_version": NEWS_CLASSIFICATION_VERSION,
            "news_supplies_direction_magnitude_model_supplies_pair_horizon_rank": True,
            "pair_score_contract_id": PAIR_SCORE_CONTRACT_ID,
            "pair_direction_is_signed_base_minus_quote": True,
            "equal_same_episode_leg_scores_cancel": True,
            "one_pair_expression_owner_per_episode": True,
        },
        "additional_shadow_cohorts": [
            {
                "cohort_id": OFFICIAL_RELEASE_FAST_CONFIRMATION_COHORT_ID,
                # This child was frozen under v38.  A later parent rollout
                # cannot silently rewrite its immutable genealogy definition.
                "parent_cohort_id": (
                    OFFICIAL_RELEASE_FAST_CONFIRMATION_PARENT_COHORT_ID
                ),
                "created_at": "2026-09-01T02:30:00+00:00",
                "idea_origin": "official_release_bundle_multileg_first_reaction",
                "data_sources": [
                    "official_release_bundle",
                    "OANDA_executable_bid_ask",
                    "one_minute_quote_history",
                ],
                "feature_contract": {
                    "maximum_decision_age_minutes": 10,
                    "minimum_aligned_pair_legs": 2,
                    "single_currency_factor": True,
                    "causal_consensus_required_for_publish": True,
                },
                "label_contract": {
                    "target": "after_cost_market_response",
                    "horizon_minutes": 15,
                },
                "execution_eligible": False,
                "can_promote": False,
                "can_place_orders": False,
            },
            *[
                {
                    "cohort_id": cohort_id,
                    "parent_cohort_id": (
                        OFFICIAL_SURPRISE_RATE_PARENT_COHORTS[arm]
                    ),
                    "created_at": "2026-09-01T17:45:00+00:00",
                    "idea_origin": (
                        "causal_official_numeric_surprise_plus_event_time_rate_repricing"
                    ),
                    "data_sources": [
                        "official_release_bundle",
                        "causal_pre_release_consensus_ledger",
                        "timestamp_safe_event_time_rate_repricing",
                        "OANDA_executable_bid_ask",
                    ],
                    "feature_contract": {
                        "causal_pre_release_consensus_required": True,
                        "standardized_surprise_required": True,
                        "minimum_absolute_standardized_surprise": (
                            OFFICIAL_SURPRISE_RATE_MIN_ABS_Z
                        ),
                        "explicit_series_direction_semantics_required": True,
                        "event_time_rate_repricing_required": True,
                        "minimum_absolute_rate_change_bps_15m": (
                            OFFICIAL_SURPRISE_RATE_MIN_ABS_CHANGE_BPS
                        ),
                        "surprise_rate_and_source_direction_must_agree": True,
                        "eligible_arm": "confirmed" in arm,
                    },
                    "label_contract": {
                        "target": "after_cost_market_response",
                        "horizon_minutes": 15,
                    },
                    "selection_rule": (
                        "confirm only when causally captured surprise semantics "
                        "and timestamp-safe rate repricing agree; preserve every "
                        "missing, weak, or conflicting row as a negative control"
                    ),
                    "execution_eligible": False,
                    "can_promote": False,
                    "can_place_orders": False,
                }
                for arm, cohort_id in OFFICIAL_SURPRISE_RATE_COHORTS.items()
            ],
            {
                "cohort_id": RECONFIRMATION_COHORT_ID,
                # Frozen before the v2 technical-only sampling repair; retain
                # the original immutable parent declared at creation.
                "parent_cohort_id": RECONFIRMATION_PARENT_COHORT_ID,
                "created_at": "2026-09-01T02:30:00+00:00",
                "idea_origin": "news_technical_reconfirmation",
                "data_sources": [
                    "point_in_time_news",
                    "technical_signal_snapshot",
                    "OANDA_executable_bid_ask",
                ],
                "feature_contract": {
                    "trigger": "first_neutral_or_conflicted_to_confirmed_transition",
                    "one_entry_per_event_pair": True,
                    "initialization_is_silent": True,
                },
                "label_contract": {
                    "target": "after_cost_market_response",
                    "horizon_minutes": 15,
                },
                "selection_rule": (
                    "first observed nonconfirmation-to-confirmation transition "
                    "inside an active news episode"
                ),
                "execution_eligible": False,
                "can_promote": False,
                "can_place_orders": False,
            },
            *[
                {
                    "cohort_id": cohort_id,
                    "parent_cohort_id": UNCORROBORATED_NEWS_PARENT_COHORTS[horizon],
                    "created_at": "2026-09-01T02:30:00+00:00",
                    "idea_origin": "uncorroborated_secondary_news_currency_response",
                    "data_sources": [
                        "secondary_point_in_time_news_discovery",
                        "OANDA_executable_bid_ask",
                    ],
                    "feature_contract": {
                        "source_grade": "secondary_requires_corroboration",
                        "maximum_pair_legs_per_currency_factor": (
                            MAX_UNCORROBORATED_PAIR_LEGS_PER_FACTOR
                        ),
                        "production_publish_eligible": False,
                        "technical_confirmation_used": False,
                        "pair_score_contract_id": PAIR_SCORE_CONTRACT_ID,
                        "equal_same_episode_leg_scores_cancel": True,
                        "secondary_crypto_primary_gate_contract_id": (
                            SECONDARY_CRYPTO_PRIMARY_GATE_CONTRACT_ID
                        ),
                        "secondary_crypto_primary_stories_remain_context": True,
                    },
                    "label_contract": {
                        "target": "after_cost_market_response",
                        "horizon_minutes": horizon,
                    },
                    "selection_rule": (
                        "lowest-spread current pair leg per inferred currency "
                        "factor; separate secondary-source shadow evidence"
                    ),
                    "execution_eligible": False,
                    "can_promote": False,
                    "can_place_orders": False,
                }
                for horizon, cohort_id in UNCORROBORATED_NEWS_COHORTS.items()
            ],
            *[
                {
                    "cohort_id": cohort_id,
                    "parent_cohort_id": NEWS_MAGNITUDE_PARENT_COHORTS[arm],
                    "created_at": "2026-09-01T02:30:00+00:00",
                    "idea_origin": "news_direction_plus_cost_clearance_pair_ranking",
                    "data_sources": [
                        "point_in_time_news",
                        OPPORTUNITY_MODEL_COHORT_ID,
                        "OANDA_executable_bid_ask",
                    ],
                    "feature_contract": {
                        "direction_source": "news",
                        "ranking_source": "predicted_cost_clearance_then_magnitude_cost_ratio",
                        "maximum_pair_legs_per_currency_factor": MAX_PAIR_LEGS_PER_FACTOR,
                        "price_direction_confirmation_required": "direction_confirmed" in arm,
                        "pair_score_contract_id": PAIR_SCORE_CONTRACT_ID,
                    },
                    "label_contract": {
                        "target": "after_cost_market_response",
                        "horizon_minutes": {
                            "h5": 5,
                            "h15": 15,
                            "h30": 30,
                        }[arm.rsplit("_", 1)[-1]],
                    },
                    "selection_rule": (
                        "top three current pair legs by predicted cost-clearance; "
                        "news fixes side; direction-confirmed arm also requires "
                        "price-model agreement"
                    ),
                    "execution_eligible": False,
                    "can_promote": False,
                    "can_place_orders": False,
                }
                for arm, cohort_id in NEWS_MAGNITUDE_COHORTS.items()
            ],
            *[
                {
                    "cohort_id": cohort_id,
                    "parent_cohort_id": NEWS_REMAINING_MOVE_PARENT_COHORTS[arm],
                    "created_at": "2026-09-01T02:30:00+00:00",
                    "idea_origin": (
                        "news_remaining_move_after_age_path_and_executable_cost"
                    ),
                    "data_sources": [
                        "point_in_time_news",
                        OPPORTUNITY_MODEL_COHORT_ID,
                        "OANDA_executable_bid_ask",
                        "one_minute_quote_history",
                    ],
                    "feature_contract": {
                        "direction_source": "news",
                        "pair_expression_scope": "all_current_liquid_pair_legs",
                        "subtract_completed_thesis_direction_move": True,
                        "declared_horizon_age_decay": True,
                        "required_cost_clearance_multiple": (
                            REMAINING_MOVE_COST_SAFETY_MULTIPLE
                        ),
                        "minimum_clear_probability": (
                            REMAINING_MOVE_MIN_CLEAR_PROBABILITY
                        ),
                        "missing_or_failed_inputs_are_negative_controls": True,
                        "eligible_arm": "cost_clear" in arm,
                        "pair_score_contract_id": PAIR_SCORE_CONTRACT_ID,
                    },
                    "label_contract": {
                        "target": "after_cost_market_response",
                        "horizon_minutes": int(arm.rsplit("h", 1)[-1]),
                    },
                    "selection_rule": (
                        "compare every current liquid pair expression; admit only "
                        "conservative remaining movement that clears executable "
                        "cost by the frozen safety multiple and retain all failures "
                        "in a separate negative-control arm"
                    ),
                    "execution_eligible": False,
                    "can_promote": False,
                    "can_place_orders": False,
                }
                for arm, cohort_id in NEWS_REMAINING_MOVE_COHORTS.items()
            ],
        ],
        "inserted_entries": inserted,
        "matured_entries": matured,
        "causal_context_states": dict(sorted(factor_states.items())),
        "currency_factors": factors,
        "persistent_policy_state": persistent_policy_state,
        "watchlist": entries,
        "evidence": summary,
        "policy": {
            "one_currency_factor_per_episode": True,
            "pair_score_contract_id": PAIR_SCORE_CONTRACT_ID,
            "pair_direction_is_signed_base_minus_quote": True,
            "equal_same_episode_leg_scores_cancel": True,
            "mentioned_entities_do_not_create_direction_without_currency_score": True,
            "maximum_pair_legs_per_factor": MAX_PAIR_LEGS_PER_FACTOR,
            "maximum_entry_spread_pips": MAX_ENTRY_SPREAD_PIPS,
            "technical_only_sampling": "one_entry_per_upstream_forecast_instrument_side_horizon",
            "missing_consensus_is_not_neutral": True,
            "missing_rates_is_not_neutral": True,
            "closed_or_stale_market_creates_no_entry": True,
            "arms": [
                "news_only", "technical_only", "news_technical_confirmed",
                "news_technical_conflicted", "official_release_fast_multileg_h15",
                *OFFICIAL_SURPRISE_RATE_COHORTS,
                *NEWS_MAGNITUDE_COHORTS,
                *NEWS_REMAINING_MOVE_COHORTS,
                *[
                    f"uncorroborated_news_response_h{horizon}"
                    for horizon in sorted(UNCORROBORATED_NEWS_COHORTS)
                ],
            ],
        },
    }
    connection.execute(
        "INSERT OR REPLACE INTO watchlist_cycles VALUES (?,?,?,?,?,?)",
        (iso(observed), market_state, len(factors), len(technical), len(entries), json.dumps(payload, sort_keys=True)),
    )
    connection.commit()
    connection.close()
    atomic_json(state_path, payload)
    lines = [
        "# News/technical shadow watchlist", "", f"Generated: `{payload['generated_utc']}`", "",
        "Research-only. Cannot place orders, promote a hypothesis, or alter Practice 007.", "",
        f"- Market/technical state: **{market_state} / {technical_state}**",
        f"- Currency factors / technical signals / current arms: **{len(factors)} / {len(technical)} / {len(entries)}**",
        f"- Current technical pair rows / independent signed-factor clusters: **{technical_factor_clusters['raw_entry_count']} / {technical_factor_clusters['cluster_count']}**",
        f"- Inserted / matured this cycle: **{inserted} / {matured}**", "",
        "| Arm | Matured N | Direction accuracy | After-cost win | Mean net pips |",
        "|---|---:|---:|---:|---:|",
    ]
    for arm, row in sorted(summary["arms"].items()):
        lines.append(
            f"| {arm} | {row['n']} | {100*safe_float(row['direction_accuracy']):.1f}% | "
            f"{100*safe_float(row['after_cost_win_rate']):.1f}% | {safe_float(row['mean_executable_pips']):.3f} |"
        )
    lines.extend(
        [
            "", "Consensus, rate repricing, and priced-in state are explicit. Missing fields cause an unverified shadow arm; they never silently confirm direction.",
            "", "Term-to-spike statistics are prospective and episode-deduplicated. They remain descriptive until governed sample and multiplicity gates pass.", "",
        ]
    )
    atomic_text(report_path, "\n".join(lines))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--news-database", type=Path, default=NEWS_DB)
    parser.add_argument("--macro-database", type=Path, default=MACRO_DB)
    parser.add_argument("--signals", type=Path, default=SIGNALS)
    parser.add_argument("--quotes", type=Path, default=QUOTES)
    parser.add_argument("--quote-history", type=Path, default=QUOTE_HISTORY)
    parser.add_argument("--rates", type=Path, default=RATES)
    parser.add_argument("--daily-rates", type=Path, default=DAILY_RATES)
    parser.add_argument("--policy-state", type=Path, default=POLICY_STATE)
    parser.add_argument("--database", type=Path, default=DEFAULT_DB)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--interval-sec", type=float, default=60.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        run(
            news_db=args.news_database, macro_db=args.macro_database,
            signals_path=args.signals, quotes_path=args.quotes,
            history_path=args.quote_history, rates_path=args.rates,
            daily_rates_path=args.daily_rates,
            policy_state_path=args.policy_state,
            database_path=args.database, state_path=args.state, report_path=args.report,
        )
        if args.interval_sec <= 0 or (args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec):
            break
        time.sleep(max(5.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "canonical_episode_id", "term_features", "load_currency_factors",
    "load_technical_signals", "load_quotes", "priced_in_proxy",
    "remaining_move_assessment", "macro_interpretation_direction",
    "official_surprise_rate_assessment",
    "build_watchlist", "open_database", "persist_entries", "mature_entries", "run",
]
