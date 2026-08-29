#!/usr/bin/env python3
"""Continuous, research-only narrative state for all 21 FX currencies.

The meter is deliberately separate from execution.  It turns point-in-time
story observations into compact five-minute currency states, preserves the
individual model formulas that produced each score, and derives pair state as
base minus quote.  Price/technical confirmation is evaluated by a separate
backtest so prose cannot silently become an order.
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
import statistics
import tempfile
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Iterable, Mapping


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
DEFAULT_NEWS = DATA / "local_news_sentiment" / "local_news_sentiment_v1.sqlite"
DEFAULT_DB = DATA / "state" / "continuous_narrative_meter_v11.sqlite"
DEFAULT_LATEST = DATA / "state" / "continuous_narrative_meter_v11.json"
METER_CONTRACT_ID = "continuous_currency_narrative_meter_v11_20260824"
TERM_FACTOR_CONTRACT_ID = METER_CONTRACT_ID + ".term_factor_timeseries_v1"
RECENT_ARTICLE_FILTER_SQL = "relevant IN (0,1) AND first_seen_utc >= ?"
BUCKET_MINUTES = 5
CURRENCIES = (
    "AUD", "CAD", "CHF", "CNH", "CZK", "DKK", "EUR", "GBP", "HKD",
    "HUF", "JPY", "MXN", "NOK", "NZD", "PLN", "SEK", "SGD", "THB",
    "TRY", "USD", "ZAR",
)

# Exact 68-pair Practice-007 universe.  Pair scores are always base - quote.
INSTRUMENTS = (
    "AUD_CAD", "AUD_CHF", "AUD_HKD", "AUD_JPY", "AUD_NZD", "AUD_SGD", "AUD_USD",
    "CAD_CHF", "CAD_HKD", "CAD_JPY", "CAD_SGD", "CHF_HKD", "CHF_JPY", "CHF_ZAR",
    "EUR_AUD", "EUR_CAD", "EUR_CHF", "EUR_CZK", "EUR_DKK", "EUR_GBP", "EUR_HKD",
    "EUR_HUF", "EUR_JPY", "EUR_NOK", "EUR_NZD", "EUR_PLN", "EUR_SEK", "EUR_SGD",
    "EUR_TRY", "EUR_USD", "EUR_ZAR", "GBP_AUD", "GBP_CAD", "GBP_CHF", "GBP_HKD",
    "GBP_JPY", "GBP_NZD", "GBP_PLN", "GBP_SGD", "GBP_USD", "GBP_ZAR", "HKD_JPY",
    "NZD_CAD", "NZD_CHF", "NZD_HKD", "NZD_JPY", "NZD_SGD", "NZD_USD", "SGD_CHF",
    "SGD_JPY", "TRY_JPY", "USD_CAD", "USD_CHF", "USD_CNH", "USD_CZK", "USD_DKK",
    "USD_HKD", "USD_HUF", "USD_JPY", "USD_MXN", "USD_NOK", "USD_PLN", "USD_SEK",
    "USD_SGD", "USD_THB", "USD_TRY", "USD_ZAR", "ZAR_JPY",
)

MODEL_REGISTRY: dict[str, dict[str, Any]] = {
    "published_semantic_v1": {
        "input": "currency_scores_json",
        "formula": "quality_and_decay_weighted_mean_of_independent_story_scores",
        "purpose": "frozen published semantic direction baseline",
    },
    "research_semantic_v1": {
        "input": "payload.research_currency_scores_else_published",
        "formula": "quality_and_decay_weighted_mean_of_independent_story_scores",
        "purpose": "current research-only semantic mapping candidate",
    },
    "secondary_directional_discovery_v1": {
        "input": "directional_evidence rows excluded from publish relevance",
        "formula": "quality_and_decay_weighted_mean_in_separate_secondary_channel",
        "purpose": "measure timely secondary discovery without granting publish or order authority",
    },
    "recovered_blurb_analog_v1": {
        "input": "spike_blurb_factor_response_analogs",
        "formula": "prior_matured_same_factor_response_orientation_only",
        "purpose": "original movement-to-news macro-memory model",
        "status": "inactive_no_prequential_orientation_rows",
    },
    "linguistic_tone_v1": {
        "input": "generic_sentiment_score_for_explicitly_mapped_currency",
        "formula": "quality_and_decay_weighted_mean_of_independent_story_tone",
        "purpose": "language-tone negative control; not assumed currency direction",
    },
    "source_balanced_v1": {
        "input": "published_semantic_v1",
        "formula": "equal_mean_of_source_family_means_after_story_deduplication",
        "purpose": "prevents a high-volume aggregator from manufacturing consensus",
    },
    "narrative_acceleration_v1": {
        "input": "published_semantic_v1,source_balanced_v1,attention_acceleration",
        "formula": "mean(published,source_balanced)*positive_attention_acceleration_multiplier",
        "purpose": "continuous directional weather meter candidate",
    },
}

TERM_FIELDS = {
    "category": "category",
    "event_type": "event",
    "event_family": "event",
    "factor_type": "factor",
    "factor_family": "factor",
    "factor_terms": "factor",
    "topic": "topic",
    "topics": "topic",
    "tags": "tag",
    "topic_signature": "topic",
}
MAX_TERMS_PER_STORY = 16


def clamp(value: float, lower: float = -1.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, float(value)))


def finite(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def parse_json(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def parse_time(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def ceil_clock(value: dt.datetime, minutes: int = BUCKET_MINUTES) -> dt.datetime:
    value = value.astimezone(dt.timezone.utc)
    width = minutes * 60
    seconds = int(value.timestamp())
    return dt.datetime.fromtimestamp(((seconds + width - 1) // width) * width, dt.timezone.utc)


def atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="") as stream:
            json.dump(payload, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _story_id(
    row: Mapping[str, Any],
    payload: Mapping[str, Any],
    *,
    conservative_secondary: bool = False,
) -> str:
    if conservative_secondary:
        # Secondary discovery rows have source-local event lineage IDs, so a
        # syndicated wire story can otherwise masquerade as dozens of
        # independent narratives and repeatedly reset the clock.  The
        # classifier's semantic topic signature is deliberately conservative:
        # undercounting secondary consensus is safer than manufacturing it.
        topic_signature = str(payload.get("topic_signature") or "").strip()
        if topic_signature:
            return "secondary_topic:" + topic_signature
    explicit = payload.get("story_cluster_id") or payload.get("event_lineage_id")
    if explicit:
        return str(explicit)
    headline = " ".join(str(row.get("headline") or "").casefold().split())
    normalized = headline.rsplit(" - ", 1)[0]
    return normalized or str(row.get("event_id") or "")


def _source_family(row: Mapping[str, Any], payload: Mapping[str, Any]) -> str:
    explicit = payload.get("independent_source_family") or payload.get("source_family")
    if explicit:
        return str(explicit).casefold()
    domain = str(row.get("domain") or payload.get("domain") or "").casefold().strip()
    if domain:
        return domain.removeprefix("www.")
    return str(row.get("source_id") or row.get("source_kind") or "unknown").casefold()


def _score_map(value: Any) -> dict[str, float]:
    parsed = parse_json(value, {})
    if not isinstance(parsed, Mapping):
        return {}
    return {
        str(currency).upper(): clamp(finite(score))
        for currency, score in parsed.items()
        if str(currency).upper() in CURRENCIES and abs(finite(score)) >= 1e-12
    }


def _reaction_minutes(payload: Mapping[str, Any]) -> int:
    explicit = int(finite(payload.get("estimated_reaction_horizon_minutes"), 0.0))
    if explicit <= 0:
        explicit = int(finite(payload.get("estimated_reaction_horizon_sec"), 0.0) / 60.0)
    return max(15, min(1440, explicit or 360))


def _quality_weight(row: Mapping[str, Any], payload: Mapping[str, Any]) -> float:
    direct = payload.get("source_direct") is True
    verified = bool(row.get("source_verified"))
    trusted = payload.get("observation_clock_trusted") is True
    weight = 1.0 if direct and verified else 0.85 if verified else 0.55
    if trusted:
        weight *= 1.10
    confidence = finite(row.get("directional_confidence"), 0.5)
    weight *= 0.50 + 0.50 * clamp(confidence, 0.0, 1.0)
    return max(0.05, min(1.10, weight))


def _normalized_term(value: Any) -> str:
    """Normalize an existing structured label; never tokenize article prose."""

    text = re.sub(r"[^a-z0-9]+", "_", str(value or "").casefold()).strip("_")
    return text[:64]


def _structured_terms(row: Mapping[str, Any], payload: Mapping[str, Any]) -> list[str]:
    terms: set[str] = set()
    for field, namespace in TERM_FIELDS.items():
        value = row.get(field) if field == "category" else payload.get(field)
        if isinstance(value, Mapping):
            values = value.keys()
        elif isinstance(value, (list, tuple, set)):
            values = value
        elif field == "topic_signature":
            values = str(value or "").split("|")
        else:
            values = [value]
        for item in values:
            normalized = _normalized_term(item)
            if normalized:
                terms.add(f"{namespace}:{normalized}")
    return sorted(terms)[:MAX_TERMS_PER_STORY]


def _lineage(payload: Mapping[str, Any], key: str, *aliases: str) -> str:
    for name in (key, *aliases):
        value = payload.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def load_articles(path: Path, *, since: dt.datetime | None = None) -> list[dict[str, Any]]:
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    connection.row_factory = sqlite3.Row
    try:
        sql = """
            SELECT event_id,source_id,source_name,source_kind,source_verified,
                   published_utc,first_seen_utc,last_seen_utc,headline,summary,
                   source_url,domain,relevant,category,currencies_json,
                   currency_scores_json,generic_sentiment_score,
                   directional_confidence,severity,movement_potential,
                   duplicate_count,payload_json
            FROM articles
        """
        params: tuple[Any, ...] = ()
        if since is not None:
            # The live article ledger's available range index is
            # (relevant, first_seen_utc). Enumerating the two NOT NULL boolean
            # states lets SQLite range-seek the two-day window instead of
            # scanning the entire ~700 MB table each minute.
            sql += f" WHERE {RECENT_ARTICLE_FILTER_SQL}"
            params = (since.astimezone(dt.timezone.utc).isoformat(),)
        sql += " ORDER BY first_seen_utc,event_id"
        return [dict(row) for row in connection.execute(sql, params).fetchall()]
    finally:
        connection.close()


def build_contributions(rows: Iterable[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Collapse syndicated article rows to one point-in-time story/currency vote."""

    counters: defaultdict[str, int] = defaultdict(int)
    selected: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        counters["input_articles"] += 1
        payload = parse_json(row.get("payload_json"), {})
        if not isinstance(payload, Mapping):
            payload = {}
        published = _score_map(row.get("currency_scores_json"))
        research = _score_map(payload.get("research_currency_scores")) or published
        row_relevant = bool(row.get("relevant"))
        publish_relevant = bool(
            row_relevant
            and payload.get("directional_publish_eligible") is True
        )
        secondary_discovery = bool(
            not publish_relevant
            and payload.get("directional_evidence") is True
            and (published or research)
        )
        late_secondary_context = bool(
            secondary_discovery
            and payload.get("forward_signal_timely") is not True
        )
        if not publish_relevant and not secondary_discovery:
            if row_relevant:
                counters["publish_ineligible_without_direction"] += 1
            else:
                counters["not_relevant"] += 1
            continue
        if late_secondary_context:
            counters["late_secondary_context_rows"] += 1
        elif secondary_discovery:
            counters["secondary_directional_discovery_rows"] += 1
        if payload.get("reports_prior_market_move") is True:
            counters["prior_move_recaps_excluded"] += 1
            continue
        first_seen = parse_time(row.get("first_seen_utc"))
        if first_seen is None:
            counters["missing_first_seen"] += 1
            continue
        explicit_currencies = {
            str(value).upper() for value in parse_json(row.get("currencies_json"), [])
            if str(value).upper() in CURRENCIES
        }
        explicit_currencies.update(published)
        explicit_currencies.update(research)
        if not explicit_currencies:
            counters["no_currency_mapping"] += 1
            continue
        decision = ceil_clock(first_seen)
        lineage = _story_id(
            row, payload, conservative_secondary=secondary_discovery
        )
        family = _source_family(row, payload)
        horizon = _reaction_minutes(payload)
        tone = clamp(finite(row.get("generic_sentiment_score")))
        quality = _quality_weight(row, payload)
        classifier = str(payload.get("classification_version") or "unknown")
        evidence_channel = (
            "late_secondary_context"
            if late_secondary_context
            else "secondary_directional_discovery"
            if secondary_discovery
            else "published_relevant"
        )
        for currency in sorted(explicit_currencies):
            source_score = published.get(currency, 0.0)
            observed_research_score = research.get(currency, source_score)
            published_score = source_score if publish_relevant else 0.0
            secondary_score = (
                observed_research_score
                if secondary_discovery and not late_secondary_context
                else 0.0
            )
            # Preserve the discovered mapping for audit, but a secondary story
            # first observed too late cannot affect the live meter.
            research_score = (
                0.0 if late_secondary_context else observed_research_score
            )
            candidate = {
                "story_id": lineage,
                "currency": currency,
                "decision_utc": decision,
                "expires_utc": decision + dt.timedelta(minutes=horizon),
                "horizon_minutes": horizon,
                "source_id": str(row.get("source_id") or "unknown"),
                "source_family": family,
                "source_verified": bool(row.get("source_verified")),
                "source_direct": payload.get("source_direct") is True,
                "clock_trusted": payload.get("observation_clock_trusted") is True,
                "forward_timely": payload.get("forward_signal_timely") is True,
                "published_score": published_score,
                "research_score": research_score,
                "observed_research_score": observed_research_score,
                "secondary_score": secondary_score,
                "tone_score": tone,
                "quality_weight": quality,
                "classification_version": classifier,
                "directional_publish_eligible": publish_relevant,
                "evidence_channel": evidence_channel,
                "directional_corroboration_required": bool(
                    payload.get("directional_corroboration_required")
                ),
                "directional_source_grade": str(
                    payload.get("directional_source_grade") or ""
                ),
                "headline": str(row.get("headline") or ""),
                "event_id": str(row.get("event_id") or ""),
                "published_utc": str(row.get("published_utc") or ""),
                "first_seen_utc": str(row.get("first_seen_utc") or ""),
                "last_seen_utc": str(row.get("last_seen_utc") or ""),
                "terms": _structured_terms(row, payload),
                "source_contract_id": _lineage(payload, "source_contract_id"),
                "source_cohort_id": _lineage(payload, "source_cohort_id"),
                "parser_version": _lineage(payload, "parser_version"),
                "revision_id": _lineage(payload, "revision_id", "revision_version"),
                "revised_at_utc": _lineage(payload, "revised_at_utc", "revised_utc"),
                "supersedes_event_id": _lineage(payload, "supersedes_event_id"),
                "superseded_at_utc": _lineage(payload, "superseded_at_utc"),
                "raw_payload_hash": (
                    _lineage(payload, "raw_payload_hash", "payload_sha256")
                    or hashlib.sha256(
                        str(row.get("payload_json") or "").encode("utf-8")
                    ).hexdigest()
                ),
            }
            key = (lineage, currency)
            prior = selected.get(key)
            rank = (
                candidate["decision_utc"],
                -int(candidate["clock_trusted"]),
                -int(candidate["source_direct"]),
                -candidate["quality_weight"],
                candidate["source_id"],
            )
            if prior is None:
                selected[key] = candidate
            else:
                prior_rank = (
                    prior["decision_utc"], -int(prior["clock_trusted"]),
                    -int(prior["source_direct"]), -prior["quality_weight"],
                    prior["source_id"],
                )
                if rank < prior_rank:
                    selected[key] = candidate
                    counters["syndicated_representative_replaced"] += 1
                else:
                    counters["syndicated_rows_collapsed"] += 1
    result = sorted(selected.values(), key=lambda row: (row["decision_utc"], row["currency"], row["story_id"]))
    counters["story_currency_contributions"] = len(result)
    counters["independent_stories"] = len({row["story_id"] for row in result})
    counters["currencies"] = len({row["currency"] for row in result})
    return result, dict(counters)


def _weighted_mean(values: Iterable[tuple[float, float]]) -> float:
    rows = [(float(value), max(0.0, float(weight))) for value, weight in values]
    total = sum(weight for _, weight in rows)
    return sum(value * weight for value, weight in rows) / total if total > 0 else 0.0


def _decayed_mean(rows: Iterable[Mapping[str, Any]], score_key: str) -> float:
    values = list(rows)
    denominator = sum(finite(row.get("quality_weight")) for row in values)
    if denominator <= 0:
        return 0.0
    return sum(
        finite(row.get(score_key)) * finite(row.get("effective_weight"))
        for row in values
    ) / denominator


def _source_balanced(rows: list[dict[str, Any]], score_key: str) -> float:
    families: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        families[str(row["source_family"])].append(row)
    if not families:
        return 0.0
    return statistics.fmean(
        _decayed_mean(values, score_key) for values in families.values()
    )


def build_meter(
    contributions: Iterable[Mapping[str, Any]], *,
    start: dt.datetime | None = None, end: dt.datetime | None = None,
) -> list[dict[str, Any]]:
    """Build a dense five-minute series, including explicit zero-state clocks."""

    values = [dict(row) for row in contributions]
    if not values and (start is None or end is None):
        return []
    first = start or min(row["decision_utc"] for row in values)
    last = end or max(row["expires_utc"] for row in values)
    clock = ceil_clock(first)
    last = ceil_clock(last)
    incoming: defaultdict[dt.datetime, list[dict[str, Any]]] = defaultdict(list)
    for row in values:
        incoming[row["decision_utc"]].append(row)
    active: dict[str, dict[tuple[str, str], dict[str, Any]]] = {
        currency: {} for currency in CURRENCIES
    }
    attention_history: dict[str, deque[float]] = {
        currency: deque(maxlen=12) for currency in CURRENCIES
    }
    output: list[dict[str, Any]] = []
    while clock <= last:
        for row in incoming.get(clock, []):
            active[row["currency"]][(row["story_id"], row["source_family"])] = row
        for currency in CURRENCIES:
            for key, row in list(active[currency].items()):
                if row["expires_utc"] < clock:
                    del active[currency][key]
            rows: list[dict[str, Any]] = []
            new_stories = 0
            for row in active[currency].values():
                age_minutes = max(0.0, (clock - row["decision_utc"]).total_seconds() / 60.0)
                half_life = max(15.0, row["horizon_minutes"] / 2.0)
                decay = math.exp(-math.log(2.0) * age_minutes / half_life)
                item = dict(row)
                item["decay"] = decay
                item["effective_weight"] = (
                    0.0
                    if row.get("evidence_channel") == "late_secondary_context"
                    else row["quality_weight"] * decay
                )
                rows.append(item)
                if row["decision_utc"] == clock:
                    new_stories += 1
            attention = sum(finite(row["effective_weight"]) for row in rows)
            baseline = statistics.fmean(attention_history[currency]) if attention_history[currency] else 0.0
            acceleration = (attention - baseline) / max(1.0, baseline)
            acceleration = clamp(acceleration, -1.0, 3.0)
            attention_history[currency].append(attention)
            published_rows = [
                row for row in rows
                if row.get("evidence_channel") == "published_relevant"
            ]
            secondary_rows = [
                row for row in rows
                if row.get("evidence_channel") == "secondary_directional_discovery"
            ]
            scoring_rows = [
                row for row in rows
                if row.get("evidence_channel") != "late_secondary_context"
            ]
            published = _decayed_mean(published_rows, "published_score")
            research = _decayed_mean(scoring_rows, "research_score")
            secondary = _decayed_mean(secondary_rows, "secondary_score")
            tone = _decayed_mean(scoring_rows, "tone_score")
            balanced = _source_balanced(published_rows, "published_score")
            directional_base = (
                statistics.fmean((published, balanced)) if published_rows else 0.0
            )
            accelerated = clamp(directional_base * (1.0 + 0.35 * max(0.0, acceleration)))
            model_scores = {
                "published_semantic_v1": clamp(published),
                "research_semantic_v1": clamp(research),
                "secondary_directional_discovery_v1": clamp(secondary),
                "recovered_blurb_analog_v1": 0.0,
                "linguistic_tone_v1": clamp(tone),
                "source_balanced_v1": clamp(balanced),
                "narrative_acceleration_v1": accelerated,
            }
            signed_weight = sum(
                finite(row["published_score"]) * finite(row["effective_weight"])
                for row in rows
            )
            absolute_weight = sum(
                abs(finite(row["published_score"])) * finite(row["effective_weight"])
                for row in rows
            )
            agreement = abs(signed_weight) / absolute_weight if absolute_weight else 0.0
            source_families = sorted({str(row["source_family"]) for row in rows})
            classifiers = sorted({str(row["classification_version"]) for row in rows})
            trusted = sum(bool(row["clock_trusted"]) for row in rows)
            forward = sum(bool(row["forward_timely"]) for row in rows)
            output.append({
                "clock_utc": clock,
                "currency": currency,
                "model_scores": model_scores,
                "attention_level": attention,
                "attention_acceleration": acceleration,
                "new_story_count": new_stories,
                "active_story_count": len(rows),
                "published_story_count": len(published_rows),
                "secondary_story_count": len(secondary_rows),
                "source_family_count": len(source_families),
                "agreement": agreement,
                "novelty": new_stories / max(1, len(rows)),
                "trusted_story_count": trusted,
                "forward_timely_story_count": forward,
                "source_families": source_families,
                "story_ids": sorted(str(row["story_id"]) for row in rows),
                "classification_versions": classifiers,
                "historical_evidence_class": (
                    "no_current_evidence" if not rows
                    else "trusted_clock_subset" if trusted == len(rows)
                    else "classifier_adaptive_discovery"
                ),
                # Kept out of the latest dashboard payload.  Persistence uses
                # these bounded structured terms to form an immutable,
                # research-only contribution ledger beside the aggregate meter.
                "term_factor_contributions": [
                    {
                        **{key: row.get(key, "") for key in (
                            "story_id", "event_id", "source_id", "source_family",
                            "source_contract_id", "source_cohort_id", "parser_version",
                            "classification_version", "published_utc", "first_seen_utc",
                            "last_seen_utc", "revision_id", "revised_at_utc",
                            "supersedes_event_id", "superseded_at_utc", "raw_payload_hash",
                            "evidence_channel",
                        )},
                        "term": term,
                        "published_score": row["published_score"],
                        "research_score": row["research_score"],
                        "observed_research_score": row.get(
                            "observed_research_score", row["research_score"]
                        ),
                        "secondary_score": row["secondary_score"],
                        "tone_score": row["tone_score"],
                        "quality_weight": row["quality_weight"],
                        "decay": row["decay"],
                        "effective_weight": row["effective_weight"],
                    }
                    for row in rows for term in row.get("terms", [])
                ],
            })
        clock += dt.timedelta(minutes=BUCKET_MINUTES)
    return output


def latest_payload(rows: Iterable[Mapping[str, Any]], selection: Mapping[str, Any]) -> dict[str, Any]:
    rows = list(rows)
    if not rows:
        latest_clock = None
        currency_rows: dict[str, Mapping[str, Any]] = {}
    else:
        latest_clock = max(row["clock_utc"] for row in rows)
        currency_rows = {
            str(row["currency"]): row for row in rows if row["clock_utc"] == latest_clock
        }
    currencies: dict[str, Any] = {}
    for currency in CURRENCIES:
        row = currency_rows.get(currency, {})
        scores = dict(row.get("model_scores") or {model: 0.0 for model in MODEL_REGISTRY})
        meter = finite(scores.get("narrative_acceleration_v1"))
        currencies[currency] = {
            "score": round(meter, 9),
            "direction": "POSITIVE" if meter > 0.05 else "NEGATIVE" if meter < -0.05 else "NEUTRAL",
            "uncalibrated_directional_confidence": round(abs(meter), 9),
            "model_scores": {key: round(finite(value), 9) for key, value in scores.items()},
            "attention_level": round(finite(row.get("attention_level")), 9),
            "attention_acceleration": round(finite(row.get("attention_acceleration")), 9),
            "active_story_count": int(row.get("active_story_count") or 0),
            "published_story_count": int(row.get("published_story_count") or 0),
            "secondary_story_count": int(row.get("secondary_story_count") or 0),
            "source_family_count": int(row.get("source_family_count") or 0),
            "agreement": round(finite(row.get("agreement")), 9),
            "novelty": round(finite(row.get("novelty")), 9),
            "classification_versions": list(row.get("classification_versions") or []),
            "evidence_class": str(row.get("historical_evidence_class") or "no_current_evidence"),
        }
    pairs = {}
    for instrument in INSTRUMENTS:
        base, quote = instrument.split("_")
        score = currencies[base]["score"] - currencies[quote]["score"]
        pairs[instrument] = {
            "base": base,
            "quote": quote,
            "score": round(clamp(score), 9),
            "direction": "LONG" if score > 0.10 else "SHORT" if score < -0.10 else "NEUTRAL",
            "execution_eligible": False,
            "research_only": True,
        }
    return {
        "schema_version": "continuous_narrative_meter_latest_v11",
        "meter_contract_id": METER_CONTRACT_ID,
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "clock_utc": latest_clock.isoformat() if latest_clock else None,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "orders_placed": 0,
        "currency_count": len(currencies),
        "instrument_count": len(pairs),
        "selection": dict(selection),
        "models": MODEL_REGISTRY,
        "currencies": currencies,
        "pairs": pairs,
    }


def initialize_database(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS model_registry(
            model_id TEXT PRIMARY KEY,
            meter_contract_id TEXT NOT NULL,
            specification_json TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            created_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS currency_meter(
            meter_contract_id TEXT NOT NULL,
            clock_utc TEXT NOT NULL,
            currency TEXT NOT NULL,
            model_scores_json TEXT NOT NULL,
            attention_level REAL NOT NULL,
            attention_acceleration REAL NOT NULL,
            new_story_count INTEGER NOT NULL,
            active_story_count INTEGER NOT NULL,
            source_family_count INTEGER NOT NULL,
            agreement REAL NOT NULL,
            novelty REAL NOT NULL,
            trusted_story_count INTEGER NOT NULL,
            forward_timely_story_count INTEGER NOT NULL,
            source_families_json TEXT NOT NULL,
            story_ids_json TEXT NOT NULL,
            classification_versions_json TEXT NOT NULL,
            evidence_class TEXT NOT NULL,
            created_utc TEXT NOT NULL,
            PRIMARY KEY(meter_contract_id,clock_utc,currency)
        );
        CREATE INDEX IF NOT EXISTS ix_currency_meter_currency_clock
            ON currency_meter(currency,clock_utc);
        CREATE TABLE IF NOT EXISTS narrative_term_factor_clock(
            term_factor_contract_id TEXT NOT NULL,
            clock_utc TEXT NOT NULL,
            currency TEXT NOT NULL,
            active_story_count INTEGER NOT NULL,
            contribution_count INTEGER NOT NULL,
            distinct_term_count INTEGER NOT NULL,
            created_utc TEXT NOT NULL,
            PRIMARY KEY(term_factor_contract_id,clock_utc,currency)
        );
        CREATE TABLE IF NOT EXISTS narrative_term_factor_contribution(
            term_factor_contract_id TEXT NOT NULL,
            clock_utc TEXT NOT NULL,
            currency TEXT NOT NULL,
            story_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            source_family TEXT NOT NULL,
            term TEXT NOT NULL,
            evidence_channel TEXT NOT NULL,
            published_score REAL NOT NULL,
            research_score REAL NOT NULL,
            secondary_score REAL NOT NULL,
            tone_score REAL NOT NULL,
            quality_weight REAL NOT NULL,
            decay REAL NOT NULL,
            effective_weight REAL NOT NULL,
            published_utc TEXT NOT NULL,
            first_seen_utc TEXT NOT NULL,
            last_seen_utc TEXT NOT NULL,
            source_contract_id TEXT NOT NULL,
            source_cohort_id TEXT NOT NULL,
            parser_version TEXT NOT NULL,
            classification_version TEXT NOT NULL,
            revision_id TEXT NOT NULL,
            revised_at_utc TEXT NOT NULL,
            supersedes_event_id TEXT NOT NULL,
            superseded_at_utc TEXT NOT NULL,
            raw_payload_hash TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            created_utc TEXT NOT NULL,
            observed_research_score REAL NOT NULL DEFAULT 0.0,
            PRIMARY KEY(term_factor_contract_id,clock_utc,currency,story_id,source_family,term)
        );
        CREATE INDEX IF NOT EXISTS ix_narrative_term_factor_term_clock
            ON narrative_term_factor_contribution(term,currency,clock_utc);
        CREATE TRIGGER IF NOT EXISTS no_update_narrative_term_factor_clock
            BEFORE UPDATE ON narrative_term_factor_clock BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS no_delete_narrative_term_factor_clock
            BEFORE DELETE ON narrative_term_factor_clock BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS no_update_narrative_term_factor_contribution
            BEFORE UPDATE ON narrative_term_factor_contribution BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS no_delete_narrative_term_factor_contribution
            BEFORE DELETE ON narrative_term_factor_contribution BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    # Backward-safe metadata-only migration. Historical append-only rows are
    # deliberately not rewritten; SQLite exposes the DEFAULT 0.0 for them.
    # New rows preserve the raw discovered score separately from the effective
    # research_score, which is zero for late secondary context.
    contribution_columns = {
        str(row[1])
        for row in connection.execute(
            "PRAGMA table_info(narrative_term_factor_contribution)"
        )
    }
    if "observed_research_score" not in contribution_columns:
        connection.execute(
            "ALTER TABLE narrative_term_factor_contribution "
            "ADD COLUMN observed_research_score REAL NOT NULL DEFAULT 0.0"
        )


def persist(path: Path, rows: Iterable[Mapping[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    created = dt.datetime.now(dt.timezone.utc).isoformat()
    connection = sqlite3.connect(path, timeout=60.0)
    try:
        initialize_database(connection)
        with connection:
            for model_id, specification in MODEL_REGISTRY.items():
                connection.execute(
                    """INSERT OR IGNORE INTO model_registry VALUES(?,?,?,?,?,?)""",
                    (model_id, METER_CONTRACT_ID, json.dumps(specification, sort_keys=True), 1, 0, created),
                )
            count = 0
            for row in rows:
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO currency_meter VALUES(
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                    )
                    """,
                    (
                        METER_CONTRACT_ID, row["clock_utc"].isoformat(), row["currency"],
                        json.dumps(row["model_scores"], sort_keys=True), row["attention_level"],
                        row["attention_acceleration"], row["new_story_count"],
                        row["active_story_count"], row["source_family_count"], row["agreement"],
                        row["novelty"], row["trusted_story_count"], row["forward_timely_story_count"],
                        json.dumps(row["source_families"], sort_keys=True),
                        json.dumps(row["story_ids"], sort_keys=True),
                        json.dumps(row["classification_versions"], sort_keys=True),
                        row["historical_evidence_class"], created,
                    ),
                )
                count += max(0, int(cursor.rowcount))
                # The ledger is prospective and immutable.  Existing clocks
                # were already sealed by an earlier run, so do not repeatedly
                # replay their (usually much larger) term contribution set.
                if int(cursor.rowcount) <= 0:
                    continue
                contributions = list(row.get("term_factor_contributions") or [])
                connection.execute(
                    """INSERT OR IGNORE INTO narrative_term_factor_clock
                       VALUES(?,?,?,?,?,?,?)""",
                    (
                        TERM_FACTOR_CONTRACT_ID, row["clock_utc"].isoformat(), row["currency"],
                        row["active_story_count"], len(contributions),
                        len({str(item["term"]) for item in contributions}), created,
                    ),
                )
                for item in contributions:
                    connection.execute(
                        """INSERT OR IGNORE INTO narrative_term_factor_contribution
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            TERM_FACTOR_CONTRACT_ID, row["clock_utc"].isoformat(), row["currency"],
                            item["story_id"], item["event_id"], item["source_id"],
                            item["source_family"], item["term"], item["evidence_channel"],
                            item["published_score"], item["research_score"],
                            item["secondary_score"], item["tone_score"], item["quality_weight"],
                            item["decay"], item["effective_weight"], item["published_utc"],
                            item["first_seen_utc"], item["last_seen_utc"],
                            item["source_contract_id"], item["source_cohort_id"],
                            item["parser_version"], item["classification_version"],
                            item["revision_id"], item["revised_at_utc"],
                            item["supersedes_event_id"], item["superseded_at_utc"],
                            item["raw_payload_hash"], 1, 0, created,
                            item.get("observed_research_score", item["research_score"]),
                        ),
                    )
        return count
    finally:
        connection.close()


def run_once(
    *, news: Path = DEFAULT_NEWS, database: Path = DEFAULT_DB,
    latest: Path = DEFAULT_LATEST, history: bool = False,
) -> dict[str, Any]:
    now = dt.datetime.now(dt.timezone.utc)
    since = None if history else now - dt.timedelta(days=2)
    contributions, selection = build_contributions(load_articles(news, since=since))
    if history:
        rows = build_meter(contributions, end=ceil_clock(now))
    else:
        end = ceil_clock(now)
        start = end - dt.timedelta(hours=24)
        rows = build_meter(contributions, start=start, end=end)
    persist(database, rows)
    payload = latest_payload(rows, selection)
    payload["mode"] = "historical_rebuild" if history else "live_incremental"
    payload["meter_rows_built"] = len(rows)
    atomic_json(latest, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--news", type=Path, default=DEFAULT_NEWS)
    parser.add_argument("--database", type=Path, default=DEFAULT_DB)
    parser.add_argument("--latest", type=Path, default=DEFAULT_LATEST)
    parser.add_argument("--history", action="store_true")
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        payload = run_once(
            news=args.news, database=args.database, latest=args.latest,
            history=args.history,
        )
        print(json.dumps({
            "clock_utc": payload["clock_utc"],
            "currency_count": payload["currency_count"],
            "instrument_count": payload["instrument_count"],
            "meter_rows_built": payload["meter_rows_built"],
            "mode": payload["mode"],
        }, sort_keys=True), flush=True)
        if args.interval_sec <= 0 or args.history:
            break
        if args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec:
            break
        time.sleep(max(1.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
