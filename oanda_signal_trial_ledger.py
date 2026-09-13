#!/usr/bin/env python3
"""Run canonical no-order signal trials from the practice-007 research feed.

The worker opens at most one cohort per trial arm and horizon, measures every
constituent at executable bid/ask prices, and stores normalized returns.  It is
research-only: it has no broker client and cannot submit or manage orders.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import statistics
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    import oanda_top_signal_position_ledger as base
except ImportError:  # pragma: no cover - package import fallback
    from trad import oanda_top_signal_position_ledger as base


UTC = timezone.utc
ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data" / "oanda_training_manager"
STATE_ROOT = DATA_ROOT / "state"
DEFAULT_SIGNALS = STATE_ROOT / "practice_007_signal_snapshot_research_v1.json"
DEFAULT_QUOTES = STATE_ROOT / "practice_007_market_quotes_v1.json"
DEFAULT_NEWS = DATA_ROOT / "news_event_tags" / "latest_pair_news_context.json"
DEFAULT_DATABASE = STATE_ROOT / "canonical_signal_trials_v1.sqlite"
DEFAULT_STATE = STATE_ROOT / "canonical_signal_trials_v1.json"
SCHEMA_VERSION = 9
MEASUREMENT_VERSION = "canonical_signal_trial_v1"
TRIAL_HORIZONS = (300, 900, 1800, 3600, 7200)
BARRIER_COST_MULTIPLES = (1.5, 2.0, 3.0)
OPPORTUNITY_MIN_GROSS_TO_SPREAD = 2.0
OPPORTUNITY_MIN_CONFIDENCE = 0.50
H2_MIN_GROSS_TO_SPREAD = 1.35
H2_MIN_CONFIDENCE = 0.52
CONFLICT_MIN_GROSS_TO_SPREAD = 1.5
CONFLICT_MIN_CONFIDENCE = 0.55
INTRAHOUR_COST_MIN_CONFIDENCE = 0.55
INTRAHOUR_COST_MIN_GROSS_TO_SPREAD = 1.75
INTRAHOUR_COST_MIN_NET_PIPS = 0.25
INTRAHOUR_COST_MAX_SPREAD_PIPS = 3.0
INTRAHOUR_CALIBRATED_MIN_NET_PIPS = 1.0
INTRAHOUR_CALIBRATED_MAX_NET_PIPS = 3.0
MAX_NEWS_AGE_MINUTES = 360.0
FACTOR_EPISODE_WINDOW_SEC = 900.0
REVERSAL_CONFIRMATION_WINDOW_SEC = 900.0
REVERSAL_MIN_ADVERSE_SPREAD_MULTIPLE = 1.0
CROSS_FAMILY_REVERSAL_WINDOW_SEC = 1800.0
CROSS_FAMILY_REVERSAL_MIN_PROJECTED_NET_PIPS = 2.0
FAMILY_WATCH_MIN_FACTOR_EPISODES = 5
FAMILY_WATCH_MIN_WIN_RATE_PCT = 55.0
FAMILY_WATCH_MIN_PROFIT_FACTOR = 1.10
PAIR_FLIP_WINDOW_SEC = 1800.0
PAIR_FLIP_EPISODE_SEC = 900.0
PAIR_FLIP_MAGNITUDE_SPLIT_PIPS = 2.0
REENTRY_PAIR_DIRECTION_COOLDOWN_SEC = 900.0
REENTRY_INSTRUMENT_COOLDOWN_SEC = 300.0
REENTRY_JPY_FACTOR_COOLDOWN_SEC = 1800.0
ALL_OUTCOME_REFRACTORY_SEC = 900.0
ALL_OUTCOME_INSTRUMENT_REFRACTORY_SEC = 300.0
CROSS_HORIZON_THESIS_WINDOW_SEC = 60.0
PAIR_FLIP_DISCOVERY_EPOCH = datetime(
    2026, 8, 4, 9, 33, 3, tzinfo=UTC
).timestamp()


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def parse_iso_epoch(value: Any) -> float | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def session_name(epoch: float) -> str:
    hour = datetime.fromtimestamp(epoch, UTC).hour
    if 7 <= hour < 12:
        return "london"
    if 12 <= hour < 16:
        return "overlap"
    if 0 <= hour < 7:
        return "asia"
    if 16 <= hour < 21:
        return "late_ny"
    return "rollover"


def directional_currency_keys(candidate: dict[str, Any]) -> set[tuple[str, int]]:
    parts = str(candidate.get("instrument") or "").split("_")
    if len(parts) != 2:
        return set()
    direction = str(candidate.get("direction") or "").lower()
    sign = 1 if direction in {"buy", "long"} else -1
    return {(parts[0], sign), (parts[1], -sign)}


def correlation_factor_ids(candidate: dict[str, Any]) -> set[str]:
    """Map pair propagation onto shared currency-direction risk factors."""
    return {
        f"currency:{currency}:{'long' if sign > 0 else 'short'}"
        for currency, sign in directional_currency_keys(candidate)
    }


def pair_news_context(payload: dict[str, Any], instrument: str) -> dict[str, Any]:
    pairs = payload.get("pairs") or {}
    row = pairs.get(instrument) if isinstance(pairs, dict) else None
    return row if isinstance(row, dict) else {}


NEWS_CONTEXT_KEYS = (
    "active_event_count",
    "as_of_utc",
    "directional_state",
    "instrument",
)
NEWS_EVENT_SNAPSHOT_KEYS = (
    "age_minutes",
    "category",
    "context_only",
    "event_id",
    "event_time_basis",
    "event_utc",
    "execution_eligible",
    "expected_pair_direction",
    "first_known_utc",
    "headline",
    "intervention_status",
    "pair_relevance",
    "published_utc",
    "relevance_reason",
    "reports_prior_market_move",
    "research_only",
    "scheduled_utc",
    "severity",
    "source_url",
    "source_verified",
    "topic_tags",
)


def news_context_snapshot(context: dict[str, Any]) -> dict[str, Any]:
    """Return a bounded immutable copy of decision-time news evidence.

    Event stores are deliberately pruned and reclassified over time.  Retaining
    only event IDs therefore cannot reproduce what a trial actually knew.  This
    snapshot keeps the headline, causal timestamps, direction, verification and
    retrospective/context flags needed for later hit/miss attribution without
    copying large downstream routing payloads into every decision.
    """

    if not isinstance(context, dict):
        return {}
    snapshot = {
        key: context.get(key)
        for key in NEWS_CONTEXT_KEYS
        if key in context
    }
    snapshot["events"] = [
        {
            key: event.get(key)
            for key in NEWS_EVENT_SNAPSHOT_KEYS
            if key in event
        }
        for event in context.get("events") or []
        if isinstance(event, dict)
    ]
    # Round-trip through JSON to detach any nested lists from the live payload
    # and guarantee that the stored value is serializable.
    return json.loads(json.dumps(snapshot, ensure_ascii=False, default=str))


def event_ids(context: dict[str, Any]) -> list[str]:
    output: list[str] = []
    for event in context.get("events") or []:
        if not isinstance(event, dict):
            continue
        event_id = str(event.get("event_id") or "").strip()
        age = base.safe_float(event.get("age_minutes"), math.inf)
        if event_id and age <= MAX_NEWS_AGE_MINUTES and event_id not in output:
            output.append(event_id)
    return output


def independence_event_ids(context: dict[str, Any]) -> list[str]:
    """Return only prospective material events for cohort independence.

    Retrospective market-reaction headlines remain attached to positions for
    audit context, but they must not make distinct price candidates appear to
    share a causal catalyst after the move already happened.
    """

    output: list[str] = []
    for event in context.get("events") or []:
        if not isinstance(event, dict):
            continue
        event_id = str(event.get("event_id") or "").strip()
        if not event_id or event_id in output:
            continue
        if base.safe_float(event.get("age_minutes"), math.inf) > MAX_NEWS_AGE_MINUTES:
            continue
        if bool(event.get("context_only")):
            continue
        if bool(event.get("reports_prior_market_move")):
            continue
        if not (
            bool(event.get("execution_eligible"))
            or bool(event.get("source_verified"))
            or base.safe_float(event.get("severity")) >= 60.0
            or bool(str(event.get("scheduled_utc") or "").strip())
        ):
            continue
        output.append(event_id)
    return output


def news_direction(context: dict[str, Any]) -> str:
    events = [
        event
        for event in context.get("events") or []
        if isinstance(event, dict)
    ]
    current_non_context = [
        event
        for event in events
        if not bool(event.get("context_only"))
        and base.safe_float(event.get("age_minutes"), math.inf)
        <= MAX_NEWS_AGE_MINUTES
    ]
    state = str(context.get("directional_state") or "").upper()
    if state in {"BULLISH", "LONG", "BUY"} and (
        not events or current_non_context
    ):
        return "buy"
    if state in {"BEARISH", "SHORT", "SELL"} and (
        not events or current_non_context
    ):
        return "sell"
    directions = {
        str(event.get("expected_pair_direction") or "").upper()
        for event in current_non_context
    }
    directions.discard("")
    directions.discard("UNKNOWN")
    if directions and directions <= {"BULLISH", "LONG", "BUY"}:
        return "buy"
    if directions and directions <= {"BEARISH", "SHORT", "SELL"}:
        return "sell"
    return "neutral"


def event_regime(context: dict[str, Any]) -> bool:
    for event in context.get("events") or []:
        if not isinstance(event, dict):
            continue
        if base.safe_float(event.get("age_minutes"), math.inf) > MAX_NEWS_AGE_MINUTES:
            continue
        if bool(event.get("context_only")):
            continue
        if bool(event.get("reports_prior_market_move")):
            continue
        if (
            bool(event.get("execution_eligible"))
            or bool(event.get("source_verified"))
            or base.safe_float(event.get("severity")) >= 60.0
            or bool(str(event.get("scheduled_utc") or "").strip())
        ):
            return True
    return False


def verified_event_regime(context: dict[str, Any]) -> bool:
    """Identify a prospective event backed by verified/vetted evidence.

    The legacy event comparator deliberately retained high-severity discovery
    headlines for broad diagnostics.  That makes it unsuitable as a clean
    causal filter because one unverified global headline can mark every pair
    as event-driven.  This stricter comparator is additive and research-only.
    """

    for event in context.get("events") or []:
        if not isinstance(event, dict):
            continue
        if base.safe_float(event.get("age_minutes"), math.inf) > MAX_NEWS_AGE_MINUTES:
            continue
        if bool(event.get("context_only")):
            continue
        if bool(event.get("reports_prior_market_move")):
            continue
        if bool(event.get("execution_eligible")) or bool(event.get("source_verified")):
            return True
    return False


def candidate_is_opportunity(candidate: dict[str, Any]) -> bool:
    return (
        base.safe_float(candidate.get("gross_to_spread"))
        >= OPPORTUNITY_MIN_GROSS_TO_SPREAD
        and base.safe_float(candidate.get("signal_confidence"), 0.5)
        >= OPPORTUNITY_MIN_CONFIDENCE
        and base.safe_float(candidate.get("projected_net_pips")) > 0.0
    )


def candidate_directional_gross_to_spread(candidate: dict[str, Any]) -> float:
    """Return side-aligned cost coverage, failing closed when it is absent."""

    value = candidate.get("directional_gross_to_spread")
    return base.safe_float(value) if value is not None else 0.0


def candidate_is_strict_h2_opportunity(candidate: dict[str, Any]) -> bool:
    """Select an H2 signal that passed the observable execution-quality layer.

    This comparator is deliberately stricter than the generic opportunity arm.
    It remains observation-only and requires the newly published directional
    cost ratio so an opposing contributor cannot qualify the selected side.
    """

    validation = candidate.get("execution_validation") or {}
    blocked = candidate.get("blocked_by") or []
    return bool(
        int(candidate.get("horizon_sec") or 0) == 7200
        and candidate_directional_gross_to_spread(candidate)
        >= OPPORTUNITY_MIN_GROSS_TO_SPREAD
        and base.safe_float(candidate.get("signal_confidence"), 0.5)
        >= H2_MIN_CONFIDENCE
        and base.safe_float(candidate.get("projected_net_pips")) > 0.0
        and bool(candidate.get("signal_eligible"))
        and bool(candidate.get("validated"))
        and isinstance(validation, dict)
        and bool(validation.get("validated"))
        and not bool(candidate.get("direction_conflict"))
        and not blocked
        and not bool(candidate.get("negative_historical_warmup"))
        and not bool(candidate.get("promotion_rejected"))
        and not bool(validation.get("negative_historical_warmup"))
        and not bool(validation.get("promotion_rejected"))
        and str(candidate.get("policy_state") or "") == "executable"
    )


def candidate_is_intrahour_cost_capture(
    candidate: dict[str, Any],
    quotes: dict[str, dict[str, Any]] | None = None,
) -> bool:
    horizon = int(candidate.get("horizon_sec") or 0)
    spread = base.safe_float(candidate.get("spread_pips"), math.inf)
    if quotes is not None:
        values = base.quote_values(
            quotes,
            str(candidate.get("instrument") or ""),
        )
        if values is not None:
            bid, ask, pip = values
            spread = (ask - bid) / pip
    return bool(
        0 < horizon <= 3600
        and spread <= INTRAHOUR_COST_MAX_SPREAD_PIPS
        and base.safe_float(candidate.get("signal_confidence"), 0.5)
        >= INTRAHOUR_COST_MIN_CONFIDENCE
        and base.safe_float(candidate.get("gross_to_spread"))
        >= INTRAHOUR_COST_MIN_GROSS_TO_SPREAD
        and base.safe_float(candidate.get("projected_net_pips"))
        >= INTRAHOUR_COST_MIN_NET_PIPS
    )


def candidate_is_intrahour_calibrated_magnitude(
    candidate: dict[str, Any],
    quotes: dict[str, dict[str, Any]] | None = None,
) -> bool:
    """Prospectively test the live-discovered 1-3 pip projection band."""

    projected = base.safe_float(candidate.get("projected_net_pips"))
    return bool(
        candidate_is_intrahour_cost_capture(candidate, quotes)
        and INTRAHOUR_CALIBRATED_MIN_NET_PIPS
        <= projected
        <= INTRAHOUR_CALIBRATED_MAX_NET_PIPS
    )


def candidate_is_intrahour_executor_parity(
    candidate: dict[str, Any],
    quotes: dict[str, dict[str, Any]] | None = None,
) -> bool:
    """Match the observable eligibility layer of the practice executor.

    This remains a no-order research arm.  Its purpose is to keep the broad
    cost-gate experiment separate from signals that also passed historical
    validation, final eligibility, and conflict checks.
    """

    validation = candidate.get("execution_validation") or {}
    return bool(
        candidate_is_intrahour_cost_capture(candidate, quotes)
        and bool(candidate.get("signal_eligible"))
        and isinstance(validation, dict)
        and bool(validation.get("validated"))
        and not bool(candidate.get("direction_conflict"))
        and str(candidate.get("policy_state") or "")
        != "conflicted_aggressive_shadow"
    )


def independent_selection(
    ranked: Iterable[dict[str, Any]],
    news_payload: dict[str, Any],
    limit: int,
) -> list[dict[str, Any]]:
    """Greedily remove duplicated instruments, event IDs and FX exposures."""
    selected: list[dict[str, Any]] = []
    seen_instruments: set[str] = set()
    seen_events: set[str] = set()
    seen_factors: set[str] = set()
    for candidate in ranked:
        instrument = str(candidate.get("instrument") or "")
        context = pair_news_context(news_payload, instrument)
        all_candidate_events = event_ids(context)
        candidate_events = set(independence_event_ids(context))
        factors = correlation_factor_ids(candidate)
        if not instrument or instrument in seen_instruments:
            continue
        if candidate_events and candidate_events & seen_events:
            continue
        if factors & seen_factors:
            continue
        row = dict(candidate)
        row["news_direction"] = news_direction(context)
        row["event_regime"] = event_regime(context)
        row["event_ids"] = all_candidate_events
        row["independence_event_ids"] = sorted(candidate_events)
        row["news_context_snapshot"] = news_context_snapshot(context)
        row["correlation_factor_ids"] = sorted(factors)
        row["jpy_factor_ids"] = sorted(
            factor for factor in factors if factor.startswith("currency:JPY:")
        )
        selected.append(row)
        seen_instruments.add(instrument)
        seen_events.update(candidate_events)
        seen_factors.update(factors)
        if len(selected) >= max(1, int(limit)):
            break
    return selected


def top3_readiness_by_horizon(
    ranked: dict[int, list[dict[str, Any]]],
    news_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    """Explain whether top-three scarcity or independence blocks the trial."""

    diagnostics: list[dict[str, Any]] = []
    for horizon in TRIAL_HORIZONS:
        opportunities = [
            row for row in (ranked.get(horizon) or [])
            if candidate_is_opportunity(row)
        ]
        currency_capacity = len(independent_selection(opportunities, {}, 3))
        full_capacity = len(
            independent_selection(opportunities, news_payload, 3)
        )
        blockers: list[str] = []
        if len(opportunities) < 3:
            blockers.append("fewer_than_three_opportunities")
        elif currency_capacity < 3:
            blockers.append("currency_factor_overlap")
        elif full_capacity < 3:
            blockers.append("causal_event_overlap")
        diagnostics.append(
            {
                "horizon_sec": horizon,
                "horizon_label": base.horizon_label(horizon),
                "opportunity_count": len(opportunities),
                "currency_independent_capacity": currency_capacity,
                "fully_independent_capacity": full_capacity,
                "exactly_three_ready": full_capacity >= 3,
                "blocked_by": blockers,
            }
        )
    return diagnostics


def arm_candidates(
    arm: str,
    ranked: list[dict[str, Any]],
    news_payload: dict[str, Any],
    quotes: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    if arm == "conflicted_aggressive_direction_pair":
        selected = arm_candidates(
            "conflicted_aggressive_top1", ranked, news_payload, quotes
        )
        return direction_pair_shadow_candidates(
            selected, source_arm="conflicted_aggressive_top1"
        )
    if arm == "h2_strict_validated_direction_pair":
        selected = independent_selection(
            [row for row in ranked if candidate_is_strict_h2_opportunity(row)],
            news_payload,
            1,
        )
        return direction_pair_shadow_candidates(
            selected,
            source_arm="h2_strict_validated",
        )
    if arm == "opportunity_price_top1_direction_pair_h2":
        source = [
            row for row in ranked
            if int(row.get("horizon_sec") or 0) == 7200
        ]
        selected = arm_candidates(
            "opportunity_price_top1", source, news_payload, quotes
        )
        return direction_pair_shadow_candidates(
            selected, source_arm="opportunity_price_top1"
        )
    if arm == "h2_persistence_direction_pair":
        source = [
            row for row in ranked
            if int(row.get("horizon_sec") or 0) == 7200
        ]
        selected = arm_candidates("h2_persistence", source, news_payload, quotes)
        return direction_pair_shadow_candidates(
            selected, source_arm="h2_persistence"
        )
    if arm == "opportunity_price_top1_inverse_h2":
        source = [
            row for row in ranked
            if int(row.get("horizon_sec") or 0) == 7200
        ]
        selected = arm_candidates(
            "opportunity_price_top1", source, news_payload, quotes
        )
        return inverse_shadow_candidates(
            selected, source_arm="opportunity_price_top1"
        )
    if arm == "h2_persistence_inverse":
        source = [
            row for row in ranked
            if int(row.get("horizon_sec") or 0) == 7200
        ]
        selected = arm_candidates("h2_persistence", source, news_payload, quotes)
        return inverse_shadow_candidates(selected, source_arm="h2_persistence")

    opportunities = [row for row in ranked if candidate_is_opportunity(row)]
    enriched: list[dict[str, Any]] = []
    for candidate in opportunities:
        row = dict(candidate)
        context = pair_news_context(news_payload, str(row.get("instrument") or ""))
        row["news_direction"] = news_direction(context)
        row["event_regime"] = event_regime(context)
        row["verified_event_regime"] = verified_event_regime(context)
        row["event_ids"] = event_ids(context)
        enriched.append(row)

    if arm == "opportunity_price_top1":
        return independent_selection(enriched, news_payload, 1)
    if arm == "opportunity_price_top3":
        selected = independent_selection(enriched, news_payload, 3)
        # A two-member fallback would change the experiment's risk geometry
        # and make its result incomparable with the explicitly requested
        # one-best versus three-at-one-third trial.
        return selected if len(selected) == 3 else []
    if arm == "opportunity_news_aligned":
        aligned = [
            row for row in enriched
            if row["news_direction"] == str(row.get("direction") or "").lower()
        ]
        return independent_selection(aligned, news_payload, 1)
    if arm == "opportunity_event_regime":
        return independent_selection(
            [row for row in enriched if row["event_regime"]], news_payload, 1
        )
    if arm == "opportunity_verified_event_regime":
        return independent_selection(
            [row for row in enriched if row["verified_event_regime"]],
            news_payload,
            1,
        )
    if arm == "opportunity_verified_news_event":
        combined = [
            row for row in enriched
            if row["verified_event_regime"]
            and row["news_direction"]
            == str(row.get("direction") or "").lower()
        ]
        return independent_selection(combined, news_payload, 1)
    if arm == "opportunity_news_event":
        combined = [
            row for row in enriched
            if row["event_regime"]
            and row["news_direction"] == str(row.get("direction") or "").lower()
        ]
        return independent_selection(combined, news_payload, 1)
    if arm == "h2_persistence":
        eligible = [
            row for row in ranked
            if int(row.get("horizon_sec") or 0) == 7200
            and base.safe_float(row.get("gross_to_spread")) >= H2_MIN_GROSS_TO_SPREAD
            and base.safe_float(row.get("signal_confidence"), 0.5) >= H2_MIN_CONFIDENCE
            and base.safe_float(row.get("projected_net_pips")) > 0.0
        ]
        return independent_selection(eligible, news_payload, 1)
    if arm == "intrahour_cost_capture_top1":
        eligible = [
            row
            for row in ranked
            if candidate_is_intrahour_cost_capture(row, quotes)
        ]
        return independent_selection(eligible, news_payload, 1)
    if arm == "intrahour_magnitude_1_3_top1":
        eligible = [
            row
            for row in ranked
            if candidate_is_intrahour_calibrated_magnitude(row, quotes)
        ]
        return independent_selection(eligible, news_payload, 1)
    if arm == "intrahour_executor_parity_top1":
        eligible = [
            row
            for row in ranked
            if candidate_is_intrahour_executor_parity(row, quotes)
        ]
        return independent_selection(eligible, news_payload, 1)
    if arm == "conflicted_aggressive_top1":
        conflicted = [
            row
            for row in ranked
            if row.get("policy_state") == "conflicted_aggressive_shadow"
            and bool(row.get("direction_conflict"))
            and base.safe_float(row.get("gross_to_spread"))
            >= CONFLICT_MIN_GROSS_TO_SPREAD
            and base.safe_float(row.get("signal_confidence"), 0.5)
            >= CONFLICT_MIN_CONFIDENCE
            and base.safe_float(row.get("projected_net_pips")) > 0.0
        ]
        return independent_selection(conflicted, news_payload, 1)
    raise ValueError(f"unknown trial arm: {arm}")


def inverse_shadow_candidates(
    candidates: Iterable[dict[str, Any]],
    *,
    source_arm: str,
) -> list[dict[str, Any]]:
    """Invert preselected H2 decisions without creating an execution signal.

    Selection, confidence and projected magnitude remain the source signal's
    diagnostics.  Only the measured paper direction is inverted.  Recomputing
    correlation factors after the inversion keeps factor-episode reporting
    directionally truthful.
    """

    output: list[dict[str, Any]] = []
    for candidate in candidates:
        original = str(candidate.get("direction") or "").lower()
        if original not in {"buy", "sell", "long", "short"}:
            continue
        row = dict(candidate)
        row["source_direction"] = "buy" if original in {"buy", "long"} else "sell"
        row["source_arm"] = source_arm
        row["shadow_transform"] = "inverse_direction"
        row["direction"] = "sell" if original in {"buy", "long"} else "buy"
        factors = sorted(correlation_factor_ids(row))
        row["correlation_factor_ids"] = factors
        row["jpy_factor_ids"] = [
            factor for factor in factors if factor.startswith("currency:JPY:")
        ]
        output.append(row)
    return output


def direction_pair_shadow_candidates(
    candidates: Iterable[dict[str, Any]],
    *,
    source_arm: str,
) -> list[dict[str, Any]]:
    """Return a timestamp-matched original/inverse pair for one source."""

    source = list(candidates)
    if len(source) != 1:
        return []
    original = dict(source[0])
    original["source_direction"] = str(original.get("direction") or "").lower()
    original["source_arm"] = source_arm
    original["shadow_transform"] = "original_control"
    inverse = inverse_shadow_candidates(source, source_arm=source_arm)
    return [original, *inverse]


def confirmed_reversal_candidates(
    connection: sqlite3.Connection,
    ranked: list[dict[str, Any]],
    news_payload: dict[str, Any],
    quotes: dict[str, dict[str, Any]],
    horizon: int,
    now_epoch: float,
    *,
    require_same_family: bool = True,
    confirmation_window_sec: float = REVERSAL_CONFIRMATION_WINDOW_SEC,
) -> list[dict[str, Any]]:
    """Select a cost-qualified same-family flip after observable adverse motion.

    This is deliberately prospective and research-only. A candidate qualifies
    only when an opposite signal from the same strategy family, instrument and
    horizon is still open, was observed in the preceding 15 minutes, and is
    already losing at least its entry spread at executable prices. Exact
    cross-arm copies of the prior signal are collapsed before comparison.
    """

    if horizon <= 0 or horizon > 3600:
        return []
    connection.row_factory = sqlite3.Row
    earliest_epoch = now_epoch - max(1.0, confirmation_window_sec)
    selected: list[dict[str, Any]] = []
    for candidate in ranked:
        if not candidate_is_intrahour_cost_capture(candidate, quotes):
            continue
        instrument = str(candidate.get("instrument") or "")
        direction = str(candidate.get("direction") or "").lower()
        family, _ = signal_family(str(candidate.get("signal_id") or ""))
        values = base.quote_values(quotes, instrument)
        if not instrument or direction not in {"buy", "sell"} or values is None:
            continue
        bid, ask, _ = values
        prior_rows = connection.execute(
            """
            SELECT c.observed_epoch, p.direction, p.signal_id, p.entry_bid,
                   p.entry_ask, p.pip, p.entry_spread_pips
            FROM positions p
            JOIN cohorts c USING(decision_id)
            WHERE c.status='open' AND c.horizon_sec=? AND p.instrument=?
              AND c.observed_epoch>=? AND c.observed_epoch<?
            ORDER BY c.observed_epoch DESC
            """,
            (horizon, instrument, earliest_epoch, now_epoch),
        ).fetchall()
        seen_prior: set[tuple[str, str, float, float]] = set()
        confirmed = False
        for prior in prior_rows:
            prior_direction = str(prior["direction"] or "").lower()
            prior_family, _ = signal_family(str(prior["signal_id"] or ""))
            prior_key = (
                prior_direction,
                str(prior["signal_id"] or ""),
                round(base.safe_float(prior["entry_bid"]), 12),
                round(base.safe_float(prior["entry_ask"]), 12),
            )
            if prior_key in seen_prior:
                continue
            seen_prior.add(prior_key)
            if prior_direction == direction or (
                require_same_family and prior_family != family
            ):
                continue
            pip = max(1e-12, base.safe_float(prior["pip"]))
            if prior_direction == "buy":
                signed_pips = (bid - base.safe_float(prior["entry_ask"])) / pip
            else:
                signed_pips = (base.safe_float(prior["entry_bid"]) - ask) / pip
            required_adverse = (
                REVERSAL_MIN_ADVERSE_SPREAD_MULTIPLE
                * max(0.0, base.safe_float(prior["entry_spread_pips"]))
            )
            if -signed_pips >= required_adverse:
                confirmed = True
                break
        if confirmed:
            selected.append(candidate)
    return independent_selection(selected, news_payload, 1)


def cross_family_confirmed_reversal_candidates(
    connection: sqlite3.Connection,
    ranked: list[dict[str, Any]],
    news_payload: dict[str, Any],
    quotes: dict[str, dict[str, Any]],
    horizon: int,
    now_epoch: float,
) -> list[dict[str, Any]]:
    """Prospectively test large pair flips confirmed by any strategy family.

    Retrospective factor-collapsed evidence favored flips with at least two
    projected after-cost pips, while smaller flips failed. Keep this as a
    distinct no-order arm so the same-family comparator and raw signals remain
    unchanged.
    """

    eligible = [
        row
        for row in ranked
        if base.safe_float(row.get("projected_net_pips"))
        >= CROSS_FAMILY_REVERSAL_MIN_PROJECTED_NET_PIPS
    ]
    return confirmed_reversal_candidates(
        connection,
        eligible,
        news_payload,
        quotes,
        horizon,
        now_epoch,
        require_same_family=False,
        confirmation_window_sec=CROSS_FAMILY_REVERSAL_WINDOW_SEC,
    )


def post_loss_reentry_blocker(
    connection: sqlite3.Connection,
    candidate: dict[str, Any],
    now_epoch: float,
) -> str | None:
    """Return the active research-only cooldown after a matured losing signal.

    Raw trial arms remain untouched.  Exact cross-arm copies of a prior signal
    can appear multiple times in the ledger, but any matching loss is enough to
    block the policy-aware comparator, so duplicates cannot amplify the rule.
    """

    instrument = str(candidate.get("instrument") or "")
    direction = str(candidate.get("direction") or "").lower()
    if not instrument or direction not in {"buy", "sell"}:
        return None
    candidate_factors = correlation_factor_ids(candidate)
    earliest_epoch = now_epoch - max(
        REENTRY_PAIR_DIRECTION_COOLDOWN_SEC,
        REENTRY_INSTRUMENT_COOLDOWN_SEC,
        REENTRY_JPY_FACTOR_COOLDOWN_SEC,
    )
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT c.closed_epoch, p.instrument, p.direction, p.net_pips
        FROM cohorts c JOIN positions p USING(decision_id)
        WHERE c.measurement_version=? AND c.status='matured'
          AND c.closed_epoch>=? AND c.closed_epoch<=? AND p.net_pips<0
          AND COALESCE(p.shadow_transform, '') <> 'inverse_direction'
        ORDER BY c.closed_epoch DESC
        """,
        (MEASUREMENT_VERSION, earliest_epoch, now_epoch),
    ).fetchall()
    for row in rows:
        age_sec = now_epoch - base.safe_float(row["closed_epoch"])
        prior_instrument = str(row["instrument"] or "")
        prior_direction = str(row["direction"] or "").lower()
        if (
            prior_instrument == instrument
            and prior_direction == direction
            and age_sec < REENTRY_PAIR_DIRECTION_COOLDOWN_SEC
        ):
            return "pair_direction_post_loss_cooldown"
        if (
            prior_instrument == instrument
            and age_sec < REENTRY_INSTRUMENT_COOLDOWN_SEC
        ):
            return "instrument_post_loss_cooldown"
        if age_sec >= REENTRY_JPY_FACTOR_COOLDOWN_SEC:
            continue
        prior_factors = correlation_factor_ids(
            {"instrument": prior_instrument, "direction": prior_direction}
        )
        shared_jpy = {
            factor
            for factor in candidate_factors & prior_factors
            if factor.startswith("currency:JPY:")
        }
        if shared_jpy:
            return "jpy_factor_post_loss_cooldown"
    return None


def reentry_guarded_opportunity_candidates(
    connection: sqlite3.Connection,
    ranked: list[dict[str, Any]],
    news_payload: dict[str, Any],
    now_epoch: float,
) -> list[dict[str, Any]]:
    """Select the best opportunity after prospective post-loss cooldowns."""

    eligible: list[dict[str, Any]] = []
    for candidate in ranked:
        if not candidate_is_opportunity(candidate):
            continue
        row = dict(candidate)
        context = pair_news_context(news_payload, str(row.get("instrument") or ""))
        row["news_direction"] = news_direction(context)
        row["event_regime"] = event_regime(context)
        row["event_ids"] = event_ids(context)
        blocker = post_loss_reentry_blocker(connection, row, now_epoch)
        if blocker is None:
            eligible.append(row)
    return independent_selection(eligible, news_payload, 1)


def open_thesis_overlap_blocker(
    connection: sqlite3.Connection,
    candidate: dict[str, Any],
    now_epoch: float,
) -> str | None:
    """Block a distinct signal while its instrument already has an open thesis.

    Policy-arm copies of the same signal share one thesis and must not block
    each other.  A different signal ID on the same instrument is an overlapping
    re-entry/reversal decision and belongs in a separate prospective arm.
    """

    instrument = str(candidate.get("instrument") or "")
    signal_id = str(candidate.get("signal_id") or candidate.get("id") or "")
    if not instrument or not signal_id:
        return "missing_overlap_identity"
    exists = connection.execute(
        """
        SELECT 1
        FROM cohorts c JOIN positions p USING(decision_id)
        WHERE c.measurement_version=? AND c.status='open'
          AND c.observed_epoch<=? AND c.target_epoch>?
          AND p.instrument=? AND p.signal_id<>?
          AND COALESCE(p.shadow_transform, '') <> 'inverse_direction'
        LIMIT 1
        """,
        (MEASUREMENT_VERSION, now_epoch, now_epoch, instrument, signal_id),
    ).fetchone()
    return "instrument_open_thesis_overlap" if exists else None


def overlap_guarded_opportunity_candidates(
    connection: sqlite3.Connection,
    ranked: list[dict[str, Any]],
    news_payload: dict[str, Any],
    now_epoch: float,
) -> list[dict[str, Any]]:
    """Select the best opportunity without overlapping instrument theses."""

    eligible: list[dict[str, Any]] = []
    for candidate in ranked:
        if not candidate_is_opportunity(candidate):
            continue
        row = dict(candidate)
        context = pair_news_context(news_payload, str(row.get("instrument") or ""))
        row["news_direction"] = news_direction(context)
        row["event_regime"] = event_regime(context)
        row["event_ids"] = event_ids(context)
        if open_thesis_overlap_blocker(connection, row, now_epoch) is None:
            eligible.append(row)
    return independent_selection(eligible, news_payload, 1)


def open_thesis_factor_overlap_blocker(
    connection: sqlite3.Connection,
    candidate: dict[str, Any],
    now_epoch: float,
) -> str | None:
    """Block a distinct signal that repeats an open signed-currency factor.

    Instrument overlap is only one form of duplicate exposure.  For example,
    long USD_JPY and long EUR_JPY are both short-JPY theses even though the
    instruments differ.  Exact cross-arm copies of the same signal are still
    one thesis and are ignored.
    """

    signal_id = str(candidate.get("signal_id") or candidate.get("id") or "")
    candidate_factors = correlation_factor_ids(candidate)
    if not signal_id or not candidate_factors:
        return "missing_factor_overlap_identity"
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT p.instrument, p.direction
        FROM cohorts c JOIN positions p USING(decision_id)
        WHERE c.measurement_version=? AND c.status='open'
          AND c.observed_epoch<=? AND c.target_epoch>?
          AND p.signal_id<>?
          AND COALESCE(p.shadow_transform, '') <> 'inverse_direction'
        """,
        (MEASUREMENT_VERSION, now_epoch, now_epoch, signal_id),
    ).fetchall()
    for row in rows:
        prior_factors = correlation_factor_ids(
            {
                "instrument": str(row["instrument"] or ""),
                "direction": str(row["direction"] or ""),
            }
        )
        if candidate_factors & prior_factors:
            return "signed_currency_open_thesis_overlap"
    return None


def factor_overlap_guarded_opportunity_candidates(
    connection: sqlite3.Connection,
    ranked: list[dict[str, Any]],
    news_payload: dict[str, Any],
    now_epoch: float,
) -> list[dict[str, Any]]:
    """Select the best opportunity without repeated open currency factors."""

    eligible: list[dict[str, Any]] = []
    for candidate in ranked:
        if not candidate_is_opportunity(candidate):
            continue
        row = dict(candidate)
        context = pair_news_context(news_payload, str(row.get("instrument") or ""))
        row["news_direction"] = news_direction(context)
        row["event_regime"] = event_regime(context)
        row["event_ids"] = event_ids(context)
        if open_thesis_factor_overlap_blocker(connection, row, now_epoch) is None:
            eligible.append(row)
    return independent_selection(eligible, news_payload, 1)


def all_outcome_refractory_blocker(
    connection: sqlite3.Connection,
    candidate: dict[str, Any],
    now_epoch: float,
) -> str | None:
    """Block a repeated intrahour thesis after any matured outcome.

    Unlike the post-loss guard, this comparator asks whether immediately
    recycling even a winning thesis merely pays another spread for the same
    factor episode.  Opposite signed factors remain available to the separate
    reversal experiments.
    """

    instrument = str(candidate.get("instrument") or "")
    direction = str(candidate.get("direction") or "").lower()
    if not instrument or direction not in {"buy", "sell"}:
        return None
    candidate_factors = correlation_factor_ids(candidate)
    earliest_epoch = now_epoch - max(
        ALL_OUTCOME_REFRACTORY_SEC,
        ALL_OUTCOME_INSTRUMENT_REFRACTORY_SEC,
    )
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT c.closed_epoch, p.instrument, p.direction
        FROM cohorts c JOIN positions p USING(decision_id)
        WHERE c.measurement_version=? AND c.status='matured'
          AND c.closed_epoch>=? AND c.closed_epoch<=?
          AND COALESCE(p.shadow_transform, '') <> 'inverse_direction'
        ORDER BY c.closed_epoch DESC
        """,
        (MEASUREMENT_VERSION, earliest_epoch, now_epoch),
    ).fetchall()
    seen: set[tuple[float, str, str]] = set()
    for row in rows:
        closed_epoch = base.safe_float(row["closed_epoch"])
        prior_instrument = str(row["instrument"] or "")
        prior_direction = str(row["direction"] or "").lower()
        key = (round(closed_epoch, 6), prior_instrument, prior_direction)
        if key in seen:
            continue
        seen.add(key)
        age_sec = now_epoch - closed_epoch
        if (
            prior_instrument == instrument
            and age_sec < ALL_OUTCOME_INSTRUMENT_REFRACTORY_SEC
        ):
            return "instrument_all_outcome_refractory"
        if age_sec >= ALL_OUTCOME_REFRACTORY_SEC:
            continue
        if prior_instrument == instrument and prior_direction == direction:
            return "pair_direction_all_outcome_refractory"
        prior_factors = correlation_factor_ids(
            {"instrument": prior_instrument, "direction": prior_direction}
        )
        if candidate_factors & prior_factors:
            return "signed_factor_all_outcome_refractory"
    return None


def all_outcome_refractory_candidates(
    connection: sqlite3.Connection,
    ranked: list[dict[str, Any]],
    news_payload: dict[str, Any],
    quotes: dict[str, dict[str, Any]],
    horizon: int,
    now_epoch: float,
) -> list[dict[str, Any]]:
    """Return the best M5/M15 cost-qualified non-recycled thesis."""

    if int(horizon) not in {300, 900}:
        return []
    eligible = [
        row
        for row in ranked
        if candidate_is_intrahour_cost_capture(row, quotes)
        and all_outcome_refractory_blocker(connection, row, now_epoch) is None
    ]
    return independent_selection(eligible, news_payload, 1)


TRIAL_ARMS = (
    "opportunity_price_top1",
    "opportunity_price_top1_direction_pair_h2",
    "h2_strict_validated_direction_pair",
    "opportunity_price_top1_reentry_guarded",
    "opportunity_price_top1_overlap_guarded",
    "opportunity_price_top1_factor_overlap_guarded",
    "opportunity_price_top3",
    "opportunity_news_aligned",
    "opportunity_event_regime",
    "opportunity_verified_event_regime",
    "opportunity_verified_news_event",
    "opportunity_news_event",
    "h2_persistence",
    "h2_persistence_direction_pair",
    "intrahour_cost_capture_top1",
    "intrahour_magnitude_1_3_top1",
    "intrahour_executor_parity_top1",
    "conflicted_aggressive_top1",
    "conflicted_aggressive_direction_pair",
    "intrahour_confirmed_reversal_top1",
    "intrahour_cross_family_reversal_top1",
    "intrahour_all_outcome_refractory_top1",
)


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=5.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA busy_timeout=5000")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS cohorts(
            decision_id TEXT PRIMARY KEY,
            measurement_version TEXT NOT NULL,
            arm TEXT NOT NULL,
            horizon_sec INTEGER NOT NULL,
            horizon_label TEXT NOT NULL,
            observed_epoch REAL NOT NULL,
            observed_at TEXT NOT NULL,
            target_epoch REAL NOT NULL,
            target_at TEXT NOT NULL,
            market_session TEXT NOT NULL,
            member_count INTEGER NOT NULL,
            event_ids_json TEXT NOT NULL,
            correlation_factor_ids_json TEXT NOT NULL DEFAULT '[]',
            status TEXT NOT NULL,
            closed_epoch REAL,
            closed_at TEXT,
            weighted_net_pips REAL,
            weighted_return_pct REAL,
            weighted_spread_units REAL,
            win INTEGER
        );
        CREATE UNIQUE INDEX IF NOT EXISTS one_open_arm_horizon
        ON cohorts(arm, horizon_sec) WHERE status='open';
        CREATE INDEX IF NOT EXISTS cohort_status_target
        ON cohorts(status, target_epoch);

        CREATE TABLE IF NOT EXISTS positions(
            decision_id TEXT NOT NULL,
            rank_in_cohort INTEGER NOT NULL,
            weight REAL NOT NULL,
            instrument TEXT NOT NULL,
            direction TEXT NOT NULL,
            side TEXT NOT NULL,
            signal_id TEXT NOT NULL,
            strategy_family TEXT NOT NULL DEFAULT '',
            lane_id TEXT NOT NULL DEFAULT '',
            input_timeframe TEXT NOT NULL DEFAULT '',
            direction_source TEXT NOT NULL DEFAULT '',
            policy_state TEXT NOT NULL DEFAULT '',
            signal_eligible INTEGER NOT NULL DEFAULT 0,
            validated INTEGER NOT NULL DEFAULT 0,
            direction_conflict INTEGER NOT NULL DEFAULT 0,
            blocked_by_json TEXT NOT NULL DEFAULT '[]',
            execution_validation_json TEXT NOT NULL DEFAULT '{}',
            source_direction TEXT NOT NULL DEFAULT '',
            source_arm TEXT NOT NULL DEFAULT '',
            shadow_transform TEXT NOT NULL DEFAULT '',
            confidence REAL NOT NULL,
            projected_net_pips REAL NOT NULL,
            gross_to_spread REAL NOT NULL,
            directional_gross_to_spread REAL NOT NULL DEFAULT 0.0,
            all_contributor_gross_to_spread REAL NOT NULL DEFAULT 0.0,
            news_direction TEXT NOT NULL,
            event_regime INTEGER NOT NULL,
            event_ids_json TEXT NOT NULL,
            news_context_json TEXT NOT NULL DEFAULT '{}',
            entry_bid REAL NOT NULL,
            entry_ask REAL NOT NULL,
            entry_mid REAL NOT NULL,
            pip REAL NOT NULL,
            entry_spread_pips REAL NOT NULL,
            exit_bid REAL,
            exit_ask REAL,
            exit_mid REAL,
            net_pips REAL,
            return_pct REAL,
            spread_units REAL,
            win INTEGER,
            path_tracking_eligible INTEGER NOT NULL DEFAULT 0,
            path_sample_count INTEGER NOT NULL DEFAULT 0,
            max_adverse_pips REAL NOT NULL DEFAULT 0.0,
            max_favorable_pips REAL NOT NULL DEFAULT 0.0,
            PRIMARY KEY(decision_id, rank_in_cohort),
            FOREIGN KEY(decision_id) REFERENCES cohorts(decision_id)
        );

        CREATE TABLE IF NOT EXISTS competing_risk_barrier_paths(
            decision_id TEXT NOT NULL,
            rank_in_cohort INTEGER NOT NULL,
            barrier_id TEXT NOT NULL,
            target_pips REAL NOT NULL,
            stop_pips REAL NOT NULL,
            sample_count INTEGER NOT NULL DEFAULT 0,
            first_target_epoch REAL,
            first_stop_epoch REAL,
            first_event TEXT NOT NULL DEFAULT '',
            first_event_epoch REAL,
            last_signed_pips REAL,
            PRIMARY KEY(decision_id, rank_in_cohort, barrier_id),
            FOREIGN KEY(decision_id, rank_in_cohort)
                REFERENCES positions(decision_id, rank_in_cohort)
        );
        CREATE INDEX IF NOT EXISTS competing_risk_first_event
        ON competing_risk_barrier_paths(barrier_id, first_event);
        """
    )
    position_columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(positions)")
    }
    position_migrations = {
        "correlation_factor_ids_json": "TEXT NOT NULL DEFAULT '[]'",
        "strategy_family": "TEXT NOT NULL DEFAULT ''",
        "lane_id": "TEXT NOT NULL DEFAULT ''",
        "input_timeframe": "TEXT NOT NULL DEFAULT ''",
        "direction_source": "TEXT NOT NULL DEFAULT ''",
        "policy_state": "TEXT NOT NULL DEFAULT ''",
        "signal_eligible": "INTEGER NOT NULL DEFAULT 0",
        "validated": "INTEGER NOT NULL DEFAULT 0",
        "direction_conflict": "INTEGER NOT NULL DEFAULT 0",
        "blocked_by_json": "TEXT NOT NULL DEFAULT '[]'",
        "execution_validation_json": "TEXT NOT NULL DEFAULT '{}'",
        "source_direction": "TEXT NOT NULL DEFAULT ''",
        "source_arm": "TEXT NOT NULL DEFAULT ''",
        "shadow_transform": "TEXT NOT NULL DEFAULT ''",
        "directional_gross_to_spread": "REAL NOT NULL DEFAULT 0.0",
        "all_contributor_gross_to_spread": "REAL NOT NULL DEFAULT 0.0",
        "news_context_json": "TEXT NOT NULL DEFAULT '{}'",
        "path_tracking_eligible": "INTEGER NOT NULL DEFAULT 0",
        "path_sample_count": "INTEGER NOT NULL DEFAULT 0",
        "max_adverse_pips": "REAL NOT NULL DEFAULT 0.0",
        "max_favorable_pips": "REAL NOT NULL DEFAULT 0.0",
    }
    for column, declaration in position_migrations.items():
        if column not in position_columns:
            connection.execute(
                f"ALTER TABLE positions ADD COLUMN {column} {declaration}"
            )
    connection.commit()
    return connection


def decision_id(
    arm: str,
    horizon: int,
    observed_epoch: float,
    members: list[dict[str, Any]],
) -> str:
    bucket = int(observed_epoch // max(300, horizon))
    member_key = ";".join(
        f"{row.get('instrument')}:{row.get('direction')}"
        for row in members
    )
    events = sorted(
        {event for row in members for event in row.get("event_ids") or []}
    )
    raw = f"{MEASUREMENT_VERSION}|{arm}|{horizon}|{bucket}|{member_key}|{events}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def open_cohort(
    connection: sqlite3.Connection,
    arm: str,
    horizon: int,
    members: list[dict[str, Any]],
    quotes: dict[str, dict[str, Any]],
    observed_epoch: float,
) -> bool:
    if not members:
        return False
    if connection.execute(
        "SELECT 1 FROM cohorts WHERE arm=? AND horizon_sec=? AND status='open'",
        (arm, horizon),
    ).fetchone():
        return False
    prepared: list[tuple[dict[str, Any], float, float, float]] = []
    for member in members:
        values = base.quote_values(quotes, str(member.get("instrument") or ""))
        if values is None:
            return False
        prepared.append((member, *values))
    cohort_id = decision_id(arm, horizon, observed_epoch, members)
    all_events = sorted(
        {event for row in members for event in row.get("event_ids") or []}
    )
    inserted = connection.execute(
        """
        INSERT OR IGNORE INTO cohorts(
            decision_id, measurement_version, arm, horizon_sec, horizon_label,
            observed_epoch, observed_at, target_epoch, target_at,
            market_session, member_count, event_ids_json, status
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?, 'open')
        """,
        (
            cohort_id,
            MEASUREMENT_VERSION,
            arm,
            horizon,
            base.horizon_label(horizon),
            observed_epoch,
            datetime.fromtimestamp(observed_epoch, UTC).isoformat(),
            observed_epoch + horizon,
            datetime.fromtimestamp(observed_epoch + horizon, UTC).isoformat(),
            session_name(observed_epoch),
            len(prepared),
            json.dumps(all_events, separators=(",", ":")),
        ),
    ).rowcount
    if inserted != 1:
        return False
    weight = 1.0 / len(prepared)
    for rank, (member, bid, ask, pip) in enumerate(prepared, start=1):
        connection.execute(
            """
            INSERT INTO positions(
                decision_id, rank_in_cohort, weight, instrument, direction,
                side, signal_id, strategy_family, lane_id, input_timeframe,
                direction_source, policy_state, signal_eligible, validated,
                direction_conflict, blocked_by_json, execution_validation_json,
                source_direction, source_arm, shadow_transform, confidence,
                projected_net_pips, gross_to_spread,
                directional_gross_to_spread,
                all_contributor_gross_to_spread, news_direction, event_regime,
                event_ids_json, news_context_json, correlation_factor_ids_json,
                entry_bid, entry_ask, entry_mid, pip, entry_spread_pips,
                path_tracking_eligible, path_sample_count
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                cohort_id,
                rank,
                weight,
                member["instrument"],
                member["direction"],
                base.display_side(member["direction"]),
                str(member.get("signal_id") or ""),
                str(member.get("family") or ""),
                str(member.get("lane_id") or ""),
                str(member.get("input_timeframe") or ""),
                str(member.get("direction_source") or ""),
                str(member.get("policy_state") or ""),
                int(bool(member.get("signal_eligible"))),
                int(bool(member.get("validated"))),
                int(bool(member.get("direction_conflict"))),
                json.dumps(member.get("blocked_by") or [], separators=(",", ":")),
                json.dumps(
                    member.get("execution_validation") or {},
                    separators=(",", ":"),
                ),
                str(member.get("source_direction") or ""),
                str(member.get("source_arm") or ""),
                str(member.get("shadow_transform") or ""),
                base.safe_float(member.get("signal_confidence"), 0.5),
                base.safe_float(member.get("projected_net_pips")),
                base.safe_float(member.get("gross_to_spread")),
                candidate_directional_gross_to_spread(member),
                base.safe_float(
                    member.get("all_contributor_gross_to_spread"),
                    base.safe_float(member.get("gross_to_spread")),
                ),
                str(member.get("news_direction") or "neutral"),
                int(bool(member.get("event_regime"))),
                json.dumps(member.get("event_ids") or [], separators=(",", ":")),
                json.dumps(
                    member.get("news_context_snapshot") or {},
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                json.dumps(
                    member.get("correlation_factor_ids")
                    or sorted(correlation_factor_ids(member)),
                    separators=(",", ":"),
                ),
                bid,
                ask,
                (bid + ask) / 2.0,
                pip,
                (ask - bid) / pip,
                1,
                1,
            ),
        )
    connection.commit()
    return True


def update_open_excursions(
    connection: sqlite3.Connection,
    quotes: dict[str, dict[str, Any]],
    now_epoch: float | None = None,
) -> int:
    """Sample executable adverse/favorable paths for new shadow cohorts.

    Existing rows are deliberately ineligible because their full price paths
    were not observed prospectively.  These fields only support research
    counterfactuals and cannot alter selection or execution.
    """
    connection.row_factory = sqlite3.Row
    sampled_epoch = time.time() if now_epoch is None else float(now_epoch)
    rows = connection.execute(
        """
        SELECT p.* FROM positions p
        JOIN cohorts c USING(decision_id)
        WHERE c.status='open' AND p.path_tracking_eligible=1
        """
    ).fetchall()
    updated = 0
    for row in rows:
        values = base.quote_values(quotes, str(row["instrument"]))
        if values is None:
            continue
        bid, ask, _ = values
        pip = max(1e-12, base.safe_float(row["pip"]))
        if str(row["direction"]) == "buy":
            signed_pips = (bid - base.safe_float(row["entry_ask"])) / pip
        else:
            signed_pips = (base.safe_float(row["entry_bid"]) - ask) / pip
        connection.execute(
            """
            UPDATE positions SET
                path_sample_count=path_sample_count+1,
                max_adverse_pips=MAX(max_adverse_pips, ?),
                max_favorable_pips=MAX(max_favorable_pips, ?)
            WHERE decision_id=? AND rank_in_cohort=?
            """,
            (
                max(0.0, -signed_pips),
                max(0.0, signed_pips),
                row["decision_id"],
                row["rank_in_cohort"],
            ),
        )
        spread_pips = max(1e-9, base.safe_float(row["entry_spread_pips"]))
        for multiple in BARRIER_COST_MULTIPLES:
            barrier_id = f"cost_{str(multiple).replace('.', '_')}x_symmetric"
            barrier_pips = spread_pips * multiple
            connection.execute(
                """
                INSERT OR IGNORE INTO competing_risk_barrier_paths(
                    decision_id, rank_in_cohort, barrier_id,
                    target_pips, stop_pips
                ) VALUES(?, ?, ?, ?, ?)
                """,
                (
                    row["decision_id"],
                    row["rank_in_cohort"],
                    barrier_id,
                    barrier_pips,
                    barrier_pips,
                ),
            )
            hit_target = signed_pips >= barrier_pips
            hit_stop = signed_pips <= -barrier_pips
            connection.execute(
                """
                UPDATE competing_risk_barrier_paths SET
                    sample_count=sample_count+1,
                    first_target_epoch=CASE
                        WHEN first_target_epoch IS NULL AND ? THEN ?
                        ELSE first_target_epoch END,
                    first_stop_epoch=CASE
                        WHEN first_stop_epoch IS NULL AND ? THEN ?
                        ELSE first_stop_epoch END,
                    first_event=CASE
                        WHEN first_event='' AND ? THEN 'target'
                        WHEN first_event='' AND ? THEN 'stop'
                        ELSE first_event END,
                    first_event_epoch=CASE
                        WHEN first_event_epoch IS NULL AND (? OR ?) THEN ?
                        ELSE first_event_epoch END,
                    last_signed_pips=?
                WHERE decision_id=? AND rank_in_cohort=? AND barrier_id=?
                """,
                (
                    int(hit_target),
                    sampled_epoch,
                    int(hit_stop),
                    sampled_epoch,
                    int(hit_target),
                    int(hit_stop),
                    int(hit_target),
                    int(hit_stop),
                    sampled_epoch,
                    signed_pips,
                    row["decision_id"],
                    row["rank_in_cohort"],
                    barrier_id,
                ),
            )
        updated += 1
    connection.commit()
    return updated


def mature_cohorts(
    connection: sqlite3.Connection,
    quotes: dict[str, dict[str, Any]],
    now_epoch: float,
) -> int:
    connection.row_factory = sqlite3.Row
    cohorts = connection.execute(
        "SELECT * FROM cohorts WHERE status='open' AND target_epoch<=? ORDER BY target_epoch",
        (now_epoch,),
    ).fetchall()
    matured = 0
    for cohort in cohorts:
        rows = connection.execute(
            "SELECT * FROM positions WHERE decision_id=? ORDER BY rank_in_cohort",
            (cohort["decision_id"],),
        ).fetchall()
        outcomes: list[tuple[sqlite3.Row, float, float, float, float, float]] = []
        for row in rows:
            values = base.quote_values(quotes, str(row["instrument"]))
            if values is None:
                outcomes = []
                break
            bid, ask, _ = values
            direction = str(row["direction"])
            entry_price = row["entry_ask"] if direction == "buy" else row["entry_bid"]
            exit_price = bid if direction == "buy" else ask
            signed_change = exit_price - entry_price if direction == "buy" else entry_price - exit_price
            net_pips = signed_change / max(1e-12, base.safe_float(row["pip"]))
            return_pct = 100.0 * signed_change / max(1e-12, base.safe_float(entry_price))
            spread_units = net_pips / max(1e-12, base.safe_float(row["entry_spread_pips"]))
            outcomes.append((row, bid, ask, net_pips, return_pct, spread_units))
        if not outcomes:
            continue
        weighted_pips = 0.0
        weighted_return = 0.0
        weighted_spread_units = 0.0
        for row, bid, ask, net_pips, return_pct, spread_units in outcomes:
            weight = base.safe_float(row["weight"])
            weighted_pips += weight * net_pips
            weighted_return += weight * return_pct
            weighted_spread_units += weight * spread_units
            connection.execute(
                """
                UPDATE positions SET exit_bid=?, exit_ask=?, exit_mid=?,
                    net_pips=?, return_pct=?, spread_units=?, win=?
                WHERE decision_id=? AND rank_in_cohort=?
                """,
                (
                    bid,
                    ask,
                    (bid + ask) / 2.0,
                    net_pips,
                    return_pct,
                    spread_units,
                    int(return_pct > 0.0),
                    cohort["decision_id"],
                    row["rank_in_cohort"],
                ),
            )
        connection.execute(
            """
            UPDATE cohorts SET status='matured', closed_epoch=?, closed_at=?,
                weighted_net_pips=?, weighted_return_pct=?,
                weighted_spread_units=?, win=? WHERE decision_id=?
            """,
            (
                now_epoch,
                datetime.fromtimestamp(now_epoch, UTC).isoformat(),
                weighted_pips,
                weighted_return,
                weighted_spread_units,
                int(weighted_return > 0.0),
                cohort["decision_id"],
            ),
        )
        matured += 1
    connection.commit()
    return matured


def summarized_results(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT * FROM cohorts WHERE status='matured'
        AND measurement_version=? ORDER BY closed_epoch
        """,
        (MEASUREMENT_VERSION,),
    ).fetchall()
    grouped: dict[tuple[str, int], list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["arm"]), int(row["horizon_sec"]))].append(row)
    output: list[dict[str, Any]] = []
    for (arm, horizon), items in sorted(grouped.items()):
        returns = [base.safe_float(row["weighted_return_pct"]) for row in items]
        spread_units = [base.safe_float(row["weighted_spread_units"]) for row in items]
        pips = [base.safe_float(row["weighted_net_pips"]) for row in items]
        payoff = payoff_summary(pips)
        output.append(
            {
                "arm": arm,
                "horizon_sec": horizon,
                "horizon_label": base.horizon_label(horizon),
                "independent_decisions": len(items),
                "wins": sum(int(row["win"] or 0) for row in items),
                "win_rate_pct": round(
                    100.0 * sum(int(row["win"] or 0) for row in items) / len(items), 3
                ),
                "avg_weighted_return_pct": round(statistics.fmean(returns), 6),
                "median_weighted_return_pct": round(statistics.median(returns), 6),
                "avg_weighted_spread_units": round(statistics.fmean(spread_units), 6),
                "avg_weighted_net_pips": round(statistics.fmean(pips), 6),
                "median_weighted_net_pips": round(statistics.median(pips), 6),
                "total_weighted_net_pips": payoff["total_net_pips"],
                "best_weighted_net_pips": payoff["best_net_pips"],
                "worst_weighted_net_pips": payoff["worst_net_pips"],
                "avg_weighted_win_pips": payoff["avg_win_pips"],
                "avg_weighted_loss_pips": payoff["avg_loss_pips"],
                "payoff_ratio": payoff["payoff_ratio"],
                "profit_factor": payoff["profit_factor"],
            }
        )
    return output


def summarized_cost_results(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT c.arm, c.horizon_sec,
               CASE
                   WHEN p.entry_spread_pips <= 2.0 THEN 'liquid_le_2'
                   WHEN p.entry_spread_pips <= 3.0 THEN 'normal_2_to_3'
                   WHEN p.entry_spread_pips <= 5.0 THEN 'elevated_3_to_5'
                   ELSE 'wide_gt_5'
               END AS cost_bucket,
               COUNT(*) AS n,
               SUM(CASE WHEN p.win=1 THEN 1 ELSE 0 END) AS wins,
               AVG(p.entry_spread_pips), AVG(p.net_pips), AVG(p.spread_units)
        FROM positions p JOIN cohorts c USING(decision_id)
        WHERE c.status='matured' AND c.measurement_version=?
        GROUP BY c.arm, c.horizon_sec, cost_bucket
        ORDER BY c.arm, c.horizon_sec, cost_bucket
        """,
        (MEASUREMENT_VERSION,),
    ).fetchall()
    return [
        {
            "arm": str(row[0]),
            "horizon_sec": int(row[1]),
            "horizon_label": base.horizon_label(int(row[1])),
            "cost_bucket": str(row[2]),
            "members": int(row[3]),
            "after_cost_wins": int(row[4]),
            "after_cost_win_rate_pct": round(
                100.0 * int(row[4]) / max(1, int(row[3])), 3
            ),
            "avg_entry_spread_pips": round(base.safe_float(row[5]), 4),
            "avg_net_pips": round(base.safe_float(row[6]), 4),
            "avg_spread_units": round(base.safe_float(row[7]), 4),
        }
        for row in rows
    ]


def inverse_outcome_summary(values: Iterable[float]) -> dict[str, Any]:
    pips = [float(value) for value in values]
    payoff = payoff_summary(pips)
    wins = sum(value > 0.0 for value in pips)
    return {
        "decisions": len(pips),
        "wins": wins,
        "win_rate_pct": round(100.0 * wins / len(pips), 3) if pips else None,
        "avg_net_pips": round(statistics.fmean(pips), 6) if pips else None,
        "median_net_pips": round(statistics.median(pips), 6) if pips else None,
        **payoff,
    }


def h2_inverse_pair_group(instrument: str) -> str:
    currencies = str(instrument or "").upper().split("_")
    if "JPY" in currencies:
        return "jpy_pair"
    if "USD" in currencies:
        return "usd_pair_ex_jpy"
    return "non_usd_cross"


def h2_inverse_cost_bucket(spread_pips: float) -> str:
    spread = float(spread_pips)
    if spread <= 2.0:
        return "liquid_le_2"
    if spread <= 3.0:
        return "normal_2_to_3"
    if spread <= 5.0:
        return "elevated_3_to_5"
    return "wide_gt_5"


def summarized_h2_inverse_shadow(connection: sqlite3.Connection) -> dict[str, Any]:
    """Compare source decisions with honest opposite-side measurements.

    The retrospective section uses stored executable entry and exit bid/ask
    quotes and is diagnostic only.  The prospective section reports the two
    dedicated direction-pair arms created after discovery, including the
    prospective conflicted-aggressive comparator; those rows are opened and
    matured normally by this no-order ledger.  Nothing in this summary is read
    by the executor.
    """

    connection.row_factory = sqlite3.Row
    source_arms = (
        "opportunity_price_top1",
        "h2_persistence",
        "conflicted_aggressive_top1",
    )
    placeholders = ",".join("?" for _ in source_arms)
    rows = connection.execute(
        f"""
        SELECT c.arm, c.observed_epoch, c.market_session,
               p.instrument, p.direction, p.signal_id, p.strategy_family,
               p.event_regime,
               p.entry_bid, p.entry_ask, p.exit_bid, p.exit_ask, p.pip,
               p.entry_spread_pips, p.net_pips
        FROM cohorts c JOIN positions p USING(decision_id)
        WHERE c.measurement_version=? AND c.status='matured'
          AND c.horizon_sec=7200 AND c.member_count=1
          AND p.rank_in_cohort=1 AND p.net_pips IS NOT NULL
          AND p.exit_bid IS NOT NULL AND p.exit_ask IS NOT NULL
          AND c.arm IN ({placeholders})
        ORDER BY c.observed_epoch, c.arm
        """,
        (MEASUREMENT_VERSION, *source_arms),
    ).fetchall()

    prepared: list[dict[str, Any]] = []
    for row in rows:
        direction = str(row["direction"]).lower()
        pip = max(1e-12, base.safe_float(row["pip"]))
        if direction in {"buy", "long"}:
            inverse_net = (
                base.safe_float(row["entry_bid"])
                - base.safe_float(row["exit_ask"])
            ) / pip
        else:
            inverse_net = (
                base.safe_float(row["exit_bid"])
                - base.safe_float(row["entry_ask"])
            ) / pip
        legacy_family, variant = signal_family(str(row["signal_id"]))
        family = str(row["strategy_family"] or "") or legacy_family
        prepared.append(
            {
                "arm": str(row["arm"]),
                "observed_epoch": base.safe_float(row["observed_epoch"]),
                "market_session": str(row["market_session"]),
                "instrument": str(row["instrument"]),
                "direction": direction,
                "signal_id": str(row["signal_id"]),
                "event_regime": "event" if bool(row["event_regime"]) else "no_event",
                "pair_group": h2_inverse_pair_group(str(row["instrument"])),
                "cost_bucket": h2_inverse_cost_bucket(
                    base.safe_float(row["entry_spread_pips"])
                ),
                "strategy_family": family,
                "strategy_variant": variant,
                "entry_spread_pips": base.safe_float(row["entry_spread_pips"]),
                "original_net_pips": base.safe_float(row["net_pips"]),
                "inverse_net_pips": inverse_net,
            }
        )

    exact: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in prepared:
        key = (
            round(row["observed_epoch"], 3),
            row["instrument"],
            row["direction"],
            row["signal_id"],
        )
        exact.setdefault(key, row)

    populations: dict[str, list[dict[str, Any]]] = {
        "all_source_decisions_unique": list(exact.values()),
    }
    for arm in source_arms:
        populations[arm] = [row for row in prepared if row["arm"] == arm]

    retrospective: list[dict[str, Any]] = []
    dimensions = (
        "pair_group",
        "market_session",
        "event_regime",
        "cost_bucket",
        "strategy_family",
    )
    for population, items in populations.items():
        segments: dict[str, list[dict[str, Any]]] = {}
        for dimension in dimensions:
            grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for item in items:
                grouped[str(item[dimension])].append(item)
            segments[dimension] = [
                {
                    "value": value,
                    "original": inverse_outcome_summary(
                        item["original_net_pips"] for item in group
                    ),
                    "inverse": inverse_outcome_summary(
                        item["inverse_net_pips"] for item in group
                    ),
                }
                for value, group in sorted(grouped.items())
            ]
        retrospective.append(
            {
                "population": population,
                "original": inverse_outcome_summary(
                    item["original_net_pips"] for item in items
                ),
                "inverse": inverse_outcome_summary(
                    item["inverse_net_pips"] for item in items
                ),
                "segments": segments,
            }
        )

    pair_arms = (
        "opportunity_price_top1_direction_pair_h2",
        "h2_persistence_direction_pair",
        "h2_strict_validated_direction_pair",
        "conflicted_aggressive_direction_pair",
    )
    pair_placeholders = ",".join("?" for _ in pair_arms)
    prospective_rows = connection.execute(
        f"""
        SELECT c.decision_id, c.arm, c.horizon_sec, c.horizon_label,
               c.market_session, p.rank_in_cohort,
               p.instrument, p.signal_id, p.strategy_family, p.event_regime,
               p.entry_spread_pips, p.net_pips
        FROM cohorts c JOIN positions p USING(decision_id)
        WHERE c.measurement_version=? AND c.status='matured'
          AND c.member_count=2
          AND p.rank_in_cohort IN (1, 2) AND p.net_pips IS NOT NULL
          AND c.arm IN ({pair_placeholders})
        ORDER BY c.observed_epoch, c.arm, p.rank_in_cohort
        """,
        (MEASUREMENT_VERSION, *pair_arms),
    ).fetchall()
    paired: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in prospective_rows:
        paired[str(row["decision_id"])].append(row)
    prospective_pairs: list[dict[str, Any]] = []
    for pair in paired.values():
        by_rank = {int(row["rank_in_cohort"]): row for row in pair}
        if set(by_rank) != {1, 2}:
            continue
        original = by_rank[1]
        inverse = by_rank[2]
        if str(original["instrument"]) != str(inverse["instrument"]):
            continue
        legacy_family, _ = signal_family(str(original["signal_id"]))
        family = str(original["strategy_family"] or "") or legacy_family
        prospective_pairs.append(
            {
                "arm": str(original["arm"]),
                "horizon_sec": int(original["horizon_sec"]),
                "horizon_label": str(original["horizon_label"]),
                "instrument": str(original["instrument"]),
                "pair_group": h2_inverse_pair_group(str(original["instrument"])),
                "market_session": str(original["market_session"]),
                "event_regime": (
                    "event" if bool(original["event_regime"]) else "no_event"
                ),
                "cost_bucket": h2_inverse_cost_bucket(
                    base.safe_float(original["entry_spread_pips"])
                ),
                "strategy_family": family,
                "original_net_pips": base.safe_float(original["net_pips"]),
                "inverse_net_pips": base.safe_float(inverse["net_pips"]),
            }
        )

    prospective: list[dict[str, Any]] = []
    for arm in pair_arms:
        arm_rows = [row for row in prospective_pairs if row["arm"] == arm]
        segments: dict[str, list[dict[str, Any]]] = {}
        for dimension in (*dimensions, "horizon_label"):
            grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for item in arm_rows:
                grouped[str(item[dimension])].append(item)
            segments[dimension] = [
                {
                    "value": value,
                    "original": inverse_outcome_summary(
                        item["original_net_pips"] for item in group
                    ),
                    "inverse": inverse_outcome_summary(
                        item["inverse_net_pips"] for item in group
                    ),
                }
                for value, group in sorted(grouped.items())
            ]
        prospective.append(
            {
                "arm": arm,
                "matched_pairs": len(arm_rows),
                "original": inverse_outcome_summary(
                    row["original_net_pips"] for row in arm_rows
                ),
                "inverse": inverse_outcome_summary(
                    row["inverse_net_pips"] for row in arm_rows
                ),
                "segments": segments,
            }
        )

    legacy_inverse_arms = (
        "opportunity_price_top1_inverse_h2",
        "h2_persistence_inverse",
    )
    legacy_placeholders = ",".join("?" for _ in legacy_inverse_arms)
    legacy_rows = connection.execute(
        f"""
        SELECT c.arm, p.net_pips FROM cohorts c JOIN positions p USING(decision_id)
        WHERE c.measurement_version=? AND c.horizon_sec=7200
          AND c.member_count=1 AND p.rank_in_cohort=1
          AND c.arm IN ({legacy_placeholders})
          AND p.net_pips IS NOT NULL
        """,
        (MEASUREMENT_VERSION, *legacy_inverse_arms),
    ).fetchall()

    return {
        "status": "shadow_only_no_execution_hook",
        "can_change_execution": False,
        "selection_policy": (
            "store matched original/opposite pairs for selected H2 "
            "opportunity-top1, persistence, and separately strict validated "
            "decisions, plus prospective conflicted-aggressive decisions at "
            "their declared horizon"
        ),
        "retrospective_method": (
            "same stored entry and maturity; opposite side pays its own "
            "executable bid/ask spread; exact cross-arm copies collapsed only "
            "in all_source_decisions_unique"
        ),
        "retrospective": retrospective,
        "prospective_direction_pairs": prospective,
        "unpaired_bootstrap_inverse_outcomes": [
            {
                "arm": arm,
                "excluded_from_paired_evidence": True,
                "outcomes": inverse_outcome_summary(
                    base.safe_float(row["net_pips"])
                    for row in legacy_rows
                    if str(row["arm"]) == arm
                ),
            }
            for arm in legacy_inverse_arms
        ],
    }


def summarized_underlying_decisions(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    """Collapse identical cross-arm cohorts for truthful decision-level reporting.

    Trial arms remain separate experiments.  This view only prevents one market
    decision, opened at the same instant with the same executable members, from
    being described as several independent forecasts when multiple arms select it.
    """
    connection.row_factory = sqlite3.Row
    cohorts = connection.execute(
        """
        SELECT * FROM cohorts WHERE measurement_version=?
        AND status IN ('open', 'matured') ORDER BY observed_epoch, arm
        """,
        (MEASUREMENT_VERSION,),
    ).fetchall()
    grouped: dict[tuple[Any, ...], list[sqlite3.Row]] = defaultdict(list)
    for cohort in cohorts:
        members = connection.execute(
            """
            SELECT rank_in_cohort, instrument, direction, weight, signal_id,
                   entry_bid, entry_ask
            FROM positions WHERE decision_id=? ORDER BY rank_in_cohort
            """,
            (cohort["decision_id"],),
        ).fetchall()
        signature = (
            int(cohort["horizon_sec"]),
            round(base.safe_float(cohort["observed_epoch"]), 6),
            tuple(
                (
                    int(member["rank_in_cohort"]),
                    str(member["instrument"]),
                    str(member["direction"]),
                    round(base.safe_float(member["weight"]), 12),
                    str(member["signal_id"]),
                    round(base.safe_float(member["entry_bid"]), 12),
                    round(base.safe_float(member["entry_ask"]), 12),
                )
                for member in members
            ),
        )
        grouped[signature].append(cohort)

    status_counts: dict[str, int] = defaultdict(int)
    arm_cohort_counts: dict[str, int] = defaultdict(int)
    matured_by_horizon: dict[int, list[sqlite3.Row]] = defaultdict(list)
    open_groups: list[dict[str, Any]] = []
    for items in grouped.values():
        representative = items[0]
        status = str(representative["status"])
        status_counts[status] += 1
        arm_cohort_counts[status] += len(items)
        if status == "matured":
            matured_by_horizon[int(representative["horizon_sec"])].append(
                representative
            )
        else:
            open_groups.append(
                {
                    "observed_at": str(representative["observed_at"]),
                    "target_at": str(representative["target_at"]),
                    "horizon_sec": int(representative["horizon_sec"]),
                    "horizon_label": str(representative["horizon_label"]),
                    "arms": sorted(str(item["arm"]) for item in items),
                    "arm_cohort_count": len(items),
                    "member_count": int(representative["member_count"]),
                }
            )

    results: list[dict[str, Any]] = []
    for horizon, items in sorted(matured_by_horizon.items()):
        returns = [base.safe_float(item["weighted_return_pct"]) for item in items]
        pips = [base.safe_float(item["weighted_net_pips"]) for item in items]
        wins = sum(int(item["win"] or 0) for item in items)
        results.append(
            {
                "horizon_sec": horizon,
                "horizon_label": base.horizon_label(horizon),
                "unique_underlying_decisions": len(items),
                "wins": wins,
                "win_rate_pct": round(100.0 * wins / len(items), 3),
                "avg_weighted_return_pct": round(statistics.fmean(returns), 6),
                "avg_weighted_net_pips": round(statistics.fmean(pips), 6),
            }
        )

    total_arm_cohorts = sum(arm_cohort_counts.values())
    total_unique = sum(status_counts.values())
    return {
        "policy": (
            "reporting-only collapse of cohorts with identical horizon, observed "
            "time, signal members, weights, and executable entry quotes; arm-level "
            "A/B results remain unchanged"
        ),
        "arm_cohorts": total_arm_cohorts,
        "unique_underlying_decisions": total_unique,
        "cross_arm_duplicates_excluded": total_arm_cohorts - total_unique,
        "status_counts": dict(sorted(status_counts.items())),
        "arm_cohort_status_counts": dict(sorted(arm_cohort_counts.items())),
        "results": results,
        "open_groups": sorted(
            open_groups, key=lambda row: (row["target_at"], row["horizon_sec"])
        ),
    }


def summarized_factor_episodes(
    connection: sqlite3.Connection,
    window_sec: float = FACTOR_EPISODE_WINDOW_SEC,
) -> dict[str, Any]:
    """Collapse nearby signed-currency propagation for reporting only.

    The arm tables and exact underlying-decision view remain unchanged.  This
    additional view stops several entries into the same currency move from
    masquerading as independent evidence merely because they used different
    pairs, arms, or timestamps a few minutes apart.
    """
    connection.row_factory = sqlite3.Row
    cohorts = connection.execute(
        """
        SELECT * FROM cohorts WHERE measurement_version=?
        AND status IN ('open', 'matured') ORDER BY observed_epoch, arm
        """,
        (MEASUREMENT_VERSION,),
    ).fetchall()
    exact_groups: dict[tuple[Any, ...], dict[str, Any]] = {}
    for cohort in cohorts:
        members = connection.execute(
            """
            SELECT rank_in_cohort, instrument, direction, weight, signal_id,
                   entry_bid, entry_ask, correlation_factor_ids_json
            FROM positions WHERE decision_id=? ORDER BY rank_in_cohort
            """,
            (cohort["decision_id"],),
        ).fetchall()
        signature = (
            int(cohort["horizon_sec"]),
            round(base.safe_float(cohort["observed_epoch"]), 6),
            tuple(
                (
                    int(member["rank_in_cohort"]),
                    str(member["instrument"]),
                    str(member["direction"]),
                    round(base.safe_float(member["weight"]), 12),
                    str(member["signal_id"]),
                    round(base.safe_float(member["entry_bid"]), 12),
                    round(base.safe_float(member["entry_ask"]), 12),
                )
                for member in members
            ),
        )
        factor_ids: set[str] = set()
        for member in members:
            try:
                stored = json.loads(str(member["correlation_factor_ids_json"] or "[]"))
            except (TypeError, ValueError, json.JSONDecodeError):
                stored = []
            factor_ids.update(str(value) for value in stored if str(value))
            if not stored:
                factor_ids.update(
                    correlation_factor_ids(
                        {
                            "instrument": member["instrument"],
                            "direction": member["direction"],
                        }
                    )
                )
        group = exact_groups.setdefault(
            signature,
            {
                "representative": cohort,
                "arms": set(),
                "factor_ids": factor_ids,
            },
        )
        group["arms"].add(str(cohort["arm"]))
        group["factor_ids"].update(factor_ids)

    episodes: list[dict[str, Any]] = []
    for group in sorted(
        exact_groups.values(),
        key=lambda item: base.safe_float(item["representative"]["observed_epoch"]),
    ):
        representative = group["representative"]
        observed_epoch = base.safe_float(representative["observed_epoch"])
        factors = set(group["factor_ids"])
        match = next(
            (
                episode
                for episode in reversed(episodes)
                if episode["horizon_sec"] == int(representative["horizon_sec"])
                and observed_epoch - episode["start_epoch"] <= float(window_sec)
                and factors.intersection(episode["factor_ids"])
            ),
            None,
        )
        if match is None:
            match = {
                "horizon_sec": int(representative["horizon_sec"]),
                "start_epoch": observed_epoch,
                "end_epoch": observed_epoch,
                "factor_ids": set(factors),
                "groups": [],
            }
            episodes.append(match)
        match["end_epoch"] = max(match["end_epoch"], observed_epoch)
        match["factor_ids"].update(factors)
        match["groups"].append(group)

    matured_by_horizon: dict[int, list[dict[str, Any]]] = defaultdict(list)
    status_counts: dict[str, int] = defaultdict(int)
    open_episodes: list[dict[str, Any]] = []
    for episode in episodes:
        representatives = [group["representative"] for group in episode["groups"]]
        status = (
            "open"
            if any(str(item["status"]) == "open" for item in representatives)
            else "matured"
        )
        status_counts[status] += 1
        if status == "matured":
            net_pips = statistics.fmean(
                base.safe_float(item["weighted_net_pips"])
                for item in representatives
            )
            return_pct = statistics.fmean(
                base.safe_float(item["weighted_return_pct"])
                for item in representatives
            )
            matured_by_horizon[episode["horizon_sec"]].append(
                {
                    "net_pips": net_pips,
                    "return_pct": return_pct,
                    "win": int(net_pips > 0.0),
                    "underlying_decisions": len(representatives),
                }
            )
        else:
            open_episodes.append(
                {
                    "observed_at": datetime.fromtimestamp(
                        episode["start_epoch"], UTC
                    ).isoformat(),
                    "last_observed_at": datetime.fromtimestamp(
                        episode["end_epoch"], UTC
                    ).isoformat(),
                    "horizon_sec": episode["horizon_sec"],
                    "horizon_label": base.horizon_label(episode["horizon_sec"]),
                    "factor_ids": sorted(episode["factor_ids"]),
                    "underlying_decisions": len(representatives),
                    "arms": sorted(
                        {
                            arm
                            for group in episode["groups"]
                            for arm in group["arms"]
                        }
                    ),
                }
            )

    results: list[dict[str, Any]] = []
    for horizon, items in sorted(matured_by_horizon.items()):
        pips = [item["net_pips"] for item in items]
        wins = sum(item["win"] for item in items)
        payoff = payoff_summary(pips)
        results.append(
            {
                "horizon_sec": horizon,
                "horizon_label": base.horizon_label(horizon),
                "independent_factor_episodes": len(items),
                "underlying_decisions": sum(
                    item["underlying_decisions"] for item in items
                ),
                "factor_repeats_collapsed": sum(
                    item["underlying_decisions"] for item in items
                )
                - len(items),
                "wins": wins,
                "win_rate_pct": round(100.0 * wins / len(items), 3),
                "avg_episode_return_pct": round(
                    statistics.fmean(item["return_pct"] for item in items), 6
                ),
                "avg_episode_net_pips": round(statistics.fmean(pips), 6),
                "median_episode_net_pips": round(statistics.median(pips), 6),
                "episode_total_net_pips": payoff["total_net_pips"],
                "episode_best_net_pips": payoff["best_net_pips"],
                "episode_worst_net_pips": payoff["worst_net_pips"],
                "episode_payoff_ratio": payoff["payoff_ratio"],
                "episode_profit_factor": payoff["profit_factor"],
            }
        )

    total_underlying = len(exact_groups)
    return {
        "policy": (
            "reporting-only 15-minute episode collapse by overlapping signed "
            "currency factor and horizon; raw cohorts, exact decisions, and "
            "arm-level A/B results remain unchanged"
        ),
        "window_sec": float(window_sec),
        "underlying_decisions": total_underlying,
        "independent_factor_episodes": len(episodes),
        "factor_repeats_collapsed": total_underlying - len(episodes),
        "status_counts": dict(sorted(status_counts.items())),
        "results": results,
        "open_episodes": sorted(
            open_episodes,
            key=lambda row: (row["observed_at"], row["horizon_sec"]),
        ),
    }


def summarized_pair_direction_flips(
    connection: sqlite3.Connection,
    *,
    flip_window_sec: float = PAIR_FLIP_WINDOW_SEC,
    episode_window_sec: float = PAIR_FLIP_EPISODE_SEC,
    magnitude_split_pips: float = PAIR_FLIP_MAGNITUDE_SPLIT_PIPS,
    discovery_epoch: float = PAIR_FLIP_DISCOVERY_EPOCH,
) -> dict[str, Any]:
    """Report short-window pair/horizon reversals without altering policy.

    Exact cross-arm copies are removed first. Nearby repeated flips in the same
    pair, horizon, direction and projected-magnitude band are then averaged into
    one episode so rapid refreshes cannot inflate the evidence. Rows observed
    before this diagnostic was designed remain explicitly retrospective.
    """

    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT c.observed_epoch, c.observed_at, c.horizon_sec, c.horizon_label,
               p.instrument, p.direction, p.signal_id, p.entry_bid, p.entry_ask,
               p.projected_net_pips, p.net_pips
        FROM cohorts c JOIN positions p USING(decision_id)
        WHERE c.measurement_version=? AND c.status='matured'
          AND c.member_count=1 AND p.net_pips IS NOT NULL
        ORDER BY c.observed_epoch, c.arm
        """,
        (MEASUREMENT_VERSION,),
    ).fetchall()
    exact: dict[tuple[Any, ...], sqlite3.Row] = {}
    for row in rows:
        key = (
            round(base.safe_float(row["observed_epoch"]), 6),
            int(row["horizon_sec"]),
            str(row["instrument"]),
            str(row["direction"]),
            str(row["signal_id"]),
            round(base.safe_float(row["entry_bid"]), 12),
            round(base.safe_float(row["entry_ask"]), 12),
        )
        exact.setdefault(key, row)

    history: dict[tuple[str, int], list[sqlite3.Row]] = defaultdict(list)
    flips: list[dict[str, Any]] = []
    for row in sorted(
        exact.values(), key=lambda item: base.safe_float(item["observed_epoch"])
    ):
        observed = base.safe_float(row["observed_epoch"])
        series_key = (str(row["instrument"]), int(row["horizon_sec"]))
        prior = next(
            (
                candidate
                for candidate in reversed(history[series_key])
                if observed - base.safe_float(candidate["observed_epoch"])
                <= float(flip_window_sec)
                and str(candidate["direction"]) != str(row["direction"])
            ),
            None,
        )
        if prior is not None:
            projected = base.safe_float(row["projected_net_pips"])
            flips.append(
                {
                    "observed_epoch": observed,
                    "instrument": str(row["instrument"]),
                    "horizon_sec": int(row["horizon_sec"]),
                    "direction": str(row["direction"]),
                    "net_pips": base.safe_float(row["net_pips"]),
                    "projected_net_pips": projected,
                    "magnitude_band": (
                        "projected_net_ge_2"
                        if projected >= float(magnitude_split_pips)
                        else "projected_net_lt_2"
                    ),
                    "evidence_scope": (
                        "prospective"
                        if observed >= float(discovery_epoch)
                        else "retrospective_discovery"
                    ),
                }
            )
        history[series_key].append(row)

    episodes: list[dict[str, Any]] = []
    for flip in flips:
        match = next(
            (
                episode
                for episode in reversed(episodes)
                if episode["instrument"] == flip["instrument"]
                and episode["horizon_sec"] == flip["horizon_sec"]
                and episode["direction"] == flip["direction"]
                and episode["magnitude_band"] == flip["magnitude_band"]
                and episode["evidence_scope"] == flip["evidence_scope"]
                and flip["observed_epoch"] - episode["start_epoch"]
                <= float(episode_window_sec)
            ),
            None,
        )
        if match is None:
            match = {
                "instrument": flip["instrument"],
                "horizon_sec": flip["horizon_sec"],
                "direction": flip["direction"],
                "magnitude_band": flip["magnitude_band"],
                "evidence_scope": flip["evidence_scope"],
                "start_epoch": flip["observed_epoch"],
                "net_pips": [],
            }
            episodes.append(match)
        match["net_pips"].append(flip["net_pips"])

    grouped_flips: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    grouped_episodes: dict[tuple[str, str], list[float]] = defaultdict(list)
    for flip in flips:
        grouped_flips[(flip["evidence_scope"], flip["magnitude_band"])].append(flip)
    for episode in episodes:
        grouped_episodes[(
            episode["evidence_scope"], episode["magnitude_band"]
        )].append(statistics.fmean(episode["net_pips"]))

    results: list[dict[str, Any]] = []
    for key in sorted(set(grouped_flips) | set(grouped_episodes)):
        scope, band = key
        raw = grouped_flips.get(key, [])
        episode_pips = grouped_episodes.get(key, [])
        raw_pips = [item["net_pips"] for item in raw]
        raw_payoff = payoff_summary(raw_pips)
        episode_payoff = payoff_summary(episode_pips)
        results.append(
            {
                "evidence_scope": scope,
                "magnitude_band": band,
                "exact_flips": len(raw_pips),
                "exact_wins": sum(value > 0.0 for value in raw_pips),
                "exact_win_rate_pct": round(
                    100.0 * sum(value > 0.0 for value in raw_pips)
                    / max(1, len(raw_pips)),
                    3,
                ),
                "exact_total_net_pips": raw_payoff["total_net_pips"],
                "factor_episodes": len(episode_pips),
                "repeats_collapsed": len(raw_pips) - len(episode_pips),
                "factor_episode_wins": sum(value > 0.0 for value in episode_pips),
                "factor_episode_win_rate_pct": round(
                    100.0 * sum(value > 0.0 for value in episode_pips)
                    / max(1, len(episode_pips)),
                    3,
                ),
                "factor_episode_total_net_pips": episode_payoff["total_net_pips"],
                "factor_episode_profit_factor": episode_payoff["profit_factor"],
                "factor_episode_worst_net_pips": episode_payoff["worst_net_pips"],
            }
        )

    return {
        "status": "reporting_only_no_execution_hook",
        "can_change_execution": False,
        "flip_window_sec": float(flip_window_sec),
        "episode_window_sec": float(episode_window_sec),
        "magnitude_split_pips": float(magnitude_split_pips),
        "discovery_utc": datetime.fromtimestamp(discovery_epoch, UTC).isoformat(),
        "method": (
            "single-member matured canonical decisions; exact cross-arm copies "
            "removed; same pair/horizon/direction repeats collapsed within 15 minutes"
        ),
        "results": results,
    }


def signal_family(signal_id: str) -> tuple[str, str]:
    """Return the archetype and full variant embedded in a live signal ID."""
    parts = str(signal_id or "").split("-")
    variant = "-".join(parts[1:-1]).strip() if len(parts) >= 3 else ""
    if not variant:
        variant = "unknown"
    return variant.split(".", 1)[0], variant


def payoff_summary(values: Iterable[float]) -> dict[str, float | None]:
    """Expose outcome asymmetry without letting win rate hide tail losses."""

    pips = [float(value) for value in values]
    positive = [value for value in pips if value > 0.0]
    negative = [value for value in pips if value < 0.0]
    gross_gain = sum(positive)
    gross_loss = -sum(negative)
    avg_win = statistics.fmean(positive) if positive else None
    avg_loss = statistics.fmean(negative) if negative else None
    return {
        "total_net_pips": round(sum(pips), 6),
        "best_net_pips": round(max(pips), 6) if pips else None,
        "worst_net_pips": round(min(pips), 6) if pips else None,
        "avg_win_pips": round(avg_win, 6) if avg_win is not None else None,
        "avg_loss_pips": round(avg_loss, 6) if avg_loss is not None else None,
        "payoff_ratio": (
            round(avg_win / abs(avg_loss), 6)
            if avg_win is not None and avg_loss not in {None, 0.0}
            else None
        ),
        "profit_factor": (
            round(gross_gain / gross_loss, 6) if gross_loss > 0.0 else None
        ),
    }


def summarized_horizon_readiness(
    factor_summary: dict[str, Any],
    *,
    minimum_independent_factor_episodes: int = 30,
    minimum_win_rate_pct: float = 52.0,
    minimum_profit_factor: float = 1.10,
) -> dict[str, Any]:
    """Expose whether broad horizon evidence supports trading or abstention.

    This is intentionally a reporting-only guardrail.  It prevents a small
    recent arm or one favorable family cell from being described as evidence
    for the entire horizon while leaving all trial and execution policies
    unchanged.
    """

    rows: list[dict[str, Any]] = []
    for result in factor_summary.get("results") or []:
        episodes = int(result.get("independent_factor_episodes") or 0)
        win_rate = base.safe_float(result.get("win_rate_pct"))
        average = base.safe_float(result.get("avg_episode_net_pips"))
        total = base.safe_float(result.get("episode_total_net_pips"))
        profit_factor_value = result.get("episode_profit_factor")
        profit_factor = (
            base.safe_float(profit_factor_value)
            if profit_factor_value is not None
            else None
        )
        sample_gate = episodes >= int(minimum_independent_factor_episodes)
        expectancy_gate = average > 0.0 and total > 0.0
        win_rate_gate = win_rate >= float(minimum_win_rate_pct)
        profit_factor_gate = (
            profit_factor is not None
            and profit_factor >= float(minimum_profit_factor)
        )
        ready = bool(
            sample_gate
            and expectancy_gate
            and win_rate_gate
            and profit_factor_gate
        )
        if ready:
            disposition = "shadow_ready_for_frozen_holdout"
        elif not sample_gate:
            disposition = "abstain_insufficient_independent_evidence"
        elif not expectancy_gate:
            disposition = "abstain_negative_after_cost_expectancy"
        elif not win_rate_gate:
            disposition = "abstain_directional_reliability_below_gate"
        else:
            disposition = "abstain_payoff_asymmetry_below_gate"
        rows.append(
            {
                "horizon_sec": int(result.get("horizon_sec") or 0),
                "horizon_label": str(result.get("horizon_label") or ""),
                "independent_factor_episodes": episodes,
                "win_rate_pct": win_rate,
                "avg_episode_net_pips": average,
                "episode_total_net_pips": total,
                "episode_profit_factor": profit_factor,
                "sample_gate_passed": sample_gate,
                "expectancy_gate_passed": expectancy_gate,
                "win_rate_gate_passed": win_rate_gate,
                "profit_factor_gate_passed": profit_factor_gate,
                "shadow_ready_for_frozen_holdout": ready,
                "disposition": disposition,
            }
        )
    return {
        "status": "shadow_only_no_execution_hook",
        "can_change_execution": False,
        "policy": (
            "broad horizon readiness uses factor-collapsed after-cost episodes; "
            "a favorable recent arm or family cell cannot make a horizon ready"
        ),
        "thresholds": {
            "minimum_independent_factor_episodes": int(
                minimum_independent_factor_episodes
            ),
            "minimum_win_rate_pct": float(minimum_win_rate_pct),
            "minimum_profit_factor": float(minimum_profit_factor),
            "requires_positive_average_and_total_net_pips": True,
        },
        "ready_horizon_count": sum(
            int(row["shadow_ready_for_frozen_holdout"]) for row in rows
        ),
        "results": rows,
    }


def summarized_family_horizons(connection: sqlite3.Connection) -> dict[str, Any]:
    """Score strategy archetypes by horizon without cross-arm duplication.

    This is a reporting-only position view.  An identical signal/quote decision
    copied into several A/B arms is counted once, while distinct observations
    remain separate.  Signed-currency factor episodes are intentionally left to
    ``summarized_factor_episodes`` so this view does not hide family identity.
    """
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT c.observed_epoch, c.horizon_sec, c.horizon_label,
               p.rank_in_cohort, p.instrument, p.direction, p.signal_id,
               p.strategy_family,
               p.entry_bid, p.entry_ask, p.net_pips, p.return_pct,
               p.correlation_factor_ids_json
        FROM cohorts c JOIN positions p USING(decision_id)
        WHERE c.measurement_version=? AND c.status='matured'
          AND p.net_pips IS NOT NULL
        ORDER BY c.observed_epoch, c.arm, p.rank_in_cohort
        """,
        (MEASUREMENT_VERSION,),
    ).fetchall()
    exact: dict[tuple[Any, ...], sqlite3.Row] = {}
    for row in rows:
        key = (
            int(row["horizon_sec"]),
            round(base.safe_float(row["observed_epoch"]), 6),
            str(row["instrument"]),
            str(row["direction"]),
            str(row["signal_id"]),
            round(base.safe_float(row["entry_bid"]), 12),
            round(base.safe_float(row["entry_ask"]), 12),
        )
        exact.setdefault(key, row)

    grouped: dict[tuple[int, str], list[tuple[sqlite3.Row, str]]] = defaultdict(list)
    for row in exact.values():
        legacy_family, variant = signal_family(str(row["signal_id"]))
        family = str(row["strategy_family"] or "") or legacy_family
        grouped[(int(row["horizon_sec"]), family)].append((row, variant))

    results: list[dict[str, Any]] = []
    for (horizon, family), items in sorted(grouped.items()):
        pips = [base.safe_float(row["net_pips"]) for row, _ in items]
        returns = [base.safe_float(row["return_pct"]) for row, _ in items]
        wins = sum(int(value > 0.0) for value in pips)
        exact_payoff = payoff_summary(pips)
        episodes: list[dict[str, Any]] = []
        for row, _ in sorted(
            items, key=lambda item: base.safe_float(item[0]["observed_epoch"])
        ):
            try:
                stored_factors = json.loads(
                    str(row["correlation_factor_ids_json"] or "[]")
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                stored_factors = []
            factors = {str(value) for value in stored_factors if str(value)}
            if not factors:
                factors = correlation_factor_ids(
                    {
                        "instrument": row["instrument"],
                        "direction": row["direction"],
                    }
                )
            observed_epoch = base.safe_float(row["observed_epoch"])
            match = next(
                (
                    episode
                    for episode in reversed(episodes)
                    if observed_epoch - episode["start_epoch"]
                    <= FACTOR_EPISODE_WINDOW_SEC
                    and factors.intersection(episode["factor_ids"])
                ),
                None,
            )
            if match is None:
                match = {
                    "start_epoch": observed_epoch,
                    "factor_ids": set(factors),
                    "net_pips": [],
                    "returns": [],
                }
                episodes.append(match)
            match["factor_ids"].update(factors)
            match["net_pips"].append(base.safe_float(row["net_pips"]))
            match["returns"].append(base.safe_float(row["return_pct"]))
        episode_pips = [statistics.fmean(item["net_pips"]) for item in episodes]
        episode_returns = [statistics.fmean(item["returns"]) for item in episodes]
        episode_wins = sum(int(value > 0.0) for value in episode_pips)
        episode_payoff = payoff_summary(episode_pips)
        results.append(
            {
                "horizon_sec": horizon,
                "horizon_label": base.horizon_label(horizon),
                "strategy_family": family,
                "strategy_variants": sorted({variant for _, variant in items}),
                "matured_exact_positions": len(items),
                "wins": wins,
                "win_rate_pct": round(100.0 * wins / len(items), 3),
                "avg_net_pips": round(statistics.fmean(pips), 6),
                "median_net_pips": round(statistics.median(pips), 6),
                "avg_return_pct": round(statistics.fmean(returns), 6),
                **exact_payoff,
                "independent_factor_episodes": len(episodes),
                "factor_repeats_collapsed": len(items) - len(episodes),
                "factor_episode_wins": episode_wins,
                "factor_episode_win_rate_pct": round(
                    100.0 * episode_wins / len(episodes), 3
                ),
                "avg_factor_episode_net_pips": round(
                    statistics.fmean(episode_pips), 6
                ),
                "median_factor_episode_net_pips": round(
                    statistics.median(episode_pips), 6
                ),
                "avg_factor_episode_return_pct": round(
                    statistics.fmean(episode_returns), 6
                ),
                "factor_episode_total_net_pips": episode_payoff[
                    "total_net_pips"
                ],
                "factor_episode_best_net_pips": episode_payoff[
                    "best_net_pips"
                ],
                "factor_episode_worst_net_pips": episode_payoff[
                    "worst_net_pips"
                ],
                "factor_episode_avg_win_pips": episode_payoff["avg_win_pips"],
                "factor_episode_avg_loss_pips": episode_payoff["avg_loss_pips"],
                "factor_episode_payoff_ratio": episode_payoff["payoff_ratio"],
                "factor_episode_profit_factor": episode_payoff["profit_factor"],
            }
        )
    return {
        "policy": (
            "reporting-only strategy-family x horizon position scorecard; "
            "identical cross-arm signal/quote decisions count once; each family "
            "also receives a 15-minute overlapping signed-currency episode view; "
            "no execution hook"
        ),
        "raw_matured_arm_positions": len(rows),
        "matured_exact_positions": len(exact),
        "cross_arm_position_duplicates_excluded": len(rows) - len(exact),
        "strategy_family_count": len({family for _, family in grouped}),
        "results": sorted(
            results,
            key=lambda row: (
                row["horizon_sec"],
                -row["matured_exact_positions"],
                row["strategy_family"],
            ),
        ),
    }


def summarized_family_horizon_watchlist(
    family_summary: dict[str, Any],
) -> dict[str, Any]:
    """Surface promising family/horizon cells without promoting execution.

    The watchlist uses factor-collapsed episodes, positive after-cost payoff,
    and a minimum profit factor.  It is deliberately labelled early evidence:
    five independent episodes are enough to monitor a cell, not enough to
    change account-007 policy.
    """

    candidates: list[dict[str, Any]] = []
    for row in family_summary.get("results") or []:
        episode_count = int(row.get("independent_factor_episodes") or 0)
        win_rate = base.safe_float(row.get("factor_episode_win_rate_pct"))
        avg_net = base.safe_float(row.get("avg_factor_episode_net_pips"))
        total_net = base.safe_float(row.get("factor_episode_total_net_pips"))
        profit_factor = row.get("factor_episode_profit_factor")
        worst = row.get("factor_episode_worst_net_pips")
        no_losing_episode = worst is not None and base.safe_float(worst) > 0.0
        payoff_ready = no_losing_episode or (
            profit_factor is not None
            and base.safe_float(profit_factor) >= FAMILY_WATCH_MIN_PROFIT_FACTOR
        )
        if not (
            episode_count >= FAMILY_WATCH_MIN_FACTOR_EPISODES
            and win_rate >= FAMILY_WATCH_MIN_WIN_RATE_PCT
            and avg_net > 0.0
            and total_net > 0.0
            and payoff_ready
        ):
            continue
        candidates.append(
            {
                "horizon_sec": int(row["horizon_sec"]),
                "horizon_label": str(row["horizon_label"]),
                "strategy_family": str(row["strategy_family"]),
                "independent_factor_episodes": episode_count,
                "factor_episode_win_rate_pct": win_rate,
                "avg_factor_episode_net_pips": avg_net,
                "factor_episode_total_net_pips": total_net,
                "factor_episode_profit_factor": profit_factor,
                "factor_episode_best_net_pips": row.get(
                    "factor_episode_best_net_pips"
                ),
                "factor_episode_worst_net_pips": worst,
                "evidence_tier": (
                    "monitoring" if episode_count >= 20 else "early_watch"
                ),
                "execution_eligible": False,
            }
        )
    candidates.sort(
        key=lambda row: (
            -row["independent_factor_episodes"],
            -row["factor_episode_total_net_pips"],
            row["horizon_sec"],
            row["strategy_family"],
        )
    )
    return {
        "status": "shadow_only_no_execution_hook",
        "policy": (
            "factor-collapsed family/horizon cells with >=5 independent "
            "episodes, >=55% wins, positive average and total after-cost "
            "pips, and >=1.10 profit factor; early watch is not promotion"
        ),
        "thresholds": {
            "minimum_independent_factor_episodes": (
                FAMILY_WATCH_MIN_FACTOR_EPISODES
            ),
            "minimum_factor_episode_win_rate_pct": (
                FAMILY_WATCH_MIN_WIN_RATE_PCT
            ),
            "minimum_factor_episode_profit_factor": (
                FAMILY_WATCH_MIN_PROFIT_FACTOR
            ),
        },
        "candidate_count": len(candidates),
        "candidates": candidates,
    }


def summarized_cross_horizon_theses(
    connection: sqlite3.Connection,
    window_sec: float = CROSS_HORIZON_THESIS_WINDOW_SEC,
) -> dict[str, Any]:
    """Count one nearby multi-horizon signal as one forecasting thesis.

    Horizon tables remain untouched.  This reporting-only view first removes
    exact cross-arm copies, then groups the same signal, pair and direction
    observed within a short window.  The longest matured horizon is the stated
    representative outcome; every endpoint remains visible for disagreement
    analysis.
    """

    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT c.arm, c.observed_epoch, c.observed_at, c.horizon_sec,
               c.horizon_label, p.instrument, p.direction, p.signal_id,
               p.strategy_family, p.entry_bid, p.entry_ask, p.net_pips
        FROM cohorts c JOIN positions p USING(decision_id)
        WHERE c.measurement_version=? AND c.status='matured'
          AND p.net_pips IS NOT NULL
        ORDER BY c.observed_epoch, c.horizon_sec, c.arm, p.rank_in_cohort
        """,
        (MEASUREMENT_VERSION,),
    ).fetchall()
    exact: dict[tuple[Any, ...], sqlite3.Row] = {}
    for row in rows:
        signature = (
            round(base.safe_float(row["observed_epoch"]), 6),
            int(row["horizon_sec"]),
            str(row["instrument"]),
            str(row["direction"]),
            str(row["signal_id"]),
            round(base.safe_float(row["entry_bid"]), 12),
            round(base.safe_float(row["entry_ask"]), 12),
        )
        exact.setdefault(signature, row)

    theses: list[dict[str, Any]] = []
    for row in sorted(
        exact.values(),
        key=lambda item: (
            base.safe_float(item["observed_epoch"]),
            int(item["horizon_sec"]),
        ),
    ):
        legacy_family, _ = signal_family(str(row["signal_id"]))
        family = str(row["strategy_family"] or "") or legacy_family
        identity = (
            str(row["instrument"]),
            str(row["direction"]),
            str(row["signal_id"]) or family,
        )
        observed_epoch = base.safe_float(row["observed_epoch"])
        match = next(
            (
                thesis
                for thesis in reversed(theses)
                if thesis["identity"] == identity
                and observed_epoch - thesis["start_epoch"]
                <= max(1.0, float(window_sec))
            ),
            None,
        )
        if match is None:
            match = {
                "identity": identity,
                "start_epoch": observed_epoch,
                "observed_at": str(row["observed_at"]),
                "instrument": str(row["instrument"]),
                "direction": str(row["direction"]),
                "signal_id": str(row["signal_id"]),
                "strategy_family": family,
                "endpoints": [],
            }
            theses.append(match)
        match["endpoints"].append(
            {
                "horizon_sec": int(row["horizon_sec"]),
                "horizon_label": str(row["horizon_label"]),
                "net_pips": base.safe_float(row["net_pips"]),
            }
        )

    published: list[dict[str, Any]] = []
    representative_values: list[float] = []
    for thesis in theses:
        endpoints = sorted(
            thesis["endpoints"], key=lambda item: item["horizon_sec"]
        )
        representative = endpoints[-1]
        representative_values.append(representative["net_pips"])
        wins = sum(item["net_pips"] > 0.0 for item in endpoints)
        state = (
            "all_win"
            if wins == len(endpoints)
            else "all_loss"
            if wins == 0
            else "mixed"
        )
        published.append(
            {
                "observed_at": thesis["observed_at"],
                "instrument": thesis["instrument"],
                "direction": thesis["direction"],
                "signal_id": thesis["signal_id"],
                "strategy_family": thesis["strategy_family"],
                "endpoint_count": len(endpoints),
                "cross_horizon_state": state,
                "representative_policy": "longest_matured_horizon",
                "representative_horizon_sec": representative["horizon_sec"],
                "representative_net_pips": representative["net_pips"],
                "endpoints": endpoints,
            }
        )

    payoff = payoff_summary(representative_values)
    return {
        "status": "reporting_only_no_execution_hook",
        "can_change_execution": False,
        "window_sec": float(window_sec),
        "raw_matured_position_rows": len(rows),
        "exact_cross_arm_duplicates_excluded": len(rows) - len(exact),
        "unique_horizon_endpoints": len(exact),
        "unique_cross_horizon_theses": len(theses),
        "cross_horizon_endpoints_collapsed": len(exact) - len(theses),
        "all_win_theses": sum(
            row["cross_horizon_state"] == "all_win" for row in published
        ),
        "all_loss_theses": sum(
            row["cross_horizon_state"] == "all_loss" for row in published
        ),
        "mixed_theses": sum(
            row["cross_horizon_state"] == "mixed" for row in published
        ),
        "representative_longest_horizon_payoff": payoff,
        "theses": published,
    }


def summarized_h2_stop_counterfactuals(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    """Report quote-sampled H2 stop arms without changing live policy.

    Exact cross-arm duplicates are counted once.  Rows created before path
    tracking was enabled are excluded so a missing excursion history can never
    be mistaken for a clean path.  A hit is scored at the nominal stop and is
    therefore explicitly idealized; broker slippage is not claimed.
    """
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT c.observed_epoch, c.observed_at, c.status,
               p.instrument, p.direction, p.signal_id, p.net_pips,
               p.path_sample_count, p.max_adverse_pips,
               p.max_favorable_pips, p.correlation_factor_ids_json
        FROM cohorts c JOIN positions p USING(decision_id)
        WHERE c.horizon_sec=7200 AND p.rank_in_cohort=1
          AND p.path_tracking_eligible=1
        ORDER BY c.observed_epoch
        """
    ).fetchall()
    decisions: dict[tuple[Any, ...], sqlite3.Row] = {}
    for row in rows:
        key = (
            round(base.safe_float(row["observed_epoch"]), 3),
            str(row["instrument"]),
            str(row["direction"]),
            str(row["signal_id"]),
        )
        decisions.setdefault(key, row)
    matured = [
        row for row in decisions.values()
        if str(row["status"]) == "matured" and row["net_pips"] is not None
    ]
    episodes: list[dict[str, Any]] = []
    for row in sorted(
        decisions.values(), key=lambda value: base.safe_float(value["observed_epoch"])
    ):
        try:
            stored_factors = json.loads(
                str(row["correlation_factor_ids_json"] or "[]")
            )
        except (TypeError, ValueError, json.JSONDecodeError):
            stored_factors = []
        factors = {str(value) for value in stored_factors if str(value)}
        if not factors:
            factors = correlation_factor_ids(
                {
                    "instrument": row["instrument"],
                    "direction": row["direction"],
                }
            )
        observed_epoch = base.safe_float(row["observed_epoch"])
        match = next(
            (
                episode
                for episode in reversed(episodes)
                if observed_epoch - episode["start_epoch"]
                <= FACTOR_EPISODE_WINDOW_SEC
                and factors.intersection(episode["factor_ids"])
            ),
            None,
        )
        if match is None:
            match = {
                "start_epoch": observed_epoch,
                "factor_ids": set(factors),
                "rows": [],
            }
            episodes.append(match)
        match["factor_ids"].update(factors)
        match["rows"].append(row)
    matured_episodes = [
        episode
        for episode in episodes
        if all(
            str(row["status"]) == "matured" and row["net_pips"] is not None
            for row in episode["rows"]
        )
    ]
    arms: list[dict[str, Any]] = []
    factor_episode_arms: list[dict[str, Any]] = []
    for stop_pips in (20.0, 30.0):
        values = [
            -stop_pips
            if base.safe_float(row["max_adverse_pips"]) >= stop_pips
            else base.safe_float(row["net_pips"])
            for row in matured
        ]
        arms.append(
            {
                "stop_pips": stop_pips,
                "matured": len(values),
                "wins": sum(value > 0.0 for value in values),
                "win_rate": (sum(value > 0.0 for value in values) / len(values))
                if values else None,
                "average_net_pips": statistics.mean(values) if values else None,
                "median_net_pips": statistics.median(values) if values else None,
                "total_net_pips": sum(values),
                "stop_hits": sum(
                    base.safe_float(row["max_adverse_pips"]) >= stop_pips
                    for row in matured
                ),
            }
        )
        episode_values = [
            statistics.fmean(
                -stop_pips
                if base.safe_float(row["max_adverse_pips"]) >= stop_pips
                else base.safe_float(row["net_pips"])
                for row in episode["rows"]
            )
            for episode in matured_episodes
        ]
        factor_episode_arms.append(
            {
                "stop_pips": stop_pips,
                "matured_factor_episodes": len(episode_values),
                "wins": sum(value > 0.0 for value in episode_values),
                "win_rate": (
                    sum(value > 0.0 for value in episode_values)
                    / len(episode_values)
                )
                if episode_values else None,
                "average_net_pips": statistics.mean(episode_values)
                if episode_values else None,
                "median_net_pips": statistics.median(episode_values)
                if episode_values else None,
                "total_net_pips": sum(episode_values),
            }
        )
    return {
        "status": "prospective_shadow_only",
        "method": "5-second executable-quote samples; nominal stop fill; no slippage model",
        "can_change_execution": False,
        "tracked_underlying_decisions": len(decisions),
        "matured_underlying_decisions": len(matured),
        "arms": arms,
        "tracked_factor_episodes": len(episodes),
        "matured_factor_episodes": len(matured_episodes),
        "factor_episode_arms": factor_episode_arms,
    }


def summarized_competing_risk_barriers(
    connection: sqlite3.Connection,
    thesis_window_sec: float = CROSS_HORIZON_THESIS_WINDOW_SEC,
) -> dict[str, Any]:
    """Summarize prospectively ordered target/stop outcomes.

    Historical excursion maxima are intentionally excluded because maxima do
    not preserve which barrier was reached first.  Exact decisions emitted to
    multiple trial arms are collapsed before aggregation, as are nearby copies
    of the identical signal/pair/direction/horizon.  This is a label quality
    diagnostic only and has no execution consumer.
    """
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT c.observed_epoch, c.status, c.horizon_sec,
               p.instrument, p.direction, p.signal_id, p.strategy_family,
               p.net_pips, p.correlation_factor_ids_json,
               b.barrier_id, b.target_pips, b.stop_pips, b.sample_count,
               b.first_event, b.first_event_epoch
        FROM competing_risk_barrier_paths b
        JOIN positions p
          ON p.decision_id=b.decision_id
         AND p.rank_in_cohort=b.rank_in_cohort
        JOIN cohorts c USING(decision_id)
        WHERE p.path_tracking_eligible=1
        ORDER BY c.observed_epoch
        """
    ).fetchall()
    exact: dict[tuple[Any, ...], sqlite3.Row] = {}
    for row in rows:
        key = (
            round(base.safe_float(row["observed_epoch"]), 3),
            int(row["horizon_sec"]),
            str(row["instrument"]),
            str(row["direction"]),
            str(row["signal_id"]),
            str(row["barrier_id"]),
        )
        exact.setdefault(key, row)

    underlying: list[dict[str, Any]] = []
    latest_underlying_by_identity: dict[tuple[Any, ...], dict[str, Any]] = {}
    same_signal_duplicates_excluded = 0
    nearby_fallback_duplicates_excluded = 0
    for row in sorted(
        exact.values(), key=lambda value: base.safe_float(value["observed_epoch"])
    ):
        signal_id = str(row["signal_id"] or "")
        identity = (
            int(row["horizon_sec"]),
            str(row["instrument"]),
            str(row["direction"]),
            signal_id or str(row["strategy_family"]),
            str(row["barrier_id"]),
        )
        observed_epoch = base.safe_float(row["observed_epoch"])
        match = latest_underlying_by_identity.get(identity)
        if (
            match is not None
            and not signal_id
            and observed_epoch - match["start_epoch"]
            > max(1.0, float(thesis_window_sec))
        ):
            match = None
        if match is None:
            match = {
                "identity": identity,
                "start_epoch": observed_epoch,
                "row": row,
            }
            underlying.append(match)
            latest_underlying_by_identity[identity] = match
        elif signal_id:
            same_signal_duplicates_excluded += 1
        else:
            nearby_fallback_duplicates_excluded += 1

    collapsed_rows = [decision["row"] for decision in underlying]
    factor_episodes: list[dict[str, Any]] = []
    factor_episode_indexes: dict[tuple[int, str, str], set[int]] = defaultdict(set)
    for row in sorted(
        collapsed_rows,
        key=lambda value: base.safe_float(value["observed_epoch"]),
    ):
        try:
            stored_factors = json.loads(
                str(row["correlation_factor_ids_json"] or "[]")
            )
        except (TypeError, ValueError, json.JSONDecodeError):
            stored_factors = []
        factors = {str(value) for value in stored_factors if str(value)}
        if not factors:
            factors = correlation_factor_ids(
                {
                    "instrument": row["instrument"],
                    "direction": row["direction"],
                }
            )
        observed_epoch = base.safe_float(row["observed_epoch"])
        horizon_sec = int(row["horizon_sec"])
        barrier_id = str(row["barrier_id"])
        candidate_indexes: set[int] = set()
        for factor_id in factors:
            candidate_indexes.update(
                factor_episode_indexes.get((horizon_sec, barrier_id, factor_id), set())
            )
        matching_indexes = [
            index
            for index in candidate_indexes
            if observed_epoch - factor_episodes[index]["start_epoch"]
            <= FACTOR_EPISODE_WINDOW_SEC
        ]
        match_index = max(matching_indexes) if matching_indexes else None
        match = factor_episodes[match_index] if match_index is not None else None
        if match is None:
            match = {
                "horizon_sec": horizon_sec,
                "barrier_id": barrier_id,
                "start_epoch": observed_epoch,
                "factor_ids": set(factors),
                "rows": [],
            }
            factor_episodes.append(match)
            match_index = len(factor_episodes) - 1
        new_factors = factors.difference(match["factor_ids"])
        match["factor_ids"].update(factors)
        for factor_id in factors if not match["rows"] else new_factors:
            factor_episode_indexes[(horizon_sec, barrier_id, factor_id)].add(
                int(match_index)
            )
        match["rows"].append(row)

    arms: list[dict[str, Any]] = []
    for barrier_id in sorted({str(row["barrier_id"]) for row in collapsed_rows}):
        barrier_rows = [
            row for row in collapsed_rows if str(row["barrier_id"]) == barrier_id
        ]
        matured = [
            row
            for row in barrier_rows
            if str(row["status"]) == "matured"
            and int(row["sample_count"] or 0) > 1
        ]
        target_first = [row for row in matured if str(row["first_event"]) == "target"]
        stop_first = [row for row in matured if str(row["first_event"]) == "stop"]
        neither = [row for row in matured if not str(row["first_event"])]
        event_times = [
            max(
                0.0,
                base.safe_float(row["first_event_epoch"])
                - base.safe_float(row["observed_epoch"]),
            )
            for row in matured
            if row["first_event_epoch"] is not None
        ]
        decisive = len(target_first) + len(stop_first)
        arms.append(
            {
                "barrier_id": barrier_id,
                "tracked_underlying_decisions": len(barrier_rows),
                "matured_underlying_decisions": len(matured),
                "target_first": len(target_first),
                "stop_first": len(stop_first),
                "neither_before_horizon": len(neither),
                "target_first_rate": len(target_first) / len(matured)
                if matured
                else None,
                "target_share_of_decisive": len(target_first) / decisive
                if decisive
                else None,
                "median_first_event_sec": statistics.median(event_times)
                if event_times
                else None,
                "average_fixed_horizon_net_pips": statistics.mean(
                    base.safe_float(row["net_pips"]) for row in matured
                )
                if matured
                else None,
            }
        )
    factor_episode_arms: list[dict[str, Any]] = []
    for barrier_id in sorted(
        {str(episode["barrier_id"]) for episode in factor_episodes}
    ):
        barrier_episodes = [
            episode
            for episode in factor_episodes
            if str(episode["barrier_id"]) == barrier_id
        ]
        matured_episodes = [
            episode
            for episode in barrier_episodes
            if str(episode["rows"][0]["status"]) == "matured"
            and int(episode["rows"][0]["sample_count"] or 0) > 1
        ]
        representative_rows = [
            episode["rows"][0] for episode in matured_episodes
        ]
        target_first = [
            row for row in representative_rows
            if str(row["first_event"]) == "target"
        ]
        stop_first = [
            row for row in representative_rows
            if str(row["first_event"]) == "stop"
        ]
        neither = [row for row in representative_rows if not str(row["first_event"])]
        decisive = len(target_first) + len(stop_first)
        mixed_repeat_labels = 0
        for episode in matured_episodes:
            labels = {
                str(row["first_event"] or "neither")
                for row in episode["rows"]
                if str(row["status"]) == "matured"
                and int(row["sample_count"] or 0) > 1
            }
            mixed_repeat_labels += int(len(labels) > 1)
        factor_episode_arms.append(
            {
                "barrier_id": barrier_id,
                "episode_policy": (
                    "first observed decision per 15-minute overlapping signed-"
                    "currency-factor and horizon episode"
                ),
                "tracked_factor_episodes": len(barrier_episodes),
                "matured_factor_episodes": len(matured_episodes),
                "underlying_decisions": sum(
                    len(episode["rows"]) for episode in barrier_episodes
                ),
                "factor_repeats_collapsed": sum(
                    len(episode["rows"]) for episode in barrier_episodes
                ) - len(barrier_episodes),
                "target_first": len(target_first),
                "stop_first": len(stop_first),
                "neither_before_horizon": len(neither),
                "target_first_rate": (
                    len(target_first) / len(matured_episodes)
                    if matured_episodes else None
                ),
                "target_share_of_decisive": (
                    len(target_first) / decisive if decisive else None
                ),
                "average_fixed_horizon_net_pips": (
                    statistics.mean(
                        base.safe_float(row["net_pips"])
                        for row in representative_rows
                    )
                    if representative_rows else None
                ),
                "mixed_repeat_label_episodes": mixed_repeat_labels,
            }
        )
    return {
        "status": "prospective_shadow_only_no_execution_hook",
        "can_change_execution": False,
        "label_contract": (
            "5-second executable-quote samples; first target versus first stop "
            "versus neither; pre-collector history excluded"
        ),
        "raw_rows": len(rows),
        "exact_cross_arm_duplicates_excluded": len(rows) - len(exact),
        "nearby_same_thesis_duplicates_excluded": len(exact) - len(underlying),
        "same_signal_duplicates_excluded": same_signal_duplicates_excluded,
        "nearby_fallback_duplicates_excluded": (
            nearby_fallback_duplicates_excluded
        ),
        "same_thesis_window_sec": float(thesis_window_sec),
        "factor_episode_window_sec": float(FACTOR_EPISODE_WINDOW_SEC),
        "arms": arms,
        "factor_episode_arms": factor_episode_arms,
    }


def summarized_spread_scaled_take_profit_counterfactuals(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    """Compare fixed-horizon exits with small spread-scaled profit captures.

    The comparison is prospective and research-only.  It uses the executable
    quote path sampled after a position was opened, deduplicates identical
    decisions emitted into multiple arms, and never substitutes missing path
    history.  A threshold hit assumes a nominal fill without slippage, so the
    result is a capture diagnostic rather than executable performance.
    """
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT c.observed_epoch, c.status, c.horizon_sec,
               p.instrument, p.direction, p.signal_id, p.net_pips,
               p.entry_spread_pips, p.path_sample_count,
               p.max_favorable_pips
        FROM cohorts c JOIN positions p USING(decision_id)
        WHERE p.rank_in_cohort=1 AND p.path_tracking_eligible=1
        ORDER BY c.observed_epoch
        """
    ).fetchall()
    decisions: dict[tuple[Any, ...], sqlite3.Row] = {}
    for row in rows:
        key = (
            round(base.safe_float(row["observed_epoch"]), 3),
            int(row["horizon_sec"]),
            str(row["instrument"]),
            str(row["direction"]),
            str(row["signal_id"]),
        )
        decisions.setdefault(key, row)
    matured = [
        row
        for row in decisions.values()
        if str(row["status"]) == "matured"
        and row["net_pips"] is not None
        and int(row["path_sample_count"] or 0) > 1
    ]
    horizons: list[dict[str, Any]] = []
    for horizon_sec in TRIAL_HORIZONS:
        horizon_rows = [
            row for row in matured if int(row["horizon_sec"]) == horizon_sec
        ]
        if not horizon_rows:
            continue
        baseline_values = [base.safe_float(row["net_pips"]) for row in horizon_rows]
        arms: list[dict[str, Any]] = []
        for spread_multiple in (1.5, 2.0):
            values: list[float] = []
            hits = 0
            for row in horizon_rows:
                target_pips = (
                    spread_multiple * base.safe_float(row["entry_spread_pips"])
                )
                hit = (
                    target_pips > 0.0
                    and base.safe_float(row["max_favorable_pips"]) >= target_pips
                )
                hits += int(hit)
                values.append(target_pips if hit else base.safe_float(row["net_pips"]))
            arms.append(
                {
                    "spread_multiple": spread_multiple,
                    "take_profit_hits": hits,
                    "hit_rate": hits / len(values),
                    "wins": sum(value > 0.0 for value in values),
                    "win_rate": sum(value > 0.0 for value in values) / len(values),
                    "average_net_pips": statistics.mean(values),
                    "median_net_pips": statistics.median(values),
                    "total_net_pips": sum(values),
                    "change_vs_fixed_horizon_pips": sum(values)
                    - sum(baseline_values),
                }
            )
        horizons.append(
            {
                "horizon_sec": horizon_sec,
                "horizon_label": base.horizon_label(horizon_sec),
                "matured_underlying_decisions": len(horizon_rows),
                "fixed_horizon": {
                    "wins": sum(value > 0.0 for value in baseline_values),
                    "win_rate": sum(value > 0.0 for value in baseline_values)
                    / len(baseline_values),
                    "average_net_pips": statistics.mean(baseline_values),
                    "median_net_pips": statistics.median(baseline_values),
                    "total_net_pips": sum(baseline_values),
                },
                "take_profit_arms": arms,
            }
        )
    return {
        "status": "prospective_shadow_only",
        "method": (
            "5-second executable-quote samples; threshold fill assumed at "
            "1.5x/2.0x entry spread; no slippage model"
        ),
        "can_change_execution": False,
        "tracked_underlying_decisions": len(decisions),
        "matured_underlying_decisions": len(matured),
        "horizons": horizons,
    }


def publish_state(
    connection: sqlite3.Connection,
    path: Path,
    last_cycle: dict[str, Any],
) -> None:
    connection.row_factory = sqlite3.Row
    counts = dict(
        connection.execute("SELECT status, COUNT(*) FROM cohorts GROUP BY status").fetchall()
    )
    open_rows = [
        dict(row)
        for row in connection.execute(
            "SELECT * FROM cohorts WHERE status='open' ORDER BY horizon_sec, arm"
        ).fetchall()
    ]
    family_horizon_summary = summarized_family_horizons(connection)
    factor_episode_summary = summarized_factor_episodes(connection)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "measurement_version": MEASUREMENT_VERSION,
        "generated_utc": utc_now(),
        "status": "ok",
        "research_only": True,
        "can_place_orders": False,
        "account_scope": "practice_007_observation_only",
        "horizons_sec": list(TRIAL_HORIZONS),
        "trial_arms": {
            "opportunity_price_top1": "best >=2x-spread opportunity",
            "opportunity_price_top1_direction_pair_h2": (
                "prospective shadow-only matched original/opposite pair for "
                "the selected H2 opportunity_price_top1 decision; both sides "
                "share entry time and quotes; cannot place orders"
            ),
            "h2_strict_validated_direction_pair": (
                "prospective matched original/opposite H2 pair requiring final "
                "eligibility, validation, no conflict/blockers, positive "
                "historical disposition, and >=2x direction-aligned cost "
                "coverage; observation-only"
            ),
            "opportunity_price_top1_reentry_guarded": (
                "research-only best >=2x-spread opportunity after a losing "
                "signal, with 15-minute pair-direction, 5-minute instrument, "
                "and 30-minute shared-JPY-factor cooldowns; raw arm preserved"
            ),
            "opportunity_price_top1_overlap_guarded": (
                "research-only best >=2x-spread opportunity excluding a "
                "distinct signal on any instrument that already has an open "
                "thesis; exact cross-arm copies share one thesis"
            ),
            "opportunity_price_top1_factor_overlap_guarded": (
                "research-only best >=2x-spread opportunity excluding a "
                "distinct signal that repeats any signed currency factor of "
                "an open thesis; exact cross-arm copies share one thesis"
            ),
            "opportunity_price_top3": "risk-equal independent top three >=2x-spread opportunities",
            "opportunity_news_aligned": "price opportunity with aligned pair-news direction",
            "opportunity_event_regime": "price opportunity during a material event regime",
            "opportunity_verified_event_regime": (
                "prospective-only price opportunity during a verified or "
                "execution-vetted event; excludes severity-only discovery "
                "headlines and never changes execution"
            ),
            "opportunity_verified_news_event": (
                "prospective-only price opportunity aligned with directional "
                "pair news during a verified or execution-vetted event; "
                "neutral verified releases do not qualify"
            ),
            "opportunity_news_event": "price opportunity with aligned news and event regime",
            "h2_persistence": "separate H2 lane with >=1.35x spread and >=52% side confidence",
            "h2_persistence_direction_pair": (
                "prospective shadow-only matched original/opposite pair for "
                "the selected H2 persistence decision; both sides share entry "
                "time and quotes; cannot place orders"
            ),
            "intrahour_cost_capture_top1": (
                "liquid M5-H1 top signal with >=55% confidence, >=1.75x "
                "gross-to-spread, and >=0.25 projected after-cost pips"
            ),
            "intrahour_magnitude_1_3_top1": (
                "prospective-only liquid M5-H1 calibration arm discovered "
                "2026-08-03: same cost gate with projected after-cost movement "
                "bounded to 1-3 pips; no retrospective trial credit"
            ),
            "intrahour_executor_parity_top1": (
                "prospective-only intrahour cost-capture signal that also "
                "passes final eligibility, validation, and conflict checks; "
                "observation-only executor-policy parity"
            ),
            "conflicted_aggressive_top1": (
                "research-only top conflicted consensus with >=1.5x spread "
                "and >=55% confidence; never executable"
            ),
            "conflicted_aggressive_direction_pair": (
                "prospective shadow-only matched original/opposite pair for "
                "the selected conflicted-aggressive decision; both sides "
                "share entry time and executable quotes; never executable"
            ),
            "intrahour_confirmed_reversal_top1": (
                "same-family intrahour direction flip within 15 minutes after "
                "the prior signal is adverse by at least one entry spread; "
                "prospective research only"
            ),
            "intrahour_cross_family_reversal_top1": (
                "prospective-only opposite pair signal from any family within "
                "30 minutes after one-spread adverse confirmation, requiring "
                ">=2 projected after-cost pips; never executable"
            ),
            "intrahour_all_outcome_refractory_top1": (
                "prospective M5/M15 cost-qualified comparator that suppresses "
                "same-pair or same-signed-factor thesis recycling for 15 "
                "minutes after any matured outcome; observation-only"
            ),
        },
        "independence_policy": (
            "one open cohort per arm/horizon; no duplicate instrument, event ID, "
            "or directional currency factor inside top-three cohorts; all "
            "same-direction JPY pair propagation is one JPY factor"
        ),
        "counts": {
            "open": int(counts.get("open", 0)),
            "matured": int(counts.get("matured", 0)),
            "total": int(sum(counts.values())),
        },
        "last_cycle": last_cycle,
        "results": summarized_results(connection),
        "results_by_cost_bucket": summarized_cost_results(connection),
        "underlying_decision_summary": summarized_underlying_decisions(connection),
        "factor_episode_summary": factor_episode_summary,
        "horizon_readiness_summary": summarized_horizon_readiness(
            factor_episode_summary
        ),
        "pair_direction_flip_diagnostic": summarized_pair_direction_flips(connection),
        "family_horizon_summary": family_horizon_summary,
        "family_horizon_shadow_watchlist": summarized_family_horizon_watchlist(
            family_horizon_summary
        ),
        "cross_horizon_thesis_summary": summarized_cross_horizon_theses(
            connection
        ),
        "h2_stop_counterfactual_summary": summarized_h2_stop_counterfactuals(connection),
        "competing_risk_barrier_summary": summarized_competing_risk_barriers(
            connection
        ),
        "h2_inverse_shadow_summary": summarized_h2_inverse_shadow(connection),
        "spread_scaled_take_profit_counterfactual_summary": (
            summarized_spread_scaled_take_profit_counterfactuals(connection)
        ),
        "open_cohorts": open_rows,
    }
    base.write_json_atomic(path, payload)


def run_cycle(
    connection: sqlite3.Connection,
    signal_path: Path,
    quote_path: Path,
    news_path: Path,
    state_path: Path,
) -> dict[str, Any]:
    snapshot = base.load_json(signal_path)
    quote_payload = base.load_quote_snapshot(quote_path)
    news_payload = base.load_json(news_path)
    now_epoch = time.time()
    quotes = base.quote_map(quote_payload, now_epoch=now_epoch)
    ranked = base.candidates_by_horizon(snapshot)
    path_samples = update_open_excursions(connection, quotes, now_epoch=now_epoch)
    matured = mature_cohorts(connection, quotes, now_epoch)
    opened = 0
    arm_attempts: dict[str, int] = defaultdict(int)
    for horizon in TRIAL_HORIZONS:
        rows = ranked.get(horizon) or []
        for arm in TRIAL_ARMS:
            if arm in {
                "h2_persistence",
                "h2_persistence_direction_pair",
                "opportunity_price_top1_direction_pair_h2",
                "h2_strict_validated_direction_pair",
            } and horizon != 7200:
                continue
            if arm == "opportunity_price_top1_reentry_guarded":
                members = reentry_guarded_opportunity_candidates(
                    connection,
                    rows,
                    news_payload,
                    now_epoch,
                )
            elif arm == "opportunity_price_top1_overlap_guarded":
                members = overlap_guarded_opportunity_candidates(
                    connection,
                    rows,
                    news_payload,
                    now_epoch,
                )
            elif arm == "opportunity_price_top1_factor_overlap_guarded":
                members = factor_overlap_guarded_opportunity_candidates(
                    connection,
                    rows,
                    news_payload,
                    now_epoch,
                )
            elif arm == "intrahour_confirmed_reversal_top1":
                members = confirmed_reversal_candidates(
                    connection,
                    rows,
                    news_payload,
                    quotes,
                    horizon,
                    now_epoch,
                )
            elif arm == "intrahour_cross_family_reversal_top1":
                members = cross_family_confirmed_reversal_candidates(
                    connection,
                    rows,
                    news_payload,
                    quotes,
                    horizon,
                    now_epoch,
                )
            elif arm == "intrahour_all_outcome_refractory_top1":
                members = all_outcome_refractory_candidates(
                    connection,
                    rows,
                    news_payload,
                    quotes,
                    horizon,
                    now_epoch,
                )
            else:
                members = arm_candidates(arm, rows, news_payload, quotes)
            arm_attempts[arm] += len(members)
            if open_cohort(connection, arm, horizon, members, quotes, now_epoch):
                opened += 1
    cycle = {
        "time": utc_now(),
        "signal_updated_at": snapshot.get("updated_at"),
        "quote_generated_utc": quote_payload.get("generated_utc"),
        "news_generated_utc": news_payload.get("generated_utc"),
        "quote_count": len(quotes),
        "ranked_horizons": len(ranked),
        "candidate_members_by_arm": dict(sorted(arm_attempts.items())),
        "opportunity_top3_readiness_by_horizon": top3_readiness_by_horizon(
            ranked, news_payload
        ),
        "opened": opened,
        "matured": matured,
        "path_samples": path_samples,
        "market_session": session_name(now_epoch),
    }
    publish_state(connection, state_path, cycle)
    return cycle


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--signals", type=Path, default=DEFAULT_SIGNALS)
    parser.add_argument("--quotes", type=Path, default=DEFAULT_QUOTES)
    parser.add_argument("--news-context", type=Path, default=DEFAULT_NEWS)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--interval-sec", type=float, default=5.0)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    connection = open_database(args.database)
    try:
        stop_at = time.monotonic() + max(0.0, args.duration_sec)
        while True:
            run_cycle(
                connection,
                args.signals,
                args.quotes,
                args.news_context,
                args.state,
            )
            if args.once or time.monotonic() >= stop_at:
                break
            time.sleep(max(0.25, args.interval_sec))
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
