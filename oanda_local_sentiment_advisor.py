#!/usr/bin/env python3
"""Build a GPT-shaped, read-only FX sentiment decision from local artifacts.

This module has no broker client, credential access, or order path. It turns the
causal local-news surface into an explainable shadow portfolio review so the
replacement for GPT can be validated before it is wired to account execution.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence


UTC = dt.timezone.utc
ROOT = Path(__file__).resolve().parent
DEFAULT_NEWS = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_news_sentiment"
    / "pair_sentiment_latest.json"
)
DEFAULT_SIGNAL = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "state"
    / "practice_007_signal_snapshot_v1.json"
)
DEFAULT_ACCOUNTS = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "state"
    / "account_dashboard_v1.json"
)
DEFAULT_MARKET = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "market_sentiment_ticker"
    / "LATEST.json"
)
DEFAULT_OUTPUT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_sentiment_advisor"
)
SCHEMA_VERSION = "local_sentiment_advisor_shadow_v1"
ACCOUNT_SUFFIX = "-002"
NEWS_MAX_AGE_SECONDS = 300.0


def utc_now() -> dt.datetime:
    return dt.datetime.now(tz=UTC)


def iso_utc(value: dt.datetime | None = None) -> str:
    return (value or utc_now()).astimezone(UTC).isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def clamp(value: float, low: float = -1.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def parse_time(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def normalize_instrument(value: Any) -> str:
    text = re.sub(r"[^A-Z]", "", str(value or "").upper())
    return f"{text[:3]}_{text[3:6]}" if len(text) == 6 else ""


def split_instrument(value: str) -> tuple[str, str]:
    parts = normalize_instrument(value).split("_", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else ("", "")


def event_quality(events: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    if not events:
        return {
            "freshness": 0.0,
            "verified_fraction": 0.0,
            "source_diversity": 0.0,
            "direction_consistency": 0.0,
        }
    ages = [max(0.0, safe_float(row.get("age_minutes"), 1e6)) for row in events]
    freshness = sum(math.exp(-age / 180.0) for age in ages) / len(ages)
    verified = sum(bool(row.get("source_verified")) for row in events) / len(events)
    sources = {
        str(row.get("source_name") or "").strip().lower()
        for row in events
        if str(row.get("source_name") or "").strip()
    }
    diversity = min(1.0, len(sources) / min(4.0, float(len(events))))
    signs = [
        1 if safe_float(row.get("pair_score")) > 0 else -1
        for row in events
        if safe_float(row.get("pair_score")) != 0
    ]
    consistency = abs(sum(signs)) / len(signs) if signs else 0.0
    return {
        "freshness": freshness,
        "verified_fraction": verified,
        "source_diversity": diversity,
        "direction_consistency": consistency,
    }


def signal_confirmation(
    instrument: str,
    signal_lookup: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    signal = signal_lookup.get(instrument) or {}
    points = [
        row
        for row in signal.get("horizon_breakdown") or []
        if isinstance(row, dict)
        and 0 < int(safe_float(row.get("horizon_sec"), 0.0)) <= 3600
    ]
    point = max(points, key=lambda row: int(row.get("horizon_sec") or 0), default={})
    probability_up = safe_float(
        point.get("raw_probability_up", point.get("probability_up")),
        0.5,
    )
    direction = "LONG" if probability_up >= 0.52 else "SHORT" if probability_up <= 0.48 else "NEUTRAL"
    return {
        "direction": direction,
        "probability_up": round(probability_up, 6),
        "confidence": round(abs(probability_up - 0.5) * 2.0, 6),
        "horizon_sec": int(safe_float(point.get("horizon_sec"), 0.0)),
        "account_eligible": bool(point.get("paper_consensus_eligible")),
    }


def market_confirmation(
    instrument: str,
    market_payload: Mapping[str, Any],
) -> dict[str, Any]:
    pair = (market_payload.get("pair_moves") or {}).get(instrument) or {}
    windows = pair.get("windows") or {}
    move_15 = safe_float((windows.get("15") or {}).get("return_bps"))
    move_60 = safe_float((windows.get("60") or {}).get("return_bps"))
    composite = 0.70 * move_15 + 0.30 * move_60
    direction = "LONG" if composite >= 1.0 else "SHORT" if composite <= -1.0 else "NEUTRAL"
    return {
        "direction": direction,
        "confidence": round(math.tanh(abs(composite) / 8.0), 6),
        "return_15m_bps": round(move_15, 6),
        "return_60m_bps": round(move_60, 6),
        "market_fresh": bool(market_payload.get("fresh")),
    }


def combine_confirmations(
    signal: Mapping[str, Any],
    market: Mapping[str, Any],
) -> dict[str, Any]:
    signal_direction = str(signal.get("direction") or "NEUTRAL")
    market_direction = str(market.get("direction") or "NEUTRAL")
    signal_confidence = safe_float(signal.get("confidence"))
    market_confidence = safe_float(market.get("confidence"))
    if not market.get("market_fresh"):
        direction = signal_direction
        confidence = signal_confidence
        state = "signal_only"
    elif signal_direction == "NEUTRAL":
        direction = market_direction
        confidence = market_confidence
        state = "market_only"
    elif market_direction == "NEUTRAL":
        direction = signal_direction
        confidence = signal_confidence
        state = "signal_only"
    elif signal_direction == market_direction:
        direction = signal_direction
        confidence = min(1.0, 0.5 * (signal_confidence + market_confidence) + 0.15)
        state = "signal_market_aligned"
    else:
        direction = "NEUTRAL"
        confidence = max(signal_confidence, market_confidence)
        state = "signal_market_conflict"
    return {
        "direction": direction,
        "confidence": round(confidence, 6),
        "state": state,
        "signal": dict(signal),
        "market": dict(market),
    }


def score_pair(
    instrument: str,
    news: Mapping[str, Any],
    confirmation: Mapping[str, Any],
) -> dict[str, Any]:
    score = clamp(safe_float(news.get("score")))
    news_direction = "LONG" if score >= 0.12 else "SHORT" if score <= -0.12 else "NEUTRAL"
    events = [row for row in news.get("events") or [] if isinstance(row, dict)]
    quality = event_quality(events)
    base_confidence = clamp(safe_float(news.get("confidence")), 0.0, 1.0)
    evidence_quality = (
        0.35 * quality["freshness"]
        + 0.25 * quality["source_diversity"]
        + 0.20 * quality["direction_consistency"]
        + 0.20 * quality["verified_fraction"]
    )
    confidence = 100.0 * base_confidence * (0.45 + 0.55 * evidence_quality)
    technical_direction = str(confirmation.get("direction") or "NEUTRAL")
    if news_direction != "NEUTRAL" and technical_direction == news_direction:
        relationship = "ALIGNED"
        confidence += 8.0 * safe_float(confirmation.get("confidence"))
    elif technical_direction == "NEUTRAL" or news_direction == "NEUTRAL":
        relationship = "UNCONFIRMED"
    else:
        relationship = "OPPOSED"
        confidence -= 18.0 * safe_float(confirmation.get("confidence"))
    unverified_only = bool(events) and quality["verified_fraction"] == 0.0
    if unverified_only:
        confidence = min(confidence, 45.0)
    confidence = clamp(confidence, 0.0, 80.0)
    action = (
        "WATCH"
        if news_direction != "NEUTRAL"
        and confidence >= 25.0
        and relationship != "OPPOSED"
        else "REJECT"
    )
    headline = str(events[0].get("headline") or "")[:180] if events else ""
    reason_parts = [
        f"local news score={score:+.3f}",
        f"evidence confidence={confidence:.1f}",
        f"price relationship={relationship.lower()}",
    ]
    if unverified_only:
        reason_parts.append("top evidence is unverified")
    if headline:
        reason_parts.append(f"lead: {headline}")
    return {
        "instrument": instrument,
        "action": action,
        "direction": news_direction,
        "outlook_confidence": round(confidence, 3),
        "risk_pct": 0.0,
        "expected_hold_hours": 1.0,
        "why_now": "; ".join(reason_parts),
        "reason": "; ".join(reason_parts),
        "what_would_change_my_mind": (
            "Verified contradictory evidence, a reversal in the pair score, "
            "or persistent opposing completed-bar confirmation."
        ),
        "sentiment_score": round(score, 6),
        "news_confidence": round(base_confidence, 6),
        "active_event_count": int(news.get("active_event_count") or 0),
        "evidence_quality": {key: round(value, 6) for key, value in quality.items()},
        "price_confirmation": dict(confirmation),
        "research_only": True,
        "execution_eligible": False,
    }


def currency_ranking(rows: Sequence[Mapping[str, Any]]) -> tuple[list[str], dict[str, float]]:
    totals: dict[str, float] = defaultdict(float)
    weights: dict[str, float] = defaultdict(float)
    for row in rows:
        base, quote = split_instrument(str(row.get("instrument") or ""))
        if not base or not quote:
            continue
        score = safe_float(row.get("sentiment_score"))
        weight = max(0.05, safe_float(row.get("news_confidence")))
        totals[base] += score * weight
        totals[quote] -= score * weight
        weights[base] += weight
        weights[quote] += weight
    scores = {
        currency: totals[currency] / weights[currency]
        for currency in totals
        if weights[currency] > 0
    }
    ranked = sorted(scores, key=lambda currency: scores[currency], reverse=True)
    return ranked, scores


def account_snapshot(payload: Mapping[str, Any], suffix: str) -> dict[str, Any]:
    for row in payload.get("accounts") or []:
        if not isinstance(row, dict):
            continue
        account_id = str(row.get("account_id") or "")
        if account_id.endswith(suffix):
            return dict(row)
    return {}


def account_snapshot_status(
    payload: Mapping[str, Any], account: Mapping[str, Any]
) -> dict[str, Any]:
    aggregate = (
        payload.get("aggregate")
        if isinstance(payload.get("aggregate"), Mapping)
        else {}
    )
    snapshot_state = str(aggregate.get("snapshot_state") or "").strip().lower()
    explicit_current = account.get(
        "account_values_current", aggregate.get("account_values_current")
    )
    explicit_ok = account.get("ok")
    legacy_values_present = any(
        account.get(key) is not None for key in ("NAV", "balance", "pl")
    )
    current = bool(account) and snapshot_state not in {
        "unavailable",
        "retained_stale_account_values",
        "stale",
        "failed",
    }
    if explicit_current is False or explicit_ok is False:
        current = False
    elif explicit_current is None and explicit_ok is None:
        current = current and legacy_values_present
    positions_current = bool(
        current
        and account.get(
            "positions_current", aggregate.get("positions_current", True)
        )
    )
    orders_current = bool(
        current
        and account.get("orders_current", aggregate.get("orders_current", True))
    )
    return {
        "account_current": current,
        "account_ok": bool(explicit_ok) if explicit_ok is not None else current,
        "snapshot_state": snapshot_state or ("current" if current else "unavailable"),
        "positions_current": positions_current,
        "orders_current": orders_current,
        "last_verified": aggregate.get("last_verified")
        or account.get("last_verified"),
    }


def build_decision(
    news_payload: Mapping[str, Any],
    signal_payload: Mapping[str, Any],
    accounts_payload: Mapping[str, Any],
    *,
    market_payload: Mapping[str, Any] | None = None,
    account_suffix: str = ACCOUNT_SUFFIX,
    maximum_candidates: int = 12,
) -> dict[str, Any]:
    market_payload = market_payload or {}
    generated = str(news_payload.get("generated_utc") or "")
    generated_time = parse_time(generated)
    age_seconds = (
        max(0.0, (utc_now() - generated_time).total_seconds())
        if generated_time
        else None
    )
    stale = age_seconds is None or age_seconds > NEWS_MAX_AGE_SECONDS
    news_pairs = {} if stale else (news_payload.get("pairs") or {})
    signal_rows = [
        row for row in signal_payload.get("top_signals") or [] if isinstance(row, dict)
    ]
    signal_lookup = {
        normalize_instrument(row.get("instrument")): row
        for row in signal_rows
        if normalize_instrument(row.get("instrument"))
    }
    pair_rows = [
        score_pair(
            normalize_instrument(instrument),
            value,
            combine_confirmations(
                signal_confirmation(normalize_instrument(instrument), signal_lookup),
                market_confirmation(normalize_instrument(instrument), market_payload),
            ),
        )
        for instrument, value in news_pairs.items()
        if normalize_instrument(instrument) and isinstance(value, dict)
    ]
    pair_rows.sort(
        key=lambda row: (
            row["action"] == "WATCH",
            row["outlook_confidence"],
            abs(row["sentiment_score"]),
        ),
        reverse=True,
    )
    ranked_currencies, currency_scores = currency_ranking(pair_rows)
    usd_score = currency_scores.get("USD", 0.0)
    defensive_score = (
        currency_scores.get("JPY", 0.0)
        + currency_scores.get("CHF", 0.0)
        - currency_scores.get("AUD", 0.0)
        - currency_scores.get("NZD", 0.0)
    ) / 4.0
    if defensive_score >= 0.12:
        portfolio_mode = "DEFENSIVE"
    elif usd_score >= 0.12:
        portfolio_mode = "USD_BULLISH"
    elif usd_score <= -0.12:
        portfolio_mode = "USD_BEARISH"
    elif len(ranked_currencies) >= 2:
        portfolio_mode = "RELATIVE_VALUE"
    else:
        portfolio_mode = "MIXED_UNCLEAR"
    if stale:
        portfolio_mode = "DATA_STALE"

    account = account_snapshot(accounts_payload, account_suffix)
    account_state = account_snapshot_status(accounts_payload, account)
    open_actions: list[dict[str, Any]] = []
    pair_by_instrument = {row["instrument"]: row for row in pair_rows}
    for trade in (
        account.get("trades") if account_state["positions_current"] else []
    ) or []:
        if not isinstance(trade, dict):
            continue
        instrument = normalize_instrument(trade.get("instrument"))
        row = pair_by_instrument.get(instrument, {})
        units = safe_float(trade.get("currentUnits", trade.get("units")))
        trade_direction = "LONG" if units > 0 else "SHORT" if units < 0 else "NONE"
        alignment = (
            "supports"
            if row.get("direction") == trade_direction
            else "opposes"
            if row.get("direction") in {"LONG", "SHORT"}
            else "does not resolve"
        )
        open_actions.append(
            {
                "trade_id": str(trade.get("id") or trade.get("trade_id") or ""),
                "instrument": instrument,
                "action": "HOLD",
                "direction": trade_direction,
                "outlook_confidence": safe_float(row.get("outlook_confidence")),
                "risk_pct": 0.0,
                "reason": (
                    f"Shadow sentiment {alignment} this position. No local "
                    "sentiment action is execution-authorized."
                ),
                "what_would_change_my_mind": row.get(
                    "what_would_change_my_mind",
                    "Validated deterministic position-management evidence.",
                ),
            }
        )

    candidates = []
    for rank, row in enumerate(pair_rows[: max(1, maximum_candidates)], start=1):
        candidates.append({"rank": rank, **row})
    watch_count = sum(row["action"] == "WATCH" for row in pair_rows)
    verified_candidates = sum(
        row["evidence_quality"]["verified_fraction"] > 0 for row in pair_rows
    )
    strongest = ", ".join(
        f"{row['instrument']} {row['direction']} {row['outlook_confidence']:.0f}"
        for row in candidates[:3]
    )
    market_regime = str(market_payload.get("current_regime") or "UNKNOWN")
    market_fresh = bool(market_payload.get("fresh"))
    return {
        "schema_version": SCHEMA_VERSION,
        "as_of": iso_utc(),
        "decision_engine": "deterministic_local_sentiment_shadow",
        "market_summary": (
            "Local-news input was hard-blocked because its snapshot exceeded "
            f"{int(NEWS_MAX_AGE_SECONDS)} seconds; market regime={market_regime}."
            if stale
            else (
                f"Ranked {len(pair_rows)} pairs from local causal news context; "
                f"{watch_count} watch candidates; market regime={market_regime}. "
                f"Strongest: {strongest or 'none'}."
            )
        ),
        "portfolio_bias": (
            f"{portfolio_mode}; currency order: "
            + " > ".join(ranked_currencies[:8])
        ),
        "portfolio_mode": portfolio_mode,
        "currency_ranking": [
            f"{currency}:{currency_scores[currency]:+.3f}"
            for currency in ranked_currencies
        ],
        "why_mixed_usd_exposure_is_allowed": (
            "This shadow surface ranks relative currency evidence; it does not "
            "authorize exposure."
        ),
        "non_usd_cross_pair_review": (
            "Non-USD crosses were ranked in the same global sentiment surface "
            "as USD pairs."
        ),
        "risk_notes": [
            "Research-only: no broker client, credentials, or order path exists.",
            "All orders_to_execute and event_permissions are forced empty.",
            (
                f"Only {verified_candidates} pair scores include a verified "
                "source among their top retained events."
            ),
            (
                "The local-news snapshot is stale or unavailable."
                if stale
                else "The local-news snapshot passed the five-minute freshness check."
            ),
            (
                f"The completed-quote market ticker is fresh and reports {market_regime}."
                if market_fresh
                else "The completed-quote market ticker is stale or unavailable."
            ),
            (
                "Overnight prospective evidence did not establish executable "
                "news edge; confidence is capped pending validation."
            ),
            (
                "The account snapshot is current."
                if account_state["account_current"]
                else "The account snapshot is unavailable; financial and position fields are unknown."
            ),
        ],
        "event_permissions": [],
        "open_position_actions": open_actions,
        "new_trade_candidates": candidates,
        "orders_to_execute": [],
        "account": {
            "account_suffix": account_suffix,
            "account_current": account_state["account_current"],
            "account_ok": account_state["account_ok"],
            "snapshot_state": account_state["snapshot_state"],
            "positions_current": account_state["positions_current"],
            "orders_current": account_state["orders_current"],
            "balance": (
                safe_float(account.get("balance"))
                if account_state["account_current"]
                else None
            ),
            "nav": (
                safe_float(account.get("NAV"))
                if account_state["account_current"]
                else None
            ),
            "open_trades": (
                int(account.get("openTradeCount") or 0)
                if account_state["positions_current"]
                else None
            ),
            "pending_orders": (
                int(account.get("pendingOrderCount") or 0)
                if account_state["orders_current"]
                else None
            ),
            "last_verified": account_state["last_verified"],
        },
        "input_state": {
            "news_generated_utc": generated,
            "news_age_seconds": round(age_seconds, 3) if age_seconds is not None else None,
            "news_stale": stale,
            "news_hard_blocked": stale,
            "news_max_age_seconds": NEWS_MAX_AGE_SECONDS,
            "signal_updated_at": signal_payload.get("updated_at"),
            "pair_count": len(pair_rows),
            "signal_pair_count": len(signal_lookup),
            "market_regime": market_regime,
            "market_fresh": market_fresh,
            "market_quote_age_seconds": market_payload.get("quote_age_seconds"),
        },
        "research_only": True,
        "execution_eligible": False,
    }


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--news", type=Path, default=DEFAULT_NEWS)
    parser.add_argument("--signal", type=Path, default=DEFAULT_SIGNAL)
    parser.add_argument("--accounts", type=Path, default=DEFAULT_ACCOUNTS)
    parser.add_argument("--market", type=Path, default=DEFAULT_MARKET)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--account-suffix", default=ACCOUNT_SUFFIX)
    parser.add_argument("--maximum-candidates", type=int, default=12)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    decision = build_decision(
        load_json(args.news),
        load_json(args.signal),
        load_json(args.accounts),
        market_payload=load_json(args.market),
        account_suffix=str(args.account_suffix),
        maximum_candidates=max(1, args.maximum_candidates),
    )
    atomic_write_json(args.output_root / "LATEST.json", decision)
    append_jsonl(args.output_root / "decision_history.jsonl", decision)
    print(json.dumps(decision, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
