#!/usr/bin/env python3
"""Pure 21-currency strength ranker for the Practice-006 challenger.

The model consumes the synchronized 68-pair market-sentiment ticker.  It does
not place orders.  It ranks one factor per currency, constructs only
strong-versus-weak pair expressions that exist in the executable universe,
requires short-term price confirmation, applies an executable-cost hurdle,
and greedily prevents a currency factor from appearing twice.
"""

from __future__ import annotations

import math
import statistics
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping


UTC = timezone.utc
MODEL_ID = "currency_rank_5m15m60m_v1_20260821"
EXPECTED_SCHEMA = "oanda_cross_currency_market_sentiment_v1"
EXPECTED_CURRENCIES = (
    "AUD", "CAD", "CHF", "CNH", "CZK", "DKK", "EUR", "GBP", "HKD",
    "HUF", "JPY", "MXN", "NOK", "NZD", "PLN", "SEK", "SGD", "THB",
    "TRY", "USD", "ZAR",
)
EXPECTED_INSTRUMENTS = (
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
WEIGHTS = {"5": 0.50, "15": 0.35, "60": 0.15}


@dataclass(frozen=True)
class RankSettings:
    max_snapshot_age_sec: float = 90.0
    min_leg_strength_bps: float = 0.10
    min_factor_gap_bps: float = 0.50
    min_pair_confirmation_bps: float = 0.20
    min_confirmation_to_cost: float = 1.05
    spread_stress_multiple: float = 1.20
    max_spread_bps: float = 12.0
    max_selected: int = 8


def _number(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _parse_utc(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _robust_z(values: Mapping[str, float]) -> dict[str, float]:
    ordered = [float(values[currency]) for currency in EXPECTED_CURRENCIES]
    center = statistics.median(ordered)
    deviations = [abs(value - center) for value in ordered]
    scale = 1.4826 * statistics.median(deviations)
    if scale <= 1e-9:
        scale = statistics.pstdev(ordered)
    if scale <= 1e-9:
        return {currency: 0.0 for currency in EXPECTED_CURRENCIES}
    return {
        currency: max(-5.0, min(5.0, (float(values[currency]) - center) / scale))
        for currency in EXPECTED_CURRENCIES
    }


def _pair_shape(instrument: str, strong: str, weak: str) -> tuple[str, float] | None:
    base, quote = instrument.split("_", 1)
    if base == strong and quote == weak:
        return "buy", 1.0
    if base == weak and quote == strong:
        return "sell", -1.0
    return None


def _pair_mapping(payload: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    raw = payload.get("pair_moves")
    if not isinstance(raw, Mapping):
        return {}
    return {
        str(instrument): row
        for instrument, row in raw.items()
        if isinstance(row, Mapping)
    }


def rank_snapshot(
    payload: Mapping[str, Any],
    *,
    now: datetime | None = None,
    settings: RankSettings | None = None,
    reserved_currencies: Iterable[str] = (),
) -> dict[str, Any]:
    """Return a closed, diagnostic ranking result or a fail-closed error."""
    cfg = settings or RankSettings()
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    reasons: Counter[str] = Counter()
    if str(payload.get("schema_version") or "") != EXPECTED_SCHEMA:
        reasons["source_schema_mismatch"] += 1
    if str(payload.get("status") or "") != "ready" or payload.get("fresh") is not True:
        reasons["source_not_ready"] += 1
    generated = _parse_utc(payload.get("generated_utc"))
    age_sec = math.inf if generated is None else max(0.0, (observed - generated).total_seconds())
    if not math.isfinite(age_sec) or age_sec > cfg.max_snapshot_age_sec:
        reasons["source_stale"] += 1
    pairs = _pair_mapping(payload)
    if set(pairs) != set(EXPECTED_INSTRUMENTS):
        reasons["instrument_universe_mismatch"] += 1

    raw_strength: dict[str, dict[str, float]] = {}
    for horizon in WEIGHTS:
        horizon_row = (payload.get("horizons") or {}).get(horizon) or {}
        values = horizon_row.get("currency_strength_bps") or {}
        if not isinstance(values, Mapping) or set(values) != set(EXPECTED_CURRENCIES):
            reasons[f"currency_universe_mismatch_h{horizon}"] += 1
            continue
        raw_strength[horizon] = {
            currency: _number(values.get(currency), math.nan)
            for currency in EXPECTED_CURRENCIES
        }
        if any(not math.isfinite(value) for value in raw_strength[horizon].values()):
            reasons[f"currency_strength_invalid_h{horizon}"] += 1

    if reasons:
        return {
            "status": "blocked",
            "model_id": MODEL_ID,
            "generated_utc": str(payload.get("generated_utc") or ""),
            "snapshot_age_sec": round(age_sec, 3) if math.isfinite(age_sec) else None,
            "currency_count": 0,
            "instrument_count": len(pairs),
            "currency_ranks": [],
            "rank_theses": [],
            "pair_candidates": [],
            "selected_pairs": [],
            "rejection_counts": dict(reasons),
        }

    standardized = {horizon: _robust_z(values) for horizon, values in raw_strength.items()}
    scores = {
        currency: sum(WEIGHTS[horizon] * standardized[horizon][currency] for horizon in WEIGHTS)
        for currency in EXPECTED_CURRENCIES
    }
    ranks = sorted(EXPECTED_CURRENCIES, key=lambda currency: (-scores[currency], currency))
    rank_rows = [
        {
            "rank": index + 1,
            "currency": currency,
            "score": round(scores[currency], 6),
            "strength_5m_bps": round(raw_strength["5"][currency], 6),
            "strength_15m_bps": round(raw_strength["15"][currency], 6),
            "strength_60m_bps": round(raw_strength["60"][currency], 6),
            "trend": (
                "strong"
                if raw_strength["5"][currency] >= cfg.min_leg_strength_bps
                and raw_strength["15"][currency] >= cfg.min_leg_strength_bps
                else "weak"
                if raw_strength["5"][currency] <= -cfg.min_leg_strength_bps
                and raw_strength["15"][currency] <= -cfg.min_leg_strength_bps
                else "mixed"
            ),
        }
        for index, currency in enumerate(ranks)
    ]

    structural_theses: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    for strong in ranks:
        if (
            raw_strength["5"][strong] < cfg.min_leg_strength_bps
            or raw_strength["15"][strong] < cfg.min_leg_strength_bps
        ):
            continue
        for weak in reversed(ranks):
            if strong == weak:
                continue
            if (
                raw_strength["5"][weak] > -cfg.min_leg_strength_bps
                or raw_strength["15"][weak] > -cfg.min_leg_strength_bps
            ):
                continue
            gap_5 = raw_strength["5"][strong] - raw_strength["5"][weak]
            gap_15 = raw_strength["15"][strong] - raw_strength["15"][weak]
            if min(gap_5, gap_15) < cfg.min_factor_gap_bps:
                reasons["factor_gap_below_floor"] += 1
                continue
            matching = [
                instrument for instrument in EXPECTED_INSTRUMENTS
                if _pair_shape(instrument, strong, weak) is not None
            ]
            if not matching:
                reasons["strong_weak_pair_not_listed"] += 1
                continue
            instrument = matching[0]
            side, sign = _pair_shape(instrument, strong, weak) or ("", 0.0)
            thesis_id = f"{MODEL_ID}:{strong}>{weak}:{instrument}:{side}"
            structural_theses.append(
                {
                    "instrument": instrument,
                    "direction": side,
                    "strong_currency": strong,
                    "weak_currency": weak,
                    "strong_score": round(scores[strong], 6),
                    "weak_score": round(scores[weak], 6),
                    "factor_gap_5m_bps": round(gap_5, 6),
                    "factor_gap_15m_bps": round(gap_15, 6),
                    "thesis_id": thesis_id,
                }
            )
            pair = pairs[instrument]
            windows = pair.get("windows") or {}
            directional_5 = sign * _number((windows.get("5") or {}).get("return_bps"))
            directional_15 = sign * _number((windows.get("15") or {}).get("return_bps"))
            if min(directional_5, directional_15) < cfg.min_pair_confirmation_bps:
                reasons["pair_reaction_not_confirmed"] += 1
                continue
            spread_bps = _number(pair.get("spread_bps"), math.inf)
            stressed_cost = spread_bps * cfg.spread_stress_multiple
            if not math.isfinite(spread_bps) or spread_bps <= 0.0 or spread_bps > cfg.max_spread_bps:
                reasons["spread_above_cap"] += 1
                continue
            gross_proxy = 0.60 * directional_5 + 0.40 * min(directional_15, gap_15)
            cost_ratio = gross_proxy / max(stressed_cost, 1e-9)
            if cost_ratio < cfg.min_confirmation_to_cost:
                reasons["reaction_below_stressed_cost"] += 1
                continue
            score = (
                scores[strong] - scores[weak]
                + 0.20 * min(10.0, cost_ratio)
                + 0.05 * min(20.0, directional_5)
            )
            candidates.append(
                {
                    "instrument": instrument,
                    "direction": side,
                    "strong_currency": strong,
                    "weak_currency": weak,
                    "strong_rank": ranks.index(strong) + 1,
                    "weak_rank": ranks.index(weak) + 1,
                    "strong_score": round(scores[strong], 6),
                    "weak_score": round(scores[weak], 6),
                    "factor_gap_5m_bps": round(gap_5, 6),
                    "factor_gap_15m_bps": round(gap_15, 6),
                    "pair_directional_5m_bps": round(directional_5, 6),
                    "pair_directional_15m_bps": round(directional_15, 6),
                    "pair_directional_5m_pips": round(
                        sign * _number((windows.get("5") or {}).get("return_pips")), 6
                    ),
                    "pair_directional_15m_pips": round(
                        sign * _number((windows.get("15") or {}).get("return_pips")), 6
                    ),
                    "spread_bps": round(spread_bps, 6),
                    "stressed_cost_bps": round(stressed_cost, 6),
                    "gross_movement_proxy_bps": round(gross_proxy, 6),
                    "after_cost_proxy_bps": round(gross_proxy - stressed_cost, 6),
                    "confirmation_to_cost": round(cost_ratio, 6),
                    "rank_score": round(score, 6),
                    "thesis_id": thesis_id,
                }
            )

    candidates.sort(key=lambda row: (-_number(row.get("rank_score")), str(row.get("instrument"))))
    used = {str(currency) for currency in reserved_currencies if str(currency) in EXPECTED_CURRENCIES}
    selected: list[dict[str, Any]] = []
    for candidate in candidates:
        legs = {str(candidate["strong_currency"]), str(candidate["weak_currency"])}
        if used.intersection(legs):
            reasons["currency_factor_already_reserved"] += 1
            continue
        selected.append(candidate)
        used.update(legs)
        if len(selected) >= cfg.max_selected:
            break

    return {
        "status": "ready",
        "model_id": MODEL_ID,
        "generated_utc": str(payload.get("generated_utc") or ""),
        "snapshot_age_sec": round(age_sec, 3),
        "currency_count": len(rank_rows),
        "instrument_count": len(pairs),
        "currency_ranks": rank_rows,
        "rank_theses": structural_theses,
        "pair_candidates": candidates,
        "selected_pairs": selected,
        "rejection_counts": dict(reasons),
        "settings": {
            key: value for key, value in vars(cfg).items()
        },
    }
