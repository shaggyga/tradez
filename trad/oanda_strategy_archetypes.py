#!/usr/bin/env python3
"""Canonical independence taxonomy for the OANDA strategy surface.

Family names are useful for diagnostics, but they are not automatically
independent evidence.  Four profiles of one family are parameter variants and
several differently named families consume the same directional price state.
This module provides a deliberately conservative archetype layer so research
and dashboards can report both raw family breadth and effective independent
breadth without silently changing execution policy.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable, Mapping


ARCHETYPE_DEFINITIONS: dict[str, dict[str, str]] = {
    "trend_momentum": {
        "label": "Trend / momentum",
        "description": "Continuation from directional price persistence or smoothed trend.",
    },
    "breakout_expansion": {
        "label": "Breakout / expansion",
        "description": "Range escape, compression release, or threshold-crossing continuation.",
    },
    "mean_reversion": {
        "label": "Mean reversion / reversal",
        "description": "Fade of extension, exhaustion, failed break, or value deviation.",
    },
    "cross_sectional_value": {
        "label": "Cross-sectional / relative value",
        "description": "Currency-strength, pair-rank, residual, or cross-market comparison.",
    },
    "statistical_forecast": {
        "label": "Statistical forecast",
        "description": "Forecast from fitted state, transition, autoregressive, or supervised models.",
    },
    "volatility_liquidity": {
        "label": "Volatility / liquidity",
        "description": "Directional use of volatility, jump, volume, or spread state.",
    },
    "regime_state": {
        "label": "Regime state",
        "description": "Direction conditioned on persistence, variance, or market-state classification.",
    },
    "ensemble_control": {
        "label": "Ensemble / control",
        "description": "Combination or veto logic derived from other strategy votes.",
    },
    "event_fundamental": {
        "label": "Event / fundamental",
        "description": "Causally timestamped macro, policy, carry, or news-event evidence.",
    },
    "unclassified": {
        "label": "Unclassified",
        "description": "Family without an audited independence assignment; conservatively one shared vote.",
    },
}


FAMILY_ARCHETYPE: dict[str, str] = {
    # Trend and continuation variants.
    "momentum": "trend_momentum",
    "ahl_multihorizon_trend": "trend_momentum",
    "ema_trend_cross": "trend_momentum",
    "higher_timeframe_alignment": "trend_momentum",
    "rsi_trend_continuation": "trend_momentum",
    "linear_regression_trend": "trend_momentum",
    "efficiency_filtered_momentum": "trend_momentum",
    "kama_adaptive_trend": "trend_momentum",
    "trend_momentum_confluence": "trend_momentum",
    "permutation_entropy_momentum": "trend_momentum",
    "theil_sen_trend": "trend_momentum",
    "multi_timeframe_trend": "trend_momentum",
    "moving_average_feature_grid": "trend_momentum",

    # Breakout and expansion variants.
    "donchian_breakout": "breakout_expansion",
    "volatility_squeeze_breakout": "breakout_expansion",
    "range_expansion": "breakout_expansion",
    "spread_compression_momentum": "breakout_expansion",
    "breakout_retest": "breakout_expansion",
    "session_range_breakout": "breakout_expansion",
    "micro_channel_break": "breakout_expansion",
    "breakout_volume_confluence": "breakout_expansion",
    "cusum_breakout": "breakout_expansion",
    "garch_volatility_breakout": "breakout_expansion",
    "breakout_change_point": "breakout_expansion",

    # Reversal and value-deviation variants.
    "pullback": "mean_reversion",
    "macd_rsi_reversal": "mean_reversion",
    "bollinger_reversion": "mean_reversion",
    "stochastic_reversal": "mean_reversion",
    "candlestick_reversal": "mean_reversion",
    "atr_mean_reversion": "mean_reversion",
    "failed_breakout_reversal": "mean_reversion",
    "cci_reversion": "mean_reversion",
    "volume_climax_reversal": "mean_reversion",
    "volatility_shock_fade": "mean_reversion",
    "oscillator_reversion_confluence": "mean_reversion",
    "bipower_jump_reversal": "mean_reversion",
    "vwap_deviation_reversion": "mean_reversion",
    "ny_session_vwap_sell_reversion": "mean_reversion",

    # Cross-sectional price relationships.
    "currency_strength": "cross_sectional_value",
    "relative_value_reversion": "cross_sectional_value",
    "cross_sectional_pair_rank": "cross_sectional_value",
    "cross_pair_lead_lag": "cross_sectional_value",
    "cross_market_confluence": "cross_sectional_value",
    "cross_currency_impulse": "cross_sectional_value",
    "cross_pair_graph_forecasting": "cross_sectional_value",

    # Fitted directional forecasts.
    "supervised_return_rank": "statistical_forecast",
    "pattern_count_forecast": "statistical_forecast",
    "kalman_local_trend": "statistical_forecast",
    "markov_sign_transition": "statistical_forecast",
    "online_ar_forecast": "statistical_forecast",
    "ridge_return": "statistical_forecast",
    "second_ridge_forecast": "statistical_forecast",
    "timeframe_equation_matrix": "statistical_forecast",
    "live_model_forecast": "statistical_forecast",
    "differenced_path_analog": "statistical_forecast",
    "intrasecond_ridge": "statistical_forecast",
    "major_neural_forecasters": "statistical_forecast",
    "modern_tabular_probabilistic": "statistical_forecast",
    "neural_state_space": "statistical_forecast",
    "representation_and_transfer_learning": "statistical_forecast",
    "time_series_foundation_models": "statistical_forecast",
    "decision_learning": "statistical_forecast",

    # Volatility, activity, and spread state used directionally.
    "volume_impulse": "volatility_liquidity",
    "spread_mean_reversion": "volatility_liquidity",
    "har_volatility_momentum": "volatility_liquidity",
    "move_alert_causal_state": "volatility_liquidity",

    # State and persistence classifiers.
    "regime_switching": "regime_state",
    "variance_ratio_regime": "regime_state",
    "hurst_regime_forecast": "regime_state",

    # These consume other strategy votes and cannot count as independent alpha.
    "inverse_correlation_veto": "ensemble_control",
    "signal_combination_rules": "ensemble_control",

    # Reserved explicit fundamental lanes.  These names are not currently part
    # of the default strategy lab, but prevent future event signals from being
    # silently merged with technical families.
    "news_event_direction": "event_fundamental",
    "macro_surprise": "event_fundamental",
    "carry_rate_differential": "event_fundamental",
}


def strategy_archetype(family: Any) -> str:
    """Return the audited archetype, conservatively grouping unknown families."""

    normalized = str(family or "").strip().lower()
    return FAMILY_ARCHETYPE.get(normalized, "unclassified")


def archetype_vote_summary(
    rows: Iterable[Mapping[str, Any]],
    *,
    target_direction: str | None = None,
    family_key: str = "family",
    direction_key: str = "direction",
    weight_key: str = "matrix_weight",
) -> dict[str, Any]:
    """Summarize independent directional support without counting profiles twice.

    Each family contributes at most its strongest profile weight in each
    direction.  Each archetype then contributes one normalized directional
    share, so adding more correlated families cannot manufacture extra
    independent votes.
    """

    normalized_target = str(target_direction or "").strip().lower()
    if normalized_target not in {"buy", "sell"}:
        normalized_target = ""

    family_direction_weight: dict[tuple[str, str, str], float] = {}
    families: set[str] = set()
    unclassified_families: set[str] = set()
    for row in rows:
        family = str(row.get(family_key) or row.get("lane_id") or "").strip()
        direction = str(row.get(direction_key) or "").strip().lower()
        if not family or direction not in {"buy", "sell"}:
            continue
        archetype = strategy_archetype(family)
        weight_value = row.get(weight_key)
        try:
            weight = abs(float(weight_value)) if weight_value is not None else 1.0
        except (TypeError, ValueError):
            weight = 1.0
        if weight <= 0.0:
            weight = 1.0
        key = (archetype, family, direction)
        family_direction_weight[key] = max(weight, family_direction_weight.get(key, 0.0))
        families.add(family)
        if archetype == "unclassified":
            unclassified_families.add(family)

    grouped: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "families": set(),
            "buy_families": set(),
            "sell_families": set(),
            "buy_weight": 0.0,
            "sell_weight": 0.0,
        }
    )
    for (archetype, family, direction), weight in family_direction_weight.items():
        entry = grouped[archetype]
        entry["families"].add(family)
        entry[f"{direction}_families"].add(family)
        entry[f"{direction}_weight"] += weight

    agreeing: list[str] = []
    opposing: list[str] = []
    conflicted: list[str] = []
    details: list[dict[str, Any]] = []
    aligned_share = 0.0
    opposing_share = 0.0
    active_share_count = 0
    for archetype in sorted(grouped):
        entry = grouped[archetype]
        buy_weight = float(entry["buy_weight"])
        sell_weight = float(entry["sell_weight"])
        total_weight = buy_weight + sell_weight
        if buy_weight > sell_weight:
            direction = "buy"
        elif sell_weight > buy_weight:
            direction = "sell"
        else:
            direction = "conflicted"
        if direction == "conflicted":
            conflicted.append(archetype)
        elif normalized_target and direction == normalized_target:
            agreeing.append(archetype)
        elif normalized_target:
            opposing.append(archetype)
        if normalized_target and total_weight > 0.0:
            aligned_weight = buy_weight if normalized_target == "buy" else sell_weight
            opposed_weight = sell_weight if normalized_target == "buy" else buy_weight
            aligned_share += aligned_weight / total_weight
            opposing_share += opposed_weight / total_weight
            active_share_count += 1
        details.append(
            {
                "archetype": archetype,
                "label": ARCHETYPE_DEFINITIONS[archetype]["label"],
                "direction": direction,
                "families": sorted(entry["families"]),
                "buy_families": sorted(entry["buy_families"]),
                "sell_families": sorted(entry["sell_families"]),
                "buy_weight": round(buy_weight, 6),
                "sell_weight": round(sell_weight, 6),
                "conviction": round(
                    abs(buy_weight - sell_weight) / total_weight if total_weight > 0.0 else 0.0,
                    6,
                ),
            }
        )

    archetype_count = len(grouped)
    raw_family_count = len(families)
    denominator = len(agreeing) + len(opposing) + len(conflicted)
    return {
        "policy": "one_normalized_vote_per_archetype_shadow_v1",
        "shadow_only": True,
        "raw_family_count": raw_family_count,
        "archetype_count": archetype_count,
        "independence_ratio": round(
            archetype_count / raw_family_count if raw_family_count else 0.0,
            6,
        ),
        "target_direction": normalized_target or None,
        "agreeing_archetype_count": len(agreeing),
        "opposing_archetype_count": len(opposing),
        "conflicted_archetype_count": len(conflicted),
        "agreement_probability": round(
            (len(agreeing) + 1.0) / (denominator + 2.0),
            6,
        )
        if normalized_target
        else None,
        "aligned_weight_pct": round(
            100.0 * aligned_share / active_share_count,
            3,
        )
        if active_share_count
        else 0.0,
        "opposing_weight_pct": round(
            100.0 * opposing_share / active_share_count,
            3,
        )
        if active_share_count
        else 0.0,
        "agreeing_archetypes": agreeing,
        "opposing_archetypes": opposing,
        "conflicted_archetypes": conflicted,
        "unclassified_families": sorted(unclassified_families),
        "details": details,
    }
