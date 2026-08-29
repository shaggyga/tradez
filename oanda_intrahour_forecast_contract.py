#!/usr/bin/env python3
"""Source-of-truth contract for the structural next-hour FX forecast."""

from __future__ import annotations

import math
from typing import Any, Mapping


CONTRACT_VERSION = "intrahour_mtf_forecast_v3_opportunity"
DECISION_TIMEFRAME = "M1"
FORECAST_CONTEXT_TIMEFRAMES = ("M1", "M30", "H1", "H4")
FORECAST_HORIZONS_SEC = (60, 120, 180, 300, 600, 900, 1800, 3600)

# These values may improve order placement or exit timing, but they are not part
# of the structural forecast until a separately validated causal history exists.
EXECUTION_ONLY_FEATURES = frozenset(
    {
        "live_spread_pips",
        "bid_top_liquidity",
        "ask_top_liquidity",
        "bid_total_liquidity",
        "ask_total_liquidity",
        "depth_imbalance",
        "bid_levels",
        "ask_levels",
        "depth_total_liquidity",
        "depth_log_total_liquidity",
        "depth_top_imbalance",
        "microprice",
        "microprice_offset_pips",
        "quote_receive_age_sec",
        "order_book_available",
        "position_book_available",
    }
)
EXECUTION_ONLY_PREFIXES = (
    "depth_",
    "microprice_",
    "order_book_",
    "position_book_",
)

# OANDA book snapshots currently cover only a small prospective sample. These
# requirements make future eligibility explicit instead of enabling them when
# a few fresh rows happen to exist.
ORDER_BOOK_MIN_HISTORY_DAYS = 30
ORDER_BOOK_MIN_INSTRUMENTS = 10


def is_execution_only_feature(name: str) -> bool:
    normalized = str(name)
    return normalized in EXECUTION_ONLY_FEATURES or normalized.startswith(
        EXECUTION_ONLY_PREFIXES
    )


def structural_feature_view(features: Mapping[str, Any]) -> dict[str, Any]:
    """Return a structural model view with execution microstructure removed."""

    return {
        str(name): value
        for name, value in features.items()
        if not is_execution_only_feature(str(name))
    }


def filter_context_views(
    views: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {
        timeframe: structural_feature_view(views[timeframe])
        for timeframe in FORECAST_CONTEXT_TIMEFRAMES
        if timeframe in views and isinstance(views[timeframe], Mapping)
    }


def flatten_context_views(
    views: Mapping[str, Mapping[str, Any]],
) -> dict[str, float | int | bool | None]:
    """Flatten causal scalar context into a namespaced unified model row."""

    output: dict[str, float | int | bool | None] = {}
    for timeframe, features in filter_context_views(views).items():
        prefix = f"{timeframe.lower()}__"
        for name, value in features.items():
            if isinstance(value, bool) or value is None:
                output[prefix + name] = value
            elif isinstance(value, (int, float)):
                numeric = float(value)
                output[prefix + name] = numeric if math.isfinite(numeric) else None
    return output


def is_forecast_cell(timeframe: str, horizon_sec: int) -> bool:
    return (
        str(timeframe).upper() in FORECAST_CONTEXT_TIMEFRAMES
        and int(horizon_sec) in FORECAST_HORIZONS_SEC
    )


def contract_payload() -> dict[str, Any]:
    return {
        "version": CONTRACT_VERSION,
        "decision_timeframe": DECISION_TIMEFRAME,
        "context_timeframes": list(FORECAST_CONTEXT_TIMEFRAMES),
        "forecast_horizons_sec": list(FORECAST_HORIZONS_SEC),
        "forecast_target": (
            "causal structural midpoint path with barrier-first, favorable "
            "excursion, continuation-lifetime, and spread-normalized "
            "opportunity outputs"
        ),
        "barrier_distances_pips": [1, 3, 5],
        "opportunity_horizons_sec": [300, 900, 1800, 3600],
        "same_bar_barrier_policy": "ambiguous_neither_direction_wins",
        "tradability_target_role": "separate executable bid_ask diagnostic_only",
        "completed_bars_only": True,
        "candle_volume_semantics": "oanda_price_update_count_not_exchange_volume",
        "s1_role": "optional_execution_timing_only",
        "s5_role": "legacy_research_and_execution_replay_only",
        "pricing_depth_role": "execution_timing_and_cost_diagnostics_only",
        "order_position_book_role": "prospective_research_only_pending_validation",
        "order_position_book_training_eligible": False,
        "order_position_book_min_history_days": ORDER_BOOK_MIN_HISTORY_DAYS,
        "order_position_book_min_instruments": ORDER_BOOK_MIN_INSTRUMENTS,
        "account_or_position_state_used": False,
    }
