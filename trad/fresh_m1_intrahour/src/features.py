from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pandas as pd

from .common import write_json


LABEL_LIKE_PREFIXES = (
    "target_",
    "abs_move_pips_",
    "future_range_pips_",
    "long_endpoint_",
    "short_endpoint_",
    "long_mfe_",
    "short_mfe_",
    "long_mae_",
    "short_mae_",
    "endpoint_",
    "mfe_",
    "mae_",
    "net_after_cost_",
    "net_account_pnl_",
    "policy_",
    "gross_mid_",
    "cost_drag_",
)

LABEL_LIKE_COLUMNS = {
    "target_ev_usd_1k_units",
    "target_positive_ev",
    "target_policy",
    "target_horizon_minutes",
    "best_exit_policy",
    "best_horizon",
    "best_policy_net_account_pnl_1k_units",
    "best_horizon_net_account_pnl_1k_units",
    "raw_best_horizon_any_side",
}

NON_FEATURE_COLUMNS = {
    "decision_time_utc",
    "datetime",
    "time",
    "instrument",
    "granularity",
    "side",
    "side_sign",
    "base_currency",
    "quote_currency",
    "tier",
    "entry_delay_bars",
    "same_bar_ambiguity_mode",
    "execution_cost_mode",
}

FUTURE_LIKE_TOKENS = (
    "future",
    "oracle",
    "actual_",
    "realized_",
    "label",
    "tp_before_sl",
    "sl_before_tp",
    "timeout",
    "time_to_exit",
)


# Every model feature must be observable at the close of the decision bar. New
# engineered columns are excluded until they are explicitly reviewed here.
DECISION_TIME_NUMERIC_COLUMNS = frozenset({
    "acceleration_3_10",
    "acceleration_5_15",
    "ask_close",
    "ask_high",
    "ask_low",
    "ask_open",
    "atr_15m_pips",
    "atr_30m_pips",
    "atr_5m_pips",
    "atr_60m_pips",
    "base_minus_quote_strength_15m",
    "base_minus_quote_strength_5m",
    "base_strength_15m",
    "base_strength_5m",
    "bid_close",
    "bid_high",
    "bid_low",
    "bid_open",
    "bounce_from_low_30m_pips",
    "choppiness_15m",
    "close",
    "cost_pressure_score",
    "dist_recent_high_30m_pips",
    "dist_recent_low_30m_pips",
    "efficiency_ratio_15m",
    "ema_10_gap_pips",
    "ema_10_slope_pips",
    "ema_20_gap_pips",
    "ema_20_slope_pips",
    "ema_50_gap_pips",
    "ema_50_slope_pips",
    "ema_5_gap_pips",
    "ema_5_slope_pips",
    "h1_momentum_pips",
    "high",
    "hour_cos",
    "hour_sin",
    "is_asia",
    "is_friday_late",
    "is_london",
    "is_london_ny_overlap",
    "is_london_open",
    "is_new_york",
    "is_new_york_open",
    "is_rollover_risk",
    "low",
    "m1_h1_alignment",
    "m1_m15_alignment",
    "m1_m5_alignment",
    "m15_momentum_pips",
    "m30_momentum_pips",
    "m5_momentum_pips",
    "minute_cos",
    "minute_sin",
    "momentum_10m_atr",
    "momentum_15m_atr",
    "momentum_3m_atr",
    "momentum_5m_atr",
    "movement_forecast_proxy",
    "movement_quality",
    "open",
    "pair_residual_5m",
    "pip_value_usd_per_unit",
    "pullback_from_high_30m_pips",
    "quote_strength_15m",
    "quote_strength_5m",
    "range_15m_pips",
    "range_30m_pips",
    "range_5m_pips",
    "range_60m_pips",
    "range_position_30m",
    "return_10m_pips",
    "return_13m_pips",
    "return_15m_pips",
    "return_1m_pips",
    "return_21m_pips",
    "return_2m_pips",
    "return_30m_pips",
    "return_3m_pips",
    "return_5m_pips",
    "return_60m_pips",
    "return_8m_pips",
    "reversal_count_15m",
    "round_trip_cost_pips",
    "round_trip_cost_pips_used",
    "rv_15m_pips",
    "rv_30m_pips",
    "rv_5m_pips",
    "rv_60m_pips",
    "session_liquidity_score",
    "side_acceleration",
    "side_breakout_pressure",
    "side_currency_strength_15m",
    "side_currency_strength_5m",
    "side_exhaustion_risk",
    "side_momentum_15m_atr",
    "side_momentum_3m_atr",
    "side_momentum_5m_atr",
    "side_pair_residual_5m",
    "side_pullback_pressure",
    "side_range_position",
    "side_return_13m_pips",
    "side_return_1m_pips",
    "side_return_21m_pips",
    "side_return_2m_pips",
    "side_return_3m_pips",
    "side_return_5m_pips",
    "side_return_8m_pips",
    "side_trend_alignment",
    "slippage_pips",
    "spread_pips",
    "spread_pips_used",
    "spread_to_atr_15m",
    "volatility_expansion_15_60",
    "volume",
    "weekday_cos",
    "weekday_sin",
    "wick_body_ratio",
    "xs_opportunity_score",
    "xs_rank_atr_15m_pips",
    "xs_rank_cost_pressure_score",
    "xs_rank_movement_quality",
    "xs_rank_side_currency_strength_5m",
    "xs_rank_side_momentum_5m_atr",
    "xs_rank_spread_to_atr_15m",
})


def is_label_like_column(col: str) -> bool:
    return col in LABEL_LIKE_COLUMNS or any(col.startswith(prefix) for prefix in LABEL_LIKE_PREFIXES)


def is_future_like_column(col: str) -> bool:
    lowered = col.lower()
    return any(token in lowered for token in FUTURE_LIKE_TOKENS)


def is_decision_time_feature_column(col: str) -> bool:
    return (
        col in DECISION_TIME_NUMERIC_COLUMNS
        and col not in NON_FEATURE_COLUMNS
        and not is_label_like_column(col)
        and not is_future_like_column(col)
    )


def assert_decision_time_feature_columns(columns: list[str]) -> None:
    rejected = sorted({col for col in columns if not is_decision_time_feature_column(col)})
    if rejected:
        raise ValueError(f"non-decision-time model features rejected: {rejected}")


def allowed_numeric_feature_columns(df: pd.DataFrame) -> list[str]:
    cols: list[str] = []
    for col in df.columns:
        if not is_decision_time_feature_column(col):
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            cols.append(col)
    return sorted(cols)


def feature_family_columns(df: pd.DataFrame) -> dict[str, list[str]]:
    numeric = set(allowed_numeric_feature_columns(df))

    def keep(names: list[str]) -> list[str]:
        return sorted(c for c in names if c in numeric)

    price_action = keep([
        "return_1m_pips",
        "return_2m_pips",
        "return_3m_pips",
        "return_5m_pips",
        "return_8m_pips",
        "return_13m_pips",
        "return_21m_pips",
        "momentum_3m_atr",
        "momentum_5m_atr",
        "momentum_10m_atr",
        "momentum_15m_atr",
        "acceleration_3_10",
        "acceleration_5_15",
        "ema_5_gap_pips",
        "ema_10_gap_pips",
        "ema_20_gap_pips",
        "ema_5_slope_pips",
        "ema_10_slope_pips",
        "ema_20_slope_pips",
        "side_return_1m_pips",
        "side_return_2m_pips",
        "side_return_3m_pips",
        "side_return_5m_pips",
        "side_return_8m_pips",
        "side_momentum_3m_atr",
        "side_momentum_5m_atr",
        "side_momentum_15m_atr",
        "side_acceleration",
        "side_trend_alignment",
        "side_range_position",
        "side_breakout_pressure",
        "side_pullback_pressure",
        "side_exhaustion_risk",
    ])
    spread_cost = keep([
        "spread_pips",
        "spread_pips_used",
        "slippage_pips",
        "round_trip_cost_pips",
        "round_trip_cost_pips_used",
        "spread_to_atr_15m",
        "cost_pressure_score",
        "session_liquidity_score",
    ])
    volatility_motion = keep([
        "atr_5m_pips",
        "atr_15m_pips",
        "atr_30m_pips",
        "atr_60m_pips",
        "rv_5m_pips",
        "rv_15m_pips",
        "rv_30m_pips",
        "range_5m_pips",
        "range_15m_pips",
        "range_30m_pips",
        "volatility_expansion_15_60",
    ])
    currency_strength = keep([
        "base_strength_5m",
        "quote_strength_5m",
        "base_minus_quote_strength_5m",
        "side_currency_strength_5m",
        "base_strength_15m",
        "quote_strength_15m",
        "base_minus_quote_strength_15m",
        "side_currency_strength_15m",
    ])
    cross_sectional = keep([c for c in numeric if c.startswith("xs_rank_") or c == "xs_opportunity_score"])
    session_time = keep([
        "hour_sin",
        "hour_cos",
        "minute_sin",
        "minute_cos",
        "weekday_sin",
        "weekday_cos",
        "is_asia",
        "is_london",
        "is_new_york",
        "is_london_ny_overlap",
        "is_london_open",
        "is_new_york_open",
        "is_rollover_risk",
        "is_friday_late",
    ])
    higher_timeframe = keep([
        "m5_momentum_pips",
        "m15_momentum_pips",
        "m30_momentum_pips",
        "h1_momentum_pips",
        "m1_m5_alignment",
        "m1_m15_alignment",
        "m1_h1_alignment",
    ])
    quality = keep([
        "wick_body_ratio",
        "reversal_count_15m",
        "efficiency_ratio_15m",
        "choppiness_15m",
        "movement_quality",
    ])

    return {
        "A_m1_price_action": price_action,
        "B_m1_spread_cost_liquidity": sorted(set(price_action + spread_cost)),
        "C_m1_volatility_motion": volatility_motion,
        "D_m1_currency_strength": currency_strength,
        "E_m1_cross_sectional_ranks": cross_sectional,
        "F_m1_session_time": session_time,
        "G_m1_higher_timeframe": higher_timeframe,
        "H_movement_quality": quality,
        "I_full_tabular": sorted(numeric),
        "K_minimal_low_leakage_baseline": sorted(set(price_action[:8] + spread_cost + session_time)),
    }


def _signature(columns: list[str]) -> str:
    payload = "\n".join(sorted(columns)).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_feature_family_report(df: pd.DataFrame) -> dict[str, Any]:
    excluded_label = sorted(c for c in df.columns if is_label_like_column(c))
    excluded_future = sorted(c for c in df.columns if is_future_like_column(c) and c not in excluded_label)
    numeric_columns = sorted(c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]))
    approved_numeric = allowed_numeric_feature_columns(df)
    rejected_unapproved = sorted(
        c for c in numeric_columns
        if c not in approved_numeric and c not in excluded_label and c not in excluded_future
    )
    families = []
    for name, cols in feature_family_columns(df).items():
        families.append({
            "feature_family_name": name,
            "column_count": int(len(cols)),
            "columns": cols,
            "excluded_label_like_columns": excluded_label,
            "excluded_future_like_columns": excluded_future,
            "feature_set_sha256": _signature(cols),
        })
    selected_columns = sorted({col for family in families for col in family["columns"]})
    forbidden_selected = sorted(c for c in selected_columns if not is_decision_time_feature_column(c))
    return {
        "family_count": int(len(families)),
        "families": families,
        "decision_time_allowlist_enforced": True,
        "approved_numeric_feature_columns": approved_numeric,
        "excluded_label_like_columns": excluded_label,
        "excluded_future_like_columns": excluded_future,
        "rejected_unapproved_numeric_columns": rejected_unapproved,
        "forbidden_selected_feature_columns": forbidden_selected,
        "no_feature_leakage": not forbidden_selected,
        "feature_leakage_suspect_count": int(len(excluded_label) + len(excluded_future)),
    }


def write_feature_family_report(df: pd.DataFrame, output_path: Path) -> Path:
    write_json(output_path, build_feature_family_report(df))
    return output_path
