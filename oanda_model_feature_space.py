#!/usr/bin/env python3
"""Build an auditable feature space for forecast, execution, and controller models.

This does not enqueue trainer jobs or change live trading behavior.  It writes a
structured feature-space spec that can be used to decide which parts belong in
the tabular trainer, which need external evaluators, and which are execution or
lifecycle controls.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from oanda_intrahour_forecast_contract import contract_payload
except ModuleNotFoundError:
    from trad.oanda_intrahour_forecast_contract import contract_payload


PROJECT_ROOT = Path(os.environ.get("TRAD_PROJECT_ROOT", Path(__file__).resolve().parent))
CONFIG_PATH = PROJECT_ROOT / "config" / "model_feature_space.json"
MODEL_SPACE_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager" / "model_space"
LATEST_PATH = MODEL_SPACE_ROOT / "model_feature_space_latest.json"
MARKDOWN_PATH = PROJECT_ROOT / "MODEL_FEATURE_SPACE.md"


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def atomic_write_json(path: Path, payload: Any) -> None:
    atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


TIMEFRAMES = ["M1", "M30", "H1", "H4"]
FORECAST_HORIZONS_MINUTES = [1, 2, 3, 5, 10, 15, 30, 60]
INSTRUMENT_SUBSETS = [
    "all",
    "majors",
    "volatile",
    "non_usd_volatile",
    "volatile_non_usd",
    "volatile_exotic",
    "exotic",
    "exotic_high_spread",
    "jpy_risk",
    "chf_safe_haven",
    "commodity",
    "eur_gbp_cross",
    "usd_other",
]

USER_MODEL_TERMS = [
    "ARIMA",
    "SARIMAX",
    "Event-Gated ARIMA",
    "ATR Model",
    "ADR Model",
    "Cumulative Pip Movement Model",
    "Volatility Regime Model",
    "Dummy / Paper Account Model",
    "Risk-Adjusted P&L Model",
    "Grace Period / Cooldown Model",
    "Randomized Parameter Sweep Model",
    "Top-N Cluster Selection Model",
    "Throttle / Split Capital Model",
    "OANDA Live Mirror Model",
    "Forecast Flood Model",
    "Performance Ranking Model",
    "Multi-Timeframe Consensus Model",
    "Regime-Switching Controller Model",
    "ARIMA(0,1,1)",
    "ARIMA(1,1,0)",
    "ARIMA(1,1,1)",
    "ARIMA(2,1,1)",
    "ARIMA(1,1,2)",
    "ARIMA(0,2,1)",
    "ARIMA(1,2,1)",
    "ARIMA(1,2,2)",
    "EV_091",
    "EV_121",
    "EV_212",
    "EV_015",
    "M5_ARIMA",
    "M15_ARIMA",
    "H1_ARIMA",
    "M5_EV_091",
    "M15_ATR_032",
    "EV_091_H1",
    "ATR_032",
    "Reverse-on-Flip ARIMA",
    "No-Reverse ARIMA",
    "Edge-Gated ARIMA",
    "Minimum-Hold ARIMA",
    "TP/SL ARIMA",
    "Trailing-Stop ARIMA",
    "Time-Stop ARIMA",
    "ARIMA_PRE",
    "ARIMA_POST",
    "ACTIVE Model",
    "THROTTLE Model",
    "REHAB Model",
    "HARD_KILL Model",
    "Reverse Edge Model",
    "Churn Model",
    "Churn-Reduced Model",
    "Trend-Continuation Model",
    "Range-Failure Model",
    "Mean-Reversion Failure Model",
    "Equity-Preserving Model",
    "Drawdown-Controlled Model",
    "Exposure-Capped Model",
    "Equity-Gated Meta Model",
    "Exposure-Cap Meta Model",
    "Lifecycle Controller",
    "Throttle Controller",
    "Rehab Controller",
    "Kill-Switch Controller",
    "Kalman Filter",
    "Adaptive Kalman Filter",
    "State-Space Model",
    "Linear State-Space Model",
    "Regime-Switching State-Space Model",
    "Hidden Markov Model",
    "Markov Regime Filter",
    "Trend Regime Classifier",
    "Volatility Regime Model",
    "ARIMA + Kalman Hybrid",
    "ARIMA + State-Space Controller",
    "ARIMA + Regime Filter",
    "ARIMA + Volatility Filter",
    "ARIMA Meta-Model",
    "ARIMA Ensemble",
    "Voting ARIMA Ensemble",
    "Confidence-Weighted ARIMA Ensemble",
    "Offline ARIMA Backtest Model",
    "PRE vs POST Comparative Model",
    "Churn-vs-Profit Diagnostic Model",
    "Model Health Scoring Model",
    "Regime Performance Model",
    "Currency Strength Basket Model",
    "Jump Shock Decay Model",
    "Spread Persistence Model",
    "Session Transition Model",
    "Rolling Skew/Kurtosis Model",
    "Candle Pressure Model",
    "Multi-Timeframe Feature Join Model",
    "Feature Audit Manifest",
]

ARIMA_ORDERS = [
    [0, 1, 1],
    [1, 1, 0],
    [1, 1, 1],
    [2, 1, 1],
    [1, 1, 2],
    [0, 2, 1],
    [1, 2, 1],
    [1, 2, 2],
    [3, 1, 0],
]

EVENT_GATES = {
    "EV_015": {
        "threshold": 0.15,
        "intent": "very permissive early warning gate",
    },
    "EV_091": {
        "threshold": 0.91,
        "intent": "high-confidence event trigger",
    },
    "EV_121": {
        "threshold": 1.21,
        "intent": "large-move event trigger",
    },
    "EV_212": {
        "threshold": 2.12,
        "intent": "extreme-move event trigger",
    },
}

FEATURE_BLOCKS = {
    "arima_forecast": {
        "description": "Direct time-series forecast and residual state for each pair/timeframe/order.",
        "features": [
            "arima_forecast_return",
            "arima_forecast_pips",
            "arima_forecast_z",
            "arima_forecast_slope",
            "arima_residual",
            "arima_residual_z",
            "arima_residual_autocorr_5",
            "arima_residual_autocorr_20",
            "arima_direction",
            "arima_direction_flip",
            "arima_confidence",
            "arima_order_id",
            "arima_timeframe",
        ],
    },
    "sarimax_exogenous": {
        "description": "ARIMA-style forecast with lagged exogenous context.",
        "features": [
            "sarimax_forecast_return",
            "sarimax_exog_beta_dxy",
            "sarimax_exog_beta_yield",
            "sarimax_exog_beta_gold",
            "sarimax_exog_beta_oil",
            "sarimax_exog_beta_us_equity",
            "sarimax_residual_z",
            "sarimax_exog_missing_flag",
        ],
    },
    "event_gate": {
        "description": "Boolean and continuous event filters around forecast confidence, move size, and spread.",
        "features": [
            "event_gate_id",
            "event_score",
            "event_gate_passed",
            "event_score_minus_threshold",
            "event_gate_time_since_last_pass",
            "event_gate_recent_pass_count",
            "event_gate_session_pass_rate",
        ],
    },
    "atr_adr_volatility": {
        "description": "Volatility, range, and sizing context.",
        "features": [
            "atr_14_pips",
            "atr_32_pips",
            "atr_032_norm",
            "atr_percentile_20d",
            "adr_5_pips",
            "adr_20_pips",
            "adr_used_pct",
            "range_compression_ratio",
            "range_expansion_ratio",
            "realized_vol_30m",
            "realized_vol_2h",
            "spread_to_atr",
        ],
    },
    "cumulative_pip_movement": {
        "description": "Path-aware movement and exhaustion features.",
        "features": [
            "cum_pips_5",
            "cum_pips_15",
            "cum_pips_30",
            "cum_pips_60",
            "cum_abs_pips_60",
            "cum_directional_efficiency",
            "pip_acceleration",
            "pip_deceleration",
            "max_favorable_pips_lookback",
            "max_adverse_pips_lookback",
        ],
    },
    "regime_state": {
        "description": "Trend, volatility, liquidity, and hidden-state labels.",
        "features": [
            "trend_regime_id",
            "trend_regime_prob",
            "volatility_regime_id",
            "volatility_regime_prob",
            "hmm_state_id",
            "hmm_state_prob",
            "markov_transition_prob",
            "regime_duration_bars",
            "regime_age_bars",
            "regime_flip_count_24h",
            "changepoint_score",
            "risk_on_off_score",
        ],
    },
    "kalman_state_space": {
        "description": "Smoothed state, dynamic beta, and residual mean-reversion context.",
        "features": [
            "kalman_level",
            "kalman_slope",
            "kalman_residual",
            "kalman_residual_z",
            "kalman_gain",
            "adaptive_kalman_process_var",
            "state_space_smoothed_return",
            "state_space_trend_prob",
            "dynamic_beta_to_basket",
        ],
    },
    "execution_policy": {
        "description": "Trade handling variants to test apart from signal quality.",
        "features": [
            "reverse_on_flip_enabled",
            "no_reverse_enabled",
            "minimum_hold_minutes",
            "tp_r_multiple",
            "sl_atr_multiple",
            "trailing_stop_atr_multiple",
            "time_stop_minutes",
            "cooldown_minutes_remaining",
            "grace_period_minutes_remaining",
            "churn_guard_active",
        ],
    },
    "risk_and_portfolio": {
        "description": "Position, account, and capital throttle state.",
        "features": [
            "risk_adjusted_pnl",
            "pnl_per_atr",
            "max_drawdown_recent",
            "equity_curve_slope",
            "equity_gate_passed",
            "exposure_pct",
            "pair_family_exposure_pct",
            "correlation_cluster_exposure_pct",
            "margin_used_pct",
            "split_capital_bucket",
            "throttle_factor",
        ],
    },
    "lifecycle_controller": {
        "description": "Meta-state for promotion, throttle, rehab, and hard-kill decisions.",
        "features": [
            "lifecycle_state",
            "active_state_score",
            "throttle_state_score",
            "rehab_state_score",
            "hard_kill_state_score",
            "shadow_pass_streak",
            "canary_pass_streak",
            "live_vs_shadow_delta_pips",
            "model_health_score",
            "days_since_promotion",
        ],
    },
    "diagnostics": {
        "description": "Features for judging whether a model is tradable or only statistically interesting.",
        "features": [
            "churn_rate",
            "reverse_edge_score",
            "profit_per_flip",
            "regime_performance_score",
            "pre_post_delta_pips",
            "pre_post_delta_auc",
            "forecast_flood_count",
            "forecast_agreement_ratio",
            "top_n_cluster_rank",
            "performance_rank_lookback",
        ],
    },
    "microstructure_liquidity": {
        "description": "Observed OANDA pricing liquidity plus order/position-book research features.",
        "features": [
            "bid_ask_spread_pips",
            "spread_percentile_20d",
            "session_liquidity_score",
            "rollover_window_flag",
            "weekend_gap_risk_flag",
            "quote_staleness_seconds",
            "slippage_proxy_pips",
            "bid_top_liquidity",
            "ask_top_liquidity",
            "bid_total_liquidity",
            "ask_total_liquidity",
            "depth_imbalance",
            "depth_top_imbalance",
            "depth_log_total_liquidity",
            "microprice_offset_pips",
            "order_book_near_5_imbalance",
            "order_book_near_10_imbalance",
            "order_book_near_25_imbalance",
            "order_book_near_50_imbalance",
            "order_book_above_25_net",
            "order_book_below_25_net",
            "order_book_concentration",
            "position_book_near_5_imbalance",
            "position_book_near_10_imbalance",
            "position_book_near_25_imbalance",
            "position_book_near_50_imbalance",
            "position_book_above_25_net",
            "position_book_below_25_net",
            "position_book_concentration",
        ],
    },
    "calendar_macro": {
        "description": "Optional lagged context layer; never leak future news or use unvalidated live text as a trigger.",
        "features": [
            "session_id",
            "hour_of_week",
            "days_to_month_end",
            "high_impact_news_window_flag",
            "central_bank_window_flag",
            "macro_bias_score_lagged",
            "usd_strength_proxy",
            "risk_sentiment_proxy",
        ],
    },
    "currency_strength": {
        "description": "Synthetic single-currency basket factors extracted from pair returns.",
        "features": [
            "usd_strength_ret_5",
            "eur_strength_ret_5",
            "gbp_strength_ret_5",
            "jpy_strength_ret_5",
            "chf_strength_ret_5",
            "aud_strength_ret_5",
            "nzd_strength_ret_5",
            "cad_strength_ret_5",
            "base_currency_strength",
            "quote_currency_strength",
            "base_minus_quote_strength",
            "currency_strength_rank",
            "currency_strength_dispersion",
            "currency_strength_reversal_z",
        ],
    },
    "jump_shock": {
        "description": "Abrupt return, gap, and post-shock decay features.",
        "features": [
            "jump_z",
            "gap_pips",
            "ret_abs_z_30",
            "ret_abs_z_120",
            "bipower_jump_30",
            "bipower_jump_120",
            "shock_flag",
            "shock_direction",
            "shock_decay_5",
            "shock_decay_15",
            "shock_decay_30",
            "post_shock_reversal_score",
        ],
    },
    "distribution_shape": {
        "description": "Rolling return distribution shape used to identify unstable forecast regimes.",
        "features": [
            "ret_skew_30",
            "ret_skew_60",
            "ret_skew_120",
            "ret_kurt_30",
            "ret_kurt_60",
            "ret_kurt_120",
            "downside_tail_ratio_60",
            "upside_tail_ratio_60",
            "tail_asymmetry_120",
        ],
    },
    "candle_pressure": {
        "description": "Close-location, wick imbalance, and candle pressure features.",
        "features": [
            "close_location_value",
            "wick_imbalance",
            "body_direction",
            "body_direction_persistence_5",
            "body_direction_persistence_20",
            "range_breakout_flag",
            "range_breakout_distance_pips",
            "close_above_recent_high_flag",
            "close_below_recent_low_flag",
            "candle_pressure_score",
        ],
    },
    "multi_timeframe_alignment": {
        "description": "Higher-timeframe trend, volatility, basket, and ARIMA agreement joined onto M1 rows.",
        "features": [
            "m5_trend_score",
            "m15_trend_score",
            "h1_trend_score",
            "m5_vol_regime",
            "m15_vol_regime",
            "h1_vol_regime",
            "m5_basket_beta",
            "m15_basket_beta",
            "h1_basket_beta",
            "m5_arima_direction",
            "m15_arima_direction",
            "h1_arima_direction",
            "mtf_direction_agreement",
            "mtf_volatility_agreement",
            "mtf_conflict_score",
        ],
    },
    "spread_persistence": {
        "description": "Duration and recovery behavior for live spread widening.",
        "features": [
            "spread_widening_streak",
            "spread_above_p90_duration",
            "spread_above_p95_duration",
            "spread_recovery_rate_5",
            "spread_recovery_rate_15",
            "spread_recovery_rate_30",
            "spread_persistence_score",
            "spread_veto_duration_remaining",
        ],
    },
    "session_transition": {
        "description": "Distance to/from session opens, closes, rollover, and weekend risk windows.",
        "features": [
            "minutes_to_london_open",
            "minutes_since_london_open",
            "minutes_to_ny_open",
            "minutes_since_ny_open",
            "minutes_to_london_close",
            "minutes_to_ny_close",
            "is_rollover_30m",
            "is_london_open_window",
            "is_ny_open_window",
            "is_london_close_window",
            "is_ny_close_window",
            "is_friday_close_window",
            "is_monday_reopen_window",
        ],
    },
    "target_path_metadata": {
        "description": "Forward-safe label metadata stored beside training rows, never used as live features.",
        "features": [
            "target_horizon_minutes",
            "future_mfe_pips",
            "future_mae_pips",
            "time_to_mfe_minutes",
            "time_to_mae_minutes",
            "time_to_tp_minutes",
            "time_to_sl_minutes",
            "future_net_after_spread",
            "future_net_after_commission",
            "triple_barrier_outcome",
            "label_generation_version",
        ],
    },
    "feature_audit": {
        "description": "Feature provenance and leakage-control metadata for generated datasets.",
        "features": [
            "feature_source_columns",
            "feature_lookback_bars",
            "feature_max_lookback_minutes",
            "feature_timeframe",
            "feature_is_cross_sectional",
            "feature_is_pca",
            "feature_is_forward_label",
            "feature_join_lag_minutes",
            "feature_generated_utc",
            "feature_schema_version",
        ],
    },
}

MODEL_LAYERS = [
    {
        "id": "forecast_arima_sarimax",
        "layer": "forecast_engine",
        "terms": [
            "ARIMA",
            "SARIMAX",
            "M5_ARIMA",
            "M15_ARIMA",
            "H1_ARIMA",
            "ARIMA_PRE",
            "ARIMA_POST",
        ],
        "feature_blocks": [
            "arima_forecast",
            "sarimax_exogenous",
            "currency_strength",
            "multi_timeframe_alignment",
        ],
        "outputs": [
            "forecast_return",
            "forecast_direction",
            "forecast_confidence",
            "residual_z",
            "pre_post_delta",
        ],
        "primary_validation": "walk_forward_net_pips_after_costs",
    },
    {
        "id": "event_gated_forecast",
        "layer": "signal_filter",
        "terms": [
            "Event-Gated ARIMA",
            "EV_091",
            "EV_121",
            "EV_212",
            "EV_015",
            "M5_EV_091",
            "EV_091_H1",
            "Edge-Gated ARIMA",
        ],
        "feature_blocks": [
            "arima_forecast",
            "event_gate",
            "atr_adr_volatility",
            "currency_strength",
            "jump_shock",
            "multi_timeframe_alignment",
            "spread_persistence",
            "session_transition",
        ],
        "outputs": ["gated_signal", "gate_margin", "event_score"],
        "primary_validation": "precision_recall_plus_net_pips_by_event_gate",
    },
    {
        "id": "volatility_range_models",
        "layer": "risk_context",
        "terms": [
            "ATR Model",
            "ADR Model",
            "ATR_032",
            "M15_ATR_032",
            "Volatility Regime Model",
        ],
        "feature_blocks": [
            "atr_adr_volatility",
            "distribution_shape",
            "jump_shock",
            "regime_state",
        ],
        "outputs": ["volatility_forecast", "stop_distance", "position_size_multiplier"],
        "primary_validation": "drawdown_reduction_without_edge_destruction",
    },
    {
        "id": "movement_path_models",
        "layer": "path_quality",
        "terms": [
            "Cumulative Pip Movement Model",
            "Trend-Continuation Model",
            "Range-Failure Model",
            "Mean-Reversion Failure Model",
            "Forecast Flood Model",
            "Top-N Cluster Selection Model",
        ],
        "feature_blocks": [
            "cumulative_pip_movement",
            "candle_pressure",
            "currency_strength",
            "diagnostics",
            "multi_timeframe_alignment",
            "regime_state",
        ],
        "outputs": ["continuation_score", "failure_score", "cluster_rank", "flood_score"],
        "primary_validation": "mfe_mae_endpoint_retention_and_net_pips",
    },
    {
        "id": "execution_policy_models",
        "layer": "execution_policy",
        "terms": [
            "Reverse-on-Flip ARIMA",
            "No-Reverse ARIMA",
            "Minimum-Hold ARIMA",
            "TP/SL ARIMA",
            "Trailing-Stop ARIMA",
            "Time-Stop ARIMA",
            "OANDA Live Mirror Model",
            "Grace Period / Cooldown Model",
        ],
        "feature_blocks": [
            "execution_policy",
            "microstructure_liquidity",
            "spread_persistence",
            "session_transition",
        ],
        "outputs": ["entry_allowed", "exit_action", "reverse_allowed", "cooldown_state"],
        "primary_validation": "same_signal_different_execution_ablation",
    },
    {
        "id": "risk_portfolio_models",
        "layer": "portfolio_allocator",
        "terms": [
            "Risk-Adjusted P&L Model",
            "Throttle / Split Capital Model",
            "Equity-Preserving Model",
            "Drawdown-Controlled Model",
            "Exposure-Capped Model",
            "Equity-Gated Meta Model",
            "Exposure-Cap Meta Model",
        ],
        "feature_blocks": [
            "risk_and_portfolio",
            "diagnostics",
            "spread_persistence",
            "session_transition",
        ],
        "outputs": ["risk_multiplier", "capital_bucket", "exposure_cap", "trade_veto"],
        "primary_validation": "portfolio_sharpe_max_drawdown_margin_and_tail_loss",
    },
    {
        "id": "lifecycle_health_models",
        "layer": "lifecycle_controller",
        "terms": [
            "ACTIVE Model",
            "THROTTLE Model",
            "REHAB Model",
            "HARD_KILL Model",
            "Lifecycle Controller",
            "Throttle Controller",
            "Rehab Controller",
            "Kill-Switch Controller",
            "Performance Ranking Model",
            "Model Health Scoring Model",
        ],
        "feature_blocks": [
            "lifecycle_controller",
            "risk_and_portfolio",
            "diagnostics",
            "feature_audit",
            "target_path_metadata",
        ],
        "outputs": ["lifecycle_state", "promotion_score", "demotion_score", "kill_switch"],
        "primary_validation": "shadow_to_live_stability_and_loss_containment",
    },
    {
        "id": "state_space_regime_models",
        "layer": "state_estimator",
        "terms": [
            "Kalman Filter",
            "Adaptive Kalman Filter",
            "State-Space Model",
            "Linear State-Space Model",
            "Regime-Switching State-Space Model",
            "Hidden Markov Model",
            "Markov Regime Filter",
            "Trend Regime Classifier",
            "Regime-Switching Controller Model",
        ],
        "feature_blocks": [
            "kalman_state_space",
            "jump_shock",
            "regime_state",
        ],
        "outputs": ["smoothed_trend", "regime_probability", "dynamic_residual_z"],
        "primary_validation": "regime_conditioned_edge_and_transition_stability",
    },
    {
        "id": "hybrid_ensemble_models",
        "layer": "meta_model",
        "terms": [
            "ARIMA + Kalman Hybrid",
            "ARIMA + State-Space Controller",
            "ARIMA + Regime Filter",
            "ARIMA + Volatility Filter",
            "ARIMA Meta-Model",
            "ARIMA Ensemble",
            "Voting ARIMA Ensemble",
            "Confidence-Weighted ARIMA Ensemble",
            "Multi-Timeframe Consensus Model",
        ],
        "feature_blocks": [
            "arima_forecast",
            "currency_strength",
            "jump_shock",
            "kalman_state_space",
            "multi_timeframe_alignment",
            "regime_state",
            "atr_adr_volatility",
            "distribution_shape",
            "diagnostics",
        ],
        "outputs": ["ensemble_vote", "confidence_weighted_signal", "consensus_score"],
        "primary_validation": "ensemble_beats_best_member_after_costs",
    },
    {
        "id": "offline_diagnostics",
        "layer": "research_diagnostic",
        "terms": [
            "Offline ARIMA Backtest Model",
            "PRE vs POST Comparative Model",
            "Churn-vs-Profit Diagnostic Model",
            "Regime Performance Model",
            "Reverse Edge Model",
            "Churn Model",
            "Churn-Reduced Model",
        ],
        "feature_blocks": [
            "diagnostics",
            "lifecycle_controller",
            "target_path_metadata",
            "feature_audit",
        ],
        "outputs": ["diagnostic_score", "failure_mode", "recommended_state"],
        "primary_validation": "explains_failures_without_creating_live_orders",
    },
    {
        "id": "dummy_paper_randomized_search",
        "layer": "paper_research",
        "terms": [
            "Dummy / Paper Account Model",
            "Randomized Parameter Sweep Model",
        ],
        "feature_blocks": [
            "execution_policy",
            "risk_and_portfolio",
            "diagnostics",
            "target_path_metadata",
            "feature_audit",
        ],
        "outputs": ["paper_trade_result", "parameter_sensitivity", "robustness_rank"],
        "primary_validation": "paper_only_shadow_until_multiple_independent_passes",
    },
]

VALUABLE_ADDITIONS = [
    {
        "id": "garch_egarch_volatility",
        "why": "Forecasts conditional volatility better than ATR in clustered-volatility regimes.",
        "feature_blocks": ["atr_adr_volatility"],
        "outputs": ["garch_vol_forecast", "egarch_leverage_effect"],
        "first_use": "risk sizing and stop distance, not direction.",
    },
    {
        "id": "var_vecm_cointegration",
        "why": "Captures cross-pair relationships that single-pair ARIMA misses.",
        "feature_blocks": ["kalman_state_space", "regime_state"],
        "outputs": ["cointegration_residual_z", "basket_reversion_score"],
        "first_use": "EUR/GBP/JPY/USD cluster baselines.",
    },
    {
        "id": "bayesian_online_changepoint",
        "why": "Detects abrupt regime shifts and protects stale forecasts.",
        "feature_blocks": ["regime_state", "diagnostics"],
        "outputs": ["changepoint_probability", "post_changepoint_age"],
        "first_use": "throttle and hard-kill controller input.",
    },
    {
        "id": "quantile_forecast_model",
        "why": "Predicts distribution tails, not just mean direction.",
        "feature_blocks": ["risk_and_portfolio", "atr_adr_volatility"],
        "outputs": ["p10_return", "p50_return", "p90_return", "expected_shortfall_proxy"],
        "first_use": "TP/SL sizing and drawdown control.",
    },
    {
        "id": "survival_time_to_target",
        "why": "Models how long a trade usually takes to hit target, stop, or stagnation.",
        "feature_blocks": ["execution_policy", "cumulative_pip_movement"],
        "outputs": ["prob_hit_tp_by_t", "prob_hit_sl_by_t", "time_stop_score"],
        "first_use": "minimum-hold and time-stop ARIMA policies.",
    },
    {
        "id": "meta_labeling_triple_barrier",
        "why": "Separates signal direction from whether the trade is worth taking.",
        "feature_blocks": ["diagnostics", "risk_and_portfolio"],
        "outputs": ["take_trade_probability", "triple_barrier_label"],
        "first_use": "edge gate on top of ARIMA forecasts.",
    },
    {
        "id": "contextual_bandit_allocator",
        "why": "Learns which validated model to allocate to under current regime while capped by risk.",
        "feature_blocks": ["lifecycle_controller", "risk_and_portfolio", "regime_state"],
        "outputs": ["model_allocation_weight", "exploration_budget"],
        "first_use": "paper-only allocation after shadow candidates are stable.",
    },
    {
        "id": "microstructure_spread_model",
        "why": "Avoids signals that look good on mid candles but fail after real spread/slippage.",
        "feature_blocks": ["microstructure_liquidity"],
        "outputs": ["expected_spread_pips", "expected_slippage_pips", "liquidity_veto"],
        "first_use": "isolated OANDA execution-cost diagnostics.",
    },
    {
        "id": "currency_strength_basket_model",
        "why": "FX pairs are two-currency spreads; synthetic currency legs can expose the true driver behind a pair move.",
        "feature_blocks": ["currency_strength"],
        "outputs": ["base_minus_quote_strength", "currency_strength_rank", "strength_reversal_score"],
        "first_use": "ARIMA/event-gate confirmation and cross-pair cluster ranking.",
    },
    {
        "id": "jump_shock_decay_model",
        "why": "Separates tradable impulse continuation from one-bar shocks that mean-revert or create spread traps.",
        "feature_blocks": ["jump_shock", "spread_persistence"],
        "outputs": ["shock_decay_score", "post_shock_reversal_score", "shock_trade_veto"],
        "first_use": "event-gated ARIMA and churn reduction.",
    },
    {
        "id": "multi_timeframe_alignment_model",
        "why": "Most false M1 signals occur when higher timeframes disagree or volatility states are misaligned.",
        "feature_blocks": ["multi_timeframe_alignment"],
        "outputs": ["mtf_direction_agreement", "mtf_conflict_score", "mtf_volatility_agreement"],
        "first_use": "M5/M15/H1 ARIMA consensus and signal throttling.",
    },
    {
        "id": "feature_audit_manifest",
        "why": "A wide feature space is only useful if each column has provenance, max lookback, and leakage status.",
        "feature_blocks": ["feature_audit", "target_path_metadata"],
        "outputs": ["feature_manifest", "max_lookback_minutes", "forward_label_inventory"],
        "first_use": "dataset validation before queueing model families.",
    },
]

PRIORITY_EXPERIMENTS = [
    {
        "id": "P0_event_gated_arima_shadow",
        "goal": "Turn current ARIMA pair challengers into gated shadow signals.",
        "models": ["ARIMA", "Event-Gated ARIMA", "ATR Model"],
        "orders": ARIMA_ORDERS,
        "timeframes": ["M5", "M15", "H1"],
        "event_gates": ["EV_015", "EV_091", "EV_121", "EV_212"],
        "execution_policies": ["no_reverse", "minimum_hold", "time_stop"],
        "promotion_gate": "two independent shadow passes plus nonzero adapter signal generation",
    },
    {
        "id": "P1_arima_kalman_regime_filter",
        "goal": "Reduce ARIMA churn by requiring agreement with smoothed state and regime filter.",
        "models": ["ARIMA + Kalman Hybrid", "ARIMA + Regime Filter", "Hidden Markov Model"],
        "timeframes": ["M15", "H1"],
        "outputs": ["confidence_weighted_signal", "regime_probability", "churn_guard_active"],
        "promotion_gate": "beats plain ARIMA on churn, drawdown, and net pips",
    },
    {
        "id": "P2_execution_policy_ablation",
        "goal": "Find whether reverse-on-flip, no-reverse, trailing stop, TP/SL, or time stop is best per pair.",
        "models": [
            "Reverse-on-Flip ARIMA",
            "No-Reverse ARIMA",
            "TP/SL ARIMA",
            "Trailing-Stop ARIMA",
            "Time-Stop ARIMA",
        ],
        "timeframes": ["M5", "M15", "H1"],
        "outputs": ["same_signal_policy_delta_pips", "profit_per_flip", "churn_rate"],
        "promotion_gate": "execution variant improves active forecast without increasing tail loss",
    },
    {
        "id": "P3_lifecycle_controller",
        "goal": "Map model health into ACTIVE, THROTTLE, REHAB, or HARD_KILL state.",
        "models": [
            "Lifecycle Controller",
            "Throttle Controller",
            "Rehab Controller",
            "Kill-Switch Controller",
        ],
        "inputs": [
            "model_health_score",
            "drawdown",
            "live_vs_shadow_delta_pips",
            "churn_rate",
            "regime_performance_score",
        ],
        "promotion_gate": "loss containment improves without suppressing high-quality periods",
    },
    {
        "id": "P4_oanda_execution_diagnostic",
        "goal": "Compare signal quality before and after realistic execution costs.",
        "models": ["OANDA Live Mirror Model", "Microstructure Spread Model"],
        "outputs": ["spread_adjusted_pnl", "slippage_proxy_pips", "mirror_delta_pips"],
        "promotion_gate": "live mirror preserves at least 70 percent of shadow edge",
    },
    {
        "id": "P5_currency_strength_mtf_consensus",
        "goal": "Test whether currency-strength baskets plus M5/M15/H1 agreement improve ARIMA and event-gate precision.",
        "models": [
            "Multi-Timeframe Consensus Model",
            "ARIMA + Regime Filter",
            "Event-Gated ARIMA",
            "Currency Strength Basket Model",
        ],
        "feature_blocks": [
            "currency_strength",
            "multi_timeframe_alignment",
            "event_gate",
            "arima_forecast",
        ],
        "outputs": [
            "base_minus_quote_strength",
            "mtf_direction_agreement",
            "event_gate_passed",
            "forecast_agreement_ratio",
        ],
        "promotion_gate": "improves precision and net pips versus same ARIMA gates without MTF/currency-strength filters",
    },
    {
        "id": "P6_jump_spread_session_veto",
        "goal": "Determine which shock, persistent spread, and session-transition states should veto or throttle signals.",
        "models": [
            "Jump Shock Decay Model",
            "Grace Period / Cooldown Model",
            "OANDA Live Mirror Model",
        ],
        "feature_blocks": [
            "jump_shock",
            "spread_persistence",
            "session_transition",
            "microstructure_liquidity",
        ],
        "outputs": [
            "shock_trade_veto",
            "spread_veto_duration_remaining",
            "cooldown_state",
            "mirror_delta_pips",
        ],
        "promotion_gate": "reduces churn and live/slippage losses without materially reducing validated edge",
    },
    {
        "id": "P7_feature_audit_and_target_metadata",
        "goal": "Guarantee wide-feature datasets remain leakage-safe and compatible with TP/SL, time-stop, and triple-barrier labels.",
        "models": [
            "Offline ARIMA Backtest Model",
            "PRE vs POST Comparative Model",
            "Model Health Scoring Model",
        ],
        "feature_blocks": ["feature_audit", "target_path_metadata"],
        "outputs": [
            "feature_manifest",
            "feature_max_lookback_minutes",
            "future_mfe_pips",
            "future_mae_pips",
            "triple_barrier_outcome",
        ],
        "promotion_gate": "all training rows have deterministic max-lookback and forward-label metadata before model comparison",
    },
]


def feature_counts() -> dict[str, int]:
    return {
        "user_model_terms": len(USER_MODEL_TERMS),
        "model_layers": len(MODEL_LAYERS),
        "feature_blocks": len(FEATURE_BLOCKS),
        "feature_columns": sum(len(block["features"]) for block in FEATURE_BLOCKS.values()),
        "valuable_additions": len(VALUABLE_ADDITIONS),
        "priority_experiments": len(PRIORITY_EXPERIMENTS),
    }


def build_feature_space() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "purpose": (
            "Feature-space map for ARIMA/SARIMAX, event-gated forecasts, "
            "volatility/range models, execution policies, portfolio controls, "
            "state-space/regime models, and lifecycle controllers."
        ),
        "integration_policy": {
            "structural_forecast": (
                "Use one M1 decision row with completed M1, M30, H1, and H4 "
                "context to forecast the next 1 through 60 minutes."
            ),
            "market_data_roles": (
                "OANDA candle volume is causal quote activity. Raw/S1 pricing "
                "and depth are execution timing inputs. OANDA client books stay "
                "research-only until their prospective history passes quality "
                "and validation gates."
            ),
            "tabular_trainer": (
                "Use model outputs, residuals, gates, regimes, and diagnostics as lagged "
                "features or targets. Do not force execution controllers into the row "
                "classifier interface."
            ),
            "external_evaluators": (
                "ARIMA/SARIMAX, state-space, HMM, Kalman, GARCH, VECM, and execution "
                "policy ablations should have separate walk-forward reports, then feed "
                "validated outputs into promotion manifests."
            ),
            "live_safety": (
                "Live assignment requires shadow evidence, canary evidence, spread/cost "
                "checks, lifecycle state not HARD_KILL, and explicit execution gates."
            ),
        },
        "forecast_contract": contract_payload(),
        "axes": {
            "timeframes": TIMEFRAMES,
            "forecast_horizons_minutes": FORECAST_HORIZONS_MINUTES,
            "instrument_subsets": INSTRUMENT_SUBSETS,
            "arima_orders": ARIMA_ORDERS,
            "event_gates": EVENT_GATES,
            "lifecycle_states": ["ACTIVE", "THROTTLE", "REHAB", "HARD_KILL"],
            "execution_modes": [
                "reverse_on_flip",
                "no_reverse",
                "edge_gated",
                "minimum_hold",
                "tp_sl",
                "trailing_stop",
                "time_stop",
            ],
            "deployment_lanes": [
                "offline_backtest",
                "dummy_paper",
                "shadow",
                "canary",
                "oanda_live_mirror",
                "oanda_live",
            ],
        },
        "feature_blocks": FEATURE_BLOCKS,
        "model_layers": MODEL_LAYERS,
        "valuable_additions": VALUABLE_ADDITIONS,
        "priority_experiments": PRIORITY_EXPERIMENTS,
        "counts": feature_counts(),
        "generated_files": {
            "config": str(CONFIG_PATH),
            "latest": str(LATEST_PATH),
            "markdown": str(MARKDOWN_PATH),
        },
    }


def render_markdown(space: dict[str, Any]) -> str:
    counts = space["counts"]
    lines = [
        "# Model Feature Space",
        "",
        f"Generated UTC: {space['generated_utc']}",
        "",
        "This file summarizes the feature-space spec generated from the requested model list.",
        "The programmatic spec is in `trad/config/model_feature_space.json`.",
        "",
        "## Scope",
        "",
        f"- Model terms mapped: {counts['user_model_terms']}",
        f"- Model layers: {counts['model_layers']}",
        f"- Feature blocks: {counts['feature_blocks']}",
        f"- Feature columns: {counts['feature_columns']}",
        f"- Added high-value model families: {counts['valuable_additions']}",
        "",
        "## Layers",
        "",
    ]
    for layer in space["model_layers"]:
        lines.extend([
            f"### {layer['id']}",
            "",
            f"- Layer: {layer['layer']}",
            f"- Terms: {', '.join(layer['terms'])}",
            f"- Feature blocks: {', '.join(layer['feature_blocks'])}",
            f"- Outputs: {', '.join(layer['outputs'])}",
            f"- Validation: {layer['primary_validation']}",
            "",
        ])
    lines.extend(["## High-Value Additions", ""])
    for item in space["valuable_additions"]:
        lines.extend([
            f"### {item['id']}",
            "",
            f"- Why: {item['why']}",
            f"- First use: {item['first_use']}",
            f"- Outputs: {', '.join(item['outputs'])}",
            "",
        ])
    lines.extend(["## Priority Experiments", ""])
    for item in space["priority_experiments"]:
        lines.extend([
            f"### {item['id']}",
            "",
            f"- Goal: {item['goal']}",
            f"- Promotion gate: {item['promotion_gate']}",
            "",
        ])
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Build model feature-space spec")
    parser.add_argument("--dry-run", action="store_true", help="Print summary without writing files")
    args = parser.parse_args()

    space = build_feature_space()
    if not args.dry_run:
        atomic_write_json(CONFIG_PATH, space)
        atomic_write_json(LATEST_PATH, space)
        atomic_write_text(MARKDOWN_PATH, render_markdown(space))

    print(json.dumps({
        "dry_run": bool(args.dry_run),
        "counts": space["counts"],
        "config": str(CONFIG_PATH),
        "latest": str(LATEST_PATH),
        "markdown": str(MARKDOWN_PATH),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
