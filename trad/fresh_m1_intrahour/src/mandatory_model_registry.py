from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


MANDATORY_MODEL_NAMES = [
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
]

ARIMA_ORDERS = [
    (0, 1, 1),
    (1, 1, 0),
    (1, 1, 1),
    (2, 1, 1),
    (1, 1, 2),
    (0, 2, 1),
    (1, 2, 1),
    (1, 2, 2),
]

EVENT_THRESHOLDS = {
    "EV_015": 0.15,
    "EV_091": 0.91,
    "EV_121": 1.21,
    "EV_212": 2.12,
}


@dataclass(frozen=True)
class MandatoryModelSpec:
    model_name: str
    family: str
    category: str
    signal: str
    policy: str = "top1_endpoint"
    timeframe: str = "H1"
    parameters: dict[str, Any] = field(default_factory=dict)
    prediction_target: str = "next_bar_signed_return_pips"
    execution_mode: str = "offline_research"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _spec(
    name: str,
    family: str,
    category: str,
    signal: str,
    policy: str = "top1_endpoint",
    timeframe: str = "H1",
    **parameters: Any,
) -> MandatoryModelSpec:
    return MandatoryModelSpec(
        model_name=name,
        family=family,
        category=category,
        signal=signal,
        policy=policy,
        timeframe=timeframe,
        parameters=parameters,
    )


def build_mandatory_registry() -> dict[str, MandatoryModelSpec]:
    specs: dict[str, MandatoryModelSpec] = {}

    def add(spec: MandatoryModelSpec) -> None:
        if spec.model_name in specs:
            raise ValueError(f"duplicate mandatory model: {spec.model_name}")
        specs[spec.model_name] = spec

    add(_spec("ARIMA", "arima", "forecast", "arima_selected"))
    add(_spec("SARIMAX", "sarimax", "forecast", "sarimax_selected"))
    add(_spec("Event-Gated ARIMA", "event_gated_arima", "forecast_gate", "arima_selected", "event_gate", event_threshold=0.91))
    add(_spec("ATR Model", "atr", "movement_forecast", "atr_momentum"))
    add(_spec("ADR Model", "adr", "movement_forecast", "adr_momentum"))
    add(_spec("Cumulative Pip Movement Model", "cumulative_pip_movement", "forecast", "cumulative_pips"))
    add(_spec("Volatility Regime Model", "volatility_regime", "regime", "volatility_regime"))
    add(_spec("Dummy / Paper Account Model", "paper_account", "account_simulation", "arima_selected", "paper_account"))
    add(_spec("Risk-Adjusted P&L Model", "risk_adjusted_pnl", "sizing", "arima_selected", "volatility_scaled"))
    add(_spec("Grace Period / Cooldown Model", "cooldown", "controller", "arima_selected", "cooldown", cooldown_bars=3))
    add(_spec("Randomized Parameter Sweep Model", "randomized_sweep", "selector", "arima_selected", "randomized_validation_selector", seed=42))
    add(_spec("Top-N Cluster Selection Model", "top_n_cluster", "allocator", "arima_selected", "top_n_cluster", top_n=3))
    add(_spec("Throttle / Split Capital Model", "split_capital", "allocator", "arima_selected", "split_capital", top_n=3))
    add(_spec("OANDA Live Mirror Model", "oanda_mirror_simulation", "execution_simulation", "arima_selected", "oanda_mirror_simulation"))
    add(_spec("Forecast Flood Model", "forecast_flood", "allocator", "confidence_weighted_ensemble", "forecast_flood", top_n=5))
    add(_spec("Performance Ranking Model", "performance_ranking", "selector", "performance_ranked", "top1_endpoint"))
    add(_spec("Multi-Timeframe Consensus Model", "multi_timeframe_consensus", "ensemble", "multi_timeframe_consensus", "agreement_gate"))
    add(_spec("Regime-Switching Controller Model", "regime_switching_controller", "controller", "regime_switching_state", "regime_controller"))

    for order in ARIMA_ORDERS:
        order_text = ",".join(str(value) for value in order)
        add(_spec(f"ARIMA({order_text})", "arima", "forecast", f"arima_{order[0]}{order[1]}{order[2]}", order=list(order)))

    for event_name, threshold in EVENT_THRESHOLDS.items():
        add(_spec(event_name, "event_gated_arima", "forecast_gate", "arima_122", "event_gate", event_threshold=threshold))

    add(_spec("M5_ARIMA", "timeframe_arima", "forecast", "m5_arima", timeframe="M5"))
    add(_spec("M15_ARIMA", "timeframe_arima", "forecast", "m15_arima", timeframe="M15"))
    add(_spec("H1_ARIMA", "timeframe_arima", "forecast", "arima_selected", timeframe="H1"))
    add(_spec("M5_EV_091", "event_gated_arima", "forecast_gate", "m5_arima", "event_gate", timeframe="M5", event_threshold=0.91))
    add(_spec("M15_ATR_032", "atr_arima", "forecast_gate", "m15_atr_032", "atr_gate", timeframe="M15", atr_multiplier=0.8))
    add(_spec("EV_091_H1", "event_gated_arima", "forecast_gate", "arima_122", "event_gate", timeframe="H1", event_threshold=0.91))
    add(_spec("ATR_032", "atr_arima", "forecast_gate", "atr_032", "atr_gate", atr_multiplier=0.7))

    add(_spec("Reverse-on-Flip ARIMA", "arima_policy", "trade_policy", "arima_selected", "reverse_on_flip"))
    add(_spec("No-Reverse ARIMA", "arima_policy", "trade_policy", "arima_selected", "no_reverse"))
    add(_spec("Edge-Gated ARIMA", "arima_policy", "trade_policy", "arima_selected", "edge_gate"))
    add(_spec("Minimum-Hold ARIMA", "arima_policy", "trade_policy", "arima_selected", "minimum_hold", minimum_hold_bars=3))
    add(_spec("TP/SL ARIMA", "arima_policy", "exit_policy", "arima_selected", "tp_sl", tp_pips=8.0, sl_pips=12.0))
    add(_spec("Trailing-Stop ARIMA", "arima_policy", "exit_policy", "arima_selected", "trailing_stop", trailing_pips=8.0))
    add(_spec("Time-Stop ARIMA", "arima_policy", "exit_policy", "arima_selected", "time_stop", time_stop_minutes=60))
    add(_spec("ARIMA_PRE", "arima_accounting", "diagnostic", "arima_selected", "gross_pre_cost"))
    add(_spec("ARIMA_POST", "arima_accounting", "diagnostic", "arima_selected", "top1_endpoint"))

    add(_spec("ACTIVE Model", "lifecycle_state", "controller", "arima_selected", "lifecycle_active"))
    add(_spec("THROTTLE Model", "lifecycle_state", "controller", "arima_selected", "lifecycle_throttle"))
    add(_spec("REHAB Model", "lifecycle_state", "controller", "arima_selected", "lifecycle_rehab"))
    add(_spec("HARD_KILL Model", "lifecycle_state", "controller", "arima_selected", "lifecycle_hard_kill"))
    add(_spec("Reverse Edge Model", "failure_mode", "diagnostic", "arima_selected", "reverse_edge"))
    add(_spec("Churn Model", "churn", "diagnostic", "arima_selected", "churn_unrestricted"))
    add(_spec("Churn-Reduced Model", "churn", "controller", "arima_selected", "cooldown", cooldown_bars=4))
    add(_spec("Trend-Continuation Model", "trend_continuation", "forecast_gate", "arima_selected", "trend_gate"))
    add(_spec("Range-Failure Model", "range_failure", "forecast_gate", "arima_selected", "range_failure"))
    add(_spec("Mean-Reversion Failure Model", "mean_reversion_failure", "diagnostic", "cumulative_pips", "reverse_edge"))
    add(_spec("Equity-Preserving Model", "equity_preserving", "controller", "arima_selected", "equity_preserving"))
    add(_spec("Drawdown-Controlled Model", "drawdown_controlled", "controller", "arima_selected", "drawdown_controlled"))
    add(_spec("Exposure-Capped Model", "exposure_capped", "allocator", "arima_selected", "exposure_cap", max_currency_exposure=1))
    add(_spec("Equity-Gated Meta Model", "equity_gated_meta", "meta_controller", "arima_meta", "equity_gate"))
    add(_spec("Exposure-Cap Meta Model", "exposure_cap_meta", "meta_controller", "arima_meta", "exposure_cap", max_currency_exposure=1))
    add(_spec("Lifecycle Controller", "lifecycle_controller", "meta_controller", "model_health", "lifecycle_dynamic"))
    add(_spec("Throttle Controller", "lifecycle_controller", "meta_controller", "model_health", "lifecycle_throttle"))
    add(_spec("Rehab Controller", "lifecycle_controller", "meta_controller", "model_health", "lifecycle_rehab"))
    add(_spec("Kill-Switch Controller", "lifecycle_controller", "meta_controller", "model_health", "lifecycle_hard_kill"))

    add(_spec("Kalman Filter", "kalman", "state_space", "kalman"))
    add(_spec("Adaptive Kalman Filter", "kalman", "state_space", "adaptive_kalman"))
    add(_spec("State-Space Model", "state_space", "state_space", "state_space"))
    add(_spec("Linear State-Space Model", "state_space", "state_space", "linear_state_space"))
    add(_spec("Regime-Switching State-Space Model", "state_space", "state_space", "regime_switching_state"))
    add(_spec("Hidden Markov Model", "hidden_markov", "regime", "hidden_markov"))
    add(_spec("Markov Regime Filter", "markov_regime", "regime", "markov_regime"))
    add(_spec("Trend Regime Classifier", "trend_regime_classifier", "regime", "trend_classifier"))

    add(_spec("ARIMA + Kalman Hybrid", "arima_hybrid", "hybrid", "arima_kalman"))
    add(_spec("ARIMA + State-Space Controller", "arima_hybrid", "hybrid", "arima_state_space", "agreement_gate"))
    add(_spec("ARIMA + Regime Filter", "arima_hybrid", "hybrid", "arima_selected", "regime_gate"))
    add(_spec("ARIMA + Volatility Filter", "arima_hybrid", "hybrid", "arima_selected", "volatility_gate"))
    add(_spec("ARIMA Meta-Model", "arima_meta", "meta_model", "arima_meta"))
    add(_spec("ARIMA Ensemble", "arima_ensemble", "ensemble", "arima_ensemble"))
    add(_spec("Voting ARIMA Ensemble", "arima_ensemble", "ensemble", "voting_arima"))
    add(_spec("Confidence-Weighted ARIMA Ensemble", "arima_ensemble", "ensemble", "confidence_weighted_ensemble"))

    add(_spec("Offline ARIMA Backtest Model", "arima_diagnostic", "diagnostic", "arima_selected", "top1_endpoint"))
    add(_spec("PRE vs POST Comparative Model", "arima_diagnostic", "diagnostic", "arima_selected", "pre_post_comparison"))
    add(_spec("Churn-vs-Profit Diagnostic Model", "arima_diagnostic", "diagnostic", "arima_selected", "churn_profit_diagnostic"))
    add(_spec("Model Health Scoring Model", "model_health", "diagnostic", "model_health", "health_score"))
    add(_spec("Regime Performance Model", "regime_performance", "diagnostic", "arima_selected", "regime_breakdown"))

    missing = [name for name in MANDATORY_MODEL_NAMES if name not in specs]
    extra = [name for name in specs if name not in MANDATORY_MODEL_NAMES]
    if missing or extra:
        raise AssertionError(f"mandatory registry mismatch missing={missing} extra={extra}")
    return specs


def registry_payload() -> list[dict[str, Any]]:
    return [spec.to_dict() for spec in build_mandatory_registry().values()]
