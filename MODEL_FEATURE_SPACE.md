# Model Feature Space

## Current operational reconciliation — 2026-08-27

This document is the preserved 2026-08-03 design inventory. Its 251 columns and
95 mapped terms describe possible contracts; they do **not** all represent live,
populated, or account-eligible information.

Current distinctions:

- OANDA executable quotes/candles, spread/cost state, official-source mapping,
  narrative meter v12, CFTC positioning, governed proof cohorts, and the clean
  causal level-band cohort are current inputs or research collectors.
- Order-book, position-book, pricing-depth, and microprice contracts remain
  unavailable/inert. A 14,888,709-forecast census found no causally populated
  book observations or informative depth.
- Thirty declared model-gap contributors have zero fresh live outputs and are
  dormant; they are not counted as active predictive breadth.
- Static signal-combination rule artifacts older than 24 hours now fail closed
  in the strategy lab. Their historical database remains evidence, but stale
  `account_eligible` flags cannot become active signals.
- The exhausted H1 baseline and executable-opportunity ranker produce no new
  rows and only mature already-issued outcomes.
- The current source-response contract is V2 and freezes exact
  1/5/10/15/30/60/120-minute currency-factor responses. Its preactivation rows
  are diagnostic only; post-activation rows retain source/factor/episode,
  knowledge cutoff, executable entry path, MFE/MAE, and cost-clearance lineage.
- `source_conditioned_currency_rank_v1` is a research comparison layer, not an
  execution allocator. It evaluates price-only, source-only, and
  source-plus-price/spread timing arms with post-source executable quotes and
  immutable cohort-separated outcomes.
- `news_band_resolution_flow_h15_v1` adds a bounded timing feature contract:
  known news direction x prior frozen band x distinct completed contact and
  break/rejection minutes x completed 30/120-second quote-change imbalance.
  It compares fixed H15 and band-invalidation outcomes against matched no-flow
  controls and preserves exact upstream source cohort/contract lists.
- Retired executable-opportunity magnitude forecasts are no longer a live
  watchlist feature. Quote intensity remains explicitly a signed executable-
  quote-change proxy, not trade/order flow.
- All current execution remains gated by lifecycle confirmation and exact
  canary authorization; zero candidates are confirmed.

The current machine-readable runtime disposition is
`config/shadow_runtime_retirements_v1.json`. The generated
`config/model_feature_space.json` remains dated provenance and should not be
read as a live-availability report.

Generated UTC: 2026-08-03T20:29:37.807705+00:00

This file summarizes the feature-space spec generated from the requested model list.
The programmatic spec is in `trad/config/model_feature_space.json`.

## Scope

- Model terms mapped: 95
- Model layers: 11
- Feature blocks: 22
- Feature columns: 251
- Added high-value model families: 12

## Layers

### forecast_arima_sarimax

- Layer: forecast_engine
- Terms: ARIMA, SARIMAX, M5_ARIMA, M15_ARIMA, H1_ARIMA, ARIMA_PRE, ARIMA_POST
- Feature blocks: arima_forecast, sarimax_exogenous, currency_strength, multi_timeframe_alignment
- Outputs: forecast_return, forecast_direction, forecast_confidence, residual_z, pre_post_delta
- Validation: walk_forward_net_pips_after_costs

### event_gated_forecast

- Layer: signal_filter
- Terms: Event-Gated ARIMA, EV_091, EV_121, EV_212, EV_015, M5_EV_091, EV_091_H1, Edge-Gated ARIMA
- Feature blocks: arima_forecast, event_gate, atr_adr_volatility, currency_strength, jump_shock, multi_timeframe_alignment, spread_persistence, session_transition
- Outputs: gated_signal, gate_margin, event_score
- Validation: precision_recall_plus_net_pips_by_event_gate

### volatility_range_models

- Layer: risk_context
- Terms: ATR Model, ADR Model, ATR_032, M15_ATR_032, Volatility Regime Model
- Feature blocks: atr_adr_volatility, distribution_shape, jump_shock, regime_state
- Outputs: volatility_forecast, stop_distance, position_size_multiplier
- Validation: drawdown_reduction_without_edge_destruction

### movement_path_models

- Layer: path_quality
- Terms: Cumulative Pip Movement Model, Trend-Continuation Model, Range-Failure Model, Mean-Reversion Failure Model, Forecast Flood Model, Top-N Cluster Selection Model
- Feature blocks: cumulative_pip_movement, candle_pressure, currency_strength, diagnostics, multi_timeframe_alignment, regime_state
- Outputs: continuation_score, failure_score, cluster_rank, flood_score
- Validation: mfe_mae_endpoint_retention_and_net_pips

### execution_policy_models

- Layer: execution_policy
- Terms: Reverse-on-Flip ARIMA, No-Reverse ARIMA, Minimum-Hold ARIMA, TP/SL ARIMA, Trailing-Stop ARIMA, Time-Stop ARIMA, OANDA Live Mirror Model, Grace Period / Cooldown Model
- Feature blocks: execution_policy, microstructure_liquidity, spread_persistence, session_transition
- Outputs: entry_allowed, exit_action, reverse_allowed, cooldown_state
- Validation: same_signal_different_execution_ablation

### risk_portfolio_models

- Layer: portfolio_allocator
- Terms: Risk-Adjusted P&L Model, Throttle / Split Capital Model, Equity-Preserving Model, Drawdown-Controlled Model, Exposure-Capped Model, Equity-Gated Meta Model, Exposure-Cap Meta Model
- Feature blocks: risk_and_portfolio, diagnostics, spread_persistence, session_transition
- Outputs: risk_multiplier, capital_bucket, exposure_cap, trade_veto
- Validation: portfolio_sharpe_max_drawdown_margin_and_tail_loss

### lifecycle_health_models

- Layer: lifecycle_controller
- Terms: ACTIVE Model, THROTTLE Model, REHAB Model, HARD_KILL Model, Lifecycle Controller, Throttle Controller, Rehab Controller, Kill-Switch Controller, Performance Ranking Model, Model Health Scoring Model
- Feature blocks: lifecycle_controller, risk_and_portfolio, diagnostics, feature_audit, target_path_metadata
- Outputs: lifecycle_state, promotion_score, demotion_score, kill_switch
- Validation: shadow_to_live_stability_and_loss_containment

### state_space_regime_models

- Layer: state_estimator
- Terms: Kalman Filter, Adaptive Kalman Filter, State-Space Model, Linear State-Space Model, Regime-Switching State-Space Model, Hidden Markov Model, Markov Regime Filter, Trend Regime Classifier, Regime-Switching Controller Model
- Feature blocks: kalman_state_space, jump_shock, regime_state
- Outputs: smoothed_trend, regime_probability, dynamic_residual_z
- Validation: regime_conditioned_edge_and_transition_stability

### hybrid_ensemble_models

- Layer: meta_model
- Terms: ARIMA + Kalman Hybrid, ARIMA + State-Space Controller, ARIMA + Regime Filter, ARIMA + Volatility Filter, ARIMA Meta-Model, ARIMA Ensemble, Voting ARIMA Ensemble, Confidence-Weighted ARIMA Ensemble, Multi-Timeframe Consensus Model
- Feature blocks: arima_forecast, currency_strength, jump_shock, kalman_state_space, multi_timeframe_alignment, regime_state, atr_adr_volatility, distribution_shape, diagnostics
- Outputs: ensemble_vote, confidence_weighted_signal, consensus_score
- Validation: ensemble_beats_best_member_after_costs

### offline_diagnostics

- Layer: research_diagnostic
- Terms: Offline ARIMA Backtest Model, PRE vs POST Comparative Model, Churn-vs-Profit Diagnostic Model, Regime Performance Model, Reverse Edge Model, Churn Model, Churn-Reduced Model
- Feature blocks: diagnostics, lifecycle_controller, target_path_metadata, feature_audit
- Outputs: diagnostic_score, failure_mode, recommended_state
- Validation: explains_failures_without_creating_live_orders

### dummy_paper_randomized_search

- Layer: paper_research
- Terms: Dummy / Paper Account Model, Randomized Parameter Sweep Model
- Feature blocks: execution_policy, risk_and_portfolio, diagnostics, target_path_metadata, feature_audit
- Outputs: paper_trade_result, parameter_sensitivity, robustness_rank
- Validation: paper_only_shadow_until_multiple_independent_passes

## High-Value Additions

### garch_egarch_volatility

- Why: Forecasts conditional volatility better than ATR in clustered-volatility regimes.
- First use: risk sizing and stop distance, not direction.
- Outputs: garch_vol_forecast, egarch_leverage_effect

### var_vecm_cointegration

- Why: Captures cross-pair relationships that single-pair ARIMA misses.
- First use: EUR/GBP/JPY/USD cluster baselines.
- Outputs: cointegration_residual_z, basket_reversion_score

### bayesian_online_changepoint

- Why: Detects abrupt regime shifts and protects stale forecasts.
- First use: throttle and hard-kill controller input.
- Outputs: changepoint_probability, post_changepoint_age

### quantile_forecast_model

- Why: Predicts distribution tails, not just mean direction.
- First use: TP/SL sizing and drawdown control.
- Outputs: p10_return, p50_return, p90_return, expected_shortfall_proxy

### survival_time_to_target

- Why: Models how long a trade usually takes to hit target, stop, or stagnation.
- First use: minimum-hold and time-stop ARIMA policies.
- Outputs: prob_hit_tp_by_t, prob_hit_sl_by_t, time_stop_score

### meta_labeling_triple_barrier

- Why: Separates signal direction from whether the trade is worth taking.
- First use: edge gate on top of ARIMA forecasts.
- Outputs: take_trade_probability, triple_barrier_label

### contextual_bandit_allocator

- Why: Learns which validated model to allocate to under current regime while capped by risk.
- First use: paper-only allocation after shadow candidates are stable.
- Outputs: model_allocation_weight, exploration_budget

### microstructure_spread_model

- Why: Avoids signals that look good on mid candles but fail after real spread/slippage.
- First use: isolated OANDA execution-cost diagnostics.
- Outputs: expected_spread_pips, expected_slippage_pips, liquidity_veto

### currency_strength_basket_model

- Why: FX pairs are two-currency spreads; synthetic currency legs can expose the true driver behind a pair move.
- First use: ARIMA/event-gate confirmation and cross-pair cluster ranking.
- Outputs: base_minus_quote_strength, currency_strength_rank, strength_reversal_score

### jump_shock_decay_model

- Why: Separates tradable impulse continuation from one-bar shocks that mean-revert or create spread traps.
- First use: event-gated ARIMA and churn reduction.
- Outputs: shock_decay_score, post_shock_reversal_score, shock_trade_veto

### multi_timeframe_alignment_model

- Why: Most false M1 signals occur when higher timeframes disagree or volatility states are misaligned.
- First use: M5/M15/H1 ARIMA consensus and signal throttling.
- Outputs: mtf_direction_agreement, mtf_conflict_score, mtf_volatility_agreement

### feature_audit_manifest

- Why: A wide feature space is only useful if each column has provenance, max lookback, and leakage status.
- First use: dataset validation before queueing model families.
- Outputs: feature_manifest, max_lookback_minutes, forward_label_inventory

## Priority Experiments

### P0_event_gated_arima_shadow

- Goal: Turn current ARIMA pair challengers into gated shadow signals.
- Promotion gate: two independent shadow passes plus nonzero adapter signal generation

### P1_arima_kalman_regime_filter

- Goal: Reduce ARIMA churn by requiring agreement with smoothed state and regime filter.
- Promotion gate: beats plain ARIMA on churn, drawdown, and net pips

### P2_execution_policy_ablation

- Goal: Find whether reverse-on-flip, no-reverse, trailing stop, TP/SL, or time stop is best per pair.
- Promotion gate: execution variant improves active forecast without increasing tail loss

### P3_lifecycle_controller

- Goal: Map model health into ACTIVE, THROTTLE, REHAB, or HARD_KILL state.
- Promotion gate: loss containment improves without suppressing high-quality periods

### P4_oanda_execution_diagnostic

- Goal: Compare signal quality before and after realistic execution costs.
- Promotion gate: live mirror preserves at least 70 percent of shadow edge

### P5_currency_strength_mtf_consensus

- Goal: Test whether currency-strength baskets plus M5/M15/H1 agreement improve ARIMA and event-gate precision.
- Promotion gate: improves precision and net pips versus same ARIMA gates without MTF/currency-strength filters

### P6_jump_spread_session_veto

- Goal: Determine which shock, persistent spread, and session-transition states should veto or throttle signals.
- Promotion gate: reduces churn and live/slippage losses without materially reducing validated edge

### P7_feature_audit_and_target_metadata

- Goal: Guarantee wide-feature datasets remain leakage-safe and compatible with TP/SL, time-stop, and triple-barrier labels.
- Promotion gate: all training rows have deterministic max-lookback and forward-label metadata before model comparison
