# Saved model and cross-pair inventory — 2026-09-06

The current four-family research study is much narrower than the historical model catalogue. More model names do not establish more independent information. This review inspected source and bounded saved metadata; it did not fit models, start workers, access databases, deserialize checkpoints, or change runtime state.

## Current four-family study

`config/causal_forecast_study_v1_io_r2_20260906.json` enables collection for EUR/USD, M1 inputs and a fixed one-hour target. Its seven input pairs are EUR/USD, GBP/USD, AUD/USD, NZD/USD, USD/JPY, USD/CHF and USD/CAD. All four numerical functions come from `oanda_proof_shadow_predictors.py`; their inputs and original timing are now wrapped by the separate causal study. The observed study heartbeat had zero forecasts/outcomes and was waiting for a tradeable quote. That is a point-in-time observation, not a current-market claim.

| Family label | Actual numerical implementation | What it is not |
|---|---|---|
| `ridge_return_repaired` | Per-pair standardized ridge return regression; residual standard deviation produces a Gaussian probability (`ridge_predictions`, line 397). | A broad catalogue of independently trained models. |
| `modern_tabular_probabilistic_repaired` | Pooled scikit-learn histogram gradient boosting regressor and classifier using volatility-normalized technical features (`pooled_tabular_predictions`, line 416). | CatBoost, NGBoost, TFT or a foundation model. |
| `cross_pair_graph_transfer` | Base/quote currency return aggregates over 1/5/15/60-minute windows plus own-pair lags, fitted by ridge with alpha 25 (`currency_factor_matrix`, line 463; `graph_predictions`, line 484). | StemGNN, a learned graph neural network, VAR/VECM, or a fitted dynamic-factor transition model. |
| `probabilistic_state_space` | An exponentially updated drift/innovation variance filter with alpha 0.08; Gaussian probability, clipped mean (`state_space_predictions`, line 554). | A fitted hidden Markov, switching-state, S4 or Mamba model. |

These functions share price history, horizons and much of their trend information. The current configuration explicitly retains source/input/version identities without claiming that fitted weights are stored. Prediction/error correlations need measurement on matched decisions; this inventory does not infer their numeric correlation from model names.

## Cross-pair relationships: implemented versus missing

| Capability | Source evidence and disposition |
|---|---|
| Historical return correlation graph | `oanda_cross_pair_graph_adapter.py:64` builds historical same-time return correlations and shared-currency exposure edges. Its label “lagged” means observations precede the cutoff; it does not itself estimate directed lead-lag shifts. |
| Neural cross-pair forecasting | `oanda_model_gap_market_validation.py:733` actually constructs and fits NeuralForecast StemGNN. The separately calculated correlation/exposure graph appears in returned diagnostics and `adjacency_shape`; that adjacency is not passed to the StemGNN constructor or fit call. It must not be described as the graph explicitly driving that model. This is historical offline research, not a current four-family member. |
| Currency lead-lag regression | `oanda_currency_lead_lag_shadow.py:51,111` reconstructs latent currency returns, builds 1/2/3/5/10-minute lag features, and compares ridge network versus own-lag baselines. Saved August 5 report exists locally. Network mean net pips were -1.653407 (M5), -2.009624 (M15), and -1.617263 (M30); all rejected promotion. Improvement versus a worse baseline was not profitability. |
| Currency state and covariance | `src/forex_system/features/currency_state_engine.py:209` solves weighted base-minus-quote observations on a connected currency network, with uncertainty covariance from its least-squares normal matrix. This reconstructs observed currency moves and measurement uncertainty; it is not a forecast-error correlation matrix or a trained dynamic-factor return forecaster. Saved report explicitly labels it observed response, no trade. |
| VAR/VECM/cointegration | Explicitly `not_in_current_trainer` in `oanda_model_space_agenda.py:106`; named as a desired addition in `MODEL_FEATURE_SPACE.md`. No VAR/VECM/cointegration fitting or testing implementation was found in the inspected project Python source. |
| Dynamic factors / fitted switching regimes | No fitted PCA/dynamic-factor, Gaussian-HMM or Markov-regression implementation found in the inspected project Python source. `regime_switching` in the strategy lab is a threshold rule based on efficiency ratio and displacement, while the current four-family state model is EWMA. Agenda placeholders are not trained models. |
| Ensembles / dependence | `oanda_strategy_lab_ensemble_replay.py:161,185` deduplicates profile votes into families and hand-labelled clusters. `oanda_ensemble_candidate_backtester.py:298` uses clipped log-score member weights. These are real ensemble research implementations, but no current four-family residual-covariance/stacking optimizer was found. |

## Historical modern-model catalogue and retained artifacts

The July completion/unified summaries list 30 entries, 28 with bounded market evaluations, zero production-eligible and zero account-wired. All eight exact source reports referenced by the completion receipt still exist on D and their bounded file hashes match the retained receipt. These are preserved historical research results, not fresh operational validation.

- Actual CatBoost, NGBoost, logistic and HGB `.joblib` files exist under `D:\forex\trad\data\oanda_training_manager\models\modern_model_gap\full_matrix_candidate_20260722`; all four hashes match their report.
- TFT and DeepAR broad and H1/H4 checkpoint files exist in the D-drive `models\modern_model_gap` directory. Old report pointers reference a removed temporary C directory. Relocated files are 32–43 MB; this bounded inventory verified presence/size, not their entire content hashes.
- PatchTST, N-BEATS, N-HiTS, LSTM, GRU, TCN, temporal cross-pair GNN, S4, contextual bandit, PPO/SAC/DQN and five representation/transfer methods have actual adapter/training code and a retained 17-model market-validation report. Its summary has 17 completed backtests and zero performance passes. That report contains no retained trained checkpoint pointers for those fits; the inventory does not invent deployable artifacts from a completed fit.
- Seven foundation-model weight destinations and manifests exist on D: Chronos-2, TimesFM-2.5, Moirai, Moirai-MoE, Tiny Time Mixer, Toto-2 and Lag-Llama. Frozen pretrained weights and recorded bounded scores differ from training these models on current Forex data. Large weight content hashes were not rechecked.
- TimesFM-ICF and Mamba remain explicit blockers in the saved project configuration: no audited ICF runtime and the recorded Mamba host/runtime incompatibility. This is the project’s recorded disposition, not a fresh claim about externally available software.
- Four configured C recovery Python executables exist. Their package imports were not retested, and the current C project’s model directory is empty; D research artifacts are not automatically wired to it.
- `config/shadow_runtime_retirements_v1.json` labels the thirty contributors `dormant_not_active_breadth`: no fresh live contributors or account-eligible contributors in its retained reconciliation.

The most relevant implementation gaps for joint movements are a separately validated synchronized VAR/cointegration baseline, a genuinely fitted dynamic-factor/regime model, and matched prediction/error-dependence evidence before designing another ensemble. This is an engineering inventory, not evidence that any such addition would improve trading returns. Unavailable order-book/depth inputs also remain separate from the question of which algorithms are installed.

Reproducible observations: `model_inventory_evidence.json`, `neural_relocation_evidence.json`, and their two adjacent read-only scripts. Historical summaries, source versions and artifact locations are separately recorded; none were rewritten.
