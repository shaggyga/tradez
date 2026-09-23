# Complete Forex input and prediction-source audit — pre-upgrade baseline

> **Baseline status:** this file preserves the original point-in-time inventory
> generated around 11:10 UTC. Appended implementation notes document the
> transition, but the counts in the inventory tables are not the current
> governance scorecard. The timestamped current replacement is
> `COMPLETE_SOURCE_AUDIT_POST_UPGRADE_20260806.md`.

Point-in-time audit generated 2026-08-06 around 11:10 UTC from the canonical workspace `C:\Users\zmoor\Documents\forex\trad`.

This document expands the news-only inventory into the complete predictive stack: raw market inputs, news/event inputs, strategy families, statistical/model outputs, microstructure and cost inputs, combination/calibration layers, validation evidence, and dormant or missing model sources.

## Post-audit proof-layer update

The breadth inventory below remains a point-in-time source audit. It is not the
promotion scorecard. The first follow-up build added no strategy family. The
subsequent proof pass added only four structurally distinct, H1, prospective
model hypotheses; all are research-only and disconnected from execution:

- `canonical_forecasts` now records immutable, versioned forecast-time evidence
  before an outcome exists; `canonical_outcomes` and integrity misses are also
  append-only.
- `oanda_canonical_outcome_worker.py` separates short-horizon maturation from
  the slow 209-lane feature cycle. It observes the five-second executable quote
  snapshot without broker access and rejects any endpoint more than 15 seconds
  late. A follow-up boundary audit also rejects cached snapshots recorded before
  the declared horizon; 2,483 preserved early-snapshot labels were excluded.
  The first corrected live sample contained 349 labels with zero early/late
  boundary violations and a 5.7-second mean endpoint delay.
- `oanda_edge_evidence.py` materializes the canonical family x pair x horizon x
  session x liquidity cell, factor/episode effective N, gate distances,
  archetype evidence, economic outcome labels, historical/live parity status,
  and a fail-closed no-trade allocator. The current report is
  `data/oanda_training_manager/reports/edge_evidence/EDGE_EVIDENCE_CURRENT.md`.
- `macro_surprise_v1.sqlite` preserves structured release revisions separately
  from narrative news. At the latest proof build it held 100 releases (83
  scheduled, 14 actuals) but no consensus values, so zero numeric surprises
  were eligible.
- `oanda_proof_shadow_predictors.py` repairs a current-contract per-pair ridge
  and pooled probabilistic tabular baseline, and adds parsimonious currency-
  graph/transfer and probabilistic state-space baselines. They write only
  immutable pre-outcome forecasts; no historical result was reused as a live
  certificate and none can publish to the execution feed.
- Historical model-gap code was reconciled rather than misreported as absent:
  all 28 runnable declared adapters received bounded market tests, zero passed
  the production gate, and the remaining Mamba/TimesFM-ICF runtime blockers are
  retained explicitly. Implemented/backtested is not current canonical evidence.
- Live storage monitoring now covers the main canonical ledger and evidence
  database as well as WALs. A measured 0.3-0.57 MB/s burst was traced primarily
  to duplicated JSON on non-maturing rejects and mirrored outcomes. New reject
  and outcome rows use normalized compact diagnostic payloads while retaining
  every causal forecast/rejection and all fields consumed by evaluators. Existing
  immutable history was not deleted; archive/partition work remains evidence-
  gated. A full reloaded live cycle verified 39% smaller non-maturing forecast
  records (2.08 KB average) and approximately 0.40 KB compact outcome
  diagnostics versus roughly 2-2.7 KB previously.
- Order-book, position-book, and depth availability remain measured at zero and
  are explicitly excluded. No current evidence cell passes all promotion gates.

Historical backtests are still acknowledged where they exist, but incompatible
periods, labels, costs, code, and horizons remain side by side rather than being
combined with the newer canonical sample.

## 1. Inventory summary

| Layer | Defined/live count | Minimal current statistic | Execution role |
|---|---:|---|---|
| OANDA price stream | 68 instruments | Connected; 68/68 quoted; 2,949,112 updates; 7 reconnects | Primary live price and executable bid/ask source |
| Completed candle context | M1, M30, H1, H4 | 512-bar tail; completed bars only | Structural features |
| Main strategy lab | 53 named families, 209 lanes | Four profiles for 52 families plus one AHL lane | Mixed: some policy-eligible, many shadow-only |
| Extra equation output | 13 timeframe-equation contributors | Active; research-only | Shadow-only |
| Unique H1 research stream | 4 families | 2,445 independent observations per family | Shadow-only |
| News/event sources | 63 definitions | 55 operational, 53 healthy | Research/context-only |
| Quote-intensity stream | 68 instruments | 1,518,744 rows written; no current error | Execution-timing research only |
| Order/position books | 0 currently available | Feature flags are zero; training eligibility false | Not an effective current predictor |
| Signal contributor registry | 1,026 contributors | 659 current feed candidates; 0 currently qualified | Registry and routing |
| Matured strategy outcomes | 312,113 rows, 54 families | Data spans 2026-08-03 22:00 to 2026-08-06 11:07 UTC | Measurement only |
| Promotion evidence | 1,865,592 raw rows; 2,315 evidence cells | 0 eligible cells, 0 eligible lanes | Fail-closed promotion gate |
| Canonical trials | 518 cohorts | 500 matured, 18 open | Research comparison only |
| Top-signal position ledger | 11,283 positions | 11,268 matured, 15 open | Counterfactual/shadow measurement |
| Model-gap registry | 7 classes / 30 declared contributors | 28 never produced; one stale two-contributor class produced historically | Not live execution evidence |

## 2. Statistical definitions and limitations

The per-family table below uses the latest available main shadow-outcome database and applies a deliberately compact comparison:

- horizon fixed to **H1**;
- entry spread at or below **3 pips**;
- one observation retained per family, pair, and UTC hour;
- **N** is therefore less duplicated than raw rows, but families and pairs still share market episodes;
- **Win %** means after-spread theoretical pips greater than zero;
- **Avg net** uses the executable-side entry and exit formula, so the bid/ask spread is included;
- the requested seven-day window contains only about **2.5 days of available main-family history**;
- “eligible lanes” reports current registry policy, not proof of profitability.

These are diagnostic statistics, not a backtest certificate. They are not purged across correlated currency factors and should not be summed across families.

**Important sample note:** the table reports only the smaller, standardized recent live/shadow sample available in `strategy_shadow_outcomes_v1.sqlite`. Several families have substantially larger historical backtests and research artifacts elsewhere in the project. A low N here means limited comparable live H1 evidence under this audit definition; it does **not** mean the family was never backtested or has only that many lifetime observations. Historical results use different periods, costs, filters, horizons, and sampling rules, so they are intentionally not merged into the live figures. The historical backtest inventory is maintained in `docs/BACKTEST_AND_MODEL_REGISTRY.md` and should be consulted alongside—rather than numerically combined with—this audit.

## 3. Main strategy and prediction families

### Trend and momentum archetype

| Family | Definition | Eligible lanes | N | Pairs | Win % | Avg net pips |
|---|---|---:|---:|---:|---:|---:|
| `momentum` | Recent-return continuation | 3/4 | 122 | 20 | 37.7 | -3.834 |
| `ahl_multihorizon_trend` | AHL-style trend persistence across multiple horizons | 0/1 | 4 | 4 | 50.0 | -0.450 |
| `ema_trend_cross` | Fast/slow exponential-average trend cross | 4/4 | 8 | 8 | 37.5 | -0.238 |
| `higher_timeframe_alignment` | Lower-timeframe direction confirmed by higher frames | 4/4 | 54 | 21 | 40.7 | -0.883 |
| `rsi_trend_continuation` | RSI used as a continuation-state filter | 4/4 | 23 | 11 | 39.1 | -2.378 |
| `linear_regression_trend` | Linear price-slope continuation | 3/4 | 248 | 22 | 34.7 | -3.162 |
| `efficiency_filtered_momentum` | Momentum accepted only under directional path efficiency | 3/4 | 55 | 17 | 32.7 | -4.053 |
| `kama_adaptive_trend` | Kaufman adaptive-moving-average trend | 4/4 | 178 | 20 | 36.5 | -3.815 |
| `trend_momentum_confluence` | Multiple trend/momentum indicators combined | 3/4 | 286 | 25 | 35.0 | -3.167 |
| `permutation_entropy_momentum` | Momentum conditioned on low/high ordinal-pattern entropy | 0/4 | 271 | 25 | 39.1 | -1.337 |
| `theil_sen_trend` | Robust median-slope trend estimate | 0/4 | 380 | 24 | 43.9 | -1.338 |

**Archetype total:** 1,630 observations, 11 families, 38.3% after-cost wins, -2.477 average net pips.

### Breakout and expansion archetype

| Family | Definition | Eligible lanes | N | Pairs | Win % | Avg net pips |
|---|---|---:|---:|---:|---:|---:|
| `donchian_breakout` | Escape from a rolling high/low channel | 4/4 | 79 | 16 | 44.3 | -1.653 |
| `volatility_squeeze_breakout` | Breakout following compressed volatility/bands | 4/4 | 82 | 18 | 45.1 | +0.954 |
| `range_expansion` | Continuation when the current range expands materially | 3/4 | 160 | 21 | 43.8 | -1.643 |
| `spread_compression_momentum` | Momentum after spread/liquidity compression | 4/4 | 112 | 21 | 33.0 | -4.455 |
| `breakout_retest` | Breakout followed by a successful level retest | 4/4 | 97 | 20 | 37.1 | -2.265 |
| `session_range_breakout` | Break from an established session range | 4/4 | 57 | 17 | 36.8 | -0.253 |
| `micro_channel_break` | Escape from a short, narrow price channel | 4/4 | 186 | 21 | 41.9 | -1.825 |
| `breakout_volume_confluence` | Breakout confirmed by tick-activity expansion | 3/4 | 133 | 20 | 31.6 | -2.874 |
| `cusum_breakout` | CUSUM change detector used as a directional break trigger | 0/4 | 142 | 22 | 45.1 | -0.078 |
| `garch_volatility_breakout` | Breakout conditioned on fitted conditional volatility | 0/4 | 180 | 22 | 36.1 | -2.352 |

**Archetype total:** 1,228 observations, 10 families, 39.5% after-cost wins, -1.795 average net pips.

### Mean-reversion and reversal archetype

| Family | Definition | Eligible lanes | N | Pairs | Win % | Avg net pips |
|---|---|---:|---:|---:|---:|---:|
| `pullback` | Fade of a short extension within a broader trend/value zone | 4/4 | 30 | 14 | 26.7 | -4.557 |
| `macd_rsi_reversal` | MACD/RSI exhaustion reversal | 4/4 | 16 | 9 | 50.0 | -1.294 |
| `bollinger_reversion` | Return toward the Bollinger center after band extension | 4/4 | 95 | 21 | 34.7 | -3.292 |
| `stochastic_reversal` | Stochastic oscillator overextension reversal | 4/4 | 82 | 18 | 36.6 | -3.790 |
| `candlestick_reversal` | Reversal based on candle-shape patterns | 4/4 | 54 | 16 | 31.5 | -4.243 |
| `atr_mean_reversion` | Extension normalized by ATR and faded | 4/4 | 96 | 20 | 35.4 | -0.834 |
| `failed_breakout_reversal` | Fade after a breakout fails to hold | 4/4 | 18 | 11 | 27.8 | -5.772 |
| `cci_reversion` | Commodity Channel Index overextension fade | 4/4 | 12 | 10 | 41.7 | +0.908 |
| `volume_climax_reversal` | Reversal after extreme tick activity | 4/4 | 9 | 6 | 22.2 | -5.456 |
| `volatility_shock_fade` | Fade of an abrupt volatility shock | 4/4 | 4 | 4 | 25.0 | -14.800 |
| `oscillator_reversion_confluence` | Multiple oscillator/value deviations combined | 4/4 | 326 | 23 | 38.3 | -2.088 |
| `bipower_jump_reversal` | Realized-jump detection followed by a fade | 0/4 | 13 | 7 | 38.5 | -4.354 |
| `vwap_deviation_reversion` | Fade from a session VWAP deviation | 0/4 | 129 | 19 | 45.0 | -1.274 |
| `ny_session_vwap_sell_reversion` | Sell-only New York-session VWAP reversion hypothesis | 0/4 | 24 | 10 | 54.2 | -1.692 |

**Archetype total:** 908 observations, 14 families, 37.9% after-cost wins, -2.460 average net pips.

### Cross-sectional and relative-value archetype

| Family | Definition | Eligible lanes | N | Pairs | Win % | Avg net pips |
|---|---|---:|---:|---:|---:|---:|
| `currency_strength` | Synthetic base-minus-quote currency strength | 3/4 | 255 | 22 | 34.1 | -3.584 |
| `relative_value_reversion` | Pair deviation from related-pair/currency value | 4/4 | 42 | 20 | 35.7 | -3.045 |
| `cross_sectional_pair_rank` | Direction from ranking returns across the pair universe | 4/4 | 174 | 20 | 33.9 | -2.336 |
| `cross_pair_lead_lag` | Lagged response to related-pair movement | 3/4 | 133 | 20 | 42.1 | -1.405 |
| `cross_market_confluence` | Cross-pair/cross-market confirmation | 3/4 | 374 | 20 | 39.6 | -2.848 |

**Archetype total:** 982 observations, 5 families, 37.3% after-cost wins, -2.780 average net pips.

### Statistical-forecast archetype

| Family | Definition | Eligible lanes | N | Pairs | Win % | Avg net pips |
|---|---|---:|---:|---:|---:|---:|
| `supervised_return_rank` | Supervised cross-pair expected-return ranking | 4/4 | 12 | 6 | 33.3 | +0.900 |
| `pattern_count_forecast` | Nearest/count-based historical path-pattern forecast | 4/4 | 370 | 26 | 37.0 | -2.645 |
| `kalman_local_trend` | Local linear state/trend estimated by a Kalman filter | 0/4 | 273 | 23 | 38.8 | -1.885 |
| `markov_sign_transition` | Direction from estimated sign-transition probabilities | 0/4 | 253 | 23 | 40.7 | -2.025 |
| `online_ar_forecast` | Continuously updated autoregressive return forecast | 0/4 | 358 | 23 | 35.2 | -3.381 |
| `timeframe_equation_matrix` | Separate fitted equation outputs by input timeframe/horizon | 0/13 | 282 | 24 | 41.5 | -2.211 |

**Archetype total:** 1,549 observations, 6 families, 38.3% after-cost wins, -2.486 average net pips.

### Volatility and liquidity archetype

| Family | Definition | Eligible lanes | N | Pairs | Win % | Avg net pips |
|---|---|---:|---:|---:|---:|---:|
| `volume_impulse` | Directional impulse from OANDA price-update activity | 4/4 | 176 | 20 | 38.1 | -2.454 |
| `spread_mean_reversion` | Directional hypothesis around abnormal spread state | 4/4 | 2 | 2 | 50.0 | -0.950 |
| `har_volatility_momentum` | HAR-style volatility persistence used directionally | 0/4 | 77 | 16 | 42.9 | -0.848 |

**Archetype total:** 256 observations, 3 families, 39.5% after-cost wins, -1.952 average net pips.

### Regime-state archetype

| Family | Definition | Eligible lanes | N | Pairs | Win % | Avg net pips |
|---|---|---:|---:|---:|---:|---:|
| `regime_switching` | Trend/reversion decision conditioned on market regime | 4/4 | 154 | 22 | 35.1 | -2.785 |
| `variance_ratio_regime` | Persistence versus mean-reversion regime from variance ratios | 0/4 | 225 | 23 | 42.2 | -2.477 |
| `hurst_regime_forecast` | Persistence regime inferred from a Hurst-style exponent | 0/4 | 336 | 26 | 41.4 | -1.932 |

**Archetype total:** 715 observations, 3 families, 40.3% after-cost wins, -2.287 average net pips.

### Ensemble and control archetype

| Family | Definition | Eligible lanes | N | Pairs | Win % | Avg net pips |
|---|---|---:|---:|---:|---:|---:|
| `inverse_correlation_veto` | Contrarian diagnostic derived from other strategy votes; not independent alpha | 0/4 | 167 | 21 | 45.5 | -0.334 |
| `signal_combination_rules` | Reliability-weighted combinations of existing family signals | 4/4 | 29 | 10 | 20.7 | -3.531 |

**Archetype total:** 196 observations, 2 families, 41.8% after-cost wins, -0.807 average net pips.

## 4. Main-family statistical summary

| Measure | Result |
|---|---:|
| H1 deduplicated observations | 7,464 |
| Families represented | 54 |
| Families with at least 30 observations | 41 |
| Families with positive average net pips | 3 |
| Families with at least 50% after-cost wins | 4 |
| Promotion-eligible evidence cells | 0 |
| Promotion-eligible lanes | 0 |

The three positive-average families were `volatility_squeeze_breakout` (N=82), `cci_reversion` (N=12), and `supervised_return_rank` (N=12). The latter two are plainly too small. `volatility_squeeze_breakout` is the only positive-average result with more than 30 observations, but its 45.1% win rate and short 2.5-day evidence window are not sufficient for promotion.

No archetype was positive after costs. Named families are highly correlated variants, so raw family count must never be interpreted as independent consensus breadth.

## 5. Independent H1 research stream

These four families are produced by the continuous one-hour research worker, independent of the 209-lane strategy-lab surface. The current audit covers a seven-day window, one non-overlapping family/pair/hour observation, and entry spreads at or below 3 pips.

| Family | Definition | N | Pairs | Direction accuracy | After-cost win rate | Avg net pips | PF |
|---|---|---:|---:|---:|---:|---:|---:|
| `breakout_change_point` | Structural change detector followed by breakout continuation | 2,445 | 30 | 45.64% | 25.97% | -2.583 | 0.394 |
| `cross_currency_impulse` | Currency-leg impulse propagated across related pairs | 2,445 | 30 | 45.19% | 25.11% | -2.891 | 0.347 |
| `differenced_path_analog` | Nearest historical analogs on differenced price paths | 2,445 | 30 | 48.34% | 26.99% | -2.487 | 0.403 |
| `multi_timeframe_trend` | Trend vote synthesized across completed timeframes | 2,445 | 30 | 47.20% | 27.24% | -2.334 | 0.430 |

All four remain shadow-only. Their median predicted magnitude exceeds realized magnitude, and none has positive after-cost expectancy.

## 6. Other model and adapter sources

| Source/family | Definition | Registry state | Minimal statistic | Current interpretation |
|---|---|---|---|---|
| `ridge_return` | Large matrix of ridge-regression return predictors | Legacy/stale | 743 contributors; 6,921,282 cumulative valid rows; last seen 2026-07-31 | Registry says many were historically account-eligible, but feed TTL makes them non-current |
| `moving_average_feature_grid` | 22 moving-average feature-grid variants | External/unregistered, stale | 1,966,683 cumulative valid; last seen 2026-08-03 | Research history, not a current canonical feed |
| `modern_tabular_probabilistic` | Two probabilistic tabular-model adapters | Stale | 322,128 valid; 13,026 rejected; last seen 2026-07-31 | Historical output only |
| `intrasecond_ridge` | Sub-M1 entry/exit timing ridge | Registered but not expected | 0 valid outputs | No current predictive contribution |
| `move_alert_causal_state` | Causal state around detected large moves | Expected shadow source | 0 valid; 38,938 rejected | Contract/availability gap; not usable evidence |
| `cross_pair_graph_forecasting` | Graph/network forecast across related currency pairs | Adapter and bounded backtest exist | 208-cell historical fit was negative; new parsimonious H1 graph baseline awaiting forward maturity | Implemented, shadow only; no eligible evidence |
| `decision_learning` | Decision/policy-learning model class | Four bounded historical tests | 0 production-eligible | Implemented research, not live; deferred until state/cost labels prove reliable |
| `major_neural_forecasters` | Major neural time-series model adapters | Bounded market tests complete | None passed breadth/economic gates | Historical research only |
| `neural_state_space` | Neural state-space models | S4 tested negative; Mamba runtime blocked | New simple probabilistic state-space baseline awaiting forward maturity | Proof-only baseline; deep adapters not eligible |
| `representation_and_transfer_learning` | Learned representations and cross-pair transfer | Bounded historical tests complete | 0 production-eligible | Historical research only; graph transfer baseline collecting prospectively |
| `time_series_foundation_models` | Foundation-model forecast adapters | Seven bounded tests complete; TimesFM-ICF blocked | 0 production-eligible | Historical research only; no justification for live expansion yet |

The `MODEL_FEATURE_SPACE.md` document lists 95 model terms and 12 proposed high-value additions, but that is a design inventory. It must not be counted as 95 live predictors. The live evidence above is the authoritative distinction between implemented, stale, shadow, and merely specified sources.

## 7. Non-directional and conditioning inputs

| Input | Definition | Current state | Proper use |
|---|---|---|---|
| Live bid/ask and spread | Executable OANDA quote sides and current transaction cost | Healthy across 68 pairs | Entry economics, spread veto, mark-to-market outcomes |
| OANDA candle `volume` | Number of price updates, not centralized exchange volume | Present in completed candles | Activity proxy only |
| Quote intensity | Per-second quote-arrival/imbalance measures | 1,518,744 rows, 68 instruments, healthy | Shadow execution timing and event-intensity research |
| Order-book features | OANDA order-book concentration/peaks | Currently unavailable (`order_book_available=0`) | Prospective research only; cannot currently influence training |
| Position-book features | OANDA position-book concentration/peaks | Currently unavailable (`position_book_available=0`) | Prospective research only |
| Pricing depth | Microprice/top-depth fields | Currently zero/empty | Diagnostics only, not structural prediction |
| Session/time state | UTC/session transition, range, and liquidity timing | Active | Regime, spread, and execution filtering |
| Cross-pair breadth/strength | Return ranks, currency strength, residuals over 68 pairs | Active | Relative-value and confirmation features |
| News/topic/event context | 63-source semantic and scheduled-event layer | 55 operational; full audit linked below | Research-only context and event gating |
| Correlation/factor deduplication | Shared signed-currency and JPY-factor clustering | Active policy/diagnostic layer | Prevent duplicate bets and fake consensus |
| Re-entry state | Pair/direction and factor cooldown after exits | Active | Execution risk control, not alpha |
| Account capacity | Margin, open positions, currency-direction exposure | Active | Position sizing/veto, never forecast direction |

## 8. News and event-source detail

The complete 63-row list, including source definitions, URLs, cadence, currency scope, quality, health, and current exceptions, is in:

- [NEWS_SOURCE_AUDIT_20260806.md](C:\Users\zmoor\Documents\forex\trad\NEWS_SOURCE_AUDIT_20260806.md)

Summary: 37 first-party endpoints, 12 official-domain search fallbacks, eight optional/specialized datasets, and six broad discovery searches. Fifty-five are operational and 53 healthy. All are globally research-only and execution-ineligible.

## 9. Combination, calibration, and validation sources

These layers consume predictors or measure them; they are not independent predictors and must not inflate consensus:

| Layer | What it does | Current statistic |
|---|---|---|
| Archetype taxonomy | Collapses correlated named families into eight effective strategy archetypes | 54 main outputs map to 8 active archetypes |
| Signal-combination audit | Tests multi-family rules on historical snapshots | 398,886 snapshots and 4,663,687 outcome rows; no validated deep rule wired to execution |
| Timeframe calibration | Calibrates probability by lane/timeframe/horizon | Collecting; not account-eligible |
| Lane promotion | Applies sample, expectancy, confidence-bound, and coverage gates | 0 eligible evidence cells and 0 eligible lanes |
| Exit fitting | Estimates horizon-appropriate stop/target behavior | Conditioning/execution research, not direction |
| Canonical signal trials | Compares top-one, independent baskets, conflict and event arms | 500 matured cohorts, 18 open |
| Position ledger | Records every top-signal counterfactual at its stated horizon | 11,268 matured positions, 15 open |
| Execution policy | Applies validation, cost, spread, risk, re-entry, factor, and capacity gates | Enabled for practice-007 only; real-money routing false |

## 10. Audit conclusions

1. The system has broad **representation**, but little current evidence of **positive after-cost prediction**. Only three of 54 main H1 outputs have positive average net pips, and two have N=12.
2. The count of 53 strategy families materially overstates independent diversity. They reduce to eight active archetypes, and multiple profiles/families often use the same price history.
3. The highest-quality inputs are the OANDA bid/ask stream and first-party macro/policy releases. News direction remains incomplete because authoritative publication does not automatically provide causal actual-versus-consensus surprise.
4. Current order/position-book and pricing-depth fields are not real predictive inputs: availability is zero and their contract explicitly keeps them out of training.
5. The extra unique H1 families are statistically well populated but currently negative after cost. They should remain shadow-only.
6. The promotion system is correctly fail-closed: despite millions of raw measurements, no family/horizon evidence cell currently passes the full promotion gate.
7. The correct near-term measurement unit is family × pair × horizon × liquidity/session bucket, deduplicated by signed-currency factor and episode—not aggregate family row count.

## 11. Canonical evidence files

- News configuration: `C:\Users\zmoor\Documents\forex\trad\config\news_sources_v1.json`
- Main strategy definitions: `C:\Users\zmoor\Documents\forex\trad\oanda_practice_shadow_strategy_lab.py`
- Archetype mapping: `C:\Users\zmoor\Documents\forex\trad\oanda_strategy_archetypes.py`
- Main matured outcomes: `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\state\strategy_shadow_outcomes_v1.sqlite`
- Contributor registry and current candidates: `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\state\practice_007_signal_feed_v1.sqlite`
- Unique H1 shadow audit: `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\state\one_hour_shadow_signal_v1.json`
- Promotion evidence: `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\state\lane_promotion_v1.json`
- Feature snapshot: `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\state\live_model_feature_snapshot_v1.json`
- Canonical trials: `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\state\canonical_signal_trials_v1.json`
- Position ledger: `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\state\top_signal_position_ledger_v1.json`
