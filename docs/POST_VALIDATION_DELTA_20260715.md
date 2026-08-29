# Post-Validation Project Delta

Baseline: `unified_query_20260713_v3`, validated at
`2026-07-14T00:04:05.116917+00:00`.

## What The Baseline Actually Validated

The vault reconciled 46,368 normalized runs, 668,084 metrics, 643 models, 2,387
period results, 10,772 parameter sets, and 84,747 artifact references. All 25
catalog, relational-integrity, portability, and packaging checks passed.

It did **not** validate trade readiness, current runtime behavior, raw candles,
serialized models, credentials, or live/practice account state. The build
manifest explicitly records `trade_ready: false` and `orders_placed: false`.

## Changes After Validation

### Vault Content

- Removed one repeated metric-comparison disclaimer from all 140 generated Forex
  model pages.
- Added `encyclopedia/info/METRIC_COMPARISON_RULES.md` as the single rules/info
  document for that guidance.
- Consequence: all 140 Forex page hashes differ from `checksums.sha256`, and the
  new rules document is absent from the manifest. The old validation report is
  historically true but no longer certifies the current build byte-for-byte.

### Live Research And Forecasting

- Expanded the fast OANDA shadow lab to 156 physical lanes: 39 strategy families
  with four parameter profiles each, evaluated against one shared causal feature
  stream. Every physical signal now matures at M1/M3/M5/M10/M15/M30/H1, yielding
  1,092 independently ranked lane-horizon evidence surfaces.
- Added four explicit multi-indicator confluence families plus an automated
  fuzzy signal-combination family. The audit stores compressed indicator and
  family-vote snapshots, mines two/three-condition rules on the oldest 70%, and
  reports probability, Brier score, support, expected net pips, and lower edge
  only after the newest 30% chronological holdout.
- Added executable-price path tracking and stop/target sweeps for every family
  and lane. Fitted exits remain unavailable to practice orders until training,
  newest-holdout, profit-factor, confidence, and time-block gates all pass.
- Added cost-aware accepted, near-miss, hard-reject, missed-potential, and exact
  matured-horizon tracking.
- Added pattern-count forecasts with readable U/D sequences, historical/live
  counts, future outcomes, movement coefficients, and causal updates.
- Added quote-level micro forecasting with fixed intrasecond and variable
  next-minute-boundary horizons, online continuous equation forecasts, duplicate
  timestamp rejection, and SQLite persistence of every accepted quote and causal
  forecast.
- Added robust Huber-residual equations, four-regime causal k-means equations,
  and cross-pair relative-strength equations at 2-second and next-minute-boundary
  targets. Added exact fit/formula/feature/target metadata, matched signal
  correlations, and a shadow-only ruleset leaderboard.
- Added ensemble replay, exact historical explorer, broader historical backtest,
  S5 backfill, and S5/S30/M1/M5 strategy replay tooling.
- Added an evidence gate for practice account -007. Signals require at least 30
  matured observations, +0.10 average net pips, nonnegative median, 52% win rate,
  and a nonnegative one-sided 90% lower confidence bound.
- Replaced restart-local execution ranking with a compact persistent executable-
  net ledger and non-overlapping horizon-block validation. Promotion also
  requires newest-30% holdout stability plus pair/session breadth; historical
  backfill forces an explicit no-trade state. Added a shadow-only inverse-
  correlation veto family and removed clear-loser loose variants from account
  candidate routing while retaining their shadow outcomes.

### Data Collection

- Added one-second all-instrument OANDA depth/liquidity sampling to Parquet.
- Repaired the depth collector's hourly Parquet writer. It now writes immutable
  compressed parts instead of rereading a Hive-partitioned file, eliminating the
  repeated `date` schema conflict and growing-file rewrite cost.
- Preserved live S1 quote collection and added S5 historical backfill tooling;
  true historical S1 is not available from OANDA's candle API.
- Added volume/activity, spread change/velocity, quote-rate, smoothing, and
  multi-window movement features to short-horizon research.
- Added a public Coinbase shadow tracker for BTC, ETH, SOL, XRP, DOGE, ADA, AVAX,
  and LINK USD products. It logs ticker/trade events and shadow forecasts at
  5/30/60/300 seconds without authentication or order routing.

### Accounts And Operations

- Expanded `accounts_registry.json` to the available practice account inventory
  and current strategy attachments.
- Enabled D-based practice managers for -002, -003, -005, -006, and -013; -007
  remains the evidence-gated strategy-lab account.
- Kept -004 disabled because its available pullback evidence was negative after
  spread. Account -019 continues to collect HGB/adaptive-fit evidence, but new
  entries are paused after corrected path-adjusted validation remained negative.
- Added repeated account snapshots for all 20 practice accounts and current
  read-only GET summaries for the three configured live accounts. Old manager
  state files remain a clearly aged fallback only.
- Replaced misleading raw aggregate comparisons with equal-weight account return
  and a 100-start normalized index.
- Added an always-on D-drive supervisor for collectors, account snapshots,
  dashboard, crypto shadow tracking, shadow labs, and selected practice managers.
- Added the S1 `ridge_return` layer to the unified forecast matrix for `-007`.
  Its process remains independent for latency isolation, but its family, profile,
  pair, and horizon evidence use the shared matrix and promotion identifiers. It retains stream
  version/depth/liquidity/microprice metadata, computes causal 5/10/30/60-second
  features in memory, forecasts 15/30/60/180/300/600/900/1800/3600-second outcomes, invokes the
  account selector before research persistence, and records timing telemetry.
  A write-free hot role is isolated from both the CPU-heavy 156-lane M1
  evaluation and the persistence-enabled tracker. Both practice selectors share
  an atomic order lock plus a broker-position recheck.
  A six-hour sidecar refits compact snapshots from all available S5 pair files.

### Dashboard

- Added top-level all-account and paper/live aggregate views, strategy layers,
  model inventory/search, historical run views, horizon/timeframe breakdowns,
  signal win rate, definitions/tooltips, and live balance/status indicators.
- Added pattern metric drill-down and a continuous-equation explorer with a fixed
  30% historical / 70% future path, explicit selection, flat-future rendering,
  collapsible panes, expanded dialog, sorting, and matched matured outcomes.
- Added equation-definition, signal-correlation, and ruleset-leaderboard panes;
  variable boundary targets no longer appear as opaque `0ms` models.
- Added honest HGB historical calibration for M1/M30/H1/H4 at 120 minutes. No
  unrelated live model is borrowed as HGB evidence.
- Added ARIMA coverage records distinguishing the full M1-derived 120-minute
  SARIMA sweep from pair-level challenger reports.
- Added a compact main `-007` account/model summary, fuzzy-rule and exit-fit
  drill-down, and persistent click-to-sort behavior across dashboard tables.
- Added M1-through-H1 promotion status, backfill progress, virtual model-space
  count, and lane/horizon holdout evidence to the main `-007` panel.

### Documentation And Portability

- Added `docs/OANDA_LAB_OPERATOR_GUIDE.md` and `PORTABLE_RUNTIME.md`.
- Added this delta report and a complete fresh-chat handoff prompt to `README.md`.
- Created a source/runtime ZIP in the OneDrive vault with an internal SHA-256
  manifest and no credentials, active databases, rolling market data, model
  binaries, caches, or virtual environments.
- Created `D:\vault_backups\thevault_snapshot_20260714_235524`: 3,651 files and
  4,835,636,435 bytes, matching the OneDrive vault at copy time. The project ZIP
  and validation-report hashes matched across source and snapshot.
- Runtime code and data are on D. The editable Git working copy still exists on
  C, so the project has not yet been physically reduced to a D-only source tree.

## Verification Performed After The Baseline

- Focused Python unit tests for shadow-lab families/gates, pattern counts,
  micro-equation persistence, historical explorers/backtests, ensemble replay,
  dashboard paths/calibration, and GPT manager behavior have passed in their
  respective changes.
- The latest combined post-equation run passed 56 focused tests. The account
  assignment regression, equation definitions, Huber update, k-means state,
  cross-feature shape, correlation output, dashboard paths, and compile checks
  are included.
- Browser checks confirmed dashboard loading, stable equation selection, retained
  pane state, 30/70 path framing, model search, and no console errors before the
  latest account-table addition.
- Process inspection found active Forex services launched from `D:\forex`; no
  Forex Python runtime was launched from the C working copy.
- The v7 multi-horizon rollout passed 44 focused tests from `D:\forex`. Its
  initial compact migration scanned 1,875,202 path-ledger rows, retained 138,674
  executable signal outcomes, fitted 379 lane/horizon evidence records, and
  qualified zero for practice execution. The live selection loop consequently
  placed no order and account -007 remained flat.
- The fast-forecast sweep read 68 S5 pair files, fitted 213 usable models on 59
  pairs, and skipped nine sparse/non-contiguous pairs. Across 639 profile
  surfaces, zero had positive holdout average net pips after executable spread
  and zero passed the confidence gate. These models are deployed for forward
  shadow measurement, not as evidence of profitability.
- Thirty-seven focused strategy, promotion, second-forecast, and depth-writer
  tests passed after the low-latency integration.
- At `2026-07-15T15:51Z`, the compact ledger held 47,337 M1, 46,583 M3, 46,072
  M5, and 191 newly matured M10 signal outcomes. M15, M30, and H1 were configured
  and collecting but had not yet reached their first post-rollout maturity.
- Desktop and 390px mobile browser checks showed the 1,092-surface summary,
  M1-through-H1 selector, compact promotion blockers, no page-width overflow,
  and no console errors. The large live state request still took roughly 25-35
  seconds and remains a dashboard performance item.
- The S1 ridge/equation path is now a normal slice of the all-encompassing
  horizon/timeframe matrix, not a parallel strategy product. The dashboard
  reports 159 physical lanes, 1,119 lane/horizon surfaces, 40 families, and 535
  S1 pair/horizon model surfaces over 15/30/60/180/300/600/900/1800/3600-second
  outcomes. The full S5 fit produced usable models for 60 pairs.
- The final focused deployment run passed 52 tests from both the C working tree
  and the deployed `D:\forex` tree. Current desktop and 390px mobile checks also
  confirmed the full account table, local horizontal table scrolling, no page
  overflow, and no browser-console warnings or errors.
- After removing flat-account REST polling from the forecast thread, 20
  consecutive populated log intervals ran without the previous feature-window
  reset. At `2026-07-15T19:01Z`, hot-path p50/p95 cycle latency was about
  35/132 ms including startup history; the tracker had sampled 2,402 forecasts
  and matured 713 outcomes. Promotion remained fail closed with zero eligible
  lane/horizons across 207,429 compact signal outcomes.
- The current vault checkpoint contains the deployed source hashes. A separate
  hash-verified snapshot was created at
  `D:\vault_backups\forex_project_snapshot_20260715_150257`; all eight declared
  artifacts and the copied vault ZIP matched their source hashes.
- `D:\forex` remains the sole Forex runtime/data plane. Process inspection found
  zero Forex Python or PowerShell runtimes under the C working tree. The D drive
  had about 3.96 TB free, so compressed history is retained in full and rolling
  retention is not currently active.

These are focused checks, not a replacement for a complete vault rebuild,
end-to-end replay, or production validation.

## Pending / Not Fully Validated

- Run a full semantic vault validation when a new validation baseline is needed.
  The current automatic checkpoint and D snapshot are hash verified, but that is
  not equivalent to revalidating every historical report's methodology.
- Soak-test the six new equation variants long enough to mature meaningful
  cost-aware samples; inspect database growth and retire variants that add no
  incremental signal after the correlation analysis.
- Soak-test the one-second path and report p50/p95 inference latency, stream quote
  age, sampled-ledger growth, forward results by pair/horizon/profile, and any
  candidate-to-order latency. Do not relax promotion gates to manufacture fills.
- Run ARIMA/SARIMA sweeps across standalone M5/M15/M30/H1/H4 inputs and multiple
  horizons. Existing broad coverage is M1-derived with a 120-minute target.
- Add matched live HGB outcome logging. Current adjusted HGB columns contain no
  live HGB samples and therefore equal the capped historical prior.
- Perform a longer soak test for database growth, stream gaps, supervisor restart
  behavior, and dashboard response time. The focused suite and current desktop /
  narrow browser checks are complete.
- Accumulate enough forward `-007` snapshots to audit fuzzy combination rules
  with meaningful support. No mined rule or fitted exit is presumed eligible at
  startup; inspect chronological holdout drift and SQLite growth during the soak.
- Let newly added M10/M15/M30/H1 outcomes mature and re-fit before interpreting
  those horizons. H1 needs at least one hour for its first forward label and much
  longer for the independent-block, breadth, and confidence gates.
- Decide whether to remove/archive the C working copy only after Git metadata,
  source hashes, D runtime, vault snapshot, and portable archive are verified.

## Known Evidence Limits

- The prior 132-lane sample was negative after spread. The new 20 lanes and
  automated combination/exit fit have no matured forward sample at deployment
  and must not be described as profitable.
- Current 156 rule-family lanes remain shadow-only except for the explicit
  evidence-gated `-007` practice route.
- The strongest credible historical HGB 120-minute candidate is still historical,
  not demonstrated live profitability.
- Sub-minute direction accuracy near 50% is not tradability. Spread-adjusted net
  outcomes and confidence bounds are the controlling evidence.
- Older reports with implausible compounded returns are not valid deployment
  evidence.

## Source-File Inventory Since Baseline

This timestamp-based inventory contains 42 source/config/doc/test files modified
after the vault validation timestamp. Rolling logs, databases, Parquet files,
generated reports, caches, and virtual environments are excluded.

```text
trad/config/accounts_registry.json
trad/config/primary_forecast_rotation_demo_019.json
trad/crypto_shadow_live_tracker.py
trad/docs/OANDA_LAB_OPERATOR_GUIDE.md
trad/docs/POST_VALIDATION_DELTA_20260715.md
trad/monitor_shadow_strategy_lab.ps1
trad/oanda_account_snapshot_writer.py
trad/oanda_always_on_supervisor.ps1
trad/oanda_depth_parquet_collector.py
trad/oanda_gpt_9h_formula83_account_manager.py
trad/oanda_gpt_prod_live_account_manager.py
trad/oanda_hgb_adaptive_fit.py
trad/oanda_hgb_live_outcome_tracker.py
trad/oanda_lane_promotion.py
trad/oanda_lane_promotion_fit.py
trad/oanda_pattern_count_forecast.py
trad/oanda_primary_forecast_rotation_bot.py
trad/oanda_practice_live_dashboard.py
trad/oanda_practice_micro_pattern_lab.py
trad/oanda_practice_shadow_strategy_lab.py
trad/oanda_signal_combination_audit.py
trad/oanda_signal_combination_fit.py
trad/oanda_second_forecast.py
trad/oanda_second_forecast_fit.py
trad/oanda_second_forecast_runner.py
trad/oanda_s5_backfill_only.py
trad/oanda_s5_timeframe_strategy_replay.py
trad/oanda_strategy_lab_ensemble_replay.py
trad/oanda_strategy_lab_historical_backtest.py
trad/oanda_strategy_lab_historical_explorer.py
trad/oanda_strategy_exit_fit.py
trad/PORTABLE_RUNTIME.md
trad/README.md
trad/requirements-crypto-shadow.txt
trad/test_gpt_9h_formula83_account_manager.py
trad/test_gpt_live_news_watch.py
trad/test_oanda_lane_promotion.py
trad/test_oanda_pattern_count_forecast.py
trad/test_oanda_practice_live_dashboard.py
trad/test_oanda_practice_micro_pattern_lab.py
trad/test_oanda_practice_shadow_strategy_lab.py
trad/test_oanda_signal_combination_audit.py
trad/test_oanda_second_forecast.py
trad/test_oanda_depth_parquet_collector.py
trad/test_oanda_strategy_lab_ensemble_replay.py
trad/test_oanda_strategy_lab_historical_backtest.py
trad/test_oanda_strategy_lab_historical_explorer.py
```
