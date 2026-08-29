# Forex Research And OANDA Execution Package

> **Current canonical orientation (2026-08-27):** the active editable source
> and runtime are `C:\Users\zmoor\Documents\forex\trad`. References below to
> `D:\forex` or to automatic signal-level execution are historical and are not
> current operating authority. Start with
> [`docs/SYSTEM_ORIENTATION_CURRENT.md`](docs/SYSTEM_ORIENTATION_CURRENT.md)
> and [`config/project_layout_v1.json`](config/project_layout_v1.json).
> Practice account 007 is flat and fail-closed: a new entry requires a genuine
> lifecycle `confirmed_candidate` plus an exact, fresh canary authorization.
> Real-money routing is disabled.

> **Start here:** the canonical source, live runtime, and data root are all under
> `C:\Users\zmoor\Documents\forex\trad`. The dashboard is
> `http://127.0.0.1:8765/` when its hidden process is running. Current unfinished
> work is recorded only in
> [`FOREX_PENDING_IMPROVEMENTS.md`](FOREX_PENDING_IMPROVEMENTS.md), and immutable
> decisions are recorded in [`FOREX_PROJECT_LOG.md`](FOREX_PROJECT_LOG.md).
> Older account and canary material below is historical context, not operating
> authority.

> **2026-07-21 signal-engine refresh:** read
> [`docs/SIGNAL_ENGINE_REFRESH_20260721.md`](docs/SIGNAL_ENGINE_REFRESH_20260721.md)
> and
> [`data/oanda_training_manager/reports/modern_model_gap/signal_engine_refresh_latest.json`](data/oanda_training_manager/reports/modern_model_gap/signal_engine_refresh_latest.json).
> The engine now exposes raw, filtered, and execution consensus layers and an
> audited cross-process route for every implemented model adapter.

> **2026-07-27 live-quality upgrade:** read
> [`docs/EIGHT_HOUR_IMPROVEMENT_20260727.md`](docs/EIGHT_HOUR_IMPROVEMENT_20260727.md).
> The S1 research collector now records a 69-field causal quote/liquidity,
> activity, spread-dynamics, and leave-one-pair-out cross-pair feature set. A
> reusable chronological gate audit prevents in-sample threshold sweeps from
> authorizing account execution. The first prospective liquid-pair benchmarks
> did not clear spread and remain shadow-only.

> **2026-07-27 intensive MA-grid validation:** read
> [`docs/MA_FEATURE_GRID_INTENSIVE_V1.md`](docs/MA_FEATURE_GRID_INTENSIVE_V1.md).
> Six globally purged Ridge/XGBoost controls fitted 144 cells across all-pair,
> no-context, and liquid-pair panels. No direction-and-cost or movement-gate
> cell replicated across validation and holdout, so every MA challenger remains
> shadow-only.

> **2026-07-27 all-pair news consolidation:** read
> [`docs/ALL_PAIR_NEWS_EVENT_TAGGING.md`](docs/ALL_PAIR_NEWS_EVENT_TAGGING.md).
> The canonical event pipeline merges curated historical events, verified GPT
> news watches, and timestamped monitor context; generates explicit context for
> all 68 pairs; classifies all 7,048 significant moves; and feeds pair-aware
> evidence plus live movement links into the v5.24 GPT practice manager.

## Fresh ChatGPT Handoff Prompt

Paste this into a fresh ChatGPT/Codex chat when transferring ownership. The new
chat must inspect current files and runtime state because the project is live and
changes continuously.

```text
You are taking over a local Forex research, paper-execution, backtesting, and
monitoring project. Work as a cautious senior quant/software engineer. Read the
repository and live state before changing anything; do not assume this prompt is
newer than the files.

LOCATIONS AND SOURCE OF TRUTH
- Active Forex project root: C:\Users\zmoor\Documents\forex
- Canonical editable source and runtime: C:\Users\zmoor\Documents\forex\trad
- Runtime data: C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager
- D:\forex\trad is a stale legacy copy and is not live truth.
- Crypto public-data shadow store: D:\crypto_shadow\data
- OneDrive vault: C:\Users\zmoor\OneDrive\thevault
- Portable source/runtime ZIPs: C:\Users\zmoor\OneDrive\thevault\projects\forex
- Dashboard: http://127.0.0.1:8765/
- Read trad\README.md, trad\docs\OANDA_LAB_OPERATOR_GUIDE.md,
  docs\CHAT_HANDOFF_20260718.md,
  docs\GPT_ACCOUNT_002_WIRING_AUDIT_20260718.md,
  docs\MODEL_GAP_ROADMAP_20260718.md,
  trad\docs\POST_VALIDATION_DELTA_20260715.md,
  trad\config\accounts_registry.json, and
  trad\oanda_always_on_supervisor.ps1 first.

SAFETY AND EXECUTION
- Treat every configured OANDA account in this project as practice-only. Never
  enable or route real money, and never submit or close a discretionary/manual
  trade.
- Practice account -007 is fail-closed for new entries. An entry requires an
  immutable lifecycle `confirmed_candidate` plus an exact, fresh, unexpired,
  one-time canary authorization for the signal, proof cohort, and allocator
  cohort. Research reports and shadow collectors cannot self-authorize.
- Include entry and exit spread, slippage/fees where applicable, exact horizon,
  pair, timeframe, split, sample count, and drawdown in comparisons.
- Never compare incompatible report metrics or treat raw accuracy and implausible
  compounded returns as deployment evidence.
- Preserve active SQLite/Parquet data and user changes. Do not archive credentials,
  virtual environments, active WAL files, or large rolling market data.
- Use `C:\Users\zmoor\Documents\forex\trad` as the canonical runtime and data
  root. Keep current source, model metadata, results, and summaries in the
  OneDrive vault; raw rolling market data remains outside compact vault packages.
- Chats do not share context. Use one writer chat for canonical code changes.
  A second chat can inspect or analyze in parallel, but it must not deploy or
  restart workers. The guarded deploy lock and the supervisor global mutex are
  the project-level concurrency controls. Read
  `trad\docs\DEPLOYMENT_CONCURRENCY.md`.

CURRENT ARCHITECTURE
- oanda_always_on_supervisor.ps1 keeps C-based collectors, dashboard, labs,
  account snapshots, crypto tracker, and selected practice managers alive.
- The canonical live forecast matrix contains 224 physical inputs and 3,584
  input/horizon cells before pair expansion. The strategy lab evaluates 208
  physical lanes: 52 strategy families x 4 profiles. The continuous equation
  layer adds 13 input-timeframe lanes from S5 through H4, and the S1 ridge layer
  adds three timing lanes. Every compatible input can emit across 16 exact
  outcome horizons from 30 seconds through 24 hours; S1 also retains a separate 15-second
  timing result. Every generated setup contributes to a correlation-grouped,
  reliability-weighted final consensus. Accepted, near-miss, hard-reject,
  shadow-only, opposing-side, and timing inputs remain separately identified;
  only accepted account-eligible structural support can authorize execution.
  Input timeframe, model family, variant, pair, outcome horizon, setup class,
  and execution eligibility are separate matrix dimensions. Partial model curves
  vote only at horizons they explicitly predict. See
  `..\docs\ALL_SIGNAL_MATRIX_FINALITY_20260718.md`.
- `oanda_signal_contribution_feed.py` is the canonical producer boundary. It
  registers all 30 modern model-gap adapters, accepts fresh finite forecast
  curves from independent model/GPT processes, rejects stale or malformed
  outputs with an audit reason, and never turns historical benchmark rows into
  live predictions. All valid signals enter the raw research matrix. Quality
  filters attenuate the filtered matrix, while only policy-authorized,
  cost-clearing structural signals enter the execution matrix.
- `oanda_model_gap_live_signal_worker.py` has a nonblocking singleton lock and
  explicit startup phases. Fresh model inference publishes before historical
  outcome maturity, preventing the backlog from consuming the live signal's
  freshness window.
- oanda_lane_promotion.py and oanda_lane_promotion_fit.py maintain research
  evidence only. No report, score, legacy qualification, or producer output can
  assign a model or open a position on account -007. The current lifecycle has
  zero confirmed candidates, so the supported account decision is `no_trade`.
- Candidate ordering is normalized by confidence-adjusted executable return per
  margin-hour, spread cost, current depth/liquidity when available, and an
  instrument liquidity prior. Raw pips and confidence remain visible, while
  `normalized_rank_score` is the primary cross-pair ordering field.
- oanda_signal_combination_audit.py stores compressed once-per-candle feature
  snapshots and strategy-family votes. oanda_signal_combination_fit.py mines
  fuzzy 2-3 condition rules, selects them on the oldest 70%, and reports only
  rules that retain direction and positive cost-aware edge on the newest 30%.
- oanda_strategy_exit_fit.py tracks executable-price path barriers for every
  accepted signal and near miss. The practice executor can consume a fitted
  stop/target only after a separate chronological holdout and time-block gate.
- oanda_practice_micro_pattern_lab.py logs causal quote-level pattern and online
  equation forecasts at fixed intrasecond and next-minute-boundary horizons.
- oanda_second_forecast_runner.py and oanda_second_forecast.py implement the S1
  layer of that same matrix. They convert the OANDA stream to a causal one-second
  grid, evaluate compact in-memory ridge snapshots every second across 15 seconds
  through H1, submit candidates before SQLite work, and record sampled forward
  outcomes. Separate hot and research-tracker processes are only an operational
  latency boundary; they are not a separate model product or evidence namespace.
  The current S5
  historical sweep is not profitable after spread, so `-007` execution remains
  controlled by the persistent live promotion gate. See
  trad\docs\SECOND_FORECAST_RUNBOOK.md.
- A dedicated five-second research process stores prospective quote snapshots
  and executable outcomes without publishing to the shared account ledger. Its
  69-field feature payload includes top and total bid/ask liquidity, depth
  geometry, quote-flow and activity acceleration, tick imbalance, spread
  dynamics, price efficiency, and causal leave-one-pair-out cross-pair
  strengths. `oanda_second_microstructure_panel.py` converts only matured rows
  to Parquet; `oanda_shared_panel_model_benchmark.py` evaluates them with
  chronological folds; `oanda_live_signal_gate_audit.py` selects thresholds on
  an oldest 70% fit segment and reports a never-tuned newest 30% holdout.
- `oanda_two_stage_microstructure_benchmark.py` separates spread-clearing
  movement opportunity from LONG/SHORT direction. It can select a trailing
  direction-training window on calibration data, but its combined chosen-side
  economics must still pass the untouched holdout. The first enriched run
  improved holdout direction AUC to 0.610 and remained negative after spread,
  so the artifact is shadow-only.
- oanda_depth_parquet_collector.py samples all tradeable OANDA instruments at
  one-second cadence and stores broker price/liquidity/activity features as
  immutable compressed parts in hourly partitions.
- `oanda_timeframe_matrix_calibration.py` causally calibrates raw equation
  probabilities with expanding walk-forward bins and reports pair/timeframe/
  horizon Brier score, accuracy, executable net pips, confidence bounds, and
  independent blocks. A passing surface remains shadow-only.
- `oanda_ma_feature_grid.py` and `oanda_ma_feature_grid_fit.py` implement the
  strict moving-average-only forecast family. The current artifact covers 610
  causally resolvable input-timeframe/outcome-horizon cells from observed S5
  through D1 and 24 hours, with separate direction, signed-pip, magnitude, and
  executable-cost metrics. It contributes shadow forecasts only. See
  `trad\docs\MA_FEATURE_GRID.md`.
- `oanda_shadow_outcome_compactor.py` maintains hourly rollups and archives
  detailed outcomes older than 30 days to Zstd Parquet before deleting those
  rows from active SQLite. See
  `trad\docs\ROBUSTNESS_UPGRADE_20260716.md`.
- oanda_account_snapshot_writer.py polls all registered practice accounts and the
  three configured live accounts through read-only GET endpoints for balance,
  NAV, P/L, trades, and orders. Live orders are never modified by this writer.
- crypto_shadow_live_tracker.py consumes unauthenticated Coinbase public ticker
  and trade channels for eight USD products and produces shadow-only forecasts at
  5/30/60/300 seconds. It has no order route.
- oanda_practice_live_dashboard.py unifies full account tables, normalized
  aggregates, strategy layers, model search, calibration, signals, definitions,
  pattern/equation outcomes, reports, horizons, and timeframes.

MODEL AND DATA LAYERS
- Live strategy families include momentum, reversal, breakout, trend, currency
  strength, relative value, supervised rank, higher-timeframe alignment,
  regression, regime, volume/activity, spread/liquidity, and pattern-count lanes.
- Micro models include discrete U/D pattern counts and online continuous linear
  equations. Inspect each equation's actual fit method, feature vector, smoothing,
  target, and variable/fixed horizon; names alone are insufficient.
- Historical layers include S5/S30/M1/M5 replay, exact-horizon explorers,
  ensembles, HGB reversal, ARIMA/SARIMA challengers, GPT account managers, and
  older vault-indexed reports.
- The strict MA feature grid contains 27 input timeframes, 26 requested outcome
  horizons, and 610 fitted cells. Its first full-span chronological run found
  useful movement-magnitude information but no validation/holdout execution
  gate that replicated, so every cell remains shadow-only.
- The legacy HGB live-outcome tracker is retired. It consumed a stale D-drive
  input, had no pending work, and did not establish a promotable result; its code
  and historical evidence remain preserved for audit only.
- The broad SARIMA run covered 15 pairs and an M1-derived 120-minute target (270
  fits / 135 unique models). It did not cover every input timeframe and horizon.
- OANDA cannot backfill true S1 ticks. Preserve collected S1 and use S5 candles
  for broader historical backfill.

ACCOUNTS
- Registry contains practice accounts -001 through -020.
- Practice -007 is the governed account in current scope. It remains flat and
  fail-closed unless an exact confirmed cohort receives a narrow canary
  authorization. Other account-manager descriptions in dated reports are
  historical and do not grant current entry authority.
- Compare accounts with equal-weight normalized return and a 100-start index;
  raw sums are misleading because starting sizes and histories differ.

VALIDATION STATE
- Vault build unified_query_20260713_v3 passed catalog integrity at
  2026-07-14T00:04:05Z: 46,368 runs, 668,084 metrics, and 643 models reconciled.
- It explicitly excluded trade readiness, raw candles, serialized models,
  credentials, and live account state.
- Later documentation edits changed all 140 Forex model-page hashes and added an
  unmanifested rules file. Rebuild and revalidate before calling the vault
  byte-for-byte current.
- Read docs\POST_VALIDATION_DELTA_20260715.md for known changes and pending work.
- The modern gap registry contains 30 implemented adapter identities in seven
  families. Twenty-eight have bounded-market evidence; TimesFM-ICF and Mamba are
  runtime-blocked on this host. None is production-eligible. The signal route is
  implemented for all 30, but a model contributes live values only while its
  producer publishes fresh forecasts. Read
  `trad\docs\SIGNAL_ENGINE_REFRESH_20260721.md` and
  `..\docs\MODEL_GAP_ROADMAP_20260718.md`.

OPERATING PROCEDURE
1. Confirm Forex process command lines point to
   C:\Users\zmoor\Documents\forex\trad. Ignore unrelated
   BIGTRIAD processes unless asked to work on them.
2. Check dashboard, snapshot ages, stream gaps, disk growth, account freshness,
   unprotected trades, and supervisor events.
3. Run focused tests before deployment, preserve causal evidence, update the
   project log, and verify process health afterward.
4. Keep new robust equations, k-means regimes, cross-pair signals, correlation
   matrices, and ruleset ensembles shadow-only until cost-aware outcomes mature.
   Treat the fuzzy combination rule's support, chronological holdout probability,
   lower probability edge, and expected net pips as separate required evidence.
   Treat the inverse-correlation lane as shadow-only until its independent
   cost-aware holdout also qualifies.
5. Report exact limitations and pending validation. Never claim profitability.

Prospective microstructure panel and gate-audit commands are documented in
`trad\docs\EIGHT_HOUR_IMPROVEMENT_20260727.md`. Do not run long reads directly
against the active microstructure SQLite database; briefly pause only that
collector or work from a stable copy.

The user wants an exhaustive searchable system: all model families, variants,
timeframes, horizons, signals, misses, what-ifs, equations, pattern counts,
historical runs, accounts, metric definitions, and live balances should be visible
in the dashboard. Continue from current code and data, preserve causal evaluation,
and document every material change.
```

This folder is a working research and execution package for OANDA FX model
development. It contains three main surfaces:

1. Research pipelines that build feature matrices, train candidate models, and
   produce candidate trade rows.
2. Live-like replay/backtest tools that simulate OANDA-style account handling,
   margin, spreads, stops, rotations, and position exits.
3. Demo/live execution bots that consume the selected model stream and manage
   OANDA accounts.

The current canary deployment is documented in
[`docs/LIVE_CANARY_RUNBOOK.md`](docs/LIVE_CANARY_RUNBOOK.md). The model and
backtest registry is documented in
[`docs/BACKTEST_AND_MODEL_REGISTRY.md`](docs/BACKTEST_AND_MODEL_REGISTRY.md).

## Current Source Of Truth

Use these files first when checking what is wired:

| Purpose | Path |
|---|---|
| Canary demo config, account ending 004 | `config/canary_primary_forecast_rotation_bot.json` |
| Disabled tech003 demo config | `config/tech003_primary_forecast_rotation_bot.json` |
| Primary live config | `config/primary_forecast_rotation_bot.json` |
| Main execution bot | `oanda_primary_forecast_rotation_bot.py` |
| Strict OANDA-style replay engine | `oanda_broker_style_portfolio_replay.py` |
| Parameter sweep runner | `oanda_fresh_live_like_parameter_sweep.py` |
| Feature-space registry | `config/model_feature_space.json` |
| Account/env alias registry | `config/accounts_registry.json` |

## Selected Canary Model

The canary account is configured to use:

- Stream: `fresh_fullhist_m30_h1_h4_continuation_oanda_20260707`
- Model family: `HistGradientBoostingClassifier`
- Feature set: `technical_full`
- Target: `reversal_curve_profit_120`
- Execution policy: `follow_momentum`
- Inputs: M30, H1, and H4 full-history candidate streams
- Overlay: M1 hold filter from
  `data/oanda_training_manager/reports/m1_primary_overlay_fresh_fullhist_maxnew8_jump_narrow_20260707`

The selected operational config is the best practical strict path-quality setup:

- `atr_stop_multiplier`: `1.5`
- `take_profit_edge_capture`: `0.6`
- `take_profit_min_r_multiple`: `0.5`
- `trailing_stop_r_multiple`: `1.0`
- `max_new_positions_per_cycle`: `12`

It was selected over max-new 16 because max-new 16 only narrowly beat it in the
strict path-quality sweep while using slightly more throughput/exposure.

## Repository Map

| Area | Description |
|---|---|
| `config/` | JSON configs for account routing, model streams, feature spaces, and runtime aliases. See `config/README.md`. |
| `data/oanda_training_manager/` | Research datasets, experiment specs, candidate rows, sweep reports, and model artifacts. |
| `data/technical_scout_manager/` | Runtime state for account managers: decisions, forecasts, state, actions, logs, and model caches. |
| `archive/` | Older launch wrappers and historical operational files. |
| `mt5/` | MetaTrader experiments unrelated to the current OANDA canary. |
| `oanda_primary_forecast_rotation_bot.py` | Main model-stream forecast and rotation executor. |
| `oanda_broker_style_portfolio_replay.py` | OANDA-style replay engine with path-quality, MAE/MFE, and margin handling. |
| `oanda_fresh_live_like_parameter_sweep.py` | Grid runner for live-like parameter sweeps over stops, pacing, exits, friction, and risk. |
| `oanda_m1_primary_overlay_research.py` | M1 overlay research used to filter active candidates. |

## Main Workflows

### Run Canary Demo

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe `
  .\oanda_primary_forecast_rotation_bot.py `
  --mode demo `
  --source oanda `
  --config .\config\canary_primary_forecast_rotation_bot.json `
  --execute
```

### Run One Advice-Only Cycle

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe `
  .\oanda_primary_forecast_rotation_bot.py `
  --mode demo `
  --source oanda `
  --config .\config\canary_primary_forecast_rotation_bot.json `
  --once
```

### Run Strict Path-Quality Sweep

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe `
  .\oanda_fresh_live_like_parameter_sweep.py `
  --output-root .\data\oanda_training_manager\reports\path_quality_run `
  --preset intensive `
  --config .\config\canary_primary_forecast_rotation_bot.json `
  --candidate-rows-csv .\data\oanda_training_manager\reports\path_quality_inputs_20260708\candidate_rows_m1_oos_hold_ge_050.csv `
  --start-equity 1000 `
  --path-strict `
  --rank-by path_quality
```

## Important Backtest Caveat

The replay can show very high trade win rates while model endpoint accuracy is
much lower. That is expected for this style of trader: it tries to harvest
favorable movement inside the forecast window. For the selected wide-new12 run,
trade win rate was about 92.9%, but executed forecast endpoint accuracy was about
61.8%. See `docs/BACKTEST_AND_MODEL_REGISTRY.md` for the detailed interpretation.

## Packaging Notes

This package is self-contained only if the large data artifacts under `data/`
are included. If a handoff excludes parquet/model/report artifacts, regenerate
them with `regenerate_handoff_artifacts.py` or the relevant research scripts
listed in `docs/BACKTEST_AND_MODEL_REGISTRY.md`.

Do not commit or share credential files. The local `creds` file and environment
variables are machine-specific.
