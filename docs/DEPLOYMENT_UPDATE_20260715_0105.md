# Forex Deployment Update - 2026-07-15 01:05 EDT

## Practice account routes

- Practice `-002` now runs the operator-supplied
  `oanda_forex_gpt_advisor_all_pairs_v5_24_movement_ledger.py` checkpoint.
- The checkpoint source is byte-identical to the supplied download. SHA-256:
  `6FC030597E8CADD14A8C8FA6CDAB052E2D49B70FE84FCA02CA8DCF70EEFED28F`.
- Resolved broker environment is `practice`; `FOREX_ALLOW_LIVE` is false; data,
  decisions, recaps, and movement ledgers resolve under `D:\forex\trad\data`.
- The script was started with `--no-scan-on-launch --normal-mode`. It inherited
  two open trades and six pending orders already present on practice `-002`.
- Practice `-007` was not changed.
- Practice `-019` runs the guarded HGB M30/H1/H4 forecast-rotation lane with a
  four-position cap, 0.25% maximum risk per trade, 1.5% maximum open risk, and
  no live-account route.

## Matched live HGB outcomes

- `oanda_hgb_live_outcome_tracker.py` ingests the HGB forecast journal.
- Repeated 60-second model cycles are de-duplicated by model event, pair,
  direction, source stream, input timeframe, and model feature timestamp.
- Outcomes are sampled only after the declared horizon matures. Long forecasts
  use entry ask and exit bid; short forecasts use entry bid and exit ask.
- Only accepted forecasts expose `win` to historical-to-live calibration.
  Rejected forecasts retain a separate `shadow_win` field.
- Initial fresh cycle: 192 pending 120-minute forecasts and zero mature
  outcomes, which is the expected causal state.
- At 01:08 EDT the de-duplicated journal held 318 pending forecasts: 126 M30,
  130 H1, and 62 H4. Two passed the execution gate; none had matured.
- The guarded `-019` lane filled one 54-unit EUR/JPY practice trade with an
  estimated risk of $0.123838 (0.247625% of equity), inside the 0.25% cap.

## ARIMA/SARIMA coverage

- Installed `statsmodels 0.14.6` in the D-drive Python 3.13 research runtime.
  The old virtual environment remains unusable because it references a removed
  C-drive Python installation and is not used by the supervisor.
- `oanda_arima_h1_baseline_grid.py` now supports actual M1 candle input and
  M1/M5/M15/M30/H1/H4 resampling. Input timeframe and outcome horizon are
  written separately in every result row.
- A 15-minute smoke run completed without errors. The tested 15- and 60-minute
  outcomes were negative after spread and nothing was promoted.
- `oanda_arima_multiframe_sweep.py` is running four ARIMA variants across seven
  majors, two walk-forward windows, six input timeframes, and timeframe-specific
  horizons. It is research-only and cannot place orders.

## Equation soak and pruning

- Six experimental variants matured approximately 116,000-120,000 forecasts
  each: Huber, k-means-4 regime, and cross-relative equations at 2 seconds and
  the next minute boundary.
- Their direction accuracy remained approximately 47.6%-48.9%, with average
  executable net near -39 pips across the all-pair stream.
- These six variants are retired from active forecast emission. Their full
  database history and latest metrics remain visible as `Retired equation`
  entries. Fifteen micro models remain active, including the established
  pattern and continuous-equation families.

## Dashboard

- Account Aggregate remains at the top.
- `Top Forecasts and Signals` now follows it. Forecasts are ranked current
  predictions; signals include only strategy outputs that passed acceptance
  thresholds.
- Strategy leaderboard, patterns, equations, ensembles, calibration, model
  registry, strategy layers, blockers, and historical runs are independently
  collapsible and closed by default.
- Multiframe ARIMA progress and retired equations are included in model search.

## Vault and D-drive validation

- `forex_model_vault_sync.py` runs every 15 minutes and creates a bounded,
  credential-free checkpoint containing source, configuration, documentation,
  and deployed fitted-model caches. Raw market data is not duplicated.
- Current checkpoint: 10,581 files, 256,538,135 bytes before ZIP compression.
- Both vault copies have identical ZIP SHA-256:
  `19C0877C7BF62B18711049F77D6B0628459DE6850D9F1D6D3C82D714AB207B58`.
- Destinations:
  - `C:\Users\zmoor\OneDrive\thevault\projects\forex\forex_model_checkpoint_current.zip`
  - `D:\vault_backups\thevault_snapshot_20260714_235524\projects\forex\forex_model_checkpoint_current.zip`
- Runtime process audit found no Python or PowerShell Forex process executing
  from `C:\Users\zmoor\Documents\forex`. The canonical runtime is `D:\forex`.

## Naturally pending evidence

- HGB live outcomes cannot exist until their 120-minute targets mature.
- The six-timeframe ARIMA sweep is still running; each completed timeframe is
  written to the dashboard and its own immutable CSV/JSON report.
- These pending observations do not authorize model promotion or live trading.

## Verification

- Focused regression suite: 54 tests and 132 subtests passed.
- Covered dashboard aggregation, micro/equation persistence and retirement,
  pattern forecasts, strategy lanes, ensemble replay, ARIMA features, and
  SARIMA launcher safety.
- The served dashboard contains the top forecast/signal section and nine
  collapsible analytical groups. API state reports 15 active and six retired
  micro models.
