# Fresh M1 Intrahour FX Opportunity Engine

This folder is a clean research engine for short-horizon OANDA FX opportunities.
It does not reuse the old reversal target, old promotion flow, or the
`follow_momentum`/reversal-side logic from the primary rotation bot.

The unit of research is:

```text
decision_time_utc, instrument, side
```

Each instrument produces both `long` and `short` rows. Labels and features are
side-normalized so the model learns whether that side can get paid after costs.

## Fixed-Policy Sweep

The current milestone is a reproducible, leakage-safe fixed-policy sweep. Each
exit policy is trained and scored independently:

- `leg_2_3_5m`: TP 2 pips, SL 3 pips, max hold 5m.
- `leg_3_5_10m`: TP 3 pips, SL 5 pips, max hold 10m.
- `leg_5_8_15m`: TP 5 pips, SL 8 pips, max hold 15m.
- `leg_8_12_30m`: TP 8 pips, SL 12 pips, max hold 30m.

The allocator chooses among side-policy candidates using only predicted
account-currency EV. It can take no trade. It does not use
`best_exit_policy`, realized best-policy labels, or any oracle policy selector.

This is research only. There is no live broker execution code here.

## Forecast Surface Sweep

The default pipeline now builds a leakage-safe forecast surface over:

```text
timestamp x pair x side x horizon x policy
```

The normal `all` command uses all available OANDA M1 pairs unless a narrower
`--tier` or `--pairs` argument is supplied. For every out-of-sample row, the
surface predicts endpoint net pips, account-currency EV, MFE, MAE,
TP-before-SL probability, stop-first probability, time-to-profit, calibration
bucket, and cross-sectional rank.

Feature families:

- `A_m1_price_action`
- `B_m1_spread_cost_liquidity`
- `C_m1_spread_currency_strength`
- `D_m1_spread_strength_cross_sectional`
- `E_full_session_htf_context`

Model families:

- `simple_baseline`
- `calibrated_classifier_ev_regressor`
- `tree_gradient_boosting`
- `direct_ev_regressor`

Candidate feature/model combinations are ranked by out-of-sample allocator
performance, not in-sample AUC. The allocator uses predicted EV only and can
take no trade.

## Commands

From `D:\forex\trad`:

Run the conservative bid/ask-aware label audit before model sweeps:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\fresh_m1_intrahour\run_pipeline.py audit-labels --start 2026-07-03 --end 2026-07-07 --pairs EUR_USD GBP_USD USD_JPY --tier tier1
```

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\fresh_m1_intrahour\run_pipeline.py --smoke
```

Run the full surface sweep over all available pairs:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\fresh_m1_intrahour\run_pipeline.py all --start 2026-06-01 --end 2026-07-07 --tier all
```

Run a resumable heavy sweep by pair tier:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\fresh_m1_intrahour\run_pipeline.py all --start 2026-06-01 --end 2026-07-07 --tier all --chunk-by-tier --run-name surface_full_tiered_20260709
```

Run resumable fixed-size pair chunks:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\fresh_m1_intrahour\run_pipeline.py all --start 2026-06-01 --end 2026-07-07 --tier all --pair-chunk-size 8 --run-name surface_full_pair_chunks_20260709
```

Completed chunks are skipped on rerun. Use `--force` to rebuild completed
chunks. By default, full all-candidate surfaces are not materialized; use
`--write-all-candidates` only when disk/memory use is acceptable.

Run a small validation-only surface smoke:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\fresh_m1_intrahour\run_pipeline.py all --start 2026-07-03 --end 2026-07-07 --pairs EUR_USD --max-rows-per-pair 600 --quick-surface-smoke
```

Run the classical AR/ARIMA sanity-check family and leak-free M1 validation replay:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\fresh_m1_intrahour\run_pipeline.py arima-baseline --start 2026-06-01 --end 2026-07-07 --tier tier1
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\fresh_m1_intrahour\run_pipeline.py validation-replay --start 2026-06-01 --end 2026-07-07 --tier tier1
```

Run the full H2 SARIMA grid over 2025 and the locally available 2026 period:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\fresh_m1_intrahour\run_pipeline.py sarima-sweep --start 2025-01-01 --end 2026-07-07 --tier tier1 --workers 3
```

The command reports 270 ARIMA/SARIMA parameterizations over `p,q=0..2`,
`d=0/1`, `P,D,Q=0/1`, and daily/weekly H2 seasonal periods `12/60`.
The stationary return and equivalent integrated-level forms require 135 unique
statistical fits. A low-iteration full-grid screen is followed by a cached,
high-iteration validation-only refinement of viable and leading candidates;
refined viability requires at least 12 of 15 pair fits to converge.
It uses the existing cached CPython 3.12 runtime and dormant statsmodels package;
it does not install dependencies. Thresholds are calibrated on September-October
2025, model structure is selected on November-December 2025, and 2026 is a
previously inspected diagnostic rather than a fresh holdout. Results are
research-only and execution remains disabled.

Audit the archived 227-feature M30/H1/H4 model with corrected HTF bar timing,
next-bar M1 bid/ask economics, fixed units, and development-only selection:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\fresh_m1_intrahour\run_pipeline.py htf-validation-replay --start 2026-04-01 --end 2026-07-07 --tier tier1
```

The HTF command uses 2025 archived rolling-OOS candidates for configuration
selection and freezes the result before evaluating the supplied 2026 period.
It is a salvage diagnostic, not fresh evidence: the archived curve target used
timeframe-mismatched path windows, and the 2026 period was previously inspected.

Rebuild the strongest archived H1 family with new exact 120-minute M1 path
labels, completed-bar timing, executable bid/ask costs, and rolling 2025 OOS
selection:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\fresh_m1_intrahour\run_pipeline.py htf-corrected-rebuild --start 2026-04-01 --end 2026-07-07 --tier tier1
```

This command ignores archived outcome targets and excludes the seven known
full-history volatility metadata features. The supplied 2026 period remains a
previously inspected diagnostic; a PASS for production still requires candles
strictly after 2026-07-07 and one frozen evaluation.

Build a dataset only:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\fresh_m1_intrahour\run_pipeline.py build --start 2026-06-15 --end 2026-07-07 --tier tier1
```

Train:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\fresh_m1_intrahour\run_pipeline.py train --dataset .\fresh_m1_intrahour\reports\<run>\side_dataset.parquet
```

Backtest:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\fresh_m1_intrahour\run_pipeline.py backtest --dataset .\fresh_m1_intrahour\reports\<run>\side_dataset.parquet --model .\fresh_m1_intrahour\reports\<run>\model_bundle.joblib
```

## Mandatory Model Validation

Run the complete 87-name ARIMA/SARIMAX, ATR/ADR, state-space, regime,
controller, ensemble, and offline execution-simulation programme on all 15
Tier-1 pairs:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\fresh_m1_intrahour\run_pipeline.py mandatory-model-validation --start 2024-07-01 --end 2026-07-07 --tier tier1 --workers 3 --maxiter 50 --run-name mandatory_model_validation_tier1_202407_202607_20260711_v3
```

The command uses local candles and the existing cached statsmodels runtime. It
does not install packages, load serialized trading models, connect to a broker,
or place orders. Model parameters are fit before September 2025, policy and
model selection use November-December 2025 validation only, and 2026 is a
previously available diagnostic. Consequently, this programme can validate an
implementation or invalidate a candidate, but cannot promote a canonical
winner without new candles after 2026-07-07.

The report folder includes the exact 87-model registry, pair-level fit
diagnostics, forecast and account metrics, gate attribution, cost stress, the
combined forecast surface, and a safety manifest. A suite PASS means all model
contracts completed without unstable forecasts, optimizer failures, fallbacks,
or invariant violations; it does not mean the models were profitable.

## Model Knowledge Catalogue

Build and validate the representative catalogue smoke before publishing a full
immutable forex-library build:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\fresh_m1_intrahour\model_library_catalogue.py smoke --project-root . --vault-root C:\Users\zmoor\OneDrive\thevault
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\fresh_m1_intrahour\model_library_catalogue.py full --project-root . --vault-root C:\Users\zmoor\OneDrive\thevault
```

Revalidate an existing build using only its vault contents:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\fresh_m1_intrahour\model_library_catalogue.py validate C:\Users\zmoor\OneDrive\thevault\FOREX_MODEL_LIBRARY\builds\<build_id>
```

The builder catalogues model families, exact variants, pair/ensemble
applications, evaluations, failures, legacy evidence, datasets, source, and
opaque serialized artifacts. Large candles, tables, and model binaries are
SHA-256 references rather than vault copies. High-volume runtime events are
collection datasets; embedded model manifests remain exact evidence. The
builder never imports project modules, deserializes model artifacts, connects
to OANDA, or enables execution.

## Outputs

Each run folder contains:

- `side_dataset.parquet`: side-specific feature/label matrix.
- `dataset_manifest.json`: data ranges, pairs, columns, leakage checks.
- `feature_family_report.json`: feature whitelists, excluded label-like
  columns, and feature-set signatures.
- `surface_label_audit.json`: bid/ask-aware label invariant checks.
- `surface_execution_audit.json`: entry delay, bid/ask source, spread stress,
  slippage, and same-bar ambiguity settings.
- `policy_model_bundle.joblib`: frozen sklearn models and feature list.
- `model_report.json`: aggregate holdout metrics, per-policy reports, and
  monthly walk-forward results.
- `policy_report_<policy>.json`: AUC, EV correlation, predicted EV deciles,
  account P/L by EV bucket, threshold counts, precision@1/3/5 by minute,
  pair/session/spread/ATR/side breakdowns, and opposite-side shadow outcome.
- `holdout_policy_predictions.parquet`: out-of-sample predictions by policy.
- `monthly_walk_forward.json`: expanding-window month-by-month policy results
  when the run spans enough months.
- `forecast_surface.csv`: winner model's out-of-sample forecast surface with
  the required timestamp/pair/side/horizon/policy prediction columns.
- `forecast_surface_all_candidates.parquet`: full feature-family/model sweep
  surface, including non-winner candidates and realized diagnostics.
- `surface_sweep_report.json`: feature/model ranking, allocator comparison,
  bucket monotonicity, monthly consistency, drawdown, churn, pair/session
  breakdown, and leakage checks.
- `chunked_run_manifest.json`: parent manifest for resumable chunked runs.
- `chunk_complete.json`: per-chunk completion marker used for resume/skip.
- `trades.csv`: allocator selected trades and exits.
- `equity_curve.csv`: account-currency equity curve.
- `shadow_diagnostics.csv`: selected-side vs opposite-side outcomes.
- `backtest_summary.json`: P/L, drawdown, top-k precision, churn, margin, and failure taxonomy.

## Design Notes

- Features only use candles at or before the decision timestamp.
- Labels begin strictly after the decision timestamp.
- Research labels use next-bar entry by default, executable bid/ask prices
  when present, conservative mid-plus-spread approximation otherwise,
  adverse-first same-bar ambiguity, and round-trip slippage.
- Account-currency P/L is calculated after execution spread and estimated
  slippage, with allocator switching cost reported separately.
- Policy selection is based on predicted policy EV only.
- Tier 1 excludes wide-spread exotics by default.
- No live trading, promotion, or account connection code is present in this folder.
