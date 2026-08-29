# OANDA Practice Lab Operator Guide

## Runtime Layout

- Workspace source: `D:\forex`
- Active runtime mirror: `D:\forex`
- Dashboard: `http://127.0.0.1:8765/`
- Runtime data root: `D:\forex\trad\data\oanda_training_manager`
- Logs: `D:\forex\trad\data\oanda_training_manager\logs`
- Reports: `D:\forex\trad\data\oanda_training_manager\reports`
- Models: `D:\forex\trad\data\oanda_training_manager\models`
- Vault snapshot: `D:\vault_backups\thevault_snapshot_20260714_134039`

## Always-On Components

- `oanda_always_on_supervisor.ps1` keeps the live collectors and labs running.
- `oanda_practice_live_dashboard.py` serves the browser dashboard.
- `oanda_practice_shadow_strategy_lab.py` evaluates shadow/paper strategy lanes.
- `oanda_signal_combination_fit.py` audits fuzzy two/three-signal rules against a
  newer chronological holdout; its SQLite input is written by the strategy lab.
- `oanda_strategy_exit_fit.py` stores path-aware outcomes and exposes only exits
  that pass training, holdout, confidence, profit-factor, and time-block gates.
- `oanda_lane_promotion_fit.py` incrementally compacts signal outcomes and fits
  persistent lane/horizon execution evidence without querying the large path
  ledger from the live trading loop.
- `oanda_practice_micro_pattern_lab.py` tracks intraminute pattern and equation forecasts.
- `oanda_depth_parquet_collector.py` stores order-book/depth snapshots.

The execution path is practice/demo only unless a script is explicitly changed to use a live account. Shadow lanes do not need an account because they score signals independently from the price stream.

## Data Streams

- S1 live pricing is collected from OANDA streaming snapshots.
- S5 candle backfill is stored separately and should be used when S1 history is missing.
- S30, M1, and M5 are derived from S5 or native candle data for slower horizon checks.
- Order-book/depth data is written as partitioned parquet to avoid one huge flat file.
- `D:\forex` on the 5 TB D drive is the canonical data root. Keep compressed
  partitions by default; apply rolling retention only when a configured free-space
  watermark requires it.
- Micro/equation forecasts are summarized into JSON snapshots for the dashboard; the raw SQLite stream can be large and should not be queried directly by the dashboard loop.

## Model Groups

- Unified matrix: 159 physical lanes and 1,119 lane-horizon surfaces. The M1 and
  multi-context core contributes 39 families x 4 profiles (156 physical lanes), including explicit
  trend/momentum, breakout/volume, oscillator/reversion, cross-market
  confluence, automatically mined fuzzy signal combinations, and a shadow-only
  inverse-correlation veto family.
- Outcome surfaces: every physical lane is scored independently at 60, 180, 300,
  600, 900, 1800, and 3600 seconds. This is 1,092 lane-horizon surfaces from one
  shared feature pass; H1 is an outcome horizon, not a slower pricing poll.
- S1 ridge surface: `ridge_return` has fast/balanced/strict lanes scored at 15,
  30, 60, 180, 300, 600, 900, 1800, and 3600 seconds. These 27 surfaces use the
  same matrix/promotion keys; separate hot/tracker processes are only a latency
  implementation detail. Historical fitting currently uses S5 as the declared
  training proxy while live decisions use the S1 grid.
- Pattern count forecasts: U/D sequence count models with historical plus live frequency and forward outcome tracking.
- Microstructure models: intraminute and equation-style models using live quote changes, movement coefficients, spread, and pattern sequences.
- Ensembles: independent signal combinations from accepted and near-threshold shadow lanes.
- Historical/timeframe reports: S5, S30, M1, and M5 replay reports with horizon-specific outcomes.

## Dashboard Reading

- `Signal WR` is accepted setups divided by raw setups. It measures selectivity, not trading profitability.
- `Win` is matured accepted outcome win rate at the selected horizon.
- `Avg net` is bid/ask cost-aware average theoretical pips.
- `Near avg` shows what missed threshold signals would have done.
- `Net-return promotion` is the practice-account gate. It uses non-overlapping
  horizon blocks, oldest-70% training, newest-30% holdout, and pair/session
  breadth. `historical_backfill` means execution is intentionally held.
- Pattern `Future / direction` is bold because it is the realized forward result being compared to the prediction.
- Continuous equation plots compare predicted signed move against matured future signed move from the latest snapshot.

## Useful Checks

```powershell
D:\forex\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe D:\forex\trad\oanda_live_account_readonly_status.py --all-practice --role practice_007 --json
```

```powershell
Invoke-WebRequest http://127.0.0.1:8765/api/state -UseBasicParsing
```

```powershell
D:\forex\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe -m py_compile D:\forex\trad\oanda_practice_live_dashboard.py D:\forex\trad\oanda_practice_shadow_strategy_lab.py
```

## Packaging

The vault zip is a runnable code/docs package. It includes the compact S1 ridge
model snapshot and fit report, but intentionally excludes large live databases,
parquet stores, Python virtual environments, and full event logs. Those remain
in `D:\forex\trad\data\oanda_training_manager`.
