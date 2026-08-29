# Primary Forecast Rotation Bot

This is the all-68 forecast-ranking bot for the primary account lane. The live
primary config is wired to the six-bundle dual-history HGB model stream
`live_like_matrix_m30_h1_h4_dual_history_continuation_oanda`.

The current production stream removes the old reversal-direction ambiguity by
storing both the model target-side direction and the execution-side policy in
each forecast row. For the `reversal_curve_profit_120` HGB model, production
execution uses `execution_side_policy=follow_momentum`; forecast logs therefore
show the model's target side in `model_target_direction` and the tradable side
in `direction`.

It reads:

- `data/oanda_training_manager/reports/dual_history_live_candidate_calibration_20260705/candidate_rows_90d_m30.csv`
- `data/oanda_training_manager/reports/dual_history_live_candidate_calibration_20260705/candidate_rows_90d_h1.csv`
- `data/oanda_training_manager/reports/dual_history_live_candidate_calibration_20260705/candidate_rows_90d_h4.csv`
- `data/oanda_training_manager/reports/dual_history_live_candidate_calibration_20260705/candidate_rows_full_m30.csv`
- `data/oanda_training_manager/reports/dual_history_live_candidate_calibration_20260705/candidate_rows_full_h1.csv`
- `data/oanda_training_manager/reports/dual_history_live_candidate_calibration_20260705/candidate_rows_full_h4.csv`
- `data/oanda_training_manager/continuous_research/technical_spike_research_30m_step1.parquet`
- `data/oanda_training_manager/continuous_research/technical_spike_research_1h_step1.parquet`
- `data/oanda_training_manager/continuous_research/technical_spike_research_4h_step1.parquet`
- live OANDA `M30` candles, resampled to M30/H1/H4 feature rows when launched with `--source oanda`
- primary account credentials from `creds` via `OANDA_ACCOUNT_LIVE_PRIMARY` and `OANDA_LIVE_API_KEY`

Default behavior is advice-only. It does not place live orders unless the config
and command line both explicitly enable live execution.

## Advice Smoke

```powershell
.\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe .\trad\oanda_primary_forecast_rotation_bot.py --once --mode advice --source local --max-pairs 8
```

## Paper Loop

```powershell
.\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe .\trad\oanda_primary_forecast_rotation_bot.py --mode paper --source local
```

## OANDA Read-Only Forecast Loop

```powershell
.\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe .\trad\oanda_primary_forecast_rotation_bot.py --mode advice --source oanda
```

## Live Switch

Before live execution can place orders:

1. Confirm `live_execution_enabled` and `live_new_entries_enabled` are `true` in `config/primary_forecast_rotation_bot.json`.
2. Keep `FOREX_ALLOW_LIVE=True` and `FOREX_LIVE_EXECUTE=True` in `creds`.
3. Launch with both live flags:

```powershell
.\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe .\trad\oanda_primary_forecast_rotation_bot.py --mode live --source oanda --execute --confirm-live PRIMARY_FORECAST_ROTATION_LIVE
```

The primary live config currently has `live_execution_enabled=true` and
`live_new_entries_enabled=true`; live execution still also requires the launch
command flags and live credentials. The active guard profile is the
dual-history M30/H1/H4 growth profile:
30 max open positions, 2 new positions per cycle, 7.2% target open risk, 9.5%
hard open risk, 5.0% max gross risk per currency, 68% target margin, 82% hard
margin, 90% emergency margin, 7% daily loss kill, 18% drawdown kill, drawdown
risk throttling after 10%, and pair-level daily/total/streak loss cooldowns.

The active execution profile is the OANDA `wider_stop` winner: ATR stop `1.25`,
take-profit edge capture `0.50`, minimum TP R multiple `0.40`, trailing stop
R multiple `0.80`, minimum trailing spread multiple `2.5`, minimum edge `0.5`
pips, and maximum spread/edge ratio `0.8`.

## Outputs

- `data/technical_scout_manager/account_live_primary_forecast_rotation/latest_decision.json`
- `data/technical_scout_manager/account_live_primary_forecast_rotation/latest_forecasts.csv`
- `data/technical_scout_manager/account_live_primary_forecast_rotation/state.json`
- `data/technical_scout_manager/account_live_primary_forecast_rotation/actions.csv`

## Core Behavior

Each cycle:

1. Builds current technical feature rows from fresh candles.
2. Scores calibrated pairs with cached M30, H1, and H4 HGB full-model bundles for both 90-day and full-history training windows.
3. Converts model probability and calibrated expected move into edge pips, risk pips, stops, targets, and a normalized rank score.
4. Ranks all pairs globally.
5. Opens high-ranking paper/advice/live positions when slots are available.
6. Closes stale positions when their horizon expires, signal decays, an opposite signal wins, or stop/target logic fires.
7. Replaces the stalest weak position when a materially better forecast appears.

Safety gates include spread limits, weekend/rollover blocks, pair cooldowns,
pair daily/total loss stops, pair loss-streak cooldowns, gross currency risk
caps, portfolio open-risk caps, drawdown risk throttling, max open positions,
replacement throttling, daily loss kill switch, drawdown kill switch, and
required stop-loss/take-profit on live orders.

## Model Stream

The current live stream uses six full HGB bundles from the same expanded
technical model experiment:

- `exp_20260702_022306_e09b53ec8d`, `technical_full`, target `reversal_curve_profit_120`
- source streams: M30, H1, H4, each with trailing-90-day and full-history variants
- execution side policy: `follow_momentum`
- OANDA live candle source: `M30`, resampled to each model timeframe
- shared training row cap: `300000`

Live bundle layout:

| Bundle suffix | Feature base | Training window | Current train rows |
|---|---:|---:|---:|
| `m30_90d` | M30 | trailing 90d | 179,739 |
| `h1_90d` | H1 | trailing 90d | 90,263 |
| `h4_90d` | H4 | trailing 90d | 22,235 |
| `m30_fullhist` | M30 | full history, capped to latest 300k rows | 300,000 |
| `h1_fullhist` | H1 | full history, capped to latest 300k rows | 300,000 |
| `h4_fullhist` | H4 | full history, uncapped by row limit | 119,745 |

Dual-history OANDA-style replay:

- Output: `data/oanda_training_manager/reports/train_cap_lookback_compare_20260705_12mo/dual_history_ensemble_replay/`
- Backtest windows: monthly replays from `2025-07` through `2026-06`
- Start equity: `$1,000` per monthly replay
- Average monthly return: `+556.90%`
- Return range: `+224.15%` to `+1609.54%`
- Total trades: `11,616`
- Average PF: `8.41`
- Worst monthly DD: `1.53%`
- Max margin used: `82.52%`
- Margin-call rows: `0`

Expanded OANDA-style broker sweep:

- Output: `data/oanda_training_manager/reports/expanded_m30_h1_h4_oanda_broker_sweep_inverted/`
- Best row: `ensemble_m30_h1_h4`, all candidates, growth guard, wider stop
- Backtest window: `2026-04-20` through `2026-06-30`
- Return `+953.67%`, PF `6.42`, win rate `83.64%`, max DD `2.26%`
- `1,406` trades, max margin used `82.37%`, max open risk `3.85%`, margin-call rows `0`
- Source contribution: H1 `+$492,486.83`, M30 `+$248,006.36`, H4 `+$213,181.77`

Production preflight on `2026-07-05`:

- Full-universe OANDA read-only run loaded `67` rows for each of M30/H1/H4.
- It produced `393` candidate forecasts across the six bundles.
- The live primary bot was restarted with this dual-history stream loaded during preflight.
- Latest live decision after restart showed no open positions and no actions; weekend/spread/probability gates blocked new entries.

Historical H1/H4 guard sweep outputs were archived during the 2026-07-03 model
cleanup. Summary copies remain in the H1/H4 handoff folder, and the full
archive is at:

- `data/oanda_training_manager/reports/archive/cleanup_20260703_model_outputs/superseded_guard_and_risk_sweeps.zip`

The stricter pre-expanded H1/H4 OANDA-style broker replay was also archived:

- `oanda_broker_style_portfolio_replay.py`
- `oanda_broker_env_sweep.py`
- `data/oanda_training_manager/reports/archive/cleanup_20260703_model_outputs/superseded_h1_h4_broker_sweeps.zip`

That replay uses historical OANDA M1 candles, bid/ask side fills, integer
units, captured OANDA margin rates, live-style two-new-entries-per-cycle
limits, and M1 stop/take-profit/trailing-stop checks. Under those stricter
mechanics the prior H1/H4 growth profiles did not validate:

- H1 all-candidate growth: `-41.15%`, PF `0.30`, max DD `41.15%`
- H1 score>=0.12 growth: `-40.45%`, PF `0.30`, max DD `40.45%`
- H4 all-candidate growth: `-24.11%`, PF `0.40`, max DD `24.11%`

The first fast OANDA-environment sweep across H1/H4, all/score>=0.12 gates,
balanced/growth guards, and wider/slow/very-wide/quality-strict execution
profiles found no passing normal-direction row:

- Normal-direction sweep: `0/32` positive rows, best row `-18.98%`, PF `0.24`

Direction diagnostics then replayed the same forecast rows with LONG/SHORT
flipped. That found the forecast direction is likely inverted relative to the
broker-executed trade path. The diagnostic output is now in the same archived
pre-expanded H1/H4 broker sweep bundle:

- `oanda_signal_direction_diagnostic.py`
- Focused H4 inverted rows: `+40.06%` to `+48.70%`, PF roughly `6.35` to `6.75`

The full pre-expanded H1/H4 fast inverted sweep is archived at:

- `data/oanda_training_manager/reports/archive/cleanup_20260703_model_outputs/superseded_h1_h4_broker_sweeps.zip`

Best OANDA-style inverted row:

- H1 all-candidate, growth guard, wider stop profile
- Backtest window: `2026-04-27` through `2026-06-29`
- Return `+373.97%`, PF `9.98`, win rate `89.41%`, max DD `1.01%`
- `793` trades, `607` take-profit exits, `115` scheduled exits, `39` stop/trailing exits
- Max margin used `82.00%`, max open risk `3.27%`, margin-call rows `0`
- `46/49` traded pairs net positive

The expanded M30/H1/H4 stream now carries explicit model-target and execution
directions, with `execution_side_policy=follow_momentum`, so the old H1/H4-only
direction warning applies to the archived pre-expanded runs rather than the
current config.

Runtime model cache lives under:

- `data/technical_scout_manager/account_live_primary_forecast_rotation/model_cache/`

The cache key includes the stream config, candidate rows timestamp, research
parquet timestamp, feature set, base timeframe, execution-side policy,
training row cap, and per-model `train_lookback_days`.
