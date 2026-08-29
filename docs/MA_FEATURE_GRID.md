# Moving-Average Feature Grid

## Purpose

`moving_average_feature_grid` is a standalone forecast family. It is not a
crossover rule and it is not a filter over another model's signals. It predicts
direction, signed pips, and absolute movement using only causal SMA/EMA geometry.

The same feature builder is used by historical fitting and live inference.

## Coverage

- Instruments: all 68 OANDA FX instruments in the recovered panel.
- Input timeframes: S5, S10, S15, S30, M1, M2, M3, M4, M5, M6, M7, M8, M9,
  M10, M12, M15, M20, M30, M45, H1, H2, H3, H4, H6, H8, H12, and D1.
- Requested horizons: 5s, 10s, 15s, 30s, 1m through 10m, 15m, 20m, 30m,
  45m, 1h, 2h, 3h, 4h, 6h, 8h, 12h, and 24h.
- Fitted cells: 610.
- Structurally unsupported cells: 92. M1-and-slower history cannot resolve
  5/10/15/30-second outcomes, so those cells are labeled unsupported rather
  than interpolated.

Subminute models use observed S5 bid/ask history. Minute-and-slower models use
the recovered deep M1 panel.

## Feature Contract

The M1 model has 643 fields. Slower frames use lower period ceilings when the
live OANDA history cannot reproduce a longer average.

For each available SMA and EMA period, the model receives:

- price-to-average distance;
- one-bar and three-bar slope;
- curvature;
- SMA-versus-EMA separation.

For adjacent and canonical period pairs, it receives:

- continuous fast/slow gap;
- gap velocity and acceleration;
- crossover age;
- signed recent-cross support.

Aggregate fields describe price, slope, and MA-order alignment, fan width,
dispersion, and SMA/EMA disagreement.

Every continuous distance is normalized by a causal rolling mean of absolute
price changes. The raw rolling scale in pips is retained as an MA-derived
movement-regime field.

Excluded structural inputs include raw return lags, oscillators, volume, spread,
order/position books, pair identity, account state, and other model signals.
Spread is used only to evaluate executable economics.

## Targets And Metrics

The fitted artifact contains three multi-horizon heads per input timeframe:

1. Direction score calibrated to `P(up)` on chronological validation.
2. Signed midpoint pips, affine-calibrated on validation.
3. Absolute midpoint movement in pips, affine-calibrated and bounded at zero.

Direction metrics:

- `direction_accuracy = correct direction / N`
- balanced accuracy
- ROC AUC
- Brier score: `mean((P(up) - actual_up)^2)`
- log loss and confidence-bin calibration

Pip metrics:

- signed MAE: `mean(abs(predicted_signed_pips - actual_signed_pips))`
- signed RMSE, bias, R-squared, Pearson correlation, and Spearman correlation
- magnitude MAE/RMSE/bias/correlation against `abs(actual_signed_pips)`

Executable metrics select the predicted side:

- long: enter ask, exit bid;
- short: enter bid, exit ask;
- older midpoint-only rows pay the pair's median observed spread.

Reports include average/median/total net pips, win rate, profit factor, exact-cost
fraction, pair count, positive-pair fraction, and mean pair-level net pips.

## Validation

Each pair is split chronologically:

- oldest 60%: development fit;
- next 20%: calibration/validation;
- newest 20%: untouched holdout.

The first complete run sampled up to 250 uniformly distributed observations per
pair/timeframe across the full available date range. This is full-span sampling,
not a latest-tail test.

Current result:

- one validation cell passed the strict direction/net/pair-breadth gate;
- one different holdout cell passed;
- no cell passed both validation and holdout;
- no MA-grid cell is account-authorized.

Some magnitude correlations are high. That means MA geometry can identify when
movement will be relatively large. It does not establish side accuracy or
tradability after spread. For example, the highest raw direction percentage is
paired with an ROC AUC near 0.50, showing that class imbalance rather than useful
ranking produced the percentage.

The July 27 all-signal live audit reached the same operational conclusion. MA
contributors were included in every consolidated pair/horizon vote, but the
chronologically selected liquid-pair gate was negative on both its fit and
untouched holdout segments. MA information therefore remains useful as a
measured feature family, not as independent account authorization.

## Live Integration

`oanda_practice_shadow_strategy_lab.py`:

- retains the initial 5,000 M1 bars instead of replacing them with each 120-bar
  refresh;
- builds every reproducible minute-to-D1 MA series from already-fetched OANDA
  candles;
- parses each source stream once, batches inference across pairs, and caches
  results by completed-bar origin;
- publishes MA forecast curves to the unified signal feed before the slower
  legacy strategy-lane evaluation;
- sets `account_eligible=false` for every cell.

True S5 live inference remains dependent on the observed S5 collector. It is not
fabricated from M1.

The July 27 live acceptance run completed three audited cycles without an OANDA
API error. Individual cycles produced 1,146 to 1,178 MA forecasts across the
available pair/timeframe histories, and the unified feed rejected none of them.
Unavailable, stale, and non-tradeable quotes are explicitly excluded. The
dashboard research endpoint exposed all 27 MA timeframe rows and all 26 horizon
columns.

## Files

- Feature/runtime contract: `oanda_ma_feature_grid.py`
- Historical fit: `oanda_ma_feature_grid_fit.py`
- Tests: `test_oanda_ma_feature_grid.py`
- Artifact: `data/oanda_training_manager/models/ma_feature_grid/ma_feature_grid_latest.joblib`
- Full report: `data/oanda_training_manager/reports/ma_feature_grid/ma_feature_grid_latest.json`
- Flat metric grid: `data/oanda_training_manager/reports/ma_feature_grid/ma_feature_grid_metrics_latest.csv`
- Summary: `data/oanda_training_manager/reports/ma_feature_grid/MA_FEATURE_GRID_LATEST.md`

## Commands

Full rebuild:

```powershell
& "D:\forex\trad\..venv\Scripts\python.exe" `
  "D:\forex\trad\oanda_ma_feature_grid_fit.py" `
  --samples-per-pair-timeframe 250 `
  --minimum-split-rows 500
```

Replace selected timeframes inside the current artifact:

```powershell
& "D:\forex\trad\..venv\Scripts\python.exe" `
  "D:\forex\trad\oanda_ma_feature_grid_fit.py" `
  --timeframes S5,S10,S15,S30 `
  --samples-per-pair-timeframe 250 `
  --minimum-split-rows 500 `
  --merge-existing
```
