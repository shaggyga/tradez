# MA Feature Grid Change Recap

## Request

Build a strict moving-average feature family over the full supported
timeframe/horizon space, with both direction and pip-forecast evaluation.

## Implemented

- Added one causal SMA/EMA feature contract shared by training and live
  inference.
- Covered all 68 OANDA FX instruments.
- Covered 27 input timeframes from S5 through D1.
- Requested 26 outcome horizons from 5 seconds through 24 hours.
- Fitted separate direction, signed-pip, and absolute-movement heads for each
  supported timeframe/horizon cell.
- Added chronological development, validation/calibration, and untouched
  holdout splits by pair.
- Added observed bid/ask execution accounting where available and a documented
  pair-level spread proxy for older midpoint-only history.
- Added direction accuracy, balanced accuracy, AUC, Brier score, log loss,
  signed-pip MAE/RMSE/bias/correlation, magnitude MAE/RMSE/bias/correlation,
  after-cost net pips, win rate, profit factor, pair breadth, and confidence-bin
  evidence.
- Added the MA grid to the live unified forecast feed as shadow-only candidates.
- Batched live inference across pairs and moved MA publication ahead of the
  slower 208-lane strategy evaluation.
- Added all-grid dashboard metric modes for direction and pip accuracy.
- Added unit, causality, fitting, live-series, cache-retention, and dashboard
  integration tests.

## Reproducible Result

- Planned timeframe/horizon cells: 702.
- Fitted cells: 610.
- Unsupported cells: 92.
- Unsupported cells are the 5/10/15/30-second outcomes for M1-and-slower input
  history. They are labeled unsupported rather than inferred from coarser bars.
- M1 and most faster/minute frames use 643 MA-only fields. Higher timeframes use
  smaller period ceilings so live OANDA history can reproduce the fitted
  averages.

## Validation Finding

One cell passed the strict validation gate and one different cell passed the
strict holdout gate. No cell passed both. Some cells predict movement magnitude
well, but direction and after-spread pair breadth are not yet stable enough for
account authorization.

The artifact is therefore integrated as research-only. It contributes
backtestable live forecasts and missed-opportunity evidence, but cannot place
orders or influence account sizing until a cell passes both chronological gates
and live paper confirmation.

## Completion Audit

- Focused regression suite: 114 tests passed.
- Parameterized strategy cases: 219 subtests passed.
- Fitted artifact SHA-256:
  `faa5eb07d7643bba35054d62b025c2a56a6df29b9b545545f192583a3ba94709`.
- The report's recorded artifact hash matches the fitted file.
- The loaded artifact contains 27 timeframe models and 26 declared horizons.
- The dashboard research API contains 29 aggregate timeframe rows, 26 horizon
  columns, and 27 dedicated MA source rows.
- Three audited live cycles emitted 1,146 to 1,178 MA forecasts per cycle and
  the unified feed rejected none; the worker recorded zero OANDA API errors.

## Vault Contents

The current credential-free checkpoint includes:

- feature/runtime and fitting source;
- tests and operator documentation;
- `ma_feature_grid_latest.joblib`;
- `ma_feature_grid_latest.json`;
- `ma_feature_grid_metrics_latest.csv`;
- `MA_FEATURE_GRID_LATEST.md`;
- SHA-256 hashes in the checkpoint manifest and pointer.

Raw candle and forecast-event partitions remain outside the vault package
because they are reproducible downloads/rolling data, not required source or
model evidence.
