# Runtime Robustness Upgrade - 2026-07-16

This change completes six operational and research gaps in the canonical
`D:\forex` Forex stack.

## Worker Health

The strategy lab and both second-forecast roles publish independent five-second
heartbeats. The heartbeat thread is separate from candle refresh, feature
evaluation, SQLite writes, and outcome maturation. The supervisor and dashboard
therefore distinguish a healthy worker doing slow work from a dead process.

State files:

- `state\strategy_lab_heartbeat_v1.json`
- `state\second_forecast_hot_heartbeat_v1.json`
- `state\second_forecast_tracker_heartbeat_v1.json`

## H4 Warmup

The strategy lab now fetches 160 completed H1 candles during the slower candle
refresh. H2, H3, and H4 inputs are causally resampled from those H1 candles,
with M5 retained as fallback. Each matrix row records its real series origin.
This gives H4 at least 40 completed observations immediately after the initial
refresh instead of waiting for a long rolling M5 warmup.

## Signal Ranking

Account candidates retain executable net pips and confidence, but ranking now
also includes:

- estimated margin rate and pip return per unit of margin;
- expected return per margin-hour for the selected horizon;
- spread/cost quality;
- streamed top-of-book liquidity and imbalance when available;
- a conservative instrument liquidity prior when depth is unavailable.

`normalized_rank_score` is the primary ordering field. Existing execution gates,
currency concentration limits, maximum margin use, stops, and practice-only
account controls remain active.

## Timeframe Calibration

`oanda_timeframe_matrix_calibration.py` incrementally consumes each detailed
matrix outcome exactly once. Fixed probability bins are updated only after the
current row is scored, producing a causal expanding walk-forward calibration.
It reports raw and calibrated Brier score, accuracy, executable net pips,
confidence bounds, and independent hour blocks for pair-specific and global
timeframe/horizon surfaces.

Passing surfaces are labeled `validated_shadow`, but remain
`account_eligible=false`. The live matrix emits raw and calibrated probability,
calibration scope, sample count, Brier score, and readiness with every forecast.

## Retention

`oanda_shadow_outcome_compactor.py` first rolls every new detailed outcome into
hourly aggregates. Rows older than 30 days are written to date-partitioned Zstd
Parquet and deleted from source SQLite only after the Parquet part exists.
SQLite is checkpointed but not force-vacuumed, avoiding disruptive rewrites of
active multi-gigabyte databases.

State and storage:

- `state\shadow_outcome_rollups_v1.sqlite`
- `state\shadow_outcome_compactor_v1.json`
- `shadow_outcome_archive\<source>\<date>\*.parquet`

## Deployment Concurrency

`forex_guarded_deploy.py` uses a Windows file lock and SHA-256 manifest. A deploy
fails if a tracked canonical file changed after the deploying chat's baseline.
The supervisor also holds a global Windows mutex, so only one supervisor can
manage workers.

See `DEPLOYMENT_CONCURRENCY.md` for the required workflow.
