# Practice -007 Signal Bottleneck Fix

Generated: 2026-07-23 02:22 UTC
Scope: OANDA practice account `-007` only

## Result

The signal-to-account path is live and no longer blocked by stale promotion state,
double-discounted movement, an undersized shared-feed window, or missing shared-signal
repricing. No order was forced during verification: the current evidence did not pass
the remaining spread, confidence, validation, and consensus gates.

## Root Causes

1. The lane reconciler compared a compacted current row count with a larger incumbent
   raw count and incorrectly preserved the old state as a partial backfill.
2. Structural movement was multiplied by `2p - 1`, then probability was applied again
   in confidence and ranking.
3. Strategy candidates expired after 8 seconds even though a complete 68-pair cycle
   could take 50 to 70 seconds.
4. The hot executor did not provide a current price snapshot for shared-candidate
   repricing.
5. Contribution auditing wrote duplicate full forecast curves every cycle, growing the
   feed database to about 6.6 GiB.

## Implemented Policy

- Complete compacted promotion evidence now replaces stale incumbent state when source
  completion and horizon coverage agree.
- Structural gross movement stays in pips; probability is applied once downstream.
- A practice-only provisional consensus may validate a setup with at least two families,
  70% aligned weight, 55% confidence, +0.25 projected net pips, 1.35 gross/spread, and a
  horizon no longer than one hour.
- Individual model forecasts still require exact model/instrument/input-timeframe/horizon
  promotion and an exact artifact SHA-256 match.
- Strategy feed lifetime is 90 seconds. The hot runner reprices shared candidates from
  its current OANDA snapshot before any order decision.
- Model feed rows replace the prior row for the same model/instrument/input-timeframe.
  Audit activity is rolled up by minute with occurrence counters.

## Live Verification

- 68 priced instruments, 208 strategy lanes.
- Zero strategy API errors and zero hot-runner errors.
- Lane promotion is current and complete, with 1,233 evidence cells and zero generic
  lanes yet meeting the strict historical gate.
- Predictor promotion evaluated 32,184 exact cells. Four met the practice provisional
  evidence tier, but only one matches a currently loaded artifact: CatBoost `EUR_JPY`,
  H3 input, 10,800-second horizon.
- That current forecast was sell with 50.37% side probability, below the live confidence
  gate. Its persisted conservative historical net estimate was +0.287 pips.
- The checked strategy cycle had 1,696 matrix inputs, 7 accepted pre-consensus inputs,
  130 near-threshold inputs, 1,547 hard rejects, zero qualified consolidated signals,
  and zero orders.
- Account `-007` remained flat at balance/NAV 42.5392 with cumulative P/L -7.3944.

This means the earlier shadow opportunities were not realized trades, but future
qualifying signals now have a functioning route to the practice account.

## Verification And Storage

The extended execution suite passed: 111 tests and 219 subtests, with no failures.
The 669 warnings are third-party joblib/NumPy deprecations.

The old full-payload audit table is preserved and was not vacuumed while live workers
were using the database. New writes use compact rollups, so that legacy growth path is
closed. The raw 6.6 GiB feed database is excluded from the vault checkpoint; source,
tests, model artifacts, promotion state, current account/signal state, and this audit are
included. Credentials are excluded.
