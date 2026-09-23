# Signal Engine Refresh - 2026-07-21

## Result

The canonical signal engine now accepts strategy, equation, neural, foundation,
graph, decision-learning, representation-learning, ridge, and GPT-produced
forecasts through one audited SQLite WAL feed.

Coverage at this checkpoint:

- 52 strategy families x 4 profiles = 208 strategy parameter lanes.
- 13 continuous equation input timeframes.
- 16 canonical outcome horizons from 30 seconds through 24 hours.
- 30 registered modern model-gap adapters in 7 families.
- 28 model-gap identities with bounded-market evidence.
- 2 runtime-blocked identities: TimesFM-ICF and Mamba.
- 0 model-gap identities currently authorized for account execution.

The model-gap route is implemented for all 30 identities. That does not mean all
30 are continuously producing live forecasts. A model becomes a live contributor
only after its producer publishes a fresh forecast. Historical backtest rows are
never replayed as current signals.

## Consensus Layers

Each instrument/horizon cell has three views:

1. `raw`: every fresh, finite generated signal gets a correlation-adjusted vote.
2. `filtered`: every signal remains visible, but setup failures, weak historical
   reliability, negative prediction-quality evidence, timing-only roles, and
   correlated variants are attenuated.
3. `execution`: only account-authorized structural signals that clear confidence,
   spread, uncertainty, liquidity, cold-start, and negative-history gates vote.

An absent forecast is an abstention. It is not converted to an up/down vote.
Near misses and hard rejects remain in the research matrix with reduced weight
and explicit reasons. A failed signal cannot authorize a paper trade merely by
agreeing with other failed signals.

Partial forecast curves vote only at horizons present in the producer output.
For example, an M5 model that emits 300-second and H4 points cannot silently vote
at H1.

## Producer Contract

`oanda_signal_contribution_feed.py` owns the shared schema. A producer can submit
JSON or JSONL:

```json
{
  "model_id": "ngboost",
  "instrument": "EUR_USD",
  "input_timeframe": "M5",
  "generated_utc": "2026-07-21T19:00:00Z",
  "forecast_curve": {
    "300": {
      "probability_up": 0.61,
      "predicted_signed_pips": 1.4
    },
    "3600": {
      "probability_up": 0.57,
      "predicted_signed_pips": 3.2
    }
  }
}
```

Publish it with:

```powershell
python oanda_signal_contribution_feed.py `
  --database data\oanda_training_manager\state\practice_007_signal_feed_v1.sqlite `
  --input forecasts.json `
  --source ngboost-live `
  --ttl-sec 65 `
  --max-age-sec 120 `
  --coverage-output data\oanda_training_manager\state\signal_feed_coverage_v1.json
```

Required fields are model identity, pair, input timeframe, a fresh timestamp, and
at least one valid horizon probability. Signed movement and quantile fields are
optional but strongly preferred because probability alone cannot establish that
the forecast clears spread.

Unknown producers are accepted into research as shadow-only and cannot
self-authorize account execution. Registered model-gap producers inherit the
current production gate from `unified_model_gap_market_latest.json`.

## Runtime Changes

- The signal engine is enabled by default even without `--execute-top-signals`.
  This ranking-only mode tracks and snapshots signals without placing orders.
- `--execute-top-signals` still exclusively controls OANDA order submission.
- The shared fresh-candidate limit is 50,000, configurable with
  `--execution-signal-feed-limit`.
- All local candidates are published, including accepted, near-miss, hard-reject,
  shadow-only, opposing, and timing evidence.
- Shared candidates are repriced and economically enriched after merging, using
  the executor's current quote snapshot.
- `practice_007_signal_snapshot_v1.json` schema 2 includes contribution-feed
  coverage and raw/filtered/execution matrix metrics.

## Validation

Recreate the static inventory and all-30 routing contract:

```powershell
python oanda_signal_engine_refresh_audit.py
```

Expected report:

`data\oanda_training_manager\reports\modern_model_gap\signal_engine_refresh_latest.json`

The synthetic route probe in that report is schema validation only. It is marked
`market_evidence: false` and all 30 probe candidates remain account-ineligible.

Focused tests:

```powershell
python -m pytest -q `
  test_oanda_signal_contribution_feed.py `
  test_oanda_signal_engine_refresh_audit.py `
  test_oanda_lane_promotion.py `
  test_oanda_practice_shadow_strategy_lab.py
```

## Deployment Constraint

This checkpoint was rebuilt from the verified OneDrive vault on temporary C:
storage because D: has unresolved physical media errors. No OANDA worker was
started by this refresh. Deploy to the canonical D: runtime only after choosing
to accept that disk risk, then verify the feed database, signal snapshot, worker
heartbeat, and practice-account state before enabling order execution.
