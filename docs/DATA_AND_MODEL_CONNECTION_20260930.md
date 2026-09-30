# Data and retained forecasts across horizons

The current user scope is data reliability and connecting existing supported models
across the engineering design's horizons. New fitting and research experiments are
stopped. The earlier typed-news experiment remains deferred, not automatically next.

The dashboard now has **Saved model forecasts by horizon**, with a pair selector,
actual issue times, model names, evidence limits and explicit missing targets.
The retained worker uses existing completed-candle features and saved weights. It
does not refit models, alter the original H1 price/news cohorts or place orders.

| Exact elapsed horizon | Connected retained model | Evidence scope |
|---|---|---|
| 5, 15 minutes | compact38 Ridge | 2026 rolling comparison; two assessment weeks |
| 30 minutes | compact38 six-head raw mixture | 2026 specialist family; six assessment weeks |
| 60 minutes | compact38 direct Ridge meta-model | 2026 specialist family; six assessment weeks |
| 4 hours | full228 plus entry-cost HGB | 2024 matched rich-family development, five dates |
| 12, 24, 48, 120 hours | compact38 plus entry-cost HGB | Same 2024 matched family |

These are scoped retained-model connections, **not a verified best model across the
entire research archive**. Short-horizon inspection prioritizes number of evaluated
weeks, then worst-week MAE relative to no change. Long-horizon choices minimize
frozen rich-family MAE within that original cohort. This is an explicitly posthoc
engineering selection, not a new independent confirmation experiment. Different
dates, targets and scoring populations are not merged into a universal leaderboard.

None of the inspected 2026 signed-return choices consistently beat no-change MAE
across their available weeks. Existing specialist move-size and exit-cost results
remain useful separate outputs; signed-return MAE does not replace their evaluation.
The long suite also has stronger simple controls in several slices. No-change remains
visible as 0 bps, and these learned outputs carry no trading promotion.

Not connected: exact elapsed 1, 3, 10, 20, 45, 90 minutes and 2, 3, 6, 8, 18 hours;
next daily close, close-to-close, and 2/3/5/10/20 trading days. Elapsed 24/48/120 hours
must not be relabelled daily closes or trading days. Older classifiers, legacy26,
residual corrections and curve layers require their own target/input reconciliation;
their absence from this live adapter does not establish inferiority or nonexistence.

## Data repair and verification

Index-only maintenance was applied to both existing news archives and the technical
store. No feature, article, label, model weight or original timestamp was rewritten.
News query results retained exact ordered hashes for 4,820 checked recent records.
The technical transaction retained 658,994 observations and 2,629,925 outcomes and
identical results for the original settlement queries and outcome-state summary.
Its outcome-summary query changed from a table scan and temporary sort to a covering
index: 5.828 seconds before, 0.422 seconds after in that transaction. A separate
earlier read took 17.68 seconds; timing depends on cache and machine load.

An earlier technical cycle took 117.187 seconds; subsequent measured cycles were
28.906, 28.406 and 33.359 seconds. This is operational observation, not a controlled
whole-pipeline latency benchmark. Source omissions, stale quotes, non-tradeable pairs
and partial technical support remain visible. Missing values use the models' original
saved preprocessing; no fabricated candles or newly learned imputation were added.

Actually executed: 72 focused tests; 1,224 saved predictions replayed across nine
connections and all 68 pairs, at 1e-10 bps absolute / 1e-12 relative tolerance;
18 capsule payloads restored byte-for-byte into a separate directory; JavaScript
syntax check; native profile validation; direct local HTTP readback. The API returned
HTTP 200 and 513 current retained forecasts across all nine elapsed horizons in the
dated acceptance read. This is not a promise that every pair remains fresh.
Same-task substantive review only; independent review was not performed.

## Running and replication

The existing bounded context worker runs retained inference once per minute through
the explicit `--retained-model-registry` argument. Failure is isolated from headline
capture. Original model cohorts and other-chat EUR/USD capture remain unchanged.
The existing recovery expiry remains 2026-10-07T08:14:50Z. No new scheduled task,
chat automation or heartbeat was created.

- Registry: `trad/config/retained_forecast_connection_20260930.json`.
- Current output: `trad/data/retained_connection_20260930/current.json`.
- Immutable compressed issuance records: same directory, `issued.sqlite`.
- Original feature bytes remain in the technical store under their receipt/hash.
- Store guard: 2 GiB issuance limit and 32 GiB free-space minimum; no silent deletion.
- Evidence: `evidence/data_and_model_connection_20260930`.
- Shared packet: live Vault `DATA_AND_RETAINED_MODELS_20260930`.

Clone the matching Git source and retrieve the packet's
`retained_connection_capsule.zip` (SHA-256
`57e560f5f64b6285c7ae6e53ed7187fd1a789f08d70cc7cff931ef7cffd2acc9`).
Then, from the replica workspace:

```powershell
python tools/forex_retained_capsule.py restore --capsule PATH_TO_CAPSULE --sha256 57e560f5f64b6285c7ae6e53ed7187fd1a789f08d70cc7cff931ef7cffd2acc9
```

Restore verifies identities and refuses conflicting existing bytes. It neither loads
models nor starts services. Numerical replay and byte restoration were separate
checks; a fully isolated numerical replay from the restored directory was not claimed.
A replica still needs the qualified Python environment, original feature producers
and its own runtime paths. Missing artifacts require retrieval, not refitting.

## Exact next work

`data_freshness_and_remaining_horizon_reconciliation_v1`: inspect current ingestion
and the existing model/input contracts for the remaining horizons and layers. Retain
the nine verified adapters and current data fixes. Join comparable saved results by
exact target, population, metric and dates, including residual/curve/legacy26 controls,
before changing a selection. Do not call these family-scoped choices universal winners.
Keep unsupported targets explicit. No new fitting or typed-news experiment is authorized
by this handoff. Data capture owned by the other chat stays separate.
