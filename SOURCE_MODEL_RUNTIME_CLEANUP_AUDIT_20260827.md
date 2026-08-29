# Source, model, and runtime cleanup audit — 27 August 2026

Snapshot time: **2026-08-27 21:25 America/New_York**

Scope: canonical workspace
`C:\Users\zmoor\Documents\forex\trad`. This is a current-state audit and does
not rewrite the pre-upgrade August audits.

Post-snapshot addendum (**2026-08-27 22:42 ET**): the cleanup decision was
extended into the bounded source-first branch. The retired opportunity ranker
is no longer queried by the live news watchlist; the six-horizon source map is
sealed and superseded by a distinct seven-horizon V2; and new V2-only
source-conditioned rank plus news/band/quote-flow H15 workers are supervised as
immutable research cohorts. Final cross-worker validation passed 82 tests and
all new ledgers passed SQLite integrity/append-only checks. Their prospective
counts remain zero and they cannot place, authorize, promote, or route orders.

Post-snapshot addendum (**2026-08-28 02:12 ET**): live monitoring exposed and
closed a BOJ research-document/FX-intervention classification error, expanded-
currency preflight fan-out, pre-semantic quote latency, false supervisor
liveness restarts, repeated full-history macro payload scans, all-68 overfetch,
Windows atomic heartbeat replacement failures, and retained-quote reconnect
pollution. V1/V2 evidence remains preserved; new classifier V148, source map V3,
and rank V2 use separate contracts, ledgers, and a later prospective boundary.
The hidden supervisor was reloaded, Practice 007 remained flat, quote transport
returned healthy 68/68, and 173 integrated tests passed with one unrelated
artifact-dependent historical fixture deselected.

## Operating authority

- The C workspace above is the only live source and runtime truth.
- `D:\forex\trad` is a stale legacy copy.
- OANDA account `101-001-37981792-007` is practice-only.
- No discretionary/manual order or close is authorized. Real-money routing is
  disabled.
- The lifecycle contains **zero confirmed candidates**; the supported decision
  remains **no_trade**.
- A future Practice-007 entry still requires an exact lifecycle
  `confirmed_candidate` plus a fresh, unexpired, one-time canary authorization
  binding the account, signal, proof cohort, and allocator cohort.

## Runtime cleanup decision

| Component | Current state | Evidence and disposition |
|---|---|---|
| HGB live outcomes | `retired_obsolete_input` | Read the stale D-drive account-019 forecast source. The preserved state has 6,006 outcomes, 15 accepted rows, 6.67% accepted win rate, zero pending work, and zero additions/maturities in the last cycle. Runtime collection is disabled; code and evidence remain preserved. |
| Manager decision outcome ledger | `retired_static_input` | Re-polled the frozen `LIVE_ACCOUNT_MANAGER_FORECASTS_20260810.jsonl` file every two seconds. All three forecasts are mature and the latest cycle inserted/matured zero. Runtime collection is disabled; input, state, and SQLite ledger remain preserved. |
| Four-family H1 shadow baseline | `retiring_drain_only` | New forecasting and feed publication are disabled. At 21:13 ET the worker was healthy in `mature_only`, had 4,056,490 issued forecasts, 3,985,454 matured prediction points, 5,816 pending rows, and zero errors. It continues only until already-issued causal horizons resolve, after which the frozen stream remains a negative control. |
| Executable-opportunity ranker | `retiring_drain_only` | Seven cohorts produced 169,376 forecasts and 166,128 outcomes, zero directional frozen-gate passes, 49.31% direction accuracy, and -3.014 predicted-side net pips. New production is disabled; the worker is resolving only already-issued horizons. |
| Allocator proof | `paused_until_confirmed_candidate_exists` | It retained 3,528 decisions and more than one million candidate/integrity rows but zero fully matured eligible decisions. It remains a frozen no-trade null and wakes only after an exact lifecycle-confirmed candidate exists. |
| Four immutable proof families | `continue_collecting` | Ridge, repaired tabular, graph-transfer, and probabilistic state-space cohorts remain governed and unchanged. Their exact cells are still mostly underpowered; negative aggregate results are not used to erase unfinished cell-level proof. |
| Unavailable microstructure | `inert_until_causally_populated` | Across 14,888,709 captured forecasts there were zero order-book observations, zero position-book observations, and zero informative pricing-depth observations. These contracts remain excluded from active model eligibility and are not counted as live source breadth. |
| Thirty model-gap contributors | `dormant_not_active_breadth` | They have zero fresh live contributors and zero account-eligible contributors. Adapters remain offline research assets rather than active consensus inputs. |
| Static signal-combination rules | `inactive_stale_model_state` | Three August 3 artifacts are preserved, but a 24-hour load-time freshness boundary now excludes them from the active strategy surface while fitting is disabled. |

The retirement policy is recorded in
`config/shadow_runtime_retirements_v1.json`. Reopening a retired stream requires
a materially new information, feature, label, or decision contract and a new
cohort ID; renaming or cosmetic retuning is insufficient.

## H1 retirement evidence

The non-overlapping seven-day H1 audit found no holdout candidate. Each of the
four families had about 2,754 independent sampling slots across 31 pairs and all
24 UTC hours in the current drain snapshot:

| Family | Direction accuracy | After-cost win rate | Average net pips |
|---|---:|---:|---:|
| breakout_change_point | 49.13% | 22.69% | -2.729 |
| cross_currency_impulse | 50.29% | 24.11% | -2.481 |
| differenced_path_analog | 50.40% | 25.82% | -2.266 |
| multi_timeframe_trend | 49.20% | 23.31% | -2.578 |

This is sufficient to stop generating more copies of the same baseline, but not
permission to delete the forecasts, outcomes, or negative-control history.

## Clean level-band collection

- Worker status: **running**, research-only, and supervised.
- Cohort: `level_band_prospective_20260827a.9294e3511aae455f`.
- Geometry: `level_band_contract_v2_frozen_20260827b`.
- At 21:12 ET: **65** ready contexts, **150** frozen forecasts, and **250**
  matured outcomes.
- The old first-maturity SQL defect and nonstandard-pip heuristic are repaired.
  Compatibility/pre-contract rows remain engineering diagnostics and are
  permanently excluded from proof.
- The collector has no broker, authorization, promotion, order, or position
  surface. Evidence must still clear executable costs, factor/episode
  deduplication, concentration stress, and untouched prospective confirmation.

## Current source coverage

The latest source-governance snapshot reports:

- **187 configured**, **187 runtime-observed**, **179 operational** sources.
- **867,102** immutable source events: 867,038 causal and 64 quarantined.
- **71,969** story clusters and 796,899 immutable supersessions.
- All **21/21 currencies** have configured and operational official policy
  release and schedule coverage.
- All **68/68 pairs** have both policy legs configured and operational.
- Policy-release health is **20/21**: the RBNZ transport remains the one health
  exception, while NZD retains operational official coverage.
- The current source layer is research-only and has zero execution weight.

The project-integrity report generated two minutes before the latest
source-governance snapshot still listed 186 configured/registered definitions.
That one-row difference is recorded as snapshot timing, not silently blended;
the newer source-governance count is used here.

## Governed model evidence

- Governed hypotheses: **52,503**.
- Permanently retired for futility: **9,372**.
- Confirmed candidates: **0**.
- Fully passing evidence cells: **0**.
- Multiplicity-adjusted discovery candidates: **0**.
- Counterfactual allocator decision: **no_trade**.
- Practice 007: balance/NAV **41.6042 / 41.6042**, cumulative operational P/L
  **-8.343**, zero open trades, and zero pending orders.

The four proof cohorts remain immutable. Current production/maturity counts are
roughly 87.5k/84.9k per cohort, but effective independent evidence is only
10–12 observations per cohort after episode and currency-factor deduplication.
That is why these cohorts continue collecting despite negative aggregate
point estimates.

## Storage and preservation

- Storage guard status: **ok**.
- Current C-drive free space is approximately **124 GiB (13.3%)**.
- The managed-growth projection remains safe, with about **38.8 days** to the
  50-GiB guard at the current positive-growth estimate.
- Existing verified rotated-log reclamation has returned **7.45 GiB**.
- A recoverable organization pass moved **18,704** expired `.log`, `.bak`, and
  `.tmp` artifacts older than seven days (**394,242,063 bytes**) into
  `data/archive/runtime_cleanup/20260828_010345`; zero files were skipped. The
  JSON and CSV manifests preserve every original and archive path. Because the
  move stayed on C, it improves organization but is not claimed as newly freed
  disk space.
- No live SQLite database, WAL/SHM file, causal forecast, matured outcome,
  source event, daily snapshot, code, credential, or proof-cohort evidence was
  deleted or vacuumed.
- Two redundant account pollers were consolidated into one fetch/mirrored-write
  process, and five active/manual tools no longer default to the stale D copy.

## Historical audit lineage

These earlier reports remain unchanged and should be read as dated baselines,
not current runtime descriptions:

- `COMPLETE_SOURCE_AUDIT_20260806.md` — pre-upgrade source/model baseline.
- `COMPLETE_SOURCE_AUDIT_POST_UPGRADE_20260806.md` — first proof-first delta.
- `EVIDENCE_ACCUMULATION_FUTILITY_ALLOCATOR_AUDIT_20260806.md` — lifecycle and
  allocator-governance checkpoint.
- `CONSOLIDATED_SOURCE_EVIDENCE_BUILD_20260808.md` — later consolidated build.
- `NEWS_SOURCE_AUDIT_20260806.md` — dated news-source inventory.
- `data/oanda_training_manager/reports/HISTORICAL_CASE_INDEX_CURRENT.json` —
  current SHA-256 inventory of 163 retained move/news case artifacts.

This audit supersedes those documents only for current workspace, runtime,
retirement, coverage, and storage facts. It does not combine incompatible
historical and prospective performance evidence.

## Closeout verification at 21:38 ET

- H1 retirement worker: supervised `mature_only`, zero new forecasts, zero
  session errors, 1,924 pending exact horizons; terminal drained state now uses
  a four-minute liveness interval.
- Executable-opportunity ranker: supervised `mature_only`, zero new forecasts,
  3,213 pending exact horizons, 0 directional gate passes across 169,407
  historical forecasts and 166,194 matured outcomes.
- Practice 007: balance/NAV 41.6042, zero open trades, zero pending orders.
- C-drive free space: approximately 122.4 GiB (13.15%). No live database was
  deleted, vacuumed, or moved.
- H1 retirement regression: 10 tests passed in the configured timeseries
  runtime; relevant source files compile successfully.
- Project histories, registries, Shared Brain records, and current checkpoint
  pointer were refreshed after this closeout. Older timestamped vault archives
  are retained through a hash-verified recoverable quarantine rather than
  deletion.
