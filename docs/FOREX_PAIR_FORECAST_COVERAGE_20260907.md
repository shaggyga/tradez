# Pair forecast coverage — September 7, 2026

The new pair-local collection is running. All **68 pairs activated between
17:17:23 and 17:17:25 UTC**, under registry SHA-256
`e0aa74fa5d20b7be983050625773834ea68d74ffc15b566ab5e03df9b3380d65`.
At **17:19:00 UTC**, live verification found **20 published forecast sets across
20 pairs, containing 40 model predictions**, with 20 postcommit publications,
20 independent consumptions, 19 later quote entries, and **zero outcomes**. The
remaining published set had no retained entry yet at that observation. The
dashboard API showed 20 pairs with forecasts, 43 warming up, and 5 unavailable.
These are dated observations, not guaranteed current counts or accuracy.

The change extends independent ridge and state-space research forecasts to a
fixed registry of all 68 observed instruments. Each pair has its own inputs,
immutable contract, SQLite ledger, cadence, forecasts and outcomes. The existing
EUR/USD companion and shared four-family study keep their original contracts,
targets and evidence. Orders and promotion remain disabled.

## Why most rows had no forecast

The earlier companion publishes only EUR/USD forecasts. The market table covers
many more instruments, so a current market quote did not imply a registered
model forecast for that pair. The new pair-local worker supplies the two models
that require only their own instrument's prices. Cross-pair graph and pooled
models remain part of the separate shared study; this revision does not replace
them with single-pair stand-ins or claim that co-movement research is complete.

The read-only coverage probe at **2026-09-07 16:59:11.482249 UTC** found
**20 of 68 pairs input-ready**. This was an observation of available archive
data, not 20 published forecasts or a forecast success rate. Registration covers
all 68 instruments independently of their past returns and momentary readiness.
Other pairs retain an explicit reason for withholding a forecast.

## Inputs, models and observation clocks

Each pair requires **61 consecutive completed own-pair minute closes**, spanning
60 return intervals. Up to 1,024 retained real rows supply older training
segments, including Friday history where available. Ridge requires at least
24 mature training rows on its fixed three-minute sampling phase. Each training
example contains all 121 real prices needed for its feature and exact H1 target
interval. State-space estimation resets at the latest own-pair gap and retains
at most 256 consecutive prices. Missing prices are never filled, and gaps are
never compressed into shorter elapsed time. Both local families must be ready
before that pair publishes its comparison set.

Pip sizes come from the frozen observed quote metadata. They are never inferred
from a currency suffix: retained examples include `HKD_JPY` at `0.0001`, and
`EUR_HUF`, `USD_HUF` and `USD_THB` at `0.01`. Instrument and pip metadata are
checked across registration, protocol, input capture, computed result, quote,
forecast, decision and evaluation. The live quote's pip metadata must continue
to match the registered value.

The new ledger preserves actual activation, source observation, model completion,
issuance, postcommit publication and independent consumption boundaries. Entry
evaluation uses a later executable quote. The H1 target stays exactly 3,600
seconds after the original reference market timestamp, even when fitting,
publication or entry takes time. Bid/ask prices and reference anchors use exact
Decimal scoring with the original additional cost stresses of 0, 0.5 and 1 bps.
Pair-local rolling baselines cannot consume labels from other instruments.

## Supervision and validation

The closed research gate expands from 13 to **14 admitted workers**, adding only
`pair_local_forecast_study_v1`. It uses the bound Python 3.12 time-series runtime
at `BelowNormal` priority and the fixed pair-study heartbeat schema. The hidden
research launcher is unchanged. Unknown workers, order executors, authorization
and promotion remain outside this gate. Supervision cannot activate a study:
the worker requires separately recorded matching activation receipts and verifies
the frozen source and dependency bindings.

Validation completed during implementation:

- **845 Python test cases** passed across the final suites: 140 model/input,
  490 ledger/evaluator, 202 dashboard/worker, and 13 reload-preflight cases.
- **490 ledger/evaluator tests** passed, including cross-pair and pip contamination,
  exact price preservation, crash/recovery, immutable evidence, original targets,
  later quote selection, and real captured-input integration for EUR/USD, AUD/JPY,
  HKD/JPY and USD/HUF.
- The isolated Windows PowerShell gate review passed **14 allowed workers,
  107 blocked or unknown names, and 15 registry flag/schema cases**, with zero
  process starts, stops or runtime writes.
- **13 read-only reload-preflight tests** passed. Missing, mismatched, future or
  already-used pair registrations fail before controlled startup. Database paths
  must remain inside the intended study directory.
- Pair model/input and ledger/evaluator sources received independent reciprocal
  review. The registry helper separates preparation from actual-clock empty-ledger
  activation and refuses to overwrite registered configurations.

The controlled reload checked frozen sources and pre-existing empty pair
activations, then stopped the old supervisor and dashboard. It aborted before
relaunch because the remaining dashboard PID had already exited when its
identity was rechecked. The root task confirmed the stopped state and recovered
through the unchanged hidden research launcher. Supervisor **22400** then started
dashboard **19184/10080** and the added pair worker **11892/4068**.

Independent read-only recovery verification matched the prior and recovered
supervisor heartbeats: **all 12 untouched workers retained their 24 process
IDs**, including the EUR/USD/shared studies, quote stream, archive, news, account,
clock and storage services. Current process creation clocks predated the reload.
All 14 admitted workers were running. The reload helper now tolerates an already
exited second dashboard PID while continuing to reject changed process
identities. The corrected helper was not rerun after recovery; registered model
and worker sources were unchanged. The recovery receipt preserves this partial
reload and subsequent recovery explicitly.

## Evidence and remaining acceptance

Workspace evidence is retained under
`C:\Users\zmoor\Documents\forex\pair_coverage_20260907`:

- `COVERAGE_PREFLIGHT.json` and `QUOTE_METADATA_SNAPSHOT.json`: original bounded
  data-readiness and explicit pip-metadata observations.
- `ledger_tests/PAIR_LEDGER_EVALUATION_VALIDATION_20260907.json` and
  `ledger_tests/pair_ledger_evaluation_final.xml`: frozen source bindings and
  490-test result.
- `PAIR_SUPERVISOR_GATE_REVIEW_20260907.json` and
  `pair_reload_preflight_tests.xml`: isolated supervision and reload checks.
- `PAIR_REGISTRY_PREPARATION_20260907.json` and
  `PAIR_REGISTRY_ACTIVATION_20260907.json`: separate preparation and actual-clock
  empty-ledger activation receipts.
- `LIVE_PAIR_VERIFICATION_1788801540.json`: dated per-pair publication, input,
  original target and available later-entry checks, plus dashboard coverage.
- `PAIR_RUNTIME_RECOVERY_20260907.json`,
  `PAIR_RELOAD_RECOVERY_HEARTBEATS.json` and
  `PAIR_RELOAD_RECOVERY_PROCESS_SNAPSHOT.json`: controlled-reload interruption,
  verified recovery and preserved existing process identities.
- `before_source/`: preserved supervisor and research-launcher bytes before this
  revision.
- `prepare_pair_registry.py`, `verify_pair_reload.py`, and
  `reload_pair_collection.ps1`: separate reviewed preparation/activation,
  read-only readiness verification, and controlled reload operations.

**Remaining acceptance:** collect outcomes at the unchanged H1 targets, retain
late/missing entries and targets as exclusions, and verify each pair's actual
forecast coverage over time. Existing study source hashes and earlier dated
validation receipts were verified unchanged. Final source-bound validation and
vault synchronization are recorded separately after runtime/UI review.

These models emit **uncalibrated estimates**. A displayed probability such as
98.6% is a model output, not measured accuracy. More forecast coverage does not
establish improved accuracy or profitability, and overlapping pair/model records
are not independent trials. Fresh outcomes still need direction, probability,
magnitude and after-cost comparisons with the fixed baselines. No trading gate
is relaxed by this coverage revision.
