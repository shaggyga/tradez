# Forex causal study Windows publication repair — September 6, 2026

Recorded 2026-09-06T15:32:22.899414+00:00. This addendum supersedes the runtime registration described in
[the initial timing repair](FOREX_CAUSAL_TIMING_REPAIR_20260906.md).

## What the final runtime check found

The initial study exited twice at 15:11:26 and 15:12:03 UTC because Windows
temporarily denied replacement of heartbeat.json. The supervisor restarted it;
its own error counter and the replacement process's heartbeat counter were
zero, so those fields alone did not reveal the earlier crashes. The preserved
stderr logs and supervisor process-start events establish the failures.

The initial worker was stopped before repair. Its contract, runtime files and
logs were copied with hashes, and its database integrity check passed. It had
zero quotes, attempts, forecasts, entries and outcomes. No evidence was moved
into the replacement ledger. The original source is preserved in vault archive
forex_worktree_source_c35b3db95d65039953feeb97.zip (SHA256
efa58d7368d19a0d013e7e5ef70b8f39454888969f0365bf5947c62e39753bea); the initial dated validation
receipt describes that source snapshot rather than the subsequently changed worker.

## Repair and independent checks

Atomic publication now serializes and fsyncs once, then retries transient
Windows replacement failures up to eight times with at most 1.13 seconds of
total backoff. All attempts use identical bytes and timestamps. Temporary-file
cleanup only touches files created by that call and cannot mask its result.
Heartbeat publication failure retains the last good file, increments an error
counter for the next successful heartbeat and continues quote collection.
Persistently stale heartbeat output remains visible to the supervisor.
Scorecards use the same helper in their existing background worker.

The numerical models, inputs, calibration, ledger and scoring rules are
unchanged. Retry delays do not shift issuance, publication, entry or original
target clocks; any quote gaps remain subject to the existing exclusions.

All 63 worker/retirement/register regression tests and 16 independent
publication tests passed. The supervisor gate again admitted exactly twelve
workers and blocked 107 names, including an unknown future worker, with no
real process actions during testing. The initial timing repair's 421 tests
and 29 subtests remain historical evidence for that repair.

## Current collection and pending evidence

The separately frozen contract
`causal_four_family_future_collection_v1_io_r2_20260906` activated at
2026-09-06T15:30:24.952645+00:00. Its actual contract SHA256 is
bc99e68db720e0a43b3ec68a7820b3605013d0c2138515f23e648f5b34fadc2a. It uses new cohort identities and the separate
`data/oanda_training_manager/causal_forecast_study_v1_io_r2/study.sqlite` ledger.
The old contract file and database remain unchanged and are no longer selected
by the supervisor. The current worker rejects their obsolete source binding.

At this observation twelve collection workers are running, the new heartbeat
has zero errors and is waiting for tradable quotes, and the practice account
has zero open trades or pending orders. Trading, promotion, proof eligibility,
source V9, rank V8 and both Forex scheduled tasks remain disabled.

There are still zero new forecasts or outcomes and no demonstrated prediction
improvement. The old strict recheck remains 407 decisions, 1,628 forecasts and
zero valid causal comparisons. Fresh collection requires 335 consecutive
common minute bars after a market gap (about 5.5 hours), plus collector lag,
cadence and a one-hour target before any new outcome can be scored.
An untouched after-cost sample and independent acceptance remain pending.

The vault source archive excludes runtime databases and WAL files. Continuing
this exact registration after recovery requires its database/WAL and contract;
a source-only recovery requires a separately registered study. Only local
vault bytes are verified; cloud synchronization completion is not observed.
