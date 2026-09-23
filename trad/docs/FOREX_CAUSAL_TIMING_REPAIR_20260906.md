# Forex prediction timing repair — September 6, 2026

Recorded: 2026-09-06T15:16:14.282917+00:00. Collection is running with twelve admitted workers. The
separate four-family study activated at 2026-09-06T15:08:20.115287+00:00 and is
waiting for tradable EUR/USD quotes. It has zero forecasts and zero outcomes
at this observation. Trading, promotion and old model/outcome/calibration
producers remain disabled. The practice account has zero trades and orders.

## Repairs completed

The old four-family archive used pre-computation entry quotes, lacked
availability evidence and mixed original-entry and recording-relative targets.
The successor reads exact bounded M1 candle bytes for seven fixed major pairs,
retains hashes and observed-now clocks, aligns real timestamps, and requires
335–512 consecutive common minute bars. It reuses the four numerical algorithms
with frozen parameters; the seven-pair input universe is a new contract, not a
continuation of old cohorts. No model was selected by these new outcomes.

After all model computations complete, it records conservative feature and
learning-data availability, then samples actual issuance. All four forecasts
commit together. A separate postcommit receipt and an independent consumer
read precede the first later executable entry quote. Targets always remain the
original reference quote time plus one hour. Crashes cannot backdate visibility
or restart a horizon. Missing/late entry or target quotes become exclusions.
Captured training history is allowed as newly observed input; old forecasts,
outcomes and missing historical clocks are never imported or manufactured.

The old calibration replay still preserves its numerical diagnostics, but new
publications cannot advertise validation/proof/account readiness. A separate
V2 offline evaluator admits only unique, correctly scoped labels whose target,
quote maturity and committed availability precede each original forecast issue.
It handles unordered inputs, duplicated market events, ties and flat outcomes.
The strict comparison scorer now requires the same strictly later quote boundary
as the collector, including at equal timestamps.

## Operation and resource use

Only the separate study ledger is written. It has no broker or order interface
and does not feed the old signal, outcome, lifecycle or promotion databases.
Input capture is bounded to one MiB per pair and at most 1,024 retained source
rows; exact captured evidence is compressed in SQLite. A single model build
uses seven pairs and at most 512 common rows; native numerical thread counts
are limited to one. Quote/outcome collection continues while a model fits.
Scorecards run on a separate read-only database snapshot every fifteen minutes,
outside the quote polling loop. No automated evidence deletion was introduced.

Source, dependency, model and feature bindings are frozen in the registered
contract. Changed code or parameters require a separately registered study;
they are not silently accepted into this one. Windows process locking prevents
duplicate study workers. Both existing Forex scheduled tasks remain disabled.
The research supervisor was reloaded while existing collection children stayed
running; its old eleven-worker runtime had first been restored after it was
found stopped at the start of this resumed session.

Protocol: `config/causal_forecast_study_v1_20260906.json`.
Live heartbeat, scorecard and ledger: `data/oanda_training_manager/causal_forecast_study_v1/`.
These runtime files change; the validation evidence includes frozen observations.
The vault source export is not a backup of the active study's SQLite ledger.
Recover that ledger with its database/WAL and registered contract; a source-only
restore must use a fresh contract rather than silently recreating this study.
The scorecard uses the conservative offline evaluator's diagnostic schema.
The new ledger is prospective; independently confirmed performance remains pending.

## Verification and limits

The combined offline suite passed 421 tests and 29 subtests. A final focused
worker run passed 55 tests after retaining the original provider quote fields.
Independent ledger review passed 106 adversarial tests, including actual
synthetic candle capture and all four numerical models. Independent calibration
review matched a full-scan oracle for 5,000 randomized forecasts. Isolated
PowerShell checks admitted exactly twelve workers and rejected 107 other names,
including an unknown future worker. No test placed an order or touched a
production database. The initial combined test harness misread Windows file
URIs, producing 47 fixture-boundary errors; correcting URI decoding retained
the same write/network restrictions, and the complete rerun passed.

The actual newly registered ledger passes SQLite integrity checking and exactly
matches the frozen contract. Twelve admitted workers are running, the dashboard
responds, the new heartbeat is fresh, and no supervisor errors were observed.
The broader project audit remains degraded/no_trade; deliberately disabled
components and unresolved evidence checks were not disguised as healthy.

Both P1 repairs are implemented and awaiting evidence, rather than declared
fully accepted. The study needs at least 335 uninterrupted common minute bars
(about five and a half hours after a gap) and fresh tradeable quotes. Data
refresh cadence can add delay. After that, one-hour outcomes must mature.
Accuracy and net returns cannot be judged from zero new outcomes. Overlapping
fifteen-minute forecasts are not independent trials. Source/clock checks are
engineering evidence, not an independent audit of predictive edge or an order
authorization. Fitted weight artifacts are not retained; reproducibility binds
source, fixed parameters, exact captured inputs and dependency versions.

Rechecking all 407 old decisions still yields zero valid causal pairs, with
all 1,628 archived forecasts retained. Prior findings—49.39% shadow direction
and 8.51% positive after spread; all four fixed archived models below simple
baselines—are unchanged historical diagnostics. This repair improves measurement
integrity. It does not claim that predictions have already improved.
