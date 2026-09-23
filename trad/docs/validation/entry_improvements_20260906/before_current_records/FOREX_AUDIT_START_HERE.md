# Forex — audit starting point

Latest sanity check (2026-09-06T15:53:01.014364+00:00): start with `PREDICTION_SANITY_CURRENT.md`,
`PREDICTION_SANITY_VALIDATION_CURRENT.json`, `MODEL_INVENTORY_CURRENT.md` and
`COMOVEMENT_RESEARCH_PLAN_CURRENT.json` in this vault. Source counterparts are
`docs/FOREX_PREDICTION_SANITY_20260906.md`, `FOREX_PREDICTION_SANITY_VALIDATION_20260906.json`,
`docs/FOREX_MODEL_INVENTORY_20260906.md` and `FOREX_COMOVEMENT_RESEARCH_PLAN_20260906.json`.
Corrected broad direction is 49.30%, after-spread positives remain8.51%, and all
four archived EUR/USD percentages reproduce. Joint-movement diagnostics and
actual D-drive model assets are inventoried. Exact-price scoring follow-up is
open. Collection and its registered source are unchanged; trading stays off.
Earlier dated observations and original counts follow as history.

Current update (2026-09-06T15:32:22.899414+00:00): the initial causal study had two Windows heartbeat
replacement failures. The fixed `io_r2` worker is separately registered and
running in collection mode; the original zero-forecast registration is preserved.
Trading stays off, and improved predictions still require fresh outcomes.
See [the current operational addendum](docs/FOREX_CAUSAL_IO_REPAIR_20260906.md)
and `FOREX_CAUSAL_IO_REPAIR_VALIDATION_20260906.json`.

Earlier dated observations follow; their registration details are historical.

Latest September 6 update: read `CAUSAL_TIMING_REPAIR_CURRENT.md`,
`CAUSAL_TIMING_REPAIR_VALIDATION_CURRENT.json`, `CAUSAL_TIMING_FOLLOWUP_CURRENT.json`
and `CAUSAL_FORECAST_STUDY_PROTOCOL_CURRENT.json` in this vault. Source equivalents
are `docs/FOREX_CAUSAL_TIMING_REPAIR_20260906.md`, the corresponding dated JSONs,
and `config/causal_forecast_study_v1_20260906.json`. Twelve research workers now
run, including a separate future-only study. Trading stays disabled. The clock
fixes are implemented; fresh prediction/outcome validation remains pending.
Earlier stopped and eleven-worker observations below are dated history.

Current runtime — September 6, 2026: Forex has restarted in collection-only
mode. Eleven data/dashboard/health workers run; execution and model/proof
production remain off. See [the restart record](docs/FOREX_RESEARCH_RESTART_20260906.md)
and `FOREX_RESEARCH_RESTART_VALIDATION_20260906.json`. Earlier stopped-state
observations below describe their recorded times and are now historical.

Latest September 6 follow-up: begin with `FIXED_EVALUATION_CURRENT.md`,
`FIXED_EVALUATION_RESULTS_CURRENT.json`, `FIXED_EVALUATION_VALIDATION_CURRENT.json`,
`FIXED_EVALUATION_PROTOCOL_CURRENT.json` and `FIXED_EVALUATION_FOLLOWUP_CURRENT.json`.
These are vault names. In source, use `docs/FOREX_FIXED_EVALUATION_20260906.md`,
the corresponding `FOREX_FIXED_EVALUATION_*_20260906.json` records and
`config/fixed_forecast_evaluation_v1_20260906.json`.

All four fixed EUR/USD families underperform simple baselines on 368 matching
archived endpoints. All 407 shared decisions lack the original availability
evidence for strict causal scoring. A new P1 entry/target-clock issue is open;
the earlier calibration issue remains open. The new evaluator passed 158 tests.
No live producer, collection or trading was started. Prior records follow.

September 6 update: begin with `OPTIMIZATION_REVIEW_CURRENT.md`,
`OPTIMIZATION_VALIDATION_CURRENT.json`, `PREDICTION_QUALITY_CURRENT.md`,
`PREDICTION_QUALITY_CURRENT.json` and `OPTIMIZATION_BACKLOG_CURRENT.json`.
Those are vault names; corresponding source files are
`docs/FOREX_OPTIMIZATION_REVIEW_20260906.md`,
`FOREX_OPTIMIZATION_VALIDATION_20260906.json`,
`docs/FOREX_PREDICTION_QUALITY_20260906.md`,
`FOREX_PREDICTION_QUALITY_20260906.json` and
`FOREX_OPTIMIZATION_BACKLOG_20260906.json`.

The measured improvement reduces repeated integrity-publication overhead while
preserving full evidence separately. Current shadow direction was 49.39%, with
8.51% positive after spread; this is not a live-trade win rate. No reliable edge
is demonstrated. All measurements use saved data and isolated fixtures.
The runtime remains stopped. The following dated records remain historical.

September 5 update: read `REPAIR_REVIEW_CURRENT.md`,
`REPAIR_VALIDATION_CURRENT.json`, `PERFORMANCE_AUDIT_CURRENT.md` and
`PERFORMANCE_AUDIT_CURRENT.json` first. The original independent findings are
preserved in `INDEPENDENT_AUDIT_20260905.md` and `.json`. These are vault names;
in source use `docs/FOREX_REPAIR_REVIEW_20260905.md`,
`FOREX_REPAIR_VALIDATION_20260905.json`, `docs/FOREX_PERFORMANCE_AUDIT_20260905.md`
and `FOREX_PERFORMANCE_AUDIT_20260905.json`.

The repairs were checked offline while the project stayed stopped. Source V7/V8
and rank V6/V7 are retired as preserved diagnostics. New source V9/rank V8
configurations ship disabled and require explicit future activation without
historical imports. News fastlane V3 preserves V1/V2 receipts, commits its
cursor with each batch, and requires consumer observation before prospective
entry use. A complete live integrity pass and prospective performance are
still unverified. The dated runtime observations below remain historical.

Prepared after the September 4, 2026 close. The project is intentionally stopped
for independent review and preparation for a future demo. Market/account
reports are dated snapshots. The vault is the shared record of the project.

## Read in this order

1. `AUDIT_STATE_CURRENT.md` and `.json`: observed runtime, source/feature
   lineages, current contracts and unresolved limitations.
   Then read `FAULT_AUDIT_CURRENT.md` for independently reproduced faults,
   audit scope and restart prerequisites. This is not a bug-free certificate.
2. `WEEK_TO_DATE_PROJECT_EVENT_MOVE_RECAP_CURRENT.md`: completed market review
   frozen through September 4 at 21:00 UTC.
3. `ISSUE_REGISTER_CURRENT.json`, `PENDING_IMPROVEMENTS_CURRENT.md` and
   `PROJECT_LOG_CURRENT.md`: repairs, collecting hypotheses and external gaps.
4. `NEWS_SOURCES_CURRENT.json`, `OFFICIAL_CENTRAL_BANK_SOURCE_MAP_CURRENT.json`,
   `OFFICIAL_CURRENCY_SOURCE_DEPTH_CURRENT.json`, `SOURCE_GAP_REGISTER_CURRENT.json`:
   providers, currency mappings and data coverage.
5. `source/WORKTREE_SOURCE_LATEST.json`: exact current source ZIP and manifest.
6. `RECREATION.md`: offline verification and historical-recovery limitations.
7. `SHARED_PROJECT_STATE_CURRENT.json`: current record hashes and sizes.

These filenames are relative to the Forex vault. In the extracted source use
`FOREX_AUDIT_STATE_CURRENT.json`, `docs/AUDIT_STATE_CURRENT.md` and
`docs/VAULT_RECREATION_CURRENT.md` for the corresponding guides.

## How to interpret the record

The frozen close review records 24 independent official event clocks and 130
material factor episodes of at least 15 basis points. Twenty-two clocks had
later cost-clearing movement; strict pre-move directional matches were zero.
The supported decision remains `no_trade`. Retrospective explanations do not
establish forecasts available before the move.

News classification V164 and watchlist V49 are current. Source-response V8 and
source-conditioned rank V7 preserve their separate frozen contracts; V8's
older classification binding is intentional. New classifiers do not rewrite
old proof. Exact identities and evidence dates are in the audit-state index.

The August 3 feature JSON and August 6 audits are historical design/reference
documents. Their counts are not current runtime census figures. Current source,
configurations, tests and validation receipts are in the source ZIP.

## Contents and recovery boundary

The vault source snapshot includes tracked and untracked current files, including the
September changes missing from the old August 30 Git snapshot. It records its
base Git commit and working-tree status without modifying local Git history.

The vault holds records and source recreation materials. Bulk legacy model
checkpoints, duplicate progress directories and obsolete exports move intact
to a verified local archive with recovery pointers under `maintenance/`.
Other projects remain untouched. No historical research evidence is deleted.

Credentials, installed environments, complete market/news histories and live
SQLite/WAL files are excluded. Original first-seen clocks, source revisions,
forecast records and executable quotes cannot all be reacquired later. Exact
historical reproduction requires the original local ledgers or a coherent
verified backup. Source-only recovery starts new research evidence.

## Next use

The updated vault can support independent review and a future offline demo
of movers, source mappings, technical context, misses and evidence gates.
Display the snapshot timestamp and current proof limitations. A demo should
read records without starting broker workers. Building/publishing it is a
later change.

Remaining gates include pre-release consensus, intraday policy repricing,
direct RBNZ transport and independent prospective confirmation. Cleanup does
not mark collecting hypotheses or external data gaps complete.

For the independent chat: review the entire project and recommend a reorientation,
starting from these current records and the exact worktree snapshot. Distinguish
implemented code, verified behavior, historical diagnostics and prospective proof.
Do not assume an old `CURRENT` filename means live freshness. Keep the project
stopped unless the user explicitly requests a restart.
