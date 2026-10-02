# Forex handoff

## Current continuation — exact quote rollout staged

[Current checkpoint](docs/EXECUTION_INPUTS_STAGED_20261002.md): Exact-quote worker integration staged, not activated:33tests and33isolated restored tests;four profile-validation cases passed. Active worker/profile/contract unchanged. Next narrow service rollout authorization, then matched quote/economics binding. Parent partial;score35/45 unchanged. Earlier notices are historical.

## Current continuation — prospective paper book

[Current checkpoint](docs/PAPER_STATE_PRODUCER_20261001.md): Prospective empty paper book recorded;52 distinct regression tests plus14 final-source reruns;84 exact replay payloads.2261 available forecast slots at the recorded clock. Execution receipts/economics remain unbound;parent partial;no orders or runtime changes;score35/45 unchanged. Earlier notices are historical.

## Current continuation — paper-state lifecycle

[Current checkpoint](docs/MANAGEMENT_STATE_BINDING_20261001.md): Offline lifecycle consumer:78 tests and84 exact resumed/restored payloads. Current paper-state producer remains unqualified; parent retained_management_current_state_binding_v1 stays partial. No runtime or trading changes; score35/45 unchanged. Older notices below are historical.

## Current continuation — input repair deployed

[Current checkpoint](docs/CURRENT_NEWS_READ_DEPLOY_20261001.md):106 deployment tests; fresh source/forecast receipts and68 histories verified. Input-reliability gate accepted within scope. Score35/45 (77.78% overall;83.33% active). Next `retained_management_current_state_binding_v1`; live management remains unqualified. Earlier notices are historical.

## Current continuation — management contract verified

[Current checkpoint](docs/RETAINED_MANAGEMENT_CONTRACT_20261001.md):196 tests;84 exact resumed/restored outputs; offline contract complete. Live current-news file contention and real management inputs remain open. No activation. Score34/45 unchanged. Older notices below are historical.

## Current continuation — measured completion

[Current checkpoint](docs/COMPLETION_MEASUREMENT_20261001.md); [design scorecard](docs/COMPLETION_SCORECARD.md):34/45 milestones,75.56% overall;80.95% excluding deferred GPT. Same-task assessment; historical evidence is identified separately. Run `python tools/forex_completion.py` and review affected credits at each checkpoint. Input availability and management remain partial. Older entries below are historical.

## Current continuation — scheduler completion handoff

[Current checkpoint](docs/SCHEDULER_THROUGHPUT_20261001.md): completed work serviced between settlements;102 deployment/35 relocated tests passed; live latency reduced and68 histories preserved. Remaining pair recovery and management are partial. Older entries below are historical.

## Current continuation — bounded health-file read recovery

[Current checkpoint](docs/HEALTH_READ_RECOVERY_20261001.md): Windows health-file read retries deployed;97 deployment/30 relocated tests passed;68 histories preserved. Remaining pair recovery and management are partial. Older entries below are historical.

## Current continuation — retained postprocessing progress repaired

[Current checkpoint](docs/POSTPROCESS_PROGRESS_20261001.md): genuine completed-work progress deployed;109 deployment tests/13 relocated tests passed; full live cycle verified under unchanged180second gate. Original sources and68 histories preserved. Next: remaining pair-input recovery, then management qualification. Older entries below are historical.

## Current continuation — durable producer diagnostics

[Current checkpoint](docs/NEWS_FAILURE_JOURNAL_20261001.md): original producer failures/recoveries now persist across successful heartbeats;101 deployment tests and6 relocated tests passed. Prior14 failures remain unexplained; inspect the journal, then finish pair recovery and management qualification. Older entries below are historical.

## Current continuation — duplicate-reference recovery

[Current checkpoint](docs/PAIR_RECOVERY_20261001.md): skip guaranteed duplicate-reference fits; 90 deployment tests and 23 relocated tests passed. Original 68 histories preserved. News-producer intermittency and per-pair recovery remain partial; management follows. Older entries below are historical.

## Current continuation - collector freshness repaired

[Verified collector repair](docs/COLLECTOR_TIMING_20261001.md): actual progress publication fixed;92 deployment checks and25 relocated tests passed.246 live samples through cycle completion had no freshness refusal or new joint errors.192 sources checked,31 records inserted. Next: remaining per-pair readiness under `live_news_input_reliability_v1`, then management. Older checkpoints below are historical.

## Current continuation - news scheduler deployed

[Deployment checkpoint](docs/NEWS_SCHEDULER_DEPLOY_20261001.md): overdue-news scheduling repaired;78 tests,12 relocated tests,68 preserved ledger identities and three accepted live refresh cycles verified. Remaining unavailable-pair/refusal reconciliation is `live_news_input_reliability_v1`; management remains pending. Older checkpoints below are historical.

## Current continuation - news scheduling diagnosis

[News timing checkpoint](docs/NEWS_TIMING_20261001.md): refresh starvation reproduced and minimal correction tested; deployment remains pending source-lineage qualification. Resume `live_news_input_reliability_v1`. Existing live services unchanged. Older checkpoints below are historical.

## Live operational checkpoint â€” October 1, 13:32 UTC

[Rolling input publication repair](docs/ROLLING_INPUT_RELIABILITY_20261001.md): Windows atomic-publication repair deployed;75 local and75 relocated tests,20 operational tests plus2 subtests passed. Dashboard/current inputs verified;39 retained connections unchanged. Resolve the live Vault pointers for the current queue. Next remains `live_news_input_reliability_v1`; position management is not activated. Checkpoints below are historical.


## Current combined-observation checkpoint — October 1, 2026

[Combined quote and forecast observations](docs/RETAINED_COMBINED_OBSERVATION_20261001.md): offline integration complete; 62 tests and 84 exact resumed/restored payloads passed. All 2,652 slots covered. Preserved inputs produce zero fresh priced observations at the replay clock; live rollout and position management remain unfinished. Next `retained_management_contract_qualification_v1`. Older notices below are historical.


## Current retained-receipt checkpoint — October 1, 2026

[Retained forecast receipts](docs/RETAINED_FORECAST_RECEIPTS_20261001.md): separately named observation adapter over saved publications;69tests and84payload exact resumed/restored replay passed. Preserves4953observation receipts across2652slots;2418forecasts eligible only at the frozen capture clock. No new inference or activation. Next `retained_management_combined_observation_v1`; combine exact quote and forecast observations offline. Older notices below are historical.


## Current quote-receipt checkpoint — October 1, 2026

[Exact quote receipts](docs/EXACT_QUOTE_RECEIPTS_20261001.md): opt-in decimal-preserving stream hook, bounded sidecar and existing-manager adapter implemented offline. 137 tests passed;64 retained raw records parsed and replayed exactly after isolated restore. No live rollout or management activation. Next `retained_management_forecast_receipts_v1`; adapt saved forecast receipts under their real input tier. Older next-item notices below are historical.


## Current management-readiness checkpoint — October 1, 2026

[Retained management readiness](docs/RETAINED_MANAGEMENT_READINESS_20261001.md): full 2,652-slot inspection, 48 tests and 43-payload portable/resumed replay passed. Forecasts remain connected; management is not activated. Missing native receipts, exact quote identity/decimal clocks and economic/state contracts are explicit. Next `retained_management_quote_receipts_v1`; repair the existing quote receipt boundary offline. Older notices below are historical.


## Current full-horizon connection checkpoint — October 1, 2026

[Preserved parents and curve](docs/RETAINED_CURVE_CONNECTION_20261001.md): 39 connections across 14 elapsed horizons; exact eight-horizon Ridge/HGB panels and two negative-result curve research references. No fits. 105 Python tests, four dashboard scenarios and 2,476-output relocated replay passed. Next `retained_position_management_readiness_v1`. Position management and exact-target gaps remain open; earlier notices below are historical.


## Current learned-state checkpoint — October 1, 2026

[Preserved learned residuals](docs/RETAINED_LEARNED_RESIDUAL_CONNECTION_20261001.md): two distinct saved 18h layers connected; zero-weight 6h states alias existing projections. 25 connections, no fits. 91 Python tests, three dashboard scenarios and 1,580-output relocated replay passed. Exact next `retained_curve_parent_panel_connection_v1`. Broader work remains unfinished; earlier notices below are historical.


## Current freshness checkpoint — October 1, 2026

[Freshness continuity](docs/RETAINED_FRESHNESS_CONTINUITY_20261001.md): existing saved inference now follows technical publication changes. 82 tests passed; seven native publications and six exact intervals showed no total forecast-expiry gap during the six-minute observation. Current news and 23 tracking groups verified. Next `retained_learned_residual_state_connection_v1` qualifies preserved state reuse without fitting. Broader project work remains unfinished; older notices below are historical.


## Current projection checkpoint - October1,2026

[Currency projection connection](docs/RETAINED_PROJECTION_CONNECTION_20261001.md):23connections across11elapsed horizons; eight fixed currency layers added to fifteen unchanged saved connections. Zero fits. Historical528output replication,1456output relocated replay and native23group tracking verified. Next `retained_input_freshness_continuity_v1` repairs the observed input-expiry gap. Learned residuals and position management remain unfinished. Older notices below are historical.


## Current checkpoint - October1,2026

[Saved6h/18h connections](docs/REMAINING_CONNECTIONS_20261001.md): fifteen connections across11 elapsed horizons; original11 preserved. No new fits. Bounded target and parent reconciliation is complete with16 unqualified exact targets explicit. Next `retained_currency_projection_connection_v1`. Layer integration and position management remain unfinished. Earlier next-item notices below are historical.


The Vault is the shared brain; Git supplies the common source. Read the repository
[README](README.md), then use this machine's `thevault/projects/forex` path.
This page contains navigation, not a second copy of the current queue.

New machine: [START_HERE.md](START_HERE.md). A versioned [Forex Vault knowledge
copy](vault/README.md) is included for reading; use the live shared Vault for claims.
The dashboard is Forex-only; [current diagnosis](docs/DASHBOARD_STATUS_20260930.md)
separates page availability from measured input and forecast health.
[Pipeline operations](docs/PIPELINE_OPERATIONS.md) supplies the canonical local
status/validation/start commands and recovery expiry.

## Current user scope — data and retained-model connection

Read [data repair and horizon coverage](docs/DATA_AND_MODEL_CONNECTION_20260930.md). New fits and experiments are stopped by the user. Eleven retained connections across nine elapsed horizons are connected; missing design targets and unverified global model rankings remain explicit. Exact next is `data_freshness_and_remaining_horizon_reconciliation_v1`. The earlier typed-news candidate is deferred. Live Vault `LEGACY26_RESTORE_COMPLETION_20260930` records the current connection checkpoint; older research notices below are historical.

## Earlier orientation — 2026-09-30

Read [project state and existing evidence](docs/PROJECT_STATE.md) and its
[machine-readable index](artifacts/research_evidence_index.json) before proposing
research. The existing mean-reversion, specialist and residual-error studies must
be reconciled with any new news/technical result. The September30 momentum batch
did not evaluate the complete technical model stack and does not supersede it.

The user authorized restoration of the flowing research pipeline and Git/Vault
publication. Current repair: Vault `ROLLING_NEWS_PIPELINE_20260930`, pointer
`PROJECT_CONTEXT_LATEST.json`. Joint V11 consumes a bounded rolling parsed-news
index; full transport replay no longer blocks current capture. Native ownership,
fresh capture and the corrected technical reader passed live verification. Joint
forecasts still require prospective mature history and varied news context.
Read the measured health and exact resume before assuming forecasts are available.
Scientific readiness remains separate from Git synchronization.
The September30 technical successor remains offline. The corrected typed news context
is now live in its supervised publisher and dashboard consumer; see
[news context](docs/CURRENCY_NEWS_CONTEXT_20260930.md). Original model inputs and
cohorts remain unchanged pending prospective feature qualification.

Current capture/mapping completion: [fast headline feeds and prospective tracking](docs/NEWS_CAPTURE_MAPPING_20260930.md). The native context worker records current technical snapshots and saved forecasts with actual observation clocks. Typed context still needs separate forecasting qualification; pending targets and joint-model warmup remain explicit.

## Completed successor — 2026-09-30

Extra Trees is now completed: [results and restore](docs/EXTRA_TREES_AND_MAPPING_20260930.md). The historical source-only notice below is superseded. At that historical checkpoint, queue next was `typed_currency_news_feature_cohort_qualification_v1`; it is now deferred by the user; joint warmup remains a separate real-data requirement.

## Historical design-queue checkpoint — 2026-09-25 (superseded)

The four-hour correction session ended at its fixed deadline; no worker remains
active under that claim. The shared state is the Vault packet
`FOUR_HOUR_DEADLINE_HANDOFF_20260925_180439/HANDOFF.md` and Git commit
`6eb514ab1856190b39d83ab020d5f77700ca8770`. Six scoped repair/qualification
packets are complete. The exact next queue item is
`extra_trees_matched_development_comparison_v1`, with a **source-only partial
draft** at `stage_c_alignment_integrity_v2/extra_trees_matched_v1.py`.
No Extra Trees fit, forecast, score, review, or scientific result is claimed.
Start it only after fresh authorization, current preflight, and focused tests.

| Question | Authoritative Vault record |
|---|---|
| What must be finished before research? | `OPERATIONAL_READINESS_LATEST.json` and its `OPS_STATUS.json` |
| Which design/source is current? | `DESIGN_ALIGNMENT_LATEST.json` and its design, coverage, path-forward and source manifest |
| Which checkpoint was reviewed? | `CHECKPOINT_REVIEW_LATEST.json` and `REVIEW_QUEUE.json` |
| What is next? | `REVIEW_QUEUE.json`; finish required operational work and unresolved review findings first |
| Is somebody already working? | Active section of `CHAT_COORDINATION_BOARD.md` |
| Which Git commit/checkpoint is shared? | `SHARED_GIT_REMOTE_LATEST.json` |
| Does this model/run already exist? | `VAULT_FIRST_REUSE.md`, Git `artifacts/reuse_catalog.json`, and original run records |

Before implementation, read the active claim and run
[operational preflight](docs/OPERATIONAL_PREFLIGHT.md). Reconcile any blocked result;
do not bypass a missing dependency or replace an existing model. After preflight,
claim the exact scope on the Vault board and re-read it before edits or computation.
The synced board is advisory, not an atomic cross-machine lock. If ownership or shared
freshness is uncertain, defer duplicate computation and continue unrelated eligible work.

Use [artifact guidance](docs/ARTIFACT_REUSE.md) for byte retrieval. A recovery recipe
can refit; inspect its contract before treating restoration as saved-model retrieval.
The code/data/source/environment identities attached to a model remain binding.

During work keep `WORK_LOG.jsonl`, `PENDING_CHANGES.md`, raw command outputs and
baseline/after identities in the local step evidence directory. Follow the Vault
`CHECKPOINT_REVIEW.md`; publish a compact immutable packet and link it from
[project history](trad/FOREX_PROJECT_LOG.md). The
[legacy pending record](trad/FOREX_PENDING_IMPROVEMENTS.md) preserves context;
the live Vault queue sets order. [Directory map](docs/DIRECTORY_MAP.md) explains the layout.

Timed forecasting selection follows [forecasting continuation](docs/FORECASTING_CONTINUATION.md)
and the Vault FOREX_FORECASTING_CONTINUATION.md. A blocked experiment is local to its
dependents; continue supported siblings during the user's requested duration.

Timed launches and final-response checks use [run control](docs/TIMED_RUN_CONTROL.md).
The September 24 stopped session and its correction history are historical records;
do not restart it or count its idle gap as work. The current shared pointers above,
not historical handoff prose, determine the next action.

For detailed research continuation and a two-hour launch prompt, use
[research handoff](docs/RESEARCH_HANDOFF.md), then the Vault
`FOREX_RESEARCH_PATH.md`. That path defines the next implementation acceptance gate
and progression toward indicator/layer comparisons and position rotation.

Read the full engineering design once, then relevant requirements and verified handoff.
Preserve original data, models, user edits and sealed packets. Passing tests, independent
review, forecast evidence, policy evidence and trading authorization are separate.
GPT/advisor comparisons, paid calls, broker/account/service actions and D-drive work
remain deferred. Operational readiness itself does not launch or authorize research.

Earlier notices and their exact bytes are retained in
[handoff history](docs/history/FOREX_HANDOFF_before_operations_20260923.md).


Historical September30 research handoff repair, superseded by the completion above: `RESEARCH_HANDOFF_REPAIR_20260930_195000/REVIEW.json`; see `docs/RESEARCH_HANDOFF_REPAIR_20260930.md`. Extra Trees still partial; no new fit/result yet.


Current continuation: [retained outcome tracking and data diagnostics](docs/RETAINED_TRACKING_20260930.md). Per-horizon prospective monitoring is implemented; no universal best-model claim. Exact next remains data_freshness_and_remaining_horizon_reconciliation_v1, including the original legacy26 transform for two supported saved Extra Trees comparisons.


Current legacy26 checkpoint: [saved Extra Trees connections](docs/LEGACY26_CONNECTION_20260930.md). Two existing frozen 4h/120h candidates join the nine preserved references using original receipt-qualified legacy26 inputs. 104 tests and 2696 exact saved forecast replications; no new fit. Next remains data_freshness_and_remaining_horizon_reconciliation_v1 for residual/curve and exact remaining targets.


Legacy26 final restore completion: LEGACY26_RESTORE_COMPLETION_20260930 supersedes the initial handoff for canonical CLI retrieval. Existing restore tool fixed/tested; canonical20-artifact restore and four numerical recorded-forecast replays passed. 104 implementation tests plus4 capsule tests; no deployed inference change or new fit. Final Git receipt LEGACY26_RESTORE_PUBLICATION_20260930/RECEIPT.json. Next remains data_freshness_and_remaining_horizon_reconciliation_v1.
