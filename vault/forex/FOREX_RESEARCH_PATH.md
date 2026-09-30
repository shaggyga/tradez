# Research path: indicators, forecast layers and position rotation

This is the detailed execution handoff for the user's next offline session, including
GPT-5.6 Terra with medium thinking. Follow it with `FOREX_NEXT_PROMPT.md` or
`FOREX_TIMED_PROMPT.md`. Preparing this document did not start that session. The full
engineering design remains the goal; this page explains its next actionable route.


Read [FOREX_FORECASTING_CONTINUATION.md](FOREX_FORECASTING_CONTINUATION.md) for
the current forecasting selection policy and independent alternatives. A blocked
confirmation experiment does not stop development siblings during a timed run.

## 1. What the user wants

Establish whether improved indicators, macro information and additional forecast layers
improve honest forecasts, and whether those forecasts improve holding, reducing or
rotating positions after costs. Deliver reproducible backtests and explicit evidence,
including negative or inconclusive results. Do not equate more models, a dashboard,
passing software tests or a profitable development slice with that outcome.

Git holds the complete shared source. The Vault holds shared knowledge, queue, artifact
identities and reviews. Local machines execute. Reuse existing models and runs before
fitting anything. G verifies his own machine; his setup is not a prerequisite for our
offline work. Live-bot recovery is a separate scope and does not block this path.

## 2. Authority and starting point

Resolve these live records at session start; the anchor below is a dated orientation,
not permission to roll back a newer accepted checkpoint:

1. `CURRENT_STATUS.md`, `OPERATIONAL_READINESS_LATEST.json` and its status.
2. `CHECKPOINT_REVIEW_LATEST.json` and `REVIEW_QUEUE.json` for current document pins,
   open review findings, partial work and the exact next item.
3. `DESIGN_ALIGNMENT_LATEST.json` for the full design, source manifest, coverage,
   acceptance addendum and broader queue. Read the full design once; then use the
   relevant sections. This path supplements it.
4. `CHAT_COORDINATION_BOARD.md`, `VAULT_FIRST_REUSE.md`, project
   `artifacts/reuse_catalog.json`, `LOWER_MODEL_OPERATIONS_DESIGN.md` and the current
   project `stage_c_alignment_integrity_v2/OPERATOR_CONTRACT_V2.md`, then the selected
   package's specific contract/recipe. Old dated notices are historical; live pointers govern.

Verified anchor on 2026-09-23:

| Record | Identity / interpretation |
|---|---|
| Engineering package | `MACRO_METER_20260923_042731` |
| Design SHA256 | `61d9713034f6f058d9f59ff11c0dd05b2a7e36583dffb2cd6d6300c64b8dc374` |
| Engineering manifest SHA256 | `f29d0e398ed6e9acc812baf9169d11e84d67bfaae141f1c027f1917a09ddeefe` |
| Immediate predecessor | `macro_currency_meter_evidence_view_v2`, accepted within scope by `FOLLOWUP_REVIEW_20260923_042826`; same-implementer review, independent review not performed |
| Historical next item at that anchor | **`macro_currency_meter_numeric_evidence_join_v2`** (subsequently completed) |
| Operational prerequisite | Complete and independently reviewed; resolve its current pointer |
| Current Git revision | Resolve `SHARED_GIT_REMOTE_LATEST.json`; do not copy an older commit from a dated log |

The old meter packet's `RUN_STATUS.json` and `FULL_DESIGN_QUEUE.md` still say to review
the meter. That review was subsequently completed in the live queue. Do not reopen it
just because the immutable packet predates the review.

`research_authorization: false` in the operational receipt means that operational
verification itself granted no research authorization. A subsequent user NEXT/timed
instruction supplies its own offline scope. Leave the receipt flag unchanged; current
preflight intentionally expects it. This is not a permanent research veto.

## 3. Session startup and partial work

Original roots are `C:/Users/zmoor/Documents/forex` (whole Git root), its
`stage_c_alignment_integrity_v2` source folder, and
`C:/Users/zmoor/OneDrive/thevault/projects/forex` (Vault). Map equivalent roots on a
replica. Use the existing isolated interpreter; on this machine it is
`C:/Users/zmoor/AppData/Local/CodexRuntimes/timeseries312/Scripts/python.exe`.

Before new edits or runs, inspect `git status --short`, active board claims, live queue
and any last session/step evidence. Preserve user edits. Resume an unfinished owned
step using its exact evidence and command; do not create a competing copy. A stale
claim is not permission to take over another operator's work. Reconcile ownership.

For a clean new baseline, run the existing read-only preflight from the Git root:

```powershell
& 'C:/Users/zmoor/AppData/Local/CodexRuntimes/timeseries312/Scripts/python.exe' -X utf8 -I -B tools/forex_preflight.py --vault 'C:/Users/zmoor/OneDrive/thevault/projects/forex'
```

Use the engineering profile for implementation/numerical work. `--profile stdlib`
qualifies document/byte work only. An already approved recipe may instead use the
documented `--recipe` and independently recorded `--recipe-sha256` options. Read
project `docs/OPERATIONAL_PREFLIGHT.md` for exact refusal handling.

After a passing baseline, claim the exact scope and reread the board before editing.
For your existing claim, preflight accepts `--claim-id <the-recorded-task-id>`. During
implementation the tree is expected to be dirty: retain the recorded clean baseline,
use scoped tests and operator checks, and finish publication before the next clean
preflight. Do not discard partial work, alter approval hashes or weaken checks to
make startup green. An interrupted publication must be reconciled from its receipts.

The board is advisory, not an atomic cross-machine lock. Unknown ownership of the same
experiment blocks duplicate computation, not unrelated eligible engineering work.
Local readback does not prove OneDrive synchronization.

## 4. Historical completed package: numeric evidence in the currency meter

**Historical goal (completed; do not restart):** complete the bounded package within Design 14.2–14.5 / WP6 by joining already retained numeric and
provenance evidence to the current meter's selected documents. This is bounded
evidence-consumer engineering. It does not admit historical features, train models,
establish surprise or prove better forecasts.

### Reuse map

All source paths below are relative to `stage_c_alignment_integrity_v2`:

| Existing component | Purpose |
|---|---|
| `macro_meter_v2.py` | Existing build/render consumer and shared currency/pair views |
| `macro_meter_operator_v2.py` | Existing `status`, `run`, `resume`, `verify` protocol |
| `macro_meter_checkpoint_v2.py` | Bounded `export` / hash-pinned `restore` |
| `MACRO_METER_CONTRACT_V2.md`, `MACRO_METER_OPERATOR_RECIPE.json`, `test_macro_meter_v2.py` | Preserve predecessor contract, recipe and regression behavior |
| `macro_numeric_state_v2.py` | Numeric source-as-of states and numeric material cache |
| `macro_component_state_v2.py` | Repaired component state, `validate_parent`, `evidence_index`, `join_event` |
| `macro_unit_binding_v2.py` | Typed unit bindings and unresolved evidence |
| `macro_provenance_v2.py` | `join_provenance` and provenance bindings |
| `publication.py`, `contracts.py` | Existing identity/publication contracts |

Retained local evidence is under the source folder's `evidence/`:

- `four_next_20260923_040615/meter/inputs` and `meter/runs/meter-20260914-20260921`;
  the sibling `provenance` folder contains the provenance run.
- `timed_20260922_194458/macro_numeric_state` and `macro_component_state`.
- `next_unit_binding_20260922_204402/unit_binding`.

Portable predecessor archives in the Vault:

- `MACRO_METER_20260923_042731/checkpoint/forex_macro_meter.zip`
- `MACRO_NUMERIC_STATE_20260922_200911/checkpoint/forex_macro_numeric_state.zip`
- `MACRO_COMPONENT_STATE_20260922_203059/checkpoint/forex_macro_component_state.zip`
- `MACRO_UNIT_BINDING_20260922_205445/checkpoint/forex_macro_unit_binding.zip`
- `MACRO_PROVENANCE_20260923_041831/checkpoint/forex_macro_provenance.zip`

Resolve approved hashes and exact members from the sealed parent manifests/receipts.
Do not derive a new approval from possibly modified local bytes. Inspect each restore
contract before running it; retrieval is not permission to refit. Preserve all parent
manifests, recipes, inputs and completed results. Issue a new successor identity for
the changed consumer. Do not rewrite the old recipe to make changed code look approved.

### Implement in this order

1. Record a step contract, baseline and acceptance checklist in the local evidence
   directory. Inspect the existing schemas and bind each parent by approved hash.
2. Join `numeric_source_asof` / `numeric_material_cache`,
   `repaired_component_source_asof` / `component_evidence_cache`, `unit_source_asof` /
   `unit_source_bindings` / `unit_unresolved_evidence`, and `provenance_source_asof` /
   `provenance_bindings` to the meter by exact cutoff, event, selected version, source
   and content identity. Verify actual filenames/schema within each parent run.
3. Extend structured meter output and its existing document drilldown together. Show
   actual, prior and revised-prior values where supported; typed units, component
   reference periods/seasonality, exact retained evidence and availability reasons.
   Keep semantic concepts and numeric qualifications inspectable side by side.
4. Preserve all 21 currencies and 68 instruments, their shared-state references and
   current text-count semantics. Preserve distinct absent-source, usable-no-concept,
   explicit-hold and unavailable-numeric states.
5. Freeze the successor recipe with source/config/input/environment hashes, bounded
   resources and deterministic status/run/resume/verify. Carry checkpoint export and
   relocated restoration through the actual changed outputs and consumer.

Never cross-aggregate incompatible units/periods, invent consensus or surprise, infer
FX direction, borrow a different document version, infer missing ABS reference years,
backdate BLS numeric readiness, or turn provenance into runtime adapter activation.

### Required acceptance evidence

| Case | Required result |
|---|---|
| Correct retained event/version/cutoff | Exact numeric/component/unit/provenance evidence attached to the selected document |
| Wrong event, version, source, content, cache or cutoff; duplicate/conflicting binding | Refusal or explicit unavailable state according to the frozen contract; no silent fallback |
| Future or missing numeric readiness; unit/period mismatch | Cannot enter an earlier historical state or become a usable numeric feature |
| Missing prior/consensus/year/seasonality/provenance | Preserve nulls and concrete reason; distinguish them from an observed zero |
| Actual inspector | Exercise numeric selection, reset, empty/blocked state and escaped hostile text through the consumer |
| Universe regression | All 21 currencies/68 instruments and shared references preserved |
| Corruption / interrupted writer | Hash refusal, one-writer behavior and exact same-run resume |
| Bounded real-data replay and unrelated-directory restore | Verify changed payloads/identities and deterministic replay equality under the declared contract |

Run relevant regressions and new failure cases; record actual counts/commands/outputs.
Do not replace consumer verification with filename/import checks. Complete the review
packet and resolve required findings. This package is done when the joined evidence,
consumer, operator and restore gates are demonstrated—not when every missing macro
source is recovered. Forecast and policy evidence statuses remain unconfirmed.

## 5. After that checkpoint: progress toward the research endpoint

First resolve review findings on the exact join diff. A timed session may review and
continue; record reviewer identity and independence honestly. Accepted historical
same-implementer review is not automatically an unfinished independent-review task.
Never promote a pending review to accepted merely because tests passed or time remains.

The numeric-join starting point above is historical. Resolve current candidate cards
through FOREX_FORECASTING_CONTINUATION.md and REVIEW_QUEUE.json. Use Design 27.5
and current R04/R05/R09/WP7 evidence to select
the earliest eligible remaining gap that isolates the largest supported uncertainty.
Make that choice concrete with the candidate card below, append the exact work item
and dependencies to the live queue, and execute within the user's offline scope.
Use existing nonblocking defaults; ask only when an actual missing decision is needed.

| Stage and design sections | Deliverable and advancement gate |
|---|---|
| Supported features / WP6–7, §§10–15, 27.4 | Matched baseline versus one material indicator-family, cross-currency, event/text/numeric or observed-reaction change. Report incremental value and coverage. Unsupported numeric expectations do not block technical/text/event work. |
| Forecast layers / WP7, §§11–13, 16 | Bounded interaction, distinct supported method, calibration or stacking comparison. Reuse saved predictions; establish honest chronological base-prediction and mature-label support before any learned second layer. |
| Rotation / WP8, §§17–18, 27.3 | Hold/cash versus eligible recovered, naive, cost-aware and persistence/hysteresis policies on the same qualified tape. Diagnose forecast quality, cost clearance and management before tuning additional knobs. |
| Confirmation / WP9, §§13, 20, 27.5 | Freeze the procedure, evaluate protected later/prospective blocks and execution/risk stresses, report matched uncertainty and all selection attempts. |

These are logical dependencies, not four exhaustive projects that must finish in
sequence. Existing qualified forecasts can support independent rotation work; a
profitable simple model and complete macro coverage are not universal prerequisites.
Do not create another dashboard, storage framework or audit campaign unless a
specific defect blocks the chosen experiment. A two-hour session promises bounded
progress and a safe handoff, not completion of this whole roadmap.

### Candidate card required before new computation

Write `EXPERIMENT_CONTRACT.json` in that step's evidence directory with concrete values:

- `experiment_id`, design requirement IDs, hypothesis, nearest predecessor/artifact
  IDs, material difference and reuse/retrieval decision;
- input/source/config/environment hashes, target family, instruments, dates,
  origin cadence, horizons, point-in-time availability and target maturity;
- baseline and ablations, matched support plus native-coverage reporting;
- global chronological splits, purge/maturity rules, fit/update schedule, seeds,
  untouched confirmation route and known prior inspection of evaluation data;
- metrics and paired comparison/uncertainty method, support thresholds, economically
  material effect, acceptance, negative/inconclusive definitions, futility and stop rules;
- cost, risk, sizing and execution contract identities for economic comparisons,
  including the declared treatment of unsupported financing/fill assumptions;
- explicit wall-time/CPU/memory/disk/fit-count caps grounded in existing measurements,
  shared caches, dependencies, checkpoint/resume command and output identities.

Do not fill missing scientific decisions with arbitrary values to start a sweep.
Use reviewed existing settings where applicable and document the justification for
any material change. If the contract cannot be supported, record the exact blocker
and choose independent eligible work. Record all attempted, failed and abandoned
candidates. Do not reopen completed fits without a changed dependency or hypothesis.

### Rules that protect the result

- Existing rich-feature, fixed-interaction, noise-control, calibration and event-age
  comparisons are preserved development evidence. Read the catalog and original
  run records before proposing repetitions. A negative event-age result does not
  retire all macro information; residual calibration is not full stacking.
- Stacking, reliability weighting, learned ranking and continuation must consume
  chronological out-of-fold/prequential base predictions with original model and
  cutoff identities. A layer's label is usable only after the original prediction
  existed and the outcome matured. Test future labels, in-sample columns, learned
  transform/selection leakage and backdated model readiness (TST05/08/09/10).
- Keep direction, magnitude/movement, timing/barrier, ranking and calibration results
  distinct. Movement value can help management even without standalone directional
  profit. Preserve all 68-pair coverage and daily/multiday targets with explicit
  support blockers; never silently replace trading sessions with elapsed days.
- Rotation arms share forecasts, sizing, hard risk, costs and execution assumptions,
  with independent ledgers and cash. Compare from the same current liquidation
  wealth over a common horizon with explicit continuation values. Old entry costs
  are sunk; future costs enter once. Original expected return minus realized return
  is not a fresh remaining forecast. Hard risk overrides grace; changed pair, side,
  horizon or availability resets persistence. Learned continuation needs the same
  honest-layer gates. Cover TST25–31/53 as applicable.
- Report turnover, replacement/whipsaw, cooldown, risk exits and empty/no-opportunity
  cases. Changing spread, latency, financing or fill eligibility requires replaying
  the affected policy path, not subtracting fees from an unchanged ledger.
- Report matched and native coverage, raw rows versus distinct origins/events/blocks,
  and dependence-aware uncertainty. Once outcomes inform redesign, those dates are
  development data. No relabeling them as unseen. Negative/inconclusive findings are
  valid completion; engineering correctness and market evidence remain separate.

## 6. Package completion, publication and resume

Follow `CHECKPOINT_REVIEW.md`. Keep raw outputs, `WORK_LOG.jsonl`,
`PENDING_CHANGES.md`, baseline and exact commands in one local step evidence directory.
Publish a compact immutable review packet with actual diffs, source/config/input
identities, declared coverage, test results, unresolved findings and restore proof.
Keep these four fields separate: `engineering_ready`, `forecast_evidence_status`,
`policy_evidence_status`, `demo_authorization_status`.

Update the live queue with implementation and review status separately; name the
exact next item or partial resume. Link the same step/packet from the existing
`trad/FOREX_PROJECT_LOG.md` and `trad/FOREX_PENDING_IMPROVEMENTS.md`, preserving history.
Commit shared source/document changes and record the exact published revision and
checkpoint in `SHARED_GIT_REMOTE_LATEST.json` through the established reviewed flow.

Publication must preserve the latest documentation addendum, operational gate and
unchanged current-document pins, while a scientific source change receives a new
engineering manifest/source closure. Do not run an old one-off publisher unchanged:
older `publish_batch.py` scripts encode completed owner IDs and a historical
four-step authorization. Adapt and verify the concrete successor publication first.
An operational-only review pointer cannot stand in for a changed scientific manifest.
Never edit an old sealed package or regenerate its hashes in place.

At each checkpoint verify manifests, changed links, actual payloads and the new
current pointers. Mark the claim DONE or HANDOFF with exact resume. Partial work
stays partial. Resume the existing process/run ID; do not spawn a duplicate when a
tool yields. Report platform limits truthfully instead of claiming work continues
after the response ends.

## 7. Boundaries

GPT/advisor comparisons, paid calls, broker orders, service/account changes, D-drive
investigation, destructive cleanup and global environment changes remain deferred.
Use the user's selected model/reasoning setting; do not switch it automatically.
Terra follows bounded contracts and reports concrete exceptions. This handoff is not
a claim that Terra has already completed or been benchmarked on the next package.
