# Forecasting continuation during timed runs

User clarification, 2026-09-24: use the requested duration for useful forecasting
work. A blocked experiment must not park the whole session. This is the current
selection policy for NEXT and TIMED; it supersedes historical single-item stop
instructions. Preparing these instructions starts no research or timer.

## Select and continue

1. Read the actual UTC clock at timed invocation and preserve the original deadline
   in SESSION_STATE.json. Keep working until the checkpoint window while useful
   authorized work remains. A checkpoint is a saved result during a timed run.
2. Resume owned partial work if its dependencies are available. If it is blocked,
   preserve its exact files/run identity and record the blocker; proceed to a sibling.
3. Use REVIEW_QUEUE.json.forecasting_continuation.candidate_order. Inspect the
   candidate's own dependencies, retained artifacts and available training/assessment
   support. Missing confirmation inputs do not block development experiments.
   candidate_requires_support_check means permission to implement the bounded
   research package after checking support, not a claim that its numerical run is
   already approved or completed. Complete the supported package through results,
   tests, review and handoff; do not stop after writing a candidate card.
4. Before computation, record the exact input/model/target/version identities,
   chronological training and mature-label rules, comparisons and resource limits.
   Reuse inherited settings where justified; document any changed hypothesis.
   This records a reproducible experiment. It does not freeze the project, prohibit
   adaptive updates, require a new user approval, or end development on inspected
   data. Preserve previous predictions and label subsequent redesign as development.
5. A correctness, missing-input, ownership or resource issue blocks that experiment
   and affected descendants only. Record the evidence and concrete remedy in
   PENDING_CHANGES.md and SESSION_STATE.json.blocked_items. Continue the next
   eligible sibling without asking the user to restate the timed request.
6. At every checkpoint, maintain at least three useful independent candidate cards
   where the remaining design supports them. Replenish from design sections 9-16
   and 27.2/27.4/27.5 after checking existing artifacts. An exhausted named list is
   a reason to select the next supported design gap, not conclude all work is done.
7. Pending independent review alone does not stop the loop. Record same-task review
   honestly when necessary; concrete correctness findings still constrain consumers.

## Current forecasting alternatives

These are development candidates, not a promise of statistical support. Exact cards
and predecessor dependencies are in the queue and this documentation packet.

| Priority | Candidate | Material question | Existing work to reuse |
|---|---|---|---|
| 1 | currency_projection_residual_layer_comparison_v2 | Does a weight learned from earlier matured outcomes improve on direct, full projection and fixed half-residual forecasts? | Currency projection frames/diagnostics; chronological outcomes; convex/magnitude layer chronology and support checks |
| 2 | forecast_curve_shape_layer_comparison_v2 | Do contemporaneously available cross-horizon shape/disagreement features add to signed-only and magnitude controls? | Saved chronological frames; signed-only/magnitude calibrators; original model IDs and target maturity |
| 3 | rich_feature_family_incremental_comparison_v2 | Which still-uncompared retained information family adds value on matched support? | Populated 228-field inputs, completed three-group comparisons, original feature definitions and matched fitter |
| 4 | distinct_forecast_method_comparison_v2 | Does one justified recovered distinct model mechanism improve the same target/support beyond ridge/HGB? | Model reuse catalog and original experiments; corrected baseline populations/preprocessing |

The first two are separate experiments and need not wait for each other's results.
Feature and model alternatives depend on their own retained inputs, not on the
projection confirmation cohort. Existing equal-weight/convex blends, magnitude
conditioning, fixed projection, rich groups and fixed-product tests are completed;
do not repeat their exact experiments as new work.

## Blockers and stopping

No untouched cohort currently qualifies the protected projection confirmation.
Keep that step blocked for confirmation only. It is not a prerequisite for the
development candidates above, and lower forecast error on inspected data is not
independent confirmation. GPT/advisor comparisons, paid calls, broker/account/service
actions and D-drive investigation remain deferred.

Forecasting is the priority. Do operational maintenance only to resolve a concrete
defect necessary for the selected work or its required handoff. Missing live-bot
health, the other person's machine setup, publication polish or general cleanup must
not become substitute research projects or universal research blockers.

Before any early stop, record an ELIGIBILITY_REVIEW.json listing each remaining
candidate/design branch inspected, its exact blocking evidence, available independent
work and the reason none can proceed. Do not fabricate work or repeat completed
checks to fill time. Actual resource/platform limits, a user stop, or exhaustion of
all useful authorized work can still end a run early and must be reported honestly.

At a selection change, publish a compact queue/document addendum using one writer.
Preserve the blocked row and its history. exact_next_item names selected work;
active_step_id names its completed accepted engineering predecessor for existing
preflight semantics. Refresh associated document pins rather than bypassing them.
Do not rewrite sealed scientific evidence or regenerate recipe approval hashes.

## Queue quality and handoff

Keep scheduling status, implementation/review status and scientific outcome separate.
A finished negative or inconclusive experiment is completed research; do not silently
retry it until it looks positive. Add a successor only for a material new question or
changed dependency, linking the earlier attempt and preserving its result.

Each card records a concrete deliverable, controls, support checks, meaningful failure
tests, comparison metrics and a completion gate. It also records estimated runtime,
peak memory/disk and estimate provenance once measured. Unknown estimates are null,
never fabricated. Check time remaining before starting a long job; choose a smaller
useful sibling or a tested resumable job when the larger package will not fit.

Record blocked_at_utc, reason, affected_dependencies, evidence_refs, resume_action
and retry_when for a blocked item. Reconsider it only when that dependency changes;
do not repeatedly hash the same missing inputs or rerun a failed fit unchanged.
Track selected_at_utc, attempt identity, current phase and original process ID for
in-progress work. At each checkpoint record completed items, scientific outcomes,
skipped items/reasons, time remaining and the next selected candidate in SESSION_STATE.

Within equally eligible branches, prefer a material question that reuses verified
artifacts, changes one information source/mechanism at a time and can deliver an
interpretable result within the remaining time. Finish runnable partial work first.
Reorder with a recorded scientific reason; no outcome-driven cherry-picking, silent
dependency removal, duplicate fits or documentation-only filler.

## Timed run correction, 2026-09-24

Before a timed launch or final response follow [FOREX_TIMED_RUN_CONTROL.md](FOREX_TIMED_RUN_CONTROL.md). The prior four-hour session is stopped by the user and was not fulfilled; do not resume it. The residual-layer acceptance is superseded by changes_requested in the current review. A later authorized run resumes that partial item and its recorded fixes first. No research is started by this control repair.
