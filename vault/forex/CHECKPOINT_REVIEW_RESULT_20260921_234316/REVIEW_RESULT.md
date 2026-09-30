# Batch05 checkpoint review — changes requested

Reviewed 2026-09-21T23:49:41.779243+00:00. This is a **follow-up self-review**, not an independent review: the reviewer
previously implemented the changes. The exact sealed source and live source hashes match.
No engine code or sealed checkpoint was changed; the findings below are reproduced, not fixed.

## P1 B05-R2 — Pin package initialization before loading the reviewed predecessor

Source: [reference_accounting_adapter_v2.py](../ACCOUNTING_EVENTS_20260921_213557/source_snapshot/reference_accounting_adapter_v2.py)
lines 27-34; related forex_operator_v2.py:15.

Trigger: An unlisted src/__init__.py is added under the selected trad root while both pinned predecessor files retain their approved hashes.

Observed: The operator status command exits 0 and reports ready; Python executes the unapproved initializer first. A harmless marker file proves execution in an isolated copy.

Impact: The claimed pre-import source-change guard omits executable dependencies. A normal package-initialization change can alter behavior or trigger side effects without changing the recipe identity.

Required fix: Bind the complete local import closure, including initializer presence/absence, or load the reviewed modules through an isolated import mechanism that cannot execute unapproved parent packages. Apply the same contract to the operator and portable package before imports.

Acceptance: Changed/added parent initializers are refused before any marker side effect; unchanged approved live and restored layouts still work; original core source hashes remain enforced.

## P1 B05-R1 — Preserve activation while cancellation is awaiting acknowledgement

Source: [accounting_events_v2.py](../ACCOUNTING_EVENTS_20260921_213557/source_snapshot/accounting_events_v2.py)
lines 361-364; related accounting_events_v2.py:382.

Trigger: Intent at 1000, cancel request at 1030, activation at 1060 and an explicitly observed fill at 1061, before any cancel acknowledgement.

Observed: cancel_request sets status=cancel_pending. activate requires pending, so activation is rejected; the subsequent fill is rejected as order_not_active. The identical tape without the cancel request fills. Both reference and optimized paths reproduce the error.

Impact: An unconfirmed cancellation effectively suppresses execution and exposure. Reference/optimized parity cannot detect this because both share lifecycle code.

Required fix: Track cancellation request independently from activation/trigger eligibility, or explicitly allow activation of cancel-pending orders while preserving the outstanding cancellation. Only confirmed cancellation/expiry should remove eligible pending quantities under this asynchronous contract.

Acceptance: Cover cancellation requested before activation with ack before and after activation, fills before ack, resting triggers, partial fills, exact reservations, and interrupted recovery in both paths.

## P2 B05-R3 — Validate the actual packaged operator approval during export and restore

Source: [accounting_checkpoint_v2.py](../ACCOUNTING_EVENTS_20260921_213557/source_snapshot/accounting_checkpoint_v2.py)
lines 51-65; related test_forex_operator_v2.py:16.

Trigger: A harmless comment changes a copied fixture source while its old OPERATOR_RECIPE.json is retained, then the copied source is exported and restored with run_tests=True.

Observed: Export reports COMPLETE and restore reports VERIFIED with all included tests passing, but the restored operator using the packaged recipe exits 2 with source_dependency_environment_or_recipe_drift.

Impact: The acceptance tests mint fresh test recipes and never validate the frozen one shipped to the next operator. A future engineering checkpoint can be declared verified while its advertised operator is unusable.

Required fix: Validate the frozen packaged approval against the exact packaged source/dependencies before sealing, and execute its operator preflight/run/verify in relocated acceptance. Do not silently regenerate an approval to make the check pass.

Acceptance: A stale approval causes export/restore acceptance to fail explicitly; a newly reviewed coherent approval runs successfully after relocation. Keep a regression showing fresh test-created recipes cannot mask stale shipped approval.

## Evidence and scope

[Reproduction results](evidence/RESULTS.json), [exact reproduction script](evidence/reproduce_review_findings.py)
and [baseline hashes](evidence/BASELINE.json) are preserved. The script ran locally with the isolated
Python interpreter from stage_c_alignment_integrity_v2/evidence/review_batch05_20260921_234316.
It writes only into that new evidence folder. To repeat, copy it into a fresh evidence/<review-id>
folder under the equivalent stage, resolving its explicit Vault root for another machine.
Do not execute it against the sealed Vault package directory or overwrite its original results.

352 sealed files and 38 source snapshot files verified. All three new failure cases reproduced.
The deliberately stale-approval copied checkpoint passed its included restore tests before its
actual packaged operator refused. The historical 245-test and five-check receipts were inspected;
the full original suite was not rerun without a changed implementation.

This review does not invalidate the original fixture's recorded arithmetic/restore results.
It rejects broader acceptance until the missing cases are fixed. Historical execution provenance,
full policy integration and campaign-scale optimization remain the already documented limitations.
No separate independent review, broker change, D scan or GPT/advisor comparison occurred.

## Exact next step

accounting_review_repairs_v2: fix B05-R2 import-closure binding, B05-R1 pre-activation cancellation race and B05-R3 packaged-approval validation; add regressions, reseal the approved recipe/checkpoint and request re-review before policy integration.

The pending review/repair queue takes precedence over the historical package's next-policy item.
That sealed PATH_FORWARD/RUN_STATUS remains a truthful prior checkpoint; do not rewrite it.
Preserve each finding, record its fix and retest evidence in a successor, then re-review.
