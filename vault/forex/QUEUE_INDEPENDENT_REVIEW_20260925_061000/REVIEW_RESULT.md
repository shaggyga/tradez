# Queue review: changes requested

Reviewed source: `3113dfa28ad2049dc6194c4a251d5d8ec714d319`. Independent queue review by `/root/queue_independent_review`; root separately inspected publication/source/coverage. This is not full scientific acceptance.

## QREV-01 P1 — Queue exhaustion is unsupported

The stop receipt asserts all paths reviewed without the required branch-by-branch ELIGIBILITY_REVIEW. The named list is not the whole design. Pending review alone is nonblocking.

Evidence: FOREX_FORECASTING_CONTINUATION.md:33-35,70; evidence/timed_residual_r4_20260925_025955/FINISH_EVIDENCE.json:2-6

Required correction: Replenish supported design candidates after an exact reuse/support census; preserve the actual early-stop history.

## QREV-02 P1 — Residual R4 restore/replay completion is not demonstrated

Receipt records focused tests in the current tree, result counts/hash and manifest readback. It provides no restored execution path/command or reproducible relocated replay receipt. These checks do not close the original portable restore gate.

Evidence: CURRENCY_PROJECTION_RESIDUAL_R4_CLOSURE_20260925_031000/RESTORE_REPLAY.json; WORK_LOG.jsonl; original R4 finding

Required correction: Finish exact portable source/input recipe and isolated restore/replay verification; reuse preserved bases and label any necessary replay as replication. Preserve existing residual output.

## QREV-03 P1 — Curve all-68 coverage gate is incomplete

Saved result has 24 origins and 2 bases: 3264 expected all-68 slots, but coverage contains only 2156 unique slots. Missing cross-horizon panels are dropped; 182784 input coverage rows survive only as a count. 1108 absent slots lack explicit reason records. Candidate requires matched/native all-68 coverage.

Evidence: stage_c_alignment_integrity_v2/curve_chronological_operator_v2.py run and _load_frame_predictions; curve_shape_layer_v2.py join_curve; CURVE_CHRONOLOGICAL_COMPARISON_20260925_032500/CURVE_CHRONOLOGICAL_RESULT.json

Required correction: Retain full expected population with missing/unsupported/maturity reasons and matched/native denominators. Add missing-panel test and complete reproducible handoff; preserve negative result.

## QREV-04 P1 — Rich candidate incorrectly closed by predecessor reuse

Card asks for a still-untested family comparison. Prior compact38/50/full228 results alone do not prove all family add/remove hypotheses are covered or unsupported.

Evidence: FORECASTING_CONTINUATION_20260924_191848/cards/rich_feature_family_incremental_comparison_v2.json:14,24,45; RICH_FEATURE_REUSE_CURRENT_20260925_034000/REUSE_SOURCE_REVIEW.md:3-8

Required correction: Reopen family mapping/support inventory; map each comparison to original fingerprints or exact blockers and select a material untested supported hypothesis.

## QREV-05 P2 — Macro/text successor and broader model branches remain unassessed

Existing event/text audit discovery does not establish completion or unavailable support for the selected incremental text-state/change/reaction question. Missing a third recovered model blocks that specific card, not all new bounded method work.

Evidence: MATERIAL_INPUT_COHORT_CONTRACT_20260925_032500/MATERIAL_INPUT_COHORT_CONTRACT_V2.json; MACRO_EVENT_TEXT_SUPPORT_INVENTORY_20260925_033000/REVIEW.json; design sections 12.2,12.4,27.2,27.4

Required correction: Assess exact support for the material contract and design alternatives; freeze a bounded successor before any new computation.

## QREV-06 P1 — Current handoff fails operational preflight

Fresh stdlib preflight exited 1, blocked vault_packages/current_documents/engineering_source/revision/coordination/operational_gate. Missing package/review_manifest/board/queue keys and stale source/revision prevent normal startup. Git receipt still points to f679ae17, actual HEAD 3113dfa2. Finished session claim still IN_PROGRESS.

Evidence: tools/forex_preflight.py invocation during review; SHARED_GIT_REMOTE_LATEST.json; CHAT_COORDINATION_BOARD.md

Required correction: Repair current pointer/schema/document/source identities and reconcile owned finished claim without overwriting historical seals or touching EURUSD capture. Verify preflight at reviewed source.

## High-level recap

Baseline ridge/HGB, retained rich models, causal inputs, layer diagnostics and policy backtesting infrastructure exist. Most latest closures were reuse/readback of prior work, not new scientific experiments. Curve development produced 946 learned rows and 54 paired scores with no overall MAE improvement over any of three controls; preserve that result. Residual and curve still need acceptance-gate repairs. Untouched confirmation data remains a separate blocker; it does not block development. No proven improved forecast or trading readiness follows from queue completion.

Checks actually run: 29 packet members hashed with zero mismatches; current curve operator matches sealed source; saved coverage population counted; read-only stdlib preflight failed six checks. No fits, services, data capture, scientific reruns or new unit tests.

Exact next: Repair handoff/preflight identities required for residual R4 restore, then finish R4 portable restore/replay; repair curve coverage, reassess rich/material support and replenish design candidates.
