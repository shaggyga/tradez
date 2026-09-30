# Policy integration checkpoint

Status: ready_for_review. Independent review not performed.

Completed the bounded policy_thesis_continuation_and_operator_recipe_v2 package. Reused the reviewed Decimal quote/conversion/sizing and legacy policy selectors, existing EventLedger/optimized close path, independent arithmetic audit, RunPublisher and checkpoint machinery. Added a policy/controller consumer, not another accounting engine.

Six isolated arms: fixed hold, recovered rotation, naive top rank, continuation, persistence/grace hysteresis, cash. Original and current theses, snapshot identities, episode/replacement links and observed excursions are retained. HOLD/EXIT/REPLACE values use common executable liquidation wealth and common target, with explicit future financing/cost assumptions. A replacement cannot reuse capacity until actual exit fills; partial exits keep remaining exposure. Every fill needs explicit synthetic execution evidence after latency.

Verification: 293 integrated tests passed, zero failures/errors/skips. Twelve actual process-death/resume cases match all payload bytes. Reference/optimized decisions, event ledgers and states agree. Both frozen operator recipes passed in the relocated checkpoint, including the relocated test suite. Initial resume ordering failures were repaired by canonical arm/lot ordering; failed outputs are preserved.

Fixture: two instruments, six arms, two days, eleven frames; no historical or fitted prediction claim. Historical execution/financing provenance and fitted-consumer causality still gate a complete all-68 campaign. All 68 instruments and original 37 pending IDs remain in the full design queue.

Checkpoint: [ZIP](checkpoint/forex_policy_integration.zip), SHA-256 `77d14fe10e7c2878873d21dbd39bb00d9307239b580c33b3a5ef53d668d5455b`. Restored at `C:\Users\zmoor\Documents\forex\checkpoint_restores\POLICY_INTEGRATION_20260922_004348`. See POLICY_OPERATOR_APPROVAL.json for the exact frozen status command.

Next: `review_policy_integration_checkpoint`; then `causal_fitted_consumer_v2` (B/C) before campaign E. Original design is in specification/; FULL_DESIGN_QUEUE.md preserves the complete queue and historical mapping. Full engineering_ready remains false; forecast evidence unchanged; policy evidence synthetic only; demo authorization not granted. GPT/advisor comparisons remain deferred.

Restore: use the packaged accounting_checkpoint_v2.py restore --package <ZIP> --destination <new-empty-directory> --sha256 77d14fe10e7c2878873d21dbd39bb00d9307239b580c33b3a5ef53d668d5455b --run-tests with the dependency-locked interpreter. Baseline_source and before_documents preserve exact rollback/reference bytes. Previous sealed packages, trad executable source, services and D-drive contents remain unchanged.
