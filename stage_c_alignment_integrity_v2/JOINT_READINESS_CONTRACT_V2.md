# Joint fit readiness and original-artifact fallback projection

R03/R10/R11/R13, WP7/WP10. Address RF-CAPACITY-1 on the existing inspected development campaign. Preserve every previous model, forecast, score and checkpoint. This is a separately identified conservative offline scheduling projection, not a live-service timing certificate or a new model search.

## Predeclared reservations

Reuse all56 joint ridge/HGB fit pairs:28 at each original cutoff (legacy26, compact38/cost2, compact50/cost2, full228/cost2; seven targets in original ascending order). Reserve30 seconds per pair, preserving the original mature training cutoff and measured per-fit bound. Order by cutoff, then the stated group order, then target. No quality score or measured realized fit time selects the order or readiness.

Reserve one worker. At each of the20 original decision origins, reserve56 two-second prediction slots: group, target, frozen/adaptive procedure. Each slot covers both ridge/HGB across all68 pairs, for112 seconds total. Reserve even an unavailable slot, so missingness cannot accelerate other jobs. Fit reservations may not overlap any prediction reservation; defer a fit to the end of an intersecting prediction window rather than preempting an in-progress fit. Original origins are60 seconds after the corresponding cutoff: only the first two fits can finish before the first decision. The full28-fit reservation ends952 seconds after that cutoff under this fixed grid.

This conditional model assumes prepared feature packets exist at their recorded origins. Raw ingestion/feature construction, OS interference, service/startup and other workloads are outside the reservation envelope. Validate original fit resource receipts and measure actual reused-artifact prediction batches against the two-second reservation; never replace deterministic readiness with future measured wall times. A resource violation blocks acceptance. Full concurrent/live deployment remains unqualified.

## Selection, fallback and forecast lineage

At the original origin (not a later prediction-slot start), frozen selects only the initial fit if jointly ready. Adaptive selects the latest fit jointly ready at that origin; if the scheduled update is pending, it uses the prior ready artifact. If none is ready, retain all68 explicit unavailable rows. Feature missingness remains separately visible. Do not use a fit that finishes during later prediction slots, because model selection is frozen at the original information cutoff.

Reuse corresponding original frozen/adaptive forecast values. When adaptive falls back to the initial artifact, reuse that origin's frozen source record, not the unavailable updated record. Assert artifact identity and independently reproduce actual selected predictions from retained weights without fitting. Output a new forecast ID binding original record, scheduling identity, requested procedure, joint readiness and later availability. The original model ID/training view/target remain unchanged; joint serving readiness is distinct from the artifact's original per-fit readiness assumption. Keep original forecast bytes and source hashes as lineage.

Publication occurs at each reserved prediction-slot end. Validate the standard forecast.v2 contract. The forecast ID binds the fixed reservation-policy hash and selected original record, not the full inventory hash of future fitted models; changes to future fitted weights cannot change an earlier selected forecast. Full dependency/schedule identities remain in the run manifest. No outcome field enters the forecast packet. Outcome reconciliation is separate and uses the original labels at their original evaluation-as-of time, with actual support hashes and explicit comparison with zero and the original procedure on that same support. Report all groups, no winner or confirmation.

## Consumer limits

A consumer cannot see a projected forecast before its new available time. Report conditioning age explicitly. The existing native policy contract requires at most two seconds of fresh conditioning: most sequential slots will exceed that bound. Do not backdate availability, relabel old features as freshly conditioned, or waive that policy gate. This projection can establish unavailable/stale coverage; it does not establish a deployable all-target native curve or executable policy result. A later scheduling/batching/fresh-input design needs its own qualification.

## Operation, recovery and evidence

Frozen source/configuration and original family/baseline/rich dependencies precede numerical imports. Reuse CampaignReader, existing feature views, retained model preprocessing/weights, forecast.v2 validation and RunPublisher. Keep deterministic payload identities separate from authenticated bounded timing receipts. No model is fitted; repeated checkpoint prediction/reconstruction is not another research candidate.

Verify schedule arithmetic/non-overlap, readiness boundaries and missing/prior-artifact fallback, all68 coverage, exact source-record selection, actual retained-weight prediction parity, immutable target/training clocks, later publication and two-second consumer refusals, outcome support/arithmetic, source/model/timing drift, real interruption/resume and isolated restoration. Publish the compact Vault review packet and current queue only after meaningful actual-consumer verification. Same-host restoration is distinct from another machine and independent review remains unperformed.

Engineering_ready=false; forecast evidence=scheduled inspected-development projection; policy evidence=not evaluated, freshness constraints explicit; demo authorization=not granted. Shared-currency/overlap, short inspected window, later-research feature selection, incomplete historical attempt inventory and protected confirmation remain open. GPT/advisor comparisons, paid calls, broker/service/account and D-drive work remain deferred.
