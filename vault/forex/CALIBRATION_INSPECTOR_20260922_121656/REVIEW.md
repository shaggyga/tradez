# Exact original-record calibration inspection

The pinned inspector returns exact calibration records, base forecasts and authenticated snapshot provenance for registered instrument/origin/group/target/procedure/method/mode queries. It withholds numerical values and snapshots before the base forecast's modeled availability. Outcomes remain absent unless explicitly requested with a separate as-of at or after their own maturity. Missing labels stay null; unavailable base forecasts and unsupported calibration are never replaced.

1224 deterministic smoke cases span all68 instruments,3 fixed scopes,2 calibration modes and3 visibility states. Compact receipts bind full returned-record digests without duplicating long training lists. The original records remain available through the inspect action. The reader returns detached JSON so caller mutation cannot alter its authenticated cache. Actual publication qualification stays false; no production calibrator availability is invented.

21 local and21 relocated tests pass: exact query/schema, visibility boundaries, separate outcome maturity, exact original identities, insufficient five-day support, pre-frozen phase, unavailable base coverage, null outcomes, returned-data mutation, snapshot and input tampering, no fitting across all1224 cases, idempotence, actual operator consumer, source/recipe guards, false completion, one writer, two process-death/resume boundaries and stdlib-only preflight.8 inspection payloads reconstruct exactly after replaying116 calibration payloads from the pinned original-record capsule. No base models are fitted; upstream full suites are not claimed rerun.

Engineering_ready=false; these are inspected modeled-clock development records, not independent confirmation or live authorization. Independent review remains unperformed. Next native_remaining_target_readiness_rebuild_v2 addresses aggregate readiness in the isolated existing remaining-target workload. Full seven-target serving and actual issuance remain open; deferred scopes unchanged.

Main 6.516seconds/175861760bytes sampled RSS; recovery/tests 150.063seconds/418930688bytes.


Checkpoint: checkpoint/forex_calibration_inspector.zip, SHA256 a8114f7f28cf34ec2e6da42b86c2be4a1172a2c90a1c1c7f9ee00857d3863638. Exact next: review_calibration_inspector_checkpoint; then native_remaining_target_readiness_rebuild_v2. Prior checkpoints remain sealed in their original packages; approvals reference them without duplicating archives.
