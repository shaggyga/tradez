# WP6 raw receipt versus canonical detail storage audit

The original collector records raw observations at line17080, selects the active
projection at17092 and binds it at17161. Its original ledger select_observation
already rejects an enriched-to-non-enriched downgrade. The frozen original guard
rejects all five such replacement pairs, including BOTH rate-action-loss pairs.
Five original-function challenge cases pass. The collector itself was not run.

The demonstrated mismatch is in our offline latest-raw-version selector: raw
receipt retention includes later listings that the canonical projection protects
against. This is not evidence that the live canonical article lost its detail.
Historical canonical projections and complete intermediate ingestions were not
reconstructed. The next offline selector repair must reuse the existing detail
protection intent while retaining all raw receipts, visible-time gating and
same-clock ambiguity. It must not invent correction/retraction semantics.

Six local/six relocated tests pass, five outputs reproduce exactly, including
source order, original guard behavior, exact content pins and crash recovery.
One initial test expected the call one line too late; its failed output is kept
and the assertion now matches the inspected source line. No production code was
changed for that correction. No live collector/DB action, model or outcome input.
Independent review remains unperformed; full engineering is unfinished.


Checkpoint: checkpoint/forex_macro_storage_audit.zip, SHA256 a9f73c17d2ed06afe71c499eac234788cd16eb56b6e7cd750b7e9a3518f77729. Exact next: review_macro_storage_audit_checkpoint_v2; then macro_detail_representation_selector_repair_v2. Prior checkpoints remain sealed in their original packages; approvals reference them without duplicating archives.
