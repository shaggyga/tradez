REVIEW THE LATEST FOREX CHECKPOINT.

Read CHECKPOINT_REVIEW.md, REVIEW_QUEUE.json and CHECKPOINT_REVIEW_LATEST.json in
C:\Users\zmoor\OneDrive\thevault\projects\forex. Resolve equivalent Vault paths after restore.
Select the newest ready_for_review step; inspect unresolved earlier findings it depends on.
Read its exact sealed review packet, source hashes/diff, design acceptance and test receipts.
Use DESIGN_ALIGNMENT_LATEST.json for the governing design and queue.

Review correctness, causal timing, accounting, isolation, recovery, operator behavior and whether
the evidence supports the claimed scope. Inspect affected consumers, not only new helpers.
Confirm that the reviewed source matches the packet. Run targeted checks when needed to resolve
a question; do not rerun everything without a reason. Preserve findings even when tests passed.

Write a dated review result with accepted_within_scope, changes_requested or blocked. Include
reproducible findings, severity, file/line, impact, required fix and evidence. Update REVIEW_QUEUE.json
and retain its previous version; keep sealed packets unchanged. If fixes are required, state the
exact next corrective step. Do not silently implement a different work package during this review.

End with the verdict, findings, checks actually run, review-result location and next action.
GPT/advisor comparisons remain deferred. No broker/service/account actions are authorized.
