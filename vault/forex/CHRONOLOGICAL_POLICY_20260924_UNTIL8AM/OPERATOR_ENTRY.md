# Reuse before reproduction

Source revision: `705be858b7870a14d4d22145864694c52cc18748`. Recipe SHA256: `07514564b23e7d7fae398c088dca683feae637d671b1f5121aa194fe1669ab0a`.
Checkpoint SHA256: `bb2e12baff009c3db331ede60403163f223dd01085b36dd6e2564dd9d1735b5a`.

Read REVIEW.json and report/RESULT_SUMMARY.json for completed evidence. The current
queue is the Vault REVIEW_QUEUE.json; do not infer unfinished work from historical
preparation notes copied into evidence. ACTIVE_RESTORE_PROCESS.json records the
completed process, and portable_restore/RESTORE_RECEIPT.json records its result.
Independent review is pending/nonblocking; same-task acceptance is not independent.

Ordinary continuation reuses completed local outputs and their original manifests.
Do not repeat the112-path replay just to read its results. On a new machine needing
the raw policy outputs, use this exact Git revision or this packet's source_snapshot
with the pinned Python environment; a future checkout is not equivalent frozen code.
Run chronological_policy_checkpoint_v2.py restore against the capsule and hash above,
with a new empty destination and --run-tests. Restore reconstructs2912payloads and
the report; expect roughly43minutes on the originating machine and about18GiB logical
outputs plus inputs, with at least8GiB free required throughout the replay.

The compact capsule is not a duplicate store of those raw outputs. Selected original
input subsets retain their original complete manifest bindings. Same-machine replay
and fresh Git clone have separate receipts; neither proves G's machine or cloud sync.

Next candidate: chronological_policy_attribution_v2. Read PATH_FORWARD.md,
evidence/NEXT_CANDIDATE.json and evidence/NEXT_IMPLEMENTATION_NOTES.md. The last is
preparation, not a claim that the successor is frozen, implemented or verified.
