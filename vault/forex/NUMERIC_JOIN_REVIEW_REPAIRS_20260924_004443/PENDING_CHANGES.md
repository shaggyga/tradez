# Current repairs

- PUB-01: Missing current-document manifests and stale Git receipt. Preserve prior packet, publish successor with verified inventory and exact logs.
- PUB-02: Duplicate active scientific step records from rerunning publisher. Preserve before queue; consolidate as one step with packet history.
- JOIN-REVIEW: Independent reviewer is testing cutoff mapping, content identity, existing UI, and reset/empty behavior. Resolve before dependent research.

Resolution: NJR-01 through NJR-04 repaired and independently accepted at exact hashes
in independent_join_review/RESOLUTION_REVIEW.json. 36 local regressions, 16 independent
invariants, 3 actual Chrome consumer groups, and fresh replay of all five payloads
passed; portable unit tests: 16 passed, 3 retained-evidence cases skipped.
PUB-01/02 remain active until the immutable successor and shared Git receipt pass readback.

Baseline is clean Git 4daa6c6, all 375 source bytes matched prior packet. Original session deadline 2026-09-24T01:44:43Z.
