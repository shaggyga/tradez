# Accounting and lower-model operating checkpoint

Read [current status](RUN_STATUS.json), [exact next work](PATH_FORWARD.md),
[operator contract](OPERATOR_CONTRACT_V2.md), [operating design](LOWER_MODEL_OPERATIONS_DESIGN.md)
and [requirement/test coverage](DESIGN_ACCEPTANCE_ADDENDUM.md).
The full design remains authoritative; this package completes a bounded accounting foundation.

Verified: 245 integrated tests, 7 full-path synthetic stress runs, per-event
reference/optimized parity, 40 independent event/arm arithmetic checks per baseline,
real crash/resume and exact 25-payload relocated baseline replay.

Checkpoint: [ZIP](checkpoint/forex_accounting_20260921_213557.zip), SHA-256 `b457420c2b858a41668d26e91735f0829f85a4ff52183f13530411d7ef06ae3f`.
The [publication receipt](checkpoint/forex_accounting_20260921_213557.zip.receipt.json) and
[restore receipt](evidence/RESTORE_RECEIPT.json) record tested bytes.

Another machine: obtain the ZIP and its trusted external SHA; provision its exact
DEPENDENCIES.json/requirements.lock in an isolated Python environment. Extract the
verified ZIP into a new bootstrap folder, then run the packaged tool:

```text
python -I -B <bootstrap>/source/accounting_checkpoint_v2.py restore --package <zip> --sha256 b457420c2b858a41668d26e91735f0829f85a4ff52183f13530411d7ef06ae3f --destination <new-empty-folder> --run-tests
```

Require RESTORE_RECEIPT.json with VERIFIED and exact replay parity. The source and
two pinned predecessor files are included. The restored operator contract supplies
the approved recipe workflow. No original username/path is needed by the runtime.

Excluded: bulk historical prices, credentials, broker accounts, running services,
full fitted models/campaign, historical execution certification and cloud-sync proof.
The broader [previous recovery scopes](../RECOVERY_CHECKPOINT_LATEST.json) remain separate.
The previous [neutral checkpoint](../INTEGRITY_REPAIR_20260921_210502/README.md) remains sealed.
Source snapshots also preserve staging modules outside this runnable allowlist;
those may require broader project dependencies and are not certified by this ZIP.

Development history is retained. The early accounting-events-development run failed
while producing its report; the completed final runs in evidence supersede it.
The earlier 225-test receipt predates final fastpath/operator changes; only the
final_integrated_gate receipt describes the sealed runtime.

Next: Step D / WP2-WP8: integrate the recovered policy manager with the verified event ledger; preserve original/current thesis and compare HOLD/EXIT/REPLACE from common executable wealth and continuation horizon, with isolated arms, event-level checks and a frozen operator recipe.
