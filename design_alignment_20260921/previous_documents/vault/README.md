# Forex — start here

[September 21 Vault audit summary and known limits](VAULT_AUDIT_SUMMARY_20260921.md)

Updated September 21, 2026. The portable package passed local extraction/hash readback and eleven synthetic contracts. [Current recovery pointer](RECOVERY_CHECKPOINT_LATEST.json) binds the exact manifest and [restore receipt](maintenance/vault_refresh_20260921/RESTORE_VERIFICATION.json). Cloud synchronization is not verified.

The current project on the original machine is **C:\Users\zmoor\Documents\forex\trad**. This Vault preserves explanations, source snapshots, research evidence and recovery records. It does not describe current account state and is not the running application.

Start with the new [recovery checkpoint](RECOVERY_CHECKPOINT_20260921/README.md) and [new-machine handoff](RECOVERY_CHECKPOINT_20260921/NEW_MACHINE_HANDOFF.md). The checkpoint is scoped to **offline research**: reviewed source, audited long and native 68-pair input collections, audit/test receipts and a dependency snapshot. Its manifest defines exactly what is included. It is not a full runtime/news database/model archive backup or proven environment recreation.

The latest audit established substantial reusable history and recovered primary model evidence. It also found a localized native spread-field unit defect and an exposed legacy alignment/rotation consumer. The immediate next engineering task is to repair that consumer in an isolated copy, then build a bounded all-68 comparison through at least 24 hours with correct chronology and accounting. The checkpoint does not start that experiment.

The isolated repair is now [validated and ready for the all-68 integration gate](LEGACY_PIP_REPAIR_20260921/README.md). It remains deliberately unmerged while that offline gate is prepared.

The [all-68 offline gate](ALL68_OFFLINE_GATE_20260921/README.md) has now bound the source snapshot, history archive directory and pip map. Its next implementation contract is kept with that record.

The [authority and reuse map](AUTHORITY_AND_REUSE_MAP_20260921.md) maps the new engineering design to the actual recovered project components and records what is safe to reuse, repair, or defer.

Read in this order:

1. Recovery checkpoint manifest and destination verification instructions.
2. New-machine handoff and September 21 audit conclusions.
3. Native spread source trace and the next offline slice contract.
4. [Research index](RESEARCH_INDEX.md), [project history](PROJECT_HISTORY.md) and [historical pending register](PENDING_IMPROVEMENTS_CURRENT.md) for predecessor work.
5. [Recreation scope](RECREATION.md) for exclusions and historical source-layout conventions.

Keep the long and native history vintages separate until explicit merge precedence and derived corrections are defined. Preserve all 68 instruments, sparse coverage and unknown availability. Historical fitting success, operational activity and profitable predictive evidence are different statuses.

D: remains excluded because of fresh bad-block evidence. No recovery package authorizes a supervisor restart, account change, demo/live order, paid experiment or real-money deployment. Local hashes establish local bytes; they do not certify OneDrive cloud synchronization.

## Earlier checkpoints retain their dates

- [September 14 audit](AUDIT_20260914/README.md) and [source recovery guide](RECREATION.md).
- September 15 rolling data, models, specialist and replication supplements.
- September 16 operational, all-68 stability, derived-feed and V18 supervisor supplements.

These are historical evidence with their own manifests, limitations and observation times. The September 21 checkpoint should link them rather than overwrite their records or imply that old running/stopped claims remain current. The source-only pointer `source/WORKTREE_SOURCE_LATEST.json` now names the same privacy-reviewed source snapshot included in the recovery package. The older committed-source baseline and root August20 CHECKPOINT_LATEST.json remain historical. Explicit private-source exclusions are in the recovery package; this is not a full legacy runtime restore.
