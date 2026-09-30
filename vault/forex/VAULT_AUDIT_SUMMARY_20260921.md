# Vault audit summary — September 21, 2026

The local Vault audit passed with declared historical limits. Its initial pass hashed **3,346 files / 3,119,451,150 bytes (about 3.12 GB)**. The [final publication receipt](maintenance/vault_refresh_20260921/VAULT_PUBLICATION_RECEIPT.json) and [inventory](VAULT_FILE_INVENTORY.json) also cover this subsequently added summary and updated README. Current navigation has **zero unresolved links**. Cloud synchronization and destination availability remain unverified.

The [recovery checkpoint](RECOVERY_CHECKPOINT_20260921/README.md) preserves 4,881 current source members, nine source supplements, and 136 input files across two separate 68-pair vintages. All 171 checkpoint files matched their manifest hashes and sizes. Local restoration verified 5,193 restored files; 13 restore-tool safety tests and 11 synthetic contracts passed. These checks do not establish market performance or complete environment/runtime recreation. [Current source preservation](maintenance/vault_refresh_20260921/CURRENT_SOURCE_PRESERVATION.json) records unchanged original eligible source and Git status; live workers and account configuration were untouched.

Four retained records have format limitations. Paths below are relative to this Forex project directory:

- `OPERATIONAL_REPAIR_20260916/source/docs/validation/rolling_operations_20260916/FORWARD_CACHE_REAL_PREFLIGHT_V2.json`: malformed JSON at line 286, column 5; a property name requires double quotes.
- `RECOVERY_CHECKPOINT_20260921/audit_20260921/deep_audit_02/MODEL_LINEAGE_EVIDENCE_INDEX.json`: four nonstandard non-finite JSON constants.
- `RECOVERY_CHECKPOINT_20260921/evidence/h1_20260713_v4/MODEL_REPLICATION_SPEC.json`: three nonstandard non-finite JSON constants.
- `RECOVERY_CHECKPOINT_20260921/evidence/moments_20260722/runs/run_20260722_232506/FROZEN_SELECTION_BEFORE_FINAL.json`: one nonstandard non-finite JSON constant.

Strict JSON readers may reject these records. Their original bytes remain preserved. The [readability report](VAULT_READABILITY_REPORT.json) also records **2,803 unresolved historical link occurrences across 180 documents**. Those links are separate from current navigation. Historical files and the sealed checkpoint were not rewritten to conceal these limitations.

Privacy review intentionally withheld **69 source files and two primary evidence receipts**. Their identities and dependency limitations are recorded in the checkpoint; older archives are not certified private-data-free.

[Cleanup results](maintenance/vault_refresh_20260921/CLEANUP_RESULTS.json) record **308,509,222 bytes of logical savings after receipts**. Two task-created restore folders retain **3,136,832,090 bytes**: `C:\ForexRestore_20260921` and `C:\Users\zmoor\Documents\forex\maintenance_20260921\restore_validation`. Automatic approval review blocked their recursive cleanup; deletion was not executed or retried. See the [retention record](maintenance/vault_refresh_20260921/RESTORE_SCRATCH_CLEANUP.json). These retained copies and the new checkpoint mean **no net C-drive space gain is claimed**.

D-drive work remains deferred after disk errors. The next research task remains the isolated legacy pip repair and bounded all-68 comparison described in the checkpoint handoff; it was not started by this maintenance.
