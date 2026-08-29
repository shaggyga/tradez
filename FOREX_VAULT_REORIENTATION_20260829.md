# Forex vault reorientation

Date: 2026-08-29

## Decision

The canonical live workspace remains
`C:\Users\zmoor\Documents\forex\trad`. The vault is a recovery and shared-
progress surface; it is not a second live runtime and must not be read as live
account truth.

The older mixed checkpoints under the Forex vault are preserved as legacy
recovery evidence. They are not deleted, rewritten, or silently treated as the
current source baseline.

## New source baseline

`forex_git_source_vault_sync.py` publishes an exact, source-only archive from a
clean local Git commit to:

`C:\Users\zmoor\OneDrive\thevault\projects\forex\source`

The profile contains tracked source, configuration, tests, and documentation.
It excludes runtime databases, WAL/SHM files, logs, model artifacts, caches,
credentials, private keys, and the account registry. A credential/privacy audit
runs against the exact resolved commit before publication. The ZIP member inventory,
CRC, byte size, and SHA-256 digest are verified before the current pointer is
replaced.

The publisher rejects tracked symlinks and any force-added runtime, database,
log, model, archive, credential, or key artifact even if `.gitignore` was
bypassed. Current records are read from the audited commit object—not the
mutable worktree. A single-writer lock serializes publication; immutable
manifests are write-once and identity-checked; and
`SOURCE_BASELINE_LATEST.json` is the final canonical pointer update. The
publisher performs no retention deletion and has no fallback to the stale
`D:\forex\trad` copy. Generated host/package runtime-lock inventories remain
local and are excluded from this portable source baseline.

## Runtime evidence

Runtime evidence remains local and append-only in the canonical workspace.
Large databases and transient worker state should be recreated or separately
checkpointed under an explicit runtime profile; they must not be mixed into the
source archive. Existing legacy checkpoints are retained until a separately
reviewed retention policy proves that they are redundant and recoverable.

## Git policy

The repository is local and private. There is no configured remote and this
pass does not push, publish, or expose the project. Material source changes are
committed only after focused tests, a staged diff check, and a staged credential
audit. Account identifiers may remain private project metadata, but bearer
credentials and credential files are forbidden from the tree and the source
vault.

## Shared-project use

Other projects may consume the small current records and source manifests as a
shared progress index. They must not import Forex runtime state, infer current
account state from the vault, or alter the Forex lifecycle/authorization
databases. The canonical pending queue and project log remain the human-readable
cross-chat handoff surface.
