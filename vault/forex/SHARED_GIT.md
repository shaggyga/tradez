# Shared source and local setup

Repository: https://github.com/shaggyga/tradez (private), default branch `forex`.
The older `main` branch is preserved. The repository address may change; use ordinary
`git remote set-url origin <new-url>` after the shared Git receipt records the new location.

The workspace root is the Git root. Each machine clones locally and supplies its own
Vault path. Start with the repository README, `docs/OPERATIONAL_PREFLIGHT.md` and
`docs/ARTIFACT_REUSE.md`. Model weights, live databases and credentials stay out of Git.

The exact current commit, verified remote identity and offline bundle are in
[SHARED_GIT_REMOTE_LATEST.json](SHARED_GIT_REMOTE_LATEST.json). Verify the bundle hash
and use its explicit branch-aware restore command. The operation receipt distinguishes
the source implementation from later handoff-document commits; these are not new experiments.

Before work, fetch/reconcile shared Git changes without overwriting local edits, read
[CURRENT_STATUS.md](CURRENT_STATUS.md), check the board and run the read-only preflight.
Before a fit, inspect existing exact model/run/source/input/environment identities.
Retrieve completed work first. Ordinary checkpoint reconstruction may refit and is a
different, separately authorized operation.

Reviews, source/tests, source checkpoints and operational receipts are immutable dated
records. Current pointers select which records apply. Retired verification checkout
paths in older receipts remain historical observations, not promised live directories.

The shared board is advisory; neither Git publication nor a local Vault read proves
another machine has synced or provides an atomic distributed run reservation.
