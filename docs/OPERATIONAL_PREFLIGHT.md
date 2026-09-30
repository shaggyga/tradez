# Offline operational preflight

Run from a clean Git checkout before implementation or computation. Read the current
handoff and Vault reuse policy first. This command reads local metadata and bytes;
it installs nothing, launches no services, loads no models and makes no network calls.

```powershell
python -I -B tools/forex_preflight.py --vault 'C:/path/to/thevault/projects/forex'
```

The JSON result is `pass` (exit 0) or `blocked` (exit 2), with separate checks and
concrete refusal reasons. A pass describes local setup at that instant. It does not
grant research, execution or trading authorization, prove cloud synchronization,
reserve work or qualify prediction compatibility. In particular, this operational
checkpoint does not authorize resuming research until the user requests that work.

The command checks:

- The supplied folder is the repository root, its tracked/untracked working tree is
  clean, its branch matches the Vault Git receipt, and HEAD equals that receipt's
  final handoff-document commit. `--expected-revision <full-40-character-SHA>` instead
  selects an explicitly reviewed exact revision; it never means any newer commit.
- Both current pointer hashes, all bounded immutable package members, current Vault
  and project document pins, the complete current engineering root source snapshot,
  and stability of read files and Git state across the inspection.
- The current queue's operational prerequisite and manifest-covered `OPS_STATUS.json`.
  Both statuses must be `complete` or `accepted_within_scope`; pending review remains
  blocked. The gate pointer and status must name the exact `active_operational_step`
  required by the queue; its unique operational queue entry must be complete,
  reviewed `accepted_within_scope`, and pin a packet member/hash in that same verified
  gate package. An older complete gate cannot satisfy a newer unfinished prerequisite.
  The selected next item and its immediate engineering prerequisite must
  be present and the prerequisite reviewed. Historical unrelated approvals are not
  treated as universal blockers.
- Unresolved board claims. Completed/abandoned rows are history in either table.
  `--claim-id <your-active-task-ID>` identifies your existing active claim; it does
  not create or take over one. Other unresolved owners require scope reconciliation.
  The Markdown board cannot reliably establish non-overlap automatically, so the
  tool conservatively reports unresolved claims until the board is reconciled.
  A current sealed review manifest may contain `coordination_reconciliations`:
  each binds the exact parsed other claim, selected work item, own claim and a
  substantive manual non-overlap decision. Changed/additional claims fail closed.
  This preserves unrelated capture ownership; it does not release or take over it.
- Exact Python/package metadata in `requirements-engineering.lock.txt`. Use
  `--profile stdlib` only for document and byte-retrieval work; it explicitly leaves
  the numerical environment unchecked. No packages are imported to inspect versions.

For an already approved recipe, use its independently recorded hash. This checks
its exact environment and source closure in place of the general engineering profile.
It does not run the recipe, read its input population or waive its operator gates.

```powershell
python -I -B tools/forex_preflight.py --vault 'C:/path/to/thevault/projects/forex' --recipe 'C:/path/to/approved-recipe.json' --recipe-sha256 '<64-character-SHA256>' --claim-id '<your-active-task-ID>'
```

Add `--artifact matched_remaining_saved` to verify that registered archive's local
availability, archive hash, complete member inventory and original run bindings using
the existing `forex_workspace` verifier. The option can repeat. Verification reads
bytes only and does not extract, deserialize or refit models. Without `--artifact`,
archive availability is not claimed. Discovery-only catalog entries that lack a
registered retrieval contract require their documented manual byte verification.

If blocked, preserve all changes and identify the stated mismatch. Update a stale
checkout through ordinary reviewed Git changes, finish interrupted checkpoint
publication, reconcile the board owner, or retrieve the exact missing artifact.
Never regenerate hashes, weaken recipe pins, discard edits, or fit new models just
to make preflight pass. Missing or cloud-only Vault files need normal synchronization
and offline availability on that machine before retrying. The helper reuses the
existing no-hydration/no-redirect path checks.

The current local publisher already verifies exact run identity and uses a local
single-writer lock. It does not lock an identity across two independent computers.
Until an atomic shared reservation exists, establish shared freshness and one owner
for the exact identity before any run. An offline preflight cannot prove that a second
machine has not begun the same work; uncertainty keeps that run blocked.

Offline refusal/recovery tests retain synthetic fixtures and use isolated temporary
Git repositories; they perform no network or model operations:

```powershell
python -I -B tests/test_forex_preflight.py --evidence 'C:/local-test-evidence/preflight-001'
```
