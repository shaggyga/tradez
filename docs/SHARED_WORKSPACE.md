# Shared source and saved artifacts

The Forex Vault is the shared brain: current engineering queue, ownership, review status,
run identities and artifact references. Git carries source and this small read-only helper.
The helper neither claims work nor updates the Vault. Read `FOREX_HANDOFF.md`, the live
Vault's `VAULT_FIRST_REUSE.md`, pointers, queue and coordination board before engineering.
The board is advisory; it is not an atomic cross-machine lock.

Use Python 3.10 or later. Byte retrieval needs only Python's standard library and installs
nothing. Run from the Git checkout root; explicitly supply the equivalent local Vault path.
These PowerShell examples use an existing Python interpreter:

```powershell
python -I -B tools/forex_workspace.py status --vault 'C:/path/to/thevault/projects/forex'
python -I -B tools/forex_workspace.py doctor
python -I -B tools/forex_workspace.py retrieve --vault 'C:/path/to/thevault/projects/forex' --artifact matched_remaining_saved --destination 'C:/local-artifacts/matched-remaining-native-inputs'
```

The destination's parent must already exist. The destination itself must be new, or an
exact complete copy previously retrieved by this helper. An existing empty, partial,
corrupt or extra-file destination returns `review_required` (exit 2) without overwriting
or deleting it. Preserve the evidence and reconcile the mismatch; a missing local model
is a retrieval problem, never an implicit authorization to fit another model.

`status` reads both current pointers, the live review queue, the complete coordination
board and the reuse-policy bytes. It checks the two pointer manifest hashes and reports
the live queue's exact next item, active step and review status. Sealed package text may
predate a later review. It does not validate every member of those large packages or
certify remote synchronization. A detected change during the read returns review required;
the output is an advisory local snapshot, not a work reservation.

`retrieve` checks the pinned ZIP hash and size, saved-artifact manifest hash, original
completion and identity hashes, original fingerprint, payload inventory and every file's
SHA256. It rejects unsafe/non-portable paths, symlinks and redirecting/unknown reparse points, duplicate or case
collisions, file/directory collisions, encrypted or unsupported compressed entries, and
expansion beyond 256 MiB. It creates files exclusively and verifies the resulting inventory.
An interruption may leave a partial destination for review; the helper does not clean it
up or silently resume it. Use a private local parent directory where other processes do
not modify paths during retrieval. This is not a security sandbox for concurrently hostile
filesystem writers.

Locally available OneDrive Cloud Files are permitted for reading the Vault and registry.
Only the documented `IO_REPARSE_TAG_CLOUD` family is allowed on source paths; symlinks,
junctions, name-surrogate tags and unknown reparse tags remain rejected. Destination
paths and their ancestors still reject all reparse points, including Cloud Files.
Offline or recall-marked cloud source files return `review_required` before opening;
make the required files available offline first using your normal OneDrive workflow.
The helper does not change pin/hydration settings or request downloads. This distinction
follows Microsoft's [reparse-tag definitions](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/c8e77b37-3909-4fe6-a4ea-2b9d423b1ee4).

The registry has one deliberately bounded entry: `matched_remaining_saved`, the original
`matched-remaining-native-inputs` run with fingerprint
`fc0a1423ca93b62e94f215b49a6a55aaf280524aadc1501b231efef4031a2763`.
Its archive contains 26 files: 23 original payloads (including eight fitted `.joblib`
files and associated fit metadata), the original completion and run identity records,
and the preservation manifest. Replicas remain this same experiment. The archive hash is
`70c3d4fb54fb86e79b4bcd65c9cd923768592887e5278c93bc27a7ab5244a10b`.

`artifacts/registry.json` pins the Vault-relative archive, preservation receipt, original
recipe, 13 original source references and original environment. The references come from
the preserved `BUILD_RECEIPT.json` and manifest. They describe the original runnable
consumer context; the helper does not import, execute or copy those external sources.
It verifies their reference records against the pinned manifest, not their live files.
The archive excludes the full predecessor input/model graph. Ordinary engineering source
changes do not qualify a new consumer for these saved models.

`doctor` reads installed package metadata without importing numerical/model libraries.
Even matching version strings do not prove model-loading or prediction compatibility.
Retrieval executes no model deserialization, prediction, fitting, checkpoint reconstruction,
API call or broker/service operation. Runtime qualification, independent review and trading
authorization remain separate. Earlier checkpoint restore recipes may refit missing models;
do not invoke them merely to retrieve these already saved artifacts.

Run the helper's offline tests with an explicit evidence directory. Synthetic fixtures
are retained there for inspection; no cleanup or model execution occurs:

```powershell
python -I -B tests/test_forex_workspace.py --evidence 'C:/local-test-evidence/bootstrap-test-001'
```

The registry is a pinned retrieval catalog, not an exhaustive model census or automated
cross-machine deduplication service. Extend it only after checking the Vault's current
reuse records, exact source/input/model identities, artifact availability and ownership.
