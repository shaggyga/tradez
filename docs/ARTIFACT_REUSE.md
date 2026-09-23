# Reuse retained models and results

Read the Vault's current pointers, `VAULT_FIRST_REUSE.md` and coordination board before running work. This repository's [reuse catalog](../artifacts/reuse_catalog.json) identifies preserved bytes and their original run identities. The [retrieval registry](../artifacts/registry.json) covers packages supported by `tools/forex_workspace.py retrieve`. Neither file grants execution or trading approval.

| Catalog ID | Preserved fitted payloads | Retrieval |
| --- | ---: | --- |
| `matched_remaining_saved` | 8 joblib files and 23 original completion payloads | Verified `retrieve` command |
| `matched_campaign_saved` | 14 joblib files and 34 original completion payloads | Verified `retrieve` command |
| `rich_family_saved` | 84 joblib files and 216 original completion payloads | Verified `retrieve` command |
| `directional_retained_saved` | 8 original directional joblib files; frozen study, results, source and input evidence in the original 99-member archive | Verified byte copy of the existing archive; automatic extraction is unsupported |
| `macro_event_forecast_saved` | 6 existing JSON parameter records and companion results | Verified byte copy of the existing JSON files |

These 120 payloads/parameter records represent five retained groups, not 120 independent experiments. Some weights are reused between runs. The catalog additionally indexes 48 inherited approvals and byte-checks 45 distinct archives. It links the earlier 3,124-record experiment census and lineage index without recounting or requalifying them. This is a bounded inventory, not an exhaustive census of every model ever produced.

## Retrieve supported packages

From the repository root, with the appropriate Python interpreter:

```powershell
python tools/forex_workspace.py retrieve --vault "<this-machine-vault-forex-root>" --artifact matched_campaign_saved --destination "<new-local-directory>"
python tools/forex_workspace.py retrieve --vault "<this-machine-vault-forex-root>" --artifact rich_family_saved --destination "<another-new-local-directory>"
```

The destination parent must already exist and the destination must be outside the Vault. Retrieval verifies the complete archive, original fingerprint, original completion inventory and every preserved payload. A matching existing destination is verified and reused. A partial, corrupt or unexpected destination stops for review and is not overwritten. There is no fit, model loading or prediction in this command.

The new packages preserve the exact original runs:

- Matched campaign: `b3c3ea235bb15727633b665f86ea535b48bfe14cc35d00e60b12e7d848ea750d`.
- Rich family: `4516f210457f2ef721ed8e6bc4a4db11a0b5b2381eb519d070e65bca26093af1`.

Original source, recipe and environment references remain in the registry and archive manifests. The inherited current campaign approval has a historical recipe mismatch; the saved campaign package instead binds the exact original completed-run recipe. Retrieving it does not make that inherited approval current or waive preflight.

## Retrieve older formats as bytes

For `directional_retained_saved` and `macro_event_forecast_saved`, automated `retrieve` is unsupported. Resolve the selected catalog entry's `artifact_refs` under the supplied Vault root, confirm every file's `bytes` and SHA-256, and copy it to a new local directory while retaining its Vault-relative path. Verify the copied bytes again. If a destination exists, reuse only when its bytes already match; otherwise stop and preserve it for review.

For example, the complete directional archive has SHA-256 `a9f29a3d51e40c114655d27465ec03b269cba5c26c5bb24105c97da096d86fd9` at `DIRECTIONAL_RECOVERY_20260922_031840/checkpoint/local_restoration_001.zip`. Copying this original archive retains all eight saved models and their companion evidence without creating a replacement artifact. The catalog includes all 99 member hashes for any subsequent extraction into a new empty directory. Reject unsafe paths, links, duplicate names, unexpected files or hash differences; do not execute anything from the archive.

The directional study predates modern run fingerprints. Its original `FROZEN_RUN.json`, study ID and spec hash identify it; do not manufacture a modern fingerprint. Archived source revisions and recorded original fit bindings are retained separately and have not been freshly qualified for model loading.

The macro entry preserves `MACRO_EVENT_FORECAST_20260922_165513/results/models.json` and its original results/status files. Those six coefficient/mean/scale records are existing fitted parameters even though they are not joblib files. Its recorded run fingerprint is `c2709be60f699dba7b433a7b78f3b0a70c84e489d8546ee738a37e0dfcdede37`; the original result remains a negative development layer comparison.

## Stop duplicate work before it starts

Match the original objective, target, source, inputs/vintages, features, split, model/configuration, seeds, environment and predecessor identity. A new path, machine or owner is not a new experiment. Respect active claims and resume matching partial work before making a new claim.

The catalog distinguishes saved fitted bytes, results-only evidence, recipe-only recovery, unavailable references and historical unqualified approvals. A checkpoint with source and expected replay hashes can run fitting when restored; it is not a saved-model download. When a required archive is missing locally, make it available from the shared Vault or producing machine. Do not run a restore script or refit to fill the gap.

Byte verification does not establish runtime compatibility, scientific acceptance, independent review, live authorization, cloud sync or installation on G's machine. Retain negative results, original fingerprints and predecessor links. Read the current Vault for the next scientific item rather than taking any historical record's next-action text as current.
