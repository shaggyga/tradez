# Forex Vault knowledge included in Git

This is a versioned documentation copy of the shared `thevault/projects/forex` folder.
It lets a collaborator read the project without the original machine. The live shared
Vault remains the authority for claims, queue updates and checkpoint publication.
This copy does not synchronize itself.

Start with [the Vault entry page](forex/START_HERE.md), then:

- [Design pointer](forex/DESIGN_ALIGNMENT_LATEST.json) and the full design it identifies.
- [Queue](forex/REVIEW_QUEUE.json) and [checkpoint review](forex/CHECKPOINT_REVIEW_LATEST.json).
- [Reuse rules](forex/VAULT_FIRST_REUSE.md).
- [Current context](forex/PROJECT_CONTEXT_CURRENT.md).
- [Dashboard display qualification](forex/DASHBOARD_DISPLAY_SOURCE_REVIEW_20260930/REVIEW.md).
- [Producer source compatibility follow-up](forex/DASHBOARD_PRODUCER_QUALIFICATION_20260930/REVIEW.md).
- [Copied-file hashes and omissions](forex/SNAPSHOT_MANIFEST.json).

Included: top-level and packet-level UTF-8 Markdown, small JSON records, text receipts,
hashes and nested design specifications, preserving original relative paths and bytes.
Excluded: deeper historical material, embedded source checkouts, raw evidence/data
directories, model binaries, archives, nonlocal cloud files, documents over 2 MiB and
potential credentials. Every omission is recorded. Copied manifests may reference excluded
artifacts: retrieve those from their original store. Source belongs in the project itself.

This is a **knowledge snapshot, not a complete restore capsule**. It cannot establish
current ownership, pass scientific preflight or qualify model loading. Original absolute
paths and historical `CURRENT` labels remain historical provenance.

## Refreshing the copy

Export to a new directory and verify it before reviewing and publishing:

```powershell
python tools/forex_vault_snapshot.py export --source 'C:/your/shared/thevault/projects/forex' --destination 'C:/local-review/forex-vault-next'
python tools/forex_vault_snapshot.py verify --destination 'C:/local-review/forex-vault-next'
```

The exporter refuses existing destinations, credentials, redirects and source changes
during copying. Inspect its omission list and the Git diff. Replace `vault/forex` only
after preserving its Git checkpoint and verifying the exact destination. Do not merge
old and new snapshots or automatically write back to the live Vault.
