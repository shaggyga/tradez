# Storage retention and recovery

The Vault holds compact shared knowledge and exact artifact identities. Git holds
runnable source. Completed local runs hold canonical saved models and scientific
outputs. A successful test restore does not require a permanent second full output
tree on C:.

After a restore has passed, retain its receipt, completion manifests, tests, unique
files, and checkpoint archive. Duplicate payloads may be retired only after hashing
both the duplicate and a retained local original. Record both relative paths, byte
count and SHA-256 in an append-only retirement journal before removal. Never retire
the only saved model or replace byte retrieval with a refit. Never recursively delete
a run just because its directory is named `restored`.

The 2026-09-24 cleanup journals are in local `cleanup_20260924/` and the Vault packet
`STORAGE_CLEANUP_20260924/`. A retired restore tree is an evidence record with retired
duplicates, not an immediately runnable complete tree. Retrieve its missing bytes
before validating or consuming it. Originals remain in their recorded locations.

Preview or recover a retired file without research computation:

```powershell
python tools/forex_restore_retired.py --journal cleanup_20260924/RETIREMENT.jsonl --path '<retired relative path>'
python tools/forex_restore_retired.py --journal cleanup_20260924/RETIREMENT.jsonl --path '<retired relative path>' --apply
```

For checkpoint restore copies use `CHECKPOINT_RETIREMENT.jsonl`. The helper checks
the retained file's hash, refuses to overwrite different bytes, preserves an 8 GiB
disk reserve, and limits paths to this project. It performs no fits or replays.
Another machine must retrieve the referenced original artifacts first.

Budget storage from the actual proposed run and restore output inventory, including
peak temporary data. A whole historical package includes originals, restores, source
clones and archives; its total is not the size of one new replay. Keep a safety reserve
and record free bytes immediately before launching. Do not relax a frozen run's cap.

Live databases and training data require their own retention review. Database size,
age, a name containing `old`, or a failed run does not prove data are disposable.
Keep sealed Vault packets unchanged; publish a successor to repair documentation.
