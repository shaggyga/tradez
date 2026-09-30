# Restoration and retained dependencies

## Games

| Game | Launcher path (junction on C) | Authoritative verified files | File count |
| --- | --- | --- | ---: |
| VALORANT | `C:\Riot Games\VALORANT` | `D:\Games\VALORANT` | 271 |
| NFS_HEAT | `C:\Program Files (x86)\Steam\steamapps\common\Need for Speed Heat` | `D:\Games\Need for Speed Heat` | 393 |

Keep D attached with its current drive letter. The original C backup copies were removed only after full content-hash verification and probes through the final junctions. Gameplay was not tested. To deliberately return a game to C, close its launcher and game, copy D to a fresh C staging directory, fully verify every content hash, remove only that game's junction, then rename the verified staging directory to the original launcher path. Never recursively delete through a junction. The exact restoration map and original path are in each relocation receipt.

## Archived diagnostic logs

Local authoritative compressed copies are in `C:\Users\zmoor\Documents\forex\cold_log_archive_20260915\payloads`. All 246 receipts are retained in this package and locally under the matching `receipts` directory. From the actual `trad` project, using the recorded Python runtime:

```powershell
python -B tools/archive_forex_cold_logs_v1.py --restore-receipt C:\Users\zmoor\Documents\forex\cold_log_archive_20260915\receipts\ITEM.json --restore-copy C:\Users\zmoor\Documents\forex\restored_logs\ORIGINAL.out.log
```

Substitute an exact recorded receipt and a new output path. The restore command verifies the archive and original content hash, creates a separate copy and refuses overwrites. It does not restart a service or put restored files back into an active writer's path. The one independently restored example remains local and is excluded from the vault because stdout may contain private details.

## Caches and protected material

Browsers redownload removed HTTP/code caches; GPU applications rebuild removed shader caches; the package manager redownloads/rebuilds its removed download cache. Deleted old crash dumps cannot be recreated byte for byte. Two locked shader files and recent entries were skipped. User documents, browser profiles/session data, Codex attachments/plugins/runtimes, current temporary files, models, datasets, current service outputs and Windows recovery/setup material were retained by this cleanup.

## Reproducible research

Preserve the local prepared datasets, historical archive, normalizers, source bindings and models listed by the checkpoint manifests. The vault holds compact source/evidence and dependency hashes, not every raw data file. Failed original period studies remain alongside fresh recovery outputs. Reproduce via the preserved pre-fit scope and recovery plan into new output directories; never overwrite the recorded original studies. Final results require the independent acceptance publication.

## Temporary test junction

`C:\Users\zmoor\Documents\forex\cleanup_audit_20260915\junction_capability_test` is a temporary link to D:\Games\VALORANT whose unlink was blocked by automatic approval review. Leave it out of recursive cleanup/inventory and packaging. The explicit notice is preserved; it is not the game's launcher junction.
