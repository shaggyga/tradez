# Verified storage cleanup — September 15, 2026

Completed the authorized cache cleanup, lossless old-log compression, and moves of VALORANT and Need for Speed Heat to D. The selected files account for **83.15 GiB** of logical space recovered from C, after the retained compressed logs, archive metadata and local restoration example. C had **128.62 GiB free** at September 16 00:15:07 UTC (September 15 8:15 p.m. EDT). Whole-drive free-space changes also include concurrent application and research activity.

| Work | GiB recovered | Verification |
|---|---:|---|
| Old disposable caches and crash dumps | 2.26 | 44,116 deletions checked; locked and recent files skipped |
| Cold stdout logs | 21.03 | 246 full compressed/decompressed hashes matched; a local example restored |
| VALORANT | 25.41 | All 271 files verified; original launcher path and executables checked |
| Need for Speed Heat | 34.45 | All 393 files verified; original launcher path and executables checked |

The games now reside at `D:\Games\VALORANT` and `D:\Games\Need for Speed Heat`. Their original C paths are directory junctions to those verified copies. Exact old C backups were removed only after copy and launcher-path verification. Riot and Steam metadata remained unchanged, and Steam was restored to its prior running state. D must remain connected as D. Games were not launched; gameplay and anti-cheat behavior were not tested.

The old Forex logs are stored under `C:\Users\zmoor\Documents\forex\cold_log_archive_20260915`, with original filenames, byte counts and SHA256 restoration proofs. `tools/archive_forex_cold_logs_v1.py --restore-receipt <item.json> --restore-copy <new-file>` restores a verified new copy and refuses to overwrite an existing file. Private log contents and the restored example remain local. Cache downloads and shader caches can be rebuilt by their applications; discarded old crash dumps cannot be regenerated.

The [sealed vault checkpoint](C:/Users/zmoor/OneDrive/thevault/projects/forex/CLEANUP_CHECKPOINT_20260915/FINAL_README.md) contains the source, test receipts, exact deletion/archive/game manifests, restoration map and a verified ZIP. Its final manifest SHA256 is `4c868ecd6cb4ea02a7909ffa44c5b5e740f70dd201f1eed5c9d1cad4b02753eb`. All 87 precleanup payloads remain intact within the 407-file final package. The [local publication receipt](../../cleanup_audit_20260915/FINAL_VAULT_PUBLICATION.json) records the ZIP and root README readback. Local verification does not establish cloud synchronization.

An [independent final readback](../../cleanup_audit_20260915/CLEANUP_VAULT_INDEPENDENT_READBACK.json) passed for all 407 payloads, all 408 ZIP entries, the 87 preserved originals, all 28 game evidence files and the root README insertion. Its SHA256 is `6979cb6fe59544fc360dcc92151c964a99ac4d13ae48b06a53c09a282ea689ee`.

Historical prices, databases, accepted model evidence, current research outputs and active collector files were retained. This cleanup did not change collection or trading settings. Its dated research snapshot is separate from the [rolling-period research report](FOREX_ROLLING_PERIOD_REPLICATION_20260915.md).

Automatic approval review rejected removal of the temporary `cleanup_audit_20260915\junction_capability_test` junction with only “blocked by policy” as its reason. It remains documented and excluded from recursive scans and packaging; it contains no duplicate game data. Do not recursively delete through it.
