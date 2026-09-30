# Verified cleanup checkpoint — September 15, 2026

Sealed after cleanup; observation time: 2026-09-16T00:15:07.912862+00:00. The original [pre-cleanup README](README.md), all 87 original payloads, and their exact [manifest](PRE_CLEANUP_MANIFEST.json) are preserved. This final page supersedes their in-progress cleanup status.

| Completed work | Logical bytes recovered from C | GiB | Verification |
| --- | ---: | ---: | --- |
| Audited caches and old crash dumps | 2,425,883,718 | 2.26 | 44,116 removed files; locked and recent entries retained |
| Lossless cold-log archive, net of receipts and retained restore example | 22,584,115,791 | 21.03 | 246 original logs; all restored-stream hashes verified |
| VALORANT moved to D | 27,279,166,680 | 25.41 | 271 file hashes and launcher-path probes verified |
| NFS_HEAT moved to D | 36,994,505,751 | 34.45 | 393 file hashes and launcher-path probes verified |
| Total of the stated logical measures | 89,283,671,940 | 83.15 | Includes net compressed-log storage and retained restore example |

Observed free space was 128.62 GiB on C and 1917.98 GiB on D at the observation time. These totals are not an isolated physical-allocation experiment: research writes and other authorized work ran concurrently. D now holds the moved games.

The [restoration and dependency map](RESTORATION.md) describes the local authoritative copies. Cache downloads and shader builds can be recreated by their applications. Nine old crash dumps were discarded diagnostic residue; their original bytes cannot be regenerated. Compressed private logs and the verified restored example remain local. No cache payload, raw private log, game binary, credential, database or full research row dataset is copied into this vault package.

Both games retain their original launcher paths through verified NTFS directory junctions to D. Launcher metadata stayed unchanged. File and executable-path probes passed; gameplay was not tested. D must remain connected with its current drive letter. A separate temporary junction in the cleanup evidence directory remains explicitly documented; do not follow it during cleanup or packaging.

## Research state at this cleanup checkpoint

rolling_period_replication_20260915_a_v2: running (3/7 new family fits); rolling_period_replication_20260915_b_v2: running (3/7 new family fits). These are status observations, not completed study results. The first A/B study runs failed at an all-missing-column library edge case after retaining their completed full-context grids. The [repair plan](source/forex/trad/docs/validation/rolling_period_replication_20260915/RECOVERY_PLAN.md), exact failed manifests, scoped compatibility source, continuation adapter and tests are retained. Fresh v2 outputs preserve the closed grids and run only the seven remaining family fits. Final research acceptance and conclusions remain a separate publication after both continuations and independent audits complete. No model was promoted, and this cleanup did not change live collection or trading.

The [fixed pre-fit scope](source/forex/trad/docs/validation/rolling_period_replication_20260915/PRE_FIT_SCOPE.md), original source closure, build commands, manifests and normalizer extracts remain in the preserved checkpoint. Full prepared input, quote, OOF and forecast rows are local dependencies; the vault is not a complete copy of every database. The [latest completed specialist study](../ROLLING_SPECIALISTS_20260915/README.md) remains a separate historical result.

## Evidence and independent readback

The final MANIFEST.json and cleanup_checkpoint_20260915.zip bind every payload. PUBLICATION.json records complete payload and ZIP-entry readback, CRC verification, original-byte preservation and the root README navigation change. Cloud synchronization is not asserted.

- [Cache receipt](source/forex/cleanup_audit_20260915/CACHE_CLEANUP_002.json) and [independent deletion readback](source/forex/cleanup_audit_20260915/CACHE_CLEANUP_002_READBACK.json).
- [Cold-log reconciliation](source/forex/cleanup_audit_20260915/CLEANUP_RECONCILIATION.json), with all 246 item receipts and the exact archiver/source tests.
- [Game evidence index](source/forex/cleanup_audit_20260915/GAME_RELOCATION_EVIDENCE_INDEX.json), including failed attempt history and both completed restoration maps.
- [Finalizer tests](source/forex/cleanup_audit_20260915/FINALIZER_TESTS_002.json), exact finalizer source and explicit final input. Packaging uses named regular files only and excludes reparse points.
