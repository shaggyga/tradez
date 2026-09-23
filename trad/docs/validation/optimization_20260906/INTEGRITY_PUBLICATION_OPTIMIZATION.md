# Integrity publication optimization

The integrity publisher now omits only `episode_rows` from its five embedded move-first alignment sections. Every other existing field, including all checks, verdicts, failures, counts, contracts, arm metrics and authority flags, remains in its original location and retains its value. Input reports and validator execution are unchanged. No current production snapshot or database was rewritten during this work.

`oanda_integrity_publication.py` stores each omitted array in a canonical UTF-8 JSON envelope at a SHA-256-addressed path beneath the integrity snapshot directory. References bind the detail contract, hash, size and row count. Summary timestamps and metrics can change while unchanged rows reuse the same artifact. New rows create a new artifact; old artifacts are never automatically removed. `restore_integrity_details` reconstructs the full original JSON value, and the read-only verified-artifact APIs support coherent exports.

Artifacts are fully written, flushed, atomically linked without overwriting an existing name, and hash-verified before any reference is published. Existing corrupt names fail closed and are preserved. Publication retains its monotonic generation ownership guard; superseded passes cannot create artifacts or publish. The existing current JSON replacement, Markdown replacement and JSONL append remain separate filesystem operations, not a multi-file transaction. Detail or current-write failures may leave complete unreferenced artifacts, which are safe to retain; a later ancillary write failure has the existing partial-publication limitation.

All detail paths are relative to an explicitly supplied trusted integrity snapshot directory. Readers of history or embedded copies must supply that original directory, not the directory of the copied JSON. Validation rejects absolute paths, drive/UNC paths, traversal, unexpected aliases, escape through resolved parents, incorrect metadata/envelopes, missing details and hash/size/count mismatches. Recovery integration owns the vault exporter and copied-snapshot reference locator.

## Verification

The guarded focused suite passed **33 tests** with **one skip** in 2.64 seconds. The skipped test requires Windows symlink-creation privileges unavailable on this host. Ordinary path-escape and metadata rejection tests passed. The suite covers exact reconstruction without input mutation, every retained field, array deduplication, changed-clock reuse, changed-row history preservation, immutable corruption/missing-data behavior, coherent captured bytes, artifact-write failure before snapshot/report/history mutation, current-write failure, stale owners and existing history rotation. The test guard forbids broker/network/subprocess/worker starts and production database access; SQLite use is confined to temporary ownership fixtures. Initial harness setup blocked pytest's default temporary/logging writes; directing temporary files into the fixture directory and disabling its unused logging plugin resolved the harness setup without allowing external writes.

The benchmark read the unchanged saved integrity JSON with SHA-256 `a93a6bfcaf4c9c752c8601175537a1837b434ade23bac5a802618b4be53fe6ca`. It verified exact reconstruction, all retained values, unchanged artifact size/mtime across changed summary clocks, and unchanged source bytes. All benchmark writes used disposable fixtures under this optimization directory; there were no database connections or runtime calls.

| Measurement | Original | Compact |
|---|---:|---:|
| Current JSON bytes | 98,926,690 | 167,893 |
| One history row, bytes | 73,356,296 | 136,573 |
| Current JSON reduction | — | 99.8303% |
| History row reduction | — | 99.8138% |
| Snapshot plus history publication, median | 3,325.810 ms | 1,172.738 ms first publication; 1,078.923 ms unchanged-detail repeat |
| Snapshot JSON serialization, median | 2,551.172 ms | 3.815 ms after detail preparation |
| Snapshot JSON decoding, median | 422.254 ms | 0.759 ms |

The first publication retained five detail artifacts totaling **73,162,637 bytes**, containing 2,074 live-arm rows and 60/420/366/1,392 mapping V1/V2/V3/V4 rows. That retained evidence cost is additional to the compact current/history files; it is reused when the arrays are unchanged. The first and repeat publication timings include array encoding, artifact hash verification and compact snapshot/history writes. Repeat timings include changed summary clocks and hash-verification of existing artifacts. There were five samples per timing, with likely warm OS caches and no percentile claim.

One separate `tracemalloc` sample measured incremental Python allocations with the input object already resident: full snapshot serialization peaked at 364,305,136 bytes; compact repeat transformation plus serialization peaked at 196,030,476 bytes. Full/compact decoding peaked at 230,935,741/469,655 bytes. These are explicitly scoped Python allocation measurements, not process RSS or full-runtime peaks.

The benchmark excludes audit computation, raw input report parsing, database queries, ownership-guard timing, full ledger load, worker contention and broker latency. It does not establish full-cycle speedup, runtime readiness or improved predictions/profitability. Source inputs remain large and full evidence remains available. A future authorized restart must still complete the full integrity cycle and existing operational gates.

Evidence: `pytest_publication.xml`, `run_publication_tests.py`, `integrity_publication_benchmark.json`, `benchmark_integrity_publication.py`, and `integrity_publication_receipt.json` in this directory. Original dated audit and repair receipts were left untouched.
