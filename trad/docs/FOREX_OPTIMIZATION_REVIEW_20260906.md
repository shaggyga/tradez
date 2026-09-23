# Forex optimization and improvement log — September 6, 2026

Implemented the integrity-publication optimization identified by the September 5 performance audit, recorded the remaining priorities, and independently assessed the saved predictions. The actual project remains stopped. No production database, saved current integrity report, broker account, trading policy or frozen research cohort was changed by this optimization.

## Measured improvement

The compact publisher removes only `episode_rows` from five embedded alignment sections. All checks, failures, status, authority flags, contracts, counts, metrics and other existing fields remain at their original paths. The omitted arrays are retained in immutable files addressed by SHA-256, with schema, size and row-count validation. Reconstructing the saved report produces exactly the original JSON value.

| Saved-payload measurement | Before | After |
|---|---:|---:|
| Current report JSON | 98,926,690 bytes | 167,893 bytes |
| One appended history row | 73,356,296 bytes | 136,573 bytes |
| Snapshot + history publication, median | 3.326 seconds | 1.173 seconds with new artifacts; 1.079 seconds with unchanged detail |
| Summary decode, median | 422.254 ms | 0.759 ms |

The current summary is **99.83% smaller**. Repeated publication took **67.56% less time** in this bounded benchmark. Five retained detail files occupy **73,162,637 bytes**, in addition to the compact summary and history. They are reused when only summary clocks or metrics change. Changed rows create new immutable detail files; no historic artifact is automatically removed. This is not a claim that all project storage shrank by 99.83%.

The benchmark used the actual saved 98.9 MB report, five timing samples per operation and likely warm filesystem caches. First and repeat publication timings include row serialization, artifact verification and snapshot/history writes. Decode timing includes only reading the JSON value into memory. Database queries, audit computation, raw alignment input parsing, ownership-guard latency and complete runtime behavior were excluded. The earlier saved 16.125-second full audit has **not** been remeasured.

One separately instrumented sample reduced incremental Python allocation from 364,305,136 bytes for original summary serialization to 196,030,476 bytes for compact transformation plus serialization. This excludes the already resident input object and is not process RSS. The full standalone inputs are still read and validated.

## Publication, consumers and recovery

Artifacts are complete, flushed and verified before any references are published. Existing corrupted hash-addressed files cause failure and are preserved. The existing generation-ownership check still prevents an older audit pass from publishing. Current JSON, Markdown and JSONL remain separate filesystem operations; there is no new claim of a transaction across those files.

Available consumer source uses summary fields, while alignment validators read their existing full standalone inputs. Current checkpoint exports now capture the exact current snapshot bytes and its verified detail bytes together, including when the snapshot changes between file discovery and ZIP creation. Missing or corrupt dependencies abort export before replacing an existing checkpoint. Only referenced detail files are included.

To reconstruct a compact snapshot, use `oanda_integrity_publication.restore_integrity_details(payload, snapshot_dir=...)`, supplying the directory corresponding to `data/oanda_training_manager/state` in the original or restored project. References are relative to this trusted snapshot directory. Four-hour saved records carry an explicit project-relative source locator and can use `restore_snapshot_integrity(..., project_root=...)` after relocation. An archived historical JSONL needs the union of its own referenced detail files and this original reference base; exporting the current checkpoint alone does not promise a historical-log closure. No detail garbage collection or historical rewriting was added.

Source-only vault recreation still excludes runtime data. Its purpose is to preserve the reviewed implementation and records. The compact-current transitive export path is tested on disposable fixtures; the full runtime checkpoint job was not run for this handoff.

## Validation and evidence

The combined guarded offline suite passed **131 tests**, with **one Windows symlink-privilege skip**, across compact publication, integrity validation, exporter coherence, relocated recovery, the four-hour observer, canonical record sync and issue-register validation. The guard blocked workers, subprocesses, networking and SQLite outside disposable fixtures. Independent reviews examined both publisher and consumers. Ordinary path traversal and malformed-reference rejection passed despite the platform-specific symlink skip.

Exact reconstruction, unchanged non-row values, changing-clock reuse, retained historical artifacts, missing/corrupt metadata, publication faults, stale owners, concurrent export changes and copied-snapshot restoration were checked. The actual saved source report's hash remained unchanged. [The validation receipt](../FOREX_OPTIMIZATION_VALIDATION_20260906.json) binds reviewed source and evidence. [Detailed benchmark](validation/optimization_20260906/integrity_publication_benchmark.json) and [consumer review](validation/optimization_20260906/integrity_consumer_review.md) preserve scope and measurements.

The calculation and benchmark scripts under `docs/validation` are preserved records of this local run. For reproduction, copy them into a disposable review directory directly under `C:\Users\zmoor\Documents\forex` and retain their documented original input paths; the fixture runners infer the sibling `trad` project. They are not runtime entrypoints. Original runtime inputs are not all in the source-only vault export. Prior dated audit, repair and performance receipts remain unchanged.

## How good were the predictions?

**No reliable useful edge has been demonstrated.** The broad current strict-horizon shadow diagnostic got direction right on **4,156 / 8,414 outcomes (49.39%)**; **716 / 8,414 (8.51%)** had a positive executable result after spread. These are correlated simulated signal outcomes, not an account's live-trade win rate or a single score for every model.

Equation calibration improved replay direction to 52.34% globally and 51.57% by pair. However, completed-outcome ordering does not establish that the calibration labels were available before each forecast was issued. These are diagnostic replay scores, not verified forward prediction accuracy. This is registered as the new open P1 research issue `FX-20260906-CALIBRATION-REPLAY-ISSUE-CLOCK`; account eligibility remains false. Global Brier 0.249301 is only slightly better than the constant-50% reference of 0.25; pair Brier 0.250504 is worse. Neither substitutes for a frozen baseline evaluated on the same decisions.

The four current model families have negative saved after-cost means and only 8–9 effective episodes each. Their exact-cohort directional hit rates are absent. Historical movement-clearance models show some discrimination, but selected no trades under the strict allocation rules and remain inspected-archive discovery. Current source-news/rank prospective evidence and confirmed hypotheses are zero. [The prediction assessment](FOREX_PREDICTION_QUALITY_20260906.md) separates cohorts, horizons, costs, baseline limitations and causality; [its receipt](../FOREX_PREDICTION_QUALITY_20260906.json) retains denominators and input hashes.

## Remaining improvements

[The dated backlog](../FOREX_OPTIMIZATION_BACKLOG_20260906.json) records what was implemented and what remains: profile expensive queries on coherent realistic read-only fixtures; measure storage growth and verified retention candidates; profile duplicate source work and useful per-source yield; and create a separate time-causal prediction scorecard with frozen baselines and independent after-cost evidence. No model was selected or tuned from the favorable slices of these already-inspected results.

Live timing, a complete clean integrity cycle, source V9/rank V8 activation and new prospective prediction evidence remain unverified. Source V9/rank V8 are still disabled. Forex autostart remains disabled and the original stopped-state requirement remains in force.
