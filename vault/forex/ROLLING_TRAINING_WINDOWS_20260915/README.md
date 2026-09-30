# Historical rolling training windows — September 15 supplement

This separately sealed supplement records the completed eight-week historical technical dataset, training-only input diagnostic and endpoint-label overlay. It follows the [first rolling technical supplement](../ROLLING_TECHNICAL_20260915/README.md). The September 14 snapshots, first September 15 supplement and their dated claims remain unchanged.

The actual project is `C:\Users\zmoor\Documents\forex\trad`. This package preserves source and compact evidence. It does not contain the raw price archive, materialized Parquet shards, live database, credentials or fitted models.

## What was completed

- All 68 pairs have 3,645,778 original minute origins for July 13–September 7, 2026. The base has 544 pair/week core shards and 544 matching peer sidecars, with 216 local technical fields and 12 peer fields. The base records 4,245,831,410 bytes; those data shards are external.
- A separate endpoint overlay preserves the same origins, clocks and split boundaries in 544 sidecars. It records 482,318,552 bytes externally and checks 60,962,440 finite strict numerical cells bit-for-bit. Missing intermediate minutes remain relevant to path/management claims even when both exact endpoints exist.
- At H60, complete-path valid returns number 2,710,875; exact-endpoint valid returns number 3,504,026, including 793,151 additional endpoint-only rows before split purging. These are overlapping historical observations, not trades or independent trials.
- The input diagnostic read 2,734,524 training origins and used 91,288 deterministic clock samples for correlation. It flagged 42 highly correlated input relationships involving 52 distinct fields; it removed no correlation-based inputs and measured no predictive advantage.
- The accepted scope records 92 tests and 30 subtests passed. Independent full base and endpoint readbacks passed. No model fitting, news enrichment, management reconstruction or trading changes belong to this package.

Read the [final implementation and recreation guide](source/forex/trad/docs/FOREX_TRAINING_WINDOWS_20260915.md), [acceptance](source/forex/trad/docs/validation/rolling_training_windows_20260915/ACCEPTANCE.json), [base readback](source/forex/trad/docs/validation/rolling_training_windows_20260915/DATASET_READBACK.json), [endpoint readback](source/forex/trad/docs/validation/rolling_training_windows_20260915/ENDPOINT_READBACK.json), and [training diagnostic](source/forex/trad/docs/validation/rolling_training_windows_20260915/TRAINING_DIAGNOSTIC.md). The retained collection snapshot is a dated observation, not a live status assertion.

All three windows were already examined historically: train July 13–August 24, validation August 24–31, later development test August 31–September 7, using UTC minute starts and exclusive ends. They are development comparisons, not untouched confirmation. No dependable profitability or improved forecast accuracy is established by this data package.

## Inspect and recreate

The readable `source/forex/trad` tree preserves project-relative imports. Its sibling `source/forex/revamp_8h_20260912/data` preserves the primary population manifest, seal, four bound code files and 136 relative receipt paths required by archive discovery. The [dependency map](DEPENDENCIES.json) distinguishes these included prerequisites from external raw candles. The [runtime record](RUNTIME.json) lists the observed package versions.

The [ZIP](rolling_training_windows_source_20260915.zip) contains the manifest and the same payload files using package-relative member names, including the `source/forex` layout. Verify the ZIP hash in [the publication receipt](PUBLICATION.json), then verify its member hashes against [MANIFEST.json](MANIFEST.json). Extract into a new empty inspection directory. The base/endpoint manifests and all 136 corresponding new range/reread receipts are retained under `evidence`; their Parquet paths intentionally remain external.

Recreate from `source/forex/trad` using the commands in the implementation guide, with the original external inputs available and new output directories. Supply a compatible environment; do not start a second writer against the active dataset. On another machine, explicit source-path migration and a new receipt/manifest are required. Recorded paths must not be silently rewritten while claiming the original hashes. The test oracle's legacy source dependencies are included for the named isolated calculator comparisons; copying them does not activate their trading runtimes.

The copied [project README](source/forex/trad/README.md), [pending-work record](source/forex/trad/FOREX_PENDING_IMPROVEMENTS.md) and [project log](source/forex/trad/FOREX_PROJECT_LOG.md) are dated copies. Their broader links and operational history extend outside this small supplement. The original project and older vault snapshots remain the source for that wider context.

This publication verifies exact copied bytes, accepted source/evidence bindings, ZIP readback, and Python syntax without executing the archived project. [ASSEMBLY_STAGE.json](ASSEMBLY_STAGE.json) preserves the earlier staging inventory and its then-pending records; MANIFEST.json and PUBLICATION.json describe the final package. [The previous vault README](before/README.md) is preserved. Older vault inventories and shared manifests do not seal this addition. The publication receipt is intentionally outside its own manifest/ZIP, and local hash verification does not establish OneDrive cloud synchronization.
