# Rolling technical dataset — September 15 addendum

This supplement records the subsequently authorized shared rolling technical dataset work. It adds source and evidence to the vault without replacing the September 14 audit, source snapshot or inventories. The older paused-work statement describes the earlier audit boundary; this package covers the specific later authorization. News, management, model fitting and trading changes were outside it.

The [implementation document](source/docs/FOREX_ROLLING_TECHNICAL_DATASET_20260915.md) explains the completed components and limits. There are 216 canonical pair-local technical fields in nine families, plus 12 separately stored currency-peer fields. Thirteen exact aliases are metadata rather than duplicate model columns. Missing support remains missing; defined fields do not imply every value is finite. Under the present universe and peer rule, only 55 pairs can receive all 12 peer fields even with synchronized data.

The common calculator now serves historical inputs and a separate rolling collector with immutable observations, actual observation/publication clocks, and later 5/15/30/60-minute outcomes. These are technical measurements, not forecasts or demonstrated trading returns. Existing trained weights are not automatically compatible with this versioned feature contract.

## Evidence and dated operating observation

- [Acceptance receipt](source/docs/validation/rolling_technical_20260915/ACCEPTANCE.json): 96 focused tests passed; exact source, test and configuration hashes are retained.
- [Historical validation](source/docs/validation/rolling_technical_20260915/history/README.md): all 68 pairs passed bounded old-Parquet, canonical-CSV and recent-tail checks. The run checked 835,426 selected OHLC rows; 556,898 distinct sampled rows met primary-source precedence. Whole-batch and overlapping-chunk features matched exact finite bits and missing masks. This did not build or validate every row of the 53.5-million-row archive.
- [Persisted replay](source/docs/validation/rolling_technical_20260915/persisted_replay.json): latest stored pair-local vectors matched replay from retained consumed inputs for all 68 pairs. This is a data-pipeline check, not a model-performance result.
- [Runtime snapshot](source/docs/validation/rolling_technical_20260915/runtime_status.json): at **September 15, 2026, 11:16:34 a.m. EDT / 15:16:34 UTC**, all 68 pairs had stored observations; 65 were current and EUR_TRY, TRY_JPY and USD_TRY were stale. The final configured output was `data/rolling_technical_dataset_20260915_v3`. This preserved observation does not assert freshness at the time of reading.

Earlier bootstrap receipts and pre-fix store-source excerpts are retained as dated engineering evidence. The final acceptance receipt identifies the accepted source/configuration. Machine-restart recovery is not installed for this collector; duration and storage are bounded. No new predictive edge or profitability was tested. The wider trading system is not certified by this addition.

## Inspect and recreate

The [source ZIP](rolling_technical_source_20260915.zip) and readable [source tree](source/) contain the same 100 exact-byte files: 14 selected source/configuration/documentation/test files and 86 evidence records, including 68 pair validation receipts. The [manifest](MANIFEST.json) records their hashes, source paths, verification and external dependencies. The ZIP members are relative to `trad`, without an enclosing `trad/` directory.

1. Verify the ZIP hash and member hashes against the manifest, then extract into a new empty inspection directory. Read the code and configuration before running it. This publication verified ZIP readback and Python syntax without executing archived project code.
2. Supply a compatible environment. The recorded test environment was Python 3.12.10 with NumPy 2.5.1; the historical adapter also needs PyArrow, and the focused tests need pytest. The manifest records the available package versions, but this is not a complete environment image.
3. The worker requires the external original candle CSVs. Historical validation additionally requires the sealed `PRICE_ONLY_POPULATION_MANIFEST_001.json`, its `.seal.json`, bound reader code and source receipts, original reacquired Parquet files, and the declared pair metadata JSON. Those are outside this supplement. Their original paths/hashes and the source-precedence rule are in the retained validation receipt.
4. Archived configuration paths name the actual machine's inputs and output. Use a separately reviewed isolated configuration for a reconstruction; do not inadvertently start another writer against the existing dataset. The raw SQLite database, its WAL, historical raw inputs, credentials and fitted models are not copied here. Recreating an original live arrival clock or exact running database state is therefore not claimed.

The copied implementation document retains one source-context link to `../FOREX_PENDING_IMPROVEMENTS.md`, outside this small source bundle. Use the dated [vault pending-work index](../PENDING_IMPROVEMENTS_CURRENT.md) for the pre-existing queue and this addendum for the completed rolling-data scope; the canonical pending file remains at the actual project path. No source document was silently rewritten to hide this boundary.

The September 14 [source pointer](../source/WORKTREE_SOURCE_LATEST.json), [inventory](../VAULT_FILE_INVENTORY.json) and [shared manifest](../SHARED_PROJECT_STATE_CURRENT.json) are unchanged and do not seal this supplement or its new README pointer. This supplement has its own manifest. The [previous vault README](before/README.md) is preserved byte-for-byte. Nothing was deleted or reclaimed; no local hash check proves OneDrive cloud synchronization.
