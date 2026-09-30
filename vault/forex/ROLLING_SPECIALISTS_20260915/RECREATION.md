# Recreating this specialist checkpoint

## What is self-contained

The compact package contains every saved prefix/final head bundle, meta model, calibrator, prior, and exact prefix normalizer used in the accepted comparison. The model files contain the exact meta scalers and selected feature names. Read the [input recipe](evidence/inputs/SPECIALIST_INPUTS.json) and [normalizer registry](source/forex/trad/docs/validation/rolling_specialists_20260915/NORMALIZERS.json) before inference.

The source directory is a preserved tree, not a ready standalone deployment. Merge its `source/forex` tree with the exact predecessor trees identified in [DEPENDENCIES.json](DEPENDENCIES.json), preserving file hashes. This yields `forex/trad` and sibling `forex/direction_decision_20260911/src/signed_cost_models_v1.py`; the core verifies the legacy helper by SHA256. Do not flatten that layout. Eighteen already archived source pins are referenced instead of copied again.

Use the recorded [runtime](RUNTIME.json). The retained joblib objects depend on compatible Python, NumPy and scikit-learn versions. Recreating input features and normalizing them is separate from replaying saved estimator output. Select the correct prefix index for a historical OOF week; final assessment uses index 4, with August 24 TRAIN cutoff. Never apply final TRAIN normalization to an earlier OOF origin.

## Row-data dependencies

The eight-week base core/peer Parquet, endpoint-only label sidecars, corrected V2 quote/ARIMA panel, old prepared inputs and specialist raw feature matrices remain in their accepted local paths. The 68 source records, hashes and boundaries are retained; the large row matrices are excluded here. The old comparison's forecasts are also needed for the recorded prior-comparator analysis. Exact raw M1 inputs and primary archive recipes are described by the preceding training-window package. Existing sealed recipe hashes do not substitute for absent raw bytes.

Saved specialist forecast Parquet, six-head forecasts and OOF matrices are excluded. To regenerate those, restore matching registered inputs and execute the retained runner in a new output directory, using the recorded versions and immutable recipe. A full refit is different from saved-state inference replay and was not performed during publication. The independent accepted audit replayed bounded saved-model rows and scalar accounting checks; it did not independently refit every estimator or every concentration ranking.

Use the source guide's [recreation commands](source/forex/trad/docs/FOREX_ROLLING_SPECIALISTS_20260915.md) with new output directories. Existing accepted outputs must not be overwritten. Restore package `evidence/comparison` to a separate local research directory if needed; absolute paths in original receipts identify the accepted source location, not portable data already bundled here.

## Verification

Verify every payload SHA256 against [MANIFEST.json](MANIFEST.json), and the ZIP SHA256/readback against [PUBLICATION.json](PUBLICATION.json). Verify the predecessor MANIFEST hashes and exact inherited source paths in DEPENDENCIES.json. Original accepted sources, tests, evidence and project documents retain their exact bytes. No collector, trading configuration, broker account or live model was changed.

The root publication receipt and project-guide alias are written after sealing and intentionally excluded from MANIFEST.json and the ZIP; both copies must equal the project publication receipt. The earlier root snapshots and package manifests retain their original dates and scope. No cloud synchronization was tested.
