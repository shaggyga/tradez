# Rolling specialist checkpoint — September 15, 2026

This supplement preserves the completed, accepted specialist-combination comparison. It does not replace the earlier dated snapshots. All 68 pairs use the July 13–September 7 rolling dataset; these are already examined development periods.

Start with the [completed project guide](source/forex/trad/docs/FOREX_ROLLING_SPECIALISTS_20260915.md), [complete summary](source/forex/trad/docs/validation/rolling_specialists_20260915/summary/SUMMARY.md), and [component-baseline comparison](source/forex/trad/docs/validation/rolling_specialists_20260915/component_baselines/POSTHOC_COMPONENT_BASELINES.md). The [accepted receipt](source/forex/trad/docs/validation/rolling_specialists_20260915/ACCEPTANCE.json) binds the evidence; the [independent audit](source/forex/trad/docs/validation/rolling_specialists_20260915/INDEPENDENT_AUDIT.json) and 100 distinct tests verify the stated numerical/recreation scope.

The retained movement and exit-cost components beat specified TRAIN-only baselines in this slice. The combined entry policies did not establish repeated profitability: neither gate has a full-endpoint policy positive in both assessment periods. The four positive variant-period cells per gate and their concentration, cost, delay and unsuccessful periods remain in the summary. Numerical acceptance is not trading approval. Collection and trading settings were unchanged.

## Preserved assets

- 20 six-head bundles (120 individual base estimators), 16 combining models, four probability calibrators and two horizon prior files, all under [models](evidence/comparison/models).
- All 28 metrics, 28 component diagnostics and four probability-calibration diagnostics, plus [complete RESULTS](evidence/comparison/RESULTS.json).
- Exact [prefix normalizers](source/forex/trad/docs/validation/rolling_specialists_20260915/NORMALIZERS.json) and compact NPZ: five cutoffs × 68 pairs × 50 fields, with ordered names and source-array hashes. No raw feature rows are in that NPZ.
- The [68-pair input recipe](evidence/inputs/SPECIALIST_INPUTS.json), exact new source/tests, the exact inherited legacy helper, summary, audit and the dated [first-context case review](source/forex/trad/docs/validation/rolling_specialists_20260915/COMPACT38_30M_CASE_REVIEW.json).

## Recreation and dependencies

See [RECREATION.md](RECREATION.md), the hash-bound [dependency closure](DEPENDENCIES.json), and [runtime versions](RUNTIME.json). The earlier [model comparison](../ROLLING_MODEL_COMPARISON_20260915/README.md), [training-window package](../ROLLING_TRAINING_WINDOWS_20260915/README.md), and [signed-cost study](../SIGNED_COST_RESEARCH_20260912/README.md) remain intact. Eighteen exact inherited source files resolve into the first two packages; the legacy helper is additionally copied here at its required sibling path.

This is a compact research checkpoint, not a full raw-data backup. Raw prices, databases, credentials, prepared feature/label rows, OOF arrays and forecast Parquet are excluded. Full refit/evaluation requires the accepted local row datasets or their exact reconstruction; saved estimators and normalizers alone do not regenerate missing source data.

[MANIFEST.json](MANIFEST.json) enumerates payload hashes. The ZIP contains that payload and MANIFEST.json, excluding itself. [PUBLICATION.json](PUBLICATION.json) is a later external sealing receipt, as is its copied project-guide alias; those receipts are deliberately outside the manifest and ZIP to avoid circular hashes. [before/README.md](before/README.md) preserves the vault root README exactly before the new dated pointer. Local verification does not assert cloud synchronization.
