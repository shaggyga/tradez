# Rolling model comparison — September 15 supplement

This separately sealed supplement preserves the fixed historical model comparison that follows the [eight-week rolling training dataset](../ROLLING_TRAINING_WINDOWS_20260915/README.md). It adds saved model weights, their training-only normalizers and the result/audit records. Earlier vault packages retain their dates and hashes.

The actual project remains `C:\Users\zmoor\Documents\forex\trad`. This is a research/recreation package. It does not activate the models, change trading settings or contain broker credentials.

## What this comparison covers

The accepted input population contains 68 pairs and 3,645,778 original minute origins for July 13–September 7, 2026. The comparison uses 1,094,794 prepared rows: 183,540 original-clock training samples and all 911,254 assessment origins. Per-pair normalization uses the full training inputs before that sampling.

There are 32 fixed learned fits: Ridge and histogram gradient boosting across four input groups and 5/15/30/60-minute horizons. Twenty horizon-specific controls preserve no-change, training mean, momentum, reversal and the newly aligned conditional-OLS ARIMA comparator. These are previously examined development periods, not an untouched confirmation set; multiple cells are not independent trials. No model is promoted by this package.

Start with the [completed guide](source/forex/trad/docs/FOREX_ROLLING_MODEL_COMPARISON_20260915.md), [complete comparison table](source/forex/trad/docs/validation/rolling_model_comparison_20260915/summary/README.md), [structured summary](source/forex/trad/docs/validation/rolling_model_comparison_20260915/summary/COMPARISON_SUMMARY.json), [acceptance](source/forex/trad/docs/validation/rolling_model_comparison_20260915/ACCEPTANCE.json), and [independent audit](source/forex/trad/docs/validation/rolling_model_comparison_20260915/INDEPENDENT_AUDIT.json). The [full results manifest](evidence/comparison/RESULTS.json) maps every model and compact metric record. Its forecast-Parquet paths remain external.

The metrics keep costs, delayed entry, missing outcomes, path support and concentration visible. Endpoint bps are descriptive equal-notional proxies, not account returns or broker fills. The final guide and summary provide the results and limitations; completed infrastructure or a direction score above 50% does not establish a trading edge.

## Saved-model recreation

All 32 saved estimators and four pair-prior records are under `evidence/comparison/models`. The [normalizer array bundle](source/forex/trad/docs/validation/rolling_model_comparison_20260915/NORMALIZERS.npz) and its [registry](source/forex/trad/docs/validation/rolling_model_comparison_20260915/NORMALIZERS.json) retain the exact count, mean, scale and support arrays for 68 pairs × 228 ordered fields. No feature rows or future labels are stored in that bundle. Current causal feature values are still required for inference.

The [dependency map](DEPENDENCIES.json) identifies ten exact technical-source prerequisites already preserved by the preceding training-window supplement. For an isolated recreation, first verify both packages, copy the preceding `source/forex` tree into a new empty directory, then overlay this package's `source/forex` tree there. This preserves project-relative imports and documentation links without changing either archived package. The [runtime record](RUNTIME.json) records compatible library versions. Apply the saved per-pair normalizer, the estimator's selected feature order and the recorded learner/pair-context transformation before prediction; the registry gives that sequence.

The corrected [version 2 quote manifest](evidence/quotes_v2/QUOTE_PANEL.json), [column registry](evidence/quotes_v2/QUOTE_COLUMN_REGISTRY.json) and 68 receipts retain source identity and per-pair ARIMA parameters. The partial first attempt is retained only as [invalidated provenance](evidence/invalidated_quotes_v1/INVALIDATION.json), with its original manifest and before-correction source; none of its 57 completed pair shards is included or accepted.

Full historical reconstruction still needs the external raw candle archives and accepted dataset shards, or an explicit rerun from those original inputs. Prepared row NPZs, quote/forecast Parquets and live databases are excluded. The compact metric files and passed independent-audit record are included; they do not replace the external prediction rows for a fresh row-level audit. Migrating original source paths requires a new explicit receipt rather than silently altering sealed hashes.

## Verify this package

The [ZIP](rolling_model_comparison_source_20260915.zip) contains the manifest and the same payload bytes as the readable tree. Verify its hash using [PUBLICATION.json](PUBLICATION.json), then each member against [MANIFEST.json](MANIFEST.json). The publication receipt and its `source/forex/trad/docs/validation/rolling_model_comparison_20260915/VAULT_PUBLICATION.json` alias are deliberately outside their own manifest and ZIP. Copy hashes, Python syntax and ZIP readback are checked without executing the archived trading project. Local verification does not establish OneDrive cloud synchronization.

[ASSEMBLY_STAGE.json](ASSEMBLY_STAGE.json) preserves the earlier staging inventory and then-pending work. Final MANIFEST/PUBLICATION records supersede its staging status. The [previous vault README](before/README.md) is preserved. Old shared vault inventories do not seal this supplement. Any copied broader project log, README or pending-work record retains its date and can reference material outside this scoped package.
