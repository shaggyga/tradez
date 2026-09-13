# Feature dictionary source metadata refresh, 9 September 2026

The dictionary now binds reviewed advisor manager `f07fb5dc4c1eb460b40bd1108636e1213b082c7c6ca6aa592998e8dcb8389faa`. Its only two advisor references belong to `atr_14_pips`: `atr_pips` moved from line 1942 to 1950, and the `candle_snapshot` ATR field from 1972 to 1980. Both complete function ASTs match the retained prior source exactly.

Only that source hash and two line references changed in the authored catalog. The existing builder regenerated JSON and Markdown, and `--check` passed. All other parsed generated-document values are unchanged; Markdown differs only at those two references. Existing dictionary tests passed: 34, no failures. This refresh changed no feature definition, model, manager source, registry, worker or runtime.

Exact before catalog, generated JSON/Markdown, builder/test sources and Sep8 validation remain under `before_source/`. The prior cited MD identity `340349f4c55628234fb354e97ab7d251c1e9182c0ed190cfa65e87a369315374` maps to retained before bytes. Original selection manifests and historical citations remain unchanged.

`FEATURE_DICTIONARY_VALIDATION_CURRENT.json` still maps to the dated Sep8 validation (`2026-09-08T02:28:00Z`) and its original hashes. Navigation must label it historical, not imply that it certifies the refreshed documents. Current metadata verification is at `docs/validation/overnight_curve_buildout_20260909/feature_dictionary_source_refresh_v1/FEATURE_DICTIONARY_METADATA_REFRESH_VALIDATION_20260909.json`. No mapper or publisher change was needed.

Current receipt SHA-256: `214e282531d0541445f35b692b991f17096b2f34ebf8c42e22b344fe0807a293`. This verifies documentation/source metadata, not predictive profitability or operational readiness.
