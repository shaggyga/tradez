# Existing Forex features and horizon curves: completed audit

Recorded September 8, 2026 UTC (September 7 evening in New York). Canonical project: `C:/Users/zmoor/Documents/forex/trad`. Historical source and models: `D:/forex/trad`. Additional unified research artifacts: `C:/Users/zmoor/AppData/Local/ForexResearchData/unified_intrahour_v1`.

**The broad feature models and horizon curves already exist. The earlier review was incomplete because it treated the current H1 study as the whole project.** This report corrects that scope without rewriting the earlier dated evidence. The appropriate next work is to reconcile and repair existing implementations, then measure their contribution alongside news.

## What exists

| Existing implementation | Actual inputs and horizons | Verified retained evidence |
|---|---|---|
| Original higher-timeframe curve | 227 fields, M30/H1/H4 inputs, nominal two-hour target | Real ten-fold experiment and validation reports on D. Later audits found timing and target defects. |
| Corrected feature family | 220 safe source fields; two-hour forecast candidates add interactions, pair/currency identities and calendar terms, up to 267 candidate fields | Direction and cost-survival fitted bundles exist on both C and D. All four copies match their saved manifest hashes. Candidate selection can reduce the columns actually consumed. |
| Unified intrahour v4 | **795 numeric inputs** plus three categorical identities; **1/2/3/5/10/15/30/60-minute** outputs | 205,029-row matrix over 68 pairs and a 500,050-byte fitted model in AppData. Actual model hash matches validation. |
| Moving-average grid | Up to **643 fields**; 27 input timeframes and 26 horizons, from seconds to one day | **610 fitted cells**, 92 unsupported. D model hash matches its retained report; expected C artifact is absent. |
| Second-ridge curves | 14 numeric inputs, **13 horizons from 15 seconds to four hours** | 871 pair-specific parameter surfaces plus 13 pooled proxies on D. The 2,652 fast/balanced/strict profiles are configurations, not separately fitted models. |
| Direct currency panel | 223 final columns per horizon, including technical, nonlinear, currency and calendar terms | Retained M5/M15/M30/H1/H2 fitted-result reports on D. |
| Current combined H1 study | **34 numeric inputs:** 24 price/time/missingness, eight captured-news fields and two price/news interactions | Separate live research implementation. It does not consume the older wide-feature matrix or its cross-pair strength fields. |

The vault's 251-column design catalogue is another count: a specification of 22 blocks and 95 model terms. Likewise, 208/209 strategy lanes are configurations. Neither number replaces the implemented feature and model counts above.

## Material findings

**1. The existing engine is disconnected from the current checkout.** All 36 Python modules under `fresh_m1_intrahour/src` are absent on C. They exist on D and match a retained August 3 checkpoint member-for-member. C's unified producer imports that missing package. Expected C moving-average and second-ridge artifacts are also missing. The current H1 research path is separate, so its narrower capacity does not describe the historical project.

**2. The source archive is incomplete for this engine.** C's `.gitignore:48` ignores the entire `fresh_m1_intrahour/` directory. The Git-based exporter therefore omitted that package from all 19 source archives inspected at audit time. Their hashes can be valid while the engine is absent. The unique fitted unified model in AppData was absent from 64 explicitly inventoried ZIPs. This is a bounded backup finding, not a search of every possible device. This audit's evidence package does not restore those sources or weights.

**3. The 795 inputs are real, but five windows are duplicated.** The matrix contains 771 multitimeframe technical fields, 20 cross-currency fields and four calendar fields. All 795 are present, nonempty and nonconstant historically; 585 have some nulls, with a maximum of 6.35%. All five nominal 15-minute cross-currency columns equal their one-minute counterparts across every one of the 205,029 rows. The current D builder falls back to one-minute returns when a requested window is absent. The duplicate data are directly proven; attributing their historical creation to that exact current code is an inference because the saved matrix/model are v4 and current D source is v5.

**4. Historical news was not actually learned in the older wide models.** Across every row group of the 3,197,854-row technical dataset and 693,254-row H1 dataset, nine macro fields are zero and the age field is 999; two derived macro interactions are also zero. The builder supplies these neutral defaults. The 795-field registry contains no news/sentiment fields. Existing technical and cross-currency interactions are implemented, but those constant macro columns cannot demonstrate a learned news effect. The current 34-input study does combine captured prices and news independently; its improvement still needs its own outcomes.

**5. Old validation does not justify simply activating the models.** The original 227-field experiment reported mean AUC 0.62543 and a passing historical gate. Later retained audits identified completed-bar lookahead, full-history volatility metadata and a path bug: nominal 120-minute targets spanned 12/24/96 hours on M30/H1/H4. The corrected 220-field rebuild removed seven unsafe fields and repaired exact timing, but its selected policy remained no-trade after validation. Previously inspected 2026 diagnostics cannot now be called untouched holdout data.

**6. Predicting movement size was stronger than predicting direction.** The retained unified model's final mean direction accuracy across eight horizons was **49.416%**; its mean MAE relative to no change was **1.000914**, slightly worse than that baseline. Both forecast and after-cost gates failed. The separate two-hour direction model had development AUC **0.5215**, while its cost-survival model had **0.6822**. Cost survival predicts whether either side's move can overcome costs, not which side to buy. A two-stage filter reached 53.76% development direction accuracy on only 10.97% of rows, with its directional gate still false. These are retained historical metrics, not new scores or current account returns.

**7. Curve evaluation and availability should be separated carefully.** The MA grid has no cell that passed both validation and holdout; its M1/H1 holdout direction rate was 49.28%, with only 22.94% of cost observations using exact bid/ask rather than the recorded spread proxy. Second-ridge has 87 individually passing historical profiles, although all 13 horizon-wide after-cost averages were negative; overlapping profiles are not independent successes. Unified allocation ranks raw native pips across pairs, which can favor exotic pip scales. Those pips are not comparable dollar P/L. Each pair's own valid horizon endpoints should retain their own clocks and missing-input reasons; an observed historical move census or a best-pair-per-horizon matrix is not one pair's forecast path.

The dormant C wide-feature snapshot is dated September 5, with zero accepted instruments: all 68 quotes were rejected as nontradeable with September 4 timestamps. That explains that historical snapshot, not current market health. Its legacy publisher and old forecast workers are excluded by the research supervisor. The standalone unified shadow producer also requires a passing forecast-validation receipt; its shadow-emission gate does not separately require the after-cost gate. Execution authority remains separate.

## Corrected improvement order

1. Reconcile C, D and AppData into a documented, recoverable source/artifact inventory. Recover a source version compatible with each fitted schema; fix source-export coverage for actual code. Retain old weights and reports unchanged.
2. Repair the duplicate cross-currency windows and cross-pair native-pip ranking in a new version. Reuse the existing technical builders, currency interactions, per-horizon labels and cost-survival comparisons.
3. Join actual point-in-time news to those features, recording missing news explicitly. Compare technical-only, news-only and combined variants on the same eligible observations, without substituting retrospective news or constant placeholders.
4. Reuse the causal forecast ledger for per-pair, per-horizon publication and original-target outcomes. Use time-blocked evaluation, cost stress and uncertainty that reflect overlapping horizons and correlated pairs. A valid endpoint need not wait for every other horizon, but missing prices must not be invented.

No model was trained, deserialized, activated, rescored or restored in this audit. No broker orders, runtime reloads, registrations or forecast/outcome ledgers were changed. Only audit records, navigation, the pending-work correction and vault export mappings were updated. A read-only MA feature-name function verified report counts without loading the fitted model. This is an implementation, artifact and retained-validation audit, not proof of current prediction improvement.

## Detailed evidence

- [Model validation and artifact hashes](C:/Users/zmoor/Documents/forex/trad/docs/validation/existing_feature_horizon_20260908/EXISTING_TRAINED_HORIZON_MODELS_REVIEW_20260908.md)
- [Feature lineage and populated-data checks](C:/Users/zmoor/Documents/forex/trad/docs/validation/existing_feature_horizon_20260908/FEATURE_IMPLEMENTATION_LINEAGE_REVIEW_20260908.md)
- [Source, vault and runtime integration](C:/Users/zmoor/Documents/forex/trad/docs/validation/existing_feature_horizon_20260908/SOURCE_VAULT_RUNTIME_REVIEW_20260908.md)
- [MA grid evidence](C:/Users/zmoor/Documents/forex/trad/docs/validation/existing_feature_horizon_20260908/MA_GRID_EXISTING_CURVE_AUDIT_20260908.json)
- [Audit validation and preserved-source bindings](C:/Users/zmoor/Documents/forex/trad/FOREX_EXISTING_FEATURE_HORIZON_AUDIT_VALIDATION_20260908.json)

The evidence directory contains exact copies of the audit reports, JSON records and capture scripts. Large source datasets and fitted models remain at their inventoried local paths. Parquet population checks use footer metadata and the ten directly compared cross columns; they are not a full recomputation of all historical features. Earlier horizon-coverage reports remain immutable dated history and are superseded only in project-wide scope by this correction.
