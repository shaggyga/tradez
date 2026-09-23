# Historical rolling training windows — September 15, 2026

This is the next data package after the [shared rolling technical collector](FOREX_ROLLING_TECHNICAL_DATASET_20260915.md). It builds real date ranges from the original archive, not the earlier prefix/tail samples. The active collector and trading configurations are unchanged.

## Completed materialization and input review

The actual eight-week build completed with **3,645,778 original pair/minute rows across all 68 pairs**, in 544 pair/week core partitions plus 544 matching peer sidecars. Recorded output is 4,245,831,410 bytes (3.95 GiB); the build took 1,179.266 seconds. All saved tables passed complete Arrow readback, finite float64 bit comparison and missing-mask checks. Output: `data/rolling_technical_training_20260915_v1`.

The training-only diagnostic read all **2,734,524 training origins** and used 91,288 deterministic original-clock 30-minute samples for correlation. All 228 registered inputs have some finite variation in the full training population; none was globally constant or entirely missing. **42 input pairs involving 52 distinct fields** met absolute correlation 0.995 with at least 1,000 joint samples after within-pair scaling. These are 42 relationships, not 42 independent fields to delete. Return/mean-return/SMA-slope identities and near-equivalent unit transformations dominate the strongest flags. Formula aliases excluded in the earlier registry and these empirical overlaps are different kinds of redundancy.

No correlation-based inputs were dropped. Availability also matters: the mean training availability across trend fields was 69.56%, versus 83.36% for peers and 97.69% for OHLC fields. These are family averages, not percentages of complete feature vectors. A model must retain explicit missingness and its declared support policy; all 68 having rows does not mean all 228 values exist in every row.

Detailed input counts, correlation pairs and recreation metadata are in [the training diagnostic](validation/rolling_training_windows_20260915/TRAINING_DIAGNOSTIC.md). No model was fitted, no predictive feature-to-outcome correlations were measured and no profitability improvement is claimed by this package.

## Population and boundaries

| Role | Original minute-start interval, UTC |
|---|---|
| Training | July 13, 2026 inclusive to August 24 exclusive |
| Validation | August 24 inclusive to August 31 exclusive |
| Later development test | August 31 inclusive to September 7 exclusive |

Retained origin counts are 2,734,524 training, 455,240 validation and 456,014 later development test. The independent [all-partition readback](validation/rolling_training_windows_20260915/DATASET_READBACK.json) checked every pair/week through the actual loader, with zero violations across 207 integrity and numerical-consistency check categories. This is not 207 statistical discoveries or a reconstruction of all raw candles.

All 68 pairs are included. These dates were examined in earlier research. The later block is a chronological development comparison, **not untouched confirmation**. No new model is fitted or promoted by this build.

The range reader uses each pair's original manifest cutoff: reacquired Parquet through that cutoff inclusively, then canonical CSV strictly afterward. July 24 cutoffs differ by pair. A 602-minute input prefix supports the finite technical calculator; an extra hour after the origin window supplies the longest outcomes. Neither becomes additional training origins.

Each requested origin remains a row, including quiet markets, missing features and absent future targets. No future outcome is used to choose the origin population. Targets have separate clock-coverage states and numerical-validity flags. For each horizon, split eligibility requires its terminal candle **end strictly before** the origin's split boundary. Future labels cannot cross from training into validation or from validation into the later test.

Closed historical query coverage is explicit. An absent target inside the completed range is missing, even when it is later than the pair's last observed candle; it is not an indefinite wait for a bar. Numerical bid/ask validity and midpoint validity are separate from time coverage.

## Storage and use

- Builder: [tools/build_rolling_technical_dataset_v1.py](../tools/build_rolling_technical_dataset_v1.py).
- Primary range reader: [oanda_rolling_technical_ranges_v1.py](../oanda_rolling_technical_ranges_v1.py).
- Outcomes: [oanda_rolling_technical_labels_v1.py](../oanda_rolling_technical_labels_v1.py).
- Exact-clock batch peers: [oanda_rolling_technical_panel_batch_v1.py](../oanda_rolling_technical_panel_batch_v1.py).
- Partition loader: [oanda_rolling_technical_dataset_v1.py](../oanda_rolling_technical_dataset_v1.py).

Each pair/week has a core Parquet shard and a smaller peer sidecar. Their original minute keys must match exactly; absent origins are never fabricated to fill a calendar. Sidecars avoid rewriting or duplicating all 216 core fields. The combined registered feature list contains 216 technical plus 12 peer columns. Support counts, pair/time metadata and all future labels are outside that list.

The loader verifies file hashes and original keys, then returns a bounded Arrow table. **The table also contains labels and metadata.** A fit must explicitly select `DATASET.json.feature_names` or a declared training-only subset, never all numeric columns. The loader preserves rows with invalid labels; fitting and scoring must separately select the appropriate label-validity and split-maturity masks without redefining which origins receive predictions.

The builder reads back every written table and checks all finite float64 bits and null positions. Inputs, schemas, metadata, code and source receipts are identified in `DATASET.json`. A dataset becomes `complete` only after both parts of every partition and metadata/source checks finish. Failed or building directories are not admitted by the loader.

The output budget is 12 GiB, with a 32-GiB reserve on C. This is a bounded eight-week materialization, not the complete approximately 53.5-million-row archive. No raw price archive is copied. Historical availability assumes original bar end; the actual original provider publication delays cannot be recreated from these files. Outcome net values include observed endpoint bid/ask spreads, with additional slippage/financing still requiring an explicit later test.

## Endpoint returns and complete paths

The original `label__*` outcomes require every intervening minute. That support is appropriate for the declared path and close-excursion measurements, but is unnecessarily restrictive for an endpoint return when both exact endpoints exist. The separate [endpoint calculator](../oanda_rolling_technical_endpoint_labels_v1.py) and [overlay builder](../tools/build_rolling_technical_endpoints_v1.py) therefore add `endpoint_label__*` return, direction and bid/ask net proxies without modifying the base dataset or its strict labels.

The overlay rereads the same source ranges at the recorded observation cutoff and verifies normalized-array and selected-source-row hashes against the base receipts. Every finite strict endpoint number must match its overlay counterpart bit-for-bit. Original rows, target clocks and split eligibility remain identical. Full-path outcomes and endpoint-only outcomes remain separately identifiable; later comparisons must show the shared complete-path cohort and the added endpoint-only cohort separately.

An endpoint-labelled row does not prove intermediate quotes, continuous tradability or an executable fill existed. Future endpoint availability is a training/scoring label mask, never a feature or a condition for retrospectively deciding which origins should have received forecasts. Both loaders return labels alongside inputs; fit only the explicit base feature allowlist.

The completed overlay covers the exact same 3,645,778 origins in 544 partitions, adds 482,318,552 bytes (0.45 GiB), and took 232.187 seconds with three bounded workers. Source rereads matched all 68 original receipts. It verified 60,962,440 already-finite strict numerical cells bit-for-bit against the new endpoint values.

| Horizon | Complete-path valid returns | Exact-endpoint valid returns | Additional endpoint-only returns |
|---|---:|---:|---:|
| 5 minutes | 3,377,841 | 3,545,373 | 167,532 |
| 15 minutes | 3,148,861 | 3,534,241 | 385,380 |
| 30 minutes | 2,954,911 | 3,523,319 | 568,408 |
| 60 minutes | 2,710,875 | 3,504,026 | 793,151 |

These counts cover all three development splits before boundary purging; they are not independent trials or trades. The one-hour exact-endpoint count after split-boundary purging is 3,493,082. Bid/ask endpoint validity matched midpoint validity in this actual dataset, but the flags remain separate in the schema for other populations.

The [focused test receipt](validation/rolling_training_windows_20260915/TESTS.json) records **92 tests and 30 subtests passed** across the new range reader, label calculators, historical peer batch, dataset/overlay loaders, builder integration and training diagnostic. This is a scoped data-package acceptance, not an entire-project health claim.

The independent [endpoint readback](validation/rolling_training_windows_20260915/ENDPOINT_READBACK.json) passed all 544 actual joined partitions in 47.781 seconds with zero violations. It verified all 68 source receipt identities, unchanged base metadata, original split populations and all 60,962,440 finite strict numerical cells. The [acceptance manifest](validation/rolling_training_windows_20260915/ACCEPTANCE.json) binds the completed artifacts, source and tests.

## Feature selection boundary

Full-training finite counts, missing counts, minima and maxima are measured without labels or later periods. Only fields that are entirely missing or exactly constant over the full training population can be excluded by that basic candidate list. Sampled correlations are separate diagnostics, not automatic feature drops or evidence of predictive value.

Any near-redundancy analysis uses original-clock, 30-minute samples from training only, with centering/scaling within each pair. This reduces price-level artifacts when pooling instruments but does not remove regime dependence. Meaningful family ablations still require later matched predictions and costs.

## Closest completed comparisons

Do not treat boosting, peer features or conditional models as untried approaches.

| Existing work | Relevant finding and limitation |
|---|---|
| Compact archive Ridge/HGB, 24 technical versus 99 technical/peer inputs | 32 of 36 cells negative, two positive, two inactive. Peers worsened MAE in 18 of 18 matched comparisons. Only seven compact fields varied under that study's strict support rules. |
| Richer close/clock HGB, 655 inputs | Five of six selected means negative. The H1 2026 cell selected 324 of 186,582 rows and averaged +1.1129 bps after bid/ask plus 1 bp. It remained +0.1129 bps at 2 bp extra cost and +0.8330 bps with one-minute delayed entry. Its weakness was concentration: July 31 supplied more than the entire cell's net sum; the other dates combined were negative. Do not incorrectly describe this cell as failing the 2-bp cost check. |
| Short technical/peer/news direction study | All 32 learned group/horizon mean returns negative, despite some direction rates above 52%. |
| Conditional direction/magnitude/cost-head study | Already tested the probability-times-conditional-gain/loss decomposition. Seventeen of 24 means negative, four positive and three inactive; positive subsets were small or unstable, and every arm lost to zero-return MAE. |

Sources: [compact comparison](FOREX_ARCHIVE_DIRECTION_COMPARISON_20260912.md), [model reuse register](FOREX_MODEL_REUSE_REGISTER_20260911.md), and the canonical external research records `revamp_8h_20260912/direction_richer_archive_003/results_concentration_004/DESCRIPTIVE_APPENDIX_001.md`, `direction_research_20260911/DIRECTION_RESEARCH_REPORT.md`, and `direction_decision_20260911/DIRECTION_DECISION_REPORT.md` under the parent Forex folder. These are existing results, not measurements produced by this data build.

The material change available for the next comparison is the shared rolling **OHLC, activity and spread** representation, correct horizon-specific peers, and explicit support masks. The compact and close-only expansions above did not use that exact contract. Initial controls should remain simple and matched: no-change/no-trade, train-only pair mean/prevalence, momentum/reversal, additive Ridge/logistic and one fixed HGB recipe. Compare local 216 versus local plus 12 peers on the same origins. More elaborate conditional heads should follow only if those measurements justify another materially different test.

ARIMA(1,1,0) should also remain a fixed comparator, with freshly aligned predictions. The retained July 11 corrected-v3 record covers 15 pairs and 47,580 final H1 forecast rows: 49.651% direction, with RMSE 11.943194 versus 11.940490 for no change. Those hourly open/close labels and costs differ from this M1 close-to-close contract, so the scores are not a current ranking. Its mathematical recipe is retained in `fresh_m1_intrahour/mandatory_model_validation_worker.py`. The later `oanda_arima_h1_baseline_grid.py` wrapper has an additional review finding: it forecasts from the preceding row while scoring the current row's shifted outcome, compresses missing clocks, and uses an origin-spread/median cost approximation. Recreate the baseline with the corrected origins, exact targets and declared costs; do not rerun that wrapper unchanged or silently rewrite its old scores.

## Recreation

Run from `C:\Users\zmoor\Documents\forex\trad` using the existing timeseries312 environment:

```powershell
& 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe' -B tools/build_rolling_technical_dataset_v1.py --manifest ../revamp_8h_20260912/data/PRICE_ONLY_POPULATION_MANIFEST_001.json --output data/rolling_technical_training_20260915_v1

& 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe' -B tools/analyze_rolling_technical_training_v1.py --dataset data/rolling_technical_training_20260915_v1 --output data/rolling_technical_training_diagnostics_20260915_v1

& 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe' -B tools/build_rolling_technical_endpoints_v1.py --base data/rolling_technical_training_20260915_v1 --output data/rolling_technical_endpoints_20260915_v1 --workers 3
```

Output directories must be new. Source/cost/missingness receipts and completed partition counts determine acceptance; a successful command launch alone does not.
