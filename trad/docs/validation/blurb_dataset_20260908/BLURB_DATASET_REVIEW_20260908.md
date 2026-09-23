# Retained blurb and movement-factor datasets

Read-only inventory observed September 8, 2026 UTC. The user is correct: **a substantial blurb/news-attribution dataset exists separately from the older wide models' constant macro columns.** The earlier finding about those particular models does not mean the project had no blurb dataset.

| Actual artifact | Directly verified contents |
| --- | --- |
| `C:/Users/zmoor/OneDrive/thevault/PROJECT_COMMONS/artifacts/FOREX_NEWS_BLURB_RESEARCH_RECOVERED.zip` | 119,040 bytes, SHA256 `f0b79f7f1f58084573541c428d68b929041d890b7e7f6bdf4db4527b64210903`; 14 files: eight Python research builders, a worked XLSX example and supporting documentation/manifest. All ZIP members passed CRC checks. It is a recovered source package, not the later bulk dataset. |
| `C:/Users/zmoor/Documents/forex/trad/data/oanda_training_manager/news_event_tags/significant_move_news_tags.csv` | **7,048 movement-linked rows across 68 pairs**, 2,905,992 bytes. Dates June 24, 2024–July 2, 2026. **1,095** retained primary headlines/source URLs. Actual headers and file hash are recorded. |
| `C:/Users/zmoor/Documents/forex/trad/data/oanda_training_manager/state/spike_blurb_factor_reconstruction_v1.sqlite` | 405,794,816 bytes. **41,454 movement candidates**, **6,465 distinct market-episode IDs**, **2,935 source attributions and 2,935 factor observations**, **265,510 movement-factor links**. Also retains 7,048 legacy attribution cases with reacquired price windows and 2,316 factor-response observations. |
| `C:/Users/zmoor/Documents/forex/trad/data/oanda_training_manager/state/movement_news_episode_research_v1.sqlite` | 92,176,384 bytes. **8,387 episode rows across four retained contracts** and **170,355 source links**. Contract cohorts overlap; these rows must not be summed as independent market events. |

The recovered builders explicitly support the sequence **abnormal price move → source-backed entry/exit explanation → macro factor tags → historical analog retrieval**. The later SQLite implementation is real, with clocks, source/provenance hashes, executable prices, costs, response paths and separate attribution/entry-eligibility fields. The 6,465 count is the reconstruction table's distinct market-episode count, not its row count and not a count of successful forecasts.

## Timing and causal-use distinctions

The factor ledger spans 21 currencies. Its 2,935 factors retain publication, first-seen and known-at clocks, with no missing known-at or provenance-hash fields in this query. Stored known-at times span July 28–August 19, 2026, while publication dates extend back to 2001. Publication alone therefore cannot establish that the bot knew an old article at the historical entry.

The ledger labels **2,762** factors `point_in_time_observed` and **173** `historical_backfill_only`. These are retained classifications, not fresh causal certifications. **2,754** factors are `semantic_model_score_only`, **147** are official rate-curve level/shape, and **34** are actual-minus-latest-known-prior measurements. Currency direction is explicitly unresolved or hypothesis-only; none is forecast-proof or execution eligible.

The reconstruction's 265,510 movement-factor links distinguish:

- **101,107** pre-entry causal-labelled links, with decision-time eligibility recorded.
- **56,494** during-move links.
- **78,254** articles published before entry but observed late.
- **29,655** ex-post overlaps.

Every one of these movement-linked rows remains forecast-proof ineligible. Movement selection used the subsequently observed move; many rows share a source, currency factor or market episode. Link counts are not independent predictions.

The **7,048 legacy attribution cases** break down as **161 candidate pre-entry but unverified**, **177 early-move confirmation**, **23 ex-post explanation**, **734 stale context**, and **5,953 unmatched/unknown**. Their original “predictive” flag also remains set on 734 stale-context cases, illustrating why that legacy flag cannot be treated as a current validation result. All cases explicitly remain outcome-selected and forecast-proof ineligible. Price coverage is exact for 7,002 and partial for 46.

The separate movement-news database records 129,773 pre-entry causal-labelled source links and 40,582 in-window ex-post links. Its four retained contracts preserve their own creation times, source-event high-water marks, code/config hashes and price-manifest hashes. Those are separate retained cohorts, not a single new live forecast stream.

## Existing analog machinery and what it achieved

The factor-response table contains **1,042 v1** and **1,274 v2** observations at 1–120-minute horizons. Only 13 v1 and 23 v2 rows had any earlier mature analog; **both cohorts contain zero nonzero prequential prediction signs and zero scored prequential predictions**. This confirms existing analog machinery and retained response data, while giving no basis to claim that it produced validated forecasts. No models or outcomes were recomputed in this audit.

The blurb corpus is therefore a real reusable research input. Its existence does **not** establish that the 227/220 model learned news from its constant historical macro fields, or that the 795 model consumed news columns absent from its registry. Those earlier, model-specific findings still hold. Connecting this corpus requires preserving actual availability, removing post-entry/late evidence from entry-time features, and evaluating contributions on an explicitly later cohort.

Machine evidence: `BLURB_DATASET_INVENTORY_20260908.json`, SHA256 **`366483a10878352a78d737c851329c012abff38ac9e3bca2e4610daf30b93641`**. The record contains exact fixed SQL, timestamped read-only transaction results, result/schema hashes, legacy CSV headers/file hash, ZIP member hashes and ten source/config/audit bindings. Database files were not fully hashed or copied; result hashes bind the returned observations, not all external raw content. SQLite connections used `mode=ro` and `PRAGMA query_only=ON`, closed after rollback. No source/runtime/DB writes, model deserialization, training, broker requests or orders occurred.
