# Blurb dataset: audit addendum

Observed September 8, 2026 UTC, September 7 evening in New York. This adds a separate existing news/factor research lineage to the preceding feature/horizon audit. **The project does contain historical blurb and movement-linked news data.** The finding that some older model matrices contained constant macro fields must not be read as absence of news research across the project.

## Actual retained data

| Artifact | Directly checked contents |
|---|---|
| `data/oanda_training_manager/news_event_tags/significant_move_news_tags.csv` | **7,048 rows across 68 pairs**, covering movements from June 2024 through July 2026. There are **1,095 retained headline/source-URL matches** and 5,953 unmatched moves. Actual SHA matches the retained legacy contract. |
| `data/oanda_training_manager/state/spike_blurb_factor_reconstruction_v1.sqlite` | **405,794,816 bytes**. Contains 41,454 movement candidates representing **6,465 distinct market episodes**, **2,935 source factors**, and 265,510 factor/movement links, plus later response and official-source research tables. Candidate rows and links are not independent news events. |
| `data/oanda_training_manager/state/movement_news_episode_research_v1.sqlite` | **92,176,384 bytes**. Contains 8,387 movement rows across four contracts and 170,355 links. This is a separate retained movement/news research database. |
| `FOREX_NEWS_BLURB_RESEARCH_RECOVERED.zip` | Actual 119,040-byte vault archive matches SHA `f0b79f7f1f58084573541c428d68b929041d890b7e7f6bdf4db4527b64210903`. Its 14 files include eight Python programs and a worked EUR/USD workbook. The bulk generated blurb CSV/JSONL corpus is absent from this ZIP; that does not mean the separate datasets above are absent. |

The database and CSV paths above are relative to `C:/Users/zmoor/Documents/forex/trad`. The recovered ZIP is at `C:/Users/zmoor/OneDrive/thevault/PROJECT_COMMONS/artifacts/FOREX_NEWS_BLURB_RESEARCH_RECOVERED.zip`.

The original design was movement-first: find abnormal price episodes, collect entry/exit news explanations, extract macro factors, then retrieve similar historical responses. This supplies more than a generic sentiment score. It also selects episodes using known movements, so a successful explanation is not automatically an advance prediction.

## What reached prediction

The legacy matches divide into **161 pre-entry candidates, 177 early-confirmation cases, 734 stale-context cases and 23 post-hoc explanations**. The pre-entry ledger cases remain unverified for causal use; all legacy cases are proof-ineligible. The reconstruction distinguishes source clocks, backfills, factors and episode relations. A stored point-in-time label alone is not independent proof of historical availability or profit.

The two retained analog versions contain **2,316 factor-response rows but zero nonzero prequential predictions and zero scored analog predictions**. The narrative meter explicitly registers `recovered_blurb_analog_v1` as `inactive_no_prequential_orientation_rows`, and its implementation emits `0.0`. Its exact-source/factor/relevance matching lacked sufficient earlier, already-matured examples. This is an actual connection and evidence-coverage gap: the data exists, but that analog arm was not supplying forecasts.

Separate historical code did evaluate news direction, currency response and news-plus-technical arms. Therefore it would also be wrong to say combining news and technical information had never been attempted. Those comparisons and their retained limitations are documented in the accompanying model-use trace. They do not establish that the 227/220/795 fitted matrices or the current 34-input H1 study consumed this blurb corpus.

The earlier feature findings remain specific: historical macro columns in the audited 227/220-family datasets were constant, and the 795 registry had no news columns. Existing blurb data should be assessed as a separate source for event-response and analog features, with actual entry-time availability checked. Post-move explanations remain useful for discovery and labelling. A future combined test should retain matched technical-only and news/analog comparisons and evaluate original future outcomes; this audit did not train or enable such a model.

## Evidence and scope

- [Dataset inventory and timing checks](C:/Users/zmoor/Documents/forex/trad/docs/validation/blurb_dataset_20260908/BLURB_DATASET_REVIEW_20260908.md)
- [Machine-readable inventory](C:/Users/zmoor/Documents/forex/trad/docs/validation/blurb_dataset_20260908/BLURB_DATASET_INVENTORY_20260908.json)
- [Model-use trace](C:/Users/zmoor/Documents/forex/trad/docs/validation/blurb_dataset_20260908/BLURB_MODEL_USE_REVIEW_20260908.md)

SQLite queries were read-only and bounded; no database, registration, model, runtime or broker state was changed. Source and small-artifact hashes were checked without loading serialized models. Database sizes and query observations are dated; they are not claims that every database byte was snapshotted. The preceding feature/horizon report and validation receipt remain unchanged. This addendum and its exact evidence copies supplement them.
