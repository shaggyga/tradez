# Forex — start here

The actual project is **`C:\Users\zmoor\Documents\forex\trad`**. This vault explains the system and contains readable audit records, source snapshots, history and mappings. You can understand and inspect the exported system without access to that computer. Exact historical reproduction or a fresh runtime audit may additionally require excluded databases, fitted artifacts and credentials; missing state must not be inferred or invented.

**Read in this order:** [system guide](SYSTEM_GUIDE.md) → [worked example](FEATURE_GENERATION_WORKED_EXAMPLE.md) → [feature dictionary](FEATURE_DICTIONARY_CURRENT.md) → [research index](RESEARCH_INDEX.md). [The knowledge index](KNOWLEDGE_INDEX.md) lists every canonical record and resolves source-layout references. [Recreation](RECREATION.md) explains inspect-only recovery and what is missing.

The latest narrative records describe research collection with orders and promotion disabled. That is dated evidence, not a fresh check of the computer. For a concrete retained runtime observation, see `runtime_verification.observed_utc` in [the combined-study receipt](JOINT_PRICE_NEWS_VALIDATION_CURRENT.json): September 7 at 22:48:10 UTC; its later news/forecast observation is separately timestamped. Do not infer permission to execute trades from a healthy feed, a forecast or a probability estimate.

## Current map

- `REVAMP_BASELINE_RECOVERY_CURRENT.md` and `REVAMP_BASELINE_RECOVERY_VALIDATION_CURRENT.json`: September 8 source/selected-database recovery, isolated second-ridge inference, fresh runtime blocker reproduction and original-outcome performance baseline. The D recovery checkpoint is local and scoped; it is not a complete vault database backup or live deployment.

- [Feature generation dictionary](FEATURE_DICTIONARY_CURRENT.md) and `FEATURE_DICTIONARY_CURRENT.json`: explanations and provenance for every entry in the 251-field design catalogue, the 34 current joint inputs, and the six older wide-feature schemas. Intended definitions, inspected calculations, unavailable inputs and missing archived source are explicitly different. [Worked example](FEATURE_GENERATION_WORKED_EXAMPLE.md) follows prices/news through actual feature math without inventing a forecast or trade.

  `FEATURE_DICTIONARY_VALIDATION_CURRENT.json` records documentation checks and source hashes. Read the short worked example first, then use the dictionary for individual fields. Recovery gaps are stated rather than represented as completed implementations.

- `BLURB_DATASET_AUDIT_CURRENT.md` and `BLURB_DATASET_AUDIT_VALIDATION_CURRENT.json`: actual retained movement/news CSV and factor databases, recovered source bundle, historical joint comparisons and inactive analog-input trace. Constant macro columns in one model family do not mean the project lacked blurb data.

- `EXISTING_FEATURE_HORIZON_AUDIT_CURRENT.md` and `EXISTING_FEATURE_HORIZON_AUDIT_VALIDATION_CURRENT.json`: completed audit of the existing 227/220/795/MA/second-ridge engines. The current H1 model is only one separate path. Older source/models remain at `D:/forex/trad` and `C:/Users/zmoor/AppData/Local/ForexResearchData/unified_intrahour_v1`. The ignored intrahour source package is absent from the current C checkout and inspected vault source archives; a valid archive hash does not establish a complete historical-engine backup.

- `ACTIVE_PIPELINE.md`: collectors → price/news inputs → forecast studies → dashboard → positions/results.
- `RESEARCH_INDEX.md`: active studies, baseline comparisons and retired experiments.
- `PAIR_FAMILY_REPAIR_CURRENT.md` and `PAIR_FAMILY_REPAIR_VALIDATION_CURRENT.json`: sparse-minute coverage repair, independent family ledgers, live evidence and limitations.
- `JOINT_PRICE_NEWS_CURRENT.md` and `JOINT_PRICE_NEWS_VALIDATION_CURRENT.json`: joint-model implementation and its actual activation/validation state.
- `CURRENT_NEWS_AUDIT_AND_REPAIR.md` and `NEWS_CAUSAL_AGGREGATION_VALIDATION.json`: the audited false corroboration example, member admission fix and current-news verification.
- `source/WORKTREE_SOURCE_LATEST.json`: latest verified source archive; its manifest and hash establish exactly what was exported.

The [shared-state manifest](SHARED_PROJECT_STATE_CURRENT.json) maps vault aliases to original project paths and hashes. Its `source` paths include `trad/`; ZIP member paths do not. The actual project README links source counterparts. Earlier reports retain their dated observations. The previous audit guide is preserved at ZIP member `docs/AUDIT_GUIDE_HISTORY_THROUGH_20260907.md`.

The machine-local dashboard is [Signals vs live](http://127.0.0.1:8765/#oanda); it is not hosted by this vault. A current quote is not a forecast; a published forecast is not measured success; a research quote-entry record is not an executed position. A source export timestamp cannot make old account or worker counts current.

The recorded scheduled bot-health automation is paused. The one-hour watch's [report](LIVE_WATCH_REPORT_20260907.md) and [supporting summary](LIVE_WATCH_SUMMARY_20260907.json) are retained, not current telemetry. [The readability check](VAULT_READABILITY_REPORT.json) verifies document/JSON readability, hashes and entrypoint links; it lists external/unresolved references without reading them.
