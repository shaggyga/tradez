# Forex research project

Canonical project: `C:\Users\zmoor\Documents\forex\trad`.

**Vault-independent orientation:** [the system guide](docs/VAULT_SYSTEM_GUIDE.md) explains the full flow, evidence boundaries and offline reconstruction. Its links target the vault aliases; [the local research index](docs/RESEARCH_INDEX.md) uses source-layout links. Vault `KNOWLEDGE_INDEX.md` and `VAULT_READABILITY_REPORT.json` are generated after record/source export by `tools/audit_forex_vault_readability.py`.

**How features are generated:** [the explanatory feature dictionary](docs/FOREX_FEATURE_DICTIONARY_CURRENT.md) separates the 251-entry design catalogue, six historical model schemas and the current 34-input price/news calculation. It includes formulas or explicitly unverified definitions, sources, units, lookbacks, missingness, timing and implementation status. Start with [the worked example](docs/feature_dictionary/active_joint_walkthrough.md); do not infer 251 active predictors from an inventory count.

[Open the live dashboard](http://127.0.0.1:8765/#oanda) · [Active pipeline](docs/ACTIVE_PIPELINE.md) · [Audit and research index](docs/RESEARCH_INDEX.md)

**Existing model inventory corrected September 8 UTC:** the project already has 227/220-feature models, a 795-input eight-horizon curve, a 643-feature MA grid and second-ridge curves. The current 34-input H1 study is a separate narrow path. Read [the completed implementation and validation audit](docs/FOREX_EXISTING_FEATURE_HORIZON_AUDIT_20260908.md) before proposing another curve. Some older source/models remain on D and in AppData; the vault source archive omits the ignored intrahour source package.

The [blurb dataset audit addendum](docs/FOREX_BLURB_DATASET_AUDIT_20260908.md) adds the retained 7,048-row movement/news dataset, 2,935-factor reconstruction and earlier news/technical comparisons. These are separate from the constant macro columns found in specific older model matrices.

**Revamp baseline completed September 8:** [baseline and recovery](docs/FOREX_REVAMP_BASELINE_RECOVERY_20260908.md) records an isolated source copy, 272 coherent study backups and recovered second-ridge inference. The fresh observation found a news topic-identity collision blocking combined forecasts and an intermittent price-display generation mismatch. Completed forecasts still lost after spread; these are dated findings, not current telemetry.

The project is organized around **prices and vetted news → combined forecast → costs and risk → positions and results**.

The active combined study learns from technical price history and news available at the relevant time. The separately registered [repaired-news v3 study](docs/FOREX_NEWS_RESEARCH_DEPLOYMENT_20260908.md) covers 68 pairs; current coverage and original one-hour outcomes are shown in the dashboard. Prior joint cohorts and price-only models remain separate comparisons. Orders and promotion remain disabled.

Read [the combined-model and news repair](docs/FOREX_JOINT_PRICE_NEWS_20260907.md), [the latest measured price-model results](docs/FOREX_PRICE_V2_FIRST_OUTCOMES_20260907.md), and [source-bound validation](FOREX_JOINT_PRICE_NEWS_VALIDATION_20260907.json). Forecast availability is not demonstrated accuracy or profitability.

Read current work in [pending improvements](FOREX_PENDING_IMPROVEMENTS.md) and the [project log](FOREX_PROJECT_LOG.md). Runtime ledgers, raw captures and private credentials are local. The Forex vault holds audit records and a verified source snapshot, with its README pointing back to this project.

The scheduled bot-health automation is paused. The requested one-hour live watch was completed in chat; its evidence is indexed in the research page.

Earlier README observations are preserved in [the dated history](docs/README_HISTORY_THROUGH_20260907T2150Z.md). Use their dates when comparing results; old runtime descriptions are not current health claims.
