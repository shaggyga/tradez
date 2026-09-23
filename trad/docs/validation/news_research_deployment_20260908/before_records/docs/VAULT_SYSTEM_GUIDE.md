# Forex system guide: understand first, reconstruct offline

This guide explains the project from the Forex vault without requiring a running dashboard, a broker connection, or access to the original computer. It describes the September 7–8, 2026 source and recorded research state; it is not a fresh runtime-health observation.

The project asks whether information genuinely available at a decision time—prices, technical patterns, news and event context—can forecast future currency-pair movement accurately enough to survive trading costs. It retains unsuccessful and incomplete experiments as evidence. The current path is research-only: collection and forecasting do not authorize orders, promotion or proof eligibility.

Start with the [worked feature example](FEATURE_GENERATION_WORKED_EXAMPLE.md), then the [feature dictionary](FEATURE_DICTIONARY_CURRENT.md). Use the [knowledge index](KNOWLEDGE_INDEX.md) to locate records and distinguish direct vault files from source-archive members and external evidence. [Research history](RESEARCH_INDEX.md) and [active ownership](ACTIVE_PIPELINE.md) provide the next level of detail.

## Reading locations and authority

In this guide, **vault root** means `thevault/projects/forex`. Markdown links use filenames relative to that root. The canonical editable source was `C:/Users/zmoor/Documents/forex/trad`; runtime data are a separate resource, principally under its `data/oanda_training_manager` directory.

Paths such as `config/joint_price_news_study_v2_20260907.json` below are **exact source ZIP member paths**, not clickable files at the vault root. ZIP members are relative to `trad` itself: do not prepend `trad/`. Find them in the manifest selected by [the current source pointer](source/WORKTREE_SOURCE_LATEST.json), then read them in the verified archive or an isolated extraction. A local Windows path recorded in an old report is provenance, not a portable link or evidence that the named file was exported.

Authority has several layers, which must agree:

1. The source pointer and manifest establish which exported bytes exist, including base commit and uncommitted changes.
2. Versioned study contracts establish instruments, features, model versions, clocks, evaluation rules and safety flags.
3. Source-bound validation establishes what was checked, at its recorded time and within its stated scope.
4. Original runtime ledgers and independent observations establish what actually happened. Static documentation and a successful source export cannot replace them.

Older reports remain dated evidence even when their filenames contain `CURRENT`. In particular, [recreation history](RECREATION_HISTORY_THROUGH_20260907.md) and [maintenance history](maintenance/README.md) retain earlier stopped/running states, monitor descriptions and export counts. They are not instructions to restart those states. The latest narrative records say the scheduled bot-health automation is paused; this guide neither checks nor changes it.

## The information flow

```text
Timestamped prices + source news/releases
    => admitted, point-in-time inputs and explicit missingness
    => technical/news features
    => fitted forecast + matched comparisons
    => immutable publication + independent consumption
    => later research entry quote + original future target quote
    => errors, direction, probability quality and after-cost outcomes

Independent governance => exact authorization => execution is a separate boundary.
The research path above has no such authorization.
```

| Stage | What it contributes | Source owners / vault explanation |
|---|---|---|
| Price ingestion | Observed bid/ask quotes and real one-minute candles for registered pairs; missing candles are not fabricated. | `oanda_practice_quote_stream.py`, `oanda_all68_m1_forward_updater.py`; [pipeline](ACTIVE_PIPELINE.md). |
| News ingestion and interpretation | Publications, original observation/visibility clocks, governed source identities and currency/claim context. | `oanda_local_news_sentiment.py`, `oanda_news_classification_contract.py`, `config/news_sources_v1.json`; [source inventory](NEWS_SOURCES_CURRENT.json). |
| News admission | Separates usable context from independently supported directional claims; a later related story cannot inherit another story's timeliness. | `oanda_news_causal_aggregation_guard_v1.py`; [news repair](CURRENT_NEWS_AUDIT_AND_REPAIR.md). |
| Feature generation | Converts eligible price history and captured news into numeric inputs while preserving coverage, age and missingness. | `oanda_causal_forecast_inputs_joint_news_v1.py`; [dictionary](FEATURE_DICTIONARY_CURRENT.md) and [machine-readable dictionary](FEATURE_DICTIONARY_CURRENT.json). |
| Model fitting and prediction | Fits a joint price/news Ridge model, a matched price-only model, and a same-model neutral-news comparison. Price-only Ridge and State-space families also have separate ledgers. | `oanda_joint_price_news_models_v1.py`, `oanda_joint_price_news_forecast_study_v2.py`; [combined study](JOINT_PRICE_NEWS_CURRENT.md). |
| Evidence and scoring | Retains original forecasts, publication/consumer clocks, quote selection, missing/late exclusions and matured outcomes. | `oanda_causal_forecast_ledger_joint_news_v1.py`, `oanda_fixed_forecast_evaluation_joint_news_v1.py`, `oanda_exact_price_scoring.py`; [evaluation protocol](FIXED_EVALUATION_PROTOCOL_CURRENT.json). |
| Display and accounts | Shows observed movement separately from forecast and measured outcome. Account/position snapshots are read-only observations; research entries are not positions. | `oanda_practice_live_dashboard.py`, `oanda_main_signal_dashboard.html`, `oanda_account_snapshot_writer.py`; [pipeline](ACTIVE_PIPELINE.md). |

The domains under `src/forex_system/` describe a compatibility-first organization, not proof that every root script has migrated. Source imports, supervisor commands and hash bindings depend on stable paths. Do not bulk-move them to make the tree look cleaner.

## Pairs, currencies, features and horizons are different counts

The current joint registry names **68 currency pairs drawn from 21 currency codes**, counted from that registry's pair keys. For `EUR_USD`, EUR is the base and USD the quote currency; the price is USD per EUR. The 68 pairs are neither 68 currencies nor 68 statistically independent markets: many share a currency and move together. Registration of a pair does not guarantee fresh input, a fitted prediction or an eligible outcome for it.

| Count | Meaning—not an interchangeable measure of model breadth |
|---|---|
| 251 catalogue entries | A design catalogue across 22 blocks; it includes intended, unavailable, inert and inspected definitions. It is not the active model vector. |
| 34 current joint inputs | 24 price/time/coverage fields, eight news fields and two price/news products. The fitted intercept is not a 35th feature. |
| 227 / 220 historical fields | Original and corrected wider technical feature families; downstream candidate expansions and feature selection can change the number actually consumed. |
| 795 historical numeric inputs | The unified intrahour matrix, plus three categorical identities; it predicts eight horizons: 1/2/3/5/10/15/30/60 minutes. |
| Up to 643 MA-grid fields | A separate moving-average engine spanning multiple input timeframes and horizons; its cells are separate fitted configurations, not additional current joint features. |
| 14 second-ridge numeric inputs | A separate historical curve engine across 13 horizons from 15 seconds to four hours. Profiles and parameter surfaces are not independent successful trials. |

The current joint model's **M1 input** means one-minute price observations; its **H1 target** means a one-hour future outcome. A 15-minute forecast cadence does not change that target to 15 minutes. The present H1 path does not consume the older 795-field matrix or its cross-pair strength fields. See the [existing-engine audit](EXISTING_FEATURE_HORIZON_AUDIT_CURRENT.md) for the direct-currency panel, MA cells, fitted artifacts, validation limitations and recovery gaps.

In the current vector, price-window rates use actual elapsed time; coverage and unavailable flags prevent a missing bar from masquerading as a flat price. News context can remain nonzero while vetted direction is neutral. The documented no-context news vector is `(0, 0, 0, 1, 0, 0, 0, 0)`, not eight zeros. An unverifiable or stale snapshot is unavailable, not neutral. Features are standardized using eligible training rows, and Ridge learns their coefficients jointly: there is no fixed news-score-to-pips conversion. The [worked example](FEATURE_GENERATION_WORKED_EXAMPLE.md) is explicitly synthetic arithmetic, not an observed trade or prediction.

## Current research versus preserved experiments

The recorded active combined path is joint **scheduler-v2**, registered in `config/joint_price_news_study_v2_20260907.json`. It alternates capture and fit work with independent cursors. Its model family is still named `ridge_price_news_v1`: scheduler, feature/model and cohort versions are separate identities, so the `v1` family name does not make it the old scheduler.

Price-v2 is the selected displayed price-only study; it keeps independent `ridge_return_repaired` and `probabilistic_state_space` family ledgers. Pair-v1 and joint scheduler-v1 remain named comparisons in the latest narrative. Joint-v1 retains its already-issued forecasts and their original pending targets. `gap_v2` and `eurusd_v1` are retired from new live attempts; their evidence must not be relabelled as the successor's results. These are recorded dispositions, not a new process census.

Older wide-feature models, curves, source-response studies, movement-first news/blurb research, proof workers and execution artifacts are real parts of the project, not all active inputs. The [model inventory](MODEL_INVENTORY_CURRENT.md), [existing-engine audit](EXISTING_FEATURE_HORIZON_AUDIT_CURRENT.md), [blurb audit](BLURB_DATASET_AUDIT_CURRENT.md) and [research index](RESEARCH_INDEX.md) explain their scope. The blurb corpus exists separately from the constant macro placeholders found in particular older matrices; neither finding proves that the current H1 model consumes that corpus. Movement-linked explanations selected after a move are not automatically advance predictions. The audited recovered-blurb analog arm had no scored predictions and was inactive as a forecasting contributor.

Do not resume a retired experiment because its source remains present. Check its exact registration and `config/shadow_runtime_retirements_v1.json`. A materially changed source, feature, label, cost rule or model requires a new contract/cohort; preserving prior outcomes is part of the design.

## Clocks, costs, comparisons and proof

**Clocks:** source publication, first observation, committed visibility, input cutoff, computation, issue, publication and independent consumption are different events. A historical story's publication time does not prove it was available to this system then. Later mapping or consumption cannot renew an expired reaction window. Historical training inputs and prospectively issued forecasts are separate evidence classes.

For the current H1 contracts, the registered limits include 900-second maximum price-input age, 300-second maximum news age, 60-second quote age, and 60-second maximum entry/target-quote delay; an earlier original news expiry still applies. Read the exact pair/family contract for acceptance details. Target time remains the originally declared one-hour target even when publication or entry is delayed. Missing, stale, late or unverifiable observations remain explicit exclusions or abstentions; replay and later downloads cannot manufacture the original first-seen or publication clocks.

**Costs:** reference-midpoint movement and executable-entry results answer different questions. The exact scorer uses target bid minus entry ask for a buy and entry bid minus target ask for a sell. It separately normalizes net results to basis points using the entry midpoint, with additional registered cost stresses of 0, 0.5 and 1 basis point. Pair pip size comes from the contract: native pips across different pairs are not comparable dollar profit. Research quote-based net results are not account P/L and do not establish actual fills, financing or every real execution cost.

**Comparisons:** the joint forecast, separately fitted price-only model and neutral-news ablation must be scored on the same valid decisions and quotes. Neutral-news ablation changes the news inputs of the same fitted joint model; it is not the separately fitted price-only model. The displayed difference is a model decomposition, not proven causal news impact. Registered evaluation also includes fair-coin, zero-move, no-trade and rolling-class-rate baselines. Direction accuracy, magnitude error, Brier probability score and after-cost return measure different things; an uncalibrated probability is not a demonstrated success rate.

**Proof:** source integrity, successful tests, forecast coverage, prediction quality, after-cost usefulness and order authority are separate claims. Overlapping H1 outcomes, correlated pairs and repeated profiles cannot be counted as independent successes. The [initial price-v2 results](FOREX_PRICE_V2_FIRST_OUTCOMES_20260907.md) and [performance audit](PERFORMANCE_AUDIT_CURRENT.md) retain adverse outcomes. The [joint study](JOINT_PRICE_NEWS_CURRENT.md) states its remaining evaluation requirements; its implementation and preflight do not establish forecasting improvement. Research gates remain false for `proof_eligible`, `account_eligible`, `can_authorize`, `can_promote` and `can_place_orders`.

## What the vault does and does not contain

| Location / evidence class | What is available and what it establishes |
|---|---|
| Forex vault root | Curated Markdown/JSON audit records, configuration/evaluation copies, dictionary and worked example. These explain decisions and recorded observations; they are not complete runtime datasets. |
| `source/` | Versioned ZIPs, member manifests, current working-tree pointer and older committed-baseline records. Only listed members are included. A valid archive hash does not imply every historical source package or trained model is present. |
| `maintenance/` | Dated export/repair/reset receipts and before-state records. A receipt may identify a local recoverable archive without containing that archive's payload. It does not establish OneDrive cloud upload. |
| Elsewhere in the wider vault | The blurb audit identifies `../../PROJECT_COMMONS/artifacts/FOREX_NEWS_BLURB_RESEARCH_RECOVERED.zip`: a small recovered source/worked-workbook bundle, outside this Forex folder. It is not the bulk generated blurb corpus or the runtime factor databases. |
| Canonical local runtime, not a source-only restore | Original SQLite ledgers, coherent WAL-backed history, raw captures, training matrices and model artifacts must be inventoried and preserved separately. A report quoting their paths/counts is not their backup. Private credentials are intentionally outside the portable source workflow. |
| Historical external locations | The existing-engine audit records older source/models under `D:/forex/trad` and a unique unified fitted model under `C:/Users/zmoor/AppData/Local/ForexResearchData/unified_intrahour_v1`. This guide has not rechecked those locations. The audited `fresh_m1_intrahour/src` package is missing from C and the inspected source exports; do not describe that engine as restored. |

The exact export inventory is always the manifest selected by [the source pointer](source/WORKTREE_SOURCE_LATEST.json), not an old record count. The [readability report](VAULT_READABILITY_REPORT.json) classifies navigation/recovery references; unresolved or external references remain explicit gaps, not inferred file copies. A readable vault can make an absent dependency understandable without making it available.

## Inspect-only reconstruction contract

The objective here is **an independently inspectable source copy**, not a running bot or a replay of original performance. No broker, credentials, dependency installation, source execution or runtime-status call is required.

1. Read [the source pointer](source/WORKTREE_SOURCE_LATEST.json). Resolve its `archive` and `manifest` names only within `source/`. Read the manifest's `scope`, `ignored_scope`, exclusions, member list, hashes and dirty-worktree provenance. Do not use an older baseline pointer or an archived bootstrap script as the current entry point.
2. Use a local hashing tool such as PowerShell `Get-FileHash -LiteralPath '<exact manifest path>' -Algorithm SHA256` and the equivalent command for the ZIP. Compare the manifest hash with the pointer and the ZIP hash with both pointer and manifest. Stop on a mismatch. Agreement establishes consistency with the retained receipt, not model correctness or complete historical coverage.
3. Inspect the ZIP directory without running its contents. Require unique relative member paths matching the manifest; reject absolute/drive paths, parent traversal (`..`) and entries that could escape the destination. Use the manifest's exact members to locate the source contracts listed below. Reading archive text directly is sufficient for orientation.
4. If an extracted copy is needed, use a newly created empty inspection directory outside live source and runtime data. Extract only after the path checks, never over the canonical project. Compare extracted member sizes and SHA-256 hashes against the manifest and check for missing or extra members. Keep the original ZIP and manifest unchanged. Extraction creates an inspection copy; it does not restore databases, clocks, account state, model weights or activation.
5. Read `README.md`, the controlling JSON contracts and their source files as text. Inspect dependency declarations, not installed environments: [the dependency inventory](RESEARCH_DEPENDENCIES_CURRENT.txt) corresponds to `config/requirements-research-audit-20260905.txt`; narrower source members include `requirements-forex-news.txt` and `config/runtime_requirements/requirements-core_timeseries-exact.txt`. These are captured requirements, not a universal platform guarantee. The current study registries record Python 3.12.10, NumPy 2.5.1 and scikit-learn 1.9.0. Do not install packages or load serialized models merely to read the project.
6. Record the reconstruction boundary: which source members verified, which referenced artifacts are absent, and which original ledgers/models would be needed for a particular historical result. Stop there. Source-only acceptance must not require the canonical historical issue validator to pass against absent databases. No missing-evidence result may be rewritten as a verified historical result.

The archive contains `tools/vault_worktree_snapshot.py`, whose documented verifier checks ZIP/member hashes, paths and CRC and performs syntax compilation without importing trading modules. It is **not executed by this guide's inspect-only procedure**. Tests and integration checks are a separate, explicitly scoped next phase: successful syntax or fixture checks would still not recreate historical observations. Importing a module or deserializing a model is execution, not text inspection.

Do not invoke `oanda_always_on_supervisor.ps1`, start scripts, logon tasks, workers, account readers, collectors, broker APIs, execution commands or promotion tools during offline reconstruction. Do not import old ledger rows as fresh prospective evidence, edit frozen registrations to accommodate changed source, or download replacement data and assign it original first-seen times. A later research restart, missing-source recovery or practice-execution review requires a separate authorized plan; this document grants none.

## Configuration and safety reference

These are source-archive member paths; inspect the versions in the selected manifest, and retain each contract's source hash bindings.

| Authoritative record | Scope |
|---|---|
| `config/joint_price_news_study_v2_20260907.json` | Active combined-study registry; 68 instruments, per-pair cohorts, source/model versions, timing limits and research-only safety flags. |
| `config/pair_local_forecast_study_v2_20260907.json` | Separate price-family contracts, ledgers and evaluators. |
| `config/pair_forecast_primary_current.json` | Selects price-v2 for the displayed price study; it does not authorize trading or replace the combined-study collection authority. |
| `oanda_news_classification_contract.py`, `config/news_sources_v1.json` | News interpretation identity and governed source declarations. A version change needs coordinated producers/consumers and fresh validation. |
| `config/model_feature_space.json` | Design feature catalogue, not a guarantee that all fields are populated or actively fitted. |
| `config/shadow_runtime_retirements_v1.json` | Historical retirement, drain-only and inactive-feature dispositions; use with the exact cohort contract and newer study selection records. |
| `config/project_layout_v1.json` | Ownership, protected evidence paths, compatibility-first migration and separation of research, governance and execution. |
| `oanda_always_on_supervisor.ps1` | Explicit worker admission and research-only safety checks; an unknown or execution/promotion worker is not admitted by the research allowlist. Read it; do not launch it. |

The older `config/README.md` contains account-selection examples and historical execution settings. It is not current activation authority. The package design permits execution only through its separate exact practice authorization boundary; that architectural possibility is not permission to enable it. The current studies explicitly cannot place orders, authorize, promote or become account/proof eligible. Real-money routing remains disabled in the layout contract.

## Glossary

- **Pair / instrument:** one quoted base/quote currency combination, such as `EUR_USD`.
- **Feature:** a model input derived from information admitted by its time and source rules; an inventory entry may be only a specification.
- **Horizon / cadence:** how far ahead a prediction targets / how often an attempt is scheduled. They are not the same clock.
- **Contract / cohort:** frozen rules and version bindings / the separately identified evidence stream collected under those rules.
- **Prospective:** issued and independently observed before the original future outcome, not reconstructed after it.
- **Point-in-time / first-seen:** what this system could actually know by a cutoff / when it first observed the source information.
- **Abstention / exclusion:** no admissible prediction or exposure / an observation that fails a declared evaluation rule; neither is an invented zero move.
- **Ledger / WAL:** persistent evidence records / SQLite's write-ahead log, which may contain committed data needed for a coherent backup.
- **Baseline / ablation:** a simple reference forecast or no-trade policy / a controlled change to inputs or components used to test contribution.
- **Pip / basis point:** pair-specific price increment / one ten-thousandth of a normalized value; neither alone is account dollar P/L.
- **Publication / consumption:** a producer making a forecast available / an independent reader observing it; computing a result is not enough.
- **Manifest / hash binding:** an exact file inventory / a recorded content identity tying code or artifacts to a contract or receipt, not proof of profitability.
