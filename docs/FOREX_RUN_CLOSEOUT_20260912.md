# Forex run closeout — September 12, 2026

Closed at the user's request at approximately **6:58pm Eastern /22:58UTC**, before the original9:42pm deadline. The owned archive preparation and all agent work are stopped. The two completed pair preparations and all earlier failures are preserved. No Forex Python process was returned by the final project-path process check. No broker query, new trading trial or service activation was performed during this run.

The project is better audited and has ten groups of source repairs integrated. It still has no consistently profitable setup established by the completed comparison. Functional correctness and predictive performance remain separate findings.

## Completed historical comparison

The completed compact technical/cross-pair study uses the full declared C-drive primary archive: **53,512,475 minute rows across68pairs**,875,146 hourly observations and1,799,440 eligible direct-horizon labels. It completed36fixed Ridge/HGB fits over three periods and H15/H60/H240, with81learned/baseline reports and18matched technical/peer comparisons.

- Direction accuracy ranged **49.4–52.5%** across learned cells.
- Of36model/horizon/period cells, **32had negative selected-position net after declared costs, two positive and two no-position**. Neither positive setup held across every period; one became negative with the declared one-minute entry delay.
- Adding the75cross-pair features worsened MAE in all18matched comparisons. This result applies to these fixed inputs/models; it does not establish that every possible joint factor or news design is useless.
- Exact saved-artifact recreation reproduced all36models'5,425,308 estimates/actions with maximum absolute difference0.0, plus45baseline arms. These overlapping historical observations are not broker-executed trades or account-profit evidence.

[Complete results and limitations](FOREX_ARCHIVE_DIRECTION_COMPARISON_20260912.md).

## Integrated source repairs

| Area | What was repaired |
| --- | --- |
| Evaluator | Outcome/metric contract checks and explicit interpretation of results. |
| M5 supervised inputs | Bar completeness, feature/label maturity and cache consistency. |
| Pip metadata | Explicit instrument-specific units and source identity. |
| Fuzzy selection | Training/selection/final-period boundaries; unsupported final promotion refused. |
| Fuzzy reader | Freshness reevaluated after reads; stale or changed sources cannot remain ready. |
| Forecast publication | New inactive producer version binds publication acknowledgement to exact persisted readback. |
| News reaction evaluation | Exact bid/ask endpoints, common basis-point costs, actual maturity and separate policy selection/reporting. |
| News source/classification history | Immutable source observations and separate later classification availability; old story age is retained. |
| News consumers | Separate v2 consumers preserve the new source/clock meanings and old consumer versions. |
| Ensemble calibration | Preserve fitted calibrators, verify class mappings and bundle/score consistency, and refuse unsupported qualification. |

The ensemble change passed **133cases** against both staged and installed helper/caller ASTs, then independent source/backup verification. The installed collector now passes **500cases**, including four new provenance regressions. Its current fixture migration preserves historical tests and explicitly distinguishes old observation time from newly available classification. Root also removed a whole-cycle test's accidental dependency on live quote files. The new news consumers passed197cases. Repeated staged/canonical suites overlap; their counts are not independent predictive validations.

The new worker/consumer cohorts remain inactive. Tests use synthetic inputs and owned temporary databases; they do not establish successful live operation. [Ensemble repair](ENSEMBLE_CALIBRATION_REPAIR_20260912.md), [news lineage](NEWS_SOURCE_LINEAGE_REPAIR_20260912.md), [news consumers](NEWS_CONSUMER_V2_REPAIR_20260912.md).

## Richer features: repaired and frozen, performance unfinished

The200-plus feature history was real. The audit traced the795-field registry and separated631own-close/clock fields from inputs unavailable under the retained source contract. The new comparison adds those631to compact24, plus fixed pair identities. It is not a recreation of old795-engine weights or every prior feature experiment.

The first frozen preparation failed on a NumPy integer in a JSON receipt and exposed expensive per-segment computation. A separate batching implementation preserves complete segment histories and reset boundaries. Independent tests then exposed future-input sensitivity in the original rolling skew/kurtosis implementation. Revision003 corrects32moment fields using only their own trailing windows;599other fields retain exact fixture parity. All old versions, failures and numerical differences are recorded.

The final30-source protocol is frozen under SHA256 `870a22150ab6124f37cc32480ab9577af8e3cdee1cd9214880a2135950167083`. Preparation passed70synthetic checks, including full68-pair fixture publication; independent review passed85checks covering causal moments, preparation and evaluator/resource behavior. These groups overlap.

Real revision003 preparation completed **AUD_CAD and AUD_CHF** before the requested stop, in38.75and34.17seconds respectively. It stopped while preparing AUD_HKD. The earlier AUD_CAD implementation took543.391seconds and failed receipt serialization; the corrected version also changes32moment formulas, so that comparison is end-to-end runtime evidence rather than a pure isolated batching benchmark. **There is no completed richer preparation receipt and no richer model fit or performance result.**

[Frozen protocol](../../revamp_8h_20260912/direction_richer_archive_003/PROTOCOL_003.md), [stop/partial-output receipt](../../revamp_8h_20260912/direction_richer_archive_003/USER_STOPPED_001.json).

## Audit coverage and remaining work

The five Forex chats, vault records, pending register and source histories were cross-referenced. All300distinct source versions in the frozen census have dispositions. This includes support code and wrappers; it does not mean300trained models were rerun or every old artifact is recreatable. Missing exact historical weights/source matches and the unavailable D-drive portion remain explicitly unresolved. Existing gradient boosting, neural, curve, MA/SMA and joint-model experiments are recorded to avoid repeating them without a distinct hypothesis.

The separate shared-feed/model-gap audit found misleading probability meanings: a relative side-profit score can be presented as midpoint direction probability, and generic confidence/signed pips can become invented probabilities. Its27synthetic probes retain23counterexamples and four controls. The proposed versioned adapters preserve target-specific side estimates and leave unsupported midpoint probabilities null. **That follow-on implementation is incomplete, untested and not integrated.** Existing forecasts and trade records were not rescored or rewritten.

Priorities on resumption:

1. Complete revision003 preparation in a new owned output directory, run its six predeclared H60/H240fits, and finish/test the matching artifact verifier before claiming richer-feature results. The old001verifier is not valid for003.
2. Finish and review the typed probability adapters with the existing nullable outcome/consensus interfaces; prevent unsupported legacy aggregation or promotion.
3. Continue original-known-time official-news/technical comparisons and position-management evaluation against the same original terminal. A move detector alone does not establish profitable direction, entry or exit.
4. Assemble and verify one portable current source/artifact recovery profile. Existing vault snapshots predate these ten repair groups; documentation checkpoints are not a complete portable release.
5. Resolve operational readiness separately before any new trial. The earlier clock-monitor startup was rejected by automatic approval review with **“blocked by policy.”** No alternate startup route was attempted. Existing deadlines, latches and historical ledgers remain intact.

[Full model audit](FOREX_MODEL_SPACE_AUDIT_20260912.md), [pending change register](FOREX_CHANGE_REGISTER_20260911.md), [work log](../../revamp_8h_20260912/WORK_LOG.md).
