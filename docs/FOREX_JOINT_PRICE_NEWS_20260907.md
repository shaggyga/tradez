# Combined price and news research — September 7

The research model now learns from **both technical price history and news available at the relevant time**. Its inputs include news context, separately vetted directional evidence, and interactions between price momentum and news. This is a fitted Ridge model with conservative shrinkage; no arbitrary news-to-pips multiplier was introduced.

The project and dashboard follow one path: **prices and news → combined forecast → costs and risk → positions and measured results**. The main table keeps observed market moves distinct from one-hour forecasts. It displays the combined prediction, the same fitted model with neutral news, and their difference. A separately fitted price-only comparison appears in details; it is not incorrectly added to the news adjustment. Other price models and historical diagnostics are secondary views. Positions come from the account observation, not research quote-entry records.

## What was repaired in news

The audited snapshot at 21:27:57 UTC showed 50 directional pair labels driven by one broad topic. An Energy Intelligence story about Iranian domestic output had arrived promptly, but a later CNN story about military escalation was observed about 56 minutes after publication. The old aggregation allowed that late, different claim to inherit the first article's timeliness and count as independent corroboration.

The v165 admission guard checks each member's publication, actual availability and original reaction expiry. Independent support must concern the same claim. Later mapping or consumption cannot renew the reaction window. The original records and first-seen clocks remain unchanged.

At the independently verified 22:19:57 UTC observation, all 15 current topic proofs replayed, there were zero vetted directional topics, and all 68 pair labels were neutral. The faulty topic was directly verified as context with insufficient corroboration. Context remains a possible learned model input; a neutral directional label does not mean the model ignores news. The current producer publishes a compact, source-bound evidence artifact for the joint model.

The SCB parser now recognizes an explicitly preliminary CPI page awaiting a regular CPI/CPIF release, emits zero numeric rows, and preserves the old final data's timestamp. Its source test used an actual captured page. The live source state remains subject to its next scheduled fetch. GDELT rate-limit cooldown and the observed HKMA timeout/fallback remain separately reported transport conditions.

## Training and prospective evaluation

Historical training uses preserved immutable governance records after their committed visibility and own observation times. Mutable article tables are not treated as trustworthy historical feature snapshots. The frozen numerical/input audit found 3,631 retained records and 61 of 68 pairs meeting the retrospective training requirements at that observation. Training covers roughly one trading day, with strongly overlapping outcomes and correlated pairs.

Every new forecast retains its actual capture, computation, issue, publication and independent consumer clocks, plus the original one-hour target. A later executable quote supplies the research entry. Missing or late quote observations remain visible exclusions. Current price inputs must be within 900 seconds and news within 300 seconds, including any earlier original evidence expiry.

The evaluator scores the retained combined, matched price-only and neutral-news predictions using the same valid decisions and exact entry/target quotes. It reports paired magnitude-error and after-spread differences. It does not invent probabilities for the two comparison predictions or silently shrink their denominator. The decomposition alone does not establish that news improves accuracy or returns.

## Scheduling repair and preserved versions

The first joint worker passed a four-pair live trial, but its 68-pair startup exposed a refresh-priority delay: a full input sweep exceeded the 30-second rescan interval, and changing sources repeatedly displaced ready fits. It eventually published when the input interval stabilized. That is a documented delay, not a claim that it could never publish.

The separate scheduler-v2 worker alternates capture and fit work with independent cursors. Tests cover continuous source updates, slow full-universe sweeps and repeated fit failures. An independent probe also caught and closed a shared-cursor parity defect before v2 activation.

The actual 68-pair v2 preflight ran for 180 seconds: 59 publications and independent consumptions, 52 later entries, three properly timed entry exclusions, four pending entries, one unfinished reserved attempt at bounded shutdown, and zero worker errors. No one-hour target was due. All 59 published captures and 177 retained model/comparison calculations were independently replayed. Disposable preflight cohorts are excluded from canonical performance.

The canonical scheduler-v2 registry is `config/joint_price_news_study_v2_20260907.json`, SHA-256 `20fea86e661efd758536b81fa5c474a440feee2e6af48b28a1016b1538d3691e`. Its 68 ledgers have distinct `prospective_scheduler_v2` cohorts. The original joint-v1 worker remains a named comparison so its published forecasts can receive their original outcomes. Price-v1 and price-v2 evidence also remain separate. None of their frozen source or registration bytes was rewritten.

## What the measured results say

The first price-v2 outcome audit retained 188 scores across 136 ledgers and independently checked 758 publication chains, with no actual hash, quote-selection, scoring or duplicate mismatch. The observation covered approximately 22:12–22:21 UTC targets; it is a narrow initial period.

| Price-only family | Direction correct | Positive after spread | Mean net bps per decision | Brier |
|---|---:|---:|---:|---:|
| State-space | 16 / 95 (16.8%) | 0 / 95 | −13.821 | 0.3097 |
| Ridge | 58 / 93 (62.4%) | 1 / 93 | −10.308 | 0.2338 |

Observed spread drag averaged roughly 11.5 bps. Both families also had worse magnitude error than the zero-move baseline in their respective samples. These are normalized research results, not account P/L, and the two families have separate denominators. The findings do not establish a profitable model. They do not evaluate the new joint model, whose prospective one-hour outcomes must be assessed separately.

[Detailed first-outcome report](FOREX_PRICE_V2_FIRST_OUTCOMES_20260907.md).

## Operation, capacity and remaining acceptance

The supervisor targets 15 explicit research workers, preserving both joint versions. The dashboard's primary collection status follows joint v2; price-only and original legacy collection statuses remain explicitly named comparisons. Fresh worker observations are distinguished from retained inactive-component failures. The storage guard inventories 352 registered/core database paths and waits for a matching inventory baseline before projecting growth.

Shared news captures are losslessly compressed and hash-verified: the audited 6.49 MB snapshot became 1.43 MB. No evidence deletion was introduced. At one new capture per minute this still implies roughly 2 GB/day; actual capture cadence and disk growth must be observed. Input reconstruction has explicit 48-hour, 5,000-row and byte limits. Exceeding a bound withholds inputs rather than silently truncating training history.

Remaining acceptance is fresh joint one-hour outcomes, paired comparison results over more independent sessions, calibration, after-cost usefulness and broader immutable training history. Co-movement/pooled currency models remain separate pending research. The prior joint scheduler can be retired after its outcome and comparison review; no automatic retirement or scheduled task was created.

Orders, promotion and proof eligibility remain disabled. The scheduled bot-health automation is paused; the user-requested one-hour watch was completed in this chat. Passing tests and current collection do not demonstrate predictive success.

[Final source-bound validation and live receipts](../FOREX_JOINT_PRICE_NEWS_VALIDATION_20260907.json) · [Active pipeline](ACTIVE_PIPELINE.md) · [Research index](RESEARCH_INDEX.md) · [Curated evidence manifest](validation/joint_price_news_20260907/CURATED_EVIDENCE_COPY_MANIFEST_20260907.json).
