# Forex revamp: baseline and recovery completed

September 8, 2026. This completes the agreed first phase: reconcile the clarified backlog, obtain a fresh operational/performance baseline, preserve the assets needed for the first changes, and prove one existing inference path works in isolation. It does not claim the bot is fully operational or profitable, and it does not activate a replacement model.

## What is ready

- **Isolated source:** `C:/Users/zmoor/Documents/forex/revamp_baseline_20260908/workspace/trad`. All **2,202** members of the latest source snapshot match both the canonical C checkout and the extracted copy; **1,217 Python files** passed syntax compilation. Uncommitted working-tree changes were preserved, not reset.
- **Recovery checkpoint:** `D:/ForexRecovery/revamp_20260908T1353Z`. The source ZIP/manifest/pointer and **63 static files totalling 597,968,093 bytes** were copied with source/copy hashes checked, including 36 historical intrahour modules, actual fitted artifacts, retained reports and the full unified training matrix. A further 9,146,916-byte EUR/USD S5 input is preserved for the comparator check.
- **Coherent study backups:** **272/272 SQLite backups, 6,319,161,344 bytes**, covering joint scheduler v2, joint scheduler v1 and both price-v2 families across their registered pairs. Integrity, schema, contract/activation identity and table-count checks passed. Each online backup is coherent; the set spans **14:00:00–14:03:28 UTC**, rather than one simultaneous global instant.
- **First recovered inference path:** the existing second-ridge JSON model reproduced **all 13 horizons twice**, and an independent high-precision standardized-coefficient calculation agreed to floating-point precision. This used original source in the isolated copy, a copied artifact and copied real input; no fitting or opaque model deserialization was involved.

The recovery checkpoint is local, not an off-device disaster backup. It excludes private credentials, complete raw-feed history and databases outside the selected study cohorts. The source copy retains original operational entrypoints and absolute paths; it is not an execution sandbox. Use explicit copied inputs for offline work and do not launch its supervisor. Read `revamp_baseline_20260908/RECOVERY_README.md` and the recovery receipts before any restore.

## Fresh runtime findings

The three retained runtime captures span **13:58:33–14:06:48 UTC**. Fifteen expected research workers were PID-confirmed with current supervision. Quotes and existing local account observations were current; positions were flat. The inspected registrations retained disabled authorization, order, promotion, account-eligibility and proof flags.

**The combined model was blocked despite healthy worker heartbeats.** Joint-v2 remained at 2,864 publications and zero active forecasts. A read-only query across all 68 joint ledgers identified the last successful publication at **10:33:00.880046 UTC**; all latest targets had elapsed by the baseline observation. This is a publication stall, not proof the process stopped.

The collector produced a fresh news snapshot whose state was unavailable with `conflicting_current_topic_identity`. A retained minimal example reproduces the failure: two individually valid context-only records share one topic/story identity, but contain different publication/availability clocks and disjoint ten-member versus seven-member syndication groups. The unchanged guard accepts either record alone and rejects their combination. This establishes a producer grouping/identity incompatibility; absence of directional news is not the cause. Later observations also caught news age exceeding 300 seconds. Repair the producer identity/grouping contract and test the retained case before any new coordinated publication version; do not remove the consistency check or silently choose one conflicting record.

**Price-only publication continued, but the display was intermittent.** The initial API showed 68 forecast pairs. A follow-up withheld all pairs after repeated `pair_summary_generation_mismatch` reads even though the price worker reported 136 arms with no errors. A bounded recovery read at **14:06:46 UTC** restored 68 pairs; publications advanced from 9,004 to 9,121 and outcomes from 8,036 to 8,158. Both the failed read and recovery are retained. This does not establish uninterrupted dashboard coverage or fix the previously logged generation mismatch.

## Frozen prediction baseline

The unchanged exact evaluators were applied to **original completed outcomes in the copied ledgers**, preserving original forecast, publication, consumption, entry and target records. All 12,034 completed model records reconciled with no quote/score-payload mismatches or unscored retained outcomes. Six price outcomes were ahead of their separately captured original scorecards; both observations are retained rather than rewriting a card. Exclusions and incomplete forecasts are counted separately. One price forecast was past its target but still awaiting an outcome or exclusion at its snapshot; it was not scored.

| Cohort / model | Completed records | Direction correct | Mean after-spread result | Brier score |
|---|---:|---:|---:|---:|
| Joint scheduler v2 | 2,779 | 52.07% | -2.9693 bps | 0.251804 |
| Joint scheduler v1 | 1,154 | 52.25% | -3.4260 bps | 0.250457 |
| Price-v2 state space | 4,101 | 49.31% | -3.7373 bps | 0.261466 |
| Price-v2 Ridge | 4,000 | 45.90% | -4.6865 bps | 0.254734 |

These rows cover different decisions and times, so this table is not a matched model ranking. All four have worse magnitude error than their zero-move baseline and worse Brier score than a constant 0.5 probability (0.25 Brier). After-spread results use normalized executable quote outcomes, not actual account P/L, fills or a complete financing/slippage model.

The joint-v2 internal comparisons do use the same **2,779 decisions and quotes**. Joint direction was 52.07%, versus 50.20% for its separately fitted matched price-only comparator and 46.71% for its same-model neutral-news ablation. Mean net results were -2.9693, -3.1702 and -4.1205 bps respectively. The +0.2009-bps difference versus matched price-only is a measured sample difference, not demonstrated causal news value or profitability. Only **852/2,779 (30.66%)** joint outcomes were positive after spread. Its magnitude MAE was 6.3317 bps, versus 6.1582 for no change.

Joint-v2's completed observations span 63 pairs, 47 quarter-hour reference buckets, 13 UTC hours and two calendar dates. Pairs, models and overlapping H1 targets share market information. These are not 2,779 independent trials. Cross-study exact-endpoint matches are sparse and remain separately counted in the detailed report. More independent sessions and calibrated, cost-aware evidence are still required.

## Reconciled implementation order

1. **Repair the news producer identity conflict**, using the retained two-topic reproduction and preserving original clocks, distinct evidence and old study records. Verify stable current publication before attributing any availability gain to model changes.
2. **Resolve the price display generation mismatch** without accepting mismatched or stale summaries. The worker and display have separate observed states.
3. **Use the recovered second-ridge inference as the first comparator**, with explicit input/horizon contracts and future original-target outcomes. Then perform the separately scoped MA-grid dependency/schema/inference check. Its 643-field capacity already exists.
4. **Reconstruct unified v4 source compatibility** before pairing its fitted artifact with D's v5 code. Repair duplicated cross-currency windows and native-pip comparisons in a new research version, preserving the old matrix and results.
5. **Join the existing blurb/event and currency features incrementally**, using entry-time-eligible information and matched technical/news/combined comparisons. Historical attribution, a registered inactive analog arm and repeated event rows are not extra predictive evidence.

The second-ridge smoke check closes present source/artifact/feature/inference compatibility only. Its July 22 inputs precede the July 31 fit, so it is intentionally an engineering replay, not a holdout, prospective forecast, current-market signal or historical profitability test. The original S5 training transform is also distinct from the old S1 live feature path. All diagnostic output flags disable account eligibility, authorization, orders and promotion; execution profiles were omitted. A separate UTC check verified the original transform's parsed epochs. An initial clock-verifier assumption failed and was retained; its correction did not rerun or change the model result.

## Evidence and unchanged boundaries

Evidence is retained at `C:/Users/zmoor/Documents/forex/revamp_baseline_20260908`, with selected exact copies under `trad/docs/validation/revamp_baseline_20260908`. The canonical validation receipt binds the combined report, selected evidence, unchanged baseline source and registrations, recovery manifests and comparator acceptance.

- [Canonical validation receipt](C:/Users/zmoor/Documents/forex/trad/FOREX_REVAMP_BASELINE_RECOVERY_VALIDATION_20260908.json)
- [Backlog reconciliation](C:/Users/zmoor/Documents/forex/revamp_baseline_20260908/reconciliation/BACKLOG_RECONCILIATION_20260908.md)
- [Second-ridge recovery check](C:/Users/zmoor/Documents/forex/revamp_baseline_20260908/reconciliation/SECOND_RIDGE_ENGINEERING_SMOKE_20260908.md)
- [Source/artifact recovery manifest](C:/Users/zmoor/Documents/forex/revamp_baseline_20260908/SOURCE_ARTIFACT_RECOVERY_20260908.json)
- [Study backup manifest](C:/Users/zmoor/Documents/forex/revamp_baseline_20260908/performance/SELECTED_LEDGER_BACKUP_MANIFEST_20260908.json)

No live model/source implementation, study registration, trading setting, worker or broker operation was changed in this phase. Original live ledgers were read for online backup, not edited; ongoing workers may naturally append new records after each snapshot. Canonical documentation and export mappings are updated separately from the preserved pre-change source baseline. Existing studies continue with their original outcomes and exclusions. The bot is still in research, and the identified operational blockers remain open.
