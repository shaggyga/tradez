# Preserved prediction performance baseline — September 8, 2026

The completed forecasts reproduce consistently, and their after-spread performance is poor. The current joint-v2 cohort is modestly better than its retained matched price-only comparator, but still loses on average, has worse magnitude error than predicting zero movement, and has a Brier score worse than a fixed 50% probability. This is a preserved baseline for the revamp, not evidence supporting trading.

The snapshot observations span **14:00:00–14:03:28 UTC** (10:00–10:03 AM EDT). Each database was captured in its own pinned SQLite read transaction; the set is not globally atomic. All **272 selected databases** passed backup integrity, schema, contract, activation and row-count checks. The copies total **6,319,161,344 bytes**, under `D:/ForexRecovery/revamp_20260908T1353Z/databases`. This includes joint v2 (68), joint v1 (68), and the two price-v2 families (136); it excludes unrelated legacy ledgers and large raw archives. Registered source/configuration hashes remained unchanged.

| Original cohort | Completed / directional | Correct direction | Positive after spread | Mean net bps | MAE bps | Brier | Zero-move MAE bps |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Joint price + news v2 | 2,779 / 2,779 | 1,447 (52.07%) | 852 (30.66%) | -2.9693 | 6.3317 | 0.251804 | 6.1582 |
| Joint price + news v1 | 1,154 / 1,154 | 603 (52.25%) | 329 (28.51%) | -3.4260 | 5.8556 | 0.250457 | 5.7040 |
| Price state space v2 | 4,101 / 4,101 | 2,022 (49.31%) | 1,157 (28.21%) | -3.7373 | 7.2926 | 0.261466 | 5.8683 |
| Price Ridge v2 | 4,000 / 4,000 | 1,836 (45.90%) | 940 (23.50%) | -4.6865 | 6.1954 | 0.254734 | 5.8260 |

Brier uses up versus not-up, with a flat outcome labeled zero; smaller is better and the fixed 50% baseline scores 0.25. These are uncalibrated model probabilities. Net bps use the originally retained executable entry/target bid and ask, with the original H1 target. They are not account-dollar P/L, actual fills, leverage-adjusted returns or financing-inclusive results. All four model groups were directional on every scored decision.

On the **same 2,779 joint-v2 decisions and identical quotes**, the retained matched price-only comparator scored 50.20% direction, 29.83% positive after spread, −3.1702 net bps and 6.3972 bps MAE. Joint v2 improved net by **0.2009 bps** and reduced MAE by **0.0656 bps**. Its retained neutral-news ablation scored 46.71% direction, −4.1205 net bps and 6.5453 bps MAE; joint improved net by 1.1512 bps. Those comparators retained magnitude forecasts but supplied no probability, so no comparator Brier is invented. In the earlier joint-v1 cohort, joint net was 0.1095 bps worse than matched price-only. This is a paired diagnostic comparison, not proof of a stable or causal news contribution.

An additional 0.5 / 1.0 bps of cost changes the four main model means to, respectively: joint v2 −3.4693 / −3.9693; joint v1 −3.9260 / −4.4260; state space −4.2373 / −4.7373; price Ridge −5.1865 / −5.6865 bps. No-trade is zero. The compact evidence retains all original 0/0.5/1 stress values; the initial summary formatter omitted the whole-number keys, and the compacting step restored them from unchanged per-decision results without rerunning scoring.

Coverage and missingness are preserved:

| Cohort | Issued / published / consumed | Stored completed outcomes | Recorded exclusions | Unexpired publications at each snapshot |
| --- | ---: | ---: | ---: | ---: |
| joint_price_news_study_v2 | 2,864 / 2,864 / 2,864 | 2,779 | 85 | 0 |
| joint_price_news_study_v1 | 1,186 / 1,186 / 1,186 | 1,154 | 32 | 0 |
| pair_local_forecast_study_v2 | 9,024 / 9,024 / 9,024 | 8,101 | 434 | 497 |

Joint v2 has 45 missing-entry and 40 missing-target exclusions; joint v1 has 21 and 11. Price v2 retains 265 missing-entry and 169 missing-target exclusions. At its own capture times, price v2 also had one target-due forecast awaiting an outcome or exclusion; it was not scored. The unexpired count is a lifetime-ledger count of overlapping publications, not one forecast per dashboard row. Joint forecasts had previously reached 63 pairs; price v2 reached all 68. This baseline does not diagnose why neither joint cohort had an unexpired publication.

All **12,034 originally stored completed outcomes** passed the unchanged exact evaluators, with the same selected entry/target IDs and no score-payload mismatch. Six price outcomes were ahead of their separately captured original scorecards; those scorecards are retained unchanged, and the six outcomes are explicitly identified in the per-ledger evidence. No outcome was generated, no forecast refit, and no original scorecard overwritten. Full original-export hashes and completed-only selection hashes remain distinct.

The joint-v2 rows span only 47 quarter-hour reference buckets, 13 UTC hour blocks and two calendar days. Joint v1 has 48 buckets; state space 65 and price Ridge 64. Within-pair nonoverlapping H1-window counts are 664 / 485 / 961 / 959 respectively, but shared currencies and factors still prevent treating these as independent trials. Independent sample size is therefore left unknown. The separate price-family cohorts have different clocks and denominators; only 30 Ridge and 35 state-space cases share the joint-v2 exact reference/entry/target prices and market times, so broad cohort averages must not be presented as matched comparisons.

Evidence: [backup manifest](C:/Users/zmoor/Documents/forex/revamp_baseline_20260908/performance/SELECTED_LEDGER_BACKUP_MANIFEST_20260908.json), [compact baseline](C:/Users/zmoor/Documents/forex/revamp_baseline_20260908/performance/PRESERVED_PREDICTION_BASELINE_COMPACT_20260908.json), and [full scoring index](C:/Users/zmoor/Documents/forex/revamp_baseline_20260908/performance/CURRENT_PREDICTION_BASELINE_20260908.json). The index binds all 272 per-ledger result files. Original database/input evidence remains in the D-drive recovery copies.
