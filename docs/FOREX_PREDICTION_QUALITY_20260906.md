# Prediction quality from the saved Forex project

Assessment completed from nine saved JSON files and the metric-producing source code. No production database was connected, no worker or broker was started, and no new predictions were generated. Saved observations remain dated August–September 5, 2026, despite this review finishing on September 6 UTC. The exact inputs, hashes, denominators, all pair cells and calculation script accompany this report in [the canonical receipt](../FOREX_PREDICTION_QUALITY_20260906.json) and [the preserved calculation script](validation/assess_saved_predictions_20260906.py). The script records the original local project path and writes its receipt beside itself; for reproduction, copy it to a disposable review directory before running with Python -B. The original input snapshots remain local and are not all included in the source-only vault export.

## Answer

**The project has not demonstrated a reliable useful predictive edge.** Its broad current shadow signals got gross direction right on **4,156 of 8,414 outcomes (49.39%)**. Only **716 (8.51%)** had a positive executable result after spread. These are simulated signal outcomes, not an account's executed-trade win rate. Some historical probability and movement-size models show descriptive improvements, but those improvements have not become independent prospective, after-cost evidence.

Do not compress the entire project into one accuracy figure: the broad signal consensus, equation calibration, four active model families, historical opportunity model and source-news forecasts are different populations. They have different targets, clocks, horizons and eligibility rules.

## Current strict-horizon broad signal diagnostics

The saved `state/top_signal_position_ledger_v1.json` is generated at **2026-09-05 02:08:02 UTC**, and its current measurement is `raw_all_signal_consensus_v5_strict_horizon_lineage`. The producer's aggregate query requires this exact measurement version, `status='matured'` and `maturity_valid=1` (`oanda_top_signal_position_ledger.py:918–951`). Earlier versions and quarantined maturities are excluded from these 8,414 rows.

Direction means a strictly positive midpoint move in the forecast's direction. A flat midpoint is not a hit. Win means positive executable net after entry and exit spread. Neither is the percentage of actual orders that won. The first/last issue times and independent episode count for this whole population are not stored in the snapshot; its latest 80 displayed positions cannot supply the missing full-period range. Class prevalence and a majority-class baseline are also absent, so 49.39% is descriptive rather than a formal test against chance.

| Horizon | Matured signal rows | Direction correct | Positive after spread |
|---|---:|---:|---:|
| 1 minute | 3,487 | 49.50% | 3.33% |
| 2 minutes | 1,897 | 50.24% | 8.59% |
| 3 minutes | 1,309 | 49.89% | 12.99% |
| 5 minutes | 785 | 49.30% | 10.19% |
| 10 minutes | 406 | 46.80% | 15.02% |
| 15 minutes | 220 | 47.73% | 18.64% |
| 30 minutes | 136 | 42.65% | 21.32% |
| 1 hour | 72 | 38.89% | 25.00% |
| 2 hours | 36 | 72.22% | 44.44% |
| 3 hours | 24 | 66.67% | 50.00% |
| 4 hours | 17 | 41.18% | 29.41% |
| 6 hours | 11 | 36.36% | 18.18% |
| 8 hours | 6 | 33.33% | 33.33% |
| 12 hours | 6 | 16.67% | 16.67% |
| 24 hours | 2 | 0.00% | 0.00% |

The two-hour direction result is a small, correlated slice of a broad horizon search; it still had a negative mean after spread. The positive three-hour and eight-hour pip means occur in only 24 and 6 rows. They cannot justify selecting those horizons after inspection.

By policy, the 8,085 ordinary diagnostic rows had 49.20% direction accuracy and 7.12% after-cost wins. The 328 conflicted aggressive shadow rows had 53.96% direction accuracy and 42.38% after-cost wins, but negative mean net. The remaining aggressive row won; a single row establishes nothing. No policy population establishes a positive edge here.

The raw pooled mean was approximately **+0.003 gross pips and −6.319 net pips per row**. This shows costs swamping the signed movement in that diagnostic accounting, but pips from different currency pairs are not comparable economic weights. It is not a dollar return, capital-weighted portfolio return, or valid universal transaction-cost estimate. Pair-normalized bps or account-currency economics are required for such comparisons.

## Equation calibration: a small diagnostic improvement with a clock limitation

The separate `timeframe_equation_matrix` snapshot is generated at **2026-09-05 02:05:20 UTC**. It contains 208 global and 10,366 pair surfaces. The scopes reuse the same underlying outcomes and must not be added together. Its saved summary has no aggregate first/last forecast date range or independently validated sample size.

| Replay scope | Overlapping OOS row evaluations | Raw direction | Recalibrated direction | Raw Brier | Recalibrated Brier |
|---|---:|---:|---:|---:|---:|
| Global surfaces | 1,608,622 | 47.74% | 52.34% | 0.255257 | 0.249301 |
| Pair surfaces | 1,202,961 | 48.43% | 51.57% | 0.254109 | 0.250504 |

Lower Brier is better. A constant 50% probability has a mathematical Brier reference of **0.25** for binary outcomes. Global recalibration is only 0.000699 better than that reference; pair recalibration is 0.000504 worse. This is not a comparison against a fitted, time-valid historical class-rate forecast.

Exact class balance for the OOS slice is not stored. The saved bins include the 40-row warmup per surface. Recovering their integer up counts and allowing all possible warmup labels bounds the global OOS up rate between 49.23% and 49.75%; the hindsight pooled majority-class accuracy is therefore between 50.25% and 50.77%. The global 52.34% replay score exceeds that crude pooled reference. It still does not establish a deployable forecast advantage. Pair surfaces have much more excluded warmup, giving an uninformative 50.00–66.78% bound for this aggregate majority reference. A proper baseline must be frozen from earlier available data for each pair/horizon and scored on exactly the same decisions.

**The recalibrated metrics are completed-outcome replay diagnostics, not verified probabilities published at original forecast time.** `SurfaceStats.observe` calculates a calibrated score before incorporating that row's own label, which prevents direct reuse of the same label (`oanda_timeframe_matrix_calibration.py:117–154`). However, `run_once` reads completed outcomes ordered by `row_id` and passes them to that replay (`:446–489`). It does not check that every previously processed label had matured before the scored forecast was originally issued. Overlapping forecasts can therefore benefit from information unavailable at their issue times. The “causal OOS” label in the saved display does not resolve this distinction.

All recalibrated net means are negative when pooled within each scope (−24.514 global, −23.478 pair pips, with the same mixed-pair-unit caveat). Only eight surfaces meet local validation thresholds; the snapshot still says `account_eligible:false`. Surface counts and row-wise lower bounds are not multiplicity-adjusted independent confirmation. These equation results must not be presented as the accuracy of the four active model families below.

## Four active model families

The lifecycle snapshot at **2026-09-05 01:41:08 UTC** reports four prospective collection cohorts beginning **2026-08-29 12:36:36 UTC**. Each produced 25,626 forecasts on a frozen 15-minute cadence. These are recorded research-cohort statistics, not promoted predictions.

| Family | Matured rows | Effective episodes | Pooled after-cost mean | Excess confidence interval |
|---|---:|---:|---:|---:|
| Cross-pair graph transfer | 24,203 | 8 | −111.239 pips | −134.767 to +47.832 |
| Modern tabular probabilistic, repaired | 24,202 | 8 | −118.724 pips | −151.299 to +31.299 |
| Probabilistic state space | 24,184 | 8 | −135.951 pips | −150.717 to +31.881 |
| Ridge return, repaired | 24,165 | 9 | −120.856 pips | −140.766 to +33.566 |

The confidence intervals are the saved **time-uniform bounds for clipped excess (net pips minus the configured economic threshold, clipped to ±60)**, not confidence intervals centered on the displayed raw pooled means. Mixed currency pip scales make those raw means unsuitable as portfolio-performance numbers. Even so, the saved diagnostics provide no clear positive outcome: all four point estimates are negative, uncertainty is wide, and effective samples are tiny relative to the raw rows. The four counts share market events and are not four independent replications.

Current saved reports do **not** supply gross directional accuracy, Brier or RMSE for these exact prospective cohorts. Their separate exit-fit matrix uses the oldest 70% for fitting and newest 30% for calibration holdout. All 68 pair cells per family have a one-hour horizon and only 1–3 independent blocks; gross directional accuracy is null in every one of these cells. Therefore an honest answer cannot manufacture a per-family prediction hit rate from win rates.

Across the 511 holdout rows per family, after-cost win rates are 8.22% graph, 14.09% tabular, 17.42% state space and 8.61% ridge. Positive mean pair cells number 0/68, 3/68, 6/68 and 0/68 respectively; none is eligible. The full 272-cell population is preserved in the receipt. As one fixed common-pair comparison, all four EUR/USD holdouts have eight rows and three blocks; their respective mean net results are −2.1125, −3.9250, −2.2250 and −2.7375 pips. These small selected-model holdouts are not the full prospective cohort and must not replace it.

Across the complete current matrix, 544 of 11,658 observed cells have positive means and 277 have positive displayed lower bounds, but **zero eligible cells**. This broad search is why the few best cells cannot be treated as independent discoveries.

## Historical magnitude/clearance study and current opportunity evidence

The August 14 `executable_opportunity_ranking_v2_20260814` archive study explicitly declares `already_inspected_archive_engineering_discovery`, `proof_eligible:false`, a short archive, prior inspection of the validation period, modeled slippage and no untouched prospective outcomes.

| Horizon | Validation rows / clearance-positive rows | Clearance AUC | Clearance Brier | Constant validation-base-rate Brier | Direction on clearance-positive rows | Magnitude MAE |
|---|---:|---:|---:|---:|---:|---:|
| 5 minutes | 10,461 / 997 | 0.815 | 0.0737 | 0.0862 | 48.85% | 0.766 pips |
| 15 minutes | 3,543 / 855 | 0.749 | 0.1570 | 0.1831 | 48.19% | 1.278 pips |
| 30 minutes | 1,769 / 619 | 0.712 | 0.1994 | 0.2275 | 40.23% | 1.849 pips |

This suggests some **historical ability to distinguish larger, cost-clearing moves**, while direction on those moves remained poor. The baseline column is calculated from the validation prevalence itself; it is a hindsight descriptive reference, not a frozen deployable baseline. No naive magnitude MAE comparator is saved, so the absolute MAE values alone cannot establish improvement. Strict top-one and disjoint basket policies selected zero rows at every horizon; their no-trade comparator is zero net. There is no demonstrated profitable allocation in this study.

The current collection cohort `executable_opportunity_ranking_v2_20260814.collector.fdf810578a5acb2f`, as of September 5, has **zero forecasts and maturities**. Its separate seven-cohort historical diagnostic totals have 166,194 maturities across 3,904 decision epochs, **49.30% direction accuracy**, −3.014 raw mean predicted-side net pips and zero directional gate passes. They span recorded issue times August 9–28. These older cohorts cannot be pooled into current proof, and they do not rescue the historical directional result.

## Source-news predictions and comparison baselines

Source V8 has 903 factor/horizon forecast records, 326 non-abstaining, from only 13 events/10 underlying episodes across 1–120 minutes. Every event is labeled preactivation diagnostic, and there are **zero prospective proof forecasts**. Its issued records span August 30–September 1; its saved snapshot is September 5. Its `strengthening_rate` describes realized currency responses and is **not prediction accuracy**. No forecast-versus-actual accuracy statistic is supplied by that snapshot.

Rank V7 has **zero decisions, zero independent source episodes and zero matured outcomes** in all four intended arms: price-only, source-only, source-plus-price timing and no-trade. Thus it supplies no measured incremental benefit from news over price-only predictions. The repaired source V9/rank V8 successors remain inactive with no production evidence; tests of their chronology are engineering evidence, not predictive results. Old semantic/entry-clock defects and quarantine records must never be imported into successor proof.

## What the evidence supports improving

Keep a compact scorecard for each exact cohort, pair and horizon: forecast issue/publication clocks; frozen prediction and calibration version; direction hits including neutral outcomes; Brier and a frozen rolling class-rate comparator; magnitude error against zero-move and own-price-history comparators; executable net in normalized economic units; coverage/abstention; and independent event count. Compare source-only and source-plus-price against price-only and no-trade on the same available decision set.

The existing calibration replay can remain useful for diagnosing probability behavior, but credit any improved calibrated forecast only in a separately registered prospective study with label maturity enforced at original forecast issue. Similarly, promising historical clearance discrimination is a research lead, not an optimized trading system. Freeze the chosen change before obtaining a fresh sample, retain the losers and abstentions, and require enough independent confirmation rather than manufacturing more repeated rows.

The lifecycle remains **53,357 hypotheses, zero confirmed, 43,872 continuing and 9,485 futility rejected**. The honest present conclusion is that costs, weak direction, dependence and availability limitations prevent calling these predictions reliably useful.
