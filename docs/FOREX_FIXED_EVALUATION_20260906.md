# Fixed EUR/USD forecast evaluation — September 6, 2026

The narrower test is implemented and was applied to a preserved extract of the actual project. On **368 matching archived observations**, all four existing models scored worse than simple probability and price-change baselines and had negative stored bid/ask results. **Zero of the 407 shared forecast decisions qualifies for the new strict causal comparison**, because original availability timestamps are missing. Forex remains stopped.

This is a bounded engineering evaluation of already inspected history. It does not establish prospective predictive skill or realizable trading performance. The new scorer cannot promote a strategy or start collection.

## Fixed scope and actual results

The comparison includes all four existing active families, EUR/USD, M1 inputs and the existing one-hour forecast horizon. Exact cohort and predictor/feature version identities are fixed in `config/fixed_forecast_evaluation_v1_20260906.json`. The window covers the active cohorts through shutdown, with actual extracted observations from August 30 through September 4. No family, horizon, threshold or parameter was chosen for a favorable result.

There are **1,628 forecasts: 407 per family**, with 1,543 recorded outcomes and **85 missing outcomes retained**. Of 407 original reference epochs, 22 lack one or more outcomes and 17 have differing endpoint clocks or prices. The remaining **368 epochs have identical entry and endpoint prices and clocks across all four families**. They overlap and span only six UTC days; this is not 368 independent trials.

| Original family | Direction correct | Probability Brier | Signed price-change MAE | Stored quote net, mean |
|---|---:|---:|---:|---:|
| Graph transfer | 42.39% | 0.3615 | 5.555 pips | −1.930 pips |
| Tabular | 47.55% | 0.2738 | 4.436 pips | −2.328 pips |
| State space | 47.83% | 0.3868 | 7.722 pips | −1.759 pips |
| Ridge | 44.29% | 0.3784 | 6.825 pips | −1.673 pips |
| Constant 50% probability | No direction | **0.2500** | — | No trading action |
| Zero price change | Neutral | — | **3.934 pips** | No trading action |
| No trade | No direction | — | — | **0 pips** |

Lower Brier and MAE are better. Brier compares the emitted up probability against whether the midpoint increased; flat is non-up and is also reported separately. Directional hits use the original emitted direction and count flats as misses. Price-change error uses the original signed expected movement. Mean stored spread drag is 1.645 pips. These values concern one pair, so there is no mixed-pair pip averaging here, but they are still not dollar returns or a portfolio equity curve.

The complete archive also has 82 forecasts whose emitted direction differs from the side favored by a 50% probability threshold. This can reflect a difference between expected return and movement probability. We retain both rather than silently replacing the original trading direction. No reversal, calibration fitting, threshold tuning or winning-family selection was performed.

## Why none is strict causal evidence

Every stored entry quote precedes forecast recording. Recording is a pending-list timestamp before database flush, not a certificate of consumer-visible publication. The median recording delay from the original reference is **84.63 seconds**, with a maximum approximately **2 hours 58 minutes**. No record supplies the required original issue, postcommit availability, feature first-availability, or training-label maturity/availability maxima.

The outcome clocks are also inconsistent with one fixed original target. The canonical worker produced **1,483 outcomes** at recording time plus one hour while retaining the earlier entry quote. Another **60 recovered outcomes** use a different provider-clock convention and lack local quote receipt times. Of the 368 matching archive epochs, 353 use the canonical convention and 15 the recovered convention. Their stored outcomes can be compared descriptively across models, but cannot be relabeled into a verified original-reference one-hour test.

These defects are recorded in the new open P1 `FX-20260906-FOUR-FAMILY-ENTRY-TARGET-AVAILABILITY`. Existing source and cohorts were preserved. The evaluator explicitly rejects missing provenance; it never substitutes recording time, row order, file mtime or a data cutoff for availability. Correcting these measurement gaps would not by itself prove that the models predict well.

## Implemented evaluation behavior

`oanda_fixed_forecast_evaluation.py` is a standalone, offline JSON evaluator. It requires all four exact cohorts and versions at a common decision, original issued/publication clocks, features available by issue, and training labels both mature and available strictly before issue. Every forecast binds its original reference midpoint and unchanged target. The economic comparison uses the first eligible executable quote after all four publications and exits at the original target within the fixed tolerance; entry delay cannot extend the forecast horizon.

The evaluator preserves original sides and abstentions. It reports coverage and explicit exclusions before scores. It calculates probability Brier, directional hits, signed-return MAE/RMSE, executable net in normalized basis points, and fixed extra cost stress of 0, 0.5 and 1.0 bps. It never sums overlapping simulated positions into portfolio performance or treats calendar blocks as independent sample size.

`oanda_causal_prediction_baselines.py` supplies constant 50%, zero move, no trade and a rolling class-rate baseline. The rolling baseline uses at most 100 unique prior matching market outcomes, with Beta(1,1) smoothing and a minimum of 20 labels; earlier or insufficient history falls back to 50%. Both maturity and availability must strictly precede the earliest original issue in the paired comparison. Repeated labels from four model families count once. Contradictory duplicates fail.

The rolling baseline is tested on controlled fixtures but **unavailable for this historical archive**, because its label-availability evidence is absent. Historical momentum is also unavailable without the original price windows and availability clocks. No timestamps or baselines were fabricated to fill these gaps. The strict run reports 1,628 forecasts, 407 shared decisions and **zero scored pairs**.

## Verification and reproduction

The combined guarded suite passed **158 tests**, with zero failures or skips. It covers future or simultaneous training labels, row-order changes, original target preservation, delayed/missing publication, price-anchor mismatches, side/probability disagreement, flat outcomes, abstentions, malformed quotes, conflicting identifiers, paired arithmetic, coverage, adapter provenance, canonical record sync and issue validation. Independent reviewers examined the scorer and data extraction. Root independently recomputed the 368-observation archive results and all 1,628 forecast payload hashes.

The source database was queried using an index, URI `mode=ro`, `query_only` and a single read transaction. No logical write, checkpoint, broker request or runtime start occurred. Double SHA-256 checks of the approximately 59.8 GB main database and 712 MB WAL matched; sizes and modification times stayed unchanged. SQLite may maintain ephemeral reader marks in SHM, whose byte identity is not claimed. The bounded query took about 1.1 seconds; complete duplicate hashing took about 307 seconds. Those are audit extraction costs, not runtime performance measurements.

The source archive includes the bounded original extract, normalized records, complete archive calculations, strict input/output, source/hash bindings and test evidence under `docs/validation/fixed_evaluation_20260906`. It excludes the full production database and credentials. Earlier dated audit, optimization and prediction receipts remain unchanged.

For offline reproduction from the source root, use a new output path:

```powershell
python -B oanda_fixed_forecast_evaluation.py --input docs/validation/fixed_evaluation_20260906/strict_input_final.json --protocol config/fixed_forecast_evaluation_v1_20260906.json --output ../evaluation_reproduction/strict_result.json
```

Expected result: `no_clock_valid_paired_decisions`, 407 shared decisions, zero scored. This verifies the preserved archive is rejected consistently; it does not produce new market data. To reproduce the archival arithmetic, copy `selected_candidate_rows.json` and `normalize_and_score_archive.py` to a disposable folder and run that script there. The source-database inventory scripts are historical acquisition records and are unnecessary for this replay.

## Remaining work and operating state

The evaluation code and archival comparison are complete. A separately versioned producer must capture true forecast visibility, feature/training-label availability, a subsequent executable entry quote and one original target before a new strict sample can be collected. Legacy calibration's issue-time availability issue also remains open; the new baseline helper does not modify that producer. A fresh prospective study must preserve these fixed comparisons and independent evidence requirements.

`FOREX_FIXED_EVALUATION_FOLLOWUP_20260906.json` records these prerequisites. `FOREX_FIXED_EVALUATION_VALIDATION_20260906.json` binds the reviewed source and evidence. The new configuration has collection, proof and account eligibility disabled. Forex workers and autostart remain stopped; no strategy was activated.
