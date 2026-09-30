# Forex prediction sanity check and joint-movement review — September 6, 2026

The saved numbers mostly reproduce, with one small numerical correction. They
show many correct individual directions, but do not demonstrate reliable useful
forecasting. This review independently checked raw rows, model implementations,
artifact locations and relationships between both currencies and model outputs.
No model was fitted or activated, and the registered collection study was unchanged.

## Broad signal ledger: reproducible counts, small flat-price correction

An indexed read-only query returned the exact 8,414 mature, maturity-valid rows
of `raw_all_signal_consensus_v5_strict_horizon_lineage`. It reproduced 4,156
stored positive direction results (49.39%) and 716 positive after-spread results
(8.51%). The main database and WAL hashes stayed unchanged during extraction.

Independent decimal arithmetic on the retained bid/ask prices found 18 moves
that are exactly flat but appeared infinitesimally nonzero in floating-point
arithmetic. Eight of those had been counted as direction hits. The corrected
diagnostic is **4,148/8,414 = 49.30%**, with 296 flat outcomes. All 716 positive
after-spread outcomes remain positive, so their percentage remains **8.51%**.
The original dated receipts and stored results are preserved.

| Interpretation of the same rows | Result |
|---|---:|
| Exact direction hits, flats counted as misses | 4,148 / 8,414 = 49.30% |
| Exact direction hits, excluding 296 flats | 4,148 / 8,118 = 51.10% |
| Fair random direction, expected hit rate with flats counted as misses | 48.24% |
| Positive executable bid/ask difference | 716 / 8,414 = 8.51% |

The nonflat rate is an explicitly different denominator, not a replacement
chosen to make performance look better. Below 50% alone does not establish
worse-than-random skill when flats count as misses. Neither rate establishes
statistical significance or a profitable trading rule. These are simulated
signal outcomes, not executed account trades.

The 8,414 rows reuse 6,465 aggregate signal IDs and 2,837 source signal IDs;
79.55% use one-to-three-minute horizons. They are not independent trials.
Although all retained provider target quotes meet the implemented 0–60-second
tolerance, 624 entry quote times precede their signal snapshot times, 1,907
signals are more than 90 seconds old at the entry quote, and 27 signal snapshot
times are at or after the original target. Original committed publication and
local quote-receipt clocks are absent. Reproducible arithmetic does not repair
these historical timing limitations.

The exact-price correction is implemented in a separate offline calculation.
The broad producer and the registered strict scorer still use floating midpoint
arithmetic; a scoring precision follow-up is logged before any performance
acceptance. Captured bid/ask prices permit separately versioned rescoring without
rewriting forecasts. This review did not silently alter the frozen study.

## Four-model EUR/USD comparison: all percentages confirmed

Independent raw-byte and decimal recomputation verified all 1,628 forecast
payload hashes, 407 shared reference times and 368 matched endpoints. The 22
missing-outcome and 17 mismatched-endpoint exclusions reproduce. No sign,
pip-conversion or quote-net error was found in this fixed EUR/USD comparison.

| Existing family | Correct directions | Accuracy | Probability Brier |
|---|---:|---:|---:|
| Cross-pair graph transfer | 156 / 368 | 42.39% | 0.3615 |
| Pooled tabular | 175 / 368 | 47.55% | 0.2738 |
| State space | 176 / 368 | 47.83% | 0.3868 |
| Ridge | 163 / 368 | 44.29% | 0.3784 |

Lower Brier is better; a constant 50% probability scores 0.25 on this binary
target. All four models remain worse on this archive, with negative mean stored
quote-net outcomes. There were 172 up, 195 down and one flat endpoint. An always
sell direction would score 52.99% descriptively on these same rows; this is a
hindsight comparator, not a selected strategy.

The tabular model's expected-return side differs from its majority-probability
side on 74 matched rows. They are distinct outputs; replacing one with the other
would change the metric rather than fix a demonstrated sign bug. These six
already inspected UTC days contain overlapping outcomes and retain the known
entry/publication/target-clock defects. The archived percentages are useful
diagnostics, not verified prospective execution performance.

## Which models are missing, dormant or simpler than their names suggest?

The current study runs per-pair ridge, pooled histogram gradient boosting,
currency-factor ridge and EWMA drift/variance. The current graph label describes
currency aggregates plus lag features, and the state-space label describes an
exponential drift filter. Neither name means every advanced graph or regime
model in the catalogue is active.

The most relevant implementation gaps found in inspected source are:

- VAR/VECM/cointegration baselines for synchronized currency baskets.
- A fitted dynamic-factor or hidden-state regime forecaster.
- A current four-family ensemble that measures forecast-error dependence and
  learns weights only from labels available before each original issue.

Existing work includes a currency lead-lag ridge study, currency-state
reconstruction and covariance, historical StemGNN training, and several voting
or weighted-ensemble experiments. The saved lead-lag net results are negative.
The hand-built correlation graph in the StemGNN benchmark is returned as
diagnostics rather than passed into that model. Currency reconstruction
covariance is different from model forecast-error covariance.

The larger catalogue is not missing from the computer. Eight referenced
historical reports on `D:\forex` match their recorded hashes, as do four tabular
checkpoints. Relocated TFT/DeepAR checkpoints and seven foundation-model weight
destinations/manifests also exist. Large weight hashes and package imports were
not rerun. These research assets are dormant and unwired to the current C-drive
study; their presence does not establish current readiness. Mamba and TimesFM-ICF
remain the project's recorded runtime blockers. See the [full model inventory](FOREX_MODEL_INVENTORY_20260906.md).

## What is moving together?

The fixed August 30–September 4 candle window contains 6,904 common minute bars
across seven majors. After excluding gaps, 6,739 one-minute returns remain.
Returns were oriented consistently so positive means non-USD strength versus
USD. EUR/USD and GBP/USD correlation was **0.840**; EUR versus CHF relative to
USD was **0.822**. The first standardized component explained **71.19%** of
same-time variance. Shared USD exposure can create much of this common movement.
This percentage is not forecast accuracy or future explained variance.

All 35 fixed pair/lookback lead-lag cells are retained in the evidence, using
1/5/15/60/120-minute past returns against the next EUR/USD hour. No favorable cell
was selected. Only 68–87 hourly observations survived the varying continuity
requirements, so this week cannot establish a stable leading relationship.

Model output co-movement is different. Graph and ridge probabilities correlated
0.617; other model-probability correlations ranged from -0.212 to 0.083. In the
368 matched archive decisions, all four agreed on 40 occasions and were right
on only **9/40 (22.5%)**. At least three agreed on 228 occasions, with **92/228
(40.35%)** correct. Equal probability averaging had Brier **0.2966**, still worse
than 0.25. These are retrospective small slices, not grounds for inversion,
threshold selection or a new trading strategy.

[View the currency and model correlation chart](validation/prediction_sanity_20260906/comovement/comovement_sanity.svg).

## Next research priority and evidence preservation

The proposed order is exact-price scoring; synchronized, normalized returns;
own-price versus peer-lag/leave-target-out currency-factor ablations; then
regularized VAR/cointegration and fitted factor/regime candidates. Any ensemble
should use genuinely prior labels and test improvement on the same future
decisions after costs. The new research plan is proposed and inactive.

Independent reviewers verified both the original four-model arithmetic and all
98 correlation-matrix cells plus 35 lead-lag cells. The broad exact counts also
passed a second offline implementation. Frozen source rows, calculation scripts,
inventory evidence, plots and hashes accompany the validation receipt under
`docs/validation/prediction_sanity_20260906`. `broad_signal/selected_rows.txt`
preserves the exact JSONL bytes in a source-archive-supported extension.

No historical receipt was rewritten, no order was placed and no model source,
configuration or registered study was changed by this review. Collection remains
research-only. A causal fresh sample and independent after-cost acceptance are
still required before claiming improved predictions.
