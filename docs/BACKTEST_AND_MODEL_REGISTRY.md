# Backtest And Model Registry

## Current governance reconciliation — 2026-08-27

The material below this section is retained historical research lineage. It is
not an active model authorization and must not be numerically combined with the
current prospective cohorts unless code, features, labels, horizons, sampling,
and executable costs are reproduced exactly.

Current operational disposition:

- Practice-007 remains `no_trade`; the lifecycle has zero confirmed candidates.
- The July `fresh_fullhist` bundles and M1 overlay are historical replay
  references, not current execution authority.
- The four immutable governed proof families (ridge, modern tabular,
  graph-transfer, and probabilistic state-space) continue under their frozen
  prospective contracts until cell-level sequential confirmation or futility.
- The four-family H1 baseline is statistically exhausted and now runs
  `mature_only`: it publishes no new forecasts and is retained as a negative
  control after its pending horizons resolve.
- The executable-opportunity ranking v2 stream is also `mature_only` after
  169,376 forecasts produced zero directional frozen-gate passes and averaged
  -3.014 pips on the predicted side after costs.
- HGB live-outcome tracking from stale D-drive/account-019 input and the frozen
  manager-decision outcome poller are retired with their artifacts preserved.
- The content-hashed current case-history inventory is
  `data/oanda_training_manager/reports/HISTORICAL_CASE_INDEX_CURRENT.json`.

The authoritative runtime disposition is recorded in
`config/shadow_runtime_retirements_v1.json`; current implementation evidence is
in `SOURCE_MODEL_RUNTIME_CLEANUP_AUDIT_20260827.md`.

This document ties the active model stream to its replay evidence and explains
how to interpret the headline numbers.

## Historical July stream

```text
fresh_fullhist_m30_h1_h4_continuation_oanda_20260707
```

The stream combines three full-history model bundles:

| Bundle | Feature timeframe | Feature set | Target | Execution |
|---|---:|---|---|---|
| `hgb_m30_fullhist_reversal_120_continuation_exec_fullhist` | M30 | `technical_full` | `reversal_curve_profit_120` | `follow_momentum` |
| `hgb_h1_fullhist_reversal_120_continuation_exec_fullhist` | H1 | `technical_full` | `reversal_curve_profit_120` | `follow_momentum` |
| `hgb_h4_fullhist_reversal_120_continuation_exec_fullhist` | H4 | `technical_full` | `reversal_curve_profit_120` | `follow_momentum` |

All three point to experiment:

```text
data/oanda_training_manager/continuous_research/experiments/exp_20260702_022306_e09b53ec8d.json
```

## M1 Overlay

The active canary uses:

```text
data/oanda_training_manager/reports/m1_primary_overlay_fresh_fullhist_maxnew8_jump_narrow_20260707/m1_primary_overlay_model.joblib
```

The current filter requires:

```text
m1_hold_prob >= 0.50
```

## Key Replay Artifacts

| Artifact | Purpose |
|---|---|
| `data/oanda_training_manager/reports/path_quality_current_m1_hold050_strict_20260708/sweep_summary.csv` | Baseline current/live-like strict path replay. |
| `data/oanda_training_manager/reports/path_quality_m1_hold050_wide_pacing_sweep_20260708/sweep_summary.csv` | Wide-stop pacing/exit sweep used to select the canary config. |
| `data/oanda_training_manager/reports/path_quality_m1_hold050_wide_friction_stress_20260708/sweep_summary.csv` | Spread/slippage stress test for the selected family. |
| `data/oanda_training_manager/reports/path_quality_inputs_20260708/candidate_rows_m1_oos_hold_ge_050.csv` | M1-filtered OOS candidate rows used in the strict replay. |

## Selected Variant

```text
all__gate_live__friction_live__stops_wide__risk_live__new_12__exit_exact_config
```

Summary from `$1,000` start equity:

```text
opened_trades: 2350
return_pct: 3922.7
path_adjusted_return_pct: 3701.6
win_rate: 92.9%
clean_win_rate: 92.0%
deep_adverse_trade_share: 2.94%
rescued_winner_share: 0.94%
p95_mae_r: 0.420
max_open_risk_pct: 9.45
max_margin_used_pct: 82.47
margin_call_rows: 0
```

Max-new 16 had a slightly higher path-quality score, but the gain was too small
to justify using it as the default canary when max-new 12 keeps slightly lower
throughput pressure.

## Why `signal_no_longer_positive` Can Have A High Win Rate

`signal_no_longer_positive` does not mean the trade was wrong from entry. It
means that, when the bot checked the position, the current forecast was no
longer positive enough to keep holding.

In the selected replay, most of those exits were already profitable because the
trade had captured part of the move before the signal faded:

```text
signal_no_longer_positive trades: 477
win rate: 98.1%
average result: +0.082R
deep-adverse trades: 9
```

So that exit reason acts more like a profit-preserving fade exit than a pure
loss-control exit. This is why it can show a very high win rate.

## Important Interpretation

The replay trade win rate is not the same thing as model endpoint accuracy.

For the selected wide-new12 run:

```text
trade win rate: 92.9%
clean trade win rate: 92.0%
executed endpoint-right rate at 120m: 61.8%
executed quick 30m success: 77.0%
executed hold 120m success: 72.9%
```

This strategy is a trader-style selection/rotation system. It can win trades by
harvesting favorable movement inside the forecast window even when the endpoint
forecast is not clean.

The live risk is the reverse case: the forecast can be directionally right later
while the trade loses because the path goes adverse, the exit fires at the wrong
time, or the position is held beyond the useful window. That is why path-quality
metrics are tracked separately from raw win rate.

## Path-Quality Terms

| Term | Meaning |
|---|---|
| `mae_r` | Maximum adverse excursion divided by initial risk. |
| `mfe_r` | Maximum favorable excursion divided by initial risk. |
| `deep_adverse` | Trade moved at least `0.5R` against the position at some point. |
| `rescued_winner` | Trade closed profitable after being `deep_adverse`. |
| `clean_winner` | Trade closed profitable without being `deep_adverse`. |
| `path_adjusted_pnl` | Penalized PnL that treats deep-adverse rescued trades more conservatively. |
| `path_quality_objective` | Sweep ranking metric combining path-adjusted return, profit factor, clean win rate, drawdown, MAE, and rescued/deep-adverse penalties. |

## Regeneration Notes

Use the research Python runtime:

```text
data/oanda_training_manager/.research_py313/Scripts/python.exe
```

Plain `python` is not the supported runtime for this package.

## Moving-Average Crossover Sweep

The 2026-07-26 crossover audit is generated by:

```text
oanda_moving_average_crossover_sweep.py
```

Full-history artifacts:

```text
data/oanda_training_manager/reports/moving_average_crossover_sweep/all68_wide_20260726
```

Recent exact-bid/ask confirmation:

```text
data/oanda_training_manager/reports/moving_average_crossover_sweep/all68_recent_exact_20260622_20260717_20260726
```

The full run tested 10,440 configurations across all 68 pairs, nine input
timeframes, four MA type pairings, 58 period pairs, five confirmation modes, and
ten horizons. No configuration passed the pair-majority, positive-net, and
above-50%-win gate in both development and validation.

The exact-cost confirmation tested 2,400 focused configurations. Twelve
24-hour rows also passed holdout, but no 1-240 minute row passed. These are
shadow-only higher-timeframe context candidates. The current M1 EMA crossover
profiles are not supported as standalone fast-entry signals.

Read `ASSESSMENT.md` in the full-history artifact directory before using the
ranked CSVs. Positive pooled pips can be driven by rare exotic-pair moves and do
not imply a high-win or account-executable strategy.

## Moving-Average Crossover Entry Quality

The endpoint sweep is complemented by an executable path study:

```text
oanda_moving_average_crossover_entry_quality.py
```

Exact bid/ask run:

```text
data/oanda_training_manager/reports/moving_average_crossover_entry_quality/all68_recent_exact_entry_20260622_20260717_20260726
```

Full-history focused refinement:

```text
data/oanda_training_manager/reports/moving_average_crossover_entry_quality/all68_full_history_entry_refinement_20260726
```

The study measures MFE, MAE, spread recovery, the opposite direction at the
same timestamp, and conservative first-touch profit/loss barriers. Same-M1-bar
target/stop collisions count as losses.

No configuration passed the development/validation entry-quality gate in
either run. The current M1 EMA crossover profiles became executable-positive
about 22% of the time within five minutes and 61%-62% within 60 minutes, but the
crossover side produced the larger excursion only about 19% and 42% of the
time, respectively. Binary crossover events remain features, not standalone
entry triggers.

## Multi-Timeframe Crossover Stacks

Extended and mixed crossover stacks are generated by:

```text
oanda_multitimeframe_crossover_stack_sweep.py
```

Exact-cost artifacts:

```text
data/oanda_training_manager/reports/multitimeframe_crossover_stack/all68_extended_exact_20260622_20260717_20260726
```

The historical hierarchy now supports M1, M2, M3, M4, M5, M6, M8, M10, M12,
M15, M20, M30, M45, H1, H2, H3, H4, H6, H8, H12, and D1. M1 is the lower
bound for this recovered candle panel; sub-minute stacks require the separate
seconds dataset.

The 2026-07-26 exact run tested 4,604 configurations. A primary crossover could
run alone or require one or two completed context states, and context states
could use different MA definitions. No stack passed the full entry-quality
gate. Some contexts produced small, durable improvements over their primary
controls, but the maximum all-split lift was 2.54 percentage points and the
resulting absolute directional dominance remained below 50%.

Use stack state, strength, age, and disagreement as continuous model features.
Do not treat hard multi-timeframe agreement as an independently validated
execution signal.

## Wide SMA Candidate Filter

Moving-average state is also evaluated as a meta-filter over signals emitted by
the existing strategy and forecast families:

```text
oanda_sma_signal_filter.py
oanda_sma_signal_filter_fit.py
```

The filter does not emit a direction. Each candidate's proposed side is fixed
first, then the feature builder conditions 1,501 SMA fields on that side. The
net includes periods 2 through 200 where the live history supports them, input
timeframes M1, M5, M10, M15, M30, H1, H2, H3, and H4, and:

- price-to-SMA distance and one/three-bar SMA slope;
- adjacent and canonical SMA gaps;
- crossover age, separation velocity, and recent-cross support;
- within-timeframe ordering and dispersion;
- cross-timeframe alignment and disagreement.

The live collector stores one compressed feature vector per
instrument/completed-bar/direction state and references it from every real
candidate. Later executable bid/ask outcomes are attached at each configured
horizon. This avoids repeating the 1,501-field vector for every family and
horizon.

Historical fitting uses the real shadow signal ledger. Bars are available only
after their close, events sharing the same market state receive a combined
sample weight of one, and each horizon uses an oldest-60% training, next-20%
validation, newest-20% untouched holdout split with a horizon-length purge.
Threshold selection occurs on validation only.

An SMA score is neutral in the account signal matrix unless the newest holdout
has positive average and lower-confidence executable net pips, majority wins,
positive baseline lift, adequate support, and replication across independent
time blocks and pairs. A validated rejection removes that component from
eligible consensus; an unvalidated score remains visible shadow evidence.

Canonical artifacts:

```text
data/oanda_training_manager/state/sma_signal_filter_v1.json
data/oanda_training_manager/state/sma_signal_filter_v1.joblib
data/oanda_training_manager/reports/sma_signal_filter/all68_signal_candidates_20260726
```

The 2026-07-26 full fit covered 258,243 executable signal outcomes, 14,206
unique entry snapshots, and all 68 pairs. No horizon was promoted. The
60-second model found a 2.79%-coverage holdout slice averaging +0.839 pips
against a -17.272-pip unfiltered baseline, but its validation average was
-2.264 pips, its holdout lower confidence bound was -0.914 pips, and its
holdout win rate was only 33.9%. It also failed pair and time-block
replication. Selected holdout averages at 300, 900, and 3,600 seconds remained
negative (-5.171, -3.017, and -17.977 pips). The canonical model therefore
loads in shadow mode and contributes a neutral account weight.
