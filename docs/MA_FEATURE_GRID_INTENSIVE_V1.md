# Intensive Moving-Average Grid

## Purpose

This experiment tests whether the existing 643-feature moving-average surface
contains repeatable directional or after-spread information. It does not assume
that a crossover is a signal. SMA/EMA distances, slopes, curvature, fan
geometry, pairwise gaps, crossover age, and support are inputs to the fitted
models.

The incumbent 610-cell artifact remains unchanged and shadow-only. Intensive
challengers are written to separate model and report directories.

## Surface

- Instruments: all 68 OANDA FX pairs, plus a separate 16-liquid-pair challenger.
- Input timeframes: M1, M5, M15, H1.
- Outcome horizons: 60, 300, 900, 3,600, 14,400, and 86,400 seconds.
- Models: regularized Ridge and CUDA XGBoost.
- Targets: probability-up, signed pips, absolute movement, and optional
  executable bid/ask edge.
- Pair context: pair, base currency, and quote currency indicators.
- Target normalization: local MA movement scale, fitted on development only.

The sampled rows are uniformly distributed over each pair's full available
history. They are not merely the newest observations. Sampling is necessary
because materializing every bar across 68 pairs and 643 features would exceed
the machine's working memory; the source outcome period itself is not
truncated.

The all-pair estimators share a fingerprinted derived-dataset cache so Ridge and
XGBoost see identical rows. The cache is invalidated when a source file's size
or modification timestamp, the feature schema, the timeframe/horizon surface,
or the sample contract changes.

## Validation

The intensive challengers use one global calendar split:

1. Development: first 60%.
2. Validation: next 20%.
3. Holdout: final 20%, untouched until scoring.

Rows are purged when their maximum-horizon outcome crosses the development or
validation boundary. Calibration, direction thresholds, and movement/cost
entry thresholds are fitted on validation and applied unchanged to holdout.

The movement gate uses only forecast-time values:

`predicted absolute movement / decision-time spread`

It does not use the future exit spread to select observations. Executable
outcomes still use recorded entry and exit bid/ask prices where available,
falling back to the pair's median observed spread only when necessary.

## Evidence

- Direction accuracy: share of correct future midpoint signs.
- Balanced accuracy: equal weight for up and down classes.
- Macro direction accuracy: equal weight for each pair.
- Pair lower 95% bound: lower confidence bound across pair-level averages.
- Within-pair correlation: correlation after removing each pair's mean; this
  prevents different natural pip scales from masquerading as timing skill.
- Executable net pips: realized side result after recorded or proxy spread.
- Net cost units: executable net divided by decision-time spread, used to stop
  naturally high-volatility exotic pairs from dominating pooled raw-pip ranks.
- Movement gate: validation-fitted magnitude/spread and confidence filter,
  evaluated unchanged on holdout.

Row-pooled binomial bounds are diagnostic only because neighboring forecasts
are dependent. A model must replicate across validation and holdout with pair
breadth and positive after-spread lower bounds. Prospective paper replication
is still required before any account eligibility change.

## Reproduction

The machine-readable experiment definition is:

`config/ma_feature_grid_intensive_v1.json`

The comparison audit is generated with:

```powershell
python oanda_ma_feature_grid_intensive_audit.py
```

Canonical outputs are under:

- `data/oanda_training_manager/reports/ma_feature_grid_intensive_*`
- `data/oanda_training_manager/models/ma_feature_grid_intensive_*`

## Status

Completed on 2026-07-27:

- Six matched challengers: Ridge/XGBoost, context/no-context all-pair controls,
  and Ridge/XGBoost liquid-pair controls.
- 144 fitted challenger cells: four timeframes by six horizons by six models.
- All-pair retained rows: approximately 68,000-76,000 per timeframe.
- Liquid-pair retained rows: approximately 27,000-31,000 per timeframe.
- All-pair cache: 0.565 GB.
- Liquid-pair cache: 0.227 GB.
- Strict direction-and-cost replications: 0.
- Strict movement-gate replications: 0.

The highest near miss was the all-pair, pair-context XGBoost `H1 -> 24h`
movement gate. It had 1,018 validation and 925 holdout entries. Its gated macro
direction lower bounds were 51.62% and 59.03%, and its pair-level
net-per-spread lower bounds were +1.19 and +2.85. It failed because the
validation raw-pip pair lower bound was -24.42 pips.

The saved model and cache were independently replayed at pair level; all five
aggregate checks matched the report exactly. The positive averages were
concentrated in high-volatility exotic pairs, especially EUR_HUF, USD_HUF,
EUR_ZAR, CHF_ZAR, and related ZAR/HUF instruments. The denser 16-liquid-pair
XGBoost control did not reproduce the result: its `H1 -> 24h` holdout macro
direction lower bound was 48.59%, its pair-level net-per-spread lower bound was
-2.67, and its profit factor was 0.84.

All challengers remain research-only. No MA model is automatically connected
to an OANDA account by this experiment.

Primary evidence:

- `data/oanda_training_manager/reports/ma_feature_grid_intensive_audit/MA_FEATURE_GRID_INTENSIVE_AUDIT_LATEST.md`
- `data/oanda_training_manager/reports/ma_feature_grid_intensive_audit/pair_diagnostics_h1_86400_latest.json`
- `data/oanda_training_manager/reports/ma_feature_grid_intensive_audit/intensive_run_latest.json`

## Completion Receipt

- Focused regression suite: 129 tests passed.
- Parameterized strategy cases: 219 subtests passed.
- Modified Python modules: compilation passed.
- Six fitted artifact hashes: all match their recorded report hashes.
- Pair-level `H1 -> 24h` recreation: all five aggregate checks match.
- Account-eligible horizons across all six challengers: 0.
- D volume after the run: NTFS healthy/OK, not dirty, 3.62 TB free.
- New disk Event ID 7 errors during the intensive window: 0.
