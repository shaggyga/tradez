# Rich-feature candidate reuse reconciliation

`rich_feature_family_incremental_comparison_v2` is already covered by the preserved
matched rich-family comparison and its paired dependence report. The original
comparison reused 28 baseline estimators, evaluated compact38/cost2, compact50/cost2,
and full228/cost2 groups, and preserved all 84 fitted rich models, 113,232 forecasts,
and 84 score groups. Its dependence successor reused those forecasts for 336 paired
comparisons, 5,616 origin panels, and 1,008 block sensitivities without fitting.

The retained suite includes maturity/population, preprocessing, future-data
perturbation, model/metadata/source guards, paired-score arithmetic and support tests.
It is development evidence only and has no independent confirmation claim. A duplicate
family fit would violate the Vault-first reuse rule; no rerun was performed.

The next distinct forecasting candidate is `distinct_forecast_method_comparison_v2`.
