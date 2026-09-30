# Residual comparison reuse reconciliation

The current queue card `currency_projection_residual_layer_comparison_v2` was checked
against its existing source and evidence. It is already implemented by
`CURRENCY_PROJECTION_RESIDUAL_REPAIR_20260925_000700`, so no duplicate run was made.

The authenticated result has 3,786 learned forecasts and 126 paired score rows. It
compares the learned residual prediction with direct, currency-projection, and
half-residual controls on matched support, with aggregate, origin, and UTC-day
metrics. The existing focused tests include future-outcome perturbation and
chronology/maturity failures. This is retrospective development evidence only;
independent scientific review remains pending and is recorded separately.

The queue needs a later pointer reconciliation, but that bookkeeping is not a reason
to rerun this completed experiment. The next distinct eligible sibling is
`forecast_curve_shape_layer_comparison_v2`.
