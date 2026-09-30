# Curve-shape support check

Candidate: forecast_curve_shape_layer_comparison_v2.

Reusable sources were inspected before implementation:

- chronological_layer_v2.py already preserves exact all-68 coverage, original
  record/target identities, explicit maturity, and per-UTC-day matched diagnostics.
- magnitude_layer_v2.py supplies the existing regularized signed-only and
  magnitude-interaction controls, with prequential training-membership guards.
- later_surface_layer_v2.py confirms saved base predictions and applies causal
  snapshots, but its rows are scoped one horizon at a time.
- curve_capacity_* measures retained whole-curve execution capacity; it does not
  provide a forecast-ablation consumer or labels for curve shape.

There is no existing cross-horizon feature consumer to reuse. A new bounded consumer
is therefore justified only if it joins exact original instrument/origin/base records
across horizons, admits each feature after all constituent forecasts are available,
and uses the existing regularized layer machinery without base refits.

No computation, base-model fitting, service call, policy replay, or live-bot action
was performed by this support check.

Implementation now started: curve_shape_layer_v2.py joins only complete contemporaneously
available horizon panels, retains a target-ID map for every constituent horizon, and
uses the existing weighted Ridge implementation. Synthetic tests cover causal fitting,
cross-horizon target identity retention, and no-imputation refusal. This is not yet an
authenticated retained-data run or checkpoint.

Authenticated retained-data diagnostic completed using the sealed 195-payload parent:
2,156 complete curve panels, 48 snapshots, 946 issued learned rows, and 1,210 explicit
insufficient-support coverage rows. The output is CURVE_SHAPE_RESULT.json. This remains
an unreviewed offline development diagnostic; all-68 reporting, paired scoring and its
checkpoint packet are still required before completion.
