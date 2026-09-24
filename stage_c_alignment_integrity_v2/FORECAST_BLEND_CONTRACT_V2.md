# Retained equal-weight forecast blend diagnostic v2

This package creates one fixed, assessment-only diagnostic from preserved Ridge and
recovered-HGB forecast records. It retains all 68 original instruments, seven elapsed
endpoint targets, 20 origins, and both frozen/adaptive procedures. It does not fit or
deserialize a model, call an external service, issue a forecast, choose a weight, or
promote a result.

Freeze `FORECAST_BLEND_OPERATOR_RECIPE.json` with
`freeze_forecast_blend_recipe_v2.py` before execution. The operator checks that frozen
recipe against exact source hashes, the pinned calibration recipe, and the two registered
predecessor paths before importing the runner. A result can only join exact matched base
records by original record, target, decision, procedure and selected-fit identity. Missing
or mismatched partners stay explicit coverage or cause refusal; no base forecast is
reconstructed.

Assessment labels are kept out of blend payloads and may only be used when their original
availability and label-end clocks are no later than the frozen assessment cutoff. The
reported moving-block intervals are descriptive sensitivity only. They do not establish
an effective sample size, p-value, winner, economic claim, forecast confirmation, policy
readiness, or demo authorization.
