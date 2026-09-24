# Magnitude-conditioned signed forecast layer

This tests whether saved direct movement-size forecasts improve signed endpoint
forecasts beyond a signed-only recalibration. It is a paired ablation of the same
prequential training population, not another fit of the technical base models.
The frozen contract is `MAGNITUDE_LAYER_CONTRACT_V2.json`.

For each base learner, horizon and base update procedure, signed-only Ridge uses one
feature: its original signed forecast. The augmented Ridge adds the corresponding
direct absolute-movement forecast and their product. Both use the same intercept,
penalty20, training-only weighted normalization and equal total weight per origin.
Weights sum to the number of training rows. No setting is selected from the scores.

Original signed and magnitude records must match instrument, origin, elapsed target,
selected original fit, cutoff, readiness/publication clocks and signed source hashes.
The magnitude target remains distinct from the signed target. Neither the magnitude
nor a ratio of forecasts is a probability or executable net return.

Only earlier original forecasts whose endpoint outcomes have matured by the layer
cutoff can train a layer. Minimum support is eight distinct origins, three UTC days
and20pairs, reused from the existing qualified calibration contract. Frozen and
expanding prefixes retain separate results. Unavailable layers produce explicit
coverage reasons; they never fall back silently to another method. Production
availability is null and native policy admission is false.

## Operator and restore

Use the exact Python/package environment and SHA256 in the approved recipe. Run
`magnitude_layer_operator_v2.py status|run|resume|verify --recipe <recipe>
--recipe-sha256 <pin> --paths <PATHS.json> --runs-dir <directory>`.
The paths file contains `joint`, `technical`, `absolute` and `baseline_metadata`.
The first three are complete authenticated runs; the last is the existing explicitly
selected signed-metadata capsule. Source, environment and input drift refuse before
runner import. Model pickle files in the absolute input are hashed, never loaded.
Completed chunks are reused during resume; their layer coefficients are not fitted
again. A completed run is read directly.

`magnitude_layer_checkpoint_v2.py export --package <zip> --paths <paths>
--runs-dir <runs>` exports exact source and original inputs. Restore with
`restore --package <zip> --sha256 <pin> --destination <new-directory> --run-tests`.
This deterministic verification recomputes the small layer regressions from saved
base predictions and compares58scientific payload hashes. It does not refit or load
any original signed or magnitude model. Operational reuse should read the completed
run. The capsule is bounded to384members,128MiBcompressed,384MiBexpanded and8MiBper
input member. Its complete original absolute-run inventory includes saved weights
solely so its declared completion hashes can be verified.

## Evidence limits

All seven elapsed horizons, both base procedures, both learners and both layer
update modes remain visible. Scores include paired MAE/MSE/bias, nearest-rank95th
absolute error, per-origin/day strata and descriptive circular origin-panel
sensitivity. Sparse, irregular or single-full-block support receives no resampling
interval. Raw rows are not independent observations across shared currencies and
overlapping horizons. All dates remain previously inspected development; this is
neither protected confirmation nor evidence of policy profit. The Vault packet
records actual results and the exact next candidate. GPT/advisor work stays deferred.
