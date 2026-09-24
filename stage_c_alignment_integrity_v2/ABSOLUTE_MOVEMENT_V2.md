# Absolute endpoint movement comparison

This implements the design's separate movement-size target. It estimates absolute
midpoint endpoint return in basis points, using the original 26 technical features,
all 68 instruments, seven elapsed horizons, two fit cutoffs and frozen/adaptive
procedures. It does not estimate continuous path movement, calendar sessions,
directional confidence, executable profit or policy utility.

`ABSOLUTE_MOVEMENT_CONTRACT_V2.json` was frozen before new fits and scores. Ridge and
HGB reuse existing fit code and unchanged hyperparameters. Each fit checks its exact
eligible record population and train-only feature statistics against authenticated
original signed-model metadata. Only mature labels are transformed at fit time.
Direct magnitude predictions use the frozen `max(0, raw)` projection; raw predictions
and clipping counts are retained. Controls are zero, training-only absolute mean
and median, and absolute values of the original saved signed predictions.

Timing is a counterfactual replacement of original legacy26 reservations in the
single-worker schedule. It preserves original selected cutoffs, fallback behavior,
readiness gaps and forecast publication times; it does not add parallel capacity or
claim historical issuance. Both new estimators must fit within the original 30-second
pair slot; each inference batch must fit its original two-second slot. Prepared input
ingestion and live operating-system load are outside those measurements. Native policy
admission and production availability remain unset.

## Operator

Use the pinned Python environment recorded in the recipe. `PATHS.json` has exactly
`joint`, `technical` and `baseline_metadata`. The first two are complete original runs.
The last is an explicitly selected metadata capsule with the original identity and
manifest; it does not claim to contain or revalidate absent original model weights.
The initial intake separately verified the complete original signed run.

Run `absolute_movement_operator_v2.py status|run|resume|verify` with `--recipe`,
`--recipe-sha256`, `--paths` and `--runs-dir`. The operator validates source, environment,
input identity and consumed bytes before numerical imports or model loading. A source
or recipe mismatch requires a reviewed successor; never silently regenerate approval.
`--reuse-only` refuses any missing saved fit rather than retraining. A completed run is
reused directly. Attempt receipts distinguish actual fits from the 28-model inventory.

## Checkpoint and recovery

`absolute_movement_checkpoint_v2.py export --package ... --paths ... --runs-dir ...`
creates a capsule containing exact source/recipe, original inputs, selected original
metadata, 14 new saved fit pairs, and their original timing evidence. The explicit
schema bounds it to 320 members, 128 MiB compressed, 384 MiB expanded and 8 MiB per
member. Existing fixture limits are unchanged.

Use `restore --package ... --sha256 ... --destination ... --run-tests` to authenticate
and extract into a new directory, seed saved models, and replay with `--reuse-only`.
The restore must report zero new-target refits and 14 saved fit pairs reused. All
scientific payload hashes must match. Fit timings are preserved; new inference and
resource timings are measured separately. Synthetic tests intentionally fit small
fixtures; they are separate from historical model reuse.

## Assessment and next work

Each horizon/procedure reports all seven methods on identical mature support: MAE,
MSE, ten direct-versus-control differences, and per-origin/per-UTC-day errors. Missing
outcomes and readiness exclusions remain visible. Shared currencies and overlapping
horizons prevent interpreting rows as independent evidence. This is a previously
inspected development experiment; there is no protected confirmation set or selected
winner. Same-task review and independent review remain separate statuses.

The Vault review packet supplies actual results and the next accepted queue item.
Before using magnitude in position rotation, freeze a separate signed/magnitude
compatibility and policy-utility contract. Magnitude by itself supplies no direction
or expected net return. GPT/advisor comparisons and paid/broker/service work remain
deferred.
