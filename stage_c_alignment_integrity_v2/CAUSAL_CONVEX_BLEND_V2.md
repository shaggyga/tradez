# Causal convex forecast layer

This fixed experiment learns one constrained scalar Ridge/HGB combination from saved
prequential forecasts, separately by horizon and base procedure. It reuses the residual
calibration package's eight-origin, three-day, twenty-instrument support rules and its
frozen/expanding prefix schedules. All68 instruments and seven elapsed targets retain
explicit coverage; unsupported scopes produce no learned prediction or fallback.

Training requires an earlier forecast origin and base fit, a forecast available before
its endpoint, and a label matured by the training cutoff. The analytic least-squares
weight is clipped to[0,1], with equal total weight per origin and no intercept. All
training member hashes and model identities are retained. Outcomes are assessment-only
and never inserted into original layer prediction payloads. A later untouched cohort
is required for confirmation; these records are already inspected development evidence.

Use `causal_convex_operator_v2.py status|run|resume|verify --recipe <recipe>
--recipe-sha256 <published-pin> --paths <original-joint-technical-PATHS.json>
--runs-dir <local-runs>`. A completed identity is verified without recomputation;
resume refuses source/input drift and a live writer. Engineering alone may use
`freeze_causal_convex_recipe_v2.py`; ordinary operators must not regenerate a pin.

There are294 declared scalar snapshot attempts, zero base-model fits or model loads,
one worker, and phase-boundary300second/1GiB memory/1GiB output caps. Resource observations
are sampled, not continuous OS quotas. Portable restoration compares58 deterministic
scientific payloads and separately validates process-dependent resource receipts.

The output is an offline modeled-clock diagnostic. Actual layer publication latency
and historical issuance are unqualified; production availability remains null and
native policy admission false. Report all comparator/coverage outcomes; do not pick a
winning horizon, revise weights after assessment, or infer economic readiness.
