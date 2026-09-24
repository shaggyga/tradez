# Warm curve operator: bounded no-fit successor

Resume the existing partial prototype; use the exact 466-file original-input capsule
SHA256 `25061ff42b9dac4027cfcfb3042a3778b5db448e449428d8415c951de2b7676f`.
All 68 instruments, 20 original origins and the original seven horizon/group/procedure
grid remain. No fitting, new forecast issuance, live readiness, services or broker actions.

`warm_curve_operator_v2.py status|run|resume|verify --recipe <WARM_CURVE_OPERATOR_RECIPE.json>
--recipe-sha256 <reviewed-sha256> --paths <PATHS.json> --trad-root <trad> --runs-dir <runs>`

The recipe pins scientific source, predecessor source, original saved-input identities
and environment. Ready-only weight loading uses a fixed conservative 120-second
modeled prewarm reservation. Numerical input caches reset each origin. After process
death, resume reconstructs all past origins chronologically, verifies identical
scientific bytes/cache state, and preserves existing per-origin timing receipts.
New or validated payload actions are logged in RECONSTRUCTION_LOG.jsonl. Missing
original timing is measured as validated-existing reconstruction, never called an
original run measurement. A completed matching run only verifies, without recomputation.

Each run has one writer, a 180-second startup limit, 30-second per-engine origin limit,
120-second prewarm limit and 900-second complete run limit. The frozen operator checks aggregate RSS at 1 GiB and run-output disk at 2 GiB at phase boundaries and immediately before completion. These checks are sampled bounds, not continuous OS quotas. Each process attempt records resource observations; the completion manifest authenticates the combined receipt. Engineering validation additionally samples aggregate RSS and wall time in an external supervisor. The two-second target is diagnostic; all
existing native freshness gates remain. Same-host OS cache and modeled prewarm
availability do not establish a historical or live deadline guarantee.

The 900-second runner clock starts after operator preflight. The validation supervisor
bounds the entire subprocess at 900 seconds; portable invocation uses a 1,000-second
subprocess timeout. These are distinct scopes, not a portable 900-second CLI guarantee.
The 2 GiB disk check covers run outputs, with 1 MiB reserved for completion metadata;
it does not bound the existing saved-input capsule or the whole workspace.

All 143,904 forecast values and 152,320 coverage rows per engine must match retained
source lineage and the shared-engine comparison. Forty-four scientific payloads
must reproduce exactly on relocation; 22 timing/startup/resource payloads are separately
authenticated and bounded, not expected to be byte-identical across runs.

Export: `warm_curve_checkpoint_v2.py export --package <new.zip> --paths <PATHS.json>
--trad-root <trad> --runs-dir <runs>`.
Restore: `warm_curve_checkpoint_v2.py restore --package <new.zip> --sha256 <reviewed-sha256>
--original-inputs <forex_warm_curve_original_inputs.zip> --destination <new-empty-directory>
--run-tests`. This restores saved weights and performs no fitting. Never substitute
a raw-to-model reconstruction for missing saved inputs.

Any drift, cache mismatch, timing overrun, partial corruption or active writer returns
review_required and retains evidence. New source requires a newly reviewed recipe.
Tests, independent review, forecast evidence and trading authorization remain separate.
