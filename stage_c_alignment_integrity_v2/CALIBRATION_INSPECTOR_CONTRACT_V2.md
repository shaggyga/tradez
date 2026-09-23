# Original-record calibration inspector

Purpose: inspect exact pinned original calibration, joint forecast and technical
outcome records. This is a modeled-clock development inspection, not attested
historical publication, confirmation, an execution policy or live authorization.
No fitting occurs in status/run/resume/verify/inspect. A checkpoint reconstruction
replays the dependency's residual snapshots, then inspects them; no base model fits.

Query JSON requires exactly: group, horizon_minutes, base_procedure, method,
instrument, origin_epoch, calibration_mode, asof_epoch, reveal_outcome and
outcome_asof_epoch. Use original registered scopes/origins and instruments.
reveal_outcome is a JSON boolean. When false, outcome_asof_epoch must be null.
When true, a separate integer outcome as-of is required. The forecast as-of never
implicitly authorizes an outcome. Missing source labels remain null, not zero.

Before modeled base availability, no numerical forecast, snapshot or calibration
record is exposed. At availability, the exact record and snapshot are returned
with actual_publication_qualified=false and production availability null. A
snapshot is authenticated against its content identity, scope and application;
its training support must not exceed cutoff. Explicit outcome reveal is withheld
until that original outcome's own available epoch. Future outcomes never alter
the numerical forecast or snapshot. Unavailable base forecasts and insufficient
support are retained without a fallback substitute.

The reader caches authenticated original bytes only during its lifetime and
returns detached JSON. A new operator invocation revalidates all input/source
pins. Smoke output contains1224 exact inspection digests:68 instruments,3 fixed
profiles,2 calibration modes and3 visibility states. Full original records remain
available through inspect; smoke receipts avoid duplicating long training lists.

Invoke calibration_inspector_operator_v2.py status/run/resume/verify/inspect with
--recipe, --recipe-sha256, --paths and --runs-dir. inspect also takes --query.
Paths JSON contains calibration, joint and technical completed-run directories.
Use the externally approved recipe hash. Completed matching runs verify without
rerunning inspection. Changed sources, clocks, data or recipe require review.

Main300seconds/1GiB; checkpoint900seconds/2GiB; one worker. The small checkpoint
requires the exact residual calibration capsule companion. Its original records
are authenticated and reconstructed; upstream full suites are not claimed rerun.
Specific cheaper-model behavior, other-host capacity and independent review remain
unqualified. GPT/advisor, broker/service/account and D-drive scopes stay deferred.
