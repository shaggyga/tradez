# Currency projection policy attribution

`currency_projection_policy_attribution_v2` completed a zero-replay attribution over the already verified 48-path policy matrix. It retained every six forecast methods, four dated cost scenarios and six policy arms. The report has 144 arm-level rows from 24 reference-engine paths, after engine-neutral accounting parity was already verified.

It records 436 ENTER/REPLACE selections, 266 actual opening fills and 170 selected-but-unopened outcomes. Financing status and terminal lot/pending state remain per arm and path. No rows are aggregated into a forecast winner because the two cohorts overlap and the paths are dependent inspected-development scenarios.

Tests passed: 3 focused attribution/policy tests. The report build completed with zero base/layer fits, model loads, replays, API calls, or external actions. Source Git: `f062ebf8299b882aa84572648646b05aaada2edf`; report hash: `6a574ed4075705e96ab4a338109942ccd011b56d40d377d5630c724155b317f5`.

Next: `currency_projection_policy_checkpoint_v2`; freeze a compact checkpoint/restore recipe using the measured 10.49GiB main output and 8GiB safety reserve. GPT/advisor comparisons remain deferred.

Checkpoint R2 is present beside this review: `CURRENCY_PROJECTION_POLICY_CHECKPOINT_R2.zip`, SHA-256 `37c17453a506d59c0a4c920e24f66b804999a0d5a6e62afd4f7625e134ef1d90`. It contains 123 authenticated source/input members and expected manifests for all 48 outputs. A fresh restore completed all 48 replays and matched every payload manifest exactly; it also reran the engine-neutral parity review. Restore output was retained locally at `timed_20260924_110416/currency_projection_policy_step/portable_restore_r2`; C: had 163.57GiB free afterward.

Confirmation preflight is implemented in Git `12c38199e688a29db5e884c8ca4397143a73e2a5`. It refuses origins at or before `1722535260`, requires at least eight newer origins, all-68 qualified quotes, mature labels, and all six variants. The retained inputs contain no such third cohort, so no confirmation computation was launched.
