# Separate practice/research logon recovery bootstrap

New canonical files only: `trad/start_oanda_practice_recovery_v1.ps1` and `trad/test_start_oanda_practice_recovery_v1.ps1`. Existing runner, broker, forecast/policy sources, research supervisor, launchers, model registrations and account state were not edited by this owner.

The bootstrap runs hidden under a unique local named mutex. It revalidates its exact source closure, original trial configuration bytes and finite September 11 20:45 UTC cutoff on each iteration. Its loaded source identity is held separately from current disk bytes, so a running old bootstrap cannot silently adopt a changed source/manifest identity. Failures produce bounded codes in its own `bootstrap.json` and are retried after 15 seconds without an action while invalid.

Before the deadline it starts the existing research launcher only when no expected supervisor exists; any conflicting supervisor is a refusal. The existing trial watchdog starts only once an exact canonical SafeCoreOnly/ResearchCollectionOnly supervisor is present. An existing watchdog or worker is left alone, including a surviving worker whose watchdog disappeared. Duplicate process dispositions refuse recovery rather than killing processes. After the unchanged Friday cutoff it never restarts research, but it may resume the existing watchdog for the runner's close-only path until `completed_flat`; it cannot extend the deadline, reset a ledger, clear a loss latch or authorize a new entry. A completed-flat status ends the bootstrap.

`-CheckOnly` and `-Once` are both single **read-only inspections**: no mutex creation, child launch or bootstrap status write. The default invocation is the recovery loop. Every child uses `Start-Process -WindowStyle Hidden`. JSON/file reads are bounded and reject reparse paths; status reads share deletion to allow the existing writer's atomic replacement. An original status clock is compared against the actual post-read clock. The research deadline is checked again immediately before its launcher is spawned.

The root-created manifest must be `trad/config/practice_recovery_launcher_20260910.json` with:

- `schema_version`: `practice_recovery_launcher_v1_20260910`; `enabled`: boolean true.
- `project_root`: exact canonical trad path; `trial_id`: `practice007_joint_v3_20260909_v1`.
- `stop_epoch`: numeric `1789159500`.
- `trial_config_sha256`: SHA-256 of the original active trial configuration's exact raw bytes.
- `source_bindings`: exact four-name map for `start_oanda_practice_recovery_v1.ps1`, `start_oanda_research_collection.ps1`, `start_oanda_practice_trial_v1.ps1`, and `oanda_always_on_supervisor.ps1` to their current SHA-256 hashes.

Final source SHA-256: `c027f122be82631f0eae518f79c2bc470c5f0b505de1738eebd61d709fea0fd6`. Final native test SHA-256: `03e50acc317e354af9163f1c0cee9d1b8bc61c4dfc0aa2171a2bab42cd8b2c24`.

**29 native Windows PowerShell 5.1 checks passed**, with no failures: [final receipt](native_checks/NATIVE_CHECKS_20260910T150411882.json), SHA-256 `5685a86084a8e2f38942ee4d59e988eacf2091fe23f45a1cff7a868463268827`. Tests cover exact process paths/flags, conflicting and duplicate supervisors, surviving workers without watchdogs, source/config changes, fixed cutoff/completed status, bounds, both native read-only modes, absence of bootstrap status mutation, actual Windows replacement while a reader remains open, and status publication during a read. The preceding 27-case receipt remains retained. Test fixtures remain outside the project. The first attempt without per-process execution policy did not execute because Windows script execution was disabled; native tests then used the same explicit per-process Bypass convention as the approved launchers. No machine execution policy was changed.

These tests did not launch services, stop processes, register startup entries or call the broker. Actual manifest creation, valid live inspection, startup registration, default-loop activation and recovery verification belong to root. Successful startup registration and actual recovery are separate claims from this source/test receipt. Logon recovery still requires a user logon and is not guaranteed pre-login boot execution.
