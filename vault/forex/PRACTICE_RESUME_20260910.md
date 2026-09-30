# Practice007 resumed and Windows sign-in recovery — September 10, 2026

The user explicitly requested that trading be enabled. The original finite OANDA Practice007 trial and research services were restarted, and the broker confirmed new practice fills. Final independent GET-only observation completed **2026-09-10T15:10:40.288780+00:00**. This later observation supersedes the September 9 reboot/stopped status; it does not revise the earlier six-trade loss review.

At that observation, worker 11988 was managing an open **NZD/JPY short, 283 units**, broker trade 2493, entry 89.395. Broker-held stop 2494 was pending at 89.474. The account had one open trade and one pending protective order, balance **USD 41.1189**, marked NAV **USD 41.0233**. These are timestamped values, not a promise of subsequent position state or performance. The first resumed USD/CHF long was independently confirmed earlier; by this final observation the account balance had fallen another USD 0.1678 from the previous review's USD 41.2867.

The [final runtime and broker receipt](PRACTICE_RESUME_20260910_EVIDENCE/RESUME_FINAL_VERIFIED_20260910.json) retains the enabled config validation, separate account/trade/order observations, worker/watchdog/bootstrap status and native Windows task observation. Worker status was about four seconds old when printed; entry and session-loss halts were false. The two Python PIDs shown by Windows are the virtual-environment launcher and its actual worker, not two independent trading workers.

## Recovery installed

The current-user Windows task **Forex Practice Research Recovery 20260910** starts a hidden recovery guard 30 seconds after sign-in. It is enabled, uses limited interactive privileges, allows only one task instance and has no execution-time limit. Task Scheduler was used to start the installed action; the running guard observed one correct research supervisor, one practice watchdog and the existing Python worker without creating duplicates. Native task result 267009 (0x41301) means the task is currently running.

This is application sign-in recovery. No Codex scheduled monitoring or chat watch was created. A real reboot was not performed during validation, and services cannot be assumed to start before Windows sign-in. Earlier disabled broad/legacy trading tasks remain disabled.

The new guard validates the exact source files, original config bytes and finite deadline before recovery. It invokes the existing research-only launcher and original practice watchdog, leaves surviving workers alone, refuses conflicting supervisors, and preserves the ledger, baseline, stop requests and loss latches. It never restarts research after the cutoff; the original practice runner may still resume solely to finish required closes until it confirms flat. The guard exits on completed-flat status.

## Existing trial contract retained

The model, forecast adapter, execution policy, broker adapter, runner, research registrations and study authorities were not replaced. Research studies retain their research-only authority; the user-authorized separate Practice007 worker executes admitted forecasts. The actual route is OANDA practice, not a real-money account.

The original USD 41.6042 baseline remains intact. Limits remain one position, 0.5% NAV modeled stop risk, 20% NAV entry margin, eight claimed attempts per UTC day, and a persistent session-loss stop of the lesser of 10% original NAV or USD 5. Entries still require fresh eligible forecasts and cost/risk checks. The original H1 target and attached protective stop remain the position exit contract. The final cutoff remains **Friday September 11, 2026, 20:45 UTC / 4:45 p.m. Eastern**, with latest admitted target at 20:40 UTC. Resuming the process did not reset the account, loss history, limits or deadline.

## Validation and retained evidence

- **29 native Windows PowerShell checks passed** for the new bootstrap, including exact process selection, surviving-worker adoption, duplicate refusal, source/config binding, fixed cutoff, read-only modes and Windows file replacement during reads.
- Independent source review found no remaining blocker. It binds the exact reviewed source and tests to the 29-check receipt. The earlier 286 practice-component tests remain earlier evidence; no new model-performance validation is claimed.
- Actual validation includes current broker GETs and invoking the installed task action against already-running services. Missing-service/reboot decisions were tested in native fixtures; the active trading worker was not killed for a recovery test.
- [Task installation receipt](PRACTICE_RESUME_20260910_EVIDENCE/LOGON_RECOVERY_INSTALLED_20260910.json), [installed task XML](PRACTICE_RESUME_20260910_EVIDENCE/RECOVERY_TASK_INSTALLED.xml), [first action verification](PRACTICE_RESUME_20260910_EVIDENCE/RECOVERY_ACTION_VERIFIED_001.json).
- [Bootstrap implementation](PRACTICE_RESUME_20260910_EVIDENCE/RECOVERY_BOOTSTRAP_IMPLEMENTATION_20260910.md), [native test receipt](PRACTICE_RESUME_20260910_EVIDENCE/NATIVE_CHECKS_20260910T150411882.json), [independent source review](PRACTICE_RESUME_20260910_EVIDENCE/RECOVERY_BOOTSTRAP_INDEPENDENT_REVIEW_20260910.json).

Canonical bootstrap SHA-256: `c027f122be82631f0eae518f79c2bc470c5f0b505de1738eebd61d709fea0fd6`. Recovery manifest SHA-256: `5a1347797fc45427875dff1530242f3602e5db7a9e2cbe1ff5d5daf8fd4f6e46`. Original trial config raw SHA-256 remains `ae2901395ba117b2df7a6d13670d48cb3d4f10963bd3b983d6b3775be90b5af8`.

Prediction quality, realistic exit spreads, matched H1/stop/remaining-risk comparisons and currency-factor concentration remain pending research. Restoring operation does not resolve the earlier losses or establish profitability. The user's planned end-of-week review should reconcile later broker outcomes and refused attempts with retained original decisions.

This standalone publication preserves all earlier reports and source snapshots. Its own publication receipt inventories the added files and changed documentation; older shared vault maps/ZIPs retain their earlier cutoffs.
