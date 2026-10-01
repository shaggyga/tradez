# Live pipeline recovery — October 1

The existing nontrading supervisor/watchdog and dashboard were restored under
the user's instruction to finish. No forecast model was fitted or replaced by
this repair, and no orders or account actions were performed.

The dashboard initially refused HTTP connections. After restoration, two actual
`/api/main` reads showed advancing retained forecasts. At 05:07 UTC it returned
2,535 eligible forecasts across 39 registered connections; 81 slots were
non-tradeable, 24 lacked a base forecast, six lacked exact controls and six lacked
an exact-origin curve. Currency-news context was current, about 30 seconds old.
The supervisor and watchdog were fresh, with exactly one supervisor.

## Persistent recovery

Both existing Windows tasks previously had only logon triggers. A stopped
controller or dashboard could therefore remain stopped until another logon.
`tools/forex_recovery_schedule.ps1` adds five-minute retries to these same tasks,
ending at the existing profile expiry, **2026-10-07 08:14:50 UTC**. It preserves
logon triggers and saves the original task XML before applying changes.

The dashboard task now uses its existing source-pinned, single-owner launcher.
The pipeline keeps its existing validated profile and ownership checks. Healthy
children are adopted, not duplicated. The first scheduled pipeline retry returned
zero with one supervisor still running. A dashboard retry while its existing
long-running instance remained active was refused by Task Scheduler's existing
single-instance policy; the dashboard continued serving HTTP successfully.

Inspect without modifying tasks:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools/forex_recovery_schedule.ps1
```

On a replica with the original tasks, qualified runtime and matching reviewed
dashboard bytes, apply with `-Apply -EvidenceDirectory <new-local-directory>`.
The script refuses expired recovery, unexpected task actions and changed
dashboard source. It does not automatically bless changed source hashes. No chat
automation or heartbeat was created. These are the existing operational tasks.

## Not resolved by restart

The whole system is **not** marked healthy or trading-ready:

- Joint price/news V11 still alternates between warmup and unavailable states,
  with `upstream_collector_stale_or_future`. The collector processes roughly
  7,678 relevant and 55,102 context articles in its clustering phase; progress
  can age while processing continues. A separate transient `PermissionError`
  reading the collector heartbeat was observed. The repaired news snapshot did
  subsequently return to `current`. Neither timeouts nor source pins were
  loosened to hide these errors.
- The historical official-event horizon worker reports
  `entry_cohort_source_binding_changed`. Its immutable source-bound cohort must
  remain preserved. A compatible source restore or separately qualified current
  cohort is required; overwriting its binding is not a repair.
- A feature-forward worker remains stale and its restart circuit is open. Its
  supervised output/error logs inspected during this repair were empty; the
  reason has not yet been established. The circuit was not reset blindly.
- Position management remains inactive. Common-terminal conditional value,
  explicit position state and complete economic/risk inputs are still required.

Immediate operational next: resolve the news progress/read failures and inspect
the stalled feature-forward worker before any further forecasting research.
The preserved event-cohort mismatch must be reconciled without relabeling old
evidence. The queued offline management item remains
`retained_management_contract_qualification_v1` after those operational repairs.

Evidence: `evidence/live_recovery_20261001`; shared packet
`LIVE_PIPELINE_RECOVERY_20261001`. Validation is actual profile validation,
scheduled-task readback, HTTP observations and native process/heartbeat checks;
no new unit-test pass count is claimed. Review is same-task, not independent.
