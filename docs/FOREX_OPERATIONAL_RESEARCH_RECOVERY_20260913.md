# Operational research health and recovery, 2026-09-13

This change adds explicit recovery for `forex_operational_runtime_v1_20260913`.
It does not activate, stop, or restart any service by itself. Profile creation and
activation are separate recorded operational actions. No order executor is in
the operational profile.

## Scope

The ten managed profile workers are:

| Worker | Source |
| --- | --- |
| revision_news_collector_v2 | oanda_local_news_sentiment.py |
| local_news_sentiment_repair_v2 | oanda_local_news_sentiment_repair_v2.py |
| revision_news_transport_v4 | revision_transport_v4.py |
| joint_price_news_study_v7 | oanda_joint_price_news_forecast_study_v7.py |
| pair_local_forecast_study_v3 | oanda_pair_local_forecast_study_v3.py |
| retained_price_settlement_v1 | oanda_retained_price_settlement_v1.py |
| native_feature_candles_v1 | oanda_native_feature_candle_updater_v1.py |
| research_feature_observations_v2 | oanda_research_feature_observation_worker_v2.py |
| research_feature_forward_v2 | oanda_feature_forward_worker_v1.py |
| official_pair_horizon_v2 | oanda_official_event_pair_horizon_capture_v2.py |

The original canonical news collector remains for older consumers. The revision
collector has a separate source database and process identity; legacy news rows
are not re-labelled as reconstructable native revisions. Together with retained
core services, current supervision must report all **21 required workers**.
The legacy no-profile health contract continues to require 15 workers.

The operational allowlist rejects the old pair-local v1/v2, news repair v1,
joint v3, and feature observation/forward v1 producers. Explicitly disabling a
required current worker does not turn its absence into healthy retirement.

`oanda_project_runtime_health.py` interprets the newest complete supervisor
heartbeat and its start-event profile binding. It does not independently inspect
the operating system or re-open profile sources. A current operational worker
requires fresh output **and** the supervisor's explicit `reported_failure=false`.
Reported status, phase and error remain visible. A recent error heartbeat is
liveness evidence, not successful operation or predictive performance.

## Recovery contract

`oanda_operational_recovery_contract.ps1` validates exactly ten named sources,
their SHA-256 hashes, the research/no-orders booleans, bounded arguments and ages,
exact process needles, profile and heartbeat locations, and a finite UTC expiry.
Path parents containing reparse points and the legacy D drive are rejected.
SHA-256 uses .NET directly: inherited PowerShell module paths on this computer
can make `Get-FileHash` unavailable in a Windows PowerShell child.

`start_oanda_operational_research.ps1` can only invoke:

```text
oanda_always_on_supervisor.ps1 -SafeCoreOnly -ResearchCollectionOnly -OperationalProfilePath <validated profile>
```

It refuses an existing incompatible or duplicate supervisor. It never selects
`start_oanda_safe_core.ps1` as an operational-profile fallback. The existing
watchdog supports optional `OperationalProfilePath` and revalidates source and
profile hashes before recovery. Changed source, a changed profile, expired
authority, or an incompatible supervisor is retained as a recovery refusal.
An existing supervisor with another profile is preserved for explicit review.

Without `OperationalProfilePath`, previous watchdog behavior is preserved for
compatibility. That legacy mode does **not** establish the new research profile.

## Deployed profile and activation handoff

The profile V3 handover occurred on **2026-09-14 at 00:17:57 UTC**. The current
path is `config/operational_runtime_v3_20260913.json`, SHA-256
`ce3e6189c71e9a7673f3c3504ed50a90255ccdce5a98d4697694dec1e257b4bd`.
The schema name still ends in V1; that schema is distinct from the profile file
generation. The commands below name the deployed V3 file.

For a later controlled handover, validate the profile against frozen worker
sources before launch. The currently deployed profile path is:

`C:\Users\zmoor\Documents\forex\trad\config\operational_runtime_v3_20260913.json`

Validate without starting processes:

```powershell
& 'C:\Users\zmoor\Documents\forex\trad\start_oanda_operational_research.ps1' -Root 'C:\Users\zmoor\Documents\forex' -OperationalProfilePath 'C:\Users\zmoor\Documents\forex\trad\config\operational_runtime_v3_20260913.json' -RecoveryUntilUtc '2026-09-20T23:59:00Z' -ValidateOnly
```

After an explicit operational handover, launch or recover the research supervisor:

```powershell
& 'C:\Users\zmoor\Documents\forex\trad\start_oanda_operational_research.ps1' -Root 'C:\Users\zmoor\Documents\forex' -OperationalProfilePath 'C:\Users\zmoor\Documents\forex\trad\config\operational_runtime_v3_20260913.json' -RecoveryUntilUtc '2026-09-20T23:59:00Z'
```

Then enable the same-profile watchdog, whose child process is launched hidden:

```powershell
& 'C:\Users\zmoor\Documents\forex\trad\start_oanda_supervisor_watchdog.ps1' -Root 'C:\Users\zmoor\Documents\forex' -OperationalProfilePath 'C:\Users\zmoor\Documents\forex\trad\config\operational_runtime_v3_20260913.json' -RecoveryUntilUtc '2026-09-20T23:59:00Z'
```

If Windows Task Scheduler is used by the integrating operator, its action must
target the same watchdog launcher with both explicit profile and expiry
arguments; hidden Windows PowerShell is sufficient. These instructions do not
register a task. An old watchdog with another profile is an explicit conflict,
not permission to replace it silently.

The expiry is **2026-09-20 23:59 UTC**. It ends future recovery launches and the
watchdog loop. It does not stop an already healthy research supervisor or its
children. Research child duration is 604800 seconds and is independent of any
separate practice-trading trial. Do not imply this recovery expiry limits an
order executor: no order executor is part of this profile.

## Read-only task inventory

Inspection during this repair found:

| Task | Observed state | Meaning |
| --- | --- | --- |
| Forex Practice Research Recovery 20260910 | Ready | Invokes `start_oanda_practice_recovery_v1.ps1`, whose pinned trial stop epoch 1789159500 is 2026-09-11 20:45 UTC. This is expired practice recovery, not the new research setup. |
| ForexGptOrigConstantRotationDemoWatchdog | Disabled | Does not provide current recovery. |
| ForexSafeCoreAtLogon | Disabled | Legacy broad launcher action; does not establish operational-profile recovery. |

The table above records the initial read-only inventory. The integrating repair
subsequently updated **Forex Practice Research Recovery 20260910** to the V2
practice launcher and started that action; its heartbeat adopted the existing
practice worker without a restart. It also installed **Forex Operational Research
Recovery 20260913**, using the explicit V3 profile and the stated expiry.

An actual recovery exercise stopped only research supervisor PID 24040 at
**2026-09-14 00:20:52 UTC**. Its watchdog launched replacement PID 13856 at
**00:21:04 UTC** while collection and practice remained running. The practice
recovery heartbeat continued to report `already_running` and no restart claims.
The [exercise receipt](../../operational_repairs_20260913/RESEARCH_RECOVERY_EXERCISE_RESULT.json)
and [installed research task](../../operational_repairs_20260913/CURRENT_RESEARCH_RECOVERY_TASK.xml)
retain this evidence. Reboot recovery has not been exercised.

## Validation and limits

Focused tests exercise legacy and operational health, every required worker,
retired producers, missing profile bindings, recent error heartbeats, pure
Windows PowerShell profile validation, source/identity/expiry failures, exact
safe research arguments, and parser validation of all four PowerShell files.
Tests never launch services. The separate runtime activation receipt must verify
real process ownership, heartbeat schemas, producer status, prospective capture,
source hashes, and worker progress. Passing these tests does not demonstrate
forecast quality, positive returns, or complete official-source availability.
