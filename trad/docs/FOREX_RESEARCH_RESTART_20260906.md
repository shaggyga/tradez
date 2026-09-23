# Forex collection restart — September 6, 2026

Forex restarted at 12:16:44 UTC following the user's authorization to turn it
on for today's market opening. Verification timestamp: 2026-09-06T12:22:30.1330341Z.
This supersedes the earlier keep-stopped instruction for collection only.

One supervisor runs the new `-ResearchCollectionOnly` mode. Eleven workers
provide practice-account reads, localhost dashboard, quote streaming, news
collection/mapping, source-governance fastlane V3, M1 candle updates, clock
monitoring, storage monitoring and the existing integrity audit. The dashboard
responds at http://127.0.0.1:8765/ and the OANDA practice quote stream is connected.
The practice account has zero open trades and zero pending orders.

The allow-list excludes all execution, model production, calibration, outcome
production, promotion and lifecycle workers. An unknown future worker is also
blocked. Source V9 and rank V8 remain disabled. The existing broad safe-core
launcher includes practice execution, so it was not used. Both existing Forex
scheduled tasks remain disabled; no watchdog or new scheduled task was enabled.
The supervisor restarts its admitted children while this session is running.
Restart after a computer reboot requires the collection launcher again.

The dedicated quote stream now owns the canonical quote snapshot and records
its own producer identity. The previous JSON and SQLite/WAL/SHM were copied
and hashed before launch. Broker quote timestamps remain original; all 68
observed weekend quotes are nontradeable. Connection health does not establish
fresh executable market prices. The broader integrity audit remains an
unrelaxed diagnostic with deliberately absent inputs; this receipt is not a
passing full-project integrity or trading-readiness certificate. At the
verification timestamp its initial refresh was still running and the prior
saved integrity publication remained historical.

Fastlane V3 is alive and committing scan progress through retained input.
Its initial scans rejected preactivation rows and created no new prospective
receipts. Successful new-event acceptance remains unverified. The two open
prediction-clock findings and historical evaluation results are unchanged.

Validation: 20 offline startup/quote/watchdog and 21 vault-sync regression tests passed. An
independent isolated PowerShell AST/function review tested 107 blocked worker
names, the exact eleven-name allow-list, existing-disallowed-worker handling,
quote ownership, and the sole guarded process-launch path. Live process ancestry,
heartbeats, dashboard response and account reads were also checked. Windows
virtual-environment launcher/child pairs are not duplicate independent workers.

OANDA US lists the Sunday reopening at 17:05 New York time (21:05 UTC today),
except TRY pairs, and regular FX hours for September 6 and 7:
[regular hours](https://www.oanda.com/us-en/trading/hours-of-operation/),
[holiday hours](https://www.oanda.com/us-en/trading/holiday-trading-hours/).

Start command from the parent Forex directory:

```powershell
.\trad\start_oanda_research_collection.ps1
```

Read `FOREX_RESEARCH_RESTART_VALIDATION_20260906.json` for hashes and observations.
Prior dated audit/repair/evaluation receipts remain historical. The original
saved large integrity snapshot is retained in `../restart_20260906/before/`.
