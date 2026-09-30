# Forex research pipeline

The local pipeline collects Forex quotes, minute candles and public news, records
causal news observations, produces price and joint price/news research forecasts,
and serves them on the dashboard. Collection, forecast availability and model
performance are separate facts. No order process is part of this launcher.

## Current machine

From the repository root, inspect without starting anything:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools/forex_pipeline.ps1 -Action Status
powershell -NoProfile -ExecutionPolicy Bypass -File tools/forex_pipeline.ps1 -Action Validate
```

`Status` reads embedded heartbeat clocks and schema identities. A fresh heartbeat
does not prove a successful cycle or a new forecast: inspect reported failures,
the selected dashboard summaries, forecast issue/target clocks and pair coverage.
It writes no historical `LIVE_CURRENT` record. `Validate` verifies exact source
and configuration hashes, bounded arguments, no-orders flags and recovery expiry.

To start or recover the already configured local research pipeline:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools/forex_pipeline.ps1 -Action Start
```

The launcher reuses the V6 supervisor/watchdog, its single-owner checks and the
18-role profile `trad/config/operational_runtime_current_20260930.json`. It adopts
compatible running workers instead of creating competing writers. A conflict or
changed source must be investigated, not overridden by starting a second process.
The existing Windows task `Forex Operational Research Recovery 20260913` now calls
this entry point. It is an operating-system recovery task, not a chat automation.

Recovery is bounded through **2026-10-07T08:14:50Z**. This profile is not an
indefinite service authorization. Before renewal, inspect live health and ownership,
verify source/configuration identities and publish the new bounded profile.
Changing a running profile invalidates its seal: stop only its exact controllers,
preserve data-producing children, validate the replacement and restart recovery.
Worker argument changes additionally require a controlled stop of that specific
worker; never kill every Python process.

## Source and data ownership

- The quote and all-68 M1 collectors use existing practice-market credentials for
  read-only prices/candles. They are distinct from the other task's EUR/USD recorder.
- Default and revision news collectors retain their original archive roots and
  pinned collector source. Optional keyed providers are disabled in child process
  environments; stored credentials are unchanged. Public feed failures remain visible.
- Joint V9 reads the retained revision transport and its original registered input
  configuration. Cold validation resumes checked history; it still requires a fresh
  capture and original availability clocks before issuing a forecast.
- The display explicitly selects the registered price V3 and joint V9 studies.
  Original source, activation, summary, publication and target-time checks still apply.
- The rolling technical dataset retains its original numerical/store contract.
  Its separate September30 runtime configuration provides a bounded 16 GiB cap and
  retains the 32 GiB free-space guard. No automatic evidence deletion is enabled.
- Crypto has no active dashboard route, polling or Forex startup role. GPT/advisor
  comparisons and order/account operations are outside this recovery.

The September30 interpretation adapter and change-triggered worker remain separate
offline successors; restoring an old qualified producer does not deploy those changes.

## Troubleshooting

Open `http://127.0.0.1:8765/`; the dashboard is served by its existing Windows task.
An HTTP 200 confirms the server, not its inputs. A source mismatch requires review
of the changed bytes. Do not relabel stale results or simply rehash a model registry.

Cold-start history can take several minutes. The rolling worker has a bounded
30-minute startup grace; normal output freshness remains three minutes. News
validation progress is stored under the selected IO configuration's reader cache.
Transport success, current health, a validated capture and joint forecasts must be
checked separately. Historical last-error fields may remain after a later success.

Read `docs/DASHBOARD_STATUS_20260930.md` and live Vault `PROJECT_CONTEXT_LATEST.json`
for the latest measured result and any unfinished correction. Local raw receipts are
in `evidence/dashboard_live_recovery_20260930`; compact shared review is in Vault
`DASHBOARD_LIVE_RECOVERY_20260930`. Large data and credentials are excluded from Git.

## Another machine

Git supplies runnable source and the dated Vault knowledge snapshot. It does not
supply private credentials, local databases or fitted artifacts. Follow
`START_HERE.md` and `docs/ARTIFACT_REUSE.md`, retrieve exact existing artifacts,
and resolve the live shared Vault before claiming work. This machine's absolute
paths, activated study roots and recovery expiry must not be blindly reused.

The read-only inspector supports an explicit `--profile` for a prepared replica.
Starting a replica requires its own verified, source-pinned runtime configuration
and original activation/restore evidence; a clone alone is not an activated system.
