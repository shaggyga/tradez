# Collector freshness repair - October 1, 2026

The observed freshness refusal was real at the consumer boundary, but six captured
reads were looking at an old heartbeat after the collector had already progressed.
The 30-second periodic publisher delayed that update. Published progress age reached
191.7 seconds, crossing the unchanged 180-second limit.

The deployed collector wrapper publishes genuine phase changes immediately,
throttles same-phase work updates, and serializes snapshot/publication. Snapshot
time is sampled while progress cannot advance underneath it. Periodic heartbeats
never invent progress. Original ingestion/classification source and article clocks
are unchanged. The wrapper is explicitly identified in the heartbeat and pinned in
the operational profile.

A separate exact-source test reproduced the reader's start-time race: a publication
appearing after read-start can be called future. The scheduling wrapper now permits
one complete new capture only after the original health checks pass again. Stale or
unreadable probes, unrelated failures and a second capture failure still refuse.
No timestamp, retained handle or freshness threshold is rewritten. The new helper
is source-bound and its retries receive durable operational receipts.

## Executed verification

- 92 final deployment checks passed. This includes the scheduler, retry, progress
  publication and operational profile tests.
- 25 focused tests passed in a relocated 29-file source fixture. An initial fixture
  omitted the original reader used by the AST test; that failure is retained and
  the corrected fixture passed. This is not a trained-model restoration claim.
- 246 samples over 494 seconds observed collection, the long post-processing
  phase, derived publication and cycle completion. No invalid freshness sample and
  no increase in joint-worker errors occurred in that window. Maximum published
  progress age was 133.74 seconds, below the original 180-second limit.
- The completed cycle checked 192 configured sources and inserted 31 records.
- All 68 original ledger contracts, activations and pre-change forecast identities
  were preserved. Original collector, rolling I/O and numeric worker hashes match.
- Supervision reported `role_liveness_current`. Retry budgets and recovery expiry
  were preserved. No model replacement, paid call or order action was introduced.

The old cumulative error count remains historical; this repair does not erase it.
The observation window is evidence of this deployment, not a promise that external
feeds can never become stale. Genuine stale inputs must continue to refuse.

## Handoff and rollback

Local evidence: `evidence/collector_timing_20261001`. Live Vault packet:
`COLLECTOR_TIMING_20261001`. `PUBLICATION_LAG_PROOF.json` binds the original failure;
`VERIFICATION.json` binds the successful window. Raw observations remain local with
their hashes in the compact packet. Actual commands and the corrected portable
fixture inventory are retained with the step.

Resume `live_news_input_reliability_v1` at remaining per-pair readiness/refusal
reconciliation under the repaired runtime, then position-management qualification.
Do not repeat this diagnosis or start replacement fits. Preserve genuine sparse
quote/unsupported cases and original gate thresholds.

Rollback is ordered: restore `progress_contract.before` / `progress_profile.before`
using `deploy_progress.ps1` ownership checks to remove only the collector wrapper;
then, if needed, restore `scheduler.before`, `scheduler_config.before` and
`profile.before` using `deploy.ps1`. Stop only exact owned roles/controllers and
restart with the qualified launcher. Preserve ledgers, archives and receipt journals;
never reset recovery budgets. The other-task EUR/USD recorder is untouched.

Same-task review only. Scoped engineering repair verified; scientific forecast
evidence unchanged; position-management evidence unqualified; trading authorization
not granted.
