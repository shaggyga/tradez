# Current rolling repair — 2026-09-30T16:54:37.937984+00:00

The replay blocker is resolved. Native supervisor/watchdog ownership and all17
selected roles were verified against the exact current profile. JointV11 now
stores parsed sentiment in a rolling index and archives audit records separately.
Two new native-worker captures advanced the retained index after the foreground
verification. The joint worker reported zero errors and no joint forecasts yet.

The HTTP dashboard selected current V11 and65 price-forecast pairs, with fresh
quote observations. Joint forecasts require48 mature quarter-hour H1 training
origins,12 nonzero context rows and8 patterns. Those are prospective warmup gates;
old history was not imported and unavailable sentiment was not zero-filled.
The final native receipt had57 warming pairs,7 awaiting their current input read,
3 without tradeable quotes and1 without a fresh quote. Coverage varies with time.

Technical availabilityV2 now verifies the exact reviewed capacity runtime:
57 complete,8 partial and3 not-tradeable pairs; no source/receipt-binding errors.
The numerical features were not changed.137 tests passed from an isolated Git
source restore; all8749 tracked files matched. Actual ten-record archive replay
passed and correctly could not authorize a fresh forecast. Same-task review only.

Read [rolling pipeline](ROLLING_NEWS_PIPELINE_20260930.md), Vault
`ROLLING_NEWS_PIPELINE_20260930/RESUME.md` and `NATIVE_FINAL.json`. Next operational
item: `joint_v11_mature_forecast_verification_v1`. Existing global research
preflight/independent review and offline interpretation work remain separate.
The currency rankers are inactive: Practice006 retired, source-conditionedV7
has zero decisions/forecasts/outcomes, V8 collection disabled.

The capacity and older diagnoses below are retained as dated history.

# Current capacity correction — 2026-09-30T13:56:05.677183+00:00

The storage blocker has been corrected and deployed. Original news receipts and
availability clocks are retained. The new transport has passed the old20,000-object
boundary with actual published and acknowledged batches; it is draining a backlog
of roughly20,000 source records. This is not yet full joint-forecast readiness.

The HTTP dashboard selects the verified V10 capacity cohort and, in the13:53 UTC
readback, admitted65 current price-forecast pairs and zero joint forecasts. The
joint reader is replaying retained history. It must then obtain a fresh caught-up
capture and satisfy unchanged model support before issuing forecasts. Do not
reinterpret the historical last-error field as a current failure without checking
later success and progress; transient read/status-write failures are recorded.

Read [capacity recovery](NEWS_CAPACITY_RECOVERY_20260930.md) and live Vault
`PROJECT_CONTEXT_LATEST.json`. Local evidence is `evidence/joint_news_capacity_20260930`;
final scoped packet is `JOINT_NEWS_CAPACITY_RECOVERY_20260930`.164 final-scope tests
passed, an isolated Git source restore passed, and every original row was compared
byte-for-byte against the pre-cutover backups. Same-task review only. No orders.

The following08:51 UTC diagnosis is retained as history, not the current status.

# Measured dashboard status — September 30, 2026

The September30 live recovery supersedes the offline-only status below. The user
explicitly authorized a flowing research pipeline and Git/Vault publication.

- Fresh quote, M1 candle and public-news collectors are running. The actual HTTP
  dashboard admitted **35 current price-forecast pairs** in the 08:51 UTC receipt;
  this is a dated measurement, not a promise of continuous all-pair coverage.
- The selected joint V9 worker validated and archived 680 original consumer
  observations. It still has **zero current forecasts**. Its fresh-news gate is
  correctly withholding them: the retained transport failed its next publication
  with `cas_store_work_bound` before it could catch up.
- The publication store retains 19,953 evidence objects (20,000 limit),
  200,680,373 compressed bytes (201,326,592 limit), and 2,046,097,910 expanded
  bytes (2,147,483,648 limit). Over 20,000 source entries remain beyond the last
  successful cursor. Waiting or restarting unchanged does not remove this bound.
- The rolling technical worker completed its outage catch-up cycle, then stopped
  gracefully for a runtime-capacity cutover. Its new configuration retains the
  identical numerical/store contract with a 16 GiB dataset cap and 32 GiB free-space
  guard. The previous cap was 4 GiB. Final readback is in the recovery packet.
- Recovery uses the canonical [pipeline entry point](PIPELINE_OPERATIONS.md).
  The expired September20 recovery configuration was replaced with a verified
  bounded profile through October7 08:14:50 UTC. The supervisor preserves both
  joint-role restart histories when switching out of isolation; restart circuits
  were not erased. Account state remains unrefreshed; no orders are enabled.

This is **partial operational recovery**, not full joint-pipeline readiness. Next
correction: qualify a bounded news-storage/partition successor with exact old
receipt replay and new source/cohort identities as required. Preserve the current
stores and model seals. Do not delete records, reset the source cursor, bypass
freshness, or relabel the unavailable joint forecast as current output.

Local evidence: `evidence/dashboard_live_recovery_20260930`. Shared packet:
Vault `DASHBOARD_LIVE_RECOVERY_20260930`. Git includes its compact knowledge copy;
large pre-retention news database backups and raw runtime data remain local with
hash references. Tests and same-task review are recorded separately. Browser UI
inspection was unavailable (no connected browser); HTTP and renderer tests ran.

## Earlier diagnostic record (historical)

# Dashboard diagnosis and crypto removal — September 30

The page and JSON endpoints responded with HTTP 200. Empty Forex panels were not a
server crash: the saved operational selection rejected changed feature-mapping and
feature-observation hashes (`dashboard_source_changed`). No fresh verified price/forecast
producer was running in the inspected inventory. News collection is separate. Account
state remains unknown without a current verified snapshot.

Follow-up review recovered the exact pinned feature revisions from Git. Changes add
optional forward-frame support; the dashboard's default full-envelope validation is
retained. After 71 feature/export and 24 display/selection tests passed, only the
display source bindings were requalified. Readback now identifies the actual remaining
conditions: price-only `stale_or_future_summary`, joint `model_source_changed` in its
news dependency. Model registries, study identities and freshness checks are unchanged.
Review: Vault `DASHBOARD_DISPLAY_SOURCE_REVIEW_20260930/REVIEW.md`.

Crypto Shadow is removed from the page, JavaScript polling, server readers and Forex
supervisor launch entry. `/api/crypto` returns HTTP 410 without reading data. All 36
inspected BIGTRIAD Crypto/Kraken/Aggressive Horizon tasks were already disabled, and
no matching crypto worker was running. Historical source/data were preserved. No crypto
collector was started; Forex news/capture ownership is unchanged.

The page now distinguishes a connected dashboard from unavailable live inputs. Existing
model/producer seals remain unchanged: presentation changes do not qualify model output.
Reviewing the selected source and restoring authorized fresh producers remain separate
work. Old signals must not be presented as live.

Checks cover a real isolated HTTP request proving the retired route returns 410 without
file reads, JavaScript syntax/status and Forex dashboard regressions. Stale label tests,
local configuration leakage in one fixture and a Windows command-length issue were
repaired. No numerical models were run.

Local evidence: `evidence/dashboard_cleanup_20260930`. Shared handoff: Forex Vault
`DASHBOARD_PROJECT_CLEANUP_20260930/REVIEW.md`. Only the dashboard was reloaded through
its existing Windows task; collector and trading processes were not changed.

## Resumed source qualification

The joint news dependency now matches its original registered SHA256: `44e66d85f82b52d6dc82e2e277bad31d2ee8c16304bec9c6a083a8b17eeb6c50`. The exact qualified collector was restored from Git; the newer interpretation remains available through a separate opt-in adapter. All68 joint registry pairs validate against the existing dependencies and contracts. Both producer registries and the display selection are byte-unchanged.

The current local readback and running `/api/main` return `stale_or_future_summary` for both producers, with no forecasts admitted. The source mismatch is resolved; fresh live prices and producer summaries are still unavailable. No services, capture, account or broker actions were performed.

Fresh checks:587 tests and13 subtests passed for the current collector, interpretation/adapter, repair-v2, selection and timing paths. The broader first run also exposed27 legacy repair-v1 failures; the same27 fail using the pre-change collector, with `guarded_member_evidence_missing`. This historical v1 defect is recorded, not repaired or hidden. The selected joint producer binds repair-v2. Same-task review only.

Next: qualify the missing current producer inputs and an authorized restart procedure, including current news IO routing and quote provenance, before any service launch. Global preflight remains blocked by current pointer-schema and manifest-coverage findings (the engineering environment passed; model runtime is not qualified); this repair does not clear that gate. See Vault `DASHBOARD_PRODUCER_QUALIFICATION_20260930/RESUME.md` and local `evidence/dashboard_producer_qualification_20260930/`.
