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
