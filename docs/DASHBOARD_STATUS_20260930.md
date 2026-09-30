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
