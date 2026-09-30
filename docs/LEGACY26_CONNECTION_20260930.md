# Saved legacy26 connections

Two preserved Extra Trees regressors now have an input adapter for 240-minute and
7,200-minute elapsed returns. They join the existing nine connections; the earlier
4-hour/120-hour HGB forecasts remain separate references. This is later-input
inference using saved 2024 development models, not new training, a globally best
model determination, a calendar-day target or an execution policy.

## Original inputs and clocks

`trad/oanda_retained_legacy26_v1.py` calls the unchanged original
`stage_c_alignment_integrity_v2/retained_direction_features_v1.py::_technical`.
It reads the existing technical SQLite store in a read-only snapshot, verifies
bar and receipt hashes, instrument identity and actual observation clocks, and
refuses unresolved revisions. It requires a preceding gap inside a bounded
10,082-row search before accepting the current session's start. It retains the
last hour and complete current contiguous session; a truncated tail is never
treated as a fresh session. Reads have a 15-second total SQL budget.

The 24 original technical fields and two current bid/ask entry-cost fields stay in
their original order. Missing windows use the original explicit flags and zero
sentinels; no price is filled or rolling216/228 column renamed. A forecast records
the selected history identity and feature identity separately from the original
current-bar receipt. Its reference is the completed minute close, and its target
is that exact close plus 240 or 7,200 minutes. The existing outcome reader requires
that exact minute endpoint for these aligned targets; missing endpoints remain
pending/unavailable rather than borrowing a later price.

## Reuse and evidence

Both fits use the preserved cutoff `1721606400`. The source capsule is
`EXTRA_TREES_AND_MAPPING_COMPLETION_20260930/artifacts/extra_trees_saved.zip`,
SHA-256 `297353184a803ac9544f1d5cbf19e42a0eaf49e12b66b3f62441594aa337e031`.
The original prepared-input capsule is independently hash-verified. Model bytes,
metadata fingerprints, feature schema, target and fit readiness are checked
before use. Neither base models nor original prepared features were refitted.

The preceding `RETAINED_TRACKING_20260930/SAVED_LONG_MODEL_RECONCILIATION.json`
records the matched historical MAE comparison. Extra Trees was lower at 4 hours
and 120 hours on those inspected development populations; the connected HGB was
lower at 12, 24 and 48 hours. This is not independent confirmation or a live
performance ranking. The two new connections retain separate model identities
in the dashboard's outcome table.

## Verification

- 104 focused tests passed, including gap/session boundaries, future inputs,
  altered receipts, revisions, model readiness, refusal isolation and clocks.
- All 2,696 preserved frozen forecasts replayed with zero numerical difference,
  including through `SavedConnection.predict_entry`. These are replications of
  the same forecasts, not two independent samples or new forecast evidence.
- Actual read-only qualification found 58 fresh pairs in its dated input read;
  isolated current inference later produced 627 forecasts across all 11
  connections. Availability changes with the source clock.
- 23 capsule payloads restored as exact bytes. `CAPSULE.json` records the final
  capsule hash; original nine connection definitions, weights, normalizers,
  instrument universe and unsupported-target list remain unchanged.
- JavaScript syntax and native runtime profile validation passed. The final
  native/API readback is in the checkpoint's `LIVE_ACCEPTANCE.json`.

The first native dashboard API requests timed out. An isolated diagnostic HTTP request
stack included SciPy's cold import on the request thread: merely reading saved
JSON imported the specialist estimator stack. The specialist import now occurs
only inside inference. The cold isolated HTTP request then returned 200 in 3.08
seconds; a regression test confirms the display reader imports neither SciPy nor
scikit-learn. No estimator calculations were changed. Browser visual QA was not
available in this session.

## Handoff

Local evidence: `evidence/legacy26_connection_20260930`.
Shared packet: `LEGACY26_CONNECTION_20260930`.
Final source/Git/Vault receipt: `LEGACY26_CONNECTION_PUBLICATION_20260930/RECEIPT.json`.
Review is substantive same-task review, not independent review. Orders, paid calls,
new fits and the other chat's EUR/USD capture are outside this change. Existing
native recovery expiry remains `2026-10-07T08:14:50Z`.

Exact queue next remains `data_freshness_and_remaining_horizon_reconciliation_v1`:
reconcile original residual/curve consumers and exact remaining elapsed/calendar
targets using saved artifacts and their original populations. Do not redo the
legacy26 adapter or replay unchanged. Separate current news attribution, joint
history support and position-management qualification remain unfinished; connected
forecasts do not imply an active manager or validated trading strategy.

The regular dashboard task also ran at Below Normal process priority under the
competing research workload. Its existing task priority changed from 7 to 4
(Normal); no new task, High/Realtime priority or trading service was added. A
Normal-priority readback returned in 6.807 seconds while context was in cooldown.
The supervised context restart limit/history was preserved; the next permitted
restart after deployment was 2026-09-30T23:50:48.469Z. The final native receipt
records actual fresh outputs after recovery, not merely that saved deadline.

Additional restore verification reproduced four recorded current forecasts
(two pairs and both added models) exactly from an isolated receipt/bar fixture
and relocated model payloads. The reconstructed feature/history hashes matched
the originally recorded inference. See RESTORED_LIVE_REPLAY.json; this is
replication of existing observations, not new scientific evidence.


Final native readback 2026-09-30T23:51:45.345975+00:00: HTTP 200 in 7.899seconds; 671 current forecasts across61pairs and11connections; news and per-registry tracking current. Existing supervisor recovered automatically without restart-ledger reset. This is engineering/readback evidence, not a model skill or trading result.
