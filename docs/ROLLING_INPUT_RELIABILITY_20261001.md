# Rolling input publication repair — October 1

The technical producer and retained-model reader now tolerate brief Windows file
locks without accepting mixed generations or renewing old timestamps. The live
pipeline was verified after deployment at 13:32:49 UTC: HTTP200, 2,543 eligible
retained forecasts across39 connections, current currency-news context, one
supervisor and current role liveness. Joint price/news reported forecasts for63
pairs, four unavailable and one warming. These are observed availability counts,
not accuracy or trading-readiness claims.

## Defect and repair

A real two-file publication test reproduced `PermissionError: [WinError 5]` when
the producer replaced a JSON file while a reader held it open. This can prevent
the status publication after the feature envelope advances. A separate native
probe showed that DELETE sharing alone did not solve replacement on this host;
that attempted reader change was discarded.

The existing V2 technical worker now retries **only PermissionError** during
atomic replacement: six attempts, at most1.55seconds of sleep. It writes the
payload once and retries those exact bytes. Generation, issue and observation
timestamps remain unchanged. Persistent failure still raises and preserves the
previous destination; its temporary file is removed. Original feature calculators,
observation contracts and stored records are unchanged.

The retained reader retries a complete before-status/envelope/after-status read
five times, with at most750ms of sleep, including temporary permission or missing
file errors. Invalid JSON, size and schema failures are not retried as sharing
failures. It still refuses unmatched generations. Exact attempted generation,
producer status/reason and read errors now reach forecast input diagnostics.

This repairs a reproduced Windows failure mechanism. The four-minute live
baseline capture saw six publication observations and no unmatched generation;
it did not reproduce the earlier historical outage. Other causes of intermittent
input failure are therefore not declared eliminated.

## Deployment and verification

- 75 consumer, receipt, integration and publication tests passed locally.
- The same75 tests passed from a relocated source-only fixture capsule.
- 20 operational tests and two subtests passed against the deployed candidates.
- The actual runtime profile passed source/config/argument/expiry validation.
- All39 saved-model definitions are unchanged. Two reader source bindings changed;
  the other19 registry bindings and all numerical model artifacts remain unchanged.
- All16 previously recorded database contract/runtime identities remained byte
  identical; a new operational runtime was appended. Historical seals were not
  overwritten. No fit or model-history reset was performed by this repair.
- Actual dashboard publications used the new registry, matching generations and
  current data. Original freshness gates remained in force.

Cutover attempts rolled back when Python wrapper processes exited during identity
checks. The deployment script now verifies process instances and waits for exits;
it never ignores an identity mismatch on a running process. The final deployment
succeeded. One explicit context-worker recovery followed the maintenance cutover;
automatic restart ledgers and limits were preserved. No account/order action or
other-task EUR/USD recorder change occurred. Recovery expiry remains October7,
08:14:50UTC.

Review is substantive same-task review, not independent review. A failed initial
portability staging attempt exceeded Windows path length; it was preserved as an
unsuccessful attempt. The final source-only fixture capsule is deliberately scoped
and does not claim to restore the saved models or live database.

## Resume and restore

Local evidence: `evidence/rolling_input_reliability_20261001`. Vault packet:
`ROLLING_INPUT_RELIABILITY_20261001`. Exact source/config identities, original
backups, commands and outcomes are in the packet and local journal. Git carries
the runnable source; the source capsule supports relocated fixture tests.

Do not roll back one pinned source file in isolation. Stop only the affected owned
controllers/workers, restore the recorded source/config/registry group, validate
the profile, then restart that same group. Preserve current and historical
database rows and retry histories. Live operation needs the existing verified
model/data artifacts and qualified runtime; a fresh clone must follow reuse
instructions rather than refitting missing models.

Next remains `live_news_input_reliability_v1`: examine any fresh collector
progress/read refusal using actual clocks and the new diagnostics. Cumulative
`last_error` is not a current failure. Joint warmup/support requirements remain
real, and most pairs have now progressed beyond warmup. After input reliability,
resume `retained_management_contract_qualification_v1`; position management is
still unqualified and inactive. New fits remain stopped by the user.
