# Retained prediction tracking and data diagnostics

The nine retained elapsed-horizon connections now have a separate prospective
outcome monitor. It records forecasts actually observed before their targets,
keeps exact model/input/reference/issue/observation identities, and compares
forecast error with a zero-return control on the same observed targets. Original
H1 price/joint ledgers and saved model weights are unchanged. No new fit or order
action is part of this change.

`trad/oanda_retained_forecast_tracking_v1.py` stores records in
`trad/data/retained_connection_20260930/tracking.sqlite` and publishes
`tracking_current.json`. The existing context worker calls it once per minute;
there is no additional service. The dashboard shows settled, pending and
unavailable counts and mean absolute errors for the current registry, separately
by connection and horizon. Older registry groups stay in the ledger.

Repeated inference of the same model, input, origin and prediction counts once.
`issued.sqlite` still preserves every original publication. The tracking ledger
does not import earlier issuance retrospectively. Its actual first-observation
time remains immutable. A missing target retries after five minutes without
blocking other targets; after one extra day it becomes explicitly unavailable.
Original source revisions refuse settlement. Endpoints use the first completed
M1 close at or after the exact elapsed target, with actual availability recorded.
These are diagnostic close-price outcomes, not fills, policy returns or proof of
future skill. Overlapping predictions remain dependent.

The monitor also records news interpretations and the retained forecasts observed
together for each explicit currency/pair. It preserves original interpretation
identities and availability; it does not imply that these technical models used
the typed context. The existing original-H1 news mapping stays separate.

The tracking store has a two-GiB cap, SQL time budgets, bounded settlement batches
and no silent deletion. A tracking error is exposed separately and cannot stop
news capture or retained inference. Capacity needs observation; reaching the cap
requires an explicit archival checkpoint, not changing timestamps or deleting
evidence to keep a green status.

## Input handling

The retained reader retries at most three paired reads when the producer's
feature and status files are between publication generations. It still requires
matching generations and the original receipt/freshness checks. Publications now
retain root source errors, receipt errors, feature/quote counts, generation and
per-pair refusal reasons. Real missing history and stale quotes remain unavailable.
Optional model import and even diagnostic-write errors are isolated from headline
capture. No input was filled, backdated or assigned a looser age limit.

## Verification and deployment

90 focused tests passed, including prospective clocks, repeated publication,
changed models/targets, same-target controls, missing/revised outcomes, future news,
source drift, bounded publication retries and optional-import failure. JavaScript
syntax and native profile validation passed. All 18 capsule payloads were restored
as exact bytes in a separate directory. This is byte retrieval, not a new numerical
replay; the preceding package's 1,224 prediction replay remains historical evidence.

The source update encountered the retained context role's existing three-restarts
per-30-minute limit. Its retry history and limit were preserved. Recovery proceeds
through the existing supervisor after cooldown; see dated native acceptance in
`evidence/retained_tracking_20260930`. This checkpoint does not promise uninterrupted
feed availability. Existing recovery expires at `2026-10-07T08:14:50Z`.

## Saved model reconciliation

The historical census was authenticated: 3,124 records and 63 target/outcome
combinations. Many are continuation/reversal/profit/stop-policy classifiers;
their probability outputs cannot be substituted for signed-return forecasts.
This census is navigation, not a fresh validation of every original model.

Frozen 2024 Extra Trees and currently connected long-horizon HGB scores have
identical target/support hashes and row counts for these comparisons:

| Elapsed target | Connected HGB MAE, bps | Extra Trees MAE, bps | Matched rows |
|---|---:|---:|---:|
| 4 hours | 16.5399 | 16.2565 | 1,262 |
| 12 hours | 27.2903 | 27.3009 | 1,201 |
| 24 hours | 43.0001 | 43.6401 | 1,070 |
| 48 hours | 65.6018 | 70.1534 | 803 |
| 120 hours | 57.9036 | 55.1905 | 803 |

These are reused inspected development results, not a new experiment or universal
model ranking. The 4-hour and 120-hour Extra Trees candidates need their original
24 technical fields plus two entry-cost fields. Reuse
`stage_c_alignment_integrity_v2/retained_direction_features_v1.py::_technical`;
do not rename the rolling 216/228 fields as those inputs. In particular,
`tech_session_age_hours` requires the actual preceding contiguous run, and missing
minute flags/rates use that original function's rules. A bounded live adapter must
prove history/observation support, exact preprocessing and saved forecast replay
before these candidates are connected. No model switch occurred in this package.

The older raw-news/ranker/management audit is preserved in the local predecessor's
`followup_audit/REVIEW.md`. V7 has zero forecast/outcome records; V8 collection is
disabled. Historical position mechanics are not an active manager for the nine
retained predictors. No broker execution was enabled.

## Exact continuation

Continue `data_freshness_and_remaining_horizon_reconciliation_v1`: finish the
legacy26 live-input adapter and authenticate/replay the two supported Extra Trees
connections, preserving existing alternatives and controls. Reconcile residual,
curve and exact missing elapsed/calendar targets using original artifacts. Calendar
targets need their own qualified session contracts; elapsed hours are not trading
days. New fitting, typed-news experiments, broker actions, GPT/advisor comparisons,
paid calls and other-chat EUR/USD capture remain outside this scope.

Local evidence: `evidence/retained_tracking_20260930`. Shared packet:
`RETAINED_TRACKING_20260930`. Read its final receipt for Git and capsule hashes.
