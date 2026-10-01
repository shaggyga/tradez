# Saved 6-hour and 18-hour connections

The retained pipeline now has **15 connections across 11 elapsed horizons**:
5/15/30/60 minutes and 4/6/12/18/24/48/120 hours. Four added connections reuse the
original frozen matched Ridge and HGB models at6h and18h. The previous11 model
definitions, normalizers and68-pair universe are unchanged. Both methods remain
separate references; neither is claimed universally best. No model was fitted.

The original source is the catalog's `matched_remaining_saved` archive, SHA256
`70c3d4fb54fb86e79b4bcd65c9cd923768592887e5278c93bc27a7ab5244a10b`, run identity
`fc0a1423ca93b62e94f215b49a6a55aaf280524aadc1501b231efef4031a2763`.
Its original development test conditions forecasts on successive observations
toward one fixed July24,2024 endpoint. These saved weights now receive later
observations under the exact original26-feature transform. Current forecasts use
the completed minute midpoint and an exact elapsed6h or18h endpoint, with actual
issuance times and the existing input/receipt/clock refusals. Later-input inference
does not turn that small original cohort into independent live evidence.

## What passed

-68 focused tests, including15 new matched-consumer contract cases: original
  feature order, saved metadata/model identity, exact target, malformed/resealed
  Ridge parameters, altered bytes and missing-history isolation.
-270 original saved forecasts reproduced exactly through `SavedConnection`.
-24 canonical capsule payloads restored. Eight recorded forecasts, two pairs
  across four additions, replayed using copied original bar/receipt inputs and
  relocated model/metadata bytes within1e-12bps absolute tolerance. Tiny Ridge
  batch-versus-single-row floating differences are not claimed byte-identical.
-Existing runtime profile validation and actual dashboard HTTP200 readback:
  960 current forecasts across64pairs,15 tracking groups, current news. Readback
  took13.656seconds; this is availability evidence, not a low-latency guarantee.
  Browser visual QA was not performed.

The initial evidence script incorrectly treated the publication count summary as
the forecast payload. The successful saved publication was retained and read
directly after the script correction; it was not regenerated to conceal the error.
Raw failure and corrected output are both in the local packet.

## Remaining layer and target reconciliation

The original residual replay repair and curve population repair are complete;
their sealed successors remain authoritative. They were not rerun. New metadata
inspection verified exact parent-model matches for all four new connections in
the residual result (473 saved rows per6h method;338 per18h method). This enables
a scoped projection-consumer integration next. Current richHGB and ExtraTrees
forecasts are different parents and must not receive those historical corrections.

The historical curve result covers3,264 expected slots:2,156 complete and1,108
explicitly incomplete. Its negative result remains intact. Its full same-base
6/12/18/24/30/36/42/48h panel is not currently connected; mixing model families
does not satisfy that input contract.

Sixteen exact design targets still lack a qualified connection:1/3/10/20/45/90/
120/180/480minutes, next daily close, close-to-close, and2/3/5/10/20trading days.
The authenticated3,124-record historical census was inspected for target metadata.
Its profit/event classifiers are not equivalent to signed midpoint-return forecasts.
This is not proof that a suitable artifact can never be found. Missing exact
calendar targets require an explicit session/calendar model and endpoint contract;
120elapsed hours is not five trading days. The registry retains all these gaps.

`RECONCILIATION.json` records original result hashes, parent IDs, actual target
metadata, preserved historical metrics and remedies. No historical score was
presented as a newly run experiment. Position management remains separate and
is not enabled by these forecast connections.

## Checkpoint and replica

Local evidence: `evidence/remaining_connections_20261001`.
Shared packet: `REMAINING_CONNECTIONS_20261001`.
Review: substantive same-task review; independent review not performed.
Final Git receipt: `REMAINING_CONNECTIONS_PUBLICATION_20261001/RECEIPT.json`.

Use the matching Git source and the packet's `remaining_connections_capsule.zip`.
`CAPSULE.json` records its exact SHA256. The existing command restores verified
bytes, refuses conflicting local files, and does not fit models or start services:

```powershell
python -I -B tools/forex_retained_capsule.py restore --root . --capsule PATH_TO_CAPSULE --sha256 HASH_FROM_CAPSULE_JSON
```

Rollback requires the preceding Git/config revision and its original capsule;
preserve new issuance records under their registry identity. Only the existing
research supervisor/watchdog and context worker were restarted for deployment.
Other producers, other-chat EURUSD capture, restart history, order restrictions
and recovery expiry `2026-10-07T08:14:50Z` remain unchanged.

Exact next: **`retained_currency_projection_connection_v1`**. Reuse the original
projection solver over current matched6h/18h parent forecasts, preserve the
original weighting/controls, and explicitly account for all68pairs and graph
support. Verify original replay, current timing and relocated execution before
deployment. Qualify any saved learned-residual application separately; no refit,
automatic winner promotion or order action is authorized by this checkpoint.
