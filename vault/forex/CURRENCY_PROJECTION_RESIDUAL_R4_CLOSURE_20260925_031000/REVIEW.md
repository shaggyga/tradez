# Residual R4 publication and restore/replay closure

Status: same-task R4 closure complete; independent scientific review remains pending.

This compact packet closes the unfinished R4 publication/restore portion of `currency_projection_residual_layer_comparison_v2` without rerunning the residual calculation. The preserved result remains in `CURRENCY_PROJECTION_RESIDUAL_REPAIR_20260925_000700/evidence/RESIDUAL_RESULT_V2.json` and is referenced by SHA-256 `a1c518b93224cfeeb7a1f66ed5905ffb5d7138f0021f8c3bee50383b72bb295c` (59349278 bytes).

## What was verified now

- Replayed focused residual tests from the current source tree: 14 passed. The only warning was pytest cache write access.
- Re-read the preserved result and verified exact counts: 384 scopes/snapshots, 3786 learned rows, 126 paired score rows, 195 parent payloads, 136 rows at 2,520 minutes, and 0 rows at 2,880 minutes.
- Re-read the predecessor manifest and source packet; the compact publication records the result descriptor, test evidence, local work log and pending review state.

## Review status

R1, R2, R3, R5, R6 and R7 remain preserved from the repair packet. R4 is closed for compact publication and restore/replay evidence. This is a same-task review, so `independent_review=false`; independent scientific review is still pending and nonblocking for eligible siblings by user clarification.

## Readiness

- engineering_ready: false
- forecast_evidence_status: offline development residual diagnostic preserved; not confirmation
- policy_evidence_status: not evaluated; no policy replay
- demo_authorization_status: not granted

## Next

Continue the timed queue with `forecast_curve_shape_layer_comparison_v2` support check. Do not recompute the residual result unless the preserved hash becomes unavailable or a concrete defect is found.
