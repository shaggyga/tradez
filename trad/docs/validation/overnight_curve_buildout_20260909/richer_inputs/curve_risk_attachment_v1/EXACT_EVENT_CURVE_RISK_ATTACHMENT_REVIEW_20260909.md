# Exact-event attachment of original curve and M1 risk forecasts

The new, unregistered `trad/oanda_curve_risk_attachment_v1.py` implements a pure diagnostic join. It does not change the direction-admission module, any registered source, model, forecast, target, policy, position or worker. Fifty-five focused tests passed in 27.29 seconds. Synthetic successful joins are engineering fixtures, not evidence that the current live families are compatible or accurate.

`attach_risk_for_target` receives the full original curve/issue publication/consumption and risk issue/publication/consumption, original risk input bytes/source receipt/metadata/fixed fit bytes, the trusted registered source maps and curve model/cohort/policy identity. It calls the frozen curve `validate_consumption` and risk `validate_chain` before comparing events. The latter reconstructs every original risk node from its real input and fixed training artifact. Original curve semantic validation does not independently rerun the recovered model input/inference; that remains the registered producer's separate evidence boundary.

Six serial downstream read receipts bind the canonical identity and actual individual read start/completion of those original objects. Each original issue/publication/consumption clock must precede its own downstream read, and all reads must precede the supplied decision information cutoff. The new diagnostic samples and retains its actual computation completion after replay. This never claims that a later diagnostic existed at an earlier decision, refreshes the original forecast, or authenticates filesystem I/O from a self-declared hash. The caller must retain and verify original files, source closure and actual reads.

The join requires equal instrument/currencies/pip size, effective reference **price** epoch, explicitly described official-M close convention, exact decimal reference price, exact original native target, and zero-width exact target-selection policy. Original bar labels and durations remain separate: S5 label plus five seconds and M1 label plus sixty seconds may describe the same effective close only when the actual price and all other fields match. No clock rounding, resampling, interpolation, target resetting or silent conversion of a 0–7-second window is permitted. Missing/withheld/expired curve nodes and stale original decisions remain unavailable.

The caller names a candidate, incumbent or neutral distribution context. Nonzero side selects the corresponding long/short label family; this labels the requested interpretation and does not authenticate a position or validate candidate direction. Neutral context exposes the four common labels only. Every successful attachment retains **both** frozen methods, their original quantiles, scale and training support; neither method can be selected because it appears favorable. The directional candidate itself remains independent of a refused risk attachment.

The output is explicitly original full-horizon marginal risk. It is not conditional remaining-path risk, a joint direction/path distribution, calibrated coverage, a probability for a stop or fill, or a sizing/exit rule. The original candle bid/ask basis is retained, but no later actual entry basis is certified. Close-path and intrabar-envelope labels remain separate. No quantile rebasing, MFE-minus-MAE score, method mixing, or subtraction of already observed excursions is performed. Invalid original evidence raises; a valid but incompatible event returns explicit reason codes and no risk values, never measured zero.

## Actual bounded diagnostic

`actual_diagnostic_001/RETAINED_CURVE_RISK_ATTACHMENT_DIAGNOSTIC_20260909.json` is a fresh read-only observation from September 9, 10:16:44.435–10:16:45.756 UTC. The helper read 2,489,445 bytes in 1.321 seconds and bracketed the unchanged pilot 14-source and risk 8-source registries. Selection was the latest complete GBP/USD official-M chain in each original issue schedule, without looking at outcomes or choosing a favorable prediction. No broker request, issue, forecast replay, database write or runtime modification occurred.

| Field | Original S5 curve | Original M1 risk |
|---|---|---|
| Effective reference | 08:53:50 UTC | 10:16:00 UTC |
| Reference price | 1.3544 | 1.35310 |
| Requested/native targets | H1 09:53:50 UTC | 10:31 / 10:46 / 11:16 UTC |
| Target selection | First real bar at/after nominal, up to 7 seconds | Exact M1 target |
| New diagnostic result | H1 already elapsed; original identity retained | No exact corresponding event |

The exact reasons were `reference_price_mismatch`, `reference_price_epoch_mismatch`, `risk_exact_native_target_unavailable`, `target_selection_window_mismatch` and `original_target_elapsed_at_decision`. The old pilot issue cutoff was already closed; continued source collection is not a new issued H1 curve. This is one actual same-instrument mismatch diagnostic, not an exhaustive pair census or performance comparison.

## Smallest next compatible experiment

Reuse the original chain validators, strict M1 capture/label contracts, both fixed risk methods, and this attachment boundary. A separately specified cohort must issue both families from an actually observed common effective reference close and the same original native target/price convention. The curve producer also needs a defensible exact target policy compatible with the risk labels. The frozen recovered model's trained 0–7-second target selection cannot simply be relabelled exact; an explicitly supported new target contract or separately validated target-specific model is required. Both actual publications/consumptions and new downstream reads must exist before the new decision.

That prospective common-grid experiment would make the event attachment meaningful. It would not by itself establish calibrated conditional remaining risk, a profitable manager or an appropriate stop/size policy. Those are separate hypotheses and remain unimplemented here.
