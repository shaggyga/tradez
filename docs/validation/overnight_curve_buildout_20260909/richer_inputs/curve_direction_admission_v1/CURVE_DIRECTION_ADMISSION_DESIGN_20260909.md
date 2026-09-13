# Original forecast direction and remaining-move admission

The smallest addition is a separately versioned, pure semantic envelope around the existing native-curve adapter. It names two different quantities and prevents the second from impersonating the first:

- **Original forecast side:** sign of the issued node's predicted move from its original reference price. This is not the market direction at a later issue/read time.
- **Rebased remaining side:** sign of the unchanged terminal estimate minus the current decision midpoint. This is arithmetic using an old forecast and a new quote, not evidence that the model has produced an updated conditional forecast.

The new-entry condition is exactly `original_side != 0 and remaining_side == original_side`. An opposite remaining side produces `countertrend_from_original_terminal_crossing`. A neutral original node never gains directional evidence from subsequent price drift. Zero remaining move is valid zero continuation, with no entry admission. No epsilon, age, confidence, cost or switching threshold is added or selected from episode 02.

This is one necessary semantic condition. It does not establish complete trading eligibility, predictive edge or correct confidence, and it does not prevent every repeated same-direction entry. Its treatment of episode 02 is a structural fixture, not a counterfactual profit calculation or policy performance claim.

## Implemented pure interface

New unregistered module: `trad/oanda_curve_direction_admission_v1.py`.

```python
admit_candidate_for_target(
    curve, publication, consumption, *,
    decision_epoch, target_epoch, quote, metadata,
    expected_source_bindings, maximum_quote_age_sec,
    target_window_policy='exact_only', incumbent=None,
) -> sealed_admission

validate_admission(admission, curve, publication, consumption, **arguments)
    -> independently_rebuilt_admission
```

Optional incumbent descriptor has exact keys `instrument`, `side`, `original_target_epoch`, `observed_epoch`, `state_sha256`. It is immutable caller evidence with a checked original decision cutoff; the pure helper does not authenticate the caller's state read or create a position. Different incumbent instrument/target is explicitly incomparable, with no silently changed horizon.

The envelope retains the original verified numerical candidate and exposes separate channels:

| Channel | Meaning |
|---|---|
| `new_entry` | Necessary original-direction condition and explicit refusal reason. Numerical entry candidate is supplied only when that condition is met. All execution/authorization flags remain false. |
| `opposite_side_rotation` | Same condition, plus an actual comparable incumbent on the other side. It is not a completed cost/holding/switch decision. |
| `continuation` | Original estimate remains available independently of entry admission. For a comparable incumbent, `incumbent_signed_remaining_move_pips = incumbent.side * remaining_move_pips`. Negative continuation stays negative; it is not erased as missing. |
| `original_prediction` | Original reference, native pip size, signed prediction, issue time, terminal estimate and original probability scope. |
| `forecast_update_status` | `not_assessed_by_direction_gate`; `fresh_input_update_verified=False`. The helper never calls an ID change, new read, new seal or republication an independent input update. |

Malformed evidence raises through the existing strict contract. Existing expiry, native-target absence, missing/stale/nontradeable quote, and target-window refusals remain unavailable rather than becoming zero predictions. `validate_admission` replays the complete original curve/publication/consumption/quote arguments; merely recomputing an envelope hash cannot turn a refusal into admission. Actual model-computation or I/O clocks are not fabricated: this helper's clock is an explicitly supplied decision context, not a newly claimed execution receipt.

## Existing branches to reuse; integration deliberately deferred

`oanda_curve_management_adapter_v1.py:34–76` already validates the original publication/consumption, age, exact target and quote conditions. Lines 101–105 compute the unchanged-terminal difference; lines 131–156 retain original reference/pips/probability/uncertainty. The new module calls that function without editing it.

`oanda_curve_management_replay_v1.py:318–438` already checks native target/quote identity and computes USD sizing, costs and candidate values. `choose_usd_action` at lines 483–530 already values the incumbent with its original side/units, retains continuation below the new-entry cost hurdle, and compares hold, exit and rotation on the same dollar basis. These arithmetic branches should be reused in a **new** selector adapter accepting separate entry and continuation pools.

Do not simply remove direction-refused entries from v1's single `candidates` argument. That same list supplies the incumbent estimate at line 502; filtering it would manufacture `incumbent_estimate_unavailable` exits and change an unrelated behavior. A later small versioned selector should use admission-filtered rows only for `eligible`/rotation alternatives, and unfiltered, verified continuation rows for the incumbent lookup. Existing cost, conversion, execution-delay, deadline and failure rules can stay unchanged. No active selector or management worker has been changed here.

The existing paper worker explicitly consumes its initial anchor again at `oanda_observed_curve_management_worker_v1.py:147–171`; `oanda_observed_curve_management_v1.py:151–180` rejects replacement of that original node. A future current-update reader therefore needs its own version and experiment registration. Repeated consumption of the initial anchor cannot be relabeled as a live model update.

## What would qualify as an independently updated forecast

A future reader must validate actual input advancement independently of the envelope: retained real input bytes and their source observations, a separate genuine computation over those inputs, and the exact issued/publication/consumption chain available before the decision. A changed digest alone does not prove different information: wrapper clocks, reserialization, repeated observations and republication can all change hashes without changing model inputs. Input advancement must be established using the producer's substantive capture lineage and observation times, with source closure and approved model/cohort identity checked.

The update must contain an original native target comparable to the current management deadline. A newly issued rolling H1 often has a different terminal time; that is a new forecast domain, not an update of the incumbent's old H1 endpoint. Missing exact-target support remains `incomparable_native_target`; no horizon interpolation or target reset is implicit. New-entry candidates with their own native deadlines are a separate decision from valuing continuation to an incumbent's old target.

Once such a forecast is independently supplied, its own issued direction may genuinely be opposite to the prior forecast. It still passes through the same original-versus-remaining semantic condition. The implemented envelope does not certify the external input-advancement step and does not pretend to provide this future reader.

## M1 risk distributions: diagnostic attachment contract only

Reuse `oanda_m1_risk_distribution_contract_v1.validate_chain` at lines 280–291 to replay the original M1 raw capture, source receipt, fit artifact, issued nodes, publication and consumption. Its issue/publish/consume boundaries are already explicit at lines 144–211 and 248–277. A future attachment should bind these original hashes plus the actual attachment read/observation time; both the original risk publication/consumption and the new downstream observation must precede the relevant management decision. Rereading does not refresh the original distribution's origin or forecast clock.

Recommended future pure API:

```python
attach_risk_diagnostics(admission, verified_risk_chain, *,
                        side_context, attachment_observation, expected_risk_sources)
    -> sealed_diagnostic_attachment_or_explicit_mismatch
```

`verified_risk_chain` must be rebuilt from full original evidence or an independently authenticated immutable read handle; an arbitrary caller dictionary bearing a self-declared hash is insufficient. The diagnostic attachment itself may remain unavailable without invalidating the directional candidate.

The exact join key is instrument and currency metadata; reference **price** epoch (with its label/duration separately retained); compatible explicitly verified official-midpoint convention and reference price; native target price epoch; target-window semantics; and a named side context. Preserve the distribution's original cohort, method, label, quantile levels and training-artifact identity. Do not round S5 clocks onto M1 labels or join merely on pair and the word H1. Different bar conventions/targets remain explicitly unmatched unless a separate validated mapping contract exists. In particular, the current paper's S5 reference/target seconds and 0–7-second training target window do not automatically equal the risk companion's exact M1 grid and exact nominal targets. The small first implementation should refuse this pairing, not fabricate a useful attachment.

| Side context | Original risk labels to expose unchanged |
|---|---|
| Either side / distribution context | `signed_terminal_bps`, `absolute_terminal_bps`, `mid_future_range_bps`, `realized_path_variation_bps` |
| Long candidate or long incumbent | `long_close_MFE_bps`, `long_close_MAE_bps`, and separately `long_envelope_MFE_bps`, `long_envelope_MAE_bps` |
| Short candidate or short incumbent | Corresponding four `short_*` labels |

Candidate side and incumbent side require separate named attachments when they differ. Neutral is not a long or short entry. Both fixed methods (`training_empirical`, `training_empirical_volatility_scaled` in the issued contract) and all original quantiles remain separate; do not select whichever appears favorable.

The label definitions at `oanda_m1_path_risk_labels_v1.py:136–169` use original reference midpoint as the bps denominator. Long excursion uses original entry ask versus future bid; short excursion uses original entry bid versus future ask. Thus these path labels already include the original candle-close spread geometry. A virtual position opened later or at a different quote is **not** that original hypothetical entry: retain `entry_basis_matches=False` and do not call the attached values its calibrated drawdown or stop-loss distribution. Original full-horizon risk is also not the conditional remaining-path risk after observing part of that horizon.

No quantile addition, subtraction of realized excursions, cross-horizon interpolation, MFE-minus-MAE ranking, sum across methods, joint direction/path probability, barrier ordering, fill probability or position sizing follows from this attachment. The 10/50/90 percentiles and nominal 80% interval are unconfirmed marginal predictions, not calibrated joint uncertainty. Envelope intrabar extrema remain separate from candle-close path diagnostics.

## Bounded implementation and evidence

Parent approved the pure admission module and targeted tests after the machine-readable design was retained. Risk attachment, split-pool selector, actual model-update reader, new manager experiment and runtime activation remain deferred. The tests use synthetic geometry, valid original contract chains, neutral/zero cases, incumbent-side values, expiry, invalid clocks/identities, reissued/reread evidence, tamper/reseal attempts, detached outputs and low Decimal precision. They do not retest episode 02 as a strategy, optimize thresholds, or claim performance gains.
