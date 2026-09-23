# Shared-grid fresh-update management: document-only preregistration draft

Status: **design only; not registered, launched or approved for execution**. No study identifier, model artifact, source closure, activation clock or calendar dates have been assigned. Every execution, order, account, promotion and live-policy authority remains false. This document proposes a new research cohort; it cannot modify any September 9 curve, risk or paper cohort.

The question is whether genuinely recomputed forecasts for the **same original terminal** improve a USD-valued virtual manager over an otherwise identical manager using its initial forecast. A separate comparison isolates the original-direction admission condition. The three completed paper episodes motivate examining this semantic gap; they are already inspected development evidence, excluded from validation and not used to choose new numerical thresholds.

## Fixed event grid and scope

The first experiment is GBP/USD only, official midpoint, USD-valued paper accounting, with one common fixed terminal `T = R0 + 3600` for every arm in an episode. No cross-pair ranking, portfolio covariance or actual account balance is inferred. Nominal reference-close opportunities are:

| Original forecast reference close | Required native horizon | Original target | M1 risk counterpart |
|---|---:|---|---|
| `R0` | 60 minutes | `T` | 60-minute distribution issued from `R0` |
| `R1 = R0 + 1800` | 30 minutes | `T` | 30-minute distribution issued from `R1` |
| `R2 = R0 + 2700` | 15 minutes | `T` | 15-minute distribution issued from `R2` |

`R0` must satisfy the existing risk grid, `R0 % 300 == 60`; for example, 06:01 UTC is a reference **price-close** time, with M1 bar label 06:00. S5 and M1 original labels remain distinct. The episode terminal is never reset when an update arrives. A fresh rolling H1 ending after `T` is not an eligible incumbent update.

The retained native curve inventory contains 15, 30 and 60 minutes, but does not contain every intermediate remaining horizon on a one-minute or five-minute decision grid. Therefore this draft permits **three explicitly scheduled forecast opportunities**, not a claim of fresh forecasts every minute. Quote/position decisions remain every 60 seconds; between forecast opportunities, the retained latest valid model estimate is clearly labelled unchanged. Producing a fresh exact-target estimate at every decision would require additional native target heads or a separately validated model of residual horizons. No interpolation or inferred nodes supply that gap.

Proposed bounded calendar rule, to be resolved and frozen before launch: the first 20 UTC calendar weekdays strictly after actual registration, with episodes starting at reference closes 06:01, 08:01 and 10:01 UTC, yielding 60 scheduled episode attempts. Exact dates remain unset here. All dates and attempts must be enumerated in the eventual registry before data collection; missing quotes, holiday closures and failed initialization remain recorded attempts, not replacements. The schedule cannot be shifted to obtain better returns. Twenty dates and 60 attempts are a collection plan, not a power calculation or an independent sample size.

## Preconditions that prevent registration today

1. **Exact target semantics.** The frozen recovered second-ridge model was trained against the first real S5 bar at/after nominal within 0–7 seconds. The existing M1 risk contract is exact. Neither a coincident observed bar nor this draft permits relabelling the old model exact. Supply a new, source-bound exact-target predictor with a valid training/validation contract, or design and independently validate a different common target domain in a separate protocol. No old coefficient file or target policy is silently reinterpreted.
2. **Common original reference.** The new producer must select actually observed complete S5 and M1 rows ending at the declared reference `Ri`, retain both exact raw responses and original receipts, and prove identical official-M reference price and effective close time. A later complete S5 row cannot replace `Ri` because it happens to be the last returned row. Both full original feature windows remain causal and unfilled. Independent acquisitions may legitimately disagree; disagreement withholds the common-event diagnostic and is retained.
3. **Genuine fresh computation.** Every `Ri` update must use newly advanced substantive real inputs, a complete computation under the frozen new model, and a new issue/publication/consumption chain. Changed timestamps, wrapper hashes or rereading the initial anchor do not establish an update. Model-fitting and training-target availability must precede prospective input use; no episode outcomes enter fitting or online recalibration.
4. **New manager adapter.** Implement separate entry/rotation and continuation input channels in a new manager version. The old selector's single list serves both purposes; filtering refused entries out of it would accidentally delete incumbent estimates. Preserve the tested fractional-clock compatibility bridge without changing original DTO timestamps or seals.
5. **Explicit registration.** Freeze all source, model, fit, metadata and policy hashes, input/capture/storage bounds, episode calendar, initialization and run hard stop, evidence locations and resource budget. Complete meaningful synthetic and real-input compatibility tests, independent review and fresh empty-state activation. This document is not that activation.

## Information and action clocks

For each `Ri`, raw source reading, derived input validation, computation, issue, durable publication and independent original consumption must complete by `Ri + 20 seconds`, inclusive. This deadline reuses the M1 publication contract; it does not permit backdating. Actual downstream read evidence for both curve and risk must also be retained before the decision using it. Computation completion, file publication completion and the decision information cutoff are distinct clocks.

Prepare episode configuration, empty isolated states and calendar durably before `R0`; initialization must not depend on a subsequently observed return. The first planned action slot is `R0 + 60 seconds`. Continue 60-second scheduled slots through `T`, using actual information cutoffs and the existing ten-second lateness limit, with no catch-up burst. A valid update from `R1` or `R2` becomes eligible only at its next scheduled slot, respectively `R1 + 60` or `R2 + 60`, after its timely original and downstream receipts exist. It never changes a previous plan or fill.

All arms decide before any selected execution observation. The shared earliest permissible observation is `max(actual plan-computation completion + 1 second, actual durable plan-publication completion)`. Observe the first independently read, current, explicitly tradeable quote at/after that bound, polling at one-second intervals for at most ten seconds. Quote market time may be earlier if the quote is genuinely current and at most five seconds old; its actual observation must be after the bound. A current quote is an observed hypothetical bid/ask execution price, not a broker fill.

The full nonterminal entry observation window must end before `T`, with the existing 12-second entry cutoff. Terminal action is forced independently of whether a decision quote exists, but still requires an actual valid selected exit quote in `[T, T + 15 seconds]`. Missing terminal observation leaves the position unresolved; it is not liquidated at a made-up price or carried into a fresh flat account. New settlement state becomes known only after actual all-arm computation and durable state publication. No next decision may use an earlier fictitious known-state clock.

## Five predeclared isolated arms

| Arm | Information and entry/continuation treatment | Purpose |
|---|---|---|
| `no_trade` | Always flat; observes the same records | Zero-position reference |
| `fixed_initial_hold` | Initial original direction, one entry at the first eligible common slot, no re-entry or reversal, mandatory original terminal exit | Simple holding reference |
| `original_fixed_manager_reference` | Initial curve only; reuse original USD manager selection, including its existing signed terminal-distance interpretation | Original manager reference within the new common timing/target constraints |
| `direction_gated_static_manager` | Initial curve only; separate original-direction-gated entry/rotation and unfiltered signed incumbent continuation | Isolate the guard change from the original manager |
| `direction_gated_fresh_manager` | Same gated selector and settings as static, but consume eligible actual `R1`/`R2` recomputations to the same `T` | Isolate the information refresh from the guard change |

The fixed-hold arm's single initial eligibility window is the first common action slot; a refused/missed entry remains a flat held scenario, not an opportunity chosen later by return. Static/fresh gated arms receive the same initial chain, quote, rules and state; differences may arise only after a registered refresh opportunity. A new forecast can legitimately reverse its own original issued direction. The admission guard compares each candidate with **that forecast's** original direction, not forever with `R0`'s direction.

Candidate entry or opposite-side rotation requires nonzero original direction matching the rebased remaining direction. A comparable incumbent receives the unfiltered signed remaining estimate projected onto its existing side even when new entry is refused. Negative or zero continuation is retained, not converted to missingness. Original probabilities remain attached to their original reference-to-target event and are not rebased or blended with risk quantiles.

At a required refresh deadline, if the new curve is missing, late, corrupt, wrong-reference, wrong-target or has not advanced substantive inputs, the fresh manager records `required_refresh_unavailable`; it must not call the old curve fresh. From the next planned action slot, withhold new entries/rotations and treat the incumbent continuation estimate as unavailable, following the existing explicit exit-on-unavailable rule at the next valid shared quote. The static arm continues its registered static policy. A later `R2` update may become eligible at its own predeclared slot; no backfill of `R1` occurs. This fail-closed behavior is part of the new experiment and must be reported separately from numerical forecast reversals. An unavailable risk attachment alone does **not** disable a valid curve update: risk is diagnostic only.

## Shared economics and existing controls

Use the existing values unchanged: USD2,500 research notional per arm/episode, integer base units with increment one frozen at the actual decision quote, maximum entry spread 5 bps, entry cost ratio 1.0, switch incremental hurdle USD0.05, hypothetical slippage 0.1 bps per leg, maximum holding 3,600 seconds and the common original terminal. A GBP/USD position is valued directly in USD; this pilot does not infer external conversion quotes, account leverage or margin. Simultaneous paper arms are never summed as positions in one account.

Reuse bid/ask orientation, both-leg costs, atomic reversal, isolated state, exact original-target constraints, no-trade behavior and the original observed-quote/settlement chronology. Every arm has the same quote observation selection, resource limits, capacity of one position, costs and terminal rules. The original manager reference is explicitly wrapped by the new exact-target/calendar constraints; it is not a byte-for-byte replay of the entire old deployment.

Existing stop, trailing-stop, profit, time, curve-opposition and partial-reduction implementations remain mapped for later reuse. This first comparison must not enable or tune extra stop, take-profit, trailing, partial-size or risk-dependent rules in only the fresh arm. Such a change would confound information refresh with a different exit policy. Any later position-control study needs its own frozen trigger parameters, matching comparator and real future-quote observation rules; OHLC extremes cannot fabricate stop execution or barrier order.

## Risk attachment and evaluation

Run the accepted exact-event attachment validator on each same-reference, same-target original curve/risk pair. Preserve both fixed risk methods and all original label/quantile identities. Missing exact matches are explicit, and original price/cost bases are retained. At `R1`, a genuine 30-minute risk forecast to `T` is a **new original 30-minute forecast**, not an algebraic update of `R0`'s H1 marginals. Even that new forecast does not automatically describe the drawdown of a position entered earlier at a different price or the path conditional on its realized history. No risk values route to sizing, stop/exit, entry or promotion in this experiment.

Primary comparison: terminal realized virtual USD of fresh versus direction-gated static on the same predeclared episode denominator. Secondary comparisons are static-gated versus original-fixed manager, and each manager versus fixed hold/no-trade. Decompose every realized difference into gross midpoint movement, actual retained entry/exit half-spreads and fixed assumed slippage, using the accepted verifier and cost attribution principles. Report per-episode and calendar-date totals; no arm or row is an independent trial by assertion.

Preserve all 60 scheduled attempts and all due slots, including failed initialization, unavailable updates, missed decisions, late computation/publication, absent quotes and unresolved terminal positions. Do not assign missing episodes zero PnL. Report completed common-arm terminal denominator and every excluded/missing episode separately. The full operational comparison includes required-refresh failures under the registered policy. A secondary availability description may show support with all refreshes present, but must not become a selectively favorable replacement result or a replay at different fills.

Independently score each newly issued curve/risk forecast at its **own** original reference and target. These forecasts share terminal prices and overlapping paths; repeated origins/methods/labels are not new independent trials. Preserve zero-return magnitude and constant-probability benchmarks where defined, sign-zero treatment, original uncalibrated probability scope, and per-method risk quantile calibration/missingness. Position efficacy remains separate from forecast correctness. This draft sets no automatic promotion or profitability threshold and authorizes no change in trading permissions.

## Later feature ablation, kept separate

A later registered experiment may join existing technical inputs with entry-eligible news/blurb/factor features using original independent source visibility, classification/version, expiry, original event clocks and maturity/response-known cutoffs. Compare technical-only, admitted-news-only where defined, and combined variants on the same original event grid and matched prospective endpoints. Retain missingness instead of neutral defaults, deduplicate shared events, and do not admit post-entry rationales or profitable-response-selected memory. This is a separate new feature/fitting/validation study; neither the present narrow joint results nor this management design establish a combined news edge.

## Documentary completion boundary

This protocol can be reviewed and retained now. Registration remains blocked until the exact-target predictor, common reference producer, separate entry/continuation manager version, fit/source identities and actual future schedule are concretely specified and tested. No endpoint GET, fitting, file-store publication, runtime change, scheduled automation or paper session is launched by this draft.
