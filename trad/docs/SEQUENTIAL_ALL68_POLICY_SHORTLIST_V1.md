# Sequential All-68 Policy Shortlist V1

Status: historical discovery specification; research-only; no execution authority

## Bound discovery evidence

This frozen shortlist was derived only from the then-current, already-inspected
all-68 replay cohort
`sequential_all68_portfolio_batch_replay_v1.9d6e7e0f27ad289c4c83`, bound to
source pack `sequential_replay_source_pack_v1.ee6d6fd4d38744ecc1da`. It is a
policy-definition exercise, not confirmation. Every definition selected here
must be frozen before it is evaluated on a later, untouched cohort.

The immediate predecessor `7681e61bd31ac8877bd8` contains the same historical
decisions and result but used a mutable wall-clock field in otherwise unchanged
cohort bytes. It remains preserved and superseded. Replay `9d6e7e...` later
added a deterministic schedule-bound timestamp and byte-immutable rerun
contract. The current cross-runtime-portable successors are source pack
`sequential_replay_source_pack_v1.b9d1526f3dfa057bd06d` and replay
`sequential_all68_portfolio_batch_replay_v1.efda27295d5107241262`; they retain
the same historical decisions and **-120.25-pip** result. The frozen shortlist
implementation is now recorded as challenger
`sequential_all68_policy_challenger_v1.0b5267c7a5c730a150cf`. None of these
identity changes creates new market evidence.

The baseline used an expected-move-to-cost score threshold of 0.75 and lost
120.25 pips across 110 execution legs. Small, predeclared diagnostic variants
showed that greater abstention reduced turnover and loss but did not produce
edge:

| Diagnostic | Candidate threshold | Primary legs | Same-window result |
|---|---:|---:|---:|
| frozen baseline | 0.75x | 110 | -120.25 pips |
| cost hurdle | 1.25x | 30 | -34.35 pips |
| cost hurdle | 1.50x | 18 | -13.35 pips |
| cost hurdle | 2.00x | 8 | -7.50 pips |
| no trade | n/a | 0 | 0.00 pips |

These are post-selection training results. The 2.00x row is not a winner; it
is merely the least-negative traded row in this small inspected comparison.

## Logic defect exposed by the replay

The V1 rotation rule compares the best new candidate with the incumbent only
when the incumbent still clears the entry threshold and remains in the ranked
candidate list. If the incumbent is directionally aligned but falls below the
entry hurdle, its continuation score becomes absent rather than explicitly
measured. V1 then exits or rotates as though continuation value were zero.

That behavior must remain immutable in V1. V2 must calculate an incumbent
continuation estimate separately from new-entry eligibility and compare the
economics of holding, exiting, and switching before choosing an action.

## Frozen research arms to implement

1. `no_trade`
   - Zero positions, orders, spread, slippage, and value.
   - Required comparator at every scheduled global clock.

2. `cost_hurdle_2x`
   - New entry requires predicted remaining gross movement at least two times
     the recorded executable round-trip cost.
   - This is a frozen discovery hypothesis, not an authorization threshold.

3. `remaining_move_calibrated`
   - Predict remaining executable movement, not trailing movement or direction
     alone.
   - The estimator must be fitted only within the declared training partition,
     versioned, and applied out of fold. Missing calibration means abstain.

4. `explicit_hold_vs_switch`
   - Recompute the incumbent's direction, remaining movement, liquidation cost,
     and continuation value even when it is not eligible as a new entry.
   - Compare `hold`, `exit`, and `rotate` after current unwind cost, prospective
     new-entry cost, latency, and missed-entry stress.
   - A missing incumbent estimate cannot silently become zero; it produces an
     explicit unavailable state and a fail-closed action.

5. `factor_conflict_suppressed`
   - Deduplicate candidates by signed currency resources at each global clock.
   - A switch that repeats the incumbent signed-currency thesis must clear a
     stricter incremental-value hurdle than a disjoint thesis.
   - Shared USD, JPY, or other currency exposure cannot count as independent
     confirmation.

## Required comparisons

Every arm uses the same scheduled clocks, source paths, bid/ask quotes, latency,
cost contract, capacity, and terminal flattening. Report paired differences
against:

- no trade;
- V1 baseline;
- highest raw predicted movement;
- lowest-cost eligible candidate;
- hold-current-position/no-rotation; and
- deterministic random selection from the identical eligible set.

One global clock is one market-repetition unit. Pair contexts, candidate
variants, counterfactual branches, shared currency factors, and reruns have
zero additional repetition weight.

## Graduation sequence

1. Build and independently verify the all-68 mistake curriculum.
2. Freeze the exact shortlist code, configuration, data contract, costs,
   factor mapping, and thresholds.
3. Evaluate mechanics on a calendar-selected historical expansion, retaining
   every missing context and fill as an explicit failure.
4. Select at most one definition under the predeclared training rule.
5. Open a strictly later untouched prospective cohort with decisions sealed
   before outcomes.
6. Continue `no_trade` unless adjusted discovery and untouched confirmation
   both clear the minimum economic effect and all stress gates.

Nothing in this document can promote, authorize, route, or place a Practice-007
order. Real-money routing remains disabled.
