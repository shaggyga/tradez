# Sequential Deliberate-Practice Pipeline

Status: research-only historical training; no execution authority

## Why this line exists

The project previously generated very large numbers of overlapping forecasts
and virtual orders. That is useful for mechanics, but it is not the same as a
trader making one portfolio choice, carrying the consequence forward, and
learning from the next decision. This pipeline separates practice volume from
market evidence so thousands of attempts can be generated without claiming
thousands of independent regimes.

## Current layers

| Layer | What it measures | Honest repetition unit |
|---|---|---|
| Counterfactual SIM gym | Matched rule, side, delay, horizon and control outcomes | Physical market clock/path, not each virtual intent |
| Deliberate replay case bank | Blind situations and legal actions | One portfolio-choice clock |
| Sequential portfolio replay | Persistent wait/enter/hold/exit/rotate state with executable costs | One global decision clock |
| Learner curriculum | Precommitted attempts plus spaced review | First attempt on a case; reviews have zero evidence weight |
| Mistake curriculum | Direction, entry, management, exit, rotation, cost, calibration and selection errors | One structural error cluster |
| Exact-window source pack | Frozen all-68 causal, fill and feedback inputs | One global scheduled clock; pair contexts are dependent |
| Availability-aware all-68 replay | One allocator over all ready pair contexts | One global action, never one action per storage shard |

## Completed checkpoints

1. The counterfactual SIM gym generated 49,230 intents but correctly reduced
   them to 478 decision clocks and 5,700 executable physical paths.
2. The first persistent four-pair portfolio session recorded 48 decisions, 18
   execution legs and 37 depth-one alternatives. The frozen baseline lost
   13.25 pips after costs; that loss was retained.
3. The first learner curriculum precommitted 48 attempts over four sessions.
   Thirty-six were distinct cases and 12 were zero-weight spaced reviews.
4. The mistake curriculum reduced 34 nonexclusive labels to 21 structural
   clusters. Cost awareness, entry quality, direction and calibration are the
   leading practice needs.
5. The corrected all-68 exact-window pack scheduled 144 global clocks and
   9,792 pair contexts across three homogeneous 12:00–16:00 UTC
   calendar-selected sessions. It retained 1,441 context failures, 382 missing
   delayed-entry quotes and 290 missing feedback quotes rather than deleting
   those observations. The prior Friday 10:00–14:00 pack remains an immutable
   coverage-driven engineering diagnostic, not overlap proof.
6. The first corrected availability-aware all-68 portfolio replay retained all
   144 decisions and 9,792 pair contexts, ranked 261 candidates, executed 110
   bid/ask legs, and recorded 172 depth-one alternatives. It ended flat at
   **−120.25 pips**. Broader activity therefore did not rescue the unchanged
   momentum-ranking policy; the negative result remains part of the training
   record.

None of these counts is a promotion sample. Every source week was already
inspected, independent regime count remains unknown, and every output supports
`no_trade` only.

## Non-negotiable deduplication rules

1. A scheduled global clock exists before quote availability is inspected.
2. A storage batch or pair context never creates another portfolio action.
3. Two expressions sharing a currency, thesis, physical path, or overlapping
   decision-to-feedback interval remain dependent.
4. Counterfactual branches, parameter variants and repeat attempts always have
   market-repetition weight zero.
5. Missing exact quotes are explicit failures; the nearest candle is never a
   substitute.
6. A material source, feature, cost, model, policy, schedule or deduplication
   change opens a new cohort ID.
7. Selection and confirmation cannot use the same market period.
8. Historical practice can improve a frozen policy definition, but it cannot
   confirm the resulting definition.

## Next expansion ladder

1. Generate a new learner/mistake curriculum from the corrected all-68
   decision cases,
   targeting clustered cost, entry, direction and calibration mistakes.
2. Freeze a small policy shortlist from that training evidence: cost-aware
   abstention, remaining-move-versus-cost calibration, factor-conflict
   suppression, and hold-versus-rotate comparison. Do not call same-window
   improvements confirmation.
3. If mechanics remain stable, open a calendar-only seven-Wednesday expansion:
   28 fixed UTC blocks, 1,344 global clocks and 91,392 pair contexts. Continue
   to count one action per global clock.
4. Add levels, official events, news, rates and positioning only as separate
   source-conditioned policies with exact snapshot IDs. They cannot be
   retrofitted into price-only cases.
5. Select and freeze a policy using historical training, then open a strictly
   later untouched prospective cohort with decisions sealed before feedback.
6. Only a genuinely confirmed candidate may receive a narrow Practice-007
   canary authorization. Real-money routing remains disabled.
