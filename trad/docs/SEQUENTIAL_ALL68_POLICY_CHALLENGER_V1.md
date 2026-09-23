# Sequential All-68 Policy Challenger V1

Status: frozen historical training comparison; research-only, nonexecuting,
proof-ineligible, and unable to promote or authorize anything

## Exact immutable inputs

- Replay cohort:
  `sequential_all68_portfolio_batch_replay_v1.efda27295d5107241262`
- Source pack:
  `sequential_replay_source_pack_v1.b9d1526f3dfa057bd06d`
- Mistake curriculum:
  `sequential_all68_mistake_curriculum_v1.26b13486af3803244125`
- Challenger cohort:
  `sequential_all68_policy_challenger_v1.0b5267c7a5c730a150cf`

The `a02365972fb81cba1f5f` challenger remains preserved byte-for-byte. The
successor binds the canonical cross-runtime gzip writer and rebuilt immutable
inputs; matched policy results are unchanged.

The earlier functional cohort
`sequential_all68_policy_challenger_v1.efafd05f9f14a83ac946` is preserved as
an immutable predecessor.  The intermediate bounded-I/O cohort
`sequential_all68_policy_challenger_v1.ebb8b63d36ada93dca64` is also preserved.
The current cohort supersedes them by freezing 4 MiB
compressed, 64 MiB decompressed, and 20,000-row per-dataset limits; using
bounded streaming decompression; and rejecting symlinks, junctions/reparse
points, and path escapes on bound source and output dataset paths.

Every source state, verifier receipt, dataset-root map, source-pack manifest,
and mistake-curriculum artifact is bound by an exact SHA-256 contract.  The
challenger cannot access a broker, account, lifecycle, authorization file,
signal feed, or runtime supervisor.

## Matched design

All arms use the same 144 predeclared global clocks, source candidate sets,
one-minute delayed executable bid/ask opens, recorded spread, 0.125-pip
slippage per execution leg, five-minute feedback clocks, and exact terminal
flattening.  Each arm has one portfolio state chain across all 68 pairs and all
three sessions.  One global clock is one repetition; arms, pairs, candidates,
training observations, and reruns add zero repetitions.

The frozen arms are:

1. `no_trade`.
2. `v1_baseline_reference`, byte-bound to the immutable V1 decisions.
3. `cost_hurdle_2x`, which applies the original rotation mechanics only to
   candidates whose frozen score clears 2.0 times its cost denominator.
4. `explicit_hold_vs_switch_2x`, which estimates incumbent continuation even
   when the incumbent does not meet the new-entry hurdle and explicitly
   compares hold, exit, and switch economics.
5. `factor_conflict_suppressed_2x`, which also rejects candidates whose signed
   currency legs contradict same-clock aggregate currency-factor votes.
6. `oof_remaining_move_calibrated_2x`, which trains only on completed earlier
   sessions.  Monday abstains; Wednesday may use Monday; Friday may use Monday
   and Wednesday.  A nonpositive prior-session after-cost lower bound forces
   abstention.

## Historical training result

| Arm | Execution legs | Result |
|---|---:|---:|
| no_trade | 0 | +0.00 pips |
| V1 baseline reference | 110 | -120.25 pips |
| cost hurdle 2x | 8 | -7.50 pips |
| explicit hold-versus-switch 2x | 8 | -7.60 pips |
| factor-conflict-suppressed 2x | 8 | -7.60 pips |
| OOF remaining-move calibrated 2x | 0 | +0.00 pips |

The result is negative, which is valid.  The 2x hurdle sharply reduces
turnover but does not identify positive after-cost expectancy.  Explicit
hold-versus-switch logic fixes a decision-contract defect but does not create
edge in this window.  Factor suppression makes no difference in this small
sample.  The causal OOF arm correctly abstains because prior-session evidence
does not have a positive after-cost lower bound.

This agrees with the candidate-level diagnostic: on the inspected source
window, higher frozen score and expected movement do not rank net executable
five-minute outcomes reliably.  The 2x arm is therefore an abstention control,
not a promotion candidate.

## Verification

The standalone verifier imports neither the producer nor challenger core.  It
independently reconstructs source bindings, archived executable quotes, all
six state chains, signed-currency factor votes, prior-session calibrations,
execution legs, feedback equity, terminal flattening, and summaries.  Tests
cover exact source binding, safety, repetition weights, time-ordered OOF
training, forged safety, self-consistent row tampering, source rebinding, path
traversal, compressed-size and decompression-expansion attacks, linked/reparse
dataset paths, execution isolation, and byte-identical reruns.

Nothing here is confirmation.  Any future policy selected from this historical
window must receive a new cohort and a strictly later untouched prospective
test.  Until then, `no_trade` remains the only supported decision.
