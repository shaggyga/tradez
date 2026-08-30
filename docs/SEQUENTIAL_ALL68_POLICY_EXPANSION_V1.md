# Sequential All-68 Policy Expansion V1

## Purpose

This is a separate, immutable, research-only expansion evaluator. It leaves the original three-session policy challenger and every V1 replay/core/cohort unchanged.

The evaluator is exactly bound to:

- Replay: `seq_a68_wed_exp_v1.e7de4ecdd0255eb13306`
- Source pack: `sequential_replay_source_pack_v1.554c8f74212202aa9b86`
- Seven scheduled Wednesday overlap sessions from 2026-07-15 through 2026-08-26
- 336 global clocks across all 68 instruments
- The source replay's exact delayed bid/ask quotes, spreads, slippage, terminal flattening, candidate contexts, and one-global-state accounting

Every session is historical training. `20260826_wed_overlap_prior_discovery` remains prior discovery. Nothing in this evaluator is proof, confirmation, promotion evidence, authorization, a signal, or an execution instruction.

## Matched arms

All six arms see the exact same scheduled clocks and causal source contexts:

1. `no_trade`
2. `v1_baseline_reference`
3. `cost_hurdle_2x`
4. `explicit_hold_vs_switch_2x`
5. `factor_conflict_suppressed_2x`
6. `oof_remaining_move_calibrated_2x`

One global clock is one market repetition. Arms, candidates, pairs, and reruns contribute zero additional repetitions. Missing source contexts and quotes remain represented by the frozen source replay and are not silently removed.

## Time-ordered calibration contract

The OOF arm uses a generic application-session mapping:

- The first listed Wednesday has no earlier training session and must abstain.
- Each later Wednesday may use all and only earlier completed listed Wednesdays.
- Same-session and future-session observations are forbidden.
- A liquidity cell must have at least 20 prior observations and a positive 95% lower bound for after-cost five-minute candidate return.
- Missing or ineligible calibration means abstain.

All three liquidity buckets had negative historical mean after-cost candidate returns at every application session with prior data. The OOF arm therefore made no entries, which is the intended fail-closed result.

## Final matched result

Final cohort: `sequential_all68_policy_expansion_v1.15a3aa5d039df7df9a7d`

| Arm | Execution legs | Net pips |
|---|---:|---:|
| no_trade | 0 | 0.00 |
| v1_baseline_reference | 268 | -186.20 |
| cost_hurdle_2x | 42 | +1.35 |
| explicit_hold_vs_switch_2x | 40 | +16.10 |
| factor_conflict_suppressed_2x | 40 | +16.10 |
| oof_remaining_move_calibrated_2x | 0 | 0.00 |

The positive pooled training totals are not stable evidence. `explicit_hold_vs_switch_2x` and `factor_conflict_suppressed_2x` earned +52.95 pips on 2026-08-19; excluding that best Wednesday changes each total from +16.10 to -36.85 pips. `cost_hurdle_2x` earned +39.15 pips on the same Wednesday; excluding it changes +1.35 to -37.80 pips. The August 26 prior-discovery session was negative for all three active 2x arms. The factor-suppressed and explicit arms were identical in this sample, so factor suppression demonstrated no incremental effect.

The supported operational decision remains `no_trade`.

## Integrity and isolation

The evaluator freezes exact hashes for the config, producer, hardened input helper, policy core, replay foundation, independent verifier foundation, verifier, replay state/receipt, copied source-pack manifest/receipt, and all source dataset roots.

Input and output paths reject traversal, symlinks, junctions/reparse points, and root escapes. Gzip datasets are checked against frozen compressed-size, expanded-size, and row-count caps and are decompressed under a hard streaming cap. Outputs are content addressed and immutable. The artifact timestamp is derived from the maximum bound source feedback epoch, not wall-clock time.

The standalone verifier imports neither the expansion producer nor a policy core. It independently reconstructs all clocks, prior-session calibration cells, six policy state chains, executable quote accounting, terminal flattening, dataset rows, summaries, and cohort identity. Focused adversarial tests cover source rebinding, re-sealed decision tampering, path traversal, gzip expansion, link/reparse rejection, deterministic reruns, and operational execution isolation.

The earlier expansion cohorts `e098ae1dffe05e0d7569`,
`692e1e8be5c358cff382`, and `bd9d43742d8ee85ba52b` are preserved rather than
overwritten. The current successor binds canonical cross-runtime gzip bytes.
