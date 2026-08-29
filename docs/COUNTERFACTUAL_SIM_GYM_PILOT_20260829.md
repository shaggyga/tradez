# Counterfactual SIM Gym pilot — 29 August 2026

Status: independently verified historical diagnostic; `no_trade`

## Frozen scope

- Cohort: `counterfactual_sim_gym_v1.7482ea2d37e385e90150`
- Window: 21–28 August 2026 UTC
- Instruments: AUD/NZD, EUR/USD, GBP/CHF, USD/JPY
- Decision clocks: 478 hourly pair clocks
- Causal rule signals: 2,735
- Matched virtual-order intents: 49,230
- Eligible filled intents: 44,271
- Unique reusable executable outcome paths: 5,700
- Arms: 108 combinations of six rules, three horizons, two entry delays,
  and original/flipped/deterministic-random directions
- Historical blocks: purged early, middle, and late diagnostics

The standalone verifier independently reconstructed the frozen candles,
clocks, rule fires and non-fires, directions, arms, exact bid/ask paths,
outcomes, costs, MFE/MAE, partitions, re-entry sequence, effective-N cells,
append-only controls, source archives, and snapshot roots with zero failures.

## Result

None of the 108 as-signaled rule × horizon × delay × partition cells had
positive effective after-cost expectancy. The range was -3.071 to -1.482 pips
per virtual order. No fixed rule/horizon/delay definition beat both its exact
flipped and deterministic-random controls in all three partitions.

| Partition | Least-negative as-signaled arm | Raw N | Effective N | Effective average |
|---|---|---:|---:|---:|
| Early | 15m reversion, 1m delay, H30 | 180 | 149 | -1.622 pips |
| Middle | 5m momentum, 1m delay, H15 | 119 | 100 | -2.033 pips |
| Late | 60m momentum, 1m delay, H30 | 111 | 88 | -1.482 pips |

Descriptive raw rule results are overlapping and are not independent proof:

| Rule | Raw intents | Gross average | After-cost average | Cost-clearing rate |
|---|---:|---:|---:|---:|
| 5m momentum | 2,499 | -0.135 | -2.394 | 16.0% |
| 15m momentum | 2,467 | -0.061 | -2.311 | 17.4% |
| 60m momentum | 2,337 | +0.179 | -2.052 | 19.6% |
| 5m reversion | 2,499 | +0.135 | -2.124 | 20.9% |
| 15m reversion | 2,467 | +0.061 | -2.188 | 20.1% |
| 5/15 SMA spread | 2,488 | -0.400 | -2.648 | 16.6% |

The average executable-cost drag relative to midpoint gross movement was about
2.23–2.26 pips. The SMA definition was directionally worse than its flipped
control in this sample, but the flipped version also lost after cost. A
one-minute delay had no stable advantage over the optimistic zero-delay
boundary.

## Opportunity versus selection

This null does not mean price never moved enough. A noncausal hindsight oracle
that chose the better long/short side after seeing the path found at least one
cost-clearing direction on about 19–27% of H5 clocks, 32–41% of H15 clocks,
and 47–53% of H30 clocks. At H30 the hindsight best-side average was roughly
+0.51 to +1.56 pips after cost, depending on partition and delay.

That oracle is not a strategy. It demonstrates that direction/magnitude
selection is the bottleneck in this bounded sample. Verified diagnostic labels
were 22,419 wrong-direction, 13,499 cost-consumed-move, 2,652 latency-decay,
and 8,353 captured-after-cost observations; labels may overlap.

## Incident retained

The preceding cohort `counterfactual_sim_gym_v1.1950cda25085d7219811` failed
independent replay because one floating-point moving-average dust value was
treated as directional. It remains immutable. The corrected producer uses an
explicit 1e-9-pip zero tolerance and opened the new verified cohort; no row was
rewritten.

## Interpretation and continuation

This is a valid four-pair, one-week diagnostic null, not a broad strategy-space
or significance conclusion. It cannot confirm, promote, authorize, or execute.

Next cohorts should remain bounded and immutable:

1. Publish exact matched paired-control deltas and uncertainty diagnostics.
2. Add exact-window source archives before scaling through all 68 pairs in
   deterministic batches.
3. Add causal support/resistance reaction rules and existing strategy-lab
   rules without combining correlated variants as independent evidence.
4. Add official-event/news and source-conditioned clocks as separate adapters.
5. Replay entry, exit, hold, and rotation policies against no-trade and matched
   controls.
6. Freeze only selected hypotheses, then require untouched prospective
   confirmation before any narrow Practice-007 canary.

Practice 007 and all real-money boundaries were unchanged.
