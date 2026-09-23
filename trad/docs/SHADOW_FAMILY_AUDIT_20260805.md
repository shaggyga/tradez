# Continuous H1 shadow-family audit — 2026-08-05

Scope: first matured observation per family/pair/H1 wall-clock bucket during the
latest seven days, restricted to causal entry spreads at or below 3 pips.
Execution eligibility was not changed.

| Family | N | Direction | After-cost wins | Avg net pips | PF |
|---|---:|---:|---:|---:|---:|
| breakout_change_point | 2,099 | 45.1% | 26.8% | -2.43 | 0.457 |
| cross_currency_impulse | 2,099 | 45.3% | 26.7% | -2.80 | 0.397 |
| differenced_path_analog | 2,099 | 47.0% | 27.6% | -2.73 | 0.405 |
| multi_timeframe_trend | 2,099 | 47.4% | 28.8% | -2.11 | 0.504 |

The realized median absolute H1 move was 3.1 pips. Family median predicted
magnitude ranged from 4.8 to 6.0 pips, so magnitude is materially overstated.
Roughly 50.3% of observations moved 1.5 times the entry spread and 40.3% moved
twice the spread in either direction; opportunity existed, but direction and
capture did not.

The three-versus-one minority view also remains non-promotable after collapsing
overlapping signed-currency factors within 15 minutes. Multi-timeframe trend
was directionally correct 60.2% of 123 episodes but averaged -0.68 pips after
cost. Differenced-path analog was directionally correct 58.0% of 300 episodes
but averaged -2.42 pips. Direction accuracy is therefore not a substitute for
cost-clearing expectancy.

Decision: retain every family in shadow only. The live diagnostic may nominate
a cell for a future frozen holdout only after at least 30 independent episodes,
positive after-cost expectancy and tail-removed expectancy, profit factor at
least 1.10, and at least ten pairs. Nomination still cannot enable execution.
