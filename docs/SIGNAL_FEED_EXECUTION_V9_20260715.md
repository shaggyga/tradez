# Account -007 Signal Feed Execution V9

## Purpose

Account `101-001-37981792-007` is a practice execution sink for the best current
signals across the research system. A model or lane is not promoted into the
account. Strategy, equation, ridge, pattern, and later producers remain
independent signal generators with separately matured outcomes.

## Signal Selection

All executable producers publish short-lived opportunities to:

`D:\forex\trad\data\oanda_training_manager\state\practice_007_signal_feed_v1.sqlite`

Each candidate is rescored at its available outcome horizons. The score uses:

- current projected net pips after spread;
- current directional probability or signal-to-spread strength;
- agreement and opposition counts from distinct model families;
- historical win probability and net pips, shrunk by sample and independent
  block reliability;
- a reliability penalty for train/holdout sign disagreement;
- a veto for persistently negative history when the evidence is sufficiently
  broad.

The active practice thresholds are confidence `>= 0.535` and projected net pips
`>= 0.05`. These are intentionally aggressive. Strict lane/horizon promotion
continues to report chronological holdout quality but does not block an otherwise
eligible individual signal.

## Account Allocation

- Maximum open positions: 4.
- Duplicate instrument positions: blocked.
- Same currency-direction concentration: maximum 3 positions.
- Shared account cooldown: 30 seconds, rechecked while holding the cross-process
  order lock.
- Margin targets: 55% normal, up to 68% for high confidence, 75% hard ceiling.
- Per-trade margin budget: 12% to 24% of NAV.
- Per-trade stop-risk budget: 0.8% to 2.5% of NAV.
- Absolute unit cap: 5,000.

Sizing uses current NAV, margin used, margin available, the instrument's OANDA
margin rate, stop distance, conversion rate, and confidence. It does not assume
all pairs have the same pip value.

## Exit Policy

Every entry places a broker-side hard stop. There is no fixed take-profit for the
open-ended mode. A trailing stop is attached only after the position has moved
far enough in the forecast direction. Activation and trail distance are based on
spread, stop distance, and confidence. This avoids placing a very tight trail
inside ordinary spread/noise while leaving profitable upside uncapped.

The account status helper labels the period before activation as
`open_ended_staged_trailing / pending_activation`; the hard stop means these
trades are protected even though there is deliberately no take-profit order.

## Metrics

- Signal rate: accepted signals divided by raw setups. This measures selectivity,
  not correctness.
- Win rate: positive matured executable-net outcomes divided by matured accepted
  signals at the selected horizon.
- Average net pips: mean matured bid/ask result after the modeled entry/exit
  spread path.
- Pips/hour: `average net pips * 3600 / outcome horizon seconds`. This is a
  per-signal time normalization and is not a realizable portfolio return when
  signals overlap.
- Near miss: a setup that narrowly failed an economic threshold and is retained
  for counterfactual outcome scoring.
- Hard reject: a setup that materially failed cost, spread, or strength gates.
- Strict promotion: chronological, independent-block lane validation used as
  evidence only.

## Deployment Validation

On 2026-07-15 the focused source and D-runtime suites passed `55/55`. The first
shared-feed session produced three practice fills from three families with zero
order errors. USD/JPY hit its hard stop for `-0.0441`; AUD/USD and GBP/USD
remained open at the validation snapshot. Runtime workers and the dashboard were
restarted from `D:\forex`, and the shared ledger retained the full fill count.
