# Counterfactual SIM Gym V1

Status: bounded historical research pilot; execution-ineligible

## Purpose

The SIM gym turns repeated market practice into immutable, comparable evidence.
At every frozen decision clock it evaluates the same observed setup through
matched virtual actions:

- the rule's original direction;
- the flipped direction;
- a deterministic random direction;
- no trade.

No-trade is the zero-value analytical baseline attached to every result cell;
it does not fabricate an order or quote-dependent outcome row.

Every action uses the same executable OANDA bid/ask path, cost assumptions,
entry clock, and declared horizon. The gym cannot access an account, publish a
signal, promote a hypothesis, authorize a canary, place an order, or close a
position.

## Frozen V1 pilot

The first pilot intentionally limits breadth until the independent verifier is
proven:

- instruments: `EUR_USD`, `USD_JPY`, `AUD_NZD`, and `GBP_CHF`;
- source: completed M1 OANDA bid/ask candles;
- history: trailing seven-day common data window;
- decision cadence: hourly;
- rules: 5/15/60-minute momentum, 5/15-minute reversion, and 5/15 SMA spread;
- horizons: 5, 15, and 30 minutes;
- entry delay: zero-minute optimistic boundary and matched one-minute delayed baseline;
- exit: exact declared-horizon endpoint;
- maximum virtual intents: 250,000.

This scope is a contract-validating warm-up, not a claim that the selected
rules or pairs are optimal.

## Causal and execution contract

A candle beginning at time `t` becomes usable only at `t + 60 seconds`. A
decision uses completed candles only. Its simulated order enters at the exact
next permissible M1 executable open; a missing timestamp is not replaced by a
nearby row. Long entries pay ask and exit at bid. Short entries sell bid and
cover at ask. MFE, MAE, endpoint value, spread, slippage stress, missed-entry
decay, and mistake labels all use executable sides.

The exact source-file byte prefixes, runner, core implementation, and config
are content-addressed and archived under the dedicated SIM artifact root. A
material change creates a new cohort rather than rewriting an old run.

## Evidence interpretation

Raw virtual-order count measures practice volume, not independent evidence.
Effective N collapses overlapping holding intervals and same-clock expressions
connected by a signed currency factor or the same pair. Historical replay is
split into purged early, middle, and late diagnostic blocks. It cannot provide
prospective confirmation.

The ordinary lower bound in the report is explicitly unadjusted. V1 does not
make significance, promotion, or tradability claims. Any promising diagnostic
must later receive multiplicity control, a frozen candidate definition, and an
untouched prospective cohort.

## Artifacts

The runner writes only beneath:

`data/oanda_training_manager/research_ledgers/counterfactual_sim_gym_v1/`

The append-only SQLite ledger normalizes clocks, signal observations, reusable
outcome paths, arm-to-outcome intents, cell aggregates, mistake samples, and run
snapshots. The standalone verifier does not import the producer. From the
frozen source archives it independently reconstructs every expected decision
clock, rule fire and non-fire, side, matched arm, exact executable outcome,
partition, re-entry decision, intent, effective-N cell, row identity, lineage,
and snapshot root. Missing losing rows and internally rehashed outcome
tampering are therefore detectable rather than accepted as self-consistent
data.

## First real verifier incident

The first real pilot cohort,
`counterfactual_sim_gym_v1.1950cda25085d7219811`, failed independent replay.
NumPy produced a `2.22e-12`-pip moving-average difference on one GBP/CHF clock,
while the independent implementation correctly treated it as flat. The
producer now has an explicit `1e-9`-pip signal-zero tolerance. The failed
cohort remains immutable engineering evidence and was not relabeled or merged.
The corrected material contract created a new cohort.

The verified pilot result and its limitations are recorded in
`docs/COUNTERFACTUAL_SIM_GYM_PILOT_20260829.md`.

## Commands

Run the frozen pilot:

```powershell
python oanda_counterfactual_sim_gym.py
```

Verify the resulting cohort independently:

```powershell
python oanda_counterfactual_sim_gym_verifier.py
```

Run focused regression tests:

```powershell
python -m pytest -q test_oanda_counterfactual_sim_gym.py
```

## Next adapters

After the pilot is independently reproducible, the same compact contract can
be extended in bounded cohorts to existing strategy-lab rules, causal
support/resistance reactions, official-event/news clocks, entry delays,
barrier exits, rotation policies, and more instruments. Scaling is staged so a
larger search surface cannot hide a broken clock, cost model, comparator, or
evidence calculation.

Continuation is staged, not abandoned after this pilot: add paired comparator
diagnostics, exact-window source archives for safe multi-batch scaling, then
bring in level reactions, existing strategy-lab rules, source/event clocks,
and allocator/rotation decisions as new immutable cohorts. Historical volume
continues to be training and discovery evidence only; any selected hypothesis
still requires a separately frozen untouched prospective cohort.
