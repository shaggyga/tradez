# Sequential Deliberate Replay V1

Status: independently verified historical practice curriculum; research-only

## What changed

The original counterfactual SIM gym answered: “What would many matched virtual
orders have done?” It did not provide tens of thousands of independent trading
repetitions. This layer adds the honest practice hierarchy:

1. Many parameter, direction, delay, and horizon variants are nested analyses.
2. A pair/chart clock is one market observation on one chart.
3. A portfolio clock is one opportunity set at which exactly one primary
   `wait`, `enter`, `hold`, `exit`, or `rotate` decision may be made.
4. Effective independent evidence is lower still after physical-path,
   overlapping-window, market-episode, and unsigned-currency-factor collapse.

The verified pilot contains:

- 49,230 virtual variants;
- 44,271 eligible variants;
- 5,700 two-sided executable outcome parts;
- 2,850 side-independent physical-path projections;
- 478 pair/chart clocks;
- 120 portfolio-choice clocks;
- 360 within-clock unsigned-currency components;
- unknown independent regime evidence from one already-inspected week.

The last line is deliberate: structural components and hourly timestamps are
not automatically independent market regimes.

## Deliberate-practice contract

One practice attempt is one precommitted primary decision at one portfolio
clock. Replaying a case increases the attempt count but not the distinct-market
repetition count. Alternative actions remain depth-one counterfactuals and
never increase either count.

The append-only journal supports:

- `wait` while flat;
- `enter` while flat;
- `hold` while positioned;
- `exit` while positioned;
- `rotate`, represented as one closing execution leg plus one opening leg.

An entry or rotation must declare its pair, side, size, confidence, expected
move, horizon, entry condition, invalidation, and rationale before feedback is
exposed. No outcome field participates in the causal situation fingerprint.

## Deduplication identities

The contract explicitly separates:

- global decision clock;
- market episode;
- unsigned currency resources and signed exposures;
- side-independent physical price path;
- story cluster, when a causal source exists;
- versioned strategy archetype;
- position thesis, assigned only after a precommitted entry/rotation;
- broad experiment lineage;
- exact causal situation snapshot and coarse situation fingerprint.

Shared currency resources are treated as dependent even when their trade signs
conflict. This prevents several JPY or USD pair expressions from manufacturing
independent confirmation. IDs are projected into a new sidecar; the verified
SIM V1 database and both immutable source cohorts remain untouched.

## Blind presentation and proof firewall

Each portfolio case has a stable blind alias. Its presentation contains causal
buckets and anonymized market ordinals but no pair, date, future candle,
realized return, MFE, MAE, or outcome field.

All imported cases are permanently labeled `historical_training_discovery`.
The original early, middle, and late blocks were already inspected, so none can
become an untouched holdout. Any future proof session must use a later frozen
case cohort, seal every decision before feedback, and quarantine reviewed,
revealed, similar, overlapping, and factor-linked cases.

## Mistake-curriculum correction

The source retained 60 severity-ranked diagnostic examples. They collapse to
only four pair clocks, three portfolio clocks, and six two-sided paths. Those
rows remain immutable but can no longer be presented as 60 separate lessons.
Until a learner makes a precommitted action, they are outcome diagnostics—not
learner mistakes.

## Isolation

This component cannot access an account, place or close an order, publish a
signal, promote a hypothesis, authorize a canary, or enable real money. Its
supported operational decision is `no_trade`.

## Commands

Build or idempotently reproduce the curriculum:

```powershell
python oanda_sequential_deliberate_replay.py
```

Independently verify the case hierarchy and journal:

```powershell
python oanda_sequential_deliberate_replay_verifier.py
```

Run the focused pure-contract tests:

```powershell
python -m pytest -q test_oanda_sequential_deliberate_replay.py
```

## Next continuation

The next cohort should add a real sequential session driver over completed M1
bars: one frozen policy, maximum one open position, explicit position state,
executable entry/exit/rotation economics, and nested alternatives cloned from
the same predecision portfolio. It should then add novelty-weighted case
selection and spaced mistake replay. Historical practice remains separate from
untouched prospective proof.
