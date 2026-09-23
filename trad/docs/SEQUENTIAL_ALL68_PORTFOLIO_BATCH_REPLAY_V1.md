# All-68 Sequential Portfolio Batch Replay V1

Status: independently verified historical engineering/training diagnostic; research-only, nonexecuting, and proof-ineligible

## Purpose

This replay is the first availability-aware sequential portfolio pass across all
68 instruments in the corrected homogeneous exact-window pack
`sequential_replay_source_pack_v1.b9d1526f3dfa057bd06d`. It extends the
four-pair mechanics pilot without modifying either that pilot or the source
pack.

The exact source-pack manifest, its clean verifier receipt, and all 204 source
archive identities are bound by content inside the replay cohort. A later
source-pack supersession cannot silently change this replay.

Every immutable cohort artifact also uses a deterministic, schedule-bound
timestamp: the maximum predeclared feedback epoch, rendered in UTC. Wall-clock
run time is never written into cohort state or its verifier receipt. Cohort
datasets, state, report, bound source files, and verifier receipt are
create-once artifacts: a rerun must reproduce the exact existing bytes or fail.
The mutable current state, report, and receipt are republished only by copying
the corresponding verified cohort bytes.

All three sessions are frozen at 12:00–16:00 UTC on Monday, Wednesday, and
Friday. Selection is calendar/clock based, not result based. This remains an
engineering/discovery diagnostic rather than market proof, and independent
regime count remains unknown.

## Causal and availability contract

- Three fixed sessions and 144 five-minute global clocks are scheduled before
  delayed-entry or feedback coverage is inspected.
- All 68 pair contexts exist at every clock: 9,792 rows total.
- Aliases are stable and deterministic: `P001` through `P068` in lexical
  instrument order.
- Only contexts with a complete causal 60-minute prefix may be ranked.
- Missing context, delayed-entry quote, or feedback quote remains an explicit
  fail-closed observation; no nearest candle is substituted and no clock is
  removed.
- Exact delayed-entry bid/ask prices pay the spread once. Frozen slippage is
  0.125 pip per leg, or 0.25 pip for a normal round trip.
- A rotation is one atomic close plus one open and therefore has two legs.
- The predeclared terminal rule exits on the final scheduled clock. If that
  exact delayed quote is missing, it retries the exact session-end quote. If
  both are missing, the portfolio is censored nonflat and later decisions are
  blocked rather than invented.

## Portfolio and independence contract

There is one state chain across all instruments and all three sessions. Pair
shards never receive independent portfolios. At most one normalized virtual
position is open and exactly one primary global decision exists per clock.

The 144 clocks are descriptive market repetitions. Pair contexts,
counterfactuals, and reruns all have `count_as_rep=0`. The three structural
session blocks are not asserted to be independent regimes.

## Canonical result

Cohort: `sequential_all68_portfolio_batch_replay_v1.efda27295d5107241262`

The predecessor `sequential_all68_portfolio_batch_replay_v1.9d6e7e0f27ad289c4c83`
is preserved. The successor changes artifact identity only to bind the
cross-runtime canonical gzip implementation and its rebuilt source pack.

- 144 global decisions;
- 9,792 retained pair contexts;
- 8,351 causal-ready contexts;
- 8,333 fully ready contexts;
- 1,441 missing causal prefixes;
- 382 missing exact delayed-entry quotes;
- 290 missing exact feedback quotes;
- 261 ranked candidate contexts;
- actions: 50 wait, 24 enter, 15 hold, 24 exit, 31 rotate;
- 110 execution legs;
- 172 depth-one counterfactuals;
- terminal portfolio flat;
- realized result: **−120.25 pips after executable spread and frozen slippage**.

The negative result is retained. It is not tuned away and is not a claim about
edge. It shows that broadening the same frozen mechanics baseline from four
pairs to this all-68 historical pack increased activity but did not create
after-cost profitability.

The earlier cohort `sequential_all68_portfolio_batch_replay_v1.445ddc96477b7ac284ed`
remains preserved as a superseded diagnostic against pack 1bdd. Pack ee6d adds
explicit proof-policy closure. Cohort
`sequential_all68_portfolio_batch_replay_v1.7681e61bd31ac8877bd8` remains
preserved as the immediate pre-hardening baseline; it had the same datasets but
allowed wall-clock timestamps to rewrite cohort state and receipts on rerun.
The current cohort binds the deterministic timestamp contract, immutable writes,
and the strengthened verifier. Cohort `sequential_all68_portfolio_batch_replay_v1.ea3dd3438d15cea1a24b`
remains preserved separately as an engineering diagnostic against pack 52259;
its Friday 10:00–14:00 window is not homogeneous overlap evidence.

## Verification

The independent verifier imports neither producer nor replay core. It rebuilds
the schedule and exact clock identities from the bound source manifest; then it
rebuilds aliases, all 9,792 causal/availability rows, candidate scores, primary
decisions, bid/ask execution, depth-one counterfactuals, feedback, rotation
ordering, state chain, and terminal flattening. It requires the complete six
dataset manifests and exact row identity sets, and independently reconciles
all safety flags, repetition quarantine, action/failure counts, availability
counts, execution-leg totals, terminal state, realized P/L, and content
bindings. It independently reconstructs the schedule-bound artifact timestamp.
The focused adversarial suite runs the producer and verifier twice and requires
byte identity for every immutable cohort file plus exact byte equality between
each current pointer and its cohort artifact. The canonical receipt has zero
failures.

```powershell
python oanda_sequential_all68_portfolio_batch_replay.py
python oanda_sequential_all68_portfolio_batch_replay_verifier.py
python -m pytest -q test_oanda_sequential_all68_portfolio_batch_replay.py
```
