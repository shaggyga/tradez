# Sequential Portfolio Replay V1

Status: independently verified historical training/discovery; research-only

## Purpose

The counterfactual SIM gym generated many matched virtual orders, and the
deliberate-replay sidecar corrected their repetition counts. Neither simulated
a persistent portfolio one completed bar at a time. This cohort adds that
missing mechanics layer without touching Practice 007 or any governed proof,
lifecycle, authorization, or execution path.

The frozen pilot uses:

- four archived OANDA bid/ask M1 streams: AUD/NZD, EUR/USD, GBP/CHF, USD/JPY;
- the metadata-selected Friday London/New York overlap block from 12:00 through
  15:55 UTC on 28 August 2026;
- 48 global five-minute decisions and 192 pair contexts;
- a one-minute exact-open execution delay;
- maximum one normalized virtual position;
- one frozen multiscale momentum-ranking policy;
- exact bid/ask spread plus 0.125-pip adverse slippage per execution leg;
- depth-one alternatives cloned from the exact predecision portfolio.

The interval was chosen by its clock metadata, not its outcome, but it belongs
to an already-inspected historical week. It is therefore permanently
`historical_training_discovery` and cannot confirm or promote anything.

## Sequential contract

Every scheduled global clock receives exactly one primary action:

- `wait` while flat;
- `enter` while flat;
- `hold` while positioned;
- `exit` while positioned;
- `rotate`, atomically closing the old position and opening the new one.

The action is stored before its one-minute-delayed fill and five-minute
feedback. A rotation pays two execution legs at the rotation clock. The new
position later pays its own closing leg. Bid/ask quotes already contain the
spread, so spread is never subtracted a second time.

An entry or rotation whose delayed spread exceeds five pips rejects without
mutating the portfolio. Exits are never blocked for being expensive. Any
residual position at 16:00 UTC receives a predeclared administrative close that
is not scored as learner exit skill.

## Causal inputs and identities

Each clock binds:

- the exact 60-minute completed-input slice for all four pairs;
- an outcome-independent global decision-clock ID;
- stable pair aliases A–D for the whole session;
- exact and coarse future-free situation IDs;
- explicit price, news, rates, levels, and positioning availability;
- the current portfolio, thesis, entry time, and realized state;
- a structural UTC-hour episode ID;
- side-independent physical-path and unsigned-currency resources.

News, rates, levels, and positioning are recorded as unavailable rather than
being silently zero-filled. This makes later source adapters possible without
changing the price-only cohort.

Primary clocks, execution legs, position theses, physical paths,
counterfactuals, and effective structural components are counted separately.
Counterfactual branches always have `counts_as_rep=0`. Four structural episode
components in one four-hour session are not four independent regimes; the
independent-regime count remains unknown.

## Pilot result

The verified frozen run produced:

- 25 waits;
- six entries;
- eight holds;
- six exits;
- three rotations;
- 18 execution legs, comprising nine opens and nine closes;
- 37 depth-one counterfactual branches;
- a flat terminal portfolio;
- **−13.25 realized pips**.

The loss is useful. It confirms that the sequential accounting does not turn a
known-negative price-rule family into synthetic edge. The largest local gains
came from two EUR/USD shorts, while repeated USD/JPY longs were the main loss.
Average primary local value trailed the best available depth-one alternative by
about 0.55 pip per decision. Those are curriculum diagnostics, not an
out-of-sample performance claim.

## Independent verification

The verifier imports neither producer nor replay core. It independently:

- parses and hashes the frozen gzip sources;
- rebuilds the 48 causal clocks and 192 pair slices;
- rebuilds candidates and all primary actions;
- replays bid/ask fills, per-leg slippage, rotation ordering, and P/L;
- reconstructs counterfactual branches and feedback;
- validates append-only triggers, row roots, foreign keys, SQLite integrity,
  source/code/config bindings, terminal flatness, and the immutable session
  seal;
- reads through a SQLite backup snapshot so committed WAL rows are visible but
  uncommitted rows are not.

The canonical verifier receipt has zero failures.

## Commands

```powershell
python oanda_sequential_portfolio_replay.py
python oanda_sequential_portfolio_replay_verifier.py
python -m pytest -q test_oanda_sequential_portfolio_replay.py
```

## Next continuation

1. Add pre-outcome learner-entered sessions and spaced repetition over this
   immutable case format; replay attempts must not increase market-repetition
   counts.
2. Add compact mistake clustering across direction, entry, management, exit,
   rotation, costs, and opportunity selection.
3. Add source-conditioned policies only as new immutable cohorts: official
   events, rates, levels, and news cannot be retrofitted into this price-only
   result.
4. Expand deterministically through additional sessions and eventually all 68
   pairs in bounded batches.
5. Any policy selected from historical training requires a strictly later,
   untouched prospective cohort with decisions sealed before feedback.
