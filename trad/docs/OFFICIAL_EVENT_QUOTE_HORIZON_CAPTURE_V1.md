# Official event executable-quote horizons V1

## Purpose

This research-only sidecar completes the market-clock half of the official
event experiment. The existing fast lane freezes all 68 executable bid/ask
quotes when a genuinely new official release is first observed. V1 then makes
one terminal all-68 quote attempt at 1, 5, 15, 30 and 60 minutes.

It does not predict direction, place an order, authorize a candidate or alter
Practice 007. The supported execution decision remains `no_trade`.

## Frozen lineage

- Contract: `official_event_quote_horizon_capture_v1_all68_append_only_20260830`
- Cohort: `official_event_quote_horizon_capture_v1_20260830a`
- Activation: `2026-08-30T12:00:00Z`
- Required entry contract:
  `official_release_raw_quote_capture_v1_append_boundary_all68_20260829`
- Required entry cohort: `official_release_raw_quote_capture_v1_20260829a`
- Frozen universe: 68 OANDA instruments, SHA-256
  `b6abe559dcaa9faa0a1e48217f97a12dc9043eb9595d2b946bf80f93035be142`

No preactivation raw observation is imported. An input must already contain a
valid exact 68/68 raw sidecar. The entry quote is never reacquired.

## Capture semantics

For every eligible event and declared horizon:

1. The target clock is `raw_first_seen + horizon`.
2. A target not yet due produces no row.
3. The first due attempt is terminal. It records
   `capture_read_started_utc` immediately before reading the quote snapshot
   and `attempted_utc` when that read completes; the latter is the capture
   completion clock used for age and target-offset validation.
4. An exact attempt requires current-generation 68/68 bid, ask and pip values,
   snapshot schema 2 from `practice_007_fast_executor_price_stream`, a
   nonempty source on every component, no retained-last-known quotes, bounded
   snapshot/venue age and bounded target offset.
5. A partial, stale, late, future-skewed or wrong-generation attempt persists
   an immutable diagnostic header with zero proof quote rows.
6. A later valid snapshot cannot repair or replace a failed attempt.

Headers and quote components reject update and delete. Each header retains the
canonical raw quote-snapshot material and binds it by SHA-256, along with the
input sidecar bytes, normalized payload and component root. This lets the
independent verifier recompute the snapshot hash after the rolling transport
has discarded its copy. Each quote component binds its own normalized payload
hash and must carry the same connection generation as its header.

## Economics contract

Both directions can be evaluated without choosing the hindsight winner:

- Long: entry ask to horizon bid.
- Short: entry bid to horizon ask.
- Midpoint movement is reported separately.
- Entry and exit spreads are reported separately but are already embedded in
  the executable prices and are not subtracted twice.
- Modeled round-trip slippage is a separate stress field. V1 does not claim it
  was observed.

The paired modeling layer may later compare `news_only`, `price_only`,
`news_plus_technical_timing` and `no_trade` on these identical clocks. That is
a separate frozen cohort; this collector does not select an arm.

## Acceptance and evidence boundary

Producer tests require exact coverage, terminal invalid behavior (including
malformed integer metadata), no retry, append-only storage, pre-activation-only
empty-schema migration, correct long/short endpoint math, source-contract
isolation and inert safety flags. A standalone independent-verifier
implementation separately checks source linkage, complete due horizons,
payload hashes, quote arithmetic, clocks, universe, safety and trigger presence
without importing the producer. The finalized boundary passed 34 producer and
verifier tests under both Python 3.12 and 3.13; the broader source/governance
boundary passed 548 tests, and the live empty ledger verifies with zero
failures. These checks validate the implementation, not predictive edge.

The hidden supervisor runs the independent verifier every 30 seconds and
requires a fresh `verified` heartbeat. A due event/horizon omitted after its
terminal window therefore makes the verifier fail even if the collector's own
heartbeat is still fresh.

The cohort activates at `2026-08-30T12:00:00Z`. Zero eligible events and zero
captures before that clock while the market is closed are expected. V1 does
not backfill historical events or synthesize missing horizon attempts.

The Friday Fed and earlier BOJ cases remain regression fixtures only. They are
not copied into this prospective cohort and cannot count as proof.
