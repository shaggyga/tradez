# Exact two-hour spike dataset primitives

This offline module turns the permanent significant-move 5-minute caches into
causal decision rows and exact wall-clock 120-minute outcome labels. It makes
no OANDA API calls and does not place orders.

## Data contract

`load_normalized_bars()` reads
`trad/data/significant_moves/cache/bars/*.parquet`. The default cache already
contains a complete UTC five-minute lattice. Market closures and missing input
remain explicit rows with null prices; they are never forward-filled.

Normalized bars expose `timestamp`, `instrument`, mid/bid/ask OHLC,
`spread_pips`, `pip_size`, and source-quality fields. The source contains true
bid/ask closes but only mid OHLC, so bid/ask open/high/low are spread-based
reconstructions and are marked by `bid_ask_open_high_low_is_derived`.

`build_forward_labels()` matches each decision to exactly
`timestamp + 120 minutes`. It does not use a row offset. Its core fields are:

- `decision_id`, `timestamp`, `outcome_timestamp`, and `instrument`
- `forward_signed_pips`, `forward_abs_pips`
- `forward_signed_return`, `forward_abs_return` (decimal returns)
- executable long/short endpoint results using start ask/bid and end bid/ask
- forward observation counts, validity, quality flags, and `event_cluster_id`

For full-universe work, iterate one pair at a time with
`iter_normalized_bars()` to avoid holding roughly 68 complete lattices in RAM.

## Leakage-safe research sequence

1. Load normalized bars and build exact labels.
2. Seed `assign_event_clusters()` with a fixed, predeclared candidate rule or a
   durable event inventory. Cluster IDs use forward outcomes, are hindsight
   metadata, and must never enter a causal feature matrix.
3. Call `make_nested_purged_splits()`. It keeps event clusters indivisible,
   purges training label endpoints, and embargoes training decision times.
4. For each inner or outer fold, pass only `frame.iloc[train_positions]` to
   `fit_train_only_thresholds()`. Freeze that model and call
   `apply_train_only_thresholds()` on validation/test rows without refitting.

Example:

```python
from trad.spike_account_space import (
    build_forward_labels,
    fit_train_only_thresholds,
    iter_normalized_bars,
)

_, bars = next(iter_normalized_bars(instruments=["EUR_USD"]))
labels = build_forward_labels(bars, minimum_observation_fraction=0.5)
# Fit only after selecting a chronological fold's training positions.
model = fit_train_only_thresholds(labels.iloc[train_positions], quantile=0.995)
```

The supplied split arrays are positional and must be consumed with `.iloc`.
Event IDs, thresholds, and all `forward_*` fields are label-side artifacts—not
features available at decision time.
