# EUR/USD bid/ask dataset

The recorder writes real OANDA **practice** pricing-stream observations to
`trad/data/eurusd_feed_20260925/quotes-*.csv`. Collection is scheduled to stop on
**Friday, September 25, 2026 at 4:59 p.m. New York time (20:59 UTC)**. It can continue
after the chat handoff while the recorder, computer and internet connection remain
available. Reconnects create connection identities; process restarts create segments.
No historical backfill fills outages.

OANDA's sampled stream supplies **up to four prices per second per instrument**.
It is not a uniform 250 ms series and does not include every market tick. Initial
connection snapshots have `initial_snapshot=True` and can describe an older quote.
Rows retain these snapshots, unchanged prices and all other recorded observations.

Refresh the convenient combined snapshot from the workspace root:

```powershell
python -I -B trad/oanda_eurusd_dataset.py export --input trad/data/eurusd_feed_20260925
```

This produces `eurusd_bid_ask.csv` and `DATASET_SUMMARY.json` in that directory.
Run it again for newer observations. The raw segment files continue growing; the
combined export is a snapshot, not a live-updating file. Each output file is replaced
atomically. The summary contains the combined CSV hash, so a reader racing another
export can detect a mismatched pair and retry.

The export keeps recorder segment order from `events.jsonl`, then original sequence
order. It does not deduplicate, interpolate, resample or synthesize data. It records
the exact captured complete-byte hash, byte length and capture times of every source
segment. Each segment is captured separately, not at a single simultaneous instant.
An unfinished tail is excluded and counted as bytes and fragments; malformed complete
rows refuse export. A stale `recording` status alone does not prove the process or
network is alive. The summary distinguishes an active snapshot from a confirmed
closed recording; passing the cutoff time alone does not prove completion.

`broker_time` is OANDA's timestamp, preserved with its original nanosecond text.
`received_time` is this computer's UTC receipt timestamp and retains its actual
precision. `timestamp_to_receipt_ms` includes source age, network delay and clock
differences; it is not a pure network latency measurement. Prices remain exact decimal
strings in CSV. `mid=(bid+ask)/2`, `spread=ask-bid`, and `spread_pips=spread/0.0001`.
The exporter checks these relationships with decimal arithmetic.

For analysis or a later visualization, optional pandas access works from a directory
(a fresh in-memory snapshot) or the exported CSV:

```python
from trad.oanda_eurusd_dataset import load_dataframe

df = load_dataframe("trad/data/eurusd_feed_20260925")
# Or: df = load_dataframe("trad/data/eurusd_feed_20260925/eurusd_bid_ask.csv")
print(df[["broker_time", "bid", "ask", "spread_pips"]].tail())
```

Pandas is imported only by this convenience function. It parses timestamps to UTC
nanosecond datetime columns, retains their original strings in `broker_time_raw`
and `received_time_raw`, and converts price columns to ordinary floating point by
default for plotting. Use `prices_as_float=False` to keep price strings. The original
CSV always retains the decimal text. Snapshot metadata is in
`df.attrs["dataset_summary"]`.

To stop this recording early, create its `STOP` file:

```powershell
New-Item -ItemType File -Path trad/data/eurusd_feed_20260925/STOP
```

The recorder checks the file as it reads/reconnects, records a user stop, and refuses
later restart of that recording. Exporting the retained observations remains safe.
