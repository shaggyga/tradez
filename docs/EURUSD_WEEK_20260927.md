# EUR/USD week capture, September 27–October 2, 2026

User requested removal of the chat automation and manual checking. The automation was deleted; no replacement was created. The collector runs in managed command session 44406 (initial PID 772), with internal reconnects and fixed cutoff 2026-10-02T20:59:00Z. It needs this computer and internet to remain available; a process/host exit requires manual restart.

Data: trad/data/eurusd_feed_20260927_week
- quotes-*.csv: unchanged bid/ask quote format.
- liquidity-*.csv: best bid/ask liquidity, all buckets, closeout prices and tradeability.
- messages-*.jsonl: full pricing messages and heartbeats with receipt and connection identity.
- volume-s5-*.jsonl: completed OANDA S5 candles, including bid/ask/mid OHLC and volume. Volume means number of prices, NOT traded units. Polled every 60 seconds; the most recent minute can lag. Each process starts candle coverage at its own launch; no restart-gap backfill is claimed.
- status.json and volume_status.json: stream and candle polling status. Verify the PID as well as fresh timestamps.

The first stream message was an old Friday snapshot while the Sunday market was closed; it is flagged initial_snapshot. Fresh heartbeats confirm connection, not new market prices.

Manual export:
python -I -B trad/oanda_eurusd_dataset.py export --input trad/data/eurusd_feed_20260927_week

Manual restart only if no collector is alive:
python -I -B trad/oanda_eurusd_record.py --output trad/data/eurusd_feed_20260927_week --end-utc 2026-10-02T20:59:00+00:00 --capture-extras

Stop: create trad/data/eurusd_feed_20260927_week/STOP. Stop is terminal for this dataset.

Validation: existing 10 offline recorder tests passed. Live process identity, stream connection, fresh heartbeat, raw/liquidity snapshot and successful candle endpoint polling verified. Candle rows await market reopening. Independent review not performed. General scientific preflight remains blocked by existing pointer/environment mismatches; this explicitly authorized standard-library quote collection does not run research.

The timed chat heartbeat requirement is superseded by the user's explicit instruction to remove automation and check manually. No claim of future completed collection is made.
