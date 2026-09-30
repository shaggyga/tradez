# EUR/USD dataset capture — active

User replaced the visualization request with an accessible bid/ask dataset. Recording began 2026-09-25T01:52:25.338085+00:00 and is scheduled to stop at 2026-09-25T20:59:00Z (Friday 16:59 New York). Capture remains in progress; this is a verified setup checkpoint, not a completed dataset.

Source commit: `b0dd42c9f6b0dc67fd6b9c5d141e25ed99b0320f`. Recording: `eurusd-a0972120db87`. Step: `eurusd-recording-20260925`; prior scope/claim `eurusd-live-visualization-20260925`.

Local data: `C:\Users\zmoor\Documents\forex\trad\data\eurusd_feed_20260925`. Local evidence: `C:\Users\zmoor\Documents\forex\evidence\eurusd-live-visualization-20260925`. The same source bytes are pinned in REVIEW.json and the running recorder's status. Raw data stays local; SETUP_DATASET_SUMMARY.json authenticates the bounded setup export.

The recorder uses a persistent read-only OANDA practice EUR_USD stream, exact decimal CSV values and original broker timestamps. It keeps repeated prices and reconnect snapshots, records connection failures, uses an exclusive writer lock, and rejects data received at/after the fixed cutoff. Existing source/models, canonical caches and scientific queue/pointers were not changed. General scientific preflight remains blocked and was not labelled passed.

Validation: 10 offline recorder checks; independent recorder review with R1-R4 limitations; exporter synthetic cases; real data export integrity; pandas dataframe/nanosecond check. Passing these does not establish continuous future capture.

Continuation: managed command session 38436, observed PID 12772; active same-thread heartbeat `eurusd-dataset-through-friday-close` checks the actual writer and finalizes after cutoff. The temporary system-awake request succeeded; the PC and network still need to remain available.

Next: leave healthy recording alone, export at cutoff, report actual rows/times/gaps, pause heartbeat, finish guard and publish a successor final dataset receipt. User stop is terminal. No hidden-background launch was used: its rejected attempt was replaced by the managed command session.
