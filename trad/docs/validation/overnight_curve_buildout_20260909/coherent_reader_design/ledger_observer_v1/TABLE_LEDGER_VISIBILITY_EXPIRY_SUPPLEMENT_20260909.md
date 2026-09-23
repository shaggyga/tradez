# Table expiry review supplement

Root review identified that a hung `/api/main` request could leave an already displayed forecast labelled in progress beyond its original target or evidence deadline. The earlier 122 passing cases did not exercise that DOM liveness condition. Their XML and exact before-expiry source copies remain retained.

The final candidate bounds both main and full-observer requests with a ten-second abort timer. A separate table timer remains active while the main request is pending. It rerenders at the earliest original H1 target, original row observation plus 90 seconds, or original report completion plus 90 seconds. It never updates those timestamps. Target expiry removes the applicable forecast while retaining the original report for any remaining eligible rows; stale report evidence clears the cache.

Every timer captures a generation token. Cancellation or a newer table generation prevents an old callback from clearing or repopulating a newer report. Visibility return also rechecks the table, covering browser throttling while a tab is hidden. Backward wall or monotonic time, or a wall/monotonic elapsed-time disagreement greater than one second, clears ledger eligibility until a new verified response. This clock comparison does not relax any original source/target freshness check.

The source remains table-only. Producer Bot activity and its original heartbeat diagnostics are unchanged. Current quotes, spread, account positions and news summaries retain their existing independent freshness checks. A ledger forecast is not an order, input-readiness claim, or measured-accuracy claim.

The focused fixtures include a genuinely unresolved main promise while the original target expires, abort-driven disconnection, row-age expiry before report-age expiry, a cancelled callback racing a newer report, visibility resume, and backward/forward clock anomalies. Final live browser acceptance belongs to the parent's controlled reload; this supplement describes source and fixture validation.
