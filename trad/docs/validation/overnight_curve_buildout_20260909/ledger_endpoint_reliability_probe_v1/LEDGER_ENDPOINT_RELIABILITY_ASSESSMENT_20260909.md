# Ledger endpoint reliability, 9 September 2026

The read-only probe ran from 09:51:20 to 10:11:20 UTC for 1,200.015 seconds. All four original source/config pins and both served HTML hashes stayed unchanged.

All 77 ledger HTTP polls validated; there were no HTTP or projection errors. They contained 66 distinct original observer reports. Of those, 26 had pair-budget failures, totaling 40 pair failures across 26 pairs. All reports visited all 68 pairs; 40 verified all 68. Cached repeated reports are counted once for recurrence.

Seven pairs failed repeatedly: AUD_CHF (5), AUD_HKD (5), AUD_JPY (2), CAD_SGD (2), EUR_NOK (3), GBP_PLN (2), USD_CAD (2). The other 19 affected pairs each failed in one distinct report. Visible current forecasts ranged from 63 to 66 across polls. TRY_JPY and USD_TRY consistently had no verified published forecast; they are separate from observation timeouts.

HTTP latency was 0.250–3.547 seconds (median 0.391). Original observer reports took 0.547–2.828 seconds (median 1.110); failed reports took 1.094–2.828 seconds. No global-budget unvisited pairs occurred. These are elapsed observations, not CPU/SQLite-lock measurements.

The original producer envelope was coherent in 46 distinct reports and had a generation mismatch in 20. Its reported error counter remained 13. This status is separate from the verified ledger publication chain.

The evidence supports evaluating one fresh bounded retry inside the existing eight-second ledger budget. It does not establish a root cause or prove retry gains. V1's public pair-budget reason combines deadline and clock chronology checks, and failed-stage timing/SQLite primary-code detail is absent. Integrity failures and missing publications must remain ineligible for retry. Original forecast targets and observations must remain unchanged.

There were 80 total HTTP requests including the preserved initial one-HTML-GET probe transport failure; the completed run used 77 ledger polls and two HTML reads. It retained 23,370,755 private response bytes. No account, news, or broker endpoint was accessed. Console progress sampled every fourth poll; this assessment uses the complete retained log.

Source-bound assessment: `LEDGER_ENDPOINT_RELIABILITY_ASSESSMENT_20260909.json`, SHA-256 `917d946211bcb16153ad3e3b0a74b1ca2ef6b18039fe65b6a6801bbfa0fca4f6`. Original completed receipt: `LEDGER_ENDPOINT_RELIABILITY_PROBE_20260909.json`, SHA-256 `8b3ce5c1de262e84fac40a8897e10070ddd0d92a18ce00ce4028f87fe00b7c53`. Exact responses remain private; compact summary and sanitized clock/count evidence are suitable for curation.
