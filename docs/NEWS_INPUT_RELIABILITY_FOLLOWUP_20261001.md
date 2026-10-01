# Later live readback — October 1, 05:47 UTC

This supplements the successful 05:43 read in NEWS_INPUT_RELIABILITY_20261001.md.
The forecast publication advanced but returned `inputs_unavailable`, with zero
eligible forecasts. Currency-news context remained current. Joint forecasting
had 64 warming and four unavailable pairs; its cumulative error count remained113.

The retained diagnostics reported `rolling_envelope_or_status_unreadable`, despite
an available rolling publication generation and no raw read errors. Inspection of
`oanda_all68_technical_availability_v2.py` shows that its before/after status reads
must match the envelope generation; if neither matches, it passes `None` into
the report builder and produces that error. This is evidence of a generation
binding failure, not proof that a file is absent or unreadable.

The retained consumer already retries the three-file read three times, 50 ms
apart, in `read_feature_publication`. Do not add a duplicate retry helper.
The producer writes the envelope before status, and an exception also replaces
status with an error record without a generation. Either path needs actual
failure-time capture before attributing the incident to a race. The producer
subsequently published another generation successfully at 05:49:40 UTC.

Next: capture the actual envelope and both status generations during the failure,
trace publisher ordering or an error-status replacement, then repair the proven
cause with meaningful tests. Preserve original source and observation
identities, freshness limits and frozen cohorts. No stale-data fallback or
automatic re-sealing. Continue collector progress/read reliability afterward.

Local evidence: `evidence/news_input_reliability_20261001/FOLLOWUP_READBACK.json`
and `DASHBOARD_FOLLOWUP.json`. Shared final publication receipt:
`NEWS_INPUT_RELIABILITY_PUBLICATION_20261001/RECEIPT.json`.
The pipeline is not declared fully reliable or complete.
