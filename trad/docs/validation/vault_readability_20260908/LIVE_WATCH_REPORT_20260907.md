Completed observation of the requested 2026-09-07T19:36:13Z–2026-09-07T20:36:13Z window. The bot remained partially operational in research mode; this is not a trading-readiness or profitability pass.

Persisted sampling ran 2026-09-07T19:38:51Z–2026-09-07T20:36:14Z: 116 successful API samples and 0 API errors. Earlier live checks preceded the persisted loop. Forecast coverage ranged 6–10/68 pairs; current-price coverage ranged 27–63/68. Final API observation: 2026-09-07T20:36:14Z, counts {"forecast": 9, "unavailable": 16, "warming": 43}.

NAV ranged $41.6042–$41.6042; last $41.6042. Observed open trades ranged 0–0; pending orders 0–0. Account freshness exceptions: {"account_values_current": 0, "orders_current": 0, "positions_current": 0}. No private account identifiers are included. Research quote outcomes below are separate from account P/L.

News sampled states: {"current": 79, "stale": 37}; maximum retained evidence age 411.8s. Longest observed stale run: 4 samples spanning 90.9s, 2026-09-07T20:33:21Z–2026-09-07T20:34:52Z. With nominal 30-second sampling, this is not an exact continuous outage duration. Stale news is withheld, not converted to neutral.

Collection/process checks are separately timestamped at 2026-09-07T19:40:53.670626+00:00, 2026-09-07T19:50:44.339786+00:00, 2026-09-07T20:02:40.598573+00:00, 2026-09-07T20:17:12.155939+00:00, 2026-09-07T20:36:54.526127+00:00. Final check: {"preserved_other_process_count": 26, "preserved_other_worker_count": 13, "running_worker_count": 14, "same_process_identities_as_185143": true, "status": "passed"}. Pair-worker errors: 0; original EUR companion errors: 16. API alerts: {}. Retained, heartbeat-bound summary generations served 15 samples; read-mismatch diagnostics remain in the JSON and are distinct from unavailable API results.

Performance snapshots: initial 2026-09-07T19:40:34.659853+00:00 and final 2026-09-07T20:36:57.491396+00:00. 23 newly scored pair decisions were added; exact IDs are retained in the JSON. Outcomes may belong to forecasts issued before the watch. Prior scored rows changed: 0; disappeared: 0.

| Snapshot | Model | Scored | Direction hits / directional | Positive after spread / directional | Mean net bps |
| --- | --- | ---: | ---: | ---: | ---: |
| Initial | State space | 83 | 47 / 83 | 5 / 83 | -2.902 |
| Initial | Ridge | 83 | 50 / 83 | 7 / 83 | -2.923 |
| Final | State space | 106 | 55 / 106 | 5 / 106 | -2.862 |
| Final | Ridge | 106 | 62 / 106 | 7 / 106 | -2.860 |

Newly scored decisions only:

- State space: 8 direction hits and 0 positive after spread among 23 newly scored decisions; mean net -2.720 bps.
- Ridge: 12 direction hits and 0 positive after spread among 23 newly scored decisions; mean net -2.634 bps.

Final saved-score lag: 0 fresh-scored IDs missing from saved scorecards across 0 pairs. Exact stored-versus-fresh reconciliation is retained. An old scorecard with no newer outcomes is not automatically an incorrect scorecard.

Final API missing-forecast reasons at 2026-09-07T20:36:14Z: own_minute_warmup_at_last_attempt: 43; fresh_quote_unavailable: 13; market_closed_or_no_stream_quote: 3. Minute counts in these API reasons describe the last recorded attempt, not an independently recomputed current count.

Independent missing-input audit covers ['2026-09-07T20:09:17.391769+00:00', '2026-09-07T20:09:24.319212+00:00'], with ledger reads ['2026-09-07T20:08:24.077571+00:00', '2026-09-07T20:08:24.501953+00:00']. Its dated input groups are {"input_minima_met": 7, "insufficient_consecutive_own_minutes": 57, "insufficient_mature_ridge_labels": 1, "stale_archive_endpoint": 3}. Those earlier counts are not substituted for final readiness. Remaining causes include real-minute continuity, fresh quote/archive endpoints, mature Ridge labels and the fixed attempt cadence; no bars or forecasts were fabricated.

The retained AUD/CAD incident records HTTP504 followed by recovery at 2026-09-07T20:18:50Z in the next ordinary updater cycle; the receipt does not claim omitted minutes were recovered. Final minute-cycle counts: {"dry_run": false, "error_count": 0, "gap_recovery_requests": 55, "gap_recovery_scope": "bounded_recent_small_interior_gaps_only; not_a_full_archive_completeness_check", "gap_recovery_unknown_pair_count": 0, "gap_recovery_unresolved_minutes": 3438, "generated_utc": "2026-09-07T20:32:12.685656+00:00", "pair_count": 68, "schema_version": "all68_m1_forward_updater_v1", "started_utc": "2026-09-07T20:30:24.601907+00:00", "total_rows_appended": 195, "total_rows_backfilled": 0, "total_rows_recovered": 0}.

Frozen initial-baseline agreement: 42 decisions; State space 28 hits, 2 positive after spread, mean -2.271 bps; Ridge 28 hits, 2 positive after spread, mean -2.271 bps.
Frozen initial-baseline disagreement: 41 decisions; State space 19 hits, 3 positive after spread, mean -3.548 bps; Ridge 22 hits, 5 positive after spread, mean -3.591 bps.

Agreement analysis uses only the initial 2026-09-07T19:40:34.659853+00:00 snapshot and does not validate an agreement-based strategy. Pair rows and H1 outcomes overlap. The original EUR/USD diagnostic remains supplemental: {"capacity_state": "within_limits", "excluded_decision_count": 2, "freshness": "current", "generated_utc": "2026-09-07T20:36:57.445309+00:00", "original_decision_count": 6, "original_scorer": {"error": "duplicate_market_reference_epoch", "status": "failed"}, "retained_decision_count": 4, "status": "available"}, with 4 scored retained unique decisions; it does not repair the frozen scorer failure or count as registered performance.

This finalizer only read retained files and created this report plus its JSON summary. The observation session action counts, every input hash, sampled failures and snapshot clocks are retained in LIVE_WATCH_SUMMARY_20260907.json. No model, ledger, account, runtime, project or vault changes were made by this finalizer.
