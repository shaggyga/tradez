# News feed audit — 7 September 2026

Read-only observation at 18:08:53–18:08:56 UTC (14:08 EDT). Evidence: `FEED_AUDIT.json`. No runtime, collector, source, registration, order or supervisor changes were made.

The repeated “23 articles screened” was a dashboard representation problem. Each row uses the same global active-topic count, not a separate screening count for that pair. That count had become **19** at this observation. The API also removes all NEUTRAL pair summaries and returns at most twelve ranked non-neutral pairs, hiding available contextual headlines and sentiment.

The underlying zero forward direction is real under the current rules. All **68** pair records are NEUTRAL with zero published forward scores. Reconstructing the live scoring prefilter from a read-only SQLite transaction reproduced exactly **19** active topic clusters:

- **10** secondary topics lacked required independent corroboration.
- **8** reported market moves that had already occurred.
- **1** was discovered too late for its permitted reaction timing.

Those are the topics' recorded primary withholding reasons. Separately, **9 of 19** were also beyond their initial reaction horizon. All 19 published currency-score maps were empty; research currency scores were retained. Consequently, displaying these as fresh directional forecasts would misstate what the collector produced.

There is useful pair-specific context: EUR/USD had 19 related topics, including 13 contextual directional topics; AUD/JPY had 15 and 15; USD/HUF had 16 and 10. None had an eligible forward topic. These counts are available in the raw pair snapshot but hidden from the current table. Generic tone was negative for 9 of the active topics and zero for 10; generic tone is a separate quantity from a currency direction.

Collection is active. The latest heartbeat showed a running postprocessing cycle. The last completed collector record reported 1,101 fetched/classified items, 15 inserted items, 894 duplicates, and 192 items outside retention. Local retained content published within the last hour comprised **61 article rows, 6 relevant**; within 24 hours it comprised **2,521 rows, 185 relevant**. These are raw stored articles and can include syndicated copies, whereas 19 is the deduplicated active-topic count. The most recent retained publisher timestamp was 18:01:01 UTC. All 19 active topics' representative feeds were discovery feeds: Google News (17) and Finnhub (2).

The collector reported 192 configured sources: 176 healthy, 3 degraded, 5 disabled, 5 unsupported, 1 missing a credential and 2 external adapters. The three enabled sources with current errors were GDELT discovery (HTTP 429 rate limit), HKMA press releases (read timeout), and Sweden SCB CPI/CPIF (malformed or missing summary). Trading Economics' calendar lacks a credential. Some RBNZ and other inventory feeds remain unsupported or deliberately disabled. The official release fast lane separately completed 42 configured source checks, attempted six due sources, reported no errors and inserted no new observation in that pass. Per-source last success, last retained publication and counts are recorded in the evidence.

Feed health and content freshness need separate labels. The coverage report uses a different recency-based health definition from collector error status. The API was current at this observation, but its pair snapshot was generated at 18:04:39 UTC from an analytical `as_of` of 18:02:58 UTC. A current output timestamp does not make an older story a fresh catalyst. Research-only flags and zero execution weight remained set.

Suggested next changes: show neutral context for every pair, with its actual related-topic count, headline age and withholding reason; put global collection totals in one feed-status line; distinguish contextual tone from a current forward direction; and repair the three failing feeds separately. Keep corroboration, causal timestamps, prior-move guards and research-only execution boundaries intact. This audit provides no evidence that relaxing them would improve prediction accuracy.

Source logic: `oanda_main_signal_dashboard.html:218`, `oanda_practice_live_dashboard.py:6431`, and `oanda_local_news_sentiment.py:18728`, `:19078`, `:19303`. `docs/LOCAL_NEWS_SENTIMENT.md` describes the intended research-only distinction. Evidence contains source hashes and observation times; JSON and database reads are separate snapshots, not an atomic multi-file generation.
