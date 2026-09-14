# Feature observation operational repair, 2026-09-13

This is a separate research observation producer and source cohort. It does not change model weights, fit schemas, trading permissions or earlier archives.

## Components and activation

- `oanda_native_feature_candle_updater_v1.py` uses the existing practice instrument-candle GET client only. Default cache: `data/oanda_training_manager/native_feature_candles_v1`. Run `--once` for a bounded bootstrap pass, then supervise its normal loop. It requests 720 native M5 and H1 bars per pair initially and retains at most 1,024. The September 14 scheduler prioritizes a newly closed H1 boundary ahead of M5, checks the queue at least every five seconds, and permits at most four concurrent GETs and four starts per second. Maintenance polling remains 60 seconds for M5 and 300 seconds for H1; unresolved new boundaries may retry after 15 seconds. Each pass stops launching after 50 seconds or 136 requests, then completes its bounded in-flight GETs. Deferred inputs remain explicit and are reconsidered next pass. Writes are atomic and receipts retain actual retrieval clocks, content hash, incomplete-bar exclusions, revisions and supported gap-aware suffix counts.
- `oanda_feature_candle_inputs_v2.py` checks pair/timeframe, original timestamps, elapsed completion, explicit completion when present, prices, ordering and duplicates. M1 retains only its contiguous elapsed-minute suffix. M5/H1 may cross Friday 17:00 through Sunday 17:00 America/New_York, including DST. Unknown closures and intraday gaps break supported history. No candle is filled or synthesized.
- `oanda_research_feature_observation_worker_v2.py` reads original M1 files through `--candle-root` and the dedicated native cache through `--native-candle-root`. It reuses the isolated original calculators: 643 M1 MA features require 204 consecutive real M1 bars; structural calculations need at least 40 M5 and 60 primary bars. H4 uses exact completed groups of four native H1 bars. Partial groups remain excluded.
- Optional `--news-snapshot` reads the current repaired v2 publication using its existing replay/source-binding validation. The eight context/vetted observations use the existing joint-news v3 projection, explicitly identified in source lineage. This observational projection is separate from the fitted native joint-v7 model's revision-capture pipeline. Empty populations retain their model encodings with `default_no_evidence` state, not measured zero sentiment.
- Repeated `--model-study` reads current registered study ledgers in SQLite read-only transactions. It verifies contract/source bindings, the original forecast, publication and independent consumption receipts, pair/reference identity, and the existing pure price/native-joint forecast validators. Outputs preserve original issue/target clocks. These observations are not new forecasts, refits or independent performance evidence.

Suggested deployment arguments, relative to `trad`:

```text
oanda_research_feature_observation_worker_v2.py
  --quote-snapshot data/oanda_training_manager/state/practice_007_market_quotes_v1.json
  --candle-root data/oanda_training_manager/candles
  --native-candle-root data/oanda_training_manager/native_feature_candles_v1
  --archive-root data/oanda_training_manager/operational_repair_20260913_v1/feature_observations_v2
  --heartbeat data/oanda_training_manager/state/research_feature_observations_heartbeat_v2.json
  --clock-state data/oanda_training_manager/state/clock_integrity_v1.json
  --news-snapshot data/oanda_training_manager/local_news_sentiment_repair_v2/current_news_v2.json
  --model-study data/oanda_training_manager/operational_repair_20260913_v1/pair_local_forecast_study_v3
  --model-study data/oanda_training_manager/operational_repair_20260913_v1/joint_price_news_study_v7
  --interval-sec 60 --max-cycle-sec 45 --duration-sec 172800
```

The observation heartbeat schema is `research_existing_feature_observations_v2_20260913_native_calendar_events`; native-cache heartbeat schema is `native_feature_candle_cache_v1_20260913`. A 120–150 second supervision freshness budget accommodates reading/computation and the 60-second cycle. It does not relax the individual quote, candle, event, source-clock or expiry checks.

The existing archive envelope remains `feature_observation_archive_v1`; the producer, calendar, adapter source closure and configured input roots produce a distinct source-schema identity. Keep the new archive separate. The forward mapper's EVENT/calendar support and `oanda_feature_candle_inputs_v2.py` must be included in the new evaluator source bindings. Old evidence and old evaluator cohorts remain unchanged.

## Validation and observed limits

Fourteen focused tests cover actual repaired-news replay, a disposable consumed price ledger, original native-joint anchors, tampered consumption receipts, source binding changes, expiry, empty evidence states, native bootstrap/refresh, closed/future/incomplete candles, weekend/DST versus intraday gaps, exact H4 completion, M1 warm-up and immutable archive publication. The focused suite plus v1 producer regressions passed 29 tests before the final count-receipt/gap-report refinement; the final focused suite passed 14 tests.

The authorized native bootstrap completed 136 requests for 68 pairs with zero errors at 23:42:13 UTC. A separate observation smoke archive published in 23.11 seconds: 19,170,718 uncompressed bytes, 1,656,827 compressed bytes, and 52,259,496 input bytes. See `docs/validation/operational_repair_20260913_v1/feature_observer_smoke/report.json`.

That smoke recorded 33 fresh H1/H3/H4 structural pairs, compared with the earlier missing-source state. It did not establish every pair's readiness. Native M5 has real gaps on some pairs (for example USD/JPY missing 21:05 and 21:10 UTC; CAD/CHF missing 21:45 UTC), and M1 contiguous suffixes remain short on many pairs. Rich M1 had zero fresh pairs under the 204-minute rule. Repaired news was stale at that read and the new model roots had no current output. Those were explicit exclusions, not substituted values. Supervised live operation should verify the next repaired-news refresh and new study publications after activation.

Acceptance: repeated fresh publications with original source clocks; native receipts for all configured pairs; per-pair supported row counts and gap exclusions; rich M1 only after 204 current contiguous bars; higher-frame freshness excluding only the declared weekend closure; news evidence retained and nonempty directional/context status distinguished; current independently consumed model outputs with unaltered targets; then sufficient same-cohort observation history for forward comparisons. Source availability and correct metrics do not establish profitable prediction.

## Hour-boundary scheduler follow-up

The first deployed native scheduler retrieved EUR/USD's 23:00–00:00 H1 candle at 00:04:15 UTC: its previous read was 23:58:25 and the five-minute maintenance interval plus serial M5-first processing delayed the new hour. The replacement removes that polling gap without extending feature freshness tolerances. Its deterministic simulation uses the production scheduling and rate decisions: with four prior M5 GETs occupying all slots for 20 seconds and each subsequent H1 GET taking two seconds, all 68 H1 inputs complete by 54 seconds. This verifies the scheduler under declared latencies, not an external-network service guarantee. The focused suite now passes 17 tests, including actual executor/cache tests with fake GETs.

Observation archives retain the original native CSV tail identity, actual local-read clock and unchanged bar clocks. Native GET receipts remain adjacent cache sidecars; the current observer does not embed those sidecars in its immutable feature envelope. The exact H4 UTC aggregation also intentionally omits the incomplete Sunday 20:00–00:00 bucket, because 20:00–21:00 was closed. Its next full UTC H4 bucket closes Monday 04:00; that is separate from the repaired native H1 polling delay.
