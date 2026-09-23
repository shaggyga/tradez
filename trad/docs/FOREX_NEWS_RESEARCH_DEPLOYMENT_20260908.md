# Repaired news study: deployment and live verification

Recorded observation: **2026-09-08T20:45:30.284992+00:00**. This is dated evidence, not ongoing telemetry.

The separate repaired-news study is running and selected in the dashboard. The final local observation verified **65/68 pairs with unelapsed combined H1 forecasts**, **17/17 managed research workers**, and the new registry/source bindings. The independent read-only ledger inspection passed all **68** databases: **89 forecasts, 89 publications, 89 consumptions, 83 later research entries, 0 completed outcomes, and 6 exclusions**. A research entry is a selected bid/ask quote, not an executed position. Orders, promotion and authorization remain disabled.

The independent ledger observation completed at `2026-09-08T20:38:16.678701+00:00`; its total issued decisions and the API's unique currently forecast pairs have different denominators and observation clocks.

## What was repaired

The fresh pre-deployment observation had **0/68** active joint-v2 forecasts. Its retained reasons included 39 history-row-limit failures, 24 stale-news failures, three closed/non-stream quotes and two stale quotes. This differed from the earlier morning topic-identity collision; both observations remain retained.

1. A separate producer reconciles compatible topic identities before the unchanged causal admission guard. It selects the complete relevant publication window in one read-only SQLite transaction, retains original member payloads/arrival clocks/expiry, and checks current collector progress and independently fresh clock evidence. Conflicting identities, duplicate members within a topic and stale/future observations still fail closed; compatible exact duplicates across variants collapse with their identity preserved. The classifier and original collector are unchanged.
2. A separate adapter replaces the exhausted 5,000-row ceiling with a complete, paged 48-hour read bounded by 10,000 rows, 128 MiB original wrappers, 1 MiB per row and 20 seconds. It never truncates the selected history. The final real probe read **5,508 events**, using **13,257,016 canonical bytes** and **2,394,116 compressed bytes**, within unchanged 16 MiB/8 MiB capture limits. These are finite capacity limits; continued growth remains monitored.
3. Verified immutable captures are shared while their source generation, governed database signatures and expiry remain valid. In the final 68-request probe there was one history read; median cache request time was 0.218 seconds. Source changes still invalidate the cache.
4. The live initial 15-second producer interval caused nearly one large capture per pair. The supervisor now uses the producer's existing **60-second** interval. The original 300-second news age and member expiry remain unchanged. In the first 208.9-second after window, five shared captures supported seven completed pair captures; files per completed pair fell from 1.059 to 0.714. The bytes/time rate was about 418.5 versus 197.6 MiB/hour when expressed in hourly units. The denominators differ: the before rate uses the span between captures, while the after rate includes startup time from reload completion. This is observed short-window churn, not a controlled 50% improvement or guaranteed long-run rate. Median pair-completion spacing worsened from 20.5 to 31.0 seconds in that first window; faster coverage was **not** demonstrated there. The extended comparison is also retained. The change is retained for lower capture/storage churn; processing speed remains open.

The subsequent 638.8-second cumulative window contained 44 pair captures (41 ready, three withheld), 17 shared files and 40,917,071 bytes. Median completion spacing was 10.871 seconds across 43 intervals, with 0.386 shared files per completed capture. This shows better observed reuse and spacing over the longer window; it is still not a matched controlled throughput experiment. The withheld inputs included one `future_news_mapping` and two stale/future pair references despite zero cumulative worker errors. A refusal to admit data is distinct from a worker exception.
5. The dashboard explicitly selects v3 by registry hash and actual selection time. Invalid selected evidence is withheld; old v1/v2 results remain separate. Runtime health expects 17 workers and storage inventories 420 managed databases. The retired watchlist's failed classification check now maps to its actual disabled producer. Its artifact check remains failed; its scope becomes active/unknown if that producer is running or unobserved.

## Registration and actual clocks

- Registry: `config/joint_price_news_study_v3_20260908.json`; canonical SHA-256 `ee075e69e80ca56dfdf45abe1f7a1a301612176e67f7622eab1adf2bd9af8771`.
- Empty ledger activation: `2026-09-08T20:06:42.472035+00:00` through `2026-09-08T20:06:45.009822+00:00`. All 68 began empty, with no forecast or outcome imports.
- Primary dashboard selection: `2026-09-08T20:11:01.625350+00:00`. Selection time is separate from ledger activation.
- Study root: `data/oanda_training_manager/joint_price_news_study_v3`.
- Repaired producer output: `data/oanda_training_manager/local_news_sentiment_repair_v1/current_news_v1.json`.
- Exactly 20 source bindings identify the new cohort. The four prior 68-pair registries and their registered model/input/scoring sources still match their original hashes. Existing quote, account, original news and study processes were preserved during the targeted operational reloads.

The numerical model, 34-feature definition, matched price-only comparator, neutral-news ablation and original reference-plus-3,600-second target are unchanged. The repair does not activate the older broad-feature or multi-horizon engines.

## What the news results mean

The final real EUR/USD preflight fit was ready with 176 training examples and zero vetted directional-news rows. Its computed expected move was +0.98906 pips, with an uncalibrated p(up) of 0.54207; neutral-news ablation was also +0.98906 pips, while the separately fitted matched price-only model gave +0.32063 pips. That preflight was not issued into a study. Initial live examples also retained nonzero context inputs, which can change a joint estimate without a vetted directional claim. These comparisons demonstrate correct input use and model decomposition, not causal news impact or predictive edge.

The earlier frozen joint-v2 baseline remains **2,779 completed outcomes, 52.07% directional accuracy, 30.66% positive after spread, and −2.9693 mean net basis points**. Its magnitude and probability errors did not beat the stated zero-move/fixed-50% baselines. The new study's current forecasts cannot establish improved accuracy before their original targets and subsequent independent-session evaluation. Spread-net research outcomes are not realized account P/L.

## Verification and limits

Final focused batches: producer/reconciler **118 passed**; adapter/ledger **231 passed**; worker/preparation **113 passed**; dashboard/health/storage first integration **123 passed**; operational gates after the cadence change **97 passed**; expanded storage **27 passed**; final runtime scope **30 passed**; independent read-only inspector **29 passed**. Some batches overlap; do not add them as independent test cases.

The inspector independently checked stored contract/activation, input and forecast hashes, publication and consumption receipts, new cohort identities, exact later entry quotes and original H1 endpoints on coherent per-database read-only transactions. It does not claim a single atomic snapshot across 68 databases or re-score completed outcomes.

All intermediate source-transition failures, slow benchmarks and prior test receipts remain retained. One initial primary-selection preflight withheld before any pointer write; its exact rejected projection was not retained, so its root cause is not claimed. The following observation and bounded preflight verified current publications before selection.

A later API observation withheld all rows with `pair_summary_generation_mismatch` while the independent producer retained valid forecasts. The frozen worker's separate summary/heartbeat timers can temporarily expose different generations; an exact prior generation may not be in the display cache. Further dashboard work was deferred at the user's request. An unlaunched retry edit was restored to the exact preceding source hash; the display's binding checks remain unchanged. The failed API observation and following current observation remain retained, rather than portraying the service as continuously available.

Remaining pair statuses in the final API observation: `{"ValueError:market_closed_or_no_stream_quote": 3, "forecast": 65}`. Forecast availability, quote currentness and completed evaluation are separate states. Producer cumulative errors at that observation: **0**, last error ``.

The bounded 20:38 UTC input triage found GBP/NZD and NZD/HKD both ready on current actual inputs (175/165 mature labels, 144/129 nonzero-context rows, zero vetted-direction rows). Their separate fits took 8.55/1.60 seconds and were not issued into any live ledger. A pinned 5,551-row history snapshot had no invalid clocks, visibility after the sampled cutoff or effective time after visibility. The old GBP/NZD rejection's exact offending row/cutoff was not retained. An arrival between the retained cutoff and later database snapshot is a possible mechanism, not an established historical cause. These current successes indicate recoverable input/refresh gaps, not structurally absent models; improve bounded retries and retain specific rejected clock evidence in a future registered worker/input version. Three TRY instruments reported no tradeable stream quote. No quotes or missing bars were fabricated.

The first recovery and broad-model work remain in [pending improvements](../FOREX_PENDING_IMPROVEMENTS.md): restore compatible historical engines, repair duplicated cross features and pip-scale rankings in separate versions, evaluate the blurb/analogue data causally, improve calibration/cost and currency co-movement models, and compare technical-only/news-only/combined forecasts on matched independent time blocks. Capture storage growth, finite history capacity and slow recovery after transient input refusals remain core follow-ups. Dashboard generation races are deferred per the user's stated priority. No evidence retention deletion was introduced.

## Evidence

- [Deployment validation](../FOREX_NEWS_RESEARCH_DEPLOYMENT_VALIDATION_20260908.json)
- [Finalized implementation and test copies](validation/news_identity_integration_20260908/EVIDENCE_COPY_MANIFEST_20260908.json)
- [Actual deployment/runtime copies](validation/news_research_deployment_20260908/DEPLOYMENT_EVIDENCE_MANIFEST_20260908.json)
- [Original baseline and recovery](FOREX_REVAMP_BASELINE_RECOVERY_20260908.md)

The vault export contains source and compact evidence, not all private runtime databases, raw news or credentials. Cloud synchronization is separate from verifying local OneDrive bytes.
