# News sentiment and input timing — September 30

Manual web comparison and read-only local audit completed at 17:54 UTC (13:54 New York). This is a selected-case interpretation review, not an accuracy estimate or a predictive experiment. The live parser has **not** been replaced by this audit.

## Captured news and actual delay

The active `trad/data/oanda_training_manager/local_news_sentiment/local_news_sentiment_v1.sqlite` contained 2,458 records first observed since New York midnight. These include syndicated stories and older catch-up material; they are not 2,458 independent events. After the initial catch-up, complete hourly buckets previously measured about 160–290 records/hour. A configured 60-second loop delay is not a guarantee that every source is polled once a minute.

Across all 2,458 records, first capture to **first** recorded classification was median 0.994 seconds, 90th percentile 3.614 seconds and 95th percentile 4.775 seconds. Among 1,672 records both published today and captured after 09:00 UTC, publication to capture was median 743.450 seconds (12.4 minutes), 90th percentile 52.5 minutes. This includes publisher/aggregator delays, collector scheduling and republication; it cannot isolate network latency.

Later classification versions are not initial parsing latency. Barr's first parse took 1.20 seconds; the later 29-minute timestamp change corresponded to `forward_signal_timely` changing. Williams's first parse took 0.60 seconds; later versions changed timeliness and headline/source-name identity. Hormuz versions also changed discovery-source provenance. Existing historical clocks must not be backdated to erase these differences.

The current guarded view at audit time contained **34 context topics and zero admitted directional topics**. Raw article scores below therefore must not be represented as issued trading signals.

## Manual comparison

| Story | Stored article result | Text-supported interpretation |
|---|---|---|
| Barr warns more hikes may be needed | USD +0.9 | Tightening guidance, not a new rate decision. The [Fed speech](https://www.federalreserve.gov/newsevents/speech/barr20260929a.htm) is dated September 29, though the captured secondary story was published September 30. |
| Williams sees no urgency for next hike | USD +0.9 | Distinguish possible later tightening from reduced near-term urgency. [Reuters via Kitco](https://www.kitco.com/news/off-the-wire/2026-09-30/feds-williams-sees-no-urgency-next-rate-hike) describes market repricing toward later action. Identical scores for this and the Barr headline lose that distinction. |
| Cooling inflation tempers Fed hike concerns | USD +0.9 | The qualifying relationship points toward reduced hike concerns, not an unqualified hawkish USD reading. The [MarketScreener news listing](https://www.marketscreener.com/quote/index/S-P-500-4985/) independently carries the matching September 30 Reuters headline; full article text was not retrieved in this audit. |
| Softer PCE and reported dollar decline | No raw currency scores; AUD/GBP tags | A retrospective currency response with inflation context. [Action Forex's AUD listing](https://www.actionforex.com/markets/currencies/aud/) carries the matching headline. Locally it arrived 47.5 minutes after publication; this cannot be sold as an early prediction. |
| Oil/gas production rises; price uncertainty persists | CAD/MXN/NOK +0.5, JPY −0.2 | Quantity is not price. The captured Reuters/Kitco/EnergyNow headline does not establish these currency directions. Exact full article was not independently retrieved. The [Dallas Fed release schedule](https://www.dallasfed.org/research/surveys/des) corroborates the scheduled survey date, not the reported result. |
| Iran losing grip on blockade as shipments rise | Risk-off category and nine currency scores | Supply-constraint relief is materially different from a new supply shock. Exact Business Post article was not independently retrieved; this finding concerns the stored headline's interpretation, not independent confirmation of the event. |

Manual search also found [a September 30 Reuters rand report](https://abokifx.com/news/south-african-rand-firms-as-traders-assess-raft-of-local-economic-data?type=market). A bounded title search did not find that exact headline in today's active database. That is an unmatched title, not proof the underlying event was never captured elsewhere. Web retrieval is manual present-time context; it was not added to historical model inputs.

## Moves and technicals

Reused `oanda_news_technical_timing_v1.py`, preserving each current stored version's causal availability. EUR/USD was selected as a fixed USD representative before inspecting outcomes. Of four USD cases, only the PCE-reaction case had a sufficiently fresh published technical snapshot under the existing 180-second diagnostic gate. Its snapshot had 216 finite features and was 86 seconds old. The other three retain `no_fresh_published_technical_snapshot`; later backfills do not cure historical availability.

Gap-free subsequent price paths were present for all four at 5/15/30/60 minutes. For example, after the PCE article's delayed availability, EUR/USD moved −1.58 bps over 15 minutes and −5.72 bps over 60 minutes, despite the headline describing dollar weakness. Those are retrospective endpoint moves, not saved predictions, executed trades or causal effects. This is why better text interpretation alone cannot establish forecasting improvement. Exact bid/ask proxies and clocks are in `MANUAL_NEWS_AUDIT.json`.

The old dashboard move/news snapshot and outcomes remain dated September 5. This audit does not revive that monitor or claim those old outcomes cover today.

## Simpler sentiment path

The existing configuration already includes official [Fed RSS](https://www.federalreserve.gov/feeds/feeds.htm) and [ECB feeds](https://www.ecb.europa.eu/home/html/rss.en.html). No new paid provider is required to make the current design simpler:

1. Prioritize those official releases plus a small verified FX-news feed set for prompt ingestion; keep slower broad discovery separate. Retire duplicate polling only after checking downstream consumers.
2. Parse material headline/summary content once per version. Keep original text, publisher time, first observed time and interpretation version. Store provenance separately; do not silently merge distinct evidence identities.
3. Extract event/currency, actual decision versus guidance, policy tone, reported reaction and uncertainty. Do not turn generic positive sentiment or an oil keyword into currency returns.
4. Compute freshness/expiry at read time without pretending it is new semantic understanding. Maintain a compact active table and the existing reproducible archive.
5. Feed typed context into the technical comparison with matched controls and actual issue clocks. Validate incremental value before choosing forecast weights or calling it the best model.

This is the next correction contract, not a claim that the live classifier has already been migrated. The existing additive interpretation v2 is safer on ambiguous commodities but still misses these policy-timing formulations; another helper alone will not finish the live integration.

## Deployed timing repair and verification

Changed only `trad/config/operational_runtime_current_20260930.json`: Normal CPU priority for technical and price workers, and technical **supervisor liveness** tolerance 180→600 seconds. The worker was completing valid ~307-second cycles, so the old threshold could trigger recovery against ongoing work after startup grace. Model quote/input/feature admission limits, numerical source, cohorts, original recovery expiry and retry histories are unchanged.

Validated the candidate through the real recovery contract before cutover. Restarted only the two exact recovery controllers; retained the worker processes and explicitly applied priority to their existing process chains. The supervisor read back profile SHA-256 `971d2df144e552feae24b3ec013cfdba3621c912cde93ab3b93ab151bc8fc07a` without error. Technical PID 10600 remained unchanged. Post-change observed cycles: 144.907, 182.594 and 193.062 seconds; current-pair counts 65, 64 and 63 versus 45 at the measured baseline. Workload varies, so this is not a controlled causal speed estimate. Full feature population remained 56 pairs; sparse/gap cases are not fixed by priority.

Fresh verification: 83 tests and 13 subtests passed across recovery V6, interpretation and timing. One non-fatal pytest-cache permission warning. Profile semantic comparison contains only the three operational settings plus appended notes; JSON serialization also changed line endings. Same-task substantive review only, not independent approval.

The earlier global preflight's historical design-manifest/review-schema findings remain unresolved. This operational change does not certify the scientific queue or qualify a new fitted model. Preserved Ridge/HGB development evidence does not establish a universal best replacement: all 64 learned-model/period comparisons were worse than no-change on MAE. No base model was refitted or automatically promoted.

Local evidence: `evidence/live_model_input_repair_20260930/`. Vault packet: `LIVE_MODEL_INPUT_AND_NEWS_AUDIT_20260930_175500`. Exact resume is the simplified sentiment correction above, followed by current-root move-monitor integration and saved-model prospective comparison qualification. The independent design queue still retains the Extra Trees partial draft.
