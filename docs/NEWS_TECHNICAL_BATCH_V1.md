# Large archive diagnostic — September 30

**Scope correction:** this batch is not an audit of the existing technical models.
Mean reversion, reversal-capable specialists and residual-error correction already
have retained studies. Consult [current evidence](PROJECT_STATE.md) before treating
reversal or news-versus-momentum disagreement as a new hypothesis. The batch dates
do not overlap the retained August24–September7 specialist assessment forecasts.

Read 9,048 news records first seen September 15–17 and the existing rolling technical archive. Seven USD majors; no fits, provider calls or live changes. Contract fixed before execution. New policy directions omit reported price moves and analyst opinions. The technical baseline is preceding15minute momentum, NOT the trained technical model. Timing uses stored classification availability and fresh already-published features; future/backfilled inputs excluded. Outcomes start at next completed candle close with bid/ask spread proxies.

Coverage:7,288 normalized distinct headlines;934 selected pair/time observations;229 distinct selected events. Zero selected original article records were directional-publication eligible. Duplicate headline filtering and one observation per pair/15minute clock bucket reduce redundancy, but paraphrases, cross-pair exposure and overlapping horizons remain dependent. No independent sample-size or statistical significance claim.

When new policy interpretation and technical momentum agreed:

| Horizon | Observations | Direction correct | Mean after-spread bps |
|---|---:|---:|---:|
| 5m |132|47.0%|-1.980|
|15m|131|48.1%|-0.050|
|30m|117|47.9%|-0.486|
|60m|111|47.7%|-0.065|

No profitable edge or interpretation-driven predictive improvement is demonstrated. In the small identical-support15minute subset where old news, new news and technical directions all exist (25 rows), accuracy is40%,32%,64% respectively; all three lose after spread. Do not contrast full-arm metrics as if their participation were identical. These inspected dates and newly applied interpretations are retrospective diagnostics, not original predictions, untouched confirmation or trading authorization. Slippage/financing omitted; first immutable classification clocks not independently re-attested.

Evidence: evidence/news_technical_batch_v1_20260930/CONTRACT.json and RESULTS_FINAL.json. RESULTS.json preserves the first output; final output adds matched-support aggregation over the same rows. Payload identities and feature hashes retained. Four focused tests passed, plus six smoke assertions; same-task review, not independent.

Reproduce with tools/forex_news_technical_batch.py --news <existing-news-db> --technicals <existing-technical-db> --contract <CONTRACT.json> --output <new-file>. Outputs refuse overwrite. Technical source and original news archive remain read-only. This uses the prior offline timing-repair path; global research/deployment preflight remains unqualified.

Next: retrieve actual saved technical-model forecasts over overlapping timestamps, qualify immutable news versions, and run a prespecified later-period matched comparison. Do not keep tuning these development examples. Vault: NEWS_TECHNICAL_BATCH_V1_20260930; pointer NEWS_TECHNICAL_BATCH_LATEST.json.
