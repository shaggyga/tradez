# September 13 opening session: prices and news

This review examines the original histories for all 68 configured FX pairs, independently of the dashboard. The requested interval is September 13, 2026, 5–10 p.m. EDT. The first retained opening M1 bar is 5:04 p.m.; final closes are at 10 p.m. There are observations for 65 pairs. EUR/TRY, USD/TRY and TRY/JPY have no bars in this interval. Missing minutes are retained as missing; only four observed pairs have a complete uninterrupted interval.

**Independent broker cross-check, September 14 around 03:49 UTC:** all 68 retrospective instrument-candle GETs returned HTTP 200. The broker returned exactly the same timestamp sets and all 12 bid/ask/mid OHLC fields as the original cached snapshots: zero additional bars, missing broker bars or changed prices. The three TRY responses were empty. All endpoint, range and hourly rankings below are unchanged. These later reads verify the market-history report; they do not establish that newly requested evidence was available at an earlier decision. Raw responses, original hashes and actual later receipt times are retained in [the cross-check folder](../../operational_repairs_20260913/first_five_hours_20260913/broker_crosscheck_20260914T034625Z).

## Observed movement

NZD weakness is the strongest common pattern. AUD also weakens, followed by a later rise in USD/JPY. These are retrospective observations, not predictions made by the bot.

| Pair | Opening-to-10 p.m. mid change |
| --- | ---: |
| NZD/HKD | −0.386% |
| NZD/CAD | −0.345% |
| NZD/USD | −0.325% |
| NZD/SGD | −0.262% |
| USD/HUF | +0.255% |
| GBP/NZD | +0.252% |
| NZD/CHF | −0.236% |
| NZD/JPY | −0.230% |
| AUD/USD | −0.193% |
| USD/JPY | +0.135% |

NZD/USD falls from 0.58143 to 0.57954, or 18.9 pips. Its largest fully populated 60-minute decline is 7:30–8:30 p.m., −0.313% (approximately 18.2 pips). The sharpest fully populated 15-minute decline is 8:13–8:28 p.m., −0.172%. AUD/USD falls 0.196% during 7:30–8:30 p.m.; USD/JPY rises 0.230% during 8:21–9:21 p.m.

USD/SEK has the largest recorded high–low envelope, 0.627%, and finishes +0.185%. Its 6:15–6:30 p.m. midpoint rise of 0.267% becomes −0.045% using the starting ask and ending bid. USD/HUF's opening-to-close +0.255% mid change likewise becomes approximately −0.031% for a long at the actual endpoint ask/bid. These examples distinguish visible movement from executable profit. No rule for choosing these intervals was established in advance.

USD/ZAR and EUR/ZAR have opening gaps of about +0.251% and +0.263% versus the last retained pre-open candles, but little or negative additional movement afterward. Opening spreads and asynchronous first quotes affect those comparisons.

## News and the information available at the time

- An oil/Hormuz/Saudi-pipeline risk headline was published at 6:22 p.m. and first retained locally at 6:37:54 p.m. It provides possible energy/inflation context before the later AUD/NZD decline. It already describes an oil gap, and its presence does not prove that it caused the FX moves.
- New Zealand's services PSI was **51.2 versus 50.6**. The [official BusinessNZ release](https://businessnz.org.nz/psi/three-on-the-trot) describes improved but fragile conditions. The selected article was published at 6:43:37 p.m. and first retained at 7:04:22 p.m.: **20 minutes 45 seconds later**. No contemporaneous consensus estimate is retained, so the release does not establish a negative surprise merely because NZD subsequently falls.
- AUD commentary discusses a stalled rally and inflation/rate concerns. The retained FXStreet story is a recap, not evidence of a new Sunday US inflation release.
- Explicitly negative New Zealand growth commentary was first retained at 9:58 p.m., after the sharp selloff. It cannot be assigned to those earlier moves as a locally available signal.
- The retained [ECB interview](https://www.ecb.europa.eu/press/inter/date/2026/html/ecb.in260912~3cc706f4d6.en.html) was published September 12. The BOJ item is routine balance-sheet accounts, not a rate decision. The latter's original official text is locally retained; fresh web retrieval failed.

The two collector populations overlap and must not be added. The canonical collector has 487 distinct IDs first retained during the window, including older publications and four inferred publication times. The native collector has 2,824 first-retained IDs, includes startup inventory, and has 1,883 inferred publication times. These counts are not counts of fresh, independent macroeconomic releases.

## Concrete interpretation gaps

Two isolated classification replays reproduce problems. The AUD/USD "rally stalls" framing is overwritten by a positive rally score. The NZ PSI article includes an embedded reference to an unrelated oil article, which supplies a six-currency risk-off vector; removing only that reference removes the vector. Both actual records were already withheld from directional publication and execution. Neither has been shown to cause an order.

Required follow-up is claim-level separation of article content and related links, explicit handling of stalled/reversing moves, and release surprise versus a contemporaneous expectation. Any classifier repair needs new source-bound, actually timed revisions; historical rows and knowledge times must remain intact. Price co-movement alone does not prove a news explanation or a useful trading edge.

## Reproduction and retained evidence

- [Complete 68-pair table and interval calculations](../../operational_repairs_20260913/first_five_hours_20260913/PRICE_REVIEW.md)
- [Independent endpoint/hash/count verification](../../operational_repairs_20260913/first_five_hours_20260913/PRICE_VERIFICATION.json)
- [News chronology, original IDs, hashes and collection clocks](../../operational_repairs_20260913/first_five_hours_20260913/news_audit.md)
- [Classifier replay and proposed correction boundaries](../../operational_repairs_20260913/first_five_hours_20260913/CLASSIFICATION_GAPS.md)
- [Price chronology chart](../../operational_repairs_20260913/first_five_hours_20260913/opening_session_moves.png)

The adjacent analysis scripts preserve bounded original CSV tails and their hashes. No historical price/news rows were edited, no missing bars were synthesized, and no forecasts were rescored for this review.
