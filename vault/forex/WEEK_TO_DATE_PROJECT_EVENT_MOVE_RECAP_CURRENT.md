# Frozen week-to-date Forex project, event, and movement recap

Compiled: `2026-09-04T21:05:30.179235+00:00`  
Frozen cutoff: `2026-09-04T21:00:00+00:00`  
Week: `2026-08-31T00:00:00-04:00` through the frozen cutoff.

> **Supported decision: `no_trade`.** This report is retrospective research only. It cannot place orders, promote a hypothesis, change policy, or authorize Practice 007.

## Executive result

- Official source items: **40**, collapsed to **24** independent five-minute currency/event clocks.
- Calendar-only clocks: **9**; these prove schedule coverage, not release observation.
- Official clocks with any post-hoc cost-clearing path: **22**.
- Strict timing/movement candidates: **4**; source direction aligned/opposed: **3/0**.
- Causally clocked pre-release consensus observations: **0**.
- Move history: **5088** physical rows, **5088** logical cases, **2001** factor episodes.
- Material episodes >= 15.0 bps: **130**; strict pre-move directional matches: **0**.
- Practice 007: balance/NAV **$41.6042/$41.6042**, cumulative P/L **$-8.343**, open trades/orders **0/0**.

A cost-clearing move after an official clock is not automatically a news forecast win. Pair, side, and horizon are selected after the outcome unless the strict source-direction and causal-timing fields say otherwise.

## All independent official event clocks

| # | Clock UTC | CCY | Source / event | Timing | A / causal C / P | Direction | Best post-hoc path | Factor | Gross | Spread | Net |
|---:|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | `2026-08-31T06:00:00Z` | DKK | denmark_statistics_releas… — Denmark Registered Unemployment | pre-known 10.7d; calendar_only_clock | — / — / — | source_direction_unresolved | USD_DKK short 120m | +6.20 bps | -78.2p | 14.9p | **+63.7p** |
| 2 | `2026-08-31T06:30:00Z` | HUF | hungary_ksh_industrial_pp… — Hungary industrial producer price annual change | +1306.0m; scheduled_release_clock | 1.0 / — / -0.4 | source_direction_unresolved | USD_HUF short 120m | +34.49 bps | -123.2p | 21.9p | **+103.0p** |
| 3 | `2026-08-31T08:30:00Z` | HKD | hong_kong_censtatd_releas… — Hong Kong Retail Sales | pre-known 14.2d; calendar_only_clock | — / — / — | source_direction_unresolved | HKD_JPY long 120m | +0.43 bps | +168.8p | 26.2p | **+143.8p** |
| 4 | `2026-09-01T08:30:00Z` | GBP | sp_global_uk_pmi_calendar — S&P Global UK Manufacturing PMI | pre-known 26.9d; calendar_only_clock | — / — / — | source_direction_unresolved | GBP_JPY long 120m | +5.10 bps | +14.1p | 3.2p | **+10.9p** |
| 5 | `2026-09-01T09:00:00Z` | EUR | eurostat_economy_finance — Euro area annual inflation up to 3.3% | +198s; scheduled_release_clock | 3.3 / — / 2.9 | source_direction_unresolved | EUR_ZAR short 60m | +0.00 bps | -289.2p | 102.6p | **+189.9p** |
| 6 | `2026-09-01T14:00:00Z` | USD | bls_major_timeseries_batc… — United States JOLTS Job Openings: actual 7.271 millio… | +9.3m; publisher_or_observatio… | 7.271 / — / 7.182 | source_direction_unresolved | EUR_USD short 120m | +6.61 bps | -8.1p | 2.2p | **+6.2p** |
| 7 | `2026-09-01T16:00:00Z` | EUR | ecb_press — Boris Vujčić: Household expectations and monetary pol… | +245s; publisher_or_observatio… | — / — / — | source_direction_aligned | EUR_ZAR long 15m | +0.65 bps | +125.9p | 90.5p | **+36.4p** |
| 8 | `2026-09-01T22:00:00Z` | PLN | nbp_policy_decision_calen… — Narodowy Bank Polski MPC Decision Release Window | pre-known 16.7d; calendar_only_clock | — / — / — | source_direction_unresolved | USD_PLN long 120m | -3.88 bps | +22.7p | 70.4p | **-23.0p** |
| 9 | `2026-09-01T22:45:00Z` | NZD | stats_nz_calendar — New Zealand Building consents issued: July 2026 | pre-known 1.2d; calendar_only_clock | — / — / — | source_direction_unresolved | NZD_USD long 60m | +1.65 bps | +1.1p | 1.3p | **-0.3p** |
| 10 | `2026-09-02T01:30:00Z` | JPY | boj_updates — Speech by Board Member TAKATA in Sapporo (Economic Ac… | +240s; publisher_or_observatio… | — / — / — | source_direction_aligned | NZD_JPY short 60m | +3.06 bps | -72.3p | 3.2p | **+69.2p** |
| 11 | `2026-09-02T02:00:00Z` | NZD | rbnz_policy_decision_cale… — Reserve Bank of New Zealand Monetary Policy Decision | pre-known 16.8d; scheduled_release_clock | — / — / — | source_direction_unresolved | AUD_NZD long 120m | -91.90 bps | +99.2p | 4.3p | **+95.8p** |
| 12 | `2026-09-02T08:00:00Z` | ZAR | sarb_publications_rss_dir… — Agenda and Minutes: Market Practitioners Group - 12 J… | +452.3m; publisher_or_observatio… | — / — / — | source_direction_unresolved | GBP_ZAR short 120m | +0.00 bps | -189.7p | 103.5p | **+84.2p** |
| 13 | `2026-09-02T08:30:00Z` | GBP | ons_published_releases — Geographical variation in the prevalence of young peo… | +10.6m; publisher_or_observatio… | — / — / — | source_direction_unresolved | GBP_CAD short 120m | -11.69 bps | -29.9p | 3.6p | **+26.4p** |
| 14 | `2026-09-02T13:45:00Z` | CAD | boc_policy_decision_calen… — Bank of Canada Monetary Policy Interest Rate Decision | pre-known 17.3d; scheduled_release_clock | — / — / — | source_direction_unresolved | EUR_CAD short 120m | +38.34 bps | -67.4p | 5.0p | **+63.5p** |
| 15 | `2026-09-02T14:00:00Z` | USD | census_economic_indicators — Manufacturers' Shipments, Inventories, and Orders | +148s; publisher_or_observatio… | 0.9 / — / -0.2 | source_direction_unresolved | USD_JPY long 60m | +9.74 bps | +22.9p | 2.0p | **+21.0p** |
| 16 | `2026-09-02T22:45:00Z` | NZD | stats_nz_calendar — New Zealand Livestock slaughtering statistics: July 2… | pre-known 2.2d; calendar_only_clock | — / — / — | source_direction_unresolved | NZD_JPY long 15m | +3.43 bps | +4.8p | 3.2p | **+1.6p** |
| 17 | `2026-09-03T08:30:00Z` | GBP | sp_global_uk_pmi_calendar — S&P Global UK Services and Composite PMI | pre-known 28.9d; calendar_only_clock | — / — / — | source_direction_unresolved | GBP_USD short 120m | -6.33 bps | -10.3p | 2.2p | **+8.3p** |
| 18 | `2026-09-03T10:30:12Z` | SEK | riksbank_speeches — Erik Thedéen in Visby on the economic situation and m… | +62s; publisher_or_observatio… | — / — / — | source_direction_unresolved | EUR_SEK short 120m | +14.90 bps | -58.5p | 29.0p | **+24.7p** |
| 19 | `2026-09-03T12:30:00Z` | USD | dol_eta_ui_claims_schedul… — United States Unemployment Insurance Weekly Claims | +248s; scheduled_release_clock | 206000.0 / — / 204000.0 | source_direction_unresolved | GBP_USD long 120m | -11.32 bps | +19.8p | 2.0p | **+17.9p** |
| 20 | `2026-09-03T14:00:00Z` | USD | ism_us_pmi_calendar — United States ISM Services PMI | pre-known 29.0d; calendar_only_clock | — / — / — | source_direction_unresolved | EUR_USD short 5m | +2.96 bps | -5.8p | 2.1p | **+4.0p** |
| 21 | `2026-09-03T22:45:00Z` | NZD | stats_nz_calendar — New Zealand Value of building work put in place: June… | pre-known 3.2d; calendar_only_clock | — / — / — | source_direction_unresolved | NZD_USD long 120m | +6.37 bps | +5.1p | 1.5p | **+3.6p** |
| 22 | `2026-09-04T09:10:00Z` | EUR | ecb_press — Philip R. Lane: Diversity at the European Central Bank | +6.9m; publisher_or_observatio… | — / — / — | source_direction_unresolved | EUR_SEK long 15m | +0.00 bps | +59.5p | 30.0p | **+28.0p** |
| 23 | `2026-09-04T12:30:00Z` | CAD | statcan_major_indicators_… — Labour Force Survey - August 2026 | +285.2m; publisher_or_observatio… | -0.2 / — / — | source_direction_unresolved | USD_CAD long 15m | -28.28 bps | +75.0p | 4.7p | **+71.8p** |
| 24 | `2026-09-04T12:30:00Z` | USD | bls_employment_situation_… — Average Hourly Earnings Monthly Change - August 2026 | +156.5m; publisher_or_observatio… | 0.3 / — / — | source_direction_aligned | USD_CAD long 5m | +31.87 bps | +74.0p | 4.7p | **+70.8p** |

The companion JSON retains all fixed 5/15/60/120-minute responses and explicitly labels horizons that had not matured at the cutoff.

## Factor-deduplicated movement census

Magnitude counts: >=5 bps **1353**, >=10 **422**, >=15 **130**, >=20 **59**, >=25 **34**.

Broad-context alignment: **884 aligned / 905 opposed / 194 neutral-conflicted / 18 no source**. Strict alignment: **0 aligned / 0 opposed / 1983 neutral-conflicted / 18 no source**.

### Complete >= 15.0 bps set at the cutoff

| # | Factor | Pair | Start -> end UTC | Move | Gross | Cost | Net | Broad / strict | Trend / breakout / exhaustion | Closest broad source |
|---:|---|---|---|---:|---:|---:|---:|---|---|---|
| 1 | JPY+ | TRY_JPY | `2026-09-04T12:30:00+00:00` -> `2026-09-04T12:52:00+00:00` | +78.85 bps | +2.5p | 1.1p | **+1.4p** | aligned / neutral_or_conflicted | aligned / aligned / none | How have interest rate expectations changed after this week… |
| 2 | HUF+ | EUR_HUF | `2026-09-03T14:26:00+00:00` -> `2026-09-03T14:34:00+00:00` | +76.57 bps | +280.8p | 25.0p | **+255.8p** | neutral_or_conflicted / neutral_or_conflicted | aligned / inside / none | — |
| 3 | JPY+ | HKD_JPY | `2026-09-04T12:47:00+00:00` -> `2026-09-04T12:52:00+00:00` | +67.23 bps | +1341.5p | 34.8p | **+1306.7p** | aligned / neutral_or_conflicted | aligned / inside / none | How have interest rate expectations changed after this week… |
| 4 | NZD- | NZD_CHF | `2026-09-02T01:58:00+00:00` -> `2026-09-02T02:04:00+00:00` | +63.75 bps | +30.6p | 4.0p | **+26.5p** | aligned / neutral_or_conflicted | aligned / inside / none | Department of War Invests $22.1 Million to Expand Critical … |
| 5 | JPY- | TRY_JPY | `2026-09-03T07:09:00+00:00` -> `2026-09-03T07:36:00+00:00` | +55.79 bps | +1.8p | 1.1p | **+0.7p** | opposed / neutral_or_conflicted | opposed / inside / lower_extreme | Iraq Oil Exports Jump 73% On Hormuz Nod, PetroChina And Rel… |
| 6 | USD+ | USD_CAD | `2026-09-04T12:29:00+00:00` -> `2026-09-04T12:34:00+00:00` | +53.21 bps | +73.4p | 2.9p | **+70.5p** | aligned / neutral_or_conflicted | opposed / inside / none | US Fed's Waller says August inflation data will determine s… |
| 7 | HKD+ | CAD_HKD | `2026-09-04T12:29:00+00:00` -> `2026-09-04T12:34:00+00:00` | +52.62 bps | +299.1p | 13.0p | **+286.1p** | opposed / neutral_or_conflicted | opposed / inside / none | How have interest rate expectations changed after this week… |
| 8 | JPY+ | EUR_JPY | `2026-09-02T13:18:00+00:00` -> `2026-09-02T13:27:00+00:00` | +48.29 bps | +89.2p | 2.6p | **+86.6p** | aligned / neutral_or_conflicted | aligned / inside / none | European Central Bank’s Nagel backs September rate hike as … |
| 9 | JPY+ | ZAR_JPY | `2026-09-03T08:06:00+00:00` -> `2026-09-03T08:55:00+00:00` | +45.98 bps | +4.5p | 2.2p | **+2.3p** | aligned / neutral_or_conflicted | aligned / inside / none | Kitchener revives trade task force as tariff conflict with … |
| 10 | USD- | USD_THB | `2026-09-02T12:36:00+00:00` -> `2026-09-02T13:27:00+00:00` | +38.95 bps | +12.9p | 2.1p | **+10.8p** | opposed / neutral_or_conflicted | opposed / inside / none | 10-year Treasury yield hits highest level since late 2023 a… |
| 11 | JPY- | ZAR_JPY | `2026-09-04T00:05:00+00:00` -> `2026-09-04T00:32:00+00:00` | +37.59 bps | +3.6p | 2.2p | **+1.4p** | opposed / neutral_or_conflicted | opposed / inside / lower_extreme | Department of War Secures Unmodified SOC-1 Type 2 Opinion f… |
| 12 | HUF+ | USD_HUF | `2026-09-02T06:50:00+00:00` -> `2026-09-02T06:55:00+00:00` | +37.42 bps | +120.1p | 25.0p | **+95.1p** | opposed / neutral_or_conflicted | opposed / inside / none | South Korean shares fall more than 3% as Iran war escalates… |
| 13 | JPY- | ZAR_JPY | `2026-09-04T12:52:00+00:00` -> `2026-09-04T13:06:00+00:00` | +36.55 bps | +3.5p | 2.2p | **+1.3p** | opposed / neutral_or_conflicted | opposed / inside / none | How have interest rate expectations changed after this week… |
| 14 | ZAR+ | ZAR_JPY | `2026-09-02T06:52:00+00:00` -> `2026-09-02T07:14:00+00:00` | +34.52 bps | +3.4p | 2.2p | **+1.2p** | aligned / neutral_or_conflicted | opposed / opposed / lower_extreme | Bank of Japan Hawk Calls for Flexible Rate Hikes, Fueling S… |
| 15 | HUF- | EUR_HUF | `2026-09-04T12:26:00+00:00` -> `2026-09-04T12:31:00+00:00` | +33.37 bps | +121.0p | 24.9p | **+96.1p** | aligned / neutral_or_conflicted | opposed / inside / none | How have interest rate expectations changed after this week… |
| 16 | JPY- | NZD_JPY | `2026-09-03T07:35:00+00:00` -> `2026-09-03T07:40:00+00:00` | +32.88 bps | +30.2p | 4.1p | **+26.1p** | opposed / neutral_or_conflicted | aligned / inside / none | Kitchener revives trade task force as tariff conflict with … |
| 17 | JPY- | NZD_JPY | `2026-09-04T13:41:00+00:00` -> `2026-09-04T14:10:00+00:00` | +30.42 bps | +27.9p | 3.5p | **+24.4p** | opposed / neutral_or_conflicted | aligned / inside / none | USD Technical Analysis:What levels are in play for the EURU… |
| 18 | NZD- | NZD_JPY | `2026-09-04T12:27:00+00:00` -> `2026-09-04T12:38:00+00:00` | +30.33 bps | +27.9p | 3.7p | **+24.2p** | aligned / neutral_or_conflicted | opposed / inside / none | Risk-Off Wave Sweeps Global Markets: Investors Add Gold for… |
| 19 | USD- | NZD_USD | `2026-09-04T12:57:00+00:00` -> `2026-09-04T13:30:00+00:00` | +30.25 bps | +17.8p | 2.4p | **+15.4p** | opposed / neutral_or_conflicted | aligned / inside / none | Business News / US Fed's Waller Says August Inflation Data … |
| 20 | HUF- | USD_HUF | `2026-09-03T08:09:00+00:00` -> `2026-09-03T08:14:00+00:00` | +29.44 bps | +92.8p | 23.1p | **+69.7p** | aligned / neutral_or_conflicted | opposed / inside / none | What are the main events for today? |
| 21 | HUF+ | USD_HUF | `2026-09-03T07:55:00+00:00` -> `2026-09-03T08:05:00+00:00` | +28.92 bps | +91.7p | 19.8p | **+71.9p** | opposed / neutral_or_conflicted | opposed / inside / none | What are the main events for today? |
| 22 | CHF+ | AUD_CHF | `2026-09-03T05:58:00+00:00` -> `2026-09-03T06:33:00+00:00` | +28.73 bps | +16.7p | 2.0p | **+14.7p** | aligned / neutral_or_conflicted | opposed / inside / none | US-Iran strikes raise fears of renewed war across the Middl… |
| 23 | HUF+ | USD_HUF | `2026-09-03T12:01:00+00:00` -> `2026-09-03T12:32:00+00:00` | +28.65 bps | +90.7p | 20.3p | **+70.4p** | opposed / neutral_or_conflicted | aligned / inside / none | Fed rate-hike risks mount as Vietnamese market braces for v… |
| 24 | CHF- | SGD_CHF | `2026-09-04T12:27:00+00:00` -> `2026-09-04T12:33:00+00:00` | +27.97 bps | +17.9p | 1.6p | **+16.2p** | opposed / neutral_or_conflicted | opposed / inside / none | How have interest rate expectations changed after this week… |
| 25 | JPY- | HKD_JPY | `2026-09-02T13:27:00+00:00` -> `2026-09-02T13:32:00+00:00` | +27.60 bps | +558.0p | 42.1p | **+515.8p** | opposed / neutral_or_conflicted | opposed / inside / none | Iraqi oil shipments surge by 70% in August - IraqiNews |
| 26 | HUF+ | USD_HUF | `2026-09-02T07:06:00+00:00` -> `2026-09-02T07:11:00+00:00` | +27.50 bps | +88.1p | 21.7p | **+66.4p** | opposed / neutral_or_conflicted | aligned / inside / none | South Korean shares fall more than 3% as Iran war escalates… |
| 27 | CAD- | CAD_SGD | `2026-09-04T12:03:00+00:00` -> `2026-09-04T12:31:00+00:00` | +27.44 bps | +25.2p | 2.4p | **+22.8p** | opposed / neutral_or_conflicted | opposed / opposed / none | How have interest rate expectations changed after this week… |
| 28 | GBP+ | GBP_HKD | `2026-09-03T14:42:00+00:00` -> `2026-09-03T15:08:00+00:00` | +26.15 bps | +276.8p | 17.2p | **+259.5p** | neutral_or_conflicted / neutral_or_conflicted | opposed / inside / lower_extreme | — |
| 29 | HKD- | NZD_HKD | `2026-09-03T12:27:00+00:00` -> `2026-09-03T12:32:00+00:00` | +25.93 bps | +119.2p | 13.0p | **+106.2p** | opposed / neutral_or_conflicted | opposed / inside / lower_extreme | Iran fires missiles and drones at Kuwait as strikes resume … |
| 30 | JPY+ | HKD_JPY | `2026-09-03T07:04:00+00:00` -> `2026-09-03T07:09:00+00:00` | +25.86 bps | +517.2p | 30.2p | **+487.0p** | neutral_or_conflicted / neutral_or_conflicted | aligned / inside / none | Iraq Oil Exports Jump 73% On Hormuz Nod, PetroChina And Rel… |
| 31 | USD- | USD_HUF | `2026-09-03T23:28:00+00:00` -> `2026-09-04T00:06:00+00:00` | +25.79 bps | +80.4p | 44.7p | **+35.7p** | opposed / neutral_or_conflicted | opposed / inside / none | Waller vs. Warsh: The Fed's Communication Debate Over Rate … |
| 32 | ZAR+ | GBP_ZAR | `2026-09-04T13:07:00+00:00` -> `2026-09-04T13:31:00+00:00` | +25.65 bps | +554.2p | 114.7p | **+439.6p** | opposed / neutral_or_conflicted | opposed / inside / none | Brits brace for pain as BoE chief economist urges interest … |
| 33 | NZD+ | EUR_NZD | `2026-09-03T14:40:00+00:00` -> `2026-09-03T15:19:00+00:00` | +25.53 bps | +50.5p | 6.3p | **+44.2p** | neutral_or_conflicted / neutral_or_conflicted | opposed / inside / none | — |
| 34 | USD- | NZD_USD | `2026-09-03T12:27:00+00:00` -> `2026-09-03T12:32:00+00:00` | +25.41 bps | +14.9p | 2.3p | **+12.6p** | opposed / neutral_or_conflicted | opposed / inside / lower_extreme | Fed rate-hike risks mount as Vietnamese market braces for v… |
| 35 | ZAR+ | USD_ZAR | `2026-09-04T13:39:00+00:00` -> `2026-09-04T13:47:00+00:00` | +24.40 bps | +389.5p | 73.7p | **+315.8p** | opposed / neutral_or_conflicted | aligned / inside / none | Oil price shock raises prospect of a SARB interest rate hik… |
| 36 | THB+ | USD_THB | `2026-09-03T01:54:00+00:00` -> `2026-09-03T02:30:00+00:00` | +24.13 bps | +8.0p | 1.9p | **+6.1p** | opposed / neutral_or_conflicted | opposed / inside / none | Trump wants lower rates. That won’t stop a Fed rate hike. -… |
| 37 | CHF+ | USD_CHF | `2026-09-03T15:42:00+00:00` -> `2026-09-03T16:30:00+00:00` | +24.08 bps | +19.4p | 1.6p | **+17.8p** | opposed / neutral_or_conflicted | opposed / inside / none | Fed Governor Waller’s Support for Rate Hike Depends on Upco… |
| 38 | NZD- | NZD_HKD | `2026-09-02T02:14:00+00:00` -> `2026-09-02T02:22:00+00:00` | +23.55 bps | +108.2p | 16.2p | **+92.0p** | aligned / neutral_or_conflicted | aligned / inside / none | Department of War Invests $22.1 Million to Expand Critical … |
| 39 | SEK+ | USD_SEK | `2026-09-03T12:36:00+00:00` -> `2026-09-03T12:54:00+00:00` | +22.84 bps | +218.6p | 34.2p | **+184.4p** | opposed / neutral_or_conflicted | aligned / inside / none | Waller, The Economic Outlook and Some Comments on My Policy… |
| 40 | HUF+ | EUR_HUF | `2026-09-04T14:52:00+00:00` -> `2026-09-04T15:31:00+00:00` | +22.71 bps | +82.5p | 20.8p | **+61.7p** | neutral_or_conflicted / neutral_or_conflicted | opposed / inside / none | — |
| 41 | ZAR- | EUR_ZAR | `2026-09-01T15:45:00+00:00` -> `2026-09-01T16:24:00+00:00` | +22.49 bps | +419.8p | 90.6p | **+329.2p** | aligned / neutral_or_conflicted | opposed / inside / none | Eurozone Inflation Rises in August and Adds Pressure on ECB… |
| 42 | HUF- | EUR_HUF | `2026-09-02T07:45:00+00:00` -> `2026-09-02T07:51:00+00:00` | +22.28 bps | +82.2p | 20.8p | **+61.4p** | aligned / neutral_or_conflicted | opposed / inside / lower_extreme | ECB rate hike looms as energy shock pushes inflation to 3.3… |
| 43 | ZAR+ | CHF_ZAR | `2026-09-03T08:55:00+00:00` -> `2026-09-03T09:22:00+00:00` | +21.79 bps | +432.9p | 100.6p | **+332.3p** | opposed / neutral_or_conflicted | opposed / inside / none | Swiss inflation nudges up in August amid higher energy pric… |
| 44 | NZD+ | GBP_NZD | `2026-09-01T12:06:00+00:00` -> `2026-09-01T13:00:00+00:00` | +21.74 bps | +50.0p | 9.2p | **+40.8p** | opposed / neutral_or_conflicted | opposed / inside / none | Watch Oil Tankers Struck in Strait of Hormuz as Iran War Es… |
| 45 | HUF- | USD_HUF | `2026-09-02T13:27:00+00:00` -> `2026-09-02T13:32:00+00:00` | +21.51 bps | +68.1p | 22.4p | **+45.7p** | aligned / neutral_or_conflicted | opposed / inside / none | 10-year Treasury yield hits highest level since late 2023 a… |
| 46 | HUF- | EUR_HUF | `2026-09-03T09:29:00+00:00` -> `2026-09-03T10:04:00+00:00` | +21.45 bps | +78.6p | 20.4p | **+58.2p** | neutral_or_conflicted / neutral_or_conflicted | opposed / inside / lower_extreme | — |
| 47 | HKD- | NZD_HKD | `2026-09-04T12:46:00+00:00` -> `2026-09-04T12:52:00+00:00` | +21.32 bps | +98.0p | 13.8p | **+84.2p** | opposed / neutral_or_conflicted | opposed / inside / none | How have interest rate expectations changed after this week… |
| 48 | CAD+ | CAD_JPY | `2026-09-02T13:44:00+00:00` -> `2026-09-02T13:51:00+00:00` | +21.04 bps | +24.0p | 4.6p | **+19.4p** | opposed / neutral_or_conflicted | aligned / inside / none | Venezuelan Oil Dealmaking Surges as New Energy Barons Capit… |
| 49 | USD- | NZD_USD | `2026-09-03T14:41:00+00:00` -> `2026-09-03T15:02:00+00:00` | +20.97 bps | +12.3p | 2.4p | **+9.9p** | neutral_or_conflicted / neutral_or_conflicted | opposed / inside / none | — |
| 50 | HUF- | USD_HUF | `2026-09-03T15:12:00+00:00` -> `2026-09-03T15:18:00+00:00` | +20.96 bps | +65.3p | 28.1p | **+37.2p** | aligned / neutral_or_conflicted | opposed / inside / lower_extreme | Fed Governor Waller’s Support for Rate Hike Depends on Upco… |
| 51 | HUF+ | USD_HUF | `2026-09-02T12:10:00+00:00` -> `2026-09-02T12:21:00+00:00` | +20.91 bps | +66.5p | 20.3p | **+46.2p** | opposed / neutral_or_conflicted | opposed / inside / none | 10-year Treasury yield hits highest level since late 2023 a… |
| 52 | HUF+ | USD_HUF | `2026-09-04T13:10:00+00:00` -> `2026-09-04T13:16:00+00:00` | +20.77 bps | +65.1p | 19.9p | **+45.2p** | opposed / neutral_or_conflicted | opposed / inside / none | Business News / US Fed's Waller Says August Inflation Data … |
| 53 | USD+ | USD_HUF | `2026-09-01T07:15:00+00:00` -> `2026-09-01T08:07:00+00:00` | +20.73 bps | +65.2p | 18.6p | **+46.7p** | aligned / neutral_or_conflicted | opposed / inside / none | EXCLUSIVE: September Fed Rate Hike? Louis Navellier Says In… |
| 54 | HUF+ | EUR_HUF | `2026-09-04T00:00:00+00:00` -> `2026-09-04T00:56:00+00:00` | +20.44 bps | +74.0p | 35.0p | **+39.0p** | neutral_or_conflicted / neutral_or_conflicted | opposed / inside / none | — |
| 55 | USD+ | USD_THB | `2026-09-04T13:47:00+00:00` -> `2026-09-04T14:04:00+00:00` | +20.39 bps | +6.7p | 2.0p | **+4.7p** | aligned / neutral_or_conflicted | opposed / inside / none | US Fed's Waller says August inflation data will determine s… |
| 56 | NZD+ | NZD_JPY | `2026-09-02T21:29:00+00:00` -> `2026-09-02T22:19:00+00:00` | +20.34 bps | +18.9p | 12.6p | **+6.3p** | opposed / neutral_or_conflicted | aligned / inside / none | Readout of Secretary of War Pete Hegseth's Meeting With Aus… |
| 57 | HUF- | USD_HUF | `2026-09-02T09:06:00+00:00` -> `2026-09-02T09:24:00+00:00` | +20.12 bps | +64.2p | 18.6p | **+45.5p** | aligned / neutral_or_conflicted | opposed / inside / none | Germany Says Russia Behind Leipzig Airport Drone Attack, An… |
| 58 | HUF+ | USD_HUF | `2026-09-04T12:31:00+00:00` -> `2026-09-04T12:36:00+00:00` | +20.09 bps | +63.1p | 20.7p | **+42.4p** | opposed / neutral_or_conflicted | aligned / inside / none | How have interest rate expectations changed after this week… |
| 59 | USD- | USD_HUF | `2026-09-02T05:01:00+00:00` -> `2026-09-02T05:33:00+00:00` | +20.05 bps | +63.9p | 25.9p | **+38.0p** | opposed / neutral_or_conflicted | opposed / inside / none | US-Iran war escalates as fresh strikes hit Iran: Does peace… |
| 60 | HUF- | EUR_HUF | `2026-09-02T05:52:00+00:00` -> `2026-09-02T06:05:00+00:00` | +19.83 bps | +73.0p | 27.6p | **+45.4p** | aligned / neutral_or_conflicted | opposed / inside / none | ECB rate hike looms as energy shock pushes inflation to 3.3… |
| 61 | JPY+ | CAD_JPY | `2026-08-31T06:37:00+00:00` -> `2026-08-31T07:16:00+00:00` | +19.42 bps | +22.4p | 3.6p | **+18.7p** | aligned / neutral_or_conflicted | opposed / inside / none | FOREX-Dollar near two-week high as Warsh boosts rate-hike b… |
| 62 | CAD- | GBP_CAD | `2026-09-04T12:25:00+00:00` -> `2026-09-04T12:31:00+00:00` | +19.42 bps | +36.2p | 5.5p | **+30.7p** | aligned / neutral_or_conflicted | opposed / inside / lower_extreme | Brits brace for pain as BoE chief economist urges interest … |
| 63 | CAD+ | CAD_JPY | `2026-09-02T14:42:00+00:00` -> `2026-09-02T14:47:00+00:00` | +19.42 bps | +22.2p | 3.6p | **+18.6p** | opposed / neutral_or_conflicted | aligned / inside / none | Bessent Leaves BOJ in No-Win Situation Over Japan Rate Hike… |
| 64 | AUD+ | AUD_CHF | `2026-09-02T00:41:00+00:00` -> `2026-09-02T01:33:00+00:00` | +19.22 bps | +11.2p | 1.9p | **+9.2p** | opposed / neutral_or_conflicted | opposed / inside / none | ‘Stop doing memes’ and ‘start being serious’: Canada’s Carn… |
| 65 | NOK- | USD_NOK | `2026-09-04T12:39:00+00:00` -> `2026-09-04T12:46:00+00:00` | +19.02 bps | +177.0p | 33.5p | **+143.5p** | aligned / neutral_or_conflicted | aligned / inside / none | How have interest rate expectations changed after this week… |
| 66 | ZAR+ | GBP_ZAR | `2026-09-02T12:14:00+00:00` -> `2026-09-02T12:21:00+00:00` | +18.98 bps | +412.1p | 121.2p | **+290.9p** | opposed / neutral_or_conflicted | opposed / inside / none | US-Iran War To Escalate? Two Oil Tankers Disabled After Hit… |
| 67 | NZD- | NZD_CHF | `2026-09-02T02:56:00+00:00` -> `2026-09-02T03:14:00+00:00` | +18.90 bps | +9.0p | 4.0p | **+5.0p** | aligned / neutral_or_conflicted | opposed / inside / none | New Zealand Delivers Back-to-Back Rate Hikes to Curb Inflat… |
| 68 | ZAR+ | EUR_ZAR | `2026-09-04T14:53:00+00:00` -> `2026-09-04T15:23:00+00:00` | +18.76 bps | +347.9p | 88.0p | **+260.0p** | opposed / neutral_or_conflicted | opposed / inside / none | 2 Filipino seafarers killed after oil tanker attacked near … |
| 69 | HUF+ | USD_HUF | `2026-09-02T09:01:00+00:00` -> `2026-09-02T09:06:00+00:00` | +18.72 bps | +59.8p | 20.1p | **+39.7p** | opposed / neutral_or_conflicted | opposed / inside / upper_extreme | Germany Says Russia Behind Leipzig Airport Drone Attack, An… |
| 70 | AUD- | AUD_CAD | `2026-09-01T08:14:00+00:00` -> `2026-09-01T08:58:00+00:00` | +18.69 bps | +18.6p | 2.6p | **+15.9p** | aligned / neutral_or_conflicted | opposed / inside / none | Threat of RBA rate hikes looms as economy nears speed limit… |
| 71 | CHF- | AUD_CHF | `2026-09-03T08:55:00+00:00` -> `2026-09-03T09:25:00+00:00` | +18.68 bps | +10.8p | 1.9p | **+9.0p** | opposed / neutral_or_conflicted | opposed / inside / none | Swiss inflation nudges up in August amid higher energy pric… |
| 72 | USD- | USD_HUF | `2026-09-03T15:42:00+00:00` -> `2026-09-03T16:04:00+00:00` | +18.57 bps | +58.0p | 30.4p | **+27.7p** | opposed / neutral_or_conflicted | opposed / inside / none | Fed Governor Waller’s Support for Rate Hike Depends on Upco… |
| 73 | CAD- | NZD_CAD | `2026-09-02T12:58:00+00:00` -> `2026-09-02T13:27:00+00:00` | +18.46 bps | +14.9p | 4.5p | **+10.5p** | opposed / neutral_or_conflicted | opposed / inside / lower_extreme | Iraq's oil exports rose to around 2.34 million bpd in Augus… |
| 74 | ZAR+ | CHF_ZAR | `2026-09-02T09:39:00+00:00` -> `2026-09-02T10:25:00+00:00` | +18.27 bps | +362.5p | 101.2p | **+261.3p** | opposed / neutral_or_conflicted | opposed / inside / none | Korean stocks sink 4% as oil, bond yields deepen risk-off r… |
| 75 | NZD+ | NZD_SGD | `2026-09-03T12:27:00+00:00` -> `2026-09-03T12:32:00+00:00` | +18.16 bps | +13.5p | 2.4p | **+11.1p** | opposed / neutral_or_conflicted | opposed / inside / lower_extreme | Iran fires missiles and drones at Kuwait as strikes resume … |
| 76 | JPY+ | GBP_JPY | `2026-09-02T13:33:00+00:00` -> `2026-09-02T13:39:00+00:00` | +18.13 bps | +38.9p | 3.4p | **+35.5p** | aligned / neutral_or_conflicted | aligned / inside / none | Venezuelan Oil Dealmaking Surges as New Energy Barons Capit… |
| 77 | HUF+ | USD_HUF | `2026-09-02T10:39:00+00:00` -> `2026-09-02T11:01:00+00:00` | +18.04 bps | +57.6p | 20.4p | **+37.3p** | opposed / neutral_or_conflicted | aligned / inside / none | Korean stocks sink 4% as oil, bond yields deepen risk-off r… |
| 78 | NOK- | USD_NOK | `2026-09-03T11:59:00+00:00` -> `2026-09-03T12:23:00+00:00` | +17.61 bps | +163.8p | 31.2p | **+132.5p** | aligned / neutral_or_conflicted | opposed / inside / lower_extreme | Fed rate-hike risks mount as Vietnamese market braces for v… |
| 79 | THB+ | USD_THB | `2026-09-03T06:25:00+00:00` -> `2026-09-03T07:03:00+00:00` | +17.57 bps | +5.8p | 1.9p | **+3.9p** | neutral_or_conflicted / neutral_or_conflicted | opposed / inside / none | — |
| 80 | USD- | USD_SEK | `2026-09-04T13:39:00+00:00` -> `2026-09-04T13:47:00+00:00` | +17.50 bps | +167.3p | 29.7p | **+137.6p** | opposed / neutral_or_conflicted | aligned / inside / none | US Fed's Waller says August inflation data will determine s… |
| 81 | ZAR- | USD_ZAR | `2026-09-04T13:47:00+00:00` -> `2026-09-04T13:53:00+00:00` | +17.44 bps | +277.8p | 72.2p | **+205.6p** | aligned / neutral_or_conflicted | opposed / inside / none | Oil price shock raises prospect of a SARB interest rate hik… |
| 82 | HUF- | USD_HUF | `2026-09-04T07:50:00+00:00` -> `2026-09-04T07:56:00+00:00` | +17.40 bps | +54.2p | 19.2p | **+35.0p** | aligned / neutral_or_conflicted | opposed / inside / none | US pressure on Iran starting to tell, as sanctions and bloc… |
| 83 | GBP- | GBP_ZAR | `2026-09-04T12:30:00+00:00` -> `2026-09-04T12:39:00+00:00` | +17.33 bps | +374.9p | 173.3p | **+201.6p** | opposed / neutral_or_conflicted | aligned / inside / none | How have interest rate expectations changed after this week… |
| 84 | ZAR+ | GBP_ZAR | `2026-09-03T07:51:00+00:00` -> `2026-09-03T08:05:00+00:00` | +17.31 bps | +375.6p | 108.2p | **+267.5p** | opposed / neutral_or_conflicted | opposed / inside / upper_extreme | What are the main events for today? |
| 85 | HUF+ | USD_HUF | `2026-09-01T04:42:00+00:00` -> `2026-09-01T05:40:00+00:00` | +17.31 bps | +54.5p | 28.5p | **+26.0p** | opposed / neutral_or_conflicted | opposed / inside / none | Iran war latest: Tanker attacked near Oman as it exits Stra… |
| 86 | JPY+ | CAD_JPY | `2026-09-03T08:02:00+00:00` -> `2026-09-03T08:09:00+00:00` | +17.14 bps | +19.5p | 3.9p | **+15.6p** | aligned / neutral_or_conflicted | opposed / inside / none | Kitchener revives trade task force as tariff conflict with … |
| 87 | CAD- | NZD_CAD | `2026-09-04T12:47:00+00:00` -> `2026-09-04T12:52:00+00:00` | +17.10 bps | +13.9p | 4.7p | **+9.2p** | opposed / neutral_or_conflicted | aligned / inside / none | USD Technical Analysis:What levels are in play for the EURU… |
| 88 | HUF+ | USD_HUF | `2026-09-02T13:38:00+00:00` -> `2026-09-02T13:43:00+00:00` | +17.07 bps | +54.2p | 29.9p | **+24.3p** | opposed / neutral_or_conflicted | opposed / inside / none | 10-year Treasury yield hits highest level since late 2023 a… |
| 89 | THB- | USD_THB | `2026-08-31T08:02:00+00:00` -> `2026-08-31T08:56:00+00:00` | +16.93 bps | +5.6p | 1.9p | **+3.7p** | aligned / neutral_or_conflicted | opposed / opposed / lower_extreme | Market Again Allows Fed Rate Hike: Euro Reaction - Курс Укр… |
| 90 | THB- | USD_THB | `2026-09-02T13:27:00+00:00` -> `2026-09-02T13:34:00+00:00` | +16.91 bps | +5.6p | 2.2p | **+3.4p** | aligned / neutral_or_conflicted | opposed / inside / none | 10-year Treasury yield hits highest level since late 2023 a… |
| 91 | JPY- | GBP_JPY | `2026-09-03T15:01:00+00:00` -> `2026-09-03T15:06:00+00:00` | +16.90 bps | +35.5p | 3.3p | **+32.2p** | neutral_or_conflicted / neutral_or_conflicted | opposed / inside / none | — |
| 92 | USD- | USD_PLN | `2026-09-03T07:51:00+00:00` -> `2026-09-03T08:05:00+00:00` | +16.88 bps | +63.0p | 14.4p | **+48.6p** | opposed / neutral_or_conflicted | opposed / inside / none | What are the main events for today? |
| 93 | JPY+ | CAD_JPY | `2026-09-03T13:12:00+00:00` -> `2026-09-03T13:18:00+00:00` | +16.78 bps | +18.9p | 3.6p | **+15.3p** | aligned / neutral_or_conflicted | opposed / inside / none | Iran tests Trump with Gulf strikes as US touts Hormuz oil s… |
| 94 | USD- | USD_ZAR | `2026-09-03T06:14:00+00:00` -> `2026-09-03T06:35:00+00:00` | +16.66 bps | +267.5p | 76.1p | **+191.4p** | neutral_or_conflicted / neutral_or_conflicted | opposed / inside / none | — |
| 95 | JPY+ | GBP_JPY | `2026-09-02T05:24:00+00:00` -> `2026-09-02T06:00:00+00:00` | +16.61 bps | +35.9p | 3.3p | **+32.6p** | aligned / neutral_or_conflicted | opposed / inside / none | BOJ's Takata urges nimble rate hikes to counter inflation p… |
| 96 | NOK+ | EUR_NOK | `2026-08-31T08:01:00+00:00` -> `2026-08-31T08:08:00+00:00` | +16.56 bps | +179.9p | 34.2p | **+145.7p** | opposed / neutral_or_conflicted | aligned / inside / none | What are the main events for today? |
| 97 | ZAR+ | USD_ZAR | `2026-09-02T13:39:00+00:00` -> `2026-09-02T13:44:00+00:00` | +16.52 bps | +265.4p | 66.7p | **+198.7p** | opposed / neutral_or_conflicted | aligned / inside / none | 10-year Treasury yield hits highest level since late 2023 a… |
| 98 | USD- | USD_CHF | `2026-09-03T12:42:00+00:00` -> `2026-09-03T12:54:00+00:00` | +16.52 bps | +13.3p | 1.6p | **+11.8p** | opposed / neutral_or_conflicted | aligned / inside / none | Waller, The Economic Outlook and Some Comments on My Policy… |
| 99 | HUF- | EUR_HUF | `2026-09-04T10:17:00+00:00` -> `2026-09-04T11:03:00+00:00` | +16.50 bps | +59.9p | 20.4p | **+39.4p** | opposed / neutral_or_conflicted | opposed / inside / none | Eurozone retail sales fall in July as consumer spending sof… |
| 100 | ZAR- | USD_ZAR | `2026-09-04T06:00:00+00:00` -> `2026-09-04T06:50:00+00:00` | +16.43 bps | +262.4p | 72.1p | **+190.3p** | aligned / neutral_or_conflicted | opposed / inside / none | What are the main events for today? |
| 101 | HUF+ | USD_HUF | `2026-09-03T14:42:00+00:00` -> `2026-09-03T14:47:00+00:00` | +16.43 bps | +51.5p | 28.4p | **+23.1p** | neutral_or_conflicted / neutral_or_conflicted | aligned / inside / none | — |
| 102 | ZAR+ | EUR_ZAR | `2026-09-01T13:13:00+00:00` -> `2026-09-01T13:39:00+00:00` | +16.42 bps | +307.4p | 92.5p | **+214.9p** | opposed / neutral_or_conflicted | opposed / inside / none | ECB Rate Hike Looms as Euro Zone Inflation Surges to 3.3% o… |
| 103 | HUF- | USD_HUF | `2026-09-03T14:01:00+00:00` -> `2026-09-03T14:10:00+00:00` | +16.40 bps | +51.6p | 19.4p | **+32.3p** | aligned / neutral_or_conflicted | opposed / inside / none | Waller, The Economic Outlook and Some Comments on My Policy… |
| 104 | HUF+ | USD_HUF | `2026-09-03T20:04:00+00:00` -> `2026-09-03T20:39:00+00:00` | +16.22 bps | +50.5p | 33.5p | **+17.0p** | opposed / neutral_or_conflicted | opposed / inside / none | Trump Wants Lower Rates. That Won’t Stop a Fed Rate Hike. -… |
| 105 | CHF+ | USD_CHF | `2026-09-03T06:59:00+00:00` -> `2026-09-03T07:05:00+00:00` | +16.17 bps | +13.1p | 1.7p | **+11.4p** | neutral_or_conflicted / neutral_or_conflicted | opposed / inside / none | — |
| 106 | HUF+ | USD_HUF | `2026-09-04T07:00:00+00:00` -> `2026-09-04T07:05:00+00:00` | +16.16 bps | +50.5p | 19.6p | **+30.8p** | opposed / neutral_or_conflicted | opposed / inside / none | What are the main events for today? |
| 107 | SEK- | EUR_SEK | `2026-09-04T12:28:00+00:00` -> `2026-09-04T12:33:00+00:00` | +16.14 bps | +179.2p | 37.3p | **+141.9p** | aligned / neutral_or_conflicted | opposed / inside / lower_extreme | How have interest rate expectations changed after this week… |
| 108 | ZAR- | USD_ZAR | `2026-09-03T04:41:00+00:00` -> `2026-09-03T05:23:00+00:00` | +16.06 bps | +257.4p | 80.7p | **+176.7p** | aligned / neutral_or_conflicted | opposed / inside / lower_extreme | US-Iran strikes raise fears of renewed war across the Middl… |
| 109 | USD+ | USD_HUF | `2026-09-02T08:44:00+00:00` -> `2026-09-02T08:57:00+00:00` | +16.03 bps | +51.1p | 20.2p | **+30.9p** | aligned / neutral_or_conflicted | opposed / inside / none | Germany Says Russia Behind Leipzig Airport Drone Attack, An… |
| 110 | MXN+ | USD_MXN | `2026-09-04T14:52:00+00:00` -> `2026-09-04T15:21:00+00:00` | +16.00 bps | +270.2p | 40.8p | **+229.5p** | opposed / neutral_or_conflicted | aligned / inside / none | US Fed's Waller says August inflation data will determine s… |
| 111 | CAD+ | USD_CAD | `2026-09-02T13:40:00+00:00` -> `2026-09-02T13:46:00+00:00` | +15.96 bps | +22.2p | 1.9p | **+20.3p** | opposed / neutral_or_conflicted | aligned / inside / none | 10-year Treasury yield hits highest level since late 2023 a… |
| 112 | JPY- | NZD_JPY | `2026-09-03T03:21:00+00:00` -> `2026-09-03T03:37:00+00:00` | +15.96 bps | +14.8p | 3.8p | **+11.0p** | opposed / neutral_or_conflicted | opposed / inside / none | Which states stand to suffer most in escalated U . S .- Can… |
| 113 | NZD+ | NZD_CAD | `2026-09-02T02:05:00+00:00` -> `2026-09-02T02:13:00+00:00` | +15.91 bps | +12.9p | 5.5p | **+7.4p** | opposed / neutral_or_conflicted | opposed / inside / lower_extreme | Australian National Accounts: National Income, Expenditure … |
| 114 | ZAR- | USD_ZAR | `2026-08-31T09:18:00+00:00` -> `2026-08-31T09:25:00+00:00` | +15.90 bps | +256.0p | 74.0p | **+182.0p** | aligned / neutral_or_conflicted | opposed / inside / lower_extreme | From climate crisis to cost crisis: inflation dynamics in a… |
| 115 | AUD- | AUD_NZD | `2026-09-03T02:12:00+00:00` -> `2026-09-03T03:10:00+00:00` | +15.85 bps | +19.4p | 3.3p | **+16.1p** | neutral_or_conflicted / neutral_or_conflicted | opposed / inside / none | — |
| 116 | HUF+ | USD_HUF | `2026-09-01T13:04:00+00:00` -> `2026-09-01T13:30:00+00:00` | +15.81 bps | +50.1p | 19.1p | **+31.0p** | opposed / neutral_or_conflicted | opposed / inside / none | Ringgit ends lower after US Fed hints at rate hike |
| 117 | USD+ | USD_ZAR | `2026-09-02T12:21:00+00:00` -> `2026-09-02T12:28:00+00:00` | +15.77 bps | +253.3p | 79.2p | **+174.1p** | aligned / neutral_or_conflicted | aligned / inside / none | 10-year Treasury yield hits highest level since late 2023 a… |
| 118 | JPY+ | USD_JPY | `2026-09-02T05:59:00+00:00` -> `2026-09-02T06:05:00+00:00` | +15.73 bps | +25.1p | 1.4p | **+23.7p** | opposed / neutral_or_conflicted | aligned / inside / lower_extreme | Sensex, Nifty slide nearly 1% as crude oil prices surge ami… |
| 119 | HUF- | USD_HUF | `2026-09-02T00:22:00+00:00` -> `2026-09-02T00:54:00+00:00` | +15.72 bps | +50.0p | 31.8p | **+18.2p** | aligned / neutral_or_conflicted | aligned / inside / none | ‘Stop doing memes’ and ‘start being serious’: Canada’s Carn… |
| 120 | USD+ | USD_HUF | `2026-09-03T12:54:00+00:00` -> `2026-09-03T13:06:00+00:00` | +15.64 bps | +49.2p | 21.1p | **+28.2p** | aligned / neutral_or_conflicted | opposed / inside / none | Waller, The Economic Outlook and Some Comments on My Policy… |
| 121 | NOK- | EUR_NOK | `2026-08-31T07:05:00+00:00` -> `2026-08-31T07:37:00+00:00` | +15.62 bps | +169.3p | 31.6p | **+137.7p** | aligned / neutral_or_conflicted | opposed / inside / none | What are the main events for today? |
| 122 | CAD- | CAD_JPY | `2026-09-03T12:36:00+00:00` -> `2026-09-03T12:44:00+00:00` | +15.54 bps | +17.6p | 3.8p | **+13.8p** | aligned / neutral_or_conflicted | aligned / inside / none | Yen recovery gathers momentum as BoJ turns hawkish and NFP … |
| 123 | ZAR- | GBP_ZAR | `2026-09-02T13:27:00+00:00` -> `2026-09-02T13:39:00+00:00` | +15.50 bps | +335.7p | 99.9p | **+235.8p** | aligned / neutral_or_conflicted | opposed / inside / none | Iran fires on its Gulf neighbors, retaliating for US milita… |
| 124 | HUF- | USD_HUF | `2026-09-02T08:08:00+00:00` -> `2026-09-02T08:14:00+00:00` | +15.46 bps | +49.2p | 20.2p | **+29.0p** | aligned / neutral_or_conflicted | opposed / inside / none | Iran-US war escalates as strikes resume - The New Indian Ex… |
| 125 | NZD+ | NZD_JPY | `2026-09-02T14:42:00+00:00` -> `2026-09-02T14:49:00+00:00` | +15.43 bps | +14.3p | 3.4p | **+10.9p** | opposed / neutral_or_conflicted | aligned / inside / none | Bessent Leaves BOJ in No-Win Situation Over Japan Rate Hike… |
| 126 | HKD+ | CHF_HKD | `2026-09-04T13:47:00+00:00` -> `2026-09-04T14:00:00+00:00` | +15.29 bps | +148.1p | 18.7p | **+129.4p** | opposed / neutral_or_conflicted | aligned / inside / none | USD Technical Analysis:What levels are in play for the EURU… |
| 127 | CHF- | USD_CHF | `2026-09-01T14:32:00+00:00` -> `2026-09-01T14:51:00+00:00` | +15.11 bps | +12.2p | 1.4p | **+10.8p** | neutral_or_conflicted / neutral_or_conflicted | aligned / inside / upper_extreme | — |
| 128 | JPY+ | NZD_JPY | `2026-09-03T02:47:00+00:00` -> `2026-09-03T03:03:00+00:00` | +15.11 bps | +14.0p | 3.2p | **+10.8p** | aligned / neutral_or_conflicted | opposed / inside / none | Which states stand to suffer most in escalated U . S .- Can… |
| 129 | HUF- | USD_HUF | `2026-09-03T13:14:00+00:00` -> `2026-09-03T13:32:00+00:00` | +15.04 bps | +47.4p | 19.0p | **+28.4p** | aligned / neutral_or_conflicted | aligned / inside / none | Waller, The Economic Outlook and Some Comments on My Policy… |
| 130 | USD- | NZD_USD | `2026-09-02T13:16:00+00:00` -> `2026-09-02T13:23:00+00:00` | +15.03 bps | +8.8p | 2.4p | **+6.4p** | opposed / neutral_or_conflicted | opposed / inside / lower_extreme | 10-year Treasury yield hits highest level since late 2023 a… |

## News and technical scorecard

The retained on-demand audit has **292** effective theses, **6** diagnostic wins, and **106** misses/gaps. These are not strict publishable event wins.

Largest overlapping failure labels: strict signal absent **58**; no after-cost edge **49**; bad direction/entry **42**; verification gap **19**; latency/decay **18**; giveback/reversal **13**; directional mapping gap **10**.

Completed-M1 descriptive labels: 15-minute trend **480 aligned / 1516 opposed / 5 neutral**; 20-minute breakout **1917 inside / 80 opposed / 4 aligned**. Swing-start selection mechanically biases the trend comparison; it is not forecast accuracy.

### Indicators attached to causal move clocks

- completed-M1 5/15/60-minute return and five-minute velocity
- SMA5-SMA20, SMA20-SMA60, EMA5-EMA20, 20-minute breakout, 60-minute range/exhaustion, ATR14
- all-68 least-squares currency strength, factor breadth, residual, coverage watermark, and factor conflict
- executable bid/ask spread and liquidity/cost bucket
- MFE, MAE, cost-clearance time, order-latency/missed-entry proxy, and giveback
- quote-update intensity and executable-quote imbalance over 5/30/120 seconds in detailed live cases
- shadow trend, breakout, reversion, cross-sectional, pattern, Kalman/Markov/AR, ridge, tabular, graph-transfer, and state-space families (not independent votes)

## Source readiness and unresolved gaps

- Broad live source map: **21/21 currencies**, **68/68 pairs**; all currencies configured `True`, all pairs emitted `True`.
- Central-bank map: configured **21/21**, release-operational **20/21**, release-healthy **20/21**; both legs operational **59/68**.
- Economic minimum: future clocks **21/21**, numeric parsers **21/21**, actual-observation currencies **21/21**, prospectively observed actuals **0/21**.
- Causal consensus: **0/21**; prospective surprise-ready: **0/21**; provider state `blocked_no_permitted_pre_release_consensus_access`.
- Rate context: **8 currencies**, **135 prospective rows**, intraday event-time confirmation `False`.
- V12 narrative meter: **21/21 currencies**, **68/68 pairs**, sealed through `2026-09-04T20:55:00+00:00`, integrity `attention`; research-only `True`.

- Causal pre-release consensus is absent unless an event contains an explicit pre-release observation clock.
- Daily rates are context, not timestamp-safe intraday OIS/futures repricing around releases.
- Broad story proximity cannot establish causality; many matches remain recaps, repeats, late arrivals, or unrelated context.
- A causal intraday oil/commodity transmission path remains missing for NOK and high-beta attribution.
- Independent prospectively frozen event repetitions remain too sparse for a tradable news-plus-technical rule.

## Mapping architecture: before versus current

### Earlier path

`headline/article pool -> semantic currency score -> pair fan-out -> nearby mover/context join -> technical state -> fixed-window outcome`

### Current governed path

`original authority/vendor -> immutable publication/first-seen/revision clocks -> story/event dedup -> calendar/release/recap split -> semantic and publish gates -> sealed 21-currency meter -> base-minus-quote pair state -> all-68 causal factor/episode -> technical timing -> executable cost/path -> immutable outcome -> proof governance -> no_trade or exact canary`

1. Official facts, broad context, strict direction, narrative state, and technical timing are separate evidence arms.
2. Five-minute narrative clocks seal only after bucket close plus grace; partial live state is never proof eligible.
3. Syndications collapse to story clusters and correlated pairs collapse to signed-currency episodes.
4. Completed-M1 features exclude the still-open bar and bind to the move-start knowledge clock.
5. Economics use executable bid/ask cost, MFE/MAE, latency, cost-clearance time, and giveback.
6. Retrospective best pair/horizon selection is labeled discovery and cannot authorize an entry.

## Material live case-audit inventory

- No separately written live case audit existed in this frozen window.

## Operational boundary

- Account `101-001-37981792-007` is `practice` only; open trades/orders `0/0` and margin used `0.0`.
- No report, source collector, meter, outcome worker, or research model can place or close a trade or self-promote.
- New practice entry still requires an exact fresh authorization plus independent lifecycle confirmation; real-money routing remains disabled.
- Captured project-integrity state: `ok`. Captured storage state: `ok`; free space `89.815` GiB.
- A stale or post-cutoff state snapshot is excluded and labeled in the companion JSON rather than silently mixed into the report.

## Evidence artifacts

- `embedded:official_event_response_audit` — SHA-256 `ad100c34a0948e6ac8c075ff4548f2b708806401705a18a8e1c85f297f0fbed0`
- `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\reports\wtd_live_move_reconstruction\WTD_LIVE_MOVE_RECONSTRUCTION_20260904T210000Z.json` — SHA-256 `f2fdb3ddfd1b665c143be72d2cee9d4116b5cd3198df0248cc68214f5d00842d`
- `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\state\news_outcome_improvement_audit_v2.json` — `included` — selected-record SHA-256 `67f684de57784c8ef915197c14536cac6cf960a72ba26e83107b3e33e70f0e11`
- `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\state\continuous_narrative_meter_v12.sqlite` — `included_historical_sealed_meter_bucket`; latest_bucket_with_clock_and_sealed_at_at_or_before_cutoff — selected-record SHA-256 `576966531bd91c0287563ba37cb5d1f96306df7c1c85b9e2383f29f91efc95bd`
- `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\logs\account_snapshot_007_supervised_20260827_205202.out.log` — `included_historical_account_log_record`; latest_append_only_account_snapshot_at_or_before_cutoff — selected-record SHA-256 `35757edf2a27e91a3835da91f5707cadd4edf563e6c0c7ae08c821f046ea0e4b`
- `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\local_news_sentiment\source_coverage_latest.json` — `included` — selected-record SHA-256 `5282134825c576cd3cf8d7ef21d7af9c9c151d9ea84020391e90f0814019dbec`
- `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\reports\official_central_bank_coverage\OFFICIAL_CENTRAL_BANK_COVERAGE_CURRENT.json` — `included` — selected-record SHA-256 `194b818825bc05a5dce2ec6b7b81ead724ea38a02cc0a189a22b6a3c6f58f3f8`
- `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\reports\economic_feed_completeness\ECONOMIC_FEED_COMPLETENESS_CURRENT.json` — `included` — selected-record SHA-256 `753205c5651223c92ca983cc0894049e255a096deac3f9134e502b88de774885`
- `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\state\macro_consensus_access_audit_v1.json` — `included` — selected-record SHA-256 `cbb8eaa8efbc5d643ea61b654624b4a183ca0e91e745225b01373df28997a992`
- `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\state\official_daily_rate_context_v1.json` — `included` — selected-record SHA-256 `4d5477cf0c48da6a07245b590623f994da64f901e5f11e6d06ebb1202d865b0c`
- `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\state\clock_integrity_v1.json` — `excluded_after_cutoff` — selected-record SHA-256 `c770bf7ff09d8fa581f175f2274e3e1259fc6ffcc604d807bf40b9b427ade402`
- `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\state\storage_headroom_v1.json` — `included` — selected-record SHA-256 `7250890e433f8cf1a5777d4e3f2f21bc610fe23d111142c166d47672073864c4`
- `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\state\project_integrity_audit_v1.json` — `included` — selected-record SHA-256 `bca66ce88a0d592e43647e79a5cf96cb445324b7af723380b8a242369a8de0eb`

The companion JSON retains every official event clock and fixed declared horizon, plus every material factor episode and its causal news/technical state. Full sub-threshold move rows remain in the hashed WTD reconstruction artifact.

