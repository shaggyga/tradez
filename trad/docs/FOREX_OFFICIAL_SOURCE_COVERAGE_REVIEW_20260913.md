# Official source coverage and the recalled Bank of Japan episode

Read-only source/configuration review on September 13, 2026, after checkpoint `695f20cb93bbf31d2b53a19134f27cdd52838686`. No network requests, runtime imports, services, database queries, model loading or market-data payload reads were performed. This establishes configuration and implementation, plus explicitly dated saved observations. It does not establish current live delivery or profitable prediction.

The project already defines **21 currencies, 68 pairs and 24 policy-release transport IDs**. All 21 currencies have mapped monetary authorities and seven additional official-source categories. The 192-entry news registry is broader than those policy transports. None of the policy/depth source references was missing from the news/rate registries. These are configured-source counts, not counts of operational feeds or independent successful signals. See [authority map](C:/Users/zmoor/Documents/forex/trad/config/official_central_bank_source_map_v1.json:4), [source depth](C:/Users/zmoor/Documents/forex/trad/config/official_currency_source_depth_v1.json:7) and [news registry](C:/Users/zmoor/Documents/forex/trad/config/news_sources_v1.json).

## The 21-currency matrix

Each row has configured transport for policy, inflation, labour, growth, trade, intervention/reserves, market rates and fiscal/debt. The final column is narrower: numeric parser families recognized by the current readiness allowlist and source contracts, plus dedicated daily-rate adapters. It is not a current observation count or a sentiment-accuracy score.

I = inflation; L = labour; G = growth; T = trade; R = daily market rates; P = policy-rate level. Every policy channel below is enabled in configuration; NZD additionally has an explicit runtime block. Missing family letters do not mean that official documents are absent.

| Currency | Monetary authority and configured policy channel | Other configured official agencies/channels | Recognized numeric families |
|---|---|---|---|
| AUD | Reserve Bank of Australia; RSS + direct minutes HTML | ABS releases; Treasury; RBA reserves/statistics; speeches | I, L, G, R |
| CAD | Bank of Canada; RSS | Statistics Canada; Finance Canada; official reserves; speeches | L, G, R |
| CHF | Swiss National Bank; RSS | Federal Statistical Office; Federal Finance Administration; SNB data/press | I |
| CNH | People's Bank of China; direct HTML | National Bureau of Statistics; Ministry of Finance | I, G |
| CZK | Czech National Bank; RSS | Czech Statistical Office; Ministry of Finance; CNB statistics | I |
| DKK | Danmarks Nationalbank; direct HTML | Statistics Denmark; Nationalbank statistics and government debt | I |
| EUR | European Central Bank; RSS | Eurostat; ECB statistics, market operations and yield curves | I, G, R |
| GBP | Bank of England; RSS | ONS; HM Treasury; official reserves; BoE speeches | I, L, G, R |
| HKD | Hong Kong Monetary Authority; direct HTML | Census and Statistics Department; budget releases; HIBOR | I, R |
| HUF | Magyar Nemzeti Bank; Hungarian + English direct HTML | KSH first releases/CPI/PPI; MNB statistics | I |
| JPY | Bank of Japan; RSS with official document/PDF attachment enrichment | Statistics Bureau; Cabinet Office GDP; Customs; Ministry of Finance intervention, debt and press conferences | I, R |
| MXN | Banco de Mexico; direct HTML | INEGI; Banxico economic information; Finance public-finance portal | I |
| NOK | Norges Bank; RSS | Statistics Norway; Norges statistics, debt and speeches | I |
| NZD | Reserve Bank of New Zealand; OCR snapshot **runtime blocked** | Stats NZ; Treasury; official calendar/discovery fallback; RBNZ reserves/rates also blocked | P, I, L; P is implemented but blocked |
| PLN | National Bank of Poland; JSON release API | Statistics Poland/GUS; Finance; NBP statistics | I, G |
| SEK | Sveriges Riksbank; RSS + direct policy HTML | Statistics Sweden; National Debt Office; Riksbank statistics/speeches | I |
| SGD | Monetary Authority of Singapore; JSON policy API | SingStat; Ministry of Finance; SGS rates | I, R |
| THB | Bank of Thailand; direct HTML V2 | BoT SDDS/statistics; Public Debt Management Office | I |
| TRY | Central Bank of the Republic of Turkey; RSS | TurkStat; Treasury and Finance; CBRT statistics | I |
| USD | Federal Reserve; RSS | BLS; BEA; Census; Department of Labor; Treasury; regional Fed speeches | I, L, G, T, R |
| ZAR | South African Reserve Bank; policy-rate snapshot | Statistics South Africa; Treasury; SARB statistics and publications RSS | P, I |

The frozen universe is AUD, CAD, CHF, CNH, CZK, DKK, EUR, GBP, HKD, HUF, JPY, MXN, NOK, NZD, PLN, SEK, SGD, THB, TRY, USD and ZAR. CNH is the configured offshore renminbi code. HKD/USD and DKK/EUR policy dependencies are explicit; these should not become independent confirmations.

## What is parsed, and what remains missing

Static reproduction of the existing readiness logic gives **168/168 configured currency/family cells and 43/168 recognized structured-parser cells**: inflation 20/21, labour 5/21, growth 7/21, trade 1/21, daily market rates 8/21 and numeric policy 2/21. Intervention/reserves and fiscal/debt have zero recognized structured-parser cells. The calculation follows [the allowlist](C:/Users/zmoor/Documents/forex/trad/oanda_official_currency_source_depth_readiness_v2.py:85) and [contract checks](C:/Users/zmoor/Documents/forex/trad/oanda_official_currency_source_depth_readiness_v2.py:581); it did not query the runtime evidence ledgers.

The readiness allowlist can lag implementation. For example, `statcan_major_indicators_direct_v1` has an implemented collector parser and a current labour numeric contract, but is absent from that allowlist. Reconcile adapter-to-family declarations before treating 43 as an exhaustive measure of all implemented extraction. [Registry entry](C:/Users/zmoor/Documents/forex/trad/config/news_sources_v1.json:1868), [implemented parser](C:/Users/zmoor/Documents/forex/trad/oanda_local_news_sentiment.py:8414).

RSS, direct HTML, JSON and authority-specific parsers are dispatched in [fetch_source](C:/Users/zmoor/Documents/forex/trad/oanda_local_news_sentiment.py:9113). Official text/PDF extraction and same-authority attachment enrichment already exist. Sentiment is more specific than a generic positive/negative word count: there are hawkish/dovish terms, negation and conditional-action handling, issuer binding, policy-document typing and decision-section selection. The code includes Japanese rate-change phrases. These mechanisms propose semantic context; they do not establish a calibrated future currency return. [Phrase rules](C:/Users/zmoor/Documents/forex/trad/oanda_local_news_sentiment.py:621), [issuer binding](C:/Users/zmoor/Documents/forex/trad/oanda_local_news_sentiment.py:11475), [BoJ attachment restriction](C:/Users/zmoor/Documents/forex/trad/oanda_local_news_sentiment.py:11642), [decision-section selection](C:/Users/zmoor/Documents/forex/trad/oanda_local_news_sentiment.py:11747).

Concrete access/configuration gaps:

- RBNZ direct policy/OCR, wholesale rates and reserves are explicitly unsupported because saved bounded access attempts received HTTP 403 and publisher permission is required. An official calendar or discovery fallback is not equivalent to delivery of the decision body. The September 2 gap register reports a dated 20/21 minimum live-readiness observation; that is not a September 13 live-health measurement. [RBNZ configuration](C:/Users/zmoor/Documents/forex/trad/config/news_sources_v1.json:2767), [dated gap register](C:/Users/zmoor/Documents/forex/trad/config/forex_source_gap_register_v1.json:30).
- The separate China Customs direct source is explicitly unsupported after a certificate-chain problem. The broad CNH trade category still points to NBS, so the category count conceals this narrower agency gap. [China Customs entry](C:/Users/zmoor/Documents/forex/trad/config/news_sources_v1.json:4508).
- DOL claims and Stats SA PPI have `runtime_supported=false` in configuration but **must not be classified as blocked solely from that flag**: both declare the exact scheduled-PDF contract accepted by the collector's explicit exception. Actual successful delivery still needs observation. [Runtime gate](C:/Users/zmoor/Documents/forex/trad/oanda_local_news_sentiment.py:3650).
- RBNZ access, pre-release consensus and timestamp-safe intraday policy-path repricing remain separate gaps. The saved register reports no connected intraday OIS/policy-futures sources and no eligible causal consensus rows. Daily yields, current OCR levels and release calendars cannot fill those event-time expectation fields. [Gap register](C:/Users/zmoor/Documents/forex/trad/config/forex_source_gap_register_v1.json).
- The source-first architecture already exists, but its current successor configurations remain inactive: source-factor response V9 and source-conditioned rank V8 both have `collection_enabled=false`. The saved September 9 architecture review identifies their existing source-only and price-timing comparison arms; it also distinguishes them from the joint pair-aggregate model. [V9 configuration](C:/Users/zmoor/Documents/forex/trad/config/source_factor_response_v9.json:3), [V8 configuration](C:/Users/zmoor/Documents/forex/trad/config/source_conditioned_currency_rank_v8.json:3), [saved architecture review](C:/Users/zmoor/Documents/forex/official_currency_architecture_audit_20260909/observed_001/OFFICIAL_CURRENCY_ARCHITECTURE_AUDIT_20260909.md).

## Bank of Japan: two saved cases, no confirmed match to the recollection

The user recalls a release around **4:00** and model receipt around **4:04**; both times are approximate and the time zone/date are unresolved. The inspected records do not prove which episode this was.

1. **July 31, 2024 policy decision and governor press conference.** The verified-source configuration records a policy release at **03:56 UTC**, a rate increase and conditional guidance to continue raising rates if the outlook is realized. This resembles the policy-path description, but it is a historical reconstruction retrieved in August 2026. The saved later event watch is anchored to **06:30 UTC**; the press-conference transcript was published the following day and cannot be treated as contemporaneously available text. There is no matching model-first-seen **04:04** record here. Consensus and rate repricing are explicitly missing, and the case is `candidate_not_proven`, with forecast proof disabled. [Case and clocks](C:/Users/zmoor/Documents/forex/trad/config/spike_blurb_verified_source_cases_v1.json:12), [guidance factor](C:/Users/zmoor/Documents/forex/trad/config/spike_blurb_verified_source_cases_v1.json:113).
2. **September 1, 2026 Bond Market Survey (August 2026).** The saved audit records publication at **07:00:00 UTC** and first seen at **07:03:18.306379 UTC**, a 198.306-second delay. The timing resembles a few-minute ingestion delay, but the document is a directionless bond-market-functioning survey. The audit explicitly says `credited_as_prediction=false`; the classifier repair was to prevent treating it as a policy event. It cannot substantiate the recalled successful rate-outlook signal. [Saved audit](C:/Users/zmoor/Documents/forex/trad/FOREX_BOJ_MARKET_STRUCTURE_OVERLAY_VALIDATION_20260901.json:35).

Separate August 31 and September 2 notes retain Fed/BoJ context as a rate-differential hypothesis, and explicitly decline to credit prior commentary as a same-minute causal prediction. [Pending case](C:/Users/zmoor/Documents/forex/trad/FOREX_PENDING_IMPROVEMENTS.md:1622), [saved movement note](C:/Users/zmoor/Documents/forex/trad/FOREX_PROJECT_LOG.md:5572).

The source-side vault research index was read. The OneDrive vault index files are reparse points and were not followed; [the repository mirror](C:/Users/zmoor/Documents/forex/trad/docs/RESEARCH_INDEX_VAULT.md) preserves the navigation and dated-status caveats. No database or raw document archive was opened to identify the episode.

## Existing paths for the requested exploratory move/feature view

The user's clarified target is a live ranking of the largest percentage currency/pair moves, linked to feature changes and a contemporaneous snapshot. That is a useful descriptive view even before predictive skill is established.

Relevant existing paths encountered in this review are:

- [Executable move census V3](C:/Users/zmoor/Documents/forex/trad/oanda_executable_move_census_v3.py), plus its retained verifier versions.
- [Live move/news snapshot](C:/Users/zmoor/Documents/forex/trad/oanda_live_move_news_snapshot.py) and [V7R3 successor](C:/Users/zmoor/Documents/forex/trad/oanda_live_move_news_snapshot_v7r3.py).
- [Persistent move/news context V5](C:/Users/zmoor/Documents/forex/trad/oanda_live_move_persistent_news_context_v5.py) and [move/news outcomes V4](C:/Users/zmoor/Documents/forex/trad/oanda_live_move_news_outcomes_v4.py).
- [Current feature dictionary](C:/Users/zmoor/Documents/forex/trad/docs/FOREX_FEATURE_DICTIONARY_CURRENT.md), [saved blurb/movement-link audit](C:/Users/zmoor/Documents/forex/trad/docs/FOREX_BLURB_DATASET_AUDIT_20260908.md), and the `live_movement_first_source_attribution` / `live_movement_persistent_context` entries in [the gap register](C:/Users/zmoor/Documents/forex/trad/config/forex_source_gap_register_v1.json).

These identify reuse candidates and retained evidence; this bounded review did not establish which version currently supplies the live display. The exploratory row should show the chosen window, percentage move, currency/base/quote orientation, before/after feature values, observed official/news items and their publication/first-seen times. A relationship may be labelled contemporaneous, delayed context, opposed or unexplained without calling it a successful prediction.

## Next fix recommendation

For the immediate exploratory view, connect the existing move ranking, feature dictionary/snapshots and timed official/news context into one inspectable row. Preserve the selected interval and source timestamps so a user can see what changed around the move. This does not require training or activating a prediction model.

Reuse the existing official-source and source-factor branches. First reconcile source IDs, parser contracts and the 168-cell readiness inventory, then obtain a new observation per authority recording the original release URL, publication/first-seen/body-ready/mapped times and parser outcome. This should expose the RBNZ delivery gap and distinguish a downloaded document from an extracted policy surprise.

For JPY, freeze an issuer-bound **change in policy-path expectations** hypothesis against a matched price-only baseline. Preserve actual/prior/revision, conditional guidance, explicit missing consensus and U.S.-versus-Japan repricing fields. Evaluate after the text was actually available, with executable spreads and independent future events; deduplicate the same release across JPY pairs. Do not enable the inactive research branch or award a historical win merely because a release and price move align.

Before another model run, consult the retained blurb/model-use evidence: the September 8 audit reports substantial historical material but zero scored/prequential predictions from the narrow recovered analog arm. That record supports repairing information and connection gaps, rather than repeating already completed movement-selected experiments under a new name. [Saved blurb audit](C:/Users/zmoor/Documents/forex/trad/docs/FOREX_BLURB_DATASET_AUDIT_20260908.md).
