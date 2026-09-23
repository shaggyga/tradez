# Forex news-source audit

Generated from the canonical source configuration and live collector state on 2026-08-06 at 10:45:42 UTC.

## Executive inventory

| Measure | Current value |
|---|---:|
| Configured source definitions | 63 |
| Enabled/operational | 55 |
| Healthy | 53 |
| Degraded | 2 |
| Missing credentials | 3 |
| Disabled | 5 |
| Configured currencies | 21 |
| Mapped FX pairs | 68 |
| Direct two-leg live coverage | 45 pairs |
| Mixed two-leg live coverage | 23 pairs |
| Current collector policy | Research-only; never execution-eligible |

The 63 entries are not 63 independent newsrooms. They comprise 37 first-party policy/statistical endpoints, 12 official-domain search fallbacks, eight optional/specialized datasets, and six broad discovery searches. Search fallbacks and aggregators may repeat the same underlying report; downstream canonical-URL, semantic, and syndication deduplication is therefore essential.

At the audit timestamp, the collector was running normally. Its current event catalog was fresh with 641 events and 27,420 pair-event tags. Fifty pairs had a global-proxy context score, but zero pairs had a direct-current score. This is a useful context state, not a claim that 50 pairs had independently verified directional news.

## Field definitions

- **First-party**: fetched from the issuing central bank, ministry, or statistical authority.
- **Official-domain search**: Google News is used only as transport/discovery, and accepted publishers are restricted to the named official domain. This is a fallback, not an independent source.
- **Discovery aggregator**: finds potential events across publishers. It is lower-trust until publisher identity, time, and claim are verified.
- **Calendar**: release timing and event identity. A calendar entry does not supply a directional surprise unless actual and consensus values are also causally available.
- **Context**: positioning, volatility, revisions, or narrative information; it should not be interpreted as an immediate long/short vote by itself.
- **Quality**: configured provenance/reliability prior from 0 to 1. It is not measured forecast accuracy.
- **Healthy 200/304**: the last request succeeded or the source correctly reported “not modified.”
- **Not due**: the source is healthy but was inside its configured polling interval on the latest collector cycle.
- **Degraded**: enabled, but currently accumulating transport errors.
- **Credential missing**: adapter exists but cannot operate without an API credential.
- **Disabled**: retained as a defined future/research source but not part of the live operational set.
- **Cadence**: explicit poll interval where configured. First-party RSS defaults to 180 seconds; adapter-specific sources marked “adapter” manage cadence internally.

All sources are globally `research_only=true` and `execution_eligible=false`. Individual items are later classified, deduplicated, time-normalized, mapped to currencies/pairs, and may remain context-only. No source below directly places an order.

## A. First-party central-bank and policy sources (22)

| Source | Scope | Definition | Transport / cadence | Quality | Live state |
|---|---|---|---|---:|---|
| [`fed_monetary_policy`](https://www.federalreserve.gov/feeds/press_monetary.xml) | USD | Federal Reserve monetary-policy decisions and releases | RSS / 180s | 1.00 | Healthy, HTTP 304 |
| [`fed_speeches`](https://www.federalreserve.gov/feeds/speeches.xml) | USD | Federal Reserve speeches; policy communication rather than a scheduled decision | RSS / 180s | 0.95 | Healthy, HTTP 304 |
| [`us_treasury_press`](https://home.treasury.gov/news/press-releases) | USD | U.S. Treasury policy actions and press releases | HTML links / 180s | 1.00 | Healthy, HTTP 200 |
| [`ecb_press`](https://www.ecb.europa.eu/rss/press.html) | EUR | European Central Bank press releases and policy communications | RSS / 180s | 1.00 | Healthy, HTTP 200 |
| [`boe_news`](https://www.bankofengland.co.uk/rss/news) | GBP | Bank of England news and formal releases | RSS / 180s | 1.00 | Healthy, HTTP 200 |
| [`boe_speeches`](https://www.bankofengland.co.uk/rss/speeches) | GBP | Bank of England speeches and policy communication | RSS / 180s | 0.95 | Healthy, HTTP 200 |
| [`boc_press`](https://www.bankofcanada.ca/content_type/press-releases/feed/) | CAD | Bank of Canada press releases and policy decisions | RSS / 180s | 1.00 | Healthy, HTTP 304 |
| [`boc_speeches`](https://www.bankofcanada.ca/content_type/speeches/feed/) | CAD | Bank of Canada speeches and policy guidance | RSS / 180s | 0.95 | Healthy, HTTP 304 |
| [`rba_media`](https://www.rba.gov.au/rss/rss-cb-media-releases.xml) | AUD | Reserve Bank of Australia media and policy releases | RSS / 180s | 1.00 | Healthy, HTTP 304 |
| [`rba_speeches`](https://www.rba.gov.au/rss/rss-cb-speeches.xml) | AUD | Reserve Bank of Australia speeches | RSS / 180s | 0.95 | Healthy, HTTP 304 |
| [`boj_updates`](https://www.boj.or.jp/en/rss/whatsnew.xml) | JPY | Bank of Japan updates, including policy material | RSS / 180s | 1.00 | Healthy, HTTP 200 |
| [`japan_mof_international_policy`](https://www.mof.go.jp/english/public_relations/whats_new/2026international_policy.html) | JPY | Japan Ministry of Finance international-policy and intervention-relevant releases | HTML links / 180s | 1.00 | Healthy, HTTP 304 |
| [`snb_press`](https://www.snb.ch/public/rss/en/pressrel) | CHF | Swiss National Bank press releases | RSS / 180s | 1.00 | Healthy, HTTP 200 |
| [`snb_monetary_policy`](https://www.snb.ch/public/rss/en/mopo) | CHF | Swiss National Bank monetary-policy releases | RSS / 180s | 1.00 | Healthy, HTTP 200 |
| [`norges_press`](https://www.norges-bank.no/en/rss-feeds/Press-releases---Norges-Bank/) | NOK | Norges Bank press releases | RSS / 180s | 1.00 | Healthy, HTTP 200 |
| [`norges_speeches`](https://www.norges-bank.no/en/rss-feeds/Speeches---Norges-Bank/) | NOK | Norges Bank speeches | RSS / 180s | 0.95 | Healthy, HTTP 200 |
| [`riksbank_press`](https://www.riksbank.se/en-gb/rss/press-releases/) | SEK | Sveriges Riksbank press releases | RSS / 180s | 1.00 | Healthy, HTTP 304 |
| [`riksbank_speeches`](https://www.riksbank.se/en-gb/rss/speeches/) | SEK | Sveriges Riksbank speeches | RSS / 180s | 0.95 | Healthy, HTTP 200 |
| [`cnb_press`](https://www.cnb.cz/en/.content/rss-feed/rss-feed_tz.xml) | CZK | Czech National Bank press releases | RSS / 180s | 1.00 | Healthy, HTTP 200 |
| [`hkma_press_api`](https://api.hkma.gov.hk/public/press-releases?lang=en) | HKD | Hong Kong Monetary Authority press-release API | JSON / adapter | 1.00 | **Degraded: HTTP 502, 4 consecutive errors** |
| [`hkma_press_html`](https://www.hkma.gov.hk/eng/news-and-media/press-releases/) | HKD | HKMA HTML fallback; covers the same publisher when the API fails | HTML links / 180s | 1.00 | Healthy, HTTP 200 |
| [`tcmb_press`](https://www.tcmb.gov.tr/wps/wcm/connect/EN/TCMB%2BEN/Bottom%2BMenu/Other/RSS/Press%2BReleases) | TRY | Central Bank of the Republic of Türkiye press releases | RSS / 180s | 1.00 | Healthy, HTTP 304 |

## B. First-party statistical releases and calendars (15)

| Source | Scope | Definition | Transport / cadence | Quality | Live state |
|---|---|---|---|---:|---|
| [`swiss_fso_releases`](https://d-nsbc-p.admin.ch/NSBSubscriber/feeds/rss) | CHF | Swiss Federal Statistical Office releases, including inflation and activity data | RSS / 180s | 1.00 | Healthy, HTTP 200 |
| [`bls_principal_releases`](https://www.bls.gov/feed/bls_latest.rss) | USD | U.S. BLS principal releases such as labor and inflation publications | RSS / 60s | 1.00 | Healthy, HTTP 304 |
| [`bls_jolts_job_openings`](https://api.bls.gov/publicAPI/v2/timeseries/data/JTS000000000000000JOL) | USD | Structured BLS JOLTS job-openings observation and prior value | BLS API / 60s | 1.00 | Healthy, HTTP 200 |
| [`bea_releases`](https://apps.bea.gov/rss/rss.xml) | USD | U.S. BEA national accounts, income, spending, and trade releases | RSS / 90s | 1.00 | Healthy, HTTP 200 |
| [`census_economic_indicators`](https://www.census.gov/economic-indicators/indicator.xml) | USD | U.S. Census economic-indicator releases | RSS / 90s | 1.00 | Healthy, HTTP 304 |
| [`census_release_calendar`](https://www.census.gov/economic-indicators/calendar-listview.html) | USD | Census release schedule and recurring event identity | HTML calendar / 300s | 0.99 | Healthy, HTTP 200 |
| [`eurostat_economy_finance`](https://ec.europa.eu/eurostat/en/search?_estatsearchportlet_WAR_estatsearchportlet_collection=CAT_PREREL&_estatsearchportlet_WAR_estatsearchportlet_theme=PER_ECOFIN&p_p_id=estatsearchportlet_WAR_estatsearchportlet&p_p_lifecycle=2&p_p_mode=view&p_p_resource_id=atom&p_p_state=maximized) | EUR | Eurostat economy and finance releases | Atom/RSS / 120s | 1.00 | Healthy, HTTP 200 |
| [`ons_published_releases`](https://www.ons.gov.uk/releasecalendar?highlight=true&limit=20&page=1&release-type=type-published&rss=&sort=date-newest) | GBP | UK ONS published statistical releases | RSS / 90s | 1.00 | Healthy, HTTP 200 |
| [`statcan_daily_releases`](https://www150.statcan.gc.ca/n1/rss/dai-quo/0-eng.atom) | CAD | Statistics Canada daily release feed | Atom/RSS / 90s | 1.00 | **Disabled after repeated transport failure; 6 errors** |
| [`ism_us_pmi_calendar`](https://www.ismworld.org/supply-management-news-and-reports/reports/rob-report-calendar/) | USD | Official recurring ISM manufacturing/services PMI schedule; timing only unless surprise data is separately verified | Local official calendar / 21,600s | 1.00 | Healthy, HTTP 200 |
| [`sp_global_uk_pmi_calendar`](https://pmi.spglobal.com/Public/Release/ReleaseDates?language=en&os=0) | GBP | Official S&P Global UK PMI release schedule; timing/event identity | Local official calendar / 21,600s | 0.98 | Healthy, HTTP 200 |
| [`abs_latest_releases`](https://www.abs.gov.au/release-calendar/latest-releases) | AUD | Australian Bureau of Statistics latest releases | HTML links / 180s | 0.98 | Healthy, HTTP 304 |
| [`japan_statistics_news`](https://www.stat.go.jp/english/info/news/index.html) | JPY | Statistics Bureau of Japan statistical news bulletin | HTML links / 180s | 0.98 | Healthy, HTTP 304 |
| [`stats_nz_releases`](https://www.stats.govt.nz/information-releases/) | NZD | Stats NZ published information releases | HTML links / 180s | 0.98 | Healthy, HTTP 200 |
| [`stats_nz_calendar`](https://www.stats.govt.nz/api/v1/releaseCalendarMonth/) | NZD | Structured Stats NZ release calendar | JSON calendar / 300s | 0.99 | Healthy, HTTP 200 |

## C. Optional, commercial, and specialized datasets (8)

| Source | Scope | Definition | Transport / cadence | Quality | Live state |
|---|---|---|---|---:|---|
| [`trading_economics_calendar`](https://api.tradingeconomics.com/calendar) | 21 currencies | Structured macro calendar with event/actual/forecast metadata | API / 60s | 0.95 | **Credential missing, HTTP 401** |
| [`alpha_vantage_fx_news_sentiment`](https://www.alphavantage.co/query) | Global FX | Third-party news and vendor sentiment | API / 300s | 0.72 | **Credential missing** |
| [`finnhub_fx_market_news`](https://finnhub.io/api/v1/news) | Global FX | Third-party FX market-news discovery | API / 60s | 0.70 | **Credential missing** |
| [`gdelt_fx_macro_discovery`](https://api.gdeltproject.org/api/v2/doc/doc) | Global FX | Broad multilingual macro/event discovery; not authoritative by itself | GDELT API / 900s | 0.65 | **Degraded: HTTP 429, 3 consecutive errors** |
| [`fred_alfred_vintages`](https://api.stlouisfed.org/fred/series/vintagedates) | USD | Point-in-time macro vintages for revision-safe historical research | API / adapter | 1.00 | Disabled |
| [`cftc_cot_positioning`](https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm) | AUD, CAD, CHF, EUR, GBP, JPY, MXN, NZD, USD | Weekly futures positioning; slow context, not immediate news | Snapshot / adapter | 1.00 | Disabled |
| [`cme_fx_options_analytics`](https://www.cmegroup.com/market-data/greeks-and-implied-volatility-data.html) | AUD, CAD, CHF, EUR, GBP, JPY, MXN, NZD, USD | FX option implied volatility/skew expectations | Analytics / adapter | 1.00 | Disabled |
| [`lseg_machine_readable_news`](https://developers.lseg.com/en/article-catalog/article/getting-mrn-with-data-library-for-python) | Global | Licensed low-latency machine-readable news | Licensed stream / adapter | 0.98 | Disabled |

## D. Official-domain search fallbacks (12)

These are Google News RSS searches restricted to the named official publisher domain. They improve discoverability for authorities without reliable direct feeds, but they are transport fallbacks and can lag or duplicate first-party releases.

| Source | Scope | Definition | Cadence | Quality | Live state |
|---|---|---|---:|---:|---|
| [`snb_official_search`](https://news.google.com/rss/search?q=site%3Asnb.ch%20%28%22monetary%20policy%22%20OR%20%22policy%20rate%22%20OR%20inflation%20OR%20currency%29%20when%3A30d&hl=en-US&gl=US&ceid=US%3Aen) | CHF | SNB official-domain fallback for policy, rates, inflation, and currency | 900s | 0.80 | Healthy, HTTP 200 |
| [`rbnz_official_search`](https://news.google.com/rss/search?q=site%3Arbnz.govt.nz%20%28%22monetary%20policy%22%20OR%20%22official%20cash%20rate%22%20OR%20inflation%20OR%20currency%29%20when%3A30d&hl=en-US&gl=US&ceid=US%3Aen) | NZD | RBNZ official-domain policy search | 900s | 0.80 | Healthy, HTTP 200 |
| [`banxico_official_search`](https://news.google.com/rss/search?q=site%3Abanxico.org.mx%20%28%22monetary%20policy%22%20OR%20%22interest%20rate%22%20OR%20inflation%20OR%20currency%29%20when%3A7d&hl=en-US&gl=US&ceid=US%3Aen) | MXN | Banco de México official-domain policy search | 900s | 0.80 | Healthy, HTTP 200 |
| [`sarb_official_search`](https://news.google.com/rss/search?q=site%3Aresbank.co.za%20%28%22monetary%20policy%22%20OR%20%22interest%20rate%22%20OR%20inflation%20OR%20currency%29%20when%3A7d&hl=en-US&gl=US&ceid=US%3Aen) | ZAR | South African Reserve Bank official-domain policy search | 900s | 0.80 | Healthy, HTTP 200 |
| [`pboc_official_search`](https://news.google.com/rss/search?q=site%3Apbc.gov.cn%20%28%22monetary%20policy%22%20OR%20%22interest%20rate%22%20OR%20yuan%20OR%20renminbi%29%20when%3A30d&hl=en-US&gl=US&ceid=US%3Aen) | CNH | PBOC official-domain policy/yuan search | 900s | 0.80 | Healthy, HTTP 200 |
| [`safe_official_search`](https://news.google.com/rss/search?q=site%3Asafe.gov.cn%2Fen%2F%20%28%22foreign%20exchange%22%20OR%20%22exchange%20rate%22%20OR%20reserves%20OR%20yuan%29%20when%3A60d&hl=en-US&gl=US&ceid=US%3Aen) | CNH | China SAFE official-domain FX/reserves search | 900s | 0.80 | Healthy, HTTP 200 |
| [`china_nbs_official_search`](https://news.google.com/rss/search?q=site%3Astats.gov.cn%2Fenglish%2FPressRelease%2F%20%28CPI%20OR%20inflation%20OR%20GDP%20OR%20employment%29%20when%3A60d&hl=en-US&gl=US&ceid=US%3Aen) | CNH | China NBS official-domain macro-release search | 900s | 0.80 | Healthy, HTTP 200 |
| [`nationalbanken_official_search`](https://news.google.com/rss/search?q=site%3Anationalbanken.dk%20%28%22monetary%20policy%22%20OR%20%22interest%20rate%22%20OR%20inflation%20OR%20currency%29%20when%3A7d&hl=en-US&gl=US&ceid=US%3Aen) | DKK | Danmarks Nationalbank official-domain policy search | 900s | 0.80 | Healthy, HTTP 200 |
| [`mnb_official_search`](https://news.google.com/rss/search?q=site%3Amnb.hu%20%28%22monetary%20policy%22%20OR%20%22interest%20rate%22%20OR%20inflation%20OR%20currency%29%20when%3A7d&hl=en-US&gl=US&ceid=US%3Aen) | HUF | Magyar Nemzeti Bank official-domain policy search | 900s | 0.80 | Healthy, HTTP 200 |
| [`nbp_official_search`](https://news.google.com/rss/search?q=site%3Anbp.pl%20%28%22monetary%20policy%22%20OR%20%22interest%20rate%22%20OR%20inflation%20OR%20currency%29%20when%3A7d&hl=en-US&gl=US&ceid=US%3Aen) | PLN | Narodowy Bank Polski official-domain policy search | 900s | 0.80 | Healthy, HTTP 200 |
| [`mas_official_search`](https://news.google.com/rss/search?q=site%3Amas.gov.sg%20%28%22monetary%20policy%22%20OR%20%22exchange%20rate%22%20OR%20inflation%20OR%20currency%29%20when%3A7d&hl=en-US&gl=US&ceid=US%3Aen) | SGD | Monetary Authority of Singapore official-domain policy/FX search | 900s | 0.80 | Healthy, HTTP 200 |
| [`bot_official_search`](https://news.google.com/rss/search?q=site%3Abot.or.th%20%28%22monetary%20policy%22%20OR%20%22interest%20rate%22%20OR%20inflation%20OR%20baht%29%20when%3A7d&hl=en-US&gl=US&ceid=US%3Aen) | THB | Bank of Thailand official-domain policy/baht search | 900s | 0.80 | Healthy, HTTP 200 |

## E. Broad news and event discovery (6)

| Source | Scope | Definition | Cadence | Quality | Live state |
|---|---|---|---:|---:|---|
| [`google_news_fx_macro`](https://news.google.com/rss/search?q=%28forex%20OR%20currency%20OR%20%22central%20bank%22%20OR%20inflation%29%20when%3A1h&hl=en-US&gl=US&ceid=US%3Aen) | Global | Broad hourly FX, currency, central-bank, and inflation discovery | 180s | 0.55 | Healthy, HTTP 200 |
| [`google_news_global_risk`](https://news.google.com/rss/search?q=%28oil%20OR%20tariff%20OR%20sanctions%20OR%20war%29%20%28currency%20OR%20markets%29%20when%3A1h&hl=en-US&gl=US&ceid=US%3Aen) | Global | Oil, sanctions, conflict, tariffs, and market-risk discovery | 180s | 0.55 | Healthy, HTTP 200 |
| [`google_news_systemic_catalyst`](https://news.google.com/rss/search?q=%28%22Strait%20of%20Hormuz%22%20OR%20%22currency%20intervention%22%20OR%20%22capital%20controls%22%20OR%20%22sovereign%20default%22%29%20when%3A1h&hl=en-US&gl=US&ceid=US%3Aen) | Global | Fast discovery for intervention, capital controls, sovereign default, and systemic supply-route events | 60s | 0.55 | Healthy, HTTP 200 |
| [`google_news_fx_policy_ticker`](https://news.google.com/rss/search?q=%28Bessent%20OR%20%22Treasury%20Secretary%22%20OR%20%22finance%20minister%22%20OR%20%22central%20bank%20governor%22%29%20%28yen%20OR%20dollar%20OR%20currency%20OR%20intervention%20OR%20rates%29%20when%3A1h&hl=en-US&gl=US&ceid=US%3Aen) | Global | Fast ticker for finance-minister and central-bank policy comments | 60s | 0.55 | Healthy, HTTP 200 |
| [`google_news_trade_policy`](https://news.google.com/rss/search?q=%28tariff%20OR%20tariffs%20OR%20%22trade%20policy%22%20OR%20%22customs%20duties%22%20OR%20%22import%20taxes%22%29%20%28sues%20OR%20lawsuit%20OR%20imposes%20OR%20announces%20OR%20%22strikes%20down%22%20OR%20rollback%29%20when%3A6h&hl=en-US&gl=US&ceid=US%3Aen) | Global | Material tariff, customs, and trade-policy action discovery | 180s | 0.60 | Healthy, HTTP 200 |
| [`google_news_market_ticker`](https://news.google.com/rss/search?q=%28Treasury%20yields%20OR%20oil%20OR%20gold%20OR%20copper%20OR%20equity%20futures%20OR%20risk-off%29%20%28dollar%20OR%20yen%20OR%20currency%20OR%20forex%29%20when%3A1h&hl=en-US&gl=US&ceid=US%3Aen) | Global | Rates, commodities, equity-futures, and risk-regime headlines tied to FX | 180s | 0.55 | Healthy, HTTP 200 |

## Current operational exceptions

1. **HKMA API is degraded**, but the independent HKMA HTML fallback is healthy. HKD policy coverage is therefore degraded in transport redundancy, not absent.
2. **GDELT is rate-limited (HTTP 429)**. This reduces broad global discovery but does not impair first-party central-bank/statistical feeds.
3. **Trading Economics, Alpha Vantage, and Finnhub lack credentials.** Trading Economics is the most consequential gap because it could provide a structured multi-country event calendar. Alpha Vantage and Finnhub are optional lower-quality discovery/sentiment supplements.
4. **Statistics Canada is disabled after repeated transport failures.** CAD still has direct Bank of Canada coverage and broad discovery, but first-party Canadian statistical-release coverage is incomplete.
5. **FRED/ALFRED, CFTC, CME options, and LSEG MRN are disabled.** These are historical/context/licensed enhancements, not failures of the core live official-news collector.

## Practical interpretation

- The strongest live layer is first-party policy and statistical publication coverage for USD, EUR, GBP, JPY, CHF, AUD, NZD, and the major central-bank currencies.
- The weakest first-party statistical area is CAD because the Statistics Canada transport is disabled.
- Emerging-market coverage is mostly official-domain search fallback rather than direct structured feeds; its latency and completeness should be treated as lower confidence.
- Google News entries provide discovery breadth, not six independent confirmations. Consensus must count unique underlying publishers/events, not source IDs.
- News direction remains the largest conceptual constraint: most authoritative releases identify an event but do not encode the market surprise. Direction should require causal actual-versus-consensus data, explicit policy action, or verified post-event price confirmation—not headline sentiment alone.

## Source-of-truth files

- Configuration: `C:\Users\zmoor\Documents\forex\trad\config\news_sources_v1.json`
- Live collector state: `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\local_news_sentiment\collector_state_v1.json`
- Live coverage: `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\local_news_sentiment\source_coverage_latest.json`
- Event catalog: `C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager\news_event_tags\latest_pair_news_context.json`
