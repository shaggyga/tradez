# Current setup and complementary signal sources

September 13, 2026. This answers the user's request for the strongest overall setup available from the project's existing work. Direction is one part of that objective. This is a source/retained-evidence review, not a fresh market read, model fit, subscription purchase or live activation.

## What to combine and what to measure

| Component | Strongest retained lead / current role | Limit that must remain visible |
|---|---|---|
| Movement opportunity and regime | The July 13 HGB used 164 motion, interaction and cost inputs. Its four development-fold opportunity AUC averaged 0.6822, minimum 0.6674. | This concerns either-side two-hour opportunity under its original cost assumptions. It is not directional accuracy, a universal horizon model or a newly verified live result. |
| Currency-relative information | Reuse official-release/blurb mappings for policy, activity, risk/commodity and intervention context. One matched H1 comparison improved direction from 50.20% price-only to 52.07% news-plus-price. | Both lost after costs; later samples did not consistently favor news. Numeric consensus and intraday policy-path repricing are material missing information types. |
| Technical state and timing | Retain original momentum/acceleration, compression, breakout, trend, currency-strength and cost/liquidity features with their actual clocks. The new observation path preserves feature changes and quiet controls. | Feature inclusion or contemporaneous association is not independent importance or predictive confirmation. Wider MA/peer inputs have often worsened retained comparisons. |
| Execution quality | Reuse actual bid/ask arithmetic, post-publication entry timing, spread and stress-cost measurements. | A large midpoint move can remain economically unusable. Both-side research probes cannot be treated as a direction selected in advance. |
| Curve and position management | Preserve original multi-horizon chains and existing hold/exit/reduce/rotation controls. Evaluate compatible updates against fixed hold and no-trade. | Three retained paper episodes gave fixed hold +$2.0598, curve management -$5.6738 and matched momentum -$14.8226. These hypothetical scenarios do not establish a dependable manager or represent current account P/L. |
| Calibration and controls | Retain calibrated probabilities, simple price-only/no-change controls and original native-target conventions. | Better calibration has not removed signed-move optimism or demonstrated profitable policy performance. |

The priority is a connected set of these measurements, with individual contributions visible. Averaging every available indicator or adding another algorithm is not evidence of improvement. The original 164-input HGB and other retained bundles must keep their exact input/target/source contracts; the new observation vectors cannot silently substitute for their trained columns.

Source records: [model reuse register](FOREX_MODEL_REUSE_REGISTER_20260911.md), [HGB report](../fresh_m1_intrahour/reports/feature_forecast_full_20260713_v4/FEATURE_FORECAST_REPORT.md), [H1 clarification](FOREX_ARIMA_AND_H1_READBACK_20260913.md), [signed-cost follow-up](FOREX_DIRECTION_DECISION_20260912.md), and [curve/management record](FOREX_OVERNIGHT_CURVE_BUILDOUT_20260909.md).

## Historical data is already substantial

The retained archive audit reports 53,512,475 M1 rows across 68 pairs and 28 months. Prior work includes wider features, gradient boosting, ARIMA/SARIMAX, lagged errors, currency peers, interactions, news/blurb inputs and curves. These records should be reused rather than rediscovered. [Archive and model-space audit](FOREX_MODEL_SPACE_AUDIT_20260912.md).

Price history does not itself contain the economist consensus known just before a release, the market's contemporaneous expected rate path, every original news-arrival timestamp, or observations never captured. New capture repairs those omissions prospectively; it cannot reconstruct historical knowledge by assigning an earlier timestamp to a value retrieved today.

## Additional information sources

The priority below is an inference about complementary information, not a performance ranking or a procurement decision. Availability was checked against official/provider documentation on September 13. Paid entitlements and usable historical coverage must be confirmed before a source is selected.

1. **Scheduled-release surprises.** Combine official actuals with the consensus that was available before publication, preserving prior values and revisions separately. [Trading Economics calendar](https://tradingeconomics.com/api/calendar.aspx) and its [schema](https://docs.tradingeconomics.com/economic_calendar/schema/) distinguish economist-survey `Forecast` from proprietary `TEForecast`; a [streaming endpoint](https://docs.tradingeconomics.com/economic_calendar/streaming/) is documented. This directly extends the user's official-release/currency-relative-strength idea. Do not use a later edited forecast field as the original consensus. Some official research APIs are unsuitable as immediate release triggers: [BLS documents a one-day API delay](https://www.bls.gov/bls/api_features.htm).

2. **Changes in expected rates and between-currency rate paths.** [CME FedWatch API](https://www.cmegroup.com/market-data/market-data-api/fedwatch-api.html) offers futures-implied US/FOMC probabilities, with separate end-of-day and intraday products. [ICE SONIA futures](https://www.ice.com/interest-rates/short-term-interest-rate-futures/sonia-futures) are a sterling rate-market source. Other currencies require appropriate futures/OIS data and licenses. Today's policy rate or a daily government yield is a different measurement from the expected path.

3. **Options-implied volatility and asymmetry.** [CME Greeks and Implied Volatility](https://www.cmegroup.com/market-data/greeks-and-implied-volatility-data.html) documents five-minute snapshots through REST/streaming; [FX CVOL](https://www.cmegroup.com/markets/fx.html) covers forward 30-day risk for G5 currencies. These can complement realized ATR and momentum. Product coverage and historical delivery require checking; an advertised history range is not confirmed access. CVOL skew should not be relabeled as an OTC 25-delta risk reversal.

4. **Actual cross-asset markets.** Rates, equity indices, energy and metals from [CME market-data APIs](https://www.cmegroup.com/market-data/market-data-api.html) can add risk and commodity context beyond FX peer features. Preserve exchange timestamps, calendars, contract identities and rolls. Daily versions may already be accessible through the existing FRED adapter.

5. **Positioning and crowding.** Reuse the existing CFTC adapter with the [CFTC public reporting API/export service](https://publicreporting.cftc.gov/stories/s/User-s-Guide/p2fg-u73y/). COT is normally published Friday for Tuesday positions. It is slower context and covers listed futures/options, not every OTC participant. Completion of its source-bound research bridge remains separate work.

6. **Macro and external-balance regimes.** [BIS statistics](https://data.bis.org/), [ECB SDMX](https://data.ecb.europa.eu/help/getting-data-web-services-sdmx-0) and [IMF APIs](https://data.imf.org/en/Resource-Pages/IMF-API) offer additional official series. Reuse existing FRED/ALFRED routes where appropriate. [FRED's date-based vintage parameters](https://fred.stlouisfed.org/docs/api/fred/series_observations.html) do not by themselves establish intraday availability. These inputs generally belong to slower regime context.

7. **Broker-local market quality.** Preserve spreads, quote arrivals, offered liquidity and tradeability from the existing [OANDA pricing stream](https://developer.oanda.com/rest-live-v20/pricing-ep/). It supplies at most four prices per second per instrument, selecting the last price in each 250ms window. It is neither every tick nor global FX order flow.

The first two additions to investigate are timestamped release consensus and rate-expectation repricing. Existing official feeds, FRED/ALFRED and CFTC infrastructure should be reused. None of these recommendations establishes coverage of all 21 currencies or authorizes a subscription.
