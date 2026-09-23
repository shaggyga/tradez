# Pair forecast coverage and runtime repair — September 7

The previous model requirement blocked most pairs despite usable real price history. The new registered price-v2 study uses actual UTC minute timestamps and elapsed time, exact one-hour historical endpoints and explicit missingness. State-space and Ridge now publish independently: one unavailable family cannot suppress the other.

At the independently verified **21:30:32 UTC** observation, **65 of 68 pairs had 130 active model forecasts**, with 274 publications and zero worker or heartbeat-write errors. No H1 outcomes had matured at that observation. Three TRY pairs lacked current usable inputs. These are dated coverage figures, not an accuracy result.

Each pair/family has its own immutable contract, attempts, forecasts, publication receipts, independent consumer observations, later quote-entry matches and original one-hour targets. Successful cadence buckets and market-reference times are unique. Failed attempts may retry with fresh inputs. No candle, prior forecast or outcome was backfilled.

The dashboard now shows current family availability and original forecast times, expected moves before costs, and uncertainty as an uncalibrated estimate. The API's primary collection status follows the selected pair study. Original EUR/USD/shared-gap companions were stopped and backed up read-only; their source and evidence remain intact. Pair-v1 continues separately.

The storage guard expanded from six legacy databases to 216 actual registered/core databases, retaining a fresh inventory baseline before projecting growth. The integrity audit distinguishes fresh active-worker observations from retained historical failures; inactive executor artifacts no longer imply a running executor. Source-bound tests, independent review and controlled reload evidence are linked in the validation receipt.

The completed one-hour watch before this repair found poor results: of 23 newly scored pair-v1 decisions, State-space got direction right on 8 and Ridge on 12; neither model had a positive after-spread result in those 23. The forecasts and pairs overlap, so these are not independent trials. Those results remain preserved.

This price-only repair does not incorporate news. The separate joint price/news successor and news admission repair are documented in [the joint report](FOREX_JOINT_PRICE_NEWS_20260907.md). Better coverage and passing engineering tests do not demonstrate improved prediction accuracy or profitability. Orders and promotion remain disabled.

[Source-bound validation](../FOREX_PAIR_FAMILY_REPAIR_VALIDATION_20260907.json) · [Active pipeline](ACTIVE_PIPELINE.md) · [Research index](RESEARCH_INDEX.md).
