# Missing combined forecasts — 2026-09-07T23:28:46.349568+00:00

The current API shows 61/68 combined H1 forecasts, with 7 missing pairs. This is a dated availability check, not an accuracy assessment.

| Pair | Combined-model blocker | Price-only H1 arms active |
|---|---|---|
| EUR_DKK | mature_joint_h1_training_rows:39<48 | probabilistic_state_space, ridge_return_repaired |
| EUR_TRY | ValueError:market_closed_or_no_stream_quote | None |
| GBP_HKD | nonzero_news_context_training_rows:10<12 | probabilistic_state_space, ridge_return_repaired |
| GBP_PLN | nonzero_news_context_training_rows:7<12 | probabilistic_state_space, ridge_return_repaired |
| GBP_SGD | nonzero_news_context_training_rows:5<12; distinct_news_context_patterns:6<8 | probabilistic_state_space, ridge_return_repaired |
| TRY_JPY | ValueError:market_closed_or_no_stream_quote | None |
| USD_TRY | ValueError:market_closed_or_no_stream_quote | None |

The three TRY pairs have no accepted current quote in these ledgers. Their last stream rows are explicitly nontradeable and timestamped about 14:59:55 UTC, rather than current executable prices. The other four can already be displayed as price-only research estimates with their existing uncertainty, while their combined-model availability remains separate.

At the retained 23:28:46 UTC API observation, the three GBP rows also report `current_news_stale_or_future` from their latest attempted input capture. Their last complete numerical-readiness events contain the structural counts in the table. Both observations are retained in the JSON: a current news refresh is needed, and it does not by itself supply the missing historical news-context examples. EUR/DKK has a fresh numerical-readiness observation and remains at 39/48 labels. The current status split is 61 forecasting, one warming and six unavailable; the missing set is unchanged from the earlier 61/4/3 observation.

The joint model requires at least 48 mature exact-H1 training rows at 15-minute anchors, 12 rows with nonzero news context, and eight distinct context patterns. These are fixed readiness thresholds; they are not trade-profit filters. Changing them requires a new prospective model version and separate evaluation. Simply weakening a label cannot supply missing observed prices, original news availability, or past issued forecasts.

The four structural blockers are not resolved by a scheduler restart or an additional 61 consecutive minutes. They need qualifying mature data/context under this model, or a separately defined partial/price-only estimate. Cadence affects the next publication after readiness succeeds; no ready-but-unattempted missing pair is established by this observation.

Evidence: `MISSING_JOINT_FORECASTS_20260907T232846Z.json`; SHA-256 `a3704f70dbf79915dacb7c61b54fa9f7fef4f17250802d7c6723d987b29ba6fc`. All 16 joint and nine price-v2 registered source bindings matched before and after the audit. Worker health and counts retain their own clocks in the JSON.
