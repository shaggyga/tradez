# Active joint vector: a small arithmetic walkthrough

This is a fixed synthetic illustration of feature arithmetic, not observed market data, a fitted prediction, an evaluation, or a trading example. Positional definitions and source hashes are in `docs/feature_dictionary/active_joint_34.json` in the project, or `parts.active_joint` in the vault's `FEATURE_DICTIONARY_CURRENT.json`.

## What enters the model

The raw vector contains 24 price fields, eight news fields, and two products. The price fields have documentation-assigned names because their source implementation is positional. Only the eight news names are literal source names.

For a nominal price window of w minutes, the four fields are:

1. Endpoint change in pips divided by actual elapsed minutes.
2. Actual elapsed span divided by w.
3. One minus the observed interval count divided by w.
4. A flag equal to one when fewer than two closes are available.

This quartet repeats for w = 1, 5, 15, 30, 60. Four one-hour/session statistics follow: elapsed-time RMS change, maximum gap, observed-close count divided by 61, and retained-session age. News and the two products follow those 24 fields.

## A sparse five-minute example

Let the pair pip size be 0.0001. The following timestamps are minute starts; their closes become available no earlier than one minute later.

| Minute start | Synthetic close |
|---|---:|
| 12:00 | 1.10000 |
| 12:01 | 1.10010 |
| 12:03 | 1.10020 |
| 12:05 | 1.10015 |

At anchor 12:05, the five-minute window contains four closes and three observed intervals. Its endpoint move is 1.5 pips across five elapsed minutes. The five-minute quartet is therefore:

| Field | Calculation | Raw value |
|---|---|---:|
| price_rate_5m | 1.5 / 5 | 0.3 pips/minute |
| price_span_fraction_5m | 5 / 5 | 1 |
| price_missing_fraction_5m | 1 - 3 / 5 | 0.4 |
| price_unavailable_flag_5m | four closes >= two | 0 |

The one-minute subwindow contains only 12:05: it does not reach backward to 12:03. Its quartet is (0, 0, 1, 1). That first zero is a missing-rate sentinel, not evidence of a flat market. The example's three returns and five-minute span fail joint readiness (at least eight returns and fifteen minutes); this illustrates arithmetic only and could not produce a forecast.

## Context is not vetted direction

Assume a separate, valid synthetic news frame has two already-classified, timestamp-admissible, distinct-headline context members relevant to EUR_USD:

| Member | EUR score | USD score | Pair difference | Age from original knowledge |
|---|---:|---:|---:|---:|
| A | 0.6 | 0.2 | 0.4 | 0.1 hours |
| B | -0.2 | absent, projected as 0 | -0.2 | 0.3 hours |

The four context fields are:

- context_balance = (0.4 - 0.2) / 2 = 0.1.
- context_volume_log = ln(1 + 2) = approximately 1.098612.
- context_signed_fraction = (1 - 1) / 2 = 0.
- context_mean_age_hours = (0.1 + 0.3) / 2 = 0.2.

The zero signed fraction means equal counts of positive and negative members, not no news. Assume neither member's claim passes the independent directional admission rules. The four vetted fields are then (0, 0, 0, 0), while context remains nonzero. This is not an assertion that either member should pass or fail any real claim-level guard.

A source has to pass its own arrival-time, reaction-expiry, publisher-identity and same-claim requirements to enter vetted aggregates. Multiple feeds of one syndicated headline are not independent corroboration. The dictionary traces the exact weight, deduplication and score rules. Plain research_currency_scores and unscored context text are not substituted for the original member currency_scores.

## Products and normalization

The two products use the raw one-minute price rate, not the five-minute rate: price_rate_1m * context_balance and price_rate_1m * vetted_balance. With the sparse one-minute example both are zero. The original availability/coverage fields remain in the vector.

Products are formed before standardization. As a separate fixed normalization illustration, if a training column's mean is 0.05 and its population standard deviation is 0.2, a raw query value of 0.1 becomes (0.1 - 0.05) / 0.2 = 0.25. These are invented normalization constants, not fitted values from the project. The actual model learns its means/scales only from eligible training rows; a scale below 1e-9 becomes 1.

An unpenalized intercept is added after standardization; it is not a 35th feature. Ridge fits all coefficients jointly. No feature definition fixes a conversion from news scores into pips.

## Keep the clocks and comparisons separate

Historical news at price anchor t is reconstructed as of t+60 using original source knowledge and separately gated mapping/observed availability. Live news uses the actual independent consumer observation. The latest price close can be up to 900 seconds old; current news evidence can be up to 300 seconds old, with any earlier original vetted expiry still binding. A later mapping, refresh or consumption cannot renew the original reaction window.

No-context news is the documented raw vector (0, 0, 0, 1, 0, 0, 0, 0), not eight zeros. A stale or unverifiable snapshot is unavailable and blocks capture rather than pretending to be that neutral state.

The neutral-news comparison uses the same fitted joint model and unchanged price features with that neutral vector, re-forms the products, and applies the same fitted normalization. A separate matched price-only model is fitted on the same anchors. Neither comparison establishes causal news impact or profitability.
