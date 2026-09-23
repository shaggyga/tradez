# Corrected MA feature causality and real-history audit

The retained 643-feature MA builder has two causality defects: the fallback volatility scale can use later values, and an age with no observed crossover uses the length of the entire supplied dataset. Appending future data can therefore change an earlier feature row. The new `oanda_ma_causal_features_v2.py` corrects both in a separate version and uses one canonical batch path for live vectors.

## Real retained data

This audit captured roughly62,000 M1 rows per pair from the current archive, with unchanged-on-read file identities, exact raw hashes and actual capture clocks. The rows contain real recorded mid and bid/ask OHLC. Historical ingestion times are unknown; this is retrospective feature engineering, not proof that a forecast existed at a past clock.

Each uninterrupted minute segment initializes its own EMA history. Every eligible row after204 real rows was compared, without filling gaps. The following counts are feature changes under this explicit session-reset policy, not forecast losses:

| Pair | Eligible rows | Changed rows | Share | Distinct changed fields |
|---|---:|---:|---:|---:|
| EUR_USD | 47,168 | 12,451 | 26.40% | 56 |
| GBP_USD | 49,878 | 13,388 | 26.84% | 68 |
| USD_JPY | 50,669 | 14,915 | 29.44% | 56 |

Across the three archives, **40,754/147,715 (27.59%)** eligible feature rows changed. In36 selected real prefix checks, appending future rows changed13 original rows and0 revised rows. Controlled synthetic data also reproduces the defect; those synthetic counts are kept separate from real data.

At the latest full-archive row, the old batch builder and the revised batch builder matched for all three pairs. The old independent live-vector path differed in2 EUR/USD and2 GBP/USD fields at the declared numerical tolerance. The new vector path uses the canonical batch calculation. Truncating all available EMA history to204 rows changed137,133 and120 current fields respectively, so204 is a minimum warm-up count, not permission to discard older history silently.

## Implementation and verification

The helper verifies the exact old source hash, compiles only the listed constants and pure feature functions into a private namespace, and replaces the two defective helpers. It does not load any fitted artifact or mutate old sources. The fallback uses only strictly prior valid scales; the no-cross age uses current prefix length.

The focused suite passed35 tests, including exact prefix invariance across five timeframes and four price shapes, validation failures, original-source immutability and full EMA initialization. Independent source review found no blocker. Callers must still validate timestamps, completed bars, actual availability and gap policy; the pure price-vector helper cannot establish those facts itself.

**Retained fitted weights are not proven compatible with these changed features.** A separate, predeclared historical comparison is being prepared with train-only fitting and an untouched chronological test. This feature repair has not issued a forecast, changed a running worker, or enabled an order.
