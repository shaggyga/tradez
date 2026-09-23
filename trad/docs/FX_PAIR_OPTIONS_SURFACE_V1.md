# FX pair-options surface V1

Implementation status: **schema and integrity validator only**. There is no
immutable collector, append-only options ledger, connected source, collection
worker, or collection-readiness claim. A future permitted source must open a
new material cohort and add those components before any observations exist.

## Why the pair itself matters

For expected volatility alone, a USD-leg options benchmark can be useful as a
currency-level uncertainty proxy. For pair direction/asymmetry, 25-delta risk
reversal, event pricing, and cross-pair term structure, the preferred input is
an options surface on the actual pair and tenor.

Two USD-leg surfaces are not a direct cross-pair surface. Cross variance also
depends on their timestamp-matched implied correlation/covariance. The project
therefore labels every OANDA pair as one of:

- `direct_pair_surface`
- `inverted_direct_surface`
- `two_leg_proxy`
- `unavailable`

It never upgrades a proxy to direct evidence.

## Current official-source reality

CME's official CVOL documentation covers AUD/USD, CAD/USD, CHF/USD, EUR/USD,
GBP/USD, JPY/USD and MXN/USD. Normalized to the OANDA universe, that is seven
of 68 pairs: AUD/USD, EUR/USD and GBP/USD directly, plus USD/CAD, USD/CHF,
USD/JPY and USD/MXN after inversion.

CME documents CVOL, ATM volatility, UpVar, DownVar, skew and convexity as
30-day indicators. Its CVOL skew is not the OTC 25-delta risk reversal. CME
also documents CVOL EOD/history delivery through its REST/DataMine products;
the project has no permitted connection and does not scrape the public
view-only visualizer.

Official references:

- <https://www.cmegroup.com/market-data/cme-group-benchmark-administration/cme-group-volatility-indexes.html>
- <https://www.cmegroup.com/market-data/cme-group-benchmark-administration/cme-group-volatility-indexes-faq.html>
- <https://www.cmegroup.com/market-data/market-data-api.html>
- <https://www.cmegroup.com/markets/fx/fx-product-guide.html>

## Frozen measurement rules

- Keep source, publication, first-seen, retrieval, effective and decision times.
- Keep raw and OANDA-normalized pair orientation.
- Retain both raw/source and normalized directional metrics. Inversion preserves
  ATM/BF, swaps up/down variance, and flips directional skew/RR sign; the
  validator proves that relationship rather than trusting an `inverse` label.
- Keep tenor, expiry, delta convention and volatility quote convention.
- Missing values are null, never zeros.
- A square-root-of-time ATM value is only a constant-volatility expected-move
  proxy. It is not event-implied movement.
- Event-implied movement requires an event-linked expiry/straddle or a
  preregistered expiry-stripping method.
- IV level describes magnitude/uncertainty, not direction.
- Options remain shadow-only and cannot confirm, promote, authorize, or trade.

The implementation is in
`src/forex_system/ingestion/fx_pair_options_surface_v1.py`; its frozen contract
is `config/fx_pair_options_surface_v1.json`.
