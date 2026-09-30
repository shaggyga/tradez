# Forecast tape reseal and matched-support review

## Result

`FORECAST_TAPE_20260925_013200` is superseded for downstream comparison. Its
initial tape hash was calculated before the authenticated parent provenance block was
added, so the final file did not validate against its own declared `tape_sha256`.
It was not used for a comparison. The original bytes remain retained for audit.

The resealed V2 tape includes the parent block before fingerprinting and uses curve
result V3, whose learned forecasts retain `horizon_minutes`. The corrected tape
validates its own identity and provides exact source intersections without loading
outcomes or fitting any model.

## Evidence

| Item | Identity |
|---|---|
| Source commit | `2c3563c855496541e6c0535a5c4b64ae6e2f02f0` |
| Resealed tape file SHA-256 | `edbea82508edcf6d7721f105c5e7a326549335beab01de287a707893570a4985` |
| Resealed tape identity SHA-256 | `b0391fa400973a079402301f41c94a862ed8f129ffc8bcece3d891fded89a83a` |
| Curve V3 file SHA-256 | `854d3864c165f6a21bf389ce9f4966ad22a3ebc6128ef473e7ccf5e6d843b5a7` |
| Reseal summary SHA-256 | `fab69e9d74bde67b8ad4c8b5f186f1698e95f0ec5ab4c2e65fb0dfc03b72c19f` |

The tape has 56,476 records from 17,248 projection-parent forecasts, 3,786
residual-layer forecasts, and 946 curve-shape forecasts. It covers 68 instruments
and 16 origins. Exact intersections on `(record_id, target_id, base_method,
horizon_minutes)` are: parent/residual 3,786; parent/curve 946; residual/curve 946.
Each of those intersections covers 68 instruments and seven origins.

Focused regression validation passed: 19 tests across forecast-tape, curve-shape,
and residual-layer suites. No base-model fit, API call, broker/service/account action,
policy replay, or live-bot change occurred.

## Status and next action

This fixes forecast identity and support provenance. It does not accept any forecast
candidate, use outcomes, or establish economic performance. Independent scientific
reviews for residual and curve packages remain separate and nonblocking. The next
eligible work is the design-selected forecast comparator over this validated tape,
with outcome handling and acceptance gates frozen before scoring.
