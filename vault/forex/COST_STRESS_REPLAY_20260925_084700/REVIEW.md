# Behavior-changing cost stress replay

Created: `2026-09-25T08:46:50.432132Z`  
Source commit: `b977d245bcaf39e226c3eec2d993c03d60cfc6a6`

Ran the existing prespecified synthetic full-path stress replay. This is offline accounting/policy-fixture evidence, not broker execution and not historical profitability evidence.

## Result

- Scenarios completed: 7/7
- Fill/admission path changed for: fees_10000x, capital_100, latency_120s, entry_quote_outage, entry_spread_wide
- Independent accounting audit status: verified for every scenario
- Models fitted: 0
- Network calls: 0
- Broker actions: 0

Financing 5x changed financing amounts without changing fill path, preserving the distinction between cost magnitude and behavior-changing admission/fill effects.

## Exact next

`currency_concentration_support_census_v2`.
