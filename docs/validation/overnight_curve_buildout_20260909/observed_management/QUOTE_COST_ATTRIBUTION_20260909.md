# Quote-based paper loss decomposition — 2026-09-09

All five arms reconcile exactly to the independently verified first completed episode. The arms are separate hypothetical $2,500 notional scenarios, not a combined account.

| Arm | Price movement, USD | Spread cost, USD | Assumed slippage, USD | Realized, USD | Round trips |
| --- | ---: | ---: | ---: | ---: | ---: |
| curve_hold_no_rotation | -0.746415 | 0.304095 | 0.04998446375 | -1.10049446375 | 1 |
| legacy_momentum_reference | -0.175285 | 9.941775 | 1.54944770285 | -11.66650770285 | 31 |
| no_trade | 0 | 0 | 0 | 0 | 0 |
| usd_curve_manager | -0.958880 | 1.584810 | 0.24992200590 | -2.79361200590 | 5 |
| usd_momentum_manager | 0.884415 | 4.791135 | 0.74970533365 | -4.65642533365 | 15 |

Every closed trade retains its original entry and exit quote IDs, bid/ask values, midpoint, units, side, execution price, and market/availability/action clocks. Gross signed midpoint movement minus both half-spreads and both slippage charges equals retained realized USD exactly.

The old momentum policy traded 31 round trips and lost primarily to costs. The USD momentum policy had positive gross price movement but negative net results. The curve policy reduced costs, while its gross movement remained negative and its net result was worse than hold or no-trade. This is diagnostic evidence from one episode, not proof of a profitable policy.

Validation: 21 tests passed, independent source review closed, and the actual first episode replay reconciled. Original managers, parameters, studies and broker permissions remain unchanged. Open-entry costs are listed separately from realized trades; hypothetical liquidation marks are never included as action legs.

Evidence: COST_ATTRIBUTION_IMPLEMENTATION_VALIDATION_20260909.json and COST_ATTRIBUTION_CANDIDATE_EPISODE01_20260909.json.
