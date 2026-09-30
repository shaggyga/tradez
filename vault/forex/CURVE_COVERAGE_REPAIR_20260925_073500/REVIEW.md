# Curve coverage repair — forecast_curve_shape_layer_comparison_v2

Status: accepted within scope by same-task review; independent scientific review remains separate and nonblocking.

This packet repairs QREV-03 for the prior curve-shape chronological comparison. The prior result already established a negative aggregate development diagnostic, but it only reported the 2,156 complete matched panels. This repair preserves that negative result and adds the full all-68 slot ledger required by the acceptance gate.

## Coverage repair

- Expected population: 24 origins × 68 instruments × 2 base methods = 3,264 origin/instrument/base slots.
- Complete slots: 2156.
- Incomplete slots: 1108.
- Missing constituent forecast reasons: {'base_unavailable': 26592}.
- Full slot ledger: `CURVE_CHRONOLOGICAL_RESULT_REPAIRED.json::all68_coverage.slots`.

## Scientific outcome

The prior negative outcome is preserved. The repaired run produced 946 learned rows and 54 paired score rows, with zero overall MAE improvements versus raw-matched, signed-only and magnitude-interaction controls: `{'magnitude_interaction_expanding': 0, 'raw_matched_expanding': 0, 'signed_only_expanding': 0}` out of `{'magnitude_interaction_expanding': 2, 'raw_matched_expanding': 2, 'signed_only_expanding': 2}`.

This is an offline development diagnostic only. It is not a confirmation result, policy replay, profitability claim or trading-readiness claim.

## Checks actually executed

- Focused tests: 9 passed; pytest cache warning only.
- Preserved-frame rerun: 192 authenticated frames, 51744 preserved source prediction rows.
- All-slot ledger: 3264 expected slots accounted; incomplete slots explain their missing constituent forecasts with preserved coverage reasons.

Next action: `rich_feature_family_incremental_comparison_v2` — reopen feature-family mapping/support inventory, map actual feature families and prior experiment identities, and select a materially untested supported comparison.
