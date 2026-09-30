# Curve-shape chronological comparison

Status: complete same-task review; independent scientific review pending/nonblocking.

This package implements `forecast_curve_shape_layer_comparison_v2` using the preserved chronological forecast frames instead of the earlier zero-overlap retained-output path. It authenticates all 192 frame files through `CHRONOLOGICAL_LAYER_COMPACT_20260924_082500/BULK_EVIDENCE_REFERENCE.json`, binds labels through the chronological recipe's extension payload descriptors, and fits only the small curve-shape layer over preserved forecasts. No base model fit, API call, policy replay, broker/service action, D-drive access or live-bot change occurred.

## Support and result

- Complete matched three-variant panels: 2156 across 16 origins.
- Result SHA-256: `d89ca467267699dfb10c0e870453e29169bd9551ead04649768f00bf5ca6606c` (18309805 bytes).
- Learned curve rows: 946.
- Paired score rows: 54.
- Snapshot statuses: {'fitted': 14, 'insufficient_distinct_support': 34}.
- Overall MAE improvements versus controls: {'magnitude_interaction_expanding': 0, 'raw_matched_expanding': 0, 'signed_only_expanding': 0} out of {'magnitude_interaction_expanding': 2, 'raw_matched_expanding': 2, 'signed_only_expanding': 2} groups.

The result is negative at the aggregate control comparison level: curve-shape improved zero of the overall MAE groups versus raw-matched, signed-only, and magnitude-interaction controls. This is a completed development result, not a confirmation, policy, profitability or trading claim.

## Checks

- `test_curve_chronological_operator_v2.py` and `test_curve_shape_layer_v2.py`: 7 passed; pytest cache warning only.
- Real preserved-frame run completed with 192 authenticated frames and 51744 source prediction rows.
- Support proof read all frame hashes and found no missing files or hash failures.

## Readiness

- engineering_ready: false
- forecast_evidence_status: curve-shape development result negative overall MAE versus controls
- policy_evidence_status: not evaluated; no policy replay
- demo_authorization_status: not granted

## Next

Continue the timed queue with `rich_feature_family_incremental_comparison_v2` reuse/support check. Do not repeat completed rich-family experiments unchanged.
