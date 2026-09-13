# First isolated horizon comparator: inference compatibility passed

The existing second-ridge model produced **13 EUR/USD horizon outputs twice, identically**, using its original preserved source and JSON coefficients. Every raw and magnitude-calibrated prediction matched an independent 80-digit Decimal standardized-coefficient calculation. Maximum absolute discrepancy was **4.44×10⁻¹⁶ pips raw** and **1.11×10⁻¹⁶ pips calibrated**. Independently calculated logistic probabilities also matched within tolerance. No fitting occurred.

This completes the first **engineering inference compatibility** milestone proposed in `BACKLOG_RECONCILIATION_20260908.md`. It does not complete accuracy, source-availability, original fit reproducibility, live-readiness or after-cost acceptance.

The model was read from `D:/ForexRecovery/revamp_20260908T1353Z/artifacts/legacy_model_components/state/second_ridge_models_v1.json`, SHA256 `85ff37fd77f85fa6ae990990ead19eb7d4e20389d99b774023f32c13bf62658c`. Imports came only from the preserved source copy under `C:/Users/zmoor/Documents/forex/revamp_baseline_20260908/workspace/trad`; both original module hashes were verified before and after execution. Only the pure `feature_matrix` transform and `SecondRidgeSnapshot` inference class were invoked. SQLite connections were explicitly blocked in the smoke process; no Store, forecast runtime, fitting, broker or ledger functions were invoked.

The original 9,146,916-byte EUR/USD S5 file was copied exclusively to `D:/ForexRecovery/revamp_20260908T1353Z/inputs/second_ridge/EUR_USD_S5.parquet`, preserving timestamps and matching source/copy SHA256 `9c39400b9090d6c23133f9b7a6a47a540945479ac10fd36d0f05d97dc7384484`. Its last observation is **July 22, 04:03:05 UTC**, before the model's **July 31, 15:44:22 UTC** fit. The selected feature row is **July 22, 00:01:35 UTC**, chosen as the latest eligible row under the original transform and four-hour maximum horizon. This is pre-fit historical input, potentially in-sample; exact training membership was not reconstructed. There is no historical issue-time or out-of-sample claim, and no outcome was scored.

The original transform emitted a NumPy warning about timezone representation when converting retained timestamp **strings** to `datetime64`. A separate clock check parsed the original UTC strings and verified that first/last epochs and the uniquely selected feature timestamp match the transformed values. Its initial verifier incorrectly assumed Arrow returned datetime scalars; that verifier-only `AttributeError` and original helper were preserved before correcting it. The product source and successful inference receipt were unchanged, and models were not rerun for this check.

Saved diagnostics omit execution profiles and force research-only true, account/order/authorization/promotion/proof eligibility false. The retained model's original profiles and eligibility fields remain unchanged in its preserved artifact; they grant no authority to this replay.

Evidence:

- `SECOND_RIDGE_ENGINEERING_SMOKE_20260908.json`: SHA256 `93174fab0c8f0edba3c86b3995f8bae3384785a254d30f568eb1d126e5bc2aa1`.
- `SECOND_RIDGE_ENGINEERING_CLOCK_CHECK_20260908.json`: SHA256 `65c70517701679db3d8e13a5dce411ffb09e19fc2eb66937c3f0f68b24019b0f`.
- `SECOND_RIDGE_CLOCK_VERIFIER_INITIAL_ERROR_20260908.json` preserves the verifier-only failure; `verify_smoke_clock_initial.py` retains its original source.

The broader MA comparator and unified v4/v5 compatibility tasks remain open. A successfully restored curve is useful baseline infrastructure, not evidence of profitable forecasts.
