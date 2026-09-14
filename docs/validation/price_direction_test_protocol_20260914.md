# Price direction and magnitude test protocol

This protocol is the acceptance test for the deduplicated price-only feature
space. The archived source matrix remains immutable.

1. Use chronological development, validation, and holdout blocks. Fit all
   scalers, thresholds, and feature selection on development only.
2. Score two independent targets: signed direction and absolute move
   magnitude. Report balanced accuracy, AUC, Brier score, and calibration for
   direction; MAE and rank correlation for magnitude.
3. Define a magnitude gate on development data only, then test direction inside
   the top movement quantiles on validation and holdout. Report coverage so a
   high score from almost no trades is visible.
4. Convert predictions to executable outcomes using the recorded pair spread,
   native pip value, and slippage assumptions. Report mean net pips, win rate,
   drawdown, and the no-trade baseline.
5. Compare four cases on identical endpoints: no-change, ARIMA baseline,
   original feature set, and deduplicated feature set. No model is execution
   eligible unless the direction result, cost result, and stability agree on
   untouched blocks.

The existing evidence shows magnitude/cost-clearance signal but no stable
direction edge. This protocol is designed to test whether that signal becomes
useful when direction is conditioned on large-move regimes.
