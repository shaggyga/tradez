# Practice trading review — September 9, 2026

Broker reconciliation observed 2026-09-10T01:33:50.218457+00:00 (September 9 evening Eastern). Scope is the newly authorized Practice007 trial only, beginning with USD 41.6042. Six broker-confirmed trades closed, all at losses; two additional AUD/JPY intents were refused before submission. No trades or pending orders remained at this fresh broker read.

Balance/NAV: **$41.6042 → $41.2867**. Realized balance change **$-0.3175 (-0.7631%)**. The six exact trade P/L amounts sum to the account balance change; all 26 transactions since the original baseline cursor were read. No financing or commission was charged on these six trades. Broker identifiers, original fills, close reasons and exact retained decision contexts are in BROKER_TRADE_RECONCILIATION_20260909.json; its ledger backup was captured using SQLite backup.

| Pair | Side | Units | Held | Realized P/L | Exit |
|---|---|---:|---:|---:|---|
| USD/JPY | Short | 166 | 6m 55s | $-0.0763 | Stop loss |
| CHF/JPY | Short | 134 | 52m 04s | $-0.0088 | Original H1 expiry |
| EUR/JPY | Long | 142 | 10m 31s | $-0.0532 | Stop loss |
| GBP/JPY | Long | 122 | 6m 58s | $-0.0473 | Stop loss |
| USD/CAD | Long | 414 | 0m 46s | $-0.1000 | Stop loss |
| HKD/JPY | Short | 647 | 0m 28s | $-0.0319 | Stop loss |

## Findings

- Trade win rate is 0/6; this is not automatically 0% original-H1 forecast accuracy. Five positions exited before the original target because their stops filled; CHF/JPY reached the original expiry. Exact original source-ledger joins verify 2 correct directions out of 5 available H1 outcomes (40%); HKD/JPY has no retained outcome at its21:55:30UTC target after the reboot. These small dependent samples do not establish population success rates.
- OANDA's twelve entry/exit fill transactions report $0.1824 total half-spread costs, approximately 57.45% of the net loss. These costs are already included in realized results, not another deduction. This reported spread component is not an exact reconstruction of every possible midpoint counterfactual.
- Stops were based on twice ATR14 from one-minute bars. Five stopped out; USD/CAD held 45.67 seconds and HKD/JPY 28.01 seconds. Entry spread occupied 62.5% and 74.6% of their respective initial fill-to-stop distances. Remaining room beyond the opposite executable quote was small. This exposes an unvalidated horizon/stop interaction; wider stops are not established profitable by this sample.
- Five of six trades involved JPY, sequentially: two long-JPY exposures, two short-JPY exposures, then another long-JPY exposure. This is concentration in the tested currency factors, not five simultaneous positions. Original side probabilities were only about 55.6%–61.4% and were not independently calibrated probabilities of a profitable stopped trade.
- Both AUD/JPY refusals still passed the latest pure policy assessment. Fresh prices changed the exact precommitted stop/order bound, so the immutable-order equality check correctly refused submission. Both claims nevertheless counted toward the conservative eight-attempt daily cap. Thus the day reached its cap with six actual fills, not eight trades.
- All observed stopped losses remained within their recorded modeled risk budgets, and the one original-time managed exit reconciled. Those execution checks do not establish predictive performance.

## Original H1 outcomes

Exact source-ID/hash joins distinguish reference-to-target midpoint direction from stopped-trade P/L. Last-column values compare actual entry fills with retained executable target quotes; they are price-only diagnostics, not replayed fills or hypothetical USD cashflows.

| Pair | Forecast | Reference-to-target pips | Direction | Actual entry-to-target executable pips |
|---|---|---:|---|---:|
| USD/JPY | Down | -0.05 | Correct | 0.5 |
| CHF/JPY | Down | 0.65 | Incorrect | -1.0 |
| EUR/JPY | Up | 3.05 | Correct | -5.4 |
| GBP/JPY | Up | -2.25 | Incorrect | -15.3 |
| USD/CAD | Up | -9.65 | Incorrect | -12.1 |
| HKD/JPY | Down | Missing | Missing | Missing |

Four of five known executable entry-to-expiry price changes were negative. Removing/widening stops is therefore not established as a repair. USD/JPY's correct H1 sign amounted to only0.05pip versus a predicted7.175-pip fall; EUR/JPY rose3.05pips versus a predicted7.357-pip rise. Five-point mean Brier loss was0.26979 against0.25 for constant50%, with insufficient independent observations to establish calibration.

Several targets after21:00UTC/5p.m.Eastern had wider recorded spreads: EUR/JPY15pips versus3 at entry, GBP/JPY24.6 versus3.1, and USD/CAD8.1 versus2. The entry cost calculation used current spreads; it did not forecast those later increases.

[Original forecast verification](C:/Users/zmoor/Documents/forex/practice_trade_review_20260909/forecast_review/FORECAST_TRADE_REVIEW_20260909.json), [independent cashflow verification](C:/Users/zmoor/Documents/forex/practice_trade_review_20260909/independent_math/INDEPENDENT_TRADE_MATH_V2_CLOSURE_20260909.json), and [cost presentation V2](C:/Users/zmoor/Documents/forex/practice_trade_review_20260909/COSTS_AND_ENTRY_REVIEW_V2.json) are retained. V2 corrects an initial analysis-only HKD/JPY pip display to its original broker metadata; USD amounts, spread/stop ratios and live policy were unchanged. The earlier file is preserved.

## Runtime interruption

The worker status is dated September 9 21:36:57 UTC / 5:36:57 p.m. Eastern. A separate read-only runtime audit found a Windows shutdown/reboot at approximately 21:37 UTC / 5:37 p.m. Eastern. The trial worker and watchdog did not resume; only the dashboard was found restarted among the examined Forex services. The watchdog implemented process-crash recovery but had no machine-boot startup registration. This is an operating gap in the setup that had been left to run through Friday. The fresh broker check independently confirms the account is flat. See [the runtime audit](C:/Users/zmoor/Documents/forex/practice_trade_review_20260909/runtime/PRACTICE_TRIAL_RUNTIME_REBOOT_DIAGNOSIS_20260909.md) for exact process/event evidence. Exact shutdown initiator was not established. The retained source heartbeat saying enabled is stale, not current liveness.

## Research implications

The next comparison should separate original H1 prediction quality from stop timing, costs and currency-factor concentration. Preserve the current run and original decision clocks; compare horizon-consistent stop/no-stop fixed-expiry behavior on the same prospective opportunities before treating a wider stop or another threshold as an improvement. Reusing the audited cost-survival and currency-strength work remains more defensible than adding another duplicate model. These are research directions, not changes made during this review.

This review performs read-only broker/account checks and analysis. It does not restart trading, change thresholds, fit models, alter original outcomes, or place an order. Original six-trade results and failed/refused intents are retained.
