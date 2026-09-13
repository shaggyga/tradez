# Episode 02: why the USD curve manager lost while fixed hold won

The manager followed its frozen rules. It closed a profitable short, subsequently entered long against a falling market, and held the long to the original deadline. Fixed hold retained the original short. The main difference was position direction and holding period; incremental transaction costs explain a smaller part. No arithmetic or action-selection mismatch was found in the 59 retained decisions.

This is a diagnosis of one already observed GBP/USD virtual episode on September 9, 2026. It is not a policy-selection experiment, broker profit, or evidence that fixed hold generally wins.

## Exact comparison

| Arm | Completed round trips | Gross midpoint P&L, USD | Spread cost, USD | Assumed slippage, USD | Net virtual P&L, USD |
|---|---:|---:|---:|---:|---:|
| USD curve manager | 2 | -2.027575 | 0.644875 | 0.09991636815 | -2.77236636815 |
| Curve hold, no rotation | 1 | +3.656370 | 0.322350 | 0.04993450170 | +3.28408549830 |

Both arms **did share the exact first entry**: short 1,842 GBP at the same original quote, side, hypothetical execution price and actual observation time. They did not share subsequent entries. The manager later entered a separate 1,843 GBP long; the hold arm remained short. Their final exits used the same observed quote but opposite executable sides.

Hold minus manager is exactly **$6.05645186645**: $5.683945 from gross midpoint path/position differences plus $0.37250686645 of additional manager costs. The manager recorded no atomic `rotate` action: it exited, waited, and later entered the opposite side. Thus this particular loss is not primarily a many-rotation churn example.

## The original forecast and the four action events

The episode retained one H1 node, issued at 07:42:59.957 UTC, with original reference price 1.35629 at 07:42:55 and nominal terminal time **08:42:55 UTC**. Its predicted signed move was only **-0.7231159329455111 pip**, giving a fixed terminal estimate of **1.35621768840670544889**. Every available curve candidate throughout the episode carried that same original node, target and terminal estimate. There was no fresh model fit or revised terminal forecast.

The adapter computes remaining pips as `(original terminal estimate - current decision midpoint) / 0.0001`. The value changes sign when the market crosses the fixed estimate. Prices used to make the decision and prices observed later for virtual accounting are retained separately.

| Step / actual action observation, UTC | Original decision midpoint | Remaining move used | Action and declared reason | Result / continuation evidence |
|---|---:|---:|---|---|
| 002 / 07:47:01.409 | 1.356435 | -2.173116 pips | Both arms enter short: `highest_expected_net_usd` | Manager and hold use 1,842 GBP. Prospective gross $0.400288 less round-trip cost $0.363111 leaves only $0.037177 expected net. Actual short execution 1.35633643565. |
| 004 / 07:49:01.414 | 1.356155 | +0.626884 pip | Manager exits short: `exit_dominates_in_usd`; hold retains it | Existing-short continuation gross -$0.115472; hold value -$0.297022 versus exit value -$0.181550. A new long fails the entry cost hurdle, so no immediate rotation. Actual later midpoint 1.35616; manager closes at 1.3562635616, realizing **+$0.13423400010** after $0.37231599990 costs. |
| 007 / 07:52:01.401 | 1.355825 | +3.926884 pips | Manager enters long: `highest_expected_net_usd`; hold stays short | With the market farther below the unchanged terminal, prospective gross $0.723725 less $0.363286 costs leaves $0.360439 expected net. Actual long execution 1.35592355825, 1,843 GBP. There is no manager incumbent here: it is flat before entry. |
| 058 / 08:42:55.136 | 1.35445 | No curve candidate used at terminal | Both arms exit: `predeclared_terminal` | Manager long sells at 1.3543464555, realizing **-$2.90660036825** after $0.37247536825 costs. Hold short buys back at 1.3545535445 and realizes **+$3.28408549830** overall. |

All action plans were computed and published before the selected actual quote observation. The fixed terminal exit does not invent a final forecast. The terminal pricing tick had market time 08:42:52.440 and was actually read at 08:42:55.136; it is an observed pricing-quote diagnostic, not an exact target-time candle or broker fill.

## Why the long remained open

The manager computes an existing position's continuation using its **existing side and units**, independently of whether a fresh entry clears costs. It compares that continuation less liquidation cost against immediate liquidation. This behaved correctly at step 003: it held the short even though opening a new short would have failed the round-trip hurdle. At step 004 the price had crossed below the forecast terminal; expected continuation for the existing short became negative, so exit dominated.

After long entry, steps 008–057 all retained the long. At 08:42:00, only 54.857 seconds remained; current decision midpoint was 1.35454. The unchanged endpoint now implied **+16.776884 pips** of recovery. For the existing 1,843-unit long, expected gross continuation was $3.091980, hold value $2.901146, and immediate exit value -$0.190834. The frozen comparison therefore still selected hold. The subsequent observed terminal midpoint was 1.35445, so the expected recovery did not occur.

This exposes a limitation of the declared model-to-management rule: subtracting a new market price from an old unconditional terminal estimate creates a fixed-price recovery signal. It does not establish that a conditional recovery forecast has learned from the intervening decline. Original uncertainty remains explicit—residual standard deviation 8.584296 pips and original up probability 0.478953, both uncalibrated—but the USD selector does not use them as an adverse-excursion, loss-stop or remaining-probability gate. The original small downward forecast also substantially understated this terminal-quote decline; that magnitude miss and the fixed-terminal control assumption are distinct from an implementation bug.

## Source and verification boundary

The read-only helper verified exact accepted bytes for the registry, all 20 registered sources, the original published curve/publication, initialization evidence, all 59 plans and settlements, and the accepted terminal/cost reports: 145 bounded reads, 5,870,164 bytes. It independently checked remaining-move arithmetic, incumbent-side USD continuation, entry/hold/exit branches, actual stage clocks, and exact cost/arm reconciliation. These are reconciliation checks, not new pytest cases or a rerun of model evaluation. No runtime writes, orders, new model inference, fits or policy tuning occurred.

Relevant frozen source locations:

- `trad/oanda_curve_management_adapter_v1.py:101–105,125–156`: fixed-terminal subtraction, side and original uncertainty/event provenance.
- `trad/oanda_curve_management_replay_v1.py:422–438`: current quote based prospective gross/cost accounting.
- `trad/oanda_curve_management_replay_v1.py:483–530`: flat-entry hurdle, fixed hold arm, incumbent-side continuation, switch comparison and terminal/deadline rules.
- `trad/oanda_observed_curve_management_v1.py:151–180,243–264`: fixed original node binding and the same USD selector for both curve arms, with `hold_only` set for the comparator.
- `trad/oanda_observed_curve_management_worker_v1.py:100–107`: fixed $2,500 scenario, 0.1-bps slippage per leg, entry cost ratio 1, and $0.05 switching hurdle.

Accepted inputs: terminal report SHA `d80d6564fb65745328d8df2cf4c539e00a6760b5b7ad724e3c180a558cc782b9`; cost report SHA `b011111415e5f7d96e399da5dc81ae4f7177415c66164b292f50f94e150e2eef`. Detailed source-bound trace: `EPISODE_02_CURVE_POLICY_DIAGNOSIS_20260909.json`, SHA `9358ba9ea1a844dce67e5a30561ab5bc1e50cc95fc4aa6b5a86497e4c99d7106`.

The original training target permits the first real bar 0–7 seconds after nominal; this management experiment explicitly uses the nominal boundary approximation. All positions belong to separate research scenarios, with no claim about actual account balances, margin, fills or independent sample size. This diagnosis supports documenting the observed failure mode and testing a separately frozen proposal prospectively; it does not justify changing this active episode's rules after seeing its outcome.
