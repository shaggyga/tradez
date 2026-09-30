# Batch 05 acceptance scope

Design sections 17, 18, 21, 22 and 25; R08/R10/R11/R13, WP2/WP8/WP10.
The full original design and historical coverage map remain unchanged. This
addendum records bounded implementation evidence, not blanket closure of IDs.

| Tests / requirement | Evidence and boundary |
|---|---|
| TST16/17 | EUR/USD and USD/JPY exact PnL fixtures, explicit conversion-side checks, no duplicate spread-fee field. |
| TST18 | Partial fills/closes, explicit financing, used/reserved capacity, every-event independent arithmetic; reference/optimized equality on seven stress tapes plus long/short/slippage/outage cases. Optimized kernel shares lifecycle/risk code; full campaign-scale optimization is open. |
| TST19-23 | Separate causal intent, activation, trigger and fill; next-price stop gap; limit touch does not imply fill; OCO cancellation can arrive after both fills. These are explicit synthetic quotes, not inferred historical intrabar execution. |
| TST24 | Missing/stale conversion rejects exact economics; known liability/profit side retained. |
| TST29/31 | Independent accounting/order/financing arm state and pending/filled currency concentration checks. Policy cooldown, learner memory and event dependence integration remain open. |
| TST47/48/49/51 | Duplicate fill after restored state; real process deaths at six boundaries for both engines; exact deterministic payload parity; changed identity/corruption refusal. |
| TST50/52 | Existing exclusive publisher tests remain in integrated suite. Compact accounting source, exact dependencies and two pinned predecessors replay in an unrelated root; operator source and approved recipe also packaged. |
| TST53 | Seven full event/admission replays; cost/delay/outage changes admitted fills. Model-conditioned replacement behavior is next work, not certified by fixed intents. |
| TST55/56 | No-trade/no-loss metrics explicitly undefined where applicable; real-money config rejected. |
| TST45 | No broker route in the synthetic interface; no operating-system security-isolation or live broker denial certificate is claimed. |
| TST12-14/25-28/30 | Full inverse-pair evidence mapping, unresolved-bar/censoring target contracts and HOLD/EXIT/REPLACE thesis/horizon/persistence/cash policy behavior remain dependent work. Existing component tests do not close the full consumer gate. |

Resource receipt measures actual tiny baseline and seven stress jobs including
process-tree memory; it is not a speedup or large-campaign capacity claim.
The 245-test integrated gate predates only addition of the approved recipe JSON
to the checkpoint inventory. Five package tests were rerun for that addition,
and the final exported bytes were restored and tested. Both receipts are retained.

No trained-model comparison, GPT/advisor comparison, historical policy promotion,
new broker authorization or cloud-sync verification occurred. Lower-model
operation is an engineered interface; the selected cheaper model still needs a
supervised operational trial before its own reliability is called evaluated.
