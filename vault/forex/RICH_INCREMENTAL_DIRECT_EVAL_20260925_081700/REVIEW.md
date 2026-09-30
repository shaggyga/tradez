# Rich incremental direct evaluation

Status: accepted within scope by same-task review; independent scientific review remains separate and nonblocking.

This repairs QREV-04 by reopening the rich-feature family candidate instead of treating the old compact38/compact50/full228 packets as blanket proof. The restored rich-family checkpoint reproduced 215 scientific payloads and passed relocated tests. No new candidate fit, API call, policy replay or trading action was performed.

## Feature-family map

- `compact38_cost2`: 40 features.
- `compact50_cost2`: 52 features; direct increment over compact38 adds 12 peer features.
- `full228_cost2`: 230 features; direct increment over compact50 adds 178 full-view features.

## Direct incremental result

- Peer12 compact50 minus compact38: MAE improved in 6/28 slices, MSE in 10/28, absolute bias in 15/28.
- Full178 full228 minus compact50: MAE improved in 8/28 slices, MSE in 7/28, absolute bias in 8/28.
- Support failures: 0.

This is mixed development evidence. It does not select a trading model and does not establish confirmation, policy value or live readiness.

Next action: `macro_event_text_incremental_value_assessment_v2` — assess the selected macro/event/text incremental hypothesis against exact available inputs and matched controls; do not close on inventory-only packets.
