# Alignment integrity batch 01

Started September 21, 2026 under `alignment_integrity_v2`. This is the first
implementation step in the design-aligned path, not a model fit or a completed
research campaign.

## Delivered

Fresh isolated source: `C:\Users\zmoor\Documents\forex\stage_c_alignment_integrity_v2`.
The retained `stage_c_all68_20260921` source and outputs were not changed.

- `contracts.py` supplies explicit `TrainingView` selection, forecast issuance
  independent of future endpoint support, immutable forecast-only records and
  separately linked outcome records.
- `calendar_contract.py` versions the existing midnight-UTC weekday diagnostic
  and fails closed for a true market-session target until session/holiday data
  are bound.
- `publication.py` supplies canonical dependency identity, exclusive ownership,
  immutable payloads, completion-last publication and tamper/mismatch refusal.
- `verify_alignment_integrity.ps1` checks a native Python exit code before it
  can print success.

## Verification

The isolated PowerShell verifier completed successfully on September 21:
`10 passed in 0.14s` after the final future-label regression and launcher check.
It runs no model fit, history extraction, network access, paid call, broker
action or service operation.

Mapped synthetic cases: TST01, TST05, TST07, TST10, TST32, TST33, TST48–TST51
and TST54. The tests demonstrate this small repair layer's contracts. They do
not certify all 56 design tests, adaptive replay, a calendar/session target,
historical accounting, model evidence or portable Stage C restoration.

## Next implementation within this batch

1. Build the first v2 all-68 runner on these contracts in a new run identity,
   with all forecast coverage emitted before scoring.
2. Bind an actual source/code/config dependency set, add heartbeat/lease policy
   and package a small completed fixture for relocated restore.
3. Run the bounded data/forecast slice only after those checks pass; v1
   diagnostic artifacts remain preserved and cannot be overwritten or reused as
   v2 issuance records.
