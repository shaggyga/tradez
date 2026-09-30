# Batch 05: accounting foundation and lower-model operation

Current package: [ACCOUNTING_EVENTS_20260921_213557](ACCOUNTING_EVENTS_20260921_213557/README.md). Full design readiness remains false.

Completed: partial-fill accounting, financing, margin/reservation and currency exposure,
explicit resting-order lifecycle, isolated arms, independent arithmetic checks, batched
close parity, strict source identity, interrupted-run recovery and a portable checkpoint.
Added deterministic operator commands with structured outcomes and documented escalation;
every later work package must expose the same bounded operating contract.

Passed: 245 integrated tests; 7 synthetic stress replays; baseline reference/
optimized economic payload equality; relocated baseline replay and included tests.
See [verification evidence](ACCOUNTING_EVENTS_20260921_213557/evidence/final_integrated_gate.junit.xml),
[actual run receipts](ACCOUNTING_EVENTS_20260921_213557/evidence/FINAL_RUNS.json), and [restore](ACCOUNTING_EVENTS_20260921_213557/evidence/RESTORE_RECEIPT.json).

Limitations: synthetic fixture only; explicit financing/capacity assumptions; no historical
trading result, fitted-model evidence, demo authorization or full optimized campaign.
Lower-model compatibility is engineered into the software; no specific model comparison
was run. GPT/advisor comparisons remain deferred. Existing trad edits/accounts/services preserved.

Next queue item: Step D / WP2-WP8: integrate the recovered policy manager with the verified event ledger; preserve original/current thesis and compare HOLD/EXIT/REPLACE from common executable wealth and continuation horizon, with isolated arms, event-level checks and a frozen operator recipe.
The [complete queue](ACCOUNTING_EVENTS_20260921_213557/PATH_FORWARD.md) preserves all 37 pending IDs and all 68 instruments.
The [operator instructions](ACCOUNTING_EVENTS_20260921_213557/OPERATOR_CONTRACT_V2.md) describe routine operation.
