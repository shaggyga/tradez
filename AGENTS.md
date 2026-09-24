# Forex workspace handoff

Read FOREX_HANDOFF.md before resuming project work. The Forex Vault is the central
engineering queue, checkpoint/review record and larger documentation store. Resolve
current status through its DESIGN_ALIGNMENT_LATEST.json and CHECKPOINT_REVIEW_LATEST.json.
Do not treat historical claims at the top of old project logs as current status.

Use the existing trad/FOREX_PROJECT_LOG.md and trad/FOREX_PENDING_IMPROVEMENTS.md;
preserve older entries and user edits. Keep raw run outputs and active WORK_LOG.jsonl /
PENDING_CHANGES.md in the working step's local evidence directory. At each checkpoint,
publish the compact review packet to the Vault and add its link to the project log.
Both locations must reference the same step and exact code/evidence identities.

Follow the Vault's current CHECKPOINT_REVIEW.md and NEXT/operator prompt. Normal
CONTINUE mode completes one step and returns for review. Explicit timed instructions
govern their own loop. Passing tests and independent review are separate statuses.

The full engineering design remains the goal. GPT/advisor comparisons, paid calls,
broker/service/account actions and D-drive investigation remain outside the current
offline work. Reuse preserved models/evidence before creating replacements.

Shared reuse rule: read the Vault VAULT_FIRST_REUSE.md before any new implementation, fit or run. The Vault is the common brain for all local replicas. Discover and verify existing model/run identities before claiming work; missing local outputs require retrieval, not an automatic new fit. Preserve full runnable source in Git and exact artifact references in the Vault.

Before research or computation, read the Vault OPERATIONAL_READINESS_LATEST.json and
run the read-only preflight described in docs/OPERATIONAL_PREFLIGHT.md. Resolve
required operational/review findings before the scientific queue item. A passed
preflight does not grant permission to fit or trade. Use artifacts/reuse_catalog.json
and original run identities before creating new work; retain unknown/unavailable cases.
Read only the current handoff/queue and relevant history, not every old log by default.
The Git root is the whole workspace; trad/ is a component, not a separate repository.
Prior AGENTS bytes are preserved in docs/history/AGENTS_before_operations_20260923.md.

Timed forecasting requests follow docs/FORECASTING_CONTINUATION.md and the Vault
FOREX_FORECASTING_CONTINUATION.md. One blocked experiment or pending independent
review does not stop eligible sibling work. Record per-run identities without
freezing development; preserve the original deadline and document any early stop.

Before launching or ending a timed turn, follow docs/TIMED_RUN_CONTROL.md and invoke
tools/forex_timed_session.py. A saved deadline is not execution. Do not send a normal
final while authorized work and time remain; verify a continuation backend before
yielding. User stop is terminal until new authorization. Tests do not replace the
candidate's full acceptance gates, and failed review never becomes accepted by
copying prior pointer metadata.
