# Forex chat coordination board

This is the shared, mutable coordination surface for parallel chats. It reserves work before edits begin and summarizes the result after each task. It does not replace `DESIGN_ALIGNMENT_LATEST.json`, `REVIEW_QUEUE.json`, sealed review packets, `PROJECT_LOG_CURRENT.md`, or scientific evidence.

## Required start-of-task protocol

1. Read this file, `AGENTS.md`, `DESIGN_ALIGNMENT_LATEST.json`, and `REVIEW_QUEUE.json` before changing project files.
2. Refresh this file immediately before claiming work. Never rely on a copy read earlier in the chat.
3. Check every non-completed row for overlapping scope or files. If overlap exists, inspect that task's handoff or choose non-overlapping work; do not silently take ownership.
4. Add one row with a stable task ID, chat/owner label, UTC start, short purpose, intended files or subsystem, and status `CLAIMED` before the first task edit.
5. Re-read the board after adding the claim. If a competing claim appeared, stop overlapping edits and record `BLOCKED_CONFLICT` or narrow the scope explicitly.

## Required end-of-task protocol

1. Change the same row to `DONE`, `HANDOFF`, `BLOCKED`, or `ABANDONED`.
2. Record the actual files changed, verification performed, remaining issue, and exact next action or review packet.
3. Do not delete old rows. Completed rows are the compact cross-chat history; detailed evidence remains in the normal checkpoint package and project log.
4. A stale `CLAIMED` or `IN_PROGRESS` row is not permission to overwrite work. Inspect its referenced artifacts and mark a takeover only with an explicit note explaining why ownership is being transferred.

## Status meanings

| Status | Meaning |
|---|---|
| `CLAIMED` | Scope reserved; edits have not materially started. |
| `IN_PROGRESS` | Work is actively modifying or testing the reserved scope. |
| `HANDOFF` | Partial work is safe to resume using the recorded next action. |
| `BLOCKED` | Cannot continue without a dependency or decision. |
| `BLOCKED_CONFLICT` | Another chat owns overlapping scope. |
| `DONE` | Task is finished within the stated scope; review status is recorded separately. |
| `ABANDONED` | Claim released without a usable partial change. |
| `OBSERVED_EXTERNAL` | Work was detected from another chat but no reliable owner claim existed yet. Treat it as active until reconciled. |

## Active work

| Task ID | Chat / owner | Started UTC | Description | Reserved scope / files | Status | Last update UTC | Handoff / next action |
|---|---|---|---|---|---|---|---|
| `observed-macro-provenance-20260922` | Mabel's existing Codex chat; inferred entry reconciled by that owner | unknown | Prior macro work was completed, not running at observation time | Prior macro package chain | `HANDOFF` | 2026-09-23T04:03:41Z | Reconciled against this chat's FOLLOWUP_REVIEW_20260922_205758 and completed local session. Provenance implementation had not started. User now requests four next steps; ownership explicitly continues under mabel-four-next-20260923. |
| `mabel-four-next-20260923` | Mabel's Codex chat 01a0c239-d0e3-7c53-a011-8911fccfa326 | 2026-09-23T04:03:41Z | Four queue steps: provenance implementation/review, next eligible implementation/review; plus read-only live bot review | stage_c_alignment_integrity_v2 current macro sources/evidence; Forex Vault pointers, queue and handoff docs; trad project logs. Live process/log inspection only, no runtime/broker mutations | `CLAIMED` | 2026-09-23T04:03:41Z | Preserve Geral's coordination additions. Publish checkpoint after each implementation, keep review separate, then close this claim with exact handoff. |
| `coordination-board-bootstrap-20260922` | Codex chat with Geral | 2026-09-22T23:21:55Z | Add a cross-chat claim/completion board and make it mandatory project entry reading. | `CHAT_COORDINATION_BOARD.md`, `AGENTS.md`, `README.md` | `DONE` | 2026-09-22T23:23:23Z | Board created and linked from mandatory instructions and project README. Scientific queue state was not changed. Future chats must claim and close work here. |

## Completed and handed-off work

Move no rows manually between sections if doing so risks a concurrent rewrite. It is acceptable to leave terminal-status rows in the active table temporarily; the status is authoritative. Periodic documentation-only maintenance may move terminal rows here using a narrowly scoped patch.

| Task ID | Chat / owner | Started UTC | Finished UTC | Description | Files changed | Final status | Verification / review packet | Remaining action |
|---|---|---|---|---|---|---|---|---|

## Conflict rules

- A file path, generated pointer, queue entry, study cohort, database, model artifact, or runtime process can have only one modifying owner at a time.
- Read-only inspection may proceed in parallel, but the inspecting chat must not publish a competing “current” pointer or reinterpret unfinished results as final.
- `README.md`, `AGENTS.md`, this board, `DESIGN_ALIGNMENT_LATEST.json`, and `REVIEW_QUEUE.json` are high-contention files. Re-read immediately before patching and keep edits narrow.
- If two tasks need the same file, the later task records `BLOCKED_CONFLICT` and waits for `DONE`/`HANDOFF`, or both chats explicitly divide non-overlapping sections and record that division here.
- Review of another task does not transfer implementation ownership. Record reviewer and implementation owner separately.
