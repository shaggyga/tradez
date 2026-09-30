# Chat coordination board bootstrap review

Status: documentation-only task completed on 2026-09-22 at 23:23:23 UTC.

## Purpose

Add a small shared ownership board that every chat reads and updates before and after project work. This fills the pre-edit reservation gap left by the existing post-implementation checkpoint/review process.

## Changes

- Added `projects/forex/CHAT_COORDINATION_BOARD.md` with claim, conflict, status and handoff rules.
- Made the board mandatory entry reading in `projects/forex/AGENTS.md`.
- Added a prominent board link to `projects/forex/README.md`.
- Recorded this task's baseline, milestones and remaining adoption issues locally.

## Boundaries

No scientific source, model, data, broker state, runtime, design pointer or review queue was changed. Another chat appeared to be working on the macro provenance chain, so `DESIGN_ALIGNMENT_LATEST.json`, `REVIEW_QUEUE.json` and that package chain were treated as reserved and left untouched.

## Verification

- Board link found in `README.md`.
- Mandatory claim/close rule found in `AGENTS.md`.
- Board contains the bootstrap terminal row and an observed-external warning for the active macro work.
- Design pointer and review queue hashes remained unchanged during the task.

## Readiness

- Engineering readiness: not applicable; documentation coordination only.
- Forecast evidence: unchanged.
- Policy evidence: unchanged.
- Demo/live authorization: unchanged and not granted by this task.

## Handoff

At the beginning of every future Forex task, add a `CLAIMED` row to `CHAT_COORDINATION_BOARD.md` before editing. At completion, update that same row with terminal status, actual files, verification and next action. Detailed scientific evidence continues through the existing checkpoint and review system.

