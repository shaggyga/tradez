# Checkpoint logging and review

Start with REVIEW_QUEUE.json and CHECKPOINT_REVIEW_LATEST.json. The engineering design and work
queue still come from DESIGN_ALIGNMENT_LATEST.json. Implementation and review are separate:
passing tests does not automatically mean a checkpoint was independently reviewed.

## During each step

Before editing, create a unique step directory outside sealed packages. Record the selected queue
item, scope, acceptance conditions, UTC start, exact baseline hashes/snapshot and pre-existing edits.
Use a scoped snapshot when Git HEAD does not represent the dirty worktree. Do not sweep unrelated
user changes into the step's diff. Preserve new and deleted files as well as ordinary modifications.

Append durable WORK_LOG.jsonl entries at meaningful milestones: baseline captured, implementation
change, test or run started/completed/failed, blocker discovered and checkpoint published. Record
UTC time, event, affected paths, exact invocation or command-file reference, exit status, evidence
paths and next action. Keep raw stdout/stderr or relevant run journals. Do not record credentials,
private environment values, hidden reasoning or invented activity. Do not log every keystroke.
Keep pending issues in PENDING_CHANGES.md: finding ID, concrete defect/unfinished change, affected
files, reproduction, dependency, status and next action. Preserve resolved entries with evidence.

The work log is written by the working agent under this procedure. Existing run ledgers/journals
are emitted by the software. This documentation does not create a background monitor or guarantee
that arbitrary future edits will automatically be captured.

## At the checkpoint

Publish one immutable review packet containing REVIEW.md, REVIEW.json, FILE_CHANGES.json,
CHANGES.patch, WORK_LOG.jsonl, PENDING_CHANGES.md, baseline/after identities, source/config/input
hashes, actual checks and failures, four readiness fields, unresolved decisions, checkpoint path
and hash, rollback/restore and exact resume instructions. Reference existing sealed evidence by
hash instead of duplicating bulk data. Record any excluded or unrecoverable baseline explicitly.

Verify the packet inventory and links, then atomically update REVIEW_QUEUE.json and a current
pointer. Queue entries include step ID, implementation status, review status, packet path/hash,
unresolved findings and next action. Use one writer. Preserve predecessor queue snapshots and
packets. For interrupted work, leave status in_progress or blocked with an exact resume action.

In normal NEXT/CONTINUE mode, finish one coherent step, publish it as ready_for_review and return
to the user at the checkpoint. A separately authorized timed loop may continue eligible work,
publishing each checkpoint; it cannot turn pending review into acceptance. Record dependencies
on earlier unreviewed changes. Do not add an automatic model switch, subscription or API call.

User clarification, 2026-09-24: pending independent review alone must not halt an
authorized timed run. Perform a substantive same-task review when a separate reviewer
is unavailable, label it `independent_review=false`, resolve reproducible required
findings, and continue eligible offline work. Keep external review pending and record
dependencies explicitly. Do not manufacture a mandatory independent-review queue gate
solely because the implementer cannot claim independence. Actual unresolved correctness,
input, causal, resource, or authorization dependencies still block affected descendants;
continue eligible siblings. This clarification grants no trading or evidence promotion.

Routine operation with no source changes records an operating receipt with changes=[] and links
to the approved recipe. If it only verifies an already completed receipt, link that evidence
instead of inventing a new engineering change or duplicate experiment.

## Reviewing a completed step

Use FOREX_REVIEW_PROMPT.md. Read the sealed packet and governing design, inspect the exact diff
and affected consumers, then run only checks needed for unresolved questions. Compare live hashes
before treating live files as the reviewed version. Record reproducible findings with severity,
path/line, design requirement, impact and fix. A reviewer must not label its own implementation
as independently reviewed. Never treat the absence of recorded findings as proof of no defects.

Write a new dated REVIEW_RESULT.json/REVIEW_RESULT.md with verdict accepted_within_scope,
changes_requested or blocked; name the reviewer and source/checkpoint hashes. Append follow-up
resolutions with retest evidence. Update the review queue; never erase original findings. Required
fixes become the next work on that item before it is considered reviewed/complete. Acceptance
applies only to the stated engineering scope and grants no trading or campaign authorization.

## Manifest authority

Sealed engineering packages keep their original internal files and current-document snapshots.
Mutable root prompts and indexes may advance through dated documentation addenda. The newest
CHECKPOINT_REVIEW_LATEST.json identifies this workflow's current-document manifest. A documented
root prompt update is not a change to the earlier sealed engine; validate historical root-document
hashes against that package's preserved current_documents copy, and validate the current root
documents against the latest documentation addendum. Do not ignore unexplained differences.
