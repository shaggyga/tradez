# Timed run execution and truthful status

This procedure prevents the September 24 failure: a task finished one package,
sent a final reply, and claimed it would keep working although no continuation
mechanism existed. Editing a deadline or writing `IN_PROGRESS` never runs work.

## Launch only on a new authorized run

1. Read the actual UTC clock, current queue, ownership and selected scope. Preserve
   the original start/deadline; never extend it to hide idle time. An explicit user
   extension must be recorded separately with its instruction. A stopped session
   cannot be revived by an old prompt or scheduler.
2. Create a local guard state with `tools/forex_timed_session.py` alongside the
   working session record. Use actual timestamps and request evidence. The helper
   is standard-library-only and does not run research, a worker, or a scheduler.
3. For a timed run that must survive a turn ending, use the supported Codex
   heartbeat tool in this task, bounded by the original deadline. Inspect existing
   automations first and avoid duplicates. Its instruction must read current user
   stop status before any action, resume owned partial work, respect dependencies,
   skip blocked independent branches and disable itself at the deadline or stop.
   Keep it quiet unless progress, failure, completion or a required action changes.
   Record the actual tool receipt and read it back; never invent a scheduler ID.
   If the tool is unavailable, keep working in the active turn; report a real
   execution limitation if that becomes impossible. Do not say a file is a backend.
4. Register the receipt with the guard. Start one observed turn; retain tool/job
   IDs and log evidence at milestones. Use fresh evidence for `touch`. Subagents
   do not multiply wall time. Gaps longer than 120 seconds are conservatively
   uncredited; this is an observation lower bound, not exact CPU or thinking time.

The normalized receipt must contain `id`, `kind="heartbeat"`, `status="ACTIVE"`,
`session_id`, `deadline_utc`, `tool_result_path`, and `original_request_path`.
The session and deadline must match the original guard contract exactly. Both
referenced files must contain the actual tool readback and authorizing request.
The guard pins their bytes and refuses changed, expired or unrelated receipts.
Registration never starts or cancels the backend; use its actual tool for that.

## Package and final-response gate

A package checkpoint is an intermediate event during a timed session. Read the
clock, update the exact partial/next step and continue useful authorized work.
Do not send a completion response just because one package passed tests.

Immediately before ending a turn, invoke the guard:

- `finish --reason normal` refuses before the deadline while any authorized work
  remains. Completion requires an evidence file identifying the queue assessment.
- `yield` before the deadline requires a registered continuation receipt. Its
  status is **awaiting_continuation**, never proof that a worker is running. Check
  the backend's fresh tool status before claiming a scheduled continuation exists.
- `finish --reason user_stop` records the actual user instruction and leaves the
  session terminal. Disable its heartbeat first, if one exists. Do not create a
  replacement goal, job, session or timer without a new user instruction.
- Other supported stops are the actual deadline, all authorized work completed,
  a documented scan with every independent path blocked, or a concrete platform
  limit. Do not use an affected single package to claim every path is blocked.

Example commands (substitute actual values; these examples launch nothing):

```powershell
python -I -B tools/forex_timed_session.py create --state <guard.json> --owner <claim> --session-id <id> --started-utc <original-UTC> --deadline-utc <original-UTC> --step-id <partial-step> --resume-command <exact-command> --evidence <request.json>
python -I -B tools/forex_timed_session.py register-backend --state <guard.json> --owner <claim> --evidence <verified-heartbeat-receipt.json>
python -I -B tools/forex_timed_session.py start --state <guard.json> --owner <claim> --turn-id <unique-turn> --evidence <start-evidence.json>
python -I -B tools/forex_timed_session.py status --state <guard.json> --owner <claim>
python -I -B tools/forex_timed_session.py finish --state <guard.json> --owner <claim> --turn-id <unique-turn> --reason normal --evidence <finish-assessment.json>
```

Finish evidence declares `reason`, `detail`, and boolean
`authorized_unfinished_eligible_work`. Completion also requires
`authorized_unfinished_work=false`. A user/platform stop needs
`supporting_evidence_path`; a total blocker requires individual blocked paths and
their evidence. These assertions must reflect actual work, not convenient flags.

## Acceptance and limitations

Passing focused tests does not complete a package. Verify every candidate gate,
inherited support threshold, input identity, consumer, required diagnostics and
reproduction evidence. Preserve failed attempts and use `changes_requested` for
concrete review findings. Independent review must name a separate reviewer.
Manifest hashes must identify the manifest bytes, not the result it contains.

The helper rejects invalid transitions when invoked. It cannot intercept the
assistant's final response, prove declared evidence is truthful, prevent the host
sleeping, or guarantee a scheduled callback will execute. Only an actual verified
backend can resume an idle task. State files and written promises cannot do that.
Report wall elapsed, observed activity and idle/unknown gaps separately. Never
claim an unmet duration was fulfilled.

Verification: `python -I -B tests/test_forex_timed_session.py`.
