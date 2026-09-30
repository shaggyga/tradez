# Timed Forex research continuation

When the user invokes this prompt, work for their stated duration; default to
**2 hours** if they say "a couple of hours" or give no duration. A request merely to
prepare or review this prompt does not start a timed run. Preserve the user's selected
model and reasoning setting (currently intended: GPT-5.6 Terra, medium).

Read and follow [FOREX_NEXT_PROMPT.md](FOREX_NEXT_PROMPT.md) and
[FOREX_RESEARCH_PATH.md](FOREX_RESEARCH_PATH.md). They define the user's actual research
goal, current queue, exact first package, reuse paths, acceptance gates and boundaries.
The launch instruction authorizes ordinary offline work within those boundaries.
GPT/advisor comparisons and paid calls remain deferred.


Read [FOREX_FORECASTING_CONTINUATION.md](FOREX_FORECASTING_CONTINUATION.md) for
the current forecasting selection policy and independent alternatives. A blocked
confirmation experiment does not stop development siblings during a timed run.

## Establish the clock once

At actual invocation, read the real UTC clock before substantial work. Record
`started_utc`, requested duration and `deadline_utc = started_utc + duration`. Reading,
implementation, verification and publication all count toward elapsed wall time.

Create one session evidence directory and write `SESSION_STATE.json` with:

- session ID, original start/deadline UTC, duration and last-observed UTC;
- workspace/Vault roots, user scope, selected model/reasoning (as supplied, not guessed);
- claim ID, current step ID and local evidence path;
- completed package/review identities, pending findings and exact resume command;
- active process/run IDs and their output paths; checkpoint window and session status.

Reference that file in the board handoff and each step's work log. Update it at package
boundaries, failures and before context compaction. Preserve the original start and
deadline through tool yields, restarts and compaction; elapsed time never resets.
Resume an existing timed session when instructed to resume it. A new user-requested
duration starts a new session only when it is actually a new timed request. Record any
explicit extension against the original session instead of silently resetting it.

If goal tracking is available, use it only as authorized by the timed request and
preserve the same deadline in the durable session record. Do not invent a token/work
budget or substitute it for wall time. Work in this task. Before any turn ends
with time and authorized work remaining, follow FOREX_TIMED_RUN_CONTROL.md: invoke
the finish/yield gate and verify a real continuation backend if yielding. A
status file or a promise is not a running worker. Do not report continuing
work without verified execution support. Never restart a user-stopped session.

## Repeat the full cycle

1. Resume partial owned work; otherwise select the next eligible live queue item.
2. Complete its versioned experiment contract: implementation/run, meaningful tests, actual consumer,
   review, evidence, Vault packet, queue and shared source/checkpoint handoff.
3. Resolve required findings before dependent work. Use a permitted independent
   reviewer when available and record it accurately; self-review is never independent.
   Pending independent review alone is not a stop condition (user clarification,
   2026-09-24). When no separate reviewer is available, conduct and record a substantive
   same-task review with independent_review=false, resolve concrete findings, and
   continue eligible offline work. Keep independent review pending; do not claim it
   passed. Actual unresolved correctness or input dependencies still block affected work.
4. Read the clock, record the checkpoint in SESSION_STATE.json, then immediately begin
   the next eligible package while useful authorized work and sufficient time remain.
   Use the forecasting-continuation candidate order and replenish eligible siblings
   from the design. Keep blocked-item evidence and selection changes in session state.

Do not stop after one package merely because it is complete. Do not idle, repeat valid
checks or manufacture experiments to consume the clock. Record resource usage and
enforce the package's measured limits. Before any expensive job, compare its expected
run/checkpoint duration with time remaining. Poll its original process ID; never
duplicate a yielded job. A tool/platform limit is not permission to fake activity.

## Safe finish

Reserve **10 minutes** of a two-hour session for checkpoint and handoff; use a
proportionate 5–10 minutes for shorter requests. Do not begin a job that cannot finish
or checkpoint safely in the remaining time. The deadline is a work-planning boundary:
finish the current atomic publication/safe stop rather than leaving corruption or
killing unrelated processes. Report any actual overrun, and do not start a new item.

Leave incomplete work explicitly partial with exact source/evidence identities,
running process status and tested resume instructions. Update the claim, Vault review
queue/pointers, local logs and session state. Do not mark the full design achieved
because the duration ended. Do not report that work continues after ending the turn.

Stop early only at the user's request, completion of all useful authorized work, a
genuine blocker leaving no independent eligible work after the documented branch
scan in FOREX_FORECASTING_CONTINUATION.md, or an actual platform/resource limit.
Explain the concrete reason and actual elapsed time. During work give concise progress
updates. End with completed packages, actual verification and review results,
unfinished work/blockers, checkpoint location, original start/deadline, actual elapsed
time and exact next item/resume command.

## Mandatory execution guard

Read [FOREX_TIMED_RUN_CONTROL.md](FOREX_TIMED_RUN_CONTROL.md) before a new launch.
Use project tools/forex_timed_session.py for immutable original clock, evidence-linked
milestones and the final-response gate. Distinguish elapsed wall time from observed
activity and idle/unknown gaps. Keep active-time claims conservative. A package
checkpoint does not end an authorized timed loop. Latest user stop takes priority.
