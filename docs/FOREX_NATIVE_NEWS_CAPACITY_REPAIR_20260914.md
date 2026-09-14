# Native news capacity repair — September 14, 2026

This is the implementation and acceptance record for the admission failure found during the September 13 operational repair. **03:14 UTC checkpoint:** the replacement remains isolated; deployed native V3 still reports `total_json_byte_bound`. Core V4 passes the actual 12,000-story writer/restart/193-origin checks, but IO V5 misses the unchanged 30-second ordinary capture budget. IO V6 is being measured; it is not accepted or installed. Do not infer deployment or trading readiness from a passing fixture. Version numbers in the candidate workspace identify implementation iterations, not live study generations.

## Problem and retained evidence

The original admission path retained expanded source and classification evidence repeatedly. Its 128-MiB aggregate JSON bound was reached after six successful live cycles, around 1,005 of 2,698 projections. A pure source replay had passed earlier, but had not exercised durable admission, consumer reconstruction or continuing archive growth. That earlier test is retained with its narrower scope.

The replacement uses compressed objects addressed by their content hashes and a compact member index. It retains the original lexical JSON, payload hashes, source identities, revision order, publication and consumption clocks. Source policy is stored once and reconstructed exactly. Accessors resolve the latest revision before applying relevance or clock filters. Provenance lists have explicit new ordered-hash schemas; they are not represented as the former expanded readback digest.

The candidate adds `compact_projection_store_v1.py` to the registered source set: 46 files instead of 45. Downstream source pins and native contracts are rebound into a new cohort. The numerical model source is unchanged. Prior study ledgers, registrations and sources are retained; no original availability time is backdated into the successor.

## Startup and concurrent ingestion

A cold read originally held a SQLite read transaction while validating the full archive. A concurrent writer with its normal two-second commit timeout failed during that validation. Candidate V2 captures bounded packed rows in a short transaction, releases it, then validates owned bytes. Both publisher and consumer concurrency tests require the other writer to commit while semantic validation is deliberately paused. File replacement, invalid evidence and changed receipts remain refusals.

The capture has a one-second transaction ceiling and a 256-MiB retained packed-data ceiling. Three actual capture-only reads of the 12,000-story fixture took 0.516, 0.531 and 0.531 seconds for about 125.3 MB and 12,580 rows. These measurements are not complete cold-read timings.

Startup cache preparation is asynchronous and explicitly cannot return a current capture or trading authority. The worker continues observing quotes, settling outcomes and publishing status while preparation runs. A separate normal capture must prove fresh health afterward. A failed bootstrap invalidates prior usable state and retries with a finite cooldown. The ordinary capture deadline and original maturity tests remain distinct from startup work.

## Recorded acceptance runs

Counts below overlap and must not be added together.

| Evidence | Result and limit |
| --- | --- |
| Candidate V1, actual-source prefix 003 | 2,901 projections / 2,745 distinct stories; exact evidence and clock round trips. Warm publish 0.625 s, acknowledgment 0.688 s, preparation 1.062 s. Historical replay 38.81 s did not establish current authority. |
| Candidate V1, synthetic 12,000-story volume | 193 original publication/observation steps, 155 sampled source identities under the 190-source policy. All 12,000 final members round-tripped exactly. 1,213,549,104 expanded evidence bytes; approximately 121.7 MB compressed evidence and 6.05 MB member index. Warm publisher read 2.50 s, preparation 6.31 s. |
| Candidate V1, fresh-process 12,000-story preparation | **Failed:** `stream_validation_time_bound` at 120.125 s wall / 118.266 s CPU, before the historical-origin/archive benchmark. Warm-process volume success does not close this restart gap. |
| Candidate V2 core | 56 focused tests passed, including both concurrent-writer regressions. |
| Candidate V2 IO and adapter | 15 focused tests passed in 36.21 s, including corruption, original timing, numerical-frame parity and non-authorizing bootstrap behavior. |
| Full 46-source V2 integration, stage 004 | Seven tests passed in 131.64 s. Actual native owners carry synthetic evidence from quotes through an original-target H1 outcome; the numerical fit is spied in that integration fixture. Four scheduling cases include a slow failed bootstrap whose cooldown starts at failure completion. |
| Staged supervisor health | Eight executed PowerShell reader tests passed. Starting a retry does not retire the preceding failed completion; a later successful completion does. Unreadable operational status fails closed. |
| Core V4, 12,000-story actual-writer volume | Passed 193 publication/acknowledgment groups, 12,000 distinct final members, 1,213,549,104 expanded evidence bytes, approximately 121.7 MB compressed objects and 6.05 MB member index. Warm fixed-store preparation 5.625 s. |
| Core V4, unprofiled fresh-process cold validation | Passed: publisher 157.250 s, consumer read/preparation 6.484 s, 12,000 headers and three exact full-payload sentinels 0.281 s; total 164.015 s. Peak process working set approximately 681 MB. SQL capture itself was 0.563 s. This uses a separate 300-second non-authorizing cold allowance; it does not relax ordinary capture. |
| Core V4, original historical contexts | All 193 original origins × 68 pairs (13,124 numerical feature cells) passed in 114.094 s. Coverage cells are not 13,124 directional news signals or profitable forecasts. |
| Actual publication/acknowledgment cadence, 720 groups | Passed from zero through actual writers: total 1,971 s, warm preparation 8.797 s, cold read/preparation 21.593 s. The separate 2,880-group run remains in progress; a 720 prefix is not a 48-hour-capacity result. |
| IO V4, 12,000-story original per-object archive | **Failed ordinary timing:** first archive/readback 407.500 s, repeat 250.015 s; 16,334 files. Original byte fidelity passed, but this does not make current capture operational. |
| IO V5, final stage 006, 3,150-story packed archive | Passed exact-byte fidelity and repeat no-growth: fresh read 1.229 s; initial write/readback 5.235/3.871 s; repeat 3.574/3.797 s. File count reduced from 3,455 to 148. |
| IO V5, 12,000-story packed archive | Cold validation passed 160.27 s. **Failed ordinary timing:** fresh read 6.35 s + repeat put 15.62 s + readback 16.82 s = 38.79 s before health. Initial write/readback 25.65/17.06 s; 872 files. Preserved as a failed larger-capacity test. |
| Final stage 006 integration | Seven original native source/quote/publication/outcome/bootstrap tests passed in 128.76 s. This is an exact source closure for IO V5, which still fails the independent ordinary timing gate. |
| Native slow-news handoff candidate | Eleven tests passed: four bootstrap and seven scheduling cases, including virtual 114-second news capture and 66-second pair preparation before guarded fit. Pair/quote/outcome progress continues; next news refresh becomes eligible 60 seconds after completion or failure. Original fit, source, clock, consumption and scoring guard ASTs remain unchanged. |

The 12,000-story fixture is accelerated synthetic traffic through actual ledger writers. It is not a record of live releases or profitable forecasts. Its exhaustive final audit materialized all payloads and peaked at approximately 751 MB RSS; that is separate from the warm operational read measurements. Its final-audit phase timer was not validly sampled, so that phase duration is unavailable; the full run duration of 1,417.55 seconds remains valid.

## Current storage and scheduling design

Core V4 captures packed rows in a transaction bounded to one second and 256 MiB, with at most three fresh capture-only attempts for transaction-deadline failures. Validation occurs after closing the transaction. Compressed objects, member index, expanded evidence and database each have independent byte bounds. The cold semantic allowance is 300 seconds; startup IO/transport has a separate 330-second non-authorizing allowance. Ordinary current capture remains 30 seconds.

IO V5 retains exact object bytes in fixed 32-object compressed blocks and uses a bounded 384-MiB temporary spool. Full prefix blocks can be reused. The compact catalog uses 256-row leaves. Every durable graph must be independently read and hash-verified before a current capture can exist. V6 is investigating redundant encoding/readback work inside that same proof boundary; proposed optimizations are not accepted performance evidence.

The slow-history capture test also exposed a scheduling issue: a refresh that lasts longer than its interval can immediately recapture and starve pair work sharing its lock. The staged worker gives completed pair capture its original guarded fit before another overdue refresh, and starts refresh cooldown at completion/failure. Non-authorizing startup, actual current capture, model fitting and original-target settlement remain separate states.

## Deployment gates still open

1. Accept the final IO successor against the 12,000-story full current-capture timing and exact durable readback. Core V4 cold validation and 193-origin numerical history have passed; IO V5's ordinary timing remains a failure.
2. Complete the publication-cadence scaling check. Separate source-volume and receipt-cadence experiments; do not claim that separate fixtures tested simultaneous worst-case traffic.
3. Preserve the complete 46-source native registry and source-to-publication-to-original-H1-outcome integration acceptance. Stage 006 passed for its frozen sources; subsequent IO/scheduling changes require a fresh exact closure and appropriate checks.
4. Preserve old practice accounting, original finite cutoff and all source-bound cohorts during any actual handover. A maintenance-time process-preflight failure has durably halted new practice entries with zero intents; it must be reconciled, not silently reset.
5. Perform the controlled live handover, verify multiple successful transport cycles and caught-up backlog, then report actual native training readiness.

Native training still requires 48 mature contexts at the original 15-minute training stride, including 12 nonzero news contexts and eight distinct context patterns. The first valid successor consumption cannot become evidence available at an earlier decision. Operational repair therefore does not promise immediate native forecasts or entries, and does not change profitability evidence.

Implementation and raw acceptance receipts are retained under `C:\Users\zmoor\Documents\forex\operational_repairs_20260913`. The original deployed source checkpoint is Git commit `fb05f37`. See [the consolidated operational repair record](FOREX_OPERATIONAL_REPAIRS_20260913.md) for the other seven repair groups and the still-blocked RBNZ delivery route.

The stage 003 integration receipt retains one fixture-setup failure: its test inventory was written as a mapping where the inherited fixture expects a list of file records. Stage 004 corrects the fixture and includes the completed-failure cooldown repair. The earlier failed receipt is not presented as a passing run. Stage 004 remains staged pending the capacity gates above.
