# Bounded joint-v3 observer retry design â€” September 9, 2026

The original probe remains on frozen V1 sources. This is a separately versioned scheduling proposal, not an activated reliability fix. Parent approved the first-pass0.25s / retry0.5s / same total8s envelope.

Finish the unchanged first pass across all68 registered pairs before any retry. Retry only an actually attempted transient failure, at most once, in the same deterministic pair order. Each retry performs a fresh read-only transaction and all original publication/identity checks; its deadline is `min(original_deadline, actual_retry_start + 0.5s)`. Do not retry unvisited pairs, successful unavailable/no-publication rows or integrity errors.

Public error strings are insufficient. V1 combines chronology and elapsed-time checks under `observer_pair_time_budget`, and collapses every SQLite exception into one broad public reason. Keep original caught SQLite primary codes, actual monotonic expiry and a bounded transparent original-clock trace. Only genuine eligible timeout/lock failures enter the queue; unknown codes, reversed/invalid clocks or integrity errors do not. Root confirmed BUSY/LOCKED are eligible before the pair deadline when clocks are valid; INTERRUPT still requires actual deadline exhaustion. The original0.05s connection busy timeout is unchanged.

Retain first failure, actual attempt timing, eligibility and retry result. A recovered ledger without a publication is not a recovered forecast. Final current counts use original reference/issue/target and fresh retry observation clocks; prior successful rows can expire while retries run. Final source/registry checks are mandatory and never bypassed.

New report/API/spec/summary versions bind the new module plus unchanged V1 helper/immutable-boundary dependencies and the original20-source model closure. The original forecast cohort and its rows do not change. The API response remains bounded at2MiB.

## Required tests

1. No failures: V2 calls original _pair exactly once per pair and yields same original rows/forecast identities under a replayed clock trace; only version/scheduler metadata differs.
2. Complete sorted first pass precedes any retry; first-pass successes, no-publication, expired and unvisited rows never enter retry queue.
3. Pair-time error before actual deadline is nonretryable, even though its public reason matches a time-budget error.
4. Pair-time error after deadline with reversed/nonfinite/overflow clock trace is nonretryable; finite exact clock values pass through without replacement.
5. SQLite BUSY/LOCKED primary and extended codes map to the correct primary class; unknown/CORRUPT/SCHEMA/READONLY errors never retry. INTERRUPT before deadline never retries.
6. Initial .25s cap, retry .5s cap and same8s global deadline enforced with equality fixtures; no newly admitted attempt after total deadline, no third attempt.
7. First pass exhausts total budget: exact remaining rows unvisited, no retries. Small remaining budget limits retry deadline below .5s.
8. Retry opens a distinct query-only connection/transaction and performs original publication and row-identity checks; original first-attempt row objects cannot be reused.
9. Fresh retry success replaces only final observation with retry actual clocks, preserves original forecast reference/issue/target; first failure and timing remain in first-pass diagnostics.
10. Fresh retry verifies ledger but no published forecast: recovered-ledger count increments, recovered-forecast count does not.
11. Retry integrity failure remains final failure alongside its first transient cause; no fallback to old cached evidence or averaged values.
12. First-pass successful rows expire during later retries: aggregate completion rechecks native target and never retimestamps them.
13. Before/after source or registry change invalidates complete report even if retry succeeded; exact V1 helper dependency and new V2 file are bound.
14. New observer/API/spec/summary schemas and exact source closure; old API refuses V2 and new API does not relabel V1. Counts/attempt logs and2MiB response cap tested.
15. All original authority flags false; no writer constructor, broker GET, model fit, ledger mutation or row import.
16. API serves an in-flight cached report only under original report/row clocks and generation guards. Retry diagnostics cannot refresh original forecast eligibility.
17. Controlled transient delay fixtures demonstrate recovery within same budget; compare full original rows on successful exact-input paths, not favorable performance outcomes.

Exact design JSON SHA256 `7848aac4cd1daada8a07efdcc652a9d60485a4851535288b7f2ced93f57999c3`. No code, registered source, runtime, broker or ledger mutation performed by this design task.
