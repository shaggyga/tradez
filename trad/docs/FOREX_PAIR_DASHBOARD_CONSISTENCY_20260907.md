# Pair dashboard consistency — September 7, 2026

This follow-up repairs an intermittent dashboard reader failure after the
68-pair forecast coverage rollout. The final reader change passed **226 focused
test cases** and independent review. A second dashboard-only reload replaced
processes **30192/42144** with **35648/14260**, while supervisor **22400** and
all 13 other workers retained their 26 process identities. All 68 registrations
and the current API passed the reload check. The final 50-second steady check
returned current data on **all 248 reads (124 paired samples)** across three
summary transitions, with zero collector errors and unchanged registered
sources. API latency was 0.212125 seconds median and 0.858056 seconds p95.
The final source-bound result is recorded in
`FOREX_PAIR_DASHBOARD_CONSISTENCY_VALIDATION_20260907.json`.

The registered pair study and all nine bound study sources remain unchanged.
The registry remains
`e0aa74fa5d20b7be983050625773834ea68d74ffc15b566ab5e03df9b3380d65`.
No model, learning rule, forecast, original H1 target, activation or order gate
changes in this follow-up.

## Observed failure and repair

A post-export dashboard request briefly reported the pair coverage as
unavailable; a subsequent request returned current data. Review identified two
reader-side races. First, the request sampled its clock before validating source
hashes and reading the publications. A genuinely newer heartbeat could then
appear to be in the future relative to that earlier clock. Second, the summary
and heartbeat are individually atomic files. Reading across a publication update
can pair the new summary with the preceding heartbeat, or the reverse.

The first retry-only fix passed 213 tests but failed the longer live check:
**38 of 184 reader/API results were unavailable** across 92 paired samples in
35.015 seconds. The failed observations are preserved. The worker publishes
summaries and heartbeats on separate 15-second and 5-second cadences; their
phase drift left observed summary-to-matching-heartbeat clock gaps of about
3.2–3.5 seconds. Two 10 ms retry pauses could not bridge those gaps. These were
display failures; the collector reported zero errors.

The reader now samples the actual observation clock **after reading the retained
publication bytes and computing their hashes**. It retries at most three times
when a bounded source read was replaced or when summary and heartbeat hashes
belong to different publication generations, with a 10 ms pause between retries.
When those retries see a generation mismatch, the reader may use a previously
fully verified summary **only if the current fresh heartbeat binds that exact
retained summary hash**. A thread-safe cache holds immutable bytes for at most
two generations per path/registry key, bounded to four keys. Late concurrent
readers cannot replace the newest generations with older ones. Caller mutation
cannot alter retained bytes.

Retained summaries keep their original generated, row-observation and forecast
clocks. They must still pass registry/source, freshness, pair identity and
original H1 target checks; retention does not extend the 90-second limit.
The unmatched new generation is not displayed or trusted. Retained-generation
selection, original clocks, hash bindings and retry failures are exposed as
diagnostics. A cold reader without a verified matching generation remains
unavailable until it can read one.

The retry does not wait out genuinely future timestamps, stale observations,
clock regression, invalid identities or broken payload seals. Such cases remain
unavailable. Failed retry observations and attempt counts are retained in the
reader result. This repair changes how the dashboard observes publication files;
it does not rewrite or republish study evidence.

The tested dashboard source SHA-256 is
`6eb16350ba63a832bc184c3a238898707905b6f949feba8d1fcfe5bb850fe7cc`.

## Operational scope and validation

The controlled reload required that tested dashboard hash, verified all 68
existing immutable pair registrations and the nine registered source bindings,
and captured the exact identities of the 14 supervised worker pairs. It stopped
only the verified dashboard process pair, accepting a companion PID that had
already exited. Existing supervisor **22400** performed its normal dashboard
replacement. The script neither started a second supervisor nor directly launched
a dashboard, and it performed no study activation.

Post-reload verification confirmed that supervisor 22400 and the other 13 workers'
26 process identities remained unchanged, that the new dashboard pair belonged to
that supervisor, and that the current API retained 68 registered rows and disabled
order/promotion flags. Transient pair coverage counts are separate from reader
consistency and prediction performance. The
admitted news collector may spawn its transient
event-tagger child; that verified descendant is recorded separately from the
14 stable supervised workers and is never stopped by this reload.

A separate bounded concurrent probe samples the live API and reader across
multiple publication generations. Its actual sample count, generation
transitions, errors and request timings must be retained, including failures.
Passing a bounded probe does not promise that every future request will succeed.

Cold priming lasted 3.703 seconds and retained ten expected generation-mismatch
reads from the new direct observer before it had a verified cache; the running
API stayed current. The raw final detector report remains **incomplete for its
additional live provenance-coverage criterion**: three retained API results
referenced a generation from before the probe's observation window, and zero
retained cases were independently reconstructed within that window. There were
zero observed binding failures. The separate hash-bound assessment passes only
the stated steady availability/clock/source criteria. Retained-generation
provenance and mutation/concurrency boundaries are covered by the 226 automated
tests and independent code review, not claimed as independently reconstructed
live evidence. Raw incomplete/failed reports are preserved without rewriting.

Workspace evidence is under
`C:\Users\zmoor\Documents\forex\pair_dashboard_consistency_20260907`:

- `HELPER_READONLY_PREFLIGHT_20260907.json`: source, registration and process
  inspection before any dashboard reload.
- `reload_dashboard_only.ps1` and `verify_runtime.py`: controlled dashboard-only
  reload and independent read-only process/registration checks.
- `DASHBOARD_CONSISTENCY_RUNTIME_VERIFICATION_20260907.json`: first reload and
  current API verification before the longer retry-only check failed.
- `DASHBOARD_API_CONSISTENCY_PROBE_20260907.json`: preserved failed retry-only
  live check, including every unavailable result.
- `DASHBOARD_CONSISTENCY_RUNTIME_VERIFICATION_RETAINED_20260907.json`: final
  retained-generation reader reload and current API verification.
- `DASHBOARD_API_CONSISTENCY_PROBE_RETAINED_20260907.json`: preserved detector
  priming attempt. It incorrectly required this new observer to reconstruct
  an API-retained generation from before its observation window and shortened
  the last request timeout; it did not reach its steady measurement window.
- `DASHBOARD_API_CONSISTENCY_PROBE_RETAINED2_20260907.json`: final bounded
  multi-generation observations, with cold priming and steady checks separated.
- `DASHBOARD_API_CONSISTENCY_ASSESSMENT_20260907.json`: hash-bound assessment of
  the narrower successful steady checks and the unobserved retention coverage.
- `publish_vault.py`: prepared source-only publisher; publishing follows final
  source-bound validation.

## Preserved records and export

The earlier pair-coverage validation remains frozen at SHA-256
`6953f2d7259be748847ec0dd6a34abd1b2f482dd8be7aa6913bc7d906ff4cfb5`.
The preceding signals/live validation remains
`3671297affd5addd4c2e4080c66d4d74382c24678a3430d7e4694814e5993792`.
Their dated counts and conclusions are not rewritten to describe this follow-up.

The follow-up adds two canonical report/receipt mappings for **147 records**.
The prepared publisher preserves prior current pointers and records in
`maintenance/before_pair_dashboard_consistency_20260907`, retains the source
archive `forex_worktree_source_8fccbba671ee4620a5bf1d8e.zip` at SHA-256
`8fb5d2535d71025a59ba13bb997161b765d01bc19228d49c27bbe560e9e12b3b`, and records its
new export separately in
`maintenance/PAIR_DASHBOARD_CONSISTENCY_EXPORT_VERIFICATION_20260907.json`.
Verification covers local OneDrive bytes; it does not attest cloud sync or full
database recovery. This UI consistency repair does not establish better
predictions, calibrated probabilities or trading eligibility.
