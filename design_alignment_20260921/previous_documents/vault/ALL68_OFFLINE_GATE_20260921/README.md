# All-68 offline gate — started

This records the first read-only gate for the next offline experiment. The
gate passed on September 21, 2026. It binds the actual dirty-worktree snapshot,
the reviewed pip map, and the long-history archive's 68-member central
directory. No rows were extracted and no forecasting or trading code ran.

A pure synthetic global-clock contract layer also passes four checks: stable
timestamp/instrument ordering, explicit missing support, the eight required
horizons with availability gates, and separate policy-arm capital using the
same forecast IDs. It is groundwork only; it has not consumed history.

The first real-data benchmark then read a single 24-hour long-history window
serially and read-only. It examined 93,002 rows from all 68 pairs in 5.916
seconds. No pair was empty; 4,646 pair-minutes were retained as explicit
missing support over 1,436 observed global minutes. It did not create features,
fit a model, issue a forecast, or run a policy ledger.

During tape construction, the availability contract was corrected and retested:
a forecast is gated by features, mature training data, and fit completion, not
by its own future outcome maturity. This prevents target-horizon leakage.

The neutral control is now complete: [781,184 forecast records and 1,562,368
no-trade ledger rows](NEUTRAL_CONTROL_RECEIPT.json) over the same 24 hours. It
uses all eight horizons and two separate policy arms, with missing-support rows
retained. This establishes an engineering baseline, not a predictive result.

The same tape is now [settled by the existing read-only label contract](NEUTRAL_SETTLEMENT_RECEIPT.json)
against a distinct 48-hour history query. Complete and incomplete outcomes are
both retained; the recorded endpoint values remain a retrospective proxy, not
proof of executable fills or original receipt availability.

The raw source does not retain original receipt or complete-bar clocks. The
tape reconstructs every raw minute timestamp as a **bar start** and uses
`start + 60 seconds` as its decision/availability clock. That assumption is
explicit in the tape and is not historical publication evidence.

The [model admission boundary](MODEL_ADMISSION_BOUNDARY.md) records which
recovered historical evidence can inform the next fitted baseline and what it
cannot establish. Advisor comparisons are deferred.

The next accounting layer also passes four focused contracts for executable
sides, fill delay, account-currency conversion, and financing boundaries. It
is still synthetic: no real position, conversion feed, or financing schedule
has been applied to historical policy outcomes yet.

## Bound inputs

- Worktree snapshot: `9ba4f7d6ff91a501f414d8bfee96a0385cdb2ce0a92bb39b923e48124324a3d4`
  with content hash
  `943f0a385893784825934ad5387b8d1d239d04f26d683d094f73f25c40b24f9e`.
- Long-history archive: `inputs/long_m1_68.zip`, declared SHA-256
  `aca163639a18e22b2c60dc4ff36d55d82c655af617836a6d351dd1339e5a6006`.
- Pip map: `pair_local_operational_v2_20260913.json`, SHA-256
  `c9464343f012e0bdd83582480a0044bf1c6774eef2622f312bcdd4599e3869c5`.

The ZIP directory contains exactly 68 declared parquet members with matching
names and uncompressed sizes. Its instrument set exactly equals the pip-map
instrument set. The native CSV vintage remains excluded until its source
selection and derived spread-correction contract is implemented.

`RUN_STATUS.json` states the exact completed and uncompleted scope. The local
read-only preflight script and full receipt remain at
`C:\Users\zmoor\Documents\forex\stage_c_all68_20260921`; their hashes are
recorded in `RUN_STATUS.json`. This Vault package is the machine-independent
handoff for the next implementation gate.

Read [next implementation](NEXT_IMPLEMENTATION.md) before extracting any bars.

## Endpoint-only forecast diagnostic

The strict exact-minute-path audit found that sampled 24h, 48h, and 72h labels
were blocked by intermediate gaps. A distinct endpoint-only midpoint target was
therefore audited instead; it preserves the strict-path blocker and makes no
execution claim. Across the same 93,002 all-pair origins, exact endpoints were
available for 89,505 (24h), 89,978 (48h), and 89,565 (72h) cases.

The first isolated all-68 causal benchmark is now complete: a no-change
abstention and pooled ridge model on causal price/spread/time features, trained
for 21 days and assessed on a later 7-day hourly held-forward block. Its 48h
result is negative by error; 24h and 72h direction diagnostics are not model
admission evidence. See [the endpoint baseline receipt](ENDPOINT_BASELINE_RECEIPT_20260921.md)
for the target contract, metrics, hashes, reproduction, and next work.

The first true daily-close/two-day/five-day endpoint-only benchmark is also
complete, using pair-resumable extraction and a fixed causal fit cutoff. Its
one-block results are forecast diagnostics only; [the calendar baseline receipt](CALENDAR_BASELINE_RECEIPT_20260921.md)
records the full scope, metrics, hashes, and limitations.

The current requirement-by-requirement state, including the retired ridge
baseline and remaining execution blockers, is in [requirement traceability](REQUIREMENT_TRACEABILITY_20260921.md).
Use the [reproduction run matrix](REPRODUCTION_RUN_MATRIX_20260921.md) to rerun
or inspect each retained ridge and HGB benchmark without mixing artifacts.
The [automated stability report](C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\ALL68_MODEL_STABILITY_REPORT.json)
is the consolidated numeric source for both retirement decisions.
Read [state of research](STATE_OF_RESEARCH_20260921.md) first for the current
evidence, boundaries, and next safe implementation step.
