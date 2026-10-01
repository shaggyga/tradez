# Combined retained observations

The quote and retained-forecast consumers now have one bounded offline entry point:
`tools/forex_retained_combined_observation.py`. This completes queue item
`retained_management_combined_observation_v1`. It does **not** activate position
management or roll out the exact-price collector. Existing running source and
configuration remain unchanged.

## What works

The consumer authenticates the original forecast capture, exact request bytes and
19-file source closure. The request binds the declared quote session/generation,
all 68 instruments, each connection/instrument target, both observation clocks and
the decision clock. Original model, input, forecast origin and terminal identities
survive the join. Each of the 39 connections receives 68 explicit rows, including
missing, refused and priced-observation states. Re-reading does not renew either
forecast or market-price freshness. Original decimal quote strings are retained.

The program reuses the completed forecast and quote validators, existing manager
quote checks and resumable publication engine. It never turns a first forecast
observation into a position. It does not derive conditional remaining value by
subtracting a realized move, supply costs/financing, or grant action authority.
The native policy gate remains unchanged.

## Executed checks and preserved-data result

62 tests passed across the combined consumer, retained forecast receipts and exact
quote receipts. Matched-clock fixtures produce 68 priced observations. Actual
consumer tests reject wrong session/generation, source/capture drift, missing
population, wrong terminal targets, impossible clocks and stale input. Missing
and corrupted quotes have explicit refusal states. Restart and output tampering
are tested through the publication entry point.

The real evidence replay reuses the original October 1 forecast capture at
03:23:03 UTC and the already verified 64-record raw-quote sample. It selects the
record with the latest original receive timestamp. The snapshot is an **offline
reconstruction**, with a declared archive replay session; it is not claimed to be
an original live sidecar snapshot or original collector session identity.

The replay's combined consumption clock is the later recorded archive inspection
clock, 03:52:26 UTC. This is a declared frozen replay clock, not evidence that this
combined consumer actually ran then. Original forecast, provider and receive
clocks are unchanged. The 2,418 present forecasts are refused at that later clock;
234 slots have no current forecast. EUR/USD's retained raw quote is stale and the
other 67 pairs are missing from this bounded quote sample. There are zero priced
observations. These results do not describe current feed health or establish a
new forecasting result.

All 84 output payloads match between uninterrupted execution, checkpoint/resume
and a fresh process in an extracted directory. The restored application is denied
reads from the original project and Vault roots. This is same-host relocation,
not a claim of testing another computer. Same-task substantive review was
performed; independent review was not.

## Run and restore

Retrieve the immutable packet `RETAINED_COMBINED_OBSERVATION_20261001` from the
live Vault. Its `CAPSULE.json` identifies the overlay archive and original bulk
input dependency by SHA-256. Verify both archive hashes before extraction. The
overlay carries exact source, request, expected payload hashes and a replay
runner. The original inputs are retrieved unchanged from
`RETAINED_MANAGEMENT_READINESS_20261001/retained_management_evidence.zip`.

```powershell
python -I -B <overlay>/replay.py <retained_management_evidence.zip> <original-project-root> <original-vault-root>
```

The regular entry point is:

```powershell
python -I -B tools/forex_retained_combined_observation.py --input <verified-capture-directory> --request <REQUEST.json> --sha256 <exact-request-sha256> --output <run-parent> --run-id combined
```

Use `--max-new N` to checkpoint after N new 32-row payloads, then the identical
command with `--resume`. Completed runs are verified; damaged payloads and changed
identities are refused. The request only supports `frozen_replay`: a recent
decision clock must never be used to label old inputs as live. Capture missing
targets are declared requests using the captured minute boundary plus registered
horizon, not invented forecasts. Original forecast targets are used when present.

Local command/output evidence: `evidence/combined_observation_20261001`.
Git supplies runnable code and the dated `vault/forex` knowledge snapshot; the
live Vault remains the coordination authority. Runtime registry pins and current
pipeline configurations were verified unchanged. Rollback baseline is
`4bd43d43bb1588639d6d3f08909efe4d371f59ac`; preserve the evidence.

## Remaining completion boundary

The offline quote/forecast join is finished. The broader position-management
system is not finished. The remaining dependencies are concrete:

1. Roll out the opt-in exact quote observer in the existing quote worker, with
   current process/session/generation and publisher-health checks. Collector or
   service changes are outside this checkpoint; this step did not make broker
   requests or touch the other task's recorder.
2. Qualify an explicit observation-only management contract against preserved
   policy/accounting work: common valuation terminal, fresh conditional input,
   position-state lineage, sizing/risk, currency conversion and all future costs.
   Unknown terms must refuse action. Existing negative management results remain
   negative; observations and fixtures do not establish a profitable policy.
3. Unqualified exact horizon targets, global best-model claims and incremental
   news forecasting value remain separate research questions. No fitting or new
   experiment was performed here.

Exact next queue item: `retained_management_contract_qualification_v1`. Reuse
design sections 17–18, the September 9 observed management study, the preserved
September 25 policy-exposure reference, `oanda_curve_management_replay_v1.py`,
`reference_accounting_adapter_v2.py` and the existing continuation tests. Implement
one explicit contract validator connected to the observation consumer; inventory
the exact retained support and reject missing conditional/state/cost prerequisites.
Test against preserved fixtures without relabeling them as observed positions or
rerunning policy experiments. Publish substantive review, deterministic restore
and matching Git/Vault evidence. Do not silently activate management or fabricate
economic defaults to make the gate pass.
