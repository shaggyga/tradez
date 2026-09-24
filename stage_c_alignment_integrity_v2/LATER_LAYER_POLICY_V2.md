# Later remaining-layer position comparison

Read the Vault queue, current checkpoint and coordination claim first. This package
reuses the completed later forecast surface to compare position management. It does
not train or load base models, refit layers, call an advisor, access a broker, or
change a live service. Its results are inspected-development candle scenarios.

## Frozen comparison

`LATER_LAYER_POLICY_CONTRACT_V2.json` fixes the hypotheses, dates, 14 forecast
variants, two cost scenarios, two accounting engines and resource limits before
P&L. The 56 runs retain the six original independent policy accounts: fixed hold,
naive, continuation, hysteresis, cash and the recovered momentum selector. The last
two explicitly abstain when their original requirements are unavailable.

The original policy, risk, notional, margin, integer sizing, fee, persistence and
grace settings remain unchanged. Each frame uses the same original all-68 price
panel. Only the cohort dates and corresponding financing date move. Financing is
at origin + 86,401 seconds, one second before the second-day decision, with the
original hypothetical 0/1 bp cost. Slippage is 0/1 bp per leg and the original fee
is $0.10 per fill; the zero-slippage/financing case still has that fee. Full pending
units fill at the next completed M1 close after the original 58-second delay.
These are declared assumptions, not observations of broker execution or financing.

For each learner and layer update mode, raw matched, signed-only and magnitude
interaction variants have identical causal forecast availability. Each gets its
own full policy path and account state. Raw unrestricted is retained separately;
differences caused by that coverage change are labelled availability effects.
The contract does not select the best forecast or policy after scoring.

## Authority and reuse

The approved operator recipe is `LATER_LAYER_POLICY_OPERATOR_RECIPE_V2.json`.
Obtain its exact SHA from the current verified Vault checkpoint, not from an
untrusted replacement. It pins the full consumed source/environment/native closure.

The input path map has exactly `surface`, `technical`, and `trad`. The operator
verifies the complete original surface and technical runs. It rederives
`LATER_POLICY_AUTHORITY_V2.json` from those bytes and requires an exact match.
That compact authority binds each original prediction list, prepared packet list,
coverage list, observation panel and market panel. It is not a substitute for
verifying the parent runs and must never be edited to approve a replacement input.

At each policy decision, the adapter checks those parent identities, reconstructs
the native packet from the preserved prediction and original observation/reference,
then uses the existing Decimal candidate consumer. Changed self-hashed forecasts,
quotes, native candidates, rollover assumptions, financing signs and fill quantities
are refused before account state changes. No new inference or model selection occurs.

The historical adapter's old dated scenarios retain their old values. Its later
scenario extension explicitly uses each scenario's registered rollover epoch for
both remaining financing and the event charge. The core ledger and policy decision
logic are reused. Reference and optimized engines must match five economic payloads
for every method/scenario pair.

## Missingness and interpretation

All 544 instrument/origin slots remain in each method/scenario path. Forecast,
layer, quote and conversion gaps are reported separately. There is no future-endpoint
universe filter. The retained EUR/DKK terminal quote is unavailable; if a policy
holds an affected position, the ledger must retain its missing valuation or incomplete
liquidation rather than create a price or declare it closed.

The report distinguishes closed, marked-open and unpriced terminal states. An
unpriced state is never zero P&L. Paired terminal P&L differences require both paths
to be closed. The JSON preserves all six arms, 144 matched differences, 48 availability
diagnostics, fees, financing, turnover by instrument, decisions, episodes, reversals
and missing marks. One dependent inspected cohort supports descriptive comparisons;
there is no protected confirmation, confidence interval or general profit claim.

## Run and resume

Use the pinned Python environment and `later_policy_operator_v2.py` with `run`,
`resume`, `verify` or `status`, plus `--recipe`, `--recipe-sha256`, `--paths` and
`--runs-dir`. All modes authenticate inputs; status/verify also reconstruct candidate
fixtures and can take several minutes. During an active run, inspect its recorded
PID and `OPERATOR_PROGRESS.jsonl`; do not launch a second operator just to check it.

The existing local single-writer journal saves each policy frame. Resume the same
run directory after confirming that its owner process has stopped. A completed
run is reused; a partial run resumes from its authenticated snapshot. The first
run is deliberately interrupted after frame seven to check this path. Source,
recipe or input drift requires review and an explicit successor, never changing
the existing run identity in place.

The main bound is one worker, 3,600 seconds, 2 GiB sampled RSS and 16 GiB outputs,
with at least 8 GiB free disk. Phase samples are not a continuous OS quota. This
allowance is grounded in the prior 227.7 MB largest policy run and measured replay
times. Stop affected work on a bound; preserve already completed paths. Do not
remove original evidence or weaken scientific settings to force completion.

## Checkpoint and handoff

`later_policy_report_v2.py` requires a pinned completed 56-run operator receipt.
`later_policy_checkpoint_v2.py export` verifies the runs and packages parent inputs,
source and expected output hashes. `restore --run-tests` uses a new directory,
replays all 56 paths with zero model work, compares every payload and report, and
runs the standalone adapter/report tests. It does not duplicate large policy
snapshots inside the archive; those are regenerated by the same frozen replay.

Keep raw run output and WORK_LOG/PENDING_CHANGES locally. Publish the compact review
packet and checkpoint to the Vault, link the same step and identities from both
existing project logs, and update the shared Git receipt. Report implementation,
same-task review and pending independent review separately. A future machine must
qualify its own environment; a same-machine relocation is not cloud-sync proof.
