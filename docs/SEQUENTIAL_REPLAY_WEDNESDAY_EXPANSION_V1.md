# Sequential replay Wednesday expansion V1

Status: sealed and independently verified historical-training source pack; no
policy result has been run or inspected at the time of this precommit record.

The calendar was fixed as seven consecutive Wednesdays from 2026-07-15 through
2026-08-26, 12:00-16:00 UTC. Dates were selected by calendar and the all-68
source intersection, never by movement, event, candidate, quote coverage, or
outcome. The 2026-08-26 block overlaps the earlier discovery pack and is marked
as prior discovery. Its 48 clocks cannot be presented as new evidence.

- Pack: `sequential_replay_source_pack_v1.554c8f74212202aa9b86`
- Instruments: **68**
- Sessions: **7**
- Scheduled global clocks: **336**
- Pair contexts: **22,848**
- Fully ready contexts: **19,782**
- Context failures retained: **3,032**
- Missing exact delayed-entry quotes retained: **794**
- Missing exact feedback quotes retained: **683**
- Exact-window slices: **476**
- Independent verifier failures: **0**

All pair contexts share their global clock repetition unit. Shared currencies,
pair variants, reruns, counterfactuals, and the overlapping discovery block do
not manufacture independent market evidence. This pack is permanently
research-only, proof-ineligible, non-promotable, non-authorizing, and supports
only `no_trade`.

The frozen policy-challenger definition was sealed as
`sequential_all68_policy_challenger_v1.0b5267c7a5c730a150cf` before the replay
result was opened.

## Frozen V1 baseline replay

- Cohort: `seq_a68_wed_exp_v1.e7de4ecdd0255eb13306`

The former pack `5cfcd2011d3b34fe4bb2` and replay `92bb0e964d2639b7e897`
remain preserved as pre-cross-runtime-normalization predecessors.
- Global decisions: **336**
- Ranked candidate contexts: **928**
- Actions: **97 wait, 59 enter, 46 hold, 59 exit, 75 rotate**
- Exact execution legs: **268**
- Executable result after fixed slippage: **-186.20 pips**
- Terminal state: **flat**
- Independent-verifier failures: **0**
- Unchanged-rerun artifact files: **15 / 15 byte-identical**

This is further evidence that the V1 trailing-momentum ranking and rotation
mechanics do not clear executable costs. It is not a result for the frozen
challenger arms yet and cannot confirm any definition. A separate expansion
challenger must remain historical training; confirmation still requires a
strictly later untouched prospective cohort.

The first build attempt used an overlong descriptive Windows artifact path and
failed before sealing a result. The successful cohort uses a bounded namespace.
The incomplete 2.05 MiB temporary tree has no state, result, or evidentiary
status and is retained only as quarantined build debris pending ordinary safe
cleanup.
