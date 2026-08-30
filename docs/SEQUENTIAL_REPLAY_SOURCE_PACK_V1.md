# Sequential Replay Exact-Window Source Pack V1

Status: independently verified historical training infrastructure; research-only

## Purpose

The first sequential replay used four full-prefix archives from the verified
counterfactual SIM cohort. That was appropriate for a bounded mechanics pilot,
but it could not scale honestly to the 68-pair universe. This component creates
small, content-addressed M1 bid/ask slices for predeclared sessions and records
missing context, execution, and feedback quotes without deleting the scheduled
clock.

It has no broker, account, authorization, lifecycle, signal-publication, or
execution path. Every slice is historical training/discovery and cannot promote
or authorize a trade.

## Frozen schedule

The V1 pack selects three four-hour London/New York overlap blocks using only
weekday and UTC clock metadata:

- Monday, 24 August 2026, 12:00–16:00 UTC;
- Wednesday, 26 August 2026, 12:00–16:00 UTC;
- Friday, 28 August 2026, 12:00–16:00 UTC.

Each session schedules 48 five-minute global clocks before inspecting quote
coverage. The global clock is the market-repetition unit. Sixty-eight pair
contexts at the same clock do not become 68 independent repetitions, and the
three weekday blocks are not claimed to be independent regimes.

Each exact source slice contains the full 60-minute causal feature prefix, the
one-minute delayed executable-entry time, the five-minute feedback time, and
the predeclared terminal-close time. Missing rows are neither filled nor used
to remove clocks.

## Verified pack

Current pack: `sequential_replay_source_pack_v1.b9d1526f3dfa057bd06d`

This successor uses the canonical portable gzip header (`mtime=0`, OS byte
`0xFF`) and is byte-identical under the supported Python 3.12 and 3.13
runtimes. Pack `sequential_replay_source_pack_v1.ee6d6fd4d38744ecc1da`
remains preserved as the pre-cross-runtime-normalization predecessor.

- 68/68 instruments;
- three deterministic sessions;
- 204 content-addressed exact-window gzip archives;
- 144 scheduled global clocks;
- 9,792 scheduled pair contexts;
- 8,333 fully ready pair contexts;
- 1,441 context-history failures;
- 382 missing exact delayed-entry quotes;
- 290 missing exact feedback quotes;
- 2,299 missing exact source minutes.

These failure counts overlap. They explain why a full-universe replay must be
availability-aware instead of requiring every pair to have a continuous hour
at every global clock. Sparse pairs such as DKK, HKD, THB, TRY and some ZAR
crosses cannot be treated like EUR/USD, nor silently discarded after observing
the next quote.

The independent verifier reconstructed all 204 slices from the retained source
files, bounded both compressed and expanded bytes, enforced the exact
session-by-instrument Cartesian manifest, rejected links and duplicate archive
paths, rebuilt aggregate coverage and schedules, and reproduced the material
pack ID with zero failures. The material contract now binds the complete
research-safety policy, and the verifier independently rejects any forged
state or material safety flag. Twenty-five focused/adversarial tests passed;
one
native Windows symlink creation case was skipped because the test process lacks
that platform privilege.

Pack `sequential_replay_source_pack_v1.1bddc89d33ec30767c96` is preserved as
the immediately preceding exact-window build. It has the same schedule and
coverage census but predates the complete state-safety binding, so it is
superseded for downstream work rather than relabeled or deleted.

The earlier pack `sequential_replay_source_pack_v1.52259f543a3be80f3d3a`
is preserved as a coverage-driven engineering diagnostic. Its Friday block was
10:00–14:00 UTC to improve TRY coverage, so it is not homogeneous with the
declared London/New York overlap schedule and cannot be presented as that
metadata-only experiment. It remains immutable and proof-ineligible; the
corrected pack above supersedes it for current replay work.

## Downstream rule

A batch replay may rank only pair contexts whose causal prefix is available.
It must retain every global clock and every unavailable pair context. An entry,
rotation, feedback mark, or exit that lacks its exact declared quote must fail
closed and be recorded; it must never use the nearest candle. Later replay
attempts and counterfactual branches remain nested beneath the original global
clock and cannot increase market-repetition or regime counts.

## Commands

```powershell
python oanda_sequential_replay_source_pack.py
python oanda_sequential_replay_source_pack_verifier.py
python -m pytest -q test_oanda_sequential_replay_source_pack.py
```
