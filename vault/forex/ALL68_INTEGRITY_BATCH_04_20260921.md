# Alignment integrity batch 04 — actual recovery and qualified inputs

This supersedes the affected claims in batches 02/03 and completes the bounded
neutral issuance/recovery repair (WP1/WP2/WP10; relevant portions of
TST01/03/05/07/10/11/32/33/48-52/54). It does not certify all design acceptance
cases or fitted/adaptive campaign behavior. The full engineering design remains
the destination; GPT/advisor comparisons remain deferred.

## What actually changed

- Windows ownership now uses a held kernel lock, a non-destructive process
  liveness query and atomic owner metadata. Recovery verifies host, dead owner,
  identity and immutable payload hashes. A real process crash during first
  owner publication was reproduced during review and repaired.
- Forecast issuance reads every Parquet row group, normalizes UTC timestamps
  and rejects nonfinite/nonpositive/ambiguous origins. Reports are deterministic.
  Journal-verified origin inputs allow interrupted archive-free report recovery.
  Physical input integrity and column reads include later bytes; only origin
  values enter issuance. Future endpoint mutation/removal tests compare complete
  forecast/coverage/outcome tapes, rather than a fabricated helper mask.
- Required inventory is bound to identity; empty, duplicate, dropped, reserved
  or unsafe paths and mismatched persisted identities are rejected. The Windows
  launcher has a real failing-native-child test and reports success only after
  the configured suite succeeds.
- Session convention v2 counts Monday-Friday 17:00 New York closes strictly
  after the decision. Sunday opening never counts as a completed close. Actual
  target use requires bounded declared full/partial holiday metadata. Historical
  2024 venue availability has not been established by synthetic calendar tests.
- A complete portable issuance fixture now contains frozen runtime source,
  exact environment/configuration, 68 synthetic histories, expected outputs,
  internal inventory and external SHA. Two unrelated-root replays matched,
  including one launched by the restored tool itself.

## Actual verification and real data

The durable test totals are in [INTEGRITY_REPAIR_20260921_210502/RUN_STATUS.json](INTEGRITY_REPAIR_20260921_210502/RUN_STATUS.json)
and its copied JUnit evidence. The core integrated Windows launcher passed
130 tests with zero failures/errors/skips before the subsequent Step D input
and adapter tests. The final integrated Windows launcher passed **160 tests**
with zero failures/errors/skips. No predecessor's historical 143/35/19 passes are re-counted
as tests executed in this batch.

The new historical run is `C:\Users\zmoor\Documents\forex\stage_c_alignment_integrity_v2\runs\all68-neutral-repair-20260921-210502`; identity `4ebea9916995c3ff3da92c84e37ddfb956f00b1b542aadb52ed5940e9a933523`.
It verified the archive container and all members and issued 68 coverage rows,
68 no-change forecasts and 68 PENDING outcomes. No target was evaluated and no
model or portfolio was fitted. A justified second identical raw pass repaired
resource measurement: the original measurement sampled only the Windows Python
launcher. The corrected process-tree pass took 14.08
seconds, with a sampled aggregate working-set high-water sum of
324.02 MiB. The first 4 MiB figure
is not accepted as runner memory evidence. Both result directories are retained.

The quote qualification verified all 68 actual member hashes/sizes and the exact
ZIP inventory. All 68 have bid/ask candle fields and valid origin side prices;
67 have valid exact 24-hour targets. EUR_DKK lacks the target bar. At the checked
stamps there were no duplicate/crossed/nonpositive/nonfinite quote pairs. This
retracts batch 03's midpoint-only assertion. Candle closes still do not prove
arrival time or simultaneously executable quotes; original first-known/revision
provenance, financing, margin and full execution economics are unqualified.

Step D progressed through two more bounded gates: all 68 quote currencies have
declared USD conversion routes at both checked stamps, across 20 currencies;
own-candle plus conversion support remains 68 origins and 67 targets. The report
preserves the reviewed predecessor's direct/inverse and profit/loss side rules,
without computing performance or inventing missing conversion rates.

The synthetic reference adapter reuses the existing Decimal engine with an
explicit research capital/notional contract. Ten event/arm rows match independent
hand calculations; decision-time sizing stays frozen and changing one arm does
not alter the other. Missing/late conversion or quote evidence rejects actions.
The two required predecessor files were hash-checked and copied; an isolated
replay from an unrelated folder reproduced the entire report byte for byte.
This two-instrument intraday fixture proves bounded mechanics, not full reference
versus optimized parity, real fills or a successful trading strategy.

Full Step D remains partial. A static
feature registry or macro source binding is not populated causal input evidence.

## Recovery and exact resume

The runnable checkpoint is
[INTEGRITY_REPAIR_20260921_210502/checkpoint/forex_integrity_20260921_210502.zip](INTEGRITY_REPAIR_20260921_210502/checkpoint/forex_integrity_20260921_210502.zip).
External SHA-256: `6a7dcd2b8b02c98bbbe79f23b6fc59f0a36136ed09cca7ed6df679e2e9cc0167`. It is 45,274 bytes with 23 members.
Its scope is synthetic all-68 issuance/recovery only. Bulk historical data,
trading state, live services, complete feature/macro/accounting campaigns and
credentials are excluded. The addendum also retains current Stage C source and
tests as a supplemental snapshot; those broader modules may require the prior
audited `trad` recovery tree and are not certified by the fixture replay.

Read [INTEGRITY_REPAIR_20260921_210502/README.md](INTEGRITY_REPAIR_20260921_210502/README.md) for machine-independent bootstrap,
dependencies, exact local verification/resume commands and the scope of the
older project checkpoint. Existing results and old sealed manifests were
preserved. The old documentation manifest has modified current paths; the
addendum records their actual mismatch list and supplies a new manifest rather
than silently rewriting history. Cloud synchronization remains unverified.

## Readiness and next item

- Engineering ready: false; bounded integrity scope passed, complete system pending.
- Forecast evidence: no new model evidence; neutral issuance only.
- Policy evidence: unevaluated for the design campaign.
- Demo authorization: not granted for this campaign.

Step D / WP2-WP8: extend the reviewed synthetic reference-accounting adapter through remaining event-level parity, partial fills/closes, financing, margin/reservations and currency exposure, with isolated policy state and explicit input-tier blockers.

Independent eligible work: actual technical/peer population and non-GPT macro
first-known availability. The first complete all-68 daily/multiday adaptive
campaign follows its causal/accounting dependencies. Broker/service/account
changes, paid calls, GPT comparisons and D-drive operations remain outside scope.
