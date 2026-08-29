# Complete Forex source audit — post-upgrade proof and governance state

Generated: `2026-08-06T18:36:19.516483+00:00` from `C:\Users\zmoor\Documents\forex\trad`.

This document replaces the earlier 11:10 UTC inventory as the proof/governance
checkpoint captured at 18:36 UTC. The later operational lifecycle, permanent
futility retirement, allocator-proof, and Practice-007 attribution state is in
`EVIDENCE_ACCUMULATION_FUTILITY_ALLOCATOR_AUDIT_20260806.md`. The original is
preserved as `COMPLETE_SOURCE_AUDIT_20260806.md` and explicitly labeled
as the pre-upgrade baseline; incompatible counts are not merged.

## Decision

- Allocator: **no_trade**.
- Canonical cells: `18,858`.
- Multiplicity-adjusted discovery candidates: `0`.
- Locked prospective confirmations: `0`.
- Practice canary auto-routing: disabled.
- Real-money routing: disabled and outside this phase.

## Canonical proof contract

- Forecasts, outcomes, integrity misses, cohort definitions, transitions,
  completed-day snapshots, and governance snapshots are append-only.
- Evaluation unit: family × pair × horizon × UTC session × executable
  spread/liquidity bucket, deduplicated by signed currency factor and
  market episode.
- Promotion requires discovery control, time-uniform monitoring control,
  and a later untouched confirmation cohort. A discovery window cannot
  certify itself.
- Minimum economic effects are explicit by horizon and liquidity. Small
  positive EV is not a sufficient trading result.
- Pooled graph/tabular estimates may prioritize research, but direct
  cell-level prospective evidence is mandatory.

## Evidence census

- Usable exact-horizon signal outcomes: `335,328`.
- Raw-N percentiles: `{"p00": 1, "p10": 1.0, "p100": 237, "p25": 2.0, "p50": 7.0, "p75": 20.0, "p90": 49.0, "p95": 74.0, "p99": 141.0}`.
- Effective-N percentiles: `{"p00": 1, "p10": 1.0, "p100": 125, "p25": 1.0, "p50": 2.0, "p75": 6.0, "p90": 14.0, "p95": 21.0, "p99": 44.0}`.
- Effective N ≥25/50/100/200/500: `{"100": 12, "200": 0, "25": 726, "50": 134, "500": 0}`.
- Positive point EV / positive unadjusted LCB / multiplicity survivors: `2170` / `38` / `0`.
- Economically inadequate at current power: `8989` cells.
- Blocked only by sample size: `0` cells.
- Hierarchical FDR survivors: `0` family/horizon and `0` cell.
- Governance fingerprint: `7363430454082fa033606e440da8795d1f3027a186a2e4aa15d3d6f718004147`.

## Frozen proof cohorts

| Family | Active cohort | Prior/superseded forecasts excluded | Governed forecasts | Matured |
|---|---|---:|---:|---:|
| cross_pair_graph_transfer | `cross_pair_graph_transfer.20260806.4c0f2489c29a711e` | 700 | 64 | 0 |
| modern_tabular_probabilistic_repaired | `modern_tabular_probabilistic_repaired.20260806.904bf7e3bec45447` | 700 | 64 | 0 |
| probabilistic_state_space | `probabilistic_state_space.20260806.e1282daf5077122e` | 700 | 64 | 0 |
| ridge_return_repaired | `ridge_return_repaired.20260806.9410e27583e10819` | 700 | 64 | 0 |

The proof worker is observation-only, has `0` errors, and cannot place orders.

## Graduation ladder

- Stage A contract validity: `True`.
- Stage B discovery candidates: `0`.
- Stage C locked confirmations: `0`.
- Stage D practice canaries: `0`.

## News and macro state

- News sources: `63` configured, `55` operational, `54` healthy.
- Macro ledger: `100` releases, `83` scheduled, `14` actuals.
- Causal pre-release consensus observations: `0`; usable actual-plus-consensus surprises: `0`.
- Post-release consensus remains rejected unless a verifiable causal
  pre-release snapshot exists. Narrative news cannot substitute.

## Account and execution separation

- Practice 007 balance/NAV: `41.6042` / `41.6042`.
- Open trades/orders: `0` / `0`.
- Proof and governance modules have no order route. Confirmed research
  would still require a separately locked Practice 007 canary whose
  purpose is execution validation, not rediscovery of profitability.

## Remaining blockers

1. Governed H1 proof forecasts need later executable-side maturities.
2. No discovery candidate survives the full multiplicity, sequential,
   economic-effect, stability, cost, and concentration contract.
3. The macro ledger still lacks a causal consensus source.
4. Order book, position book, and informative pricing depth remain
   unavailable and excluded.
5. One-bar/delayed-entry path stress requires a timestamp-complete
   executable quote path before any candidate confirmation.

The phase succeeds either by confirming robust positive after-cost edge
or by accumulating enough power to reject the configured minimum
economic effect. `no_trade` is therefore a valid and expected result.
