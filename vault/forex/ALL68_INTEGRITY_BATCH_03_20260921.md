# Alignment integrity batch 03 — resume, session calendar, and feature binding

## Completed work

`stage_c_alignment_integrity_v2` now completes three additional offline design
gates without changing `trad`, historical results, raw inputs, services,
accounts, or D: content.

1. **WP10 / TST48 resume parity.** `publication.py` now supports an explicit
   recovery path only when a lock has an expired lease, the identity matches,
   the lock is on this host, and its recorded process is dead. It never removes
   an active or foreign-host lock. Deterministic payloads can be reused only
   when their bytes hash exactly. An injected interruption after the first
   artifact now resumes to the same two payload descriptors and bytes as an
   uninterrupted run.
2. **WP1/WP2 / TST32 session targets.** `calendar_contract.py` now separates
   the existing UTC weekday diagnostic from `fx_ny_1700_session_close.v1`.
   The latter has 17:00 America/New_York close boundaries, skips Saturday,
   handles both 2024 DST transitions, and accepts explicit caller-supplied
   full closure dates. It does not invent a historical venue-holiday feed.
3. **WP4 feature lineage.** `feature_registry_audit_v2.py` binds the retained
   228-row registry: 216 `m1__` rows exactly match the current pure technical
   kernel and 12 `peer__` rows exactly match the current exact-clock peer-panel
   generator. The audit rejects kernel drift, duplicate names, missing timing
   fields, nonexact elapsed lookbacks, and imputation claims.
4. **WP6 non-GPT macro lineage.** `macro_lineage_audit_v2.py` binds the
   official exact-clock registry, causal consensus-import contract, immutable
   macro ledger and exact-clock audit source without fetching any provider. It
   verifies direction-free/execution-ineligible exact clocks and that numeric
   surprises fail closed without a trusted, pre-release archived capture.

## Verification actually run

- v2 focused suite: **17 passed**.
- Windows v2 verifier: **17 passed**.
- Existing pure accounting predecessor suites:
  `test_oanda_curve_management_replay_v1.py` and
  `test_causal_score_management_replay_v1.py`: **143 passed**.
- Feature-binding completed run:
  `C:\Users\zmoor\Documents\forex\stage_c_alignment_integrity_v2\runs\feature-registry-v2-20260921`.
  Its completion manifest and both payload hashes verified. Its identity binds
  the technical source, stored registry and audit source.
- Peer-panel suite: **35 passed**. The current peer-panel source is included in
  the later `feature-registry-v2-peer-20260921` binding.
- Macro contract suites: **19 passed**. Static macro lineage completed run:
  `C:\Users\zmoor\Documents\forex\stage_c_alignment_integrity_v2\runs\macro-lineage-v2-20260921`,
  with its identity and payload hash verified.

## Accounting dependency disposition

The inspected accounting predecessor
`trad\oanda_curve_management_replay_v1.py` (SHA-256
`52fd04c6b087df4be775e490002afd0744caeb245604b8f577ab0fcff41de8f3`)
already implements Decimal calculations, decision-time integer sizing,
direct/inverse executable bid/ask conversion, isolated policy arms and exact
execution-time checks. Its stated limits are material: no financing, margin,
broker fills or account access.

The audited long archive used by the current v2 runner contains raw midpoint
bars only. It lacks decision/execution bid/ask and conversion quotes, so exact
cost-aware accounting cannot be connected to this data slice without invented
facts. This blocks the accounting-dependent campaign path but does not block
further offline feature/macro lineage work.

## Readiness

- Engineering readiness: **false**; timing/publication/session contracts have
  advanced, but an integrated campaign and accounting data tier are absent.
- Forecast evidence: **none**; no-change output remains unevaluated.
- Policy evidence: **unevaluated**; predecessor mechanics passing tests is not
  campaign evidence.
- Demo authorization: **not granted**; no service or order route was used.

## Exact next item

Continue WP4/WP6 by assessing populated peer and macro availability on an
explicit historical/first-known input tier. Do not run a fitted/policy campaign
until that population is causal and executable accounting bid/ask/conversion
inputs are bound. Keep GPT/advisor comparisons deferred.

## Checkpoint and exact resume command

The isolated implementation and completed receipts are under
`C:\Users\zmoor\Documents\forex\stage_c_alignment_integrity_v2`, especially
`runs\all68-neutral-v2-20260921`,
`runs\feature-registry-v2-peer-20260921`, and
`runs\macro-lineage-v2-20260921`. The prior compact relocated fixture remains
`fixtures\all68-neutral-v2-20260921.zip`.

```powershell
& 'C:\Users\zmoor\Documents\forex\stage_c_alignment_integrity_v2\verify_alignment_integrity.ps1'
```

Then read `C:\Users\zmoor\OneDrive\thevault\projects\forex\FOREX_NEXT_PROMPT.md`
and start the populated peer/macro first-known availability package. Do not
restart old writers or rerun a completed run ID.
