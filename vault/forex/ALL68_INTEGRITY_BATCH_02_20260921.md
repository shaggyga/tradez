# Alignment integrity batch 02 — bounded all-68 issuance and recovery

## Scope

This handoff advances `alignment_integrity_v2` in the isolated workspace at
`C:\Users\zmoor\Documents\forex\stage_c_alignment_integrity_v2`. It does
not modify `trad`, Stage C v1 outputs, raw inputs, services, accounts, or D:
content. GPT/advisor comparisons and paid calls remain deferred.

The implemented consumer is `all68_neutral_runner_v2.py` SHA-256
`699d7ffae14c1349b75d893f0932b9de0b38aafe2a1725f874cec588757d92d4`.
It uses `contracts.py` SHA-256
`3a1776f0862f6bb4bc3ba73f4c0febec573350d712aa50e9dbcc0e253b11aecd`
and `publication.py` SHA-256
`1cefcf56b1de746cff84adf661a773472afd214a5e4413ef0ad54dbfa236f7d6`.

## Completed bounded artifact

Run ID: `all68-neutral-v2-20260921`.

- Input: audited long 68-member archive, SHA-256
  `aca163639a18e22b2c60dc4ff36d55d82c655af617836a6d351dd1339e5a6006`.
- Origin: `2024-06-24T00:00:00+00:00`; decision time: completed-bar end one
  minute later; horizon: 86,400 seconds.
- Output: 68 coverage records, 68 no-change forecasts, and 68 separate
  `PENDING` outcome records. All instruments were eligible for that origin.
- No target bar was read and no outcome was evaluated. The forecast file has
  no actual/outcome/settlement fields.
- Effective run-identity fingerprint:
  `06d01a0580fc3d8ff906ed627bff7efcd79cc28ad6bc2152464676c7643990a8`.

The compact package is
`stage_c_alignment_integrity_v2\fixtures\all68-neutral-v2-20260921.zip`,
SHA-256 `0128ce425a314242c7a50fdc680ac50fb60d9278b0c095a8b6191a42ab2c1d82`.
It was restored into the distinct `restores\all68-neutral-v2-20260921` path
and all four required payload hashes and the recomputed run identity verified.

## Verification actually run

1. `pytest` on `test_alignment_integrity.py`: **14 passed**, including a
   real-reader perturbation test: changing a later raw endpoint from 1.2 to
   9.9 leaves the earlier origin-only issuance input unchanged.
2. `py_compile` on the contracts, publisher, runner and recovery CLI: passed.
3. The real bounded runner: completed, 68 of 68 eligible, pending outcomes
   only.
4. Completed-run verifier: recomputed identity and verified every manifested
   payload.
5. Actual package/relocated restore: completed with four payloads restored and
   verified.

## Readiness and limitations

- Engineering readiness: **false**. This is a real bounded issuance/recovery
  check, but not the complete timing/calendar/recovery acceptance package.
- Forecast evidence: **no predictive evidence**. The predictor is explicitly
  no-change and outcomes were not evaluated.
- Policy evidence: **unevaluated**. No policy ledger, accounting, fills, or
  P&L was produced.
- Demo authorization: **not granted**. No broker or order route was used.
- Checkpoint scope: package includes the completed v2 run outputs and identity;
  it is not a full `trad` or runtime backup. OneDrive synchronization remains
  unverified.

## Exact command before resuming

```powershell
& 'C:\Users\zmoor\Documents\forex\stage_c_alignment_integrity_v2\verify_alignment_integrity.ps1'
```

## Exact next queue item

`alignment_integrity_v2` remains open: complete its remaining B/C acceptance
gates before a fitted campaign. The next item is interrupted-versus-
uninterrupted publication parity, followed by the declared trading-session
calendar fixtures (Friday/Sunday/DST). Do not advance to
accounting or fitted all-68 campaigns until those gates are evidenced.
