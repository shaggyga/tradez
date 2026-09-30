# Long-history archive hash verification

On 2026-09-21, the complete read-only SHA-256 of
`RECOVERY_CHECKPOINT_20260921\inputs\long_m1_68.zip` was recomputed over
1,593,012,000 bytes.

Computed SHA-256: `aca163639a18e22b2c60dc4ff36d55d82c655af617836a6d351dd1339e5a6006`

This exactly matches the declared archive hash in `INPUT_ARCHIVES.json` and
the bound hash used by Stage C. No archive contents were extracted, modified,
or copied during verification.

The separately retained, still-excluded native archive was also recomputed:
`native_m1_68.zip` SHA-256 is
`e85ee53bd8391426356c26cc5140e4fe785c600029c0ffc5de1a0ec66e0ba9bf`,
matching its manifest. This verifies integrity only; it does not change its
excluded status pending source selection and spread-correction policy.
