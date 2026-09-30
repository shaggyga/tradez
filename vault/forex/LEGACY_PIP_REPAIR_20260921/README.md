# Legacy pip-unit repair — isolated result

This package records the first engineering milestone after the September 21
recovery checkpoint. It is an **unmerged isolated repair**. The live project,
running processes, accounts, and original data were not changed.

## Result

The legacy alignment and rotation path used a quote-currency shortcut for pip
sizes. It mismeasured EUR_HUF, HKD_JPY, USD_HUF and USD_THB. The repair replaces
that shortcut with the reviewed hash-bound 68-instrument map in
`pair_local_operational_v2_20260913.json`, matching the newer forward updater.
Unknown instruments fail closed.

The isolated test suite passed 11 checks on September 21, 2026. It verifies all
68 registered values, each exceptional pair, synthetic and observed quote
spread units, an HUF ATR/range example, and fail-closed behavior. The copied
source/configuration inputs were byte-identical before and after the test.

## Contents

- `PATCH_INSTRUCTIONS.md` — the exact small source change for a later reviewed
  integration.
- `test_legacy_rotation_pip_units.py` — the validated regression tests.
- `PIP_UNIT_TEST_RESULTS.json` — machine-readable test receipt and input hashes.
- `MANIFEST.json` — package status and immutable input/output hashes.

The rotation consumer itself is intentionally not copied here: it contains
credential-handling code. The test used a private isolated copy only and never
resolved credentials or made requests.

## Next work

Keep this repair unmerged until it is exercised through the separate all-68,
global-clock offline comparison. That comparison must use the two audited
history vintages separately, model at least the 5 to 1,440 minute horizons,
and emit chronological and execution-accounting receipts. It must not start a
runtime, access a broker, or use paid/model services.
