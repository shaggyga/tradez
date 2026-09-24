# Later remaining forecast surface

This package supplies exact later common-target forecasts to the existing native
curve/Decimal candidate consumer. It does not replay a portfolio or establish
profitability, historical issuance, live readiness, or demo authorization.

Read the Vault current queue and checkpoint before running. The frozen contract
is `LATER_REMAINING_SURFACE_CONTRACT_V2.json`. The current recipe is
`LATER_REMAINING_SURFACE_OPERATOR_RECIPE_REVIEWED_V2.json`; its exact SHA must come
from the verified checkpoint receipt, never be accepted from an arbitrary file.
The original recipe and interrupted evidence remain preserved.

## Scope and reuse

- All 68 original instruments; eight six-hour origins from 2024-07-24 18:01 UTC
  to 2024-07-26 12:01 UTC; exact common endpoint 2024-07-26 18:01 UTC.
- The previously suggested Saturday endpoint had zero available endpoint labels.
  Friday has 66/68; selection used availability before new scores. Missing pairs
  remain in coverage, without a future-conditioned universe filter.
- Reuse all eight original signed fit pairs and three compatible absolute fit
  pairs. Only absolute targets 360/1080/1800/2160/2520 minutes receive new fits.
  Original training rows, statistics, Ridge/HGB settings and first cutoff remain
  fixed. Five new pairs contain ten estimators. Restore must use saved weights.
- Build the original 20-origin prequential stream at all eight exact horizons.
  Each later layer uses only earlier forecasts with outcomes mature by its
  cutoff. Frozen and expanding layers retain original support/penalty settings.
- For each base learner: raw unrestricted; raw matched, signed-only and magnitude
  interaction under each update mode. Matched controls share exactly the same
  causal support. Unsupported layers produce coverage reasons, never substitutes.

## Timing and authority

One hypothetical worker reserves sixteen 30-second fit-pair slots, skips each
16-second prediction window, then takes a 120-second prewarm. The first origin
is unavailable; later horizon slots are sequential. The selected remaining
horizon goes first. Current inference plus layer fit must finish in one second;
all selected-target native preparation must finish in two. Runtime CPU discovery
is in prewarm. Every current feature matrix is fresh; cached objects are fixed
model/runtime state only. No claim of actual historical publication is made.

Native source is authenticated at both batch boundaries, with input/clock/hash
validation for each packet. Exact prediction recomputation precedes the common
Decimal consumer. Zero-cost consumption exercises the interface only. The next
policy package must explicitly shift scenario rollover and freeze identical
cost/risk/sizing assumptions; the old dated cost scenario cannot be reused as-is.

The initial full preparation exceeded its limit because source authentication was
repeated per packet. The successor batches those checks, preserving the numerical
contract and all already published partial forecast/packet bytes. It also pins
the Decimal core and prohibited shadow-import paths in the operator recipe.

## Operator and recovery

Use the pinned Python environment and call `later_surface_operator_v2.py` with
`status`, `run`, `resume` or `verify`, plus `--recipe`, `--recipe-sha256`, `--paths`
and `--runs-dir`. The path map has exactly technical, remaining, absolute, slices,
and trad. Complete dependency manifests, slice hashes, source closure, native
predecessors, environment and explicit recipe pin must match before model loading.

Partial runs preserve journaled weights. `resume --reuse-only` refuses missing
new-target saved weights. Completion requires the full payload inventory, all
7,616 coverage slots, matched controls, resource receipts and native timing checks.
Actual fits belong to attempt receipts; artifact inventory is not a refit count.

`later_surface_checkpoint_v2.py export` bundles authenticated inputs, slices,
native source and saved weights. `restore --run-tests` uses a fresh directory and
must match every scientific payload with zero base refits. Small causal layer
regressions are explicitly recomputed. Its bounded new schema allows 16 MiB per
member for the original 9.4 MB labels; existing checkpoint limits are unchanged.

## Exact next package

`later_remaining_layer_policy_comparison_v2`: use this verified surface to compare
position rotation under identical ledger, accounting, risk, sizing, fill, cost
and financing assumptions across coverage-matched variants. Freeze shifted dates
before replay; preserve unavailable terminal marks explicitly. Report all arms
and unsupported scopes. Do not tune after inspecting these development results.
GPT/advisor comparisons and broker/service/account operations remain deferred.
