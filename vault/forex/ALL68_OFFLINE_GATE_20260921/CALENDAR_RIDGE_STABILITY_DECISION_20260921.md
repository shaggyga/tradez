# Calendar ridge stability decision — 2026-09-21

## Evidence reviewed

The same all-68 endpoint-only UTC calendar-target pipeline ran on three separately stored, fixed held-forward blocks. All use causal 60m/240m/1440m returns, quoted spread, UTC time sine, pooled ridge lambda 20, and no-change comparison.

| Block | One-day MAE: ridge / no-change | Two-day MAE: ridge / no-change | Five-day MAE: ridge / no-change | Decision |
|---|---|---|---|---|
| July test | 20.25 / 20.44 | 46.34 / 46.64 | 84.74 / 89.04 | Early favorable diagnostic only |
| August test | 24.85 / 24.78 | 41.58 / 39.50 | 60.08 / 58.76 | Negative at every target |
| September test | 31.54 / 31.14 | 44.81 / 42.56 | 71.81 / 68.97 | Negative at every target |

August two/five-day directional accuracy was 46%/46%; September was 39%/44%. The first block is not selected as evidence after these later results.

## Decision

**Retire the pooled ridge feature/model combination from policy and execution research.** Retain its code, immutable forecast tapes, and all three reports as a reproducible negative benchmark. It has no model-admission status, may not generate candidate actions, and may not be presented as trading evidence.

This decision does not reject endpoint-only calendar targets themselves. Those targets remain useful for future forecast-skill research, subject to their endpoint-only/no-execution boundary. A future model must be evaluated on prespecified repeated blocks, with interval-aware purge and episode/block uncertainty before any admission review.

## September artifact fingerprints

Report SHA-256: `09240265f9e34015556b61ed0ea5aad28ee5c5999276c97520c4a31796318007`  
Forecast tape SHA-256: `8c8bdcf0f641e5d31d05de978abad7c524d8bdf145e1d36c27cb532f241b6b3f`

Advisor/GPT comparisons remain deferred. No broker, network, account, or trading actions were used.
