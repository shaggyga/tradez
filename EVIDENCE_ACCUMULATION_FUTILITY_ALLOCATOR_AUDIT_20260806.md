# Evidence Accumulation, Futility Retirement, and Allocator Proof

Generated from the canonical workspace `C:\Users\zmoor\Documents\forex\trad` on 2026-08-06.

## Governed decision

- Current production decision: **no_trade**.
- Real-money routing: **disabled**.
- New research and allocator cohorts: **shadow-only**.
- Practice-007 canary auto-routing: **disabled until a frozen hypothesis completes adjusted discovery and a later untouched confirmation**.
- The final Practice-007 entry boundary now independently requires a fresh,
  explicit authorization matching the exact signal ID, proof cohort ID, and
  allocator cohort ID. Legacy qualification alone cannot route an entry.
- Manual/discretionary orders and manual closes: **not authorized and not performed**.

## Permanent hypothesis lifecycle

The fixed historical governance snapshot was reproduced at source row high-water `1,810,066` before any lifecycle rows were committed.

- Snapshot governance SHA-256: `7363430454082fa033606e440da8795d1f3027a186a2e4aa15d3d6f718004147`.
- Reproduced governed cells: **18,858**.
- Permanently retired for fixed-snapshot economic futility: **8,989**.
- Continue collecting: **9,869**.
- Confirmed candidates: **0**.
- Retirement evidence SHA-256: `24aeb63dd2e7708d6ca9328151a94d1183243c9666e37f4daf278f41ac3e19f0`.

Retirement records and lifecycle events are append-only. A retired hypothesis cannot transition back to collecting. Reconsideration requires a new hypothesis ID plus a material change class such as a genuinely new source, target, horizon, causal variable, executable observation, execution policy, regime definition, or structural cross-market relationship. A rename, small hyperparameter adjustment, or the same information under a new model name is explicitly insufficient.

The later live cache currently contains **21,012** cells. The additional
**2,154** cells entered `continue_collecting`; no later cell crossed a time-
uniform futility boundary and none confirmed. Current totals are **12,023
collecting / 8,989 permanently retired / 0 confirmed**. The fixed high-water
snapshot and its 8,989 retirements remain unchanged; live growth does not
rewrite them.

The one-time fixed snapshot uses the already-frozen ordinary upper-bound result. Every later retirement requires a time-uniform upper confidence bound below the configured minimum tradable effect. Repeated ordinary intervals cannot retire or promote a cell.

## Frozen allocator proof

The allocator is now its own prospective hypothesis. It records the full causal candidate set, filters, rankings, factor conflicts, capacity and re-entry vetoes, selected action or explicit no-trade, best rejected alternative, and comparator arms.

Primary frozen cohort:

- Policy: `governed_top_one_allocator_v1`.
- Active cohort: `governed_top_one_allocator_v1.discovery.20260806.ee36ab358eb3fea2`.
- Decision cadence: fixed five-minute buckets with inclusion probability `1.0`.
- First active decision: **982 prefilter candidates, 0 policy-eligible, explicit no-trade**.
- Decisions / proof outcomes: **6 / 0**; an empty eligible set does not count as a zero-return observation or increase effective sample size.
- Lifecycle: `continue_collecting`.

An earlier engineering cohort, `governed_top_one_allocator_v1.discovery.20260806.84bb33ffcb1f6658`, contains one no-trade transport snapshot. It was superseded when testing showed that no-op timestamps must not be counted as statistical evidence. It remains immutable and is excluded from the active proof cohort.

Comparators are frozen as:

1. No trade.
2. Deterministic random selection from the same eligible set.
3. Highest raw predicted return.
4. Lowest-cost eligible candidate.
5. Hold the current position rather than rotate.
6. Strongest individual constituent family (`volatility_squeeze_breakout`).

Only timestamps with a genuinely eligible frozen-policy opportunity enter comparative proof. Candidate outcomes use the first executable bid/ask snapshot at or after the declared horizon and reject missing or more-than-15-second-late endpoints. Discovery cannot confirm itself, and a discovery result can only open a strictly later confirmation cohort; it cannot route Practice 007.

## Practice-007 accounting

Account continuity is preserved; no reset was performed.

- Account: `101-001-37981792-007`, environment `practice`.
- Balance / NAV: **$41.6042 / $41.6042**.
- Cumulative account P/L: **-$8.3430**.
- Open trades / pending orders / margin used: **0 / 0 / $0.0000**.
- Existing execution attempts: **38**; fills: **33**.
- All existing executions are classified `legacy_pre_governance`.
- Governed but incompletely attributed executions: **0**.

Account cumulative P/L remains operational context and cannot judge new governed cohorts. Future canary attribution requires both an exact immutable proof cohort ID and allocator cohort ID. Missing either ID fails closed into `unattributed_governed_blocked`. Separate virtual equity curves start at zero for each exact confirmed canary cohort without resetting the broker account.

The daily routeability sentinel passed for Practice 007. It exercised the production executor's pure order builder and verified practice environment, account suffix, MARKET/FOK/OPEN_ONLY form, one-unit synthetic sizing, price bound, broker-side stop, and client tag. It has no broker client, attempted no submission, and performed no account action.

The running fast executor now consumes
`practice_007_governed_canary_authorization_v1.json` at the final order boundary.
The current artifact is fresh and says `entry_authorized=false`, contains zero
authorized entries, and reports zero confirmed candidates. Missing, malformed,
stale, wrong-environment, wrong-account, expired, unconfirmed, or version-
mismatched authorization fails closed. Open-position management runs before
entry selection and remains active; the gate only restricts new orders. At the
validated checkpoint the executor was healthy with 68 priced instruments,
zero fills in the restarted session, and `real_money_routing=false`; Practice
007 remained flat with no pending orders.

The authorization document cannot self-certify a canary. Before delegating to
the inherited order-submission method, the executor independently reads the
immutable lifecycle database and requires the exact governed hypothesis's
latest event to be `confirmed_candidate` with the same proof cohort. A forged
`confirmed_candidate=true` authorization against a collecting or retired
hypothesis is rejected.

## Proof-family evidence velocity

The four immutable H1 proof cohorts remain unchanged. At the latest validated
checkpoint each had produced **437** prospective forecasts and matured **118**
valid outcomes. The first 64 endpoints per cohort were permanently rejected
after a maturity-worker restart loop made the executable quote 27 seconds
late; they were not backfilled. The queue/supervision defect was repaired, and
all subsequent reported proof outcomes matured within the governed endpoint
contract.

Those 118 contemporaneous pair forecasts still reduce to only **one aggregate
independent market episode per family** after signed-currency-factor propagation
is collapsed. All aggregate point estimates, MDEs, and distances therefore
remain diagnostic; MDE and required-additional-N are unavailable until variance
can be estimated from at least two independent episodes. In the current liquid
bucket, average after-cost results remain negative for all four families:
graph-transfer `-1.411`, tabular `-1.168`, state-space `-3.846`, and ridge
`-0.650` pips. None is a candidate.

Every cohort report now includes produced, matured, raw and effective N; independent episodes and signed-currency-factor clusters; effective observations per day; after-cost EV and time-uniform bounds; minimum detectable effect; distance to promotion and futility; expected additional effective N; concentration by pair/session/day/episode; and conflict/cost/invalid-data exclusion rates. Graph or hierarchical pooled confidence remains diagnostic and cannot substitute for direct cell evidence.

The four proof contracts prohibit interim retuning, adaptive pair selection, adaptive forecast frequency, transitional-record merging, and post-hoc bucket changes.

## Macro consensus and post-null rule

Structured macro surprise remains blocked because there are still zero provable pre-release consensus observations. Consensus retrieved after release cannot be backfilled into proof evidence. This does not block the price-derived proof cohorts.

If all four proof families and the frozen allocator reject the minimum tradable effect with adequate power, the system will record that the currently represented price-derived predictor space does not support the configured after-cost objective. Another model using essentially the same information will not qualify as a new research direction.

## Runtime and validation

- The lifecycle, allocator, accounting, and sentinel databases use immutable rows and no-update/no-delete triggers.
- Read-only SQLite `quick_check` is `ok` for the lifecycle, allocator,
  governed-accounting, and edge-evidence databases.
- A replaceable current-cell cache is operational only; immutable daily evidence and frozen governance snapshots remain unchanged.
- The hidden evidence-operations worker is supervised at below-normal priority and has no order authority.
- Focused validation: **37 tests passed** across lifecycle permanence, material reconsideration, allocator cohort rollover, no-op evidence exclusion, deterministic comparators, executable bid/ask maturity, due-endpoint queue priority, attribution fail-close, exact canary authorization, independent lifecycle confirmation, broker-submission denial, authorization reporting, routeability without submission, edge evidence, sequential governance, and frozen predictor contracts.

The source audit `COMPLETE_SOURCE_AUDIT_20260806.md` remains the labeled pre-upgrade baseline. `COMPLETE_SOURCE_AUDIT_POST_UPGRADE_20260806.md` remains the proof/governance checkpoint. This file is the current operational-phase addendum; incompatible historical counts are not merged.
