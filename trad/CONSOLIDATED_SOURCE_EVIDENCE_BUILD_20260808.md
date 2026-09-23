# Consolidated Forex source, sentiment, evidence, and improvement build

Generated: 2026-08-08

This is the post-build companion to the immutable August 6 point-in-time audit.
It does not replace or rewrite `COMPLETE_SOURCE_AUDIT_20260806.md`.

## Supported decision

`no_trade`

There are zero confirmed candidates. No real-money route is enabled, no
Practice-007 canary is active, and no signing key was created. The four frozen
proof cohorts and the frozen allocator were not retuned or redefined.

## Current operational state

| Surface | Current state |
|---|---:|
| Governed hypotheses | 49,275 |
| Continue collecting | 40,235 |
| Permanently retired for futility | 9,040 |
| Confirmed candidates | 0 |
| Allocator decisions | 480 |
| Fully matured eligible allocator opportunities | 0 |
| Practice-007 balance / NAV | $41.6042 / $41.6042 |
| Practice-007 cumulative operational P/L | -$8.3430 |
| Open trades / pending orders | 0 / 0 |
| Retained executable pair quotes | 68 / 68 |
| Configured / runtime-observed / operational news sources | 63 / 53 / 49 |
| Free disk | 90.6 GiB / 9.73% |
| Verified inactive-log reclamation | 1.74 GiB net |

The market is weekend-closed. Zero current live-feature instruments is expected;
the last 68-pair close snapshot is retained for diagnostics but is not treated
as synchronized live input.

## Proof-family evidence

Each active cohort has 6,792 governed forecasts and 6,407 valid maturities, but
the aggregate still collapses to only one independent market episode. Raw
forecast count therefore does not imply independent confirmation.

| Frozen H1 family | Liquid-bucket raw N | Liquid effective N | Mean after cost |
|---|---:|---:|---:|
| cross_pair_graph_transfer | 1,041 | 2 | -2.455 pips |
| modern_tabular_probabilistic_repaired | 1,041 | 1 | -1.182 pips |
| probabilistic_state_space | 1,041 | 1 | -2.087 pips |
| ridge_return_repaired | 1,041 | 1 | -1.746 pips |

These are diagnostic point estimates, not promotion evidence. Wide-spread
observations remain segregated and do not masquerade as ordinary liquid-pair
costs.

## Implemented in this build

1. **Canonical source governance.** An append-only registry now stores 112
   material source contracts, 24,560 source-event versions, 622 independent
   story clusters, and 24 supersessions. It distinguishes article, story,
   economic event, and market episode identity and preserves point-in-time
   timestamps, parser/vendor versions, payload hashes, licenses, and cohorts.
2. **Research genealogy.** An append-only registry now contains 49,288
   experiment definitions and current observations, including every governed
   cell plus proof, squeeze, and allocator cohorts. Unknown old parameter-search
   detail is marked `not_reconstructed`.
3. **Independent verification.** A separate read-only implementation rebuilt
   all lifecycle states, futility permanence, allocator state, append-only
   protections, genealogy coverage, and authorization consistency. Current
   result is `MATCH`, with zero failed checks and zero independently confirmed
   candidates.
4. **Knowledge-time replay.** A query now reconstructs only source versions
   whose decision cutoff, effective time, and retrieval time precede the target
   timestamp, applying supersession only after it was locally observed. The
   2026-08-08 13:00 UTC probe returned 24,425 events from 51 sources and 622
   story clusters.
5. **Coverage, synchronization, and drift.** A durable audit reconciles the 68
   instruments, M1/M30/H1/H4 context, eight intrahour horizons, 63 source
   definitions, four proof families, account state, lifecycle, authorization,
   clock mitigation, and source gaps. It records deltas but never retrains
   automatically.
6. **Predefined episode ontology.** Episode categories and dimensions are now
   fixed without reference to strategy profit. Existing cohorts are unchanged;
   the ontology applies only to a newly validated causal labeller/cohort.
7. **Authorization hardening.** A future canary must use a signed schema-v2
   payload with the full Practice-007 account, exact signal/hypothesis/proof/
   allocator identities, exact instrument/direction, explicit unit/notional/
   total-exposure ceilings, issued/expiry times, and a one-time nonce. Atomic
   consumption prevents two executors from using the same authorization.
8. **Immediate pre-write revalidation.** Expiry, revocation/tampering,
   lifecycle state, independent-verifier state, quote freshness, and unit caps
   are checked again immediately before the broker write.
9. **Durable supervision.** Source governance, genealogy, independent
   verification, and integrity auditing are configured as hidden BelowNormal
   workers. Expensive evidence inventory subqueries now publish explicit
   progress phases.
10. **Restart-safe quote coverage.** The asynchronous research snapshot now
    republishes each increase during OANDA's initial stream snapshot, so the
    95% threshold cannot strand the canonical mirror at 65/68. It can also seed
    earlier-closing instruments from the independent stream, but retained rows
    are explicitly non-executable and never enter the executor's live map.
    Both transports currently reconcile at 68/68; live-feature coverage remains
    zero because all broker timestamps are stale during the weekend close.
11. **Evidence-preserving storage control.** A supervised read-only guard now
    records disk headroom, SQLite allocation/free pages, WALs, logs, and
    verified archive size. A second supervised worker compresses only inactive
    rotated JSONL logs and requires a byte-for-byte gzip round trip, SHA-256
    hashes, and a manifest before deleting the original. Fifteen rotated logs
    have been reduced from 2,014,827,818 bytes to 151,511,562 compressed bytes,
    reclaiming 1,863,303,517 net bytes after manifests. The archives remain
    recoverable and no forecast, outcome, source, or causal-news evidence was
    pruned.
12. **Verified shadow-outcome archive.** Three Parquet archive parts now carry
    62,015 rows with row-count/schema checks and SHA-256 manifests. A later
    no-new-work cycle was idempotent. Live `VACUUM` is forbidden; reusable
    SQLite free pages and checkpointed WAL allocation are reported rather than
    destructively compacted under active writers.
13. **Calendar causality repair.** The UK PMI working-day calendar now includes
    the official 2027 England/Wales bank holidays. The CFTC forward-positioning
    collector now maps report dates to the tentative official 2026 publication
    schedule, including delayed holiday releases, while truthfully keeping
    older history and exceptional announcements blocked.

## Existing components reused rather than duplicated

- OANDA executable bid/ask, spread, completed candles, quote-intensity proxy,
  and Practice-007 account boundary.
- The 63-source news configuration, including existing GDELT, Alpha Vantage,
  Trading Economics, CFTC, CME, and FRED/ALFRED declarations.
- The live separate CFTC forward collector.
- Immutable forecast/outcome ledgers, exact-horizon maturity, factor/episode
  deduplication, sequential inference, hierarchical FDR, futility, untouched
  confirmation, allocator proof, practice attribution, and routeability sentinel.

## Truthful source disposition

| Source family | Disposition | Next gate |
|---|---|---|
| CFTC COT | Forward integrity collection active | Publication exceptions, vintage replay, placebos |
| GDELT | Configured discovery/media context | Pacing, story-cluster and incremental-value validation |
| Alpha Vantage | Configured, credential missing | Licensed key and integrity-only collection |
| Trading Economics consensus | Configured, credential missing | Prospective pre-release consensus archive |
| Myfxbook | Not collected | Verify API/license/retention rights |
| IG client sentiment | Not collected | Verify official programmatic access and rights |
| FRED/ALFRED | Declared, runtime adapter incomplete | Vintage-safe point-in-time ingestion |
| CME futures/options | Declared, runtime adapter incomplete | Start with daily volume/OI/settlement/IV |
| Rates/policy paths | Missing | Source and license causal curves |
| Multi-venue flow/depth | Missing and expensive | Defer until slower orthogonal sources prove value |

Retail, institutional, media, macro-expectation, and market-implied sentiment
remain separate populations. The system does not create a generic sentiment
score.

## Validation

- Executor, strategy-lab, transport, and governance safety: 150 tests plus 220
  subtests passed.
- Source governance, genealogy, independent verification, synchronization, and
  knowledge replay focused suites passed.
- New source and genealogy SQLite quick checks returned `ok`.
- Current independent verifier returned `MATCH` with no failed check.

## Remaining blockers and gates

1. The active full evidence rebuild must complete, then a second cycle must
   complete without restart and with matching lifecycle/futility counts. The
   current cycle reached 1,597,217 scanned signal rows, about 2.28 GB measured
   peak resident memory, and the final `persisting_evidence` phase. The older
   full edge report remains explicitly stale until commit.
2. The four proof families and allocator need genuinely independent episodes,
   not more correlated forecast rows.
3. External retail/vendor feeds need lawful access, credentials, and prospective
   timestamp integrity before model fitting.
4. Every new source must pass integrity collection, point-in-time replay,
   placebos, incremental-information and ablation tests, frozen hypotheses,
   factor/episode-deduplicated evidence, sequential/multiplicity control, and
   untouched confirmation.
5. Host time remains about one minute behind broker time. Broker timestamp
   normalization mitigates causal labeling, but OS clock synchronization still
   requires administrator action.

No item in this report grants execution eligibility. A future practice canary
still requires a genuine immutable `confirmed_candidate` plus a fresh narrow
authorization. Real-money routing remains outside the project phase.
