# Forex performance audit — 5 September 2026

The evidence supports `no_trade`. The repaired project remains stopped. Saved research results do not establish a profitable strategy, and the new publication-clock cohorts have not collected live evidence. This review separates saved trading observations, offline repair benchmarks and resource costs.

The machine-readable companion is source-root `FOREX_PERFORMANCE_AUDIT_20260905.json`, exported to the vault as `PERFORMANCE_AUDIT_CURRENT.json`. It binds each saved input to its SHA-256 and timestamp. Measurements were made locally on September 5, with no broker requests or production database connections. SQLite storage figures use file metadata only.

## Trading and research results

The saved accounting report at 2026-09-05T01:41:25Z records balance/NAV **41.6042**, cumulative account P/L **-8.343**, no open trades and no pending orders. Its attribution record contains 38 attempts and 33 fills, but only **one audited closed trade**, all in the legacy pre-governance bucket. That trade records -0.093 account units and -10 pips. There is no governed cohort equity curve. The full account loss cannot be attributed to the present models from this record; return, win rate, Sharpe ratio and maximum drawdown are not established by a complete reconciled history.

The saved lifecycle at 01:41:08Z records **53,357 hypotheses, zero confirmed candidates, 43,872 continuing hypotheses and 9,485 futility rejections**. Approximately 25,626 forecasts per active family reduce to only 8–9 effective episodes after dependence controls. Producing more correlated forecasts does not supply equivalent independent evidence.

| Active family | Effective episodes | Published pooled after-cost EV, pips | Clipped excess lower / upper confidence bounds, pips |
|---|---:|---:|---:|
| cross_pair_graph_transfer | 8 | -111.24 | -134.77 / 47.83 |
| modern_tabular_probabilistic_repaired | 8 | -118.72 | -151.30 / 31.30 |
| probabilistic_state_space | 8 | -135.95 | -150.72 / 31.88 |
| ridge_return_repaired | 9 | -120.86 | -140.77 / 33.57 |

These are the producer's pooled diagnostic metrics across pairs and cells, with different pip scales; they are not expected account returns. All four raw point estimates are negative. The alpha-spending bounds concern net pips minus the economic threshold, clipped to the configured ±60-pip range; they are not confidence intervals for the adjacent raw pooled EV. All four families require direct-cell prospective evidence. The explicit `pooled_model_confidence_cannot_promote` flag is true for cross-pair and modern-tabular families and false for the other two. These publications do not establish independent, prospective, after-cost cell confirmation.

The frozen source V8 snapshot contains 13 diagnostic events, 903 forecasts, 326 non-abstaining forecasts and **zero prospective proof forecasts**. Rank V7 has zero decisions. These old contracts remain preserved diagnostics; the repaired V9 source and V8 rank configurations ship disabled and import no predecessor ledger. No repaired-source profitability is claimed.

## Runtime and latency

The single saved execution latency observation reports 1,296 ms forecast-to-submit and 72.646 ms order roundtrip. One sample cannot establish latency percentiles, reliability or sustained performance. The last executor heartbeat's sub-millisecond cycle processed an empty feed, so it is an idle-path observation.

Offline benchmarks after the relevant fixes used disposable local fixtures:

| Operation | Samples | Median | p95 | Boundary |
|---|---:|---:|---:|---|
| Final signed authorization gate | 100, after 5 warmups | 2.292 ms | 2.405 ms | Tiny lifecycle/verifier/nonce SQLite fixtures; no broker |
| Existing exposure plus sizing, cached conversion | 100, after 5 warmups | 0.0257 ms | 0.0269 ms | Fake candidate/account, valid cached rate |
| Fastlane empty cycle after 5,000 fixture rows | 25 | 9.900 ms | 10.823 ms | No rows rescanned |
| Fastlane 10-row incremental update | 15 | 29.198 ms | 31.840 ms | Synthetic SQLite rows |
| Fastlane 1,000-row import batch | 5 | 308.746 ms | 352.971 ms | Small sample; maximum equals reported p95 |
| Integrity JSON decoding | 5 | 431.357 ms | Not estimated | Repeated reads, likely warm OS cache |

Fixture results exclude real network latency, production ledger size, worker contention and loaded broker execution. They establish the measured operations only. Conversion now expires after 15 seconds of source age; sustained entry activity may require more rate refreshes than before. Fastlane retry recovery measured approximately 10.6 ms and reread no old input rows in its fixture.

The last complete saved integrity audit took **16.125 seconds**: input snapshots 4.796 s, extended checks 10.031 s, core checks 0.563 s and assembly 0.735 s. The largest timed components were official fast mapping (3.953 s), news fastlane (3.250 s) and live forward proof (2.031 s). These are pre-repair observations. Its saved `degraded` result is retained; offline tests are not a replacement live attestation.

## Resource findings and improvement priorities

1. **Reduce repeated historical payloads in the integrity publication.** Its actual file is 98,926,690 bytes (94.34 MiB). Five reads took 27.6–33.6 ms each and decoding took 406.1–451.1 ms. Compact serialization of the V4, V2 and V3 mapping-alignment sections alone accounts for about 62.2 MB. Keep immutable detailed evidence separately with hash references, and publish bounded counts, failures and current contract checks. Acceptance should compare all verdicts and proof identities against the full version and measure total cycle time, serialized size and peak memory. Do not remove evidence checks merely to shorten the report.

2. **Measure the large integrity queries independently.** The historical timings identify where to profile, but do not establish which query/index causes the cost. Benchmark read-only coherent copies with realistic row counts, then consider incremental checks plus scheduled full reconciliation. Preserve snapshot consistency and delayed-commit rejection. Tiny fastlane fixtures cannot establish production-scale improvement.

3. **Control storage growth while preserving evidence.** Current top-level inventory found 144 SQLite files in `state` and `local_news_sentiment`, totaling 106,259,353,600 bytes (98.96 GiB), plus 998,993,520 bytes of WAL. Top-level logs contain 13,237 files totaling 41,503,063,704 bytes (38.65 GiB). This is a stated directory sample, not the entire project. Free disk space at measurement was **92.20 GiB**, above the saved 50 GiB reserve. The older storage report sampled only six databases, so its 73.29 GiB total is not directly comparable. Retention and log rotation should use verified archives and bounded output; do not delete immutable research history or run live VACUUM during this audit. No credible days-to-full forecast can be made from a stopped or five-minute zero-growth window.

4. **Improve useful collection yield before increasing frequency.** The last complete collector cycle attempted 97 of 192 configured sources, classified 1,330 items, retained 1,028 eligible items and found 1,025 duplicates, with three new inserts. Scheduling means unattempted sources are not automatically failures. Measure per-source incremental yield, cache/reclassification work and official-event capture delay before changing cadence. The new source/rank CLIs use bounded summaries in their stdout.

5. **Prioritize independent evidence and attribution.** The limiting research metric is effective episodes and valid causal entries, not the 53,357 hypothesis count. Before evaluating a future strategy, reconcile its complete cash flows and closed trades, bind every forecast to a consumer-visible publication and executable quote, and compare after-cost outcomes against its frozen no-trade and price-only controls. The existing external source gaps and independent confirmation requirements remain open.

No sustained live load test, production restart, new trade, database maintenance or performance-driven strategy change was performed. A future authorized restart must verify the final complete integrity cycle, new cohort activation, actual data availability and recovery behavior before any operational readiness claim.
