# Research feature capture and forward evaluation connections

September 13, 2026. This follows the [feature observation repairs](FOREX_FEATURE_MOVE_REPAIRS_20260913.md). It implements the missing research-only producer and subsequent-outcome connection. Validation uses newly created synthetic files; no market archive, model artifact, account, credential or existing runtime ledger was opened. No service was started and no trading configuration was enabled.

## Implemented connections

The research supervisor includes a dedicated feature-observation producer and forward evaluator. It does not need to start the legacy strategy-lab trading loops. The producer reuses the original source-verified structural, cross-sectional and microstructure calculations plus the actual 643-field causal M1 MA vector. Other timeframes retain structural fields with their own native bar clocks. Source definitions and exact implementations identify each observation cohort. Existing fitted models do not silently receive new columns.

The producer reads bounded local dedicated quote snapshots and native M1/M5/H1 candle tails, then writes the immutable observation archive. It retains pair exclusions, missing families, source receipts and source ages. The new quote-only component-clock option prevents a fresh spread from being hidden behind an old candle; features that require a candle remain subject to candle freshness. Old snapshots without the option retain their original normalized representation.

The mapper exposes all comparisons before its display limit and shares one bounded archive read, frame validation and feature-entry preparation across the three windows. Every source envelope is hash checked and its normalized frame recreated. Reuse is confined to one call, with no cache carrying observations between decisions. Quiet controls, excluded features and archive sampling remain visible.

The new ledger freezes complete 5/15/60-minute comparison populations every five minutes and evaluates subsequent 5/15/60-minute outcomes. Its [protocol](FOREX_FEATURE_FORWARD_PROTOCOL_20260913.md) declares the exploratory alert threshold, minimum history, fixed magnitude threshold, observation and publication clocks, costs, missing outcomes and storage rules before any cohort activation. Feature sign is not converted into a trade direction. Long and short executable probes are reported separately using the existing exact decimal bid/ask scorer and additional cost stress.

All admission requires the existing strict verified-clock contract. A later replacement clock file cannot rescue an initial proof that has aged out during calculation or publication. Publication and outcome timestamps describe the actual owner-observed stages. Restart retains clock high-water marks, refusal diagnostics and immutable outcomes. A held OS lock prevents another ledger owner from observing an incomplete publication sequence.

## Validation and resource measurements

The connection test uses actual reused calculators, newly written archives, the shared mapper, dedicated-quote file reader, verified-clock file reader and forward worker. It then supplies strictly later synthetic entry/target quotes. It checks a large move, a quiet move, missing targets, all comparison rows beyond the displayed 50, separate long/short costs and restart persistence. These are software checks, not historical or live performance results.

Final source hashes, test results and measured full-universe resource bounds are recorded in [connection validation](../../feature_forward_connections_20260913/CONNECTION_VALIDATION.json). Earlier failed source generations and fixture failures remain in the same audit workspace or linked peer receipts. Implementation and tests are tracked in Git; local receipts remain beside the project.

The final integrated run passed **340 tests**, including the 13 independently authored forward-boundary checks rerun against the final sources. Nine additional subtests passed. The 68-pair, 1,024-row-per-native-timeframe file fixture completed capture and publication in 26.57 seconds with all 643 rich M1 fields per pair. Its archive was approximately 2.19 MB compressed; at one archive per minute that fixture projects approximately 2.94 GiB/day. The supervisor declares a 4 GiB daily observation cap, a 4 GiB total forward-ledger/sidecar cap and a 4 GiB free-space floor. Compression, contention and real missingness can differ; elapsed bounds and refusals remain explicit.

The final eight-frame rich-reader check took **16.84 seconds**, versus the previously measured 52.73 seconds. All 61,948 comparisons per window were byte-identical to the pre-optimization calculation for 5/15/60 minutes, and all eight retained synthetic frames recreated identically. This is one synthetic performance measurement, not a live latency guarantee.

## What remains

Live activation and operational verification remain open. The existing operational checkpoint records a blocked clock-monitor activation; this work does not retry or bypass it. Source wiring and passing synthetic checks cannot establish that current feeds or services are running.

Native M1 refreshes currently take roughly four to five minutes for a full market pass. Rich candle features are therefore intermittently unavailable under their existing freshness rules; fresh quote components remain separately observable. A genuinely continuous completed-minute producer is separate work. The new producer also reports missing verified-news fields explicitly; the existing news research path and a future verified-news adapter are not replaced by invented values.

Historical omissions cannot be backfilled as information known earlier. The bounded observation reader may sample dense archives, and missing history remains unranked. Storage caps refuse admission without deleting evidence; they are not a guarantee of indefinite collection. Newly proposed consensus, policy-expectation and options feeds have not been purchased or connected.

The objective remains the strongest whole setup: movement opportunity, direction, timing, execution costs, calibration and position management. [Current evidence and complementary signal sources](FOREX_CURRENT_SETUP_AND_SIGNAL_SOURCES_20260913.md) records which earlier work to reuse and the new information types to investigate. No new profitable model or independent predictive advantage is claimed here.
