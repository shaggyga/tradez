# Final 2,880-cycle cadence evidence and remaining capacity gap

The publication/acknowledgment loop completed, and retained data survived an independent process restart with exact historical selection. The complete ordinary-read capacity gate did **not** pass: the original test's final consumer preparation exceeded its unchanged 30-second deadline. The later diagnostic pass does not erase that failure.

The workload used actual source/classification ledger writers and 2,880 actual publisher/consumer commits under monotonic synthetic 60-second clocks. It was one canonical event revised repeatedly, with a 192-source policy. It is a cadence/receipt-volume scenario, not 48 hours of real-time live monitoring and not the combined 12,000-distinct-projection plus 2,880-observation workload.

All six durable counts were independently read through SQLite read-only connections and equal 2,880: publisher attempts, published records, publisher acknowledgments, evidence objects, consumer observations and consumer acknowledgments. No receipt or source record was imported, deleted, backdated or rewritten.

| Measurement | Result |
| --- | --- |
| Publication/ack loop | 2,880 completed; 10,128.812 seconds |
| Largest single publish | 7.578 seconds |
| Largest single consumer acknowledgment | 4.687 seconds |
| Original final ordinary consumer read | **Failed: `consumer_owned_preparation_time_bound`** |
| Original run total | 10,164.562 seconds; exit 1 |
| Separate-process cold publisher | 66.625 seconds |
| Actual publisher read transaction | 0.266 seconds, one attempt; closed before semantic validation |
| Fresh-process consumer read/preparation after publisher warmup | 27.656 seconds |
| Fresh-process first/latest as-of parity | Passed; sequences 1 and 2,880, original first-seen clock unchanged |
| Fresh-process total / CPU / peak RSS | 94.485 seconds / 77.766 seconds / 325,742,592 bytes |
| Before/after publisher and consumer database SHA256 | Identical |
| Original evidence bytes checked | 81,599,022 |
| Expanded scan work checked | 204,917,985 bytes |
| Compact manifest at 2,880 observations | 30,211,320 bytes |

Both runs used BelowNormal process priority and the unchanged V4 core source hashes, which remain the same core bytes in the later IO/source stages. No timing limit, data cap or validation guard was increased. The original run's recorded peak RSS of 633,675,776 bytes was sampled at progress saves and is not a continuous peak measurement; the separate-process peak was sampled every 100 ms.

Evidence locations:

- `compact_news_candidate_v4/publisher_cadence_2880_001/report.json`: original failed complete gate, with successful durable cycle count.
- `compact_news_candidate_v4/fresh_cadence_2880_diagnostic_001/report.json`: separate read-only restart and historical-selection proof.
- `compact_news_candidate_v4/run_publisher_cadence.py`: original workload harness.
- `compact_news_candidate_v4/check_cadence_fresh_process.py`: independent diagnostic harness; it derives configuration from the retained immutable profile and does not modify the failed fixture.

The next bounded repair is consumer preparation across many retained receipts. A verified immutable consumer-prefix cache or equivalent incremental validation can reuse already validated original receipt/scan facts only after each actual read compares the complete prior stored bytes and identities. Original publication/consumer clocks, latest-revision ordering, source hashes, current-failure invalidation, scan freshness and full replay must remain intact. This is a proposed seam, not an implemented or measured fix.

Acceptance must include both the full 2,880-observation consumer history and the complete ordinary IO capture, including archive readback. The separate earlier 12,000-projection/193-observation capture pass and this restart result must not be combined into a claimed 48-hour operational certification. A final combined-content-and-receipt workload is still missing. Existing finite bounds fail closed; this document does not claim forecasts, trading qualification or profitable model performance.
