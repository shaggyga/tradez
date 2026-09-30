# News/technical timing repair

The successor `trad/oanda_rolling_technical_worker_v3.py` reuses the pinned V2 calculator/store and original single-writer lock. It probes local candle-file metadata once per second and processes changes or minute boundaries. This removes the fixed additional 60-second sleep; it is not an OS notification or a subsecond provider feed. The last published dataset remains readable while the next cycle runs. Total latency can still exceed one second during long cycles or before upstream files are written.

`latency_current.json` reports candle close, source read completion, feature publication, trigger detection and cycle completion. Read completion is not an upstream-arrival timestamp; unknown upstream timing stays null. No source collector, account, broker or live process was changed.

## Availability join and evaluation

`trad/oanda_news_technical_timing_v1.py` selects the latest row whose publication and read clocks precede the decision and whose completed bar is at most 180 seconds old. Backfilled old rows, unpublished/future rows and revision-tainted pairs are refused. It preserves older available snapshots instead of using newer unseen ones. This is an explicit new diagnostic path, not an automatic migration of legacy watchlist/model consumers.

The report command uses the later of stored news first-seen, classification-available and causal-known clocks. These are recorded clocks, not independently re-attested here. Classification lag may include later reclassification. The report retains original publication eligibility and does not convert withheld news into a forecast.

Outcomes start at the first completed minute close at/after decision (maximum wait 60 seconds), then evaluate exact gap-free paths at 5/15/30/60 minutes. Both long/short bid/ask endpoint cost proxies are reported. These are retrospective proxies, not fills, live predictions or proof of alpha. Slippage and financing remain unmodeled.

## Corrected evidence

`evidence/news_technical_timing_v1_20260930/corrected_report_final.json` supersedes the earlier informal pre-publication comparisons. Reproduce the argv in `command_final.json` with a NEW output path. SQLite is opened read-only; original archives and stored outcomes remain untouched.

The Fed-hike row's stored classification was approximately 69 minutes after first capture; hawkish-Fed approximately 20 minutes. EUR/CAD is excluded because contemporaneous technicals were unavailable. GBP/USD uses the earlier published snapshot, not the newer candle that had yet to publish. All four original news records were withheld from directional publication. These are illustrative diagnostic cases, not strategy performance.

EUR/USD archive counts for a fixed absolute 10-pip move over 15 minutes, greedy nonoverlapping episodes: Sep 14: 5, Sep 15: 0, Sep 16: 6, Sep 17: 3. UTC start dates; observed bars respectively 1136,1426,1436,815. Incomplete coverage and gaps prohibit interpreting these as complete-day opportunity rates. Endpoint moves do not count every intrawindow excursion. These hindsight-selected episodes must not be used as a predictive test cohort.

## Verification and rollout

35 targeted/regression tests plus 2 subtests passed. After adding an entrypoint integration test, all 10 timing tests passed; that test invokes the real calculator, SQLite store, file outputs and telemetry on synthetic input. Existing V2 source-pinned configuration validates. Review is same-task, not independent.

Runnable successor (not launched):

```powershell
& 'C:/Users/zmoor/AppData/Local/CodexRuntimes/timeseries312/Scripts/python.exe' -B trad/oanda_rolling_technical_worker_v3.py --config trad/config/rolling_technical_operations_v2_20260916.json --duration-sec 3600
```

Before rollout reconcile the current deployment preflight and technical-worker ownership; stop any previous technical writer gracefully, keep the owned source collector unchanged, then use the successor and verify fresh outputs/latency. Its lock refuses a duplicate writer. The existing operations stop-request file remains honored. No automation or service installed. Global preflight still reports source/revision and Vault pointer-schema/coverage mismatches; this repair does not mark that gate passed.

Source, tests, local logs and exact evidence are checkpointed in Vault `NEWS_TECHNICAL_TIMING_V1_20260930`, with scoped pointer `NEWS_TECHNICAL_TIMING_LATEST.json`. Legacy sealed numerical source contracts and forecasting queue remain unchanged.
