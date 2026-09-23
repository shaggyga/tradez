# Forex entry audit and missing improvements — September 6, 2026

The saved records support zero position entries in the last completed FX week, August 30 at 21:00 UTC through September 4 at 21:00 UTC (Sunday–Friday, 5 p.m. New York). The executor ran in six observed sessions. All 510 valid account snapshots inside that interval, plus snapshots bracketing it, retain transaction ID 2461 and balance/NAV 41.6042. The retained submission ledger has no attempts during the week; its latest submission is August 4. These are local retained records, without a new broker request.

The immediate blockage was at the final entry rules. It was not simply an empty feed:

| Saved observation | Count and scope |
|---|---|
| Selection summaries | 6,997, sampled at least 60 seconds apart |
| Feed candidates across those summaries | 2,440,194 repeated observations, not unique forecasts |
| Qualified candidates across those summaries | 1,276 repeated observations |
| Nonconflicting qualified candidates in those summaries | 1 observation |
| Final-selection skip logs | 6,099, independently rate limited |
| Direction-conflict reasons in skip details | 7,972 candidate observations, 1,386 distinct aggregate IDs |
| Movement-to-cost reasons in skip details | 6 candidate observations, 2 distinct aggregate IDs |
| Retained weekly submission attempts / position entries | 0 / 0 |

These rows have different sampling intervals and are not a unique-signal conversion funnel. Two nonconflicting candidates appear in the more frequent skip details. CHF/JPY sell projected 3.6 pips of movement against a 2.9-pip spread, below the required 2× movement/spread ratio. AUD/JPY buy projected 2.5 pips against a 2.0–2.1-pip spread and also failed. Their current projected net moves were below the 1-pip requirement. A larger historical calibrated estimate does not make the current move clear its spread.

The conflict veto includes opposing contributors for the same instrument and preferred horizon, including zero-weight or research-only contributors. The source explicitly retains them for vetoes. The old weekly logs omit sufficient contributor lineage to determine how many conflicts came only from those rows. That is a research priority, not evidence for removing the gate. Eligibility-aware consensus and replacement of stale model versions need an offline comparison with complete availability clocks before any execution-policy change.

There were also receipt gaps of about 2 hours 26 minutes and 7 minutes 48 seconds. They do not establish how many opportunities were missed. At five restarts, 55 old exits were replayed, producing 275 recovery observations; these must not be counted as weekly exits. Detailed evidence and limitations are in [the week audit](FOREX_WEEK_ENTRY_AUDIT_20260906.md) and `FOREX_WEEK_ENTRY_AUDIT_20260906.json` at the source root.

## Implemented in this pass

**Entry diagnostics.** Selection logs now explicitly distinguish local candidate generation from the shared feed, label their sampled scope, and identify selection before final entry rules. Final rejection logs retain complete reason counts before truncating examples to twelve, and say whether the authorization stage was reached. The legacy reason is preserved for compatibility. Dashboard payload diagnostics are covered separately in the validation receipt; no dashboard or executor was started. This is better sampled telemetry, not a durable count of every cycle.

**Exact-price scoring.** `oanda_exact_price_scoring.py` uses supplied decimal prices to distinguish up, down and unchanged midpoints, with separate forecast-reference and executable-entry returns. It preserves emitted direction, explicit pip size, bid/ask costs and cost stresses. Labels and wins use exact price differences; derived ratios use 80 decimal digits. `oanda_fixed_forecast_evaluation_exact.py` is a separate offline evaluator with exact quote/reference checks and exact rolling-baseline labels, while retaining the prior clock/identity rules. Its JSON prices and metrics are explicit decimal strings. No registered worker or historical forecast was rewritten.

The new scorer reproduces the prior independent correction on all 8,414 retained broad rows: **4,148 direction hits (49.30%), 296 flats and 716 positive after spread (8.51%)**. The old stored total was 4,156 hits; eight were floating-point-only artifacts. This fixes arithmetic, not prediction quality. Genuine first-known clocks remain missing from that history. Integration into a future registered producer/evaluator remains pending, including exact original reference-price anchors; the existing study must not silently acquire a new scorer.

**Joint-predictor baselines.** `oanda_joint_return_baselines.py` compares three fixed EUR/USD regressions on identical matured training rows: own trailing returns, own plus six peer pairs, and own plus a peer dollar factor excluding EUR/USD. USD-first pairs are inverted consistently. Inputs require actual synchronized completed M1 bars and actual availability times, with no filling across gaps. Scaling uses training rows only. Defaults are trailing 1/5/15/60-minute log returns, one-hour target, ridge alpha 10, 120–4,096 training rows. This is direct-horizon multivariate lag regression; recursive VAR, VECM/cointegration, fitted dynamic factors/HMM and a dependence-aware ensemble remain separate gaps.

All joint-baseline validation so far is engineering or synthetic. A three-arm fit on 600 synthetic common minutes took a median 0.0435 seconds over three runs; that is not a measured speedup of the project. Archived candle close times cannot supply their missing original availability clocks. No real-market accuracy gain, after-cost improvement or account eligibility has been established.

## Validation and disposition

`FOREX_ENTRY_IMPROVEMENTS_VALIDATION_20260906.json` binds the final sources, guarded test results, archived replay and audit evidence. The tests cover exact flats/tiny moves/exotic pips, clocks and leakage, synchronized training, final entry blockers and the bounded dashboard diagnostic path. Five existing quote-publisher fixtures require background threads and were excluded from the final strictly stopped test selection after the guard blocked them; the initial result is preserved. No external action or production database write was allowed by the test harness.

The current registered six-file study source and registration remain unchanged. All project workers remain stopped and both Forex scheduled tasks are disabled. The source and audit changes are on **C:\Users\zmoor\Documents\forex\trad**. The vault is a derived local audit/source archive; D-drive artifacts remain historical evidence only. [The follow-up record](../FOREX_ENTRY_RESEARCH_FOLLOWUP_20260906.json) records remaining research, integration and operational work with their acceptance conditions.
