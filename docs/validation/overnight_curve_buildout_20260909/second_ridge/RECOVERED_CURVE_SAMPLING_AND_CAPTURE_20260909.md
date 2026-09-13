# Recovered S5 curve: sampling, target and capture correction

This addendum supersedes the earlier strict-grid-only description and exact-target wording in `RECOVERED_SECOND_CURVE_BUILDOUT_20260909.md`. The earlier receipts and test runs remain unchanged. The recovered model and both original model sources also remain unchanged.

The original `oanda_second_forecast_fit.py:229` selects 13 real rows whose first-to-last label span is between 55 and 70 seconds. Its 14 features use record-count windows; it does not require every interior interval to be five seconds. The new adapter retains `exact_grid` as an explicit narrower policy and adds `retained_fit_window`. Neither inserts bars or compresses their clocks. Duplicate, reordered, non-S5-grid and incomplete rows still reject. With 13 unique grid labels, the admitted span is 60, 65 or 70 seconds; the retained original 55-second lower threshold does not authorize duplicate/off-grid bars.

Every capture records adjacent intervals, missing-interval counts and each feature's actual temporal support. A feature named `return_60_pips` remains the original 12-increment formula, while its metadata shows whether the actual elapsed span was 60, 65 or 70 seconds. This preserves the trained transform without claiming that nominal feature names always equal actual elapsed time.

The original target is also more specific than an exact future timestamp. `fit_horizon`, starting at line 663, computes `reference_label + horizon`, selects the first real bar at or after that timestamp with `searchsorted(..., side='left')`, and admits a delay between zero and seven seconds inclusive. This is timestamp-based, not a row-count shift. Source-extracted tests accept delays of 0, 5 and 7 seconds and reject 8 and 10 seconds without invoking fitting. On official S5-aligned labels, the admissible observed delays are normally 0 or 5 seconds.

For reference label `t` and native horizon `h`, the output preserves:

- Reference label `t` and completed-price clock `t+5`.
- Nominal target label `t+h` and nominal target-price clock `t+h+5`.
- Original admissible target-label window `[t+h, t+h+7]` and price-clock window `[t+h+5, t+h+12]`.
- A structured `target_selection_policy`, with actual future endpoint fields null until observed.

An exact nominal outcome view is a declared narrower evaluation. Management using a fixed nominal deadline for a model trained with a target window is an explicit approximation; the generic curve contract and replay now retain that distinction. Neither target may be reset to publication time plus the horizon.

Historical midpoint ingestion remains unresolved. Some retained parquet midpoints differ from the arithmetic bid/ask midpoint. The current historical downloader alone does not prove how those older rows were created. The two new research interpretations therefore remain separate: original official midpoint fields, and midpoint fields derived from exact bid/ask arithmetic. Neither is described as proven identical to the historical training source.

One previously authorized EUR/USD MBA response was actually received at **2026-09-09 04:52:13.345040 UTC**. Its latest 13 complete rows span 70 seconds, with omitted intervals at 04:51:10 and 04:51:30. Both interpretations now produce all 13 native horizon nodes twice with identical numerical results. Reference midpoint is 1.16316 for official M and 1.163155 for BA-derived. These computations happened later in engineering scope; they were not issued and are not prospective performance evidence. No additional request was made for this comparison.

The new `oanda_s5_mba_research_capture_v1.py` is a separate, reviewed acquisition/mapper component. `capture_once` performs at most one practice-host candle GET for EUR/USD, GBP/USD or USD/JPY, with 120 MBA S5 candles and smoothing disabled. It returns raw bytes and a sealed receipt, or empty bytes and a sanitized failure receipt. It writes no files, selects no account and places no orders. Redirects and automatic retries are disabled. The response limit is 256 KiB, with a 15-second acceptance budget and separate connect/read timeouts; that is not a hard cancellation guarantee for every possible DNS or stalled-stream condition.

`map_verified_capture` replays the raw response, source and receipt hashes, exact registry metadata, request contract, original request/read clocks, complete/incomplete counts, full ordered OHLC and coverage. It returns all complete rows with exact price strings, incomplete rows separately, and the last 13 interpreted feature rows when available. BA and spread arithmetic uses an independent 192-digit Decimal context before float conversion for features; a caller's low Decimal precision cannot alter the mapping. Original official M/B/A strings remain available for outcome scoring.

Coverage is limited to the first and last returned complete labels. An omitted interval remains missing, and neither a candle nor this receipt proves current tradeability. The outcome helper receives a raw/receipt-bound source-query attestation and actual original read completion. Execution/entry eligibility requires independently observed pricing evidence elsewhere. All account, order, promotion, authorization, execution and proof flags are false.

Validation and review:

- Recovered adapter: **59 passing tests**, including actual regular and irregular retained-input compatibility, all 13 original coefficient calculations, original target-prefix behavior and clock/integrity failures. Source SHA `dbc2d606afdeb23840dd9038cd1200301c2211abb4228ef9bab193763323f41d`.
- New capture/mapper: **58 passing tests**, including malformed/altered source data, exact coverage, HTTP failures, response budgets, low-precision independence and unchanged actual response parsing. Source SHA `38fe20f4e55d970f074c71f7acee5f5adaebc9ac7766a532efcd782b34abbb45`. Its HTTP tests are mocked; this new 120-candle module has made no live request at this acceptance.
- Independent source reviews closed the sampling/target, capture, bridge and outcome boundaries. Reviews are not counted as additional tests.

Receipts: `RECOVERED_SECOND_CURVE_SAMPLING_TARGET_ACCEPTANCE_20260909.json` (`1ed483669a7a25ba04c5d69dd14624c621f1ec8e2198d4836c032ebb0688fa5e`), `S5_MBA_CAPTURE_IMPLEMENTATION_VALIDATION_20260909.json` (`aa10f56a7dd03c0116a5e4ca46a86fdcdc11ca02aed1162b4d85c68c2adf0426`), and `S5_CAPTURE_INDEPENDENT_SOURCE_REVIEW_20260909.json` (`6b6533c5df45955c1c129d85ec0b37a7feceee7afb0b16d540ac21195b569e70`).

These results establish source/transform/inference compatibility and a reviewed data boundary. They do not establish forecast calibration, out-of-sample edge, management profitability or trading readiness. The original S1 live transform remains different from the S5 fit transform, and no historical availability is reconstructed from later reads.
