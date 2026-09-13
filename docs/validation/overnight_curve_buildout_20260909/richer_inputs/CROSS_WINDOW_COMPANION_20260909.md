# Three-pair cross-window feature companion

This new, unregistered helper captures real 1-, 15-, and 60-minute market changes for EUR/USD, GBP/USD and USD/JPY. It produces sealed features and coverage evidence only. It has no model fit, input injection, forecasting, broker, or worker path.

The implementation is `C:/Users/zmoor/Documents/forex/trad/oanda_cross_window_currency_capture_v1.py`. Its companion test file is `test_oanda_cross_window_currency_capture_v1.py`. The final focused run passed 47 cases. Initial 42-case XML is retained, including the first run's numeric-string formatting assertion failure; the final source-bound receipt identifies the authoritative 47-case run.

## Input and feature contract

`capture_sources(candle_root, clock=...)` reads each fixed `{PAIR}_M1.csv` once. It retains at most 512 complete rows from a 256 KiB tail per pair, the original header/tail bytes and their SHA-256, byte offsets, actual read start/completion clocks and source path. The checksum is explicitly for the retained bytes, not the whole source file. Changed, incomplete, missing or reparse-point sources are refused. The capture itself is sealed and bound to the implementation SHA and the audited definition sources.

`build_features(capture, common_price_epoch=..., consumer_epoch=..., expected_source_sha256=..., maximum_age_sec=900)` is pure replay. The caller declares one whole-minute price cutoff and its actual consumer time. Each close is located at its original archived M1 label plus 60 seconds. The caller's consumer time is the availability of this observation; it is not proof that the retained archive rows were available historically. The optional age policy may be made stricter, but cannot exceed 900 seconds.

Every 1/15/60-minute return requires its exact start and end timestamps. The ATR normalizer requires 15 consecutive real M1 OHLC rows, producing 14 true ranges that include each prior close. An absent endpoint is never replaced with a shorter return. A gap in ATR support withholds normalized features while retaining an otherwise available raw endpoint change. Future data at its original source-read time remains invalid even if replay happens later.

The signed normalized return is `price delta / max(ATR14, 0.1 native pip)`, clipped to [-10, 10]. The base currency receives the positive leg and the quote currency the negative leg. Currency strength is the mean of observed legs; ranks use average ties and only the available three-pair denominator. Dispersion uses the sample denominator. Pair and currency breadth exclude the pair itself. `strength_gap15_x_strength_gap60` is an explicitly named product of two available strength gaps.

These definitions reuse the audited unified builder's return/ATR, currency-strength, breadth, rank and dispersion formulas, plus the audited interaction pattern. They do not reproduce its full 68-pair ranks or 795-column matrix, float32 representation, historical fallback behavior, model coefficients or predictive results. Exact source paths, hashes and line references are retained in every feature record.

USD is shared across all three pairs: EUR/USD and GBP/USD contribute negative USD legs; USD/JPY contributes a positive USD leg. EUR, GBP and JPY each have only one observed pair, so their leave-self-out currency breadth has no independent peer and is explicitly unavailable. Strength still includes the own-pair return, which is disclosed. Neither three pair rows nor overlapping windows are three independent trials; this helper estimates no correlation coefficient or confidence.

## Actual retained observation

At **05:45:27 UTC on September 9**, the single authorized bounded current M1 capture retained 512 rows for each pair. The latest exact common price close was **05:41:00 UTC**, **267.764 seconds** before the consumer observation. All three pairs had all three required endpoints and the complete ATR support: **9 of 9 pair/window features available under the declared 900-second feature age ceiling**.

The capture took 19.38 ms and pure replay took 86.71 ms on this machine. The canonical retained capture was 382,733 bytes. This one measurement is an engineering observation, not a throughput benchmark, a fresh short-horizon forecast, or proof of improved prediction accuracy. Original price/read/consumer clocks and all derived values are in `CROSS_WINDOW_ACTUAL_FEATURES_20260909.json`; compact measurements are in `CROSS_WINDOW_ENGINEERING_OBSERVATION_20260909.json`.

The raw retained market CSV capture stays in the private `cross_window_private_001` directory. Compact results contain market feature values and provenance, with no news bodies, account identifiers or credentials. Before/after checks found all 14 newly registered pilot source files unchanged. No registered sources, runtime processes, model registrations or trading controls were changed by this work.

## Integration boundary

A future companion caller must preserve these actual clocks, supply the externally expected implementation hash, select and retain its common-cutoff rule, and carry all missingness and three-pair scope. Any fit or forecast that uses these changed inputs needs its own retained input and evaluation contract. Existing model coefficients must not receive these fields under an old feature-schema claim. Runtime adoption remains a separate parent-owned decision.
