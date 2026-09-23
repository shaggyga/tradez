# Shared rolling technical dataset — September 15, 2026

This is the first implementation package authorized after the audit/reorientation. It supplies a common technical observation layer for 68 pairs. News enrichment, position-management reconstruction, model fitting and changes to trading are outside this package.

## What is implemented

| Component | Delivered behavior |
|---|---|
| Shared calculator | 216 canonical M1 fields in nine families: returns, path, OHLC, volatility, oscillator, trend, activity, spread and calendar. Historical batches and live tails use the same function. |
| Exact deduplication | Thirteen declared aliases are metadata rather than additional model columns. Related windows and correlated representations remain; statistical redundancy has not been assessed on a new training partition. |
| Currency context | Twelve separate peer fields at 1/5/15/60 minutes, constituting the tenth family. Original clocks must match exactly. Each currency leg excludes the target pair and needs two other pairs. The 15-minute return is actually 15 minutes. |
| Source adapter | Reads original CSV/Parquet OHLC and bid/ask closes. It validates identity, ordering, prices and bounded-read stability; keeps real gaps; distinguishes inferred completed CSV bars from explicit completion flags. |
| Durable observations | SQLite stores consumed inputs, receipt hashes, immutable feature values, actual first-observation and post-commit publication clocks. Revised consumed inputs or deleted overlapping bars stop that pair's append. |
| Later outcomes | Separate 5/15/30/60-minute targets, signed/absolute returns, long/short bid/ask endpoint net and close-only favorable/adverse paths. Quiet observations remain in the population. Missing targets and path gaps remain explicit. |
| Rolling operation | One writer lock; incremental publication after an initial tail bootstrap; bounded restart-context extension; source/config binding; fresh/stale/unavailable status; disk and duration limits. |

These are observed technical states, not predictions. The outcome prices are retrospective candle-close proxies, not demonstrated fills or returns after slippage and financing. The close-only path does not reconstruct intrabar stop/take-profit order or a management policy.

## Coverage and timing

The initial read is 2,048 source rows per pair and emits the latest 1,440 rows. The preceding 608 rows cover the maximum 602-row overlap. Shorter fields publish as soon as their own support exists; there is no universal 603-bar gate. A short complete source remains eligible for partial features.

Features whose elapsed window crosses a missing minute remain unavailable. Constant-input statistics can also be undefined. Therefore 216 defined fields does not imply 216 finite numbers at every minute, even for a functioning feed. Current means the original bar ended within 180 seconds at final publication; this is a dataset status criterion, not permission to trade.

Only 55 pairs can obtain all 12 peer fields under the current universe and two-peer rule. EUR_CZK, EUR_DKK, EUR_HUF, EUR_NOK, EUR_SEK, USD_CNH, USD_CZK, USD_DKK, USD_HUF, USD_MXN, USD_NOK, USD_SEK and USD_THB lack sufficient quote-currency peers. They can still have their own technical features and the available base-currency peer fields. Asynchronous or stale sources further reduce a particular panel's support. Every panel preserves its constituents, source feature hashes and original minute; its publication time is separate from the pair-local rows.

The historical CSV has no original provider receipt timestamp or retained complete flag. Historical feature availability therefore uses an explicit bar-end assumption. Actual reads and publication performed by this worker have their own clocks; a retrospective bootstrap cannot be treated as an earlier live prediction.

## Validation

The bounded all-pair historical run passed on **835,426 selected original OHLC rows**, of which **556,898 distinct sampled rows** satisfy the manifest's primary-source precedence. It sampled old Parquet prefixes, canonical CSV prefixes and recent CSV tails for each pair. Full-batch and overlapping-chunk outputs matched exact finite float64 bits and missing masks. Independent return formulas and registry alias checks also passed.

This is not full materialization or validation of the approximately 53.5-million-row archive. The historical command exports only bounded samples; date arguments filter those samples and do not turn it into a complete date-range importer. Full-archive expansion would need deliberate sharding and storage budgeting.

The production test suite covers formula oracles, causality, gaps, chunk parity, source changes, cost arithmetic, quiet/missing outcomes, restart/crash visibility, clock ordering, immutable panel generations, locks and stale output suppression. The exact acceptance receipt and current runtime observation are retained under [validation/rolling_technical_20260915](validation/rolling_technical_20260915/).

## Use and reconstruction

**Acceptance observation, 15:16:34 UTC / 11:16:34 EDT:** 96 tests passed. All 68 latest persisted feature vectors replayed exactly from their retained inputs. The final worker had 98,201 observations, 385,499 outcome records, five peer generations and zero detected revisions. Four ordinary cycles after bootstrap took 28.5–33.3 seconds. At that cutoff 65 feeds were current; EUR_TRY, TRY_JPY and USD_TRY were stale. Forty-eight pairs had all 216 technical values finite; other pairs retained explicit missingness. This is a dated observation, not a permanent health assertion.

The active root is `trad/data/rolling_technical_dataset_20260915_v3`. The two earlier acceptance bootstraps were preserved before clock-ordering repairs, with source versions and byte-verified archive receipts. Automatic approval review rejected removing their generated working folders; originals remain and reclaimed space is **zero**. The final database was approximately 510 MiB and C had approximately 59.2 GiB free at acceptance. No original historical data was deleted.

- Configuration: [rolling_technical_dataset_v1_20260915.json](../config/rolling_technical_dataset_v1_20260915.json). It declares the exact output root, 68 pip sizes and source hashes.
- Worker: [oanda_rolling_technical_worker_v1.py](../oanda_rolling_technical_worker_v1.py).
- Calculator: [oanda_rolling_technical_features_v1.py](../oanda_rolling_technical_features_v1.py).
- Peer calculator: [oanda_rolling_technical_panel_v1.py](../oanda_rolling_technical_panel_v1.py).
- Input adapter: [oanda_rolling_technical_inputs_v1.py](../oanda_rolling_technical_inputs_v1.py).
- Historical validation: [tools/build_rolling_technical_history.py](../tools/build_rolling_technical_history.py).

From `C:\Users\zmoor\Documents\forex\trad`, with the existing timeseries312 environment:

```powershell
& 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe' -B oanda_rolling_technical_worker_v1.py --config config/rolling_technical_dataset_v1_20260915.json --once
```

Omit `--once` for the bounded collector. A second writer is refused. The configured ceiling is 4 GiB for the SQLite dataset and a 32-GiB drive reserve; writes stop at the bound. The worker has a seven-day maximum invocation duration. It is not installed as a scheduled job or in the existing recovery supervisor, so machine/process interruption requires a restart. Backlogs exceeding the bounded source tail remain explicitly blocked; the sampled historical validator is not a backlog-repair tool.

The output contains `technical.sqlite`, `feature_registry.json`, `status.json`, `latest_features.json`, `owner.json` and worker logs. Open SQLite read-only while collection is active. The frozen metadata contract supplies the ordered feature names. Observation values are zlib-compressed JSON arrays; future outcomes and peer generations are separate tables. Consumers must join original pair/minute keys and respect publication cutoffs, never use outcomes as inputs or assume the latest panel and latest pair row share a minute.

## What comes next

1. Select an explicit historical development span and build bounded, source-precedence-correct rolling shards with matured targets. Do not expand the full archive by default or claim sampled exports cover it.
2. Fit any constant/correlation selection on training periods only. Preserve feature families and specialist identities; do not treat 228 fields as independent signals.
3. Compare the materially changed family/interaction inputs with the nearest existing additive, gradient-boosting and specialist/gating experiments. The model-reuse register remains the starting point; these families and earlier conditional/joint experiments have already been explored. The repaired dataset does not make a repeated model family a new hypothesis by itself.
4. Evaluate direction, movement size, calibration, coverage and executable costs on matched later rows, with uncertainty and separate confirmation. Neither crossing 50% accuracy nor a favorable discovery subgroup is sufficient profitability evidence.

The new finite-window EMA and missingness semantics are deliberately versioned. Existing trained weights are not compatible merely because feature names resemble their old inputs. No new model has been trained or promoted by this package.

The separate existing operational watchdog/source-binding failure observed during the read-only survey is not repaired by starting this additive collector. This document does not certify the whole trading system or supersede unrelated gaps in the [pending queue](../FOREX_PENDING_IMPROVEMENTS.md).
