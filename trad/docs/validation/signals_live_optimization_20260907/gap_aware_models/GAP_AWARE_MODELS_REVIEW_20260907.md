# Gap-aware four-family numerical revision

`trad/oanda_gap_aware_four_family_models.py` exposes:

```python
predict_all(rows_by_pair, cutoff_epoch)
# -> {family: (expected_signed_pips, probability_up, diagnostics)}
```

Each input is a per-pair dictionary from actual UTC M1 **START** epoch to positive finite close price. The seven fixed major pairs are required, with at most 1,024 retained real rows each. The cutoff is the latest shared START timestamp, and inputs after cutoff are rejected. Every close/feature/label maturity diagnostic adds 60 seconds. Callers must reject duplicate source rows before forming dictionaries; a dictionary cannot recover discarded duplicates.

The caller independently captures exact source bytes and actual observation times. This pure numerical helper does not read files, inspect a database, observe clocks, issue forecasts, persist weights, launch workers or authorize trading. Its market cutoff is never used as an inferred availability timestamp. A later actual completion/issue/publication sequence is required in the separately registered integration.

The change preserves ridge, pooled histogram gradient boosting, lagged currency-factor ridge and EWMA hyperparameters while replacing compressed arrays with actual timestamp addressing. It requires 61 current shared closes for the original 60 return intervals, then gathers training samples from every valid retained segment, including Friday prices separated by the weekend. It never truncates retained rows by 1,024 calendar minutes, fills a gap, or turns a weekend jump into a one-minute return.

Sampling uses fixed UTC minute modulo strides of three (ridge/graph) and five (pooled HGB). A supervised row requires all 121 prices covering the 60-minute feature history and 60-minute target path. Its exact label is `price[t+3600]-price[t]`; the target cannot slide to the next available bar. Graph history additionally needs a shared seven-pair feature window. EWMA restarts at the latest EURUSD gap, using at most 256 consecutive prices. Current pooled normalization also uses only consecutive EURUSD/peer prices, with no gap bridging.

Ridge requires at least 24 rows, graph at least 32 for its 16 features, and pooled HGB at least 24 rows for each contributing pair, at least 300 pooled rows and both target classes. EURUSD must contribute enough pooled rows to receive that forecast. Insufficient models are omitted; the caller must retain whole-batch abstention when any registered family is absent. Sixty clean intervals alone do not promise all models will be available.

Validation: 48 synthetic tests passed. A fixture with 512 retained Friday prices plus 61 Sunday prices per pair returns all four families with their original minima. A fully continuous 512-price fixture matches all four original pure numerical predictions to 1e-9. Independent timestamp-window checks cover interior omissions, missing exact targets, peer gaps, fixed stride, chronological input disorder, current-window failures, malformed/future data and state resets across weekend jumps. No original worker is imported by the legacy numerical comparison.

The graph remains the old per-pair pip factor construction; this patch does not implement normalized covariance, VAR, VECM, HMM or a new probabilistic calibration. It requires a new source-bound registration and cannot upgrade old forecasts or historical proof. Synthetic correctness and latency results do not establish historical accuracy, prospective accuracy or economic edge.

Original six registered source bindings were verified unchanged in the receipt. The subtask made no runtime, database, configuration or activation changes.
