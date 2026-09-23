# Moving-Average Crossover Audit Overlay

This overlay supplements the vault base checkpoint identified in
`CHECKPOINT_LATEST.json`. It does not replace that checkpoint.

## Scope

- 68 OANDA forex instruments.
- Full-history run: 49,811,888 M1 rows and 10,440 configurations.
- Exact-cost run: 1,814,626 bid/ask M1 rows and 2,400 configurations.
- Signal timeframes: M1 through H4.
- Outcome horizons: 1 minute through 24 hours.
- Moving averages: SMA/SMA, EMA/EMA, SMA/EMA, and EMA/SMA.
- Confirmations: cross-only, slope, dual-slope, volume, and higher-timeframe
  alignment.
- Chronological development, validation, and holdout segments.
- Completed-bar signals, next-observed-M1-open entries, and spread-aware exits.

The full-history run produced all 10,440 configured variants. The shorter
exact-cost panel produced results for 2,288 of 2,400 variants; the other 112 had
no eligible exact events. All 68 instruments loaded successfully in both runs.

## Decision

No moving-average crossover is approved as a standalone fast-entry signal for
account `-007`.

No 1-240 minute configuration passed the development, validation, and holdout
requirements on the recent exact-cost panel. Twelve 24-hour rows survived, but
their holdout counts were only 25-86 and several are correlated or equivalent.
They may be evaluated as shadow higher-timeframe context features only.

The current M1 EMA 3/9, 5/13, 8/21, and 12/26 profiles were pair-weighted net
negative after exact spread at every tested horizon.

## Restore

Overlay the `source/` tree onto the extracted base checkpoint, preserving
relative paths. The `reports/` tree contains compact reproducibility artifacts;
raw market data is intentionally omitted.

Run:

```powershell
python -m unittest -v test_oanda_moving_average_crossover_sweep.py
python -m pytest -q test_forex_model_vault_sync.py
```

The run commands and complete experiment contracts are in each report's
`manifest.json`. Read the full-history `ASSESSMENT.md` before interpreting
ranked CSV files.

## Safety

- Credentials included: no.
- Raw market data included: no.
- Account wiring changed: no.
- Model promoted: no.
- Account processes started by this audit: no.
