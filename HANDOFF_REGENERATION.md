# Handoff Regeneration

The lightweight handoff intentionally does not include the large H1/H4 parquet
datasets or candidate-row CSVs. Regenerate them from local candle/feature data
with:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\regenerate_handoff_artifacts.py
```

From repo root, use:

```powershell
.\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe .\trad\regenerate_handoff_artifacts.py
```

This rebuilds:

- `data/oanda_training_manager/continuous_research/technical_spike_research_1h_step1.parquet`
- `data/oanda_training_manager/continuous_research/technical_spike_research_4h_step1.parquet`
- `data/oanda_training_manager/reports/all68_latest_hgb_reversal_h1_full_trader_style_backtest/candidate_rows.csv`
- `data/oanda_training_manager/reports/all68_latest_hgb_reversal_h4_full_trader_style_backtest/candidate_rows.csv`

Useful modes:

```powershell
# Only verify what exists and write a manifest.
.\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe .\trad\regenerate_handoff_artifacts.py --verify-only

# Include M30, which is part of the newer primary live ensemble.
.\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe .\trad\regenerate_handoff_artifacts.py --include-m30

# Show commands without running heavy generation.
.\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe .\trad\regenerate_handoff_artifacts.py --dry-run --include-m30
```

Prerequisites:

- OANDA candle files under `data/oanda_training_manager/candles`.
- The model settings CSV:
  `data/oanda_training_manager/reports/all68_latest_hgb_reversal_pool_settings_technical_full_only.csv`.
- Python environment with the project research dependencies.

Each run writes a manifest under:

`data/oanda_training_manager/reports/handoff_regeneration/`

The current primary live account also uses six dual-history calibration CSVs
under `data/oanda_training_manager/reports/dual_history_live_candidate_calibration_20260705/`.
This regeneration script fixes the original H1/H4 handoff omissions and can
rebuild the M30/H1/H4 research datasets, but it does not recreate those six
live calibration CSVs by name.
